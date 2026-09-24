// ══════════════════════════════════════════════════════
// FILE: server.js (v197)
// UPLOAD TO: GitHub repo "krw-backend"
// PURPOSE: KRW Lead Intake + Call Revenue tracking
// ══════════════════════════════════════════════════════

require('dotenv').config();
const express    = require('express');
const { Pool }   = require('pg');
const path    = require('path');
const fs      = require('fs');
const app     = express();

app.use(express.json());
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

const PORT     = process.env.PORT || 3000;
const API_KEY  = process.env.API_KEY;
const LEAD_KEY = process.env.LEAD_API_KEY;
const pool     = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

// ── Auth middleware ───────────────────────────────────
function requireKey(req, res, next) {
  const key = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  if (!API_KEY || key !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

function requireLeadKey(req, res, next) {
  const key = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  // Accept either the lead key or the main key
  const valid = [API_KEY, LEAD_KEY].filter(Boolean);
  if (!valid.includes(key)) return res.status(401).json({ status: 'rejected', reason: 'Invalid API key' });
  next();
}

// ── Email notifications ───────────────────────────────
// Uses Resend API — no SMTP, works reliably from Railway
async function sendEmailNotification(subject, html) {
  try {
    if (!process.env.RESEND_API_KEY) {
      console.log('RESEND_API_KEY not set — skipping email notification');
      return;
    }
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${process.env.RESEND_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        from:    'KRW Dashboard <onboarding@resend.dev>',
        to:      [process.env.NOTIFY_EMAIL || 'kyler@leadbloom.co'],
        subject,
        html,
      }),
    });
    const d = await res.json();
    if (res.ok) { console.log('Email sent:', subject); return; }
    console.error('Resend error:', JSON.stringify(d));
  } catch(err) {
    console.error('Email send failed:', err.message);
  }
}

// ── DB init ───────────────────────────────────────────
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS calls (
      id              SERIAL PRIMARY KEY,
      received_at     TIMESTAMPTZ DEFAULT NOW(),
      call_date       TEXT,
      call_datetime   TEXT,
      call_duration   INTEGER,
      vertical        TEXT,
      campaign        TEXT,
      campaign_id     TEXT,
      buyer_name      TEXT,
      buyer_id        TEXT,
      supplier_name   TEXT,
      caller_id       TEXT,
      caller_name     TEXT,
      publisher_sub   TEXT,
      payout_amount   NUMERIC(10,2),
      revenue         NUMERIC(10,2),
      cost            NUMERIC(10,2),
      profit          NUMERIC(10,2),
      billable        BOOLEAN DEFAULT NULL,
      call_status_label TEXT DEFAULT 'pending',
      disposition     TEXT,
      call_status     TEXT,
      invoice_status  TEXT DEFAULT 'pending',
      invoice_id      TEXT,
      invoice_date    TEXT,
      paid_date       TEXT,
      source_system   TEXT DEFAULT 'partner',
      raw             JSONB
    );
    -- Add columns if upgrading existing table
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS caller_name   TEXT;
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS publisher_sub TEXT;
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS source_system TEXT DEFAULT 'partner';
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_status_label TEXT DEFAULT 'pending';
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS caller_name TEXT;
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS publisher_sub TEXT;
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS campaign TEXT;
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_status_label TEXT DEFAULT 'pending';
    -- Set existing calls that have billable value to correct label
    UPDATE calls SET call_status_label = CASE WHEN billable=true THEN 'cpa' WHEN billable=false THEN 'not_converted' ELSE 'pending' END WHERE call_status_label IS NULL OR call_status_label='pending';
  `);
  console.log('✅ DB ready');
}

async function initLeadsDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS leads (
      id               SERIAL PRIMARY KEY,
      received_at      TIMESTAMPTZ DEFAULT NOW(),
      campaign         TEXT NOT NULL,
      vertical         TEXT,
      status           TEXT DEFAULT 'received',
      first_name       TEXT,
      last_name        TEXT,
      email            TEXT,
      phone            TEXT,
      street           TEXT,
      city             TEXT,
      state            TEXT,
      zip              TEXT,
      notes            TEXT,
      trusted_form_url TEXT,
      jornaya_id       TEXT,
      facebook_lead_id TEXT,
      publisher_sub    TEXT,
      websource        TEXT,
      zapier_status    TEXT,
      buyer_intake_id  TEXT,
      buyer_error      TEXT,
      raw              JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_leads_campaign ON leads (campaign);
    CREATE INDEX IF NOT EXISTS idx_leads_received ON leads (received_at);
  `);
  console.log('✅ Leads table ready');
}

// ══════════════════════════════════════════════════════
//  LEAD INTAKE
// ══════════════════════════════════════════════════════

// POST /lead/:campaign - receive lead from publisher
// Stores it and fires to Zapier webhook if configured
app.post('/lead/:campaign', requireLeadKey, async (req, res) => {
  const campaign = req.params.campaign.toLowerCase();
  const b        = req.body || {};

  // Validate required fields
  const required = ['firstName', 'lastName', 'email', 'phone'];

  const phone = String(b.phone).replace(/\D/g, '');
  if (phone.length < 10) {
    return res.status(422).json({ status: 'rejected', reason: 'Phone must be 10 digits' });
  }

  // Store lead
  try {
    const r = await pool.query(`
      INSERT INTO leads (campaign, vertical, status, first_name, last_name, email, phone,
        street, city, state, zip, notes, trusted_form_url, jornaya_id,
        facebook_lead_id, publisher_sub, websource, raw)
      VALUES ($1,$2,'received',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
      RETURNING id
    `, [
      campaign,
      b.vertical || campaign,
      String(b.firstName).trim(),
      String(b.lastName).trim(),
      String(b.email).trim().toLowerCase(),
      phone,
      b.street  || null,
      b.city    || null,
      b.state   || null,
      b.zip     || null,
      b.notes   || null,
      b.trustedFormCertUrl || null,
      b.jornayaLeadId      || null,
      b.facebookLeadId     || null,
      b.publisherSub       || null,
      b.websource          || null,
      JSON.stringify(b),
    ]);

    const leadId = r.rows[0].id;
    const leadRef = `KRW-${campaign.toUpperCase()}-${leadId}`;

    // Respond immediately to publisher
    res.json({ status: 'received', leadId: leadRef, message: 'Lead accepted' });

    // Forward to buyer in background (don't block response)
    // Priority: 1) env var BUYER_ENDPOINT_DEPO  2) stored in DB  3) log and skip
    const buyerUrl = process.env[`BUYER_ENDPOINT_${campaign.toUpperCase()}`];
    if (buyerUrl) {
      forwardToBuyer(leadId, leadRef, campaign, b, buyerUrl);
    } else {
      // Try to get endpoint from campaigns table
      pool.query('SELECT apex_endpoint FROM campaigns WHERE slug=$1 AND active=true', [campaign])
        .then(function(r) {
          const endpoint = r.rows[0]?.apex_endpoint;
          if (endpoint) {
            forwardToBuyer(leadId, leadRef, campaign, b, endpoint);
          } else {
            console.log(`Lead ${leadRef} stored. No buyer endpoint set for campaign: ${campaign}`);
            console.log(`Set env var BUYER_ENDPOINT_${campaign.toUpperCase()} or add endpoint in Campaigns tab`);
          }
        }).catch(function() {
          console.log(`Lead ${leadRef} stored. No buyer endpoint configured for: ${campaign}`);
        });
    }

  } catch (err) {
    console.error('Lead intake error:', err.message);
    res.status(500).json({ status: 'error', reason: err.message });
  }
});

function toBooleanField(val) {
  if (val === undefined || val === null || val === 'undefined') return null;
  if (typeof val === 'boolean') return val;
  const s = String(val).trim().toLowerCase();
  if (s === 'true'  || s === 'yes' || s === '1') return true;
  if (s === 'false' || s === 'no'  || s === '0') return false;
  return null;
}

async function forwardToBuyer(leadId, leadRef, campaign, data, buyerUrl) {
  try {
    await pool.query(`UPDATE leads SET status='forwarding' WHERE id=$1`, [leadId]);

    // Get campaign config from DB to know buyer format + LP credentials
    let campRow = null;
    try {
      const cr = await pool.query('SELECT * FROM campaigns WHERE slug=$1', [campaign]);
      campRow = cr.rows[0] || null;
    } catch(e) {}

    const buyerNotes  = campRow?.buyer_notes || '';
    const isLeadProsper = buyerUrl.includes('leadprosper') || buyerUrl.includes('direct_post');
    const isLawmatics   = buyerUrl.includes('lawmatics.com');

    // Hardcoded LP credentials per campaign (always reliable)
    const LP_CREDS = {
      'mva':        { id: '31080',  sup: '110928', key: 'ke21sx0koi7dld' },
      'rideshare':  { id: '31036',  sup: '99237',  key: 'jz2gawz23t17g5' },
      'lyft':       { id: '31036',  sup: '99237',  key: 'jz2gawz23t17g5' },
      'uber':       { id: '31036',  sup: '99237',  key: 'jz2gawz23t17g5' },
      'roundup':    { id: '30976',  sup: '113017', key: 'rlv6tzwn1tzw5r' },
      'roundup-lt': { id: '30976',  sup: '113017', key: 'rlv6tzwn1tzw5r' },
    };

    // Use hardcoded first, then fall back to buyer_notes
    const hardcoded = LP_CREDS[campaign] || {};
    const lpCampId  = hardcoded.id  || (buyerNotes.match(/LP Campaign ID:\s*(\d+)/i)||[])[1] || '';
    const lpSuppId  = hardcoded.sup || (buyerNotes.match(/LP Supplier ID:\s*(\d+)/i)||[])[1] || '';
    const lpKey     = hardcoded.key || (buyerNotes.match(/LP Key:\s*(\S+)/i)||[])[1] || '';

    // Extract Apex URL parts: /intake/<vertical>/<apexCampaign>/zapier/<seller>/submit
    const apexMatch   = buyerUrl.match(/\/intake\/([^/]+)\/([^/]+)\/zapier\/([^/]+)\/submit/);
    const apexCampaign = apexMatch ? apexMatch[2] : campaign;  // e.g. 'talc-leads', 'depo'
    const apexSeller   = apexMatch ? apexMatch[3] : (process.env['BUYER_SELLER_'+campaign.toUpperCase()] || 'tuell');

    let payload;

    if (isLeadProsper && lpCampId && lpSuppId && lpKey) {
      // ── LeadProsper format ──────────────────────────────
      payload = {
        lp_campaign_id: lpCampId,
        lp_supplier_id: lpSuppId,
        lp_key:         lpKey,
        lp_subid1:      data.publisherSub || '',
        first_name:     data.firstName,
        last_name:      data.lastName,
        email:          data.email,
        phone:          String(data.phone).replace(/\D/g,''),
        date_of_birth:  data.dateOfBirth   || null,
        gender:         data.gender        || null,
        address:        data.street        || null,
        city:           data.city          || null,
        state:          data.state         || null,
        zip_code:       data.zip           || null,
        jornaya_leadid:      data.jornayaLeadId      || null,
        trustedform_cert_url: data.trustedFormCertUrl || null,
        tcpa_text:      data.tcpaText      || null,
        incident_state: data.incidentState || null,
        case_description: data.caseDescription || data.notes || null,
        ip_address:     data.ipAddress     || null,
        landing_page_url: data.websource   || 'https://krwmarketingsolutions.github.io/forms',
        // Roundup-specific fields (passed through if present)
        have_attorney:    'Yes',
        used_roundup:     'Yes',
        which_cancer:     data.whichCancer     || null,
        what_year:        data.whatYear        || null,
        exposed_location: data.exposedLocation || null,
      };
      // Remove null values
      Object.keys(payload).forEach(k => { if(payload[k]===null) delete payload[k]; });
    } else if (isLawmatics) {
      // ── Lawmatics format (Rideshare Uber/Lyft) ──────────
      payload = {
        first_name:               data.firstName,
        last_name:                data.lastName,
        email:                    data.email,
        phone:                    String(data.phone).replace(/\D/g,''),
        birthdate:                data.dateOfBirth    || null,
        zipcode:                  data.zip            || null,
        state:                    data.state          || null,
        city:                     data.city           || null,
        street:                   data.street         || null,
        custom_field_368623:      data.trustedFormCertUrl || null,  // TrustedForm
        custom_field_266045:      data.publisherSub   || null,      // Publisher
        custom_field_766975:      data.rideshareGender     || null, // Gender ID
        custom_field_766959:      data.sexuallyAssaulted   || null, // Assaulted?
        custom_field_766960:      data.rideshareCompany    || null, // Uber/Lyft ID
        custom_field_766964:      data.driverOrPassenger   || null, // Passenger?
        custom_field_766968:      data.abuseType           || null, // Abuse type ID
        custom_field_766962:      data.incidentDate        || null, // Incident date
        custom_field_766980:      data.incidentStateText   || null, // Incident state
        custom_field_631075:      data.hasReceipt          || null, // Receipt?
        custom_field_766976:      data.fraudConviction     || null, // Fraud?
        'custom_field_812732[]':  data.reportedTo          || null, // Reported to
        'custom_field_375335[]':  data.bestTimeToCall      || null, // Best time
      };
      // Remove null values
      Object.keys(payload).forEach(k => { if(payload[k]===null) delete payload[k]; });
    } else {
      // ── Apex / generic format ───────────────────────────
      payload = {
        firstName: data.firstName,
        lastName:  data.lastName,
        email:     data.email,
        phone:     String(data.phone).replace(/\D/g,''),
        street:    data.street || null,
        city:      data.city   || null,
        state:     data.state  || null,
        zip:       data.zip    || null,
        notes:     data.notes  || null,
        meta: {
          id:                 leadRef,
          Timestamp:          new Date().toISOString(),
          createDt:           new Date().toISOString(),
          claimant:           `${data.firstName} ${data.lastName}`,
          websource:          data.websource || 'https://krwmarketingsolutions.github.io/forms',
          trustedFormCertUrl: data.trustedFormCertUrl || null,
          jornayaLeadId:      data.jornayaLeadId      || null,
          seller:             apexSeller,
          campaign:           apexCampaign,
          publisherSub:       data.publisherSub || null,
        },
      };
    }

    if (campaign === 'roundup' || campaign === 'roundup-lt') {
      console.log(`[${leadRef}] LP roundup fields → used_roundup=${payload.used_roundup} have_attorney=${payload.have_attorney} which_cancer=${payload.which_cancer} what_year=${payload.what_year} exposed_location=${payload.exposed_location}`);
    }

    const resp = await fetch(buyerUrl, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });

    const result = await resp.json().catch(() => ({ status: resp.status }));

    const accepted = result.status === 'ACCEPTED' || result.status === 'Success' ||
                     result.status === 'success' || result.ok === true ||
                     (isLawmatics && resp.ok);
    if (resp.ok && accepted) {
      const buyerId = String(result.lead_id || result.id || result.ids?.[0] || result.leadId || '');
      await pool.query(
        `UPDATE leads SET status='forwarded', buyer_intake_id=$1, buyer_error=null WHERE id=$2`,
        [buyerId, leadId]
      );
      console.log(`✅ Lead ${leadRef} → buyer accepted. Buyer ID: ${buyerId}`);
    } else {
      const errMsg = result.message || result.statusDetail || result.error || result.status || `HTTP ${resp.status}`;
      await pool.query(
        `UPDATE leads SET status='buyer_rejected', buyer_error=$1 WHERE id=$2`,
        [errMsg, leadId]
      );
      console.log(`⚠️  Lead ${leadRef} → buyer rejected: ${errMsg}`);
    }
  } catch (err) {
    await pool.query(
      `UPDATE leads SET status='forward_failed', buyer_error=$1 WHERE id=$2`,
      [err.message, leadId]
    );
    console.error(`❌ Lead ${leadRef} → forward error: ${err.message}`);
  }
}

// ── Test email endpoint (remove after confirming email works) ─────────────────
app.get('/test-email', requireKey, async (req, res) => {
  const html = `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:520px;margin:0 auto;background:#f4f6fb;padding:32px 20px">
      <div style="background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
        <div style="background:#0f1c3f;padding:24px 28px">
          <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.15em;color:rgba(255,255,255,.5);margin-bottom:6px">KRW Marketing Solutions</div>
          <div style="font-size:22px;font-weight:700;color:#fff">✓ Billable Lead</div>
          <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">Test Email — ${new Date().toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'})}</div>
        </div>
        <div style="padding:28px">
          <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px 20px;margin-bottom:20px">
            <div style="font-size:18px;font-weight:700;color:#15803d;margin-bottom:4px">Jane Smith</div>
            <div style="font-size:13px;color:#166534">Marked billable — Accepted by Firm - Billable</div>
          </div>
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <tr style="border-bottom:1px solid #f1f5f9"><td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;width:38%">Campaign</td><td style="padding:10px 0;font-weight:600;color:#0f1c3f">RIDESHARE-TB — Rideshare</td></tr>
            <tr style="border-bottom:1px solid #f1f5f9"><td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">CID</td><td style="padding:10px 0;font-family:monospace;color:#475569">1234567890</td></tr>
            <tr style="border-bottom:1px solid #f1f5f9"><td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Email</td><td style="padding:10px 0;color:#475569">jane.smith@email.com</td></tr>
            <tr><td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Phone</td><td style="padding:10px 0;color:#475569">3105550123</td></tr>
          </table>
        </div>
        <div style="padding:16px 28px;background:#f9fafb;border-top:1px solid #f1f5f9;font-size:11px;color:#9ca3af;text-align:center">KRW Marketing Solutions · Lead Notification System</div>
      </div>
    </div>`;

  await sendEmailNotification('✓ Billable Lead — Jane Smith | Rideshare | RIDESHARE-TB', html);
  res.json({ ok: true, message: 'Test email sent to kyler@leadbloom.co' });
});
// ── End test email ────────────────────────────────────────────────────────────

// ══════════════════════════════════════════════════════
//  LEAD READ ENDPOINTS (dashboard)
// ══════════════════════════════════════════════════════

app.get('/leads/summary', requireKey, async (req, res) => {
  try {
    const weekAgo    = new Date(Date.now()-7*86400000).toISOString().split('T')[0];
    const monthStart = new Date().toISOString().slice(0,7)+'-01';
    const [todayQ,weekQ,monthQ,statusQ] = await Promise.all([
      pool.query(`SELECT campaign, COUNT(*) as count FROM leads WHERE (received_at AT TIME ZONE 'America/New_York')::date=(NOW() AT TIME ZONE 'America/New_York')::date AND COALESCE(vertical,'') != 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true' GROUP BY campaign`),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1 AND COALESCE(vertical,'') != 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true'`,[weekAgo]),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1 AND COALESCE(vertical,'') != 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true'`,[monthStart]),
      pool.query(`SELECT status, COUNT(*) as count FROM leads WHERE COALESCE(vertical,'') != 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true' GROUP BY status ORDER BY count DESC`),
    ]);
    res.json({ ok:true, today:todayQ.rows, week:parseInt(weekQ.rows[0]?.count||0), month:parseInt(monthQ.rows[0]?.count||0), by_status:statusQ.rows });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.get('/leads/feed', requireKey, async (req, res) => {
  // This endpoint's data changes constantly (new leads arrive continuously) -
  // explicitly prevent any caching so publishers always see current data,
  // never a stale response from before their latest submission. Found via
  // a real bug (Aug 26): browser was returning 304 Not Modified and showing
  // a publisher's portal as empty even though the lead was correctly stored.
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  try {
    const { campaign, status, limit=100, pub, days, portal_id } = req.query;
    const where=[], params=[];
    let i=1;
    if (campaign) {
      where.push(`campaign=$${i++}`);
      params.push(campaign);
    } else if (!portal_id && !pub) {
      // Exclude ALL SSDI lead campaigns from the general feed — they show on
      // the SSDI tab only. Filtered by vertical, not campaign name, so any
      // future SSDI campaign is automatically covered without needing a
      // separate code change each time one is added.
      where.push(`COALESCE(vertical,'') != 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true'`);
    }
    if (status)   { where.push(`status=$${i++}`);   params.push(status); }

    // Support portal_id lookup — finds all pub_ids for that portal then filters
    if (portal_id) {
      const pubs = await pool.query(
        `SELECT pub_id FROM publishers WHERE (portal_id=$1 OR pub_id=$1) AND active=true`, [portal_id]
      );
      const pubIds = pubs.rows.map(p => p.pub_id);
      if (pubIds.length) {
        where.push(`publisher_sub = ANY($${i++})`);
        params.push(pubIds);
      } else {
        // No publisher matches this portal_id/pub_id at all — fail closed, never return unfiltered data
        where.push('1=0');
      }
    } else if (pub) {
      where.push(`publisher_sub=$${i++}`);
      params.push(pub);
    }

    if (req.query.include_excluded == null) where.push("COALESCE(raw->>'excluded','') <> 'true'");
    if (days && parseInt(days) < 9999) {
      where.push(`received_at >= NOW() - INTERVAL '${parseInt(days)} days'`);
    }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(
      `SELECT id,received_at,campaign,first_name,last_name,email,phone,state,
              status,zapier_status,buyer_intake_id,buyer_error,buyer_status,notes,publisher_sub,billable,revenue,
              raw->>'buyer_name' as buyer_name,
              COALESCE(NULLIF(raw->>'case_description',''), NULLIF(raw->>'summary',''), NULLIF(raw->>'description','')) as case_description,
              COALESCE(NULLIF(raw->>'injury',''), raw->>'physical_injury') as injury,
              raw->>'incident_date' as incident_date, raw->>'county' as county
       FROM leads ${wc} ORDER BY received_at DESC LIMIT $${i}`, params);
    res.json({ ok:true, count:r.rows.length, leads:r.rows });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// ── Manual billable toggle (admin-only, not exposed to publisher portal) ────
app.post('/leads/:id/billable', requireKey, async (req, res) => {
  const { id } = req.params;
  const { billable } = req.body || {};
  if (typeof billable !== 'boolean') {
    return res.status(400).json({ ok: false, error: 'billable must be true or false' });
  }
  try {
    const r = await pool.query(
      `UPDATE leads SET billable=$1 WHERE id=$2 RETURNING id, first_name, last_name, billable`,
      [billable, id]
    );
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'Lead not found' });
    res.json({ ok: true, lead: r.rows[0] });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});


// Full single lead incl. the raw payload the publisher posted - admin dashboard drawer only.
// The feed stays light (5,000 rows); the detail is fetched one lead at a time on open.
app.get('/leads/detail/:id', requireKey, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ ok: false, error: 'Bad lead id' });
  try {
    const r = await pool.query('SELECT * FROM leads WHERE id=$1', [id]);
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'Lead not found' });
    res.json({ ok: true, lead: r.rows[0] });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/leads/export/:campaign', requireKey, async (req, res) => {
  try {
    const { campaign } = req.params;
    const { from, to } = req.query;
    const where=[`campaign=$1`], params=[campaign];
    let i=2;
    if (from) { where.push(`received_at::date>=$${i++}`); params.push(from); }
    if (to)   { where.push(`received_at::date<=$${i++}`); params.push(to); }
    const r = await pool.query(
      `SELECT id,received_at,first_name,last_name,email,phone,street,city,state,zip,
              notes,status,buyer_intake_id,publisher_sub,websource,trusted_form_url,jornaya_id
       FROM leads WHERE ${where.join(' AND ')} ORDER BY received_at DESC`, params);
    const headers = ['ID','Received','First','Last','Email','Phone','Street','City','State','Zip','Notes','Status','Buyer ID','Publisher','Source','TrustedForm','Jornaya'];
    const rows = r.rows.map(row => [
      `KRW-${campaign.toUpperCase()}-${row.id}`, row.received_at,
      row.first_name, row.last_name, row.email, row.phone,
      row.street||'', row.city||'', row.state||'', row.zip||'', row.notes||'',
      row.status, row.buyer_intake_id||'', row.publisher_sub||'',
      row.websource||'', row.trusted_form_url||'', row.jornaya_id||'',
    ].map(v=>`"${String(v).replace(/"/g,'""')}"`).join(','));
    res.setHeader('Content-Type','text/csv');
    res.setHeader('Content-Disposition',`attachment; filename="${campaign}-leads-${new Date().toISOString().split('T')[0]}.csv"`);
    res.send([headers.join(','),...rows].join('\n'));
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// ══════════════════════════════════════════════════════
//  CALL / REVENUE ENDPOINTS (existing TrackDrive flow)
// ══════════════════════════════════════════════════════

app.post('/postback', requireKey, async (req, res) => {
  try {
    const b = req.body?.querystring || req.body || {};
    const field = (...keys) => { for (const k of keys) { if (b[k] !== undefined && b[k] !== '') return b[k]; } return ''; };
    const vertical  = field('Vertical Campaign Name','vertical');
    const buyer     = field('Buyer Name','buyer_name','buyer');
    const revenue   = parseFloat(field('Payout Amount','payout_amount','revenue')) || 0;
    const caller_id = field('Caller ID','caller_id','phone');
    const call_date = field('Call Date Time','call_date');
    const campaign_id = field('Campaign ID','campaign_id');
    const billable  = true; // default all calls to billable
    await pool.query(`
      INSERT INTO calls (call_date,vertical,buyer_name,caller_id,payout_amount,campaign_id,billable,raw)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    `, [call_date, vertical, buyer, caller_id, revenue, campaign_id, billable, JSON.stringify(b)]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.get('/summary', requireKey, async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now()-7*86400000).toISOString().split('T')[0];
    const [todayQ,weekQ,monthQ] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(payout_amount),0) as total, COUNT(*) as calls FROM calls WHERE call_date=$1 AND billable=true`,[today]),
      pool.query(`SELECT COALESCE(SUM(payout_amount),0) as total, COUNT(*) as calls FROM calls WHERE call_date>=$1 AND billable=true`,[weekAgo]),
      pool.query(`SELECT COALESCE(SUM(payout_amount),0) as total, COUNT(*) as calls FROM calls WHERE call_date>=$1 AND billable=true`,[new Date().toISOString().slice(0,7)+'-01']),
    ]);
    res.json({ ok:true, today:{ total:parseFloat(todayQ.rows[0].total), calls:parseInt(todayQ.rows[0].calls) }, week:{ total:parseFloat(weekQ.rows[0].total), calls:parseInt(weekQ.rows[0].calls) }, month:{ total:parseFloat(monthQ.rows[0].total), calls:parseInt(monthQ.rows[0].calls) } });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.get('/calls', requireKey, async (req, res) => {
  try {
    const { limit=100, from, to } = req.query;
    const where=[], params=[];
    let i=1;
    if (from) { where.push(`call_date>=$${i++}`); params.push(from); }
    if (to)   { where.push(`call_date<=$${i++}`); params.push(to); }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(`SELECT * FROM calls ${wc} ORDER BY received_at DESC LIMIT $${i}`, params);
    res.json({ ok:true, calls:r.rows });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.get('/invoice-summary', requireKey, async (req, res) => {
  try {
    const { from, to } = req.query;
    const where=['billable=true',"invoice_status='pending'","payout_amount>0","source_system='trackdrive'"], params=[];
    let i=1;
    if (from) { where.push(`received_at::date>=$${i++}`); params.push(from); }
    if (to)   { where.push(`received_at::date<=$${i++}`); params.push(to); }
    const calls = await pool.query(`SELECT * FROM calls WHERE ${where.join(' AND ')} ORDER BY received_at DESC`, params);
    const byBuyer = {};
    calls.rows.forEach(c => {
      const k = (c.buyer_name||'Unknown')+'|'+(c.vertical||'');
      if (!byBuyer[k]) byBuyer[k] = { buyer_name:c.buyer_name||'Unknown', vertical:c.vertical||'', calls:[], total_owed:0, call_count:0 };
      byBuyer[k].calls.push(c);
      byBuyer[k].total_owed += parseFloat(c.payout_amount||0);
      byBuyer[k].call_count++;
    });
    const by_buyer = Object.values(byBuyer);
    const total_owed = by_buyer.reduce((s,b)=>s+b.total_owed,0);
    res.json({ ok:true, by_buyer, calls:calls.rows, totals:{ total_owed, call_count:calls.rows.length } });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.patch('/calls/:id', requireKey, async (req, res) => {
  try {
    const { id } = req.params;
    const { invoice_status, invoice_id, invoice_date, paid_date } = req.body;
    const sets=[], params=[];
    let i=1;
    if (invoice_status) { sets.push(`invoice_status=$${i++}`); params.push(invoice_status); }
    if (invoice_id)     { sets.push(`invoice_id=$${i++}`);     params.push(invoice_id); }
    if (invoice_date)   { sets.push(`invoice_date=$${i++}`);   params.push(invoice_date); }
    if (paid_date)      { sets.push(`paid_date=$${i++}`);      params.push(paid_date); }
    if (!sets.length) return res.status(400).json({ error:'Nothing to update' });
    params.push(id);
    await pool.query('UPDATE calls SET '+sets.join(',')+'WHERE id=$'+i, params);
    res.json({ ok:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.patch('/calls/:id/billable', requireKey, async (req, res) => {
  try {
    const { billable } = req.body;
    await pool.query('UPDATE calls SET billable=$1 WHERE id=$2',[billable===true||billable==='true', req.params.id]);
    res.json({ ok:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.post('/send-invoice', requireKey, async (req, res) => {
  try {
    const { zapier_webhook, ...payload } = req.body;
    if (!zapier_webhook) return res.status(400).json({ error:'zapier_webhook required' });
    const r = await fetch(zapier_webhook, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) });
    res.json({ ok:true, zapier_status:r.status });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// ── Dashboard HTML ────────────────────────────────────
app.get('/dashboard', (req, res) => {
  const file = path.join(__dirname, 'dashboard.html');
  if (!fs.existsSync(file)) return res.status(404).send('<h2>Upload dashboard.html to your GitHub repo</h2>');
  res.setHeader('Content-Type','text/html');
  res.setHeader('Cache-Control','no-cache');
  res.sendFile(file);
});

// ── Health / Debug ────────────────────────────────────
app.get('/health', (req, res) => res.json({ ok:true, status:'healthy' }));
app.get('/debug',  (req, res) => res.json({ api_key_set:!!process.env.API_KEY, lead_key_set:!!process.env.LEAD_API_KEY, db_url_set:!!process.env.DATABASE_URL }));


// ══════════════════════════════════════════════════════
//  CAMPAIGNS - create/edit from dashboard, no env vars needed
// ══════════════════════════════════════════════════════

async function initCampaignsDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS campaigns (
      id               SERIAL PRIMARY KEY,
      slug             TEXT UNIQUE NOT NULL,
      name             TEXT NOT NULL,
      vertical         TEXT,
      apex_endpoint    TEXT,
      payout           NUMERIC(10,2) DEFAULT 0,
      buyer_notes      TEXT,
      required_fields  JSONB DEFAULT '["firstName","lastName","email","phone"]',
      optional_fields  JSONB DEFAULT '["state","zip","notes","trustedFormCertUrl","jornayaLeadId","publisherSub"]',
      field_labels     JSONB DEFAULT '{}',
      description      TEXT,
      active           BOOLEAN DEFAULT true,
      created_at       TIMESTAMPTZ DEFAULT NOW(),
      updated_at       TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  // Seed DEPO if not exists
  const existing = await pool.query('SELECT slug FROM campaigns WHERE slug=$1', ['depo']);
  if (!existing.rows.length) {
    await pool.query(`
      INSERT INTO campaigns (slug, name, vertical, apex_endpoint, required_fields, optional_fields)
      VALUES ($1,$2,$3,$4,$5,$6)
    `, [
      'depo',
      'DEPO - Lead Tree (WTC)',
      'Mass Tort - Depo',
      process.env.BUYER_ENDPOINT_DEPO || '',
      JSON.stringify(['firstName','lastName','email','phone']),
      JSON.stringify(['street','city','state','zip','notes','trustedFormCertUrl','jornayaLeadId','facebookLeadId','publisherSub']),
    ]);
    console.log('✅ DEPO campaign seeded');
  }
  console.log('✅ Campaigns table ready');
}

// GET /campaigns - list all (dashboard)
app.get('/campaigns', requireKey, async (req, res) => {
  try {
    const r = await pool.query(
      'SELECT * FROM campaigns WHERE active=true ORDER BY created_at ASC'
    );
    res.json({ ok: true, campaigns: r.rows });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /campaigns/:slug/config - public, used by publisher form
app.get('/campaigns/:slug/config', async (req, res) => {
  try {
    const r = await pool.query(
      'SELECT * FROM campaigns WHERE slug=$1 AND active=true',
      [req.params.slug.toLowerCase()]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Campaign not found: '+req.params.slug });
    const row = r.rows[0];
    res.json({
      ok:          true,
      slug:        row.slug,
      name:        row.name,
      vertical:    row.vertical || '',
      description: row.description || '',
      required:    row.required_fields,
      optional:    row.optional_fields,
      fieldLabels: row.field_labels || {},
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /campaigns - create or update (upsert)
app.post('/campaigns', requireKey, async (req, res) => {
  try {
    const {
      slug, name, vertical, apex_endpoint, payout,
      buyer_notes, required_fields, optional_fields,
      field_labels, description
    } = req.body;
    if (!slug || !name) return res.status(400).json({ error: 'slug and name required' });
    const r = await pool.query(`
      INSERT INTO campaigns
        (slug, name, vertical, apex_endpoint, payout, buyer_notes,
         required_fields, optional_fields, field_labels, description)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (slug) DO UPDATE SET
        name=$2, vertical=$3, apex_endpoint=$4, payout=$5,
        buyer_notes=$6, required_fields=$7, optional_fields=$8,
        field_labels=$9, description=$10, updated_at=NOW()
      RETURNING *
    `, [
      slug.toLowerCase(), name, vertical||'', apex_endpoint||'',
      parseFloat(payout)||0, buyer_notes||null,
      JSON.stringify(required_fields||['firstName','lastName','email','phone']),
      JSON.stringify(optional_fields||[]),
      JSON.stringify(field_labels||{}),
      description||null,
    ]);
    res.json({ ok: true, campaign: r.rows[0] });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// PATCH /campaigns/:slug - partial update
app.patch('/campaigns/:slug', requireKey, async (req, res) => {
  try {
    const slug = req.params.slug.toLowerCase();
    const allowed = ['name','vertical','apex_endpoint','payout','buyer_notes',
                     'required_fields','optional_fields','field_labels','description','active'];
    const sets = [], params = [];
    let i = 1;
    allowed.forEach(function(col) {
      if (req.body[col] !== undefined) {
        sets.push(col+'=$'+i++);
        const val = req.body[col];
        params.push(['required_fields','optional_fields','field_labels'].includes(col)
          ? JSON.stringify(val) : val);
      }
    });
    if (!sets.length) return res.status(400).json({ error: 'Nothing to update' });
    sets.push('updated_at=NOW()');
    params.push(slug);
    await pool.query('UPDATE campaigns SET '+sets.join(',')+" WHERE slug=$"+i, params);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});


// ── Publishers table ──────────────────────────────────
async function initPublishersDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS publishers (
      id            SERIAL PRIMARY KEY,
      pub_id        TEXT UNIQUE NOT NULL,
      name          TEXT NOT NULL,
      email         TEXT,
      phone         TEXT,
      company       TEXT,
      portal_id     TEXT UNIQUE,
      active        BOOLEAN DEFAULT true,
      created_at    TIMESTAMPTZ DEFAULT NOW()
    );
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS phone     TEXT;
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS company   TEXT;
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS portal_id TEXT;
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS did       TEXT;
    -- Add unique constraint to publisher_campaigns if not exists
    DO $$ BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'publisher_campaigns_pub_id_campaign_key'
      ) THEN
        ALTER TABLE publisher_campaigns ADD CONSTRAINT publisher_campaigns_pub_id_campaign_key UNIQUE (pub_id, campaign);
      END IF;
    END $$;

    -- Campaign assignments table (many per publisher)
    CREATE TABLE IF NOT EXISTS publisher_campaigns (
      id            SERIAL PRIMARY KEY,
      pub_id        TEXT REFERENCES publishers(pub_id) ON DELETE CASCADE,
      campaign      TEXT NOT NULL,
      sub_id        TEXT,
      did           TEXT,
      payout_rate   NUMERIC(10,2) DEFAULT 0,
      vertical      TEXT,
      active        BOOLEAN DEFAULT true,
      created_at    TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS campaign_settings (
      campaign      TEXT PRIMARY KEY,
      mode          TEXT NOT NULL DEFAULT 'default',
      updated_at    TIMESTAMPTZ DEFAULT NOW()
    );
    INSERT INTO campaign_settings (campaign, mode)
      VALUES ('mva-routing', 'default')
      ON CONFLICT (campaign) DO NOTHING;
  `);
  console.log('Publishers table ready');
}

// ── PUBLISHER ENDPOINTS ───────────────────────────────

// Verify publisher login (pub_id lookup)
app.post('/publishers/login', async (req, res) => {
  const { pub_id } = req.body || {};
  if (!pub_id) return res.status(400).json({ ok: false, error: 'pub_id required' });
  try {
    // Look up by portal_id first, then pub_id
    const r = await pool.query(
      'SELECT * FROM publishers WHERE (portal_id=$1 OR pub_id=$1) AND active=true LIMIT 1',
      [pub_id]
    );
    if (!r.rows.length) return res.status(401).json({ ok: false, error: 'Publisher not found' });
    const pub = r.rows[0];
    // Get all campaign assignments
    const camps = await pool.query(
      'SELECT campaign, did, payout_rate FROM publisher_campaigns WHERE pub_id=$1 AND active=true',
      [pub.pub_id]
    );
    res.json({
      ok:        true,
      name:      pub.name,
      pub_id:    pub.pub_id,
      portal_id: pub.portal_id || pub.pub_id,
      email:     pub.email,
      company:   pub.company,
      did:       pub.did || null,
      payout_rate: pub.payout_rate || null,
      campaigns: camps.rows,
    });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Publisher portal config — returns all campaigns grouped by portal_id ──────
app.post('/publishers/portal-config', async (req, res) => {
  const { portal_id } = req.body || {};
  if (!portal_id) return res.status(400).json({ ok: false, error: 'portal_id required' });
  try {
    // Get all publisher records for this portal_id
    const pubs = await pool.query(
      `SELECT pub_id, name, campaign, company, portal_id
       FROM publishers
       WHERE portal_id = $1 AND active = true`,
      [portal_id]
    );
    if (!pubs.rows.length) return res.status(401).json({ ok: false, error: 'Portal ID not found' });

    // Build unique campaign list with display names
    const campaignMap = {
      'mva-funnel':   { label: 'MVA — Motor Vehicle Accident', color: 'blue' },
      'mva-nld2':     { label: 'MVA — Motor Vehicle Accident', color: 'blue' },
      'rideshare-tb': { label: 'Rideshare — Uber & Lyft',      color: 'green' },
      'roblox-mt':    { label: 'Roblox Mass Tort',             color: 'blue' },
      'roundup':      { label: 'Roundup Mass Tort',            color: 'green' },
      'roundup-lt':   { label: 'Roundup LT',                   color: 'green' },
      'ssdi':         { label: 'SSDI',                         color: 'blue' },
      'Lssdi-shore':  { label: 'SSDI Filed',                   color: 'blue' },
      'depo':         { label: 'Depo-Provera',                 color: 'green' },
    };

    // Collect unique campaigns across all pub records for this portal
    const seen = new Set();
    const campaigns = [];
    const pubIds = [];

    for (const pub of pubs.rows) {
      if (pub.pub_id && !pubIds.includes(pub.pub_id)) pubIds.push(pub.pub_id);
      if (pub.campaign && !seen.has(pub.campaign)) {
        seen.add(pub.campaign);
        campaigns.push({
          slug:  pub.campaign,
          label: (campaignMap[pub.campaign] || {}).label || pub.campaign.toUpperCase(),
          color: (campaignMap[pub.campaign] || {}).color || 'blue',
        });
      }
    }

    // Use the first record's name as display name
    const displayName = pubs.rows.find(p => p.name && !p.name.toLowerCase().includes('roblox') && !p.name.toLowerCase().includes('rideshare'))?.name
      || pubs.rows[0].name;

    res.json({
      ok:        true,
      portal_id,
      name:      displayName,
      pub_ids:   pubIds,
      campaigns,
    });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/publishers/:pub_id/calls', requireKey, async (req, res) => {
  const { pub_id } = req.params;
  const { days = 30, billable_only } = req.query;
  try {
    // Verify publisher exists
    const pubCheck = await pool.query('SELECT * FROM publishers WHERE pub_id=$1 AND active=true', [pub_id]);
    if (!pubCheck.rows.length) return res.status(401).json({ ok: false, error: 'Unauthorized' });

    const daysInt = parseInt(days) >= 9999 ? 36500 : parseInt(days);

    // Use pub_id directly to find calls
    const allSubs = [pub_id]; // always include their main pub_id

    let query = `SELECT id, call_date, caller_id, caller_name,
                        call_duration, billable, call_status_label, disposition,
                        payout_amount, campaign, received_at
                 FROM calls
                 WHERE publisher_sub = ANY($1::text[])
                   AND source_system IN ('partner','google_sheet','trackdrive_webhook','j_signed_postback','ringfuel_webhook','sheet_import')`;
    if (daysInt < 9999) query += ` AND received_at >= NOW() - INTERVAL '${daysInt} days'`;
    if (billable_only === 'true') query += ' AND billable=true';
    query += ' ORDER BY received_at DESC LIMIT 500';

    const r = await pool.query(query, [allSubs]);
    const total = r.rows.length;
    const billable_count = r.rows.filter(c => c.billable).length;
    const total_payout = r.rows.filter(c => c.billable).reduce((sum, c) => sum + parseFloat(c.payout_amount||0), 0);

    res.json({ ok: true, calls: r.rows, total, billable_count, total_payout: total_payout.toFixed(2) });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// CRUD publishers (dashboard only)
app.get('/publishers/:pub_id/campaigns', requireKey, async (req, res) => {
  try {
    const r = await pool.query(
      'SELECT * FROM publisher_campaigns WHERE pub_id=$1 ORDER BY campaign',
      [req.params.pub_id]
    );
    res.json({ ok: true, campaigns: r.rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/publishers/:pub_id/campaigns', requireKey, async (req, res) => {
  const { campaign, sub_id, did, payout_rate, vertical } = req.body || {};
  if (!campaign) return res.status(400).json({ ok: false, error: 'campaign required' });
  try {
    await pool.query(
      `INSERT INTO publisher_campaigns (pub_id, campaign, did, payout_rate)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (pub_id, campaign) DO UPDATE SET
         did=$3, payout_rate=$4, active=true`,
      [req.params.pub_id, campaign, did||null,
       parseFloat(payout_rate||0), vertical||null]
    );
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.delete('/publishers/:pub_id/campaigns/:campaign', requireKey, async (req, res) => {
  try {
    await pool.query(
      'UPDATE publisher_campaigns SET active=false WHERE pub_id=$1 AND campaign=$2',
      [req.params.pub_id, req.params.campaign]
    );
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/publishers', requireKey, async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM publishers ORDER BY created_at DESC');
    res.json({ ok: true, publishers: r.rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/publishers', requireKey, async (req, res) => {
  const { pub_id, name, email, phone, company, portal_id, campaigns } = req.body || {};
  if (!pub_id || !name) return res.status(400).json({ ok: false, error: 'pub_id and name required' });
  try {
    const r = await pool.query(
      `INSERT INTO publishers (pub_id, name, email, phone, company, portal_id)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (pub_id) DO UPDATE SET
         name=$2, email=$3, phone=$4, company=$5,
         portal_id=COALESCE($6, publishers.portal_id),
         active=true
       RETURNING *`,
      [pub_id, name, email||null, phone||null, company||null, portal_id||null]
    );
    // Upsert campaign assignments if provided
    if (campaigns && Array.isArray(campaigns)) {
      for (const camp of campaigns) {
        await pool.query(
          `INSERT INTO publisher_campaigns (pub_id, campaign, did, payout_rate)
           VALUES ($1,$2,$3,$4)
           ON CONFLICT (pub_id, campaign) DO UPDATE SET
             did=$3, payout_rate=$4, active=true`,
          [pub_id, camp.campaign, camp.did||null,
           parseFloat(camp.payout_rate||0), camp.vertical||null]
        ).catch(() => {}); // ignore if no unique constraint yet
      }
    }
    res.json({ ok: true, publisher: r.rows[0] });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.delete('/publishers/:pub_id', requireKey, async (req, res) => {
  try {
    await pool.query('UPDATE publishers SET active=false WHERE pub_id=$1', [req.params.pub_id]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── TRACKDRIVE POSTBACK ENDPOINT (FE calls → Invoicing) ─
app.post('/trackdrive/postback', async (req, res) => {
  const key = req.headers['x-api-key'] || req.headers['authorization'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
    process.env.TRACKDRIVE_API_KEY || '',
  ].filter(Boolean);
  if (key && !validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }
  const b = req.body || {};
  const callDate    = b.call_date   || b.date     || new Date().toISOString().split('T')[0];
  // Accurate timestamp from TrackDrive fields
  const tdDatetime  = b.call_datetime || b.created_at || b.timestamp || b.start_time || null;
  const tdTime      = b.call_time || b.time || null;
  let   tdReceivedAt = null;
  if (tdDatetime) {
    tdReceivedAt = new Date(tdDatetime).toISOString();
  } else if (callDate && tdTime) {
    tdReceivedAt = new Date(callDate + 'T' + tdTime).toISOString();
  } else if (callDate) {
    tdReceivedAt = new Date(callDate + 'T12:00:00').toISOString();
  }
  const callerId    = b.caller_id   || b.ani      || b.phone    || null;
  const callerName  = b.caller_name || b.name     || null;
  const duration    = parseInt(b.call_duration || b.duration || 0);
  const billable    = b.billable === true || b.billable === 'true' || b.billable === 1;
  const pubSub      = b.publisher_sub || b.pub_id || null;
  const payout      = parseFloat(b.payout_amount || b.payout || 0);
  const buyerName   = b.buyer_name  || b.buyer    || null;
  const vertical    = b.vertical    || b.campaign || 'FE';
  const campaign    = b.campaign_name || vertical;

  try {
    await pool.query(
      `INSERT INTO calls
        (call_date, caller_id, caller_name, call_duration, billable,
         publisher_sub, payout_amount, buyer_name, vertical, campaign,
         source_system, raw, received_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [callDate, callerId, callerName, duration, billable,
       pubSub, payout, buyerName, vertical, campaign,
       'trackdrive', JSON.stringify(b),
       tdReceivedAt || new Date().toISOString()]
    );
    res.json({ ok: true, message: 'TrackDrive call recorded' });
  } catch(err) {
    console.error('TrackDrive postback error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});


// ─── SSDI DISPO UPDATE (Google Sheet webhook) ───────────────────────────────
// Receives row data from the Google Sheet whenever a new row is added or edited.
// Looks up the CID in existing SSDI calls to match publisher via DID.
// FE calls are NEVER touched by this endpoint.
app.post('/ssdi/dispo-update', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Parse CID - strip country code, formatting
  let rawPhone = String(b.phone || b.caller_id || b.cid || '').replace(/\D/g, '');
  if (rawPhone.startsWith('1') && rawPhone.length === 11) rawPhone = rawPhone.slice(1);
  if (!rawPhone) return res.status(400).json({ ok: false, error: 'Missing phone/CID' });

  const callerName   = b.full_name   || b.name        || null;
  const state        = b.state       || null;
  const caseStatus   = b.case_status || b.disposition  || null;
  const caseSubStatus = b.case_sub_status || null;
  const convertedDate = b.converted_date || b.call_date || null;
  const leadOwner    = b.lead_owner  || null;
  const centerCode   = b.center_code || null;
  const age          = b.age         || null;
  const payout       = parseFloat(b.payout_amount || b.amnt || 0) || null;

  const client = await pool.connect();
  try {
    // Step 1: Look up existing SSDI call by CID to get publisher/DID
    const lookup = await client.query(
      `SELECT c.id, c.publisher_sub, c.campaign, c.raw, p.payout_rate
       FROM calls c
       LEFT JOIN publishers p ON p.pub_id = c.publisher_sub
       WHERE c.caller_id = $1
         AND c.source_system = 'partner'
         AND c.publisher_sub IS NOT NULL
         AND c.publisher_sub != ''
       ORDER BY c.received_at DESC
       LIMIT 1`,
      [rawPhone]
    );

    const matched = lookup.rows.length > 0;
    const existingCall = matched ? lookup.rows[0] : null;
    const publisherSub = matched ? existingCall.publisher_sub : null;

    // Step 2: Build raw payload for storage
    const rawData = {
      phone: rawPhone,
      full_name: callerName,
      state,
      case_status: caseStatus,
      case_sub_status: caseSubStatus,
      converted_date: convertedDate,
      lead_owner: leadOwner,
      center_code: centerCode,
      age,
      payout_amount: payout,
      matched_publisher: publisherSub,
      source: 'google_sheet'
    };

    // Step 3: If matched, UPDATE the existing call record with dispo info
    // Presence on the sheet = billable regardless of payout value
    if (matched) {
      // Get publisher payout rate if payout not explicitly provided
      const pubRate = payout || parseFloat(existingCall.payout_rate) || 160;
      await client.query(
        `UPDATE calls SET
           caller_name       = COALESCE($1, caller_name),
           disposition       = 'Billable',
           call_status_label = 'cpa',
           billable          = true,
           payout_amount     = $2,
           raw               = raw || $3::jsonb
         WHERE id = $4`,
        [callerName, pubRate, JSON.stringify(rawData), existingCall.id]
      );
    }

    // Step 4: Always INSERT a dispo record for audit trail
    await client.query(
      `INSERT INTO calls
         (call_date, caller_id, caller_name, billable, payout_amount, disposition,
          campaign, vertical, publisher_sub, source_system, call_status_label, raw)
       VALUES
         ($1, $2, $3, $4, $5, $6, 'SSDI', 'SSDI', $7, 'google_sheet',
          CASE WHEN $5 IS NOT NULL THEN 'cpa' ELSE
            CASE WHEN $8 THEN 'matched' ELSE 'unmatched_publisher' END
          END,
          $9::jsonb)
       ON CONFLICT DO NOTHING`,
      [
        convertedDate || new Date().toISOString().split('T')[0],
        rawPhone,
        callerName,
        payout != null,
        payout,
        caseStatus,
        publisherSub,
        matched,
        JSON.stringify(rawData)
      ]
    );

    res.json({
      ok: true,
      matched,
      publisher_sub: publisherSub,
      message: matched
        ? 'Matched to publisher ' + publisherSub
        : 'CID not matched to any KRW publisher DID - flagged as unmatched_publisher'
    });

  } catch (err) {
    console.error('SSDI dispo-update error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    client.release();
  }
});
// ─── END SSDI DISPO UPDATE ──────────────────────────────────────────────────

// ─── GOOGLE SHEET POLLER (SSDI dispo sync) ──────────────────────────────────
// Polls the public Google Sheet every 5 minutes.
// Cross-references CID against existing SSDI calls to match publisher via DID.
// Only touches source_system='partner' AND campaign='SSDI' records.
// FE calls are NEVER touched.

const SHEET_CSV_URL = 'https://docs.google.com/spreadsheets/d/10o3o1IkSp4pigdOtX_Ls48NHsA9hLEWb-vajb5Ejhww/export?format=csv&gid=380172903';
const POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

// Track rows we have already processed to avoid duplicate updates
const processedRows = new Set();

async function fetchSheetCSV() {
  const https = require('https');
  const http  = require('http');
  return new Promise((resolve, reject) => {
    const get = (url, redirectCount = 0) => {
      if (redirectCount > 5) return reject(new Error('Too many redirects'));
      const lib = url.startsWith('https') ? https : http;
      lib.get(url, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return get(res.headers.location, redirectCount + 1);
        }
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => resolve(data));
      }).on('error', reject);
    };
    get(SHEET_CSV_URL);
  });
}

function parseCSV(text) {
  const lines = text.trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.replace(/"/g,'').trim());
  return lines.slice(1).map(line => {
    // Handle quoted fields with commas
    const cols = [];
    let cur = '', inQuote = false;
    for (let i = 0; i < line.length; i++) {
      if (line[i] === '"') { inQuote = !inQuote; continue; }
      if (line[i] === ',' && !inQuote) { cols.push(cur.trim()); cur = ''; continue; }
      cur += line[i];
    }
    cols.push(cur.trim());
    const row = {};
    headers.forEach((h, i) => row[h] = cols[i] || '');
    return row;
  });
}

async function pollSheet() {
  try {
    const csv  = await fetchSheetCSV();
    const rows = parseCSV(csv);
    if (!rows.length) return;

    const client = await pool.connect();
    try {
      let matched = 0, unmatched = 0, skipped = 0;

      for (const row of rows) {
        // Get phone from "Converted Account: Phone" column
        let rawPhone = String(
          row['Converted Account: Phone'] || row['Phone'] || ''
        ).replace(/\D/g, '');
        if (rawPhone.startsWith('1') && rawPhone.length === 11) rawPhone = rawPhone.slice(1);
        if (!rawPhone || rawPhone.length < 7) continue;

        // Build a unique key for this row to avoid reprocessing
        const rowKey = rawPhone + '|' + (row['Converted Date'] || '') + '|' + (row['Converted Account: Case Status'] || '');
        if (processedRows.has(rawPhone)) { skipped++; continue; }

        const callerName    = (row['Full Name'] || '').trim() || null;
        const state         = (row['State/Province'] || '').trim() || null;
        const caseStatus    = (row['Converted Account: Case Status'] || '').trim() || null;
        const caseSubStatus = (row['Converted Account: Case Sub-Status'] || '').trim() || null;
        const convertedDate = (row['Converted Date'] || '').trim() || null;
        const leadOwner     = (row['Lead Owner: Full Name'] || '').trim() || null;
        const amntRaw       = String(row['AMNT'] || row['Amnt'] || '').replace(/[$,]/g,'').trim();
        const payout        = parseFloat(amntRaw) > 0 ? parseFloat(amntRaw) : null;

        // Skip rows that are payment notes, balance owed, or settlement entries
        // These are financial updates to existing cases, not new call records
        const skipStatuses = ['paid', 'balance', 'balance owed', 'settlement', 'owed', 'payment', 'partial payment', 'write off', 'write-off'];
        const statusLower  = (caseStatus || '').toLowerCase().trim();
        const subStatLower = (caseSubStatus || '').toLowerCase().trim();
        if (skipStatuses.some(s => statusLower.includes(s) || subStatLower.includes(s))) {
          continue;
        }

        // Skip rows with no valid phone — these are header/note rows
        // (already checked above but double-confirming after parsing)
        if (!rawPhone || rawPhone.length < 7) continue;

        // Step 1: Look up CID in TrackDrive calls — MUST match one of KRW's DIDs
        // If CID came in on Laird's own DIDs (not ours) → skip entirely
        const KRW_DIDS = ['8338403897', '8338417301', '8338928548'];

        const lookup = await client.query(
          `SELECT c.id, c.publisher_sub, c.call_status_label, p.payout_rate
           FROM calls c
           LEFT JOIN publishers p ON p.pub_id = c.publisher_sub
           WHERE c.caller_id = $1
             AND c.source_system = 'partner'
             AND (c.campaign = 'SSDI' OR c.raw->>'campaign' = 'SSDI')
             AND c.publisher_sub IS NOT NULL
             AND c.publisher_sub != ''
           ORDER BY c.received_at DESC LIMIT 1`,
          [rawPhone]
        );

        // CID not on any of our DIDs — belongs to Laird's own publishers, skip it
        if (lookup.rows.length === 0) {
          unmatched++;
          processedRows.add(rowKey);
          continue;
        }

        const existingCall = lookup.rows[0];
        const publisherSub = existingCall.publisher_sub;
        const publisherPayout = parseFloat(existingCall.payout_rate) || 160;
        const currentStatus = existingCall.call_status_label;

        const rawData = JSON.stringify({
          phone: rawPhone, full_name: callerName, state,
          case_status: caseStatus, case_sub_status: caseSubStatus,
          converted_date: convertedDate, lead_owner: leadOwner,
          publisher_payout: publisherPayout,
          matched_publisher: publisherSub,
          source: 'google_sheet_poll'
        });

        // Step 2: Mark billable regardless of current status
        // This handles cases where a call was previously marked not_converted
        // but then shows up on the sheet — sheet is always the source of truth
        await client.query(
          `UPDATE calls SET
             caller_name       = COALESCE($1, caller_name),
             disposition       = 'Billable',
             billable          = true,
             payout_amount     = $3,
             call_status_label = 'cpa',
             raw               = raw || $4::jsonb
           WHERE id = $5`,
          [callerName, caseStatus || 'Billable', publisherPayout, rawData, existingCall.id]
        );

        if (currentStatus === 'not_converted') {
          console.log('[Sheet Poll] Flipped not_converted → cpa for CID ' + rawPhone + ' (' + publisherSub + ')');
        }

        matched++;
        // Only cache as processed once confirmed billable
        // This allows re-processing if status changes
        processedRows.add(rawPhone);
      }

      if (matched + unmatched > 0) {
        console.log('[Sheet Poll] Processed ' + (matched+unmatched) + ' rows — Matched: ' + matched + ', Unmatched: ' + unmatched + ', Skipped: ' + skipped);
      }
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('[Sheet Poll] Error:', err.message);
  }
}

// Start polling after 10 second delay on boot, then every 5 minutes
setTimeout(() => {
  pollSheet();
  setInterval(pollSheet, POLL_INTERVAL_MS);
}, 10000);
// ─── END GOOGLE SHEET POLLER ────────────────────────────────────────────────

// ─── STALE CALL SWEEPER (28-hour rule) ──────────────────────────────────────
// Runs every 30 minutes.
// Any SSDI call from source_system='partner' that:
//   - has call_status_label = 'pending'
//   - was received more than 28 hours ago
//   - has a publisher_sub (i.e. came through one of KRW's DIDs)
//   - CID is NOT on the current Google Sheet
// → gets marked not_converted, billable = false
// Covers all calls back to May 14, 2026.

const SWEEPER_INTERVAL_MS  = 30 * 60 * 1000; // 30 minutes
const STALE_HOURS          = 28;
const SWEEP_START_DATE     = '2026-05-14';

// In-memory cache of sheet CIDs — refreshed every poll cycle
let sheetCIDCache = new Set();

// Update the sheet CID cache (called from pollSheet too)
async function refreshSheetCIDs() {
  try {
    const csv  = await fetchSheetCSV();
    const rows = parseCSV(csv);
    const cids = new Set();
    for (const row of rows) {
      let rawPhone = String(
        row['Converted Account: Phone'] || row['Phone'] || ''
      ).replace(/\D/g, '');
      if (rawPhone.startsWith('1') && rawPhone.length === 11) rawPhone = rawPhone.slice(1);
      if (rawPhone && rawPhone.length >= 7) cids.add(rawPhone);
    }
    sheetCIDCache = cids;
    console.log('[Sheet Cache] Refreshed - ' + cids.size + ' CIDs on sheet');
    return cids;
  } catch (err) {
    console.error('[Sheet Cache] Error refreshing:', err.message);
    return sheetCIDCache; // return last known cache on error
  }
}

async function sweepStaleCalls() {
  try {
    // Refresh sheet CIDs first
    const sheetCIDs = await refreshSheetCIDs();

    const client = await pool.connect();
    try {
      // Get all pending SSDI calls older than 28 hours, back to May 14
      const result = await client.query(
        `SELECT id, caller_id, publisher_sub, received_at
         FROM calls
         WHERE call_status_label = 'pending'
           AND source_system = 'partner'
           AND (campaign = 'SSDI' OR raw->>'campaign' = 'SSDI')
           AND publisher_sub IS NOT NULL
           AND publisher_sub != ''
           AND received_at < NOW() - INTERVAL '${STALE_HOURS} hours'
           AND call_date >= $1
         ORDER BY received_at ASC`,
        [SWEEP_START_DATE]
      );

      if (result.rows.length === 0) {
        console.log('[Sweeper] No stale pending calls found');
        return;
      }

      let markedNotConverted = 0;
      let markedBillable     = 0;

      for (const call of result.rows) {
        const cid = String(call.caller_id || '').replace(/\D/g, '');

        if (sheetCIDs.has(cid)) {
          // CID IS on the sheet — mark billable (safety net)
          const pubLookup = await client.query(
            'SELECT payout_rate FROM publishers WHERE pub_id = $1',
            [call.publisher_sub]
          );
          const payout = parseFloat(
            (pubLookup.rows[0] || {}).payout_rate || 160
          );

          await client.query(
            `UPDATE calls SET
               billable          = true,
               payout_amount     = $1,
               call_status_label = 'cpa',
               disposition       = COALESCE(NULLIF(disposition,''), 'Billable')
             WHERE id = $2`,
            [payout, call.id]
          );
          markedBillable++;
        } else {
          // CID NOT on sheet after 28 hours — not billable
          await client.query(
            `UPDATE calls SET
               billable          = false,
               call_status_label = 'not_converted',
               disposition       = COALESCE(NULLIF(disposition,''), 'Not Converted')
             WHERE id = $1`,
            [call.id]
          );
          markedNotConverted++;
        }
      }

      console.log('[Sweeper] Done — Not converted: ' + markedNotConverted + ', Billable (caught): ' + markedBillable + ', Total processed: ' + (markedNotConverted + markedBillable));

    } finally {
      client.release();
    }
  } catch (err) {
    console.error('[Sweeper] Error:', err.message);
  }
}

// Start sweeper — first run after 15 seconds, then every 30 minutes
setTimeout(() => {
  sweepStaleCalls();
  setInterval(sweepStaleCalls, SWEEPER_INTERVAL_MS);
}, 15000);
// ─── END STALE CALL SWEEPER ──────────────────────────────────────────────────






// ── CALLS POSTBACK ENDPOINT (SSDI only) ─────────────────
// Called by partner system or buyer at end of day
// Accepts flexible field names to support multiple sources
app.post('/calls/postback', async (req, res) => {
  // Accept any API key from configured sources
  const key = req.headers['x-api-key'] || req.headers['authorization'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY       || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY  || 'krwleads2026secure',
    process.env.PARTNER_API_KEY || '',
    process.env.BUYER_API_KEY   || '',
  ].filter(Boolean);
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Normalize fields - accept multiple naming conventions
  const callDate     = b.call_date   || b.callDate   || b.date        || new Date().toISOString().split('T')[0];
  // Build accurate received_at from call_date + call_time if provided, else use actual call time
  const callTime     = b.call_time   || b.callTime   || b.time        || null;
  const callDatetime = b.call_datetime || b.callDatetime || b.created_at || b.timestamp || null;
  let   receivedAt   = null;
  if (callDatetime) {
    receivedAt = new Date(callDatetime).toISOString();
  } else if (callDate && callTime) {
    receivedAt = new Date(callDate + 'T' + callTime).toISOString();
  } else if (callDate) {
    // Use call_date with current time as best approximation
    receivedAt = new Date(callDate + 'T' + new Date().toTimeString().slice(0,8)).toISOString();
  }
  const callerId     = b.caller_id   || b.callerId   || b.phone       || b.ani        || null;
  const callerName   = b.caller_name || b.callerName || b.name        || b.contact    || null;
  const duration     = parseInt(b.call_duration || b.duration || b.talk_time || 0);
  const billable     = b.billable === true || b.billable === 'true' || b.billable === 1 || b.status === 'billable';
  const incomingDid  = b.did || b.DID || b.tracking_number || b.to_number || null;
  let   pubSub       = b.publisher_sub || b.pub_id || b.sub_id || b.publisher || null;
  const payout       = parseFloat(b.payout_amount || b.payout || b.revenue || b.amount || 0);
  const campaign     = b.campaign_name || b.campaign || b.vertical || null;
  const disposition  = b.disposition || b.call_status || b.status || null;
  const sourceSystem = b.source_system || b.source || 'partner';

  // Auto-resolve publisher from DID if pub_sub not provided
  if (!pubSub && incomingDid) {
    const didClean = String(incomingDid).replace(/\D/g, '');

    // Check publishers table first
    let didLookup = await pool.query(
      'SELECT pub_id FROM publishers WHERE did=$1 AND active=true LIMIT 1',
      [didClean]
    );

    // Also check publisher_campaigns table
    if (!didLookup.rows.length) {
      didLookup = await pool.query(
        'SELECT pub_id FROM publisher_campaigns WHERE did=$1 AND active=true LIMIT 1',
        [didClean]
      );
    }

    if (didLookup.rows.length) {
      pubSub = didLookup.rows[0].pub_id;
      console.log(`DID ${didClean} resolved to publisher: ${pubSub}`);
    } else {
      console.log(`DID ${didClean} not found - storing call without publisher assignment`);
    }
  }

  try {
    // If no payout sent, look up publisher's agreed rate
    let finalPayout = payout;
    if(!finalPayout && billable && pubSub){
      const pubRate = await pool.query(
        'SELECT payout_rate FROM publishers WHERE pub_id=$1 AND active=true LIMIT 1',
        [pubSub]
      );
      if(pubRate.rows.length && pubRate.rows[0].payout_rate){
        finalPayout = parseFloat(pubRate.rows[0].payout_rate);
      }
    }

    // Determine status label
    let statusLabel = 'pending';
    if (b.billable === true  || b.billable === 'true'  || b.billable === 1)  statusLabel = 'cpa';
    if (b.billable === false || b.billable === 'false' || b.billable === 0)  statusLabel = 'not_converted';
    // If billable field not sent at all, keep as pending
    if (b.billable === undefined || b.billable === null) { statusLabel = 'pending'; }

    // Force campaign to SSDI and source to partner for all postbacks
    const forcedCampaign = 'SSDI';
    const forcedSource   = 'partner';

    // Dedupe check - skip if call already exists with same caller_id + call_date + publisher_sub
    if (callerId && callDate && pubSub) {
      const dupe = await pool.query(
        `SELECT id FROM calls WHERE caller_id=$1 AND call_date=$2 AND publisher_sub=$3 LIMIT 1`,
        [callerId, callDate, pubSub]
      );
      if (dupe.rows.length) {
        console.log(`Duplicate skipped: ${callerId} on ${callDate} for ${pubSub}`);
        return res.json({ ok: true, message: 'Call already recorded (duplicate skipped)' });
      }
    }

    console.log(`[INSERT] caller=${callerId} date=${callDate} pub=${pubSub} status=${statusLabel}`);
    try {
      await pool.query(
        `INSERT INTO calls (call_date, caller_id, caller_name, call_duration, billable,
                            publisher_sub, payout_amount, campaign, disposition,
                            source_system, call_status_label, raw, received_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)`,
        [callDate, callerId, callerName, duration,
         statusLabel === 'pending' ? null : billable,
         pubSub, statusLabel === 'cpa' ? finalPayout : null,
         forcedCampaign, disposition, forcedSource, statusLabel, JSON.stringify(b),
         receivedAt || new Date().toISOString()]
      );
      console.log(`[INSERT] ✅ Success: ${callerId}`);
    } catch(insertErr) {
      console.error(`[INSERT] ❌ Failed:`, insertErr.message);
      return res.status(500).json({ ok: false, error: insertErr.message });
    }
    res.json({ ok: true, message: 'Call recorded' });
  } catch(err) {
    console.error('Postback error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── END OF DAY SWEEP ENDPOINT ────────────────────────
// Partner posts all calls with final billable status
// Matched by did + call_date, updates existing records
app.patch('/calls/update', async (req, res) => {
  const key = req.headers['x-api-key'] || req.headers['authorization'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY        || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY   || 'krwleads2026secure',
    process.env.PARTNER_API_KEY || '',
    process.env.BUYER_API_KEY   || '',
  ].filter(Boolean);
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const updates = req.body.calls || [req.body];
  const results = { updated: 0, not_found: 0, errors: 0 };

  for (const item of updates) {
    try {
      // Normalize DID
      const did       = String(item.did || item.DID || '').replace(/\D/g, '');
      const callDate  = item.call_date || item.callDate || new Date().toISOString().split('T')[0];
      const billable  = item.billable === true || item.billable === 'true' || item.billable === 1;
      const statusLabel = billable ? 'cpa' : 'not_converted';

      if (!did) { results.errors++; continue; }

      // Find publisher from DID - check both tables
      let pubSub = item.publisher_sub || null;
      if (!pubSub) {
        let didLookup = await pool.query(
          'SELECT pub_id, payout_rate FROM publishers WHERE did=$1 AND active=true LIMIT 1',
          [did]
        );
        if (!didLookup.rows.length) {
          didLookup = await pool.query(
            'SELECT pub_id FROM publisher_campaigns WHERE did=$1 AND active=true LIMIT 1',
            [did]
          );
        }
        if (didLookup.rows.length) {
          pubSub = didLookup.rows[0].pub_id;
          console.log(`EOD sweep: DID ${did} → publisher ${pubSub}`);
        } else {
          console.log(`EOD sweep: DID ${did} not matched to any publisher`);
        }
      }

      // Get payout rate
      let payout = parseFloat(item.payout_amount || item.payout || 0);
      if (!payout && billable && pubSub) {
        const rateQ = await pool.query(
          'SELECT payout_rate FROM publishers WHERE pub_id=$1 LIMIT 1', [pubSub]
        );
        if (rateQ.rows.length) payout = parseFloat(rateQ.rows[0].payout_rate || 0);
      }

      // Normalize all fields from partner spec
      const callerName  = item.caller_name || item.name        || null;
      const callerId    = String(item.caller_id || item.phone  || '').replace(/\D/g,'');
      const duration    = parseInt(item.call_duration || item.duration || 0);
      const state       = item.state        || null;
      const disposition = item.disposition  || item.status     || null;
      const notes       = item.notes        || null;
      const recording   = item.recording    || null;
      const campaign    = item.campaign     || item.campaign_name || 'SSDI';

      // Update existing record matched by DID + call_date
      const updateQ = await pool.query(
        `UPDATE calls SET
          billable          = $1,
          call_status_label = $2,
          payout_amount     = $3,
          caller_name       = COALESCE($7, caller_name),
          caller_id         = COALESCE(NULLIF($8,''), caller_id),
          call_duration     = COALESCE(NULLIF($9,0), call_duration),
          disposition       = COALESCE($10, disposition),
          campaign          = COALESCE($11, campaign)
        WHERE publisher_sub = $4
          AND call_date     = $5
          AND (call_status_label = 'pending' OR caller_id = $6)
        RETURNING id`,
        [billable, statusLabel, billable ? payout : null,
         pubSub, callDate, callerId,
         callerName, callerId, duration || null, disposition, campaign]
      );

      if (updateQ.rowCount > 0) {
        results.updated++;
        console.log(`Updated call: pub=${pubSub} did=${did} date=${callDate} billable=${billable} payout=${payout}`);
      } else {
        // Record not found - check for any dupe before inserting
        const dupeCheck = await pool.query(
          `SELECT id FROM calls WHERE caller_id=$1 AND call_date=$2 LIMIT 1`,
          [callerId, callDate]
        );
        if (dupeCheck.rows.length) {
          // Update the existing record instead
          await pool.query(
            `UPDATE calls SET billable=$1, call_status_label=$2, payout_amount=$3,
             caller_name=COALESCE($4,caller_name), disposition=COALESCE($5,disposition),
             publisher_sub=COALESCE($6,publisher_sub)
             WHERE caller_id=$7 AND call_date=$8`,
            [billable, statusLabel, billable ? payout : null,
             callerName, disposition, pubSub, callerId, callDate]
          );
          results.updated++;
        } else {
          await pool.query(
            `INSERT INTO calls (call_date, caller_id, caller_name, call_duration,
                                billable, publisher_sub, payout_amount, campaign,
                                disposition, call_status_label, source_system, raw)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'partner',$11)`,
            [callDate, callerId, callerName, duration || null,
             billable, pubSub, billable ? payout : null, campaign,
             disposition, statusLabel, JSON.stringify(item)]
          );
          console.log(`Inserted new call: pub=${pubSub} did=${did} date=${callDate}`);
          results.not_found++;
        }
      }
    } catch (err) {
      console.error('Update error:', err.message);
      results.errors++;
    }
  }

  // Send email notification summary
  // Calculate stats for email
  const totalCalls   = results.updated + results.not_found;
  const converted    = results.updated;
  const convRate     = totalCalls > 0 ? Math.round((converted / totalCalls) * 100) : 0;
  const yesterday    = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const dateStr      = yesterday.toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', year:'numeric' });

  const html = `<p>You had <strong>${totalCalls}</strong> unique SSDI transfers yesterday, ${dateStr}. Out of those <strong>${totalCalls}</strong> calls, <strong>${converted}</strong> converted at <strong>${convRate}%</strong>.</p><p style="color:#999;font-size:12px;margin-top:16px">KRW Marketing Solutions</p>`;

  await sendEmailNotification(`SSDI Daily Report - ${dateStr}`, html);

  res.json({ ok: true, message: 'End-of-day sweep complete', ...results });
});

// Get calls feed (dashboard)
app.get('/calls/feed', requireKey, async (req, res) => {
  const { days = 30, pub } = req.query;
  try {
    const daysInt = parseInt(days) >= 9999 ? 36500 : parseInt(days);
    const params = [];
    let query = `SELECT * FROM calls WHERE source_system IN ('partner','sheet_import','trackdrive_webhook','ringfuel_import','ringfuel_webhook')`;
    if (daysInt < 9999) {
      query += ` AND received_at::timestamptz >= NOW() - INTERVAL '${daysInt} days'`;
    }
    if (pub) {
      params.push(pub);
      query += ` AND publisher_sub=$${params.length}`;
    }
    query += ' ORDER BY received_at DESC LIMIT 1000';
    const r = await pool.query(query, params);
    res.json({ ok: true, calls: r.rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// Calls summary (dashboard KPIs)
app.get('/calls/summary', requireKey, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        COUNT(*)                                                                        AS total,
        COUNT(*) FILTER(WHERE billable=true)                                            AS billable,
        COUNT(*) FILTER(WHERE call_date=(NOW() AT TIME ZONE 'America/New_York')::date::text OR (DATE(received_at AT TIME ZONE 'America/New_York'))=(NOW() AT TIME ZONE 'America/New_York')::date) AS today,
        COALESCE(SUM(payout_amount) FILTER(WHERE billable=true),0)                     AS total_payout
      FROM calls
      WHERE source_system IN ('partner','sheet_import','trackdrive_webhook','ringfuel_import','ringfuel_webhook')
        AND (source_system='sheet_import' OR received_at >= NOW() - INTERVAL '30 days')
    `);
    res.json({ ok: true, ...r.rows[0] });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Start ─────────────────────────────────────────────
initDB()
  .then(() => initLeadsDB())
  .then(() => initCampaignsDB())
  .then(() => initPublishersDB())
  .then(() => {
    

// ─── ROBLOX MASS TORT — TRUE BLUE FORWARDING ────────────────────────────────
// Receives a lead from a publisher, validates required fields,
// and forwards to True Blue Marketing's LeadsPedia endpoint.
// Campaign: roblox-mt | Buyer: True Blue Marketing
// This is completely separate from SSDI and FE verticals.

const TRUEBLUE_URL         = 'https://trueblue.leadspediatrack.com/post.do';
const TRUEBLUE_CAMPAIGN_ID = '6a2062006f3f4';
const TRUEBLUE_CAMPAIGN_KEY = 'qWGfxLDQMzP6mhbHkp24';

app.post('/leads/roblox', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Validate required fields
  const missing = [];
  if (!b.first_name)     missing.push('first_name');
  if (!b.last_name)      missing.push('last_name');
  if (!b.phone_home)     missing.push('phone_home');
  if (!b.email_address)  missing.push('email_address');
  if (!b.jornaya_lead_id && !b.trusted_form_cert_id) missing.push('jornaya_lead_id or trusted_form_cert_id');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  // Build True Blue payload
  const payload = new URLSearchParams();
  payload.append('lp_campaign_id',  TRUEBLUE_CAMPAIGN_ID);
  payload.append('lp_campaign_key', TRUEBLUE_CAMPAIGN_KEY);
  payload.append('lp_response',     'json');

  // Required fields
  payload.append('first_name',    b.first_name);
  payload.append('last_name',     b.last_name);
  payload.append('phone_home',    b.phone_home);
  payload.append('email_address', b.email_address);

  // Optional fields — only append if provided
  const optionalFields = [
    'phone_cell','phone_work','phone_ext','address','address2',
    'city','state','zip_code','county','country','dob',
    'ip_address','exposed','child_claim','injury','attorney',
    'incident_date','lp_s1','lp_s2','lp_s3','lp_s4','lp_s5',
    'lp_caller_id','landing_page_url','description','lp_test'
  ];
  optionalFields.forEach(f => { if (b[f]) payload.append(f, b[f]); });

  // TCPA compliance
  if (b.jornaya_lead_id)       payload.append('jornaya_lead_id',       b.jornaya_lead_id);
  if (b.trusted_form_cert_id)  payload.append('trusted_form_cert_id',  b.trusted_form_cert_id);

  // Publisher sub tracking
  const publisherSub = b.publisher_sub || b.lp_s1 || null;

  // Log the lead attempt
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, ip_address, status, raw, received_at)
       VALUES ('roblox-mt','Mass Tort - Roblox',$1,$2,$3,$4,$5,$6,'pending',$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone_home, b.email_address,
       publisherSub, b.ip_address || null,
       JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[Roblox Lead] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Forward to True Blue
  try {
    const https = require('https');
    const postData = payload.toString();

    const tbRes = await new Promise((resolve, reject) => {
      const url = new URL(TRUEBLUE_URL);
      const options = {
        hostname: url.hostname,
        path:     url.pathname,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const req2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      req2.on('error', reject);
      req2.write(postData);
      req2.end();
    });

    // Parse JSON response from True Blue
    let tbResult = {};
    try { tbResult = JSON.parse(tbRes.body); } catch(e) {
      // Try XML fallback
      const xmlResult = tbRes.body.match(/<result>(.*?)<\/result>/)?.[1] || 'unknown';
      const tbLeadId  = tbRes.body.match(/<lead_id>(.*?)<\/lead_id>/)?.[1] || null;
      const price     = tbRes.body.match(/<price>(.*?)<\/price>/)?.[1] || '0.00';
      tbResult = { result: xmlResult, lead_id: tbLeadId, price };
    }

    const accepted = tbResult.result === 'success';

    // Update lead status in DB
    if (leadId) {
      const c2 = await pool.connect();
      try {
        await c2.query(
          `UPDATE leads SET
             status           = $1,
             buyer_intake_id  = $2,
             buyer_response   = $3::jsonb,
             revenue          = $4
           WHERE id = $5`,
          [
            accepted ? 'forwarded' : 'buyer_rejected',
            tbResult.lead_id || null,
            JSON.stringify(tbResult),
            parseFloat(tbResult.price) || 0,
            leadId
          ]
        );
      } finally { c2.release(); }
    }

    console.log(`[Roblox Lead] \${accepted ? '✅' : '❌'} \${b.first_name} \${b.last_name} → \${tbResult.result} | \${tbResult.lead_id || 'no id'} | $\${tbResult.price || '0.00'}`);

    return res.json({
      ok:       accepted,
      result:   tbResult.result,
      lead_id:  tbResult.lead_id || null,
      price:    tbResult.price   || '0.00',
      message:  accepted ? 'Lead accepted by True Blue' : 'Lead rejected by True Blue',
      errors:   tbResult.errors  || null,
      krw_id:   leadId
    });

  } catch (fwdErr) {
    console.error('[Roblox Lead] Forward error:', fwdErr.message);

    if (leadId) {
      const c3 = await pool.connect();
      try {
        await c3.query(
          "UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2",
          [fwdErr.message, leadId]
        );
      } finally { c3.release(); }
    }

    return res.status(502).json({ ok: false, error: 'Failed to forward to buyer', detail: fwdErr.message });
  }
});
// ─── END ROBLOX MASS TORT ────────────────────────────────────────────────────


// ─── ROBLOX — CH-AD (LA-HI) ──────────────────────────────────────────────────
// New, separate campaign - distinct from the (dead) True Blue Roblox campaign
// above. Publisher: LA-HI | Buyer: CH-AD | CPA: $2,100 | Straight post to
// Zapier, no real-time bid/response to parse (Kyler, Sep 10).
const CHAD_WEBHOOK_URL = 'https://hooks.zapier.com/hooks/catch/23024319/4d3yhob/';

app.post('/leads/roblox-chad', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Core identifying fields only - everything else from the qualification
  // criteria is optional and passes through blank if not provided, per
  // Kyler's instruction (Sep 10). have_attorney is checked separately below
  // since it's an explicit qualification gate, not just an optional field.
  const missing = [];
  if (!b.first_name) missing.push('first_name');
  if (!b.last_name)  missing.push('last_name');
  if (!b.phone)      missing.push('phone');
  if (!b.email)      missing.push('email');
  if (missing.length) {
    { const rid = await logRejectedPost('roblox-chad', 'LA-HI-ROBLOX', b, 'Missing: ' + missing.join(', ')); return res.status(400).json({ ok: false, error: 'Missing required fields', missing, krw_id: rid }); }
  }

  // Qualification gate: "Already signed with an attorney (Must be NO)".
  // Only rejects when explicitly Yes - a blank/missing value still pushes
  // through, matching Kyler's instruction that unanswered fields shouldn't
  // block the lead.
  if (String(b.have_attorney || '').toLowerCase() === 'yes') {
    return res.status(400).json({ ok: false, error: 'Lead already represented by an attorney' });
  }

  const publisherSub = 'LA-HI-ROBLOX';

  const payload = {
    pub_id: 'LA-HI',
    buyer: 'CH-AD',
    campaign: 'roblox-chad',
    first_name: b.first_name,
    last_name: b.last_name,
    phone: b.phone,
    email: b.email,
    have_attorney: b.have_attorney || '',
    trustedform_cert_url: b.trustedform_cert_url || '',
    roblox_username: b.roblox_username || '',
    filing_for: b.filing_for || '',
    victim_name: b.victim_name || '',
    age_at_abuse: b.age_at_abuse || '',
    abuser_name: b.abuser_name || '',
    address: b.address || '',
    best_time_to_contact: b.best_time_to_contact || '',
  };

  // Log the lead attempt
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, status, raw, received_at)
       VALUES ('roblox-chad','Mass Tort - Roblox',$1,$2,$3,$4,$5,'pending',$6::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email, publisherSub, JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[Roblox CH-AD] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Forward to Zapier - straight post, no bid/response to parse
  try {
    const zapRes = await fetch(CHAD_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const zapData = await zapRes.json().catch(() => ({}));

    if (leadId) {
      const c2 = await pool.connect();
      try {
        await c2.query(
          "UPDATE leads SET status='forwarded' WHERE id=$1",
          [leadId]
        );
      } finally { c2.release(); }
    }

    return res.json({ ok: true, result: 'success', message: 'Lead forwarded to CH-AD', krw_id: leadId, zapier: zapData });
  } catch (fwdErr) {
    console.error('[Roblox CH-AD] Forward error:', fwdErr.message);
    if (leadId) {
      const c3 = await pool.connect();
      try {
        await c3.query(
          "UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2",
          [fwdErr.message, leadId]
        );
      } finally { c3.release(); }
    }
    return res.status(502).json({ ok: false, error: 'Failed to forward to buyer', detail: fwdErr.message });
  }
});
// ─── END ROBLOX — CH-AD ──────────────────────────────────────────────────────


// ─── RIDESHARE — CH-AD (LA-HI) ───────────────────────────────────────────────
// New, separate campaign - distinct from the (dead) True Blue Rideshare
// campaign. Publisher: LA-HI | Buyer: CH-AD | CPA: $2,100 | Straight post to
// Zapier, no real-time bid/response to parse (Kyler, Sep 10).
app.post('/leads/rideshare-chad', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Core identifying fields only - everything else from the qualification
  // criteria is optional and passes through blank if not provided, per
  // Kyler's instruction (Sep 10). have_attorney is checked separately below
  // since it's an explicit qualification gate, not just an optional field.
  const missing = [];
  if (!b.first_name) missing.push('first_name');
  if (!b.last_name)  missing.push('last_name');
  if (!b.phone)      missing.push('phone');
  if (!b.email)      missing.push('email');
  if (missing.length) {
    { const rid = await logRejectedPost('rideshare-chad', 'LA-HI-RIDESHARE', b, 'Missing: ' + missing.join(', ')); return res.status(400).json({ ok: false, error: 'Missing required fields', missing, krw_id: rid }); }
  }

  // Qualification gate: "Confirmation lead is not already represented by an
  // attorney". Only rejects when explicitly Yes - matching the same pattern
  // as Roblox above.
  if (String(b.have_attorney || '').toLowerCase() === 'yes') {
    return res.status(400).json({ ok: false, error: 'Lead already represented by an attorney' });
  }

  const publisherSub = 'LA-HI-RIDESHARE';

  const payload = {
    pub_id: 'LA-HI',
    buyer: 'CH-AD',
    campaign: 'rideshare-chad',
    first_name: b.first_name,
    last_name: b.last_name,
    phone: b.phone,
    email: b.email,
    have_attorney: b.have_attorney || '',
    trustedform_cert_url: b.trustedform_cert_url || '',
    active_ride_confirmed: b.active_ride_confirmed || '',
    incident_date: b.incident_date || '',
    incident_city: b.incident_city || '',
    incident_state: b.incident_state || '',
    driver_assault_confirmed: b.driver_assault_confirmed || '',
    description: b.description || '',
    police_report_filed: b.police_report_filed || '',
    medical_treatment: b.medical_treatment || '',
    address: b.address || '',
    best_time_to_contact: b.best_time_to_contact || '',
  };

  // Log the lead attempt
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, state, status, raw, received_at)
       VALUES ('rideshare-chad','Rideshare Assault',$1,$2,$3,$4,$5,$6,'pending',$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email, publisherSub, b.incident_state || null, JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[Rideshare CH-AD] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Forward to Zapier - straight post, no bid/response to parse
  try {
    const zapRes = await fetch(CHAD_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const zapData = await zapRes.json().catch(() => ({}));

    if (leadId) {
      const c2 = await pool.connect();
      try {
        await c2.query(
          "UPDATE leads SET status='forwarded' WHERE id=$1",
          [leadId]
        );
      } finally { c2.release(); }
    }

    return res.json({ ok: true, result: 'success', message: 'Lead forwarded to CH-AD', krw_id: leadId, zapier: zapData });
  } catch (fwdErr) {
    console.error('[Rideshare CH-AD] Forward error:', fwdErr.message);
    if (leadId) {
      const c3 = await pool.connect();
      try {
        await c3.query(
          "UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2",
          [fwdErr.message, leadId]
        );
      } finally { c3.release(); }
    }
    return res.status(502).json({ ok: false, error: 'Failed to forward to buyer', detail: fwdErr.message });
  }
});
// ─── END RIDESHARE — CH-AD ───────────────────────────────────────────────────


// ─── MVA — EMAIL AGENCY ROUTING ──────────────────────────────────────────────
// Priority states go to Email Agency first.
// All other states fall through to mva-nld2 (NLD) as fallback.
// Campaign: mva-email-agency | Buyer: Email Agency (LawLogic)
// Publisher: Kevin Anthony (KRW-KANTHONY-RS)

const EMAIL_AGENCY_URL  = 'https://docs.emailagency.com/api/add-lead?json=1';
const EMAIL_AGENCY_KEY  = 'e37b1b02-65cf-11f1-b481-fa163eff53f0';
const EMAIL_AGENCY_CODE = 'MVALEADS';

// 13 priority states for Email Agency
const EMAIL_AGENCY_STATES = ['AZ','CO','IL','IN','MS','NM','NV','NY','OR','TN','UT','WA','WI'];

app.post('/leads/mva-email-agency', async (req, res) => {
  // DEPRECATED as of 2026-08-13 — Email Agency replaced by NLD as the MVA CPA
  // buyer (see /leads/mva-funnel). This endpoint is hard-blocked, not just
  // unused, so nothing can ever silently reach Email Agency through it again.
  // If something is actually depending on this URL, it needs to be pointed
  // at /leads/mva-funnel instead.
  console.error('[MVA Email Agency] ✕ BLOCKED — this endpoint is deprecated. Use /leads/mva-funnel instead.');
  return res.status(410).json({
    ok: false,
    error: 'This endpoint is deprecated and no longer forwards to Email Agency. Use /leads/mva-funnel instead.',
  });

  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Required fields
  const missing = [];
  if (!b.first_name)     missing.push('first_name');
  if (!b.last_name)      missing.push('last_name');
  if (!b.phone)          missing.push('phone');
  if (!b.email)          missing.push('email');
  if (!b.state)          missing.push('state');
  if (!b.incident_date)  missing.push('incident_date');
  if (!b.have_attorney)  missing.push('have_attorney');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub || null;
  const stateUpper   = (b.state || '').toUpperCase().trim();
  const isEmailAgencyState = EMAIL_AGENCY_STATES.includes(stateUpper);

  // DB insert
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          state, zip, publisher_sub, ip_address, status, raw, received_at)
       VALUES ('mva-email-agency','MVA',$1,$2,$3,$4,$5,$6,$7,$8,'pending',$9::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       stateUpper, b.zip || null,
       publisherSub, b.ip_address || null,
       JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[MVA Email Agency] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Route based on state
  if (!isEmailAgencyState) {
    // State not in Email Agency priority list — hold for new buyer
    const c2 = await pool.connect();
    try {
      await c2.query(
        "UPDATE leads SET status='received', buyer_error='State not in Email Agency coverage — awaiting new buyer' WHERE id=$1",
        [leadId]
      );
    } finally { c2.release(); }

    console.log(`[MVA Email Agency] ⏸ ${b.first_name} ${b.last_name} | State: ${stateUpper} — not in priority list, held for new buyer`);
    return res.json({
      ok:      false,
      result:  'held',
      message: 'State not in current buyer coverage — lead saved and held for routing',
      krw_id:  leadId,
    });
  }

  // Forward to Email Agency
  try {
    const payload = {
      key:                   EMAIL_AGENCY_KEY,
      code:                  EMAIL_AGENCY_CODE,
      first_name:            b.first_name,
      last_name:             b.last_name,
      phone:                 b.phone,
      email:                 b.email,
      state:                 stateUpper,
      zip:                   b.zip                 || null,
      address:               b.address             || null,
      city:                  b.city                || null,
      dob:                   b.dob                 || null,
      ip_address:            b.ip_address          || null,
      user_agent:            b.user_agent          || null,
      attorney:              b.have_attorney || b.attorney       || null,
      date_of_incident:      b.incident_date                     || null,
      accident_fault:        b.at_fault      || b.accident_fault || null,
      settlement:            b.settlement          || null,
      cited:                 b.cited               || null,
      received_treatment:    b.doctor_treatment    || null,
      has_injuries:          b.physical_injury     || null,
      jornaya_id:            b.jornaya_leadid      || null,
      trusted_form_cert_url: b.trustedform_cert_url|| null,
      sub_id2:               aliasPub(publisherSub),
      channel:               (['Facebook','Google','Email','SMS','Display','Native','Other'].includes(b.channel) ? b.channel : 'Facebook'),
      language:              b.language            || 'English',
      // Additional fields if provided
      mva_injury:            b.mva_injury          || null,
      mva_type:              b.mva_type            || null,
      mva_treatment:         b.mva_treatment       || null,
      police_report:         b.police_report       || null,
      accident_state:        stateUpper,
      accident_location:     b.accident_location   || null,
      lost_wages:            b.lost_wages          || null,
      accident_type:         b.accident_type       || null,
      driver_or_passenger:   b.driver_or_passenger || null,
      other_party_at_fault:  b.other_party_at_fault|| null,
    };

    // Strip null values — don't send empty fields to Email Agency
    Object.keys(payload).forEach(k => { if (payload[k] === null || payload[k] === undefined) delete payload[k]; });

    const https   = require('https');
    const postData = JSON.stringify(payload);
    const url      = new URL(EMAIL_AGENCY_URL);

    const eaRes = await new Promise((resolve, reject) => {
      const options = {
        hostname: url.hostname,
        path:     url.pathname + url.search,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const r2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      r2.on('error', reject);
      r2.write(postData);
      r2.end();
    });

    let eaResult = {};
    try { eaResult = JSON.parse(eaRes.body); } catch(e) { eaResult = { status: false, message: eaRes.body }; }

    const accepted = eaResult.status === true;

    // Update DB
    const c3 = await pool.connect();
    try {
      await c3.query(
        `UPDATE leads SET
           status          = $1,
           buyer_intake_id = $2,
           buyer_response  = $3::jsonb,
           buyer_error     = $4
         WHERE id = $5`,
        [
          accepted ? 'forwarded' : 'buyer_rejected',
          eaResult.lead_id || null,
          JSON.stringify(eaResult),
          accepted ? null : eaResult.message,
          leadId
        ]
      );
    } finally { c3.release(); }

    console.log(`[MVA Email Agency] ${accepted ? '✅' : '❌'} ${b.first_name} ${b.last_name} | ${stateUpper} → ${eaResult.message} | Lead ID: ${eaResult.lead_id || 'none'}`);

    return res.json({
      ok:      accepted,
      result:  accepted ? 'success' : 'failed',
      lead_id: eaResult.lead_id || null,
      message: eaResult.message,
      krw_id:  leadId,
    });

  } catch(fwdErr) {
    console.error('[MVA Email Agency] Forward error:', fwdErr.message);
    const c4 = await pool.connect();
    try {
      await c4.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2", [fwdErr.message, leadId]);
    } finally { c4.release(); }
    return res.status(502).json({ ok: false, error: 'Failed to forward to Email Agency', detail: fwdErr.message });
  }
});
// ─── END MVA EMAIL AGENCY ─────────────────────────────────────────────────────

// ─── MVA FUNNEL — TIERED BUYER ROUTING ───────────────────────────────────────
// Routes MVA leads through a tiered buyer waterfall based on state.
// Campaign: mva-funnel
//
// TO ADD A NEW BUYER: add an entry to MVA_BUYERS array below with:
//   - name:   display name for logs
//   - states: array of 2-letter state codes this buyer accepts
//   - post:   async function(b, publisherSub) → { accepted, duplicate, lead_id, raw }
//
// SSDI and FE verticals are NEVER touched here.
// ─────────────────────────────────────────────────────────────────────────────

const EA_MVA_URL  = 'https://docs.emailagency.com/api/add-lead?json=1';
const EA_MVA_KEY  = 'e37b1b02-65cf-11f1-b481-fa163eff53f0';
const EA_MVA_CODE = 'MVALEADS';

// ── Helper: post JSON to a URL via https ──────────────────────────────────────
function postJSON(urlStr, payload) {
  return new Promise((resolve, reject) => {
    const https    = require('https');
    const postData = JSON.stringify(payload);
    const url      = new URL(urlStr);
    const options  = {
      hostname: url.hostname,
      path:     url.pathname + (url.search || ''),
      method:   'POST',
      headers:  {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(postData),
      }
    };
    const req2 = https.request(options, (r) => {
      let data = '';
      r.on('data', chunk => data += chunk);
      r.on('end', () => resolve({ status: r.statusCode, body: data }));
    });
    req2.on('error', reject);
    req2.write(postData);
    req2.end();
  });
}

// ── Publisher Alias Map ────────────────────────────────────────────────────
// Disguises internal pub_id values before they're sent to any external buyer
// as sub_id2 / publisher_sub / source. Buyers should never see which
// publisher sent a lead — only KRW needs that mapping internally.
const PUBLISHER_ALIAS = {
  'KRW-KANTHONY-RS':     'MVA K',
  'KRW-KANTHONY-2026-SMG': 'MVA K',
  'KRW-LAIRD-2026-JEM':  'MVA L',
  'KRW-LAIRD-2026-X23':  'MVA L',
  'KRW-LAIRD-2026-1L2':  'MVA L',
  'KRW-SHORE-2026-LSD':  'SSDI S',
  'KRW-JOSHUA-2026-76M': 'KRW-SSD-01',
  'KRW-MVA-2026-8RT':    'MVA 2',
  'KRW-KANTHONY-CPL':    'MVA K CPL',
};

function aliasPub(pubId) {
  if (!pubId) return null;
  return PUBLISHER_ALIAS[pubId] || pubId;
}

// ── MVA Buyer Tiers ───────────────────────────────────────────────────────────
// Add new buyers here. Order = priority (Tier 1 first).
const MVA_BUYERS = [

  // ── Tier 1: NLD CPA (campaign 31080) ──────────────────────────────────────
  // Replaced Email Agency per Kyler's explicit instruction — all CPA leads,
  // every state, route here now. Confirmed: lp_subid1 uses aliasPub(), so
  // this buyer never sees a publisher's real pub_id or name — same identity
  // protection already used everywhere else in this system.
  {
    name:   'NLD CPA',
    states: ['UT','MT','WY','AZ','CA','NV','OK','NE','ND','IA','NM'], // NLD only accepts these states (PA removed, CA added - Kyler, Sep 15; this is the array MVA_BUYERS.find() actually uses, previous fix to a different unused variable never touched this)
    async post(b, publisherSub) {
      const stateCode = (b.state || b.incident_state || '').toUpperCase().trim();
      const incidentStateFull = US_STATE_FULL_NAMES[stateCode] || b.incident_state || null;
      // Auto-correct date_of_birth format regardless of what the publisher
      // sends (mm/dd/yyyy, already-ISO, etc.) - confirmed via a real
      // rejection that this buyer requires strict YYYY-MM-DD on this field.
      // incident_date is NOT converted here - confirmed via the same real
      // rejection that this campaign accepts it as-is in mm/dd/yyyy; this is
      // a different NLD campaign than the Ping/Post one and has different
      // format requirements, don't assume they match.
      const isoDateOfBirth = convertDateToISO(b.date_of_birth);

      const payload = {
        lp_campaign_id: '31080',
        lp_supplier_id: '110928',
        lp_key:         'ke21sx0koi7dld',
        lp_action:      b.lp_test_mode === true ? 'test' : '',
        lp_subid1:      aliasPub(publisherSub) || '',
        first_name:     b.first_name,
        last_name:      b.last_name,
        email:          b.email,
        phone:          String(b.phone).replace(/\D/g, ''),
        date_of_birth:  isoDateOfBirth,
        gender:         b.gender || undefined,
        address:        b.address,
        city:           b.city,
        state:          stateCode,
        zip_code:       b.zip_code || b.zip,
        ip_address:     b.ip_address,
        user_agent:     b.user_agent || undefined,
        landing_page_url: b.landing_page_url,
        jornaya_leadid: b.jornaya_leadid || undefined,
        trustedform_cert_url: b.trustedform_cert_url || b.trusted_form_cert_url || undefined,
        tcpa_text:      b.tcpa_text || undefined,
        incident_state: incidentStateFull,
        incident_date:  b.incident_date,
        have_attorney:  b.have_attorney,
        at_fault:       b.at_fault,
        settlement:     b.settlement,
        cited:          b.cited,
        doctor_treatment: b.doctor_treatment,
        physical_injury:  b.physical_injury,
      };
      Object.keys(payload).forEach(k => { if (payload[k] === undefined) delete payload[k]; });

      const res  = await postJSON('https://api.leadprosper.io/direct_post', payload);
      let   result = {};
      try { result = JSON.parse(res.body); } catch(e) { result = { status: 'ERROR', message: res.body }; }
      return {
        accepted:  result.status === 'ACCEPTED',
        duplicate: result.status === 'DUPLICATED',
        lead_id:   result.lead_id || null,
        message:   result.message || result.status || null,
        raw:       result,
      };
    }
  },

  // ── Tier 2: MVA-003-LT — nationwide (CA/CO hard-blocked upstream) ────────
  // Added Aug 18 per Kyler's instruction — new primary buyer for everything
  // NLD doesn't accept. Delivered via Zapier catch webhook, flexible JSON
  // matching the buyer's own landing page field set (motorinjurycenter.com).
  // CA and CO never reach this far — blocked earlier in the endpoint — but
  // are excluded here too for clarity in case that upstream block ever moves.
  {
    name:   'MVA-003-LT',
    states: ['AL','AK','AZ','AR','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA',
              'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK',
              'OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'], // all states except CA, CO
    async post(b, publisherSub) {
      const stateCode = (b.state || b.incident_state || '').toUpperCase().trim();
      const incidentStateFull = US_STATE_FULL_NAMES[stateCode] || b.incident_state || null;

      const payload = {
        lp_subid1:       aliasPub(publisherSub) || '',
        first_name:      b.first_name,
        last_name:       b.last_name,
        email:           b.email,
        phone:           String(b.phone).replace(/\D/g, ''),
        at_fault:        b.at_fault,
        have_attorney:   b.have_attorney,
        has_insurance:   b.has_insurance,
        physical_injury: b.physical_injury,
        doctor_treatment: b.doctor_treatment,
        state:           incidentStateFull || stateCode,
        zip_code:        b.zip_code || b.zip,
        incident_date:   b.incident_date,
        case_description: b.case_description || `At fault: ${b.at_fault || ''}. Has attorney: ${b.have_attorney || ''}. Physical injury: ${b.physical_injury || ''}. Treatment: ${b.doctor_treatment || ''}.`,
        trustedform_cert_url: b.trustedform_cert_url || b.trusted_form_cert_url || undefined,
        ip_address:      b.ip_address || undefined,
      };
      Object.keys(payload).forEach(k => { if (payload[k] === undefined) delete payload[k]; });

      let result = {};
      try {
        const res = await postJSON('https://hooks.zapier.com/hooks/catch/23024319/4tdo5z8/', payload);
        try { result = JSON.parse(res.body); } catch(e) { result = { status: res.status, raw: res.body }; }
      } catch (err) {
        result = { status: 'ERROR', message: err.message };
      }

      // Zapier catch hooks always return 200/"success" on receipt — this
      // confirms delivery, not buyer acceptance. Treated as accepted since
      // there's no buyer-side accept/reject response defined yet.
      return {
        accepted:  true,
        duplicate: false,
        lead_id:   result.id || null,
        message:   'Delivered to MVA-003-LT',
        raw:       result,
      };
    }
  },

  // ── Tier 3: Email Agency — catch-all for states NLD/MVA-003-LT don't accept ─
  // Reactivated per Kyler's instruction (Aug 13) — NLD only accepts a
  // specific 11-state list (Tier 1 above); everything else falls through to
  // Email Agency. Uses the exact same payload logic and constants proven
  // working before today's NLD migration. Identity fully aliased, same as
  // every other buyer in this system.
  {
    name:   'Email Agency',
    states: ['AZ','IL','IN','MS','NM','NV','NY','OR','TN','UT','WA','WI'], // CO removed — CA/CO hard-blocked upstream now
    async post(b, publisherSub) {
      const payload = {
        key:          EA_MVA_KEY,
        code:         EA_MVA_CODE,
        first_name:   b.first_name,
        last_name:    b.last_name,
        phone:        b.phone,
        email:        b.email,
        ip_address:   b.ip_address,
        attorney:     b.have_attorney,
        accident_fault: b.at_fault,
        channel:      (['Facebook','Google','Email','SMS','Display','Native','Other'].includes(b.channel) ? b.channel : 'Facebook'),
        trusted_form_cert_url: b.trustedform_cert_url || b.trusted_form_cert_url,
        sub_id2:      aliasPub(publisherSub),
      };
      if (b.address)                    payload.address    = b.address;
      if (b.city)                       payload.city       = b.city;
      if (b.state || b.incident_state)  payload.state      = b.state || b.incident_state;
      if (b.zip_code || b.zip)          payload.zip        = b.zip_code || b.zip;
      if (b.date_of_birth)              payload.dob        = b.date_of_birth;
      if (b.user_agent)                 payload.user_agent = b.user_agent;

      const res  = await postJSON(EA_MVA_URL, payload);
      let   result = {};
      try { result = JSON.parse(res.body); } catch(e) { result = { status: false, message: res.body }; }
      return {
        accepted:  result.status === true,
        duplicate: (result.message || '').toLowerCase().includes('duplicate'),
        lead_id:   result.lead_id || null,
        message:   result.message || null,
        raw:       result,
      };
    }
  },

  // ── Tier 3: Placeholder — add CPL buyer here when ready ──────────────────
  // {
  //   name:   'CPL Buyer',
  //   states: ['CA','TX','FL', ...],
  //   async post(b, publisherSub) {
  //     const payload = { ... map fields to their API spec ... };
  //     const res = await postJSON('https://buyer2api.com/post', payload);
  //     let result = {};
  //     try { result = JSON.parse(res.body); } catch(e) { result = { status: false }; }
  //     return {
  //       accepted:  result.accepted === true,
  //       duplicate: false,
  //       lead_id:   result.id || null,
  //       message:   result.message || null,
  //       raw:       result,
  //     };
  //   }
  // },

  // ── Tier 3: Placeholder — add 3rd level buyer here when ready ────────────
  // {
  //   name:   'Tier 3 Buyer',
  //   states: ['GA','NC','VA', ...],
  //   async post(b, publisherSub) { ... }
  // },

];

// ─── NLD PING/POST — SHARED FAILOVER LAYER FOR MVA-FUNNEL + MVA-CPL ──────────
// Global switch: when ON, BOTH /leads/mva-funnel and /leads/mva-cpl silently
// route new leads through NLD's Ping/Post bidding campaign instead of their
// normal destination. Publishers never see a new URL — nothing changes on
// their end. Default OFF — zero behavior change until explicitly toggled.
// Publisher payout stays flat $100 regardless of NLD's actual bid amount.
// Safety: if a lead is missing fields this campaign requires, or NLD's ping
// itself comes back rejected, this silently falls back to the lead's normal
// existing destination rather than losing it — matches the whole point of
// this feature (never lose a lead), and is safe to enable even before every
// publisher is sending the expanded field set this campaign needs.

const NLD_PING_LP_CAMPAIGN_ID = '30934';
const NLD_PING_LP_SUPPLIER_ID = '115312';
const NLD_PING_LP_KEY         = 'om2pazrexa00g3';
const NLD_PING_URL            = 'https://api.leadprosper.io/ping';
const NLD_POST_URL            = 'https://api.leadprosper.io/post';

async function getMvaRoutingMode() {
  try {
    const r = await pool.query(`SELECT mode FROM campaign_settings WHERE campaign='mva-routing'`);
    return (r.rows[0] && r.rows[0].mode) || 'default';
  } catch (err) {
    console.log('[NLD Ping] Failed to read routing mode, defaulting to normal routing:', err.message);
    return 'default';
  }
}

// Fields this campaign requires at minimum to attempt a ping. If any are
// missing, we skip ping mode entirely and let the caller use normal routing.
const NLD_PING_REQUIRED_FIELDS = [
  'zip_code', 'ip_address', 'user_agent', 'landing_page_url',
  'trustedform_cert_url', 'tcpa_text', 'have_attorney', 'at_fault',
  'injury_type', 'incident_date', 'police_report', 'has_insurance',
  'medical_treatment', 'accident_type', 'compensated_before', 'case_description',
];

function hasRequiredNldPingFields(b) {
  return NLD_PING_REQUIRED_FIELDS.every(f => {
    const v = b[f] !== undefined ? b[f] : (f === 'zip_code' ? b.zip : undefined);
    return v !== undefined && v !== null && String(v).trim() !== '';
  });
}

// Attempts the full ping-then-post flow. Returns:
//   { routed: true,  billable, buyerStatus, buyerResponse, bidAmount }  — succeeded via NLD Ping
//   { routed: false, reason }                                          — caller should fall back to normal routing
function convertDateToISO(dateStr) {
  if (!dateStr) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return dateStr; // already ISO
  const match = String(dateStr).match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/); // mm/dd/yyyy or mm-dd-yyyy
  if (match) {
    const [, mm, dd, yyyy] = match;
    return `${yyyy}-${mm.padStart(2,'0')}-${dd.padStart(2,'0')}`;
  }
  return dateStr; // unrecognized format, pass through unchanged rather than silently drop it
}

async function forwardToNldPing(b, publisherSub) {
  const zip = b.zip_code || b.zip;
  const stateCode = (b.state || '').toUpperCase().trim();
  // These two are resolved server-side regardless of caller — NLD_LP_URL's
  // US_STATE_FULL_NAMES and buildNldCaseDescription are defined later in this
  // file but safe to reference here since this function only ever runs in
  // response to a request, long after the whole file has finished loading.
  const incidentStateFull = US_STATE_FULL_NAMES[stateCode] || b.incident_state || null;
  const isoIncidentDate = convertDateToISO(b.incident_date);
  const caseDescription = b.case_description || buildNldCaseDescription(b, incidentStateFull);

  // Validate against the EFFECTIVE fields (post auto-build/conversion), not
  // the raw publisher payload — case_description and a correctly-formatted
  // incident_date are now always present as long as incident_date itself was
  // originally provided, regardless of what the publisher actually sent.
  const effectiveFields = Object.assign({}, b, { incident_date: isoIncidentDate, case_description: caseDescription });

  if (!hasRequiredNldPingFields(effectiveFields)) {
    return { routed: false, reason: 'missing_required_fields' };
  }

  const basePayload = {
    lp_campaign_id: NLD_PING_LP_CAMPAIGN_ID,
    lp_supplier_id: NLD_PING_LP_SUPPLIER_ID,
    lp_key:         NLD_PING_LP_KEY,
    lp_action:      b.lp_test_mode === true ? 'test' : '',
    lp_subid1:      aliasPub(publisherSub) || '',
    zip_code:       zip,
    ip_address:     b.ip_address,
    user_agent:     b.user_agent,
    landing_page_url: b.landing_page_url,
    trustedform_cert_url: b.trustedform_cert_url || b.trusted_form_cert_url,
    tcpa_text:      b.tcpa_text,
    have_attorney:  b.have_attorney,
    at_fault:       b.at_fault,
    injury_type:    b.injury_type,
    incident_date:  isoIncidentDate,
    police_report:  b.police_report,
    has_insurance:  b.has_insurance,
    medical_treatment: b.medical_treatment,
    accident_type:  b.accident_type,
    compensated_before: b.compensated_before,
    case_description: caseDescription,
  };

  let pingResult;
  try {
    const pingRes = await postJSON(NLD_PING_URL, basePayload);
    pingResult = JSON.parse(pingRes.body);
  } catch (err) {
    console.log('[NLD Ping] Ping request failed:', err.message);
    return { routed: false, reason: 'ping_request_error' };
  }

  if (pingResult.status !== 'ACCEPTED' || !pingResult.bids || !pingResult.bids.length) {
    console.log(`[NLD Ping] ✕ Ping rejected for ${b.first_name} ${b.last_name} — ${pingResult.message || 'no message'} — falling back to normal routing`);
    return { routed: false, reason: 'ping_rejected', nldMessage: pingResult.message || null, nldResponse: pingResult };
  }

  const bidAmount = pingResult.bids[0].payout;

  const postPayload = {
    ...basePayload,
    lp_ping_id: pingResult.ping_id,
    first_name: b.first_name,
    last_name:  b.last_name,
    email:      b.email,
    phone:      String(b.phone).replace(/\D/g, ''),
    state:      (b.state || '').toUpperCase().trim(),
    date_of_birth: b.date_of_birth || undefined,
    gender:     b.gender || undefined,
    address:    b.address || undefined,
    city:       b.city || undefined,
    jornaya_leadid: b.jornaya_leadid || undefined,
    injured:    b.injured || undefined,
  };
  Object.keys(postPayload).forEach(k => { if (postPayload[k] === undefined) delete postPayload[k]; });

  let postResult;
  try {
    const postRes = await postJSON(NLD_POST_URL, postPayload);
    postResult = JSON.parse(postRes.body);
  } catch (err) {
    console.log('[NLD Ping] Post request failed:', err.message);
    return { routed: false, reason: 'post_request_error' };
  }

  const accepted = postResult.status === 'ACCEPTED';
  console.log(`[NLD Ping] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | bid $${bidAmount} | ${postResult.status}`);

  return {
    routed: true,
    billable: accepted,
    buyerStatus: postResult.status || 'ERROR',
    buyerResponse: postResult,
    bidAmount,
  };
}
// ─── END NLD PING/POST SHARED LAYER ───────────────────────────────────────────

app.post('/leads/mva-funnel', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Validate required fields
  const missing = [];
  if (!b.first_name)    missing.push('first_name');
  if (!b.last_name)     missing.push('last_name');
  if (!b.phone)         missing.push('phone');
  if (!b.email)         missing.push('email');
  if (!b.ip_address)    missing.push('ip_address');
  if (!b.have_attorney && !b.attorney) missing.push('have_attorney');
  if (!b.at_fault && !b.accident_fault) missing.push('at_fault');
  // channel is optional — defaults to 'Facebook' if missing or invalid
  if (!b.trustedform_cert_url && !b.trusted_form_cert_url && !b.jornaya_leadid) missing.push('trustedform_cert_url or jornaya_leadid');
  if (!b.publisher_sub) missing.push('publisher_sub');
  if (!b.state && !b.incident_state) missing.push('state');

  // The fields below are only required for NLD's accepted states — Email
  // Agency (the fallback for every other state) doesn't use them, so a lead
  // missing them shouldn't be blocked here if it's actually headed there.
  // Checked only after we know which state this lead is in.
  const NLD_ONLY_STATES = ['UT','MT','WY','AZ','CA','NV','OK','NE','ND','IA','NM']; // PA removed, CA added (Kyler, Sep 8)
  const stateForValidation = (b.state || b.incident_state || '').toUpperCase().trim();
  if (NLD_ONLY_STATES.includes(stateForValidation)) {
    if (!b.date_of_birth) missing.push('date_of_birth');
    if (!b.address)       missing.push('address');
    if (!b.city)          missing.push('city');
    if (!b.zip_code && !b.zip) missing.push('zip_code');
    if (!b.landing_page_url) missing.push('landing_page_url');
    if (!b.incident_date) missing.push('incident_date');
    if (!b.settlement)    missing.push('settlement');
    if (!b.cited)         missing.push('cited');
    if (!b.doctor_treatment) missing.push('doctor_treatment');
    if (!b.physical_injury)  missing.push('physical_injury');
  }

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;
  const leadState    = (b.incident_state || b.state || '').toUpperCase().trim();

  // CA and CO are blocked entirely — never forwarded to any buyer, per
  // Kyler's explicit instruction (Aug 18). Still logged/visible, not silently
  // dropped, so it's clear on the dashboard when a publisher sends one anyway.
  if (leadState === 'CA' || leadState === 'CO') {
    const clientBlocked = await pool.connect();
    let blockedLeadId = null;
    try {
      const insertBlocked = await clientBlocked.query(
        `INSERT INTO leads
           (campaign, vertical, first_name, last_name, phone, email,
            publisher_sub, ip_address, state, status, buyer_error, billable, raw, received_at)
         VALUES ('mva-funnel','MVA',$1,$2,$3,$4,$5,$6,$7,'rejected',$8,false,$9::jsonb,NOW())
         RETURNING id`,
        [b.first_name, b.last_name, b.phone, b.email,
         publisherSub, b.ip_address, leadState,
         `${leadState} is blocked — not accepted for this campaign`, JSON.stringify(b)]
      );
      blockedLeadId = insertBlocked.rows[0].id;
    } catch(dbErr) {
      console.error('[MVA Funnel] DB insert error (CA/CO block):', dbErr.message);
    } finally {
      clientBlocked.release();
    }
    console.log(`[MVA Funnel] ✕ ${b.first_name} ${b.last_name} | ${leadState} — blocked, not accepted`);
    return res.json({
      ok: false, result: 'rejected',
      message: `${leadState} is blocked and not accepted for this campaign.`,
      krw_id: blockedLeadId
    });
  }

  // Insert lead into DB
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, ip_address, state, status, raw, received_at)
       VALUES ('mva-funnel','MVA',$1,$2,$3,$4,$5,$6,$7,'pending',$8::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, b.ip_address, leadState || null,
       JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[MVA Funnel] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Check NLD Ping failover mode — if ON, attempt to route through it first.
  // Falls through to normal routing below if mode is off, or this specific
  // lead is missing fields the ping campaign requires, or NLD's ping rejects it.
  const routingMode = await getMvaRoutingMode();
  if (routingMode === 'nld_ping') {
    const pingAttempt = await forwardToNldPing(b, publisherSub);
    if (pingAttempt.routed) {
      if (leadId) {
        const cPing = await pool.connect();
        try {
          await cPing.query(
            `UPDATE leads SET
               status         = $1,
               buyer_response = $2::jsonb,
               buyer_status   = $3,
               billable       = false,
               revenue        = 0,
               raw            = COALESCE(raw,'{}'::jsonb) || $4::jsonb
             WHERE id = $5`,
            [pingAttempt.billable ? 'forwarded' : 'buyer_rejected',
             JSON.stringify(pingAttempt.buyerResponse), pingAttempt.buyerStatus,
             JSON.stringify({ nld_ping_bid: pingAttempt.bidAmount }), leadId]
          );
        } finally { cPing.release(); }
      }
      return res.json({
        ok: pingAttempt.billable,
        result: pingAttempt.billable ? 'success' : 'rejected',
        message: pingAttempt.buyerStatus,
        krw_id: leadId
      });
    }
    // pingAttempt.routed === false → fall through to normal routing below
  }

  // Find matching buyer for this state
  const buyer = MVA_BUYERS.find(byr => byr.states.includes(leadState));

  if (!buyer) {
    // No buyer configured for this state — hold lead
    if (leadId) {
      const c2 = await pool.connect();
      try {
        await c2.query(
          "UPDATE leads SET status='received', buyer_error=$1 WHERE id=$2",
          [`No buyer configured for state: ${leadState}`, leadId]
        );
      } finally { c2.release(); }
    }
    console.log(`[MVA Funnel] ⏸ ${b.first_name} ${b.last_name} | ${leadState} — no buyer for this state`);
    return res.json({
      ok:      false,
      result:  'held',
      message: `No buyer configured for state: ${leadState}. Lead saved.`,
      krw_id:  leadId
    });
  }

  // Forward to matched buyer
  try {
    const result = await buyer.post(b, publisherSub);

    if (leadId) {
      const c3 = await pool.connect();
      try {
        await c3.query(
          `UPDATE leads SET
             status          = $1,
             buyer_intake_id = $2,
             buyer_response  = $3::jsonb,
             buyer_status    = $4,
             revenue         = 0,
             raw             = COALESCE(raw,'{}'::jsonb) || $5::jsonb
           WHERE id = $6`,
          [
            result.accepted ? 'forwarded' : result.duplicate ? 'duplicate' : 'buyer_rejected',
            result.lead_id || null,
            JSON.stringify(result.raw),
            result.accepted ? 'Accepted' : result.duplicate ? 'Duplicate' : 'Rejected',
            JSON.stringify({ buyer_name: buyer.name }),
            leadId
          ]
        );
      } finally { c3.release(); }
    }

    console.log(`[MVA Funnel→${buyer.name}] ${result.accepted ? '✅' : result.duplicate ? '🔁' : '❌'} ${b.first_name} ${b.last_name} | ${leadState} | ${result.message || ''} | ID: ${result.lead_id || 'none'}`);

    return res.json({
      ok:      result.accepted,
      result:  result.accepted ? 'success' : result.duplicate ? 'duplicate' : 'rejected',
      lead_id: result.lead_id || null,
      message: result.message || null,
      buyer:   buyer.name,
      krw_id:  leadId
    });

  } catch(fwdErr) {
    console.error(`[MVA Funnel→${buyer.name}] Forward error:`, fwdErr.message);
    if (leadId) {
      const c4 = await pool.connect();
      try {
        await c4.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2",
          [fwdErr.message, leadId]);
      } finally { c4.release(); }
    }
    return res.status(502).json({ ok: false, error: `Failed to forward to ${buyer.name}`, detail: fwdErr.message });
  }
});

// ─── MVA-NYC-SPLIT — BUYER LADDER: CH-Intake / LT-Intake -> NLD -> 003 (KRW-NYC-MVA ONLY) ──
// (Sep 16 rewrite - see BUYER LADDER inside the handler. History below kept for context.)
// ─── (was) NLD / LAR-MVA-CPA ALTERNATION ────────────────────────────────────
// Fully isolated from /leads/mva-funnel above - Kevin's and Inbounds' routing
// is completely untouched by anything in this section. Built per Kyler's
// instruction (Aug 25-26): Noah's traffic alternates strictly between NLD and
// LAR-MVA-CPA, at least 50% each. Alternation is based on a DB count at
// request time (not in-memory), so it survives restarts/redeploys cleanly.
// CA/CO hard-blocked here too, matching company-wide policy (Aug 18).
//
// NLD: same credentials as the main NLD CPA buyer, $2,000/case revenue to Kyler.
// LAR-MVA-CPA: real spec confirmed via https://vividvisions.marketing/posting-instructions/mva1
//   - auth is via publisher_code in the body, no separate API key header
//   - requires EITHER trusted_form_url OR jornaya_lead_id
//   - $2,500/case revenue to Kyler
// Noah's own payout ($1,800/case) is already set on his publisher record,
// unaffected by which buyer a given lead actually routes to.

const LAR_MVA_ENDPOINT = 'https://vividvisions.marketing/api/v1/mva1';
const LAR_MVA_PUBLISHER_CODE = 'PUB-KYLER1';

app.post('/leads/mva-nyc-split', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};
  const leadState = (b.state || b.incident_state || '').toUpperCase().trim();

  // Hardcoded fallback IP for this campaign only, per Kyler's explicit
  // instruction (Aug 26) - Noah's publisher doesn't reliably capture real
  // IP data, and NLD requires the field. Applied before any downstream use
  // (DB records and the actual buyer payload both pick this up) - this
  // guarantees a missing IP can never hold up or reject a lead.
  if (!b.ip_address) b.ip_address = '8.8.8.8';

  const missing = [];
  if (!b.first_name)  missing.push('first_name');
  if (!b.last_name)   missing.push('last_name');
  if (!b.phone)        missing.push('phone');
  if (!b.email)        missing.push('email');
  if (!leadState)      missing.push('state');
  if (!b.trustedform_cert_url && !b.jornaya_leadid) missing.push('trustedform_cert_url or jornaya_leadid');

  // ── BUYER LADDER (Kyler, Sep 16) ─────────────────────────────────────────
  // Routing is by buyer priority mixed with each buyer's state list:
  //   1. CH-Intake and LT-Intake: same 13 states, 50/50 between them
  //      (LT-Intake is skipped until its posting spec is wired in - see LT_INTAKE_WEBHOOK)
  //   2. NLD CPA: its 11 states, hard cap 5/day (Eastern)
  //   3. MVA-003-LT: nationwide, bottom of the funnel, takes stragglers
  // A lead goes to the highest-priority eligible buyer (state ok, under cap,
  // enabled). If that buyer rejects, it drops to the next rung in the same
  // request, so a rejection never strands a lead. Caps count leads that
  // buyer ACCEPTED today, not attempts. Colorado is on both intake lists but
  // stays blocked by the company-wide CA/CO rule until INTAKE_TAKES_CO is
  // flipped - blocked leads never reach this ladder.
  const INTAKE_STATES = ['FL','GA','WI','TX','MI','IN','IL','MN','CO','MO','NE','OK','TN'];
  const NLD_ONLY_STATES = ['UT','MT','WY','AZ','CA','NV','OK','NE','ND','IA','NM'];
  // LT-Intake posts to a VICIdial dialer over GET (spec from Adam, Sep 16). It goes live
  // the moment LT_INTAKE_PASS is set on Railway; until then it is skipped in the ladder.
  const NYC_LADDER = [
    { name: 'CH-Intake',  priority: 1, group: 'intake', cap: null, payout: 2250, enabled: true,                        states: INTAKE_STATES },
    { name: 'LT-Intake',  priority: 1, group: 'intake', cap: null, payout: 2500, enabled: !!process.env.LT_INTAKE_PASS, states: INTAKE_STATES },
    { name: 'NLD CPA',    priority: 2, group: 'nld',    cap: 10,   payout: 2000, enabled: true,                 states: NLD_ONLY_STATES },
    { name: 'MVA-003-LT', priority: 3, group: '003',    cap: 2,    payout: 1700, enabled: true,                 states: 'ALL' },
  ];

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  // CA/CO hard-blocked, matching company-wide policy - never forwarded to any buyer
  const INTAKE_TAKES_CO = process.env.INTAKE_TAKES_CO === 'true';
  if (leadState === 'CA' || (leadState === 'CO' && !INTAKE_TAKES_CO)) {
    const clientBlocked = await pool.connect();
    let blockedLeadId = null;
    try {
      const insertBlocked = await clientBlocked.query(
        `INSERT INTO leads
           (campaign, vertical, first_name, last_name, phone, email,
            publisher_sub, ip_address, state, status, buyer_error, billable, raw, received_at)
         VALUES ('mva-nyc-split','MVA',$1,$2,$3,$4,$5,$6,$7,'rejected',$8,false,$9::jsonb,NOW())
         RETURNING id`,
        [b.first_name, b.last_name, b.phone, b.email,
         'KRW-NYC-MVA', b.ip_address, leadState,
         `${leadState} is blocked — not accepted for this campaign`, JSON.stringify(b)]
      );
      blockedLeadId = insertBlocked.rows[0].id;
    } catch(dbErr) {
      console.error('[MVA-NYC-SPLIT] DB insert error (CA/CO block):', dbErr.message);
    } finally {
      clientBlocked.release();
    }
    return res.json({
      ok: false, result: 'rejected',
      message: `${leadState} is blocked and not accepted for this campaign.`,
      krw_id: blockedLeadId
    });
  }

  // Per-state cap (Kyler, Sep 23): Pennsylvania has no buyer but 003, so it is limited to a few a day.
  // Counted on delivered leads for this campaign today (Eastern). Past the cap the lead is stored and held.
  const NYC_STATE_CAPS = { PA: parseInt(process.env.NYC_PA_DAILY_CAP || '3', 10) };
  if (NYC_STATE_CAPS[leadState] != null) {
    const sc = await pool.query(
      `SELECT COUNT(*)::int AS n FROM leads WHERE campaign='mva-nyc-split' AND state=$1 AND status IN ('forwarded','buyer_rejected','pending')
         AND (received_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date`, [leadState]);
    if (sc.rows[0].n >= NYC_STATE_CAPS[leadState]) {
      const why = 'Daily limit of ' + NYC_STATE_CAPS[leadState] + ' ' + leadState + ' leads reached for today';
      const cCap = await pool.connect();
      let capId = null;
      try {
        const insCap = await cCap.query(
          `INSERT INTO leads (campaign, vertical, first_name, last_name, phone, email, publisher_sub, ip_address, state, status, buyer_error, billable, raw, received_at)
           VALUES ('mva-nyc-split','MVA',$1,$2,$3,$4,'KRW-NYC-MVA',$5,$6,'received',$7,false,$8::jsonb,NOW()) RETURNING id`,
          [b.first_name, b.last_name, b.phone, b.email, b.ip_address, leadState, why, JSON.stringify(b)]);
        capId = insCap.rows[0].id;
      } finally { cCap.release(); }
      console.log(`[MVA-NYC-SPLIT] held ${b.first_name} ${b.last_name} | ${leadState} | ${why}`);
      return res.json({ ok: false, result: 'held', message: why, krw_id: capId });
    }
  }

  // Today's ACCEPTED count per buyer (Eastern day), for caps and the 50/50 tiebreak
  const countRes = await pool.query(
    `SELECT raw->>'buyer_name' AS buyer, COUNT(*)::int AS n,
            MAX(received_at) AS last_at
     FROM leads
     WHERE campaign='mva-nyc-split' AND status='forwarded'
       AND (received_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date
     GROUP BY raw->>'buyer_name'`
  );
  const todayCount = {}, lastAt = {};
  for (const r of countRes.rows) { todayCount[r.buyer] = r.n; lastAt[r.buyer] = r.last_at ? new Date(r.last_at).getTime() : 0; }

  // NLD needs more fields than anyone else. Rather than bouncing the lead
  // with a 400 when NLD happens to be next, NLD is simply skipped for this
  // lead if its extra fields can't be satisfied, and the lead moves on.
  if (!b.address)       b.address = '123 Main Street';   // NLD confirmed not really required (Sep 2)
  if (!b.date_of_birth) b.date_of_birth = '01/01/1985';
  const nldMissing = [];
  if (!b.city)             nldMissing.push('city');
  if (!b.zip_code && !b.zip) nldMissing.push('zip_code');
  if (!b.landing_page_url) nldMissing.push('landing_page_url');
  if (!b.incident_date)    nldMissing.push('incident_date');
  if (!b.settlement)       nldMissing.push('settlement');
  if (!b.cited)            nldMissing.push('cited');
  if (!b.doctor_treatment) nldMissing.push('doctor_treatment');
  if (!b.physical_injury)  nldMissing.push('physical_injury');
  if (!b.at_fault)         nldMissing.push('at_fault');
  if (!b.injury && !b.physical_injury) missing.push('injury (or physical_injury)');
  if (missing.length) return res.status(400).json({ ok: false, error: 'Missing required fields', missing });

  const eligible = NYC_LADDER.filter(buyer => {
    if (!buyer.enabled) return false;
    if (buyer.states !== 'ALL' && !buyer.states.includes(leadState)) return false;
    if (buyer.cap != null && (todayCount[buyer.name] || 0) >= buyer.cap) return false;
    if (buyer.name === 'NLD CPA' && nldMissing.length) return false;
    return true;
  }).sort((a, b2) => {
    if (a.priority !== b2.priority) return a.priority - b2.priority;
    // same rung: the buyer with fewer accepted today goes first; on a tie,
    // whoever did NOT get the most recent one - this is the 50/50
    const ca = todayCount[a.name] || 0, cb = todayCount[b2.name] || 0;
    if (ca !== cb) return ca - cb;
    return (lastAt[a.name] || 0) - (lastAt[b2.name] || 0);
  });
  const ladderPlan = eligible.map(x => x.name);
  console.log(`[MVA-NYC-SPLIT] ${b.first_name} ${b.last_name} | ${leadState} | ladder: ${ladderPlan.join(' -> ') || 'nobody eligible'}${nldMissing.length ? ' | NLD skipped, missing ' + nldMissing.join(',') : ''}`);

  // Insert lead first, regardless of buyer outcome
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, ip_address, state, status, raw, received_at)
       VALUES ('mva-nyc-split','MVA',$1,$2,$3,$4,$5,$6,$7,'pending',$8::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       'KRW-NYC-MVA', b.ip_address, leadState, JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[MVA-NYC-SPLIT] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  if (!eligible.length) {
    const why = `No eligible buyer for ${leadState} right now (caps hit or state not covered)`;
    const c0 = await pool.connect();
    try { await c0.query("UPDATE leads SET status='received', buyer_error=$1 WHERE id=$2", [why, leadId]); } finally { c0.release(); }
    console.log(`[MVA-NYC-SPLIT] ⏸ ${b.first_name} ${b.last_name} | ${leadState} | held: ${why}`);
    return res.json({ ok: false, result: 'held', message: why, krw_id: leadId });
  }

  // ── Per-buyer senders ─────────────────────────────────────────────────
  const incidentStateFull = US_STATE_FULL_NAMES[leadState] || leadState;
  const strip = o => { Object.keys(o).forEach(k => { if (o[k] === undefined || o[k] === null || o[k] === '') delete o[k]; }); return o; };
  const senders = {
    'NLD CPA': async () => {
      const p = strip({
        lp_campaign_id: '31080', lp_supplier_id: '110928', lp_key: 'ke21sx0koi7dld',
        lp_action: b.lp_test_mode === true ? 'test' : '',
        lp_subid1: aliasPub('KRW-NYC-MVA') || '',
        first_name: b.first_name, last_name: b.last_name, email: b.email,
        phone: String(b.phone).replace(/\D/g, ''),
        date_of_birth: convertDateToISO(b.date_of_birth), address: b.address, city: b.city,
        state: leadState, zip_code: b.zip_code || b.zip, ip_address: b.ip_address,
        landing_page_url: b.landing_page_url,
        trustedform_cert_url: b.trustedform_cert_url || undefined, jornaya_leadid: b.jornaya_leadid || undefined,
        incident_state: incidentStateFull, incident_date: b.incident_date,
        have_attorney: b.have_attorney, at_fault: b.at_fault, settlement: b.settlement, cited: b.cited,
        doctor_treatment: b.doctor_treatment, physical_injury: b.physical_injury,
        injury: b.injury, summary: b.summary, county: b.county,
      });
      if (b.lp_test_mode !== true) delete p.lp_action;
      const r = await postJSON('https://api.leadprosper.io/direct_post', p);
      let out; try { out = JSON.parse(r.body); } catch(e) { out = { status: r.status, raw: r.body }; }
      return { result: out, accepted: out.status === 'ACCEPTED' || out.success === true };
    },
    'MVA-003-LT': async () => {
      const p = strip({
        lp_subid1: aliasPub('KRW-NYC-MVA') || 'KRW-NYC-MVA',
        first_name: b.first_name, last_name: b.last_name, email: b.email,
        phone: String(b.phone).replace(/\D/g, ''),
        at_fault: b.at_fault, have_attorney: b.have_attorney, physical_injury: b.physical_injury,
        doctor_treatment: b.doctor_treatment, state: incidentStateFull || leadState,
        zip_code: b.zip_code || b.zip, incident_date: b.incident_date,
        trustedform_cert_url: b.trustedform_cert_url || undefined, ip_address: b.ip_address || undefined,
        injury: b.injury, summary: b.summary, county: b.county, cited: b.cited, settlement: b.settlement,
        date_of_birth: b.date_of_birth,
      });
      const r = await postJSON('https://hooks.zapier.com/hooks/catch/23024319/4tdo5z8/', p);
      let out; try { out = JSON.parse(r.body); } catch(e) { out = { status: r.status, raw: r.body }; }
      return { result: out, accepted: out.status === 'success' };
    },
    'CH-Intake': async () => {
      // Same payload the tested /leads/forward-to-mva-intake endpoint sends (Sep 15)
      const p = strip({
        first_name: b.first_name, last_name: b.last_name,
        phone: String(b.phone).replace(/\D/g, ''), email: b.email,
        zip_code: b.zip_code || b.zip, state: leadState,
        incident_date: b.incident_date, injury: b.injury || b.physical_injury,
        at_fault: b.at_fault, have_attorney: b.have_attorney,
        consent_url: b.trustedform_cert_url || b.jornaya_leadid || undefined,
        consent_timestamp: new Date().toISOString(),
      });
      const r = await postJSON('https://hooks.zapier.com/hooks/catch/23024319/4d50uja/', p);
      let out; try { out = JSON.parse(r.body); } catch(e) { out = { status: r.status, raw: r.body }; }
      return { result: out, accepted: out.status === 'success' || (r.status >= 200 && r.status < 300) };
    },
    'LT-Intake': async () => {
      const r = await sendToLtIntake(b, leadState, leadId);
      return { result: r.result, accepted: r.accepted };
    },
  };

  // ── Walk the ladder ───────────────────────────────────────────────────
  const attempts = [];
  let buyerName = null, result = {}, accepted = false;
  for (const buyer of eligible) {
    try {
      const out = await senders[buyer.name]();
      attempts.push({ buyer: buyer.name, accepted: out.accepted, response: out.result });
      console.log(`[MVA-NYC-SPLIT] ${out.accepted ? '✓' : '✕'} ${buyer.name} | ${b.first_name} ${b.last_name} | ${leadState} | ${out.result.message || out.result.status || ''}`);
      if (out.accepted) { buyerName = buyer.name; result = out.result; accepted = true; break; }
      buyerName = buyer.name; result = out.result;   // last rejection, for the record
    } catch (fwdErr) {
      attempts.push({ buyer: buyer.name, accepted: false, error: fwdErr.message });
      console.error(`[MVA-NYC-SPLIT] Forward to ${buyer.name} failed:`, fwdErr.message);
      buyerName = buyer.name; result = { status: 'error', message: fwdErr.message };
    }
  }

  // nld_attempted / nld_response kept for anything that reads them (Sep 15 fix)
  const nldTry = attempts.find(a => a.buyer === 'NLD CPA');
  const c2 = await pool.connect();
  try {
    await c2.query(
      `UPDATE leads SET
         status          = $1,
         buyer_status    = $2,
         buyer_response  = $3::jsonb,
         billable        = false,
         revenue         = 0,
         raw             = COALESCE(raw,'{}'::jsonb) || $4::jsonb
       WHERE id = $5`,
      [accepted ? 'forwarded' : 'buyer_rejected',
       accepted ? 'Accepted' : 'Rejected',
       JSON.stringify({ final: result, attempts, ladder: ladderPlan, nld_attempted: !!nldTry, nld_response: nldTry ? (nldTry.response || nldTry.error) : null }),
       JSON.stringify({ buyer_name: buyerName, routing_attempts: attempts.map(a => a.buyer + (a.accepted ? ':accepted' : ':rejected')) }), leadId]
    );
  } finally { c2.release(); }

  console.log(`[MVA-NYC-SPLIT] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} | -> ${buyerName} after ${attempts.length} attempt${attempts.length !== 1 ? 's' : ''}`);

  return res.json({
    ok: accepted,
    result: accepted ? 'success' : 'rejected',
    message: result.message || (accepted ? 'Lead accepted' : 'Lead rejected'),
    buyer: buyerName,
    attempts: attempts.map(a => ({ buyer: a.buyer, accepted: a.accepted })),
    krw_id: leadId
  });
});

// Also keep old route as alias so any existing integrations don't break
app.post('/leads/mva-nld2', (req, res) => {
  req.url = '/leads/mva-funnel';
  app.handle(req, res);
});

// ─── END MVA FUNNEL ───────────────────────────────────────────────────────────

// ─── MVA-CPL — NLD (NEXT LEVEL DIRECT) ────────────────────────────────────────
// Separate, isolated campaign from mva-funnel above — zero shared code.
// Kevin Anthony's team pre-filters by state on their end: leads matching
// Email Agency's states go to /leads/mva-funnel as before (unchanged).
// Everything else (nationwide minus CA) comes here and is always forwarded
// straight to NLD via LeadProsper. No state-based branching in this endpoint —
// that decision already happened before the lead arrived here.
// Payout: $100 flat CPL, billable only when NLD returns status=ACCEPTED.

const NLD_LP_CAMPAIGN_ID = '33958';
const NLD_LP_SUPPLIER_ID = '122561';
const NLD_LP_KEY         = 'd02ltknjjh25pn';
const NLD_LP_URL         = 'https://api.leadprosper.io/direct_post';

const US_STATE_FULL_NAMES = {
  AL:'Alabama', AK:'Alaska', AZ:'Arizona', AR:'Arkansas', CA:'California', CO:'Colorado',
  CT:'Connecticut', DE:'Delaware', FL:'Florida', GA:'Georgia', HI:'Hawaii', ID:'Idaho',
  IL:'Illinois', IN:'Indiana', IA:'Iowa', KS:'Kansas', KY:'Kentucky', LA:'Louisiana',
  ME:'Maine', MD:'Maryland', MA:'Massachusetts', MI:'Michigan', MN:'Minnesota',
  MS:'Mississippi', MO:'Missouri', MT:'Montana', NE:'Nebraska', NV:'Nevada',
  NH:'New Hampshire', NJ:'New Jersey', NM:'New Mexico', NY:'New York', NC:'North Carolina',
  ND:'North Dakota', OH:'Ohio', OK:'Oklahoma', OR:'Oregon', PA:'Pennsylvania',
  RI:'Rhode Island', SC:'South Carolina', SD:'South Dakota', TN:'Tennessee', TX:'Texas',
  UT:'Utah', VT:'Vermont', VA:'Virginia', WA:'Washington', WV:'West Virginia',
  WI:'Wisconsin', WY:'Wyoming',
};

function buildNldCaseDescription(b, incidentStateFull) {
  const parts = [];
  if (b.incident_date)            parts.push(`Incident date: ${b.incident_date}`);
  if (b.at_fault)                 parts.push(`At fault: ${b.at_fault}`);
  if (b.injury)                   parts.push(`Injury: ${b.injury}`);
  if (b.have_attorney)            parts.push(`Has representation: ${b.have_attorney}`);
  if (b.has_insurance)            parts.push(`Has insurance: ${b.has_insurance}`);
  if (incidentStateFull)          parts.push(`Incident state: ${incidentStateFull}`);
  return parts.join('. ') + (parts.length ? '.' : '');
}

app.post('/leads/mva-cpl', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Validate required fields — existing MVA fields + NLD's additional required fields
  const missing = [];
  if (!b.first_name)                              missing.push('first_name');
  if (!b.last_name)                                missing.push('last_name');
  if (!b.phone)                                    missing.push('phone');
  if (!b.email)                                    missing.push('email');
  if (!b.state)                                    missing.push('state');
  if (!b.zip_code && !b.zip)                       missing.push('zip_code');
  if (!b.have_attorney)                            missing.push('have_attorney');
  if (!b.at_fault)                                 missing.push('at_fault');
  if (!b.trustedform_cert_url && !b.trusted_form_cert_url) missing.push('trustedform_cert_url');
  if (!b.publisher_sub)                            missing.push('publisher_sub');
  if (!b.incident_date)                            missing.push('incident_date');
  if (!b.motor_vehicle_accident)                   missing.push('motor_vehicle_accident');
  if (!b.injury)                                   missing.push('injury');
  if (!b.settlement)                               missing.push('settlement');
  if (!b.has_insurance)                            missing.push('has_insurance');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub    = b.publisher_sub;
  const leadState       = (b.state || '').toUpperCase().trim();
  const incidentStateFull = US_STATE_FULL_NAMES[leadState] || b.incident_state || null;

  // CA is explicitly excluded from this campaign — reject clearly, never forward
  if (leadState === 'CA') {
    const client = await pool.connect();
    let leadId = null;
    try {
      const insert = await client.query(
        `INSERT INTO leads
           (campaign, vertical, first_name, last_name, phone, email,
            publisher_sub, state, status, buyer_error, billable, raw, received_at)
         VALUES ('mva-cpl','MVA',$1,$2,$3,$4,$5,$6,'rejected',$7,false,$8::jsonb,NOW())
         RETURNING id`,
        [b.first_name, b.last_name, b.phone, b.email, publisherSub, leadState,
         'CA excluded from this campaign', JSON.stringify(b)]
      );
      leadId = insert.rows[0].id;
    } catch(dbErr) {
      console.error('[MVA-CPL] DB insert error (CA reject):', dbErr.message);
    } finally {
      client.release();
    }
    console.log(`[MVA-CPL] ✕ ${b.first_name} ${b.last_name} | CA — excluded from this campaign`);
    return res.json({
      ok: false, result: 'rejected',
      message: 'CA is excluded from this campaign.',
      krw_id: leadId
    });
  }

  // Insert lead into DB
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, ip_address, state, status, raw, received_at)
       VALUES ('mva-cpl','MVA',$1,$2,$3,$4,$5,$6,$7,'pending',$8::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, b.ip_address || null, leadState,
       JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[MVA-CPL] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  // Check NLD Ping failover mode — if ON, attempt to route through it first.
  // Falls through to the existing direct-post NLD flow below if mode is off,
  // or this specific lead is missing fields the ping campaign requires, or
  // NLD's ping rejects it.
  const routingMode = await getMvaRoutingMode();
  if (routingMode === 'nld_ping') {
    const pingAttempt = await forwardToNldPing(b, publisherSub);
    if (pingAttempt.routed) {
      const cPing = await pool.connect();
      try {
        await cPing.query(
          `UPDATE leads SET
             status         = $1,
             buyer_response = $2::jsonb,
             buyer_status   = $3,
             billable       = false,
             revenue        = 0,
             raw            = COALESCE(raw,'{}'::jsonb) || $4::jsonb
           WHERE id = $5`,
          [pingAttempt.billable ? 'forwarded' : 'buyer_rejected',
           JSON.stringify(pingAttempt.buyerResponse), pingAttempt.buyerStatus,
           JSON.stringify({ nld_ping_bid: pingAttempt.bidAmount }), leadId]
        );
      } finally { cPing.release(); }
      return res.json({
        ok: pingAttempt.billable,
        result: pingAttempt.billable ? 'success' : 'rejected',
        message: pingAttempt.buyerStatus,
        krw_id: leadId
      });
    }
    // pingAttempt.routed === false → fall through to existing NLD direct-post flow below
  }

  // Always forwards to NLD — no state branching here, that already happened
  // before this lead arrived (Kevin's team routes by which URL they post to)
  const caseDescription = buildNldCaseDescription(b, incidentStateFull);

  const payload = {
    lp_campaign_id: NLD_LP_CAMPAIGN_ID,
    lp_supplier_id: NLD_LP_SUPPLIER_ID,
    lp_key:         NLD_LP_KEY,
    lp_subid1:      aliasPub(publisherSub) || '',
    first_name:     b.first_name,
    last_name:      b.last_name,
    email:          b.email,
    phone:          String(b.phone).replace(/\D/g,''),
    date_of_birth:  b.date_of_birth || undefined,
    gender:         b.gender || undefined,
    address:        b.address || undefined,
    city:           b.city || undefined,
    state:          leadState,
    zip_code:       b.zip_code || b.zip,
    ip_address:     b.ip_address || undefined,
    user_agent:     b.user_agent || undefined,
    landing_page_url: b.landing_page_url || 'https://krwmarketingsolutions.github.io/forms',
    jornaya_leadid: b.jornaya_leadid || undefined,
    trustedform_cert_url: b.trustedform_cert_url || b.trusted_form_cert_url,
    tcpa_text:      b.tcpa_text || undefined,
    case_description: caseDescription,
    incident_state: incidentStateFull || undefined,
    incident_date:  b.incident_date,
    motor_vehicle_accident: b.motor_vehicle_accident,
    injury:         b.injury,
    at_fault:       b.at_fault,
    have_attorney:  b.have_attorney,
    settlement:     b.settlement,
    has_insurance:  b.has_insurance,
  };
  Object.keys(payload).forEach(k => { if (payload[k] === undefined) delete payload[k]; });

  try {
    const nldRes = await postJSON(NLD_LP_URL, payload);
    let result = {};
    try { result = JSON.parse(nldRes.body); } catch(e) { result = { status: 'ERROR', message: nldRes.body }; }

    const accepted  = result.status === 'ACCEPTED';
    const duplicate = result.status === 'DUPLICATED';
    // Never auto-billable on acceptance - buyer confirmation (manual or
    // postback) always comes later, per Kyler (Aug 28). Status still
    // correctly reflects the lead was sent and accepted into intake.

    const c2 = await pool.connect();
    try {
      await c2.query(
        `UPDATE leads SET
           status          = $1,
           buyer_intake_id = $2,
           buyer_response  = $3::jsonb,
           buyer_status    = $4,
           billable        = false,
           revenue         = 0
         WHERE id = $5`,
        [accepted ? 'forwarded' : duplicate ? 'duplicate' : 'buyer_rejected',
         result.lead_id || null, JSON.stringify(result), result.status || 'ERROR',
         leadId]
      );
    } finally { c2.release(); }

    console.log(`[MVA-CPL] ${accepted ? '✓' : duplicate ? '⊘' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} → NLD | ${result.status}`);

    return res.json({
      ok: true,
      result: accepted ? 'success' : duplicate ? 'duplicate' : 'rejected',
      message: result.message || result.status,
      krw_id: leadId
    });
  } catch (err) {
    console.error('[MVA-CPL] Forward to NLD failed:', err.message);
    if (leadId) {
      const c3 = await pool.connect();
      try {
        await c3.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2", [err.message, leadId]);
      } finally { c3.release(); }
    }
    return res.status(502).json({ ok: false, error: 'Failed to forward to buyer', krw_id: leadId });
  }
});
// ─── END MVA-CPL — NLD ─────────────────────────────────────────────────────────

// ─── SSDI-CPQ — RINGFUEL (JOHN-G) ─────────────────────────────────────────────
// John-G posts lead data here, same as any other publisher. We forward it to
// Ringfuel's Ping API using their Data Pass mechanism — this is NOT a normal
// lead-post/accept flow. Ringfuel holds the lead data against the phone number;
// the actual delivery to the buyer happens automatically when John-G's team
// dials the DID and the call connects — entirely outside our system. We never
// see the call itself, only the Ping result.
// Payout: $100 flat to John-G. Never auto-billable - billing is confirmed
// later, manually or via postback (Kyler, Aug 28), not at ping-accept time.

const RINGFUEL_API_KEY     = 'rfp_9f87c1b9d23d26ab28f9726cc28e2825ccff8dac98feb313';
const RINGFUEL_CAMPAIGN_ID = '790dfb68-1dfc-41b4-9ea3-e955f5fddb1e';
const RINGFUEL_PING_URL    = 'https://app.ringfuel.io/api/publisher/ping';

app.post('/leads/ssdi-cpq', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  const missing = [];
  if (!b.first_name)  missing.push('first_name');
  if (!b.last_name)   missing.push('last_name');
  if (!b.phone)        missing.push('phone');
  if (!b.email)        missing.push('email');
  if (!b.state)        missing.push('state');
  if (!b.publisher_sub) missing.push('publisher_sub');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;

  // Insert lead into DB first, for our own tracking/dashboard regardless of ping outcome
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, state, status, raw, received_at)
       VALUES ('ssdi-cpq','SSDI',$1,$2,$3,$4,$5,$6,'pending',$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, (b.state || '').toUpperCase().trim(), JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[SSDI-CPQ] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  // Build Ringfuel Ping payload — caller_number is what the lead is held against
  const pingPayload = {
    api_key:      RINGFUEL_API_KEY,
    campaign_id:  RINGFUEL_CAMPAIGN_ID,
    caller_number: String(b.phone).replace(/\D/g, ''),
    caller_state: (b.state || '').toUpperCase().trim(),
    caller_zip:   b.zip || b.zip_code || undefined,
    first_name:   b.first_name,
    last_name:    b.last_name,
    email:        b.email,
    phone:        String(b.phone).replace(/\D/g, ''),
    address:      b.address || undefined,
    city:         b.city || undefined,
    state:        (b.state || '').toUpperCase().trim(),
    dob:          b.dob || undefined,
    ssn_last4:    b.ssn_last4 || undefined,
    trusted_form_cert_url: b.trustedform_cert_url || b.trusted_form_cert_url || b.trustedform_url || undefined,
    jornaya_leadid:  b.jornaya_leadid || undefined,
  };
  Object.keys(pingPayload).forEach(k => { if (pingPayload[k] === undefined) delete pingPayload[k]; });

  try {
    const pingRes = await postJSON(RINGFUEL_PING_URL, pingPayload);
    const result  = JSON.parse(pingRes.body);

    const available = result.available === true && result.targets && result.targets.count > 0;

    const c2 = await pool.connect();
    try {
      await c2.query(
        `UPDATE leads SET
           status         = $1,
           buyer_status   = $2,
           buyer_response = $3::jsonb,
           billable       = false,
           revenue        = 0
         WHERE id = $4`,
        [available ? 'forwarded' : 'buyer_rejected',
         available ? 'Ping Accepted' : 'Ping Rejected',
         JSON.stringify(result), leadId]
      );
    } finally { c2.release(); }

    console.log(`[SSDI-CPQ] ${available ? '✓' : '✕'} ${b.first_name} ${b.last_name} | available=${result.available} | bid range: ${result.targets ? result.targets.lowBid+'-'+result.targets.highBid : 'n/a'}`);

    return res.json({
      ok: available,
      result: available ? 'success' : 'rejected',
      message: available ? 'Ping accepted — lead held for dial' : 'No targets available',
      ping_id: result.pingId || null,
      dial_number: result.dialNumber || null,
      ttl: result.ttl || null,
      krw_id: leadId
    });
  } catch (err) {
    console.error('[SSDI-CPQ] Ping request failed:', err.message);
    const c3 = await pool.connect();
    try {
      await c3.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2", [err.message, leadId]);
    } finally { c3.release(); }
    return res.status(502).json({ ok: false, error: 'Failed to ping buyer', krw_id: leadId });
  }
});
// ─── END SSDI-CPQ ──────────────────────────────────────────────────────────────

// ─── SSDI-1696-NAST — CALLTOFFIC, VIA RINGFUEL PLATFORM (JOHN-SSDI-1696) ────────
// Buyer is Calltoffic 1696. Ringfuel is just the API/platform they use to receive
// pings - not to be confused with the buyer name itself.
// This is a COMPLETELY SEPARATE integration from R2D2/Aurion X below
// (/leads/ssdi-r2d2) - different buyer, different campaign, different
// credentials. Never conflate the two.
// Same Ringfuel Ping mechanism as SSDI-CPQ above, but a genuinely separate
// campaign with its own credentials, confirmed by Kyler (Aug 24) - not to be
// confused with or merged with the existing CPQ campaign either. Publisher
// posts lead data here; we ping Ringfuel and get back a dynamic tracking
// number for them to dial. We never see or handle the actual call itself.
// Payout: $500 flat. Never auto-billable - billing is confirmed later,
// manually or via postback (Kyler, Aug 28), not at ping-accept time.

const RINGFUEL_1696_API_KEY     = 'rfp_77e3ad43bf5e048d7f4566919fa968e3e341c80951bce25f';
const RINGFUEL_1696_CAMPAIGN_ID = 'c60aaee7-4a24-4a73-b318-8ded54134e15';
const RINGFUEL_PING_URL_1696    = 'https://app.ringfuel.io/api/publisher/ping';

app.post('/leads/ssdi-1696', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  const missing = [];
  if (!b.first_name)  missing.push('first_name');
  if (!b.last_name)   missing.push('last_name');
  if (!b.phone)        missing.push('phone');
  if (!b.email)        missing.push('email');
  if (!b.state)        missing.push('state');
  if (!b.publisher_sub) missing.push('publisher_sub');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;

  // Insert lead into DB first, for our own tracking/dashboard regardless of ping outcome
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, state, status, raw, received_at)
       VALUES ('ssdi-1696','SSDI',$1,$2,$3,$4,$5,$6,'pending',$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, (b.state || '').toUpperCase().trim(), JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[SSDI-1696] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  // Build Ringfuel Ping payload — caller_number is what the lead is held against
  const pingPayload = {
    api_key:      RINGFUEL_1696_API_KEY,
    campaign_id:  RINGFUEL_1696_CAMPAIGN_ID,
    caller_number: String(b.phone).replace(/\D/g, ''),
    caller_state: (b.state || '').toUpperCase().trim(),
    caller_zip:   b.zip || b.zip_code || undefined,
    first_name:   b.first_name,
    last_name:    b.last_name,
    email:        b.email,
    phone:        String(b.phone).replace(/\D/g, ''),
    address:      b.address || undefined,
    city:         b.city || undefined,
    state:        (b.state || '').toUpperCase().trim(),
    dob:          b.dob || undefined,
    ssn_last4:    b.ssn_last4 || undefined,
    trusted_form_cert_url: b.trustedform_cert_url || b.trusted_form_cert_url || b.trustedform_url || undefined,
    jornaya_leadid:  b.jornaya_leadid || undefined,
  };
  Object.keys(pingPayload).forEach(k => { if (pingPayload[k] === undefined) delete pingPayload[k]; });

  try {
    const pingRes = await postJSON(RINGFUEL_PING_URL_1696, pingPayload);
    const result  = JSON.parse(pingRes.body);

    const available = result.available === true && result.targets && result.targets.count > 0;

    const c2 = await pool.connect();
    try {
      await c2.query(
        `UPDATE leads SET
           status         = $1,
           buyer_status   = $2,
           buyer_response = $3::jsonb,
           billable       = false,
           revenue        = 0
         WHERE id = $4`,
        [available ? 'forwarded' : 'buyer_rejected',
         available ? 'Ping Accepted' : 'Ping Rejected',
         JSON.stringify(result), leadId]
      );
    } finally { c2.release(); }

    console.log(`[SSDI-1696] ${available ? '✓' : '✕'} ${b.first_name} ${b.last_name} | available=${result.available} | bid range: ${result.targets ? result.targets.lowBid+'-'+result.targets.highBid : 'n/a'}`);

    return res.json({
      ok: available,
      result: available ? 'success' : 'rejected',
      message: available ? 'Ping accepted — lead held for dial' : 'No targets available',
      ping_id: result.pingId || null,
      dial_number: result.dialNumber || null,
      ttl: result.ttl || null,
      krw_id: leadId
    });
  } catch (err) {
    console.error('[SSDI-1696] Ping request failed:', err.message);
    const c3 = await pool.connect();
    try {
      // Store a generic, publisher-safe message here, not the raw exception -
      // this field is visible on the publisher portal, and a raw network
      // error could reveal the buyer's actual domain/platform name.
      await c3.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2", ['Unable to reach buyer - technical error', leadId]);
    } finally { c3.release(); }
    return res.status(502).json({ ok: false, error: 'Failed to ping buyer', krw_id: leadId });
  }
});
// ─── END SSDI-1696 ──────────────────────────────────────────────────────────────

// ─── SSDI FIELDS LAW — JOSHUA DURAN ────────────────────────────────────────────
// Same publisher_sub as Joshua's existing Lead Tree SSDI line (KRW-JOSHUA-2026-76M)
// — his login and portal stay identical. This line is distinguished internally
// by campaign='ssdi-fieldslaw' instead of a separate publisher identity.
// Forwards via form-encoded POST to Fields Law's LeadDocket API (MediaRite Warm
// Transfer - SSD integration). Mktg_Campaign and Mktg_SubSource are injected
// server-side — generic, non-identifying values, never expose the publisher
// name to the buyer, matching the same principle used for every other buyer
// integration in this system.
// Payout: $200 — NOT auto-billed on submission. This only confirms an
// "opportunity" was created in Fields Law's system, not that the case signed.
// Billable gets set manually later, same as every other CPA-model line here.

const FIELDSLAW_API_KEY = '4f78906c';
const FIELDSLAW_FORM_URL = 'https://fieldslaw.leaddocket.com/opportunities/form/125?apikey=' + FIELDSLAW_API_KEY;
const FIELDSLAW_MKTG_CAMPAIGN   = 'KRW SSDI CPA';

app.post('/leads/ssdi-signed', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  const missing = [];
  if (!b.first_name)  missing.push('first_name');
  if (!b.last_name)   missing.push('last_name');
  if (!b.phone)        missing.push('phone');
  if (!b.email)        missing.push('email');
  if (!b.state)        missing.push('state');
  if (!b.publisher_sub) missing.push('publisher_sub');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;

  // Insert lead into DB first, for our own tracking regardless of Fields Law's response
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, state, status, billable, raw, received_at)
       VALUES ('ssdi-fieldslaw','SSDI',$1,$2,$3,$4,$5,$6,'pending',false,$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, (b.state || '').toUpperCase().trim(), JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[SSDI Fields Law] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  // Build Fields Law form-urlencoded payload — their exact field names
  const payload = new URLSearchParams();
  payload.append('First',   b.first_name);
  payload.append('Last',    b.last_name);
  payload.append('Phone',   b.phone);
  payload.append('Email',   b.email);
  payload.append('Summary', b.summary || 'SSDI Signed lead');
  if (b.city) payload.append('City', b.city);
  payload.append('State', b.state);
  if (b.zip || b.postal_code) payload.append('Postal_Code', b.zip || b.postal_code);
  payload.append('CaseLeadID', String(leadId));
  // Mktg_Campaign intentionally omitted — buyer confirmed the value we were
  // sending was incorrect and asked for it to be removed entirely (Aug 2026)
  payload.append('Mktg_SubSource', aliasPub(publisherSub) || FIELDSLAW_MKTG_CAMPAIGN);
  if (b.jornaya_leadid || b.trustedform_cert_url) {
    payload.append('Jornaya_or_Trusted_Form', b.jornaya_leadid || b.trustedform_cert_url);
  }

  try {
    const https = require('https');
    const postData = payload.toString();

    const flRes = await new Promise((resolve, reject) => {
      const url = new URL(FIELDSLAW_FORM_URL);
      const options = {
        hostname: url.hostname,
        path:     url.pathname + url.search,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const req2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      req2.on('error', reject);
      req2.write(postData);
      req2.end();
    });

    let result = {};
    try { result = JSON.parse(flRes.body); } catch(e) { result = { success: false, message: flRes.body }; }

    const c2 = await pool.connect();
    try {
      await c2.query(
        `UPDATE leads SET
           status         = $1,
           buyer_status   = $2,
           buyer_intake_id = $3,
           buyer_response = $4::jsonb
         WHERE id = $5`,
        [result.success ? 'forwarded' : 'buyer_rejected',
         result.success ? 'Opportunity Created' : 'Rejected',
         result.opportunityId ? String(result.opportunityId) : null,
         JSON.stringify(result), leadId]
      );
    } finally { c2.release(); }

    console.log(`[SSDI Fields Law] ${result.success ? '✓' : '✕'} ${b.first_name} ${b.last_name} | opportunityId: ${result.opportunityId || 'n/a'}`);

    return res.json({
      ok: !!result.success,
      result: result.success ? 'success' : 'rejected',
      message: result.message || (result.success ? 'Opportunity created' : 'Rejected'),
      opportunity_id: result.opportunityId || null,
      krw_id: leadId
    });
  } catch (err) {
    console.error('[SSDI Fields Law] Forward failed:', err.message);
    const c3 = await pool.connect();
    try {
      await c3.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2", [err.message, leadId]);
    } finally { c3.release(); }
    return res.status(502).json({ ok: false, error: 'Failed to forward to buyer', krw_id: leadId });
  }
});
// ─── END SSDI FIELDS LAW ────────────────────────────────────────────────────────

// ─── SSDI R2D2 (AURION X) ──────────────────────────────────────────────────────
// Simple lead-data forwarding, same pattern as SSDI Fields Law above. Publisher
// posts to us, we forward to R2D2's lead/insert API. Call transfer (whatever DID
// gets dialed) happens entirely outside our system on the publisher's side -
// same as Fields Law, we never touch the actual call.
// Buyer: R2D2 / Aurion X, campaign key D3JKXH21ZP, pubid "kdmr1" (buyer-assigned,
// confirmed by Kyler - NOT "r2d2", that was just Kyler's internal nickname).

const R2D2_API_KEY      = 'c2cc5f885d2a3790c85b9c1bde3fa2c3'; // Real key provided by R2D2 (Aug 28), labeled "KEY FOR KIRK Ping" on their end
const R2D2_CAMPAIGN_KEY = 'D3JKXH21ZP';
const R2D2_PUBID        = 'kdmr1';
const R2D2_INSERT_URL   = 'https://api.aurionx.ai/api/vendors/lead/insert';

function calculateAge(dobStr) {
  if (!dobStr) return null;
  const dob = new Date(dobStr);
  if (isNaN(dob.getTime())) return null;
  const today = new Date();
  let age = today.getFullYear() - dob.getFullYear();
  const m = today.getMonth() - dob.getMonth();
  if (m < 0 || (m === 0 && today.getDate() < dob.getDate())) age--;
  return age;
}

app.post('/leads/ssdi-r2d2', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Required per R2D2's own 400 error spec: pubid, email, phone, first_name, last_name, zip
  const missing = [];
  if (!b.first_name)  missing.push('first_name');
  if (!b.last_name)   missing.push('last_name');
  if (!b.phone)        missing.push('phone');
  if (!b.email)        missing.push('email');
  if (!b.zip && !b.zip_code) missing.push('zip');
  if (!b.publisher_sub) missing.push('publisher_sub');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;

  // Insert lead into DB first, for our own tracking regardless of buyer response
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, state, status, billable, raw, received_at)
       VALUES ('ssdi-r2d2','SSDI',$1,$2,$3,$4,$5,$6,'pending',false,$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, (b.state || '').toUpperCase().trim(), JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[SSDI R2D2] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  // Send both dob and age - R2D2's schema has both fields and it's not fully
  // clear which is actually required, so cover both rather than guess
  const calculatedAge = b.age || calculateAge(b.dob || b.date_of_birth);

  const payload = {
    pubid:      R2D2_PUBID,
    email:      b.email,
    phone:      String(b.phone).replace(/\D/g, ''),
    first_name: b.first_name,
    last_name:  b.last_name,
    zip:        b.zip || b.zip_code,
    ipaddress:  b.ip_address || b.ipaddress || undefined,
    address:    b.address || undefined,
    city:       b.city || undefined,
    state:      (b.state || '').toUpperCase().trim() || undefined,
    subid:      aliasPub(publisherSub) || '',
    age:        calculatedAge != null ? String(calculatedAge) : undefined,
    dob:        b.dob || b.date_of_birth || undefined,
    gender:     b.gender || undefined,
    country:    'US',
    leadtype:   'SSDI',
    sourcecertificate: b.trustedform_cert_url || b.trusted_form_cert_url || b.sourcecertificate || undefined,
  };
  Object.keys(payload).forEach(k => { if (payload[k] === undefined) delete payload[k]; });

  try {
    const r2d2Res = await new Promise((resolve, reject) => {
      const https = require('https');
      const url = new URL(R2D2_INSERT_URL);
      const postData = JSON.stringify(payload);
      const options = {
        hostname: url.hostname,
        path:     url.pathname + url.search,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(postData),
          'x-api-key':      R2D2_API_KEY,
        }
      };
      const req2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      req2.on('error', reject);
      req2.write(postData);
      req2.end();
    });

    let result = {};
    try { result = JSON.parse(r2d2Res.body); } catch(e) { result = { message: r2d2Res.body }; }
    const accepted = r2d2Res.status === 200 && !!result.leadId;

    const c2 = await pool.connect();
    try {
      await c2.query(
        `UPDATE leads SET
           status          = $1,
           buyer_intake_id = $2,
           buyer_response  = $3::jsonb,
           buyer_status    = $4
         WHERE id = $5`,
        [accepted ? 'forwarded' : 'buyer_rejected',
         result.leadId || null, JSON.stringify(result),
         accepted ? 'Accepted' : (result.message || 'Rejected'), leadId]
      );
    } finally { c2.release(); }

    console.log(`[SSDI R2D2] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | leadId: ${result.leadId || 'none'} | ${result.message || ''}`);

    return res.json({
      ok: accepted,
      result: accepted ? 'success' : 'rejected',
      message: result.message || (accepted ? 'Lead created' : 'Rejected'),
      lead_id: result.leadId || null,
      krw_id: leadId
    });
  } catch (err) {
    console.error('[SSDI R2D2] Forward failed:', err.message);
    const c3 = await pool.connect();
    try {
      await c3.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2", [err.message, leadId]);
    } finally { c3.release(); }
    return res.status(502).json({ ok: false, error: 'Failed to forward to buyer', krw_id: leadId });
  }
});
// ─── END SSDI R2D2 ──────────────────────────────────────────────────────────────

// ─── MVA PING/POST — DEDICATED ENDPOINT (NLD Dynamic Ping/Post, campaign 30934) ─
// Separate, standalone URL from /leads/mva-funnel (CPA) and /leads/mva-cpl
// (direct-post CPL). This endpoint ALWAYS routes through NLD's Ping/Post flow —
// no dependency on the campaign_settings toggle, no ambiguity about which
// campaign a lead hits. Publishers who want Ping/Post specifically post HERE;
// existing CPA and CPL posting instructions are completely unaffected.
// Reuses the already-verified forwardToNldPing() helper (auto-builds
// case_description, auto-converts incident_date to the ISO format this
// specific campaign requires) — same logic already proven working today.

app.post('/leads/mva-ping-post', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  const missing = [];
  if (!b.first_name)                              missing.push('first_name');
  if (!b.last_name)                                missing.push('last_name');
  if (!b.phone)                                    missing.push('phone');
  if (!b.email)                                    missing.push('email');
  if (!b.state)                                    missing.push('state');
  if (!b.zip_code && !b.zip)                       missing.push('zip_code');
  if (!b.publisher_sub)                            missing.push('publisher_sub');
  if (!b.ip_address)                               missing.push('ip_address');
  if (!b.user_agent)                               missing.push('user_agent');
  if (!b.landing_page_url)                         missing.push('landing_page_url');
  if (!b.trustedform_cert_url && !b.trusted_form_cert_url) missing.push('trustedform_cert_url');
  if (!b.tcpa_text)                                missing.push('tcpa_text');
  if (!b.have_attorney)                            missing.push('have_attorney');
  if (!b.at_fault)                                 missing.push('at_fault');
  if (!b.injury_type)                              missing.push('injury_type');
  if (!b.incident_date)                            missing.push('incident_date');
  if (!b.police_report)                            missing.push('police_report');
  if (!b.has_insurance)                            missing.push('has_insurance');
  if (!b.medical_treatment)                        missing.push('medical_treatment');
  if (!b.accident_type)                            missing.push('accident_type');
  if (!b.compensated_before)                       missing.push('compensated_before');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;
  const leadState = (b.state || '').toUpperCase().trim();

  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, state, status, raw, received_at)
       VALUES ('mva-ping-post','MVA',$1,$2,$3,$4,$5,$6,'pending',$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, leadState, JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[MVA Ping/Post] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  const pingAttempt = await forwardToNldPing(b, publisherSub);

  if (!pingAttempt.routed) {
    // Should not normally happen given the required-field check above, but
    // fail loudly here rather than silently — this endpoint's whole purpose
    // is guaranteed Ping/Post routing, so a fallback would defeat the point.
    const errorDetail = pingAttempt.nldMessage || pingAttempt.reason || 'ping_not_routed';
    const cErr = await pool.connect();
    try {
      await cErr.query(
        "UPDATE leads SET status='error', buyer_error=$1, buyer_response=$2::jsonb WHERE id=$3",
        [errorDetail, JSON.stringify(pingAttempt.nldResponse || {}), leadId]
      );
    } finally { cErr.release(); }
    console.error(`[MVA Ping/Post] ✕ Failed to route ${b.first_name} ${b.last_name} — ${errorDetail}`);
    return res.status(502).json({ ok: false, error: 'Failed to route through Ping/Post', reason: pingAttempt.reason, nld_message: pingAttempt.nldMessage || null, krw_id: leadId });
  }

  const c2 = await pool.connect();
  try {
    await c2.query(
      `UPDATE leads SET
         status         = $1,
         buyer_response = $2::jsonb,
         buyer_status   = $3,
         billable       = false,
         revenue        = 0,
         raw            = COALESCE(raw,'{}'::jsonb) || $4::jsonb
       WHERE id = $5`,
      [pingAttempt.billable ? 'forwarded' : 'buyer_rejected',
       JSON.stringify(pingAttempt.buyerResponse), pingAttempt.buyerStatus,
       JSON.stringify({ nld_ping_bid: pingAttempt.bidAmount }), leadId]
    );
  } finally { c2.release(); }

  console.log(`[MVA Ping/Post] ${pingAttempt.billable ? '✓' : '✕'} ${b.first_name} ${b.last_name} | bid $${pingAttempt.bidAmount} | ${pingAttempt.buyerStatus}`);

  return res.json({
    ok: pingAttempt.billable,
    result: pingAttempt.billable ? 'success' : 'rejected',
    message: pingAttempt.buyerStatus,
    krw_id: leadId
  });
});
// ─── END MVA PING/POST ─────────────────────────────────────────────────────────

// ─── MVA FUNNEL POSTBACK — EMAIL AGENCY STATUS UPDATES ───────────────────────
// Receives lead status postbacks from Email Agency.
// Matches by lead_id (buyer_intake_id) or phone.
// Updates buyer_status, status, and notes in the leads table.
// Fires billable email notification if lead flips to billable.
// No publisher info is exposed — internal only.

app.post('/postback/mva-funnel', async (req, res) => {
  // Accept postbacks with or without API key — Email Agency won't send one
  const b = req.body || {};

  const leadId     = (b.lead_id      || '').trim();
  const phone      = (b.phone        || '').replace(/\D/g, '').trim();
  const status     = (b.status       || '').trim();
  const disposition = (b.disposition || b.last_call_disposition || b.dispo || '').trim();
  const firstName  = (b.first_name   || '').trim();
  const lastName   = (b.last_name    || '').trim();
  const state      = (b.state        || '').trim();

  if (!leadId && !phone) {
    return res.status(400).json({ ok: false, error: 'lead_id or phone required' });
  }
  if (!status) {
    return res.status(400).json({ ok: false, error: 'status required' });
  }

  try {
    const client = await pool.connect();
    try {
      // Look up lead by buyer_intake_id first, then phone
      let lookup;
      if (leadId) {
        lookup = await client.query(
          `SELECT id, first_name, last_name, phone, email, buyer_status, campaign
           FROM leads
           WHERE buyer_intake_id = $1 AND campaign = 'mva-funnel'
           LIMIT 1`,
          [leadId]
        );
      }
      if (!lookup || !lookup.rows.length) {
        lookup = await client.query(
          `SELECT id, first_name, last_name, phone, email, buyer_status, campaign
           FROM leads
           WHERE phone = $1 AND campaign = 'mva-funnel'
           ORDER BY received_at DESC LIMIT 1`,
          [phone]
        );
      }

      if (!lookup.rows.length) {
        console.log(`[MVA Postback] No match — lead_id: ${leadId} | phone: ${phone}`);
        return res.json({ ok: false, error: 'Lead not found' });
      }

      const lead = lookup.rows[0];

      // Check if newly billable
      const wasAlreadyBillable = (() => {
        const prev = (lead.buyer_status || '').toLowerCase().trim();
        return prev.includes('billable') || prev.includes('accepted') || prev === 'signed';
      })();

      const isNowBillable = (() => {
        const cur = status.toLowerCase().trim();
        return cur.includes('billable') || cur.includes('accepted') || cur === 'signed';
      })();

      // Map status to lead status
      const statusLow = status.toLowerCase();
      let leadStatus = null;
      if (isNowBillable) leadStatus = 'forwarded';
      else if (statusLow.includes('reject') || statusLow.includes('disqualif')) leadStatus = 'buyer_rejected';

      // Build notes from disposition if provided
      const notesUpdate = disposition || null;

      // Update the lead
      const patch = JSON.stringify({
        ea_postback: {
          status:      status,
          disposition: disposition || null,
          state:       state || null,
          synced_at:   new Date().toISOString(),
        }
      });

      if (leadStatus) {
        await client.query(
          `UPDATE leads SET
             buyer_status = $1,
             status       = $2,
             notes        = CASE WHEN $5::text IS NOT NULL AND $5::text != '' THEN $5::text ELSE notes END,
             raw          = raw || $3::jsonb
           WHERE id = $4
             AND (raw->>'billable_locked') IS DISTINCT FROM 'true'`,
          [status, leadStatus, patch, lead.id, notesUpdate || null]
        );
      } else {
        await client.query(
          `UPDATE leads SET
             buyer_status = $1,
             notes        = CASE WHEN $3::text IS NOT NULL AND $3::text != '' THEN $3::text ELSE notes END,
             raw          = raw || $2::jsonb
           WHERE id = $4
             AND (raw->>'billable_locked') IS DISTINCT FROM 'true'`,
          [status, patch, notesUpdate || null, lead.id]
        );
      }

      console.log(`[MVA Postback] ✅ ID ${lead.id} | ${lead.first_name} ${lead.last_name} | ${status}`);

      // Fire billable email if newly flipped
      if (isNowBillable && !wasAlreadyBillable) {
        const name     = [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown';
        const dateStr  = new Date().toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });
        const emailHtml = `
          <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:520px;margin:0 auto;background:#f4f6fb;padding:32px 20px">
            <div style="background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
              <div style="background:#0f1c3f;padding:24px 28px">
                <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.15em;color:rgba(255,255,255,.5);margin-bottom:6px">KRW Marketing Solutions</div>
                <div style="font-size:22px;font-weight:700;color:#fff">✓ Billable Lead</div>
                <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">${dateStr}</div>
              </div>
              <div style="padding:28px">
                <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px 20px;margin-bottom:20px">
                  <div style="font-size:18px;font-weight:700;color:#15803d;margin-bottom:4px">${name}</div>
                  <div style="font-size:13px;color:#166534">Marked billable — ${status}</div>
                </div>
                <table style="width:100%;border-collapse:collapse;font-size:13px">
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;width:38%">Campaign</td>
                    <td style="padding:10px 0;font-weight:600;color:#0f1c3f">MVA Funnel</td>
                  </tr>
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Phone</td>
                    <td style="padding:10px 0;color:#475569">${lead.phone || '—'}</td>
                  </tr>
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">State</td>
                    <td style="padding:10px 0;color:#475569">${state || lead.state || '—'}</td>
                  </tr>
                  <tr>
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Status</td>
                    <td style="padding:10px 0;color:#475569">${status}</td>
                  </tr>
                </table>
              </div>
              <div style="padding:16px 28px;background:#f9fafb;border-top:1px solid #f1f5f9;font-size:11px;color:#9ca3af;text-align:center">
                KRW Marketing Solutions · Lead Notification System
              </div>
            </div>
          </div>`;

        await sendEmailNotification(
          `✓ Billable Lead — ${name} | MVA | mva-funnel`,
          emailHtml
        );
        console.log(`[MVA Postback] 📧 Billable email sent for ${name}`);
      }

      return res.json({ ok: true, lead_id: lead.id, status_updated: status });

    } finally {
      client.release();
    }
  } catch(err) {
    console.error('[MVA Postback] Error:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});
// ─── END MVA FUNNEL POSTBACK ──────────────────────────────────────────────────

// ─── SSDI CALLS POSTBACK — RINGBA ────────────────────────────────────────────
// Receives call completion postbacks from Ringba for SSDI campaign.
// Stores call data in calls table and links to publisher portal.
// DID: +1 (321) 603-3068 | Publisher: Joshua Duran (KRW-JOSHUA-2026-76M)

app.post('/postback/ssdi-calls', async (req, res) => {
  const b = req.body || {};

  // Accept both our field names and Ringba's field names
  const phone        = ((b.phone || b.caller_id || '')).replace(/\D/g, '').trim() || null;
  const firstName    = (b.first_name    || '').trim() || null;
  const lastName     = (b.last_name     || '').trim() || null;
  const cid          = (b.cid || b.call_id || '').trim() || null;
  const duration     = parseInt(b.duration || b.call_length || 0) || 0;
  const recordingUrl = (b.recording_url || '').trim() || null;
  const state        = (b.state || '').trim().toUpperCase() || null;

  // publisher_sub is ALWAYS set server-side — never from buyer postback
  // Ringba sends publisher_id which we ignore and inject the real value
  const publisherSub = 'KRW-JOSHUA-2026-76M';

  if (!phone && !cid) {
    return res.status(400).json({ ok: false, error: 'phone or cid required' });
  }

  try {
    const client = await pool.connect();
    try {
      // Check for duplicate by CID
      if (cid) {
        const dup = await client.query(
          `SELECT id FROM calls WHERE buyer_call_id = $1 LIMIT 1`, [cid]
        );
        if (dup.rows.length) {
          return res.json({ ok: true, duplicate: true, message: 'Call already recorded', id: dup.rows[0].id });
        }
      }

      // Determine billable status based on duration (threshold: 120 seconds)
      const billable = duration >= 120;

      // Insert into calls table
      const insert = await client.query(
        `INSERT INTO calls
           (campaign, caller_id, caller_name, call_duration, billable,
            call_status_label, disposition, publisher_sub, buyer_call_id,
            recording_url, state, call_date, raw)
         VALUES ('ssdi',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),$11::jsonb)
         RETURNING id`,
        [
          phone,
          [firstName, lastName].filter(Boolean).join(' ') || null,
          duration,
          billable,
          billable ? 'Billable' : 'Non-Billable',
          billable ? 'Transferred' : 'Short Call',
          publisherSub,
          cid,
          recordingUrl,
          state,
          JSON.stringify(b),
        ]
      );

      const callId = insert.rows[0].id;
      console.log(`[SSDI Postback] ✅ Call ${callId} | ${firstName} ${lastName} | ${phone} | ${duration}s | ${billable ? 'Billable' : 'Not Billable'}`);

      // Fire email if billable
      if (billable) {
        const name    = [firstName, lastName].filter(Boolean).join(' ') || phone;
        const dateStr = new Date().toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });
        const emailHtml = `
          <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:520px;margin:0 auto;background:#f4f6fb;padding:32px 20px">
            <div style="background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
              <div style="background:#0f1c3f;padding:24px 28px">
                <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.15em;color:rgba(255,255,255,.5);margin-bottom:6px">KRW Marketing Solutions</div>
                <div style="font-size:22px;font-weight:700;color:#fff">✓ Billable SSDI Call</div>
                <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">${dateStr}</div>
              </div>
              <div style="padding:28px">
                <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px 20px;margin-bottom:20px">
                  <div style="font-size:18px;font-weight:700;color:#15803d;margin-bottom:4px">${name}</div>
                  <div style="font-size:13px;color:#166534">Billable transfer — ${duration}s duration</div>
                </div>
                <table style="width:100%;border-collapse:collapse;font-size:13px">
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;width:38%">Campaign</td>
                    <td style="padding:10px 0;font-weight:600;color:#0f1c3f">SSDI Filed — Campaign 1696</td>
                  </tr>
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Phone</td>
                    <td style="padding:10px 0;color:#475569">${phone || '—'}</td>
                  </tr>
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">State</td>
                    <td style="padding:10px 0;color:#475569">${state || '—'}</td>
                  </tr>
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Duration</td>
                    <td style="padding:10px 0;color:#475569">${duration} seconds</td>
                  </tr>
                  <tr style="border-bottom:1px solid #f1f5f9">
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Call ID</td>
                    <td style="padding:10px 0;font-family:monospace;color:#475569">${cid || '—'}</td>
                  </tr>
                  ${recordingUrl ? `<tr>
                    <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Recording</td>
                    <td style="padding:10px 0"><a href="${recordingUrl}" style="color:#2563eb">Listen</a></td>
                  </tr>` : ''}
                </table>
              </div>
              <div style="padding:16px 28px;background:#f9fafb;border-top:1px solid #f1f5f9;font-size:11px;color:#9ca3af;text-align:center">
                KRW Marketing Solutions · Lead Notification System
              </div>
            </div>
          </div>`;

        await sendEmailNotification(
          `✓ Billable SSDI Call — ${name} | ${duration}s | Campaign 1696`,
          emailHtml
        );
        console.log(`[SSDI Postback] 📧 Billable email sent for ${name}`);
      }

      return res.json({ ok: true, id: callId, billable, duration, message: billable ? 'Billable call recorded' : 'Call recorded' });

    } finally {
      client.release();
    }
  } catch(err) {
    console.error('[SSDI Postback] Error:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});
// ─── END SSDI CALLS POSTBACK ─────────────────────────────────────────────────

// ─── LSSDI-SHORE — CLIENT 50 PHONEXA BUYER ──────────────────────────────────
// Receives SSDI leads from publisher and forwards to Client 50's Phonexa
// "Set Data" endpoint. Campaign: Lssdi-shore | Buyer: Client 50 (Phonexa)
// centerCode is a fixed internal value, injected server-side, never exposed
// to the publisher in any request, response, or error message.

const LSSDI_SHORE_URL        = 'https://leads-inst362-client.phonexa.com/store/setdata';
const LSSDI_SHORE_API_ID     = 'B17725F3F50E44BFB2F1BD84CAC1A8C5';
const LSSDI_SHORE_API_PASS   = '0b0ebc19dc3eab7bd5d8bf19f';
const LSSDI_SHORE_PRODUCT_ID = 207;
const LSSDI_SHORE_CENTER_CODE = 'lisa 115'; // NEVER expose this value to publisher-facing responses

// Per-publisher center code overrides — buyer requirement, injected server-side.
// Publishers never see or need to send this; it's added automatically based on publisher_sub.
const LSSDI_SHORE_CENTER_CODE_OVERRIDES = {
  'KRW-SSDI-2026-4QM': 'Lisa 118', // Nexus-7 — buyer-required center code (confirmed via live test posting 7/20)
};
function getLssdiShoreCenterCode(publisherSub) {
  return LSSDI_SHORE_CENTER_CODE_OVERRIDES[publisherSub] || LSSDI_SHORE_CENTER_CODE;
}

app.post('/leads/Lssdi-shore', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Validate required fields — per updated buyer requirements
  const missing = [];
  if (!b.first_name)          missing.push('first_name');
  if (!b.last_name)            missing.push('last_name');
  if (!b.dob)                  missing.push('dob');
  if (!b.email)                missing.push('email');
  if (!b.phone)                missing.push('phone');
  if (!b.trustedform_cert_url) missing.push('trustedform_cert_url');
  if (!b.zip)                  missing.push('zip');
  if (!b.publisher_sub)        missing.push('publisher_sub');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;

  // Insert lead into DB
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, state, zip, status, raw, received_at)
       VALUES ('Lssdi-shore','SSDI',$1,$2,$3,$4,$5,$6,$7,'pending',$8::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, b.state, b.zip,
       JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[Lssdi-shore] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Build Phonexa payload — centerCode injected here, never from publisher input
  const payload = {
    apiId:                  LSSDI_SHORE_API_ID,
    apiPassword:            LSSDI_SHORE_API_PASS,
    productId:              LSSDI_SHORE_PRODUCT_ID,
    phoneNumber:            b.phone,
    trustedFormURL:         b.trustedform_cert_url,
    email:                  b.email,
    optInDate:              new Date().toISOString().split('T')[0],
    firstName:              b.first_name,
    lastname:                b.last_name,
    dob:                    b.dob,
    zip:                    b.zip,
    centerCode:             getLssdiShoreCenterCode(publisherSub),
    source:                 publisherSub,
    validateProductFields:  1,
  };

  // Optional fields — included only if provided
  if (b.age)                      payload.consumerAge            = String(b.age);
  if (b.gender)                   payload.gender                 = (b.gender || '').toUpperCase();
  if (b.address)                  payload.address                = b.address;
  if (b.city)                     payload.city                   = b.city;
  if (b.state)                    payload.state                  = (b.state || '').toUpperCase();
  if (b.currently_receiving_ssdi) payload.currentlyReceivingSsdi  = b.currently_receiving_ssdi;
  if (b.applied)                  payload.applied                = b.applied;
  if (b.injury)                   payload.injury                 = b.injury;
  if (b.injury_timeframe)         payload.injuryTimeFrame         = b.injury_timeframe;
  if (b.treated)                  payload.treated                = b.treated;
  if (b.attorney)                 payload.attorney               = b.attorney;
  if (b.work_negligence)          payload.workNegligence         = b.work_negligence;
  if (b.working_now)              payload.workingNow             = b.working_now;

  try {
    const https    = require('https');
    const postData = JSON.stringify(payload);
    const url      = new URL(LSSDI_SHORE_URL);

    const buyerRes = await new Promise((resolve, reject) => {
      const options = {
        hostname: url.hostname,
        path:     url.pathname,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const r2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      r2.on('error', reject);
      r2.write(postData);
      r2.end();
    });

    let result = {};
    try { result = JSON.parse(buyerRes.body); } catch(e) { result = { status: 0, message: buyerRes.body }; }

    const accepted = result.status === 1;

    if (leadId) {
      const c2 = await pool.connect();
      try {
        await c2.query(
          `UPDATE leads SET
             status          = $1,
             buyer_response  = $2::jsonb,
             buyer_error     = $3,
             revenue         = 0
           WHERE id = $4`,
          [
            accepted ? 'forwarded' : 'buyer_rejected',
            JSON.stringify(result),
            accepted ? null : (result.message || JSON.stringify(result.errors) || 'rejected'),
            leadId
          ]
        );
      } finally { c2.release(); }
    }

    console.log(`[Lssdi-shore] ${accepted ? '✅' : '❌'} ${b.first_name} ${b.last_name} | ${result.message || 'no msg'}`);

    // Mirror this lead into the calls table so it shows on the same publisher
    // portal Joshua uses — keeps a single consistent portal experience across
    // both calls-based and leads-based SSDI publishers.
    if (accepted) {
      const c4 = await pool.connect();
      try {
        await c4.query(
          `INSERT INTO calls
             (campaign, caller_id, caller_name, billable,
              call_status_label, disposition, publisher_sub, buyer_call_id,
              source_system, call_date, raw)
           VALUES ('ssdi',$1,$2,false,'Forwarded','Lead Forwarded',$3,$4,'partner',NOW(),$5::jsonb)`,
          [
            b.phone,
            [b.first_name, b.last_name].filter(Boolean).join(' ') || null,
            publisherSub,
            'lssdi-' + leadId,
            JSON.stringify(b)
          ]
        );
      } catch(callErr) {
        console.error('[Lssdi-shore] calls table insert error:', callErr.message);
      } finally {
        c4.release();
      }

      // No email on forward — billable is determined manually or via buyer postback
      console.log(`[Lssdi-shore] ✅ Lead forwarded for ${[b.first_name, b.last_name].filter(Boolean).join(' ') || b.phone}`);
    }

    return res.json({
      ok:      accepted,
      result:  accepted ? 'success' : 'rejected',
      message: result.message || null,
      krw_id:  leadId
    });

  } catch(fwdErr) {
    console.error('[Lssdi-shore] Forward error:', fwdErr.message);
    if (leadId) {
      const c3 = await pool.connect();
      try {
        await c3.query("UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2",
          [fwdErr.message, leadId]);
      } finally { c3.release(); }
    }
    return res.status(502).json({ ok: false, error: 'Failed to forward lead', detail: fwdErr.message });
  }
});
// ─── END LSSDI-SHORE ──────────────────────────────────────────────────────────

// ── Lssdi-shore debug endpoint — remove after testing ────────────────────────
// NOTE: Buyer requires REAL data on test calls, not the word "test"
app.get('/debug-lssdi-shore', requireKey, async (req, res) => {
  try {
    const payload = {
      apiId:                  LSSDI_SHORE_API_ID,
      apiPassword:            LSSDI_SHORE_API_PASS,
      productId:              LSSDI_SHORE_PRODUCT_ID,
      phoneNumber:            '3105550199',
      trustedFormURL:         'https://cert.trustedform.com/0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f',
      email:                  'janedoe@example.com',
      optInDate:              new Date().toISOString().split('T')[0],
      firstName:              'Jane',
      lastname:                'Doe',
      dob:                    '1968-04-12',
      zip:                    '85001',
      centerCode:             LSSDI_SHORE_CENTER_CODE,
      source:                 'KRW-debug-test',
      validateProductFields:  1,
    };

    const https    = require('https');
    const postData = JSON.stringify(payload);
    const url      = new URL(LSSDI_SHORE_URL);

    const buyerRes = await new Promise((resolve, reject) => {
      const options = {
        hostname: url.hostname,
        path:     url.pathname,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const r2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      r2.on('error', reject);
      r2.write(postData);
      r2.end();
    });

    res.json({
      http_status:  buyerRes.status,
      raw_body:     buyerRes.body,
      payload_sent: { ...payload, apiPassword: '[redacted]', centerCode: '[redacted]' },
    });
  } catch(err) {
    res.json({ error: err.message });
  }
});
// ── End Lssdi-shore debug ─────────────────────────────────────────────────────




// Receives Rideshare (Uber/Lyft) leads from publishers and forwards to
// True Blue Marketing's LeadsPedia endpoint.
// Campaign: rideshare-tb | Buyer: True Blue Marketing
// Completely separate from SSDI and FE verticals.

const TRUEBLUE_RIDESHARE_URL          = 'https://trueblue.leadspediatrack.com/post.do';
const TRUEBLUE_RIDESHARE_CAMPAIGN_ID  = '6a2061d7810c9';
const TRUEBLUE_RIDESHARE_CAMPAIGN_KEY = '3LXznRbVmPyYrv9hWjdf';

app.post('/leads/rideshare-tb', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};

  // Validate all required fields — no fallbacks, real data only
  const missing = [];
  if (!b.first_name)       missing.push('first_name');
  if (!b.last_name)        missing.push('last_name');
  if (!b.phone_home)       missing.push('phone_home');
  if (!b.email_address)    missing.push('email_address');
  if (!b.zip_code)         missing.push('zip_code');
  if (!b.ip_address)       missing.push('ip_address');
  if (!b.attorney)         missing.push('attorney');
  if (!b.landing_page_url) missing.push('landing_page_url');
  if (!b.jornaya_lead_id && !b.trusted_form_cert_id) missing.push('jornaya_lead_id or trusted_form_cert_id');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  // Publisher sub tracking
  const publisherSub = b.publisher_sub || b.lp_s1 || null;

  // Build True Blue payload
  const payload = new URLSearchParams();
  payload.append('lp_campaign_id',  TRUEBLUE_RIDESHARE_CAMPAIGN_ID);
  payload.append('lp_campaign_key', TRUEBLUE_RIDESHARE_CAMPAIGN_KEY);
  payload.append('lp_response',     'json');

  // Required fields
  payload.append('first_name',       b.first_name);
  payload.append('last_name',        b.last_name);
  payload.append('phone_home',       b.phone_home);
  payload.append('email_address',    b.email_address);
  payload.append('zip_code',         b.zip_code);
  payload.append('ip_address',       b.ip_address);
  payload.append('attorney',         b.attorney);
  payload.append('landing_page_url', b.landing_page_url);
  payload.append('lp_caller_id',     b.lp_caller_id || b.phone_home);

  // TCPA compliance
  if (b.jornaya_lead_id)      payload.append('jornaya_lead_id',      b.jornaya_lead_id);
  if (b.trusted_form_cert_id) payload.append('trusted_form_cert_id', b.trusted_form_cert_id);

  // Optional fields
  const optFields = ['phone_cell','phone_work','phone_ext','address','address2',
    'city','state','county','country','dob','experience_assault',
    'description','lp_s1','lp_s2','lp_s3','lp_s4','lp_s5','lp_test'];
  optFields.forEach(f => { if (b[f]) payload.append(f, b[f]); });

  // Publisher sub as lp_s1 if not already set
  if (publisherSub && !b.lp_s1) payload.append('lp_s1', publisherSub);

  // Log the lead attempt
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, ip_address, zip, state, status, raw, received_at)
       VALUES ('rideshare-tb','Mass Tort - Rideshare',$1,$2,$3,$4,$5,$6,$7,$8,'pending',$9::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone_home, b.email_address,
       publisherSub, b.ip_address || null,
       b.zip_code || null, b.state || null,
       JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[Rideshare TB] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Forward to True Blue
  try {
    const https = require('https');
    const postData = payload.toString();

    const tbRes = await new Promise((resolve, reject) => {
      const url = new URL(TRUEBLUE_RIDESHARE_URL);
      const options = {
        hostname: url.hostname,
        path:     url.pathname,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const req2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      req2.on('error', reject);
      req2.write(postData);
      req2.end();
    });

    // Parse JSON response
    let tbResult = {};
    try { tbResult = JSON.parse(tbRes.body); } catch(e) {
      const xmlResult = tbRes.body.match(/<result>(.*?)<\/result>/)?.[1] || 'unknown';
      const tbLeadId  = tbRes.body.match(/<lead_id>(.*?)<\/lead_id>/)?.[1] || null;
      const price     = tbRes.body.match(/<price>(.*?)<\/price>/)?.[1] || '0.00';
      tbResult = { result: xmlResult, lead_id: tbLeadId, price };
    }

    const accepted = tbResult.result === 'success';

    // Update lead status in DB
    if (leadId) {
      const c2 = await pool.connect();
      try {
        await c2.query(
          `UPDATE leads SET
             status          = $1,
             buyer_intake_id = $2,
             buyer_response  = $3::jsonb,
             revenue         = $4
           WHERE id = $5`,
          [
            accepted ? 'forwarded' : 'buyer_rejected',
            tbResult.lead_id || null,
            JSON.stringify(tbResult),
            parseFloat(tbResult.price) || 0,
            leadId
          ]
        );
      } finally { c2.release(); }
    }

    console.log(`[Rideshare TB] ${accepted ? '✅' : '❌'} ${b.first_name} ${b.last_name} → ${tbResult.result} | ${tbResult.lead_id || 'no id'} | $${tbResult.price || '0.00'}`);

    return res.json({
      ok:      accepted,
      result:  tbResult.result,
      lead_id: tbResult.lead_id || null,
      price:   tbResult.price   || '0.00',
      message: accepted ? 'Lead accepted by True Blue' : 'Lead rejected by True Blue',
      errors:  tbResult.errors  || null,
      krw_id:  leadId
    });

  } catch (fwdErr) {
    console.error('[Rideshare TB] Forward error:', fwdErr.message);
    if (leadId) {
      const c3 = await pool.connect();
      try {
        await c3.query(
          "UPDATE leads SET status='error', buyer_error=$1 WHERE id=$2",
          [fwdErr.message, leadId]
        );
      } finally { c3.release(); }
    }
    return res.status(502).json({ ok: false, error: 'Failed to forward to buyer', detail: fwdErr.message });
  }
});
// ─── END RIDESHARE TRUE BLUE ─────────────────────────────────────────────────

// ── True Blue debug endpoint — remove after testing ──────────────────────────
app.get('/debug-trueblue', requireKey, async (req, res) => {
  try {
    const payload = new URLSearchParams();
    payload.append('lp_campaign_id',  TRUEBLUE_RIDESHARE_CAMPAIGN_ID);
    payload.append('lp_campaign_key', TRUEBLUE_RIDESHARE_CAMPAIGN_KEY);
    payload.append('lp_response',     'json');
    payload.append('lp_test',         '1');
    payload.append('first_name',      'Test');
    payload.append('last_name',       'Lead');
    payload.append('phone_home',      '3105550123');
    payload.append('email_address',   'test@test.com');
    payload.append('zip_code',        '90210');
    payload.append('ip_address',      '72.21.198.66');
    payload.append('attorney',        'No');
    payload.append('landing_page_url','https://krwmarketingsolutions.com');
    payload.append('lp_caller_id',    '3105550123');
    payload.append('jornaya_lead_id', 'test-jornaya-token-123');

    const https = require('https');
    const postData = payload.toString();
    const url = new URL(TRUEBLUE_RIDESHARE_URL);

    const tbRes = await new Promise((resolve, reject) => {
      const options = {
        hostname: url.hostname,
        path:     url.pathname,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const r2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data, headers: r.headers }));
      });
      r2.on('error', reject);
      r2.write(postData);
      r2.end();
    });

    res.json({
      http_status:    tbRes.status,
      raw_body:       tbRes.body,
      response_headers: tbRes.headers,
      payload_sent:   postData,
    });
  } catch(err) {
    res.json({ error: err.message });
  }
});
// ── End True Blue debug ───────────────────────────────────────────────────────

// ── Email Agency debug endpoint — remove after testing ───────────────────────
app.get('/debug-emailagency', requireKey, async (req, res) => {
  return res.status(410).json({ ok: false, error: 'Deprecated — Email Agency is no longer the MVA CPA buyer. This debug tool has been disabled.' });
  try {
    const testPayload = {
      key:          EA_MVA_KEY,
      code:         EA_MVA_CODE,
      first_name:   'Test',
      last_name:    'Lead',
      phone:        '3105550123',
      email:        'test@test.com',
      ip_address:   '72.21.198.66',
      attorney:     'No',
      accident_fault: 'No',
      channel:      'Facebook',
      trusted_form_cert_url: 'https://cert.trustedform.com/test123',
      sub_id2:      'MVA K',
      state:        'AZ',
      zip:          '85001',
    };

    const https    = require('https');
    const postData = JSON.stringify(testPayload);
    const url      = new URL(EA_MVA_URL);

    const eaRes = await new Promise((resolve, reject) => {
      const options = {
        hostname: url.hostname,
        path:     url.pathname + url.search,
        method:   'POST',
        headers:  {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(postData),
        }
      };
      const r2 = https.request(options, (r) => {
        let data = '';
        r.on('data', chunk => data += chunk);
        r.on('end', () => resolve({ status: r.statusCode, body: data }));
      });
      r2.on('error', reject);
      r2.write(postData);
      r2.end();
    });

    let eaResult = {};
    try { eaResult = JSON.parse(eaRes.body); } catch(e) { eaResult = { raw: eaRes.body }; }

    res.json({
      http_status:  eaRes.status,
      raw_body:     eaRes.body,
      parsed:       eaResult,
      payload_sent: testPayload,
    });
  } catch(err) {
    res.json({ error: err.message });
  }
});
// ── End Email Agency debug ────────────────────────────────────────────────────


// Polls two Google Sheet tabs every hour (MVA + Rideshare).
// Matches rows by CID → buyer_intake_id in the leads table.
// Only touches leads with campaign IN ('mva-funnel', 'rideshare-tb').
// SSDI and FE verticals are NEVER touched.
// Sheet columns: Date | CID | First Name | Last Name | Email Address |
//                Intake Center Status | NLD Status | Date of Notes Updated |
//                Notes | Billable

const KA_SHEET_ID = '1_NBKeIAg7p87mTDneR_fANGx9AqGV8abpWe29EBoko4';

const KA_SHEETS = [
  {
    name:     'MVA',
    campaign: 'mva-funnel',
    pubSubs:  ['KRW-KANTHONY-RS'],
    url:      `https://docs.google.com/spreadsheets/d/${KA_SHEET_ID}/export?format=csv&gid=1713913985`,
  },
  {
    name:     'Rideshare',
    campaign: 'rideshare-tb',
    pubSubs:  ['KRW-PUB-2026-7QJ', 'KRW-KEVIN-2026-SMG'],
    url:      `https://docs.google.com/spreadsheets/d/${KA_SHEET_ID}/export?format=csv&gid=968537461`,
  },
];

const KA_POLL_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

async function fetchKASheetCSV(url) {
  const https = require('https');
  const http  = require('http');
  return new Promise((resolve, reject) => {
    const get = (u, redirectCount = 0) => {
      if (redirectCount > 5) return reject(new Error('Too many redirects'));
      const lib = u.startsWith('https') ? https : http;
      lib.get(u, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return get(res.headers.location, redirectCount + 1);
        }
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => resolve(data));
      }).on('error', reject);
    };
    get(url);
  });
}

function parseKASheetCSV(text) {
  const lines = text.trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.replace(/"/g, '').trim());
  return lines.slice(1).map(line => {
    const cols = [];
    let cur = '', inQuote = false;
    for (let i = 0; i < line.length; i++) {
      if (line[i] === '"') { inQuote = !inQuote; continue; }
      if (line[i] === ',' && !inQuote) { cols.push(cur.trim()); cur = ''; continue; }
      cur += line[i];
    }
    cols.push(cur.trim());
    const row = {};
    headers.forEach((h, i) => row[h] = cols[i] || '');
    return row;
  });
}

async function pollKALeadsSheet() {
  for (const sheet of KA_SHEETS) {
    try {
      const csv  = await fetchKASheetCSV(sheet.url);
      const rows = parseKASheetCSV(csv);
      if (!rows.length) {
        console.log(`[KA Sheet Poll] ${sheet.name}: no rows found`);
        continue;
      }

      const client = await pool.connect();
      try {
        let matched = 0, unmatched = 0, skipped = 0;

        for (const row of rows) {
          const isMVA = sheet.campaign === 'mva-funnel';

          // MVA sheet: Date, First Name, Last Name, Phone, Status, Notes (no CID)
          // Rideshare sheet: Date, CID, First Name, Last Name, Email, Intake Center Status, NLD Status, ...
          const cid = (row['CID'] || '').trim();

          // For MVA, use phone as lookup key; for Rideshare use CID
          const phone = (row['Phone'] || '').replace(/\D/g, '').trim() || null;

          if (isMVA && !phone) { skipped++; continue; }
          if (!isMVA && !cid)  { skipped++; continue; }

          // Status column differs by sheet
          const nldStatus    = isMVA
            ? (row['Status'] || '').trim() || null
            : ((row['NLD Status'] || '').trim() || (row['Intake Center Status'] || '').trim() || null);
          const intakeStatus = isMVA ? nldStatus : (row['Intake Center Status'] || '').trim() || null;
          const notes        = (row['Notes'] || '').trim() || null;
          const billableRaw  = (row['Billable'] || '').trim().toLowerCase();
          const billable     = billableRaw === 'yes' ? true : billableRaw === 'no' ? false : null;
          const notesUpdated = (row['Date of Notes Updated'] || '').trim() || null;

          // Determine lead status
          let leadStatus = null;
          const nldLower = (nldStatus || '').toLowerCase();
          if (nldLower.includes('accepted') || nldLower.includes('billable') || nldLower === 'signed' || nldLower.startsWith('signed')) {
            leadStatus = 'forwarded';
          } else if (nldLower.includes('disqualified') || nldLower.includes('rejected')) {
            leadStatus = 'buyer_rejected';
          }

          // Look up lead — by CID for Rideshare, by phone for MVA
          // CRITICAL: always scoped to this sheet's own pubSubs — never match
          // another publisher's lead even if phone/CID happens to coincide
          let lookup;
          if (isMVA) {
            lookup = await client.query(
              `SELECT id, status, buyer_status, raw->>'billable_locked' as locked
               FROM leads WHERE phone = $1 AND campaign = $2 AND publisher_sub = ANY($3) LIMIT 1`,
              [phone, sheet.campaign, sheet.pubSubs]
            );
          } else {
            lookup = await client.query(
              `SELECT id, status, buyer_status, raw->>'billable_locked' as locked
               FROM leads WHERE buyer_intake_id = $1 AND campaign = $2 AND publisher_sub = ANY($3) LIMIT 1`,
              [cid, sheet.campaign, sheet.pubSubs]
            );
          }

          if (!lookup.rows.length) {
            unmatched++;
            continue;
          }

          const lead = lookup.rows[0];

          // Skip locked leads — sheet can never overwrite them
          if (lead.locked === 'true' || lead.locked === true) {
            skipped++;
            console.log(`[KA Sheet Poll] 🔒 Skipping locked lead id=${lead.id}`);
            continue;
          }
          const wasAlreadyBillable = (() => {
            const prev = (lead.buyer_status || '').toLowerCase().trim();
            return prev === 'signed' || prev.startsWith('signed') ||
                   prev.includes('accepted') || prev.includes('billable');
          })();

          // Build raw update patch
          const patch = JSON.stringify({
            ka_sheet_sync: {
              intake_status:  intakeStatus,
              nld_status:     nldStatus,
              notes_updated:  notesUpdated,
              billable:       billable,
              source:         'ka_sheet_poll',
              synced_at:      new Date().toISOString(),
            }
          });

          // Use two separate clean queries to avoid parameter type confusion
          if (leadStatus) {
            await client.query(
              `UPDATE leads SET
                 buyer_status = $1,
                 notes        = COALESCE($2, notes),
                 raw          = raw || $3::jsonb,
                 status       = $4
               WHERE id = $5
                 AND (raw->>'billable_locked') IS DISTINCT FROM 'true'`,
              [nldStatus, notes, patch, leadStatus, lead.id]
            );
          } else {
            await client.query(
              `UPDATE leads SET
                 buyer_status = $1,
                 notes        = COALESCE($2, notes),
                 raw          = raw || $3::jsonb
               WHERE id = $4
                 AND (raw->>'billable_locked') IS DISTINCT FROM 'true'`,
              [nldStatus, notes, patch, lead.id]
            );
          }

          // Fire billable email if this lead just flipped to billable/signed
          const isNowBillable = (() => {
            const cur = (nldStatus || '').toLowerCase().trim();
            return cur === 'signed' || cur.startsWith('signed') ||
                   cur.includes('accepted') || cur.includes('billable');
          })();

          if (isNowBillable && !wasAlreadyBillable) {
            // Fetch full lead details for email
            const fullLead = await client.query(
              `SELECT first_name, last_name, email, phone, campaign, received_at
               FROM leads WHERE id = $1`, [lead.id]
            );
            const fl = fullLead.rows[0] || {};
            const name     = [fl.first_name, fl.last_name].filter(Boolean).join(' ') || 'Unknown';
            const campaign = (fl.campaign || sheet.campaign).toUpperCase();
            const vertical = sheet.name;
            const dateStr  = new Date().toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });

            const emailHtml = `
              <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:520px;margin:0 auto;background:#f4f6fb;padding:32px 20px">
                <div style="background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
                  <div style="background:#0f1c3f;padding:24px 28px">
                    <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.15em;color:rgba(255,255,255,.5);margin-bottom:6px">KRW Marketing Solutions</div>
                    <div style="font-size:22px;font-weight:700;color:#fff">✓ Billable Lead</div>
                    <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">${dateStr}</div>
                  </div>
                  <div style="padding:28px">
                    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px 20px;margin-bottom:20px">
                      <div style="font-size:18px;font-weight:700;color:#15803d;margin-bottom:4px">${name}</div>
                      <div style="font-size:13px;color:#166534">Marked billable — ${nldStatus}</div>
                    </div>
                    <table style="width:100%;border-collapse:collapse;font-size:13px">
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:10px;letter-spacing:.08em;width:40%">Campaign</td>
                        <td style="padding:10px 0;font-weight:600;color:#0f1c3f">${campaign} — ${vertical}</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:10px;letter-spacing:.08em">CID</td>
                        <td style="padding:10px 0;font-family:monospace;color:#475569">${cid}</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:10px;letter-spacing:.08em">Email</td>
                        <td style="padding:10px 0;color:#475569">${fl.email || '—'}</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:10px;letter-spacing:.08em">Phone</td>
                        <td style="padding:10px 0;color:#475569">${fl.phone || '—'}</td>
                      </tr>
                      ${notes ? `<tr>
                        <td style="padding:10px 0;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:10px;letter-spacing:.08em;vertical-align:top">Notes</td>
                        <td style="padding:10px 0;color:#475569;line-height:1.5">${notes}</td>
                      </tr>` : ''}
                    </table>
                  </div>
                  <div style="padding:16px 28px;background:#f9fafb;border-top:1px solid #f1f5f9;font-size:11px;color:#9ca3af;text-align:center">
                    KRW Marketing Solutions · Lead Notification System
                  </div>
                </div>
              </div>`;

            await sendEmailNotification(
              `✓ Billable Lead — ${name} | ${vertical} | ${campaign}`,
              emailHtml
            );
            console.log(`[KA Sheet Poll] 📧 Billable email sent for ${name} | ${campaign} | CID: ${cid}`);
          }

          matched++;
        }

        console.log(`[KA Sheet Poll] ${sheet.name}: ${matched} matched, ${unmatched} unmatched, ${skipped} skipped`);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error(`[KA Sheet Poll] ${sheet.name} error:`, err.message);
    }
  }
}

// Start polling 15 seconds after boot, then every hour
setTimeout(() => {
  pollKALeadsSheet();
  setInterval(pollKALeadsSheet, KA_POLL_INTERVAL_MS);
}, 15000);
// ─── END KEVIN ANTHONY LEADS SHEET POLLER ────────────────────────────────────

// ─── LAIRD LEADS SHEET POLLER ─────────────────────────────────────────────────
// Polls two Google Sheet tabs every hour (Roblox + Rideshare).
// Sheet: 1pT525lw2u2ziFBZwmQnYk02ykEhhFjBp6TIDYhOzx5U
// Flexible column mapping — adapts to whatever columns exist in the sheet.
// Only touches leads with campaign IN ('roblox-mt', 'rideshare-tb')
// AND publisher_sub IN ('KRW-LAIRD-2026-X23', 'KRW-LAIRD-2026-JEM').
// SSDI, FE, and Kevin Anthony leads are NEVER touched.
// New sheet: combined tab with Campaign column to split Roblox/Rideshare
// Columns: First Name, Last Name, Caller ID, Lead Date, Campaign, Status,
//          Amount, Call Date Dispositioned, Invoice Date, Billable (yes/no), Notes

const LAIRD_SHEET_ID = '1SJi0U-Cu7OnP06YIcwKRHgVn01NbDMmrSyFtv3UgBbA';

const LAIRD_SHEETS = [
  {
    name:     'Roblox',
    campaign: 'roblox-mt',
    pub_ids:  ['KRW-LAIRD-2026-X23'],
    url:      `https://docs.google.com/spreadsheets/d/${LAIRD_SHEET_ID}/export?format=csv&gid=0`,
    campaignFilter: 'roblox',
  },
  {
    name:     'Rideshare',
    campaign: 'rideshare-tb',
    pub_ids:  ['KRW-LAIRD-2026-JEM'],
    url:      `https://docs.google.com/spreadsheets/d/${LAIRD_SHEET_ID}/export?format=csv&gid=0`,
    campaignFilter: 'rideshare',
  },
];

const LAIRD_POLL_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

// Flexible column finder — tries multiple common header variations
function findCol(row, ...candidates) {
  for (const c of candidates) {
    const key = Object.keys(row).find(k => k.trim().toLowerCase() === c.toLowerCase());
    if (key && row[key] && row[key].trim()) return row[key].trim();
  }
  return null;
}

async function pollLairdLeadsSheet() {
  for (const sheet of LAIRD_SHEETS) {
    try {
      const csv  = await fetchKASheetCSV(sheet.url);

      // The sheet has metadata rows at top — find the real header row
      // by looking for a row containing key column identifiers
      const rawLines = csv.split('\n').map(l => l.trim()).filter(Boolean);
      let headerIdx = -1;
      for (let i = 0; i < rawLines.length; i++) {
        const lower = rawLines[i].toLowerCase();
        if (lower.includes('first') || lower.includes('caller') || lower.includes('phone') || lower.includes('status')) {
          headerIdx = i;
          break;
        }
      }

      // Log what we found for debugging
      console.log(`[Laird Sheet Poll] ${sheet.name}: total lines ${rawLines.length}, header at ${headerIdx}`);
      if (rawLines.length > 0) console.log(`[Laird Sheet Poll] Line 0: ${rawLines[0].substring(0,80)}`);
      if (rawLines.length > 1) console.log(`[Laird Sheet Poll] Line 1: ${rawLines[1].substring(0,80)}`);
      if (rawLines.length > 2) console.log(`[Laird Sheet Poll] Line 2: ${rawLines[2].substring(0,80)}`);
      if (rawLines.length > 3) console.log(`[Laird Sheet Poll] Line 3: ${rawLines[3].substring(0,80)}`);

      if (headerIdx === -1) {
        console.log(`[Laird Sheet Poll] ${sheet.name}: could not find header row`);
        continue;
      }

      // Rebuild CSV from the real header row onwards
      const cleanCsv = rawLines.slice(headerIdx).join('\n');
      const rows = parseKASheetCSV(cleanCsv);

      if (!rows.length) {
        console.log(`[Laird Sheet Poll] ${sheet.name}: empty sheet`);
        continue;
      }

      const dataRows = rows.filter(r => {
        const phone = (findCol(r, 'caller id', 'caller_id', 'phone') || '').replace(/\D/g,'');
        const name  = findCol(r, 'first name', 'firstname') || '';
        return phone.length >= 10 || name.length > 1;
      });

      if (!dataRows.length) {
        console.log(`[Laird Sheet Poll] ${sheet.name}: no data rows yet`);
        continue;
      }

      console.log(`[Laird Sheet Poll] ${sheet.name}: found ${dataRows.length} data rows`);
      if (dataRows.length > 0) {
        console.log(`[Laird Sheet Poll] Sample keys: ${Object.keys(dataRows[0]).join(' | ')}`);
        console.log(`[Laird Sheet Poll] Sample vals: ${Object.values(dataRows[0]).slice(0,6).join(' | ')}`);
      }

      const client = await pool.connect();
      try {
        let matched = 0, unmatched = 0, skipped = 0;

        for (const row of dataRows) {
          // Filter by Campaign column — each sheet entry processes only its vertical
          const rowCampaign = (findCol(row, 'campaign') || '').toLowerCase();
          console.log(`[Laird Sheet Poll] Row campaign: "${rowCampaign}" | filter: "${sheet.campaignFilter}" | match: ${!sheet.campaignFilter || rowCampaign.includes(sheet.campaignFilter)}`);
          if (sheet.campaignFilter && !rowCampaign.includes(sheet.campaignFilter)) {
            skipped++; continue;
          }

          // Flexible field extraction — handles new sheet column names
          const firstName   = findCol(row, 'first name', 'firstname', 'first');
          const lastName    = findCol(row, 'last name', 'lastname', 'last');
          const phone       = (findCol(row, 'caller id', 'caller_id', 'phone', 'phone number', 'cell') || '').replace(/\D/g, '') || null;
          const state       = findCol(row, 'state', 'st') || null;
          const status      = findCol(row, 'status', 'nld status', 'intake status', 'disposition') || null;
          const notes       = findCol(row, 'notes', 'note', 'comments') || null;
          const cid         = findCol(row, 'cid', 'id', 'lead id', 'leadid') || null;
          const billableRaw = (findCol(row, 'billable (yes/no)', 'billable', 'billed') || '').toLowerCase().trim();
          const billable    = billableRaw === 'yes' ? true : billableRaw === 'no' ? false : null;

          // Skip rows with no identifying info
          if (!phone && !cid && !firstName) { skipped++; continue; }

          // Determine lead status from sheet
          const statusLow = (status || '').toLowerCase();
          let leadStatus = null;
          if (billable === true || statusLow.includes('accepted') || statusLow.includes('billable') || statusLow === 'signed') {
            leadStatus = 'forwarded';
          } else if (statusLow.includes('disqualified') || statusLow.includes('rejected')) {
            leadStatus = 'buyer_rejected';
          }
          // Intake in Process, Call Attempted, Pending — leave status as-is

          // Look up lead — by phone, also check without pub_id filter since some came in untagged
          let lookup;
          if (cid) {
            lookup = await client.query(
              `SELECT id, status, buyer_status, raw->>'billable_locked' as locked
               FROM leads WHERE buyer_intake_id = $1 AND campaign = $2 LIMIT 1`,
              [cid, sheet.campaign]
            );
          } else if (phone) {
            lookup = await client.query(
              `SELECT id, status, buyer_status, raw->>'billable_locked' as locked
               FROM leads WHERE phone = $1 AND campaign = $2
               ORDER BY received_at DESC LIMIT 1`,
              [phone, sheet.campaign]
            );
          } else {
            unmatched++; continue;
          }

          if (!lookup.rows.length) { unmatched++; continue; }

          const lead = lookup.rows[0];

          // Respect billable lock
          if (lead.locked === 'true' || lead.locked === true) {
            skipped++;
            continue;
          }

          const patch = JSON.stringify({
            laird_sheet_sync: {
              status:    status,
              billable:  billable,
              notes:     notes,
              source:    'laird_sheet_poll',
              synced_at: new Date().toISOString(),
            }
          });

          // Check if newly billable for email notification
          const wasAlreadyBillable = (() => {
            const prev = (lead.buyer_status || '').toLowerCase().trim();
            return prev === 'signed' || prev.startsWith('signed') ||
                   prev.includes('accepted') || prev.includes('billable');
          })();

          if (leadStatus) {
            await client.query(
              `UPDATE leads SET
                 buyer_status = $1,
                 notes        = COALESCE($2, notes),
                 raw          = raw || $3::jsonb,
                 status       = $4
               WHERE id = $5
                 AND (raw->>'billable_locked') IS DISTINCT FROM 'true'`,
              [status, notes || null, patch, leadStatus, lead.id]
            );
          } else {
            await client.query(
              `UPDATE leads SET
                 buyer_status = $1,
                 notes        = COALESCE($2, notes),
                 raw          = raw || $3::jsonb
               WHERE id = $4
                 AND (raw->>'billable_locked') IS DISTINCT FROM 'true'`,
              [status, notes || null, patch, lead.id]
            );
          }

          // Fire billable email if newly flipped
          const isNowBillable = (() => {
            const cur = (status || '').toLowerCase().trim();
            return cur === 'signed' || cur.startsWith('signed') ||
                   cur.includes('accepted') || cur.includes('billable');
          })();

          if (isNowBillable && !wasAlreadyBillable) {
            const name     = [firstName, lastName].filter(Boolean).join(' ') || 'Unknown';
            const campaign = sheet.campaign.toUpperCase();
            const vertical = sheet.name;
            const dateStr  = new Date().toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });

            const emailHtml = `
              <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:520px;margin:0 auto;background:#f4f6fb;padding:32px 20px">
                <div style="background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
                  <div style="background:#0f1c3f;padding:24px 28px">
                    <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.15em;color:rgba(255,255,255,.5);margin-bottom:6px">KRW Marketing Solutions</div>
                    <div style="font-size:22px;font-weight:700;color:#fff">✓ Billable Lead</div>
                    <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">${dateStr}</div>
                  </div>
                  <div style="padding:28px">
                    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px 20px;margin-bottom:20px">
                      <div style="font-size:18px;font-weight:700;color:#15803d;margin-bottom:4px">${name}</div>
                      <div style="font-size:13px;color:#166534">Marked billable — ${status}</div>
                    </div>
                    <table style="width:100%;border-collapse:collapse;font-size:13px">
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;width:38%">Publisher</td>
                        <td style="padding:10px 0;font-weight:600;color:#0f1c3f">Laird</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Campaign</td>
                        <td style="padding:10px 0;font-weight:600;color:#0f1c3f">${campaign} — ${vertical}</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Phone</td>
                        <td style="padding:10px 0;color:#475569">${phone || '—'}</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">State</td>
                        <td style="padding:10px 0;color:#475569">${state || '—'}</td>
                      </tr>
                      ${notes ? `<tr><td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;vertical-align:top">Notes</td><td style="padding:10px 0;color:#475569;line-height:1.5">${notes}</td></tr>` : ''}
                    </table>
                  </div>
                  <div style="padding:16px 28px;background:#f9fafb;border-top:1px solid #f1f5f9;font-size:11px;color:#9ca3af;text-align:center">KRW Marketing Solutions · Lead Notification System</div>
                </div>
              </div>`;

            await sendEmailNotification(
              `✓ Billable Lead — ${name} | ${vertical} | ${campaign} | Laird`,
              emailHtml
            );
          }

          matched++;
        }

        console.log(`[Laird Sheet Poll] ${sheet.name}: ${matched} matched, ${unmatched} unmatched, ${skipped} skipped`);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error(`[Laird Sheet Poll] ${sheet.name} error:`, err.message);
    }
  }
}

// Start polling 30 seconds after boot (offset from KA poller), then every hour
setTimeout(() => {
  pollLairdLeadsSheet();
  setInterval(pollLairdLeadsSheet, LAIRD_POLL_INTERVAL_MS);
}, 30000);
// ── Manual Laird sheet poll trigger ──────────────────────────────────────────
app.get('/debug-poll-laird', requireKey, async (req, res) => {
  try {
    await pollLairdLeadsSheet();
    res.json({ ok: true, message: 'Laird sheet poll complete — check logs for details' });
  } catch(err) {
    res.json({ ok: false, error: err.message });
  }
});
// ─────────────────────────────────────────────────────────────────────────────

// ─── JOSHUA DURAN SSDI CALLS SHEET POLLER ────────────────────────────────────
// Polls Lead Tree / Forge SSDI Google Sheet every hour.
// Sheet: 1ouur8pCxP8pnyc1lyqlUqdsUF4S0mGFj2sq0dVXH91w
// Tabs: DEALS (billable/signed) and DISPOS (all dispositions)
// Match by: caller_primary_phone (strip leading 1) against calls.caller_id
// Signed = YES when both retained_date AND filed_date are present
// Updates calls table: billable=true, call_status_label='Signed'
// Fires billable email on new sign

const JOSHUA_SHEET_ID = '1ouur8pCxP8pnyc1lyqlUqdsUF4S0mGFj2sq0dVXH91w';
const JOSHUA_SHEETS = [
  {
    name: 'DEALS',
    url:  `https://docs.google.com/spreadsheets/d/${JOSHUA_SHEET_ID}/gviz/tq?tqx=out:csv&sheet=DEALS`,
  },
  {
    name: 'DISPOS',
    url:  `https://docs.google.com/spreadsheets/d/${JOSHUA_SHEET_ID}/gviz/tq?tqx=out:csv&sheet=DISPOS`,
  },
];
const JOSHUA_POLL_INTERVAL_MS = 60 * 60 * 1000;
const JOSHUA_PUB_ID = 'KRW-JOSHUA-2026-76M';

async function pollJoshuaCallsSheet() {
  for (const sheet of JOSHUA_SHEETS) {
    try {
      const csv = await fetchKASheetCSV(sheet.url);

      // Normalize multiline quoted cells in header — the sheet has:
      // "contacted_date,"\n\nintakecompleted_date"" which breaks column alignment
      // Collapse any quoted fields containing only whitespace/newlines into a single token
      const normalizedCsv = csv.replace(/"[\s\n\r]+([^"]+)"/g, '"$1"');

      const rawLines = normalizedCsv.split('\n').map(l => l.trim()).filter(Boolean);

      // Find real header row
      let headerIdx = -1;
      for (let i = 0; i < rawLines.length; i++) {
        const lower = rawLines[i].toLowerCase();
        if (lower.includes('intake_id') || lower.includes('caller_primary_phone')) {
          headerIdx = i;
          break;
        }
      }
      if (headerIdx === -1) {
        console.log(`[Joshua Sheet Poll] ${sheet.name}: could not find header row — is sheet public?`);
        console.log(`[Joshua Sheet Poll] Line 0: ${(rawLines[0]||'').substring(0,80)}`);
        continue;
      }

      const cleanCsv = rawLines.slice(headerIdx).join('\n');
      const rows = parseKASheetCSV(cleanCsv);

      const dataRows = rows.filter(r => {
        const phone = findCol(r, 'caller_primary_phone', 'phone w/ leading 1') || '';
        return phone.replace(/\D/g, '').length >= 10;
      });

      if (!dataRows.length) {
        console.log(`[Joshua Sheet Poll] ${sheet.name}: no data rows`);
        continue;
      }

      console.log(`[Joshua Sheet Poll] ${sheet.name}: ${dataRows.length} rows to process`);

      const client = await pool.connect();
      try {
        let matched = 0, unmatched = 0;

        for (const row of dataRows) {
          // Strip leading 1 from phone
          let rawPhone = (findCol(row, 'caller_primary_phone') || '').replace(/\D/g, '');
          if (rawPhone.startsWith('1') && rawPhone.length === 11) rawPhone = rawPhone.slice(1);
          if (rawPhone.length !== 10) { unmatched++; continue; }

          // Use direct key scan since findCol requires exact match
          // Sheet columns: retained_date, filed_date — scan all keys for partial match
          function findColLoose(row, ...candidates) {
            for (const c of candidates) {
              const key = Object.keys(row).find(k => k.trim().toLowerCase().includes(c.toLowerCase()));
              if (key !== undefined && row[key] !== undefined && String(row[key]).trim()) return String(row[key]).trim();
            }
            return null;
          }

          const intakeId     = findColLoose(row, 'intake_id', 'intakeid')          || null;
          const retainedDate = findColLoose(row, 'retained_date', 'retained')      || null;
          const filedDate    = findColLoose(row, 'filed_date', 'filed')            || null;
          const intakeDate   = findColLoose(row, 'intake_date', 'intakedate')      || null;
          const age          = findColLoose(row, 'age')                            || null;

          // Debug log to confirm values
          console.log(`[Joshua Sheet Poll] Phone: ${rawPhone} | retained: "${retainedDate}" | filed: "${filedDate}" | keys: ${Object.keys(row).join('|')}`);

          // Signed = retained AND filed both present and non-empty
          const isSigned = !!(retainedDate && retainedDate.trim().length > 0 && filedDate && filedDate.trim().length > 0);

          // Look up call by phone and publisher
          let lookup = await client.query(
            `SELECT id, caller_id, caller_name, billable, call_status_label
             FROM calls
             WHERE caller_id = $1 AND publisher_sub = $2 AND campaign = 'ssdi'
             ORDER BY call_date DESC LIMIT 1`,
            [rawPhone, JOSHUA_PUB_ID]
          );

          // Fallback — campaign-agnostic
          if (!lookup.rows.length) {
            lookup = await client.query(
              `SELECT id, caller_id, caller_name, billable, call_status_label
               FROM calls
               WHERE caller_id = $1 AND publisher_sub = $2
               ORDER BY call_date DESC LIMIT 1`,
              [rawPhone, JOSHUA_PUB_ID]
            );
          }

          if (!lookup.rows.length) {
            // No existing call — create one from sheet data so it shows on portal
            const callerName = findCol(row, 'caller_name', 'name', 'client_name') || null;
            const callDate   = intakeDate ? intakeDate.split('T')[0] : new Date().toISOString().split('T')[0];
            const insert = await client.query(
              `INSERT INTO calls
                 (campaign, caller_id, caller_name, billable, call_status_label,
                  disposition, publisher_sub, source_system, call_date, payout_amount, raw)
               VALUES ('ssdi',$1,$2,$3,$4,$5,$6,'sheet_import',$7::date,$8,$9::jsonb)
               RETURNING id, caller_id, caller_name, billable, call_status_label`,
              [
                rawPhone,
                callerName,
                isSigned,
                isSigned ? 'Signed'      : (retainedDate ? 'Retained'  : 'In Progress'),
                isSigned ? 'Filed'       : (retainedDate ? 'Retained'  : 'Intake'),
                JOSHUA_PUB_ID,
                callDate,
                isSigned ? 400.00 : 0,
                JSON.stringify({
                  joshua_sheet_sync: {
                    intake_id:     intakeId,
                    retained_date: retainedDate || null,
                    filed_date:    filedDate    || null,
                    intake_date:   intakeDate   || null,
                    age:           age          || null,
                    signed:        isSigned,
                    synced_at:     new Date().toISOString(),
                    source:        'joshua_sheet_poll_created',
                  }
                }),
              ]
            );
            lookup = { rows: [insert.rows[0]] };
            console.log(`[Joshua Sheet Poll] ➕ Created call record for ${rawPhone} from sheet`);
          }

          const call = lookup.rows[0];
          const wasAlreadySigned = call.billable === true;

          // Only run UPDATE if this was an existing record (not just created)
          if (call.id && !call.source_system) {
            // Never downgrade a record already marked billable/signed
            if (call.billable === true && !isSigned) {
              console.log(`[Joshua Sheet Poll] Skipping downgrade for ${rawPhone} — already billable`);
              matched++;
              continue;
            }
            await client.query(
              `UPDATE calls SET
                 billable          = $1,
                 call_status_label = $2,
                 disposition       = $3,
                 payout_amount     = $4,
                 raw               = COALESCE(raw, '{}'::jsonb) || $5::jsonb
               WHERE id = $6`,
              [
                isSigned,
                isSigned ? 'Signed' : (retainedDate ? 'Retained' : 'In Progress'),
                isSigned ? 'Filed'  : (retainedDate ? 'Retained' : 'Intake'),
                isSigned ? 400.00 : 0,
                JSON.stringify({
                  joshua_sheet_sync: {
                    intake_id:     intakeId,
                    retained_date: retainedDate || null,
                    filed_date:    filedDate    || null,
                    intake_date:   intakeDate   || null,
                    age:           age          || null,
                    signed:        isSigned,
                    synced_at:     new Date().toISOString(),
                    source:        'joshua_sheet_poll',
                  }
                }),
                call.id,
              ]
            );
          }

          // Fire billable email on newly signed
          if (isSigned && !wasAlreadySigned) {
            const name    = call.caller_name || rawPhone;
            const dateStr = new Date().toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });
            const emailHtml = `
              <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:520px;margin:0 auto;background:#f4f6fb;padding:32px 20px">
                <div style="background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
                  <div style="background:#0f1c3f;padding:24px 28px">
                    <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.15em;color:rgba(255,255,255,.5);margin-bottom:6px">KRW Marketing Solutions</div>
                    <div style="font-size:22px;font-weight:700;color:#fff">✓ Signed SSDI Case</div>
                    <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">${dateStr}</div>
                  </div>
                  <div style="padding:28px">
                    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px 20px;margin-bottom:20px">
                      <div style="font-size:18px;font-weight:700;color:#15803d;margin-bottom:4px">${name}</div>
                      <div style="font-size:13px;color:#166534">Retained & Filed — CPA triggered</div>
                    </div>
                    <table style="width:100%;border-collapse:collapse;font-size:13px">
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;width:38%">Campaign</td>
                        <td style="padding:10px 0;font-weight:600;color:#0f1c3f">SSDI Filed — Campaign 1696</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Publisher</td>
                        <td style="padding:10px 0;color:#475569">Joshua Duran</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Phone</td>
                        <td style="padding:10px 0;color:#475569">${rawPhone}</td>
                      </tr>
                      <tr style="border-bottom:1px solid #f1f5f9">
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Retained</td>
                        <td style="padding:10px 0;color:#475569">${retainedDate || '—'}</td>
                      </tr>
                      <tr>
                        <td style="padding:10px 0;color:#9ca3af;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em">Filed</td>
                        <td style="padding:10px 0;color:#475569">${filedDate || '—'}</td>
                      </tr>
                    </table>
                  </div>
                  <div style="padding:16px 28px;background:#f9fafb;border-top:1px solid #f1f5f9;font-size:11px;color:#9ca3af;text-align:center">
                    KRW Marketing Solutions · Lead Notification System
                  </div>
                </div>
              </div>`;

            try {
              await sendEmailNotification(
                `✓ Signed SSDI Case — ${name} | Retained & Filed | Campaign 1696`,
                emailHtml
              );
              console.log(`[Joshua Sheet Poll] 📧 Billable email sent for ${name}`);
            } catch(emailErr) {
              console.error('[Joshua Sheet Poll] Email error:', emailErr.message);
            }
          }

          matched++;
        }

        console.log(`[Joshua Sheet Poll] ${sheet.name}: ${matched} matched, ${unmatched} unmatched`);
      } finally {
        client.release();
      }
    } catch(err) {
      console.error(`[Joshua Sheet Poll] ${sheet.name} error:`, err.message);
    }
  }
}

// ── Joshua sheet poller PAUSED — re-enable when ready ────────────────────────
// setTimeout(() => {
//   pollJoshuaCallsSheet();
//   setInterval(pollJoshuaCallsSheet, JOSHUA_POLL_INTERVAL_MS);
// }, 60000);

// Manual trigger (still available for testing)
app.get('/debug-poll-joshua', requireKey, async (req, res) => {
  try {
    await pollJoshuaCallsSheet();
    res.json({ ok: true, message: 'Joshua sheet poll complete — check logs' });
  } catch(err) {
    res.json({ ok: false, error: err.message });
  }
});
// ─── END JOSHUA SSDI CALLS SHEET POLLER ──────────────────────────────────────



// ─── NEXUS-7 SSDI DISPOSITION SHEET POLLER ───────────────────────────────────
// Read-only sync: pulls lead disposition status from the buyer's (Email Agency/
// Lisa) Google Sheet into the `calls` table, so it displays on Nexus-7's
// existing portal (calls-based, same login as always — KRW-NEXUS-2026).
// This NEVER sets billable=true or touches payout_amount — billing stays a
// fully manual, explicit action, same as every other publisher in this system.
// The sheet's "Center Code" column is intentionally read and discarded —
// never stored, never surfaced anywhere.
const NEXUS7_SHEET_CSV_URL = 'https://docs.google.com/spreadsheets/d/1WjTRF2Ani3YwRW0d-8hDhNXYVzS5x3gBSuqf0yNXCDI/export?format=csv&gid=0';
const NEXUS7_PUBLISHER_SUB = 'KRW-SSDI-2026-4QM'; // Nexus-7's confirmed real pub_id

function normalizePhone10(raw) {
  const digits = (raw || '').replace(/\D/g, '');
  if (digits.length === 11 && digits[0] === '1') return digits.slice(1);
  return digits;
}

async function pollNexus7SsdiSheet() {
  try {
    const resp = await fetch(NEXUS7_SHEET_CSV_URL);
    if (!resp.ok) {
      console.log(`[Nexus-7 Sheet Poll] Fetch failed: ${resp.status}`);
      return;
    }
    const csv  = await resp.text();
    const rows = parseCSV(csv);
    console.log(`[Nexus-7 Sheet Poll] ${rows.length} rows fetched`);

    for (const row of rows) {
      const phone = normalizePhone10(row['Phone']);
      if (!phone) continue;

      const firstName   = (row['First Name'] || '').trim();
      const lastName    = (row['Last Name']  || '').trim();
      const leadStatus  = (row['Lead Status'] || '').trim();
      const reason      = (row['Reason Disqualified/Sub-Status'] || '').trim();
      const trustedForm = (row['Trusted Form URL'] || '').trim();
      const createDate  = (row['Create Date'] || '').trim();
      // NOTE: row['Center Code'] is intentionally never read into anything stored.

      const syncData = {
        nexus7_sheet_sync: {
          source: 'buyer_sheet_import',
          lead_status: leadStatus,
          reason: reason || null,
          synced_at: new Date().toISOString(),
        }
      };

      const existing = await pool.query(
        `SELECT id, billable FROM calls WHERE caller_id=$1 AND publisher_sub=$2 LIMIT 1`,
        [phone, NEXUS7_PUBLISHER_SUB]
      );

      if (existing.rows.length) {
        // Update disposition only — never touch billable/payout_amount on existing rows
        await pool.query(
          `UPDATE calls
           SET disposition=$1, call_status_label=$2, raw = COALESCE(raw,'{}'::jsonb) || $3::jsonb
           WHERE id=$4`,
          [reason || leadStatus, leadStatus, JSON.stringify(syncData), existing.rows[0].id]
        );
      } else {
        // New row — insert with billable explicitly false; nothing here is a billing action
        await pool.query(
          `INSERT INTO calls
             (call_date, caller_id, caller_name, publisher_sub, campaign, buyer_name,
              disposition, call_status_label, billable, source_system, raw)
           VALUES ($1,$2,$3,$4,'ssdi','Lissa (SSDI buyer)',$5,$6,false,'sheet_import',$7)`,
          [createDate || null, phone, `${firstName} ${lastName}`.trim(), NEXUS7_PUBLISHER_SUB,
           reason || leadStatus, leadStatus, JSON.stringify(syncData)]
        );
      }
    }
    console.log('[Nexus-7 Sheet Poll] Sync complete');
  } catch (err) {
    console.log('[Nexus-7 Sheet Poll] Error:', err.message);
  }
}

// Schedule: once daily at 12:00 PST, resilient to server restarts.
// Uses the database itself (not an in-memory variable) as the source of truth
// for "did today's sync already run" — an in-memory flag resets on every
// Railway restart/deploy, which was silently causing missed days.
function todayPSTDateString() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Los_Angeles' })).toISOString().slice(0, 10);
}

async function nexus7LastSyncDatePST() {
  const result = await pool.query(
    `SELECT MAX((raw->'nexus7_sheet_sync'->>'synced_at')::timestamptz) as last_sync
     FROM calls WHERE publisher_sub=$1 AND raw ? 'nexus7_sheet_sync'`,
    [NEXUS7_PUBLISHER_SUB]
  );
  const lastSync = result.rows[0].last_sync;
  if (!lastSync) return null;
  return new Date(new Date(lastSync).toLocaleString('en-US', { timeZone: 'America/Los_Angeles' })).toISOString().slice(0, 10);
}

// Regular check every 15 min — checks any time of day, not just at noon.
// Previously this only checked during the 12:00-12:15 PST window, which meant
// any gap (Railway sleep/wake, missed window, etc.) could go undetected for
// up to 24h. Checking constantly is more robust: it self-heals within 15
// minutes of any gap, regardless of what caused it.
setInterval(async () => {
  try {
    const lastSync = await nexus7LastSyncDatePST();
    if (lastSync !== todayPSTDateString()) {
      console.log('[Nexus-7 Sheet Poll] Periodic check — not yet synced today, running now');
      await pollNexus7SsdiSheet();
    }
  } catch (err) {
    console.log('[Nexus-7 Sheet Poll] Scheduled check error:', err.message);
  }
}, 15 * 60 * 1000);

// Startup catch-up — runs immediately if today's sync is missing, regardless of
// current time. This is what actually fixes missed days: if the server was
// restarted (deploy, crash, etc.) and the 12:00 PST window passed unattended,
// this catches it up the moment the server comes back online instead of
// silently waiting up to 24h for the next scheduled window.
(async () => {
  try {
    const lastSync = await nexus7LastSyncDatePST();
    const today = todayPSTDateString();
    if (lastSync !== today) {
      console.log(`[Nexus-7 Sheet Poll] Startup check — last synced ${lastSync || 'never'}, today is ${today} — catching up now`);
      await pollNexus7SsdiSheet();
    } else {
      console.log('[Nexus-7 Sheet Poll] Startup check — already synced today, skipping');
    }
  } catch (err) {
    console.log('[Nexus-7 Sheet Poll] Startup check error:', err.message);
  }
})();

// Manual trigger for testing — same pattern as the other sheet pollers
app.get('/debug-poll-nexus7', requireKey, async (req, res) => {
  try {
    await pollNexus7SsdiSheet();
    res.json({ ok: true, message: 'Nexus-7 sheet poll complete — check logs' });
  } catch(err) {
    res.json({ ok: false, error: err.message });
  }
});
// ─── END NEXUS-7 SSDI DISPOSITION SHEET POLLER ───────────────────────────────


// ─── FUNNEL DASHBOARD DATA ──────────────────────────────────────────────────
// Powers the new "Funnel" visualization page. Aggregates real lead counts by
// publisher and buyer for MVA and SSDI separately. Admin-only (own dashboard
// use), so real publisher/buyer names are fine here - this is not a
// publisher-facing surface.
app.get('/dashboard/funnel', requireKey, async (req, res) => {
  const period = req.query.period || 'week';
  let sinceClause;

  // Custom range takes priority when both from/to are given and valid.
  // Dates are parsed and validated before ever touching the query string,
  // rather than interpolating req.query values directly.
  const fromRaw = req.query.from;
  const toRaw   = req.query.to;
  let customFrom = fromRaw ? new Date(fromRaw + 'T00:00:00Z') : null;
  let customTo   = toRaw   ? new Date(toRaw + 'T23:59:59Z')   : null;
  if (customFrom && isNaN(customFrom.getTime())) customFrom = null;
  if (customTo && isNaN(customTo.getTime())) customTo = null;

  if (customFrom || customTo) {
    const parts = [];
    if (customFrom) parts.push(`received_at >= '${customFrom.toISOString()}'`);
    if (customTo)   parts.push(`received_at <= '${customTo.toISOString()}'`);
    sinceClause = parts.join(' AND ');
  }
  else if (period === 'today')      sinceClause = "(received_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date";
  else if (period === 'month') sinceClause = "received_at >= (date_trunc('month', (NOW() AT TIME ZONE 'America/New_York')) AT TIME ZONE 'America/New_York')";
  else                         sinceClause = "received_at >= (date_trunc('week', (NOW() AT TIME ZONE 'America/New_York')) AT TIME ZONE 'America/New_York')"; // default: this week

  try {
    // ── MVA ──────────────────────────────────────────────────────────────
    // Inbounds.com (KRW-MVA-2026-8RT) removed from the funnel and Leadbloom
    // (KRW's own brand) shown in its place (Kyler, Sep 16). LEADBLOOM_PUB_ID
    // must match the publisher_sub Leadbloom's forms post with - until it
    // does, the node shows 0.
    const LEADBLOOM_PUB_ID = process.env.LEADBLOOM_PUB_ID || 'KRW-LEADBLOOM-MVA';
    const mvaPubs = {
      'KRW-KANTHONY-RS': 'Kevin Anthony (CPA)',
      [LEADBLOOM_PUB_ID]: 'Leadbloom',
      'KRW-NYC-MVA': 'Lumrah LLC',
    };
    const mvaRows = await pool.query(
      `SELECT publisher_sub, status, billable, revenue, phone,
              raw->>'buyer_name' as buyer_name
       FROM leads
       WHERE campaign IN ('mva-funnel','mva-nyc-split')
         AND publisher_sub = ANY($1::text[]) AND COALESCE(raw->>'excluded','') <> 'true'
         AND ${sinceClause}`,
      [Object.keys(mvaPubs)]
    );

    const mva = { publishers: {}, buyers: {} };
    for (const pubId of Object.keys(mvaPubs)) {
      mva.publishers[pubId] = { name: mvaPubs[pubId], received: 0, forwarded: 0, accepted: 0, revenue: 0 };
    }
    // Always show the full known buyer set, even at zero volume - the frontend
    // needs every buyer node to exist so it can draw the correct structural
    // connections from each publisher, not just the buyers that happened to
    // receive traffic this specific period.
    // Email Agency and LAR-MVA-CPA dropped as MVA buyers; CH-Intake (was
    // "MVA-Intake") and LT-Intake (new intake buyer, posting spec pending)
    // added (Kyler, Sep 16).
    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'LT-Intake'];
    for (const b of mvaKnownBuyers) mva.buyers[b] = { received: 0, accepted: 0, revenue: 0 };
    // De-dupe by phone per publisher for display purposes only (Kyler, Sep 1)
    // - a real duplicate-dial issue was found inflating raw counts. This
    // never touches the leads table or intake - purely how this dashboard
    // endpoint counts for display, so the numbers Kyler looks at are accurate.
    const mvaSeenPhones = {};
    for (const row of mvaRows.rows) {
      const p = mva.publishers[row.publisher_sub];
      if (!p) continue;
      if (!mvaSeenPhones[row.publisher_sub]) mvaSeenPhones[row.publisher_sub] = new Set();
      const seen = mvaSeenPhones[row.publisher_sub];
      if (row.phone && seen.has(row.phone)) continue; // duplicate - skip entirely
      if (row.phone) seen.add(row.phone);

      p.received++;
      if (row.status !== 'rejected') p.forwarded++; // 'rejected' = blocked before reaching any buyer (e.g. CA/CO)
      if (row.billable) { p.accepted++; p.revenue += parseFloat(row.revenue || 0); }

      const buyerName = row.buyer_name || 'Unknown';
      if (row.status !== 'rejected') {
        if (!mva.buyers[buyerName]) mva.buyers[buyerName] = { received: 0, accepted: 0, revenue: 0 };
        mva.buyers[buyerName].received++;
        if (row.billable) { mva.buyers[buyerName].accepted++; mva.buyers[buyerName].revenue += parseFloat(row.revenue || 0); }
      }
    }

    // ── SSDI ─────────────────────────────────────────────────────────────
    // These are dedicated 1:1 endpoints (publisher -> single buyer), so the
    // buyer is known directly from which campaign the lead came through,
    // no buyer_name lookup needed.
    const ssdiPubs = {
      'SSDI-AZ-1696':      { name: 'Joshua Duran (AZ-1696)',    buyer: 'Calltoffic 1696' },
      'KRW-JOSHUA-SIGNED': { name: 'Joshua Duran (Signed)',      buyer: 'Signed (TD)' },
      'SSDI-SLC-1696':     { name: 'Grow My Firm Online (SLC)',  buyer: 'Calltoffic 1696' },
    };
    const ssdiRows = await pool.query(
      `SELECT publisher_sub, status, billable, revenue, phone
       FROM leads
       WHERE publisher_sub = ANY($1::text[])
         AND ${sinceClause}`,
      [Object.keys(ssdiPubs)]
    );

    // KRW-JOSHUA-SIGNED's real activity lives in the calls table (Trackdrive
    // postback), not leads - unlike the other SSDI publishers, which post
    // lead data directly. Queried separately and merged into the same
    // aggregation below, so the Funnel page actually reflects it.
    const ssdiCallsRows = await pool.query(
      `SELECT publisher_sub, billable, payout_amount AS revenue, caller_id
       FROM calls
       WHERE publisher_sub = 'KRW-JOSHUA-SIGNED'
         AND ${sinceClause}`
    );

    const ssdi = { publishers: {}, buyers: {} };
    for (const pubId of Object.keys(ssdiPubs)) {
      ssdi.publishers[pubId] = { name: ssdiPubs[pubId].name, buyer: ssdiPubs[pubId].buyer, received: 0, forwarded: 0, accepted: 0, revenue: 0 };
    }
    const ssdiKnownBuyers = ['Signed (TD)', 'Calltoffic 1696'];
    for (const b of ssdiKnownBuyers) ssdi.buyers[b] = { received: 0, accepted: 0, revenue: 0 };
    // De-dupe by phone per publisher, display only - same fix and same
    // reasoning as the MVA section above (Kyler, Sep 1).
    const ssdiSeenPhones = {};
    for (const row of ssdiRows.rows) {
      const p = ssdi.publishers[row.publisher_sub];
      if (!p) continue;
      if (!ssdiSeenPhones[row.publisher_sub]) ssdiSeenPhones[row.publisher_sub] = new Set();
      const seen = ssdiSeenPhones[row.publisher_sub];
      if (row.phone && seen.has(row.phone)) continue;
      if (row.phone) seen.add(row.phone);

      p.received++;
      if (row.status !== 'rejected' && row.status !== 'error') p.forwarded++;
      if (row.billable) { p.accepted++; p.revenue += parseFloat(row.revenue || 0); }

      const buyerName = ssdiPubs[row.publisher_sub].buyer;
      if (row.status !== 'rejected' && row.status !== 'error') {
        if (!ssdi.buyers[buyerName]) ssdi.buyers[buyerName] = { received: 0, accepted: 0, revenue: 0 };
        ssdi.buyers[buyerName].received++;
        if (row.billable) { ssdi.buyers[buyerName].accepted++; ssdi.buyers[buyerName].revenue += parseFloat(row.revenue || 0); }
      }
    }
    // Merge in the calls-table rows for Josh's Signed line - every call that
    // was logged at all counts as received+forwarded (no intake-rejection
    // concept for calls the way there is for leads). Same phone-based
    // de-dupe, using caller_id as the equivalent field for calls.
    for (const row of ssdiCallsRows.rows) {
      const p = ssdi.publishers[row.publisher_sub];
      if (!p) continue;
      if (!ssdiSeenPhones[row.publisher_sub]) ssdiSeenPhones[row.publisher_sub] = new Set();
      const seen = ssdiSeenPhones[row.publisher_sub];
      if (row.caller_id && seen.has(row.caller_id)) continue;
      if (row.caller_id) seen.add(row.caller_id);

      p.received++;
      p.forwarded++;
      if (row.billable) { p.accepted++; p.revenue += parseFloat(row.revenue || 0); }

      const buyerName = ssdiPubs[row.publisher_sub].buyer;
      if (!ssdi.buyers[buyerName]) ssdi.buyers[buyerName] = { received: 0, accepted: 0, revenue: 0 };
      ssdi.buyers[buyerName].received++;
      if (row.billable) { ssdi.buyers[buyerName].accepted++; ssdi.buyers[buyerName].revenue += parseFloat(row.revenue || 0); }
    }

    // ── MASS TORT (Roblox / Rideshare) ──────────────────────────────────────
    // New campaigns, both dedicated 1:1 (publisher -> single buyer CH-AD),
    // same structure as the SSDI section above. Two publisher entries under
    // one LA-HI relationship so the Funnel page shows a Roblox tab and a
    // Rideshare tab side by side (Kyler, Sep 10).
    const massTortPubs = {
      'LA-HI-ROBLOX':    { name: 'LA-HI — Roblox',    buyer: 'CH-AD' },
      'LA-HI-RIDESHARE': { name: 'LA-HI — Rideshare', buyer: 'CH-AD' },
    };
    const massTortRows = await pool.query(
      `SELECT publisher_sub, status, billable, revenue, phone
       FROM leads
       WHERE publisher_sub = ANY($1::text[])
         AND ${sinceClause}`,
      [Object.keys(massTortPubs)]
    );

    const mass_tort = { publishers: {}, buyers: {} };
    for (const pubId of Object.keys(massTortPubs)) {
      mass_tort.publishers[pubId] = { name: massTortPubs[pubId].name, buyer: massTortPubs[pubId].buyer, received: 0, forwarded: 0, accepted: 0, revenue: 0 };
    }
    mass_tort.buyers['CH-AD'] = { received: 0, accepted: 0, revenue: 0 };
    // De-dupe by phone per publisher, display only - same fix and same
    // reasoning as the MVA/SSDI sections above.
    const massTortSeenPhones = {};
    for (const row of massTortRows.rows) {
      const p = mass_tort.publishers[row.publisher_sub];
      if (!p) continue;
      if (!massTortSeenPhones[row.publisher_sub]) massTortSeenPhones[row.publisher_sub] = new Set();
      const seen = massTortSeenPhones[row.publisher_sub];
      if (row.phone && seen.has(row.phone)) continue;
      if (row.phone) seen.add(row.phone);

      p.received++;
      if (row.status !== 'rejected' && row.status !== 'error') p.forwarded++;
      if (row.billable) { p.accepted++; p.revenue += parseFloat(row.revenue || 0); }

      if (row.status !== 'rejected' && row.status !== 'error') {
        mass_tort.buyers['CH-AD'].received++;
        if (row.billable) { mass_tort.buyers['CH-AD'].accepted++; mass_tort.buyers['CH-AD'].revenue += parseFloat(row.revenue || 0); }
      }
    }

    res.json({
      ok: true,
      period,
      mva,
      ssdi,
      mass_tort,
      routing: {
        // Kevin and Leadbloom share the NLD -> MVA-003-LT waterfall.
        // Lumrah LLC (Noah) is isolated: NLD (12/day cap) with overflow to 003.
        // CH-Intake and LT-Intake are drawn as buyer nodes; their live routing
        // is wired separately once Kyler confirms the split (Sep 16).
        'KRW-KANTHONY-RS':  ['NLD CPA', 'MVA-003-LT'],
        [LEADBLOOM_PUB_ID]: ['NLD CPA', 'MVA-003-LT'],
        'KRW-NYC-MVA':      ['CH-Intake', 'LT-Intake', 'NLD CPA', 'MVA-003-LT'], // ladder order (Sep 16)
        // SSDI lines are dedicated 1:1 - each publisher only ever reaches its one buyer.
        'SSDI-AZ-1696':      ['Calltoffic 1696'],
        'KRW-JOSHUA-SIGNED': ['Signed (TD)'], // Fields Law paused - this line now routes via Trackdrive
        'SSDI-SLC-1696':     ['Calltoffic 1696'],
        // Mass tort lines are also dedicated 1:1, single buyer CH-AD (Sep 10).
        'LA-HI-ROBLOX':      ['CH-AD'],
        'LA-HI-RIDESHARE':   ['CH-AD'],
      },
    });
  } catch (err) {
    console.error('[Funnel Dashboard] Error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});
// ─── END FUNNEL DASHBOARD DATA ──────────────────────────────────────────────

// ─── TRACKDRIVE CALL WEBHOOK — JOSHUA DURAN SIGNED (TD-ROUTED) ────────────────
// This line no longer posts lead data to us at all - a DID is dialed directly,
// routed through the buyer's own Trackdrive account. This webhook is how we
// find out a call happened at all: Trackdrive notifies us once the call
// completes. Creates a new call record from scratch (nothing exists to
// update beforehand, since we never saw the lead). Never auto-billable -
// same policy as everywhere else (Kyler, Aug 28/31) - billing confirmation
// always comes later, manually or via a separate postback.
const TRACKDRIVE_WEBHOOK_KEY = 'td_wh_9f3ac7e21b8d4f0a9c6e2b1d7a4f8e35';
const RINGFUEL_CALL_WEBHOOK_KEY = 'rfc_wh_49157237ba620b79118b1ecb63c1f078';

app.post('/calls/trackdrive-webhook/joshua-signed', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  if (key !== TRACKDRIVE_WEBHOOK_KEY) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};
  const cid = b.caller_id || b.cid || b.phone || b.ani;
  const durationRaw = b.duration || b.call_duration || b.length;
  const timestampRaw = b.timestamp || b.call_datetime || b.datetime || b.date;

  if (!cid) {
    return res.status(400).json({ ok: false, error: 'Missing required field: caller_id (or cid/phone/ani)' });
  }

  const duration = parseInt(durationRaw, 10) || 0;
  let callDatetime;
  try {
    callDatetime = timestampRaw ? new Date(timestampRaw) : new Date();
    if (isNaN(callDatetime.getTime())) callDatetime = new Date();
  } catch(e) {
    callDatetime = new Date();
  }
  const callDateText = callDatetime.toISOString().slice(0, 10);

  const client = await pool.connect();
  try {
    const insert = await client.query(
      `INSERT INTO calls
         (call_datetime, call_date, caller_id, duration, call_duration,
          publisher_sub, vertical, campaign, campaign_name, buyer_name,
          disposition, call_status, call_status_label, billable,
          source_system, recording_url, raw, received_at)
       VALUES ($1, $2, $3, $4, $4,
               'KRW-JOSHUA-SIGNED', 'SSDI', 'ssdi-signed-td', 'SSDI Signed (TD)', 'TD Signed Buyer',
               'Received', 'Completed', 'pending', false,
               'trackdrive_webhook', $5, $6::jsonb, NOW())
       RETURNING id`,
      [callDatetime.toISOString(), callDateText, cid, duration,
       b.recording_url || null, JSON.stringify(b)]
    );
    const callId = insert.rows[0].id;
    console.log(`[Trackdrive Webhook] ✓ Call logged | CID: ${cid} | Duration: ${duration}s | krw_id: ${callId}`);
    return res.json({ ok: true, result: 'success', message: 'Call logged', krw_id: callId });
  } catch (err) {
    console.error('[Trackdrive Webhook] DB insert error:', err.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }
});
// ─── END TRACKDRIVE CALL WEBHOOK ──────────────────────────────────────────────

// ─── J-SIGNED POSTBACK RECEIVER (R2D3) ────────────────────────────────────────
// New buyer for the Signed line, replacing the paused original one. Unlike
// every other buyer integration, there is no outbound posting step here -
// we never send them anything up front, so there's no front-end lead data
// on our side to match against. This endpoint exists purely to receive
// their postback and create the call record directly from it. Named
// "j-signed" rather than anything buyer- or publisher-identifying, per
// Kyler (Sep 15). Always inserted as billable=false regardless of whatever
// disposition they send - same policy as every other endpoint tonight;
// nothing auto-bills, it lands in the approval queue like everything else.
const J_SIGNED_POSTBACK_KEY = 'jsg_pb_0253fc381d78f9049c521724abb10eab';

app.post('/calls/postback/j-signed', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  if (key !== J_SIGNED_POSTBACK_KEY) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};
  const cid = b.caller_id || b.cid || b.phone || b.ani;
  // Explicit undefined/null checks rather than || chaining - a genuine
  // duration of 0 is falsy in JS, so `b.duration || b.call_duration || ...`
  // would incorrectly treat a real "0" as missing and fall through to the
  // next field, wrongly rejecting a valid (if unusual) 0-second call.
  const durationRaw = (b.duration !== undefined && b.duration !== null) ? b.duration
    : (b.call_duration !== undefined && b.call_duration !== null) ? b.call_duration
    : (b.length !== undefined && b.length !== null) ? b.length
    : undefined;
  const timestampRaw = b.timestamp || b.call_datetime || b.datetime || b.date;

  // Duration is required for this integration - per Kyler (Sep 15), unlike
  // the Trackdrive endpoint above where it's optional and defaults to 0.
  const missing = [];
  if (!cid) missing.push('caller_id');
  if (durationRaw === undefined || durationRaw === null || durationRaw === '') missing.push('duration');
  if (missing.length) {
    return res.status(400).json({ ok: false, error: `Missing required field(s): ${missing.join(', ')}` });
  }
  const duration = parseInt(durationRaw, 10);
  if (isNaN(duration) || duration < 0) {
    return res.status(400).json({ ok: false, error: 'duration must be a non-negative integer (seconds)' });
  }

  let callDatetime;
  try {
    callDatetime = timestampRaw ? new Date(timestampRaw) : new Date();
    if (isNaN(callDatetime.getTime())) callDatetime = new Date();
  } catch(e) {
    callDatetime = new Date();
  }
  const callDateText = callDatetime.toISOString().slice(0, 10);

  const client = await pool.connect();
  try {
    const insert = await client.query(
      `INSERT INTO calls
         (call_datetime, call_date, caller_id, duration, call_duration,
          publisher_sub, vertical, campaign, campaign_name, buyer_name,
          disposition, call_status, call_status_label, billable,
          source_system, recording_url, raw, received_at)
       VALUES ($1, $2, $3, $4, $4,
               'KRW-JOSHUA-SIGNED', 'SSDI', 'ssdi-signed-td', 'SSDI Signed (TD)', 'J-Signed Buyer',
               'Received', 'Completed', 'pending', false,
               'j_signed_postback', $5, $6::jsonb, NOW())
       RETURNING id`,
      [callDatetime.toISOString(), callDateText, cid, duration,
       b.recording_url || null, JSON.stringify(b)]
    );
    const callId = insert.rows[0].id;
    console.log(`[J-Signed Postback] ✓ Call logged | CID: ${cid} | Duration: ${duration}s | krw_id: ${callId}`);
    return res.json({ ok: true, result: 'success', message: 'Call logged', krw_id: callId });
  } catch (err) {
    console.error('[J-Signed Postback] DB insert error:', err.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }
});
// ─── END J-SIGNED POSTBACK RECEIVER ───────────────────────────────────────────

// ─── MVA-INTAKE FORWARDING (manual/test only - NOT wired into live NYC routing yet) ──
// New buyer, built ahead of going live Thursday. Deliberately standalone:
// takes an existing lead's krw_id and forwards it, rather than sitting in
// the live mva-nyc-split flow, so it can be tested with real lead data
// without sending anything to MVA-Intake automatically. Wiring this into
// live NYC traffic is a separate, later step once volume is confirmed
// (Kyler, Sep 15).
//
// Insurance status (both parties) was on MVA-Intake's original field list
// but confirmed optional on their end - deliberately left out of the
// payload entirely rather than hard-coded, per Kyler (Sep 15).
// ─── LT-INTAKE (Lead Tree intake dialer) ────────────────────────────────────
// VICIdial non_agent_api add_lead, sent as GET (Adam, Sep 16). Credentials and
// list come from Railway env so the password never lives in this file:
//   LT_INTAKE_PASS        required - the dialer API password (turns LT-Intake on)
//   LT_INTAKE_USER        default CCSapiUSER
//   LT_INTAKE_BASE        default https://kaizen.phdialer.com/vicidial/non_agent_api.php
//   LT_INTAKE_LIST_ID     default 7010
//   LT_INTAKE_CAMPAIGN_ID optional - only sent if set
//   LT_INTAKE_SOURCE      default KRW
// The dialer answers with plain text: "SUCCESS: add_lead ..." or "ERROR: add_lead ...".
function ltIntakeUrl(b, leadState, leadId) {
  const u = new URL(process.env.LT_INTAKE_BASE || 'https://kaizen.phdialer.com/vicidial/non_agent_api.php');
  const q = u.searchParams;
  const set = (k, v) => { if (v !== undefined && v !== null && String(v).trim() !== '') q.set(k, String(v)); };
  set('function', 'add_lead');
  set('user', process.env.LT_INTAKE_USER || 'CCSapiUSER');
  set('pass', process.env.LT_INTAKE_PASS || '');
  set('phone_number', String(b.phone || '').replace(/\D/g, '').replace(/^1(?=\d{10}$)/, ''));
  set('list_id', process.env.LT_INTAKE_LIST_ID || '7010');
  set('campaign_id', process.env.LT_INTAKE_CAMPAIGN_ID);
  set('source', process.env.LT_INTAKE_SOURCE || 'KRW');
  set('vendor_lead_code', leadId ? 'KRW-' + leadId : undefined);
  set('first_name', b.first_name);
  set('last_name', b.last_name);
  set('address1', b.address && b.address !== '123 Main Street' ? b.address : undefined);
  set('title', leadId === 'TEST' ? 'TEST' : undefined);
  set('city', b.city);
  set('state', leadState);
  set('postal_code', b.zip_code || b.zip);
  set('email', b.email);
  const comments = [
    b.incident_date ? 'Incident ' + b.incident_date : null,
    b.injury || b.physical_injury ? 'Injury: ' + (b.injury || b.physical_injury) : null,
    b.at_fault ? 'At fault: ' + b.at_fault : null,
    b.have_attorney ? 'Attorney: ' + b.have_attorney : null,
    b.doctor_treatment ? 'Treatment: ' + b.doctor_treatment : null,
    b.summary ? b.summary : null,
    b.trustedform_cert_url ? 'TF ' + b.trustedform_cert_url : null,
  ].filter(Boolean).join(' | ').slice(0, 250);
  set('comments', comments);
  set('dnc_check', 'N'); set('add_to_hopper', 'N'); set('hopper_local_call_time_check', 'N');
  set('usacan_areacode_check', 'Y'); set('duplicate_check', 'DUPSYS');
  return u;
}
function httpGetText(u) {
  return new Promise((resolve, reject) => {
    const lib = u.protocol === 'http:' ? require('http') : require('https');
    const req2 = lib.get(u, { timeout: 15000 }, (r) => { let d = ''; r.on('data', c => d += c); r.on('end', () => resolve({ status: r.statusCode, body: d })); });
    req2.on('timeout', () => req2.destroy(new Error('timeout after 15s')));
    req2.on('error', reject);
  });
}
async function sendToLtIntake(b, leadState, leadId) {
  if (!process.env.LT_INTAKE_PASS) throw new Error('LT-Intake not configured (LT_INTAKE_PASS not set)');
  const u = ltIntakeUrl(b, leadState, leadId);
  const r = await httpGetText(u);
  const text = String(r.body || '').trim();
  const accepted = r.status >= 200 && r.status < 300 && /^SUCCESS/i.test(text);
  const masked = u.toString().replace(/pass=[^&]*/, 'pass=****');
  console.log(`[LT-Intake] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} | ${text.slice(0, 120)}`);
  return { accepted, result: { status: accepted ? 'success' : 'error', message: text.slice(0, 300), http: r.status }, url: masked };
}

// Manual / test: POST { krw_id } re-sends an existing lead to LT-Intake and returns what
// the dialer said. Does not touch routing; marks the lead only if the dialer accepted it.
app.post('/leads/forward-to-lt-intake', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [process.env.API_KEY || '64tgzb5ostadx1azjio9crdlduw4vf29', process.env.LEAD_API_KEY || 'krwleads2026secure'];
  if (!validKeys.includes(key)) return res.status(401).json({ ok: false, error: 'Unauthorized' });
  const { krw_id, test, phone } = req.body || {};
  // Test mode: { test: true, phone: "9493952717" } sends a fake lead ("Test" in every
  // text field, Yes/No on the yes/no fields) with vendor_lead_code KRW-TEST. Nothing
  // is written to our database and routing is untouched.
  if (test) {
    const tp = String(phone || '').replace(/\D/g, '');
    if (tp.length !== 10) return res.status(400).json({ ok: false, error: 'test needs a 10-digit phone' });
    const b = { first_name: 'Test', last_name: 'Test', phone: tp, email: 'test@krwmarketingsolutions.com', address: 'Test', city: 'Test',
      zip_code: '75001', incident_date: new Date().toLocaleDateString('en-US'), injury: 'Test', at_fault: 'No', have_attorney: 'No',
      doctor_treatment: 'Yes', summary: 'TEST LEAD - please disregard', trustedform_cert_url: 'Test' };
    try {
      const out = await sendToLtIntake(b, 'TX', 'TEST');
      return res.json({ ok: out.accepted, result: out.result.status, message: out.result.message, test: true, sent_url: out.url });
    } catch (err) { return res.status(500).json({ ok: false, error: err.message }); }
  }
  if (!krw_id) return res.status(400).json({ ok: false, error: 'krw_id required (or test:true with phone)' });
  try {
    const r = await pool.query('SELECT * FROM leads WHERE id=$1', [krw_id]);
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'Lead not found' });
    const lead = r.rows[0], b = Object.assign({}, lead.raw || {}, { first_name: lead.first_name, last_name: lead.last_name, phone: lead.phone, email: lead.email });
    const out = await sendToLtIntake(b, (lead.state || (lead.raw || {}).state || '').toUpperCase().slice(0, 2), lead.id);
    if (out.accepted) {
      await pool.query(`UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('lt_intake_test', jsonb_build_object('sent_at', NOW(), 'response', $1::text)) WHERE id=$2`, [out.result.message, lead.id]);
    }
    res.json({ ok: out.accepted, result: out.result.status, message: out.result.message, krw_id: lead.id, sent_url: out.url });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
// ─── END LT-INTAKE ──────────────────────────────────────────────────────────

app.post('/leads/forward-to-mva-intake', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [
    process.env.API_KEY      || '64tgzb5ostadx1azjio9crdlduw4vf29',
    process.env.LEAD_API_KEY || 'krwleads2026secure',
  ];
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const krwId = req.body && req.body.krw_id;
  if (!krwId) {
    return res.status(400).json({ ok: false, error: 'Missing required field: krw_id' });
  }

  const client = await pool.connect();
  let lead;
  try {
    const result = await client.query(
      `SELECT id, first_name, last_name, phone, email, state, raw, received_at FROM leads WHERE id = $1`,
      [krwId]
    );
    if (!result.rows.length) {
      return res.status(404).json({ ok: false, error: `No lead found for krw_id ${krwId}` });
    }
    lead = result.rows[0];
  } catch (dbErr) {
    console.error('[CH-Intake Forward] DB lookup error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }

  const b = lead.raw || {};
  const consentUrl = b.trustedform_cert_url || b.jornaya_leadid || null;

  const intakePayload = {
    first_name:    lead.first_name,
    last_name:     lead.last_name,
    phone:         lead.phone,
    email:         lead.email,
    zip_code:      b.zip_code || b.zip,
    state:         lead.state,
    incident_date: b.incident_date,
    injury:        b.injury,
    at_fault:      b.at_fault,
    have_attorney: b.have_attorney,
    consent_url:       consentUrl,
    consent_timestamp: lead.received_at,
  };
  Object.keys(intakePayload).forEach(k => { if (intakePayload[k] === undefined || intakePayload[k] === null) delete intakePayload[k]; });

  try {
    const intakeRes = await postJSON('https://hooks.zapier.com/hooks/catch/23024319/4d50uja/', intakePayload);
    console.log(`[CH-Intake Forward] Forwarded krw_id ${krwId} - status ${intakeRes.status}`);
    return res.json({ ok: true, result: 'success', message: 'Lead forwarded to CH-Intake', krw_id: krwId, sent_payload: intakePayload });
  } catch (fwdErr) {
    console.error('[CH-Intake Forward] Forward failed:', fwdErr.message);
    return res.status(502).json({ ok: false, error: 'Failed to forward to CH-Intake', detail: fwdErr.message });
  }
});
// ─── END MVA-INTAKE FORWARDING ────────────────────────────────────────────────
// Store a post that was rejected before routing, so it is visible (dashboard + publisher portal) with the reason.
async function logRejectedPost(campaign, pub, b, why) {
  try {
    const r = await pool.query(
      `INSERT INTO leads (campaign, vertical, first_name, last_name, phone, email, publisher_sub, ip_address, state, zip, status, buyer_status, buyer_error, billable, raw, received_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'rejected','Rejected',$11,false,$12::jsonb,NOW()) RETURNING id`,
      [campaign, /roblox/.test(campaign) ? 'Roblox' : /ride/.test(campaign) ? 'Rideshare' : 'MVA',
       b.first_name || null, b.last_name || null, b.phone || null, b.email || null, pub, b.ip_address || null,
       (b.state || '').toUpperCase().trim() || null, b.zip_code || b.zip || null, why, JSON.stringify(b)]);
    console.log(`[${campaign}] stored rejected post ${r.rows[0].id} (${pub}): ${why}`);
    return r.rows[0].id;
  } catch (e) { console.error('[logRejectedPost]', e.message); return null; }
}

// ─── MVA-INTAKE — LA-HI MVA line (Sep 22) ────────────────────────────────────
// Publisher LA-HI-MVA posts here. Leads go ONLY to the two intake buyers,
// CH-Intake and LT-Intake, split 50/50 on today's accepted count (shared with
// the NYC ladder so the buyers see one even split). Never NLD, never 003.
// A state outside the intake list is stored and HELD, not forwarded, so it
// shows on the dashboard and the publisher's portal as not delivered.
const MVA_INTAKE_PUB    = 'LA-HI-MVA';
const MVA_INTAKE_STATES = ['FL','GA','WI','TX','MI','IN','IL','MN','CO','MO','NE','OK','TN'];

app.post('/leads/mva-intake', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [process.env.API_KEY || '64tgzb5ostadx1azjio9crdlduw4vf29', process.env.LEAD_API_KEY || 'krwleads2026secure'];
  if (validKeys.indexOf(key) < 0) return res.status(401).json({ ok: false, error: 'Invalid API key' });

  const b = req.body || {};
  const leadState = (b.state || '').toUpperCase().trim();
  if (b.ip_address == null || b.ip_address === '') b.ip_address = '8.8.8.8';
  if ((b.injury == null || b.injury === '') && b.physical_injury) b.injury = b.physical_injury;

  const missing = [];
  ['first_name','last_name','phone','email','state','incident_date','injury','at_fault','have_attorney'].forEach(f => { if (b[f] == null || String(b[f]).trim() === '') missing.push(f); });
  if ((b.zip_code == null || b.zip_code === '') && (b.zip == null || b.zip === '')) missing.push('zip_code');
  if ((b.trustedform_cert_url == null || b.trustedform_cert_url === '') && (b.jornaya_leadid == null || b.jornaya_leadid === '')) missing.push('trustedform_cert_url or jornaya_leadid');
  if (missing.length) { const rid = await logRejectedPost('mva-intake', MVA_INTAKE_PUB, b, 'Missing: ' + missing.join(', ')); return res.status(400).json({ ok: false, error: 'Missing required fields', missing, krw_id: rid }); }
  if (String(b.have_attorney).toLowerCase() === 'yes') { const rid = await logRejectedPost('mva-intake', MVA_INTAKE_PUB, b, 'Already represented by an attorney'); return res.status(400).json({ ok: false, result: 'rejected', error: 'Lead already represented by an attorney', krw_id: rid }); }

  const client = await pool.connect();
  let leadId = null;
  try {
    const ins = await client.query(
      `INSERT INTO leads (campaign, vertical, first_name, last_name, phone, email, publisher_sub, ip_address, state, zip, status, raw, received_at)
       VALUES ('mva-intake','MVA',$1,$2,$3,$4,$5,$6,$7,$8,'pending',$9::jsonb,NOW()) RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email, MVA_INTAKE_PUB, b.ip_address, leadState, b.zip_code || b.zip || null, JSON.stringify(b)]);
    leadId = ins.rows[0].id;
  } catch (dbErr) {
    console.error('[MVA-Intake] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally { client.release(); }

  const takesCO = process.env.INTAKE_TAKES_CO === 'true';
  if (MVA_INTAKE_STATES.indexOf(leadState) < 0 || (leadState === 'CO' && takesCO === false)) {
    const why = 'State ' + leadState + ' is not accepted by the intake buyers';
    await pool.query("UPDATE leads SET status='received', buyer_error=$1, billable=false WHERE id=$2", [why, leadId]);
    console.log(`[MVA-Intake] held ${b.first_name} ${b.last_name} | ${leadState} | ${why}`);
    return res.json({ ok: false, result: 'held', message: why, krw_id: leadId });
  }

  // Daily cap for this line (Kyler, Sep 23): counts every MVA lead LA-HI sent today that was routed
  // (state-held leads don't count). Past the cap the lead is stored and held so it still shows on the
  // dashboard and on their portal as not delivered.
  const MVA_INTAKE_DAILY_CAP = parseInt(process.env.MVA_INTAKE_DAILY_CAP || '10', 10);
  const capRes = await pool.query(
    `SELECT COUNT(*)::int AS n FROM leads
     WHERE campaign='mva-intake' AND publisher_sub=$1 AND status IN ('forwarded','buyer_rejected','pending')
       AND id <> $2
       AND (received_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date`,
    [MVA_INTAKE_PUB, leadId]);
  if (capRes.rows[0].n >= MVA_INTAKE_DAILY_CAP) {
    const why = 'Daily cap of ' + MVA_INTAKE_DAILY_CAP + ' MVA leads reached for today';
    await pool.query("UPDATE leads SET status='received', buyer_error=$1, billable=false WHERE id=$2", [why, leadId]);
    console.log(`[MVA-Intake] capped ${b.first_name} ${b.last_name} | ${leadState} | ${why}`);
    return res.json({ ok: false, result: 'held', message: why, krw_id: leadId });
  }

  // 50/50: fewest accepted today goes first; tie goes to whoever did NOT get the last one
  const cnt = await pool.query(
    `SELECT raw->>'buyer_name' AS buyer, COUNT(*)::int AS n, MAX(received_at) AS last_at FROM leads
     WHERE campaign IN ('mva-nyc-split','mva-intake') AND status='forwarded'
       AND (received_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date
     GROUP BY 1`);
  const today = {}, lastAt = {};
  cnt.rows.forEach(r => { today[r.buyer] = r.n; lastAt[r.buyer] = r.last_at ? new Date(r.last_at).getTime() : 0; });
  const ladder = [{ name: 'CH-Intake', enabled: true }, { name: 'LT-Intake', enabled: Boolean(process.env.LT_INTAKE_PASS) }]
    .filter(x => x.enabled)
    .sort((a, c) => ((today[a.name] || 0) - (today[c.name] || 0)) || ((lastAt[a.name] || 0) - (lastAt[c.name] || 0)));

  const strip = o => { Object.keys(o).forEach(k => { if (o[k] === undefined || o[k] === null || o[k] === '') delete o[k]; }); return o; };
  const senders = {
    'CH-Intake': async () => {
      const p = strip({ first_name: b.first_name, last_name: b.last_name, phone: String(b.phone).replace(/\D/g, ''), email: b.email,
        zip_code: b.zip_code || b.zip, state: leadState, incident_date: b.incident_date, injury: b.injury, at_fault: b.at_fault,
        have_attorney: b.have_attorney, consent_url: b.trustedform_cert_url || b.jornaya_leadid, consent_timestamp: new Date().toISOString() });
      const r = await postJSON('https://hooks.zapier.com/hooks/catch/23024319/4d50uja/', p);
      let out; try { out = JSON.parse(r.body); } catch (e) { out = { status: r.status, raw: r.body }; }
      return { result: out, accepted: out.status === 'success' || (r.status >= 200 && r.status < 300) };
    },
    'LT-Intake': async () => { const r = await sendToLtIntake(b, leadState, leadId); return { result: r.result, accepted: r.accepted }; },
  };

  const attempts = []; let buyerName = null, result = {}, accepted = false;
  for (const buyer of ladder) {
    try {
      const out = await senders[buyer.name]();
      attempts.push({ buyer: buyer.name, accepted: out.accepted, response: out.result });
      buyerName = buyer.name; result = out.result;
      if (out.accepted) { accepted = true; break; }
    } catch (err) {
      attempts.push({ buyer: buyer.name, accepted: false, error: err.message });
      buyerName = buyer.name; result = { status: 'error', message: err.message };
    }
  }
  await pool.query(
    `UPDATE leads SET status=$1, buyer_status=$2, buyer_response=$3::jsonb, billable=false, revenue=0,
       raw = COALESCE(raw,'{}'::jsonb) || $4::jsonb WHERE id=$5`,
    [accepted ? 'forwarded' : (ladder.length ? 'buyer_rejected' : 'received'), accepted ? 'Accepted' : 'Rejected',
     JSON.stringify({ final: result, attempts }), JSON.stringify({ buyer_name: buyerName, routing_attempts: attempts.map(a => a.buyer + (a.accepted ? ':accepted' : ':rejected')) }), leadId]);
  console.log(`[MVA-Intake] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} | -> ${buyerName || 'nobody'}`);
  return res.json({ ok: accepted, result: accepted ? 'success' : 'rejected', message: accepted ? 'Lead accepted' : (result.message || 'Lead rejected'),
    buyer: buyerName, krw_id: leadId });
});
// ─── END MVA-INTAKE (LA-HI) ──────────────────────────────────────────────────

// ─── MVA-LEADBLOOM2 — LT-Intake only (Sep 23) ────────────────────────────────
// Publisher KRW-LEADBLOOM2-MVA posts here. Every lead goes to LT-Intake and nowhere
// else; if LT rejects it, it is marked rejected (no fallback buyer). States outside
// the intake list are stored and HELD. Requires LT_INTAKE_PASS on Railway.
const LB2_PUB    = 'KRW-LEADBLOOM2-MVA';
const LB2_STATES = ['FL','GA','WI','TX','MI','IN','IL','MN','CO','MO','NE','OK','TN'];

app.post('/leads/mva-leadbloom2', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [process.env.API_KEY || '64tgzb5ostadx1azjio9crdlduw4vf29', process.env.LEAD_API_KEY || 'krwleads2026secure'];
  if (validKeys.indexOf(key) < 0) return res.status(401).json({ ok: false, error: 'Invalid API key' });

  const b = req.body || {};
  const leadState = (b.state || '').toUpperCase().trim();
  if (b.ip_address == null || b.ip_address === '') b.ip_address = '8.8.8.8';
  if ((b.injury == null || b.injury === '') && b.physical_injury) b.injury = b.physical_injury;

  const missing = [];
  ['first_name','last_name','phone','email','state','incident_date','injury','at_fault','have_attorney'].forEach(f => { if (b[f] == null || String(b[f]).trim() === '') missing.push(f); });
  if ((b.zip_code == null || b.zip_code === '') && (b.zip == null || b.zip === '')) missing.push('zip_code');
  if ((b.trustedform_cert_url == null || b.trustedform_cert_url === '') && (b.jornaya_leadid == null || b.jornaya_leadid === '')) missing.push('trustedform_cert_url or jornaya_leadid');
  if (missing.length) { const rid = await logRejectedPost('mva-leadbloom2', LB2_PUB, b, 'Missing: ' + missing.join(', ')); return res.status(400).json({ ok: false, error: 'Missing required fields', missing, krw_id: rid }); }
  if (String(b.have_attorney).toLowerCase() === 'yes') { const rid = await logRejectedPost('mva-leadbloom2', LB2_PUB, b, 'Already represented by an attorney'); return res.status(400).json({ ok: false, result: 'rejected', error: 'Lead already represented by an attorney', krw_id: rid }); }

  let leadId = null;
  try {
    const ins = await pool.query(
      `INSERT INTO leads (campaign, vertical, first_name, last_name, phone, email, publisher_sub, ip_address, state, zip, status, raw, received_at)
       VALUES ('mva-leadbloom2','MVA',$1,$2,$3,$4,$5,$6,$7,$8,'pending',$9::jsonb,NOW()) RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email, LB2_PUB, b.ip_address, leadState, b.zip_code || b.zip || null, JSON.stringify(b)]);
    leadId = ins.rows[0].id;
  } catch (dbErr) {
    console.error('[Leadbloom2] DB insert error:', dbErr.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  }

  const takesCO = process.env.INTAKE_TAKES_CO === 'true';
  if (LB2_STATES.indexOf(leadState) < 0 || (leadState === 'CO' && takesCO === false)) {
    const why = 'State ' + leadState + ' is not accepted on this line';
    await pool.query("UPDATE leads SET status='received', buyer_error=$1, billable=false WHERE id=$2", [why, leadId]);
    console.log(`[Leadbloom2] held ${b.first_name} ${b.last_name} | ${leadState} | ${why}`);
    return res.json({ ok: false, result: 'held', message: why, krw_id: leadId });
  }
  if (Boolean(process.env.LT_INTAKE_PASS) === false) {
    const why = 'Buyer not configured (LT_INTAKE_PASS)';
    await pool.query("UPDATE leads SET status='received', buyer_error=$1 WHERE id=$2", [why, leadId]);
    return res.json({ ok: false, result: 'held', message: why, krw_id: leadId });
  }

  let accepted = false, result = {};
  try {
    const r = await sendToLtIntake(b, leadState, leadId);
    accepted = r.accepted; result = r.result;
  } catch (err) { result = { status: 'error', message: err.message }; }
  await pool.query(
    `UPDATE leads SET status=$1, buyer_status=$2, buyer_response=$3::jsonb, billable=false, revenue=0,
       raw = COALESCE(raw,'{}'::jsonb) || $4::jsonb WHERE id=$5`,
    [accepted ? 'forwarded' : 'buyer_rejected', accepted ? 'Accepted' : 'Rejected', JSON.stringify({ final: result }),
     JSON.stringify({ buyer_name: 'LT-Intake', routing_attempts: ['LT-Intake:' + (accepted ? 'accepted' : 'rejected')] }), leadId]);
  console.log(`[Leadbloom2] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} | -> LT-Intake`);
  return res.json({ ok: accepted, result: accepted ? 'success' : 'rejected', message: accepted ? 'Lead accepted' : (result.message || 'Lead rejected'), buyer: 'LT-Intake', krw_id: leadId });
});
// ─── END MVA-LEADBLOOM2 ──────────────────────────────────────────────────────


// ─── RINGFUEL CALL-COMPLETION WEBHOOK — SSDI 1696 (Filed) ──────────────────
// Fires once a real call hangs up, sending CID/duration/timestamp. This is
// separate from the ping data already flowing through /leads/ssdi-1696 -
// pings measure lead submissions, this measures actual phone calls, which
// were being conflated before and throwing off conversion % (Kyler, Sep 2).
// Ringfuel's DID is shared between AZ-1696 and SLC-1696, so publisher_sub
// is looked up dynamically by matching the caller ID against our own leads
// table - same approach used for the manual historical import.
app.post('/calls/ringfuel-call-webhook', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  if (key !== RINGFUEL_CALL_WEBHOOK_KEY) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};
  const cidRaw = b.caller_id || b.cid || b.phone || b.ani;
  const durationRaw = b.duration || b.call_duration || b.length;
  const timestampRaw = b.timestamp || b.call_datetime || b.datetime || b.date;

  if (!cidRaw) {
    return res.status(400).json({ ok: false, error: 'Missing required field: caller_id (or cid/phone/ani)' });
  }
  const cid = String(cidRaw).replace(/\D/g, '').replace(/^1(?=\d{10}$)/, '');

  const duration = parseInt(durationRaw, 10) || 0;
  let callDatetime;
  try {
    callDatetime = timestampRaw ? new Date(timestampRaw) : new Date();
    if (isNaN(callDatetime.getTime())) callDatetime = new Date();
  } catch(e) {
    callDatetime = new Date();
  }
  const callDateText = callDatetime.toISOString().slice(0, 10);

  const client = await pool.connect();
  try {
    const matchRes = await client.query(
      `SELECT publisher_sub FROM leads
       WHERE phone = $1 AND publisher_sub IN ('SSDI-AZ-1696','SSDI-SLC-1696')
       LIMIT 1`,
      [cid]
    );
    const publisherSub = matchRes.rows[0] ? matchRes.rows[0].publisher_sub : 'SSDI-1696-UNATTRIBUTED';

    const insert = await client.query(
      `INSERT INTO calls
         (call_datetime, call_date, caller_id, duration, call_duration,
          publisher_sub, vertical, campaign, campaign_name, buyer_name,
          disposition, call_status, call_status_label, billable,
          source_system, recording_url, raw, received_at)
       VALUES ($1, $2, $3, $4, $4,
               $5, 'SSDI', 'ssdi-1696', 'SSDI 1696 (Filed)', 'Calltoffic 1696',
               'Received', 'Completed', 'pending', false,
               'ringfuel_webhook', $6, $7::jsonb, NOW())
       RETURNING id`,
      [callDatetime.toISOString(), callDateText, cid, duration,
       publisherSub, b.recording_url || null, JSON.stringify(b)]
    );
    const callId = insert.rows[0].id;
    console.log(`[Ringfuel Call Webhook] ✓ Call logged | CID: ${cid} | Duration: ${duration}s | Publisher: ${publisherSub} | krw_id: ${callId}`);
    return res.json({ ok: true, result: 'success', message: 'Call logged', publisher: publisherSub, krw_id: callId });
  } catch (err) {
    console.error('[Ringfuel Call Webhook] DB insert error:', err.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }
});
// ─── END RINGFUEL CALL-COMPLETION WEBHOOK ───────────────────────────────────

// ─── BILLABLE APPROVAL QUEUE — RINGFUEL/1696 ────────────────────────────────
// Ringfuel marks a call billable on their end and posts here. Nothing is
// ever auto-marked billable in our own leads table from this - it lands in
// a holding queue first. Kyler must explicitly Approve before the billable
// mark (and payout) ever reaches a publisher's portal. "Hold" items never
// touch the leads table at all - fully private, KRW-internal only, never
// exposed to any publisher (Kyler, Sep 1).
const RINGFUEL_BILLABLE_WEBHOOK_KEY = 'rfb_wh_3d8c1a9f42e7b6059ac3d1e4b7f92a68';
const NLD_1696_PUBLISHERS = ['SSDI-AZ-1696', 'SSDI-SLC-1696'];

app.post('/billable-webhook/ringfuel', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  if (key !== RINGFUEL_BILLABLE_WEBHOOK_KEY) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }

  const b = req.body || {};
  const cid = b.cid || b.caller_id || b.phone;
  const amount = parseFloat(b.amount || b.payout || 0) || 0;

  if (!cid) {
    return res.status(400).json({ ok: false, error: 'Missing required field: cid (or caller_id/phone)' });
  }

  const client = await pool.connect();
  try {
    // Match the CID to the most recent 1696 lead, to determine publisher
    const match = await client.query(
      `SELECT id, publisher_sub FROM leads
       WHERE phone = $1 AND publisher_sub = ANY($2::text[])
       ORDER BY received_at DESC LIMIT 1`,
      [String(cid).replace(/\D/g, ''), NLD_1696_PUBLISHERS]
    );
    const leadId = match.rows[0] ? match.rows[0].id : null;
    const publisherSub = match.rows[0] ? match.rows[0].publisher_sub : null;

    const insert = await client.query(
      `INSERT INTO billable_queue (cid, amount, publisher_sub, lead_id, status, raw)
       VALUES ($1, $2, $3, $4, 'pending', $5::jsonb)
       RETURNING id`,
      [cid, amount, publisherSub, leadId, JSON.stringify(b)]
    );
    const queueId = insert.rows[0].id;
    console.log(`[Ringfuel Billable] ✓ Queued for approval | CID: ${cid} | $${amount} | ${publisherSub || 'UNMATCHED'} | queue_id: ${queueId}`);
    sendEmailNotification(
      `New SSDI Billable — $${amount.toFixed(2)} — Needs Approval`,
      `<p>A new 1696 call was marked billable by Ringfuel and is waiting in your approval queue.</p>
       <p><b>CID:</b> ${cid}<br>
       <b>Amount:</b> $${amount.toFixed(2)}<br>
       <b>Publisher:</b> ${publisherSub || 'Unmatched — needs manual review'}</p>
       <p>Nothing is sent to any publisher until you approve it on the dashboard.</p>`
    );
    return res.json({ ok: true, result: 'success', message: 'Queued for approval', krw_id: queueId });
  } catch (err) {
    console.error('[Ringfuel Billable] DB error:', err.message);
    return res.status(500).json({ ok: false, error: 'Database error' });
  } finally {
    client.release();
  }
});

// Admin-only: list current queue (pending/approved/held) with counts
app.get('/billable-queue', requireKey, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT bq.id, bq.cid, bq.amount, bq.publisher_sub, bq.lead_id, bq.status,
              bq.received_at, bq.resolved_at,
              bq.raw->>'source' AS source, bq.raw->>'sheet_date' AS sheet_date,
              bq.raw->>'buyer' AS buyer, bq.raw->>'sheet_status' AS sheet_status, bq.raw->>'sheet_notes' AS sheet_notes,
              l.first_name, l.last_name, l.state, l.vertical, l.campaign, l.phone AS lead_phone
       FROM billable_queue bq
       LEFT JOIN leads l ON l.id = bq.lead_id
       ORDER BY bq.received_at DESC`
    );
    const counts = { pending: 0, approved: 0, held: 0 };
    rows.rows.forEach(r => { if (counts[r.status] !== undefined) counts[r.status]++; });
    res.json({ ok: true, counts, items: rows.rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Admin-only: Approve - marks billable_queue approved AND updates the real
// lead record (billable=true, revenue=amount), which is what the publisher
// portal actually reads from.
app.post('/billable-queue/:id/approve', requireKey, async (req, res) => {
  const client = await pool.connect();
  try {
    const q = await client.query('SELECT * FROM billable_queue WHERE id=$1', [req.params.id]);
    if (!q.rows[0]) return res.status(404).json({ ok: false, error: 'Not found' });
    const item = q.rows[0];
    if (item.status !== 'pending') {
      return res.status(400).json({ ok: false, error: `Already ${item.status}` });
    }

    await client.query(
      "UPDATE billable_queue SET status='approved', resolved_at=NOW() WHERE id=$1",
      [item.id]
    );
    const itemSource = item.raw && typeof item.raw === 'object' ? item.raw.source : null;
    let matchedCallId = null;
    if (item.lead_id && itemSource === 'az_signed_sheet') {
      // Signed is a second payable event on a lead that may already carry a
      // Filed payout from Ringfuel - add to revenue, never overwrite it, and
      // stamp the signed details on the lead's raw JSON (Kyler, Sep 15).
      await client.query(
        `UPDATE leads
         SET billable = true,
             revenue  = COALESCE(revenue, 0) + $1,
             raw      = COALESCE(raw, '{}'::jsonb) || $3::jsonb
         WHERE id = $2`,
        [item.amount, item.lead_id, JSON.stringify({ signed: true, signed_amount: item.amount, signed_date: item.raw.sheet_date || null, signed_approved_at: new Date().toISOString() })]
      );
    } else if (item.lead_id && itemSource === 'buyer_sheet') {
      // Approval is the moment the lead becomes Signed everywhere: portal, postbacks, revenue.
      await client.query(
        `UPDATE leads SET billable=true, revenue=$1, buyer_status='Signed', notes='Signed — retained by buyer',
           raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('buyer_disposition', COALESCE(raw->'buyer_disposition','{}'::jsonb) || jsonb_build_object('status','Signed','note','Signed — retained by buyer','awaiting_approval',false,'approved_at',NOW()))
         WHERE id=$2`,
        [item.amount, item.lead_id]
      );
    } else if (item.lead_id) {
      await client.query(
        'UPDATE leads SET billable=true, revenue=$1 WHERE id=$2',
        [item.amount, item.lead_id]
      );
    } else {
      // No lead_id means this is a calls-based item (1696 or Signed line) -
      // update the matching calls record directly by cid + publisher_sub,
      // which is what the publisher portal actually reads from. Previously
      // this branch didn't exist at all, so approving a calls-based item
      // never touched the real record the portal shows (Kyler, Sep 8).
      // One approval = ONE call. The same number often has several call
      // records (redials), and the old WHERE matched all of them, so one
      // approved item could flag 2-3 calls billable at full payout each.
      // Now only the most recent call for that number is marked (Sep 18).
      const tgt = await client.query(
        `SELECT id FROM calls WHERE caller_id=$1 AND publisher_sub=$2
         ORDER BY (billable IS TRUE) DESC, received_at DESC LIMIT 1`,
        [item.cid, item.publisher_sub]
      );
      if (tgt.rows[0]) {
        matchedCallId = tgt.rows[0].id;
        await client.query(
          "UPDATE calls SET billable=true, payout_amount=$1, call_status_label='cpa' WHERE id=$2",
          [item.amount, matchedCallId]
        );
      } else {
        console.log(`[Billable Queue] ! Approved queue_id ${item.id} but no call found for CID ${item.cid} / ${item.publisher_sub} - nothing will show on a portal`);
      }
    }
    console.log(`[Billable Queue] ✓ Approved | queue_id: ${item.id} | CID: ${item.cid} | $${item.amount}`);
    return res.json({ ok: true, result: 'success', message: 'Approved', call_id: matchedCallId });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    client.release();
  }
});

// Admin-only: Hold - marked internally only, never touches the leads table,
// never visible to any publisher.
app.post('/billable-queue/:id/hold', requireKey, async (req, res) => {
  try {
    const upd = await pool.query(
      "UPDATE billable_queue SET status='held', resolved_at=NOW() WHERE id=$1 AND status='pending' RETURNING id",
      [req.params.id]
    );
    if (!upd.rows[0]) return res.status(400).json({ ok: false, error: 'Not found or already resolved' });
    console.log(`[Billable Queue] Held | queue_id: ${req.params.id}`);
    return res.json({ ok: true, result: 'success', message: 'Held' });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});
// ─── END BILLABLE APPROVAL QUEUE ────────────────────────────────────────────


// ─── AZ-1696 SIGNED SHEET SCANNER — KRW DEALS Google Sheet ──────────────────
// SSDI-AZ-1696 (Joshua Duran) runs both Filed and Signed. Filed billables
// arrive from Ringfuel (/billable-webhook/ringfuel above). Signed dispos are
// written to the "KRW DEALS" Google Sheet at the end of each day instead:
// monthly tabs (SEPTEMBER, OCTOBER, ...), "WEEK SET mm-dd-yyyy" banner rows,
// then Date / CID / Status / Pub. A "Retained" row is a signed case.
//
// This scans that sheet twice a day, 10:00 and 15:00 Pacific, and drops any
// Retained row it hasn't seen before into billable_queue as PENDING - same
// approval box, same Approve/Hold buttons. Nothing is marked billable or
// reaches a publisher portal until Kyler approves it (Kyler, Sep 15).
const AZ_SIGNED_SHEET_ID   = '1XdryadYJw36zE6mctD5vtL5FQeKJXywfvmVqwuc0pN8';
// Tabs are read by gid (the number after gid= in the tab's link). When the
// buyer starts an OCTOBER tab, add its gid: AZ_SIGNED_GIDS env on Railway,
// comma-separated (e.g. "2101881188,123456789"), or edit the default here.
// Old month gids can stay in the list - rows already handled are skipped.
const AZ_SIGNED_GIDS       = (process.env.AZ_SIGNED_GIDS || '2101881188').split(',').map(s => s.trim()).filter(Boolean);
const AZ_SIGNED_PUBLISHER  = 'SSDI-AZ-1696';
const AZ_SIGNED_PAYOUT     = parseFloat(process.env.AZ_SIGNED_PAYOUT || '400') || 400;  // per signed case - CONFIRM with Kyler; 400 mirrors the TD signed line
const AZ_SIGNED_STATUSES   = ['retained', 'signed'];   // Filed rows on this sheet are ignored - Ringfuel owns Filed
const AZ_SIGNED_SINCE      = process.env.AZ_SIGNED_SINCE || '2026-09-01';  // rows dated before this are never queued
const AZ_SIGNED_SCAN_TIMES = ['10:00', '15:00'];       // America/Los_Angeles
const AZ_SIGNED_SOURCE     = 'az_signed_sheet';
const AZ_SIGNED_MONTHS     = ['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER'];
let   azSignedLastScan     = null;   // { at, tabs, rows, retained, queued, skipped, unmatched, errors }
const azSignedRanSlots     = new Set();

// Full CSV parser (quotes, embedded commas/newlines) - the simpler splitters
// elsewhere in this file choke on the sheet's banner rows.
function azParseCSV(text) {
  const rows = []; let row = [], cur = '', q = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (q) { if (ch === '"') { if (text[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += ch; }
    else if (ch === '"') q = true;
    else if (ch === ',') { row.push(cur); cur = ''; }
    else if (ch === '\n') { row.push(cur); rows.push(row); row = []; cur = ''; }
    else if (ch !== '\r') cur += ch;
  }
  if (cur.length || row.length) { row.push(cur); rows.push(row); }
  return rows;
}
function azParseDate(s) {
  s = String(s || '').trim();
  const m = s.match(/^(\d{1,2})[\/.-](\d{1,2})(?:[\/.-](\d{2,4}))?/);
  if (m) { const y = m[3] ? (m[3].length === 2 ? 2000 + parseInt(m[3], 10) : parseInt(m[3], 10)) : new Date().getFullYear(); return `${y}-${String(m[1]).padStart(2,'0')}-${String(m[2]).padStart(2,'0')}`; }
  const t = Date.parse(s); return isNaN(t) ? null : new Date(t).toISOString().slice(0, 10);
}
// Returns [{date, cid, status, pub, tab}] for one tab. Handles repeated
// header rows and WEEK SET banners; column positions come from the header.
function azRowsFromCSV(csv, tab) {
  const rows = azParseCSV(csv), out = [];
  let cols = null;
  for (const r of rows) {
    const up = r.map(c => String(c).trim().toUpperCase());
    // Google's name-based CSV export can fold the rows above the header into
    // it ("SEPTEMBER WEEK SET 08-31-2026 DATE"), so match on how the cell ends.
    const isDate = c => /(^|\s)DATE$/.test(c);
    if (up.indexOf('CID') > -1 && up.some(isDate)) {
      cols = {};
      up.forEach((c, j) => { if (isDate(c)) cols.DATE = j; else if (c === 'CID') cols.CID = j; else if (/STATUS$/.test(c)) cols.STATUS = j; else if (/^PUB/.test(c)) cols.PUB = j; });
      continue;
    }
    if (!cols) continue;
    const date = String(r[cols.DATE] || '').trim();
    const cid  = String(r[cols.CID]  || '').replace(/\D/g, '').replace(/^1(?=\d{10}$)/, '');
    if (!date && !cid) continue;
    if (/^week\s*set/i.test(date)) continue;
    if (cid.length < 10) continue;
    out.push({ date: azParseDate(date), cid, status: String(r[cols.STATUS] || '').trim(), pub: cols.PUB != null ? String(r[cols.PUB] || '').trim() : '', tab });
  }
  return out;
}

async function scanAzSignedSheet(trigger) {
  const now = new Date();
  const summary = { at: now.toISOString(), trigger, tabs: [], rows: 0, retained: 0, queued: 0, skipped: 0, unmatched: 0, errors: [] };
  // current month and the previous one, so month-end rows written late are not missed
  // Only the tab(s) listed by gid. Name-based reads are deliberately NOT used:
  // Google returns the workbook's first tab for an unknown name, and that tab
  // is a different list (Filed/Retained/PUB) that is not this deal (Kyler, Sep 15).
  const targets = AZ_SIGNED_GIDS.map(g => ({ label: 'gid ' + g, q: 'gid=' + g }));
  let all = [];
  summary.debug = [];
  for (const t of targets) {
    try {
      const url = t.q.startsWith('gid=')
        ? `https://docs.google.com/spreadsheets/d/${AZ_SIGNED_SHEET_ID}/export?format=csv&${t.q}`
        : `https://docs.google.com/spreadsheets/d/${AZ_SIGNED_SHEET_ID}/gviz/tq?tqx=out:csv&${t.q}`;
      const csv = await fetchKASheetCSV(url);
      summary.debug.push({ tab: t.label, first_line: csv.split('\n')[0].slice(0, 120), bytes: csv.length });
      if (/<html/i.test(csv.slice(0, 300))) continue;
      const rows = azRowsFromCSV(csv, t.label);
      if (!rows.length && !/CID/i.test(csv)) continue;   // no Date/CID header: unknown name fell back to the first tab, ignore
      summary.tabs.push(t.label); all = all.concat(rows);
    } catch (err) { summary.errors.push(`${t.label}: ${err.message}`); }
  }
  // the same tab can be reached by gid and by name - keep one copy of each row
  const seenRow = new Set();
  all = all.filter(r => { const k = r.cid + '|' + r.date + '|' + r.status.toLowerCase(); if (seenRow.has(k)) return false; seenRow.add(k); return true; });
  summary.rows = all.length;
  if (!summary.tabs.length) {
    summary.errors.push('No tab with a Date/CID header was readable. Check the gid in AZ_SIGNED_GIDS and that the sheet is shared as "anyone with the link can view".');
    azSignedLastScan = summary;
    console.log(`[AZ Signed Sheet] ✕ ${summary.errors.join(' | ')}`);
    return summary;
  }

  const signed = all.filter(r => AZ_SIGNED_STATUSES.includes(r.status.toLowerCase()) && r.date && r.date >= AZ_SIGNED_SINCE);
  summary.retained = signed.length;
  const newItems = [];
  const client = await pool.connect();
  try {
    for (const r of signed) {
      // seen before? (any status - approved, held, or still pending - never re-queue)
      const dup = await client.query(
        `SELECT id, status FROM billable_queue WHERE cid = $1 AND raw->>'source' = $2 LIMIT 1`,
        [r.cid, AZ_SIGNED_SOURCE]
      );
      if (dup.rows[0]) { summary.skipped++; continue; }

      // match to the AZ-1696 lead by phone so the queue shows the caller's name
      const match = await client.query(
        `SELECT id, first_name, last_name, state, billable, revenue FROM leads
         WHERE phone = $1 AND publisher_sub = $2
         ORDER BY received_at DESC LIMIT 1`,
        [r.cid, AZ_SIGNED_PUBLISHER]
      );
      const lead = match.rows[0] || null;
      if (!lead) summary.unmatched++;

      const raw = { source: AZ_SIGNED_SOURCE, sheet_tab: r.tab, sheet_date: r.date, sheet_status: r.status, sheet_pub: r.pub,
                    unmatched: !lead, filed_already_billable: !!(lead && lead.billable), scanned_at: now.toISOString(), trigger };
      const ins = await client.query(
        `INSERT INTO billable_queue (cid, amount, publisher_sub, lead_id, status, raw)
         VALUES ($1, $2, $3, $4, 'pending', $5::jsonb) RETURNING id`,
        [r.cid, AZ_SIGNED_PAYOUT, AZ_SIGNED_PUBLISHER, lead ? lead.id : null, JSON.stringify(raw)]
      );
      summary.queued++;
      newItems.push({ queue_id: ins.rows[0].id, cid: r.cid, date: r.date, name: lead ? `${lead.first_name || ''} ${lead.last_name || ''}`.trim() : null, state: lead ? lead.state : null, unmatched: !lead });
      console.log(`[AZ Signed Sheet] ✓ Queued for approval | CID: ${r.cid} | signed ${r.date} | ${lead ? (lead.first_name + ' ' + lead.last_name) : 'NO MATCHING LEAD'} | queue_id: ${ins.rows[0].id}`);
    }
  } catch (err) {
    summary.errors.push(`DB: ${err.message}`);
    console.error('[AZ Signed Sheet] DB error:', err.message);
  } finally {
    client.release();
  }

  azSignedLastScan = summary;
  console.log(`[AZ Signed Sheet] Scan (${trigger}) | tabs: ${summary.tabs.join(',')} | ${summary.rows} rows, ${summary.retained} signed, ${summary.queued} new queued, ${summary.skipped} already handled, ${summary.unmatched} with no matching lead`);

  if (newItems.length) {
    const lines = newItems.map(i => `<tr><td style="padding:4px 10px 4px 0">${i.date}</td><td style="padding:4px 10px 4px 0;font-family:monospace">${i.cid}</td><td style="padding:4px 10px 4px 0">${i.name || '<i>no matching AZ-1696 lead - review</i>'}${i.state ? ' (' + i.state + ')' : ''}</td></tr>`).join('');
    sendEmailNotification(
      `${newItems.length} new Signed SSDI case${newItems.length > 1 ? 's' : ''} from the AZ sheet — $${(newItems.length * AZ_SIGNED_PAYOUT).toFixed(2)} — Needs Approval`,
      `<p>The ${AZ_SIGNED_SCAN_TIMES.includes(trigger) ? trigger + ' Pacific' : trigger} scan of the KRW DEALS sheet found ${newItems.length} new Retained row${newItems.length > 1 ? 's' : ''} for ${AZ_SIGNED_PUBLISHER}. Each is waiting in your approval queue at $${AZ_SIGNED_PAYOUT.toFixed(2)}.</p>
       <table style="border-collapse:collapse;font-size:14px"><tr><th align="left" style="padding:4px 10px 4px 0">Signed</th><th align="left" style="padding:4px 10px 4px 0">CID</th><th align="left" style="padding:4px 10px 4px 0">Lead</th></tr>${lines}</table>
       <p>Nothing is marked billable or sent to any publisher until you approve it on the dashboard.</p>`
    );
  }
  return summary;
}

// Scheduler: checks every 30s against Pacific wall-clock time and runs each
// slot once per day. No cron dependency; survives DST because the timezone
// conversion is done by Intl, not by a fixed UTC offset.
function azPacificHM() {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Los_Angeles', hour12: false, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).formatToParts(new Date());
  const g = t => (parts.find(p => p.type === t) || {}).value;
  return { day: `${g('year')}-${g('month')}-${g('day')}`, hm: `${String(g('hour')).padStart(2, '0').replace('24', '00')}:${g('minute')}` };
}
setInterval(() => {
  const { day, hm } = azPacificHM();
  if (!AZ_SIGNED_SCAN_TIMES.includes(hm)) return;
  const slot = `${day} ${hm}`;
  if (azSignedRanSlots.has(slot)) return;
  azSignedRanSlots.add(slot);
  scanAzSignedSheet(hm).catch(err => console.error('[AZ Signed Sheet] Scheduled scan failed:', err.message));
}, 30 * 1000);

// Admin: run a scan right now (does not affect the schedule) and see the last result
app.post('/az-signed-sheet/scan', requireKey, async (req, res) => {
  try { const s = await scanAzSignedSheet('manual'); res.json({ ok: true, ...s }); }
  catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
app.get('/az-signed-sheet/status', requireKey, (req, res) => {
  res.json({ ok: true, publisher: AZ_SIGNED_PUBLISHER, payout: AZ_SIGNED_PAYOUT, since: AZ_SIGNED_SINCE, gids: AZ_SIGNED_GIDS, scan_times_pacific: AZ_SIGNED_SCAN_TIMES, now_pacific: azPacificHM(), last_scan: azSignedLastScan });
});
// ─── END AZ-1696 SIGNED SHEET SCANNER ───────────────────────────────────────


// ─── MVA PUBLISHER PORTAL v2 + PUBLISHER POSTBACKS (Sep 16) ─────────────────
// 1. /portal/leads         - what the rebuilt MVA portal reads. Returns ONLY
//                            publisher-safe fields (no buyer name, no revenue,
//                            no routing) and computes the publisher's own
//                            payout from their payout_rate. The general
//                            /leads/feed still returns buyer_name/revenue for
//                            the admin dashboard; the portal no longer uses it.
// 2. /portal/postback      - per-publisher postback settings (URL, method,
//                            events, on/off), editable only from their login.
// 3. Poller (every 2 min)  - finds leads whose status/disposition changed since
//                            the publisher was last notified and fires their
//                            postback. Catches every change regardless of how
//                            it was set (buyer postback, sheet, or Kyler's SQL).
//                            Retries up to 5 times; every attempt is logged.
const PB_EVENTS = ['accepted', 'rejected', 'signed', 'disposition_update'];
const PB_POLL_MS = 2 * 60 * 1000;
const PB_MAX_ATTEMPTS = 5;

async function initPortalPostbacks() {
  try {
    await pool.query(`
      ALTER TABLE publishers ADD COLUMN IF NOT EXISTS postback_url     TEXT;
      ALTER TABLE publishers ADD COLUMN IF NOT EXISTS postback_method  TEXT DEFAULT 'POST';
      ALTER TABLE publishers ADD COLUMN IF NOT EXISTS postback_events  JSONB DEFAULT '["accepted","rejected","signed"]'::jsonb;
      ALTER TABLE publishers ADD COLUMN IF NOT EXISTS postback_enabled BOOLEAN DEFAULT false;
      ALTER TABLE publishers ADD COLUMN IF NOT EXISTS postback_since   TIMESTAMPTZ;
      CREATE TABLE IF NOT EXISTS publisher_postback_log (
        id          SERIAL PRIMARY KEY,
        pub_id      TEXT NOT NULL,
        lead_id     INTEGER,
        event       TEXT NOT NULL,
        url         TEXT,
        method      TEXT,
        status_code INTEGER,
        ok          BOOLEAN DEFAULT false,
        response    TEXT,
        attempt     INTEGER DEFAULT 1,
        created_at  TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_pb_log_pub ON publisher_postback_log (pub_id, created_at DESC);
    `);
    console.log('[Portal Postbacks] schema ready');
  } catch (err) { console.error('[Portal Postbacks] schema init failed:', err.message); }
}
initPortalPostbacks();

// Resolve a portal login (portal_id or pub_id) to the publisher record(s) it covers
async function pbResolvePublisher(portalId) {
  const r = await pool.query(
    `SELECT * FROM publishers WHERE (portal_id=$1 OR pub_id=$1) AND active=true ORDER BY (pub_id=$1) DESC LIMIT 1`, [portalId]);
  if (!r.rows.length) return null;
  const pub = r.rows[0];
  const fam = await pool.query(`SELECT pub_id FROM publishers WHERE (portal_id=$1 OR pub_id=$1) AND active=true`, [portalId]);
  pub._pub_ids = fam.rows.map(x => x.pub_id);
  return pub;
}

// Publisher-facing view of a lead. This is the allowlist - nothing else leaves.
function pbLeadView(l, payoutRate) {
  const bs = (l.buyer_status || '').trim();
  const st = l.status || 'received';
  // Publisher-facing wording (Kyler, Sep 16): a lead that went through but has no
  // buyer response yet is "Delivered", never "Accepted" - delivery is not acceptance.
  // "Returned" from the buyer is a rejection. A lead the buyer would not take at all
  // is "Not delivered".
  let response = 'Delivered';
  if ((bs === 'Signed' || bs === 'Retained') && l.billable) response = 'Signed';
  else if (bs === 'Signed' || bs === 'Retained') response = 'In outreach';   // reported by buyer, not yet approved by Kyler
  else if (bs === 'Rejected' || bs === 'Returned' || st === 'buyer_rejected') response = 'Rejected';
  else if (bs === 'Test') response = 'Test';
  else if (/^open/i.test(bs)) response = 'In outreach';
  else if (/^pending/i.test(bs)) response = 'In outreach';
  else if (/^archived/i.test(bs)) response = 'Not worked';
  else if (st === 'rejected' || st === 'error') response = 'Not delivered';
  else response = 'Delivered';
  const raw = l.raw || {};
  const dispo = raw.buyer_disposition || {};
  var camp = String(l.campaign || '').toLowerCase();
  var vertical = /ride|lyft|uber/.test(camp) ? 'Rideshare' : /roblox/.test(camp) ? 'Roblox' : /mva/.test(camp) ? 'MVA' : (l.vertical || 'Other');
  // never surface an internal error message to a publisher
  var safeNotes = l.notes || l.buyer_error || null;
  if (safeNotes && /column .* does not exist|relation .* does not exist|syntax error|ECONNREFUSED|timeout/i.test(safeNotes)) safeNotes = 'Delivery error on our side - being reviewed';
  return {
    id: l.id, received_at: l.received_at, campaign: l.campaign, vertical: vertical,
    first_name: l.first_name, last_name: l.last_name, phone: l.phone, email: l.email, state: l.state,
    zip: raw.zip_code || raw.zip || null, incident_date: raw.incident_date || null,
    injury: raw.injury || raw.physical_injury || null, at_fault: raw.at_fault || null, have_attorney: raw.have_attorney || null,
    case_description: raw.case_description || raw.summary || raw.description || null, county: raw.county || null,
    submitted_status: st, response, notes: safeNotes,
    updated_at: dispo.synced_at || null, billable: !!l.billable,
    payout: l.billable ? parseFloat(payoutRate || 0) : 0,
    trustedform: raw.trustedform_cert_url || null,
  };
}

// 1. Portal lead log (allowlisted)
app.get('/portal/leads', requireKey, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  const portalId = (req.query.portal_id || '').trim();
  if (!portalId) return res.status(400).json({ ok: false, error: 'portal_id required' });
  try {
    const pub = await pbResolvePublisher(portalId);
    if (!pub) return res.status(401).json({ ok: false, error: 'Publisher not found' });
    const days = parseInt(req.query.days || '9999', 10);
    const dayClause = days < 9999 ? `AND received_at >= NOW() - INTERVAL '${days} days'` : '';
    const r = await pool.query(
      `SELECT id, received_at, campaign, first_name, last_name, email, phone, state, status, buyer_status, buyer_error, notes, billable, raw
       FROM leads WHERE publisher_sub = ANY($1::text[]) AND COALESCE(vertical,'') <> 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true' ${req.query.show_hidden ? '' : "AND COALESCE(raw->>'pub_hidden','') <> 'true'"} ${dayClause}
       ORDER BY received_at DESC LIMIT 5000`, [pub._pub_ids]);
    res.json({ ok: true, count: r.rows.length, payout_rate: parseFloat(pub.payout_rate || 0), leads: r.rows.map(l => pbLeadView(l, pub.payout_rate)) });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// 1b. Publisher removes a lead from their own portal (hidden, not deleted - admin still sees it)
async function pbSetHidden(req, res, hidden) {
  const b = req.body || {};
  const pub = await pbResolvePublisher(String(b.portal_id || '').trim()).catch(() => null);
  if (pub == null) return res.status(401).json({ ok: false, error: 'Publisher not found' });
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ ok: false, error: 'Bad lead id' });
  try {
    const r = await pool.query(
      `UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('pub_hidden', $1::boolean, 'pub_hidden_at', NOW())
       WHERE id = $2 AND publisher_sub = ANY($3::text[]) RETURNING id`,
      [hidden, id, pub._pub_ids]);
    if (r.rows.length === 0) return res.status(404).json({ ok: false, error: 'Lead not found on this portal' });
    console.log(`[Portal] ${pub.pub_id} ${hidden ? 'removed' : 'restored'} lead ${id} on their portal`);
    res.json({ ok: true, id: id, hidden: hidden });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
}
app.post('/portal/leads/:id/hide',   requireKey, (req, res) => pbSetHidden(req, res, true));
app.post('/portal/leads/:id/unhide', requireKey, (req, res) => pbSetHidden(req, res, false));

// 2. Postback settings
function pbSettingsView(pub) {
  return { url: pub.postback_url || '', method: (pub.postback_method || 'POST').toUpperCase(),
           events: Array.isArray(pub.postback_events) ? pub.postback_events : PB_EVENTS.slice(0, 3),
           enabled: !!pub.postback_enabled, since: pub.postback_since || null };
}
app.get('/portal/postback', requireKey, async (req, res) => {
  const pub = await pbResolvePublisher((req.query.portal_id || '').trim()).catch(() => null);
  if (!pub) return res.status(401).json({ ok: false, error: 'Publisher not found' });
  res.json({ ok: true, settings: pbSettingsView(pub), sample: pbBuildPayload({ id: 12345, first_name: 'Jane', last_name: 'Doe', phone: '5551234567', email: 'jane@example.com', state: 'TX', received_at: new Date().toISOString(), status: 'forwarded', buyer_status: 'Signed', notes: 'Signed — retained by buyer', billable: true, raw: {} }, 'signed', pub.payout_rate) });
});
app.post('/portal/postback', requireKey, async (req, res) => {
  const b = req.body || {};
  const pub = await pbResolvePublisher((b.portal_id || '').trim()).catch(() => null);
  if (!pub) return res.status(401).json({ ok: false, error: 'Publisher not found' });
  const url = String(b.url || '').trim();
  if (url && !/^https?:\/\/[^\s]+$/i.test(url)) return res.status(400).json({ ok: false, error: 'URL must start with http:// or https://' });
  const method = String(b.method || 'POST').toUpperCase() === 'GET' ? 'GET' : 'POST';
  const events = (Array.isArray(b.events) ? b.events : []).filter(e => PB_EVENTS.includes(e));
  const enabled = !!b.enabled && !!url && events.length > 0;
  try {
    await pool.query(
      `UPDATE publishers SET postback_url=$1, postback_method=$2, postback_events=$3::jsonb, postback_enabled=$4,
         postback_since = CASE WHEN $4 AND postback_since IS NULL THEN NOW() WHEN NOT $4 THEN NULL ELSE postback_since END
       WHERE pub_id = ANY($5::text[])`,
      [url || null, method, JSON.stringify(events), enabled, pub._pub_ids]);
    const fresh = await pbResolvePublisher(pub.pub_id);
    console.log(`[Portal Postbacks] ${pub.pub_id} settings saved: enabled=${enabled} ${method} ${url} events=${events.join(',')}`);
    res.json({ ok: true, settings: pbSettingsView(fresh) });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
app.post('/portal/postback/test', requireKey, async (req, res) => {
  const pub = await pbResolvePublisher(((req.body || {}).portal_id || '').trim()).catch(() => null);
  if (!pub) return res.status(401).json({ ok: false, error: 'Publisher not found' });
  if (!pub.postback_url) return res.status(400).json({ ok: false, error: 'Save a postback URL first' });
  const sample = { id: 0, first_name: 'Test', last_name: 'Lead', phone: '5550000000', email: 'test@example.com', state: 'TX', received_at: new Date().toISOString(), status: 'forwarded', buyer_status: 'Accepted', notes: null, billable: false, raw: {} };
  const payload = Object.assign(pbBuildPayload(sample, 'accepted', pub.payout_rate), { test: true });
  const out = await pbDeliver(pub, payload);
  await pool.query(`INSERT INTO publisher_postback_log (pub_id, lead_id, event, url, method, status_code, ok, response, attempt) VALUES ($1,NULL,'test',$2,$3,$4,$5,$6,1)`,
    [pub.pub_id, pub.postback_url, pub.postback_method || 'POST', out.status, out.ok, (out.body || out.error || '').slice(0, 500)]);
  res.json({ ok: out.ok, status_code: out.status, response: (out.body || out.error || '').slice(0, 500), sent: payload });
});
app.get('/portal/postback/log', requireKey, async (req, res) => {
  const pub = await pbResolvePublisher((req.query.portal_id || '').trim()).catch(() => null);
  if (!pub) return res.status(401).json({ ok: false, error: 'Publisher not found' });
  const r = await pool.query(
    `SELECT l.id, l.lead_id, l.event, l.method, l.status_code, l.ok, l.attempt, l.created_at, ld.first_name, ld.last_name
     FROM publisher_postback_log l LEFT JOIN leads ld ON ld.id = l.lead_id
     WHERE l.pub_id = ANY($1::text[]) ORDER BY l.created_at DESC LIMIT 20`, [pub._pub_ids]);
  res.json({ ok: true, log: r.rows });
});

// 3. Delivery
function pbBuildPayload(l, event, payoutRate) {
  const v = pbLeadView(l, payoutRate);
  return {
    event, krw_id: v.id, first_name: v.first_name, last_name: v.last_name, phone: v.phone, email: v.email, state: v.state,
    submitted_at: v.received_at, status: v.response, reason: v.notes || null, signed: v.response === 'Signed',
    payout: v.payout, updated_at: new Date().toISOString(),
  };
}
function pbDeliver(pub, payload) {
  return new Promise((resolve) => {
    try {
      const method = (pub.postback_method || 'POST').toUpperCase();
      const u = new URL(pub.postback_url);
      if (method === 'GET') Object.keys(payload).forEach(k => { if (payload[k] !== null && payload[k] !== undefined) u.searchParams.set(k, String(payload[k])); });
      const lib = u.protocol === 'http:' ? require('http') : require('https');
      const body = method === 'POST' ? JSON.stringify(payload) : '';
      const req2 = lib.request({ hostname: u.hostname, port: u.port || undefined, path: u.pathname + u.search, method,
        headers: method === 'POST' ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } : {}, timeout: 10000 },
        (r) => { let data = ''; r.on('data', c => data += c); r.on('end', () => resolve({ ok: r.statusCode >= 200 && r.statusCode < 300, status: r.statusCode, body: data })); });
      req2.on('timeout', () => { req2.destroy(new Error('timeout after 10s')); });
      req2.on('error', (e) => resolve({ ok: false, status: null, error: e.message }));
      if (body) req2.write(body);
      req2.end();
    } catch (e) { resolve({ ok: false, status: null, error: e.message }); }
  });
}
function pbEventFor(l) {
  const bs = (l.buyer_status || '').trim(), st = l.status || '';
  if (bs === 'Test') return null;
  if ((bs === 'Signed' || bs === 'Retained') && l.billable) return 'signed';
  if (bs === 'Signed' || bs === 'Retained') return 'disposition_update';
  if (bs === 'Rejected' || bs === 'Returned' || st === 'buyer_rejected') return 'rejected';
  if (bs === 'Accepted' || (st === 'forwarded' && !bs)) return 'accepted';
  if (st === 'rejected' || st === 'error') return 'rejected';
  if (bs) return 'disposition_update';
  return null;
}
async function pbPoll() {
  let pubs;
  try { pubs = await pool.query(`SELECT * FROM publishers WHERE postback_enabled = true AND postback_url IS NOT NULL AND active = true`); }
  catch (e) { return console.error('[Portal Postbacks] poll query failed:', e.message); }
  for (const pub of pubs.rows) {
    const events = Array.isArray(pub.postback_events) ? pub.postback_events : [];
    if (!events.length) continue;
    let rows;
    try {
      rows = await pool.query(
        `SELECT id, received_at, campaign, first_name, last_name, email, phone, state, status, buyer_status, buyer_error, notes, billable, raw
         FROM leads
         WHERE publisher_sub = $1 AND COALESCE(vertical,'') <> 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true'
           AND received_at >= COALESCE($2::timestamptz, NOW())
           AND COALESCE(raw->'pub_postback'->>'key','') IS DISTINCT FROM
               (COALESCE(status,'') || '|' || COALESCE(buyer_status,'') || '|' || COALESCE(raw->'buyer_disposition'->>'synced_at',''))
           AND ( COALESCE(raw->'pub_postback'->>'tried_key','') IS DISTINCT FROM
                 (COALESCE(status,'') || '|' || COALESCE(buyer_status,'') || '|' || COALESCE(raw->'buyer_disposition'->>'synced_at',''))
                 OR COALESCE((raw->'pub_postback'->>'attempts')::int, 0) < $3 )
         ORDER BY received_at ASC LIMIT 50`, [pub.pub_id, pub.postback_since, PB_MAX_ATTEMPTS]);
    } catch (e) { console.error('[Portal Postbacks] lead query failed:', e.message); continue; }
    for (const l of rows.rows) {
      const key = `${l.status || ''}|${l.buyer_status || ''}|${(l.raw && l.raw.buyer_disposition && l.raw.buyer_disposition.synced_at) || ''}`;
      const event = pbEventFor(l);
      const prev = (l.raw && l.raw.pub_postback) || {};
      const attempts = (prev.tried_key === key ? (prev.attempts || 0) : 0) + 1;
      // not subscribed to this event, or nothing to say yet: mark as seen so it isn't re-evaluated every 2 minutes
      if (!event || !events.includes(event)) {
        await pool.query(`UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('pub_postback', jsonb_build_object('key',$1::text,'skipped',true,'at',NOW())) WHERE id=$2::int`, [key, l.id]);
        continue;
      }
      const payload = pbBuildPayload(l, event, pub.payout_rate);
      const out = await pbDeliver(pub, payload);
      await pool.query(`INSERT INTO publisher_postback_log (pub_id, lead_id, event, url, method, status_code, ok, response, attempt) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [pub.pub_id, l.id, event, pub.postback_url, pub.postback_method || 'POST', out.status, out.ok, (out.body || out.error || '').slice(0, 500), attempts]);
      // success: remember the key so this state is never re-sent. failure: leave key unset so it retries,
      // and count attempts against this exact state (a later status change resets the count).
      const mark = out.ok ? { key, event, sent_at: new Date().toISOString(), status_code: out.status }
                          : { key: null, tried_key: key, attempts, event, last_error: out.error || String(out.status), last_try: new Date().toISOString() };
      await pool.query(`UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('pub_postback', $1::jsonb) WHERE id=$2`, [JSON.stringify(mark), l.id]);
      console.log(`[Portal Postbacks] ${out.ok ? '✓' : '✕'} ${pub.pub_id} | lead ${l.id} | ${event} | ${out.status || out.error}${attempts > 1 ? ' | attempt ' + attempts : ''}`);
    }
  }
}
setInterval(() => pbPoll().catch(e => console.error('[Portal Postbacks] poll error:', e.message)), PB_POLL_MS);
// ─── END MVA PUBLISHER PORTAL v2 + POSTBACKS ─────────────────────────────────


// ─── BUYER DISPOSITION SHEETS (Sep 17) ──────────────────────────────────────
// Reads each buyer's Google Sheet as the service account in GOOGLE_SA_KEY, five
// times per working day (Eastern), and:
//   - any row whose status means money is due (signed / retained / billable /
//     converted / accepted-by-firm) goes to billable_queue as PENDING for Kyler's
//     approval - nothing is ever marked billable by this code
//   - every other status is applied to the lead (buyer_status, notes, status) so
//     it shows on the right publisher's portal, with the buyer never named
//   - rows that match no lead we sent that buyer are kept in buyer_sheet_rows
//     so nothing is silently dropped, and reported per buyer
//   - each scan is logged (buyer_sheet_scans) with the sheet's Drive
//     modifiedTime, which is what the Home box shows as "last updated"
// Sheets are matched to leads by phone, restricted to leads whose recorded
// buyer is that sheet's buyer, so a number that went to two buyers cannot cross.
const BS_SCAN_TIMES = ['08:00', '11:00', '13:00', '15:00', '21:00'];   // America/New_York, Mon-Fri
const BS_BILLABLE = /\b(signed|retained|retainer|billable|converted|conversion|accepted by firm|hired|closed won)\b/i;
const BS_REJECT   = /\b(reject\w*|not qualified|unqualified|dq|disqualif\w*|unresponsive|wrong number|stop|dnc|duplicate|dupe|not viable|no injury|no insurance|out of state|outside|declin\w*|dead|closed lost|lost|returned|opted out|not interested|no contact|never (made|answered)|unable to reach)\b/i;
const BS_OPEN     = /\b(chase|outreach|attempt|contacted|in progress|working|scheduled|pending|under review|reviewing|callback|call back|open|waiting)\b|answering machine|voice ?mail|left (message|vm)|no answer|\bbusy\b|\bringing\b|^new$/i;
const BS_NOT_BILLABLE_FLAG = /^(no|n|false|not billable|non-billable)$/i;

// One entry per sheet/tab. tab null = first tab that has a phone column and a
// status column. amount = what the buyer pays on a billable (goes to the queue
// and becomes leads.revenue on approval). enabled false = read for the Home box
// only, never write anything.
const BUYER_SHEETS = [
  { key: 'nld-mva',   label: 'NLD CPA',    buyer: 'NLD CPA',    vertical: 'MVA', sheet: '1_NBKeIAg7p87mTDneR_fANGx9AqGV8abpWe29EBoko4', tab: 'MVA CPA Leads - New', amount: 2000, enabled: true },
  // Rideshare leads carry no buyer_name on the record, so this tab matches by campaign instead
  { key: 'nld-ride',  label: 'NLD Rideshare', buyer: 'CH-AD',   vertical: 'Rideshare', sheet: '1_NBKeIAg7p87mTDneR_fANGx9AqGV8abpWe29EBoko4', tab: 'Rideshare', campaigns: ['rideshare-tb'], amount: 1800, enabled: true },
  // LT's sheet: one tab per month ("Sep 2026"), no phone column - rows are keyed by "Your reference" = our KRW-#### vendor code
  { key: 'lt-intake', label: 'LT-Intake',  buyer: 'LT-Intake',  vertical: 'MVA', sheet: '16azBD-YOUB2dQbvLnwCXmPF4ntq0ntug582sJC_vs1Y', tab: 'Sheet1', amount: 2500, enabled: true },
  // Chad's intake sheet carries MVA plus his Roblox and Rideshare rows; MVA matches by buyer, the others by campaign
  { key: 'ch-intake', label: 'CH-Intake',  buyer: 'CH-Intake',  vertical: 'MVA', sheet: '1vlM4f8lqOHemrRZ1IhYdS9amU826GNV5GJS8nfGRgE0', tab: null, campaigns: ['roblox-mt','roblox-chad','rideshare-chad'], amount: 2250, enabled: true },
  { key: 'mva-003',   label: 'MVA-003-LT', buyer: 'MVA-003-LT', vertical: 'MVA', sheet: '10sbja-_waUhHvWnu2t_K_WNHMiKOOE020-70yudDoLI', tab: null, amount: 1700, enabled: true },
];

// ── Google auth (service account JWT -> access token), no dependencies ──
let bsToken = null, bsTokenExp = 0;
function bsSaKey() { try { return JSON.parse(process.env.GOOGLE_SA_KEY || ''); } catch (e) { return null; } }
async function bsAccessToken() {
  if (bsToken && Date.now() < bsTokenExp - 60000) return bsToken;
  const sa = bsSaKey(); if (!sa || !sa.client_email || !sa.private_key) throw new Error('GOOGLE_SA_KEY missing or invalid');
  const now = Math.floor(Date.now() / 1000);
  const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
  const unsigned = b64({ alg: 'RS256', typ: 'JWT' }) + '.' + b64({ iss: sa.client_email, scope: 'https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/drive.metadata.readonly', aud: 'https://oauth2.googleapis.com/token', iat: now, exp: now + 3600 });
  const sig = require('crypto').createSign('RSA-SHA256').update(unsigned).sign(sa.private_key, 'base64url');
  const body = 'grant_type=' + encodeURIComponent('urn:ietf:params:oauth:grant-type:jwt-bearer') + '&assertion=' + unsigned + '.' + sig;
  const r = await bsHttp('POST', 'https://oauth2.googleapis.com/token', body, { 'Content-Type': 'application/x-www-form-urlencoded' });
  const j = JSON.parse(r.body || '{}');
  if (!j.access_token) throw new Error('Google token error: ' + (j.error_description || j.error || r.status));
  bsToken = j.access_token; bsTokenExp = Date.now() + (j.expires_in || 3600) * 1000;
  return bsToken;
}
function bsHttp(method, url, body, headers) {
  return new Promise((resolve, reject) => {
    const u = new URL(url), lib = require('https');
    const req2 = lib.request({ hostname: u.hostname, path: u.pathname + u.search, method, headers: Object.assign({}, headers || {}, body ? { 'Content-Length': Buffer.byteLength(body) } : {}), timeout: 20000 },
      r => { let d = ''; r.on('data', c => d += c); r.on('end', () => resolve({ status: r.statusCode, body: d })); });
    req2.on('timeout', () => req2.destroy(new Error('timeout'))); req2.on('error', reject);
    if (body) req2.write(body); req2.end();
  });
}
async function bsApi(url) {
  const tok = await bsAccessToken();
  const r = await bsHttp('GET', url, null, { Authorization: 'Bearer ' + tok });
  if (r.status === 403 || r.status === 404) throw new Error('no access (share the sheet with ' + (bsSaKey() || {}).client_email + ')');
  if (r.status >= 300) throw new Error('Google API ' + r.status + ': ' + r.body.slice(0, 120));
  return JSON.parse(r.body || '{}');
}
async function bsTabs(sheetId) { const j = await bsApi(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}?fields=sheets.properties.title`); return (j.sheets || []).map(s => s.properties.title); }
async function bsValues(sheetId, tab) { const j = await bsApi(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent("'" + tab.replace(/'/g, "''") + "'")}?majorDimension=ROWS`); return j.values || []; }
async function bsModified(sheetId) { const j = await bsApi(`https://www.googleapis.com/drive/v3/files/${sheetId}?fields=modifiedTime,name`); return j; }

// ── parsing ──
function bsNorm(s) { return String(s == null ? '' : s).trim(); }
function bsPhone(s) { const d = bsNorm(s).replace(/\D/g, '').replace(/^1(?=\d{10}$)/, ''); return d.length === 10 ? d : null; }
function bsParseDate(s) { s = bsNorm(s); if (!s) return null; const m = s.match(/^(\d{1,2})[\/.-](\d{1,2})(?:[\/.-](\d{2,4}))?/); if (m) { const y = m[3] ? (m[3].length === 2 ? 2000 + +m[3] : +m[3]) : new Date().getFullYear(); return new Date(y, +m[1] - 1, +m[2]).toISOString(); } let c = s.replace(/,?\s+at\s+/i, ' '); if (!/\b(19|20)\d{2}\b/.test(c)) c = c.replace(/^([A-Za-z]{3,9}\s+\d{1,2})/, '$1, ' + new Date().getFullYear()); const t = Date.parse(c); return isNaN(t) ? null : new Date(t).toISOString(); }
// Locate the header row and map columns by name. Works for every buyer layout seen so far.
function bsMapHeader(rows) {
  for (let i = 0; i < Math.min(rows.length, 15); i++) {
    const up = (rows[i] || []).map(c => bsNorm(c).toUpperCase());
    const find = (...res) => { for (const re of res) { const j = up.findIndex(c => re.test(c)); if (j > -1) return j; } return -1; };
    const phone = find(/^(PHONE|PHONE NUMBER|PHONE #|CID|CALLER ID|PHONE_NUMBER)$/, /PHONE/, /^CID/);
    const vendor = find(/^(YOUR REFERENCE|REFERENCE|VENDOR LEAD CODE|VENDOR_LEAD_CODE|KRW ID|KRW_ID|KRW REF)$/, /REFERENCE|VENDOR|KRW/);
    const status = find(/^(STATUS|DISPOSITION|LEAD STATUS|NLD STATUS|RESULT)$/, /STATUS|DISPO/);
    if ((phone < 0 && vendor < 0) || status < 0) continue;
    return { headerRow: i, phone, status, vendor,
      date: find(/^(DATE|SENT|SUBMISSION DATE|DATE SENT|DATE SUBMITTED)$/, /DATE/), first: find(/^FIRST/, /FIRST/), last: find(/^LAST/, /LAST/), name: find(/^(NAME|FULL NAME|CLIENT|LEAD NAME|LEAD)$/),
      notes: find(/^STAGE UPDATES$/, /^STATUS NOTES$/, /^REASON$/, /^NOTES?$/, /^COMMENTS?$/, /STAGE|REASON|NOTE|COMMENT/), calls: find(/^CALLED_COUNT$/), lastcall: find(/^LAST_LOCAL_CALL_TIME$/), invoice: find(/INVOICE/), billable: find(/^BILLABLE$/), signed: find(/^SIGNED$/) };
  }
  return null;
}
function bsClassify(cfg, r) {
  const st = bsNorm(r.status), notes = bsNorm(r.notes), flag = bsNorm(r.billableFlag);
  if (flag && BS_NOT_BILLABLE_FLAG.test(flag)) { /* explicit no */ }
  else if ((flag && /^(yes|y|true|billable)$/i.test(flag)) || BS_BILLABLE.test(st)) {
    if (!/\b(not|non|un)[- ]?(signed|retained|billable)\b/i.test(st)) return { kind: 'billable', status: 'Signed', note: 'Signed — retained by buyer' + (notes ? ' (' + notes + ')' : '') };
  }
  if (!st) return null;   // blank status = nothing to say yet
  if (BS_REJECT.test(st) || BS_REJECT.test(notes)) return { kind: 'rejected', status: 'Rejected', note: 'Rejected — ' + (notes || st).replace(/^rejected\s*[-–:]?\s*/i, '') };
  if (BS_OPEN.test(st) || BS_OPEN.test(notes)) return { kind: 'open', status: 'Open — in outreach', note: 'Open — ' + (notes || st) };
  return { kind: 'other', status: 'Pending', note: 'In outreach — ' + (notes || st) };
}

// ── scan ──
async function bsScanOne(cfg, trigger) {
  const rep = { key: cfg.key, label: cfg.label, ok: false, rows: 0, matched: 0, unmatched: 0, updated: 0, queued: 0, skipped: 0, error: null, modified: null, tab: null };
  try {
    const meta = await bsModified(cfg.sheet); rep.modified = meta.modifiedTime || null;
    let tabsToRead = [];
    const allTabs = await bsTabs(cfg.sheet);
    if (cfg.tab) tabsToRead = [cfg.tab];
    else if (cfg.tabPattern) tabsToRead = allTabs.filter(t => cfg.tabPattern.test(t));
    else { for (const t of allTabs) { const v = await bsValues(cfg.sheet, t); if (bsMapHeader(v)) { tabsToRead = [t]; break; } } }
    if (!tabsToRead.length) throw new Error('no tab with a phone/reference column and a status column');
    rep.tab = tabsToRead.join(', ');
    let data = [];
    for (const tab of tabsToRead) {
      const rows = await bsValues(cfg.sheet, tab);
      const map = bsMapHeader(rows); if (!map) { if (tabsToRead.length === 1) throw new Error('could not find a header row with phone/reference + status'); continue; }
      data = data.concat(rows.slice(map.headerRow + 1).map(r => {
        const signedVal = map.signed > -1 ? bsNorm(r[map.signed]) : '';
        const signedYes = signedVal && !/^(no|n|-|false|0|not signed)$/i.test(signedVal);
        return {
          phone: map.phone > -1 ? bsPhone(r[map.phone]) : null, status: bsNorm(r[map.status]), notes: (function(){ var n = map.notes > -1 ? bsNorm(r[map.notes]) : ''; if (/cert\.trustedform\.com|^Incident \d/i.test(n)) n = ''; var cc = map.calls > -1 ? bsNorm(r[map.calls]) : '', lc = map.lastcall > -1 ? bsNorm(r[map.lastcall]) : ''; if (n === '' && cc !== '') n = 'called ' + cc + ' time' + (cc === '1' ? '' : 's') + (lc !== '' ? ', last ' + lc : ''); return n; })(),
          date: map.date > -1 ? bsParseDate(r[map.date]) : null, invoice: map.invoice > -1 ? bsNorm(r[map.invoice]) : '',
          billableFlag: map.billable > -1 && bsNorm(r[map.billable]) ? bsNorm(r[map.billable]) : (signedYes ? 'yes' : ''), vendor: map.vendor > -1 ? bsNorm(r[map.vendor]) : '',
          name: map.name > -1 ? bsNorm(r[map.name]) : [bsNorm(r[map.first]), bsNorm(r[map.last])].filter(Boolean).join(' '),
        };
      }).filter(r => r.phone || /KRW-\d+/i.test(r.vendor)));
    }
    rep.rows = data.length;
    // de-dupe: last row for a phone wins (buyers append updates)
    const byPhone = new Map(); data.forEach(r => byPhone.set(r.phone || r.vendor, r));
    const client = await pool.connect();
    try {
      for (const r of byPhone.values()) {
        const vendorId = (r.vendor.match(/KRW-(\d+)/i) || [])[1] || null;
        const lead = (await client.query(
          `SELECT id, status, buyer_status, notes, billable, raw, publisher_sub FROM leads
           WHERE ( ($1::int IS NOT NULL AND id=$1::int) OR ($2::text IS NOT NULL AND regexp_replace(phone,'\\D','','g')=$2::text) )
             AND ( raw->>'buyer_name' = $3::text OR ($4::text[] IS NOT NULL AND campaign = ANY($4::text[])) )
           ORDER BY (id=$1::int) DESC, received_at DESC LIMIT 1`, [vendorId ? parseInt(vendorId, 10) : null, r.phone, cfg.buyer, cfg.campaigns || null])).rows[0];
        const cls = bsClassify(cfg, r);
        // keep every row we see (for reconciliation + the unmatched list)
        await client.query(
          `INSERT INTO buyer_sheet_rows (buyer_key, phone, sheet_status, sheet_notes, sheet_date, invoice, lead_id, kind, first_seen, last_seen)
           VALUES ($1::text,$2::text,$3::text,$4::text,$5::timestamptz,$6::text,$7::int,$8::text,NOW(),NOW())
           ON CONFLICT (buyer_key, phone) DO UPDATE SET sheet_status=EXCLUDED.sheet_status, sheet_notes=EXCLUDED.sheet_notes, sheet_date=COALESCE(EXCLUDED.sheet_date, buyer_sheet_rows.sheet_date), invoice=EXCLUDED.invoice, lead_id=COALESCE(EXCLUDED.lead_id, buyer_sheet_rows.lead_id), kind=EXCLUDED.kind, last_seen=NOW()`,
          [cfg.key, r.phone || r.vendor, r.status, r.notes, r.date, r.invoice, lead ? lead.id : null, cls ? cls.kind : 'blank']);
        if (!lead) { rep.unmatched++; continue; }
        rep.matched++;
        if (!cls || !cfg.enabled) { rep.skipped++; continue; }
        const locked = lead.raw && lead.raw.billable_locked === 'true';
        if (cls.kind === 'billable') {
          // already queued (any state) for this lead from a sheet? never twice
          const dup = await client.query(`SELECT id FROM billable_queue WHERE lead_id=$1 AND raw->>'source'='buyer_sheet' LIMIT 1`, [lead.id]);
          if (dup.rows.length || lead.billable) { rep.skipped++; continue; }
          await client.query(
            `INSERT INTO billable_queue (cid, amount, publisher_sub, lead_id, status, raw) VALUES ($1,$2,$3,$4,'pending',$5::jsonb)`,
            [r.phone || lead.id, cfg.amount, lead.publisher_sub, lead.id, JSON.stringify({ source: 'buyer_sheet', buyer_key: cfg.key, buyer: cfg.buyer, vertical: cfg.vertical, sheet_status: r.status, sheet_notes: r.notes, sheet_date: r.date, invoice: r.invoice, scanned_at: new Date().toISOString(), trigger })]);
          // Nothing says "Signed" anywhere until Kyler approves it. Until then the lead
          // reads Pending on the portal, with no reason that reveals the buyer's report.
          await client.query(`UPDATE leads SET buyer_status='Pending', notes='In outreach', raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('buyer_disposition', jsonb_build_object('source','buyer_sheet','buyer_key',$1::text,'status','Pending','note','In outreach','sheet_status',$2::text,'awaiting_approval',true,'synced_at',NOW())) WHERE id=$3::int`, [cfg.key, r.status, lead.id]);
          rep.queued++;
          console.log(`[Buyer Sheets] $ ${cfg.label} | lead ${lead.id} ${r.name} | ${r.status} -> queued for approval ($${cfg.amount})`);
          continue;
        }
        // non-billable: apply only if it changed
        const prev = (lead.raw && lead.raw.buyer_disposition) || {};
        if (prev.source === 'buyer_sheet' && prev.status === cls.status && prev.note === cls.note) { rep.skipped++; continue; }
        if (locked || lead.billable) { rep.skipped++; continue; }   // never downgrade an approved billable from a sheet
        await client.query(
          `UPDATE leads SET buyer_status=$1::text, status=CASE WHEN $2::text='rejected' THEN 'buyer_rejected' ELSE status END, notes=$3::text,
             raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('buyer_disposition', jsonb_build_object('source','buyer_sheet','buyer_key',$4::text,'status',$1::text,'note',$3::text,'sheet_status',$5::text,'synced_at',NOW()))
           WHERE id=$6::int`, [cls.status, cls.kind, cls.note, cfg.key, r.status, lead.id]);
        rep.updated++;
      }
    } finally { client.release(); }
    rep.ok = true;
  } catch (err) { rep.error = err.message; }
  await pool.query(`INSERT INTO buyer_sheet_scans (buyer_key, ok, error, rows, matched, unmatched, updated, queued, modified_time, tab, trigger) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
    [cfg.key, rep.ok, rep.error, rep.rows, rep.matched, rep.unmatched, rep.updated, rep.queued, rep.modified, rep.tab, trigger]).catch(() => {});
  console.log(`[Buyer Sheets] ${rep.ok ? '✓' : '✕'} ${cfg.label} (${trigger}) | ${rep.error || `${rep.rows} rows, ${rep.matched} matched, ${rep.unmatched} unmatched, ${rep.updated} updated, ${rep.queued} queued`}`);
  return rep;
}
async function bsScanAll(trigger, onlyKey) {
  const out = [];
  for (const cfg of BUYER_SHEETS) { if (onlyKey && cfg.key !== onlyKey) continue; out.push(await bsScanOne(cfg, trigger)); }
  const queued = out.reduce((s, r) => s + r.queued, 0);
  if (queued) {
    const lines = out.filter(r => r.queued).map(r => `<li><b>${r.label}</b>: ${r.queued} new billable${r.queued > 1 ? 's' : ''} waiting for approval</li>`).join('');
    sendEmailNotification(`${queued} new billable${queued > 1 ? 's' : ''} from buyer sheets — Needs Approval`, `<p>The ${trigger} scan found billable dispositions on buyer sheets:</p><ul>${lines}</ul><p>They are in your approval queue. Nothing is billed until you approve it.</p>`);
  }
  return out;
}

// ── schedule: five times a working day, Eastern ──
const bsRan = new Set();
function bsEasternNow() { const p = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour12: false, weekday: 'short', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).formatToParts(new Date()); const g = t => (p.find(x => x.type === t) || {}).value; return { day: `${g('year')}-${g('month')}-${g('day')}`, hm: `${String(g('hour')).padStart(2, '0').replace('24', '00')}:${g('minute')}`, wd: g('weekday') }; }
setInterval(() => {
  const { day, hm, wd } = bsEasternNow();
  if (['Sat', 'Sun'].includes(wd) || !BS_SCAN_TIMES.includes(hm)) return;
  const slot = day + ' ' + hm; if (bsRan.has(slot)) return; bsRan.add(slot);
  bsScanAll(hm + ' ET').catch(e => console.error('[Buyer Sheets] scheduled scan failed:', e.message));
}, 30 * 1000);

// ── tables ──
(async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS buyer_sheet_rows (
        id SERIAL PRIMARY KEY, buyer_key TEXT NOT NULL, phone TEXT NOT NULL, sheet_status TEXT, sheet_notes TEXT, sheet_date TIMESTAMPTZ,
        invoice TEXT, lead_id INTEGER, kind TEXT, first_seen TIMESTAMPTZ DEFAULT NOW(), last_seen TIMESTAMPTZ DEFAULT NOW(), UNIQUE (buyer_key, phone));
      CREATE TABLE IF NOT EXISTS buyer_sheet_scans (
        id SERIAL PRIMARY KEY, buyer_key TEXT NOT NULL, scanned_at TIMESTAMPTZ DEFAULT NOW(), ok BOOLEAN, error TEXT, rows INTEGER, matched INTEGER,
        unmatched INTEGER, updated INTEGER, queued INTEGER, modified_time TIMESTAMPTZ, tab TEXT, trigger TEXT);
      CREATE INDEX IF NOT EXISTS idx_bss_key ON buyer_sheet_scans (buyer_key, scanned_at DESC);`);
    console.log('[Buyer Sheets] schema ready; service account: ' + ((bsSaKey() || {}).client_email || 'NOT SET'));
  } catch (e) { console.error('[Buyer Sheets] schema init failed:', e.message); }
})();

// ── admin / Home box ──
app.post('/buyer-sheets/scan', requireKey, async (req, res) => {
  try { res.json({ ok: true, results: await bsScanAll('manual', req.query.buyer || null) }); }
  catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
app.get('/buyer-sheets/status', requireKey, async (req, res) => {
  try {
    const out = [];
    for (const cfg of BUYER_SHEETS) {
      const last = (await pool.query(`SELECT * FROM buyer_sheet_scans WHERE buyer_key=$1 ORDER BY scanned_at DESC LIMIT 1`, [cfg.key])).rows[0] || null;
      const lastOk = (await pool.query(`SELECT scanned_at, modified_time FROM buyer_sheet_scans WHERE buyer_key=$1 AND ok=true ORDER BY scanned_at DESC LIMIT 1`, [cfg.key])).rows[0] || null;
      const rec = (await pool.query(
        `SELECT COUNT(*)::int AS sent,
                COUNT(*) FILTER (WHERE raw->'buyer_disposition' IS NOT NULL OR buyer_status IN ('Rejected','Signed','Retained','Returned'))::int AS dispositioned,
                COUNT(*) FILTER (WHERE raw->'buyer_disposition' IS NULL AND buyer_status NOT IN ('Rejected','Signed','Retained','Returned','Test') AND received_at < NOW() - INTERVAL '3 days')::int AS waiting_3d
         FROM leads WHERE raw->>'buyer_name'=$1 AND status IN ('forwarded','buyer_rejected') AND received_at >= NOW() - INTERVAL '45 days'`, [cfg.buyer])).rows[0];
      const un = (await pool.query(`SELECT COUNT(*)::int AS n FROM buyer_sheet_rows WHERE buyer_key=$1 AND lead_id IS NULL AND kind <> 'blank'`, [cfg.key])).rows[0];
      const pend = (await pool.query(`SELECT COUNT(*)::int AS n, COALESCE(SUM(amount),0)::float AS amt FROM billable_queue WHERE status='pending' AND raw->>'source'='buyer_sheet' AND raw->>'buyer_key'=$1`, [cfg.key])).rows[0];
      out.push({ key: cfg.key, label: cfg.label, vertical: cfg.vertical, enabled: cfg.enabled, sheet_url: 'https://docs.google.com/spreadsheets/d/' + cfg.sheet,
        last_modified: lastOk ? lastOk.modified_time : null, last_scan: last ? last.scanned_at : null, last_ok: !!(last && last.ok), last_error: last ? last.error : 'not scanned yet',
        rows: last ? last.rows : 0, unmatched_rows: un.n, pending_approval: pend.n, pending_amount: pend.amt,
        sent_45d: rec.sent, dispositioned_45d: rec.dispositioned, waiting_over_3d: rec.waiting_3d });
    }
    res.json({ ok: true, scan_times_et: BS_SCAN_TIMES, service_account: (bsSaKey() || {}).client_email || null, buyers: out });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
// Admin: show tab names and the first rows of each tab so a new buyer's layout can be mapped
app.get('/buyer-sheets/peek', requireKey, async (req, res) => {
  const cfg = BUYER_SHEETS.find(b => b.key === req.query.buyer);
  if (!cfg) return res.status(400).json({ ok: false, error: 'buyer must be one of ' + BUYER_SHEETS.map(b => b.key).join(', ') });
  try {
    const tabs = await bsTabs(cfg.sheet), out = [];
    for (const t of tabs) { const v = await bsValues(cfg.sheet, t); out.push({ tab: t, rows: v.length, header_found: !!bsMapHeader(v), sample: v.slice(0, 6) }); }
    res.json({ ok: true, buyer: cfg.label, tabs: out });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
app.get('/buyer-sheets/unmatched', requireKey, async (req, res) => {
  const r = await pool.query(`SELECT buyer_key, phone, sheet_status, sheet_notes, sheet_date, last_seen FROM buyer_sheet_rows WHERE lead_id IS NULL AND kind <> 'blank' ORDER BY last_seen DESC LIMIT 200`);
  res.json({ ok: true, rows: r.rows });
});
// ─── END BUYER DISPOSITION SHEETS ───────────────────────────────────────────

app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set: ${!!process.env.API_KEY}`);
      console.log(`LEAD_KEY set: ${!!process.env.LEAD_API_KEY}`);
      console.log(`DB set: ${!!process.env.DATABASE_URL}`);
    });
  })
  .catch(err => { console.error('Failed to start:', err.message); process.exit(1); });
