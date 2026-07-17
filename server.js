// ══════════════════════════════════════════════════════
// FILE: server.js (v122)
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
      pool.query(`SELECT campaign, COUNT(*) as count FROM leads WHERE received_at::date=CURRENT_DATE AND campaign NOT IN ('Lssdi-shore') GROUP BY campaign`),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1 AND campaign NOT IN ('Lssdi-shore')`,[weekAgo]),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1 AND campaign NOT IN ('Lssdi-shore')`,[monthStart]),
      pool.query(`SELECT status, COUNT(*) as count FROM leads WHERE campaign NOT IN ('Lssdi-shore') GROUP BY status ORDER BY count DESC`),
    ]);
    res.json({ ok:true, today:todayQ.rows, week:parseInt(weekQ.rows[0]?.count||0), month:parseInt(monthQ.rows[0]?.count||0), by_status:statusQ.rows });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.get('/leads/feed', requireKey, async (req, res) => {
  try {
    const { campaign, status, limit=100, pub, days, portal_id } = req.query;
    const where=[], params=[];
    let i=1;
    if (campaign) {
      where.push(`campaign=$${i++}`);
      params.push(campaign);
    } else if (!portal_id && !pub) {
      // Exclude SSDI lead campaigns from the general feed — they show on the SSDI tab only
      where.push(`campaign NOT IN ('Lssdi-shore')`);
    }
    if (status)   { where.push(`status=$${i++}`);   params.push(status); }

    // Support portal_id lookup — finds all pub_ids for that portal then filters
    if (portal_id) {
      const pubs = await pool.query(
        `SELECT pub_id FROM publishers WHERE portal_id=$1 AND active=true`, [portal_id]
      );
      const pubIds = pubs.rows.map(p => p.pub_id);
      if (pubIds.length) {
        where.push(`publisher_sub = ANY($${i++})`);
        params.push(pubIds);
      }
    } else if (pub) {
      where.push(`publisher_sub=$${i++}`);
      params.push(pub);
    }

    if (days && parseInt(days) < 9999) {
      where.push(`received_at >= NOW() - INTERVAL '${parseInt(days)} days'`);
    }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(
      `SELECT id,received_at,campaign,first_name,last_name,email,phone,state,
              status,zapier_status,buyer_intake_id,buyer_error,buyer_status,notes,publisher_sub,billable,revenue
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
                   AND source_system IN ('partner','google_sheet')`;
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
    let query = `SELECT * FROM calls WHERE source_system IN ('partner','sheet_import')`;
    if (daysInt < 9999) {
      query += ` AND (source_system='sheet_import' OR received_at::timestamptz >= NOW() - INTERVAL '${daysInt} days')`;
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
        COUNT(*) FILTER(WHERE call_date=CURRENT_DATE OR DATE(received_at)=CURRENT_DATE) AS today,
        COALESCE(SUM(payout_amount) FILTER(WHERE billable=true),0)                     AS total_payout
      FROM calls
      WHERE source_system IN ('partner','sheet_import')
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
  'KRW-JOSHUA-2026-76M': 'SSDI J',
  'KRW-MVA-2026-8RT':    'MVA 2',
};

function aliasPub(pubId) {
  if (!pubId) return null;
  return PUBLISHER_ALIAS[pubId] || pubId;
}

// ── MVA Buyer Tiers ───────────────────────────────────────────────────────────
// Add new buyers here. Order = priority (Tier 1 first).
const MVA_BUYERS = [

  // ── Tier 1: Email Agency ─────────────────────────────────────────────────
  {
    name:   'Email Agency',
    states: ['AZ','CO','IL','IN','MS','NM','NV','NY','OR','TN','UT','WA','WI'],
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

  // ── Tier 2: Placeholder — add CPL buyer here when ready ──────────────────
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
  if (!b.trustedform_cert_url && !b.trusted_form_cert_url) missing.push('trustedform_cert_url');
  if (!b.publisher_sub) missing.push('publisher_sub');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  const publisherSub = b.publisher_sub;
  const leadState    = (b.incident_state || b.state || '').toUpperCase().trim();

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
             revenue         = 0
           WHERE id = $5`,
          [
            result.accepted ? 'forwarded' : result.duplicate ? 'duplicate' : 'buyer_rejected',
            result.lead_id || null,
            JSON.stringify(result.raw),
            result.accepted ? 'Accepted' : result.duplicate ? 'Duplicate' : 'Rejected',
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

// Also keep old route as alias so any existing integrations don't break
app.post('/leads/mva-nld2', (req, res) => {
  req.url = '/leads/mva-funnel';
  app.handle(req, res);
});

// ─── END MVA FUNNEL ───────────────────────────────────────────────────────────

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
    centerCode:             LSSDI_SHORE_CENTER_CODE,
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
    url:      `https://docs.google.com/spreadsheets/d/${KA_SHEET_ID}/export?format=csv&gid=1713913985`,
  },
  {
    name:     'Rideshare',
    campaign: 'rideshare-tb',
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
          let lookup;
          if (isMVA) {
            lookup = await client.query(
              `SELECT id, status, buyer_status, raw->>'billable_locked' as locked
               FROM leads WHERE phone = $1 AND campaign = $2 LIMIT 1`,
              [phone, sheet.campaign]
            );
          } else {
            lookup = await client.query(
              `SELECT id, status, buyer_status, raw->>'billable_locked' as locked
               FROM leads WHERE buyer_intake_id = $1 AND campaign = $2 LIMIT 1`,
              [cid, sheet.campaign]
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



app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set: ${!!process.env.API_KEY}`);
      console.log(`LEAD_KEY set: ${!!process.env.LEAD_API_KEY}`);
      console.log(`DB set: ${!!process.env.DATABASE_URL}`);
    });
  })
  .catch(err => { console.error('Failed to start:', err.message); process.exit(1); });
