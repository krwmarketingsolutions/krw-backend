// ══════════════════════════════════════════════════════
// FILE: server.js
// UPLOAD TO: GitHub repo "krw-backend"
// PURPOSE: KRW Lead Intake + Call Revenue tracking
// ══════════════════════════════════════════════════════

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
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
function getMailer() {
  if (!process.env.SMTP_USER || !process.env.SMTP_PASS) return null;
  return nodemailer.createTransport({
    host:   process.env.SMTP_HOST || 'smtp.office365.com',
    port:   parseInt(process.env.SMTP_PORT || '587'),
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
    tls: { ciphers: 'SSLv3' },
  });
}

async function sendEmailNotification(subject, html) {
  try {
    const mailer = getMailer();
    if (!mailer) { console.log('Email not configured — skipping notification'); return; }
    await mailer.sendMail({
      from:    `"KRW Dashboard" <${process.env.SMTP_USER}>`,
      to:      process.env.NOTIFY_EMAIL || 'kyler@krwmarketingsolutions.com',
      subject,
      html,
    });
    console.log('Email notification sent:', subject);
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
      campaign_name   TEXT,
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

// POST /lead/:campaign — receive lead from publisher
// Stores it and fires to Zapier webhook if configured
app.post('/lead/:campaign', requireLeadKey, async (req, res) => {
  const campaign = req.params.campaign.toLowerCase();
  const b        = req.body || {};

  // Validate required fields
  const required = ['firstName', 'lastName', 'email', 'phone'];
  const missing  = required.filter(f => !b[f] || !String(b[f]).trim());
  if (missing.length) {
    return res.status(422).json({ status: 'rejected', reason: `Missing required fields: ${missing.join(', ')}` });
  }

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
      'roundup':    { id: '30976',  sup: '96279',  key: '6l5rtdz61ay1n2' },
      'roundup-lt': { id: '30976',  sup: '96279',  key: '6l5rtdz61ay1n2' },
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

// ══════════════════════════════════════════════════════
//  LEAD READ ENDPOINTS (dashboard)
// ══════════════════════════════════════════════════════

app.get('/leads/summary', requireKey, async (req, res) => {
  try {
    const weekAgo    = new Date(Date.now()-7*86400000).toISOString().split('T')[0];
    const monthStart = new Date().toISOString().slice(0,7)+'-01';
    const [todayQ,weekQ,monthQ,statusQ] = await Promise.all([
      pool.query(`SELECT campaign, COUNT(*) as count FROM leads WHERE received_at::date=CURRENT_DATE GROUP BY campaign`),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1`,[weekAgo]),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1`,[monthStart]),
      pool.query(`SELECT status, COUNT(*) as count FROM leads GROUP BY status ORDER BY count DESC`),
    ]);
    res.json({ ok:true, today:todayQ.rows, week:parseInt(weekQ.rows[0]?.count||0), month:parseInt(monthQ.rows[0]?.count||0), by_status:statusQ.rows });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

app.get('/leads/feed', requireKey, async (req, res) => {
  try {
    const { campaign, status, limit=100 } = req.query;
    const where=[], params=[];
    let i=1;
    if (campaign) { where.push(`campaign=$${i++}`); params.push(campaign); }
    if (status)   { where.push(`status=$${i++}`);   params.push(status); }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(
      `SELECT id,received_at,campaign,first_name,last_name,email,phone,state,
              status,zapier_status,buyer_intake_id,buyer_error,publisher_sub
       FROM leads ${wc} ORDER BY received_at DESC LIMIT $${i}`, params);
    res.json({ ok:true, count:r.rows.length, leads:r.rows });
  } catch(err) { res.status(500).json({ error:err.message }); }
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

// POST /calls/postback — partner endpoint for direct call submission
app.post('/calls/postback', requireKey, async (req, res) => {
  try {
    const b = req.body || {};
    const { caller_id, call_date, publisher_sub } = b;

    if (!caller_id || !call_date || !publisher_sub) {
      return res.status(422).json({ error: 'Missing required fields: caller_id, call_date, publisher_sub' });
    }

    await pool.query(
      `INSERT INTO calls (caller_id, call_date, publisher_sub, received_at, call_status_label, source_system, payout_amount, raw)
       VALUES ($1, $2, $3, NOW(), 'pending', 'partner', 0, $4::jsonb)`,
      [caller_id, call_date, publisher_sub, JSON.stringify(b)]
    );

    console.log(`[calls/postback] Inserted call — caller: ${caller_id}, pub: ${publisher_sub}, date: ${call_date}, status: pending, payout: 0`);
    res.json({ ok: true, message: 'Call recorded' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
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
//  CAMPAIGNS — create/edit from dashboard, no env vars needed
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
      'DEPO — Lead Tree (WTC)',
      'Mass Tort - Depo',
      process.env.BUYER_ENDPOINT_DEPO || '',
      JSON.stringify(['firstName','lastName','email','phone']),
      JSON.stringify(['street','city','state','zip','notes','trustedFormCertUrl','jornayaLeadId','facebookLeadId','publisherSub']),
    ]);
    console.log('✅ DEPO campaign seeded');
  }
  console.log('✅ Campaigns table ready');
}

// GET /campaigns — list all (dashboard)
app.get('/campaigns', requireKey, async (req, res) => {
  try {
    const r = await pool.query(
      'SELECT * FROM campaigns WHERE active=true ORDER BY created_at ASC'
    );
    res.json({ ok: true, campaigns: r.rows });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /campaigns/:slug/config — public, used by publisher form
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

// POST /campaigns — create or update (upsert)
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

// PATCH /campaigns/:slug — partial update
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
      campaign      TEXT,
      did           TEXT,
      payout_rate   NUMERIC(10,2) DEFAULT 0,
      active        BOOLEAN DEFAULT true,
      created_at    TIMESTAMPTZ DEFAULT NOW()
    );
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS did TEXT;
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS payout_rate NUMERIC(10,2) DEFAULT 0;
  `);
  console.log('Publishers table ready');
}

// ── PUBLISHER ENDPOINTS ───────────────────────────────

// Verify publisher login (pub_id lookup)
app.post('/publishers/login', async (req, res) => {
  const { pub_id } = req.body || {};
  if (!pub_id) return res.status(400).json({ ok: false, error: 'pub_id required' });
  try {
    const r = await pool.query('SELECT * FROM publishers WHERE pub_id=$1 AND active=true', [pub_id]);
    if (!r.rows.length) return res.status(401).json({ ok: false, error: 'Publisher not found' });
    const pub = r.rows[0];
    res.json({ ok: true, name: pub.name, pub_id: pub.pub_id, campaign: pub.campaign, did: pub.did||null });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// Get calls for a publisher
app.get('/publishers/:pub_id/calls', async (req, res) => {
  const { pub_id } = req.params;
  const { days = 30, billable_only } = req.query;
  try {
    // Verify publisher exists
    const pubCheck = await pool.query('SELECT * FROM publishers WHERE pub_id=$1 AND active=true', [pub_id]);
    if (!pubCheck.rows.length) return res.status(401).json({ ok: false, error: 'Unauthorized' });

    const daysInt = parseInt(days) >= 9999 ? 36500 : parseInt(days);
    let query = `SELECT id, call_date, caller_id, caller_name,
                        call_duration, billable, call_status_label, disposition,
                        payout_amount, campaign_name, received_at
                 FROM calls
                 WHERE publisher_sub=$1
                   AND source_system='partner'`;
    if (daysInt < 9999) query += ` AND received_at >= NOW() - INTERVAL '${daysInt} days'`;
    if (billable_only === 'true') query += ' AND billable=true';
    query += ' ORDER BY received_at DESC LIMIT 500';

    const r = await pool.query(query, [pub_id]);
    const total = r.rows.length;
    const billable_count = r.rows.filter(c => c.billable).length;
    const total_payout = r.rows.filter(c => c.billable).reduce((sum, c) => sum + parseFloat(c.payout_amount||0), 0);

    res.json({ ok: true, calls: r.rows, total, billable_count, total_payout: total_payout.toFixed(2) });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// CRUD publishers (dashboard only)
app.get('/publishers', requireKey, async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM publishers ORDER BY created_at DESC');
    res.json({ ok: true, publishers: r.rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/publishers', requireKey, async (req, res) => {
  const { pub_id, name, email, campaign, did, payout_rate } = req.body || {};
  if (!pub_id || !name) return res.status(400).json({ ok: false, error: 'pub_id and name required' });
  try {
    const r = await pool.query(
      `INSERT INTO publishers (pub_id, name, email, campaign, did, payout_rate)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (pub_id) DO UPDATE SET name=$2, email=$3, campaign=$4, did=$5, payout_rate=$6, active=true
       RETURNING *`,
      [pub_id, name, email||null, campaign||null, did||null, parseFloat(payout_rate||0)]
    );
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
    process.env.LEAD_API_KEY || 'krwleads2026secure',
    process.env.TRACKDRIVE_API_KEY || '',
  ].filter(Boolean);
  if (!validKeys.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }
  const b = req.body || {};
  const callDate    = b.call_date   || b.date     || new Date().toISOString().split('T')[0];
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
         publisher_sub, payout_amount, buyer_name, vertical, campaign_name,
         source_system, raw)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [callDate, callerId, callerName, duration, billable,
       pubSub, payout, buyerName, vertical, campaign,
       'trackdrive', JSON.stringify(b)]
    );
    res.json({ ok: true, message: 'TrackDrive call recorded' });
  } catch(err) {
    console.error('TrackDrive postback error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// NOTE: /calls/postback is defined in the CALL / REVENUE ENDPOINTS section above.

// ── END OF DAY SWEEP ENDPOINT ────────────────────────
// Partner posts all calls with final billable status
// Matched by did + call_date, updates existing records
app.patch('/calls/update', async (req, res) => {
  const key = req.headers['x-api-key'] || req.headers['authorization'] || req.query.api_key || '';
  const validKeys = [
    process.env.LEAD_API_KEY || 'krwleads2026secure',
    process.env.PARTNER_API_KEY || '',
    process.env.BUYER_API_KEY || '',
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

      // Find publisher from DID
      let pubSub = item.publisher_sub || null;
      if (!pubSub) {
        const didLookup = await pool.query(
          'SELECT pub_id, payout_rate FROM publishers WHERE did=$1 AND active=true LIMIT 1',
          [did]
        );
        if (didLookup.rows.length) {
          pubSub = didLookup.rows[0].pub_id;
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
          campaign_name     = COALESCE($11, campaign_name)
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
        // Record not found — insert it with all fields
        await pool.query(
          `INSERT INTO calls (call_date, caller_id, caller_name, call_duration,
                              billable, publisher_sub, payout_amount, campaign_name,
                              disposition, call_status_label, source_system, raw)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'partner',$11)`,
          [callDate, callerId, callerName, duration || null,
           billable, pubSub, billable ? payout : null, campaign,
           disposition, statusLabel, JSON.stringify(item)]
        );
        console.log(`Inserted new call: pub=${pubSub} did=${did} date=${callDate}`);
        results.not_found++;
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

  await sendEmailNotification(`SSDI Daily Report — ${dateStr}`, html);

  res.json({ ok: true, message: 'End-of-day sweep complete', ...results });
});

// Get calls feed (dashboard)
app.get('/calls/feed', requireKey, async (req, res) => {
  const { days = 30, pub } = req.query;
  try {
    const daysInt = parseInt(days) >= 9999 ? 36500 : parseInt(days);
    const params = [];
    let query = `SELECT * FROM calls WHERE source_system='partner'`;
    if (daysInt < 9999) {
      query += ` AND received_at >= NOW() - INTERVAL '${daysInt} days'`;
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
        COUNT(*)                                          AS total,
        COUNT(*) FILTER(WHERE billable=true)              AS billable,
        COUNT(*) FILTER(WHERE DATE(received_at)=CURRENT_DATE) AS today,
        COALESCE(SUM(payout_amount) FILTER(WHERE billable=true),0) AS total_payout
      FROM calls
      WHERE source_system='partner'
        AND received_at >= NOW() - INTERVAL '30 days'
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
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set: ${!!process.env.API_KEY}`);
      console.log(`LEAD_KEY set: ${!!process.env.LEAD_API_KEY}`);
      console.log(`DB set: ${!!process.env.DATABASE_URL}`);
    });
  })
  .catch(err => { console.error('Failed to start:', err.message); process.exit(1); });
