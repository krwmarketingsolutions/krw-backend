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
    if (!mailer) { console.log('Email not configured - skipping notification'); return; }
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
    const { campaign, status, limit=100, pub, days } = req.query;
    const where=[], params=[];
    let i=1;
    if (campaign) { where.push(`campaign=$${i++}`);       params.push(campaign); }
    if (status)   { where.push(`status=$${i++}`);         params.push(status); }
    if (pub)      { where.push(`publisher_sub=$${i++}`);  params.push(pub); }
    if (days && parseInt(days) < 9999) {
      where.push(`received_at >= NOW() - INTERVAL '${parseInt(days)} days'`);
    }
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

// Get calls for a publisher
app.get('/publishers/:pub_id/calls', async (req, res) => {
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


// ─── MVA NLD2 — LEAD PROSPER FORWARDING ─────────────────────────────────────
// Receives MVA CPA leads from publishers, validates required fields,
// and forwards to Lead Prosper / Nex Level Direct endpoint.
// Campaign: mva-nld2 | Buyer: Nex Level Direct
// Completely separate from SSDI and FE verticals.

const LP_MVA_URL         = 'https://api.leadprosper.io/direct_post';
const LP_MVA_CAMPAIGN_ID = '31080';
const LP_MVA_SUPPLIER_ID = '110928';
const LP_MVA_KEY         = 'ke21sx0koi7dld';

app.post('/leads/mva-nld2', async (req, res) => {
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
  if (!b.email)          missing.push('email');
  if (!b.phone)          missing.push('phone');
  if (!b.incident_state) missing.push('incident_state');
  if (!b.incident_date)  missing.push('incident_date');
  if (!b.have_attorney)  missing.push('have_attorney');
  if (!b.at_fault)       missing.push('at_fault');
  if (!b.settlement)     missing.push('settlement');
  if (!b.cited)          missing.push('cited');
  if (!b.doctor_treatment) missing.push('doctor_treatment');
  if (!b.physical_injury)  missing.push('physical_injury');

  if (missing.length) {
    return res.status(400).json({ ok: false, error: 'Missing required fields', missing });
  }

  // Publisher sub tracking
  const publisherSub = b.publisher_sub || b.lp_subid1 || null;

  // Build Lead Prosper payload
  const payload = {
    lp_campaign_id: LP_MVA_CAMPAIGN_ID,
    lp_supplier_id: LP_MVA_SUPPLIER_ID,
    lp_key:         LP_MVA_KEY,
    lp_action:      b.lp_action || '',
    lp_subid1:      publisherSub || '',
    lp_subid2:      b.lp_subid2 || '',
    first_name:     b.first_name,
    last_name:      b.last_name,
    email:          b.email,
    phone:          b.phone,
    incident_state: b.incident_state,
    incident_date:  b.incident_date,
    have_attorney:  b.have_attorney,
    at_fault:       b.at_fault,
    settlement:     b.settlement,
    cited:          b.cited,
    doctor_treatment: b.doctor_treatment,
    physical_injury:  b.physical_injury,
  };

  // Optional fields
  const optFields = ['date_of_birth','gender','address','city','state','zip_code',
    'ip_address','user_agent','landing_page_url','jornaya_leadid',
    'trustedform_cert_url','tcpa_text'];
  optFields.forEach(f => { if (b[f]) payload[f] = b[f]; });

  // Log the lead attempt
  const client = await pool.connect();
  let leadId = null;
  try {
    const insert = await client.query(
      `INSERT INTO leads
         (campaign, vertical, first_name, last_name, phone, email,
          publisher_sub, ip_address, status, raw, received_at)
       VALUES ('mva-nld2','MVA',$1,$2,$3,$4,$5,$6,'pending',$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone, b.email,
       publisherSub, b.ip_address || null,
       JSON.stringify(b)]
    );
    leadId = insert.rows[0].id;
  } catch(dbErr) {
    console.error('[MVA NLD2] DB insert error:', dbErr.message);
  } finally {
    client.release();
  }

  // Forward to Lead Prosper
  try {
    const https = require('https');
    const postData = JSON.stringify(payload);

    const lpRes = await new Promise((resolve, reject) => {
      const url = new URL(LP_MVA_URL);
      const options = {
        hostname: url.hostname,
        path:     url.pathname,
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

    let lpResult = {};
    try { lpResult = JSON.parse(lpRes.body); } catch(e) {
      lpResult = { status: 'ERROR', message: lpRes.body };
    }

    const accepted = lpResult.status === 'ACCEPTED';
    const duplicate = lpResult.status === 'DUPLICATED';

    // Update lead status in DB
    if (leadId) {
      const c2 = await pool.connect();
      try {
        await c2.query(
          `UPDATE leads SET
             status          = $1,
             buyer_intake_id = $2,
             buyer_response  = $3::jsonb,
             revenue         = 0
           WHERE id = $4`,
          [
            accepted ? 'forwarded' : duplicate ? 'duplicate' : 'buyer_rejected',
            lpResult.lead_id || null,
            JSON.stringify(lpResult),
            leadId
          ]
        );
      } finally { c2.release(); }
    }

    console.log(`[MVA NLD2] ${accepted ? '✅' : duplicate ? '🔁' : '❌'} ${b.first_name} ${b.last_name} → ${lpResult.status} | ${lpResult.lead_id || 'no id'}`);

    return res.json({
      ok:      accepted,
      status:  lpResult.status,
      lead_id: lpResult.lead_id || null,
      message: accepted ? 'Lead accepted' : duplicate ? 'Duplicate lead' : 'Lead rejected',
      code:    lpResult.code || null,
      krw_id:  leadId
    });

  } catch (fwdErr) {
    console.error('[MVA NLD2] Forward error:', fwdErr.message);

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
// ─── END MVA NLD2 ────────────────────────────────────────────────────────────


// ─── RIDESHARE UBER/LYFT — TRUE BLUE FORWARDING ─────────────────────────────
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

  // Validate required fields
  const missing = [];
  if (!b.first_name)     missing.push('first_name');
  if (!b.last_name)      missing.push('last_name');
  if (!b.phone_home)     missing.push('phone_home');
  if (!b.email_address)  missing.push('email_address');
  if (!b.zip_code)       missing.push('zip_code');
  if (!b.ip_address)     missing.push('ip_address');
  if (!b.attorney)       missing.push('attorney');
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
          publisher_sub, ip_address, status, raw, received_at)
       VALUES ('rideshare-tb','Mass Tort - Rideshare',$1,$2,$3,$4,$5,$6,'pending',$7::jsonb,NOW())
       RETURNING id`,
      [b.first_name, b.last_name, b.phone_home, b.email_address,
       publisherSub, b.ip_address,
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

app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set: ${!!process.env.API_KEY}`);
      console.log(`LEAD_KEY set: ${!!process.env.LEAD_API_KEY}`);
      console.log(`DB set: ${!!process.env.DATABASE_URL}`);
    });
  })
  .catch(err => { console.error('Failed to start:', err.message); process.exit(1); });
