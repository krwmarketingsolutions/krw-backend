// ══════════════════════════════════════════════════════
// FILE: server.js
// SERVICE: krw-backend
// PURPOSE: KRW Lead Intake + Call/Revenue Tracking API
// ══════════════════════════════════════════════════════

'use strict';

require('dotenv').config();
const express    = require('express');
const { Pool }   = require('pg');
const nodemailer = require('nodemailer');
const path       = require('path');
const fs         = require('fs');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Environment ───────────────────────────────────────
const API_KEY  = process.env.API_KEY  || '';
const LEAD_KEY = process.env.LEAD_API_KEY || '';

// ── Database ──────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ── Middleware ────────────────────────────────────────
app.use(express.json());
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key, authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ══════════════════════════════════════════════════════
//  AUTH MIDDLEWARE
// ══════════════════════════════════════════════════════

// Requires the main API_KEY only (dashboard / admin routes)
function requireKey(req, res, next) {
  const key = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  if (!API_KEY || key !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// Accepts either API_KEY or LEAD_API_KEY (publisher / intake routes)
function requireLeadKey(req, res, next) {
  const key   = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  const valid = [API_KEY, LEAD_KEY].filter(Boolean);
  if (!valid.includes(key)) {
    return res.status(401).json({ status: 'rejected', reason: 'Invalid API key' });
  }
  next();
}

// Accepts API_KEY, LEAD_API_KEY, TRACKDRIVE_API_KEY, or PARTNER_API_KEY
function requirePartnerKey(req, res, next) {
  const key = (
    req.headers['x-api-key'] ||
    req.headers['authorization'] ||
    req.query.api_key ||
    ''
  ).trim();
  const valid = [
    API_KEY,
    LEAD_KEY,
    process.env.TRACKDRIVE_API_KEY || '',
    process.env.PARTNER_API_KEY    || '',
    process.env.BUYER_API_KEY      || '',
  ].filter(Boolean);
  if (!valid.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid API key' });
  }
  next();
}

// ══════════════════════════════════════════════════════
//  EMAIL NOTIFICATIONS
// ══════════════════════════════════════════════════════

function getMailer() {
  if (!process.env.SMTP_USER || !process.env.SMTP_PASS) return null;
  return nodemailer.createTransport({
    host:   process.env.SMTP_HOST || 'smtp.office365.com',
    port:   parseInt(process.env.SMTP_PORT || '587', 10),
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
    if (!mailer) {
      console.log('Email not configured — skipping notification');
      return;
    }
    await mailer.sendMail({
      from:    `"KRW Dashboard" <${process.env.SMTP_USER}>`,
      to:      process.env.NOTIFY_EMAIL || 'kyler@krwmarketingsolutions.com',
      subject,
      html,
    });
    console.log('Email notification sent:', subject);
  } catch (err) {
    console.error('Email send failed:', err.message);
  }
}

// ══════════════════════════════════════════════════════
//  DATABASE INITIALISATION
//  Creates tables if they don't exist and adds any
//  missing columns to existing tables (safe to re-run).
// ══════════════════════════════════════════════════════

async function initDB() {
  // ── calls ─────────────────────────────────────────
  await pool.query(`
    CREATE TABLE IF NOT EXISTS calls (
      id                SERIAL PRIMARY KEY,
      received_at       TIMESTAMPTZ DEFAULT NOW(),
      call_date         TEXT,
      call_datetime     TEXT,
      call_duration     INTEGER,
      vertical          TEXT,
      campaign_name     TEXT,
      campaign_id       TEXT,
      buyer_name        TEXT,
      buyer_id          TEXT,
      supplier_name     TEXT,
      caller_id         TEXT,
      caller_name       TEXT,
      publisher_sub     TEXT,
      payout_amount     NUMERIC(10,2),
      revenue           NUMERIC(10,2),
      cost              NUMERIC(10,2),
      profit            NUMERIC(10,2),
      billable          BOOLEAN DEFAULT NULL,
      call_status_label TEXT DEFAULT 'pending',
      disposition       TEXT,
      call_status       TEXT,
      invoice_status    TEXT DEFAULT 'pending',
      invoice_id        TEXT,
      invoice_date      TEXT,
      paid_date         TEXT,
      source_system     TEXT DEFAULT 'partner',
      raw               JSONB
    );
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS caller_name       TEXT;
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS publisher_sub     TEXT;
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS source_system     TEXT DEFAULT 'partner';
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_status_label TEXT DEFAULT 'pending';
    UPDATE calls
       SET call_status_label = CASE
             WHEN billable = true  THEN 'cpa'
             WHEN billable = false THEN 'not_converted'
             ELSE 'pending'
           END
     WHERE call_status_label IS NULL OR call_status_label = 'pending';
  `);
  console.log('✅ calls table ready');

  // ── leads ─────────────────────────────────────────
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
  console.log('✅ leads table ready');

  // ── campaigns ─────────────────────────────────────
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
  // Seed DEPO campaign if absent
  const existing = await pool.query(`SELECT slug FROM campaigns WHERE slug = 'depo'`);
  if (!existing.rows.length) {
    await pool.query(`
      INSERT INTO campaigns
        (slug, name, vertical, apex_endpoint, required_fields, optional_fields)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      'depo',
      'DEPO — Lead Tree (WTC)',
      'Mass Tort - Depo',
      process.env.BUYER_ENDPOINT_DEPO || '',
      JSON.stringify(['firstName', 'lastName', 'email', 'phone']),
      JSON.stringify(['street', 'city', 'state', 'zip', 'notes',
                      'trustedFormCertUrl', 'jornayaLeadId', 'facebookLeadId', 'publisherSub']),
    ]);
    console.log('✅ DEPO campaign seeded');
  }
  console.log('✅ campaigns table ready');

  // ── publishers ────────────────────────────────────
  await pool.query(`
    CREATE TABLE IF NOT EXISTS publishers (
      id           SERIAL PRIMARY KEY,
      pub_id       TEXT UNIQUE NOT NULL,
      name         TEXT NOT NULL,
      email        TEXT,
      campaign     TEXT,
      did          TEXT,
      payout_rate  NUMERIC(10,2) DEFAULT 0,
      active       BOOLEAN DEFAULT true,
      created_at   TIMESTAMPTZ DEFAULT NOW()
    );
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS did         TEXT;
    ALTER TABLE publishers ADD COLUMN IF NOT EXISTS payout_rate NUMERIC(10,2) DEFAULT 0;
  `);
  console.log('✅ publishers table ready');
}

// ══════════════════════════════════════════════════════
//  HELPERS
// ══════════════════════════════════════════════════════

// Resolve a publisher record from a DID string
async function publisherFromDID(did) {
  if (!did) return null;
  const normalized = String(did).replace(/\D/g, '');
  if (!normalized) return null;
  const r = await pool.query(
    `SELECT pub_id, payout_rate FROM publishers WHERE did = $1 AND active = true LIMIT 1`,
    [normalized]
  );
  return r.rows[0] || null;
}

// Hardcoded LeadProsper credentials per campaign slug
const LP_CREDS = {
  'mva':        { id: '31080', sup: '110928', key: 'ke21sx0koi7dld' },
  'rideshare':  { id: '31036', sup: '99237',  key: 'jz2gawz23t17g5' },
  'lyft':       { id: '31036', sup: '99237',  key: 'jz2gawz23t17g5' },
  'uber':       { id: '31036', sup: '99237',  key: 'jz2gawz23t17g5' },
  'roundup':    { id: '30976', sup: '96279',  key: '6l5rtdz61ay1n2' },
  'roundup-lt': { id: '30976', sup: '96279',  key: '6l5rtdz61ay1n2' },
};

// Build the outbound payload for a given buyer URL / campaign
function buildBuyerPayload(campaign, data, buyerUrl, campRow, leadRef) {
  const buyerNotes    = campRow?.buyer_notes || '';
  const isLeadProsper = buyerUrl.includes('leadprosper') || buyerUrl.includes('direct_post');
  const isLawmatics   = buyerUrl.includes('lawmatics.com');

  const hardcoded = LP_CREDS[campaign] || {};
  const lpCampId  = hardcoded.id  || (buyerNotes.match(/LP Campaign ID:\s*(\d+)/i) || [])[1] || '';
  const lpSuppId  = hardcoded.sup || (buyerNotes.match(/LP Supplier ID:\s*(\d+)/i) || [])[1] || '';
  const lpKey     = hardcoded.key || (buyerNotes.match(/LP Key:\s*(\S+)/i)         || [])[1] || '';

  const apexMatch    = buyerUrl.match(/\/intake\/([^/]+)\/([^/]+)\/zapier\/([^/]+)\/submit/);
  const apexCampaign = apexMatch ? apexMatch[2] : campaign;
  const apexSeller   = apexMatch
    ? apexMatch[3]
    : (process.env[`BUYER_SELLER_${campaign.toUpperCase()}`] || 'tuell');

  if (isLeadProsper && lpCampId && lpSuppId && lpKey) {
    const payload = {
      lp_campaign_id:       lpCampId,
      lp_supplier_id:       lpSuppId,
      lp_key:               lpKey,
      lp_subid1:            data.publisherSub       || '',
      first_name:           data.firstName,
      last_name:            data.lastName,
      email:                data.email,
      phone:                String(data.phone).replace(/\D/g, ''),
      date_of_birth:        data.dateOfBirth         || null,
      gender:               data.gender              || null,
      address:              data.street              || null,
      city:                 data.city                || null,
      state:                data.state               || null,
      zip_code:             data.zip                 || null,
      jornaya_leadid:       data.jornayaLeadId       || null,
      trustedform_cert_url: data.trustedFormCertUrl  || null,
      tcpa_text:            data.tcpaText            || null,
      incident_state:       data.incidentState       || null,
      case_description:     data.caseDescription || data.notes || null,
      ip_address:           data.ipAddress           || null,
      landing_page_url:     data.websource           || 'https://krwmarketingsolutions.github.io/forms',
      have_attorney:        'Yes',
      used_roundup:         'Yes',
      which_cancer:         data.whichCancer         || null,
      what_year:            data.whatYear            || null,
      exposed_location:     data.exposedLocation     || null,
    };
    // Strip null values before sending
    Object.keys(payload).forEach(k => { if (payload[k] === null) delete payload[k]; });
    return payload;
  }

  if (isLawmatics) {
    const payload = {
      first_name:              data.firstName,
      last_name:               data.lastName,
      email:                   data.email,
      phone:                   String(data.phone).replace(/\D/g, ''),
      birthdate:               data.dateOfBirth         || null,
      zipcode:                 data.zip                 || null,
      state:                   data.state               || null,
      city:                    data.city                || null,
      street:                  data.street              || null,
      custom_field_368623:     data.trustedFormCertUrl  || null,
      custom_field_266045:     data.publisherSub        || null,
      custom_field_766975:     data.rideshareGender     || null,
      custom_field_766959:     data.sexuallyAssaulted   || null,
      custom_field_766960:     data.rideshareCompany    || null,
      custom_field_766964:     data.driverOrPassenger   || null,
      custom_field_766968:     data.abuseType           || null,
      custom_field_766962:     data.incidentDate        || null,
      custom_field_766980:     data.incidentStateText   || null,
      custom_field_631075:     data.hasReceipt          || null,
      custom_field_766976:     data.fraudConviction     || null,
      'custom_field_812732[]': data.reportedTo          || null,
      'custom_field_375335[]': data.bestTimeToCall      || null,
    };
    Object.keys(payload).forEach(k => { if (payload[k] === null) delete payload[k]; });
    return payload;
  }

  // Default: Apex / generic format
  return {
    firstName: data.firstName,
    lastName:  data.lastName,
    email:     data.email,
    phone:     String(data.phone).replace(/\D/g, ''),
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

// Forward a stored lead to the configured buyer endpoint
async function forwardToBuyer(leadId, leadRef, campaign, data, buyerUrl) {
  try {
    await pool.query(`UPDATE leads SET status = 'forwarding' WHERE id = $1`, [leadId]);

    let campRow = null;
    try {
      const cr = await pool.query(`SELECT * FROM campaigns WHERE slug = $1`, [campaign]);
      campRow = cr.rows[0] || null;
    } catch (_) { /* non-fatal */ }

    const payload = buildBuyerPayload(campaign, data, buyerUrl, campRow, leadRef);

    if (campaign === 'roundup' || campaign === 'roundup-lt') {
      console.log(
        `[${leadRef}] LP roundup fields → used_roundup=${payload.used_roundup}` +
        ` have_attorney=${payload.have_attorney}` +
        ` which_cancer=${payload.which_cancer}` +
        ` what_year=${payload.what_year}` +
        ` exposed_location=${payload.exposed_location}`
      );
    }

    const resp   = await fetch(buyerUrl, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });
    const result = await resp.json().catch(() => ({ status: resp.status }));

    const isLawmatics = buyerUrl.includes('lawmatics.com');
    const accepted =
      result.status === 'ACCEPTED' ||
      result.status === 'Success'  ||
      result.status === 'success'  ||
      result.ok === true           ||
      (isLawmatics && resp.ok);

    if (resp.ok && accepted) {
      const buyerId = String(
        result.lead_id || result.id || result.ids?.[0] || result.leadId || ''
      );
      await pool.query(
        `UPDATE leads SET status = 'forwarded', buyer_intake_id = $1, buyer_error = NULL WHERE id = $2`,
        [buyerId, leadId]
      );
      console.log(`✅ Lead ${leadRef} → buyer accepted. Buyer ID: ${buyerId}`);
    } else {
      const errMsg =
        result.message || result.statusDetail || result.error ||
        result.status  || `HTTP ${resp.status}`;
      await pool.query(
        `UPDATE leads SET status = 'buyer_rejected', buyer_error = $1 WHERE id = $2`,
        [errMsg, leadId]
      );
      console.log(`⚠️  Lead ${leadRef} → buyer rejected: ${errMsg}`);
    }
  } catch (err) {
    await pool.query(
      `UPDATE leads SET status = 'forward_failed', buyer_error = $1 WHERE id = $2`,
      [err.message, leadId]
    );
    console.error(`❌ Lead ${leadRef} → forward error: ${err.message}`);
  }
}

// ══════════════════════════════════════════════════════
//  HEALTH / DEBUG
// ══════════════════════════════════════════════════════

app.get('/health', (_req, res) => res.json({ ok: true, status: 'healthy' }));

app.get('/debug', (_req, res) => res.json({
  api_key_set:  !!process.env.API_KEY,
  lead_key_set: !!process.env.LEAD_API_KEY,
  db_url_set:   !!process.env.DATABASE_URL,
  smtp_set:     !!process.env.SMTP_USER,
}));

// ══════════════════════════════════════════════════════
//  DASHBOARD HTML
// ══════════════════════════════════════════════════════

app.get('/dashboard', (_req, res) => {
  const file = path.join(__dirname, 'dashboard.html');
  if (!fs.existsSync(file)) {
    return res.status(404).send('<h2>Upload dashboard.html to your GitHub repo</h2>');
  }
  res.setHeader('Content-Type', 'text/html');
  res.setHeader('Cache-Control', 'no-cache');
  res.sendFile(file);
});

// ══════════════════════════════════════════════════════
//  LEAD INTAKE
// ══════════════════════════════════════════════════════

// POST /lead/:campaign
// Accepts a lead from a publisher, stores it, then forwards to the buyer
// in the background so the publisher gets an immediate response.
app.post('/lead/:campaign', requireLeadKey, async (req, res) => {
  const campaign = req.params.campaign.toLowerCase();
  const b        = req.body || {};

  // Validate required fields
  const required = ['firstName', 'lastName', 'email', 'phone'];
  const missing  = required.filter(f => !b[f] || !String(b[f]).trim());
  if (missing.length) {
    return res.status(422).json({
      status: 'rejected',
      reason: `Missing required fields: ${missing.join(', ')}`,
    });
  }

  const phone = String(b.phone).replace(/\D/g, '');
  if (phone.length < 10) {
    return res.status(422).json({ status: 'rejected', reason: 'Phone must be 10 digits' });
  }

  try {
    const r = await pool.query(`
      INSERT INTO leads
        (campaign, vertical, status, first_name, last_name, email, phone,
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

    const leadId  = r.rows[0].id;
    const leadRef = `KRW-${campaign.toUpperCase()}-${leadId}`;

    // Respond immediately to the publisher
    res.json({ status: 'received', leadId: leadRef, message: 'Lead accepted' });

    // Resolve buyer endpoint and forward in background
    const envEndpoint = process.env[`BUYER_ENDPOINT_${campaign.toUpperCase()}`];
    if (envEndpoint) {
      forwardToBuyer(leadId, leadRef, campaign, b, envEndpoint);
    } else {
      pool.query(
        `SELECT apex_endpoint FROM campaigns WHERE slug = $1 AND active = true`,
        [campaign]
      ).then(cr => {
        const endpoint = cr.rows[0]?.apex_endpoint;
        if (endpoint) {
          forwardToBuyer(leadId, leadRef, campaign, b, endpoint);
        } else {
          console.log(
            `Lead ${leadRef} stored. No buyer endpoint for campaign: ${campaign}. ` +
            `Set BUYER_ENDPOINT_${campaign.toUpperCase()} or configure in Campaigns tab.`
          );
        }
      }).catch(() => {
        console.log(`Lead ${leadRef} stored. No buyer endpoint configured for: ${campaign}`);
      });
    }
  } catch (err) {
    console.error('Lead intake error:', err.message);
    res.status(500).json({ status: 'error', reason: err.message });
  }
});

// ── Lead read endpoints ───────────────────────────────

// GET /leads/summary — KPI counts for the dashboard
app.get('/leads/summary', requireKey, async (req, res) => {
  try {
    const weekAgo    = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
    const monthStart = new Date().toISOString().slice(0, 7) + '-01';
    const [todayQ, weekQ, monthQ, statusQ] = await Promise.all([
      pool.query(`SELECT campaign, COUNT(*) AS count FROM leads WHERE received_at::date = CURRENT_DATE GROUP BY campaign`),
      pool.query(`SELECT COUNT(*) AS count FROM leads WHERE received_at::date >= $1`, [weekAgo]),
      pool.query(`SELECT COUNT(*) AS count FROM leads WHERE received_at::date >= $1`, [monthStart]),
      pool.query(`SELECT status, COUNT(*) AS count FROM leads GROUP BY status ORDER BY count DESC`),
    ]);
    res.json({
      ok:        true,
      today:     todayQ.rows,
      week:      parseInt(weekQ.rows[0]?.count  || 0, 10),
      month:     parseInt(monthQ.rows[0]?.count || 0, 10),
      by_status: statusQ.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /leads/feed — paginated lead list
app.get('/leads/feed', requireKey, async (req, res) => {
  try {
    const { campaign, status, limit = 100 } = req.query;
    const where = [], params = [];
    let i = 1;
    if (campaign) { where.push(`campaign = $${i++}`); params.push(campaign); }
    if (status)   { where.push(`status = $${i++}`);   params.push(status); }
    params.push(parseInt(limit, 10));
    const wc = where.length ? 'WHERE ' + where.join(' AND ') : '';
    const r  = await pool.query(
      `SELECT id, received_at, campaign, first_name, last_name, email, phone, state,
              status, zapier_status, buyer_intake_id, buyer_error, publisher_sub
       FROM leads ${wc}
       ORDER BY received_at DESC
       LIMIT $${i}`,
      params
    );
    res.json({ ok: true, count: r.rows.length, leads: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /leads/export/:campaign — CSV download
app.get('/leads/export/:campaign', requireKey, async (req, res) => {
  try {
    const { campaign } = req.params;
    const { from, to } = req.query;
    const where = [`campaign = $1`], params = [campaign];
    let i = 2;
    if (from) { where.push(`received_at::date >= $${i++}`); params.push(from); }
    if (to)   { where.push(`received_at::date <= $${i++}`); params.push(to); }
    const r = await pool.query(
      `SELECT id, received_at, first_name, last_name, email, phone,
              street, city, state, zip, notes, status,
              buyer_intake_id, publisher_sub, websource, trusted_form_url, jornaya_id
       FROM leads
       WHERE ${where.join(' AND ')}
       ORDER BY received_at DESC`,
      params
    );
    const headers = [
      'ID', 'Received', 'First', 'Last', 'Email', 'Phone',
      'Street', 'City', 'State', 'Zip', 'Notes', 'Status',
      'Buyer ID', 'Publisher', 'Source', 'TrustedForm', 'Jornaya',
    ];
    const rows = r.rows.map(row =>
      [
        `KRW-${campaign.toUpperCase()}-${row.id}`,
        row.received_at,
        row.first_name, row.last_name, row.email, row.phone,
        row.street || '', row.city || '', row.state || '', row.zip || '',
        row.notes  || '', row.status,
        row.buyer_intake_id || '', row.publisher_sub || '',
        row.websource || '', row.trusted_form_url || '', row.jornaya_id || '',
      ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(',')
    );
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${campaign}-leads-${new Date().toISOString().split('T')[0]}.csv"`
    );
    res.send([headers.join(','), ...rows].join('\n'));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════
//  CALL / REVENUE TRACKING
// ══════════════════════════════════════════════════════

// POST /calls/postback
// Partner endpoint — records an inbound call with pending status.
// Accepts API_KEY or LEAD_API_KEY.
app.post('/calls/postback', requireLeadKey, async (req, res) => {
  try {
    const b = req.body || {};
    const { caller_id, call_date, publisher_sub } = b;

    if (!caller_id || !call_date || !publisher_sub) {
      return res.status(422).json({
        error: 'Missing required fields: caller_id, call_date, publisher_sub',
      });
    }

    await pool.query(
      `INSERT INTO calls
         (caller_id, call_date, publisher_sub, received_at,
          call_status_label, source_system, payout_amount, raw)
       VALUES ($1, $2, $3, NOW(), 'pending', 'partner', 0, $4::jsonb)`,
      [caller_id, call_date, publisher_sub, JSON.stringify(b)]
    );

    console.log(
      `[calls/postback] Inserted call — caller: ${caller_id},` +
      ` pub: ${publisher_sub}, date: ${call_date}, status: pending`
    );
    res.json({ ok: true, message: 'Call recorded' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /calls/update
// End-of-day sweep — partner posts final billable status for each call.
// Matched by DID + call_date; updates existing records or inserts new ones.
// Accepts API_KEY, LEAD_API_KEY, PARTNER_API_KEY, or BUYER_API_KEY.
app.patch('/calls/update', requirePartnerKey, async (req, res) => {
  const updates = req.body.calls || [req.body];
  const results = { updated: 0, inserted: 0, errors: 0 };

  for (const item of updates) {
    try {
      const did      = String(item.did || item.DID || '').replace(/\D/g, '');
      const callDate = item.call_date || item.callDate || new Date().toISOString().split('T')[0];
      const billable = item.billable === true || item.billable === 'true' || item.billable === 1;
      const statusLabel = billable ? 'cpa' : 'not_converted';

      if (!did) { results.errors++; continue; }

      // Resolve publisher from DID if not supplied directly
      let pubSub = item.publisher_sub || null;
      if (!pubSub) {
        const pub = await publisherFromDID(did);
        if (pub) pubSub = pub.pub_id;
      }

      // Resolve payout: use supplied value, then publisher rate
      let payout = parseFloat(item.payout_amount || item.payout || 0);
      if (!payout && billable && pubSub) {
        const rateQ = await pool.query(
          `SELECT payout_rate FROM publishers WHERE pub_id = $1 LIMIT 1`,
          [pubSub]
        );
        if (rateQ.rows.length) payout = parseFloat(rateQ.rows[0].payout_rate || 0);
      }

      const callerName  = item.caller_name || item.name       || null;
      const callerId    = String(item.caller_id || item.phone || '').replace(/\D/g, '');
      const duration    = parseInt(item.call_duration || item.duration || 0, 10);
      const disposition = item.disposition || item.status     || null;
      const campaign    = item.campaign    || item.campaign_name || 'SSDI';

      // Try to update an existing pending record matched by publisher + date
      const updateQ = await pool.query(
        `UPDATE calls SET
           billable          = $1,
           call_status_label = $2,
           payout_amount     = $3,
           caller_name       = COALESCE($7,  caller_name),
           caller_id         = COALESCE(NULLIF($8,  ''), caller_id),
           call_duration     = COALESCE(NULLIF($9,  0),  call_duration),
           disposition       = COALESCE($10, disposition),
           campaign_name     = COALESCE($11, campaign_name)
         WHERE publisher_sub     = $4
           AND call_date         = $5
           AND (call_status_label = 'pending' OR caller_id = $6)
         RETURNING id`,
        [
          billable, statusLabel, billable ? payout : null,
          pubSub, callDate, callerId,
          callerName, callerId, duration || null, disposition, campaign,
        ]
      );

      if (updateQ.rowCount > 0) {
        results.updated++;
        console.log(
          `Updated call: pub=${pubSub} did=${did} date=${callDate}` +
          ` billable=${billable} payout=${payout}`
        );
      } else {
        // No existing record — insert a new one
        await pool.query(
          `INSERT INTO calls
             (call_date, caller_id, caller_name, call_duration,
              billable, publisher_sub, payout_amount, campaign_name,
              disposition, call_status_label, source_system, raw)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'partner',$11)`,
          [
            callDate, callerId, callerName, duration || null,
            billable, pubSub, billable ? payout : null, campaign,
            disposition, statusLabel, JSON.stringify(item),
          ]
        );
        console.log(`Inserted new call: pub=${pubSub} did=${did} date=${callDate}`);
        results.inserted++;
      }
    } catch (err) {
      console.error('calls/update item error:', err.message);
      results.errors++;
    }
  }

  // Send end-of-day email summary
  const totalCalls = results.updated + results.inserted;
  const convRate   = totalCalls > 0 ? Math.round((results.updated / totalCalls) * 100) : 0;
  const yesterday  = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const dateStr = yesterday.toLocaleDateString('en-US', {
    weekday: 'long', month: 'long', day: 'numeric', year: 'numeric',
  });

  const html =
    `<p>You had <strong>${totalCalls}</strong> unique SSDI transfers yesterday, ${dateStr}. ` +
    `Out of those <strong>${totalCalls}</strong> calls, <strong>${results.updated}</strong> ` +
    `converted at <strong>${convRate}%</strong>.</p>` +
    `<p style="color:#999;font-size:12px;margin-top:16px">KRW Marketing Solutions</p>`;

  await sendEmailNotification(`SSDI Daily Report — ${dateStr}`, html);

  res.json({ ok: true, message: 'End-of-day sweep complete', ...results });
});

// POST /trackdrive/postback
// TrackDrive webhook — records FE calls for invoicing.
// Accepts API_KEY, LEAD_API_KEY, or TRACKDRIVE_API_KEY.
app.post('/trackdrive/postback', requirePartnerKey, async (req, res) => {
  const b = req.body || {};

  const callDate   = b.call_date   || b.date     || new Date().toISOString().split('T')[0];
  const callerId   = b.caller_id   || b.ani      || b.phone    || null;
  const callerName = b.caller_name || b.name     || null;
  const duration   = parseInt(b.call_duration || b.duration || 0, 10);
  const billable   = b.billable === true || b.billable === 'true' || b.billable === 1;
  const pubSub     = b.publisher_sub || b.pub_id || null;
  const payout     = parseFloat(b.payout_amount || b.payout || 0);
  const buyerName  = b.buyer_name  || b.buyer    || null;
  const vertical   = b.vertical   || b.campaign  || 'FE';
  const campaign   = b.campaign_name || vertical;

  try {
    await pool.query(
      `INSERT INTO calls
         (call_date, caller_id, caller_name, call_duration, billable,
          publisher_sub, payout_amount, buyer_name, vertical, campaign_name,
          source_system, raw)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'trackdrive',$11)`,
      [callDate, callerId, callerName, duration, billable,
       pubSub, payout, buyerName, vertical, campaign, JSON.stringify(b)]
    );
    res.json({ ok: true, message: 'TrackDrive call recorded' });
  } catch (err) {
    console.error('TrackDrive postback error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /postback (legacy alias — kept for backwards compatibility)
app.post('/postback', requireKey, async (req, res) => {
  try {
    const b        = req.body?.querystring || req.body || {};
    const field    = (...keys) => { for (const k of keys) { if (b[k] !== undefined && b[k] !== '') return b[k]; } return ''; };
    const vertical = field('Vertical Campaign Name', 'vertical');
    const buyer    = field('Buyer Name', 'buyer_name', 'buyer');
    const revenue  = parseFloat(field('Payout Amount', 'payout_amount', 'revenue')) || 0;
    const callerId = field('Caller ID', 'caller_id', 'phone');
    const callDate = field('Call Date Time', 'call_date');
    const campId   = field('Campaign ID', 'campaign_id');
    await pool.query(
      `INSERT INTO calls
         (call_date, vertical, buyer_name, caller_id, payout_amount, campaign_id, billable, raw)
       VALUES ($1,$2,$3,$4,$5,$6,true,$7)`,
      [callDate, vertical, buyer, callerId, revenue, campId, JSON.stringify(b)]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Call read / update endpoints ──────────────────────

// GET /calls — full call list with optional date range filter
app.get('/calls', requireKey, async (req, res) => {
  try {
    const { limit = 100, from, to } = req.query;
    const where = [], params = [];
    let i = 1;
    if (from) { where.push(`call_date >= $${i++}`); params.push(from); }
    if (to)   { where.push(`call_date <= $${i++}`); params.push(to); }
    params.push(parseInt(limit, 10));
    const wc = where.length ? 'WHERE ' + where.join(' AND ') : '';
    const r  = await pool.query(
      `SELECT * FROM calls ${wc} ORDER BY received_at DESC LIMIT $${i}`,
      params
    );
    res.json({ ok: true, calls: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /calls/feed — partner calls for the dashboard (last N days)
app.get('/calls/feed', requireKey, async (req, res) => {
  try {
    const { days = 30, pub } = req.query;
    const daysInt = parseInt(days, 10) >= 9999 ? 36500 : parseInt(days, 10);
    const params  = [];
    let query = `SELECT * FROM calls WHERE source_system = 'partner'`;
    if (daysInt < 9999) {
      query += ` AND received_at >= NOW() - INTERVAL '${daysInt} days'`;
    }
    if (pub) {
      params.push(pub);
      query += ` AND publisher_sub = $${params.length}`;
    }
    query += ' ORDER BY received_at DESC LIMIT 1000';
    const r = await pool.query(query, params);
    res.json({ ok: true, calls: r.rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /calls/summary — KPI counts for the dashboard
app.get('/calls/summary', requireKey, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        COUNT(*)                                                    AS total,
        COUNT(*) FILTER (WHERE billable = true)                     AS billable,
        COUNT(*) FILTER (WHERE DATE(received_at) = CURRENT_DATE)    AS today,
        COALESCE(SUM(payout_amount) FILTER (WHERE billable = true), 0) AS total_payout
      FROM calls
      WHERE source_system = 'partner'
        AND received_at >= NOW() - INTERVAL '30 days'
    `);
    res.json({ ok: true, ...r.rows[0] });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /summary (legacy alias)
app.get('/summary', requireKey, async (req, res) => {
  try {
    const today    = new Date().toISOString().split('T')[0];
    const weekAgo  = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
    const monthStart = new Date().toISOString().slice(0, 7) + '-01';
    const [todayQ, weekQ, monthQ] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(payout_amount),0) AS total, COUNT(*) AS calls FROM calls WHERE call_date = $1 AND billable = true`, [today]),
      pool.query(`SELECT COALESCE(SUM(payout_amount),0) AS total, COUNT(*) AS calls FROM calls WHERE call_date >= $1 AND billable = true`, [weekAgo]),
      pool.query(`SELECT COALESCE(SUM(payout_amount),0) AS total, COUNT(*) AS calls FROM calls WHERE call_date >= $1 AND billable = true`, [monthStart]),
    ]);
    res.json({
      ok:    true,
      today: { total: parseFloat(todayQ.rows[0].total), calls: parseInt(todayQ.rows[0].calls, 10) },
      week:  { total: parseFloat(weekQ.rows[0].total),  calls: parseInt(weekQ.rows[0].calls,  10) },
      month: { total: parseFloat(monthQ.rows[0].total), calls: parseInt(monthQ.rows[0].calls, 10) },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /invoice-summary — billable calls grouped by buyer for invoicing
app.get('/invoice-summary', requireKey, async (req, res) => {
  try {
    const { from, to } = req.query;
    const where  = [`billable = true`, `invoice_status = 'pending'`, `payout_amount > 0`, `source_system = 'trackdrive'`];
    const params = [];
    let i = 1;
    if (from) { where.push(`received_at::date >= $${i++}`); params.push(from); }
    if (to)   { where.push(`received_at::date <= $${i++}`); params.push(to); }
    const calls = await pool.query(
      `SELECT * FROM calls WHERE ${where.join(' AND ')} ORDER BY received_at DESC`,
      params
    );
    const byBuyer = {};
    calls.rows.forEach(c => {
      const k = `${c.buyer_name || 'Unknown'}|${c.vertical || ''}`;
      if (!byBuyer[k]) {
        byBuyer[k] = {
          buyer_name:  c.buyer_name || 'Unknown',
          vertical:    c.vertical   || '',
          calls:       [],
          total_owed:  0,
          call_count:  0,
        };
      }
      byBuyer[k].calls.push(c);
      byBuyer[k].total_owed += parseFloat(c.payout_amount || 0);
      byBuyer[k].call_count++;
    });
    const by_buyer   = Object.values(byBuyer);
    const total_owed = by_buyer.reduce((s, b) => s + b.total_owed, 0);
    res.json({
      ok:     true,
      by_buyer,
      calls:  calls.rows,
      totals: { total_owed, call_count: calls.rows.length },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /calls/:id — update invoice fields
app.patch('/calls/:id', requireKey, async (req, res) => {
  try {
    const { id } = req.params;
    const { invoice_status, invoice_id, invoice_date, paid_date } = req.body;
    const sets = [], params = [];
    let i = 1;
    if (invoice_status !== undefined) { sets.push(`invoice_status = $${i++}`); params.push(invoice_status); }
    if (invoice_id     !== undefined) { sets.push(`invoice_id = $${i++}`);     params.push(invoice_id); }
    if (invoice_date   !== undefined) { sets.push(`invoice_date = $${i++}`);   params.push(invoice_date); }
    if (paid_date      !== undefined) { sets.push(`paid_date = $${i++}`);      params.push(paid_date); }
    if (!sets.length) return res.status(400).json({ error: 'Nothing to update' });
    params.push(id);
    await pool.query(`UPDATE calls SET ${sets.join(', ')} WHERE id = $${i}`, params);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /calls/:id/billable — toggle billable flag
app.patch('/calls/:id/billable', requireKey, async (req, res) => {
  try {
    const billable = req.body.billable === true || req.body.billable === 'true';
    await pool.query(
      `UPDATE calls SET billable = $1 WHERE id = $2`,
      [billable, req.params.id]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /send-invoice — proxy to Zapier webhook
app.post('/send-invoice', requireKey, async (req, res) => {
  try {
    const { zapier_webhook, ...payload } = req.body;
    if (!zapier_webhook) return res.status(400).json({ error: 'zapier_webhook required' });
    const r = await fetch(zapier_webhook, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });
    res.json({ ok: true, zapier_status: r.status });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════
//  CAMPAIGN MANAGEMENT
// ══════════════════════════════════════════════════════

// GET /campaigns — list all active campaigns (dashboard)
app.get('/campaigns', requireKey, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT * FROM campaigns WHERE active = true ORDER BY created_at ASC`
    );
    res.json({ ok: true, campaigns: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /campaigns/:slug/config — public config used by publisher forms
app.get('/campaigns/:slug/config', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT * FROM campaigns WHERE slug = $1 AND active = true`,
      [req.params.slug.toLowerCase()]
    );
    if (!r.rows.length) {
      return res.status(404).json({ error: `Campaign not found: ${req.params.slug}` });
    }
    const row = r.rows[0];
    res.json({
      ok:          true,
      slug:        row.slug,
      name:        row.name,
      vertical:    row.vertical    || '',
      description: row.description || '',
      required:    row.required_fields,
      optional:    row.optional_fields,
      fieldLabels: row.field_labels || {},
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /campaigns — create or update (upsert by slug)
app.post('/campaigns', requireKey, async (req, res) => {
  try {
    const {
      slug, name, vertical, apex_endpoint, payout,
      buyer_notes, required_fields, optional_fields,
      field_labels, description,
    } = req.body;
    if (!slug || !name) return res.status(400).json({ error: 'slug and name required' });
    const r = await pool.query(`
      INSERT INTO campaigns
        (slug, name, vertical, apex_endpoint, payout, buyer_notes,
         required_fields, optional_fields, field_labels, description)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (slug) DO UPDATE SET
        name            = $2,
        vertical        = $3,
        apex_endpoint   = $4,
        payout          = $5,
        buyer_notes     = $6,
        required_fields = $7,
        optional_fields = $8,
        field_labels    = $9,
        description     = $10,
        updated_at      = NOW()
      RETURNING *
    `, [
      slug.toLowerCase(), name, vertical || '', apex_endpoint || '',
      parseFloat(payout) || 0, buyer_notes || null,
      JSON.stringify(required_fields || ['firstName', 'lastName', 'email', 'phone']),
      JSON.stringify(optional_fields || []),
      JSON.stringify(field_labels    || {}),
      description || null,
    ]);
    res.json({ ok: true, campaign: r.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /campaigns/:slug — partial update
app.patch('/campaigns/:slug', requireKey, async (req, res) => {
  try {
    const slug    = req.params.slug.toLowerCase();
    const allowed = [
      'name', 'vertical', 'apex_endpoint', 'payout', 'buyer_notes',
      'required_fields', 'optional_fields', 'field_labels', 'description', 'active',
    ];
    const jsonCols = ['required_fields', 'optional_fields', 'field_labels'];
    const sets = [], params = [];
    let i = 1;
    allowed.forEach(col => {
      if (req.body[col] !== undefined) {
        sets.push(`${col} = $${i++}`);
        params.push(jsonCols.includes(col) ? JSON.stringify(req.body[col]) : req.body[col]);
      }
    });
    if (!sets.length) return res.status(400).json({ error: 'Nothing to update' });
    sets.push('updated_at = NOW()');
    params.push(slug);
    await pool.query(`UPDATE campaigns SET ${sets.join(', ')} WHERE slug = $${i}`, params);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════
//  PUBLISHER MANAGEMENT
// ══════════════════════════════════════════════════════

// POST /publishers/login — verify publisher credentials (pub_id lookup)
app.post('/publishers/login', async (req, res) => {
  const { pub_id } = req.body || {};
  if (!pub_id) return res.status(400).json({ ok: false, error: 'pub_id required' });
  try {
    const r = await pool.query(
      `SELECT * FROM publishers WHERE pub_id = $1 AND active = true`,
      [pub_id]
    );
    if (!r.rows.length) return res.status(401).json({ ok: false, error: 'Publisher not found' });
    const pub = r.rows[0];
    res.json({
      ok:       true,
      name:     pub.name,
      pub_id:   pub.pub_id,
      campaign: pub.campaign,
      did:      pub.did || null,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /publishers/:pub_id/calls — call history for a publisher
app.get('/publishers/:pub_id/calls', async (req, res) => {
  const { pub_id } = req.params;
  const { days = 30, billable_only } = req.query;
  try {
    const pubCheck = await pool.query(
      `SELECT * FROM publishers WHERE pub_id = $1 AND active = true`,
      [pub_id]
    );
    if (!pubCheck.rows.length) return res.status(401).json({ ok: false, error: 'Unauthorized' });

    const daysInt = parseInt(days, 10) >= 9999 ? 36500 : parseInt(days, 10);
    let query =
      `SELECT id, call_date, caller_id, caller_name,
              call_duration, billable, call_status_label, disposition,
              payout_amount, campaign_name, received_at
       FROM calls
       WHERE publisher_sub = $1
         AND source_system = 'partner'`;
    if (daysInt < 9999) query += ` AND received_at >= NOW() - INTERVAL '${daysInt} days'`;
    if (billable_only === 'true') query += ' AND billable = true';
    query += ' ORDER BY received_at DESC LIMIT 500';

    const r = await pool.query(query, [pub_id]);
    const billable_count = r.rows.filter(c => c.billable).length;
    const total_payout   = r.rows
      .filter(c => c.billable)
      .reduce((sum, c) => sum + parseFloat(c.payout_amount || 0), 0);

    res.json({
      ok:             true,
      calls:          r.rows,
      total:          r.rows.length,
      billable_count,
      total_payout:   total_payout.toFixed(2),
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /publishers — list all publishers (dashboard)
app.get('/publishers', requireKey, async (req, res) => {
  try {
    const r = await pool.query(`SELECT * FROM publishers ORDER BY created_at DESC`);
    res.json({ ok: true, publishers: r.rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /publishers — create or update publisher (upsert by pub_id)
app.post('/publishers', requireKey, async (req, res) => {
  const { pub_id, name, email, campaign, did, payout_rate } = req.body || {};
  if (!pub_id || !name) return res.status(400).json({ ok: false, error: 'pub_id and name required' });
  try {
    const r = await pool.query(
      `INSERT INTO publishers (pub_id, name, email, campaign, did, payout_rate)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (pub_id) DO UPDATE SET
         name        = $2,
         email       = $3,
         campaign    = $4,
         did         = $5,
         payout_rate = $6,
         active      = true
       RETURNING *`,
      [pub_id, name, email || null, campaign || null, did || null, parseFloat(payout_rate || 0)]
    );
    res.json({ ok: true, publisher: r.rows[0] });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// DELETE /publishers/:pub_id — soft-delete (sets active = false)
app.delete('/publishers/:pub_id', requireKey, async (req, res) => {
  try {
    await pool.query(
      `UPDATE publishers SET active = false WHERE pub_id = $1`,
      [req.params.pub_id]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ══════════════════════════════════════════════════════
//  SERVER START
// ══════════════════════════════════════════════════════

initDB()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server listening on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set:  ${!!process.env.API_KEY}`);
      console.log(`LEAD_KEY set: ${!!process.env.LEAD_API_KEY}`);
      console.log(`DB set:       ${!!process.env.DATABASE_URL}`);
      console.log(`SMTP set:     ${!!process.env.SMTP_USER}`);
    });
  })
  .catch(err => {
    console.error('Failed to start server:', err.message);
    process.exit(1);
  });
