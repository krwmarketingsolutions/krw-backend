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
      payout_amount   NUMERIC(10,2),
      revenue         NUMERIC(10,2),
      cost            NUMERIC(10,2),
      profit          NUMERIC(10,2),
      billable        BOOLEAN DEFAULT true,
      disposition     TEXT,
      call_status     TEXT,
      invoice_status  TEXT DEFAULT 'pending',
      invoice_id      TEXT,
      invoice_date    TEXT,
      paid_date       TEXT,
      raw             JSONB
    );
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

    // Hardcoded LP credentials per campaign (always reliable)
    const LP_CREDS = {
      'mva':      { id: '31080',  sup: '110928', key: 'ke21sx0koi7dld' },
      'rideshare':{ id: '31036',  sup: '99237',  key: 'jz2gawz23t17g5' },
      'lyft':     { id: '31036',  sup: '99237',  key: 'jz2gawz23t17g5' },
      'uber':     { id: '31036',  sup: '99237',  key: 'jz2gawz23t17g5' },
      'roundup':  { id: '30976',  sup: '96279',  key: '6l5rtdz61ay1n2' },
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
        have_attorney:    data.haveAttorney    || null,
        used_roundup:     data.usedRoundup     || null,
        which_cancer:     data.whichCancer     || null,
        what_year:        data.whatYear        || null,
        exposed_location: data.exposedLocation || null,
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

    const resp = await fetch(buyerUrl, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });

    const result = await resp.json().catch(() => ({ status: resp.status }));

    const accepted = result.status === 'ACCEPTED' || result.status === 'Success' ||
                     result.status === 'success' || result.ok === true;
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
    const where=['billable=true',"invoice_status='pending'","payout_amount>0"], params=[];
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


// ── Start ─────────────────────────────────────────────
initDB()
  .then(() => initLeadsDB())
  .then(() => initCampaignsDB())
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set: ${!!process.env.API_KEY}`);
      console.log(`LEAD_KEY set: ${!!process.env.LEAD_API_KEY}`);
      console.log(`DB set: ${!!process.env.DATABASE_URL}`);
    });
  })
  .catch(err => { console.error('Failed to start:', err.message); process.exit(1); });
