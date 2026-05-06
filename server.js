// KRW Marketing Solutions — Postback Receiver v3
// Invoice-ready schema. Handles existing DB gracefully.

const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app  = express();
const PORT = parseInt(process.env.PORT || '3000', 10);

// ── CORS ──────────────────────────────────────────────────
app.use(function(req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PATCH');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key, Authorization');
  if (req.method === 'OPTIONS') { res.sendStatus(200); return; }
  next();
});
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Database ───────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ── Auth ───────────────────────────────────────────────────
function requireKey(req, res, next) {
  const provided = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  const expected = (process.env.API_KEY || '').trim();
  if (!expected) return res.status(500).json({ error: 'API_KEY not set' });
  if (provided !== expected) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── Safe column add helper ─────────────────────────────────
async function addCol(col, type) {
  try {
    await pool.query(`ALTER TABLE calls ADD COLUMN IF NOT EXISTS ${col} ${type}`);
  } catch(e) {
    // ignore — column already exists
  }
}

// ── Init DB ────────────────────────────────────────────────
async function initDB() {
  // Step 1: Create table with minimal safe schema
  await pool.query(`
    CREATE TABLE IF NOT EXISTS calls (
      id           SERIAL PRIMARY KEY,
      received_at  TIMESTAMPTZ DEFAULT NOW(),
      vertical     TEXT DEFAULT 'Unknown',
      buyer        TEXT DEFAULT 'Unknown buyer',
      campaign     TEXT,
      campaign_id  TEXT,
      caller_id    TEXT,
      duration     INTEGER DEFAULT 0,
      revenue      NUMERIC(10,2) DEFAULT 0,
      disposition  TEXT,
      billable     BOOLEAN DEFAULT true,
      raw          JSONB
    );
  `);

  // Step 2: Add new invoice columns one at a time — safe if they already exist
  await addCol('call_date',       'DATE');
  await addCol('call_datetime',   'TIMESTAMPTZ');
  await addCol('call_duration',   'INTEGER DEFAULT 0');
  await addCol('campaign_name',   'TEXT');
  await addCol('buyer_name',      'TEXT DEFAULT \'Unknown buyer\'');
  await addCol('buyer_id',        'TEXT');
  await addCol('supplier_name',   'TEXT');
  await addCol('payout_amount',   'NUMERIC(10,2) DEFAULT 0');
  await addCol('cost',            'NUMERIC(10,2) DEFAULT 0');
  await addCol('profit',          'NUMERIC(10,2) DEFAULT 0');
  await addCol('call_status',     'TEXT');
  await addCol('invoice_id',      'TEXT');
  await addCol('invoice_status',  'TEXT DEFAULT \'pending\'');
  await addCol('invoice_date',    'DATE');
  await addCol('paid_date',       'DATE');

  // Step 3: Create indexes (safe — IF NOT EXISTS)
  const indexes = [
    'CREATE INDEX IF NOT EXISTS idx_calls_received   ON calls (received_at)',
    'CREATE INDEX IF NOT EXISTS idx_calls_call_date  ON calls (call_date)',
    'CREATE INDEX IF NOT EXISTS idx_calls_vertical   ON calls (vertical)',
    'CREATE INDEX IF NOT EXISTS idx_calls_buyer_name ON calls (buyer_name)',
    'CREATE INDEX IF NOT EXISTS idx_calls_billable   ON calls (billable)',
    'CREATE INDEX IF NOT EXISTS idx_calls_inv_status ON calls (invoice_status)',
  ];
  for (const idx of indexes) {
    try { await pool.query(idx); } catch(e) {}
  }

  console.log('✅ DB ready');
}

// ── Field extractor ────────────────────────────────────────
function field(b, ...keys) {
  for (const k of keys) {
    const variants = [k, k.toLowerCase(), 'Querystring ' + k,
                      'querystring ' + k, k.replace(/_/g,' '), k.replace(/ /g,'_')];
    for (const v of variants) {
      if (b[v] !== undefined && b[v] !== null && b[v] !== '')
        return String(b[v]).trim();
    }
  }
  return '';
}

function parseDate(str) {
  if (!str) return null;
  // MM/DD/YY or MM/DD/YYYY from TrackDrive
  const slash = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (slash) {
    let [,m,d,y] = slash;
    if (y.length === 2) y = '20' + y;
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  try { return new Date(str).toISOString().split('T')[0]; } catch(e) { return null; }
}

// ── ROUTES ─────────────────────────────────────────────────

app.get('/',       (req, res) => res.json({ ok:true, service:'KRW Postback Server', time:new Date().toISOString() }));

// ── POST /postback-test — NO AUTH — shows raw body Zapier sends ────
// Use this to debug field names, then switch back to /postback
app.post('/postback-test', async (req, res) => {
  console.log('Test postback body:', JSON.stringify(req.body));
  console.log('Test postback keys:', Object.keys(req.body));
  res.json({
    ok: true,
    received_keys: Object.keys(req.body),
    received_body: req.body,
    message: 'Copy the received_keys to map fields correctly'
  });
});
app.get('/health', (req, res) => res.json({ ok:true, service:'KRW Postback Server', time:new Date().toISOString() }));
app.get('/debug',  (req, res) => res.json({
  api_key_set:    !!process.env.API_KEY,
  api_key_length: (process.env.API_KEY||'').length,
  node_env:       process.env.NODE_ENV,
  db_url_set:     !!process.env.DATABASE_URL,
}));

// ── POST /postback ─────────────────────────────────────────
app.post('/postback', requireKey, async (req, res) => {
  try {
    // TrackDrive wraps everything in a 'querystring' object
    // e.g. {"querystring": {"Buyer Name": "X", "Payout Amount": "30.0"}}
    // Flatten it so field() can find everything
    const raw = req.body;
    const qs  = raw.querystring || raw.Querystring || {};
    // Merge top-level and querystring fields together
    const b   = Object.assign({}, raw, qs);
    console.log('Postback keys:', Object.keys(b));
    console.log('Postback:', JSON.stringify(b));

    const rawDate    = field(b,'Call Date/Time','Call Date Time','call_date','call_datetime','call_start','date');
    const callDate   = parseDate(rawDate);
    const revenue    = parseFloat(field(b,'Payout Amount','payout_amount','payout','revenue','amount') || 0);
    const cost       = parseFloat(field(b,'cost','Cost','buy_price') || 0);
    const vertical   = field(b,'Vertical/Campaign Name','Vertical Campaign Name','vertical','campaign_name','Campaign Name','Campaign ID','campaign_id') || 'Unknown';
    const buyerName  = field(b,'Buyer Name','buyer_name','buyer','account') || 'Unknown buyer';
    const duration   = parseInt(field(b,'Duration','call_duration','duration','length') || 0);
    const callerId   = field(b,'Caller ID','caller_id','phone','ani','caller') || '';
    const campaignId = field(b,'Campaign ID','campaign_id','campaign') || '';
    // Default ALL calls to billable=true unless explicitly marked false
    const billableRaw = field(b,'Billable flag','Billable Flag','billable_flag','billable');
    const billable    = billableRaw === '' ? true : billableRaw !== 'false';
    const disposition= field(b,'Disposition','disposition','call_disposition') || '';

    // Write to both old columns (buyer, duration, revenue) and new columns
    // (buyer_name, call_duration, payout_amount) so everything stays in sync
    await pool.query(`
      INSERT INTO calls (
        call_date, call_duration, vertical, campaign_name, campaign_id,
        buyer, buyer_name, buyer_id, supplier_name, caller_id,
        duration, payout_amount, revenue, cost, profit,
        billable, disposition, invoice_status, raw
      ) VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,
        $16,$17,$18,$19
      )`,
      [
        callDate, duration, vertical, vertical, campaignId,
        buyerName, buyerName, '', '', callerId,
        duration, revenue, revenue, cost, revenue - cost,
        billable, disposition, 'pending', JSON.stringify(b)
      ]
    );

    console.log(`✅ ${vertical} | ${buyerName} | $${revenue} | ${callerId}`);
    res.json({ ok:true, stored:{ vertical, buyer:buyerName, revenue, call_date:callDate, billable } });

  } catch (err) {
    console.error('Postback error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /summary ───────────────────────────────────────────
app.get('/summary', requireKey, async (req, res) => {
  try {
    const weekAgo    = new Date(Date.now()-7*86400000).toISOString().split('T')[0];
    const monthStart = new Date().toISOString().slice(0,7)+'-01';

    // Check which columns actually exist in the DB
    const colCheck = await pool.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_name='calls'
    `);
    const cols = colCheck.rows.map(r => r.column_name);
    const hasBuyerName    = cols.includes('buyer_name');
    const hasPayoutAmount = cols.includes('payout_amount');
    const hasInvStatus    = cols.includes('invoice_status');

    const revCol    = hasPayoutAmount ? 'COALESCE(payout_amount, revenue, 0)' : 'COALESCE(revenue, 0)';
    const buyerCol  = hasBuyerName    ? "COALESCE(buyer_name, buyer, 'Unknown buyer')" : "COALESCE(buyer, 'Unknown buyer')";
    const invFilter = hasInvStatus    ? "(invoice_status='pending' OR invoice_status IS NULL)" : 'true';

    const [todayQ, weekQ, monthQ, vertQ, buyerQ] = await Promise.all([
      pool.query(`SELECT COALESCE(vertical,'Unknown') as vertical,
                  ${buyerCol} as buyer_name,
                  COUNT(*) as calls, COALESCE(SUM(${revCol}),0) as revenue
                  FROM calls WHERE received_at::date=CURRENT_DATE AND billable=true
                  GROUP BY 1,2 ORDER BY revenue DESC`),
      pool.query(`SELECT COALESCE(SUM(${revCol}),0) as total, COUNT(*) as calls
                  FROM calls WHERE received_at::date>=$1 AND billable=true`,[weekAgo]),
      pool.query(`SELECT COALESCE(SUM(${revCol}),0) as total, COUNT(*) as calls
                  FROM calls WHERE received_at::date>=$1 AND billable=true`,[monthStart]),
      pool.query(`SELECT COALESCE(vertical,'Unknown') as vertical,
                  COUNT(*) as total_calls,
                  COALESCE(SUM(${revCol}),0) as total_revenue
                  FROM calls WHERE billable=true
                  GROUP BY 1 ORDER BY total_revenue DESC`),
      pool.query(`SELECT ${buyerCol} as buyer_name,
                  COUNT(*) as total_calls,
                  COALESCE(SUM(${revCol}),0) as total_revenue,
                  COUNT(CASE WHEN ${invFilter} THEN 1 END) as pending_count,
                  COALESCE(SUM(CASE WHEN ${invFilter} THEN ${revCol} ELSE 0 END),0) as pending_amount
                  FROM calls WHERE billable=true GROUP BY 1 ORDER BY total_revenue DESC`),
    ]);

    const todayByVert = {};
    todayQ.rows.forEach(r => {
      const v = r.vertical || 'Unknown';
      if (!todayByVert[v]) todayByVert[v] = {vertical:v, calls:0, revenue:0};
      todayByVert[v].calls   += parseInt(r.calls   || 0);
      todayByVert[v].revenue += parseFloat(r.revenue || 0);
    });

    res.json({
      ok: true,
      today: {
        date:        new Date().toISOString().split('T')[0],
        by_vertical: Object.values(todayByVert).sort((a,b)=>b.revenue-a.revenue),
        by_buyer:    todayQ.rows,
        total:       todayQ.rows.reduce((s,r)=>s+parseFloat(r.revenue||0),0),
        calls:       todayQ.rows.reduce((s,r)=>s+parseInt(r.calls||0),0),
      },
      week:        { total:parseFloat(weekQ.rows[0]?.total||0),  calls:parseInt(weekQ.rows[0]?.calls||0) },
      month:       { total:parseFloat(monthQ.rows[0]?.total||0), calls:parseInt(monthQ.rows[0]?.calls||0) },
      by_vertical: vertQ.rows,
      by_buyer:    buyerQ.rows,
    });
  } catch(err) {
    console.error('Summary error:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /calls ─────────────────────────────────────────────
app.get('/calls', requireKey, async (req, res) => {
  try {
    const { limit=100, vertical, buyer, date, invoice_status } = req.query;
    let where=[], params=[], i=1;
    if (vertical)       { where.push(`vertical=$${i++}`);            params.push(vertical); }
    if (buyer)          { where.push(`COALESCE(buyer_name,buyer)=$${i++}`); params.push(buyer); }
    if (date)           { where.push(`received_at::date=$${i++}`);   params.push(date); }
    if (invoice_status) { where.push(`invoice_status=$${i++}`);      params.push(invoice_status); }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(
      `SELECT *, COALESCE(buyer_name,buyer,'Unknown buyer') as buyer_display,
              COALESCE(payout_amount,revenue,0) as payout_display
       FROM calls ${wc} ORDER BY received_at DESC LIMIT $${i}`, params
    );
    res.json({ ok:true, count:r.rows.length, calls:r.rows });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /invoice-summary ───────────────────────────────────
// Groups by buyer_name + vertical combination
// Only includes billable calls with payout > 0 and status = pending
app.get('/invoice-summary', requireKey, async (req, res) => {
  try {
    const { buyer, vertical, from, to } = req.query;

    // Base filters — only real billable calls with actual revenue
    let where = [
      'billable = true',
      'COALESCE(payout_amount, revenue, 0) > 0',
      "(COALESCE(invoice_status, 'pending') = 'pending')",
    ];
    let params = [], i = 1;
    if (buyer)    { where.push(`COALESCE(buyer_name, buyer, '') = $${i++}`); params.push(buyer); }
    if (vertical) { where.push(`vertical = $${i++}`);                        params.push(vertical); }
    if (from)     { where.push(`received_at::date >= $${i++}`);              params.push(from); }
    if (to)       { where.push(`received_at::date <= $${i++}`);              params.push(to); }
    const wc = 'WHERE ' + where.join(' AND ');

    // Group by buyer + vertical — each unique pair becomes one invoice row
    const byBuyerQ = await pool.query(`
      SELECT
        COALESCE(buyer_name, buyer, 'Unknown buyer') AS buyer_name,
        COALESCE(vertical, 'Unknown')                AS vertical,
        COUNT(*)                                     AS call_count,
        COALESCE(SUM(COALESCE(payout_amount, revenue, 0)), 0) AS total_owed,
        MIN(received_at::date)                       AS period_start,
        MAX(received_at::date)                       AS period_end
      FROM calls
      ${wc}
      GROUP BY
        COALESCE(buyer_name, buyer, 'Unknown buyer'),
        COALESCE(vertical, 'Unknown')
      ORDER BY total_owed DESC
    `, params);

    // Individual call line items for the expandable rows
    const callsQ = await pool.query(`
      SELECT
        id,
        received_at::date                                     AS call_date,
        COALESCE(vertical, 'Unknown')                         AS vertical,
        COALESCE(buyer_name, buyer, 'Unknown buyer')          AS buyer_name,
        COALESCE(caller_id, '')                               AS caller_id,
        COALESCE(call_duration, duration, 0)                  AS call_duration,
        COALESCE(payout_amount, revenue, 0)                   AS payout_amount,
        COALESCE(disposition, '')                             AS disposition,
        COALESCE(invoice_status, 'pending')                   AS invoice_status,
        COALESCE(invoice_id, '')                              AS invoice_id,
        COALESCE(campaign_name, campaign_id, '')              AS campaign_name,
        billable,
        received_at
      FROM calls
      ${wc}
      ORDER BY
        COALESCE(buyer_name, buyer, 'Unknown buyer'),
        COALESCE(vertical, 'Unknown'),
        received_at DESC
    `, params);

    const totalOwed = callsQ.rows.reduce((s, r) => s + parseFloat(r.payout_amount || 0), 0);

    res.json({
      ok:       true,
      by_buyer: byBuyerQ.rows,
      calls:    callsQ.rows,
      totals: {
        call_count:  callsQ.rows.length,
        total_owed:  totalOwed,
        buyer_count: byBuyerQ.rows.length,
      }
    });
  } catch(err) {
    console.error('Invoice summary error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PATCH /calls/:id ───────────────────────────────────────
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
    if (!sets.length) return res.status(400).json({ error: 'Nothing to update' });
    params.push(id);
    await pool.query(`UPDATE calls SET ${sets.join(',')} WHERE id=$${i}`, params);
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});


// ── PATCH /calls/:id/billable ──────────────────────────────
// Toggle billable — called from invoice page mark unbillable button
app.patch('/calls/:id/billable', requireKey, async (req, res) => {
  try {
    const { id }       = req.params;
    const { billable } = req.body;
    if (billable === undefined) return res.status(400).json({ error: 'billable required' });
    await pool.query('UPDATE calls SET billable=$1 WHERE id=$2',
      [billable === true || billable === 'true', parseInt(id)]);
    console.log(`Call ${id} billable → ${billable}`);
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});


// ── POST /send-invoice ─────────────────────────────────────────────
// Receives invoice payload from dashboard and forwards to Zapier
// Avoids CORS issues when dashboard runs as a local file
app.post('/send-invoice', requireKey, async (req, res) => {
  try {
    const { zapier_webhook, ...invoicePayload } = req.body;
    if (!zapier_webhook) {
      return res.status(400).json({ error: 'zapier_webhook URL is required' });
    }

    // Forward to Zapier from the server (no CORS issues)
    const https = require('https');
    const http  = require('http');
    const url   = new URL(zapier_webhook);
    const body  = JSON.stringify(invoicePayload);

    const options = {
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname + url.search,
      method:   'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };

    const proto = url.protocol === 'https:' ? https : http;
    const zapReq = proto.request(options, (zapRes) => {
      let data = '';
      zapRes.on('data', chunk => data += chunk);
      zapRes.on('end', () => {
        console.log(`Invoice forwarded to Zapier: ${invoicePayload.buyer_name} $${invoicePayload.total_amount}`);
        res.json({ ok: true, zapier_status: zapRes.statusCode, message: 'Invoice sent to Zapier successfully' });
      });
    });

    zapReq.on('error', (err) => {
      console.error('Zapier forward error:', err.message);
      res.status(500).json({ error: 'Failed to forward to Zapier: ' + err.message });
    });

    zapReq.write(body);
    zapReq.end();

  } catch (err) {
    console.error('Send invoice error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /dashboard — serves the dashboard HTML file ─────────────
app.get('/dashboard', (req, res) => {
  const path = require('path');
  const fs   = require('fs');
  const file = path.join(__dirname, 'dashboard.html');
  if (!fs.existsSync(file)) {
    return res.status(404).send(`
      <html><body style="font-family:sans-serif;padding:2rem;max-width:500px;margin:0 auto">
        <h2 style="color:#c0392b">dashboard.html not found</h2>
        <p>Upload your <strong>dashboard.html</strong> file to your GitHub repo alongside server.js and redeploy.</p>
        <p>Rename <code>deal-dashboard.html</code> to <code>dashboard.html</code> before uploading.</p>
      </body></html>
    `);
  }
  res.setHeader('Content-Type', 'text/html');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(file);
});


// ════════════════════════════════════════════════════════════
//  LEADS — Campaign Lead Intake System
//  Receives leads from publishers → validates → stores in DB
//  → forwards directly to buyer's Apex endpoint
// ════════════════════════════════════════════════════════════

// ── Campaign config (add new campaigns here) ─────────────────
const CAMPAIGNS = {
  depo: {
    name:        'DEPO — Lead Tree (WTC)',
    vertical:    'Mass Tort - Depo',
    apexEndpoint:'https://apex-services-nbd7z6aa7a-uc.a.run.app/intake/depo/depo/zapier/tuell/submit',
    websource:   'https://krwmarketing.github.io/depo',
    required:    ['firstName','lastName','email','phone'],
    optional:    ['street','city','state','zip','notes','trustedFormCertUrl','jornayaLeadId','facebookLeadId','publisherSub'],
  },
  // Add more campaigns here later:
  // mva: { name:'MVA', vertical:'MVA', apexEndpoint:'...', required:[...] },
};

// ── DB init — create leads + campaigns tables ─────────────────
async function initLeadsDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS leads (
      id              SERIAL PRIMARY KEY,
      received_at     TIMESTAMPTZ DEFAULT NOW(),
      campaign        TEXT NOT NULL,
      vertical        TEXT,
      status          TEXT DEFAULT 'received',
      -- Claimant info
      first_name      TEXT,
      last_name       TEXT,
      email           TEXT,
      phone           TEXT,
      street          TEXT,
      city            TEXT,
      state           TEXT,
      zip             TEXT,
      notes           TEXT,
      -- Compliance
      trusted_form_url TEXT,
      jornaya_id      TEXT,
      facebook_lead_id TEXT,
      -- Tracking
      publisher_sub   TEXT,
      websource       TEXT,
      -- Buyer response
      buyer_status    TEXT,
      buyer_intake_id TEXT,
      buyer_error     TEXT,
      -- Raw payload
      raw             JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_leads_campaign    ON leads (campaign);
    CREATE INDEX IF NOT EXISTS idx_leads_received    ON leads (received_at);
    CREATE INDEX IF NOT EXISTS idx_leads_status      ON leads (status);
  `);
  console.log('✅ Leads table ready');
}

// ── Lead API key auth ─────────────────────────────────────────
function requireLeadKey(req, res, next) {
  const key = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  const exp = (process.env.LEAD_API_KEY || process.env.API_KEY || '').trim();
  if (!exp || key !== exp) {
    return res.status(401).json({ status:'rejected', reason:'Invalid API key' });
  }
  next();
}

// ── POST /lead/:campaign ──────────────────────────────────────
// Publishers post leads here. Validates, stores, forwards to buyer.
app.post('/lead/:campaign', requireLeadKey, async (req, res) => {
  const slug = req.params.campaign.toLowerCase();
  const cfg  = CAMPAIGNS[slug];
  if (!cfg) return res.status(404).json({ status:'rejected', reason:`Unknown campaign: ${slug}` });

  const b = req.body || {};

  // Validate required fields
  const missing = cfg.required.filter(f => !b[f] || !String(b[f]).trim());
  if (missing.length) {
    return res.status(422).json({ status:'rejected', reason:`Missing required fields: ${missing.join(', ')}` });
  }

  // Basic email validation
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(b.email)) {
    return res.status(422).json({ status:'rejected', reason:'Invalid email format' });
  }

  // Phone: strip non-digits, require 10
  const phone = String(b.phone).replace(/\D/g,'');
  if (phone.length < 10) {
    return res.status(422).json({ status:'rejected', reason:'Phone must be 10 digits' });
  }

  // Build clean lead record
  const lead = {
    campaign:          slug,
    vertical:          cfg.vertical,
    status:            'received',
    first_name:        String(b.firstName||'').trim(),
    last_name:         String(b.lastName ||'').trim(),
    email:             String(b.email    ||'').trim().toLowerCase(),
    phone:             phone,
    street:            b.street    || null,
    city:              b.city      || null,
    state:             b.state     || null,
    zip:               b.zip       || null,
    notes:             b.notes     || null,
    trusted_form_url:  b.trustedFormCertUrl || null,
    jornaya_id:        b.jornayaLeadId      || null,
    facebook_lead_id:  b.facebookLeadId     || null,
    publisher_sub:     b.publisherSub       || null,
    websource:         b.websource || cfg.websource,
    raw:               JSON.stringify(b),
  };

  // Insert into DB
  let leadId;
  try {
    const ins = await pool.query(`
      INSERT INTO leads (campaign,vertical,status,first_name,last_name,email,phone,
        street,city,state,zip,notes,trusted_form_url,jornaya_id,facebook_lead_id,
        publisher_sub,websource,raw)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
      RETURNING id
    `, [lead.campaign,lead.vertical,lead.status,lead.first_name,lead.last_name,
        lead.email,lead.phone,lead.street,lead.city,lead.state,lead.zip,lead.notes,
        lead.trusted_form_url,lead.jornaya_id,lead.facebook_lead_id,
        lead.publisher_sub,lead.websource,lead.raw]);
    leadId = ins.rows[0].id;
  } catch (dbErr) {
    console.error('❌ DB insert failed for lead:', dbErr.message);
    return res.status(500).json({ status:'error', reason:'Failed to store lead' });
  }

  // Respond to publisher immediately — don't make them wait on Apex
  res.json({ status:'received', leadId:`KRW-DEPO-${leadId}`, message:'Lead accepted' });

  // Forward to buyer's Apex endpoint in background
  forwardToApex(leadId, lead, cfg).catch(console.error);
});

// ── Forward to Apex buyer endpoint ───────────────────────────
async function forwardToApex(leadId, lead, cfg) {
  try {
    await pool.query(`UPDATE leads SET status='forwarding' WHERE id=$1`, [leadId]);

    const payload = {
      firstName: lead.first_name,
      lastName:  lead.last_name,
      email:     lead.email,
      phone:     lead.phone,
      street:    lead.street,
      city:      lead.city,
      state:     lead.state,
      zip:       lead.zip,
      notes:     lead.notes,
      websource: lead.websource,
      meta: {
        id:               `KRW-DEPO-${leadId}`,
        Timestamp:        new Date().toISOString(),
        createDt:         new Date().toISOString(),
        claimant:         `${lead.first_name} ${lead.last_name}`,
        websource:        lead.websource,
        trustedFormCertUrl: lead.trusted_form_url,
        jornayaLeadId:    lead.jornaya_id,
      },
    };

    const resp = await fetch(cfg.apexEndpoint, {
      method:  'POST',
      headers: { 'Content-Type':'application/json' },
      body:    JSON.stringify(payload),
    });

    const rawText = await resp.text();
    console.log(`📨 Apex raw response for KRW-DEPO-${leadId} (HTTP ${resp.status}): ${rawText}`);
    console.log(`📋 Apex websource sent: ${lead.websource}`);

    let data;
    try {
      data = JSON.parse(rawText);
    } catch (parseErr) {
      console.error(`❌ Lead KRW-DEPO-${leadId} — Apex returned non-JSON: ${rawText}`);
      await pool.query(
        `UPDATE leads SET status='forward_failed', buyer_error=$1 WHERE id=$2`,
        [`Non-JSON response: ${rawText}`, leadId]
      );
      return;
    }

    if (data.status === 'Ok') {
      await pool.query(
        `UPDATE leads SET status='forwarded', buyer_status='Success', buyer_intake_id=$1 WHERE id=$2`,
        [String(data.ids?.[0]||''), leadId]
      );
      console.log(`✅ Lead KRW-DEPO-${leadId} forwarded to Apex. Buyer ID: ${data.ids?.[0]}`);
    } else {
      const buyerError = data.statusDetail || data.message || data.error || JSON.stringify(data);
      await pool.query(
        `UPDATE leads SET status='buyer_rejected', buyer_status='Failed', buyer_error=$1 WHERE id=$2`,
        [buyerError, leadId]
      );
      console.log(`⚠️  Lead KRW-DEPO-${leadId} rejected by buyer: ${buyerError}`);
      console.log(`⚠️  Full Apex rejection payload: ${JSON.stringify(data)}`);
    }
  } catch (err) {
    await pool.query(
      `UPDATE leads SET status='forward_failed', buyer_error=$1 WHERE id=$2`,
      [err.message, leadId]
    );
    console.error(`❌ Lead KRW-DEPO-${leadId} forward failed: ${err.message}`);
  }
}

// ── GET /leads/summary ────────────────────────────────────────
app.get('/leads/summary', requireKey, async (req, res) => {
  try {
    const today     = new Date().toISOString().split('T')[0];
    const weekAgo   = new Date(Date.now()-7*86400000).toISOString().split('T')[0];
    const monthStart= today.slice(0,7)+'-01';

    const [todayQ,weekQ,monthQ,statusQ,campaignQ] = await Promise.all([
      pool.query(`SELECT campaign, COUNT(*) as count FROM leads WHERE received_at::date=CURRENT_DATE GROUP BY campaign`),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1`,[weekAgo]),
      pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$1`,[monthStart]),
      pool.query(`SELECT status, COUNT(*) as count FROM leads GROUP BY status ORDER BY count DESC`),
      pool.query(`SELECT campaign, COUNT(*) as total, COUNT(CASE WHEN status='forwarded' THEN 1 END) as forwarded, COUNT(CASE WHEN status='buyer_rejected' THEN 1 END) as rejected FROM leads GROUP BY campaign`),
    ]);

    res.json({
      ok: true,
      today:      todayQ.rows,
      week:       parseInt(weekQ.rows[0]?.count||0),
      month:      parseInt(monthQ.rows[0]?.count||0),
      by_status:  statusQ.rows,
      by_campaign:campaignQ.rows,
    });
  } catch(err) {
    res.status(500).json({ error:err.message });
  }
});

// ── GET /leads/feed ───────────────────────────────────────────
app.get('/leads/feed', requireKey, async (req, res) => {
  try {
    const { campaign, status, limit=50 } = req.query;
    let where=[], params=[], i=1;
    if (campaign) { where.push(`campaign=$${i++}`); params.push(campaign); }
    if (status)   { where.push(`status=$${i++}`);   params.push(status); }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(
      `SELECT id,received_at,campaign,vertical,first_name,last_name,email,phone,
              state,status,buyer_status,buyer_intake_id,buyer_error,
              trusted_form_url,jornaya_id,publisher_sub,websource
       FROM leads ${wc} ORDER BY received_at DESC LIMIT $${i}`, params
    );
    res.json({ ok:true, count:r.rows.length, leads:r.rows });
  } catch(err) {
    res.status(500).json({ error:err.message });
  }
});

// ── GET /leads/export/:campaign ───────────────────────────────
// Returns CSV for publisher sharing
app.get('/leads/export/:campaign', requireKey, async (req, res) => {
  try {
    const { campaign } = req.params;
    const { from, to } = req.query;
    let where=[`campaign=$1`], params=[campaign], i=2;
    if (from) { where.push(`received_at::date>=$${i++}`); params.push(from); }
    if (to)   { where.push(`received_at::date<=$${i++}`); params.push(to); }

    const r = await pool.query(
      `SELECT id,received_at,first_name,last_name,email,phone,street,city,state,zip,
              notes,status,buyer_status,buyer_intake_id,publisher_sub,websource,
              trusted_form_url,jornaya_id,facebook_lead_id
       FROM leads WHERE ${where.join(' AND ')} ORDER BY received_at DESC`, params
    );

    const headers = ['ID','Received','First Name','Last Name','Email','Phone',
      'Street','City','State','Zip','Notes','Status','Buyer Status','Buyer ID',
      'Publisher Sub','Websource','TrustedForm','Jornaya','Facebook Lead ID'];

    const rows = r.rows.map(row => [
      `KRW-${campaign.toUpperCase()}-${row.id}`,
      row.received_at, row.first_name, row.last_name, row.email, row.phone,
      row.street||'', row.city||'', row.state||'', row.zip||'', row.notes||'',
      row.status, row.buyer_status||'', row.buyer_intake_id||'',
      row.publisher_sub||'', row.websource||'',
      row.trusted_form_url||'', row.jornaya_id||'', row.facebook_lead_id||'',
    ].map(v => `"${String(v).replace(/"/g,'""')}"`).join(','));

    const csv = [headers.join(','), ...rows].join('\n');
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${campaign}-leads-${new Date().toISOString().split('T')[0]}.csv"`);
    res.send(csv);
  } catch(err) {
    res.status(500).json({ error:err.message });
  }
});

// ── START ──────────────────────────────────────────────────
initDB()
  .then(() => initLeadsDB())
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set: ${!!process.env.API_KEY}`);
      console.log(`DB set: ${!!process.env.DATABASE_URL}`);
    });
  })
  .catch(err => {
    console.error('Failed to start:', err.message);
    process.exit(1);
  });
