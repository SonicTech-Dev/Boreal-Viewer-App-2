/**
 Updated server.js — station naming strictly based on serial mapping (TOPIC_TO_SERIAL).
 
 Change: /api/remote_stations now determines station display names strictly from the serials
 derived from TOPIC_TO_SERIAL (or MQTT_TOPIC_2_SERIAL / MQTT_TOPIC_3_SERIAL). It does NOT
 use IP addresses to decide the display name — IPs are still returned in the api response
 as an "ip" field when available, but they are not used to compute the display label.
 
 All other logic in the file is kept unchanged.
*/

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { exec } = require('child_process');
const { Pool } = require('pg');
const path = require('path');

const { insertLosData, fetchLosData } = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Config
const MQTT_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';
const MQTT_PREFIX = process.env.MQTT_PREFIX || 'BivicomData';
const MQTT_TOPIC_2 = process.env.MQTT_TOPIC_2 || `${MQTT_PREFIX}2`;
const MQTT_TOPIC_3 = process.env.MQTT_TOPIC_3 || `${MQTT_PREFIX}3`;
const DATA_TOPICS = [MQTT_TOPIC_2, MQTT_TOPIC_3];
const MODBUS_TOPIC = process.env.MODBUS_TOPIC || 'modbus/gsm_signal';
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : (process.env.PORT || 3000);
const SAVE_MQTT_TO_DB = (process.env.SAVE_MQTT_TO_DB === '1') || false;
const NOTIFY_COOLDOWN_SECS = parseInt(process.env.NOTIFY_COOLDOWN_SECS || '60', 10);

// Option A serial mapping: environment variables to map topic -> serial when payload lacks serial.
// Provide MQTT_TOPIC_2_SERIAL and MQTT_TOPIC_3_SERIAL in .env to match your devices.
// Defaults are the example serials used previously.
const MQTT_TOPIC_2_SERIAL = process.env.MQTT_TOPIC_2_SERIAL || 'B452A25032102';
const MQTT_TOPIC_3_SERIAL = process.env.MQTT_TOPIC_3_SERIAL || 'B452A25032103';

// Example device IP mapping (for ping loop) - these are defaults (fallbacks)
const DEFAULT_DEVICE_IP_MAPPING = {
  'B452A25032102': '10.0.0.42',
  'B452A25032103': '10.0.0.43',
};

// Topic -> serial mapping (explicit mapping as requested)
// This mapping is authoritative for the web UI station list and for inferring serials by topic.
const TOPIC_TO_SERIAL = {
  [MQTT_TOPIC_2]: MQTT_TOPIC_2_SERIAL,
  [MQTT_TOPIC_3]: MQTT_TOPIC_3_SERIAL,
};

// Ping configuration
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || String(30 * 1000), 10); // defaults 30s
const PING_ATTEMPTS = parseInt(process.env.PING_ATTEMPTS || '3', 10); // attempts per cycle
const PING_TIMEOUT_SECONDS = parseInt(process.env.PING_TIMEOUT_SECONDS || '5', 10); // per-attempt timeout (s)
const PING_DELAY_BETWEEN_MS = parseInt(process.env.PING_DELAY_BETWEEN_MS || String(5 * 1000), 10); // 5s between attempts

// Postgres config (used for thresholds + device tokens). Adjust through env.
const PG_CONFIG = {
  host: process.env.PG_HOST || 'localhost',
  port: process.env.PG_PORT ? parseInt(process.env.PG_PORT, 10) : 5432,
  user: process.env.PG_USER || 'boreal_user',
  password: process.env.PG_PASSWORD || 'password',
  database: process.env.PG_DATABASE || 'boreal_app',
};
const pool = new Pool(PG_CONFIG);

// Serve static assets
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Build device IP mapping: prefer REMOTE_STATIONS_JSON or DEVICE_IP env, but only as fallback data for serials
let envDeviceIpMap = {};
if (process.env.REMOTE_STATIONS_JSON) {
  try {
    const parsed = JSON.parse(process.env.REMOTE_STATIONS_JSON);
    if (typeof parsed === 'object' && parsed !== null) {
      envDeviceIpMap = parsed;
    }
  } catch (e) {
    console.warn('Failed parsing REMOTE_STATIONS_JSON, ignoring:', e && e.message ? e.message : e);
    envDeviceIpMap = {};
  }
}
// If a single DEVICE_IP env is provided, map it to DEVICE_SERIAL (if provided)
if (Object.keys(envDeviceIpMap).length === 0) {
  const deviceIp = process.env.DEVICE_IP || null;
  const deviceSerial = process.env.DEVICE_SERIAL || null;
  if (deviceIp && deviceSerial) {
    envDeviceIpMap[deviceSerial] = deviceIp;
  }
}

// Now create the effective deviceIPMapping used for ping loop and for returning ip in /api/remote_stations.
// The authoritative station list comes from TOPIC_TO_SERIAL (serials derived from topics).
// For each serial from TOPIC_TO_SERIAL, pick IP from envDeviceIpMap if present, else from DEFAULT_DEVICE_IP_MAPPING if present.
// Then also include any additional serials present in envDeviceIpMap that are not part of TOPIC_TO_SERIAL.
const deviceIPMapping = {};
// Primary: serials from TOPIC_TO_SERIAL
for (const serial of Object.values(TOPIC_TO_SERIAL)) {
  if (!serial) continue;
  if (envDeviceIpMap[serial]) {
    deviceIPMapping[serial] = envDeviceIpMap[serial];
  } else if (DEFAULT_DEVICE_IP_MAPPING[serial]) {
    deviceIPMapping[serial] = DEFAULT_DEVICE_IP_MAPPING[serial];
  } else {
    // leave undefined (no ip) — ping loop will skip entries without an IP
  }
}
// Secondary: include any env-provided serials not already added (these are not used to compute display labels)
for (const [serial, ip] of Object.entries(envDeviceIpMap)) {
  if (!deviceIPMapping[serial]) deviceIPMapping[serial] = ip;
}
// Final fallback: if still empty, seed DEFAULT_DEVICE_IP_MAPPING
if (Object.keys(deviceIPMapping).length === 0) {
  Object.assign(deviceIPMapping, DEFAULT_DEVICE_IP_MAPPING);
}

// Firebase Admin init (optional). If you have a service account JSON, set SERVICE_ACCOUNT_PATH env to its path.
let admin;
try {
  admin = require('firebase-admin');
  const saPath = process.env.SERVICE_ACCOUNT_PATH;
  if (saPath) {
    const serviceAccount = require(path.resolve(saPath));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
    console.log('Firebase admin initialized from SERVICE_ACCOUNT_PATH');
  } else {
    // Try default require (common in original project)
    try {
      const serviceAccount = require('./serviceAccountKey.json');
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
      });
      console.log('Firebase admin initialized from ./serviceAccountKey.json');
    } catch (e) {
      // Not fatal — notifications will be skipped if admin not initialized
      console.warn('Firebase admin not initialized (no service account). Notifications will be disabled if not configured.');
    }
  }
} catch (e) {
  // If firebase-admin isn't available, warn but continue
  console.warn('firebase-admin module not available; notifications disabled.', e && e.message ? e.message : e);
}

// --- helpers for canonicalizing keys + extracting LOS ---
function canonicalKey(k) {
  return String(k || '').toLowerCase().replace(/[\s\-\(\)_]/g, '');
}

function extractCanonicalLos(params) {
  const fieldVariants = {
    los_temp: ['LoS-Temp(c)', 'LoS-Temp', 'LoS-Temp(C)', 'lostemp', 'los_temp'],
    los_rx_light: ['LoS-Rx Light', 'LoS-RxLight', 'LoS Rx Light', 'losrxlight'],
    los_r2: ['LoS- R2', 'LoS-R2', 'LoS - R2', 'losr2'],
    los_heartbeat: ['LoS-HeartBeat', 'LoS- HeartBeat', 'losheartbeat'],
    los_ppm: ['LoS - PPM', 'LoS- PPM', 'LoS-PPM', 'los_ppm', 'ppm', 'losppm'],
  };

  const result = {};
  for (const [field, variants] of Object.entries(fieldVariants)) {
    for (const key of Object.keys(params || {})) {
      for (const variant of variants) {
        if (canonicalKey(key) === canonicalKey(variant)) {
          result[field] = params[key];
          break;
        }
      }
      if (result[field] !== undefined) break;
    }
  }

  // fallback: any key containing ppm-like sequence
  for (const key of Object.keys(params || {})) {
    const ck = canonicalKey(key);
    if ((ck === 'ppm' || ck === 'losppm' || ck.endsWith('ppm')) && result.los_ppm === undefined) {
      result.los_ppm = params[key];
    }
  }

  return result;
}

/**
 * Map a database row (which may contain weird column names like "LoS-Temp(c)" or "LoS - PPM")
 * into the normalized shape expected by the frontend / query-client:
 *  - los_temp
 *  - los_rx_light
 *  - los_r2
 *  - los_heartbeat
 *  - los_ppm
 *  - recorded_at, recorded_at_str
 */
function mapDbRowToApi(row) {
  if (!row || typeof row !== 'object') return row;
  const cMap = {};
  for (const k of Object.keys(row)) {
    cMap[canonicalKey(k)] = row[k];
  }

  const pick = (variants) => {
    for (const v of variants) {
      const val = cMap[canonicalKey(v)];
      if (val !== undefined) return val;
    }
    return undefined;
  };

  const out = Object.assign({}, row); // keep other fields like id, serial_number if present

  // normalized fields
  out.los_temp = pick(['LoS-Temp(c)', 'LoS-Temp', 'LoS-Temp(C)', 'lostemp', 'los_temp']);
  out.los_rx_light = pick(['LoS-Rx Light', 'LoS-RxLight', 'LoS Rx Light', 'losrxlight', 'los_rx_light']);
  out.los_r2 = pick(['LoS- R2', 'LoS-R2', 'LoS - R2', 'losr2', 'los_r2']);
  out.los_heartbeat = pick(['LoS-HeartBeat', 'LoS- HeartBeat', 'losheartbeat', 'los_heartbeat']);
  out.los_ppm = pick(['LoS - PPM', 'LoS- PPM', 'LoS-PPM', 'los_ppm', 'ppm', 'losppm']);

  // Coerce numeric-looking fields to numbers where appropriate; if not present set null
  ['los_temp', 'los_rx_light', 'los_r2', 'los_heartbeat', 'los_ppm'].forEach((f) => {
    if (out[f] === undefined) {
      out[f] = null;
    } else if (out[f] === null) {
      // leave null
    } else {
      const n = Number(out[f]);
      if (!Number.isNaN(n)) out[f] = n;
      // if NaN, keep the original (rare); frontend can handle empty/non-numeric
    }
  });

  // Normalize recorded_at / recorded_at_str
  out.recorded_at = row.recorded_at || row.recorded_at_raw || row.recorded_at_str || null;
  if (out.recorded_at && out.recorded_at instanceof Date) {
    out.recorded_at_str = out.recorded_at.toISOString();
  } else if (typeof out.recorded_at === 'string') {
    out.recorded_at_str = out.recorded_at;
  } else if (out.recorded_at === null) {
    out.recorded_at_str = null;
  } else {
    try {
      const d = new Date(out.recorded_at);
      if (!Number.isNaN(d.getTime())) out.recorded_at_str = d.toISOString();
      else out.recorded_at_str = String(out.recorded_at);
    } catch (e) {
      out.recorded_at_str = String(out.recorded_at);
    }
  }

  return out;
}

/**
 * The authoritative int/dec detection + merging helpers based on the original single-file server.js you provided.
 */

// normalize key for detection: lower + drop non-alphanumeric (matches original logic)
function normalizeKeyForMatch(k) {
  if (!k) return '';
  return String(k).toLowerCase().replace(/[^a-z0-9]/g, '');
}

// find integer and decimal parts for PPM in params (tolerant matching)
function findLosPpmParts(params) {
  let intPart;
  let decPart;

  for (const key of Object.keys(params || {})) {
    const nk = normalizeKeyForMatch(key);
    const raw = params[key];

    // Try to coerce numeric-ish values
    const num = raw === null || raw === undefined ? NaN : Number(raw);
    // if it's not numeric, skip
    if (Number.isNaN(num)) continue;

    // decimal key detection: contains 'ppm' and 'dec' or 'decimal'
    if (nk.includes('ppm') && (nk.includes('dec') || nk.includes('decimal'))) {
      decPart = num;
      continue;
    }

    // integer key detection: contains 'ppm' and ('int' or 'mlo' or nothing else but not 'dec')
    if (nk.includes('ppm') && (nk.includes('int') || nk.includes('mlo') || nk === 'losppm' || nk === 'losppmmloint' || nk.includes('losppm'))) {
      intPart = num;
      continue;
    }

    // Additional tolerant detection: key mentioning ppm but not dec -> treat as integer
    if (nk.includes('ppm') && !nk.includes('dec') && !nk.includes('decimal')) {
      if (typeof intPart === 'undefined') intPart = num;
    }
  }

  return { intPart, decPart };
}

// merge int and decimal parts to form numeric with at most 2 fractional digits
function mergeIntAndDec(intVal, decVal) {
  if (typeof intVal === 'undefined' || intVal === null || Number.isNaN(Number(intVal))) return undefined;
  // If decimal part missing, just return integer
  if (typeof decVal === 'undefined' || decVal === null || Number.isNaN(Number(decVal))) {
    return Number(intVal);
  }

  // Convert decimal to string digits only (drop any sign or decimal separators).
  let decStr = String(Math.abs(decVal)).replace(/[^0-9]/g, '');
  if (decStr.length === 0) decStr = '0';

  // Restrict to at most 2 digits of fraction as requested.
  if (decStr.length > 2) decStr = decStr.slice(0, 2);

  // divisor is 10^digits, but treat single-digit decimal as hundredths (e.g., '5' => 0.05)
  const digits = decStr.length;
  const divisor = digits === 1 ? 100 : 10 ** digits;
  const fraction = Number(decStr) / divisor;

  const sign = Number(intVal) < 0 ? -1 : 1;
  const merged = Number(intVal) + sign * fraction;

  // Round to 2 decimal places to avoid floating precision surprises
  return Number(merged.toFixed(2));
}

// --- Notifications via Firebase Admin and thresholds ---
async function sendNotificationToAll(title, message, data = {}) {
  if (!admin || !admin.messaging) {
    console.warn('Firebase admin SDK not available — skipping notifications.');
    return;
  }
  try {
    const { rows } = await pool.query('SELECT token FROM mobile_devices');
    const tokens = rows.map(r => r.token).filter(Boolean);
    if (!tokens.length) {
      console.log('No registered device tokens found — skipping notifications.');
      return;
    }

    const msg = {
      tokens,
      notification: {
        title,
        body: message,
      },
      data: data || {},
    };

    const response = await admin.messaging().sendEachForMulticast(msg);
    console.log(`sendMulticast: success=${response.successCount} failure=${response.failureCount}`);

    const tokensToRemove = [];
    response.responses.forEach((r, idx) => {
      if (!r.success) {
        const err = r.error;
        const code = err && (err.code || (err.errorInfo && err.errorInfo.code));
        if (code && (code.includes('registration-token-not-registered') || code.includes('invalid-registration-token'))) {
          tokensToRemove.push(tokens[idx]);
        } else {
          console.warn(`Failed sending to token index ${idx}:`, err);
        }
      }
    });

    if (tokensToRemove.length) {
      try {
        const q = `DELETE FROM mobile_devices WHERE token = ANY($1::text[])`;
        await pool.query(q, [tokensToRemove]);
        console.log(`Removed ${tokensToRemove.length} invalid token(s) from database.`);
      } catch (delErr) {
        console.error('Error deleting invalid tokens:', delErr);
      }
    }
  } catch (err) {
    console.error('Error in sendNotificationToAll:', err);
  }
}

/**
 * Get LOS threshold, preferring serial-specific thresholds.
 * 1) Try thresholds WHERE serial_number = $1
 * 2) Fallback to global thresholds WHERE serial_number IS NULL
 * 3) Fallback to any thresholds (legacy)
 * Picks indicator containing 'ppm' first, else first numeric threshold.
 */
async function getLosThreshold(serialNumber) {
  try {
    function pickThresholdFromRows(rows) {
      if (!rows || rows.length === 0) return null;
      for (const row of rows) {
        const ind = (row.indicator || '').toString().toLowerCase();
        const thr = row.threshold;
        if (!ind) continue;
        if (ind.includes('ppm')) {
          const t = Number(thr);
          if (!Number.isNaN(t)) return t;
        }
      }
      for (const row of rows) {
        const t = Number(row.threshold);
        if (!Number.isNaN(t)) return t;
      }
      return null;
    }

    if (serialNumber) {
      const q = 'SELECT indicator, threshold FROM thresholds WHERE serial_number = $1';
      const res = await pool.query(q, [serialNumber]);
      const t = pickThresholdFromRows(res.rows);
      if (t !== null) return t;
    }

    const qGlobal = 'SELECT indicator, threshold FROM thresholds WHERE serial_number IS NULL';
    const resGlobal = await pool.query(qGlobal);
    const tGlobal = pickThresholdFromRows(resGlobal.rows);
    if (tGlobal !== null) return tGlobal;

    const qAny = 'SELECT indicator, threshold FROM thresholds';
    const resAny = await pool.query(qAny);
    const tAny = pickThresholdFromRows(resAny.rows);
    if (tAny !== null) return tAny;

    return null;
  } catch (err) {
    console.error('getLosThreshold error:', err && err.message ? err.message : err);
    return null;
  }
}

// In-memory rate-limiter for notifications per serial
const lastNotificationAt = new Map();
function canNotify(serial) {
  if (!serial) return true;
  const now = Date.now();
  const last = lastNotificationAt.get(serial) || 0;
  if (now - last >= NOTIFY_COOLDOWN_SECS * 1000) {
    lastNotificationAt.set(serial, now);
    return true;
  }
  return false;
}

// --- device ping utilities (multi-attempt) ---
const deviceStatus = new Map();

function pingOnce(ip, timeoutSeconds) {
  return new Promise((resolve) => {
    if (!ip) return resolve({ online: false, rtt: null, error: 'no ip' });
    const cmd = `ping -c 1 -W ${timeoutSeconds} ${ip}`;
    exec(cmd, { timeout: (timeoutSeconds + 2) * 1000 }, (err, stdout, stderr) => {
      if (!err && stdout) {
        const m = stdout.match(/time=([0-9.]+)\s*ms/);
        const rtt = m ? Number(m[1]) : null;
        resolve({ online: true, rtt, raw: stdout });
      } else {
        resolve({ online: false, rtt: null, error: err ? err.message : stderr || 'no output' });
      }
    });
  });
}

async function pingMultipleAttempts(ip, attempts, timeoutSeconds, delayBetweenMs) {
  for (let i = 0; i < attempts; i++) {
    try {
      const res = await pingOnce(ip, timeoutSeconds);
      if (res && res.online) {
        return { online: true, rtt: res.rtt, attempt: i + 1, raw: res.raw };
      }
    } catch (e) {
      console.error('pingOnce error:', e && e.message ? e.message : e);
    }
    if (i < attempts - 1) {
      await new Promise(r => setTimeout(r, delayBetweenMs));
    }
  }
  return { online: false, rtt: null, attempt: attempts };
}

async function checkDeviceStatus(serialNumber, ip) {
  try {
    const res = await pingMultipleAttempts(ip, PING_ATTEMPTS, PING_TIMEOUT_SECONDS, PING_DELAY_BETWEEN_MS);
    const prev = deviceStatus.get(serialNumber);
    if (prev === undefined || prev.online !== res.online) {
      deviceStatus.set(serialNumber, { online: res.online, rtt: res.rtt, when: new Date().toISOString(), attempt: res.attempt });
      const payload = { serial_number: serialNumber, ip, online: res.online, rtt: res.rtt, when: new Date().toISOString(), attempt: res.attempt };
      io.emit('device_status', payload);
      io.emit('device_ping', payload); // backward compatibility
      console.log(`Device ping: ${serialNumber} -> ${res.online ? 'ONLINE' : 'OFFLINE'} (attempt ${res.attempt})`);
    } else {
      // update timestamp and rtt even if status same (so UI can show fresh ts)
      deviceStatus.set(serialNumber, { online: res.online, rtt: res.rtt, when: new Date().toISOString(), attempt: res.attempt });
    }
  } catch (err) {
    console.error(`Error checking device ${serialNumber}:`, err && err.message ? err.message : err);
  }
}

let devicePingInterval = null;
function startDevicePingLoop(intervalMs = 30 * 1000) {
  // run once immediately
  for (const [serial, ip] of Object.entries(deviceIPMapping)) {
    checkDeviceStatus(serial, ip);
  }

  // schedule loop
  devicePingInterval = setInterval(() => {
    for (const [serial, ip] of Object.entries(deviceIPMapping)) {
      checkDeviceStatus(serial, ip);
    }
  }, intervalMs);

  process.on('exit', () => {
    if (devicePingInterval) clearInterval(devicePingInterval);
  });
  process.on('SIGINT', () => {
    if (devicePingInterval) clearInterval(devicePingInterval);
    process.exit();
  });
}

// --- routes ---
// Expose remote stations (for frontend selection)
// NOTE: Station name mapping now strictly uses serial number mapping (TOPIC_TO_SERIAL / MQTT_TOPIC_*_SERIAL).
// IPs are included in the response only as an "ip" field when available, but IPs are NOT used to determine the display label.
app.get('/api/remote_stations', (req, res) => {
  try {
    // Use unique serials from TOPIC_TO_SERIAL as the authoritative list (no fallback to IP for display)
    const serials = Array.from(new Set(Object.values(TOPIC_TO_SERIAL).filter(Boolean)));
    const rows = serials.map((serial) => {
      const ip = deviceIPMapping[serial] || null; // include ip if known, but DO NOT use it to compute display
      let display;
      if (serial === MQTT_TOPIC_2_SERIAL) {
        display = 'Station 1';
      } else if (serial === MQTT_TOPIC_3_SERIAL) {
        display = 'Station 2';
      } else {
        display = serial;
      }
      return { serial_number: serial, ip, display };
    });

    return res.json(rows);
  } catch (err) {
    console.error('GET /api/remote_stations error:', err);
    return res.status(500).json({ error: 'Failed to fetch remote stations' });
  }
});

// Keep los fetch endpoint (uses fetchLosData from ./db)
app.get('/api/los', async (req, res) => {
  const { from, to, limit, offset, serial_number } = req.query;
  let parsedLimit = 500;
  if (limit) {
    const n = parseInt(limit, 10);
    if (!Number.isNaN(n) && n > 0 && n <= 5000) parsedLimit = n;
  }
  let parsedOffset = 0;
  if (offset) {
    const n = parseInt(offset, 10);
    if (!Number.isNaN(n) && n >= 0) parsedOffset = n;
  }

  try {
    const rawRows = await fetchLosData(from || null, to || null, parsedLimit, parsedOffset, serial_number || null);
    // Map DB rows to API-friendly shape expected by query-client.js
    const rows = Array.isArray(rawRows) ? rawRows.map(mapDbRowToApi) : [];
    res.json({ ok: true, rows });
  } catch (err) {
    console.error('GET /api/los error:', err && err.message ? err.message : err);
    res.status(500).json({ ok: false, error: 'Failed to query database' });
  }
});

// Device status endpoint (returns snapshot of deviceStatus map)
app.get('/_device_status', (req, res) => {
  try {
    const snapshot = [];
    for (const [serial, st] of deviceStatus.entries()) {
      snapshot.push(Object.assign({ serial_number: serial }, st));
    }
    return res.json({ ok: true, status: snapshot });
  } catch (err) {
    console.error('_device_status error:', err);
    return res.status(500).json({ ok: false, error: 'failed' });
  }
});

// Basic health
app.get('/_health', (req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// Socket.IO connection
io.on('connection', (socket) => {
  console.log('Web client connected, id=', socket.id);
  // Emit device snapshot
  try {
    const snapshot = [];
    for (const [serial, st] of deviceStatus.entries()) {
      snapshot.push(Object.assign({ serial_number: serial }, st));
    }
    if (snapshot.length) socket.emit('device_status_snapshot', snapshot);
  } catch (e) {
    console.warn('Error emitting device status snapshot:', e && e.message ? e.message : e);
  }

  socket.on('disconnect', () => {
    console.log('Web client disconnected, id=', socket.id);
  });
});

// --- MQTT subscribe / message handling ---
const mqttOptions = {};
if (process.env.MQTT_USERNAME) mqttOptions.username = process.env.MQTT_USERNAME;
if (process.env.MQTT_PASSWORD) mqttOptions.password = process.env.MQTT_PASSWORD;
const client = mqtt.connect(MQTT_URL, mqttOptions);

client.on('connect', () => {
  console.log('Connected to MQTT broker');
  const topics = Array.from(new Set([...DATA_TOPICS, MODBUS_TOPIC]));
  client.subscribe(topics, { qos: 0 }, (err) => {
    if (err) console.error(`Subscribe error on topics "${topics.join(', ')}":`, err);
    else console.log(`Subscribed to: ${topics.join(', ')}`);
  });
});

// Main message handler
client.on('message', async (topic, payloadBuffer, packet) => {
  // Optionally ignore retained messages (prevent duplicates on startup)
  if (packet && packet.retain) {
    // remove this `return` if you want to process retained messages
    return;
  }

  const payloadString = payloadBuffer.toString();

  // Forward modbus/gsm_signal raw string as 'signal' to frontend and log emitted signal
  if (topic === MODBUS_TOPIC) {
    io.emit('signal', payloadString);
    console.log(`[${new Date().toISOString()}] Emit signal -> ${payloadString}`);
    return;
  }

  // Only process data topics configured (BivicomData2 & BivicomData3 by default)
  if (!DATA_TOPICS.includes(topic)) {
    return;
  }

  let payload = payloadString;
  try {
    payload = JSON.parse(payloadString);
  } catch (e) {
    // keep as string if not JSON
  }

  // Extract params either from payload.params or payload itself
  let params = null;
  if (payload && typeof payload === 'object') {
    params = (payload.params && typeof payload.params === 'object') ? payload.params : payload;
  }
  if (!params) params = {};

  // Extract canonical los fields
  const los = extractCanonicalLos(params) || {};

  // --- NEW: derive integer & decimal parts using authoritative detection helper ---
  const { intPart, decPart } = findLosPpmParts(params);

  // --- Topic -> serial mapping (topic overrides any serial in payload per authoritative logic) ---
  let mappedSerial = null;
  if (topic === MQTT_TOPIC_2) {
    mappedSerial = MQTT_TOPIC_2_SERIAL;
  } else if (topic === MQTT_TOPIC_3) {
    mappedSerial = MQTT_TOPIC_3_SERIAL;
  } else if (TOPIC_TO_SERIAL[topic]) {
    mappedSerial = TOPIC_TO_SERIAL[topic];
  } else {
    mappedSerial = null;
  }

  // Override serial_number with mapping (authoritative)
  let serial_number = mappedSerial || null;

  // Decide merging behavior using Station 1 detection (Option A)
  // Station 1 is the device whose serial equals MQTT_TOPIC_2_SERIAL
  try {
    if (serial_number === MQTT_TOPIC_2_SERIAL) {
      // Station 1: use integer part only (ignore decimal)
      if (typeof intPart !== 'undefined' && !Number.isNaN(Number(intPart))) {
        los.los_ppm = Number(intPart);
      }
    } else {
      // Station 2 or others: attempt merge, else fallback to integer
      const merged = mergeIntAndDec(intPart, decPart);
      if (typeof merged !== 'undefined' && !Number.isNaN(Number(merged))) {
        los.los_ppm = merged;
      } else if (typeof intPart !== 'undefined' && !Number.isNaN(Number(intPart))) {
        los.los_ppm = Number(intPart);
      }
    }
  } catch (e) {
    console.error('Error applying conditional merging logic:', e && e.message ? e.message : e);
  }

  // Use numeric ts from payload if present
  let mqttTsRaw = undefined;
  if (payload && typeof payload === 'object' && typeof payload.ts !== 'undefined') {
    mqttTsRaw = payload.ts;
  }
  const mqttTs = (typeof mqttTsRaw !== 'undefined' && mqttTsRaw !== null && !Number.isNaN(Number(mqttTsRaw)))
    ? Number(mqttTsRaw)
    : undefined;
  const mqttWhenIso = (typeof mqttTs !== 'undefined') ? new Date(mqttTs).toISOString() : new Date().toISOString();

  // normalize los_ppm numeric
  if (los && typeof los.los_ppm !== 'undefined' && los.los_ppm !== null) {
    const parsed = Number(los.los_ppm);
    if (!Number.isNaN(parsed)) los.los_ppm = parsed;
  }

  // Build emitted object for frontend
  const emitted = {
    topic,
    serial_number,
    payload,
    los,
    ts: (typeof mqttTs !== 'undefined') ? mqttTs : null,
    received_at: mqttWhenIso,
  };

  // Emit to frontend
  io.emit('mqtt_message', emitted);

  // Log only what is emitted to frontend
  try {
    console.log(`[${mqttWhenIso}] Emit mqtt_message -> ${JSON.stringify({
      topic: emitted.topic,
      serial_number: emitted.serial_number,
      los: emitted.los,
      ts: emitted.ts,
      received_at: emitted.received_at,
    })}`);
  } catch (e) {
    console.log(`[${mqttWhenIso}] Emit mqtt_message`);
  }

  // Optionally persist to DB (uses insertLosData from ./db)
  try {
    if (typeof insertLosData === 'function') {
      // Only insert if we detected los fields
      if (los && Object.keys(los).length > 0) {
        try {
          await insertLosData(topic, payload, los, mqttWhenIso, serial_number);
        } catch (err) {
          console.warn('Failed saving LOS to DB (insertLosData):', err && err.message ? err.message : err);
        }
      } else if (SAVE_MQTT_TO_DB) {
        // Optional: if configured to save all incoming MQTT messages, you can implement another DB call.
      }
    }
  } catch (err) {
    console.warn('Error in DB persistence block:', err && err.message ? err.message : err);
  }

  // Notification logic: serial-aware threshold lookup and cooldown
  try {
    const rawPpm = los.los_ppm ?? los.ppm ?? (params && (params.los_ppm ?? params.ppm));
    const ppmNumber = (typeof rawPpm === 'number') ? rawPpm : (rawPpm !== undefined ? Number(rawPpm) : NaN);
    if (!Number.isNaN(ppmNumber)) {
      const lookupDevice = serial_number || topic;
      const threshold = await getLosThreshold(lookupDevice);
      if (threshold !== null && !Number.isNaN(threshold) && ppmNumber > Number(threshold)) {
        if (canNotify(lookupDevice)) {
          const title = lookupDevice ? `Alarm: ${lookupDevice}` : 'Alarm: Gas Finder';
          const message = `PPM ${ppmNumber} exceeded threshold ${threshold}`;
          await sendNotificationToAll(title, message, { serial_number: lookupDevice || '' });
          io.emit('alarm', { serial_number: lookupDevice, ppm: ppmNumber, threshold, ts: mqttTs });
          console.log(`ALARM triggered for ${lookupDevice || '(unknown)'}: ppm=${ppmNumber} threshold=${threshold} ts=${mqttTs !== undefined ? mqttTs : '(none)'}`);
        } else {
          console.log(`Alarm suppressed by cooldown for ${lookupDevice || '(unknown)'}`);
        }
      }
    }
  } catch (err) {
    console.error('Error in notification logic:', err && err.message ? err.message : err);
  }
});

client.on('error', (err) => {
  console.error('MQTT error:', err && err.message ? err.message : err);
});

// Start device ping loop
startDevicePingLoop(PING_INTERVAL_MS);

// start server
server.listen(PORT, () => {
  console.log(`Web server listening on http://localhost:${PORT}`);
  console.log(`Connecting to MQTT broker at ${MQTT_URL} and subscribing to ${DATA_TOPICS.join(', ')} (modbus: ${MODBUS_TOPIC})`);
  console.log(`Device ping monitor: will ping ${Object.keys(deviceIPMapping).length} device(s) every ${PING_INTERVAL_MS/1000}s (attempts=${PING_ATTEMPTS}, timeout=${PING_TIMEOUT_SECONDS}s per attempt)`);
});
