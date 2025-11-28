/**
 Updated server.js

 - Subscribes to two data topics (MQTT_TOPIC_2 / MQTT_TOPIC_3) and an optional MODBUS_TOPIC
 - Processes only those topics + MODBUS_TOPIC (forwarded as 'signal')
 - Merges integer + decimal PPM parts into a single los_ppm value when present
 - Emits the same 'mqtt_message' shape used previously (topic, serial_number, payload, los, ts, received_at)
 - Emits 'signal' for modbus/gsm_signal messages
 - Performs serial-aware threshold lookup and sends notifications via Firebase (if configured)
 - Uses the "multiple attempts" ping strategy (up to PING_ATTEMPTS per cycle)
 - Keeps DB insert and fetch behavior via ./db's insertLosData / fetchLosData
 - Adds /api/remote_stations to support remote station selection in the frontend
 - Option A: If incoming MQTT payload lacks a serial, this version will infer serial_number
   from the topic using environment-configurable mappings (MQTT_TOPIC_2_SERIAL / MQTT_TOPIC_3_SERIAL)
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

// Build device mapping for remote stations (for ping loop and frontend selection)
// Prefer an explicit JSON mapping via env REMOTE_STATIONS_JSON, otherwise fall back to single DEVICE_IP
let deviceIPMapping = {};
if (process.env.REMOTE_STATIONS_JSON) {
  try {
    const parsed = JSON.parse(process.env.REMOTE_STATIONS_JSON);
    if (typeof parsed === 'object' && parsed !== null) {
      deviceIPMapping = parsed;
    }
  } catch (e) {
    console.warn('Failed parsing REMOTE_STATIONS_JSON, falling back to DEVICE_IP:', e && e.message ? e.message : e);
  }
}
if (Object.keys(deviceIPMapping).length === 0) {
  const deviceIp = process.env.DEVICE_IP || null;
  const deviceSerial = process.env.DEVICE_SERIAL || 'default_station';
  if (deviceIp) deviceIPMapping[deviceSerial] = deviceIp;
  // If still empty, seed an example mapping (you should override with env)
  if (Object.keys(deviceIPMapping).length === 0) {
    deviceIPMapping = {
      'B452A25032102': '10.0.0.42',
      'B452A25032103': '10.0.0.43',
    };
  }
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
 * Merge integer + decimal PPM parts into a single numeric los_ppm value.
 * - Detects integer part keys that include 'ppm' and ('int' or 'mlo').
 * - Detects decimal part keys that include 'ppm' and ('dec' or 'decimal').
 * - If both parts exist, merges them into integer.decimal (not summed).
 * - If decimal part has single digit (e.g. 5), treat it as two-digit (05 -> divisor 100).
 * - Overrides los.los_ppm when merged.
 */
function tryMergePpmParts(params, los) {
  if (!params || typeof params !== 'object') return;

  let intVal;
  let decVal;

  for (const key of Object.keys(params)) {
    const ck = canonicalKey(key); // normalized key
    const raw = params[key];
    const num = (raw === null || raw === undefined) ? NaN : Number(raw);
    if (Number.isNaN(num)) continue;

    if (ck.includes('ppm') && (ck.includes('int') || ck.includes('mlo'))) {
      intVal = num;
      continue;
    }
    if (ck.includes('ppm') && (ck.includes('dec') || ck.includes('decimal'))) {
      decVal = num;
      continue;
    }

    // Fallback detections: keys ending with int/dec and mentioning pp-like substring
    if ((ck.endsWith('int') || ck.endsWith('mloint')) && ck.includes('pp')) intVal = num;
    if (ck.endsWith('dec') && ck.includes('pp')) decVal = num;
  }

  if (typeof intVal !== 'undefined' && typeof decVal !== 'undefined') {
    const decStr = String(Math.abs(decVal));
    const divisor = decStr.length === 1 ? 100 : 10 ** decStr.length;
    const sign = intVal < 0 ? -1 : 1;
    const merged = Number(intVal) + sign * (Number(decVal) / divisor);
    if (!Number.isNaN(merged)) {
      if (!los) return;
      los.los_ppm = merged;
    }
  }
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
app.get('/api/remote_stations', (req, res) => {
  try {
    // Return an explicit display property so frontend can show friendly names.
    // For the two example IPs, map them to "Station 1" / "Station 2" as requested.
    const rows = Object.entries(deviceIPMapping).map(([serial, ip]) => {
      let display;
      if (ip === '10.0.0.42') {
        display = 'Station 1';
      } else if (ip === '10.0.0.43') {
        display = 'Station 2';
      } else {
        display = (serial || '(unknown)') + (ip ? (' — ' + ip) : '');
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
    const rows = await fetchLosData(from || null, to || null, parsedLimit, parsedOffset, serial_number || null);
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

  // Merge integer + decimal ppm parts if present, overriding los.los_ppm
  tryMergePpmParts(params, los);

  // Serial extraction (payload or params)
  let serial_number =
    (payload && (payload.serial_number || payload.serial)) ||
    (params && (params.serial_number || params.serial)) ||
    undefined;

  // Option A: If the message lacks a serial, infer from topic using configured mapping
  if (!serial_number) {
    if (topic === MQTT_TOPIC_2) {
      serial_number = MQTT_TOPIC_2_SERIAL;
      console.log(`Inferred serial_number="${serial_number}" from topic="${topic}"`);
    } else if (topic === MQTT_TOPIC_3) {
      serial_number = MQTT_TOPIC_3_SERIAL;
      console.log(`Inferred serial_number="${serial_number}" from topic="${topic}"`);
    }
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
    // Keep compatibility with existing insertLosData signature in your ./db module.
    // Many projects expect: insertLosData(topic, payload, los, timestamp)
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
        // Left intentionally blank — implement in ./db if desired.
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
      const threshold = await getLosThreshold(serial_number);
      if (threshold !== null && !Number.isNaN(threshold) && ppmNumber > Number(threshold)) {
        if (canNotify(serial_number)) {
          const title = serial_number ? `Alarm: ${serial_number}` : 'Alarm: Gas Finder';
          const message = `PPM ${ppmNumber} exceeded threshold ${threshold}`;
          await sendNotificationToAll(title, message, { serial_number: serial_number || '' });
          io.emit('alarm', { serial_number, ppm: ppmNumber, threshold, ts: mqttTs });
          console.log(`ALARM triggered for ${serial_number || '(unknown)'}: ppm=${ppmNumber} threshold=${threshold} ts=${mqttTs !== undefined ? mqttTs : '(none)'}`);
        } else {
          console.log(`Alarm suppressed by cooldown for ${serial_number || '(unknown)'}`);
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
