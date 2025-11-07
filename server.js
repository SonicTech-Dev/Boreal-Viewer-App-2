/*
 Updated server.js

 - Replaced the simple single-call ping with a "multiple attempts" ping strategy
   inspired by your previous project: try up to 3 times per cycle (one ICMP packet each),
   each attempt uses a 5s per-attempt timeout and waits 5s between attempts.
 - If any attempt succeeds we mark the device ONLINE for that cycle; otherwise OFFLINE.
 - The server still emits socket.io "device_status" events and exposes GET /_device_status.
 - No other application logic changed (MQTT, API endpoints, DB, etc).
*/

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { exec } = require('child_process');
const { insertLosData, fetchLosData } = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const MQTT_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';
const MQTT_TOPIC = process.env.MQTT_TOPIC || 'sensors/+/reading';
const PORT = process.env.PORT || 3000;

// Ping settings
const DEVICE_IP = process.env.DEVICE_IP || '10.0.0.42';
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || String(30 * 1000), 10); // 30s
const PING_ATTEMPTS = parseInt(process.env.PING_ATTEMPTS || '3', 10); // 3 attempts per cycle
const PING_TIMEOUT_SECONDS = parseInt(process.env.PING_TIMEOUT_SECONDS || '5', 10); // per-attempt timeout (s)
const PING_DELAY_BETWEEN_MS = parseInt(process.env.PING_DELAY_BETWEEN_MS || String(5 * 1000), 10); // 5s between attempts

app.use(express.static('public'));

app.get('/_mqtt_config', (req, res) => {
  res.json({ topic: MQTT_TOPIC });
});

// Simple health endpoint (kept for compatibility)
app.get('/_health', (req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// Device status bootstrap endpoint
let lastDeviceStatus = {
  ip: DEVICE_IP,
  online: false,
  rtt: null,
  ts: null
};
app.get('/_device_status', (req, res) => {
  res.json({ ok: true, status: lastDeviceStatus });
});

/**
 * GET /api/los
 * Params:
 *  - from, to (ISO) using recorded_at column
 *  - limit
 *  - offset (for pagination)
 */
app.get('/api/los', async (req, res) => {
  const { from, to, limit, offset } = req.query; // <--- add offset here
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
    // Pass offset to fetchLosData!
    const rows = await fetchLosData(from || null, to || null, parsedLimit, parsedOffset);
    res.json({ ok: true, rows });
  } catch (err) {
    console.error('GET /api/los error:', err && err.message ? err.message : err);
    res.status(500).json({ ok: false, error: 'Failed to query database' });
  }
});

server.listen(PORT, () => {
  console.log(`Web server listening on http://localhost:${PORT}`);
  console.log(`Connecting to MQTT broker at ${MQTT_URL} and subscribing to ${MQTT_TOPIC}`);
  console.log(`Device ping monitor: will ping ${DEVICE_IP} every ${PING_INTERVAL_MS/1000}s (attempts=${PING_ATTEMPTS}, timeout=${PING_TIMEOUT_SECONDS}s per attempt)`);
});

io.on('connection', (socket) => {
  console.log('Web client connected, id=', socket.id);
  socket.on('disconnect', () => {
    console.log('Web client disconnected, id=', socket.id);
  });
});

const mqttOptions = {};
if (process.env.MQTT_USERNAME) mqttOptions.username = process.env.MQTT_USERNAME;
if (process.env.MQTT_PASSWORD) mqttOptions.password = process.env.MQTT_PASSWORD;

const client = mqtt.connect(MQTT_URL, mqttOptions);

client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe(MQTT_TOPIC, { qos: 0 }, (err, granted) => {
    if (err) {
      console.error('Subscribe error:', err);
    } else {
      console.log('Subscribed to:', granted.map(g => g.topic).join(', '));
    }
  });
});

client.on('message', (topic, payloadBuffer) => {
  const payloadString = payloadBuffer.toString();
  const timestamp = new Date().toISOString();

  let payload = payloadString;
  try {
    payload = JSON.parse(payloadString);
  } catch (e) {}

  const message = {
    topic,
    payload,
    raw: payloadString,
    received_at: timestamp
  };

  io.emit('mqtt_message', message);

  let params = null;
  if (payload && typeof payload === 'object') {
    params = (payload.params && typeof payload.params === 'object') ? payload.params : payload;
  } else {
    try {
      const parsed = JSON.parse(payloadString);
      params = (parsed && parsed.params && typeof parsed.params === 'object') ? parsed.params : parsed;
    } catch (e) {
      params = null;
    }
  }

  if (params && typeof params === 'object') {
    const los = {};
    for (const k of Object.keys(params)) {
      if (typeof k === 'string' && k.trim().toLowerCase().startsWith('los')) {
        los[k] = params[k];
      }
    }

    if (Object.keys(los).length > 0) {
      insertLosData(topic, payload, los, timestamp)
        .then((r) => {
          if (!r) {
            console.warn('DB insert returned null (see previous error)');
          }
        })
        .catch((err) => {
          console.error('Failed to insert into los_data:', err && err.message ? err.message : err);
        });
    }
  }

  console.log(`[${timestamp}] ${topic} ->`, payloadString);
});

client.on('error', (err) => {
  console.error('MQTT error:', err && err.message ? err.message : err);
});

/**
 * Ping helpers (child_process.exec)
 * - pingOnce: run `ping -c 1 -W <timeout> <ip>` once and parse RTT if available
 * - pingMultipleAttempts: perform up to N attempts, waiting between attempts, returning the first successful result or final failure
 */

function pingOnce(ip, timeoutSeconds) {
  return new Promise((resolve) => {
    const cmd = `ping -c 1 -W ${timeoutSeconds} ${ip}`;
    // exec timeout slightly larger than per-attempt timeout to avoid hanging
    exec(cmd, { timeout: (timeoutSeconds + 2) * 1000 }, (err, stdout, stderr) => {
      if (!err && stdout) {
        // parse "time=NN.N ms"
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
  // Try up to `attempts` times; return as soon as one attempt reports online
  for (let i = 0; i < attempts; i++) {
    try {
      const res = await pingOnce(ip, timeoutSeconds);
      if (res && res.online) {
        return { online: true, rtt: res.rtt, attempt: i + 1, raw: res.raw };
      }
    } catch (e) {
      // swallow and continue to next attempt
      console.error('pingOnce error:', e && e.message ? e.message : e);
    }
    // If not last attempt, wait before next attempt
    if (i < attempts - 1) {
      await new Promise(r => setTimeout(r, delayBetweenMs));
    }
  }
  // All attempts failed
  return { online: false, rtt: null, attempt: attempts };
}

/**
 * doPingEmit
 * - Calls pingMultipleAttempts for DEVICE_IP with configured attempts/timeouts
 * - Updates lastDeviceStatus and emits io.emit('device_status', lastDeviceStatus)
 */
async function doPingEmit() {
  try {
    const startTs = new Date().toISOString();
    const res = await pingMultipleAttempts(DEVICE_IP, PING_ATTEMPTS, PING_TIMEOUT_SECONDS, PING_DELAY_BETWEEN_MS);
    const now = new Date().toISOString();

    lastDeviceStatus = {
      ip: DEVICE_IP,
      online: !!res.online,
      rtt: (res.rtt !== undefined && res.rtt !== null) ? res.rtt : null,
      ts: now,
      attempt: res.attempt
    };

    io.emit('device_status', lastDeviceStatus);

    if (lastDeviceStatus.online) {
      console.log(`[PING ${now}] ${DEVICE_IP} ONLINE (rtt=${lastDeviceStatus.rtt}ms) after attempt ${res.attempt}`);
    } else {
      console.warn(`[PING ${now}] ${DEVICE_IP} OFFLINE after ${res.attempt} attempts`);
    }
  } catch (err) {
    const now = new Date().toISOString();
    console.error(`[PING ${now}] error pinging ${DEVICE_IP}:`, err && err.message ? err.message : err);
    lastDeviceStatus = { ip: DEVICE_IP, online: false, rtt: null, ts: new Date().toISOString(), attempt: PING_ATTEMPTS };
    io.emit('device_status', lastDeviceStatus);
  }
}

// Start ping loop immediately and then on an interval
doPingEmit().catch(() => {});
setInterval(() => {
  doPingEmit().catch(() => {});
}, PING_INTERVAL_MS);
