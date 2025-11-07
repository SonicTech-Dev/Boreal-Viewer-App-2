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

app.use(express.static('public'));

app.get('/_mqtt_config', (req, res) => {
  res.json({ topic: MQTT_TOPIC });
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

// --- Added: device ping status endpoint & ping loop (child_process method) ---
const DEVICE_IP = process.env.DEVICE_IP || '10.0.0.42';
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || String(30 * 1000), 10);
const PING_TIMEOUT_SECONDS = parseInt(process.env.PING_TIMEOUT_SECONDS || '5', 10);

// last known device status
let lastDeviceStatus = {
  ip: DEVICE_IP,
  online: false,
  rtt: null,
  ts: null
};

app.get('/_device_status', (req, res) => {
  res.json({ ok: true, status: lastDeviceStatus });
});

// helper to run system ping (uses child_process.exec)
// Note: uses Linux-style ping options: -c 1 (one packet) and -W timeout (seconds)
function pingHost(ip, timeoutSeconds) {
  return new Promise((resolve) => {
    // Build command - prefer -W (timeout in seconds) which works on many linux distros.
    // If -W is unavailable on a platform, the command will still fail and we treat as offline.
    const cmd = `ping -c 1 -W ${timeoutSeconds} ${ip}`;
    exec(cmd, { timeout: (timeoutSeconds + 2) * 1000 }, (err, stdout, stderr) => {
      const now = new Date().toISOString();
      if (!err) {
        // parse RTT from stdout: look for "time=NN.N ms"
        const m = stdout.match(/time=([0-9.]+)\s*ms/);
        const rtt = m ? Number(m[1]) : null;
        resolve({ online: true, rtt, ts: now });
      } else {
        resolve({ online: false, rtt: null, ts: now, error: err.message });
      }
    });
  });
}

async function doPingEmit() {
  try {
    const res = await pingHost(DEVICE_IP, PING_TIMEOUT_SECONDS);
    lastDeviceStatus = {
      ip: DEVICE_IP,
      online: !!res.online,
      rtt: res.rtt,
      ts: res.ts || new Date().toISOString()
    };
    io.emit('device_status', lastDeviceStatus);
    if (lastDeviceStatus.online) {
      console.log(`[PING ${lastDeviceStatus.ts}] ${DEVICE_IP} ONLINE rtt=${lastDeviceStatus.rtt}ms`);
    } else {
      console.warn(`[PING ${lastDeviceStatus.ts}] ${DEVICE_IP} OFFLINE`);
    }
  } catch (err) {
    const now = new Date().toISOString();
    console.error(`[PING ${now}] error pinging ${DEVICE_IP}:`, err && err.message ? err.message : err);
    lastDeviceStatus = { ip: DEVICE_IP, online: false, rtt: null, ts: now };
    io.emit('device_status', lastDeviceStatus);
  }
}

// start ping loop immediately and every 30s
doPingEmit().catch(() => {});
setInterval(() => {
  doPingEmit().catch(() => {});
}, PING_INTERVAL_MS);
// --- end ping additions ---

server.listen(PORT, () => {
  console.log(`Web server listening on http://localhost:${PORT}`);
  console.log(`Connecting to MQTT broker at ${MQTT_URL} and subscribing to ${MQTT_TOPIC}`);
  console.log(`Pinging ${DEVICE_IP} every ${PING_INTERVAL_MS/1000}s with timeout ${PING_TIMEOUT_SECONDS}s`);
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
