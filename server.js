require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
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

server.listen(PORT, () => {
  console.log(`Web server listening on http://localhost:${PORT}`);
  console.log(`Connecting to MQTT broker at ${MQTT_URL} and subscribing to ${MQTT_TOPIC}`);
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
