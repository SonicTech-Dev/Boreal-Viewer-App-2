// Simple Node.js server that connects to an MQTT broker, subscribes to a topic,
// and forwards messages to connected web clients via Socket.IO.

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Configuration from environment variables with sensible defaults
const MQTT_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';
const MQTT_TOPIC = process.env.MQTT_TOPIC || 'sensors/+/reading';
const PORT = process.env.PORT || 3000;

app.use(express.static('public'));

server.listen(PORT, () => {
  console.log(`Web server listening on http://localhost:${PORT}`);
  console.log(`Connecting to MQTT broker at ${MQTT_URL} and subscribing to ${MQTT_TOPIC}`);
});

// Socket.IO: when a client connects, we can optionally send recent history (not implemented here)
io.on('connection', (socket) => {
  console.log('Web client connected, id=', socket.id);
  socket.on('disconnect', () => {
    console.log('Web client disconnected, id=', socket.id);
  });
});

// MQTT client setup
const mqttOptions = {
  // You can put username/password or other options via env variables (see README)
};

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

  // Try to parse JSON payload if possible
  let payload = payloadString;
  try {
    payload = JSON.parse(payloadString);
  } catch (e) {
    // keep string if not JSON
  }

  const message = {
    topic,
    payload,
    raw: payloadString,
    received_at: timestamp
  };

  // Emit to all connected web clients
  io.emit('mqtt_message', message);

  // Also log to console
  console.log(`[${timestamp}] ${topic} ->`, payloadString);
});

client.on('error', (err) => {
  console.error('MQTT error:', err.message);
});