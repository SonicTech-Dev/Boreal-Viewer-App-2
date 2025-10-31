# MQTT Live View (Node.js)

A minimal Node.js project that connects to an MQTT broker, subscribes to a topic, and shows live readings in a browser using Socket.IO.

## Features
- Connects to your MQTT broker.
- Subscribes to a topic (supports wildcards).
- Forwards incoming messages to connected web clients in real-time.
- Simple web UI that displays the latest messages and a live feed.

## Requirements
- Node 16+ (or any modern Node.js)
- An MQTT broker (e.g., Mosquitto, cloud broker)
- npm

## Install
1. Clone or copy the project files.
2. Install dependencies:
   npm install

## Configuration
Create a `.env` file in the project root or set environment variables:

- MQTT_URL (default: mqtt://localhost:1883)
- MQTT_TOPIC (default: sensors/+/reading)
- PORT (default: 3000)

Example `.env`:
```
MQTT_URL=mqtt://broker.hivemq.com:1883
MQTT_TOPIC=myhome/temperature
PORT=3000
```

If your broker requires authentication, you can set:
- MQTT_USERNAME
- MQTT_PASSWORD
and modify `server.js` to include them in the mqttOptions (a small comment in server.js points where to add).

## Run
Start the server:
```
npm start
```
Open http://localhost:3000 in your browser.

## How it works
- server.js connects to the MQTT broker and subscribes to the configured topic.
- Incoming messages are parsed and emitted to browser clients via Socket.IO.
- public/index.html connects to Socket.IO and updates the page in real-time.

## Files
- server.js — Node/Express + MQTT + Socket.IO server
- public/index.html — Web UI
- public/client.js — Web client JS that handles socket events
- package.json — dependencies and scripts

## Notes
- This is intentionally minimal and meant for demonstration. For production use consider:
  - Authentication and TLS to the MQTT broker.
  - Rate-limiting, message history persistence, and sanitization.
  - Proper error handling and monitoring.

License: MIT