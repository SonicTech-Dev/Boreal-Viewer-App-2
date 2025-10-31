// client.js — live UI: receives mqtt_message, updates real-time tiles and feed.
// Works with server that writes to los_data; extracts LoS keys from incoming payloads.

(function () {
  const socket = io();
  const statusEl = document.getElementById('status');
  const tilesContainer = document.getElementById('tiles');
  const feedEl = document.getElementById('feed');
  const topicEl = document.getElementById('topic');

  // Keys we care about (human-friendly labels mapped to DB column aliases)
  const KEYS = [
    { keyVariants: ['LoS-Temp(c)'], label: 'LoS-Temp (°C)', apiField: 'los_temp' },
    { keyVariants: ['LoS-Rx Light'], label: 'LoS-Rx Light', apiField: 'los_rx_light' },
    { keyVariants: ['LoS- R2', 'LoS-R2', 'LoS - R2'], label: 'LoS-R2', apiField: 'los_r2' },
    { keyVariants: ['LoS-HeartBeat', 'LoS- HeartBeat'], label: 'LoS-HeartBeat', apiField: 'los_heartbeat' },
    { keyVariants: ['LoS - PPM', 'LoS- PPM', 'LoS-PPM'], label: 'LoS-PPM', apiField: 'los_ppm' }
  ];

  // state: latest value per label
  const latest = {};
  KEYS.forEach(k => latest[k.label] = { value: null, updated_at: null });

  // create tiles initially
  function createTiles() {
    tilesContainer.innerHTML = '';
    KEYS.forEach(k => {
      const tile = document.createElement('div');
      tile.className = 'tile';
      tile.id = 'tile-' + k.apiField;

      const label = document.createElement('div');
      label.className = 'label';
      label.textContent = k.label;

      const value = document.createElement('div');
      value.className = 'value';
      value.textContent = '-';

      const small = document.createElement('div');
      small.className = 'sub';
      small.style.marginTop = '6px';
      small.style.fontSize = '12px';
      small.style.color = 'var(--muted)';
      small.textContent = '—';

      tile.appendChild(label);
      tile.appendChild(value);
      tile.appendChild(small);

      tilesContainer.appendChild(tile);
    });
  }

  function updateTiles() {
    KEYS.forEach(k => {
      const info = latest[k.label];
      const tile = document.getElementById('tile-' + k.apiField);
      if (!tile) return;
      const valueEl = tile.querySelector('.value');
      const smallEl = tile.querySelector('.sub');

      if (info.value === null || info.value === undefined) {
        valueEl.textContent = '-';
        smallEl.textContent = 'No data';
      } else {
        const v = (typeof info.value === 'number' && !Number.isInteger(info.value)) ? info.value.toFixed(2) : String(info.value);
        valueEl.textContent = v;
        smallEl.textContent = 'updated: ' + new Date(info.updated_at).toLocaleTimeString();
      }
    });
  }

  // maintain small feed of messages
  function addToFeed(msgText) {
    const item = document.createElement('div');
    item.className = 'feed-item';
    item.style.padding = '8px';
    item.style.marginBottom = '8px';
    item.style.borderRadius = '6px';
    item.style.background = 'rgba(255,255,255,0.02)';
    item.style.fontFamily = 'monospace';
    item.style.fontSize = '13px';
    item.textContent = msgText;
    // prepend
    if (feedEl.firstChild) feedEl.insertBefore(item, feedEl.firstChild);
    else feedEl.appendChild(item);

    while (feedEl.childElementCount > 200) feedEl.removeChild(feedEl.lastChild);
  }

  // set configured topic if provided by server endpoint
  fetch('/_mqtt_config').then(r => r.json()).then(cfg => {
    if (cfg && cfg.topic) topicEl.textContent = cfg.topic;
  }).catch(()=>{});

  socket.on('connect', () => {
    statusEl.textContent = 'Connected';
  });
  socket.on('disconnect', () => {
    statusEl.textContent = 'Disconnected';
  });

  // message shape: { topic, payload, raw, received_at }
  socket.on('mqtt_message', (msg) => {
    const ts = msg.received_at || new Date().toISOString();

    // parse params similar to server logic
    let params = null;
    if (msg.payload && typeof msg.payload === 'object') {
      params = (msg.payload.params && typeof msg.payload.params === 'object') ? msg.payload.params : msg.payload;
    } else {
      try {
        const parsed = JSON.parse(msg.raw);
        params = (parsed && parsed.params && typeof parsed.params === 'object') ? parsed.params : parsed;
      } catch (e) {
        params = null;
      }
    }

    // extract LoS keys
    if (params && typeof params === 'object') {
      const los = {};
      for (const kName of Object.keys(params)) {
        if (typeof kName === 'string' && kName.trim().toLowerCase().startsWith('los')) {
          los[kName] = params[kName];
        }
      }

      if (Object.keys(los).length > 0) {
        // update latest state by matching variants
        KEYS.forEach(mapping => {
          for (const variant of mapping.keyVariants) {
            if (los.hasOwnProperty(variant)) {
              latest[mapping.label] = { value: los[variant], updated_at: ts };
              break;
            }
          }
        });

        updateTiles();
        addToFeed(`[${new Date(ts).toLocaleTimeString()}] ${msg.topic} — ${JSON.stringify(los)}`);
      }
    }
  });

  // initial setup
  createTiles();
  updateTiles();
})();