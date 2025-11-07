// client.js — live UI: receives mqtt_message, updates real-time tiles with popup details and feed.
// Full feature set restored: hover popups, real-time feed, status dot/label.

(function () {
  const socket = io();
  const statusDot = document.getElementById('status-dot');
  const statusLabel = document.getElementById('status-label');
  const tilesContainer = document.getElementById('tiles');
  const feedEl = document.getElementById('feed');

  // Tile key mappings and variants (keeps previous variants)
  const KEYS = [
    { keyVariants: ['LoS-Temp(c)'], label: 'Temp (°C)', apiField: 'los_temp' },
    { keyVariants: ['LoS-Rx Light'], label: 'Rx Light', apiField: 'los_rx_light' },
    { keyVariants: ['LoS- R2', 'LoS-R2', 'LoS - R2'], label: 'R2', apiField: 'los_r2' },
    { keyVariants: ['LoS-HeartBeat', 'LoS- HeartBeat'], label: 'HeartBeat', apiField: 'los_heartbeat' },
    { keyVariants: ['LoS - PPM', 'LoS- PPM', 'LoS-PPM'], label: 'GasFinder-PPM', apiField: 'los_ppm' }
  ];

  // Store latest values
  const latest = {};
  KEYS.forEach(k => latest[k.label] = { value: null, updated_at: null, raw: null });

  // Create tiles with popup for each metric
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

      const meta = document.createElement('div');
      meta.className = 'meta';
      meta.textContent = 'No data';

      const popup = document.createElement('div');
      popup.className = 'popup';
      popup.id = 'popup-' + k.apiField;
      popup.innerHTML = `
        <div style="color:var(--muted);font-size:12px">Latest</div>
        <div style="font-weight:700;font-size:18px" id="popup-val-${k.apiField}">-</div>
        <div style="margin-top:6px;color:var(--muted);font-size:12px" id="popup-time-${k.apiField}">—</div>
      `;

      tile.appendChild(label);
      tile.appendChild(value);
      tile.appendChild(meta);
      tile.appendChild(popup);

      tilesContainer.appendChild(tile);
    });
  }

  // Update tile values and popup times
  function updateTiles() {
    KEYS.forEach(k => {
      const info = latest[k.label];
      const tile = document.getElementById('tile-' + k.apiField);
      if (!tile) return;
      const valueEl = tile.querySelector('.value');
      const metaEl = tile.querySelector('.meta');
      const popupVal = tile.querySelector('#popup-val-' + k.apiField);
      const popupTime = tile.querySelector('#popup-time-' + k.apiField);

      if (info.value === null || info.value === undefined) {
        valueEl.textContent = '-';
        metaEl.textContent = 'No data';
        if (popupVal) popupVal.textContent = '-';
        if (popupTime) popupTime.textContent = '—';
      } else {
        const display = (typeof info.value === 'number' && !Number.isInteger(info.value)) ? info.value.toFixed(2) : String(info.value);
        valueEl.textContent = display;
        metaEl.textContent = info.updated_at ? ('updated: ' + new Date(info.updated_at).toLocaleTimeString()) : 'No data';
        if (popupVal) popupVal.textContent = display;
        if (popupTime) popupTime.textContent = info.updated_at ? ('at ' + new Date(info.updated_at).toLocaleString()) : '—';
      }
    });
  }

  // Add item to debug feed (prepend)
  function addToFeed(msgText) {
    const item = document.createElement('div');
    item.className = 'feed-item';
    item.textContent = msgText;
    if (feedEl.firstChild) feedEl.insertBefore(item, feedEl.firstChild);
    else feedEl.appendChild(item);
    while (feedEl.childElementCount > 200) feedEl.removeChild(feedEl.lastChild);
  }

  // Socket connection events
  socket.on('connect', () => {
    statusDot.style.background = 'var(--success)';
    statusLabel.textContent = 'Connected';
  });
  socket.on('disconnect', () => {
    statusDot.style.background = 'var(--danger)';
    statusLabel.textContent = 'Disconnected';
  });

  // Handle incoming mqtt messages: extract LoS fields and update tiles + feed
  socket.on('mqtt_message', (msg) => {
    const ts = msg.received_at || new Date().toISOString();

    // Extract params from expected payload { ts, params: {...} } or direct object
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

    if (!params || typeof params !== 'object') return;

    // Find keys beginning with "los" (case-insensitive)
    const los = {};
    Object.keys(params).forEach(kName => {
      if (typeof kName === 'string' && kName.trim().toLowerCase().startsWith('los')) {
        los[kName] = params[kName];
      }
    });

    if (Object.keys(los).length === 0) return;

    // Update latest by matching variants
    KEYS.forEach(mapping => {
      for (const variant of mapping.keyVariants) {
        if (Object.prototype.hasOwnProperty.call(los, variant)) {
          latest[mapping.label] = { value: los[variant], updated_at: ts, raw: los };
          break;
        }
      }
    });

    updateTiles();
    addToFeed(`[${new Date(ts).toLocaleTimeString()}] ${msg.topic} — ${JSON.stringify(los)}`);
  });

  // Initial render
  createTiles();
  updateTiles();
})();
