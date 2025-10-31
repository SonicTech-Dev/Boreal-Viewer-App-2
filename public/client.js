// LoS-specific client logic
// - Listens for 'mqtt_message' events coming from server
// - Extracts only "LoS" fields from the message.params (if present)
// - Maintains latest value per LoS key and renders them as tiles
// - Shows recent LoS messages in a feed

(function () {
  const socket = io();
  const statusEl = document.getElementById('status');
  const messagesFeed = document.getElementById('feed');
  const feedEmpty = document.getElementById('feed-empty');
  const tilesContainer = document.getElementById('tiles');
  const topicEl = document.getElementById('topic');
  const clearFeedBtn = document.getElementById('clear-feed');
  const resetTilesBtn = document.getElementById('reset-tiles');

  // Keys we want to show (normalized): any key that starts with "LoS" (case-insensitive)
  function isLosKey(k) {
    return typeof k === 'string' && k.trim().toLowerCase().startsWith('los');
  }

  // Keep latest values
  const latest = {}; // key -> { value, updated_at }

  // Keep small history of LoS messages (raw payload + timestamp)
  const history = [];

  const MAX_HISTORY = 200;
  const MAX_TILES = 40;

  // Utility: prettify key into a short label
  function labelFromKey(k) {
    // Remove common "LoS", hyphens and extra spaces, keep readable
    return k.replace(/^LoS[-\s]*/i, '').trim() || k;
  }

  // Render all tiles based on latest object
  function renderTiles() {
    // Clear
    tilesContainer.innerHTML = '';
    const keys = Object.keys(latest);
    if (keys.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'empty';
      empty.textContent = 'No LoS data yet. Waiting for messages...';
      tilesContainer.appendChild(empty);
      return;
    }

    // Sort keys: keep a stable order (alphabetical) — you can change to custom ordering
    keys.sort((a,b)=> a.localeCompare(b, undefined, {sensitivity:'base'}));

    for (const k of keys) {
      const info = latest[k];
      const tile = document.createElement('div');
      tile.className = 'tile';
      const label = document.createElement('div');
      label.className = 'label';
      label.textContent = labelFromKey(k);

      const value = document.createElement('div');
      value.className = 'value';
      // Show numeric values more cleanly
      if (typeof info.value === 'number') {
        value.textContent = Number.isInteger(info.value) ? info.value : info.value.toFixed(2);
      } else {
        value.textContent = String(info.value);
      }

      const small = document.createElement('div');
      small.className = 'small';
      small.textContent = 'updated: ' + new Date(info.updated_at).toLocaleTimeString();

      tile.appendChild(label);
      tile.appendChild(value);
      tile.appendChild(small);

      tilesContainer.appendChild(tile);
    }
  }

  // Append to feed
  function addToFeed(message) {
    // message: { topic, payload, raw, received_at } and we only push LoS subset
    const el = document.createElement('div');
    el.className = 'feed-item';

    const t = document.createElement('div');
    t.style.fontSize = '12px';
    t.style.opacity = '0.85';
    t.textContent = `[${new Date(message.received_at).toLocaleTimeString()}] ${message.topic}`;

    const pre = document.createElement('pre');
    pre.style.margin = '6px 0 0 0';
    pre.style.whiteSpace = 'pre-wrap';
    pre.style.fontFamily = 'monospace';
    pre.style.fontSize = '13px';
    pre.textContent = JSON.stringify(message.los, null, 2);

    el.appendChild(t);
    el.appendChild(pre);

    // Insert at top (feed uses column-reverse visually, so we append normally)
    messagesFeed.appendChild(el);

    // Manage feed empty state
    if (feedEmpty) {
      feedEmpty.style.display = 'none';
    }

    // Trim history DOM nodes
    while (messagesFeed.childElementCount > MAX_HISTORY) {
      messagesFeed.removeChild(messagesFeed.firstChild);
    }
  }

  // When we get a message, extract LoS keys if present
  socket.on('mqtt_message', (msg) => {
    // msg: { topic, payload, raw, received_at }
    // We expect payload to be either an object or string. If it's object, prefer payload.params or payload directly.
    let obj = null;
    if (msg && typeof msg.payload === 'object' && msg.payload !== null) {
      // The message you showed has top-level fields "ts" and "params"
      if (msg.payload.params && typeof msg.payload.params === 'object') {
        obj = msg.payload.params;
      } else {
        obj = msg.payload;
      }
    } else {
      // try to parse raw JSON
      try {
        const parsed = JSON.parse(msg.raw);
        if (parsed && parsed.params && typeof parsed.params === 'object') obj = parsed.params;
        else if (parsed && typeof parsed === 'object') obj = parsed;
      } catch (e) {
        obj = null;
      }
    }

    if (!obj) {
      // nothing to do if no object payload
      return;
    }

    // Extract only LoS keys
    const los = {};
    for (const k of Object.keys(obj)) {
      if (isLosKey(k)) {
        los[k] = obj[k];
      }
    }

    // If no LoS keys present, return
    if (Object.keys(los).length === 0) return;

    // Update latest values and tiles
    const now = msg.received_at || new Date().toISOString();
    for (const [k, v] of Object.entries(los)) {
      latest[k] = { value: v, updated_at: now };
    }

    // Save to small history (for possible future use)
    history.push({ received_at: now, topic: msg.topic, los });
    if (history.length > MAX_HISTORY) history.shift();

    // Update feed and tiles
    addToFeed({ topic: msg.topic, los, received_at: now });
    renderTiles();
  });

  // Connection state handlers
  socket.on('connect', () => {
    statusEl.textContent = 'Connected';
    statusEl.classList.remove('disconnected');
    statusEl.classList.add('connected');
  });
  socket.on('disconnect', () => {
    statusEl.textContent = 'Disconnected';
    statusEl.classList.remove('connected');
    statusEl.classList.add('disconnected');
  });

  // Optional: request topic from server (if server provides it via a simple endpoint)
  // Fallback: topic remains "(configured on server)"
  fetch('/_mqtt_config')
    .then(r => r.json())
    .then(cfg => {
      if (cfg && cfg.topic) topicEl.textContent = cfg.topic;
    })
    .catch(()=>{ /* ignore */ });

  // Controls
  clearFeedBtn.addEventListener('click', () => {
    messagesFeed.innerHTML = '';
    messagesFeed.appendChild(feedEmpty);
    feedEmpty.style.display = 'block';
  });

  resetTilesBtn.addEventListener('click', () => {
    for (const k of Object.keys(latest)) delete latest[k];
    renderTiles();
  });

  // Initial render
  renderTiles();
})();