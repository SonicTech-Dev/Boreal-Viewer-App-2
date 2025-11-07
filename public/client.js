// client.js — live UI: receives mqtt_message, updates real-time tiles with popup details and feed.
// Full feature set restored: hover popups, real-time feed, status dot/label.

(function () {
  // public/client.js
  // Shows simple Online/Offline based solely on server ping results (device_status)
  const socket = io();
  const statusDot = document.getElementById('status-dot');
  const statusLabel = document.getElementById('status-label');
  const localTimeEl = document.getElementById('local-time');
  const tilesContainer = document.getElementById('tiles');
  const feedEl = document.getElementById('feed');
  const resultsEl = document.getElementById('results'); // query results container

  // Tile key mappings and variants
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
    if (!tilesContainer) return;
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

  // Update tiles (applies R2 / 100 transformation only for display)
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
        // Apply R2 display transformation: divide by 100 before showing
        let displayValue = info.value;
        if (k.label === 'R2') {
          const n = Number(info.value);
          if (!Number.isNaN(n)) displayValue = n / 100;
        }

        const display = (typeof displayValue === 'number' && !Number.isInteger(displayValue)) ? displayValue.toFixed(2) : String(displayValue);
        valueEl.textContent = display;
        metaEl.textContent = info.updated_at ? ('updated: ' + new Date(info.updated_at).toLocaleTimeString()) : 'No data';
        if (popupVal) popupVal.textContent = display;
        if (popupTime) popupTime.textContent = info.updated_at ? ('at ' + new Date(info.updated_at).toLocaleString()) : '—';
      }
    });
  }

  // Feed helper
  function addToFeed(msgText) {
    if (!feedEl) return;
    const item = document.createElement('div');
    item.className = 'feed-item';
    item.textContent = msgText;
    if (feedEl.firstChild) feedEl.insertBefore(item, feedEl.firstChild);
    else feedEl.appendChild(item);
    while (feedEl.childElementCount > 200) feedEl.removeChild(feedEl.lastChild);
  }

  // Utility: clear all readings (set them to null so updateTiles shows '-')
  function clearAllReadings() {
    KEYS.forEach(k => {
      latest[k.label] = { value: null, updated_at: null, raw: null };
    });
    updateTiles();
  }

  // --- SIMPLE ONLINE/OFFLINE STATUS (device ping only) ---
  // deviceOnline: null = unknown (shows "Checking…"), true = online, false = offline
  let deviceOnline = null;
  let deviceIp = null;
  let deviceRtt = null;
  let deviceLastSeen = null;

  function setStatusVisual(online) {
    if (!statusDot || !statusLabel) return;
    if (online) {
      statusDot.style.background = 'var(--success)';
      statusDot.style.boxShadow = '0 0 10px rgba(16,185,129,0.18)';
      statusLabel.textContent = 'Online';
    } else {
      statusDot.style.background = 'var(--danger)';
      statusDot.style.boxShadow = '0 0 10px rgba(249,115,22,0.14)';
      statusLabel.textContent = 'Offline';
    }
  }

  function applyDeviceStatus() {
    if (deviceOnline === null) {
      // keep "Checking…" as set in index.html
      return;
    }
    // If device offline, clear readings immediately
    if (deviceOnline === false) {
      clearAllReadings();
    }
    setStatusVisual(deviceOnline);
  }

  // Listen for backend ping events
  socket.on('device_status', (s) => {
    if (!s || typeof s !== 'object') return;
    deviceIp = s.ip || deviceIp;
    deviceOnline = !!s.online;
    deviceRtt = (s.rtt !== undefined && s.rtt !== null) ? Number(s.rtt) : null;
    deviceLastSeen = s.ts || new Date().toISOString();

    applyDeviceStatus();

    addToFeed(`[${new Date(deviceLastSeen).toLocaleTimeString()}] PING ${deviceIp} — ${deviceOnline ? ('online rtt=' + (deviceRtt !== null ? deviceRtt + 'ms' : 'n/a')) : 'offline'}`);
  });

  // Bootstrap on load from /_device_status
  (async function initStatus() {
    try {
      const res = await fetch('/_device_status', { cache: 'no-cache' });
      if (res.ok) {
        const j = await res.json();
        if (j && j.status) {
          const s = j.status;
          deviceIp = s.ip || deviceIp;
          deviceOnline = (s.online === undefined) ? null : !!s.online;
          deviceRtt = (s.rtt !== undefined && s.rtt !== null) ? Number(s.rtt) : null;
          deviceLastSeen = s.ts || new Date().toISOString();
        }
      }
    } catch (e) {
      // leave deviceOnline = null (still "Checking…")
    } finally {
      if (deviceOnline === true || deviceOnline === false) {
        applyDeviceStatus();
        if (deviceLastSeen) {
          addToFeed(`[${new Date(deviceLastSeen).toLocaleTimeString()}] PING ${deviceIp} — ${deviceOnline ? ('online rtt=' + (deviceRtt !== null ? deviceRtt + 'ms' : 'n/a')) : 'offline'}`);
        }
      }
    }
  })();
  // --- end status logic ---

  // Local time updater (DD/MM/YYYY HH:MM:SS)
  function pad(n) { return String(n).padStart(2, '0'); }
  function updateLocalTime() {
    if (!localTimeEl) return;
    const now = new Date();
    const day = pad(now.getDate());
    const month = pad(now.getMonth() + 1);
    const year = now.getFullYear();
    const hours = pad(now.getHours());
    const minutes = pad(now.getMinutes());
    const seconds = pad(now.getSeconds());
    localTimeEl.textContent = `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;
  }
  updateLocalTime();
  setInterval(updateLocalTime, 1000);

  // --- Button selection visuals (Query + preset buttons) ---
  const selectionGroupSelector = '#fetch, .btn.preset';
  function clearSelectionStyles() {
    const all = document.querySelectorAll(selectionGroupSelector);
    all.forEach(el => {
      el.classList.remove('selected');
      el.style.background = '';
      el.style.color = '';
      el.style.boxShadow = '';
    });
  }
  function applySelectionStyle(el) {
    clearSelectionStyles();
    if (!el) return;
    el.classList.add('selected');
    el.style.background = 'var(--success)';
    el.style.color = '#042';
    el.style.boxShadow = '0 6px 14px rgba(16,185,129,0.12)';
  }
  function wireSelectionHandlers() {
    const elems = document.querySelectorAll(selectionGroupSelector);
    elems.forEach(el => {
      el.addEventListener('click', () => {
        if (!el.classList.contains('selected')) applySelectionStyle(el);
      });
    });
  }
  wireSelectionHandlers();

  // --- Results count logic ---
  // Adds/updates a small count display under the results table indicating total rows returned by query.
  // Works without changing your existing query-client.js by watching #results for a table being inserted/updated.

  // Create or update the count element under #results
  function setResultsCount(count) {
    if (!resultsEl) return;
    let countEl = document.getElementById('results-count');
    if (!countEl) {
      countEl = document.createElement('div');
      countEl.id = 'results-count';
      countEl.style.marginTop = '8px';
      countEl.style.color = 'var(--muted)';
      countEl.style.fontSize = '13px';
      resultsEl.appendChild(countEl);
    }
    countEl.textContent = `Total entries: ${count.toLocaleString()}`;
  }

  // Compute number of data rows in the results table
  function computeResultsRowCount() {
    if (!resultsEl) return 0;
    // Find table.table inside results
    const table = resultsEl.querySelector('table.table');
    if (!table) return 0;
    // Prefer tbody rows if present
    const tbody = table.querySelector('tbody');
    if (tbody) {
      return tbody.querySelectorAll('tr').length;
    }
    // Otherwise count tr excluding thead header row(s)
    const allRows = table.querySelectorAll('tr');
    // subtract header rows (if thead exists use its rows count)
    const thead = table.querySelector('thead');
    if (thead) {
      const headerRows = thead.querySelectorAll('tr').length;
      return Math.max(0, allRows.length - headerRows);
    }
    // fallback: assume first row is header
    return Math.max(0, allRows.length - 1);
  }

  // Run a one-time count update (useful after query completes)
  function updateResultsCountNow() {
    const count = computeResultsRowCount();
    setResultsCount(count);
  }

  // Observe mutations inside #results and update count when table changes
  if (resultsEl) {
    const mo = new MutationObserver((mutationsList) => {
      // If any childList mutation occurred, recompute the count.
      // Debounce briefly to allow the table to be fully inserted/updated.
      let relevant = false;
      for (const m of mutationsList) {
        if (m.type === 'childList' || m.type === 'subtree' || m.type === 'attributes') {
          relevant = true;
          break;
        }
      }
      if (relevant) {
        // small debounce
        clearTimeout(window.__results_count_timer__);
        window.__results_count_timer__ = setTimeout(updateResultsCountNow, 120);
      }
    });
    mo.observe(resultsEl, { childList: true, subtree: true, attributes: false });
    // Initial run in case results already present
    setTimeout(updateResultsCountNow, 200);
  }

  // Handle incoming mqtt messages: update tiles and feed
  socket.on('mqtt_message', (msg) => {
    const ts = msg.received_at || new Date().toISOString();

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

    const los = {};
    Object.keys(params).forEach(kName => {
      if (typeof kName === 'string' && kName.trim().toLowerCase().startsWith('los')) {
        los[kName] = params[kName];
      }
    });

    if (Object.keys(los).length === 0) return;

    // If device is offline, ignore incoming readings and keep them cleared
    if (deviceOnline === false) {
      // still add feed entry that we ignored the reading (optional)
      addToFeed(`[${new Date(ts).toLocaleTimeString()}] MQTT reading ignored (device offline) — ${msg.topic}`);
      return;
    }

    // Update latest values by matching variants
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
