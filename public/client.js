// client.js — live UI: receives mqtt_message, updates real-time tiles with popup details and feed.
// Fix: prefer server-provided los object (server-side merging) over client-side recompute.
// Label change: renamed GasFinder-PPM -> PPM-M-LO
//
// NOTE: Popup hover removed — tiles now do nothing on hover.

(function () {
  // VISUAL-ONLY STYLE: enlarge status, style dropdown
  try {
    const _s = document.createElement('style');
    _s.id = 'status-visuals';
    _s.textContent = `
      #status-label { font-size: 16px !important; font-weight: 800 !important; letter-spacing: 0.2px; }
      #status-dot { width: 26px !important; height: 26px !important; border-radius: 50% !important; }
      #status-wrapper { display: inline-flex; align-items: center; gap: 10px; }
      #station-select { padding: 6px 8px; font-size: 13px; border-radius: 6px; border: 1px solid rgba(0,0,0,0.08); background: var(--bg); color: inherit; }
      @media (max-width: 800px) {
        #status-label { font-size: 15px !important; }
        #status-dot { width: 24px !important; height: 24px !important; }
      }
      @media (max-width: 420px) {
        #status-label { font-size: 13px !important; }
        #status-dot { width: 18px !important; height: 18px !important; }
        #station-select { font-size: 12px; padding: 4px 6px; }
      }
    `;
    document.head && document.head.appendChild(_s);
  } catch (e) {}

  const socket = io();
  const statusDot = document.getElementById('status-dot');
  const statusLabel = document.getElementById('status-label');
  const localTimeEl = document.getElementById('local-time');
  const tilesContainer = document.getElementById('tiles');
  const feedEl = document.getElementById('feed');
  const resultsEl = document.getElementById('results');

  // Tile key mappings and variants (broad set)
  const KEYS = [
    { keyVariants: ['LoS-Temp(c)', 'LoS-Temp', 'LoS-Temp(C)', 'lostemp', 'los_temp'], label: 'Temp (°C)', apiField: 'los_temp' },
    { keyVariants: ['LoS-Rx Light', 'LoS-RxLight', 'LoS Rx Light', 'losrxlight'], label: 'Rx Light', apiField: 'los_rx_light' },
    { keyVariants: ['LoS- R2', 'LoS-R2', 'LoS - R2', 'losr2'], label: 'R2', apiField: 'los_r2' },
    { keyVariants: ['LoS-HeartBeat', 'LoS- HeartBeat', 'losheartbeat'], label: 'HeartBeat', apiField: 'los_heartbeat' },
    // Label updated to PPM-M-LO
    { keyVariants: ['LoS - PPM', 'LoS- PPM', 'LoS-PPM', 'los_ppm', 'ppm', 'losppm'], label: 'PPM-M-LO', apiField: 'los_ppm' }
  ];

  // Latest values for tiles (values correspond to currently selected station)
  const latest = {};
  KEYS.forEach(k => latest[k.label] = { value: null, updated_at: null, raw: null });

  // Remote stations
  let remoteStations = []; // array of { serial_number, ip, display, canonical }
  let selectedSerial = null; // canonical serial of currently selected station (always a station, no "All")
  const deviceStatusMap = new Map(); // canonical serial -> last status object

  // Utility: canonicalize keys/serials for robust matching (lowercase, remove spaces/hyphens/underscores/parentheses)
  function canonicalKey(k) {
    return String(k || '').toLowerCase().replace(/[\s\-\(\)_]/g, '');
  }

  // Create tiles (popup removed by design)
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

      // NOTE: popup intentionally removed so hovering tiles does nothing

      tile.appendChild(label);
      tile.appendChild(value);
      tile.appendChild(meta);

      tilesContainer.appendChild(tile);
    });
  }

  // Update tiles with current latest map
  function updateTiles() {
    KEYS.forEach(k => {
      const info = latest[k.label];
      const tile = document.getElementById('tile-' + k.apiField);
      if (!tile) return;
      const valueEl = tile.querySelector('.value');
      const metaEl = tile.querySelector('.meta');

      if (info.value === null || info.value === undefined) {
        valueEl.textContent = '-';
        metaEl.textContent = 'No data';
      } else {
        let displayValue = info.value;
        if (k.label === 'R2') {
          const n = Number(info.value);
          if (!Number.isNaN(n)) displayValue = n / 100;
        }
        const display = (typeof displayValue === 'number' && !Number.isInteger(displayValue)) ? displayValue.toFixed(2) : String(displayValue);
        valueEl.textContent = display;
        metaEl.textContent = '';
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

  function clearAllReadings() {
    KEYS.forEach(k => {
      latest[k.label] = { value: null, updated_at: null, raw: null };
    });
    updateTiles();
  }

  // --- Status handling (tied to selectedSerial only) ---
  let deviceOnline = null;
  let deviceIp = null;
  let deviceRtt = null;
  let deviceLastSeen = null;

  function setStatusVisual(online) {
    if (!statusDot || !statusLabel) return;
    if (online === null) {
      statusDot.style.background = 'var(--muted)';
      statusDot.style.boxShadow = 'none';
      statusLabel.textContent = 'Checking…';
      return;
    }
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

  // Apply currently selected serial's status
  function applySelectedDeviceStatus() {
    if (!selectedSerial) {
      deviceOnline = null;
      deviceIp = null;
      deviceRtt = null;
      deviceLastSeen = null;
      setStatusVisual(null);
      return;
    }

    const st = deviceStatusMap.get(selectedSerial);
    if (!st) {
      deviceOnline = null;
      deviceIp = null;
      deviceRtt = null;
      deviceLastSeen = null;
      setStatusVisual(null);
      return;
    }
    deviceOnline = !!st.online;
    deviceIp = st.ip || null;
    deviceRtt = (st.rtt !== undefined && st.rtt !== null) ? Number(st.rtt) : null;
    deviceLastSeen = st.when || st.ts || null;
    setStatusVisual(deviceOnline);
  }

  // Socket device_status events: update map and if it matches selection update UI
  socket.on('device_status', (s) => {
    if (!s || typeof s !== 'object') return;
    const rawSerial = s.serial_number || s.serial || s.serialNumber || null;
    if (rawSerial) {
      const c = canonicalKey(rawSerial);
      deviceStatusMap.set(c, Object.assign({}, s, { canonical_serial: c }));
    } else if (s.ip) {
      deviceStatusMap.set('ip:' + s.ip, Object.assign({}, s));
    }

    if (rawSerial && selectedSerial && canonicalKey(rawSerial) === selectedSerial) {
      applySelectedDeviceStatus();
    }

    addToFeed(`[${new Date((s.when || s.ts) || Date.now()).toLocaleTimeString()}] PING ${s.ip || ''} — ${s.online ? 'online' : 'offline'} ${rawSerial ? ('[' + rawSerial + ']') : ''}`);
  });

  // --- Init: fetch remote stations AND device status snapshot (ordering matters) ---
  async function fetchRemoteStationsAndPopulate() {
    try {
      const res = await fetch('/api/remote_stations', { cache: 'no-cache' });
      if (!res.ok) throw new Error('failed');
      const json = await res.json();
      if (Array.isArray(json)) {
        // Use server-provided display label (authoritative by serial). Do NOT remap by IP here.
        remoteStations = json.map(r => {
          const serial = r.serial_number || r.serial || r.serialNumber || '';
          const ip = r.ip || r.ip_address || '';
          const display = r.display || (serial || '(unknown)'); // strictly prefer server-provided display
          return { serial_number: serial, ip, display, canonical: canonicalKey(serial) };
        });
      } else if (json && typeof json === 'object') {
        // If server returned an object map (serial -> ip), derive list using serial only (no IP-based naming)
        remoteStations = Object.keys(json).map(k => ({ serial_number: k, ip: json[k], display: k, canonical: canonicalKey(k) }));
      } else {
        remoteStations = [];
      }
    } catch (e) {
      console.warn('Could not load remote stations:', e && e.message ? e.message : e);
      remoteStations = [];
    }

    const select = document.getElementById('station-select');
    if (!select) return;

    Array.from(select.querySelectorAll('option')).forEach(opt => opt.remove());

    remoteStations.forEach(rs => {
      const opt = document.createElement('option');
      opt.value = rs.canonical; // store canonical serial as value
      opt.dataset.display = rs.display;
      opt.textContent = rs.display;
      select.appendChild(opt);
    });

    if (remoteStations.length > 0) {
      selectedSerial = remoteStations[0].canonical;
      select.value = selectedSerial;
      applySelectedDeviceStatus();
      clearAllReadings();
      addToFeed(`[${new Date().toLocaleTimeString()}] Auto-selected station: ${remoteStations[0].display}`);
    } else {
      selectedSerial = null;
      const disabledOpt = document.createElement('option');
      disabledOpt.value = '';
      disabledOpt.textContent = 'No stations configured';
      select.appendChild(disabledOpt);
      select.disabled = true;
      addToFeed(`[${new Date().toLocaleTimeString()}] No remote stations returned from server`);
    }
  }

  async function initStatusSnapshot() {
    try {
      const res = await fetch('/_device_status', { cache: 'no-cache' });
      if (!res.ok) throw new Error('failed');
      const j = await res.json();
      const list = j && j.status ? j.status : null;
      if (Array.isArray(list)) {
        for (const st of list) {
          const serial = st.serial_number || st.serial || st.serialNumber || null;
          if (serial) {
            const c = canonicalKey(serial);
            deviceStatusMap.set(c, Object.assign({}, st, { canonical_serial: c }));
          }
        }
      } else if (list && typeof list === 'object') {
        const serial = list.serial_number || list.serial || list.serialNumber || null;
        if (serial) {
          const c = canonicalKey(serial);
          deviceStatusMap.set(c, Object.assign({}, list, { canonical_serial: c }));
        }
      }
    } catch (e) {
      // ignore; map may be empty
    } finally {
      applySelectedDeviceStatus();
      if (deviceLastSeen) {
        addToFeed(`[${new Date(deviceLastSeen).toLocaleTimeString()}] Initial ping status applied`);
      }
    }
  }

  // Create dropdown and insert next to status
  function createStationDropdown() {
    const wrapper = document.createElement('div');
    wrapper.id = 'status-wrapper';
    if (statusLabel && statusLabel.parentNode) {
      const parent = statusLabel.parentNode;
      parent.insertBefore(wrapper, statusLabel);
      if (statusDot) wrapper.appendChild(statusDot);
      wrapper.appendChild(statusLabel);
    } else {
      document.body.insertBefore(wrapper, document.body.firstChild);
      if (statusDot) wrapper.appendChild(statusDot);
      if (statusLabel) wrapper.appendChild(statusLabel);
    }

    const select = document.createElement('select');
    select.id = 'station-select';
    select.title = 'Select remote station';

    select.addEventListener('change', (ev) => {
      const val = ev.target.value || '';
      if (!val) {
        addToFeed(`[${new Date().toLocaleTimeString()}] Invalid station selected`);
        return;
      }
      selectedSerial = val;
      applySelectedDeviceStatus();
      clearAllReadings();
      const display = ev.target.selectedOptions && ev.target.selectedOptions[0] ? ev.target.selectedOptions[0].dataset.display || ev.target.selectedOptions[0].textContent : selectedSerial;
      addToFeed(`[${new Date().toLocaleTimeString()}] Selected station: ${display}`);
    });

    wrapper.appendChild(select);
  }

  // Insert dropdown first, then load stations and status snapshot
  createStationDropdown();
  fetchRemoteStationsAndPopulate().then(initStatusSnapshot).catch(() => { initStatusSnapshot(); });

  // --- Local time ---
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

  // --- Results count & selection visuals (unchanged) ---
  const selectionGroupSelector = '#fetch, #graph, .btn.preset';
  function clearSelectionStyles() {
    const all = document.querySelectorAll(selectionGroupSelector);
    all.forEach(el => { el.classList.remove('selected'); el.style.background = ''; el.style.color = ''; el.style.boxShadow = ''; });
  }
  function applySelectionStyle(el) {
    clearSelectionStyles();
    if (!el) return;
    el.classList.add('selected');
    el.style.background = '#ef4444';
    el.style.color = '#fff';
    el.style.boxShadow = '0 6px 14px rgba(239,68,68,0.12)';
  }
  function wireSelectionHandlers() {
    const elems = document.querySelectorAll(selectionGroupSelector);
    elems.forEach(el => el.addEventListener('click', () => { if (!el.classList.contains('selected')) applySelectionStyle(el); }));
  }
  wireSelectionHandlers();

  function createOrGetCountEl() {
    let countEl = document.getElementById('results-count');
    if (!countEl) {
      countEl = document.createElement('div');
      countEl.id = 'results-count';
      countEl.style.marginTop = '8px';
      countEl.style.color = 'var(--muted)';
      countEl.style.fontSize = '13px';
      countEl.style.fontWeight = '600';
    }
    return countEl;
  }
  function placeCountElementVisible() {
    if (!resultsEl) return;
    const countEl = createOrGetCountEl();
    const parent = resultsEl.parentNode || document.body;
    if (countEl.parentNode !== parent || countEl.nextSibling !== resultsEl.nextSibling) {
      if (countEl.parentNode) countEl.parentNode.removeChild(countEl);
      if (resultsEl.nextSibling) parent.insertBefore(countEl, resultsEl.nextSibling);
      else parent.appendChild(countEl);
    }
  }
  function setResultsCount(count) {
    const countEl = createOrGetCountEl();
    countEl.textContent = `Total Count: ${count.toLocaleString()}`;
    placeCountElementVisible();
  }
  function removeFetchAllButtonIfPresent() {
    if (!resultsEl) return false;
    const candidates = Array.from(resultsEl.querySelectorAll('button, input[type="button"], a'));
    for (const el of candidates) {
      const txt = (el.textContent || el.value || '').trim().toLowerCase();
      const id = (el.id || '').toLowerCase();
      const cls = (el.className || '').toLowerCase();
      if (txt.includes('fetch all') || txt === 'fetch all' || (id.includes('fetch') && id.includes('all')) || (cls.includes('fetch') && cls.includes('all'))) {
        try { el.parentNode.removeChild(el); } catch (e) {}
        return true;
      }
    }
    return false;
  }
  function updateResultsCountNow() {
    let totalAll = 0;
    if (typeof rowtotal !== 'undefined' && typeof rowtotal === 'number') totalAll = rowtotal;
    else if (typeof window.rowtotal !== 'undefined' && typeof window.rowtotal === 'number') totalAll = window.rowtotal;
    else totalAll = 0;
    removeFetchAllButtonIfPresent();
    setResultsCount(totalAll);
  }
  if (resultsEl) {
    const mo = new MutationObserver((mutationsList) => {
      let relevant = false;
      for (const m of mutationsList) {
        if (m.type === 'childList' || m.type === 'subtree' || m.type === 'attributes') { relevant = true; break; }
      }
      if (relevant) {
        clearTimeout(window.__results_count_timer__);
        window.__results_count_timer__ = setTimeout(updateResultsCountNow, 120);
      }
    });
    mo.observe(resultsEl, { childList: true, subtree: true, attributes: false });
    setTimeout(updateResultsCountNow, 200);
  }

  // --- Helpers for canonical matching / ppm merging ---
  function canonicalMatch(keyA, keyB) {
    const a = canonicalKey(keyA);
    const b = canonicalKey(keyB);
    if (a === b) return true;
    if (a.includes(b) || b.includes(a)) return true;
    return false;
  }

  // Try to extract serial from msg in several common places
  function extractSerialFromMsg(msg) {
    if (!msg || typeof msg !== 'object') return null;
    const candidates = [
      msg.serial_number, msg.serial, msg.serialNumber,
      msg.payload && (msg.payload.serial_number || msg.payload.serial || msg.payload.serialNumber),
      (msg.payload && msg.payload.params && (msg.payload.params.serial_number || msg.payload.params.serial || msg.payload.params.serialNumber)),
      msg.los && (msg.los.serial_number || msg.los.serial)
    ];
    for (const c of candidates) {
      if (c !== undefined && c !== null && String(c).trim() !== '') return String(c).trim();
    }
    return null;
  }

  // --- MQTT message handling ---
  socket.on('mqtt_message', (msg) => {
    const ts = msg.received_at || new Date().toISOString();
    const rawSerial = extractSerialFromMsg(msg);
    const msgCanonical = rawSerial ? canonicalKey(rawSerial) : null;

    if (!selectedSerial) {
      addToFeed(`[${new Date(ts).toLocaleTimeString()}] MQTT message ignored (no station selected) — ${msg.topic}`);
      return;
    }

    if (!msgCanonical || msgCanonical !== selectedSerial) {
      addToFeed(`[${new Date(ts).toLocaleTimeString()}] Ignored MQTT for ${rawSerial || '(no serial)'} — topic ${msg.topic}`);
      return;
    }

    // Prefer server-provided los object when available (server has already applied merging rules)
    let los = null;
    if (msg.los && typeof msg.los === 'object' && Object.keys(msg.los).length > 0) {
      // clone to avoid mutating the original
      los = Object.assign({}, msg.los);
    }

    let params = null;
    if (!los) {
      if (msg.payload && typeof msg.payload === 'object') {
        params = (msg.payload.params && typeof msg.payload.params === 'object') ? msg.payload.params : msg.payload;
      } else if (typeof msg.raw === 'string') {
        try {
          const parsed = JSON.parse(msg.raw);
          params = (parsed && parsed.params && typeof parsed.params === 'object') ? parsed.params : parsed;
        } catch (e) {
          params = null;
        }
      }
      if (!params || typeof params !== 'object') params = {};
    }

    // If server didn't provide los, build los from params and perform client-side merge detection as fallback.
    let mergedPpm = null;
    if (!los) {
      // Build canonical map and detect ppm int+dec
      const pmap = {};
      Object.keys(params).forEach(k => { pmap[canonicalKey(k)] = { originalKey: k, value: params[k] }; });

      let intVal, decVal;
      for (const [ck, entry] of Object.entries(pmap)) {
        const raw = entry.value;
        const num = (raw === null || raw === undefined) ? NaN : Number(raw);
        if (Number.isNaN(num)) continue;
        if (ck.includes('pp') && (ck.includes('int') || ck.includes('mlo'))) { intVal = num; continue; }
        if (ck.includes('pp') && (ck.includes('dec') || ck.includes('decimal'))) { decVal = num; continue; }
        if ((ck.endsWith('int') || ck.endsWith('mloint')) && ck.includes('pp')) intVal = num;
        if (ck.endsWith('dec') && ck.includes('pp')) decVal = num;
      }

      // MERGING: correctly combine integer and decimal parts (client-side fallback only)
      if (typeof intVal !== 'undefined' && typeof decVal !== 'undefined') {
        const iPart = Number(intVal);
        if (Number.isFinite(iPart)) {
          const decInt = Math.trunc(Math.abs(Number(decVal)));
          if (Number.isFinite(decInt)) {
            const decStr = String(decInt);
            const divisor = (decStr.length === 1) ? 10 : Math.pow(10, decStr.length);
            const sign = (iPart < 0) ? -1 : 1;
            const absMerged = Math.abs(iPart) + (decInt / divisor);
            const merged = sign < 0 ? -absMerged : absMerged;
            mergedPpm = Number.isFinite(merged) ? merged : null;
          }
        }
      }

      // Collect los-like keys from params
      los = {};
      Object.keys(params).forEach(kName => {
        if (typeof kName === 'string' && kName.trim().toLowerCase().startsWith('los')) {
          los[kName] = params[kName];
        }
      });

      // If mergedPpm present and no server los, inject a canonical PPM key so matching works
      if (mergedPpm !== null) {
        los['LoS-PPM'] = mergedPpm;
      }
    }

    if (!los || Object.keys(los).length === 0) return;

    // If device offline for the selected station, ignore incoming readings
    if (deviceOnline === false) {
      addToFeed(`[${new Date(ts).toLocaleTimeString()}] MQTT reading ignored (device offline) — ${msg.topic}`);
      return;
    }

    // Update latest using exact-match-first, then tolerant includes fallback
    KEYS.forEach(mapping => {
      let found = false;

      // Build canonical variants for this mapping once
      const variantCans = mapping.keyVariants.map(v => canonicalKey(v));

      // Exact canonical match phase
      for (const actualKey of Object.keys(los)) {
        const aCan = canonicalKey(actualKey);
        if (variantCans.includes(aCan)) {
          latest[mapping.label] = { value: los[actualKey], updated_at: ts, raw: los };
          found = true;
          break;
        }
      }

      if (!found) {
        // Fallback tolerant/includes match
        for (const variant of mapping.keyVariants) {
          for (const actualKey of Object.keys(los)) {
            if (canonicalMatch(actualKey, variant)) {
              latest[mapping.label] = { value: los[actualKey], updated_at: ts, raw: los };
              found = true;
              break;
            }
          }
          if (found) break;
        }
      }

      // final fallback: if mapping is PPM and we have mergedPpm (client-side fallback) and wasn't matched above, set it
      if (!found && mapping.apiField === 'los_ppm') {
        // Prefer server-provided los.los_ppm if given
        if (los && (los.los_ppm !== undefined)) {
          latest[mapping.label] = { value: los.los_ppm, updated_at: ts, raw: los };
        } else if (mergedPpm !== null) {
          latest[mapping.label] = { value: mergedPpm, updated_at: ts, raw: los };
        }
      }
    });

    updateTiles();

    addToFeed(`[${new Date(ts).toLocaleTimeString()}] [${rawSerial}] ${msg.topic} — ${JSON.stringify(los)}`);
  });

  // Initial render
  createTiles();
  updateTiles();
})();
