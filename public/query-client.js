// V4 — Copilot workbench rewrite
// query-client.js — full-featured query UI: presets, fetch, clear, CSV export.
// Pagination: pageSize 2000 with Prev/Next/First/Last. Controls shown only after results.
// Uses recorded_at_str if provided by server, otherwise falls back to recorded_at.
//
// NOTE: Adds a polished, read-only 24-hour preview next to the "From" and "To"
// datetime-local inputs. Styling is contained here (injected) and spacing is generous
// so the two formats are visually distinct and not confusing. No other page markup
// or behavior is changed.
//
// Added: Graph button logic that fetches paged rows and renders a Chart.js line chart for PPM vs Time.
// The chart is rendered into #chart-area (canvas #ppm-chart) and does NOT replace the table.
// Integrates chartjs-plugin-zoom for interactive mouse/gesture zoom & pan.
//
// CHANGES: Injects PDF, Clear and RESET buttons (PDF uses Graph Title for filename & PDF header).

(function () {
  const fetchBtn = document.getElementById('fetch');
  const graphBtn = document.getElementById('graph');
  const fromInput = document.getElementById('from');
  const toInput = document.getElementById('to');
  const resultsEl = document.getElementById('results');
  const resultsEmpty = document.getElementById('results-empty');
  const presets = document.querySelectorAll('.preset');
  const clearResultsBtn = document.getElementById('clear-results');
  const exportCsvBtn = document.getElementById('export-csv');
  const chartArea = document.getElementById('chart-area');
  const chartMeta = document.getElementById('chart-meta');
  const chartCanvas = document.getElementById('ppm-chart');

  // Chart.js instance reference
  let ppmChart = null;

  // Chart control injection guard
  let chartControlsInjected = false;

  // custom pan state holder so we can attach/detach once per canvas
  let __customPanAttached = false;

  // --- 24-hour preview helpers (polished look) ---
  function pad2(v) { return String(v).padStart(2, '0'); }

  function formatDateTo24Short(d) {
    if (!(d instanceof Date) || isNaN(d.getTime())) return '';
    const day = pad2(d.getDate());
    const month = pad2(d.getMonth() + 1);
    const year = d.getFullYear();
    const hours = pad2(d.getHours());
    const minutes = pad2(d.getMinutes());
    return `${day}/${month}/${year} ${hours}:${minutes}`;
  }

  function formatInputValueTo24(val) {
    if (!val) return '';
    // datetime-local string is local; constructing Date(val) treats it as local in most browsers
    const d = new Date(val);
    if (isNaN(d.getTime())) return '';
    return formatDateTo24Short(d);
  }

  // inject styles for preview (only once)
  (function injectPreviewStyles() {
    if (document.getElementById('__dt24_preview_styles__')) return;
    const css = `
      .dt24-preview {
        margin-left: 12px;
        color: var(--muted);
        background: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.03);
        padding: 5px 10px;
        font-size: 13px;
        border-radius: 8px;
        display: inline-block;
        min-width: 170px;
        text-align: left;
        user-select: none;
        pointer-events: none;
        box-shadow: 0 6px 14px rgba(2,6,23,0.35);
      }

      /* extra separation specifically when the preview follows the FROM input */
      .dt24-preview.dt24-preview--from {
        margin-right: 40px; /* visual gap between FROM block and TO block */
      }

      /* Slightly smaller on very small screens */
      @media (max-width:800px) {
        .dt24-preview {
          min-width: 140px;
          padding: 4px 8px;
          margin-left: 10px;
        }
        .dt24-preview.dt24-preview--from {
          margin-right: 20px;
        }
      }
      @media (max-width:420px) {
        .dt24-preview { font-size:12px; min-width: 120px; padding:4px 8px; margin-left:8px; }
        .dt24-preview.dt24-preview--from { margin-right: 8px; display:block; margin-top:6px; }
      }
    `;
    const s = document.createElement('style');
    s.id = '__dt24_preview_styles__';
    s.type = 'text/css';
    s.appendChild(document.createTextNode(css));
    (document.head || document.documentElement).appendChild(s);
  })();

  function create24Preview(inputEl) {
    if (!inputEl) return () => {};
    // Avoid creating duplicate preview
    if (inputEl.__dt24_preview_el__) {
      // return updater
      return () => {
        if (inputEl.__dt24_preview_el__) inputEl.__dt24_preview_el__.textContent = formatInputValueTo24(inputEl.value) || '';
      };
    }

    const span = document.createElement('span');
    span.className = 'dt24-preview';
    // Add special class for the FROM input so we can give a larger right margin (visual only)
    if (inputEl.id === 'from' || inputEl === fromInput) {
      span.classList.add('dt24-preview--from');
    }
    span.setAttribute('aria-hidden', 'true');
    span.textContent = formatInputValueTo24(inputEl.value) || '';

    // Insert after the input element
    if (inputEl.parentNode) {
      // prefer inserting after input so it sits visually next to it
      if (inputEl.nextSibling) inputEl.parentNode.insertBefore(span, inputEl.nextSibling);
      else inputEl.parentNode.appendChild(span);
    } else {
      // fallback: append to body (shouldn't normally happen)
      document.body.appendChild(span);
    }

    // updater
    const upd = () => {
      try {
        span.textContent = formatInputValueTo24(inputEl.value) || '';
      } catch (e) { /* ignore */ }
    };

    // wire events
    inputEl.addEventListener('input', upd);
    inputEl.addEventListener('change', upd);

    // store reference and return updater
    inputEl.__dt24_preview_el__ = span;
    return upd;
  }

  // Create previews and keep updater functions
  const updateFromPreview = create24Preview(fromInput);
  const updateToPreview = create24Preview(toInput);

  // Ensure previews update on user interactions that change inputs
  if (fromInput) {
    fromInput.addEventListener('input', () => {
      try { updateFromPreview(); } catch (e) { /* ignore */ }
    });
    fromInput.addEventListener('change', () => {
      try { updateFromPreview(); } catch (e) { /* ignore */ }
    });
  }
  if (toInput) {
    toInput.addEventListener('input', () => {
      try { updateToPreview(); } catch (e) { /* ignore */ }
    });
    toInput.addEventListener('change', () => {
      try { updateToPreview(); } catch (e) { /* ignore */ }
    });
  }

  // Update previews when presets, init defaults, or fetch actions modify values
  function safeUpdatePreviews() {
    try { updateFromPreview(); } catch (e) { /* ignore */ }
    try { updateToPreview(); } catch (e) { /* ignore */ }
  }

  // Pagination UI will be injected dynamically below the results
  let pagingControlsEl = null;

  // State
  const pageSize = 2000;      // rows per page shown in UI
  let currentPage = 0;        // zero-based page index
  const pageCache = new Map(); // pageIndex -> rows[]
  let lastFetchedCountForPage = new Map(); // pageIndex -> rows.length
  let lastRequestParamsHash = ''; // used to detect param change and clear cache

  // Total-scan control: avoid concurrent total scans for different queries
  let totalScanInProgressForHash = null;

  // Helper: format date for datetime-local input
  function isoLocalString(d) {
    const pad = (n) => String(n).padStart(2,'0');
    const yyyy = d.getFullYear();
    const mm = pad(d.getMonth()+1);
    const dd = pad(d.getDate());
    const hh = pad(d.getHours());
    const min = pad(d.getMinutes());
    return `${yyyy}-${mm}-${dd}T${hh}:${min}`;
  }

  // -------------------------
  // Parsing & Local Formatting
  // -------------------------
  function parseToDate(val) {
    if (val === null || val === undefined || val === '') return null;
    if (val instanceof Date && !isNaN(val.getTime())) return val;
    if (typeof val === 'number' && Number.isFinite(val)) {
      if (val > 1e12) return new Date(val);
      if (val > 1e9) return new Date(val * 1000);
      return null;
    }
    if (typeof val === 'string') {
      const s = val.trim();
      if (!s) return null;
      if (/^\d+$/.test(s)) {
        const n = Number(s);
        if (n > 1e12) return new Date(n);
        if (n > 1e9) return new Date(n * 1000);
      }
      const parsed = Date.parse(s);
      if (!Number.isNaN(parsed)) return new Date(parsed);
      const alt = s.replace(' ', 'T');
      const parsed2 = Date.parse(alt);
      if (!Number.isNaN(parsed2)) return new Date(parsed2);
      return null;
    }
    return null;
  }

  function formatDateLocal(d) {
    const day = pad2(d.getDate());
    const month = pad2(d.getMonth() + 1);
    const year = d.getFullYear();
    const hours = pad2(d.getHours());
    const minutes = pad2(d.getMinutes());
    const seconds = pad2(d.getSeconds());
    return `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;
  }

  function toLocalDisplay(val) {
    if (val === null || val === undefined || val === '') return '';
    const dt = parseToDate(val);
    if (dt) return formatDateLocal(dt);
    try { return String(val); } catch (e) { return ''; }
  }

  // Init defaults: populate inputs with last 1 hour and update previews
  (function initDefaults() {
    const now = new Date();
    const from = new Date(now.getTime() - 3600 * 1000);
    if (fromInput) fromInput.value = isoLocalString(from);
    if (toInput) toInput.value = isoLocalString(now);
    safeUpdatePreviews();
  })();

  // Inject paging controls UI (below the results element)
  (function injectPagingControls() {
    try {
      const panel = document.getElementById('query-panel');
      if (!panel || !resultsEl) return;

      pagingControlsEl = document.createElement('div');
      pagingControlsEl.style.display = 'none';
      pagingControlsEl.style.alignItems = 'center';
      pagingControlsEl.style.gap = '8px';
      pagingControlsEl.style.marginTop = '8px';
      pagingControlsEl.style.flexWrap = 'wrap';
      pagingControlsEl.style.display = 'flex';

      const firstBtn = document.createElement('button');
      firstBtn.className = 'btn';
      firstBtn.textContent = 'First';
      firstBtn.id = 'page-first';
      firstBtn.disabled = true;

      const prevBtn = document.createElement('button');
      prevBtn.className = 'btn secondary';
      prevBtn.textContent = 'Prev';
      prevBtn.disabled = true;
      prevBtn.id = 'page-prev';

      const pageLabel = document.createElement('div');
      pageLabel.id = 'page-label';
      pageLabel.style.color = 'var(--muted)';
      pageLabel.textContent = 'Page 0';

      const nextBtn = document.createElement('button');
      nextBtn.className = 'btn';
      nextBtn.textContent = 'Next';
      nextBtn.id = 'page-next';
      nextBtn.disabled = true;

      const lastBtn = document.createElement('button');
      lastBtn.className = 'btn';
      lastBtn.textContent = 'Last';
      lastBtn.id = 'page-last';
      lastBtn.disabled = true;

      pagingControlsEl.appendChild(firstBtn);
      pagingControlsEl.appendChild(prevBtn);
      pagingControlsEl.appendChild(pageLabel);
      pagingControlsEl.appendChild(nextBtn);
      pagingControlsEl.appendChild(lastBtn);

      const spacer = document.createElement('div');
      spacer.style.flex = '1';
      pagingControlsEl.appendChild(spacer);

      if (exportCsvBtn && exportCsvBtn.parentNode !== pagingControlsEl) {
        pagingControlsEl.appendChild(exportCsvBtn);
      }
      if (clearResultsBtn && clearResultsBtn.parentNode !== pagingControlsEl) {
        clearResultsBtn.classList.add('secondary');
        pagingControlsEl.appendChild(clearResultsBtn);
      }

      if (resultsEl && resultsEl.parentNode) resultsEl.parentNode.insertBefore(pagingControlsEl, resultsEl.nextSibling);
      else panel.appendChild(pagingControlsEl);

      firstBtn.addEventListener('click', () => {
        if (currentPage !== 0) {
          currentPage = 0;
          loadAndRenderPage(currentPage);
        }
      });
      prevBtn.addEventListener('click', () => {
        if (currentPage > 0) {
          currentPage -= 1;
          loadAndRenderPage(currentPage);
        }
      });
      nextBtn.addEventListener('click', () => {
        currentPage += 1;
        loadAndRenderPage(currentPage);
      });

      lastBtn.addEventListener('click', async () => {
        const params = getParamsForCurrentInputs();
        if (!params) return;
        if (!pageCache.has(0)) {
          const firstRows = await fetchPageRows(0, params.fromISO, params.toISO);
          if (firstRows === null) return;
          pageCache.set(0, firstRows);
          lastFetchedCountForPage.set(0, firstRows.length);
          if (firstRows.length === 0) {
            currentPage = 0;
            loadAndRenderPage(0);
            return;
          }
        }

        let cached = Array.from(pageCache.keys()).sort((a,b) => a - b);
        let start = (cached.length === 0) ? 0 : (cached[cached.length - 1] + 1);
        if (start === 0) {
          const c0 = lastFetchedCountForPage.get(0);
          if (typeof c0 === 'number' && c0 < pageSize) {
            currentPage = 0;
            loadAndRenderPage(currentPage);
            return;
          }
        }

        if (resultsEl) resultsEl.innerHTML = `<div style="color:var(--muted)">Locating last page…</div>`;

        const MAX_PAGES_TO_SCAN = 2000;
        let p = start;
        let found = false;

        for (; p < start + MAX_PAGES_TO_SCAN; p++) {
          if (pageCache.has(p)) {
            const rowsCached = pageCache.get(p) || [];
            lastFetchedCountForPage.set(p, rowsCached.length);
            if (rowsCached.length === 0) {
              currentPage = Math.max(0, p - 1);
              found = true;
              break;
            }
            if (rowsCached.length < pageSize) {
              currentPage = p;
              found = true;
              break;
            }
            continue;
          }

          const rows = await fetchPageRows(p, params.fromISO, params.toISO);
          if (rows === null) break;
          pageCache.set(p, rows);
          lastFetchedCountForPage.set(p, rows.length);
          if (rows.length === 0) {
            currentPage = Math.max(0, p - 1);
            found = true;
            break;
          }
          if (rows.length < pageSize) {
            currentPage = p;
            found = true;
            break;
          }
        }

        if (!found) {
          const allCached = Array.from(pageCache.keys());
          if (allCached.length > 0) {
            allCached.sort((a,b) => a - b);
            currentPage = allCached[allCached.length - 1];
          } else currentPage = 0;
        }

        loadAndRenderPage(currentPage);
      });

      hidePagingControls();
    } catch (e) {
      console.error('injectPagingControls error', e);
    }
  })();

  // Preset buttons (Last 1h, 6h, 12h, 24h, etc.)
  if (presets && presets.forEach) {
    presets.forEach(btn => {
      btn.addEventListener('click', () => {
        const range = btn.dataset.range;
        const now = new Date();
        let from;
        if (range && range.endsWith && range.endsWith('h')) {
          const hours = Number(range.replace('h','')) || 1;
          from = new Date(now.getTime() - hours * 3600 * 1000);
        } else if (range && range.endsWith && range.endsWith('d')) {
          const days = Number(range.replace('d','')) || 1;
          from = new Date(now.getTime() - days * 24 * 3600 * 1000);
        } else {
          from = new Date(now.getTime() - 3600 * 1000);
        }
        if (fromInput) fromInput.value = isoLocalString(from);
        if (toInput) toInput.value = isoLocalString(now);
        safeUpdatePreviews();
      });
    });
  }

  // Fetch button: start a new query, clear cache and load first page
  if (fetchBtn) {
    fetchBtn.addEventListener('click', () => {
      const params = getParamsForCurrentInputs();
      if (!params) return;
      clearCacheForNewQuery(params.hash);
      currentPage = 0;
      safeUpdatePreviews();
      loadAndRenderPage(currentPage);
    });
  }

  // Graph button: fetch data (paged) and render PPM vs Time chart
  if (graphBtn) {
    graphBtn.addEventListener('click', async () => {
      const params = getParamsForCurrentInputs();
      if (!params) return;
      // Keep table state as-is; chart is an additional view
      try {
        graphBtn.disabled = true;
        chartMeta.textContent = 'Loading…';
        await renderPpmGraphForParams(params);
      } finally {
        graphBtn.disabled = false;
      }
    });
  }

  // Clear button
  if (clearResultsBtn) {
    clearResultsBtn.addEventListener('click', () => {
      pageCache.clear();
      lastFetchedCountForPage.clear();
      lastRequestParamsHash = '';
      currentPage = 0;
      if (resultsEl) {
        resultsEl.innerHTML = '';
        if (resultsEmpty) resultsEl.appendChild(resultsEmpty);
      }
      hidePagingControls();
      updatePageLabel();
      setPagingButtonsState();
      try { window.rowtotal = 0; } catch (e) { /* ignore */ }
      totalScanInProgressForHash = null;

      // hide chart if visible and destroy chart
      if (chartArea) {
        chartArea.style.display = 'none';
        chartArea.setAttribute('aria-hidden', 'true');
      }
      if (ppmChart) {
        try { ppmChart.destroy(); } catch (e) { /* ignore */ }
        ppmChart = null;
      }
    });
  }

  // Export CSV
  if (exportCsvBtn) {
    exportCsvBtn.addEventListener('click', () => {
      let rowsToExport = [];
      if (pageCache.size > 1) {
        const pages = Array.from(pageCache.keys()).sort((a,b) => a - b);
        for (const p of pages) rowsToExport = rowsToExport.concat(pageCache.get(p) || []);
      } else if (pageCache.size === 1) {
        const rows = pageCache.get(currentPage) || pageCache.values().next().value || [];
        rowsToExport = rows.slice();
      } else {
        alert('No data to export. Fetch a page first.');
        return;
      }

      if (!rowsToExport || rowsToExport.length === 0) {
        alert('No data to export');
        return;
      }

      // CSV headers changed to match visible table labels
      const headers = ['Recorded_At','Temp','Rx_Light','R2','HeartBeat','PPM-M-LO'];
      const lines = [headers.join(',')];
      rowsToExport.forEach(r => {
        const vals = [
          `"${r.recorded_at_str || r.recorded_at || ''}"`,
          r.los_temp ?? '',
          r.los_rx_light ?? '',
          r.los_r2 ?? '',
          r.los_heartbeat ?? '',
          r.los_ppm ?? ''
        ];
        // Escape any commas/newlines in fields by wrapping quoted fields as above for recorded_at
        // Other numeric fields are left as-is (or empty).
        lines.push(vals.join(','));
      });
      const csv = lines.join('\n');
      const blob = new Blob([csv], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `los_query_${Date.now()}.csv`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    });
  }

  // Load and render a specific page (uses cache if available)
  async function loadAndRenderPage(pageIndex) {
    const params = getParamsForCurrentInputs();
    if (!params) return;

    if (params.hash !== lastRequestParamsHash) clearCacheForNewQuery(params.hash);

    updatePageLabel();
    if (pageCache.has(pageIndex)) {
      renderResultsTable(pageCache.get(pageIndex));
      updatePageLabel();
      setPagingButtonsState();
      startTotalScanForParamsIfNeeded(params);
      return;
    }

    if (resultsEl) resultsEl.innerHTML = `<div style="color:var(--muted)">Loading page ${pageIndex + 1}…</div>`;
    const rows = await fetchPageRows(pageIndex, params.fromISO, params.toISO);
    if (!rows) {
      if (resultsEl) resultsEl.innerHTML = `<div style="color:var(--muted)">Error fetching page ${pageIndex + 1}</div>`;
      hidePagingControls();
      return;
    }

    if (rows.length === 0 && pageIndex > 0) {
      currentPage = pageIndex - 1;
      updatePageLabel();
      setPagingButtonsState();
      loadAndRenderPage(currentPage);
      return;
    }

    pageCache.set(pageIndex, rows);
    lastFetchedCountForPage.set(pageIndex, rows.length);
    renderResultsTable(rows);
    updatePageLabel();
    setPagingButtonsState();
    startTotalScanForParamsIfNeeded(params);
  }

  // Performs the actual fetch for a page index. Returns rows[] or null on error.
  async function fetchPageRows(pageIndex, fromISO, toISO) {
    try {
      const params = new URLSearchParams();
      if (fromISO) params.set('from', fromISO);
      if (toISO) params.set('to', toISO);
      params.set('limit', String(pageSize));
      params.set('offset', String(pageIndex * pageSize));
      const r = await fetch('/api/los?' + params.toString());
      if (!r.ok) throw new Error('Server returned ' + r.status);
      const data = await r.json();
      if (!data.ok) throw new Error('Query failed');
      const rows = data.rows || [];

      // guard against server ignoring offset: if identical to previous page, treat as empty
      if (pageIndex > 0 && pageCache.has(pageIndex - 1)) {
        const prev = pageCache.get(pageIndex - 1) || [];
        if (rows.length === prev.length && rows.length > 0) {
          const same = (rows[0].id === prev[0].id && rows[rows.length-1].id === prev[prev.length-1].id);
          if (same) {
            lastFetchedCountForPage.set(pageIndex, 0);
            return [];
          }
        }
      }
      return rows;
    } catch (err) {
      console.error('Query error', err);
      return null;
    }
  }

  // Returns param object or null if invalid
  function getParamsForCurrentInputs() {
    const from = (fromInput && fromInput.value) ? new Date(fromInput.value).toISOString() : undefined;
    const to = (toInput && toInput.value) ? new Date(toInput.value).toISOString() : undefined;

    if (from && to && new Date(from) > new Date(to)) {
      alert('From must be before To');
      return null;
    }
    const hash = `f=${from||''}&t=${to||''}&ps=${pageSize}`;
    return { fromISO: from, toISO: to, hash };
  }

  function clearCacheForNewQuery(newHash) {
    pageCache.clear();
    lastFetchedCountForPage.clear();
    lastRequestParamsHash = newHash || '';
    currentPage = 0;
    hidePagingControls();
    updatePageLabel();
    setPagingButtonsState();
    try { window.rowtotal = 0; } catch (e) { /* ignore */ }
    totalScanInProgressForHash = null;
  }

  function updatePageLabel() {
    const label = document.getElementById('page-label');
    if (!label) return;
    const fetchedCount = lastFetchedCountForPage.get(currentPage);
    const isLastKnown = typeof fetchedCount === 'number' && fetchedCount < pageSize;
    label.textContent = `Page ${currentPage + 1}${isLastKnown ? ' (end)' : ''}`;
  }

  function setPagingButtonsState() {
    const prevBtn = document.getElementById('page-prev');
    const nextBtn = document.getElementById('page-next');
    const firstBtn = document.getElementById('page-first');
    const lastBtn = document.getElementById('page-last');
    if (!prevBtn || !nextBtn) return;
    prevBtn.disabled = currentPage <= 0;
    if (firstBtn) firstBtn.disabled = currentPage <= 0;

    const curCount = lastFetchedCountForPage.get(currentPage);
    if (typeof curCount === 'number' && curCount < pageSize) {
      nextBtn.disabled = true;
      if (lastBtn) lastBtn.disabled = true;
    } else {
      nextBtn.disabled = false;
      if (lastBtn) lastBtn.disabled = false;
    }
  }

  function hidePagingControls() {
    if (!pagingControlsEl) return;
    try { pagingControlsEl.style.display = 'none'; } catch (e) { /* ignore */ }
  }
  function showPagingControls() {
    if (!pagingControlsEl) return;
    try { pagingControlsEl.style.display = 'flex'; } catch (e) { /* ignore */ }
  }

  // Render results table for a set of rows
  function renderResultsTable(rows) {
    if (!resultsEl) return;
    resultsEl.innerHTML = '';
    if (!rows || rows.length === 0) {
      if (resultsEmpty) resultsEl.appendChild(resultsEmpty);
      hidePagingControls();
      return;
    }

    const table = document.createElement('table');
    table.className = 'table';
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    ['Recorded_At','Temp','Rx_Light','R2','HeartBeat','PPM-M-LO'].forEach(h => {
      const th = document.createElement('th');
      th.textContent = h;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const row of rows) {
      const tr = document.createElement('tr');
      const tdTime = document.createElement('td');
      tdTime.className = 'nowrap';
      const sourceVal = row.recorded_at || row.recorded_at_raw || row.recorded_at_str || '';
      tdTime.textContent = toLocalDisplay(sourceVal);
      tr.appendChild(tdTime);

      const addCell = (val) => {
        const td = document.createElement('td');
        td.textContent = (val === null || val === undefined) ? '' : (typeof val === 'number' && !Number.isInteger(val) ? val.toFixed(2) : String(val));
        return td;
      };
      tr.appendChild(addCell(row.los_temp));
      tr.appendChild(addCell(row.los_rx_light));
      tr.appendChild(addCell(row.los_r2));
      tr.appendChild(addCell(row.los_heartbeat));
      tr.appendChild(addCell(row.los_ppm));

      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    resultsEl.appendChild(table);

    showPagingControls();
  }

  function startTotalScanForParamsIfNeeded(params) {
    if (!params || !params.hash) return;
    if (totalScanInProgressForHash === params.hash) return;
    totalScanInProgressForHash = params.hash;

    try {
      let interim = 0;
      const cachedPages = Array.from(pageCache.keys()).sort((a,b) => a - b);
      for (const p of cachedPages) {
        const arr = pageCache.get(p) || [];
        interim += arr.length;
      }
      window.rowtotal = Number(interim) || 0;
      pokeResultsForRowTotal();
    } catch (e) {
      try { window.rowtotal = 0; } catch (e2) { /* ignore */ }
    }

    (async () => {
      try {
        const total = await computeTotalRowsForQuery(params);
        try { window.rowtotal = Number(total) || 0; } catch (e) { /* ignore */ }
        pokeResultsForRowTotal();
      } catch (e) {
        console.error('computeTotalRowsForQuery error', e);
        try { window.rowtotal = 0; } catch (e2) { /* ignore */ }
        pokeResultsForRowTotal();
      } finally {
        if (totalScanInProgressForHash === params.hash) totalScanInProgressForHash = null;
      }
    })();
  }

  async function computeTotalRowsForQuery(params) {
    let sum = 0;
    const cachedPages = Array.from(pageCache.keys()).sort((a,b) => a - b);
    for (const p of cachedPages) {
      const arr = pageCache.get(p) || [];
      sum += arr.length;
    }

    if (cachedPages.length === 0) {
      const first = await fetchPageRows(0, params.fromISO, params.toISO);
      if (first === null) return 0;
      pageCache.set(0, first);
      lastFetchedCountForPage.set(0, first.length);
      sum += first.length;
      if (first.length < pageSize) return sum;
    }

    let start = (cachedPages.length === 0) ? 1 : (cachedPages[cachedPages.length - 1] + 1);
    const allCached = Array.from(pageCache.keys()).sort((a,b) => a - b);
    if (allCached.length > 0) start = allCached[allCached.length - 1] + 1;

    const MAX_PAGES_TO_SCAN = 10000;
    for (let p = start, scans = 0; scans < MAX_PAGES_TO_SCAN; p++, scans++) {
      const rows = await fetchPageRows(p, params.fromISO, params.toISO);
      if (rows === null) break;
      pageCache.set(p, rows);
      lastFetchedCountForPage.set(p, rows.length);
      sum += rows.length;
      if (rows.length < pageSize) return sum;
    }
    return sum;
  }

  function pokeResultsForRowTotal() {
    try {
      if (!resultsEl) return;
      const id = '__rowtotal_marker__';
      const prev = document.getElementById(id);
      if (prev && prev.parentNode) prev.parentNode.removeChild(prev);

      const marker = document.createElement('div');
      marker.id = id;
      marker.style.display = 'none';
      resultsEl.appendChild(marker);
      setTimeout(() => {
        try {
          if (marker.parentNode) marker.parentNode.removeChild(marker);
        } catch (e) { /* ignore */ }
      }, 0);
    } catch (e) { /* ignore */ }
  }

  // --- Chart control injection and helpers ---
  // helper to create a higher-resolution dataURL from canvas
  function canvasToDataURLHighRes(srcCanvas, scale = 2) {
    try {
      const w = srcCanvas.width;
      const h = srcCanvas.height;
      const tmp = document.createElement('canvas');
      tmp.width = Math.floor(w * scale);
      tmp.height = Math.floor(h * scale);
      tmp.style.width = w + 'px';
      tmp.style.height = h + 'px';
      const ctx = tmp.getContext('2d');
      ctx.setTransform(scale, 0, 0, scale, 0, 0);
      ctx.drawImage(srcCanvas, 0, 0, w, h);
      return tmp.toDataURL('image/png', 1.0);
    } catch (e) {
      try { return srcCanvas.toDataURL('image/png'); } catch (e2) { return null; }
    }
  }

  function ensureChartControls() {
    if (!chartArea) return;
    if (chartControlsInjected) return;
    try {
      const header = chartArea.querySelector('.chart-header');
      if (!header) return;

      const controls = document.createElement('div');
      controls.style.display = 'flex';
      controls.style.gap = '8px';
      controls.style.alignItems = 'center';

      // PDF button (true PDF using jsPDF)
      const pdfBtn = document.createElement('button');
      pdfBtn.className = 'btn';
      pdfBtn.id = 'chart-pdf';
      pdfBtn.textContent = 'PDF';
      pdfBtn.title = 'Download chart as PDF';

      // Clear button (clears table/cache and chart — same behavior as Clear Table)
      const chartClearBtn = document.createElement('button');
      chartClearBtn.className = 'btn secondary';
      chartClearBtn.id = 'chart-clear';
      chartClearBtn.textContent = 'Clear';
      chartClearBtn.title = 'Clear chart and query results';

      // Reset button (keeps interactive plugin controls)
      const resetBtn = document.createElement('button');
      resetBtn.className = 'btn secondary';
      resetBtn.id = 'chart-reset';
      resetBtn.textContent = 'Reset';

      controls.appendChild(pdfBtn);
      controls.appendChild(chartClearBtn);
      controls.appendChild(resetBtn);
      header.appendChild(controls);

      // PDF handler: create real PDF using jsPDF if available
      pdfBtn.addEventListener('click', async () => {
        if (!chartCanvas) {
          alert('No chart available');
          return;
        }
        try {
          // Use Graph Title input as filename and as PDF header
          const titleInputEl = document.getElementById('graph-title');
          const titleRaw = (titleInputEl && titleInputEl.value) ? String(titleInputEl.value).trim() : '';
          const safeName = titleRaw ? titleRaw.replace(/[\\\/:*?"<>|]+/g, '').slice(0, 100) : `ppm_chart_${Date.now()}`;

          const dataUrl = canvasToDataURLHighRes(chartCanvas, 2);
          // Prefer the UMD jsPDF global (window.jspdf.jsPDF) or window.jsPDF
          let doc;
          if (window.jspdf && window.jspdf.jsPDF) {
            doc = new window.jspdf.jsPDF({ orientation: 'landscape' });
          } else if (window.jsPDF) {
            doc = new window.jsPDF({ orientation: 'landscape' });
          } else {
            // fallback: open image in new tab for manual save
            const w = window.open('', '_blank');
            if (!w) { alert('Popup blocked — cannot open chart.'); return; }
            w.document.write('<html><body style="margin:0"><img src="' + dataUrl + '" style="width:100%"/></body></html>');
            w.document.close();
            return;
          }

          // load image to obtain natural size for aspect
          const imgProps = await new Promise((resolve) => {
            const img = new Image();
            img.onload = () => resolve({ width: img.width, height: img.height });
            img.onerror = () => resolve({ width: 1600, height: 800 });
            img.src = dataUrl;
          });

          const pdfW = doc.internal.pageSize.getWidth();
          const pdfH = doc.internal.pageSize.getHeight();
          const margin = 8;

          // If a title was provided, draw it centered at top in bold large font
          if (titleRaw) {
            try {
              doc.setFont('helvetica', 'bold');
            } catch (e) { /* ignore */ }
            doc.setFontSize(18);
            doc.setTextColor(20, 20, 20);
            // center text
            doc.text(titleRaw, pdfW / 2, margin + 8, { align: 'center' });
          }

          // compute space for image below title
          const titleOffset = titleRaw ? 18 : 0;
          const yStart = margin + titleOffset + 10;
          const maxW = pdfW - margin * 2;
          const maxH = pdfH - yStart - margin;
          let drawW = maxW;
          let drawH = (imgProps.height / imgProps.width) * drawW;
          if (drawH > maxH) { drawH = maxH; drawW = (imgProps.width / imgProps.height) * drawH; }
          const x = (pdfW - drawW) / 2;
          doc.addImage(dataUrl, 'PNG', x, yStart, drawW, drawH);
          doc.save(`${safeName}.pdf`);
        } catch (e) {
          console.error('PDF export error', e);
          alert('Failed to generate PDF. Trying fallback (open image).');
          try {
            const dataUrl = chartCanvas.toDataURL('image/png');
            const w = window.open('', '_blank');
            if (!w) { alert('Popup blocked — cannot open chart.'); return; }
            w.document.write('<html><body style="margin:0"><img src="' + dataUrl + '" style="width:100%"/></body></html>');
            w.document.close();
          } catch (e2) { /* ignore */ }
        }
      });

      // Clear handler: perform same actions as Clear Table button
      chartClearBtn.addEventListener('click', () => {
        try {
          // Clear caches and UI like the clear-results handler
          pageCache.clear();
          lastFetchedCountForPage.clear();
          lastRequestParamsHash = '';
          currentPage = 0;
          if (resultsEl) {
            resultsEl.innerHTML = '';
            if (resultsEmpty) resultsEl.appendChild(resultsEmpty);
          }
          hidePagingControls();
          updatePageLabel();
          setPagingButtonsState();
          try { window.rowtotal = 0; } catch (e) { /* ignore */ }
          totalScanInProgressForHash = null;

          // hide chart area and destroy chart instance
          if (chartArea) {
            chartArea.style.display = 'none';
            chartArea.setAttribute('aria-hidden', 'true');
          }
          if (ppmChart) {
            try { ppmChart.destroy(); } catch (e) { /* ignore */ }
            ppmChart = null;
          }
        } catch (e) {
          console.error('chartClear error', e);
        }
      });

      // Reset handler: preference to plugin resetZoom, otherwise restore full view
      resetBtn.addEventListener('click', () => {
        if (!ppmChart) return;
        try {
          if (typeof ppmChart.resetZoom === 'function') {
            ppmChart.resetZoom();
            setTimeout(() => { try { setFullView(ppmChart); } catch (e) {} }, 0);
            return;
          }
        } catch (e) { /* ignore */ }
        setFullView(ppmChart);
      });

      chartControlsInjected = true;
    } catch (e) {
      console.error('ensureChartControls error', e);
    }
  }

  // --- Graphing helpers ---

  // Fetch all rows across pages (up to a max point limit) for graphing
  async function fetchAllRowsForGraph(fromISO, toISO, maxPoints = 5000) {
    const all = [];
    let page = 0;
    while (true) {
      const rows = await fetchPageRows(page, fromISO, toISO);
      if (rows === null) return null; // error
      if (!rows || rows.length === 0) break;
      all.push(...rows);
      if (all.length >= maxPoints) break;
      if (rows.length < pageSize) break;
      page += 1;
    }
    return all.slice(0, maxPoints);
  }

  // Render PPM chart for given params
  async function renderPpmGraphForParams(params) {
    if (!params) return;
    if (!chartArea || !chartCanvas) return;

    // If chartjs-plugin-zoom is loaded, register it (it usually registers itself when loaded after Chart.js).
    // This is defensive: attempt to register common plugin globals if available.
    try {
      if (window.Chart && window.chartjsPluginZoom && typeof window.Chart.register === 'function') {
        // plugin may already register itself; calling register twice is harmless
        try { window.Chart.register(window.chartjsPluginZoom); } catch (e) { /* ignore */ }
      }
    } catch (e) { /* ignore */ }

    // ensure chart controls exist
    ensureChartControls();

    // show chart area
    chartArea.style.display = 'block';
    chartArea.setAttribute('aria-hidden', 'false');

    // fetch rows (paged)
    chartMeta.textContent = 'Fetching data…';
    const rows = await fetchAllRowsForGraph(params.fromISO, params.toISO, 10000); // safety limit
    if (rows === null) {
      chartMeta.textContent = 'Error fetching data';
      return;
    }
    if (rows.length === 0) {
      chartMeta.textContent = 'No rows for selected range';
      // create/clear chart
      if (ppmChart) {
        ppmChart.data.labels = [];
        ppmChart.data.datasets[0].data = [];
        ppmChart.update();
      }
      return;
    }

    // rows come in DESC order by recorded_at, make ascending
    rows.sort((a, b) => {
      const ta = parseToDate(a.recorded_at || a.recorded_at_raw || a.recorded_at_str) || new Date(0);
      const tb = parseToDate(b.recorded_at || b.recorded_at_raw || b.recorded_at_str) || new Date(0);
      return ta - tb;
    });

    // Build labels and numeric data
    const labels = rows.map(r => toLocalDisplay(r.recorded_at || r.recorded_at_raw || r.recorded_at_str || ''));
    const dataPts = rows.map(r => (r.los_ppm === null || r.los_ppm === undefined) ? null : Number(r.los_ppm));

    chartMeta.textContent = `${rows.length.toLocaleString()} points`;

    // Build Chart.js dataset and config
    try {
      if (!window.Chart) {
        chartMeta.textContent = 'Chart.js not available';
        return;
      }
      const ctx = chartCanvas.getContext('2d');

      const dataset = {
        label: 'PPM-M-LO',
        data: dataPts,
        borderColor: 'rgba(96,165,250,1)',
        backgroundColor: 'rgba(96,165,250,0.08)',
        spanGaps: true,
        pointRadius: 0.5,
        pointHoverRadius: 4,
        tension: 0.15,
        borderWidth: 1.5
      };

      // Tooltip callbacks use closure 'rows' and 'labels' above to produce stable title and label text
      const tooltipConfig = {
        callbacks: {
          title: function (tooltipItems) {
            // Return a single formatted timestamp line based on the underlying row data
            if (!tooltipItems || tooltipItems.length === 0) return '';
            const idx = tooltipItems[0].dataIndex;
            const r = rows[idx];
            if (!r) {
              // fallback to label array
              return (labels[idx] || '') ;
            }
            const ts = r.recorded_at || r.recorded_at_raw || r.recorded_at_str || '';
            return toLocalDisplay(ts) || (labels[idx] || '');
          },
          label: function (context) {
            // Prefer the original los_ppm value from the underlying row if available to avoid "[Object Object]" issues
            const idx = context.dataIndex;
            const r = rows && rows[idx] ? rows[idx] : null;
            let val = null;
            if (r && (r.los_ppm !== undefined && r.los_ppm !== null)) {
              val = r.los_ppm;
            } else {
              // fallback: context.parsed may be a number or an object {x,y}
              if (context.parsed !== undefined && context.parsed !== null) {
                if (typeof context.parsed === 'number') val = context.parsed;
                else if (typeof context.parsed === 'object' && context.parsed.y !== undefined) val = context.parsed.y;
                else val = context.parsed;
              } else {
                val = context.raw;
              }
            }
            if (val === null || val === undefined || val === '') return 'PPM-M-LO: n/a';
            return `PPM-M-LO: ${val}`;
          }
        }
      };

      // Zoom plugin options (chartjs-plugin-zoom) — we keep zoom (wheel/pinch) enabled but disable plugin pan
      const zoomPluginOptions = {
        // Enable wheel zoom and pinch zoom on x axis, and drag pan in x mode.
        zoom: {
          wheel: { enabled: true },
          pinch: { enabled: true },
          mode: 'x'
        },
        // disable built-in plugin pan because we implement a custom left-click drag pan (with inverted controls)
        pan: {
          enabled: false,
          mode: 'xy'
        }
      };

      if (ppmChart) {
        ppmChart.data.labels = labels;
        ppmChart.data.datasets[0] = dataset;
        if (ppmChart.options.plugins && ppmChart.options.plugins.tooltip) {
          ppmChart.options.plugins.tooltip.callbacks = tooltipConfig.callbacks;
        }
        // ensure y-axis begins at zero and has correct label
        if (!ppmChart.options.scales) ppmChart.options.scales = {};
        if (!ppmChart.options.scales.y) ppmChart.options.scales.y = {};
        ppmChart.options.scales.y.title = { display: true, text: 'PPM-M-LO' };
        ppmChart.options.scales.y.beginAtZero = true;
        // attach zoom options if plugin available
        try {
          ppmChart.options.plugins.zoom = zoomPluginOptions;
        } catch (e) { /* ignore */ }
        ppmChart.update();
        // reset view window to show full range on new data
        if (labels.length > 0) {
          setTimeout(() => {
            try {
              if (ppmChart) {
                // if plugin provides resetZoom, use it to clear any previous zoom
                if (typeof ppmChart.resetZoom === 'function') ppmChart.resetZoom();
                setFullView(ppmChart);
              }
            } catch (e) {}
          }, 0);
        }
      } else {
        // create new chart
        ppmChart = new Chart(ctx, {
          type: 'line',
          data: {
            labels,
            datasets: [dataset]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
              mode: 'index',
              intersect: false
            },
            plugins: {
              legend: { display: false },
              tooltip: tooltipConfig,
              // add zoom options (plugin will read these if registered)
              zoom: zoomPluginOptions
            },
            scales: {
              x: {
                display: true,
                title: { display: true, text: 'Time' },
                ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 20 }
              },
              y: {
                display: true,
                title: { display: true, text: 'PPM-M-LO' },
                beginAtZero: true
              }
            }
          }
        });

        // initialize view start/end fields
        ppmChart.__viewStart = 0;
        ppmChart.__viewEnd = (ppmChart.data.labels && ppmChart.data.labels.length) ? ppmChart.data.labels.length - 1 : 0;
        // ensure controls and full view
        ensureChartControls();
        setFullView(ppmChart);

        // attach custom left-click drag panning (inverted controls)
        attachCustomPanToCanvas(ppmChart, chartCanvas);
      }
    } catch (e) {
      console.error('Chart render error', e);
      chartMeta.textContent = 'Chart render error';
    }
  }

  // helper to set full view for a chart
  function setFullView(chart) {
    if (!chart) return;
    const N = (chart.data && chart.data.labels) ? chart.data.labels.length : 0;
    if (N === 0) return;
    if (!chart.options.scales) chart.options.scales = {};
    if (!chart.options.scales.x) chart.options.scales.x = {};
    chart.options.scales.x.min = 0;
    chart.options.scales.x.max = N - 1;
    chart.__viewStart = 0;
    chart.__viewEnd = N - 1;
    try { chart.update('none'); } catch (e) { try { chart.update(); } catch (e2) {} }
  }

  // CUSTOM PANNING: attach left-button drag handlers that pan the chart.
  // Controls are inverted per requirement: drag-left pans to the right, drag-up pans down.
  function attachCustomPanToCanvas(chart, canvas) {
    if (!chart || !canvas || __customPanAttached) return;
    __customPanAttached = true;

    let dragging = false;
    let startPixel = { x: 0, y: 0 };
    let startValue = { x: 0, y: 0 };
    let startRange = { xMin: 0, xMax: 0, yMin: 0, yMax: 0 };

    function preventSelection(e) {
      e.preventDefault && e.preventDefault();
      return false;
    }

    function onMouseDown(e) {
      // left button only
      if (e.button !== 0) return;
      const rect = canvas.getBoundingClientRect();
      const canvasX = (e.clientX - rect.left);
      const canvasY = (e.clientY - rect.top);

      const xScale = chart.scales.x;
      const yScale = chart.scales.y;
      if (!xScale || !yScale) return;

      dragging = true;
      canvas.style.cursor = 'grabbing';
      startPixel.x = canvasX;
      startPixel.y = canvasY;
      try {
        startValue.x = xScale.getValueForPixel(canvasX);
        startValue.y = yScale.getValueForPixel(canvasY);
      } catch (err) {
        // fallback: estimate using left/top area
        startValue.x = xScale.getValueForPixel(canvasX);
        startValue.y = yScale.getValueForPixel(canvasY);
      }
      startRange.xMin = (typeof xScale.min === 'number') ? xScale.min : chart.options.scales.x.min || 0;
      startRange.xMax = (typeof xScale.max === 'number') ? xScale.max : chart.options.scales.x.max || (chart.data.labels ? chart.data.labels.length - 1 : 0);
      startRange.yMin = (typeof yScale.min === 'number') ? yScale.min : chart.options.scales.y.min || 0;
      startRange.yMax = (typeof yScale.max === 'number') ? yScale.max : chart.options.scales.y.max || (chart.data.datasets && chart.data.datasets[0] && chart.data.datasets[0].data ? Math.max(...chart.data.datasets[0].data.filter(v => v !== null)) : 0);

      // prevent page selection while dragging
      document.addEventListener('selectstart', preventSelection);
      document.addEventListener('dragstart', preventSelection);
      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('mouseup', onMouseUp);
    }

    function onMouseMove(e) {
      if (!dragging) return;
      const rect = canvas.getBoundingClientRect();
      const canvasX = (e.clientX - rect.left);
      const canvasY = (e.clientY - rect.top);

      const xScale = chart.scales.x;
      const yScale = chart.scales.y;
      if (!xScale || !yScale) return;

      const curValX = xScale.getValueForPixel(canvasX);
      const curValY = yScale.getValueForPixel(canvasY);

      // value delta = cur - start. Inverted pan => shift by -delta
      const deltaX = (curValX - startValue.x);
      const deltaY = (curValY - startValue.y);

      const newXMin = startRange.xMin - deltaX;
      const newXMax = startRange.xMax - deltaX;
      const newYMin = startRange.yMin - deltaY;
      const newYMax = startRange.yMax - deltaY;

      // Apply new ranges
      try {
        chart.options.scales.x.min = newXMin;
        chart.options.scales.x.max = newXMax;
        chart.options.scales.y.min = newYMin;
        chart.options.scales.y.max = newYMax;
        // update quickly without animation
        chart.update('none');
      } catch (err) {
        // ignore errors (rare)
      }
    }

    function onMouseUp() {
      if (!dragging) return;
      dragging = false;
      canvas.style.cursor = 'default';
      document.removeEventListener('selectstart', preventSelection);
      document.removeEventListener('dragstart', preventSelection);
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    }

    // attach mousedown to canvas
    canvas.style.cursor = 'grab';
    canvas.addEventListener('mousedown', onMouseDown);
    // clean up on window unload
    window.addEventListener('beforeunload', () => {
      try { canvas.removeEventListener('mousedown', onMouseDown); } catch (e) {}
    });
  }

  // Expose for debugging in console if needed
  window.__queryClient = {
    loadPage: loadAndRenderPage,
    pageCache,
    pageSize,
    currentPageRef: () => currentPage
  };
})();
