// V4 — Copilot workbench rewrite
// query-client.js — full-featured query UI: presets, fetch, clear, CSV export.
// Pagination: pageSize 2000 with Prev/Next/First/Last. Controls shown only after results.
// Fixed issues that could stop scripts from running (robust null checks, no accidental runtime errors).
// Uses recorded_at_str if provided by server, otherwise falls back to recorded_at.
//
// NOTE: Adds a polished, read-only 24-hour preview next to the "From" and "To"
// datetime-local inputs. Styling is contained here (injected) and spacing is generous
// so the two formats are visually distinct and not confusing. No other page markup
// or behavior is changed.

(function () {
  const fetchBtn = document.getElementById('fetch');
  const fromInput = document.getElementById('from');
  const toInput = document.getElementById('to');
  const resultsEl = document.getElementById('results');
  const resultsEmpty = document.getElementById('results-empty');
  const presets = document.querySelectorAll('.preset');
  const clearResultsBtn = document.getElementById('clear-results');
  const exportCsvBtn = document.getElementById('export-csv');

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

  // Clear button
  if (clearResultsBtn) {
    clearResultsBtn.addEventListener('click', () => {
      pageCache.clear();
      lastFetchedCountForPage.clear();
      lastRequestParamsHash = '';
      currentPage = 0;
      if (resultsEl) {
        resultsEl.innerHTML = '';
        resultsEl.appendChild(resultsEmpty);
      }
      hidePagingControls();
      updatePageLabel();
      setPagingButtonsState();
      try { window.rowtotal = 0; } catch (e) { /* ignore */ }
      totalScanInProgressForHash = null;
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
      const headers = ['Recorded_At','Temp','Rx_Light','R2','HeartBeat','PPM'];
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
      resultsEl.appendChild(resultsEmpty);
      hidePagingControls();
      return;
    }

    const table = document.createElement('table');
    table.className = 'table';
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    ['Recorded_At','Temp','Rx_Light','R2','HeartBeat','PPM'].forEach(h => {
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

  // Expose for debugging in console if needed
  window.__queryClient = {
    loadPage: loadAndRenderPage,
    pageCache,
    pageSize,
    currentPageRef: () => currentPage
  };
})();
