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
// CHANGES: Export CSV button now shows a tiny dropdown with two choices:
//   - Current Page  (exports current page / cached pages per existing logic)
//   - All Pages     (fetches ALL pages from server and exports combined CSV)
// Both options use the Graph Title input (if provided) to name the downloaded file.
//
// PERFORMANCE: "All Pages" export fetches pages in parallel batches (configurable concurrency)
// to reduce the time-to-download. A visual progress indicator (spinner of dots + percent)
// is shown next to the CSV button and the actual CSV download starts once progress reaches 100%.

(function () {
  // DOM elements
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

  // --- Utilities for datetime formatting ---
  function pad2(v) { return String(v).padStart(2,'0'); }

  function formatDateTo24Short(d) {
    if (!(d instanceof Date) || isNaN(d.getTime())) return '';
    const day = pad2(d.getDate());
    const month = pad2(d.getMonth() + 1);
    const year = d.getFullYear();
    const hours = pad2(d.getHours());
    const minutes = pad2(d.getMinutes());
    return `${day}/${month}/${year} ${hours}:${minutes}`;
  }

  function isoLocalString(d) {
    const pad = (n) => String(n).padStart(2,'0');
    const yyyy = d.getFullYear();
    const mm = pad(d.getMonth()+1);
    const dd = pad(d.getDate());
    const hh = pad(d.getHours());
    const min = pad(d.getMinutes());
    return `${yyyy}-${mm}-${dd}T${hh}:${min}`;
  }

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

  // --- Minimal removed preview logic kept as noop (per prior requests) ---
  (function injectPreviewStyles() { /* noop - previews intentionally removed */ })();
  function create24Preview(inputEl) { return () => {}; }
  const updateFromPreview = create24Preview(fromInput);
  const updateToPreview = create24Preview(toInput);
  function safeUpdatePreviews() { try { updateFromPreview(); } catch (e) {} try { updateToPreview(); } catch (e) {} }

  if (fromInput) {
    fromInput.addEventListener('input', () => { try { updateFromPreview(); } catch(e){} });
    fromInput.addEventListener('change', () => { try { updateFromPreview(); } catch(e){} });
  }
  if (toInput) {
    toInput.addEventListener('input', () => { try { updateToPreview(); } catch(e){} });
    toInput.addEventListener('change', () => { try { updateToPreview(); } catch(e){} });
  }

  // --- Pagination and query state ---
  const pageSize = 2000;
  let currentPage = 0;
  const pageCache = new Map(); // pageIndex -> rows[]
  let lastFetchedCountForPage = new Map();
  let lastRequestParamsHash = '';

  let serverTotalRows = null;
  let serverTotalPages = null;

  let totalScanInProgressForHash = null;

  // --- UI injection: paging controls ---
  let pagingControlsEl = null;
  (function injectPagingControlsUI() {
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

      // Page jump input (1-based)
      const pageJumpInput = document.createElement('input');
      pageJumpInput.type = 'number';
      pageJumpInput.id = 'page-jump';
      pageJumpInput.min = '1';
      pageJumpInput.style.width = '72px';
      pageJumpInput.style.padding = '6px';
      pageJumpInput.style.borderRadius = '6px';
      pageJumpInput.style.border = '1px solid rgba(255,255,255,0.06)';
      pageJumpInput.style.background = 'transparent';
      pageJumpInput.style.color = 'inherit';
      pageJumpInput.title = 'Type page number and press Enter or click Go';
      pageJumpInput.placeholder = 'Go to page';

      const pageGoBtn = document.createElement('button');
      pageGoBtn.className = 'btn';
      pageGoBtn.id = 'page-go';
      pageGoBtn.textContent = 'Go';
      pageGoBtn.style.padding = '6px 8px';
      pageGoBtn.title = 'Jump to page';

      pagingControlsEl.appendChild(firstBtn);
      pagingControlsEl.appendChild(prevBtn);
      pagingControlsEl.appendChild(pageLabel);

      // Insert jump controls after the label for convenience
      pagingControlsEl.appendChild(pageJumpInput);
      pagingControlsEl.appendChild(pageGoBtn);

      pagingControlsEl.appendChild(nextBtn);
      pagingControlsEl.appendChild(lastBtn);

      const spacer = document.createElement('div');
      spacer.style.flex = '1';
      pagingControlsEl.appendChild(spacer);

      // We'll append CSV and Clear buttons to the paging controls (CSV will be wrapped later)
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

        if (typeof serverTotalRows === 'number' && serverTotalRows >= 0) {
          const lastPageIndex = Math.max(0, Math.ceil(serverTotalRows / pageSize) - 1);
          currentPage = lastPageIndex;
          loadAndRenderPage(currentPage);
          return;
        }

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

      // Jump handlers
      function parseAndJump() {
        try {
          const v = parseInt(pageJumpInput.value, 10);
          if (Number.isNaN(v)) return;
          let targetPage = Math.max(0, v - 1);
          if (typeof serverTotalPages === 'number' && serverTotalPages > 0) {
            targetPage = Math.min(targetPage, Math.max(0, serverTotalPages - 1));
          }
          currentPage = targetPage;
          loadAndRenderPage(currentPage);
        } catch (e) {}
      }

      pageJumpInput.addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter') {
          ev.preventDefault();
          parseAndJump();
        }
      });
      pageGoBtn.addEventListener('click', () => { parseAndJump(); });

      hidePagingControls();
    } catch (e) {
      console.error('injectPagingControls error', e);
    }
  })();

  // --- Presets ---
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

  // --- Fetch / Graph / Clear handlers ---
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

  if (graphBtn) {
    graphBtn.addEventListener('click', async () => {
      const params = getParamsForCurrentInputs();
      if (!params) return;
      try {
        graphBtn.disabled = true;
        chartMeta.textContent = 'Loading…';
        await renderPpmGraphForParams(params);
      } finally {
        graphBtn.disabled = false;
      }
    });
  }

  if (clearResultsBtn) {
    clearResultsBtn.addEventListener('click', () => {
      pageCache.clear();
      lastFetchedCountForPage.clear();
      lastRequestParamsHash = '';
      currentPage = 0;
      serverTotalRows = null;
      serverTotalPages = null;
      if (resultsEl) {
        resultsEl.innerHTML = '';
        if (resultsEmpty) resultsEl.appendChild(resultsEmpty);
      }
      hidePagingControls();
      updatePageLabel();
      setPagingButtonsState();
      try { window.rowtotal = 0; } catch (e) {}
      totalScanInProgressForHash = null;

      if (chartArea) {
        chartArea.style.display = 'none';
        chartArea.setAttribute('aria-hidden', 'true');
      }
      if (ppmChart) {
        try { ppmChart.destroy(); } catch (e) {}
        ppmChart = null;
      }
    });
  }

  // --- CSV: split button with menu that opens upwards and progress next to CSV button ---
  (function enhanceCsvButton() {
    if (!exportCsvBtn) return;

    // wrapper for CSV button + menu + progress
    const wrapper = document.createElement('div');
    wrapper.style.position = 'relative';
    wrapper.style.display = 'inline-flex';
    wrapper.style.alignItems = 'center';
    wrapper.style.gap = '6px';

    const parent = exportCsvBtn.parentNode;
    if (!parent) return;
    parent.replaceChild(wrapper, exportCsvBtn);

    exportCsvBtn.style.margin = '0';
    exportCsvBtn.style.display = 'inline-flex';
    exportCsvBtn.style.alignItems = 'center';
    wrapper.appendChild(exportCsvBtn);

    // menu that opens upward (we removed the extra caret button — the CSV button toggles the menu)
    const menu = document.createElement('div');
    menu.style.position = 'absolute';
    menu.style.right = '0';
    menu.style.bottom = 'calc(100% + 6px)';
    menu.style.background = 'var(--card)';
    menu.style.border = '1px solid rgba(255,255,255,0.04)';
    menu.style.borderRadius = '6px';
    menu.style.padding = '6px';
    menu.style.boxShadow = '0 6px 16px rgba(0,0,0,0.5)';
    menu.style.zIndex = '1200';
    menu.style.display = 'none';
    menu.style.minWidth = '160px';
    wrapper.appendChild(menu);

    const optCurrent = document.createElement('div');
    optCurrent.textContent = 'Current Page';
    optCurrent.style.padding = '8px';
    optCurrent.style.cursor = 'pointer';
    optCurrent.style.borderRadius = '4px';
    optCurrent.addEventListener('mouseenter', () => optCurrent.style.background = 'rgba(255,255,255,0.02)');
    optCurrent.addEventListener('mouseleave', () => optCurrent.style.background = 'transparent');

    const optAll = document.createElement('div');
    optAll.textContent = 'All Pages';
    optAll.style.padding = '8px';
    optAll.style.cursor = 'pointer';
    optAll.style.borderRadius = '4px';
    optAll.addEventListener('mouseenter', () => optAll.style.background = 'rgba(255,255,255,0.02)');
    optAll.addEventListener('mouseleave', () => optAll.style.background = 'transparent');

    menu.appendChild(optCurrent);
    menu.appendChild(optAll);

    function closeMenu() {
      menu.style.display = 'none';
      document.removeEventListener('click', onDocClick);
    }
    function openMenu() {
      menu.style.display = 'block';
      // attach doc click listener on next tick to avoid immediate close
      setTimeout(() => { document.addEventListener('click', onDocClick); }, 0);
    }
    function onDocClick(e) { if (!wrapper.contains(e.target)) closeMenu(); }

    // clicking the CSV button toggles menu
    exportCsvBtn.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); if (menu.style.display === 'none') openMenu(); else closeMenu(); });

    // progress UI next to CSV button (in wrapper)
    (function injectSpinnerStyles() {
      if (document.getElementById('__csv_spinner_styles__')) return;
      const s = document.createElement('style');
      s.id = '__csv_spinner_styles__';
      s.textContent = `
        .__csv_progress { display:inline-flex; align-items:center; gap:8px; color:var(--muted); font-weight:700; font-size:13px; margin-left:6px; }
        .__dots_spinner { width:20px; height:20px; position:relative; display:inline-block; }
        .__dots_spinner .dot { width:4px; height:4px; background:currentColor; border-radius:50%; position:absolute; left:50%; top:50%; margin:-2px; transform-origin: 0 -7px; }
        .__dots_spinner { animation: __csv_spin 1s linear infinite; }
        @keyframes __csv_spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `;
      document.head && document.head.appendChild(s);
    })();

    function createProgressUI() {
      const container = document.createElement('div');
      container.className = '__csv_progress';
      const spinner = document.createElement('div');
      spinner.className = '__dots_spinner';
      for (let i = 0; i < 8; i++) {
        const span = document.createElement('span');
        span.className = 'dot';
        span.style.transform = `rotate(${(i * 360) / 8}deg) translate(0, -7px)`;
        spinner.appendChild(span);
      }
      const pct = document.createElement('div');
      pct.className = '__csv_progress_pct';
      pct.textContent = '0%';
      container.appendChild(spinner);
      container.appendChild(pct);
      return { container, setPct: (n) => { pct.textContent = `${Math.max(0, Math.min(100, Math.round(n)))}%`; }, remove: () => { try { container.remove(); } catch (e) {} } };
    }

    function sanitizeFilename(name) { if (!name) return ''; return name.replace(/[\\\/:*?"<>|]+/g, '').trim().slice(0, 200); }
    function buildCsvFilename(defaultBase) {
      const titleInputEl = document.getElementById('graph-title');
      const titleRaw = (titleInputEl && titleInputEl.value) ? String(titleInputEl.value).trim() : '';
      const safe = sanitizeFilename(titleRaw);
      if (safe) return `${safe}.csv`;
      return `${defaultBase}.csv`;
    }

    function exportRowsToCsv(rows, filename) {
      if (!rows || rows.length === 0) {
        alert('No data to export');
        return;
      }
      const headers = ['Recorded_At','Temp','Rx_Light','R2','HeartBeat','PPM'];
      const lines = [headers.join(',')];
      rows.forEach(r => {
        // Use frontend helper to format recorded time into local timezone (simple, readable)
        const recordedLocal = toLocalDisplay(r.recorded_at_str || r.recorded_at || '');
        const timeField = `"${String(recordedLocal || '').replace(/"/g, '""')}"`;
        const vals = [
          timeField,
          r.los_temp ?? '',
          r.los_rx_light ?? '',
          r.los_r2 ?? '',
          r.los_heartbeat ?? '',
          r.los_ppm ?? ''
        ];
        lines.push(vals.join(','));
      });
      const csv = lines.join('\n');
      const blob = new Blob([csv], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    }

    // Current Page export (unchanged behavior)
    async function handleExportCurrent() {
      try {
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
        if (!rowsToExport || rowsToExport.length === 0) { alert('No data to export'); return; }
        const filename = buildCsvFilename(`los_query_${Date.now()}`);
        exportRowsToCsv(rowsToExport, filename);
      } finally {
        closeMenu();
      }
    }

    // All Pages export (parallelized, progress shown next to CSV button in wrapper)
    async function handleExportAll() {
      let progressUI = null;
      function showProgressUI() {
        try {
          if (wrapper.querySelector('.__csv_progress')) {
            progressUI = { container: wrapper.querySelector('.__csv_progress'), setPct: (n) => { wrapper.querySelector('.__csv_progress_pct').textContent = `${Math.max(0, Math.min(100, Math.round(n)))}%`; }, remove: () => {} };
          } else {
            const ui = createProgressUI();
            wrapper.appendChild(ui.container);
            progressUI = ui;
          }
        } catch (e) { console.warn('showProgressUI failed', e); }
      }
      function hideProgressUI() { try { if (progressUI && typeof progressUI.remove === 'function') progressUI.remove(); progressUI = null; } catch (e) {} }

      try {
        const params = getParamsForCurrentInputs();
        if (!params) return;

        // Try to read server total
        let totalRows = (typeof serverTotalRows === 'number' && serverTotalRows >= 0) ? serverTotalRows : null;
        if (totalRows === null) {
          const p0 = await fetchPageRows(0, params.fromISO, params.toISO);
          if (p0 === null) { alert('Failed to fetch data from server for export'); return; }
          if (typeof serverTotalRows === 'number') totalRows = serverTotalRows;
        }

        const rowsAccum = [];
        if (totalRows !== null) {
          const pagesCount = Math.max(0, Math.ceil(totalRows / pageSize));
          if (pagesCount === 0) { alert('No data to export'); return; }

          const concurrency = 8; // adjust if server overloaded
          let nextPageIndex = 0;
          let collected = 0;
          const pageResults = new Array(pagesCount);

          showProgressUI();
          if (progressUI) progressUI.setPct(0);

          async function worker() {
            while (true) {
              const p = nextPageIndex++;
              if (p >= pagesCount) break;
              try {
                let rows;
                if (pageCache.has(p)) rows = pageCache.get(p) || [];
                else rows = await fetchPageRows(p, params.fromISO, params.toISO);
                if (rows === null) throw new Error(`Failed fetching page ${p}`);
                pageResults[p] = rows;
                collected += rows.length;
                if (progressUI) { const pct = Math.round((collected / totalRows) * 100); progressUI.setPct(Math.min(100, pct)); }
              } catch (err) { throw err; }
            }
          }

          const workers = [];
          const wc = Math.min(concurrency, pagesCount);
          for (let i = 0; i < wc; i++) workers.push(worker());

          try {
            await Promise.all(workers);
          } catch (err) {
            console.error('Parallel export error', err);
            alert('Error fetching data from server during export');
            hideProgressUI();
            return;
          }

          if (progressUI) progressUI.setPct(100);
          await new Promise(r => setTimeout(r, 240));

          for (let p = 0; p < pagesCount; p++) {
            const pr = pageResults[p] || [];
            rowsAccum.push(...pr);
          }

        } else {
          // Unknown total: sequential with soft cap and progress approximation
          showProgressUI();
          let collected = 0;
          const SOFT_CAP_PAGES = 2000;
          const MAX_PAGES_TO_FETCH = 10000;
          for (let p = 0; p < MAX_PAGES_TO_FETCH && p < SOFT_CAP_PAGES; p++) {
            if (pageCache.has(p)) {
              const cached = pageCache.get(p) || [];
              if (cached.length === 0) break;
              rowsAccum.push(...cached);
              collected += cached.length;
              if (progressUI) { const approx = Math.min(99, Math.round(((p + 1) / SOFT_CAP_PAGES) * 100)); progressUI.setPct(approx); }
              if (cached.length < pageSize) break;
              continue;
            }
            const rows = await fetchPageRows(p, params.fromISO, params.toISO);
            if (rows === null) { alert('Error fetching data from server during export'); hideProgressUI(); return; }
            if (!rows || rows.length === 0) break;
            rowsAccum.push(...rows);
            collected += rows.length;
            if (progressUI) { const approx = Math.min(99, Math.round(((p + 1) / SOFT_CAP_PAGES) * 100)); progressUI.setPct(approx); }
            if (rows.length < pageSize) break;
          }
          if (progressUI) progressUI.setPct(100);
          await new Promise(r => setTimeout(r, 240));
        }

        if (!rowsAccum || rowsAccum.length === 0) {
          alert('No data to export');
          hideProgressUI();
          return;
        }

        const filename = buildCsvFilename(`los_query_full_${Date.now()}`);
        exportRowsToCsv(rowsAccum, filename);

      } finally {
        setTimeout(() => { try { if (chartMeta) chartMeta.textContent = '—'; } catch (e) {} }, 1500);
        closeMenu();
        hideProgressUI();
      }
    }

    optCurrent.addEventListener('click', (e) => { e.stopPropagation(); handleExportCurrent(); });
    optAll.addEventListener('click', (e) => { e.stopPropagation(); handleExportAll(); });

  })(); // end enhanceCsvButton

  // --- Page loading / rendering functions ---
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

  async function fetchPageRows(pageIndex, fromISO, toISO) {
    try {
      const params = new URLSearchParams();
      if (fromISO) params.set('from', fromISO);
      if (toISO) params.set('to', toISO);
      params.set('limit', String(pageSize));
      params.set('offset', String(pageIndex * pageSize));

      try {
        const stationSelect = document.getElementById('station-select');
        const selSerial = stationSelect && stationSelect.selectedOptions && stationSelect.selectedOptions[0]
          ? stationSelect.selectedOptions[0].dataset.serial
          : null;
        if (selSerial) params.set('serial_number', selSerial);
      } catch (e) {}

      const r = await fetch('/api/los?' + params.toString());
      if (!r.ok) throw new Error('Server returned ' + r.status);
      const data = await r.json();
      if (!data.ok) throw new Error('Query failed');

      if (typeof data.total === 'number') {
        serverTotalRows = Number(data.total);
        serverTotalPages = Math.max(0, Math.ceil(serverTotalRows / pageSize));
        try { window.rowtotal = serverTotalRows; } catch (e) {}
        pokeResultsForRowTotal();
      }

      const rows = data.rows || [];

      if (pageIndex > 0 && pageCache.has(pageIndex - 1)) {
        const prev = pageCache.get(pageIndex - 1) || [];
        if (rows.length === prev.length && rows.length > 0) {
          const same = (rows[0].id === prev[0].id && rows[rows.length-1].id === prev[prev.length-1].id);
          if (same) { lastFetchedCountForPage.set(pageIndex, 0); return []; }
        }
      }
      return rows;
    } catch (err) {
      console.error('Query error', err);
      return null;
    }
  }

  function getParamsForCurrentInputs() {
    const from = (fromInput && fromInput.value) ? new Date(fromInput.value).toISOString() : undefined;
    const to = (toInput && toInput.value) ? new Date(toInput.value).toISOString() : undefined;

    if (from && to && new Date(from) > new Date(to)) { alert('From must be before To'); return null; }
    const hash = `f=${from||''}&t=${to||''}&ps=${pageSize}`;
    return { fromISO: from, toISO: to, hash };
  }

  function clearCacheForNewQuery(newHash) {
    pageCache.clear();
    lastFetchedCountForPage.clear();
    lastRequestParamsHash = newHash || '';
    currentPage = 0;
    serverTotalRows = null;
    serverTotalPages = null;
    hidePagingControls();
    updatePageLabel();
    setPagingButtonsState();
    try { window.rowtotal = 0; } catch (e) {}
    totalScanInProgressForHash = null;
  }

  function updatePageLabel() {
    const label = document.getElementById('page-label');
    if (!label) return;
    const fetchedCount = lastFetchedCountForPage.get(currentPage);
    const isLastKnown = typeof fetchedCount === 'number' && fetchedCount < pageSize;
    if (typeof serverTotalRows === 'number' && serverTotalRows >= 0) {
      const totalPages = serverTotalPages || 0;
      label.textContent = `Page ${currentPage + 1} of ${totalPages || 1}`;
    } else {
      label.textContent = `Page ${currentPage + 1}${isLastKnown ? ' (end)' : ''}`;
    }

    // Keep jump input in sync
    const jump = document.getElementById('page-jump');
    if (jump) {
      try { jump.value = String(currentPage + 1); } catch (e) {}
    }
  }

  function setPagingButtonsState() {
    const prevBtn = document.getElementById('page-prev');
    const nextBtn = document.getElementById('page-next');
    const firstBtn = document.getElementById('page-first');
    const lastBtn = document.getElementById('page-last');
    if (!prevBtn || !nextBtn) return;
    prevBtn.disabled = currentPage <= 0;
    if (firstBtn) firstBtn.disabled = currentPage <= 0;

    if (typeof serverTotalPages === 'number' && serverTotalPages >= 0) {
      const lastIndex = Math.max(0, serverTotalPages - 1);
      nextBtn.disabled = currentPage >= lastIndex;
      if (lastBtn) lastBtn.disabled = currentPage >= lastIndex;
    } else {
      const curCount = lastFetchedCountForPage.get(currentPage);
      if (typeof curCount === 'number' && curCount < pageSize) {
        nextBtn.disabled = true;
        if (lastBtn) lastBtn.disabled = true;
      } else {
        nextBtn.disabled = false;
        if (lastBtn) lastBtn.disabled = false;
      }
    }
  }

  function hidePagingControls() { if (!pagingControlsEl) return; try { pagingControlsEl.style.display = 'none'; } catch (e) {} }
  function showPagingControls() { if (!pagingControlsEl) return; try { pagingControlsEl.style.display = 'flex'; } catch (e) {} }

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

    if (typeof serverTotalRows === 'number' && serverTotalRows >= 0) {
      try { window.rowtotal = Number(serverTotalRows) || 0; } catch (e) {}
      pokeResultsForRowTotal();
      totalScanInProgressForHash = null;
      return;
    }

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
      try { window.rowtotal = 0; } catch (e2) {}
    }

    (async () => {
      try {
        const total = await computeTotalRowsForQuery(params);
        try { window.rowtotal = Number(total) || 0; } catch (e) {}
        pokeResultsForRowTotal();
      } catch (e) {
        console.error('computeTotalRowsForQuery error', e);
        try { window.rowtotal = 0; } catch (e2) {}
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
      setTimeout(() => { try { if (marker.parentNode) marker.parentNode.removeChild(marker); } catch (e) {} }, 0);
    } catch (e) {}
  }

  // --- Chart controls, graphing, PDF export, pan helpers (full implementation) ---

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

      const pdfBtn = document.createElement('button');
      pdfBtn.className = 'btn';
      pdfBtn.id = 'chart-pdf';
      pdfBtn.textContent = 'PDF';
      pdfBtn.title = 'Download chart as PDF';

      const chartClearBtn = document.createElement('button');
      chartClearBtn.className = 'btn secondary';
      chartClearBtn.id = 'chart-clear';
      chartClearBtn.textContent = 'Clear';
      chartClearBtn.title = 'Clear chart and query results';

      const resetBtn = document.createElement('button');
      resetBtn.className = 'btn secondary';
      resetBtn.id = 'chart-reset';
      resetBtn.textContent = 'Reset';

      controls.appendChild(pdfBtn);
      controls.appendChild(chartClearBtn);
      controls.appendChild(resetBtn);
      header.appendChild(controls);

      // PDF handler: uses jsPDF if available, fallback to open image
      pdfBtn.addEventListener('click', async () => {
        if (!chartCanvas) { alert('No chart available'); return; }
        try {
          const titleInputEl = document.getElementById('graph-title');
          const titleRaw = (titleInputEl && titleInputEl.value) ? String(titleInputEl.value).trim() : '';
          const safeName = titleRaw ? titleRaw.replace(/[\\\/:*?"<>|]+/g, '').slice(0, 100) : `ppm_chart_${Date.now()}`;
          const dataUrl = canvasToDataURLHighRes(chartCanvas, 2);
          let doc;
          if (window.jspdf && window.jspdf.jsPDF) {
            doc = new window.jspdf.jsPDF({ orientation: 'landscape' });
          } else if (window.jsPDF) {
            doc = new window.jsPDF({ orientation: 'landscape' });
          } else {
            const w = window.open('', '_blank');
            if (!w) { alert('Popup blocked — cannot open chart.'); return; }
            w.document.write('<html><body style="margin:0"><img src="' + dataUrl + '" style="width:100%"/></body></html>');
            w.document.close();
            return;
          }

          const imgProps = await new Promise((resolve) => {
            const img = new Image();
            img.onload = () => resolve({ width: img.width, height: img.height });
            img.onerror = () => resolve({ width: 1600, height: 800 });
            img.src = dataUrl;
          });

          const pdfW = doc.internal.pageSize.getWidth();
          const pdfH = doc.internal.pageSize.getHeight();
          const margin = 8;

          if (titleRaw) {
            try { doc.setFont('helvetica', 'bold'); } catch (e) {}
            doc.setFontSize(18);
            doc.setTextColor(20, 20, 20);
            doc.text(titleRaw, pdfW / 2, margin + 8, { align: 'center' });
          }

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
          } catch (e2) {}
        }
      });

      // Clear handler: same as clearResultsBtn
      chartClearBtn.addEventListener('click', () => {
        try {
          pageCache.clear();
          lastFetchedCountForPage.clear();
          lastRequestParamsHash = '';
          currentPage = 0;
          serverTotalRows = null;
          serverTotalPages = null;
          if (resultsEl) { resultsEl.innerHTML = ''; if (resultsEmpty) resultsEl.appendChild(resultsEmpty); }
          hidePagingControls();
          updatePageLabel();
          setPagingButtonsState();
          try { window.rowtotal = 0; } catch (e) {}
          totalScanInProgressForHash = null;
          if (chartArea) { chartArea.style.display = 'none'; chartArea.setAttribute('aria-hidden', 'true'); }
          if (ppmChart) { try { ppmChart.destroy(); } catch (e) {} ppmChart = null; }
        } catch (e) { console.error('chartClear error', e); }
      });

      resetBtn.addEventListener('click', () => {
        if (!ppmChart) return;
        try {
          if (typeof ppmChart.resetZoom === 'function') {
            ppmChart.resetZoom();
            setTimeout(() => { try { setFullView(ppmChart); } catch (e) {} }, 0);
            return;
          }
        } catch (e) {}
        setFullView(ppmChart);
      });

      chartControlsInjected = true;
    } catch (e) {
      console.error('ensureChartControls error', e);
    }
  }

  // Graphing helpers and chart creation
  async function fetchAllRowsForGraph(fromISO, toISO, maxPoints = 5000) {
    const all = [];
    let page = 0;
    while (true) {
      const rows = await fetchPageRows(page, fromISO, toISO);
      if (rows === null) return null;
      if (!rows || rows.length === 0) break;
      all.push(...rows);
      if (all.length >= maxPoints) break;
      if (rows.length < pageSize) break;
      page += 1;
    }
    return all.slice(0, maxPoints);
  }

  async function renderPpmGraphForParams(params) {
    if (!params) return;
    if (!chartArea || !chartCanvas) return;

    try {
      if (window.Chart && window.chartjsPluginZoom && typeof window.Chart.register === 'function') {
        try { window.Chart.register(window.chartjsPluginZoom); } catch (e) {}
      }
    } catch (e) {}

    ensureChartControls();

    chartArea.style.display = 'block';
    chartArea.setAttribute('aria-hidden', 'false');

    chartMeta.textContent = 'Fetching data…';
    const rows = await fetchAllRowsForGraph(params.fromISO, params.toISO, 10000);
    if (rows === null) { chartMeta.textContent = 'Error fetching data'; return; }
    if (rows.length === 0) { chartMeta.textContent = 'No rows for selected range'; if (ppmChart) { ppmChart.data.labels = []; ppmChart.data.datasets[0].data = []; ppmChart.update(); } return; }

    rows.sort((a, b) => {
      const ta = parseToDate(a.recorded_at || a.recorded_at_raw || a.recorded_at_str) || new Date(0);
      const tb = parseToDate(b.recorded_at || b.recorded_at_raw || b.recorded_at_str) || new Date(0);
      return ta - tb;
    });

    const labels = rows.map(r => toLocalDisplay(r.recorded_at || r.recorded_at_raw || r.recorded_at_str || ''));
    const dataPts = rows.map(r => (r.los_ppm === null || r.los_ppm === undefined) ? null : Number(r.los_ppm));

    chartMeta.textContent = `${rows.length.toLocaleString()} points`;

    try {
      if (!window.Chart) { chartMeta.textContent = 'Chart.js not available'; return; }
      const ctx = chartCanvas.getContext('2d');

      const dataset = {
        label: 'PPM',
        data: dataPts,
        borderColor: 'rgba(96,165,250,1)',
        backgroundColor: 'rgba(96,165,250,0.08)',
        spanGaps: true,
        pointRadius: 0.5,
        pointHoverRadius: 4,
        tension: 0.15,
        borderWidth: 1.5
      };

      const tooltipConfig = {
        callbacks: {
          title: function (tooltipItems) {
            if (!tooltipItems || tooltipItems.length === 0) return '';
            const idx = tooltipItems[0].dataIndex;
            const r = rows[idx];
            if (!r) return (labels[idx] || '');
            const ts = r.recorded_at || r.recorded_at_raw || r.recorded_at_str || '';
            return toLocalDisplay(ts) || (labels[idx] || '');
          },
          label: function (context) {
            const idx = context.dataIndex;
            const r = rows && rows[idx] ? rows[idx] : null;
            let val = null;
            if (r && (r.los_ppm !== undefined && r.los_ppm !== null)) {
              val = r.los_ppm;
            } else {
              if (context.parsed !== undefined && context.parsed !== null) {
                if (typeof context.parsed === 'number') val = context.parsed;
                else if (typeof context.parsed === 'object' && context.parsed.y !== undefined) val = context.parsed.y;
                else val = context.parsed;
              } else {
                val = context.raw;
              }
            }
            if (val === null || val === undefined || val === '') return 'PPM: n/a';
            return `PPM: ${val}`;
          }
        }
      };

      const zoomPluginOptions = {
        zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
        pan: { enabled: false, mode: 'xy' }
      };

      if (ppmChart) {
        ppmChart.data.labels = labels;
        ppmChart.data.datasets[0] = dataset;
        if (ppmChart.options.plugins && ppmChart.options.plugins.tooltip) {
          ppmChart.options.plugins.tooltip.callbacks = tooltipConfig.callbacks;
        }
        if (!ppmChart.options.scales) ppmChart.options.scales = {};
        if (!ppmChart.options.scales.y) ppmChart.options.scales.y = {};
        ppmChart.options.scales.y.title = { display: true, text: 'PPM' };
        ppmChart.options.scales.y.beginAtZero = true;
        try { ppmChart.options.plugins.zoom = zoomPluginOptions; } catch (e) {}
        ppmChart.update();
        if (labels.length > 0) { setTimeout(() => { try { if (ppmChart && typeof ppmChart.resetZoom === 'function') ppmChart.resetZoom(); setFullView(ppmChart); } catch (e) {} }, 0); }
      } else {
        ppmChart = new Chart(ctx, {
          type: 'line',
          data: { labels, datasets: [dataset] },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { display: false }, tooltip: tooltipConfig, zoom: zoomPluginOptions },
            scales: {
              x: { display: true, title: { display: true, text: 'Time' }, ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 20 } },
              y: { display: true, title: { display: true, text: 'PPM' }, beginAtZero: true }
            }
          }
        });

        ppmChart.__viewStart = 0;
        ppmChart.__viewEnd = (ppmChart.data.labels && ppmChart.data.labels.length) ? ppmChart.data.labels.length - 1 : 0;
        ensureChartControls();
        setFullView(ppmChart);
        attachCustomPanToCanvas(ppmChart, chartCanvas);
      }
    } catch (e) {
      console.error('Chart render error', e);
      chartMeta.textContent = 'Chart render error';
    }
  }

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

  function attachCustomPanToCanvas(chart, canvas) {
    if (!chart || !canvas || __customPanAttached) return;
    __customPanAttached = true;

    let dragging = false;
    let startPixel = { x: 0, y: 0 };
    let startValue = { x: 0, y: 0 };
    let startRange = { xMin: 0, xMax: 0, yMin: 0, yMax: 0 };

    function preventSelection(e) { e.preventDefault && e.preventDefault(); return false; }

    function onMouseDown(e) {
      if (e.button !== 0) return;
      const rect = canvas.getBoundingClientRect();
      const canvasX = (e.clientX - rect.left);
      const canvasY = (e.clientY - rect.top);

      const xScale = chart.scales.x;
      const yScale = chart.scales.y;
      if (!xScale || !yScale) return;

      dragging = true;
      canvas.style.cursor = 'grabbing';
      startPixel.x = canvasX; startPixel.y = canvasY;
      try {
        startValue.x = xScale.getValueForPixel(canvasX);
        startValue.y = yScale.getValueForPixel(canvasY);
      } catch (err) {
        startValue.x = xScale.getValueForPixel(canvasX);
        startValue.y = yScale.getValueForPixel(canvasY);
      }
      startRange.xMin = (typeof xScale.min === 'number') ? xScale.min : chart.options.scales.x.min || 0;
      startRange.xMax = (typeof xScale.max === 'number') ? xScale.max : chart.options.scales.x.max || (chart.data.labels ? chart.data.labels.length - 1 : 0);
      startRange.yMin = (typeof yScale.min === 'number') ? yScale.min : chart.options.scales.y.min || 0;
      startRange.yMax = (typeof yScale.max === 'number') ? yScale.max : chart.options.scales.y.max || (chart.data.datasets && chart.data.datasets[0] && chart.data.datasets[0].data ? Math.max(...chart.data.datasets[0].data.filter(v => v !== null)) : 0);

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

      const deltaX = (curValX - startValue.x);
      const deltaY = (curValY - startValue.y);

      const newXMin = startRange.xMin - deltaX;
      const newXMax = startRange.xMax - deltaX;
      const newYMin = startRange.yMin - deltaY;
      const newYMax = startRange.yMax - deltaY;

      try {
        chart.options.scales.x.min = newXMin;
        chart.options.scales.x.max = newXMax;
        chart.options.scales.y.min = newYMin;
        chart.options.scales.y.max = newYMax;
        chart.update('none');
      } catch (err) {}
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

    canvas.style.cursor = 'grab';
    canvas.addEventListener('mousedown', onMouseDown);
    window.addEventListener('beforeunload', () => { try { canvas.removeEventListener('mousedown', onMouseDown); } catch (e) {} });
  }

  // Expose for debugging in console if needed
  window.__queryClient = {
    loadPage: loadAndRenderPage,
    pageCache,
    pageSize,
    currentPageRef: () => currentPage
  };

  // Initialize default query (last hour)
  (function init() {
    const now = new Date();
    const from = new Date(now.getTime() - 3600 * 1000);
    if (fromInput) fromInput.value = isoLocalString(from);
    if (toInput) toInput.value = isoLocalString(now);
    safeUpdatePreviews();
  })();

})();
