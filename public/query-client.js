// query-client.js — full-featured query UI: presets, fetch, clear, CSV export.
// Pagination: pageSize 2000 with Prev/Next/First/Last. Controls shown only after results.
// Fixed issues that could stop scripts from running (robust null checks, no accidental runtime errors).
// Uses recorded_at_str if provided by server, otherwise falls back to recorded_at.
//
// SUMMARY OF CHANGE:
// - The only change in this file is how the "Recorded_At" cell is rendered.
// - It now converts the backend-provided timestamp into the local device time and
//   displays it in a simple human-readable format: "DD/MM/YYYY HH:MM:SS".
// - Preference order for parsing: recorded_at (ISO) -> recorded_at_raw -> recorded_at_str.
// - If parsing fails, the original string is shown unchanged.

(function () {
  const fetchBtn = document.getElementById('fetch');
  const fromInput = document.getElementById('from');
  const toInput = document.getElementById('to');
  const resultsEl = document.getElementById('results');
  const resultsEmpty = document.getElementById('results-empty');
  const presets = document.querySelectorAll('.preset');
  const clearResultsBtn = document.getElementById('clear-results');
  const exportCsvBtn = document.getElementById('export-csv');

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
  // Parse various backend timestamp formats into a JS Date if possible.
  // - Prefer ISO-like strings and numeric epochs (seconds or ms).
  // - Returns a Date object or null if parsing fails.
  function parseToDate(val) {
    if (val === null || val === undefined || val === '') return null;
    // Already a Date
    if (val instanceof Date && !isNaN(val.getTime())) return val;
    // Number
    if (typeof val === 'number' && Number.isFinite(val)) {
      // > 1e12 -> ms, > 1e9 -> seconds
      if (val > 1e12) return new Date(val);
      if (val > 1e9) return new Date(val * 1000);
      return null;
    }
    // String
    if (typeof val === 'string') {
      const s = val.trim();
      if (!s) return null;
      // Pure numeric string -> epoch
      if (/^\d+$/.test(s)) {
        const n = Number(s);
        if (n > 1e12) return new Date(n);
        if (n > 1e9) return new Date(n * 1000);
        // else continue to parse
      }
      // Try Date.parse (handles ISO and many other formats)
      const parsed = Date.parse(s);
      if (!Number.isNaN(parsed)) {
        return new Date(parsed);
      }
      // Try replacing space with 'T' (common "YYYY-MM-DD HH:MM:SS")
      const alt = s.replace(' ', 'T');
      const parsed2 = Date.parse(alt);
      if (!Number.isNaN(parsed2)) return new Date(parsed2);
      return null;
    }
    // Other types cannot be parsed
    return null;
  }

  // Format a Date object in local timezone as "DD/MM/YYYY HH:MM:SS"
  function pad2(v) { return String(v).padStart(2,'0'); }
  function formatDateLocal(d) {
    const day = pad2(d.getDate());
    const month = pad2(d.getMonth() + 1);
    const year = d.getFullYear();
    const hours = pad2(d.getHours());
    const minutes = pad2(d.getMinutes());
    const seconds = pad2(d.getSeconds());
    return `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;
  }

  // Convert backend value to a local display string. Preference:
  // 1) recorded_at (ISO) -> parse and show in local time
  // 2) recorded_at_raw (verbatim) -> try parse and show in local time; else show raw
  // 3) recorded_at_str (server-formatted) -> try parse and show in local time; else show raw
  function toLocalDisplay(val) {
    if (val === null || val === undefined || val === '') return '';
    const dt = parseToDate(val);
    if (dt) return formatDateLocal(dt);
    try { return String(val); } catch (e) { return ''; }
  }

  // Init defaults: populate inputs with last 1 hour
  (function initDefaults() {
    const now = new Date();
    const from = new Date(now.getTime() - 3600 * 1000);
    if (fromInput) fromInput.value = isoLocalString(from);
    if (toInput) toInput.value = isoLocalString(now);
  })();

  // Inject paging controls UI (below the results element)
  (function injectPagingControls() {
    try {
      const panel = document.getElementById('query-panel');
      if (!panel || !resultsEl) {
        // nothing to do; bail gracefully
        return;
      }

      pagingControlsEl = document.createElement('div');
      pagingControlsEl.style.display = 'none'; // hidden until results exist
      pagingControlsEl.style.alignItems = 'center';
      pagingControlsEl.style.gap = '8px';
      pagingControlsEl.style.marginTop = '8px';
      pagingControlsEl.style.flexWrap = 'wrap';
      pagingControlsEl.style.display = 'flex';
      // We'll hide it right after if no results. The element exists; display toggled by show/hide.

      // FIRST page button
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

      // LAST page button
      const lastBtn = document.createElement('button');
      lastBtn.className = 'btn';
      lastBtn.textContent = 'Last';
      lastBtn.id = 'page-last';
      lastBtn.disabled = true;

      // append controls in order: First, Prev, Label, Next, Last
      pagingControlsEl.appendChild(firstBtn);
      pagingControlsEl.appendChild(prevBtn);
      pagingControlsEl.appendChild(pageLabel);
      pagingControlsEl.appendChild(nextBtn);
      pagingControlsEl.appendChild(lastBtn);

      // spacer so moved buttons align to right
      const spacer = document.createElement('div');
      spacer.style.flex = '1';
      pagingControlsEl.appendChild(spacer);

      // move Clear and CSV only if they exist and are not already in the paging bar
      if (exportCsvBtn && exportCsvBtn.parentNode !== pagingControlsEl) {
        pagingControlsEl.appendChild(exportCsvBtn);
      }
      if (clearResultsBtn && clearResultsBtn.parentNode !== pagingControlsEl) {
        // ensure Clear has secondary look
        clearResultsBtn.classList.add('secondary');
        pagingControlsEl.appendChild(clearResultsBtn);
      }

      // place paging controls after the results element
      if (resultsEl && resultsEl.parentNode) {
        resultsEl.parentNode.insertBefore(pagingControlsEl, resultsEl.nextSibling);
      } else {
        panel.appendChild(pagingControlsEl);
      }

      // Event handlers for the paging buttons (safe guards if buttons missing)
      firstBtn.addEventListener('click', () => {
        // Jump to first page (0)
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
        // Aggressive: auto-fetch forward until we find a short page (< pageSize)
        // or until the server returns an empty page. Safety cap included.
        const params = getParamsForCurrentInputs();
        if (!params) return;

        // Ensure we have at least page 0 cached (so we can determine where to start)
        if (!pageCache.has(0)) {
          const firstRows = await fetchPageRows(0, params.fromISO, params.toISO);
          if (firstRows === null) return; // error
          pageCache.set(0, firstRows);
          lastFetchedCountForPage.set(0, firstRows.length);
          if (firstRows.length === 0) {
            // no results at all
            currentPage = 0;
            loadAndRenderPage(0);
            return;
          }
        }

        // Start from highest known cached page + 1
        let cached = Array.from(pageCache.keys()).sort((a,b) => a - b);
        let start = (cached.length === 0) ? 0 : (cached[cached.length - 1] + 1);
        // If start is 0 and we already have page 0 cached and it's short, use it
        if (start === 0) {
          const c0 = lastFetchedCountForPage.get(0);
          if (typeof c0 === 'number' && c0 < pageSize) {
            currentPage = 0;
            loadAndRenderPage(currentPage);
            return;
          }
        }

        // Provide UI feedback
        if (resultsEl) resultsEl.innerHTML = `<div style="color:var(--muted)">Locating last page…</div>`;

        // Safety cap to avoid infinite loops; you can increase if needed.
        const MAX_PAGES_TO_SCAN = 2000;
        let p = start;
        let found = false;

        for (; p < start + MAX_PAGES_TO_SCAN; p++) {
          // If already cached, reuse
          if (pageCache.has(p)) {
            const rowsCached = pageCache.get(p) || [];
            lastFetchedCountForPage.set(p, rowsCached.length);
            if (rowsCached.length === 0) {
              // empty page => last page is p-1 (if exists)
              currentPage = Math.max(0, p - 1);
              found = true;
              break;
            }
            if (rowsCached.length < pageSize) {
              currentPage = p;
              found = true;
              break;
            }
            // otherwise full page, continue to next
            continue;
          }

          // Fetch this page
          const rows = await fetchPageRows(p, params.fromISO, params.toISO);
          if (rows === null) {
            // error fetching; stop and fallback to highest cached page
            break;
          }
          pageCache.set(p, rows);
          lastFetchedCountForPage.set(p, rows.length);

          if (rows.length === 0) {
            // no rows for this page -> last page is p-1
            currentPage = Math.max(0, p - 1);
            found = true;
            break;
          }
          if (rows.length < pageSize) {
            // found final (partial) page
            currentPage = p;
            found = true;
            break;
          }
          // otherwise full page -> continue loop to probe next page
        }

        if (!found) {
          // didn't find a short page within cap; fall back to highest cached page
          const allCached = Array.from(pageCache.keys());
          if (allCached.length > 0) {
            allCached.sort((a,b) => a - b);
            currentPage = allCached[allCached.length - 1];
          } else {
            currentPage = 0;
          }
        }

        // load the decided page (will use cache if available)
        loadAndRenderPage(currentPage);
      });

      // start hidden until results exist
      hidePagingControls();
    } catch (e) {
      // log and continue; do not let injection errors stop the rest of the script
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
      // reset authoritative total when clearing results
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
        for (const p of pages) {
          rowsToExport = rowsToExport.concat(pageCache.get(p) || []);
        }
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

    if (params.hash !== lastRequestParamsHash) {
      clearCacheForNewQuery(params.hash);
    }

    updatePageLabel();
    if (pageCache.has(pageIndex)) {
      renderResultsTable(pageCache.get(pageIndex));
      updatePageLabel();
      setPagingButtonsState();
      // kick off background total scan (if not in progress)
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
      // requested a page beyond end -> step back
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

    // Start scanning to compute authoritative total for the current query (background)
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
    // reset authoritative total for a new query
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
    // hide visually but keep in DOM
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
    // Updated visible header labels per request:
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
      // Convert server timestamp to local device time for display.
      // Prefer recorded_at (ISO) -> recorded_at_raw -> recorded_at_str.
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

    // Show paging/utility controls now that we have results
    showPagingControls();
  }

  // Start a background total scan for the given params (if not already running for same params)
  function startTotalScanForParamsIfNeeded(params) {
    if (!params || !params.hash) return;
    if (totalScanInProgressForHash === params.hash) return; // already scanning for this query
    totalScanInProgressForHash = params.hash;

    // Set an immediate interim total from cached pages so UI doesn't show 0.
    // This ensures the "Total Count" updates immediately (even while the full scan runs).
    try {
      let interim = 0;
      const cachedPages = Array.from(pageCache.keys()).sort((a,b) => a - b);
      for (const p of cachedPages) {
        const arr = pageCache.get(p) || [];
        interim += arr.length;
      }
      // if we have no cached pages, interim remains 0 (that's fine)
      window.rowtotal = Number(interim) || 0;
      pokeResultsForRowTotal();
    } catch (e) {
      try { window.rowtotal = 0; } catch (e2) { /* ignore */ }
    }

    // run asynchronously (no await here — fire and forget)
    (async () => {
      try {
        const total = await computeTotalRowsForQuery(params);
        // expose result on window so client.js can use it
        try { window.rowtotal = Number(total) || 0; } catch (e) { /* ignore */ }
        // trigger a tiny DOM add/remove to ensure client.js mutation observer notices the update
        pokeResultsForRowTotal();
      } catch (e) {
        console.error('computeTotalRowsForQuery error', e);
        try { window.rowtotal = 0; } catch (e2) { /* ignore */ }
        pokeResultsForRowTotal();
      } finally {
        // mark done
        if (totalScanInProgressForHash === params.hash) totalScanInProgressForHash = null;
      }
    })();
  }

  // Compute total authoritative row count for current query by scanning pages until a short page is found.
  // This function intentionally does NOT modify UI state except setting window.rowtotal.
  async function computeTotalRowsForQuery(params) {
    // Sum lengths of any already cached pages
    let sum = 0;
    const cachedPages = Array.from(pageCache.keys()).sort((a,b) => a - b);
    for (const p of cachedPages) {
      const arr = pageCache.get(p) || [];
      sum += arr.length;
    }

    // If no cached pages exist, attempt to fetch page 0 to seed
    if (cachedPages.length === 0) {
      const first = await fetchPageRows(0, params.fromISO, params.toISO);
      if (first === null) return 0; // error -> treat as 0
      // store and include
      pageCache.set(0, first);
      lastFetchedCountForPage.set(0, first.length);
      sum += first.length;
      // if this page is short, it's the only page
      if (first.length < pageSize) return sum;
    }

    // Start scanning from highest cached page + 1
    let start = (cachedPages.length === 0) ? 1 : (cachedPages[cachedPages.length - 1] + 1);
    // If cachedPages included some pages but we just fetched page 0 above, recompute start
    const allCached = Array.from(pageCache.keys()).sort((a,b) => a - b);
    if (allCached.length > 0) {
      start = allCached[allCached.length - 1] + 1;
    }

    // Cap number of pages to probe to avoid runaway requests
    const MAX_PAGES_TO_SCAN = 10000; // very high cap; adjust if needed
    for (let p = start, scans = 0; scans < MAX_PAGES_TO_SCAN; p++, scans++) {
      const rows = await fetchPageRows(p, params.fromISO, params.toISO);
      if (rows === null) {
        // on error, stop and return current sum (partial)
        break;
      }
      pageCache.set(p, rows);
      lastFetchedCountForPage.set(p, rows.length);
      sum += rows.length;
      // If this page is short, we've reached the last page -> done
      if (rows.length < pageSize) {
        return sum;
      }
      // otherwise continue scanning
    }

    // If we reach here we scanned the cap without finding a short page — return the partial sum.
    return sum;
  }

  // When rowtotal is updated, client.js listens to mutations in #results. To ensure it notices
  // the new window.rowtotal value even if results DOM didn't change, perform a tiny add/remove
  // child mutation on resultsEl (invisible marker). This triggers the client's MutationObserver.
  function pokeResultsForRowTotal() {
    try {
      if (!resultsEl) return;
      const id = '__rowtotal_marker__';
      // ensure any previous marker removed
      const prev = document.getElementById(id);
      if (prev && prev.parentNode) prev.parentNode.removeChild(prev);

      const marker = document.createElement('div');
      marker.id = id;
      marker.style.display = 'none';
      // append then remove next tick to generate childList mutations
      resultsEl.appendChild(marker);
      setTimeout(() => {
        try {
          if (marker.parentNode) marker.parentNode.removeChild(marker);
        } catch (e) { /* ignore */ }
      }, 0);
    } catch (e) {
      // ignore
    }
  }

  // Expose for debugging in console if needed
  window.__queryClient = {
    loadPage: loadAndRenderPage,
    pageCache,
    pageSize,
    currentPageRef: () => currentPage
  };
})();
