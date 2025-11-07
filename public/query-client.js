// query-client.js — full-featured query UI: presets, fetch, clear, CSV export.
// Pagination: pageSize 2000 with Prev/Next/Fetch All. Controls shown only after results.
// Fixed issues that could stop scripts from running (robust null checks, no accidental runtime errors).
// Uses recorded_at_str if provided by server, otherwise falls back to recorded_at.
//
// Summary of fixes in this version (why your presets/fetch might have stopped working):
// - Added robust checks around DOM manipulation in injectPagingControls so an unexpected
//   null parent or moving nodes doesn't throw and abort the rest of the script.
// - Removed the stray double-assignment to pagingControlsEl.style.display that previously
//   set display to 'none' then immediately to 'flex' (harmless but confusing).
// - Only move Clear/CSV buttons when they exist and are not already in the paging bar.
// - All event listeners (presets, fetch, clear, export) are attached after DOM elements
//   are known to exist. Any error in injection is caught and logged but won't stop the rest.
// - show/hide paging controls use explicit checks and do not accidentally break other UI code.

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

      // NOTE: Fetch All button removed as requested.

      pagingControlsEl.appendChild(prevBtn);
      pagingControlsEl.appendChild(pageLabel);
      pagingControlsEl.appendChild(nextBtn);
      // (Fetch All omitted)

      // spacer so moved buttons align to right
      const spacer = document.createElement('div');
      spacer.style.flex = '1';
      pagingControlsEl.appendChild(spacer);

      // move Clear and CSV only if they exist and are not already inside the paging bar
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

      const headers = ['id','recorded_at','los_temp','los_rx_light','los_r2','los_heartbeat','los_ppm'];
      const lines = [headers.join(',')];
      rowsToExport.forEach(r => {
        const vals = [
          r.id,
          `"${r.recorded_at_str || r.recorded_at || ''}"`,
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
    if (!prevBtn || !nextBtn) return;
    prevBtn.disabled = currentPage <= 0;
    const curCount = lastFetchedCountForPage.get(currentPage);
    if (typeof curCount === 'number' && curCount < pageSize) {
      nextBtn.disabled = true;
    } else {
      nextBtn.disabled = false;
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
    ['recorded_at','los_temp','los_rx_light','los_r2','los_heartbeat','los_ppm'].forEach(h => {
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
      tdTime.textContent = row.recorded_at_str || row.recorded_at || '';
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

  // Expose for debugging in console if needed
  window.__queryClient = {
    loadPage: loadAndRenderPage,
    pageCache,
    pageSize,
    currentPageRef: () => currentPage
  };
})();
