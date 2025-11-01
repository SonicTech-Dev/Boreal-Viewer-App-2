// query-client.js — handles historical queries against /api/los and renders results, plus CSV export.
// Fixes wiring so buttons and inputs work reliably.

(function () {
  const fetchBtn = document.getElementById('fetch');
  const fromInput = document.getElementById('from');
  const toInput = document.getElementById('to');
  const resultsEl = document.getElementById('results');
  const resultsEmpty = document.getElementById('results-empty');
  const presets = document.querySelectorAll('.preset');
  const clearResultsBtn = document.getElementById('clear-results');
  const exportCsvBtn = document.getElementById('export-csv');
  const topicEl = document.getElementById('topic');

  let lastRows = []; // keep most recent result set for CSV export

  // show configured topic if available
  fetch('/_mqtt_config').then(r => r.json()).then(cfg => {
    if (cfg && cfg.topic) topicEl.textContent = cfg.topic;
  }).catch(()=>{});

  function isoLocalString(d) {
    const pad = (n) => String(n).padStart(2,'0');
    const yyyy = d.getFullYear();
    const mm = pad(d.getMonth()+1);
    const dd = pad(d.getDate());
    const hh = pad(d.getHours());
    const min = pad(d.getMinutes());
    return `${yyyy}-${mm}-${dd}T${hh}:${min}`;
  }

  // presets wiring (works regardless of script load order)
  presets.forEach(btn => {
    btn.addEventListener('click', () => {
      const range = btn.dataset.range;
      const now = new Date();
      let from;
      if (range.endsWith('h')) {
        const hours = Number(range.replace('h','')) || 1;
        from = new Date(now.getTime() - hours * 3600 * 1000);
      } else if (range.endsWith('d')) {
        const days = Number(range.replace('d','')) || 1;
        from = new Date(now.getTime() - days * 24 * 3600 * 1000);
      } else {
        from = new Date(now.getTime() - 3600 * 1000);
      }
      fromInput.value = isoLocalString(from);
      toInput.value = isoLocalString(now);
    });
  });

  fetchBtn.addEventListener('click', async () => {
    const from = fromInput.value ? new Date(fromInput.value).toISOString() : undefined;
    const to = toInput.value ? new Date(toInput.value).toISOString() : undefined;

    if (from && to && new Date(from) > new Date(to)) {
      alert('From must be before To');
      return;
    }

    resultsEl.innerHTML = '<div class="sub">Loading…</div>';
    try {
      const params = new URLSearchParams();
      if (from) params.set('from', from);
      if (to) params.set('to', to);
      params.set('limit', '2000');
      const r = await fetch('/api/los?' + params.toString());
      if (!r.ok) throw new Error('Server returned ' + r.status);
      const data = await r.json();
      if (!data.ok) throw new Error('Query failed');
      lastRows = data.rows || [];
      renderResultsTable(lastRows);
    } catch (err) {
      console.error('Query error', err);
      resultsEl.innerHTML = `<div class="sub">Error fetching results: ${err.message || err}</div>`;
      lastRows = [];
    }
  });

  clearResultsBtn.addEventListener('click', () => {
    lastRows = [];
    resultsEl.innerHTML = '';
    resultsEl.appendChild(resultsEmpty);
  });

  exportCsvBtn.addEventListener('click', () => {
    if (!lastRows || lastRows.length === 0) {
      alert('No data to export');
      return;
    }
    // build CSV (header + rows)
    const headers = ['id','recorded_at','los_temp','los_rx_light','los_r2','los_heartbeat','los_ppm'];
    const lines = [headers.join(',')];
    lastRows.forEach(r => {
      const vals = [
        r.id,
        `"${r.recorded_at}"`,
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

  function renderResultsTable(rows) {
    resultsEl.innerHTML = '';
    if (!rows || rows.length === 0) {
      resultsEl.appendChild(resultsEmpty);
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
      tdTime.textContent = new Date(row.recorded_at).toLocaleString();
      tr.appendChild(tdTime);

      const addCell = (val) => {
        const td = document.createElement('td');
        td.textContent = val === null || val === undefined ? '' : (typeof val === 'number' && !Number.isInteger(val) ? val.toFixed(2) : String(val));
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
  }

  // Initialize with last 1 hour preset populated (but do not auto-fetch)
  (function initDefaults() {
    const now = new Date();
    const from = new Date(now.getTime() - 3600 * 1000);
    fromInput.value = isoLocalString(from);
    toInput.value = isoLocalString(now);
  })();
})();