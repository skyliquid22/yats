/*
 * YATS Dashboard — app.js
 * Vanilla JS, zero build step, zero external libraries.
 * Polls every 30 s; pauses when tab is hidden (visibilityState).
 *
 * ── API Shape Assumptions ───────────────────────────────────────────────────
 *
 * GET /api/health
 *   → { services: [{service: str, status: "ok"|"warn"|"crit", latency_ms: int}],
 *       kill_switches: [{timestamp, experiment_id, mode, trigger, action,
 *                        triggered_by, details, resolved, resolved_at, resolved_by}] }
 *
 * GET /api/coverage
 *   → { [domain]: { symbol_count: int, from: str|null, to: str|null,
 *                   mode_rows: int, exceptions: str[], etf_absent: str[],
 *                   sessions_behind: int,
 *                   freshness_status: "ok"|"warn"|"crit" }
 *               | { error: str } }
 *   domains: equity_ohlcv, options_eod, fundamentals,
 *            insider_trades, institutional_holdings
 *
 * GET /api/storage
 *   → { [group]: [{table: str, row_count: int, disk_size: int,
 *                  wal_enabled: bool, dedup: bool}] }
 *   groups: raw, canonical, features, experiments, ops, other
 *
 * GET /api/storage/{domain}/tickers
 *   → [{symbol: str, from_ts, to_ts, row_count: int}]
 *     | [{feature_set: str, symbol: str, from_ts, to_ts, row_count: int}]
 *   domain ∈ equity_ohlcv | options_eod | fundamentals |
 *             insider_trades | institutional_holdings | features
 *
 * GET /api/runs?hours=24
 *   → [{started_at, job_name, dagster_run_id, status,
 *       finished_at, duration_s: float|null, rows_written: int|null,
 *       detail: str|null, failure_cause: str|null}]
 *
 * GET /api/experiments?q=
 *   → { results: [{experiment_id, feature_set, policy_type,
 *                  sharpe: float|null, calmar: float|null,
 *                  max_drawdown: float|null, total_return: float|null,
 *                  qualification_status: str|null,
 *                  promotion_tier: str|null, created_at, stale: bool,
 *                  evaluation_regime: "split"|"wfo-oos",
 *                  // wfo-oos only: dsr, dsr_significant, sweep
 *                 }],
 *       deflation_clock: int }
 *
 * GET /api/experiments/{id}
 *   → { experiment_id: str,
 *       spec: {universe, feature_set, policy_type, ...}|null,
 *       performance: {sharpe, calmar, total_return, max_drawdown, ...}|null,
 *       trading: {total_trades, win_rate, ...}|null,
 *       safety: {...}|null,
 *       config: {...}|null,
 *       inputs_used: [...],
 *       equity_curve: float[],   // ≤200 raw NAV values starting ~1.0
 *       qualification_report: {...}|null,
 *       index: {experiment_id, universe, feature_set, policy_type,
 *               sharpe, calmar, max_drawdown, total_return,
 *               qualification_status, promotion_tier, created_at}|null }
 *
 * GET /api/trading
 *   → { portfolio_state: [{timestamp, experiment_id, mode, nav, cash,
 *                           gross_exposure, net_exposure, leverage,
 *                           num_positions, daily_pnl, peak_nav, drawdown}],
 *       open_orders: [{experiment_id, mode, open_count}],
 *       risk_decisions: [{timestamp, rule_id, experiment_id, mode,
 *                          decision, action_taken,
 *                          original_size, reduced_size}],
 *       heartbeat: [{timestamp, experiment_id, mode, loop_iteration,
 *                    orders_pending, last_bar_received,
 *                    age_s: float,
 *                    heartbeat_status: "ok"|"crit"}],
 *       promotions: [{promoted_at, experiment_id,
 *                     tier: "live"|"shadow",
 *                     promoted_by, qualification_passed,
 *                     sharpe, max_drawdown, allocation: float}] }
 *
 * Alert thresholds (freshness_status, heartbeat_status, dedup-backlog) are
 * status ENUMS supplied by the API; this file renders them, never computes them.
 * ───────────────────────────────────────────────────────────────────────────
 */

'use strict';

// ── App state ───────────────────────────────────────────────────────────────

const state = {
  page: 'system',
  equityFilter: 'all',   // all | stocks | etfs
  expQuery: '',
  selectedExpId: null,
};

const cache = {
  health:      null,
  coverage:    null,
  storage:     null,
  runs:        null,
  experiments: null,
  trading:     null,
  drillDown:   {},       // domain → tickers[]
};

// ── Format helpers ───────────────────────────────────────────────────────────

function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function fmtNum(n) {
  if (n == null) return '—';
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(Math.round(n));
}

function fmtBytes(b) {
  if (b == null) return '—';
  if (b >= 1073741824) return (b / 1073741824).toFixed(1) + ' GB';
  if (b >= 1048576)    return (b / 1048576).toFixed(1) + ' MB';
  if (b >= 1024)       return (b / 1024).toFixed(0) + ' KB';
  return b + ' B';
}

function fmtDate(s) {
  if (!s) return '—';
  return String(s).slice(0, 10);
}

function fmtDuration(s) {
  if (s == null) return '—';
  if (s < 60) return s.toFixed(1) + 's';
  const m = Math.floor(s / 60);
  const sec = (s % 60).toFixed(0).padStart(2, '0');
  return `${m}m ${sec}s`;
}

function fmtPct(v) {
  if (v == null) return '—';
  return (v * 100).toFixed(1) + '%';
}

function fmtFloat(v, dp) {
  if (v == null) return '—';
  return Number(v).toFixed(dp == null ? 2 : dp);
}

function fmtMoney(v) {
  if (v == null) return '—';
  if (Math.abs(v) >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'K';
  return '$' + Number(v).toFixed(2);
}

// ── DOM helpers ──────────────────────────────────────────────────────────────

function el(id) { return document.getElementById(id); }

function html(id, markup) {
  const node = el(id);
  if (node) node.innerHTML = markup;
}

function dot(status) {
  const cls = status === 'ok' ? 'ok' : status === 'warn' ? 'warn' : status === 'crit' ? 'crit' : 'info';
  return `<span class="status-dot dot-${cls}"></span>`;
}

function badge(text, cls) {
  return `<span class="badge badge-${esc(cls)}">${esc(text)}</span>`;
}

// ── Equity curve ─────────────────────────────────────────────────────────────

function _findMaxDDSegment(values) {
  let curPeakIdx = 0, peakIdx = 0, troughIdx = 0, maxDD = 0;
  for (let i = 1; i < values.length; i++) {
    if (values[i] > values[curPeakIdx]) {
      curPeakIdx = i;
    } else {
      const pk = values[curPeakIdx];
      const dd = pk !== 0 ? (values[i] - pk) / Math.abs(pk) : 0;
      if (dd < maxDD) { maxDD = dd; peakIdx = curPeakIdx; troughIdx = i; }
    }
  }
  return { peakIdx, troughIdx };
}

function renderEquityCurve(curve) {
  if (!curve || curve.length < 2) return '<p class="muted">No equity curve data</p>';

  const W = 600, H = 130, PX = 12, PY = 10;
  const plotW = W - PX * 2;
  const plotH = H - PY * 2;

  const minV = Math.min.apply(null, curve);
  const maxV = Math.max.apply(null, curve);
  const range = maxV - minV || 1;

  function px(v, i) {
    return [
      PX + (i / (curve.length - 1)) * plotW,
      PY + (1 - (v - minV) / range) * plotH,
    ];
  }

  const pts = curve.map((v, i) => px(v, i).map(n => n.toFixed(1)).join(',')).join(' ');

  const firstX = PX.toFixed(1), lastX = (PX + plotW).toFixed(1), baseY = (PY + plotH).toFixed(1);
  const areaPath = `M${firstX},${baseY} ` +
    curve.map((v, i) => 'L' + px(v, i).map(n => n.toFixed(1)).join(',')).join(' ') +
    ` L${lastX},${baseY} Z`;

  const { peakIdx, troughIdx } = _findMaxDDSegment(curve);
  let ddPts = '';
  if (peakIdx < troughIdx) {
    ddPts = curve.slice(peakIdx, troughIdx + 1)
      .map((v, j) => px(v, peakIdx + j).map(n => n.toFixed(1)).join(','))
      .join(' ');
  }

  return `<div class="curve-wrap">
    <svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Equity curve with max drawdown segment highlighted">
      <path d="${areaPath}" class="curve-area"/>
      <polyline points="${pts}" class="curve-line"/>
      ${ddPts ? `<polyline points="${ddPts}" class="curve-dd" aria-label="Max drawdown segment"/>` : ''}
    </svg>
  </div>`;
}

// ── Render: System page ──────────────────────────────────────────────────────

function renderHealth(data) {
  if (!data) return;

  const chips = (data.services || []).map(s => {
    const cls = s.status === 'ok' ? 'ok' : s.status === 'warn' ? 'warn' : 'crit';
    return `<span class="health-chip">
      ${dot(s.status)}
      <span class="chip-name">${esc(s.service)}</span>
      <span class="chip-latency">${s.latency_ms != null ? s.latency_ms + 'ms' : '—'}</span>
    </span>`;
  });

  html('health-chips', chips.length ? chips.join('') : '<span class="muted">No services</span>');

  const active = (data.kill_switches || []).filter(k => !k.resolved);
  if (active.length === 0) {
    html('kill-switches', '');
    return;
  }
  html('kill-switches', `
    <div class="kill-banner">
      <div class="kill-banner-title">⚠ Kill Switches Active (${active.length})</div>
      ${active.map(k => `<div class="kill-item">
        <strong>${esc(k.experiment_id)}</strong>
        · trigger: ${esc(k.trigger)} · action: ${esc(k.action)}
        ${k.details ? `<div class="kill-detail">${esc(k.details)}</div>` : ''}
      </div>`).join('')}
    </div>`);
}

const COV_META = {
  equity_ohlcv:           { label: 'Equity OHLCV',          stockOnly: false },
  options_eod:            { label: 'Options EOD',            stockOnly: false },
  fundamentals:           { label: 'Fundamentals',           stockOnly: true  },
  insider_trades:         { label: 'Insider Trades',         stockOnly: true  },
  institutional_holdings: { label: 'Institutional Holdings', stockOnly: true  },
};

function renderCoverage(data) {
  if (!data) return;
  const filter = state.equityFilter;

  const cards = Object.entries(data).map(([domain, info]) => {
    const meta = COV_META[domain] || { label: domain, stockOnly: false };

    // Filter visibility: etfs hides stock-only; stocks hides options-only (options_eod not stock-only)
    let visible = true;
    if (filter === 'etfs' && meta.stockOnly) visible = false;
    if (filter === 'stocks' && domain === 'options_eod') visible = false;

    if (!visible) return `<div class="cov-card cov-hidden" data-domain="${esc(domain)}"></div>`;

    if (info.error) {
      return `<div class="cov-card" data-domain="${esc(domain)}">
        <div class="cov-domain">${esc(meta.label)}</div>
        <div class="err-msg">${esc(info.error)}</div>
      </div>`;
    }

    const fs = info.freshness_status || 'ok';
    const sb = info.sessions_behind;
    const sbNote = sb != null && sb >= 0
      ? `<span class="muted">${sb} session${sb !== 1 ? 's' : ''} behind</span>` : '';

    const excs = info.exceptions || [];
    const etfA = info.etf_absent || [];

    const excHtml = excs.length ? `<div class="exc-list">
      <details>
        <summary>${excs.length} exception${excs.length !== 1 ? 's' : ''}</summary>
        <div style="margin-top:4px">
          ${excs.slice(0, 40).map(s => `<span class="exc-sym">${esc(s)}</span>`).join('')}
          ${excs.length > 40 ? `<span class="muted"> +${excs.length - 40} more</span>` : ''}
        </div>
      </details>
    </div>` : '';

    const etfHtml = etfA.length ? `<div class="exc-list" style="margin-top:5px">
      <span class="muted" style="font-size:10px">ETF absent: </span>
      ${etfA.map(s => `<span class="etf-absent-sym">${esc(s)}</span>`).join('')}
    </div>` : '';

    return `<div class="cov-card" data-domain="${esc(domain)}">
      <div class="cov-domain">${esc(meta.label)}</div>
      <div class="cov-count">${fmtNum(info.symbol_count)}</div>
      <div class="cov-dates">${fmtDate(info.from)} → ${fmtDate(info.to)}</div>
      <div class="cov-status">${dot(fs)} ${badge(fs.toUpperCase(), fs)} ${sbNote}</div>
      ${excHtml}${etfHtml}
    </div>`;
  });

  html('coverage-cards', cards.join('') || '<span class="muted">No coverage data</span>');
}

const TABLE_DRILL = {
  canonical_equity_ohlcv:           'equity_ohlcv',
  canonical_options_chain:          'options_eod',
  canonical_fundamentals:           'fundamentals',
  canonical_insider_trades:         'insider_trades',
  canonical_institutional_holdings: 'institutional_holdings',
  features:                         'features',
};

function renderStorage(data) {
  if (!data) return;

  const groups = Object.entries(data);
  if (!groups.length) { html('storage-table', '<span class="muted">No storage data</span>'); return; }

  const markup = groups.map(([group, tables]) => `
    <div class="stor-group">
      <div class="stor-group-title">${esc(group)}</div>
      <div class="tbl-wrap">
        <table>
          <thead><tr>
            <th>Table</th><th>Rows</th><th>Size</th><th>Flags</th><th></th>
          </tr></thead>
          <tbody>
            ${tables.map(t => {
              const drill = TABLE_DRILL[t.table];
              const drillRowId = 'dr-' + esc(t.table).replace(/[^a-z0-9]/gi, '_');
              const flags = [
                t.wal_enabled ? badge('WAL', 'info') : '',
                t.dedup        ? badge('DEDUP', 'info') : '',
              ].filter(Boolean).join(' ');
              return `<tr>
                <td class="mono" style="font-size:11px">${esc(t.table)}</td>
                <td class="mono">${fmtNum(t.row_count)}</td>
                <td class="mono">${fmtBytes(t.disk_size)}</td>
                <td>${flags}</td>
                <td>${drill
                  ? `<button class="expand-btn" data-drill="${esc(drill)}" data-row="${drillRowId}"
                             aria-expanded="false" aria-controls="${drillRowId}">▶ tickers</button>`
                  : ''}</td>
              </tr>
              <tr id="${drillRowId}" class="drill-row hidden">
                <td colspan="5">
                  <div class="drill-inner"><span class="loading">Loading…</span></div>
                </td>
              </tr>`;
            }).join('')}
          </tbody>
        </table>
      </div>
    </div>`
  ).join('');

  html('storage-table', markup);

  el('storage-table').querySelectorAll('.expand-btn').forEach(btn => {
    btn.addEventListener('click', () => handleDrill(btn));
  });
}

async function handleDrill(btn) {
  const domain = btn.dataset.drill;
  const rowId  = btn.dataset.row;
  const row    = el(rowId);
  if (!row) return;

  const isOpen = btn.getAttribute('aria-expanded') === 'true';

  if (isOpen) {
    row.classList.add('hidden');
    btn.setAttribute('aria-expanded', 'false');
    btn.textContent = '▶ tickers';
    return;
  }

  row.classList.remove('hidden');
  btn.setAttribute('aria-expanded', 'true');
  btn.textContent = '▼ tickers';

  if (cache.drillDown[domain]) {
    _renderDrillContent(row.querySelector('.drill-inner'), cache.drillDown[domain]);
    return;
  }

  try {
    const data = await apiFetch('/api/storage/' + encodeURIComponent(domain) + '/tickers');
    cache.drillDown[domain] = data;
    _renderDrillContent(row.querySelector('.drill-inner'), data);
  } catch (e) {
    row.querySelector('.drill-inner').innerHTML = `<span class="err-msg">Failed: ${esc(e.message)}</span>`;
  }
}

function _renderDrillContent(container, tickers) {
  if (!container) return;
  if (!tickers || !tickers.length) { container.innerHTML = '<span class="muted">No tickers</span>'; return; }

  const hasFeatureSet = 'feature_set' in tickers[0];
  const shown = tickers.slice(0, 60);
  const more  = tickers.length > 60 ? `<tr><td colspan="${hasFeatureSet ? 5 : 4}" class="muted">+${tickers.length - 60} more</td></tr>` : '';

  if (hasFeatureSet) {
    container.innerHTML = `<table class="ticker-table">
      <thead><tr><th>Feature Set</th><th>Symbol</th><th>From</th><th>To</th><th>Rows</th></tr></thead>
      <tbody>
        ${shown.map(t => `<tr>
          <td class="mono">${esc(t.feature_set)}</td>
          <td class="mono">${esc(t.symbol)}</td>
          <td class="mono">${fmtDate(t.from_ts)}</td>
          <td class="mono">${fmtDate(t.to_ts)}</td>
          <td class="mono">${fmtNum(t.row_count)}</td>
        </tr>`).join('')}${more}
      </tbody>
    </table>`;
  } else {
    container.innerHTML = `<table class="ticker-table">
      <thead><tr><th>Symbol</th><th>From</th><th>To</th><th>Rows</th></tr></thead>
      <tbody>
        ${shown.map(t => `<tr>
          <td class="mono">${esc(t.symbol)}</td>
          <td class="mono">${fmtDate(t.from_ts)}</td>
          <td class="mono">${fmtDate(t.to_ts)}</td>
          <td class="mono">${fmtNum(t.row_count)}</td>
        </tr>`).join('')}${more}
      </tbody>
    </table>`;
  }
}

function renderRuns(data) {
  if (!data) return;

  const windowEl = el('runs-window');
  if (windowEl) windowEl.textContent = '(last 24 h)';

  if (!data.length) { html('runs-feed', '<span class="muted">No runs in the last 24 h</span>'); return; }

  const rows = data.slice(0, 40).map(r => {
    const isFailed  = r.status === 'failed' || r.status === 'FAILED';
    const isRunning = r.status === 'running' || r.status === 'RUNNING';
    const statusB   = isFailed  ? badge('FAILED',  'crit')
                    : isRunning ? badge('RUNNING', 'warn')
                    : badge('OK', 'ok');
    return `<div class="run-item">
      <span class="run-job">${esc(r.job_name)}</span>
      <span class="run-time">${fmtDate(r.started_at)}</span>
      ${statusB}
      <span class="run-meta">${fmtDuration(r.duration_s)} · ${fmtNum(r.rows_written)} rows</span>
      ${r.failure_cause ? `<pre class="run-cause">${esc(r.failure_cause)}</pre>` : ''}
    </div>`;
  }).join('');

  const more = data.length > 40 ? `<p class="muted" style="margin-top:8px">Showing 40 of ${data.length} runs</p>` : '';
  html('runs-feed', rows + more);
}

// ── Render: Experiments page ─────────────────────────────────────────────────

function renderExpResults(data) {
  if (!data) return;

  el('deflation-value').textContent = fmtNum(data.deflation_clock);

  const results = data.results || [];
  if (!results.length) {
    html('exp-results', '<span class="muted" style="padding:12px 14px;display:block">No experiments found</span>');
    return;
  }

  const items = results.slice(0, 100).map(r => {
    const isSelected = r.experiment_id === state.selectedExpId;
    const tier = r.promotion_tier;
    const tierB = tier === 'live' ? badge('LIVE', 'live')
                : tier ? badge(tier.toUpperCase(), 'shadow') : '';
    const regime = r.evaluation_regime === 'wfo-oos' ? badge('WFO', 'wfo') : '';
    const staleB = r.stale ? badge('STALE', 'warn') : '';

    return `<div class="exp-item${isSelected ? ' is-selected' : ''}"
        data-expid="${esc(r.experiment_id)}" tabindex="0" role="option"
        aria-selected="${isSelected}">
      <div class="exp-id">${esc(r.experiment_id)}</div>
      <div class="exp-badges">${tierB}${regime}${staleB}
        <span class="exp-policy">${esc(r.policy_type || '')} ${esc(r.feature_set || '')}</span>
      </div>
      <div class="exp-nums">
        S ${fmtFloat(r.sharpe, 2)}
        · C ${fmtFloat(r.calmar, 2)}
        · DD ${fmtPct(r.max_drawdown)}
        · R ${fmtPct(r.total_return)}
      </div>
    </div>`;
  }).join('');

  html('exp-results', items);

  el('exp-results').querySelectorAll('.exp-item').forEach(item => {
    item.addEventListener('click', () => loadExpDetail(item.dataset.expid));
    item.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        loadExpDetail(item.dataset.expid);
      }
    });
  });
}

async function loadExpDetail(id) {
  state.selectedExpId = id;
  if (cache.experiments) renderExpResults(cache.experiments);

  el('exp-inspector').classList.remove('hidden');
  html('exp-inspector-content', '<p class="loading">Loading…</p>');

  try {
    const data = await apiFetch('/api/experiments/' + encodeURIComponent(id));
    renderExpInspector(data);
  } catch (e) {
    html('exp-inspector-content', `<p class="err-msg">Error: ${esc(e.message)}</p>`);
  }
}

function renderExpInspector(data) {
  const perf  = data.performance || {};
  const trdg  = data.trading     || {};
  const spec  = data.spec        || {};
  const idx   = data.index       || {};
  const qual  = data.qualification_report || {};
  const cfg   = data.config      || spec;

  const tier = idx.promotion_tier;
  const tierB = tier === 'live'   ? badge('LIVE',   'live')
              : tier === 'shadow' ? badge('SHADOW', 'shadow')
              : tier              ? badge(tier.toUpperCase(), 'info') : '';

  const metrics = [
    { lbl: 'Sharpe',  val: perf.sharpe  ?? idx.sharpe,  sign: perf.sharpe  ?? idx.sharpe },
    { lbl: 'Calmar',  val: perf.calmar  ?? idx.calmar,  sign: perf.calmar  ?? idx.calmar },
    { lbl: 'Max DD',  val: perf.max_drawdown ?? idx.max_drawdown, pct: true, sign: -(perf.max_drawdown ?? 0) },
    { lbl: 'Return',  val: perf.total_return ?? idx.total_return, pct: true, sign: perf.total_return ?? idx.total_return },
  ];
  if (perf.sortino  != null) metrics.push({ lbl: 'Sortino', val: perf.sortino,  sign: perf.sortino });
  if (trdg.win_rate != null) metrics.push({ lbl: 'Win Rate', val: trdg.win_rate, pct: true, sign: trdg.win_rate - 0.5 });
  if (trdg.total_trades != null) metrics.push({ lbl: 'Trades', val: trdg.total_trades, raw: true });

  const metricTiles = metrics.map(m => {
    const formatted = m.raw ? String(m.val) : m.pct ? fmtPct(m.val) : fmtFloat(m.val, 3);
    const cls = m.sign == null ? '' : m.sign > 0 ? ' pos' : m.sign < 0 ? ' neg' : '';
    return `<div class="metric-tile">
      <div class="metric-label">${esc(m.lbl)}</div>
      <div class="metric-val${cls}">${formatted}</div>
    </div>`;
  }).join('');

  // Lineage
  const lineage = [];
  if (spec.universe || idx.universe)       lineage.push(['Universe',    spec.universe    || idx.universe]);
  if (spec.feature_set || idx.feature_set) lineage.push(['Feature Set', spec.feature_set || idx.feature_set]);
  if (spec.policy_type || idx.policy_type) lineage.push(['Policy',      spec.policy_type || idx.policy_type]);
  if (Array.isArray(data.inputs_used) && data.inputs_used.length)
    lineage.push(['Inputs', data.inputs_used.join(', ')]);
  if (idx.created_at) lineage.push(['Created', fmtDate(idx.created_at)]);
  if (idx.qualification_status) lineage.push(['Qual Status', idx.qualification_status]);

  const kvRows = (rows) => rows.map(([k, v]) =>
    `<tr><td>${esc(k)}</td><td>${esc(String(v))}</td></tr>`).join('');

  // Train/test/purge
  const splitRows = [];
  if (cfg.train_start || cfg.train_end)
    splitRows.push(['Train', `${fmtDate(cfg.train_start)} → ${fmtDate(cfg.train_end)}`]);
  if (cfg.test_start || cfg.test_end)
    splitRows.push(['Test',  `${fmtDate(cfg.test_start)} → ${fmtDate(cfg.test_end)}`]);
  if (cfg.purge_period != null)
    splitRows.push(['Purge', cfg.purge_period + ' days']);
  if (cfg.n_splits != null)
    splitRows.push(['Splits', String(cfg.n_splits)]);

  // Qual report (top 8 keys)
  const qualEntries = Object.entries(qual).slice(0, 8);

  html('exp-inspector-content', `
    <div class="insp-header">
      <div class="insp-id">${esc(data.experiment_id)}</div>
      ${tierB}
    </div>

    <div class="metric-grid">${metricTiles}</div>

    ${renderEquityCurve(data.equity_curve)}

    ${lineage.length ? `
    <div class="insp-section">
      <div class="insp-section-title">Lineage</div>
      <table class="kv-table"><tbody>${kvRows(lineage)}</tbody></table>
    </div>` : ''}

    ${splitRows.length ? `
    <div class="insp-section">
      <div class="insp-section-title">Train / Test / Purge</div>
      <table class="kv-table"><tbody>${kvRows(splitRows)}</tbody></table>
    </div>` : ''}

    ${qualEntries.length ? `
    <div class="insp-section">
      <div class="insp-section-title">Qualification Report</div>
      <table class="kv-table"><tbody>
        ${qualEntries.map(([k, v]) =>
          `<tr><td>${esc(k)}</td><td>${esc(JSON.stringify(v))}</td></tr>`).join('')}
      </tbody></table>
    </div>` : ''}
  `);
}

// ── Render: Trading page ──────────────────────────────────────────────────────

function renderTrading(data) {
  if (!data) return;

  // Live experiments
  const live = (data.promotions || []).filter(p => p.tier === 'live');
  html('live-experiments', live.length ? `
    <div class="tbl-wrap"><table>
      <thead><tr>
        <th>Experiment</th><th>Sharpe</th><th>Max DD</th><th>Allocation</th><th>Promoted By</th>
      </tr></thead>
      <tbody>
        ${live.map(p => `<tr>
          <td class="mono" style="font-size:11px;word-break:break-all">${esc(p.experiment_id)}</td>
          <td class="mono">${fmtFloat(p.sharpe, 2)}</td>
          <td class="mono">${fmtPct(p.max_drawdown)}</td>
          <td class="mono">${fmtPct(p.allocation)}</td>
          <td class="mono">${esc(p.promoted_by || '—')}</td>
        </tr>`).join('')}
      </tbody>
    </table></div>` : '<span class="muted">No live experiments</span>');

  // Portfolio state
  const pf = data.portfolio_state || [];
  html('portfolio-state', pf.length ? `
    <div class="pf-grid">
      ${pf.map(p => {
        const pnlCls = (p.daily_pnl || 0) >= 0 ? 'pos' : 'neg';
        const ddCls  = (p.drawdown  || 0) < -0.05 ? 'neg' : '';
        const modeB  = p.mode === 'live' ? badge('LIVE', 'live')
                     : p.mode === 'shadow' ? badge('SHADOW', 'shadow')
                     : p.mode ? badge(p.mode.toUpperCase(), 'info') : '';
        return `<div class="pf-card">
          <div class="pf-card-head">
            <div class="pf-exp-id">${esc(p.experiment_id)}</div>
            ${modeB}
          </div>
          <div class="pf-metrics">
            <span class="pf-lbl">NAV</span><span class="pf-num">${fmtMoney(p.nav)}</span>
            <span class="pf-lbl">Cash</span><span class="pf-num">${fmtMoney(p.cash)}</span>
            <span class="pf-lbl">Positions</span><span class="pf-num">${p.num_positions ?? '—'}</span>
            <span class="pf-lbl">Daily P&amp;L</span><span class="pf-num ${pnlCls}">${fmtMoney(p.daily_pnl)}</span>
            <span class="pf-lbl">Drawdown</span><span class="pf-num ${ddCls}">${fmtPct(p.drawdown)}</span>
            <span class="pf-lbl">Leverage</span><span class="pf-num">${fmtFloat(p.leverage, 2)}×</span>
          </div>
        </div>`;
      }).join('')}
    </div>` : '<span class="muted">No portfolio data</span>');

  // Heartbeat
  const hb = data.heartbeat || [];
  html('heartbeat-rows', hb.length ? `
    <div class="tbl-wrap"><table>
      <thead><tr>
        <th>Experiment</th><th>Mode</th><th>Status</th><th>Age</th><th>Iteration</th><th>Pending</th>
      </tr></thead>
      <tbody>
        ${hb.map(h => {
          const hs = h.heartbeat_status || 'crit';
          const modeB = h.mode === 'live' ? badge('LIVE', 'live')
                      : h.mode ? badge(h.mode.toUpperCase(), 'info') : '';
          return `<tr>
            <td class="mono" style="font-size:11px;word-break:break-all">${esc(h.experiment_id)}</td>
            <td>${modeB}</td>
            <td>${dot(hs)} ${badge(hs.toUpperCase(), hs === 'ok' ? 'ok' : 'crit')}</td>
            <td class="mono">${fmtDuration(h.age_s)}</td>
            <td class="mono">${h.loop_iteration ?? '—'}</td>
            <td class="mono">${h.orders_pending ?? '—'}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table></div>` : '<span class="muted">No heartbeat data</span>');

  // Open orders
  const oo = data.open_orders || [];
  html('open-orders-rows', oo.length ? `
    <div class="tbl-wrap"><table>
      <thead><tr><th>Experiment</th><th>Mode</th><th>Open Count</th></tr></thead>
      <tbody>
        ${oo.map(o => {
          const modeB = o.mode === 'live' ? badge('LIVE', 'live')
                      : o.mode ? badge(o.mode.toUpperCase(), 'info') : '';
          return `<tr>
            <td class="mono" style="font-size:11px;word-break:break-all">${esc(o.experiment_id)}</td>
            <td>${modeB}</td>
            <td class="mono">${o.open_count ?? 0}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table></div>` : '<span class="muted">No open orders</span>');

  // Risk decisions
  const rd = data.risk_decisions || [];
  html('risk-decisions-rows', rd.length ? rd.slice(0, 20).map(r => `
    <div class="risk-item">
      <span class="mono" style="font-size:11px">${esc(r.experiment_id)}</span>
      ${badge(r.decision || '—', 'warn')}
      <span class="muted"> · rule: ${esc(r.rule_id || '—')} · ${fmtDate(r.timestamp)}</span><br>
      <span class="muted">action: ${esc(r.action_taken || '—')} — size: ${fmtNum(r.original_size)} → ${fmtNum(r.reduced_size)}</span>
    </div>`).join('') : '<span class="muted">No risk decisions in the last 24 h</span>');
}

// ── API fetch ────────────────────────────────────────────────────────────────

async function apiFetch(path) {
  const resp = await fetch(path);
  if (!resp.ok) throw new Error(resp.status + ' ' + resp.statusText);
  return resp.json();
}

// ── Data loaders ─────────────────────────────────────────────────────────────

async function loadSystem() {
  try {
    const [health, coverage, storage, runs] = await Promise.all([
      apiFetch('/api/health'),
      apiFetch('/api/coverage'),
      apiFetch('/api/storage'),
      apiFetch('/api/runs'),
    ]);
    cache.health   = health;
    cache.coverage = coverage;
    cache.storage  = storage;
    cache.runs     = runs;
    renderHealth(health);
    renderCoverage(coverage);
    renderStorage(storage);
    renderRuns(runs);
  } catch (e) {
    console.error('[YATS] system load error', e);
  }
}

async function loadExperiments() {
  try {
    const data = await apiFetch('/api/experiments?q=' + encodeURIComponent(state.expQuery));
    cache.experiments = data;
    renderExpResults(data);
  } catch (e) {
    html('exp-results', `<span class="err-msg" style="padding:12px 14px;display:block">Error: ${esc(e.message)}</span>`);
  }
}

async function loadTrading() {
  try {
    const data = await apiFetch('/api/trading');
    cache.trading = data;
    renderTrading(data);
  } catch (e) {
    console.error('[YATS] trading load error', e);
  }
}

async function pollPage(page) {
  if (page === 'system')      await loadSystem();
  else if (page === 'experiments') await loadExperiments();
  else if (page === 'trading')     await loadTrading();
  _lastPollMs = Date.now();
  const ind = el('poll-indicator');
  if (ind) ind.textContent = 'Live';
}

// ── Polling ──────────────────────────────────────────────────────────────────

let _lastPollMs = 0;
let _pollTimer  = null;

function schedulePoll() {
  if (_pollTimer) clearTimeout(_pollTimer);
  _pollTimer = setTimeout(async () => {
    if (document.visibilityState !== 'hidden') await pollPage(state.page);
    schedulePoll();
  }, 30000);
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible' && Date.now() - _lastPollMs > 30000) {
    pollPage(state.page);
  }
});

// ── Navigation ────────────────────────────────────────────────────────────────

function switchPage(page) {
  state.page = page;

  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => {
    const active = b.dataset.page === page;
    b.classList.toggle('active', active);
    b.setAttribute('aria-selected', active);
  });

  const pageEl = el('page-' + page);
  if (pageEl) pageEl.classList.add('active');

  pollPage(page);
}

// ── Theme ────────────────────────────────────────────────────────────────────

function initTheme() {
  const btn = el('theme-btn');
  if (!btn) return;

  function isDark() {
    const th = document.documentElement.getAttribute('data-theme');
    if (th === 'dark') return true;
    if (th === 'light') return false;
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  }

  function syncBtn() {
    btn.textContent = isDark() ? '◐' : '◑';
    btn.setAttribute('aria-label', isDark() ? 'Switch to light theme' : 'Switch to dark theme');
  }

  btn.addEventListener('click', () => {
    document.documentElement.setAttribute('data-theme', isDark() ? 'light' : 'dark');
    syncBtn();
  });

  syncBtn();

  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', syncBtn);
}

// ── Coverage filter ──────────────────────────────────────────────────────────

function initCoverageFilter() {
  const sel = el('equity-filter');
  if (!sel) return;
  sel.addEventListener('change', () => {
    state.equityFilter = sel.value;
    if (cache.coverage) renderCoverage(cache.coverage);
  });
}

// ── Experiment search (debounced) ─────────────────────────────────────────────

let _searchTimer = null;

function initExpSearch() {
  const input = el('exp-search');
  if (!input) return;
  input.addEventListener('input', () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(() => {
      state.expQuery = input.value.trim();
      loadExperiments();
    }, 350);
  });
}

// ── Tab listeners ─────────────────────────────────────────────────────────────

function initTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchPage(btn.dataset.page));
  });
}

// ── Init ──────────────────────────────────────────────────────────────────────

function init() {
  initTheme();
  initTabs();
  initCoverageFilter();
  initExpSearch();

  pollPage(state.page);
  schedulePoll();
}

document.addEventListener('DOMContentLoaded', init);

// ── Exports for unit tests (if loaded as module) ──────────────────────────────
if (typeof module !== 'undefined') {
  module.exports = {
    esc, fmtNum, fmtBytes, fmtDate, fmtDuration, fmtPct, fmtFloat, fmtMoney,
    renderEquityCurve, _findMaxDDSegment,
  };
}
