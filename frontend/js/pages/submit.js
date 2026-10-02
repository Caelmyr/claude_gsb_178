/* 作业提交 Submit */
Components.init('submit');
const C = Components;

let SAMPLES = [];
let PREVIEW = null;       // last preview session
let PREVIEW_KEY = null;   // fingerprint of options the preview was run with

async function init() {
  const funcs = await API.get('/api/functions');
  SAMPLES = await API.get('/api/samples');

  fillSelect('mapper', funcs.mappers);
  fillSelect('reducer', funcs.reducers);

  const preset = document.getElementById('preset');
  preset.innerHTML = SAMPLES.map(s => `<option value="${s.name}">${C.esc(s.name)}</option>`).join('');
  preset.addEventListener('change', () => {
    const s = SAMPLES.find(x => x.name === preset.value);
    if (s) fillFromSample(s);
  });

  document.getElementById('form').addEventListener('submit', onSubmit);

  document.getElementById('func-list').innerHTML =
    '<h3>Map</h3>' + funcs.mappers.map(f =>
      `<div class="small" style="padding:2px 0"><span class="mono">${C.esc(f.name)}</span> — ${C.esc(f.description)}</div>`).join('') +
    '<h3 class="mt">Reduce</h3>' + funcs.reducers.map(f =>
      `<div class="small" style="padding:2px 0"><span class="mono">${C.esc(f.name)}</span> — ${C.esc(f.description)}</div>`).join('');

  initPreview();
  loadRecent();
}

function fillSelect(id, items) {
  document.getElementById(id).innerHTML = items
    .map(f => `<option value="${f.name}">${C.esc(f.name)}</option>`).join('');
}

function fillFromSample(s) {
  document.getElementById('name').value = s.name;
  document.getElementById('mapper').value = s.mapper;
  document.getElementById('reducer').value = s.reducer;
  document.getElementById('num_map_tasks').value = s.num_map_tasks;
  document.getElementById('num_reduce_tasks').value = s.num_reduce_tasks;
  document.getElementById('input_rows').value = s.input_rows;
  document.getElementById('simulate_failure').checked = false;
  // Generated-input preview follows the preset too.
  document.getElementById('pv-kind').value = (s.mapper === 'kv_mapper') ? 'kv' : 'wordcount';
}

// =====================================================================
// Pre-submission sampling preview
// =====================================================================
const REGION_LABELS = {
  head: '头部 Head', middle: '中部 Middle', tail: '尾部 Tail', coverage: '补覆盖 Coverage',
};
const LEVEL_CLASS = { error: 'bad', warning: 'warn', info: 'aqua' };

function initPreview() {
  const source = document.getElementById('pv-source');
  const toggle = () => {
    const v = source.value;
    document.getElementById('pv-files-box').style.display = v === 'files' ? '' : 'none';
    document.getElementById('pv-path-box').style.display = v === 'path' ? '' : 'none';
    document.getElementById('pv-paste-box').style.display = v === 'paste' ? '' : 'none';
    document.getElementById('pv-kind-box').style.display = v === 'synthetic' ? '' : 'none';
  };
  source.addEventListener('change', toggle);
  toggle();

  document.getElementById('pv-run').addEventListener('click', runPreview);

  // Any option change after a preview flags staleness (the stored session no
  // longer matches what the user would submit).
  ['pv-source', 'pv-kind', 'pv-num-map', 'pv-path', 'pv-paste', 'pv-delim',
   'pv-header', 'pv-skip-blank', 'pv-files', 'input_rows'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('change', () => { markStale(); updateSubmitHint(); });
  });
}

function previewIsFresh() {
  return PREVIEW && PREVIEW_KEY === JSON.stringify(previewForm());
}

function updateSubmitHint() {
  const hint = document.getElementById('submit-hint');
  if (!hint) return;
  if (PREVIEW && PREVIEW.summary.source !== 'synthetic') {
    if (previewIsFresh()) {
      hint.innerHTML = '将运行抽样所见的 <b>' + C.fmtNum(PREVIEW.summary.total_records) +
        '</b> 条记录 · Will run the sampled data.';
      hint.style.color = 'var(--good)';
    } else {
      hint.textContent = '预览已过期，建议重新抽样 Preview stale — re-sample recommended.';
      hint.style.color = 'var(--serious)';
    }
  } else {
    hint.textContent = '提交后跳转至监控页 After submit, open the Monitor page.';
    hint.style.color = '';
  }
}

function markStale() {
  const flag = document.getElementById('pv-stale');
  if (PREVIEW) flag.style.display = '';
}

function previewForm() {
  return {
    source: document.getElementById('pv-source').value,
    synthetic_kind: document.getElementById('pv-kind').value,
    input_rows: parseInt(document.getElementById('input_rows').value, 10) || 12000,
    num_map_tasks: parseInt(document.getElementById('pv-num-map').value, 10) || 8,
    paths: document.getElementById('pv-path').value.trim(),
    paste_text: document.getElementById('pv-paste').value,
    delimiter: document.getElementById('pv-delim').value,
    has_header: document.getElementById('pv-header').value,
    skip_blank: document.getElementById('pv-skip-blank').checked,
  };
}

async function runPreview() {
  const btn = document.getElementById('pv-run');
  const host = document.getElementById('pv-result');
  const form = previewForm();

  if (form.source === 'path' && !form.paths) { C.toast('请填写服务器路径 Fill in a server path', 'error'); return; }
  if (form.source === 'paste' && !form.paste_text.trim()) { C.toast('请粘贴文本 Paste some text first', 'error'); return; }

  btn.disabled = true;
  host.innerHTML = '<div class="empty">抽样中… Sampling input…</div>';
  try {
    let data;
    if (form.source === 'files') {
      const fileInput = document.getElementById('pv-files');
      if (!fileInput.files.length) { C.toast('请选择文件 Choose at least one file', 'error'); btn.disabled = false; return; }
      const fd = new FormData();
      fd.append('source', 'files');
      fd.append('num_map_tasks', String(form.num_map_tasks));
      fd.append('delimiter', form.delimiter);
      fd.append('has_header', form.has_header);
      fd.append('skip_blank', String(form.skip_blank));
      for (const f of fileInput.files) fd.append('files', f, f.name);
      const resp = await fetch('/api/input/preview', { method: 'POST', body: fd });
      data = await resp.json();
      if (!resp.ok) throw new Error(data.error || ('HTTP ' + resp.status));
    } else {
      data = await API.post('/api/input/preview', form);
    }
    PREVIEW = data;
    PREVIEW_KEY = JSON.stringify(form);
    document.getElementById('pv-stale').style.display = 'none';
    renderPreview(data);
    updateSubmitHint();
  } catch (e) {
    host.innerHTML = `<div class="empty" style="color:var(--critical)">预览失败 Preview failed: ${C.esc(e.message)}</div>`;
    C.toast('抽样失败 ' + e.message, 'error');
  } finally {
    btn.disabled = false;
  }
}

function renderPreview(d) {
  const s = d.summary;
  const host = document.getElementById('pv-result');

  // --- stat tiles ---------------------------------------------------
  const exact = s.line_counts_exact ? '' : ' <span class="muted small">(含估算 est.)</span>';
  const tiles = [
    ['分片文件 Files', C.fmtNum(s.files) + (s.readable_files !== s.files ? ` / ${C.fmtNum(s.readable_files)} 可读` : '')],
    ['总字节 Bytes', C.fmtBytes(s.total_bytes)],
    ['总行数 Lines', C.fmtNum(s.total_lines) + exact],
    ['有效记录 Records', C.fmtNum(s.total_records) + (s.truncated ? ' ⚠' : '')],
    ['空行 Blank lines', C.fmtNum(s.blank_lines)],
    ['Map 分片 Map shards', C.fmtNum(d.shards.length) + ` · ${d.shards.filter(x => x.covered).length}/${d.shards.length} 已覆盖`],
    ['编码 Encoding', C.esc(s.encoding || '-')],
    ['分隔 Delimiter', C.esc(delimLabel(s.delimiter))],
  ].map(([k, v]) => `<div class="stat"><div class="label">${k}</div><div class="value" style="font-size:20px">${v}</div></div>`).join('');

  // --- warnings -----------------------------------------------------
  const warnHtml = d.warnings.length
    ? `<div class="mt">${d.warnings.map(w => `
        <div class="log-line lvl-${w.level === 'error' ? 'ERROR' : w.level === 'warning' ? 'WARN' : 'DEBUG'}">
          <span class="lvl">${w.level === 'error' ? '错误' : w.level === 'warning' ? '警告' : '提示'}</span>
          <span class="msg">${C.esc(w.message_zh)}<br><span class="muted">${C.esc(w.message_en)}</span></span>
        </div>`).join('')}</div>`
    : '<div class="small muted mt">未发现明显异常 No anomalies detected.</div>';

  // --- file (shard) table ------------------------------------------
  const fileTable = C.table([
    { key: 'name', label: '文件/分片 File (shard)' },
    { key: 'size', label: '大小 Size', num: true, render: r => C.fmtBytes(r.size) },
    { key: 'sampled', label: '抽样 Samples', num: true, render: r => `<b>${C.fmtNum(r.sampled || 0)}</b>` },
    { key: 'encoding', label: '编码 Enc.', render: r => C.esc(r.encoding) },
    { key: 'delimiter', label: '分隔 Delim', render: r => C.esc(r.delimiter_label) },
    { key: 'columns', label: '列 Cols', num: true },
    { key: 'total_lines', label: '行数 Lines', num: true,
      render: r => `${C.fmtNum(r.total_lines)}${r.line_count_exact ? '' : '~'}` },
    { key: 'blank', label: '空行 Blank', num: true, render: r => C.fmtNum(r.blank_lines) },
    { key: 'state', label: '状态 State', render: r => r.readable
        ? (r.has_header ? '<span class="badge muted">表头 header</span>' : '<span class="badge good">ok</span>')
        : '<span class="badge bad">不可读 unreadable</span>' },
  ], d.files);

  // --- shard coverage ----------------------------------------------
  const maxCount = Math.max(1, ...d.shards.map(x => x.count));
  const coverageHtml = d.shards.map(sh => {
    const pct = Math.round(100 * sh.count / maxCount);
    const cls = sh.covered ? 'good' : 'crit';
    return `<div title="分片 ${sh.index}: 记录 ${sh.start_record}…${sh.start_record + sh.count - 1}, 采样 ${sh.sampled} 条"
              style="min-width:70px;flex:1 1 70px">
        <div class="small muted">m-${String(sh.index).padStart(4, '0')}</div>
        <div class="progress" style="height:14px"><div class="fill ${cls}" style="width:${Math.max(4, pct)}%"></div></div>
        <div class="small tabular">${C.fmtNum(sh.count)} · ${sh.sampled}样</div>
      </div>`;
  }).join('');

  // --- samples ------------------------------------------------------
  const sampleRows = d.samples.map(sm => ({
    region: REGION_LABELS[sm.region] || sm.region,
    where: `<span class="mono small">${C.esc(sm.file)}:${C.fmtNum(sm.raw_line)}${sm.approximate_line ? '~' : ''}</span>`,
    shard: sm.shard == null ? '<span class="muted">-</span>' : `m-${String(sm.shard).padStart(4, '0')}`,
    gidx: sm.global_index == null ? '<span class="muted">跳过 skipped</span>' : C.fmtNum(sm.global_index),
    text: renderSampleText(sm),
  }));
  const sampleTable = C.table([
    { key: 'region', label: '位置 Region', width: '130px' },
    { key: 'where', label: '来源位置 Location', width: '210px' },
    { key: 'shard', label: '分片 Shard', width: '90px' },
    { key: 'gidx', label: '记录# Rec#', width: '90px', num: true },
    { key: 'text', label: '样本内容 Sample' },
  ], sampleRows);

  const covBadge = d.coverage_complete
    ? '<span class="badge good">全分片已覆盖 all shards covered</span>'
    : '<span class="badge warn">部分分片未抽样 some shards not sampled</span>';

  host.innerHTML = `
    <div class="stat-tiles">${tiles}</div>
    <h3 class="mt">异常与提示 <span class="sub">Anomalies &amp; hints</span></h3>
    ${warnHtml}
    <h3 class="mt">输入分片文件 <span class="sub">Input shards (${d.files.length})</span></h3>
    ${fileTable}
    <h3 class="mt">抽样位置覆盖的 Map 分片 ${covBadge} <span class="sub">Sampling coverage</span></h3>
    <div class="flex wrap" style="gap:10px;align-items:flex-end">${coverageHtml}</div>
    <p class="small muted">每个条带宽度对应该分片的记录数；绿色表示至少有一条样本落在该分片（“补覆盖”行），红色为空分片或未覆盖。
      Bar width ∝ shard size; green = at least one sampled record, red = empty/unsampled shard.</p>
    <h3 class="mt">抽样到的记录 <span class="sub">Sampled records (${d.samples.length})</span></h3>
    ${sampleTable}
    <div class="mt small">
      <span class="badge good">预览已保存 Preview stored</span>
      <span class="mono muted">${C.esc(d.preview_id)}</span>
      — 直接“提交作业”将运行这份抽样所见的数据。Submitting now runs exactly these records.
    </div>`;
}

function delimLabel(d) {
  return { ',': '逗号 ,', ';': '分号 ;', '\t': 'Tab', '|': '竖线 |',
           ' ': '空白 Whitespace', jsonl: 'JSONL', none: '无 None' }[d] || d || '-';
}

function renderSampleText(sm) {
  if (sm.blank) return '<span class="muted">（空行 blank line）</span>';
  let inner;
  if (sm.columns && sm.columns.length > 1) {
    inner = sm.columns.map(c =>
      `<span class="badge muted" style="margin-right:4px">${C.esc(c)}</span>`).join('');
  } else {
    inner = C.esc(sm.text);
  }
  return `<span class="mono small" style="white-space:pre-wrap;word-break:break-all">${inner}</span>`
    + (sm.text_truncated ? ' <span class="muted">…</span>' : '');
}

async function onSubmit(ev) {
  ev.preventDefault();
  const body = {
    name: document.getElementById('name').value.trim(),
    mapper: document.getElementById('mapper').value,
    reducer: document.getElementById('reducer').value,
    num_map_tasks: parseInt(document.getElementById('num_map_tasks').value, 10),
    num_reduce_tasks: parseInt(document.getElementById('num_reduce_tasks').value, 10),
    input_rows: parseInt(document.getElementById('input_rows').value, 10),
    params: {},
  };
  if (document.getElementById('simulate_failure').checked) body.params.simulate_failure = true;

  // Bind the previewed data when the user is looking at a fresh preview of a
  // real (non-generated) source; warn instead of silently running other data.
  if (previewIsFresh() && PREVIEW.summary.source !== 'synthetic') {
    body.input_preview_id = PREVIEW.preview_id;
    body.input_rows = PREVIEW.summary.total_records;
  } else if (PREVIEW && PREVIEW.summary.source !== 'synthetic') {
    if (!confirm('抽样预览已过期（选项已修改）。\nPreview is stale (options changed).\n仍按当前表单提交？Submit without re-sampling?')) {
      return;
    }
  }

  const btn = ev.target.querySelector('button[type=submit]');
  btn.disabled = true;
  try {
    const job = await API.post('/api/jobs', body);
    C.toast('作业已提交 Job submitted: ' + job.job_id, 'ok');
    setTimeout(() => location.href = 'monitor.html', 600);
  } catch (e) {
    C.toast('提交失败 ' + e.message, 'error');
    btn.disabled = false;
  }
}

async function loadRecent() {
  const d = await API.get('/api/jobs');
  const jobs = d.jobs || [];
  document.getElementById('recent').innerHTML = jobs.length
    ? C.table([
        { key: 'name', label: '作业 Job' },
        { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
        { key: 'mapper', label: 'Mapper' },
        { key: 'reducer', label: 'Reducer' },
        { key: 'created_ms', label: '时间 Time', render: r => C.fmtTime(r.created_ms) },
        { key: 'link', label: '', render: r => `<a href="monitor.html">监控→</a>` },
      ], jobs)
    : C.empty();
}

init();
C.poll(loadRecent, 4000).start();
