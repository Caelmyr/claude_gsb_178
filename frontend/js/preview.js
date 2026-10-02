/* 数据抽样预览 Input preview — read-only sampling before submitting a job. */
const InputPreview = (() => {
  const SEV_CLASS = { error: 'bad', warn: 'warn', info: 'aqua' };
  const SEV_LABEL = { error: '异常 Error', warn: '注意 Warning', info: '提示 Info' };
  const REGION_LABEL = { head: '头部 head', middle: '中部 mid', tail: '尾部 tail', anchor: '分片边界 split' };

  function params() {
    return {
      sample_size: parseInt(document.getElementById('pv_samples').value, 10) || 20,
      num_map_tasks: parseInt(document.getElementById('num_map_tasks').value, 10) || 8,
      delimiter: document.getElementById('pv_delim').value || '',
      has_header: document.getElementById('pv_header').value,
    };
  }

  function setBusy(on) {
    const btn = document.getElementById('pv_run');
    if (btn) btn.disabled = on;
    const out = document.getElementById('pv_result');
    if (out) out.classList.toggle('pv-loading', on);
  }

  async function runPath() {
    const path = document.getElementById('pv_path').value.trim();
    if (!path) { C.toast('请填写服务器路径 Enter a server path', 'error'); return; }
    setBusy(true);
    try {
      const r = await API.post('/api/preview/path', { path, ...params() });
      render(r);
    } catch (e) { renderError(e.message); } finally { setBusy(false); }
  }

  async function runUpload() {
    const files = document.getElementById('pv_files').files;
    if (!files || !files.length) { C.toast('请选择文件 Choose file(s)', 'error'); return; }
    const fd = new FormData();
    Object.entries(params()).forEach(([k, v]) => fd.append(k, v == null ? '' : v));
    Array.from(files).forEach(f => fd.append('files', f));
    setBusy(true);
    try {
      const r = await API.post('/api/preview/upload', fd);
      render(r);
    } catch (e) { renderError(e.message); } finally { setBusy(false); }
  }

  async function runText() {
    const text = document.getElementById('pv_text').value;
    if (!text.trim()) { C.toast('请粘贴数据 Paste some data first', 'error'); return; }
    setBusy(true);
    try {
      const r = await API.post('/api/preview/text', { text, name: 'pasted-input.txt', ...params() });
      render(r);
    } catch (e) { renderError(e.message); } finally { setBusy(false); }
  }

  function runCurrent() {
    const mode = document.querySelector('input[name=pv_mode]:checked').value;
    if (mode === 'path') return runPath();
    if (mode === 'upload') return runUpload();
    return runText();
  }

  function renderError(msg) {
    document.getElementById('pv_result').innerHTML =
      `<div class="pv-errors"><span class="badge bad">${C.esc(msg)}</span></div>`;
  }

  // ------------------------------------------------------------------
  function renderWarnings(warnings) {
    if (!warnings || !warnings.length) {
      return '<div class="pv-okline"><span class="badge good">未发现明显异常 No obvious issues</span></div>';
    }
    return '<div class="pv-warnings">' + warnings.map(w => {
      const cls = SEV_CLASS[w.severity] || 'muted';
      return `<div class="pv-warn pv-${w.severity}">
        <span class="badge ${cls}">${C.esc(SEV_LABEL[w.severity] || w.severity)}</span>
        <span>${C.esc(w.message)}</span></div>`;
    }).join('') + '</div>';
  }

  function renderSummary(r) {
    const s = r.summary, p = r.params;
    const chips = [
      ['文件 files', C.fmtNum(s.files_total)],
      ['大小 size', C.fmtBytes(s.bytes_total)],
      ['行数 lines', C.fmtNum(s.lines_total) + (s.lines_exact ? '' : ' ~')],
      ['空行 blank', C.fmtNum(s.blank_lines_total)],
      ['主流列数 columns', s.columns_dominant == null ? '-' : s.columns_dominant],
      ['分隔符 delimiter', p.delimiter_label || '纯文本 plain'],
      ['表头 header', p.has_header == null ? '未知 ?' : (p.has_header ? '有 yes' : '无 no')],
      ['编码 encoding', (s.encodings || []).join(', ') || '-'],
    ];
    return '<div class="pv-chips">' + chips.map(([k, v]) =>
      `<div class="pv-chip"><span class="muted small">${k}</span><span class="bold">${C.esc(v)}</span></div>`
    ).join('') + '</div>';
  }

  function coverageBar(label, pct, ok) {
    const fillCls = pct >= 99 ? 'good' : (pct >= 50 ? '' : 'warn');
    return `<div class="pv-cov"><span class="small muted">${label}</span>
      <div class="progress"><div class="fill ${fillCls}" style="width:${Math.min(100, pct)}%"></div></div>
      <span class="small tabular bold">${pct}%${ok ? '' : ''}</span></div>`;
  }

  function renderCoverage(r) {
    const c = r.coverage;
    const shards = c.shards || [];
    const strip = shards.map(sh => {
      const hit = sh.samples > 0;
      const title = `分片 shard ${sh.index}: ${sh.samples} 样本, ${sh.files} 文件, ${C.fmtBytes(sh.bytes_span)}`;
      return `<div class="pv-shard ${hit ? 'hit' : 'miss'}" title="${C.esc(title)}">${sh.index}</div>`;
    }).join('');
    return `<div class="pv-covwrap">
      ${coverageBar(`打开文件 files opened ${c.files_opened}/${c.files_total}`, c.files_pct)}
      ${coverageBar(`读取字节 bytes probed ${C.fmtBytes(c.bytes_probed)} / ${C.fmtBytes(c.bytes_total)}`, Math.max(0.01, c.bytes_pct))}
      <div class="pv-cov"><span class="small muted">分片覆盖 shards ${c.shards_with_samples}/${c.shards_total}</span>
        <div class="pv-shards">${strip}</div>
        <span class="small tabular bold">${c.shards_pct}%</span></div>
      <p class="small muted pv-hint">色块表示该输入分片（按 ${r.params.num_map_tasks} 个 map 任务切分）是否抽到样本；数字为分片序号。
      Large files are split by byte range — samples carry a “n/N” split tag. 预览只读取上面比例的字节，不执行作业。</p>
    </div>`;
  }

  function renderSamples(r) {
    const rows = r.samples || [];
    if (!rows.length) return C.empty('没有可展示的样本行 No sample rows (检查输入是否为空)');
    const body = rows.map((s, i) => {
      const loc = s.lineno ? `L${s.lineno}` : `@${C.fmtNum(s.offset)}`;
      const hint = s.split_hint ? ` <span class="badge aqua small-badge">${C.esc(s.split_hint)}</span>` : '';
      const flags = [
        s.is_header ? '<span class="badge muted small-badge">表头 header</span>' : '',
        s.blank ? '<span class="badge warn small-badge">空行 blank</span>' : '',
        s.truncated ? '<span class="badge muted small-badge">截断 …</span>' : '',
      ].join(' ');
      let content;
      if (s.cells && s.cells.length) {
        content = '<span class="pv-cells">' + s.cells.map(c =>
          `<span class="pv-cell">${C.esc(c)}</span>`).join('') + '</span>';
      } else {
        content = `<span class="mono">${C.esc(s.raw) || '∅'}</span>`;
      }
      const colBadge = s.columns != null
        ? `<span class="badge ${r.summary.columns_dominant === s.columns ? 'good' : 'warn'} small-badge">${s.columns}列</span>`
        : '';
      return `<tr>
        <td class="num tabular muted">${i + 1}</td>
        <td><span class="badge run small-badge">sh${s.shard_index}</span>${hint}</td>
        <td><span class="small muted">${C.esc(REGION_LABEL[s.region] || s.region)}</span></td>
        <td class="small"><span class="mono">${C.esc(s.file)}</span><div class="small muted tabular">${loc}</div></td>
        <td>${colBadge}</td>
        <td>${content} ${flags}</td>
      </tr>`;
    }).join('');
    return `<div class="table-wrap"><table class="table">
      <thead><tr><th>#</th><th>分片 shard</th><th>位置 region</th><th>来源 source</th><th>列 cols</th><th>样本记录 record</th></tr></thead>
      <tbody>${body}</tbody></table></div>`;
  }

  function renderFiles(r) {
    const files = r.files || [];
    if (!files.length) return '';
    const rows = files.map(f => {
      const hist = Object.entries(f.columns_histogram || {}).map(([k, v]) => `${k}×${v}`).join(' ');
      const delimLabel = f.delimiter === '\t' ? 'Tab' : (f.delimiter === 'ws' ? 'WS' : (f.delimiter || '-'));
      const status = f.unreadable ? '<span class="badge bad">不可读</span>'
        : f.binary ? '<span class="badge bad">二进制</span>'
        : f.empty ? '<span class="badge warn">空</span>'
        : f.sampled ? '<span class="badge good">已抽样</span>'
        : '<span class="badge muted">仅统计</span>';
      return `<tr>
        <td class="small mono">${C.esc(f.name)}</td>
        <td class="num tabular">${C.fmtBytes(f.bytes)}</td>
        <td class="num tabular">${C.fmtNum(f.lines)}${f.lines_exact ? '' : '~'}</td>
        <td class="small">${C.esc(f.encoding)}</td>
        <td class="small">${C.esc(delimLabel)}</td>
        <td class="small">${f.columns_dominant == null ? '-' : f.columns_dominant} <span class="muted">${C.esc(hist)}</span></td>
        <td class="small">${C.esc(f.shard_index)}</td>
        <td>${status}</td>
      </tr>`;
    }).join('');
    const more = r.summary.files_truncated
      ? `<p class="small muted">仅列出前 ${r.summary.files_listed} 个文件；统计仍覆盖全部 ${r.summary.files_total} 个。</p>` : '';
    return `<details class="pv-files"><summary class="small bold">逐文件明细 Files (${files.length}${r.summary.files_truncated ? ` / ${r.summary.files_total}` : ''})</summary>
      <div class="table-wrap mt"><table class="table">
      <thead><tr><th>文件 file</th><th>大小</th><th>行数</th><th>编码</th><th>分隔符</th><th>列数分布</th><th>分片</th><th>状态</th></tr></thead>
      <tbody>${rows}</tbody></table></div>${more}</details>`;
  }

  function render(r) {
    const meta = r.source || {};
    const srcLabel = meta.mode === 'path' ? `路径 path: ${meta.path}`
      : meta.mode === 'upload' ? `上传 upload: ${(meta.names || []).join(', ')}`
      : `粘贴 paste: ${meta.name || ''}`;
    document.getElementById('pv_result').innerHTML =
      `<div class="pv-head flex between">
         <span class="small muted mono">${C.esc(srcLabel)}</span>
         <span class="small muted">耗时 ${r.elapsed_ms} ms · 只读 read-only</span>
       </div>
       ${renderWarnings(r.warnings)}
       ${renderSummary(r)}
       ${renderCoverage(r)}
       <h3 class="mt">样本记录 <span class="sub">Sample records (${(r.samples || []).length})</span></h3>
       ${renderSamples(r)}
       ${renderFiles(r)}`;
  }

  function mount() {
    document.getElementById('pv_run').addEventListener('click', runCurrent);
    document.querySelectorAll('input[name=pv_mode]').forEach(radio => {
      radio.addEventListener('change', () => {
        const mode = radio.value;
        document.getElementById('pv_mode_path').style.display = mode === 'path' ? '' : 'none';
        document.getElementById('pv_mode_upload').style.display = mode === 'upload' ? '' : 'none';
        document.getElementById('pv_mode_text').style.display = mode === 'text' ? '' : 'none';
      });
    });
    // map-tasks edits refresh nothing automatically; the next preview reads it.
  }

  return { mount };
})();
