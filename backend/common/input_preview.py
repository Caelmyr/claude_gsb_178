"""Pre-submission input sampling and data-shape preview.

Before a job is submitted the user rarely knows whether the input data actually
matches what the mapper expects: a file may be empty, the shards may be tiny
and numerous, the delimiter may differ between files, there may be a header
row or lots of blank lines, or the bytes may simply not be text.  This module
implements a **preview that never executes the job** — it only inspects bytes:

* **bounded streaming scan** — files are read in fixed-size chunks up to
  ``SCAN_CAP_BYTES``; beyond the cap the line count is extrapolated from byte
  density (and explicitly marked an estimate), so a multi-GB file costs the
  same as a 32 MB one;
* **sparse line index** — the byte offset of every 256th line is remembered, so
  head / middle samples resolve by ``seek`` + a short forward scan instead of
  re-reading the whole file;
* **head / middle / tail sampling** — every shard (input file) is sampled in
  its own right so many tiny files are all represented, not just the first one;
* **shape detection** — BOM/encoding (UTF-8 with CJK fallbacks), CSV dialect
  (``, ; \\t |`` whitespace, JSONL), header rows and blank-line ratios;
* **anomaly hints** — empty files, binary/garbled content, column-count
  mismatch, delimiters drifting between files, over-truncation;
* **shard coverage simulation** — the same even-split rule the
  ``ShardPlanner`` uses is applied to the materialised records, and an extra
  sample is pulled from any map shard the raw scan did not touch, so the
  preview truthfully reports *which positions and which shards were sampled*;
* **session materialisation** — the previewed records are persisted once as
  ``records.jsonl``; submitting the job with ``input_preview_id`` makes the
  planner read exactly those records, so "what you previewed is what runs".

The module is deliberately free of Flask/Worker dependencies so it can be unit
tested directly against files on disk.
"""

from __future__ import annotations

import bisect
import codecs
import csv
import io
import json
import os
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from . import jsonutil
from .ids import new_id
from .jsonutil import now_ms
from .storage import Storage, atomic_write_json, ensure_dir

# ---------------------------------------------------------------------------
# Tunables (all sized so a preview stays cheap regardless of input scale)
# ---------------------------------------------------------------------------
CHUNK_BYTES = 64 * 1024              # streaming read chunk
SCAN_CAP_BYTES = 32 * 1024 * 1024    # per-file bytes fully scanned/line-counted
TAIL_BYTES = 8 * 1024                # window read backwards for tail samples
INDEX_EVERY = 256                    # sparse index: remember offset every N lines
HEAD_LINES = 20                      # lines used for dialect / encoding sniffing
MAX_FILES = 2000                     # shards (files) considered in one preview
MAX_RECORDS = 1_000_000              # materialised records cap per session
MAX_TOTAL_SAMPLES = 60               # total samples returned
SAMPLES_PER_FILE = 5                 # head/head/mid/tail/tail
MAX_SAMPLE_TEXT = 500                # characters of a sample shown in the UI
MAX_UPLOAD_BYTES = 64 * 1024 * 1024  # total size accepted for browser uploads

DELIMITERS = [",", ";", "\t", "|", " "]
DELIMITER_NAMES = {",": "逗号 ,", ";": "分号 ;", "\t": "制表符 Tab", "|": "竖线 |",
                   " ": "空白 Whitespace", "jsonl": "JSONL", "none": "无分隔 None",
                   "auto": "自动 Auto"}


# ---------------------------------------------------------------------------
# Bilingual anomaly messages
# ---------------------------------------------------------------------------
@dataclass
class WarnMsg:
    level: str          # error | warning | info
    code: str
    message_zh: str
    message_en: str
    scope: str = "global"
    file: str = ""


def _w(level: str, code: str, **kw) -> WarnMsg:
    """Build a warning from a message template."""
    zh, en = _MESSAGES[code]
    return WarnMsg(level, code, zh.format(**kw), en.format(**kw),
                   scope=kw.get("scope", "global"), file=kw.get("file", ""))


# code -> (Chinese, English); both accept {file}/{n}/{cols}/{total} format fields
_MESSAGES: dict[str, tuple[str, str]] = {
    "empty_file": (
        "文件 {file} 为空（0 字节或无任何内容行）。",
        "File {file} is empty (0 bytes or no content lines).",
    ),
    "binary_file": (
        "文件 {file} 疑似二进制/包含 NUL 字节，无法当作文本解析。",
        "File {file} looks binary (contains NUL bytes) and cannot be parsed as text.",
    ),
    "unreadable_file": (
        "文件 {file} 无法读取：{detail}。",
        "File {file} could not be read: {detail}.",
    ),
    "encoding_garbled": (
        "文件 {file} 存在编码问题（{encoding} 解码后出现替换字符/乱码），请确认文件编码。",
        "File {file} has encoding problems (replacement characters under {encoding}); check the encoding.",
    ),
    "column_mismatch": (
        "文件 {file} 列数不一致：多数行有 {cols} 列，但有 {n} 行列数不同。",
        "Inconsistent columns in {file}: most rows have {cols} columns but {n} rows differ.",
    ),
    "inconsistent_delimiters": (
        "各文件分隔符不一致（检测到 {detail}），合并解析可能错位。",
        "Delimiters differ between files ({detail}); parsing the union may misalign columns.",
    ),
    "blank_lines_high": (
        "文件 {file} 空行占比约 {pct}%（{n}/{total} 行），这些空行在作业中默认跳过。",
        "About {pct}% of lines in {file} are blank ({n}/{total}); blank lines are skipped by default.",
    ),
    "blank_lines_present": (
        "文件 {file} 含有 {n} 个空行（预览中保留展示，作业默认跳过）。",
        "{file} contains {n} blank lines (shown in the preview; skipped by the job by default).",
    ),
    "header_detected": (
        "文件 {file} 首行疑似表头，作业默认跳过该表头行。",
        "The first line of {file} looks like a header; it is skipped by default.",
    ),
    "header_disagrees": (
        "只有部分文件带表头，混合输入会让列含义错位，建议统一。",
        "Only some files carry a header row; mixing them will misalign columns.",
    ),
    "records_truncated": (
        "记录数超过物化上限（{limit} 条），仅前 {n} 条会进入作业，尾部数据未被覆盖。",
        "Input exceeds the materialisation cap ({limit} records); only the first {n} are used and the tail is not covered.",
    ),
    "line_count_estimated": (
        "文件 {file} 超过扫描上限，总行数与中部抽样位置为按字节密度估算。",
        "{file} exceeds the scan cap; its line count and middle sample position are byte-density estimates.",
    ),
    "no_files": (
        "没有找到任何可读取的输入文件。",
        "No readable input files were found.",
    ),
    "no_records": (
        "所有文件都不包含有效数据行（可能全部为空行）。",
        "No effective data rows were found (files may consist entirely of blank lines).",
    ),
    "jsonl_keys_differ": (
        "JSONL 文件 {file} 各行字段集合不一致（{n} 行与主流字段不同）。",
        "JSONL rows in {file} have inconsistent key sets ({n} rows differ from the majority).",
    ),
}


# ---------------------------------------------------------------------------
# Low-level byte scanning
# ---------------------------------------------------------------------------
def iter_raw_lines(path: str) -> Iterator[tuple[int, int, bytes]]:
    """Yield ``(raw_line_no, byte_offset, raw_bytes)`` for every line.

    Lines are split on ``\\n`` (a trailing ``\\r`` is stripped, so CRLF files
    work).  A leading BOM on the first line is removed.  ``byte_offset`` is the
    physical offset of the line *content* in the file (the BOM is skipped),
    which lets callers ``seek`` straight back to any previously indexed line.
    """
    with open(path, "rb") as f:
        buf = b""
        buf_pos = 0          # physical file offset where ``buf`` starts
        raw = 0
        bom_len = 0
        while True:
            chunk = f.read(CHUNK_BYTES)
            if not chunk:
                break
            buf += chunk
            parts = buf.split(b"\n")
            buf = parts.pop()
            cursor = buf_pos
            for part in parts:
                physical = cursor
                if raw == 0:
                    bom_len = _bom_length(part)
                content_start = bom_len if raw == 0 else 0
                line = part[content_start:]
                if line.endswith(b"\r"):
                    line = line[:-1]
                cursor += len(part) + 1  # +1 for the '\n' consumed by split
                raw += 1
                yield raw, physical + content_start, line
            buf_pos = cursor
        if buf:
            content_start = _bom_length(buf) if raw == 0 else 0
            line = buf[content_start:]
            if line.endswith(b"\r"):
                line = line[:-1]
            raw += 1
            yield raw, buf_pos + content_start, line


def _bom_length(data: bytes) -> int:
    for bom in (codecs.BOM_UTF8, codecs.BOM_UTF16_BE, codecs.BOM_UTF16_LE):
        if data.startswith(bom):
            return len(bom)
    return 0


def _strip_bom(data: bytes) -> bytes:
    return data[_bom_length(data):] if _bom_length(data) else data


def detect_encoding(path: str) -> tuple[str, bool]:
    """Return ``(encoding, has_bom)`` from the first chunk of ``path``.

    Strict UTF-8 is preferred; common CJK code pages are tried in order so
    Windows-generated GBK files decode sensibly instead of turning into
    replacement characters.
    """
    with open(path, "rb") as f:
        head = f.read(CHUNK_BYTES)
    has_bom = head.startswith((codecs.BOM_UTF8, codecs.BOM_UTF16_BE, codecs.BOM_UTF16_LE))
    if head.startswith(codecs.BOM_UTF8):
        return "utf-8-sig", True
    if head.startswith((codecs.BOM_UTF16_BE, codecs.BOM_UTF16_LE)):
        return "utf-16", True
    for enc in ("utf-8", "gb18030", "big5"):
        try:
            head.decode(enc)
            return enc, has_bom
        except UnicodeDecodeError:
            continue
    # Nothing text-like decoded cleanly: latin-1 never throws, so mark the
    # fallback explicitly — the caller treats this as a garbled-encoding hint.
    return "latin-1", False


def looks_binary(path: str) -> bool:
    """A NUL byte in the first chunk is a near-certain binary signal."""
    try:
        with open(path, "rb") as f:
            return b"\x00" in f.read(CHUNK_BYTES)
    except OSError:
        return True


# ---------------------------------------------------------------------------
# Shape / dialect detection
# ---------------------------------------------------------------------------
def split_row(text: str, delim: str) -> list[str]:
    if delim == " ":
        return text.split()
    if delim in (",", ";", "\t", "|"):
        try:
            return next(csv.reader(io.StringIO(text), delimiter=delim))
        except (csv.Error, StopIteration):
            return text.split(delim)
    return [text]


def _looks_like_header(cells: list[str], delim: str) -> bool:
    # Only structured, positionally-meaningful delimiters carry headers;
    # whitespace-split prose must never be treated as a column header.
    if delim not in (",", ";", "\t", "|") or len(cells) < 2:
        return False
    for c in cells:
        c = c.strip()
        if not c:
            return False
        try:
            float(c.replace(",", "."))
            return False  # a numeric first row is data, not a header
        except ValueError:
            pass
    return True


def detect_dialect(lines: list[str]) -> dict:
    """Sniff delimiter / JSONL / column mode / header from sample text lines."""
    texts = [ln for ln in lines if ln.strip()][:HEAD_LINES]
    if not texts:
        return {"delimiter": "none", "columns": 0, "has_header": False, "header": []}

    # JSONL?  A strong signal: almost every head row parses to an object/list.
    parsed = 0
    json_rows: list[Any] = []
    for ln in texts[:5]:
        try:
            obj = json.loads(ln)
        except ValueError:
            obj = None
        if isinstance(obj, (dict, list)):
            parsed += 1
            json_rows.append(obj)
    if parsed >= max(1, int(len(texts[:5]) * 0.8)):
        key_sets = [tuple(sorted(r.keys())) for r in json_rows if isinstance(r, dict)]
        keys = list(key_sets[0]) if key_sets else []
        return {"delimiter": "jsonl", "columns": len(keys), "has_header": False,
                "header": keys, "json_rows": json_rows}

    best: Optional[dict] = None
    for delim in DELIMITERS:
        counts: dict[int, int] = {}
        for ln in texts:
            n = len(split_row(ln, delim))
            counts[n] = counts.get(n, 0) + 1
        if not counts:
            continue
        mode_cols, support = max(counts.items(), key=lambda kv: kv[1])
        ratio = support / len(texts)
        # Score: must actually split most rows into >1 column; consistency wins.
        if mode_cols >= 2 and ratio >= 0.7:
            score = (ratio, mode_cols)
            if best is None or score > best["_score"]:
                best = {"delimiter": delim, "columns": mode_cols, "_score": score,
                        "counts": counts}
    if best is None:
        return {"delimiter": "none", "columns": 1, "has_header": False, "header": []}

    first_cells = split_row(texts[0], best["delimiter"])
    has_header = _looks_like_header(first_cells, best["delimiter"])
    return {"delimiter": best["delimiter"], "columns": best["columns"],
            "has_header": has_header,
            "header": [c.strip() for c in first_cells] if has_header else [],
            "counts": best["counts"]}


# ---------------------------------------------------------------------------
# Per-file scan (the workhorse)
# ---------------------------------------------------------------------------
@dataclass
class FileScan:
    index: int
    name: str
    path: str
    size: int = 0
    encoding: str = "utf-8"
    has_bom: bool = False
    encoding_fallback: bool = False
    delimiter: str = "none"
    columns: int = 0
    header: list[str] = field(default_factory=list)
    has_header: bool = False
    total_lines: int = 0
    scanned_lines: int = 0
    blank_lines: int = 0
    replacement_chars: int = 0
    line_count_exact: bool = True
    head_texts: list[str] = field(default_factory=list)
    content_texts: list[str] = field(default_factory=list)        # scanned non-blank lines for validation
    samples: list[dict] = field(default_factory=list)
    jsonl: bool = False
    readable: bool = True


def scan_file(index: int, path: str, name: str = "",
              per_file_samples: int = SAMPLES_PER_FILE) -> FileScan:
    """Scan one input shard: stats, dialect and head/mid/tail samples.

    Never raises for bad data — an unreadable/empty/binary file is reported via
    the returned :class:`FileScan` (``readable=False``) and turned into an
    anomaly hint by the caller.
    """
    scan = FileScan(index=index, path=path, name=name or os.path.basename(path))
    try:
        scan.size = os.path.getsize(path)
    except OSError as exc:
        scan.readable = False
        scan._error = str(exc)  # type: ignore[attr-defined]
        return scan
    if scan.size == 0:
        return scan
    if looks_binary(path):
        scan.readable = False
        return scan

    scan.encoding, scan.has_bom = detect_encoding(path)
    scan.encoding_fallback = scan.encoding == "latin-1"
    capped = scan.size > SCAN_CAP_BYTES
    decoded_lines: list[str] = []
    try:
        for raw_no, offset, raw in iter_raw_lines(path):
            if offset > SCAN_CAP_BYTES:
                capped = True
                break
            scan.total_lines += 1
            scan.scanned_lines += 1
            text = raw.decode(scan.encoding, errors="replace")
            scan.replacement_chars += text.count("�")
            decoded_lines.append(text)
            if not text.strip():
                scan.blank_lines += 1
    except OSError as exc:
        scan.readable = False
        scan._error = str(exc)  # type: ignore[attr-defined]
        return scan

    if capped:
        # Extrapolate from line density observed inside the scanned window.
        scanned_bytes = min(scan.size, SCAN_CAP_BYTES)
        density = scan.total_lines / max(1, scanned_bytes)
        scan.total_lines = max(scan.total_lines, int(scan.size * density))
        scan.line_count_exact = False

    scan.head_texts = decoded_lines[:HEAD_LINES]
    scan.content_texts = [t for t in decoded_lines if t.strip()]
    dialect = detect_dialect(scan.head_texts)
    scan.delimiter = dialect["delimiter"]
    scan.columns = dialect["columns"]
    scan.header = dialect["header"]
    scan.has_header = dialect["has_header"]
    scan.jsonl = dialect["delimiter"] == "jsonl"
    scan.samples = _select_file_samples(scan, decoded_lines, per_file_samples)
    return scan


def _columns_of(text: str, scan: FileScan) -> Optional[list[str]]:
    if scan.jsonl:
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return [str(v) for v in obj.values()]
        except ValueError:
            return None
        return None
    if scan.delimiter in ("none", ""):
        return None
    return split_row(text, scan.delimiter)


def _preview_text(record: Any) -> str:
    """How a materialised record is rendered in the sample table."""
    if isinstance(record, str):
        return record
    return jsonutil.dumps_line(record)


def _sample_dict(region: str, scan: FileScan, raw_no: int, text: str,
                 approximate: bool = False) -> dict:
    blank = not text.strip()
    return {
        "region": region,                      # head | middle | tail
        "file_index": scan.index,
        "file": scan.name,
        "raw_line": raw_no,
        "approximate_line": approximate,
        "text": text[:MAX_SAMPLE_TEXT],
        "text_truncated": len(text) > MAX_SAMPLE_TEXT,
        "columns": _columns_of(text, scan),
        "blank": blank,
        "global_index": None,
        "shard": None,
    }


def _select_file_samples(scan: FileScan, decoded: list[str], budget: int) -> list[dict]:
    """Pick head / middle / tail raw lines for one shard."""
    out: list[dict] = []
    seen: set[int] = set()

    def add(region: str, raw_no: int, text: str, approximate: bool = False) -> None:
        if raw_no in seen or raw_no < 1:
            return
        seen.add(raw_no)
        out.append(_sample_dict(region, scan, raw_no, text, approximate))

    # --- head: first non-blank two (plus a blank if the file literally opens with one)
    head_count = 0
    for i, text in enumerate(decoded):
        if head_count >= 2:
            break
        add("head", i + 1, text)
        head_count += 1

    mid_no = max(1, scan.total_lines // 2)
    tail_no = max(1, scan.total_lines)
    if scan.line_count_exact:
        line_map = {i + 1: t for i, t in enumerate(decoded)}
        if mid_no not in seen and 1 <= mid_no <= len(decoded):
            add("middle", mid_no, line_map[mid_no])
        for no in (tail_no - 1, tail_no):
            if no not in seen and 1 <= no <= len(decoded):
                add("tail", no, line_map[no])
    else:
        mid_text = _read_near_offset(scan.path, scan.encoding, scan.size // 2)
        if mid_text is not None:
            add("middle", mid_no, mid_text, approximate=True)
        for k, text in enumerate(_read_tail(scan.path, scan.encoding)):
            add("tail", tail_no - (1 - k), text, approximate=True)

    out.sort(key=lambda s: (s["raw_line"], s["region"]))
    return out[:max(1, budget)]


def _read_near_offset(path: str, encoding: str, offset: int) -> Optional[str]:
    """Read one complete line at/after ``offset`` (line boundary aligned)."""
    try:
        with open(path, "rb") as f:
            f.seek(max(0, offset))
            if offset:
                f.readline()  # discard the partial line straddling the offset
            raw = f.readline()
            if not raw:
                return None
            return raw.rstrip(b"\r\n").decode(encoding, errors="replace")
    except OSError:
        return None


def _read_tail(path: str, encoding: str) -> list[str]:
    """Return up to the last two text lines of a file via a backwards window."""
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            f.seek(max(0, size - TAIL_BYTES))
            data = f.read()
    except OSError:
        return []
    parts = data.split(b"\n")
    if parts and parts[-1] == b"":
        parts.pop()  # trailing newline is not an empty record line
    return [p.rstrip(b"\r").decode(encoding, errors="replace") for p in parts[-2:]]


# ---------------------------------------------------------------------------
# Helpers shared with the ShardPlanner
# ---------------------------------------------------------------------------
def shard_ranges(total: int, num_shards: int) -> list[tuple[int, int]]:
    """``[(start, count), ...]`` — the same even split ``split_evenly`` uses."""
    if total <= 0:
        return [(0, 0) for _ in range(max(1, num_shards))]
    n = max(1, min(num_shards, total))
    base, rem = divmod(total, n)
    ranges: list[tuple[int, int]] = []
    start = 0
    for i in range(n):
        count = base + (1 if i < rem else 0)
        ranges.append((start, count))
        start += count
    return ranges


def shard_of(global_index: int, ranges: list[tuple[int, int]]) -> Optional[int]:
    for i, (start, count) in enumerate(ranges):
        if start <= global_index < start + count:
            return i
    return None


# ---------------------------------------------------------------------------
# Preview session
# ---------------------------------------------------------------------------
class PreviewManager:
    """Creates, persists and serves input-preview sessions under the store."""

    def __init__(self, storage: Storage, config: Any = None) -> None:
        self.storage = storage
        self.config = config
        self.root = "previews"

    # -- path helpers -------------------------------------------------
    def _dir(self, preview_id: str, *parts: str) -> str:
        return self.storage.path(self.root, preview_id, *parts)

    def inputs_root(self) -> str:
        """Directory server-relative input paths are resolved against."""
        root = getattr(self.config, "input_root", "data/inputs") if self.config else "data/inputs"
        if os.path.isabs(root):
            path = root
        else:
            path = self.storage.path(root)
        ensure_dir(path)
        return os.path.abspath(path)

    def resolve_path(self, raw_path: str) -> str:
        """Resolve a user-supplied path, refusing anything outside inputs root."""
        base = self.inputs_root()
        candidate = raw_path if os.path.isabs(raw_path) else os.path.join(base, raw_path)
        candidate = os.path.abspath(candidate)
        if os.path.commonpath([base, candidate]) != base:
            raise ValueError(f"path {raw_path!r} is outside the allowed input root {base!r}")
        return candidate

    # -- source discovery ---------------------------------------------
    def _expand_paths(self, paths: list[str]) -> list[tuple[str, str]]:
        files: list[tuple[str, str]] = []
        for raw in paths:
            target = self.resolve_path(raw)
            if os.path.isdir(target):
                single_dir = os.path.basename(target.rstrip(os.sep))
                for dirpath, _dirs, names in os.walk(target):
                    for nm in sorted(names):
                        full = os.path.join(dirpath, nm)
                        rel = os.path.relpath(full, target)
                        display = rel if single_dir == "inputs" else os.path.join(single_dir, rel)
                        files.append((full, display))
                        if len(files) >= MAX_FILES:
                            return files
            elif os.path.isfile(target):
                files.append((target, os.path.basename(target)))
        return files

    # -- public API ---------------------------------------------------
    def create(
        self,
        source: str,
        *,
        paths: Optional[list[str]] = None,
        uploads: Optional[list[tuple[str, bytes]]] = None,
        paste_text: str = "",
        synthetic_kind: str = "wordcount",
        rows: int = 12000,
        delimiter: str = "auto",
        has_header: Optional[bool] = None,   # None = auto-detect
        skip_blank: bool = True,
        num_map_tasks: int = 8,
    ) -> dict:
        """Build a preview session and persist its records + metadata."""
        preview_id = new_id("pv")
        raw_dir = self._dir(preview_id, "raw")
        os.makedirs(raw_dir, exist_ok=True)

        warnings: list[WarnMsg] = []
        discovered: list[tuple[str, str]] = []  # (absolute path, display name)

        if source == "files" and uploads:
            total = 0
            for nm, data in uploads:
                total += len(data)
            if total > MAX_UPLOAD_BYTES:
                raise ValueError(f"uploaded data exceeds the {MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit")
            used_names: set[str] = set()
            for nm, data in uploads:
                base_name = (os.path.basename(nm.replace("\\", "/").replace(os.sep, "_"))
                             or f"input-{len(discovered)}")
                safe = base_name
                dedup = 1
                while safe in used_names:
                    stem, dot, ext = base_name.partition(".")
                    safe = f"{stem}-{dedup}{dot}{ext}" if dot else f"{base_name}-{dedup}"
                    dedup += 1
                used_names.add(safe)
                full = os.path.join(raw_dir, safe)
                with open(full, "wb") as f:
                    f.write(data)
                discovered.append((full, safe))
        elif source == "paste":
            full = os.path.join(raw_dir, "pasted.txt")
            with open(full, "w", encoding="utf-8") as f:
                f.write(paste_text or "")
            discovered.append((full, "pasted.txt"))
        elif source == "path" and paths:
            discovered = self._expand_paths(paths)
        elif source == "synthetic":
            full = os.path.join(raw_dir, "synthetic.jsonl")
            self._write_synthetic(full, synthetic_kind, rows, preview_id)
            discovered = [(full, f"synthetic:{synthetic_kind}")]
        else:
            raise ValueError("no input source provided")

        scans = [scan_file(i, p, nm) for i, (p, nm) in enumerate(discovered[:MAX_FILES])]
        self._apply_overrides(scans, delimiter, has_header)
        warnings.extend(self._scan_warnings(scans))

        # Materialise logical records + sample provenance + sparse record index.
        records_path = self._dir(preview_id, "records.jsonl")
        total_records, truncated, record_index, sample_lookup = self._materialize(
            scans, records_path, skip_blank=skip_blank,
        )
        if truncated:
            warnings.append(_w("warning", "records_truncated", n=total_records,
                               limit=MAX_RECORDS))
        if total_records == 0 and any(s.size > 0 for s in scans):
            warnings.append(_w("error", "no_records"))
        if not scans:
            warnings.append(_w("error", "no_files"))

        warnings.extend(self._cross_file_warnings(scans))

        # Assemble samples and shard coverage.
        num_map = max(1, min(int(num_map_tasks), max(1, total_records)))
        ranges = shard_ranges(total_records, num_map)
        samples = self._build_samples(scans, sample_lookup, ranges)
        covered = self._fill_coverage(records_path, record_index, ranges, samples, scans)
        shards = self._shard_views(ranges, samples)
        samples_per_file: dict[int, int] = {}
        for sm in samples:
            samples_per_file[sm["file_index"]] = samples_per_file.get(sm["file_index"], 0) + 1

        session = {
            "preview_id": preview_id,
            "created_ms": now_ms(),
            "source": source,
            "synthetic_kind": synthetic_kind if source == "synthetic" else "",
            "options": {
                "delimiter": delimiter,
                "has_header": has_header if has_header is not None else "auto",
                "skip_blank": skip_blank,
                "num_map_tasks": int(num_map_tasks),
            },
            "summary": {
                "files": len(scans),
                "readable_files": sum(1 for s in scans if s.readable and s.total_lines > 0),
                "total_bytes": sum(s.size for s in scans),
                "total_lines": sum(s.total_lines for s in scans),
                "blank_lines": sum(s.blank_lines for s in scans),
                "total_records": total_records,
                "truncated": truncated,
                "record_cap": MAX_RECORDS,
                "line_counts_exact": all(s.line_count_exact for s in scans),
                "delimiter": self._dominant_delimiter(scans),
                "columns": self._dominant_columns(scans),
                "encoding": self._dominant_encoding(scans),
                "headers_present": sum(1 for s in scans if s.has_header),
            },
            "files": [self._file_view(s, samples_per_file.get(s.index, 0)) for s in scans],
            "shards": shards,
            "samples": samples,
            "warnings": [w.__dict__ for w in warnings],
            "coverage_complete": covered,
        }
        atomic_write_json(self._dir(preview_id, "session.json"), session)
        atomic_write_json(self._dir(preview_id, "record_index.json"), record_index)
        return session

    def get(self, preview_id: str) -> Optional[dict]:
        return self.storage.read(self.root, preview_id, "session.json", default=None)

    def load_records(self, preview_id: str) -> list[Any]:
        """All materialised records for a session (what the planner will run)."""
        from .storage import read_jsonl
        path = self._dir(preview_id, "records.jsonl")
        return [doc["r"] for doc in read_jsonl(path) if isinstance(doc, dict) and "r" in doc]

    # -- synthetic generation ----------------------------------------
    def _write_synthetic(self, path: str, kind: str, rows: int, preview_id: str) -> None:
        from backend.tasks.samples import generate_input_records

        seed = (sum(ord(c) for c in preview_id)) % (2 ** 31 - 1)
        want = max(1, min(int(rows), MAX_RECORDS))
        records = generate_input_records(kind, want, seed)
        # Written as a raw synthetic corpus (one record per JSONL line); the
        # scan sees it as JSONL/dict input and materialisation wraps it once.
        with open(path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(jsonutil.dumps_line(rec) + "\n")

    # -- option overrides / warnings ---------------------------------
    def _apply_overrides(self, scans: list[FileScan], delimiter: str,
                         has_header: Optional[bool]) -> None:
        for scan in scans:
            if not scan.readable:
                continue
            if delimiter not in ("", "auto", None):
                scan.delimiter = delimiter
                if delimiter in (" ",):
                    scan.columns = max((len(split_row(t, delimiter)) for t in scan.head_texts[:5]), default=1)
                elif delimiter in (",", ";", "\t", "|"):
                    counts = [len(split_row(t, delimiter)) for t in scan.head_texts if t.strip()]
                    scan.columns = max(set(counts), key=counts.count) if counts else 1
                else:
                    scan.columns = 1
                for s in scan.samples:
                    s["columns"] = _columns_of(s["text"], scan)
            if has_header is not None:
                if has_header:
                    if not scan.header and scan.head_texts:
                        first = scan.head_texts[0]
                        scan.header = (split_row(first, scan.delimiter)
                                       if scan.delimiter not in ("none", "jsonl")
                                       else [first])
                    scan.has_header = bool(scan.header)
                else:
                    scan.has_header = False

    def _scan_warnings(self, scans: list[FileScan]) -> list[WarnMsg]:
        out: list[WarnMsg] = []
        for s in scans:
            kw = {"scope": "file", "file": s.name}
            if not s.readable:
                detail = getattr(s, "_error", "")
                if s.size and looks_binary(s.path):
                    out.append(_w("error", "binary_file", **kw))
                elif detail:
                    out.append(_w("error", "unreadable_file", detail=detail, **kw))
                else:
                    out.append(_w("error", "binary_file", **kw))
                continue
            if s.size == 0 or (s.total_lines == 0):
                out.append(_w("error", "empty_file", **kw))
                continue
            if s.replacement_chars or s.encoding_fallback:
                out.append(_w("warning", "encoding_garbled",
                              encoding=s.encoding, **kw))
            if not s.line_count_exact:
                out.append(_w("info", "line_count_estimated", **kw))
            # column consistency over scanned content-bearing lines
            if s.delimiter not in ("none", "jsonl", "") and s.columns > 1:
                candidates = s.content_texts
                # When the whole file was scanned, also inspect a tail window so
                # a malformed row near the end of a large shard is not missed.
                if len(candidates) > 400:
                    candidates = candidates[:200] + candidates[-100:]
                bad = 0
                checked = 0
                for idx, text in enumerate(candidates):
                    if s.has_header and idx == 0:
                        continue  # header may legitimately use textual labels
                    checked += 1
                    if len(split_row(text, s.delimiter)) != s.columns:
                        bad += 1
                if checked >= 2 and bad >= 1:
                    # A single misplaced column is still worth surfacing before
                    # the user pays for a full job run; ratio tunes the level.
                    level = "warning" if bad / checked >= 0.10 or bad >= 3 else "info"
                    out.append(_w(level, "column_mismatch",
                                  n=bad, cols=s.columns, **kw))
            if s.jsonl:
                majority: dict[tuple, int] = {}
                parsed_rows = 0
                for text in s.head_texts:
                    try:
                        obj = json.loads(text)
                    except ValueError:
                        continue
                    if isinstance(obj, dict):
                        parsed_rows += 1
                        key = tuple(sorted(obj.keys()))
                        majority[key] = majority.get(key, 0) + 1
                if majority:
                    top = max(majority.values())
                    if parsed_rows - top >= max(1, int(parsed_rows * 0.2)):
                        out.append(_w("warning", "jsonl_keys_differ",
                                      n=parsed_rows - top, **kw))
            if s.blank_lines:
                total = max(1, s.scanned_lines)
                pct = round(100.0 * s.blank_lines / total)
                if pct >= 20:
                    out.append(_w("warning", "blank_lines_high",
                                  n=s.blank_lines, total=s.scanned_lines,
                                  pct=pct, **kw))
                else:
                    out.append(_w("info", "blank_lines_present",
                                  n=s.blank_lines, **kw))
            if s.has_header:
                out.append(_w("info", "header_detected", **kw))
        return out

    def _cross_file_warnings(self, scans: list[FileScan]) -> list[WarnMsg]:
        out: list[WarnMsg] = []
        structured = [s for s in scans if s.readable and s.delimiter
                      in (",", ";", "\t", "|")]
        delims = {s.delimiter for s in structured}
        if len(delims) > 1:
            detail = ", ".join(sorted(DELIMITER_NAMES.get(d, d) for d in delims))
            out.append(_w("warning", "inconsistent_delimiters", detail=detail))
        headers = {bool(s.has_header) for s in structured}
        if len(structured) > 1 and len(headers) > 1:
            out.append(_w("warning", "header_disagrees"))
        return out

    # -- materialisation ---------------------------------------------
    def _materialize(self, scans: list[FileScan], records_path: str,
                     skip_blank: bool) -> tuple[int, bool, list[list[int]], dict]:
        """Write logical records to JSONL and build a sparse offset index.

        Each line is ``{"r": record, "f": file_index, "l": raw_line}`` so a
        sampled global record can always be traced back to the exact byte
        position it came from, even though blank lines and header rows are
        skipped and therefore do not become records.

        Returns ``(total, truncated, index_entries, sample_lookup)`` where
        ``index_entries`` are ``[global_index, byte_offset]`` every
        :data:`INDEX_EVERY` records (used by the coverage resolver) and
        ``sample_lookup`` maps sampled raw lines ``(file, raw)`` to the global
        record index they became.
        """
        ensure_dir(os.path.dirname(records_path))
        # Exact-line samples key by raw line number; capped-scan middle/tail
        # samples only know an approximate line, so they key by content prefix.
        exact_wanted: dict[int, set[int]] = {}
        approx_wanted: dict[int, dict[str, int]] = {}
        for s in scans:
            exact: set[int] = set()
            approx: dict[str, int] = {}
            for sm in s.samples:
                if sm.get("approximate_line"):
                    approx[sm["text"][:80]] = -1
                else:
                    exact.add(sm["raw_line"])
            exact_wanted[s.index] = exact
            approx_wanted[s.index] = approx

        index_entries: list[list[int]] = []
        sample_lookup: dict[tuple[int, Any], int] = {}
        total = 0
        truncated = False
        with open(records_path, "w", encoding="utf-8") as out:
            for s in scans:
                if not s.readable:
                    continue
                approx = approx_wanted.get(s.index, {})
                try:
                    for raw_no, _offset, raw in iter_raw_lines(s.path):
                        text = raw.decode(s.encoding, errors="replace")
                        if skip_blank and not text.strip():
                            continue
                        if s.has_header and raw_no == 1:
                            continue
                        if total >= MAX_RECORDS:
                            truncated = True
                            break
                        if total % INDEX_EVERY == 0:
                            index_entries.append([total, out.tell()])
                        if raw_no in exact_wanted.get(s.index, set()):
                            sample_lookup[(s.index, raw_no)] = total
                        prefix = text[:80]
                        if prefix in approx and approx[prefix] == -1:
                            approx[prefix] = total
                            sample_lookup[(s.index, prefix)] = total
                        record = self._to_record(text, s)
                        out.write(jsonutil.dumps_line(
                            {"r": record, "f": s.index, "l": raw_no}) + "\n")
                        total += 1
                except OSError:
                    continue
                if truncated:
                    break
        return total, truncated, index_entries, sample_lookup

    @staticmethod
    def _to_record(text: str, scan: FileScan) -> Any:
        if scan.jsonl:
            try:
                return json.loads(text)
            except ValueError:
                return text
        return text

    # -- sample assembly / coverage ----------------------------------
    def _build_samples(self, scans: list[FileScan],
                       sample_lookup: dict[tuple[int, int], int],
                       ranges: list[tuple[int, int]]) -> list[dict]:
        samples: list[dict] = []
        # Fair per-shard quota so many tiny files all show up, while a single
        # large shard still gets its head/middle/tail positions.
        readable = max(1, sum(1 for s in scans if s.readable))
        per_file = max(3, min(SAMPLES_PER_FILE, MAX_TOTAL_SAMPLES // readable))
        for s in scans:
            for sm in s.samples[:per_file]:
                if sm.get("approximate_line"):
                    # Capped-scan middle/tail samples carry an estimated line
                    # number; map them by (file, content prefix) which the
                    # materialiser recorded after the exact pass.
                    gidx = sample_lookup.get((s.index, sm["text"][:80]))
                else:
                    gidx = sample_lookup.get((s.index, sm["raw_line"]))
                sm["global_index"] = gidx
                sm["shard"] = shard_of(gidx, ranges) if gidx is not None else None
                samples.append(sm)
        return samples

    def _fill_coverage(self, records_path: str, index_entries: list[list[int]],
                       ranges: list[tuple[int, int]], samples: list[dict],
                       scans: list[FileScan]) -> bool:
        """Add one sample per map shard the raw head/mid/tail scan missed."""
        touched = {sm["shard"] for sm in samples if sm["shard"] is not None}
        missing = [i for i, (_, count) in enumerate(ranges) if count and i not in touched]
        if not missing:
            return True
        targets = {i: ranges[i][0] + ranges[i][1] // 2 for i in missing}
        resolved = resolve_record_indices(records_path, index_entries, list(targets.values()))
        name_by_index = {s.index: s.name for s in scans}
        for shard_i in sorted(targets):
            gidx = targets[shard_i]
            record, fidx, raw_no = resolved.get(gidx, (None, None, None))
            if record is None:
                continue
            text = _preview_text(record)
            samples.append({
                "region": "coverage",
                "file_index": fidx,
                "file": name_by_index.get(fidx, "?"),
                "raw_line": raw_no,
                "approximate_line": False,
                "text": text[:MAX_SAMPLE_TEXT],
                "text_truncated": len(text) > MAX_SAMPLE_TEXT,
                "columns": None,
                "blank": False,
                "global_index": gidx,
                "shard": shard_i,
            })
        touched = {sm["shard"] for sm in samples if sm["shard"] is not None}
        return all(i in touched for i, (_, c) in enumerate(ranges) if c)

    def _shard_views(self, ranges: list[tuple[int, int]],
                     samples: list[dict]) -> list[dict]:
        per: dict[int, int] = {}
        for sm in samples:
            if sm["shard"] is not None:
                per[sm["shard"]] = per.get(sm["shard"], 0) + 1
        return [{
            "index": i,
            "start_record": start,
            "count": count,
            "sampled": per.get(i, 0),
            "covered": per.get(i, 0) > 0,
        } for i, (start, count) in enumerate(ranges)]

    # -- summary helpers ---------------------------------------------
    @staticmethod
    def _dominant(items: list[str], prefer_structured: bool = False) -> str:
        counts: dict[str, int] = {}
        for it in items:
            if it:
                counts[it] = counts.get(it, 0) + 1
        if not counts:
            return ""
        if prefer_structured:
            structured = {k: v for k, v in counts.items() if k != "none"}
            if structured:
                return max(structured, key=structured.get)
        return max(counts, key=counts.get)

    def _dominant_delimiter(self, scans: list[FileScan]) -> str:
        # Vote per *file* (each shard is one vote) and prefer a structured
        # delimiter over "none", otherwise one huge unstructured shard would
        # drown out many small CSV shards.
        return self._dominant(
            [s.delimiter for s in scans if s.readable], prefer_structured=True)

    def _dominant_encoding(self, scans: list[FileScan]) -> str:
        return self._dominant([s.encoding for s in scans if s.readable])

    def _dominant_columns(self, scans: list[FileScan]) -> int:
        cols = [s.columns for s in scans if s.readable and s.columns > 1]
        return max(set(cols), key=cols.count) if cols else 0

    def _file_view(self, s: FileScan, sampled: int = 0) -> dict:
        return {
            "index": s.index,
            "name": s.name,
            "size": s.size,
            "sampled": sampled,
            "encoding": s.encoding,
            "delimiter": s.delimiter,
            "delimiter_label": DELIMITER_NAMES.get(s.delimiter, s.delimiter),
            "columns": s.columns,
            "has_header": s.has_header,
            "header": s.header,
            "total_lines": s.total_lines,
            "scanned_lines": s.scanned_lines,
            "blank_lines": s.blank_lines,
            "line_count_exact": s.line_count_exact,
            "readable": s.readable,
            "jsonl": s.jsonl,
        }


# ---------------------------------------------------------------------------
# Random access into the materialised records (for coverage samples)
# ---------------------------------------------------------------------------
def resolve_record_indices(
    records_path: str,
    index_entries: list[list[int]],
    targets: list[int],
) -> dict[int, tuple[Any, Optional[int], Optional[int]]]:
    """Resolve ``global_index -> (record, file_index, raw_line)`` via seeks.

    The sparse index points within :data:`INDEX_EVERY` records of each target;
    each record carries its exact source file and raw line, so a short forward
    scan from an indexed jump resolves a target without any bookkeeping drift
    caused by skipped blank/header lines.
    """
    if not targets or not os.path.exists(records_path):
        return {}
    wanted = sorted(set(targets))
    first = wanted[0]
    globals_ = [e[0] for e in index_entries]
    pos = bisect.bisect_right(globals_, first) - 1
    if pos < 0:
        return {}
    base_global, offset = index_entries[pos]
    result: dict[int, tuple[Any, Optional[int], Optional[int]]] = {}
    wanted_set = set(wanted)
    current = base_global
    with open(records_path, "r", encoding="utf-8") as f:
        f.seek(offset)
        for line in f:
            if current > wanted[-1]:
                break
            if current in wanted_set:
                doc = jsonutil.parse_line(line)
                if isinstance(doc, dict) and "r" in doc:
                    result[current] = (doc["r"], doc.get("f"), doc.get("l"))
            current += 1
    return result


# ---------------------------------------------------------------------------
# Convenience used by the HTTP layer
# ---------------------------------------------------------------------------
def list_preview_sessions(storage: Storage) -> list[dict]:
    out: list[dict] = []
    root = storage.path("previews")
    if not os.path.isdir(root):
        return out
    for preview_id in sorted(os.listdir(root)):
        doc = storage.read("previews", preview_id, "session.json", default=None)
        if isinstance(doc, dict):
            out.append({
                "preview_id": doc.get("preview_id"),
                "created_ms": doc.get("created_ms"),
                "source": doc.get("source"),
                "records": doc.get("summary", {}).get("total_records", 0),
                "warnings": len(doc.get("warnings", [])),
            })
    return sorted(out, key=lambda d: d.get("created_ms", 0), reverse=True)
