"""Read-only input sampling for the pre-submission data preview (数据抽样预览).

The preview answers one question: *"does my input look like what the job
expects, before I pay for a whole run?"*  It never executes a job and never
writes — every file is opened read-only and only small, capped byte ranges are
touched.

The implementation is shaped by the messy realities listed in the requirement:

* **big files (文件很大)** — per-file scanning is byte-bounded
  (``SCAN_BYTES_TOTAL``); line counts past the scanned prefix are *estimated*
  from the newline density of the scanned region, and content comes from three
  probes (head / middle / tail) so the tail of a giant file is represented too;
* **many tiny shards (分片很碎)** — ``stat`` is collected for *every* file
  (cheap), while content probes are spread over a strided subset so the first,
  last and evenly spaced shards are all opened; the coverage report states
  exactly which files/shards were touched;
* **mixed delimiters (字段分隔不一致)** — a per-file delimiter is sniffed
  (tab/comma/pipe/semicolon/whitespace); files that disagree with the majority
  and rows whose column count drifts are surfaced as warnings;
* **headers / blank lines (表头或空行)** — a header row is guessed with a
  numeric-column heuristic (or taken from the caller override) and blank lines
  are counted during the bounded scan;
* **garbled encodings (编码乱掉)** — BOM/UTF-8/GB18030 are detected per file
  and decoding replacement ratios are reported; binary files are flagged.

The public surface is :func:`discover_file_sources` (path sandboxing),
:func:`memory_sources` (upload / paste) and :func:`preview_sources` (the
analysis itself).  Everything returns plain dicts so the result round-trips
through JSON unchanged.
"""

from __future__ import annotations

import codecs
import glob as _glob
import os
import time
from dataclasses import dataclass, field
from typing import Optional, Sequence

# ---------------------------------------------------------------------------
# Bounds — the preview stays cheap regardless of input size
# ---------------------------------------------------------------------------
SCAN_BYTES_TOTAL = 8 * 1024 * 1024     # at most ~8 MiB scanned per preview
SCAN_BYTES_PER_FILE = 1 * 1024 * 1024  # and at most ~1 MiB of one file
KEEP_OFFSETS_UNDER = 256 * 1024        # small files keep exact line numbers
PROBE_HEAD = 8 * 1024
PROBE_MID = 2 * 1024
PROBE_TAIL = 4 * 1024
PROBE_GAP = 4 * 1024                   # head/tail must be at least this far apart
RESYNC_WINDOW = 4 * 1024               # how far a mid/tail probe seeks a newline
PROBE_ANCHOR = 768                     # bytes read at each big-file split boundary
MAX_ANCHORS = 16                       # split anchors per file
MAX_PROBED_FILES = 48                  # content probes per preview
MAX_LISTED_FILES = 200                 # per-file rows returned in the JSON
MAX_DISCOVERED = 20_000                # refuse directories larger than this
MAX_SAMPLE_TEXT = 200                  # characters of raw text per sample
MAX_CELL_TEXT = 60                     # characters per parsed cell
MAX_WARNING_FILES = 10                 # file names listed inside one warning

DELIMS = {"\t": "Tab", ",": "Comma ,", "|": "Pipe |", ";": "Semicolon ;",
          "ws": "Whitespace"}
DELIM_ALIASES = {"tab": "\t", "\\t": "\t", "comma": ",", "pipe": "|",
                 "semicolon": ";", "space": "ws", "whitespace": "ws",
                 "ws": "ws"}


class PreviewError(ValueError):
    """A user-correctable problem with the preview request (bad path, …)."""


# ---------------------------------------------------------------------------
# Sources: a uniform random-access interface over real files and in-memory blobs
# ---------------------------------------------------------------------------
@dataclass
class Source:
    name: str
    size: int
    index: int = 0

    def read_at(self, offset: int, length: int) -> bytes:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass
class FileSource(Source):
    path: str = ""

    def read_at(self, offset: int, length: int) -> bytes:
        if length <= 0 or offset >= self.size:
            return b""
        with open(self.path, "rb") as f:
            f.seek(offset)
            return f.read(min(length, self.size - offset))


@dataclass
class MemorySource(Source):
    blob: bytes = b""

    def read_at(self, offset: int, length: int) -> bytes:
        if length <= 0 or offset >= self.size:
            return b""
        return self.blob[offset:offset + length]


def memory_sources(files: Sequence[tuple[str, bytes]]) -> list[Source]:
    """Build sources for uploaded / pasted input: ``[(display_name, bytes), …]``."""
    out: list[Source] = []
    for i, (name, blob) in enumerate(files):
        blob = blob or b""
        out.append(MemorySource(name=name or f"paste-{i + 1}", size=len(blob),
                                index=i, blob=blob))
    return out


def discover_file_sources(target: str, allowed_roots: Sequence[str]) -> list[Source]:
    """Resolve ``target`` (file / directory / glob) into ordered FileSources.

    ``target`` is resolved against ``allowed_roots`` and any path escaping them
    (including via symlinks, because ``realpath`` is used) is rejected.  Files
    are sorted by name so shard order is deterministic.
    """
    if not target or not str(target).strip():
        raise PreviewError("未提供输入路径 No input path given")
    target = str(target).strip()
    roots = sorted({os.path.realpath(r) for r in allowed_roots if r})
    if not roots:
        raise PreviewError("预览未配置允许访问的目录 No allowed preview root configured")

    if os.path.isabs(target):
        resolved = target
    elif any(ch in target for ch in "*?["):
        # A relative glob is anchored at the first (default) root.
        resolved = os.path.join(roots[0], target)
    else:
        resolved = os.path.join(roots[0], target)

    paths: list[str] = []
    if any(ch in target for ch in "*?["):
        paths = [p for p in sorted(_glob.glob(resolved, recursive=True))
                 if os.path.isfile(p)]
    elif os.path.isdir(resolved):
        for dirpath, dirnames, filenames in os.walk(resolved):
            dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
            for fn in sorted(filenames):
                if fn.startswith("."):
                    continue
                paths.append(os.path.join(dirpath, fn))
            if len(paths) > MAX_DISCOVERED:
                break
    elif os.path.isfile(resolved):
        paths = [resolved]
    else:
        raise PreviewError(f"路径不存在 Path not found: {target}")

    if len(paths) > MAX_DISCOVERED:
        raise PreviewError(f"文件数超过上限 Too many files (>{MAX_DISCOVERED}); 请缩小目录范围 narrow the path")

    sources: list[Source] = []
    for i, p in enumerate(paths):
        rp = os.path.realpath(p)
        if not any(_is_within(rp, root) for root in roots):
            raise PreviewError(f"路径越界 Path outside allowed roots: {target}")
        try:
            size = os.path.getsize(rp)
        except OSError as exc:
            raise PreviewError(f"无法读取文件 Cannot read {p}: {exc}") from exc
        rel = os.path.relpath(rp, os.path.commonpath([rp, roots[0]])) if _is_within(rp, roots[0]) else os.path.basename(rp)
        sources.append(FileSource(name=rel.replace(os.sep, "/"), path=rp,
                                  size=size, index=i))
    return sources


def _is_within(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Scan: bounded newline / blank-line accounting
# ---------------------------------------------------------------------------
@dataclass
class ScanResult:
    scanned: int = 0
    truncated: bool = False
    lf: int = 0                 # LF terminators in the scanned region
    cr: int = 0                 # lone CR terminators (classic Mac)
    blank: int = 0              # empty / whitespace-only LF lines
    trailing_line: bool = False
    trailing_blank: bool = False
    offsets: dict[int, int] = field(default_factory=dict)  # byte offset -> 1-based line

    @property
    def separator(self) -> str:
        return "\r" if self.cr > self.lf else "\n"


def _allocate_scan_bytes(sizes: list[int], budget: int) -> list[int]:
    """Largest-remainder proportional split of ``budget`` with a per-file cap."""
    budget = min(budget, sum(sizes))
    if not sizes or budget <= 0:
        return [0] * len(sizes)
    scale = budget / sum(sizes)
    raw = [min(s, SCAN_BYTES_PER_FILE, max(1, int(s * scale))) for s in sizes]
    while sum(raw) > budget:  # shrink the largest allocations
        i = max(range(len(raw)), key=lambda k: raw[k])
        raw[i] -= 1
    remainder = budget - sum(raw)
    # hand leftover bytes to the most under-served files
    order = sorted(range(len(sizes)),
                   key=lambda k: (raw[k] / sizes[k]) if sizes[k] else 1.0)
    for k in order:
        if remainder <= 0:
            break
        add = min(remainder, sizes[k] - raw[k], SCAN_BYTES_PER_FILE - raw[k])
        raw[k] += add
        remainder -= add
    return [min(a, s) for a, s in zip(raw, sizes)]


def scan_source(src: Source, budget: int) -> ScanResult:
    """Scan the prefix of ``src`` (up to ``budget`` bytes) for line stats.

    Small, fully-scanned files also record a byte-offset → 1-based line-number
    map, which is how samples from such files report exact line numbers.
    """
    res = ScanResult()
    target = min(budget, src.size)
    keep = src.size <= KEEP_OFFSETS_UNDER and target == src.size
    saw_content = False           # non-whitespace byte since the last LF
    line_start = 0
    line_no = 0
    pos = 0
    chunk_size = 64 * 1024
    while pos < target:
        chunk = src.read_at(pos, min(chunk_size, target - pos))
        if not chunk:
            break
        base = pos
        for j, b in enumerate(chunk):
            if b == 0x0A:
                res.lf += 1
                if not saw_content:
                    res.blank += 1
                if keep:
                    line_no += 1
                    res.offsets[line_start] = line_no
                    res.offsets[base + j + 1] = line_no + 1
                line_start = base + j + 1
                saw_content = False
            elif b not in (0x0D, 0x20, 0x09):
                saw_content = True
        # Lone CRs (classic Mac line ending); CRLF pairs are subtracted back.
        res.cr += chunk.count(b"\r") - chunk.count(b"\r\n")
        pos += len(chunk)
        res.scanned = pos

    res.truncated = pos < src.size
    if src.size > 0 and pos == src.size:
        last = src.read_at(src.size - 1, 1)
        if last not in (b"\n", b"\r"):
            res.trailing_line = True
            res.trailing_blank = not saw_content
            if keep:
                res.offsets[line_start] = line_no + 1
    return res


def estimate_lines(src: Source, scan: ScanResult) -> tuple[int, bool]:
    """Return ``(line_count, exact)`` using the scan plus density extrapolation."""
    terminators = max(scan.lf, scan.cr)
    if not scan.truncated:
        return terminators + (1 if scan.trailing_line else 0), True
    if scan.scanned <= 0 or terminators <= 0:
        return 0, False
    # Newline density of the prefix extrapolated over the whole file.
    est = round(terminators * src.size / scan.scanned)
    return max(terminators, est), False


def estimate_blanks(src: Source, scan: ScanResult) -> tuple[int, bool]:
    if not scan.truncated:
        blanks = scan.blank + (1 if scan.trailing_line and scan.trailing_blank else 0)
        return blanks, True
    if scan.scanned <= 0 or scan.lf <= 0:
        return 0, False
    return min(scan.lf, round(scan.blank * src.size / scan.scanned)), False


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------
def detect_encoding(head: bytes) -> tuple[str, bool]:
    """Return ``(codec, has_nul)``; codec is chosen BOM → UTF-8 → GB18030 → latin-1."""
    # A handful of stray NULs occurs in some noisy text; binary files (ELF/PE,
    # compressed payloads, UTF-16 without BOM) have a far higher density.
    nul_ratio = head.count(b"\x00") / max(1, len(head))
    # Text-like bytes are printable ASCII, common whitespace, or high bytes
    # (valid lead/trail bytes for GB18030/UTF-8).  Dense low control bytes
    # other than tab/LF/CR indicate a binary payload.
    controls = sum(1 for b in head
                   if b < 0x09 or b in (0x0B, 0x0C) or 0x0E <= b < 0x20)
    control_ratio = controls / max(1, len(head))
    has_nul = nul_ratio >= 0.01
    # A BOM is a strong hint, but a BOM followed by single-byte noise is just a
    # damaged file (nul_ratio far below UTF-16's ~50 %); fall through so the
    # replacement ratio reports it as garbled instead of emitting mojibake.
    bom16 = head.startswith(codecs.BOM_UTF16_LE) or head.startswith(codecs.BOM_UTF16_BE)
    if bom16 and nul_ratio >= 0.2:
        return ("utf-16-le" if head.startswith(codecs.BOM_UTF16_LE) else "utf-16-be"), False
    # UTF-16 without BOM: NULs must alternate strictly with character bytes on
    # one parity (LE: NUL at odd offsets; BE: at even).  Sporadic NULs embedded
    # in single-byte text do not satisfy this and stay "garbled single-byte".
    probe = head[:256]
    half = max(1, len(probe) // 2)
    even_nul = sum(1 for i in range(0, len(probe), 2) if probe[i:i + 1] == b"\x00") / half
    odd_nul = sum(1 for i in range(1, len(probe), 2) if probe[i:i + 1] == b"\x00") / half
    even_nonzero = sum(1 for i in range(0, len(probe), 2) if probe[i:i + 1] != b"\x00") / half
    odd_nonzero = sum(1 for i in range(1, len(probe), 2) if probe[i:i + 1] != b"\x00") / half
    if nul_ratio >= 0.3:
        # LE requires odd≈all-zero & even≈all-nonzero; BE the reverse.
        if odd_nul >= 0.85 and even_nonzero >= 0.85:
            return "utf-16-le", False
        if even_nul >= 0.85 and odd_nonzero >= 0.85:
            return "utf-16-be", False
    if control_ratio >= 0.08:
        has_nul = True  # dense non-text control bytes: treat as binary
    if head.startswith(codecs.BOM_UTF8):
        return "utf-8-sig", has_nul
    for codec in ("utf-8", "gb18030", "latin-1"):
        try:
            head.decode(codec)
            return codec, has_nul
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin-1", has_nul


def decode_region(raw: bytes, codec: str) -> str:
    return raw.decode(codec, errors="replace")


def replacement_ratio(text: str) -> float:
    return text.count("�") / max(1, len(text))


# ---------------------------------------------------------------------------
# Regions (head / middle / tail probes)
# ---------------------------------------------------------------------------
def _resync_forward(src: Source, offset: int) -> Optional[int]:
    """Skip the torn line at ``offset``; return the start after the next LF."""
    window = src.read_at(offset, min(RESYNC_WINDOW, src.size - offset))
    nl = window.find(b"\n")
    if nl < 0:
        return None
    return offset + nl + 1


def build_regions(src: Source, anchors: Optional[Sequence[int]] = None) -> list[tuple[str, int, bytes]]:
    """``[(region, byte_offset, raw_bytes), …]``.

    Always probes head, one mid anchor and tail; when ``anchors`` (big-file
    split boundaries in bytes) are given, each boundary gets a resync'd probe
    too, so every byte-range shard a giant file spans is represented.
    """
    regions: list[tuple[str, int, bytes]] = []
    if src.size <= 0:
        return regions

    head_len = min(PROBE_HEAD, src.size)
    regions.append(("head", 0, src.read_at(0, head_len)))
    head_end = head_len

    if src.size > PROBE_HEAD + PROBE_TAIL + 2 * PROBE_GAP:
        anchor = src.size // 2
        start = _resync_forward(src, anchor)
        if start is not None:
            start = max(start, head_end)
            raw = src.read_at(start, PROBE_MID)
            if raw:
                regions.append(("middle", start, raw))

    if src.size > head_end + PROBE_GAP:
        start = max(head_end + PROBE_GAP, src.size - PROBE_TAIL)
        start = _resync_forward(src, start) or start
        if start >= head_end:
            raw = src.read_at(start, src.size - start)
            if raw:
                regions.append(("tail", start, raw[:PROBE_TAIL + RESYNC_WINDOW]))

    # One resync'd probe per split boundary (deduped, capped).
    seen = {off for _, off, _ in regions}
    for n, at in enumerate(anchors or []):
        if n >= MAX_ANCHORS:
            break
        start = _resync_forward(src, int(at))
        if start is None or start < head_end or start in seen or start >= src.size:
            continue
        raw = src.read_at(start, PROBE_ANCHOR)
        if raw:
            regions.append(("anchor", start, raw))
            seen.add(start)
    return regions


def _line_starts(raw: bytes, codec: str) -> list[tuple[int, int]]:
    """``[(start_offset, content_end_offset)]`` for each line in a raw region."""
    out: list[tuple[int, int]] = [(0, 0)]
    if codec in ("utf-16-le", "utf-16-be"):
        nl = b"\n\x00" if codec == "utf-16-le" else b"\x00\n"
        pos = 0
        while True:
            i = raw.find(nl, pos)
            if i < 0:
                break
            end = i
            if raw[end - 1:end] == b"\x00" and codec == "utf-16-le":
                pass
            out[-1] = (out[-1][0], end)
            out.append((i + 2, i + 2))
            pos = i + 2
        out[-1] = (out[-1][0], len(raw))
        return [s for s in out if s[1] >= s[0]]

    pos = 0
    out = [(0, 0)]
    while True:
        i = raw.find(b"\n", pos)
        if i < 0:
            break
        end = i - 1 if i > 0 and raw[i - 1] == 0x0D else i
        out[-1] = (out[-1][0], end)
        out.append((i + 1, i + 1))
        pos = i + 1
    out[-1] = (out[-1][0], len(raw))
    return out


# ---------------------------------------------------------------------------
# Delimiter sniffing and column parsing
# ---------------------------------------------------------------------------
def sniff_delimiter(lines: list[str]) -> Optional[str]:
    """Pick the most consistent delimiter; ``'ws'`` for whitespace; else None."""
    best: Optional[str] = None
    best_score = 0
    for delim in ("\t", "|", ";", ","):
        counts = [len(ln.split(delim)) for ln in lines[:25] if ln.strip()]
        if not counts:
            continue
        multi = [c for c in counts if c >= 2]
        if len(multi) < max(2, len(counts) // 2):
            continue
        mode = max(set(multi), key=multi.count)
        agreement = sum(1 for c in counts if c == mode) / len(counts)
        if agreement > best_score:
            best_score = agreement
            best = delim
    if best is not None and best_score >= 0.6:
        return best

    # Whitespace fallback: consistent token counts across lines.
    counts = [len(ln.split()) for ln in lines[:25] if ln.strip()]
    multi = [c for c in counts if c >= 2]
    if multi and len(multi) >= max(2, len(counts) // 2):
        mode = max(set(multi), key=multi.count)
        if sum(1 for c in counts if c == mode) / len(counts) >= 0.8:
            return "ws"
    return None


def split_row(line: str, delim: Optional[str]) -> list[str]:
    if delim == "ws":
        return line.split()
    if delim:
        return [c.strip() for c in line.split(delim)]
    return [line.strip()]


def _looks_numeric(cell: str) -> bool:
    cell = cell.strip().replace(",", "")
    if not cell:
        return False
    try:
        float(cell)
        return True
    except ValueError:
        return False


def header_looks_like_header(header: list[str], rows: list[list[str]]) -> float:
    """Confidence in ``[0, 1]`` that the first row is a textual header row."""
    if len(header) < 2 or not rows:
        return 0.0
    body_cells = [c for r in rows[:30] for c in r if c.strip()]
    head_cells = [c for c in header if c.strip()]
    if not body_cells or not head_cells:
        return 0.0
    numeric_ratio = sum(1 for c in body_cells if _looks_numeric(c)) / len(body_cells)
    head_numeric = sum(1 for c in head_cells if _looks_numeric(c)) / len(head_cells)
    head_textual = 1.0 - head_numeric
    return round(max(0.0, min(1.0, 0.5 * numeric_ratio + 0.5 * head_textual)), 2)


# ---------------------------------------------------------------------------
# Shard plan: byte-balanced boundaries; a big file may span several shards
# ---------------------------------------------------------------------------
def plan_shards(sizes: list[int], n: int) -> tuple[list[list[tuple[int, int, int]]], dict[int, tuple[int, int]]]:
    """Split the byte stream into ``n`` contiguous, size-balanced shards.

    A file that straddles a boundary appears in several shards — its span is
    ``(first_shard, last_shard)`` and each membership is tagged with
    ``(file_index, split_no, split_count)`` so samples can state *which piece*
    of a big file they came from (mirrors byte-range input splits for the
    "文件很大" case).  Returns ``(bins, span_of_file)``.
    """
    n = max(1, int(n))
    bins: list[list[tuple[int, int, int]]] = [[] for _ in range(n)]
    span: dict[int, tuple[int, int]] = {}
    total = sum(sizes)
    if not sizes:
        return bins, span

    if total == 0:  # all-empty input: round-robin files across shards
        n = max(1, min(n, len(sizes)))
        bins = [[] for _ in range(n)]
        for i in range(len(sizes)):
            b = i * n // len(sizes)
            bins[b].append((i, 1, 1))
            span[i] = (b, b)
        return bins, span

    cum = 0
    for i, size in enumerate(sizes):
        start, end, cum = cum, cum + size, cum + size
        if size == 0:
            b = min(n - 1, int(start * n / total))
            bins[b].append((i, 1, 1))
            span[i] = (b, b)
            continue
        first = min(n - 1, int(start * n / total))
        last = min(n - 1, int((end - 1) * n / total))
        for b in range(first, last + 1):
            bins[b].append((i, b - first + 1, last - first + 1))
        span[i] = (first, last)
    return bins, span


def _strided_pick(items: list, k: int) -> list:
    """Choose up to ``k`` items evenly spread, always including first & last."""
    if k >= len(items):
        return list(items)
    step = (len(items) - 1) / (k - 1) if k > 1 else 0
    idx = sorted({0} | {int(round(i * step)) for i in range(1, k)} | {len(items) - 1})
    return [items[i] for i in sorted(set(idx))][:k]


# ---------------------------------------------------------------------------
# Per-file analysis
# ---------------------------------------------------------------------------
@dataclass
class FileAnalysis:
    src: Source
    scan: ScanResult
    lines: int
    lines_exact: bool
    blank_lines: int
    blank_exact: bool
    empty: bool
    codec: str = ""
    binary: bool = False
    replace_ratio: float = 0.0
    delim: Optional[str] = None
    forced_delim: bool = False
    has_header: Optional[bool] = None
    header_confidence: float = 0.0
    forced_delim_value: Optional[str] = None
    header_override: Optional[bool] = None
    histogram: dict[int, int] = field(default_factory=dict)
    candidates: list[dict] = field(default_factory=list)
    probed: bool = False
    unreadable: bool = False


def _probed_indices(total: int) -> list[int]:
    """Evenly spread file indices, always including the first and last file."""
    if total <= MAX_PROBED_FILES:
        return list(range(total))
    step = (total - 1) / (MAX_PROBED_FILES - 1)
    return sorted({int(round(i * step)) for i in range(MAX_PROBED_FILES)})


def _candidate_lines(src: Source, codec: str, anchors: Optional[Sequence[int]] = None) -> list[dict]:
    """Raw probe candidates from head/middle/tail/anchors with byte offsets."""
    out: list[dict] = []
    seen_offsets: set[int] = set()
    for region, base, raw in build_regions(src, anchors):
        if not raw:
            continue
        text = decode_region(raw, codec)
        lines = text.splitlines()
        starts = _line_starts(raw, codec)
        for line, (rel_start, rel_end) in zip(lines, starts):
            offset = base + rel_start
            if offset in seen_offsets or offset >= src.size:
                continue
            seen_offsets.add(offset)
            blank = not line.strip()
            out.append({
                "region": region,
                "offset": offset,
                "text": line.rstrip("\r")[:MAX_SAMPLE_TEXT],
                "truncated": len(line.rstrip("\r")) > MAX_SAMPLE_TEXT,
                "blank": blank,
            })
    return out


def _structure_candidates(fa: FileAnalysis) -> None:
    """Sniff delimiter / columns / header from candidates already attached."""
    nonblank = [c["text"] for c in fa.candidates if not c["blank"]]
    fa.delim = fa.forced_delim_value or sniff_delimiter(nonblank)
    for text in nonblank:
        n = len(split_row(text, fa.delim))
        fa.histogram[n] = fa.histogram.get(n, 0) + 1
    head_rows = [c for c in fa.candidates if c["region"] == "head" and not c["blank"]]
    if fa.header_override is not None:
        fa.has_header = bool(fa.header_override)
    elif fa.histogram and head_rows and max(fa.histogram) >= 2:
        header = split_row(head_rows[0]["text"], fa.delim)
        body_rows = [split_row(c["text"], fa.delim)
                     for c in fa.candidates if c is not head_rows[0] and not c["blank"]]
        conf = header_looks_like_header(header, body_rows)
        fa.header_confidence = conf
        fa.has_header = conf >= 0.65
    else:
        fa.has_header = None


def _analyze_file(src: Source, scan_budget: int,
                  delim_override: Optional[str],
                  header_override: Optional[bool]) -> FileAnalysis:
    empty = src.size == 0
    scan = scan_source(src, scan_budget)
    lines, exact = estimate_lines(src, scan)
    blanks, blank_exact = estimate_blanks(src, scan)
    fa = FileAnalysis(src=src, scan=scan, lines=lines, lines_exact=exact,
                      blank_lines=blanks, blank_exact=blank_exact, empty=empty)
    fa.forced_delim_value = delim_override
    fa.header_override = header_override
    fa.forced_delim = delim_override is not None
    if empty:
        fa.codec, fa.binary = "utf-8", False
        return fa

    head = src.read_at(0, min(PROBE_HEAD, src.size))
    codec, has_nul = detect_encoding(head)
    fa.codec = codec
    fa.binary = has_nul and codec not in ("utf-16-le", "utf-16-be")

    if fa.binary:
        return fa
    # candidates + structural analysis are filled in later (after the shard
    # plan, which supplies big-file split anchors) by preview_sources.
    return fa


# ---------------------------------------------------------------------------
# Warnings
# ---------------------------------------------------------------------------
def _w(code: str, severity: str, zh: str, en: str, files: Optional[list[str]] = None) -> dict:
    msg = f"{zh} {en}"
    if files:
        shown = files[:MAX_WARNING_FILES]
        msg += ": " + ", ".join(shown) + (f" (+{len(files) - len(shown)})" if len(files) > len(shown) else "")
    return {"severity": severity, "code": code, "message": msg,
            "files": files or []}


def _build_warnings(analyses: list[FileAnalysis], total_files: int,
                    global_delim: Optional[str], global_dom_cols: Optional[int],
                    delim_disagree: list[str],
                    lines_estimated_files: list[str],
                    not_probed: int) -> list[dict]:
    warnings: list[dict] = []
    readable = [a for a in analyses if not a.unreadable]
    nonempty = [a for a in readable if not a.empty]
    empty_files = [a.src.name for a in readable if a.empty]

    if total_files == 0 or not readable:
        warnings.append(_w("no_files", "error",
                           "未发现任何输入文件", "No input files found"))
        return warnings
    if empty_files:
        sev = "error" if not nonempty else "warn"
        warnings.append(_w("empty_file", sev,
                           f"{len(empty_files)} 个空文件",
                           f"{len(empty_files)} empty file(s)", empty_files))
    binaries = [a.src.name for a in nonempty if a.binary]
    if binaries:
        warnings.append(_w("binary_file", "error",
                           "疑似二进制文件（含 NUL 字节），无法按文本解析",
                           "Binary-looking file (NUL bytes), not text", binaries))
    garbled = [a.src.name for a in nonempty
               if not a.binary and a.replace_ratio >= 0.05]
    if garbled:
        warnings.append(_w("garbled_text", "warn",
                           "解码后出现较多替换字符，编码可能不正确（存在乱码）",
                           "Many replacement chars after decode, encoding may be wrong",
                           garbled))
    if delim_disagree:
        warnings.append(_w("delimiter_mismatch", "warn",
                           "部分文件的字段分隔符与多数文件不一致",
                           "Delimiter disagrees with the majority delimiter",
                           delim_disagree))
    col_bad: list[str] = []
    for a in nonempty:
        if a.binary or len(a.histogram) <= 1:
            continue
        total_rows = sum(a.histogram.values())
        dom = max(a.histogram, key=lambda k: a.histogram[k])
        drift = total_rows - a.histogram[dom]
        if drift >= 2 and drift / total_rows >= 0.1:
            col_bad.append(a.src.name)
    if col_bad:
        warnings.append(_w("column_mismatch", "warn",
                           "部分文件内列数与主流列数不一致",
                           "Column count drifts within a file", col_bad))
    # Cross-file: a file whose own dominant width differs from the global one.
    # Whitespace-tokenised free text is excluded (its word count is not columns).
    if global_dom_cols is not None:
        cross = [a.src.name for a in nonempty
                 if not a.binary and a.delim and a.delim != "ws" and a.histogram
                 and max(a.histogram, key=lambda k: a.histogram[k]) != global_dom_cols]
        if cross and cross != col_bad:
            warnings.append(_w("columns_cross_file", "warn",
                               f"部分文件的列数（{global_dom_cols} 列以外）与全局主流不一致",
                               f"Some files have a different width than the global mode "
                               f"({global_dom_cols} columns)", cross))
    blank_heavy = [a.src.name for a in nonempty
                   if a.lines > 0 and a.blank_lines / max(1, a.lines) >= 0.1]
    if blank_heavy:
        warnings.append(_w("blank_lines", "info",
                           "部分文件空行占比 ≥10%",
                           "≥10% blank lines in some files", blank_heavy))
    headers = [a for a in nonempty if a.has_header]
    if headers and not any(a.forced_delim for a in headers):
        warnings.append(_w("header_detected", "info",
                           f"检测到 {len(headers)} 个文件首行疑似表头，预览样本中已保留该行",
                           f"First row looks like a header in {len(headers)} file(s); "
                           "it is kept in the samples",
                           [a.src.name for a in headers]))
    if lines_estimated_files:
        warnings.append(_w("lines_estimated", "info",
                           f"{len(lines_estimated_files)} 个大文件仅扫描前缀，行数为估算值",
                           f"{len(lines_estimated_files)} large file(s) only partially scanned; "
                           "line counts are estimates",
                           lines_estimated_files))
    if not_probed:
        warnings.append(_w("partial_coverage", "info",
                           f"文件较多，仅抽样打开 {len(readable) - not_probed}/{total_files} 个文件",
                           f"Only {len(readable) - not_probed}/{total_files} files were opened "
                           "for content probes", []))
    if nonempty and global_delim is None and not any(a.binary for a in nonempty):
        warnings.append(_w("plain_text", "info",
                           "未检测到稳定字段分隔符，按纯文本行处理",
                           "No stable delimiter detected; rows treated as plain text lines"))
    if total_files >= 50:
        sizes = sorted(a.src.size for a in readable)
        if sizes[len(sizes) // 2] < 64 * 1024:
            warnings.append(_w("fragmented_input", "info",
                               f"输入由 {total_files} 个小文件组成（典型分片很碎的形态）",
                               f"Input is {total_files} small files (heavily fragmented shards)"))
    return warnings


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def preview_sources(sources: Sequence[Source], *, sample_size: int = 20,
                    num_map_tasks: int = 8, delimiter: Optional[str] = None,
                    has_header: Optional[bool] = None,
                    source_meta: Optional[dict] = None) -> dict:
    """Build the full preview report for ``sources`` without executing anything."""
    started = time.time()
    sample_size = max(1, min(int(sample_size or 20), 200))
    num_map_tasks = max(1, min(int(num_map_tasks or 8), 200))
    if delimiter:
        delimiter = DELIM_ALIASES.get(str(delimiter).strip().lower(), str(delimiter))
        if delimiter not in DELIMS:
            raise PreviewError(f"不支持的分隔符 Unsupported delimiter: {delimiter}")
    if has_header is not None and isinstance(has_header, str):
        has_header = {"auto": None, "yes": True, "true": True, "1": True,
                      "no": False, "false": False, "0": False}.get(has_header.lower())

    sources = sorted(sources, key=lambda s: s.name)
    for i, s in enumerate(sources):
        s.index = i
    total_files = len(sources)
    total_bytes = sum(s.size for s in sources)

    # ---- byte-bounded scan budget over every file ------------------------
    scan_alloc = _allocate_scan_bytes([s.size for s in sources], SCAN_BYTES_TOTAL)
    analyses: list[Optional[FileAnalysis]] = [None] * total_files
    probed = set(_probed_indices(total_files))
    for i, src in enumerate(sources):
        try:
            fa = _analyze_file(src, scan_alloc[i], delimiter, has_header)
        except OSError:
            fa = FileAnalysis(src=src, scan=ScanResult(), lines=0, lines_exact=True,
                              blank_lines=0, blank_exact=True, empty=False,
                              unreadable=True)
        fa.probed = i in probed and src.size > 0
        analyses[i] = fa
    analyses_known: list[FileAnalysis] = [a for a in analyses if a is not None]

    # ---- shard plan first (byte-balanced; big files may span shards) -----
    # It supplies the split anchors that content probes must cover.
    sizes = [s.size for s in sources]
    bins, span = plan_shards(sizes, num_map_tasks)
    shard_rows: list[dict] = []
    for si, members in enumerate(bins):
        shard_rows.append({
            "index": si,
            "files": len({fi for fi, _, _ in members}),
            "file_start": members[0][0] if members else None,
            "file_end": members[-1][0] if members else None,
            "bytes_span": 0,  # filled below
            "samples": 0,
        })
    # byte weight of each shard = width of its [start,end) window (last -> EOF)
    stream_bytes = max(1, total_bytes)
    window = stream_bytes / len(bins)
    for si, row in enumerate(shard_rows):
        row["bytes_span"] = round(window if si < len(bins) - 1
                                  else stream_bytes - window * (len(bins) - 1))

    def shard_for_offset(file_index: int, offset: int) -> int:
        first, last = span.get(file_index, (0, 0))
        if first == last:
            return first
        pos = sum(sizes[:file_index]) + offset
        return min(last, max(first, int(pos * len(bins) / stream_bytes)))

    def split_anchors(file_index: int) -> list[int]:
        first, last = span.get(file_index, (0, 0))
        if last <= first:
            return []
        size = sizes[file_index]
        return [b * size // (last - first + 1)
                for b in range(1, last - first + 1)]

    # ---- content probes + per-file structure (delimiter/header/columns) ---
    delim_votes: dict[str, int] = {}
    histogram_global: dict[int, int] = {}
    header_votes: list[FileAnalysis] = []
    cand_by_file: dict[int, list[dict]] = {}
    bytes_probed = 0
    probed_nonempty = 0
    for i, a in enumerate(analyses_known):
        if not a.probed or a.binary:
            continue
        probed_nonempty += 1
        anchors = split_anchors(i)
        a.candidates = _candidate_lines(sources[i], a.codec, anchors)
        bytes_probed += sum(len(r[2]) for r in build_regions(sources[i], anchors))
        a.replace_ratio = replacement_ratio("\n".join(c["text"] for c in a.candidates))
        _structure_candidates(a)
        cand_by_file[i] = a.candidates
        if a.delim:
            # One file, one vote: a single huge file must not out-vote many
            # structurally different shards just because it has more rows.
            delim_votes[a.delim] = delim_votes.get(a.delim, 0) + 1
        # "Whitespace" is free text rather than a structured column separator,
        # so word counts must not define the global column width.
        if a.delim and a.delim != "ws":
            for k, v in a.histogram.items():
                histogram_global[k] = histogram_global.get(k, 0) + v
        if a.has_header is not None:
            header_votes.append(a)

    global_delim = delimiter or (max(delim_votes, key=delim_votes.get) if delim_votes else None)
    delim_disagree = [a.src.name for a in analyses_known
                      if a.probed and not a.binary and a.delim
                      and global_delim and a.delim != global_delim]
    dom_cols = max(histogram_global, key=histogram_global.get) if histogram_global else None
    global_header: Optional[bool] = None
    if has_header is not None:
        global_header = has_header
    elif header_votes:
        yes = sum(1 for a in header_votes if a.has_header)
        global_header = yes * 2 >= len(header_votes)
    file_has_header = {i: bool(analyses_known[i].has_header)
                       for i in cand_by_file if analyses_known[i].has_header}
    # With many fragmented files the header repeats in every file; skip it on
    # the first pick so the preview shows actual data rows (the header is still
    # reported by the header_detected warning; single files keep it on purpose).
    skip_header_first = len(cand_by_file) > 1 and any(file_has_header.values())
    for cands in cand_by_file.values():
        for c in cands:
            c["is_header"] = False

    def mark_headers() -> None:
        for fi in file_has_header:
            head_rows = [c for c in cand_by_file.get(fi, []) if c["region"] == "head"]
            if head_rows:
                head_rows[0]["is_header"] = True

    mark_headers()

    def spread_choice(cands: list[dict], picked: list[dict], skip_header: bool,
                      file_pick_no: int = 0) -> Optional[dict]:
        """Pick one row far (in rank space) from this file's already-picked rows.

        ``file_pick_no`` rotates the starting rank for this file so identical
        fragmented shards surface different rows instead of the same first row.
        """
        if not cands:
            return None
        used_offsets = {c["offset"] for c in picked}
        avail = [c for c in cands if c["offset"] not in used]
        if skip_header:
            data_avail = [c for c in avail if not c["is_header"]]
            if data_avail:
                avail = data_avail
        if not avail:
            return None
        order = sorted(cands, key=lambda c: c["offset"])
        rank = {c["offset"]: i for i, c in enumerate(order)}
        own_picked = [c for c in picked if c["offset"] in rank]
        if not own_picked:
            # rotate the first pick across repeated shard files
            if len(avail) > 1:
                target_rank = file_pick_no % len(avail)
                return sorted(avail, key=lambda c: rank[c["offset"]])[target_rank]
            return avail[0]
        used_ranks = [rank[c["offset"]] for c in own_picked]
        best, best_gap = avail[0], -1
        for c in avail:
            r = rank[c["offset"]]
            gap = min(abs(r - u) for u in used_ranks)
            if c["region"] != "head":
                gap += 0.5  # positional spread across head/middle/tail/anchors
            if gap > best_gap:
                best, best_gap = c, gap
        return best

    # ---- sample selection: every non-empty shard is represented first ----
    ordered_shards = sorted(range(len(bins)), key=lambda si: -shard_rows[si]["bytes_span"])
    file_picks: dict[int, list[dict]] = {}
    remaining = sample_size

    def pick_file_for_shard(si: int, window_only: bool = False) -> Optional[int]:
        """Choose the least-sampled member file of ``si`` (fair round-robin)."""
        members = []
        for fi, _, _ in bins[si]:
            if fi not in cand_by_file:
                continue
            if window_only:
                used = {c["offset"] for c in file_picks.get(fi, [])}
                if not any(c["offset"] not in used and shard_for_offset(fi, c["offset"]) == si
                           for c in cand_by_file[fi]):
                    continue
            members.append(fi)
        if not members:
            return None
        return min(members, key=lambda f: (
            len(file_picks.get(f, [])),
            members.index(f) % max(1, len(members)),  # tie-break: spread across shard
            f,
        ))

    # pass 1: one spread pick per shard that owns probed files
    for si in ordered_shards:
        if remaining <= 0:
            break
        fi = pick_file_for_shard(si)
        if fi is None:
            continue
        picked = file_picks.setdefault(fi, [])
        used = {c["offset"] for c in picked}
        available = [c for c in cand_by_file[fi] if c["offset"] not in used]
        in_window = [c for c in available if shard_for_offset(fi, c["offset"]) == si]
        choice = spread_choice(in_window or available, picked, skip_header_first, sample_size - remaining)
        if choice is None:
            continue
        picked.append(choice)
        remaining -= 1
    # pass 2: distribute leftover slots round-robin over the shard windows,
    # weighted by each shard's byte size, so a giant file is sampled from
    # every byte range instead of having its head over-represented.
    shard_quota: dict[int, int] = {}
    while remaining > 0:
        eligible = [si for si in range(len(bins))
                    if shard_quota.get(si, 0) < 12
                    and pick_file_for_shard(si, window_only=True) is not None]
        if not eligible:
            break
        si = max(eligible, key=lambda k: shard_rows[k]["bytes_span"]
                 / (shard_quota.get(k, 1) + 1))
        fi = pick_file_for_shard(si, window_only=True)
        picked = file_picks.setdefault(fi, [])
        used = {c["offset"] for c in picked}
        in_window = [c for c in cand_by_file[fi]
                     if c["offset"] not in used and shard_for_offset(fi, c["offset"]) == si]
        choice = spread_choice(in_window, picked, skip_header_first, sample_size - remaining)
        if choice is None:
            shard_quota[si] = 12
            continue
        picked.append(choice)
        shard_quota[si] = shard_quota.get(si, 1) + 1
        remaining -= 1

    samples: list[dict] = []
    for fi, picks in file_picks.items():
        a = analyses_known[fi]
        first, last = span.get(fi, (0, 0))
        split_count = last - first + 1
        for c in sorted(picks, key=lambda c: c["offset"]):
            # Parse with the file's own delimiter when it disagrees with the
            # majority so a TSV shard in a CSV job still shows its columns.
            row_delim = a.delim if (a.delim and a.delim != global_delim) else global_delim
            cells = None
            if not c["blank"] and row_delim:
                cells = [cell[:MAX_CELL_TEXT] for cell in split_row(c["text"], row_delim)]
            si = shard_for_offset(fi, c["offset"])
            lineno = a.scan.offsets.get(c["offset"])
            row = {
                "file": a.src.name,
                "file_index": fi,
                "shard_index": si,
                "offset": c["offset"],
                "lineno": lineno,
                "region": c["region"],
                "blank": c["blank"],
                "is_header": bool(c.get("is_header")),
                "raw": c["text"],
                "truncated": c["truncated"],
                "columns": (len(cells) if cells is not None else None),
                "cells": cells,
            }
            if split_count > 1:
                row["split_hint"] = "{}/{}".format(si - first + 1, split_count)
            samples.append(row)
            shard_rows[si]["samples"] += 1
    samples.sort(key=lambda r: (r["file_index"], r["offset"]))

    # ---- per-file rows (capped; stats still cover every file) -------------
    lines_total = sum(a.lines for a in analyses_known)
    blanks_total = sum(a.blank_lines for a in analyses_known)
    lines_exact_all = all(a.lines_exact for a in analyses_known)
    blanks_exact_all = all(a.blank_exact for a in analyses_known)
    files_rows = []
    for a in analyses_known[:MAX_LISTED_FILES]:
        files_rows.append({
            "index": a.src.index,
            "name": a.src.name,
            "bytes": a.src.size,
            "lines": a.lines,
            "lines_exact": a.lines_exact,
            "blank_lines": a.blank_lines,
            "sampled": a.probed,
            "empty": a.empty,
            "unreadable": a.unreadable,
            "binary": a.binary,
            "encoding": _codec_label(a.codec),
            "delimiter": a.delim if not a.binary else None,
            "columns_dominant": (max(a.histogram, key=lambda k: a.histogram[k])
                                  if a.histogram else None),
            "columns_histogram": {str(k): v for k, v in sorted(a.histogram.items())},
            "header": a.has_header,
            "header_confidence": a.header_confidence,
            "shard_index": "{}–{}".format(*span[a.src.index])
                             if a.src.index in span and span[a.src.index][0] != span[a.src.index][1]
                             else str(span.get(a.src.index, (0, 0))[0]),
        })

    est_files = [a.src.name for a in analyses_known if not a.lines_exact and not a.empty]
    warnings = _build_warnings(analyses_known, total_files, global_delim, dom_cols,
                               delim_disagree, est_files,
                               not_probed=total_files - probed_nonempty
                               - sum(1 for a in analyses_known if a.empty or a.binary))

    codecs = {a.codec for a in analyses_known if not a.empty and a.codec}
    shards_with_samples = sum(1 for sh in shard_rows if sh["samples"] > 0)
    return {
        "ok": True,
        "read_only": True,
        "elapsed_ms": round((time.time() - started) * 1000),
        "source": source_meta or {"mode": "memory"},
        "params": {
            "sample_size": sample_size,
            "num_map_tasks": num_map_tasks,
            "delimiter": global_delim,
            "delimiter_label": DELIMS.get(global_delim) if global_delim else None,
            "has_header": global_header,
        },
        "summary": {
            "files_total": total_files,
            "files_listed": len(files_rows),
            "files_truncated": total_files > len(files_rows),
            "files_empty": sum(1 for a in analyses_known if a.empty),
            "files_unreadable": sum(1 for a in analyses_known if a.unreadable),
            "files_binary": sum(1 for a in analyses_known if a.binary),
            "bytes_total": total_bytes,
            "lines_total": lines_total,
            "lines_exact": lines_exact_all,
            "blank_lines_total": blanks_total,
            "blank_exact": blanks_exact_all,
            "columns_dominant": dom_cols,
            "columns_histogram": {str(k): v for k, v in sorted(histogram_global.items())},
            "encodings": sorted({_codec_label(c) for c in codecs}),
        },
        "coverage": {
            "files_opened": probed_nonempty,
            "files_total": total_files,
            "files_pct": round(100 * probed_nonempty / total_files, 1) if total_files else 0.0,
            "bytes_probed": bytes_probed,
            "bytes_total": total_bytes,
            "bytes_pct": round(100 * bytes_probed / total_bytes, 4) if total_bytes else 0.0,
            "shards_total": len(shard_rows),
            "shards_with_samples": shards_with_samples,
            "shards_pct": round(100 * shards_with_samples / len(shard_rows), 1) if shard_rows else 0.0,
            "shards": shard_rows,
        },
        "files": files_rows,
        "samples": samples,
        "warnings": warnings,
    }


def _codec_label(codec: str) -> str:
    return {"utf-8-sig": "utf-8 (BOM)", "utf-16-le": "utf-16 LE",
            "utf-16-be": "utf-16 BE"}.get(codec, codec or "unknown")
