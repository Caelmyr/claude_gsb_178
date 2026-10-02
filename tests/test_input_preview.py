"""Tests for the pre-submission input sampling preview.

Covers the properties the feature promises:

* head/middle/tail sampling on both small and large (capped-scan) files;
* every shard (input file) is represented and every simulated map shard is
  covered, including directories of many tiny shards;
* delimiter/header/blank-line detection and column-mismatch hints;
* empty files, binary files and non-UTF-8 (GBK) encodings are reported;
* records materialised by the preview are exactly what the planner runs when
  the job is submitted with ``input_preview_id``;
* large files are scanned bounded (line count estimated beyond the cap).
"""

import os
import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.input_preview import (
    PreviewManager,
    detect_dialect,
    detect_encoding,
    iter_raw_lines,
    shard_of,
    shard_ranges,
)
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.job_manager import JobManager


class PreviewTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.inputs = os.path.join(self.tmp, "inputs")
        os.makedirs(self.inputs)
        self.cfg = ClusterConfig()
        self.mgr = PreviewManager(Storage(self.tmp), self.cfg)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, name, content, binary=False):
        path = os.path.join(self.inputs, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        mode = "wb" if binary else "w"
        kwargs = {"encoding": "utf-8"} if not binary else {}
        with open(path, mode, **kwargs) as f:
            f.write(content)
        return path

    def create(self, paths, **kw):
        return self.mgr.create("path", paths=paths,
                               num_map_tasks=kw.pop("num_map_tasks", 8), **kw)

    @staticmethod
    def summary(sess):
        return sess["summary"]

    @staticmethod
    def files(sess):
        return {f["name"]: f for f in sess["files"]}

    @staticmethod
    def codes(sess):
        return {w["code"] for w in sess["warnings"]}


class TestRawLineReader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_offsets_and_crlf_and_bom(self):
        path = os.path.join(self.tmp, "x.csv")
        with open(path, "wb") as f:
            f.write(b"\xef\xbb\xbfa,b\r\nsecond,row\r\nthird,line\r\n")
        rows = list(iter_raw_lines(path))
        self.assertEqual([r[0] for r in rows], [1, 2, 3])
        self.assertEqual(rows[0][2], b"a,b")          # BOM stripped
        self.assertEqual(rows[1][2], b"second,row")   # CR stripped
        # seeking by the stored offset reproduces the line
        with open(path, "rb") as f:
            for raw_no, offset, expected in rows:
                f.seek(offset)
                self.assertTrue(f.readline().startswith(expected + b"\r\n"))


class TestDialectDetection(unittest.TestCase):
    def test_csv_semicolon_tab(self):
        self.assertEqual(detect_dialect(["a,b,c", "1,2,3", "4,5,6"])["delimiter"], ",")
        self.assertEqual(detect_dialect(["a;b;c", "1;2;3"])["delimiter"], ";")
        self.assertEqual(detect_dialect(["a\tb\tc", "1\t2\t3"])["delimiter"], "\t")

    def test_header_detected_only_for_structured(self):
        d = detect_dialect(["id,name,value", "1,a,10", "2,b,20"])
        self.assertTrue(d["has_header"])
        self.assertEqual(d["header"], ["id", "name", "value"])
        # numeric first row is data, not a header
        self.assertFalse(detect_dialect(["1,a,10", "2,b,20"])["has_header"])
        # whitespace splits tokens but a prose first row is never a header
        d = detect_dialect(["one two three", "four five six"])
        self.assertEqual(d["delimiter"], " ")
        self.assertFalse(d["has_header"])

    def test_jsonl(self):
        d = detect_dialect(['{"key": "a", "value": 1}', '{"key": "b", "value": 2}'])
        self.assertEqual(d["delimiter"], "jsonl")
        self.assertEqual(set(d["header"]), {"key", "value"})


class TestEncoding(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_utf8_bom_and_gbk(self):
        p1 = os.path.join(self.tmp, "u.txt")
        with open(p1, "wb") as f:
            f.write("﻿hello".encode("utf-8"))
        self.assertEqual(detect_encoding(p1)[0], "utf-8-sig")
        p2 = os.path.join(self.tmp, "g.txt")
        with open(p2, "wb") as f:
            f.write("分布式计算".encode("gb18030"))
        enc, _ = detect_encoding(p2)
        with open(p2, "rb") as f:
            self.assertEqual(f.read().decode(enc), "分布式计算")


class TestShardRanges(unittest.TestCase):
    def test_even_split_sizes(self):
        ranges = shard_ranges(10, 3)
        sizes = [c for _, c in ranges]
        self.assertEqual(sorted(sizes), [3, 3, 4])
        self.assertEqual(sum(s for _, s in ranges), 10)
        # every position maps to exactly one non-empty shard
        for i in range(10):
            self.assertIsNotNone(shard_of(i, ranges))

    def test_empty(self):
        self.assertEqual([c for _, c in shard_ranges(0, 4)], [0, 0, 0, 0])
        self.assertIsNone(shard_of(0, shard_ranges(0, 4)))


class TestPreviewShapeAndHints(PreviewTestBase):
    def test_empty_and_binary_files_are_errors(self):
        self.write("empty.txt", "")
        self.write("blob.bin", b"\x00\x01\x02\x00binary\x00data", binary=True)
        sess = self.create(["empty.txt", "blob.bin"])
        codes = self.codes(sess)
        self.assertIn("empty_file", codes)
        self.assertIn("binary_file", codes)
        self.assertEqual(self.summary(sess)["readable_files"], 0)

    def test_column_mismatch_is_flagged(self):
        self.write("a.csv",
                   "id,name,value\n" + "\n".join(f"{i},n{i},{i}" for i in range(20))
                   + "\n99,onlytwo\n")
        sess = self.create(["a.csv"])
        self.assertIn("column_mismatch", self.codes(sess))

    def test_blank_lines_and_header_info(self):
        self.write("b.txt", "a,b,c\n1,2,3\n\n4,5,6\n\n\n7,8,9\n")
        sess = self.create(["b.txt"])
        codes = self.codes(sess)
        self.assertIn("header_detected", codes)
        self.assertTrue("blank_lines_present" in codes or "blank_lines_high" in codes)
        # header and blank lines do not become records by default
        self.assertEqual(self.summary(sess)["total_records"], 3)

    def test_inconsistent_delimiters_across_shards(self):
        self.write("x.csv", "a,b,c\n1,2,3\n4,5,6\n")
        self.write("y.csv", "a;b;c\n1;2;3\n4;5;6\n")
        sess = self.create(["x.csv", "y.csv"])
        self.assertIn("inconsistent_delimiters", self.codes(sess))

    def test_gbk_file_decodes_and_samples_are_legible(self):
        self.write("g.txt", "分布式\n单词统计\n".encode("gb18030"), binary=True)
        sess = self.create(["g.txt"])
        self.assertEqual(self.summary(sess)["encoding"], "gb18030")
        self.assertNotIn("encoding_garbled", self.codes(sess))
        self.assertIn("分布式", [s["text"] for s in sess["samples"]])

    def test_garbled_utf8_warns(self):
        # bytes that decode under latin-1 but include U+FFFD under utf-8 replace
        with open(os.path.join(self.inputs, "bad.txt"), "wb") as f:
            f.write(b"valid \xc3\x28 broken \xc3\x28 line\n" * 4)
        sess = self.create(["bad.txt"])
        self.assertIn("encoding_garbled", self.codes(sess))


class TestSamplingPositionsAndCoverage(PreviewTestBase):
    def test_head_middle_tail_present_for_large_file(self):
        self.write("big.txt",
                   "".join(f"record line number {i}\n" for i in range(2000)))
        sess = self.create(["big.txt"], num_map_tasks=5)
        regions = {s["region"] for s in sess["samples"] if s["file"] == "big.txt"}
        self.assertIn("head", regions)
        # small files (< scan cap) get exact middle/tail via the decoded buffer
        self.assertTrue({"middle", "tail"} & regions)

    def test_large_file_beyond_scan_cap_is_estimated_and_still_sampled(self):
        # ~45 MB of text forces the bounded-scan path; each line carries its
        # number so the tail sample proves we really reached the file's end.
        line_n = 46
        self.write("huge.txt",
                   "".join(f"distributed shuffle partition word number {i} " + ("x" * max(0, line_n - 55)) + "\n"
                           for i in range(900_000)))
        sess = self.create(["huge.txt"], num_map_tasks=8)
        files = self.files(sess)
        self.assertFalse(files["huge.txt"]["line_count_exact"])
        self.assertIn("line_count_estimated", self.codes(sess))
        regions = {s["region"] for s in sess["samples"] if s["file"] == "huge.txt"}
        self.assertIn("middle", regions)
        self.assertIn("tail", regions)
        # tail sample really comes from the end of the file, and even though
        # its line number was estimated it resolves to a materialised record
        tails = [s for s in sess["samples"] if s["file"] == "huge.txt" and s["region"] == "tail"]
        self.assertTrue(tails)
        self.assertIn("899999", tails[-1]["text"])
        self.assertIsNotNone(tails[-1]["global_index"])

    def test_many_tiny_shards_are_all_represented_and_all_map_shards_covered(self):
        shard_dir = os.path.join(self.inputs, "parts")
        os.makedirs(shard_dir)
        for i in range(40):
            with open(os.path.join(shard_dir, f"part-{i:02d}.csv"), "w") as f:
                f.write("k,v\n")
                for j in range(3):
                    f.write(f"{i}-{j},{i*10+j}\n")
        sess = self.create(["parts"], num_map_tasks=10)
        sampled_files = {s["file"] for s in sess["samples"]}
        # every tiny shard got at least its head sampled
        self.assertEqual(len(sampled_files), 40)
        # coverage fill made every map shard represented
        self.assertTrue(sess["coverage_complete"])
        self.assertTrue(all(s["covered"] for s in sess["shards"]))
        covered_shards = {s["shard"] for s in sess["samples"] if s["shard"] is not None}
        self.assertEqual(covered_shards, set(range(10)))

    def test_blank_and_header_samples_are_shown_but_marked_skipped(self):
        # The preview promises to show blanks (so the anomaly is visible), yet
        # those rows must not become records / map to a shard.
        self.write("c.csv", "h1,h2,h3\n\n1,2,3\n\n4,5,6\n")
        sess = self.create(["c.csv"], skip_blank=True)
        by_line = {s["raw_line"]: s for s in sess["samples"]}
        self.assertTrue(by_line[2]["blank"])
        self.assertIsNone(by_line[2]["global_index"])
        self.assertIsNone(by_line[2]["shard"])
        # header row is shown but also skipped from records
        self.assertIsNone(by_line[1]["global_index"])
        # a real data row is mapped to a shard
        self.assertIsNotNone(by_line[3]["shard"])
        self.assertEqual(self.summary(sess)["total_records"], 2)

    def test_sample_provenance_matches_materialised_records(self):
        self.write("a.txt", "head one\nmid two\ntail three\n")
        sess = self.create(["a.txt"], num_map_tasks=3)
        records = self.mgr.load_records(sess["preview_id"])
        for sm in sess["samples"]:
            if sm["global_index"] is None:
                continue
            self.assertEqual(records[sm["global_index"]], sm["text"])
            self.assertEqual(shard_of(sm["global_index"], shard_ranges(len(records), 3)),
                             sm["shard"])


class TestPreviewSessionSources(PreviewTestBase):
    def test_paste_source(self):
        sess = self.mgr.create("paste", paste_text="one two three\nfour five six\n",
                               num_map_tasks=2)
        self.assertEqual(self.summary(sess)["total_records"], 2)
        self.assertTrue(sess["coverage_complete"])

    def test_upload_source(self):
        sess = self.mgr.create(
            "files",
            uploads=[("u.csv", b"a,b,c\n1,2,3\n4,5,6\n"),
                     ("empty", b"")],
            num_map_tasks=2,
        )
        self.assertEqual(self.summary(sess)["files"], 2)
        self.assertEqual(self.summary(sess)["total_records"], 2)
        self.assertIn("empty_file", self.codes(sess))

    def test_synthetic_kv_source(self):
        sess = self.mgr.create("synthetic", synthetic_kind="kv", rows=50,
                               num_map_tasks=4)
        self.assertEqual(self.summary(sess)["total_records"], 50)
        self.assertTrue(sess["coverage_complete"])
        rec0 = self.mgr.load_records(sess["preview_id"])[0]
        self.assertIsInstance(rec0, dict)
        self.assertIn("key", rec0)

    def test_path_escaping_is_refused(self):
        with self.assertRaises(ValueError):
            self.mgr.resolve_path("../../etc/passwd")

    def test_truncation_warning_over_record_cap(self):
        from backend.common import input_preview as ip
        old = ip.MAX_RECORDS
        ip.MAX_RECORDS = 100
        try:
            self.write("many.txt", "".join(f"line {i}\n" for i in range(500)))
            sess = self.create(["many.txt"])
        finally:
            ip.MAX_RECORDS = old
        self.assertIn("records_truncated", self.codes(sess))
        self.assertEqual(self.summary(sess)["total_records"], 100)


class TestPreviewThenSubmit(PreviewTestBase):
    def test_job_runs_exactly_previewed_records(self):
        self.write("data.txt",
                   "".join(f"map reduce shuffle word {i}\n" for i in range(600)))
        sess = self.create(["data.txt"], num_map_tasks=6)

        jm = JobManager(Storage(self.tmp), self.cfg, LogBus(Storage(self.tmp)))
        job = jm.submit({
            "name": "pv-job", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 6, "num_reduce_tasks": 2, "input_rows": 600,
            "input_preview_id": sess["preview_id"], "params": {},
        })
        self.assertEqual(job.status, "MAP")
        self.assertEqual(job.stats["input_source"], "preview")
        self.assertEqual(job.stats["total_records"], 600)
        self.assertEqual(job.input_rows, 600)
        total = sum(
            len(jm.planner.load_input_shard(job.job_id, t.input_shard))
            for t in jm.tasks_for(job.job_id, "map")
        )
        self.assertEqual(total, 600)
        # shard counts agree with the preview's coverage simulation
        planned = [len(jm.planner.load_input_shard(job.job_id, f"in-{i:04d}"))
                   for i in range(6)]
        previewed = [s["count"] for s in sess["shards"]]
        self.assertEqual(planned, previewed)

    def test_unknown_preview_id_rejected(self):
        jm = JobManager(Storage(self.tmp), self.cfg, LogBus(Storage(self.tmp)))
        with self.assertRaises(ValueError):
            jm.submit({
                "name": "x", "mapper": "wordcount_mapper", "reducer": "count_reducer",
                "num_map_tasks": 2, "num_reduce_tasks": 1, "input_rows": 10,
                "params": {"input_preview_id": "pv-does-not-exist"},
            })

    def test_skip_blank_option_changes_records(self):
        self.write("blanks.txt", "a\n\nb\n\nc\n")
        keep = self.create(["blanks.txt"], skip_blank=False)
        drop = self.create(["blanks.txt"], skip_blank=True)
        self.assertEqual(self.summary(keep)["total_records"], 5)
        self.assertEqual(self.summary(drop)["total_records"], 3)


if __name__ == "__main__":
    unittest.main()
