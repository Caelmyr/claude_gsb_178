"""Tests for the read-only pre-submission input sampler (数据抽样预览)."""

import os
import shutil
import tempfile
import unittest

from backend.common.input_sampler import (
    PreviewError,
    discover_file_sources,
    memory_sources,
    plan_shards,
    preview_sources,
    sniff_delimiter,
)


def write(root, rel, content, binary=False):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "wb" if binary else "w"
    with open(path, mode) as f:
        f.write(content)
    return path


class TestDiscovery(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        write(self.root, "a/1.csv", "x\n")
        write(self.root, "a/2.csv", "x\n")
        write(self.root, "b/3.tsv", "x\n")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_directory_walk_is_sorted(self):
        srcs = discover_file_sources(".", [self.root])
        names = [s.name for s in srcs]
        self.assertEqual(names, sorted(names))
        self.assertEqual(len(names), 3)

    def test_single_file_and_relative(self):
        srcs = discover_file_sources("a/1.csv", [self.root])
        self.assertEqual(len(srcs), 1)
        self.assertEqual(srcs[0].size, 2)

    def test_glob(self):
        srcs = discover_file_sources("**/*.tsv", [self.root])
        self.assertEqual([s.name for s in srcs], ["b/3.tsv"])

    def test_missing_path(self):
        with self.assertRaises(PreviewError):
            discover_file_sources("nope", [self.root])

    def test_empty_target(self):
        with self.assertRaises(PreviewError):
            discover_file_sources("  ", [self.root])

    def test_path_escape_rejected(self):
        with self.assertRaises(PreviewError):
            discover_file_sources("../../etc/passwd", [self.root])
        with self.assertRaises(PreviewError):
            discover_file_sources("/etc/passwd", [self.root])

    def test_symlink_escape_rejected(self):
        link = os.path.join(self.root, "evil")
        os.symlink("/etc", link)
        with self.assertRaises(PreviewError):
            discover_file_sources("evil/passwd", [self.root])


class TestDelimiterSniff(unittest.TestCase):
    def test_detects_tab_comma_pipe(self):
        self.assertEqual(sniff_delimiter(["a,b,c", "1,2,3", "x,y,z"]), ",")
        self.assertEqual(sniff_delimiter(["a\tb", "1\t2"]), "\t")
        self.assertEqual(sniff_delimiter(["a|b|c", "1|2|3"]), "|")
        self.assertEqual(sniff_delimiter(["a b c", "1 2 3", "x y z"]), "ws")

    def test_plain_text_returns_none(self):
        self.assertIsNone(sniff_delimiter(["just one column", "another line"]))

    def test_inconsistent_rows_are_weak(self):
        # mostly single commas (two columns) with one long comma line: still comma
        self.assertEqual(sniff_delimiter(["a,b", "c,d", "1,2,3,4,5"]), ",")


class TestBasicPreview(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_csv_with_header(self):
        write(self.root, "d.csv", "id,name,score\n1,a,10\n2,b,20\n3,c,30\n")
        r = preview_sources(discover_file_sources("d.csv", [self.root]),
                            sample_size=10)
        self.assertTrue(r["read_only"])
        self.assertEqual(r["summary"]["files_total"], 1)
        self.assertEqual(r["summary"]["lines_total"], 4)
        self.assertTrue(r["summary"]["lines_exact"])
        self.assertEqual(r["summary"]["columns_dominant"], 3)
        self.assertEqual(r["params"]["delimiter"], ",")
        self.assertTrue(r["params"]["has_header"])
        # header row is kept and flagged
        self.assertTrue(any(s["is_header"] for s in r["samples"]))
        # cells are parsed
        data = [s for s in r["samples"] if not s["is_header"]][0]
        self.assertEqual(data["cells"], ["1", "a", "10"])

    def test_crlf_and_no_trailing_newline(self):
        write(self.root, "c.csv", b"a,b,c\r\n1,2,3\r\n4,5,6", binary=True)
        r = preview_sources(discover_file_sources("c.csv", [self.root]))
        self.assertEqual(r["summary"]["lines_total"], 3)
        self.assertEqual(len(r["samples"]), 3)

    def test_blank_lines_counted(self):
        write(self.root, "b.txt", "x\n\n\ny\n")
        r = preview_sources(discover_file_sources("b.txt", [self.root]))
        self.assertEqual(r["summary"]["blank_lines_total"], 2)

    def test_empty_file_is_error_warning(self):
        write(self.root, "e", "")
        r = preview_sources(discover_file_sources("e", [self.root]))
        self.assertEqual(r["summary"]["files_empty"], 1)
        self.assertIn("empty_file", [w["code"] for w in r["warnings"]])
        self.assertEqual(any(w["severity"] == "error" for w in r["warnings"]), True)

    def test_empty_directory(self):
        r = preview_sources(discover_file_sources(self.root, [self.root]))
        self.assertIn("no_files", [w["code"] for w in r["warnings"]])

    def test_binary_file_flagged(self):
        write(self.root, "x.bin", bytes(range(256)) * 2, binary=True)
        r = preview_sources(discover_file_sources("x.bin", [self.root]))
        self.assertTrue(r["summary"]["files_binary"], 1)
        self.assertIn("binary_file", [w["code"] for w in r["warnings"]])

    def test_column_mismatch_warning(self):
        write(self.root, "m.csv", "a,b,c\n1,2,3\n4,5\n6,7,8\n9,10,11,12\n")
        r = preview_sources(discover_file_sources("m.csv", [self.root]))
        codes = [w["code"] for w in r["warnings"]]
        self.assertIn("column_mismatch", codes)

    def test_mixed_delimiters_flagged(self):
        write(self.root, "a.csv", "a,b,c\n1,2,3\n")
        write(self.root, "b.tsv", "a\tb\tc\n1\t2\t3\n")
        r = preview_sources(discover_file_sources(".", [self.root]))
        self.assertIn("delimiter_mismatch", [w["code"] for w in r["warnings"]])

    def test_forced_delimiter_and_header(self):
        write(self.root, "x.txt", "1;2;3\n4;5;6\n")
        r = preview_sources(discover_file_sources("x.txt", [self.root]),
                            delimiter=";", has_header="no")
        self.assertEqual(r["params"]["delimiter"], ";")
        self.assertFalse(r["params"]["has_header"])
        self.assertEqual(r["samples"][0]["cells"], ["1", "2", "3"])

    def test_bad_delimiter_rejected(self):
        write(self.root, "x.txt", "a\n")
        with self.assertRaises(PreviewError):
            preview_sources(discover_file_sources("x.txt", [self.root]), delimiter="xx")


class TestEncoding(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_utf8_bom(self):
        write(self.root, "u.csv", "﻿a,b\n1,2\n", binary=False)
        r = preview_sources(discover_file_sources("u.csv", [self.root]))
        self.assertIn("utf-8", r["summary"]["encodings"][0])

    def test_gbk_decoded(self):
        write(self.root, "g.txt", "中文行一\n中文行二\n数字123\n".encode("gbk"),
              binary=True)
        r = preview_sources(discover_file_sources("g.txt", [self.root]))
        self.assertIn("gb18030", r["summary"]["encodings"])
        self.assertTrue(any("中文" in s["raw"] for s in r["samples"]))
        self.assertFalse(any(w["code"] == "garbled_text" for w in r["warnings"]))

    def test_utf16le_without_bom(self):
        write(self.root, "u16.txt", "a,b,c\n1,2,3\n".encode("utf-16-le"), binary=True)
        r = preview_sources(discover_file_sources("u16.txt", [self.root]))
        self.assertFalse(r["files"][0]["binary"])
        self.assertTrue(r["files"][0]["encoding"].startswith("utf-16"))
        self.assertEqual(r["summary"]["columns_dominant"], 3)
        self.assertEqual(r["samples"][0]["cells"], ["a", "b", "c"])

    def test_text_with_stray_control_byte_not_binary(self):
        write(self.root, "t.txt", "good line\ttab\nok\x01oops\n" * 6)
        r = preview_sources(discover_file_sources("t.txt", [self.root]))
        self.assertFalse(r["files"][0]["binary"])


class TestScaleAndCoverage(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_many_fragmented_shards_cover_every_shard(self):
        os.makedirs(os.path.join(self.root, "shards"))
        for i in range(120):
            write(self.root, "shards/part-{:04d}.csv".format(i),
                  "id,name,v\n{},a,{}\n2,b,2\n3,c,3\n".format(i, i))
        r = preview_sources(discover_file_sources("shards", [self.root]),
                            sample_size=24, num_map_tasks=8)
        cov = r["coverage"]
        self.assertEqual(cov["shards_with_samples"], cov["shards_total"])
        # not every tiny file is opened, but first/last/strided are
        self.assertLess(cov["files_opened"], cov["files_total"])
        self.assertIn("partial_coverage", [w["code"] for w in r["warnings"]])
        self.assertIn("fragmented_input", [w["code"] for w in r["warnings"]])
        # samples spread across distinct files and rows
        self.assertGreater(len({s["file"] for s in r["samples"]}), 8)

    def test_big_file_spans_shards_and_is_bounded(self):
        # ~3 MB file: scanning must be bounded but every shard gets a sample
        path = os.path.join(self.root, "big.log")
        line = "2026 INFO map reduce shard worker heartbeat ok value=12345\n"
        with open(path, "w") as f:
            for i in range(70000):
                f.write(line)
        size = os.path.getsize(path)
        r = preview_sources(discover_file_sources("big.log", [self.root]),
                            sample_size=16, num_map_tasks=8)
        cov = r["coverage"]
        self.assertEqual(cov["shards_with_samples"], 8)
        # far less than 1 % of the bytes are actually read
        self.assertLess(cov["bytes_pct"], 1.0)
        # line count is an estimate, and the tail is represented
        self.assertFalse(r["summary"]["lines_exact"])
        offsets = [s["offset"] for s in r["samples"]]
        self.assertGreater(max(offsets), size * 0.9)
        regions = {s["region"] for s in r["samples"]}
        self.assertIn("tail", regions)
        # split hints 1/8 .. 8/8 all appear
        hints = {s["split_hint"] for s in r["samples"] if s.get("split_hint")}
        self.assertEqual(len(hints), 8)

    def test_preview_is_read_only(self):
        write(self.root, "a.csv", "a,b\n1,2\n")
        before = os.path.getmtime(os.path.join(self.root, "a.csv"))
        preview_sources(discover_file_sources("a.csv", [self.root]))
        self.assertEqual(os.path.getmtime(os.path.join(self.root, "a.csv")), before)


class TestShardPlan(unittest.TestCase):
    def test_empty_sizes_round_robin(self):
        bins, span = plan_shards([0, 0, 0, 0], 2)
        assigned = [m[0] for b in bins for m in b]
        self.assertEqual(sorted(assigned), [0, 1, 2, 3])

    def test_big_file_spans_multiple_shards(self):
        bins, span = plan_shards([10, 1000, 10], 4)
        first, last = span[1]
        self.assertGreater(last - first, 1)
        # every shard window touches the big file
        self.assertEqual(len(bins), 4)

    def test_small_files_packed_in_order(self):
        bins, span = plan_shards([1, 1, 1, 1], 2)
        order = [m[0] for b in bins for m in b]
        self.assertEqual(order, sorted(order))


class TestMemorySources(unittest.TestCase):
    def test_paste_preview(self):
        r = preview_sources(memory_sources([("p.tsv", b"a\tb\tc\n1\t2\t3\n")]),
                            sample_size=5)
        self.assertEqual(r["source"]["mode"], "memory")
        self.assertEqual(r["summary"]["columns_dominant"], 3)
        self.assertEqual(r["params"]["delimiter"], "\t")

    def test_multiple_uploads(self):
        r = preview_sources(memory_sources([
            ("a.csv", b"x,y\n1,2\n"),
            ("b.csv", b"x,y\n3,4\n"),
        ]))
        self.assertEqual(r["summary"]["files_total"], 2)
        files = {s["file"] for s in r["samples"]}
        self.assertEqual(files, {"a.csv", "b.csv"})

    def test_empty_blob(self):
        r = preview_sources(memory_sources([("e", b"")]))
        self.assertEqual(r["summary"]["files_empty"], 1)


if __name__ == "__main__":
    unittest.main()
