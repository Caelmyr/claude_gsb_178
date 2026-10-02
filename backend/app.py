"""Command-line entry point.

Run any role directly, or boot a whole cluster with one command:

    python -m backend.app master   --port 8000 --data data
    python -m backend.app worker   --master http://127.0.0.1:8000 --port 8001
    python -m backend.app demo     --workers 3 [--fault]

``demo`` starts a Master subprocess plus N real Worker **processes**, waits for
them to register, submits a sample job, and keeps everything in the foreground
so Ctrl-C tears the cluster down cleanly.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

from backend.common.http_client import HttpClient
from backend.common.ids import new_id


def cmd_preview(args) -> None:
    """Read-only data sampling preview without booting a cluster or running a job."""
    from backend.common.input_sampler import (
        PreviewError, discover_file_sources, memory_sources, preview_sources,
    )

    data_root = os.path.abspath(args.data or "data")
    roots = [os.path.join(data_root, "inputs")]
    os.makedirs(roots[0], exist_ok=True)
    if args.allow:
        roots.extend(os.path.abspath(p) for p in args.allow.split(os.pathsep) if p)
    if args.text is not None:
        sources = memory_sources([(args.name or "pasted-input.txt", args.text.encode("utf-8"))])
        meta = {"mode": "text", "name": args.name or "pasted-input.txt"}
    else:
        try:
            sources = discover_file_sources(args.path or ".", roots)
        except PreviewError as exc:
            print(f"[preview] {exc}")
            sys.exit(2)
        meta = {"mode": "path", "path": args.path or ".", "allowed_roots": roots}

    result = preview_sources(
        sources,
        sample_size=args.samples,
        num_map_tasks=args.map_tasks,
        delimiter=args.delimiter or None,
        has_header={"yes": True, "no": False, "auto": None}.get(args.header, None),
        source_meta=meta,
    )
    s, c = result["summary"], result["coverage"]
    print("== 数据抽样预览 Input preview (只读 read-only) ==")
    print(f"文件 files: {s['files_total']}  大小 bytes: {s['bytes_total']}  "
          f"行数 lines: {s['lines_total']}{'' if s['lines_exact'] else ' (估算 est.)'}  "
          f"空行 blank: {s['blank_lines_total']}")
    print(f"主流列数 dominant columns: {s['columns_dominant']}  "
          f"分隔符 delimiter: {result['params']['delimiter_label'] or 'plain text'}  "
          f"表头 header: {result['params']['has_header']}  编码 encodings: {s['encodings']}")
    print(f"覆盖 coverage: 打开 {c['files_opened']}/{c['files_total']} 文件, "
          f"抽样 {c['bytes_probed']} 字节 ({c['bytes_pct']}%), "
          f"分片 {c['shards_with_samples']}/{c['shards_total']}")
    for w in result["warnings"]:
        print(f"  [{w['severity'].upper():5}] {w['code']}: {w['message']}")
    print(f"-- 样本 samples ({len(result['samples'])}) --")
    for rec in result["samples"]:
        loc = f"sh{rec['shard_index']}"
        if rec.get("split_hint"):
            loc += f" {rec['split_hint']}"
        line = f"L{rec['lineno']}" if rec["lineno"] else f"@{rec['offset']}"
        cols = f" [{rec['columns']}列]" if rec["columns"] else (" [空行 blank]" if rec["blank"] else "")
        print(f"  {loc:>8} {rec['region']:<6} {line:>9} {rec['file']}: {rec['raw'][:100]}{cols}")


# ---------------------------------------------------------------------------
def _base_data_root(args) -> str:
    return os.path.abspath(getattr(args, "data", None) or "data")


def cmd_master(args) -> None:
    from backend.master.server import Master

    master = Master(_base_data_root(args), host=args.host, port=args.port)
    master.start()
    print(f"[master] listening on http://{args.host}:{args.port}  (frontend at "
          f"http://127.0.0.1:{args.port}/)")
    master.serve()


def cmd_worker(args) -> None:
    from backend.common.config import ClusterConfig
    from backend.worker.server import WorkerServer

    worker_id = new_id("worker")
    data_root = os.path.join(_base_data_root(args), "workers", worker_id)
    config = ClusterConfig(demo_mode=bool(args.demo))
    server = WorkerServer(
        data_root=data_root,
        master_url=args.master,
        host=args.host,
        port=args.port,
        name=args.name or f"worker-{args.port}",
        worker_id=worker_id,
        config=config,
        exec_mode=args.exec_mode,
    )
    server.start()
    print(f"[worker] {server.name} listening on http://{args.host}:{args.port} "
          f"(master {args.master})")
    server.serve()


# ---------------------------------------------------------------------------
def _wait_for(client: HttpClient, url: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if client.get_json(url, default=None) is not None:
            return True
        time.sleep(0.5)
    return False


def _submit_demo_job(client: HttpClient, master_url: str, fault: bool) -> dict:
    from backend.tasks.samples import SAMPLE_JOBS
    spec = dict(SAMPLE_JOBS[0])
    if fault:
        spec["params"] = {"simulate_failure": True}
        spec["name"] = spec["name"] + " (fault injection)"
    resp = client.post(f"{master_url}/api/jobs", spec, timeout=10.0)
    return resp.data if resp.ok else {"error": resp.status}


def cmd_demo(args) -> None:
    cwd = os.getcwd()
    py = sys.executable
    data_arg = args.data or "data"
    master_port = int(args.port)
    master_url = f"http://127.0.0.1:{master_port}"
    procs: list[subprocess.Popen] = []

    def spawn(cmd: list[str]) -> subprocess.Popen:
        proc = subprocess.Popen(cmd, cwd=cwd)
        procs.append(proc)
        return proc

    try:
        # 1. Master
        spawn([py, "-m", "backend.app", "master",
               "--port", str(master_port), "--data", data_arg, "--host", "0.0.0.0"])

        client = HttpClient(timeout=3.0, retries=2)
        if not _wait_for(client, f"{master_url}/api/overview", timeout=20.0):
            print("[demo] master failed to start")
            return

        # 2. Workers (real OS processes)
        for i in range(int(args.workers)):
            port = master_port + 1 + i
            spawn([py, "-m", "backend.app", "worker",
                   "--master", master_url, "--port", str(port),
                   "--data", data_arg, "--name", f"worker-{port}",
                   "--exec-mode", args.exec_mode])

        # 3. Wait for workers to register
        time.sleep(2.5)

        # 4. Submit a sample job (and optionally a fault-injection job)
        job = _submit_demo_job(client, master_url, fault=args.fault)
        print(f"[demo] submitted job: {job.get('job_id') if isinstance(job, dict) else job}")

        if args.jobs > 1:
            for _ in range(args.jobs - 1):
                _submit_demo_job(client, master_url, fault=False)

        print(f"[demo] cluster running — open http://127.0.0.1:{master_port}/")
        print("[demo] Ctrl-C to stop")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[demo] shutting down cluster...")
    finally:
        for proc in procs:
            proc.terminate()
        time.sleep(0.5)
        for proc in procs:
            if proc.poll() is None:
                proc.kill()


# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backend.app",
        description="Distributed MapReduce framework — Master/Worker over HTTP.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_master = sub.add_parser("master", help="run the Master")
    p_master.add_argument("--port", type=int, default=8000)
    p_master.add_argument("--host", default="0.0.0.0")
    p_master.add_argument("--data", default="data")
    p_master.set_defaults(func=cmd_master)

    p_worker = sub.add_parser("worker", help="run a Worker")
    p_worker.add_argument("--master", required=True)
    p_worker.add_argument("--port", type=int, default=8001)
    p_worker.add_argument("--host", default="0.0.0.0")
    p_worker.add_argument("--name", default="")
    p_worker.add_argument("--data", default="data")
    p_worker.add_argument("--exec-mode", choices=("process", "thread"), default="process")
    p_worker.add_argument("--demo", action="store_true", help="shrink work for quick UI demos")
    p_worker.set_defaults(func=cmd_worker)

    p_preview = sub.add_parser("preview", help="sample-preview input data without running a job")
    p_preview.add_argument("path", nargs="?", default="", help="file/dir/glob under data/inputs (or --allow roots)")
    p_preview.add_argument("--data", default="data")
    p_preview.add_argument("--allow", default="", help="extra read-only root dirs, separated by ':'")
    p_preview.add_argument("--samples", type=int, default=20)
    p_preview.add_argument("--map-tasks", type=int, default=8)
    p_preview.add_argument("--delimiter", default="", help=", | ; tab/\\t ws, or empty for auto")
    p_preview.add_argument("--header", choices=("auto", "yes", "no"), default="auto")
    p_preview.add_argument("--text", default=None, help="preview raw text instead of a path")
    p_preview.add_argument("--name", default="", help="display name for --text")
    p_preview.set_defaults(func=cmd_preview)

    p_demo = sub.add_parser("demo", help="boot a Master + N Workers and submit a job")
    p_demo.add_argument("--workers", type=int, default=3)
    p_demo.add_argument("--port", type=int, default=8000)
    p_demo.add_argument("--data", default="data")
    p_demo.add_argument("--exec-mode", choices=("process", "thread"), default="process")
    p_demo.add_argument("--fault", action="store_true", help="submit a fault-injection job")
    p_demo.add_argument("--jobs", type=int, default=1, help="number of sample jobs to submit")
    p_demo.set_defaults(func=cmd_demo)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
