"""Reproduce a paired fresh-process video-adapter allocation diagnostic.

Run prepare once, then before/after in separate Python processes sharing the
explicit local workspace. The workspace manifest contains local fixture paths;
publish metadata-projected reports rather than the manifest. This diagnostic
never rewrites the optimizer's primary evidence or relaxes its resource gates.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import gc
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import threading
import time

from benchmarks.real_world import media
from benchmarks.real_world.resource_sampling import RSSSampler

DEFAULT_BEFORE_SOURCE = Path("benchmarks/results/optimizer/reliability/media_before_streaming.py")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load_old(path):
    spec = importlib.util.spec_from_file_location("old_media_video", path)
    if spec is None or spec.loader is None:
        raise ValueError("before source must be an importable Python source file")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def census():
    import psutil
    process = psutil.Process()
    return dict(parent_rss_bytes=process.memory_info().rss,
                thread_count=process.num_threads(),
                python_threads=len(threading.enumerate()),
                descendant_count=len(process.children(recursive=True)),
                fd_count=process.num_fds() if hasattr(process, "num_fds") else None)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["prepare", "before", "after"], required=True)
    parser.add_argument("--workspace", type=Path, required=True,
                        help="local directory shared by prepare and both fresh-process modes")
    parser.add_argument("--before-source", type=Path, default=DEFAULT_BEFORE_SOURCE)
    parser.add_argument("--output", type=Path, help="required for before/after")
    args = parser.parse_args(argv)
    args.workspace = args.workspace.resolve()
    args.before_source = args.before_source.resolve()
    if args.mode != "prepare" and args.output is None:
        parser.error("--output is required for before/after")
    if args.output is not None:
        args.output = args.output.resolve()
        protected = {args.before_source, Path(media.__file__).resolve(),
                     args.workspace / "video-inputs.json"}
        if args.output in protected:
            parser.error("--output must not overwrite source or the fixture manifest")
    return args


def run(args):
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])
    args.workspace.mkdir(parents=True, exist_ok=True)
    old = load_old(args.before_source)
    manifest = args.workspace / "video-inputs.json"
    if args.mode == "prepare":
        tasks = old.prepare("video", args.workspace / "fixtures", 8, 731029,
                            scale="representative")
        expected = [old.run_task(task) for task in tasks]
        manifest.write_text(json.dumps(dict(tasks=tasks, expected=expected), indent=2) + "\n")
        print(json.dumps(dict(tasks=len(tasks),
                              expected_digest=sha(json.dumps(expected, sort_keys=True).encode()))),
              flush=True)
        return 0
    data = json.loads(manifest.read_text())
    tasks, expected = data["tasks"], data["expected"]
    fn = old.run_task if args.mode == "before" else media.run_task
    source = args.before_source if args.mode == "before" else Path(media.__file__)
    gc.collect()
    baseline = census()
    correct, elapsed = [], []
    with RSSSampler(interval=.01) as rss:
        for repeat in range(3):
            start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=2) as pool:
                actual = list(pool.map(fn, tasks))
            elapsed.append(time.perf_counter() - start)
            correct.append(actual == expected)
            del actual
    gc.collect()
    report = dict(mode=args.mode, source_sha256=sha(source.read_bytes()),
                  scope="Paired fresh-process adapter diagnostic: same 8 pre-generated tasks covering all 4 representative video profiles, 3 pool lifecycles, 2 stdlib thread workers. Not the optimizer selection or holdout matrix; not a hard memory-cap proof.",
                  tasks=24, correct_per_repeat=correct, elapsed_seconds_per_repeat=elapsed,
                  expected_digest=sha(json.dumps(expected, sort_keys=True).encode()),
                  profiles=sorted({(t["width"], t["height"], t["frames"], t["codec"]) for t in tasks}),
                  baseline=baseline, final=census(), sampled_tree_peak_rss_bytes=rss.peak,
                  rss_collection=rss.report())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)
    return 0 if all(correct) and rss.report()["complete"] else 1


def main(argv=None):
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
