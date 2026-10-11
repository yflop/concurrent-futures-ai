"""Bounded, deterministic compatibility observations against stdlib pools.

No deadlock is allowed to persist: every dependency has a gate or timeout, and
implicit-exit probes run in separately timed subprocesses. This is behavioral
evidence on the recorded interpreter, not a cross-version guarantee.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import hashlib
import json
import multiprocessing
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import threading
import time

import intelligent_futures
import intelligent_futures.executor as loaded_executor
from intelligent_futures import BackpressureError, IntelligentExecutor, TaskHints


def file_task(gate, marker):
    deadline = time.monotonic() + 10
    while gate and not Path(gate).exists():
        if time.monotonic() > deadline:
            raise TimeoutError("probe gate did not open")
        time.sleep(0.002)
    Path(marker).write_text("executed")
    return 42


def identity(value):
    return value


def pool_for(engine, **kwargs):
    if engine == "stdlib-thread":
        return ThreadPoolExecutor(max_workers=2)
    if engine == "stdlib-process":
        return ProcessPoolExecutor(max_workers=2,
                                   mp_context=multiprocessing.get_context("spawn"))
    return IntelligentExecutor(max_workers=2,
        process_workers=2 if engine == "intelligent-process" else 0,
        policy="process" if engine == "intelligent-process" else "threads",
        max_pending=kwargs.get("max_pending", 8), submit_timeout=0.2)


def submit(pool, engine, fn, *args):
    if engine == "intelligent-process":
        return pool.submit_task(fn, *args, hints=TaskHints(process_safe=True))
    return pool.submit(fn, *args)


def until(predicate, seconds=10):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("probe condition did not become true")
        time.sleep(0.002)


def blocked_callback(engine):
    entered, release = threading.Event(), threading.Event()
    callback_threads = []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        pool = pool_for(engine)
        first = submit(pool, engine, file_task, str(root / "gate"), str(root / "first"))
        second = submit(pool, engine, file_task, str(root / "second-gate"), str(root / "second"))
        def callback(_):
            callback_threads.append(threading.current_thread().name)
            entered.set()
            if not release.wait(10):
                raise TimeoutError("callback gate did not open")
        first.add_done_callback(callback)
        try:
            (root / "gate").touch()
            assert entered.wait(10)
            (root / "second-gate").touch()
            until((root / "second").exists)
            # The body is known to have run. Allow normal worker publication.
            time.sleep(0.1)
            observation = {"engine": engine, "second_body_executed": True,
                "second_result_published_while_callback_blocked": second.done(),
                "callback_thread": callback_threads[0]}
        finally:
            release.set()
            pool.shutdown(wait=True)
        assert first.result() == second.result() == 42
        return observation


def nested_submission(engine):
    pool = pool_for(engine, max_pending=1)
    def outer():
        started = time.monotonic()
        try:
            return {"nested": pool.submit(identity, 7), "elapsed_seconds": time.monotonic()-started}
        except BackpressureError as error:
            return {"admission": "BackpressureError", "reason": str(error),
                    "elapsed_seconds": time.monotonic()-started}
    try:
        result = pool.submit(outer).result(10)
        if "nested" in result:
            return {"engine": engine, "nested_submit": "accepted",
                    "nested_result": result["nested"].result(10),
                    "admission_seconds": result["elapsed_seconds"]}
        return {"engine": engine, "nested_submit": result["admission"],
                "reason": result["reason"], "admission_seconds": result["elapsed_seconds"],
                "reentrant_rejected": pool.snapshot()["counts"].get("reentrant_rejected", 0)}
    finally:
        pool.shutdown(wait=True)


def callback_dependency(engine):
    entered = threading.Event()
    outcome = []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        pool = pool_for(engine)
        first = submit(pool, engine, file_task, str(root / "gate"), str(root / "first"))
        def callback(_):
            try:
                child = submit(pool, engine, identity, 7)
                outcome.append({"dependency_result": child.result(timeout=0.3)})
            except TimeoutError:
                outcome.append({"dependency_result": "TimeoutError"})
            finally:
                entered.set()
        first.add_done_callback(callback)
        try:
            (root / "gate").touch()
            assert entered.wait(10)
        finally:
            pool.shutdown(wait=True)
        return {"engine": engine, **outcome[0]}


def worker_shutdown(engine):
    pool = pool_for(engine)
    def outer():
        try:
            pool.shutdown(wait=True)
        except RuntimeError:
            return "RuntimeError"
        return "returned"
    try:
        result = pool.submit(outer).result(10)
        try:
            later = pool.submit(identity, 8).result(10)
        except RuntimeError:
            later = "RuntimeError"
        return {"engine": engine, "worker_waiting_shutdown": result,
                "later_submit": later}
    finally:
        pool.shutdown(wait=True)


def implicit_exit(engine):
    code = '''
import json,time
from concurrent.futures import ThreadPoolExecutor
from intelligent_futures import IntelligentExecutor
engine=ENGINE
p=(ThreadPoolExecutor(max_workers=1) if engine=="stdlib-thread" else
   IntelligentExecutor(max_workers=1,max_pending=8,policy="threads"))
def task(i):
    time.sleep(0.1)
    return i
def done(f):
    try:r={"result":f.result()}
    except BaseException as e:r={"error":type(e).__name__}
    print(json.dumps(r),flush=True)
for i in range(4):
    p.submit(task,i).add_done_callback(done)
'''.replace("ENGINE", repr(engine))
    run = subprocess.run([sys.executable, "-c", code], text=True,
                         capture_output=True, timeout=15)
    rows = [json.loads(line) for line in run.stdout.splitlines() if line.startswith("{")]
    return {"engine": engine, "exit_code": run.returncode,
            "settled": rows, "stderr": run.stderr}


def run(runtime_variant):
    engines = ("stdlib-thread", "intelligent-thread", "stdlib-process", "intelligent-process")
    loaded_path = Path(loaded_executor.__file__).resolve()
    rows = {
        "blocked_callback": [blocked_callback(e) for e in engines],
        "callback_dependency": [callback_dependency(e) for e in engines],
        "nested_submission": [nested_submission(e) for e in engines[:2]],
        "worker_shutdown": [worker_shutdown(e) for e in engines[:2]],
        "implicit_exit": [implicit_exit(e) for e in engines[:2]],
    }
    assert [r["second_result_published_while_callback_blocked"] for r in rows["blocked_callback"]] == [True, False, False, False]
    assert [r["dependency_result"] for r in rows["callback_dependency"]] == [7, "TimeoutError", "TimeoutError", "TimeoutError"]
    assert [r["nested_submit"] for r in rows["nested_submission"]] == ["accepted", "BackpressureError"]
    assert all(r["exit_code"] == 0 and len(r["settled"]) == 4 for r in rows["implicit_exit"])
    return {"status": "observed", "python": sys.version, "platform": platform.platform(),
        "runtime_variant": runtime_variant, "version": intelligent_futures.__version__,
        "published_base_commit": "7c95c6fac134e2aaf53245dc20ac0e195bf5e6ed",
        "loaded_executor_path": str(loaded_path),
        "executor_sha256": hashlib.sha256(loaded_path.read_bytes()).hexdigest(),
        "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "observations": rows,
        "limits": "Single-host Python 3.12 observations. No universal callback-thread contract or performance claim."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--runtime-variant", required=True,
                        choices=("published-0.5.0", "unreleased-0.5.1-reentrant-guard"))
    args = parser.parse_args()
    result = run(args.runtime_variant)
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
