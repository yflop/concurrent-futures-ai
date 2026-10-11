"""Whole-campaign watchdog for nested fresh-session observation workers.

The campaign is owned local fixture work. Unlike the older group-only wrapper,
this records descendant PID/create-time identities and descendant session groups.
Failures remain failures after cleanup or a zero exit status from a TERM handler.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import psutil


def supervise(command, *, seconds, rss_bytes, checkpoint, report, stall_seconds=180):
    report = Path(report)
    if report.exists() or report.with_suffix('.samples.jsonl').exists():
        raise ValueError('new supervisor paths required')
    report.parent.mkdir(parents=True, exist_ok=True)
    child = None
    owned = {}
    groups = {}
    peak = 0
    reason = None
    errors = []
    remaining = []
    started = time.monotonic()
    samples = None

    def remember(process):
        created = process.create_time()
        previous = owned.get(process.pid)
        if previous is not None and previous != created:
            raise RuntimeError('owned process identity changed')
        owned[process.pid] = created
        group = os.getpgid(process.pid)
        if group != os.getpgrp():
            # A group ID is only recorded after discovering an owned descendant.
            groups.setdefault(group, time.time())

    def census():
        if child is not None and child.poll() is None:
            try:
                parent = psutil.Process(child.pid)
                remember(parent)
                for item in parent.children(recursive=True):
                    try:
                        remember(item)
                    except (psutil.NoSuchProcess, ProcessLookupError):
                        pass
            except psutil.NoSuchProcess:
                pass
        # Still-alive known children may own grandchildren after parent exit.
        for pid, created in list(owned.items()):
            try:
                parent = psutil.Process(pid)
                if parent.create_time() == created:
                    for item in parent.children(recursive=True):
                        try:
                            remember(item)
                        except (psutil.NoSuchProcess, ProcessLookupError):
                            pass
            except psutil.NoSuchProcess:
                pass
        # A spawn resource tracker can be reparented before a tree sample. An
        # already-recorded descendant session keeps that ownership discoverable.
        for item in psutil.process_iter(['pid', 'create_time', 'status']):
            try:
                group = os.getpgid(item.pid)
                if group in groups:
                    # If the old leader PID has been reused, this is a new group.
                    try:
                        leader = psutil.Process(group)
                        if group in owned and leader.create_time() != owned[group]:
                            continue
                    except psutil.NoSuchProcess:
                        pass
                    remember(item)
            except (psutil.NoSuchProcess, ProcessLookupError):
                pass
        alive = []
        for pid, created in list(owned.items()):
            try:
                item = psutil.Process(pid)
                if item.create_time() == created and item.status() != psutil.STATUS_ZOMBIE:
                    alive.append(item)
            except psutil.NoSuchProcess:
                pass
        return alive

    def stop(alive, sig):
        # Identity-checked signals also cover workers that made a new session.
        for item in reversed(alive):
            try:
                if item.create_time() == owned[item.pid]:
                    item.send_signal(sig)
            except psutil.NoSuchProcess:
                pass

    try:
        samples = report.with_suffix('.samples.jsonl').open('x')
        child = subprocess.Popen(command, start_new_session=True)
        remember(psutil.Process(child.pid))
        while child.poll() is None:
            alive = census()
            rss = 0
            for item in alive:
                try:
                    rss += item.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
            peak = max(peak, rss)
            elapsed = time.monotonic() - started
            samples.write(json.dumps({'elapsed_seconds': elapsed, 'rss_bytes': rss,
                'owned': sorted(owned.items()), 'groups': sorted(groups)}) + '\n')
            samples.flush()
            if rss > rss_bytes:
                reason = 'sampled_emergency_rss_limit'
            if elapsed > seconds:
                reason = reason or 'wall_limit'
            try:
                age = time.time() - Path(checkpoint).stat().st_mtime
            except FileNotFoundError:
                age = elapsed
            if age > stall_seconds:
                reason = reason or 'checkpoint_stall'
            if reason:
                break
            time.sleep(.25)
    except BaseException as exc:
        reason = reason or 'supervisor_error:' + type(exc).__name__
        errors.append(type(exc).__name__)
    finally:
        if child is not None:
            try:
                alive = census()
                if reason or child.poll() is None:
                    stop(alive, signal.SIGTERM)
                    deadline = time.monotonic() + 5
                else:
                    deadline = time.monotonic() + 2
                while alive and time.monotonic() < deadline:
                    time.sleep(.05)
                    alive = census()
                if alive:
                    reason = reason or 'owned_descendants_survived_runner_exit'
                    stop(alive, signal.SIGKILL)
                    deadline = time.monotonic() + 5
                    while alive and time.monotonic() < deadline:
                        time.sleep(.05)
                        alive = census()
                child.wait(timeout=5)
                remaining = [{'pid': item.pid, 'created': owned[item.pid]} for item in census()]
                if remaining:
                    reason = reason or 'live_owned_processes_after_cleanup'
            except BaseException as exc:
                reason = reason or 'cleanup_error:' + type(exc).__name__
                errors.append(type(exc).__name__)
                remaining = None  # Census unavailable, never an invented empty set.
                # Census failure never grants success. At least kill every still
                # matching recorded identity and the known fresh root group.
                for pid, created in owned.items():
                    try:
                        item = psutil.Process(pid)
                        if item.create_time() == created:
                            item.kill()
                    except psutil.NoSuchProcess:
                        pass
                    except BaseException as cleanup_error:
                        errors.append('identity_kill:' + type(cleanup_error).__name__)
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except BaseException as cleanup_error:
                    errors.append('group_kill:' + type(cleanup_error).__name__)
                try:
                    child.wait(timeout=5)
                except BaseException as cleanup_error:
                    errors.append(type(cleanup_error).__name__)
        if samples is not None:
            try:
                samples.close()
            except BaseException as exc:
                reason = reason or 'sample_close_error'
                errors.append(type(exc).__name__)
        result = {'status': 'passed' if child is not None and child.returncode == 0 and reason is None else 'failed',
                  'returncode': None if child is None else child.returncode, 'stop_reason': reason,
                  'elapsed_seconds': time.monotonic() - started, 'sampled_peak_tree_rss_bytes': peak,
                  'observed_identities': sorted(owned.items()), 'observed_groups': sorted(groups),
                  'remaining_live_owned_processes': remaining, 'cleanup_census_complete': not errors,
                  'errors': errors, 'wall_limit_seconds': seconds, 'rss_limit_bytes': rss_bytes,
                  'stall_limit_seconds': stall_seconds}
        try:
            with report.open('x') as stream:
                stream.write(json.dumps(result, indent=2) + '\n')
        except BaseException as exc:
            result.update(status='failed', stop_reason=reason or 'report_persistence_error')
            result['errors'].append(type(exc).__name__)
        print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=float, required=True)
    parser.add_argument('--rss-mib', type=int, required=True)
    parser.add_argument('--stall-seconds', type=float, default=180)
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command and args.command[0] == '--' else args.command
    if not command or not 1 <= args.seconds <= 3600 or not 64 <= args.rss_mib <= 2048 or not .1 <= args.stall_seconds <= 300:
        parser.error('invalid bounded campaign limits')
    result = supervise(command, seconds=args.seconds, rss_bytes=args.rss_mib * 1024**2,
        checkpoint=args.checkpoint, report=args.report, stall_seconds=args.stall_seconds)
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
