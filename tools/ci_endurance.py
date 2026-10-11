"""CI-only provenance and terminal evidence; the frozen experiment is unchanged."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import shutil
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / 'benchmarks/results/operational-endurance-github-v1-plan.json'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as stream:
        stream.write(json.dumps(value, indent=2, allow_nan=False) + '\n')


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def relative_key(path, root):
    return path.relative_to(root).as_posix()


def source_identity(plan):
    package = ROOT / 'src/intelligent_futures'
    files = {relative_key(p, package): sha(p) for p in sorted(package.rglob('*'))
             if p.is_file() and '__pycache__' not in p.parts}
    if files != plan['runtime_files']:
        names = sorted(set(files) | set(plan['runtime_files']))
        differences = {name: {'expected': plan['runtime_files'].get(name), 'actual': files.get(name)}
                       for name in names if files.get(name) != plan['runtime_files'].get(name)}
        raise RuntimeError('runtime files differ from frozen candidate: ' + json.dumps(differences, sort_keys=True))
    digest = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    check(digest == plan['runtime_manifest_sha256'], 'runtime manifest digest mismatch')
    frozen = {name: sha(ROOT / name) for name in plan['frozen_files']}
    check(frozen == plan['frozen_files'], 'frozen harness, protocol or test hash mismatch')
    return {'runtime_files': files, 'runtime_manifest_sha256': digest, 'frozen_files': frozen}


def preflight(directory, plan):
    import psutil
    identity = source_identity(plan)
    cpu_affinity = psutil.Process().cpu_affinity()
    info = {'status': 'checking', 'identity': identity, 'python': sys.version,
            'python_optimization_level': sys.flags.optimize,
            'platform': platform.platform(), 'machine': platform.machine(),
            'psutil_version': psutil.__version__, 'cpu_affinity': cpu_affinity,
            'logical_cpu_count': psutil.cpu_count(),
            'memory_bytes': psutil.virtual_memory().total,
            'free_disk_bytes': shutil.disk_usage(ROOT).free,
            'runner_image': os.environ.get('ImageOS'),
            'runner_image_version': os.environ.get('ImageVersion')}
    write(directory / 'host-observed.json', info)
    check(platform.system() == 'Linux' and platform.machine() == 'x86_64', 'requires Linux x86_64')
    check(platform.python_version() == plan['python_version'], 'Python version mismatch')
    check(psutil.__version__ == plan['psutil_version'], 'psutil version mismatch')
    check(sys.flags.optimize == 0 and __debug__, 'assertions must be enabled')
    check(len(cpu_affinity) >= plan['minimum_available_cpus'], 'insufficient available CPUs')
    check(info['memory_bytes'] >= plan['minimum_memory_bytes'], 'insufficient physical memory')
    check(info['free_disk_bytes'] >= plan['minimum_free_disk_bytes'], 'insufficient free disk')
    check(all(hasattr(os, name) for name in ('killpg', 'getpgid', 'setsid')), 'POSIX process groups unavailable')
    psutil.Process().num_fds()
    from intelligent_futures import __version__
    import intelligent_futures.executor as executor
    check(__version__ == '0.5.1', 'candidate version mismatch')
    check(Path(executor.__file__).resolve() == package_executor_path(), 'wrong package import directory')
    names = subprocess.check_output(['git', 'ls-files', '-z'], cwd=ROOT).decode().split('\0')
    tracked = {name: sha(ROOT / name) for name in names if name}
    write(directory / 'source-manifest.json', {'commit': os.environ.get('GITHUB_SHA'), 'files': tracked})
    write(directory / 'preflight.json', {**info, 'status': 'passed', 'host_check_status': 'passed'})


def package_executor_path():
    return (ROOT / 'src/intelligent_futures/executor.py').resolve()


def validate_terminal(summary, watchdog, plan):
    check(summary['status'] == watchdog['status'] == 'passed', 'experiment or watchdog did not pass')
    check(summary['mode'] == 'endurance', 'smoke is not endurance')
    check(summary['required_workload_seconds'] == plan['measured_seconds'], 'wrong requested duration')
    check(summary['workload_seconds'] >= plan['measured_seconds'], 'measured duration incomplete')
    check(len(summary['diagnostics']) == 16, 'sixteen checkpoints required')
    check(all(row['status'] == 'passed' for row in summary['diagnostics']), 'diagnostic failure')
    check(summary['identity']['runtime_sha256'] == plan['runtime_files'], 'measured runtime mismatch')
    check(summary['identity']['harness_sha256'] == plan['frozen_files']['tools/operational_endurance.py'], 'measured harness mismatch')
    check(summary['identity']['protocol_sha256'] == plan['frozen_files']['docs/OPERATIONAL-ENDURANCE-PROTOCOL.md'], 'measured protocol mismatch')
    check(watchdog['returncode'] == 0 and watchdog['stop_reason'] is None, 'watchdog exit failure')
    check(watchdog['remaining_owned_processes'] == [] and watchdog['errors'] == [], 'cleanup not verified')
    check(watchdog['wall_limit_seconds'] == plan['watchdog_wall_seconds'], 'wall limit mismatch')
    check(watchdog['sampled_rss_limit_bytes'] == plan['watchdog_rss_mib'] * 1024**2, 'RSS limit mismatch')
    check(watchdog['progress_limit_seconds'] == plan['watchdog_stall_seconds'], 'progress limit mismatch')


def finalize(directory, plan):
    result = {'status': 'failed', 'recorded_unix_time': time.time(),
              'run_id': os.environ.get('GITHUB_RUN_ID'), 'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
              'commit': os.environ.get('GITHUB_SHA'), 'endurance_step_outcome': os.environ.get('ENDURANCE_OUTCOME'),
              'preflight_step_outcome': os.environ.get('PREFLIGHT_OUTCOME'),
              'tests_step_outcome': os.environ.get('TESTS_OUTCOME'),
              'previous_attempt_cleanup': 'Unknown in the inaccessible original local PID namespace; no time carried forward.'}
    try:
        check(all(os.environ.get(name) == 'success' for name in ('ENDURANCE_OUTCOME', 'PREFLIGHT_OUTCOME', 'TESTS_OUTCOME')), 'required step did not succeed')
        summary = json.loads((directory / 'endurance/summary.json').read_text())
        watchdog = json.loads((directory / 'watchdog.json').read_text())
        validate_terminal(summary, watchdog, plan)
        result['identity_after'] = source_identity(plan)
        result.update(status='passed', workload_seconds=summary['workload_seconds'],
                      cleanup_verified_for_this_run=True)
    except Exception as error:
        result.update(error_type=type(error).__name__, error=str(error), traceback=traceback.format_exc(),
                      cleanup_verified_for_this_run=False)
    finally:
        result['evidence_files'] = {relative_key(p, directory): {'sha256': sha(p), 'bytes': p.stat().st_size}
                                    for p in sorted(directory.rglob('*')) if p.is_file()}
        write(directory / 'ci-terminal.json', result)
        print(json.dumps(result, indent=2), flush=True)
    return 0 if result['status'] == 'passed' else 1



def stop_supervisor(child, expected_created):
    """On observer failure, stop only this child and verified descendants."""
    import psutil
    owned = {}
    errors = []
    def collect():
        try:
            owner = psutil.Process(child.pid)
            check(expected_created is not None and owner.create_time() == expected_created,
                  'supervisor identity unavailable or changed')
            for process in [owner] + owner.children(recursive=True):
                owned[process.pid] = process.create_time()
        except psutil.NoSuchProcess:
            pass
        except Exception as error:
            errors.append('census:' + repr(error))
    def matching():
        live = []
        for pid, created in owned.items():
            try:
                process = psutil.Process(pid)
                if process.create_time() == created:
                    live.append(process)
            except psutil.NoSuchProcess:
                pass
        return live
    collect()
    try:
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
        child.wait(timeout=45)
    except subprocess.TimeoutExpired:
        collect()
    except Exception as error:
        errors.append('interrupt:' + repr(error))
    # The frozen supervisor normally performs cleanup itself. This fallback is
    # tied to observed descendants and creation times, never a process-name kill.
    try:
        survivors = matching()
        for process in survivors:
            try:
                process.terminate()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(survivors, timeout=5)
        for process in alive:
            try:
                if process.create_time() == owned[process.pid]:
                    process.kill()
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(alive, timeout=5)
        child.wait(timeout=10)
    except Exception as error:
        errors.append('fallback:' + repr(error))
    remaining = []
    try:
        remaining = [{'pid': process.pid, 'created': process.create_time(), 'status': process.status()}
                     for process in matching()]
    except Exception as error:
        errors.append('final_census:' + repr(error))
    return {'remaining_observed_owned_processes': remaining, 'errors': errors,
            'observed_owned_processes': len(owned), 'supervisor_returncode': child.poll()}


def report_progress(target):
    path = target / 'checkpoint.json'
    try:
        check(path.stat().st_size <= 1024 * 1024, 'oversized checkpoint')
        row = json.loads(path.read_text())
    except (OSError, ValueError, RuntimeError) as error:
        print('CHECKPOINT_UNAVAILABLE ' + type(error).__name__, flush=True)
        return
    values = {key: row.get(key) for key in ('event', 'unix_time', 'elapsed_seconds', 'workload_seconds')}
    counts = row.get('counts', {})
    values['counts'] = {key: counts.get(key) for key in ('accepted', 'success', 'cancelled', 'unexpected_failure')}
    print('PRODUCTIVE_CHECKPOINT ' + json.dumps(values), flush=True)


def observe(directory, plan):
    """Only report productive checkpoints; do not change measured source or files."""
    import psutil
    target = directory / 'endurance'
    check(not target.exists(), 'endurance output already exists')
    command = [sys.executable, str(ROOT / 'tools/supervise_endurance.py'),
               '--seconds', str(plan['watchdog_wall_seconds']),
               '--rss-mib', str(plan['watchdog_rss_mib']),
               '--stall-seconds', str(plan['watchdog_stall_seconds']),
               '--checkpoint', str(target / 'checkpoint.json'),
               '--report', str(directory / 'watchdog.json'), '--',
               sys.executable, str(ROOT / 'tools/operational_endurance.py'),
               '--mode', 'endurance', '--seconds', str(plan['measured_seconds']),
               '--warmup', str(plan['warmup_seconds']),
               '--cycle-seconds', str(plan['cycle_seconds']),
               '--sample-seconds', str(plan['sample_seconds']), '--output', str(target)]
    write(directory / 'command.json', {'command': command, 'started_unix_time': time.time()})
    child = subprocess.Popen(command, cwd=ROOT)
    created = None
    previous = {}
    interrupted = []
    failure = None
    returncode = 1
    def forward_interrupt(signum, frame):
        interrupted.append(signum)
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
    try:
        created = psutil.Process(child.pid).create_time()
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, forward_interrupt)
        while True:
            try:
                returncode = child.wait(timeout=60)
                break
            except subprocess.TimeoutExpired:
                if interrupted:
                    raise InterruptedError('observer interrupted')
                report_progress(target)
    except BaseException as error:
        failure = {'error_type': type(error).__name__, 'error': str(error), 'traceback': traceback.format_exc()}
    finally:
        if failure is not None or interrupted or child.poll() is None:
            cleanup = stop_supervisor(child, created)
            write(directory / 'observer-failure.json', {'status': 'failed', 'failure': failure,
                  'interrupt_signals': interrupted, 'cleanup': cleanup})
            returncode = 1
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    return returncode



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('preflight', 'observe', 'finalize'))
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    plan = json.loads(PLAN.read_text())
    if args.command == 'observe':
        return observe(args.evidence, plan)
    if args.command == 'finalize':
        return finalize(args.evidence, plan)
    try:
        preflight(args.evidence, plan)
    except Exception as error:
        write(args.evidence / 'preflight-failure.json', {'status': 'failed', 'error_type': type(error).__name__,
                                                       'error': str(error), 'traceback': traceback.format_exc()})
        raise
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
