"""Development-only 480-document OCR typography qualification, never a speed gate.

Requires Pillow, Poppler and Tesseract English data. Use a NEW workspace and report.
Runs at most four standard-library worker threads; this is not a memory/CPU cap.
Derived from the corrected per-document-index qualification, not its discarded v1.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import itertools
import json
from pathlib import Path
import time
from unittest.mock import patch


WORDS = ('ALPHA', 'BRAVO', 'DELTA', 'ECHO', 'HOTEL', 'NOVEMBER')
ORDERS = tuple(itertools.permutations(WORDS, 3))
PROFILES = ((1200, 900, 1, 6), (1275, 1650, 2, 10),
            (1654, 2339, 3, 16), (1650, 1275, 2, 8))
FONT = 'DejaVuSans-Bold.ttf'
FONT_SIZE = 42
WORKERS = 4
SEED = 6101317
DOCUMENTS = 480
CORRECTED_SOURCE_SHA256 = '8483b557d5a35e24466afc4e68b2175b42b7173f43c5c34398463a8d600b4db8'


def _dependencies():
    # --help and mocked coverage tests do not import optional media dependencies.
    from PIL import ImageFont
    from benchmarks.real_world import media
    return media, ImageFont


def headline_random(base_random):
    """Every newly seeded document selects its OWN heading by document index.

    Repeated pages in a document retain that heading. Resetting to heading zero
    for every new RNG would collapse the intended 120-order corpus to one order.
    """
    class HeadlineRandom(base_random):
        def __init__(self, seed):
            super().__init__(seed)
            if not isinstance(seed, str) or not seed.startswith('media-v1:'):
                raise ValueError('Expected an explicit media-v1 document seed')
            self._headline_index = int(seed.rsplit(':', 1)[1])
            if not 0 <= self._headline_index < len(ORDERS):
                raise ValueError('Document index is outside the declared heading corpus')

        def sample(self, population, k, *, counts=None):
            if tuple(population) == WORDS:
                if k != 3 or counts is not None:
                    raise ValueError('Heading sampling must match the declared ordered triples')
                return list(ORDERS[self._headline_index])
            return super().sample(population, k, counts=counts)
    return HeadlineRandom


def verify_profile(tasks, profile_index, profile, font_sha256):
    """Verify all 120 declared headings and exact fixture metadata before OCR."""
    if len(tasks) != len(ORDERS):
        raise AssertionError('Each profile requires exactly 120 documents')
    observed = []
    for index, task in enumerate(tasks):
        if task.get('index') != index:
            raise AssertionError('Document indices must cover 0 through 119 in order')
        heading = ' '.join(ORDERS[index])
        pages = task.get('expected_pages')
        if (not isinstance(pages, list) or len(pages) != profile[2]
                or not all(isinstance(page, str) and page.startswith(
                    'LOCAL DOCUMENT ' + heading + ' THE QUICK BROWN FOX READ EVERY WORD')
                    for page in pages)):
            raise AssertionError('Per-document expected pages do not match the declared heading')
        if task.get('expected_text') != ' '.join(pages):
            raise AssertionError('Full-document truth must be the exact generated page concatenation')
        if task.get('page_count') != profile[2] or task.get('source_size') != list(profile[:2]):
            raise AssertionError('Document dimensions/page count do not match its declared profile')
        if (task.get('font_name') != FONT or task.get('font_size') != FONT_SIZE
                or task.get('font_sha256') != font_sha256):
            raise AssertionError('Fixture font identity differs from the declared font bytes')
        observed.append(heading)
    if len(set(observed)) != 120 or set(observed) != {' '.join(order) for order in ORDERS}:
        raise AssertionError('All 120 distinct ordered headings must appear once per profile')
    return {'profile_index': profile_index, 'unique_orders': len(set(observed)), 'orders': observed}


def _prepare_corpus(media, image_font, workspace, font_sha256):
    if tuple(tuple(profile) for profile in media.OCR_PROFILES) != PROFILES:
        raise AssertionError('OCR profile definitions changed; review this development corpus')
    original_font = image_font.truetype
    declared_random = headline_random(media.random.Random)

    def chosen_font(name, size, *args, **kwargs):
        if size != FONT_SIZE:
            raise AssertionError('OCR fixture font size changed')
        return original_font(FONT, size, *args, **kwargs)

    corpus, verified = [], []
    for profile_index, profile in enumerate(PROFILES):
        # Generation is serial. All patches are restored before any OCR worker runs.
        with patch.object(media.random, 'Random', declared_random), \
                patch.object(media, 'OCR_PROFILES', (profile,)), \
                patch.object(image_font, 'truetype', chosen_font):
            tasks = media.prepare('ocr', workspace / f'profile-{profile_index}',
                                  len(ORDERS), SEED + profile_index, scale='representative')
        verified.append(verify_profile(tasks, profile_index, profile, font_sha256))
        corpus.append(tasks)
    if len(corpus) != 4 or sum(map(len, corpus)) != DOCUMENTS:
        raise AssertionError('Development corpus must contain four profiles and 480 documents')
    return corpus, verified


def _write_report(stream, report):
    # The caller exclusively created this report; no unrelated existing file is replaced.
    stream.seek(0)
    json.dump(report, stream, indent=2, allow_nan=False)
    stream.write('\n')
    stream.truncate()
    stream.flush()


def qualify(workspace, output):
    workspace, output = Path(workspace), Path(output)
    # Both destinations must be new. Also reject broken symlinks rather than following them.
    if workspace.exists() or workspace.is_symlink():
        raise FileExistsError('Workspace must be a new directory')
    if output.exists() or output.is_symlink():
        raise FileExistsError('Output report already exists')
    if workspace.resolve() == output.resolve():
        raise ValueError('Workspace and output report must be different paths')
    media, image_font = _dependencies()
    actual_font = Path(image_font.truetype(FONT, FONT_SIZE).path)
    font_sha256 = hashlib.sha256(actual_font.read_bytes()).hexdigest()
    plan = {
        'scope': 'Development-only typography qualification after original OCR failure; not confirmatory performance evidence',
        'font': FONT, 'font_size': FONT_SIZE, 'font_sha256': font_sha256,
        'profiles': PROFILES, 'headline_orders': ORDERS, 'documents': DOCUMENTS,
        'workers': WORKERS, 'truth': 'exact generated pages/text, no correction or exclusions', 'seed': SEED,
    }
    report = {
        'plan': plan, 'plan_sha256': hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
        'status': 'preparing', 'accepted': False, 'rows': [], 'verified_profile_orders': [],
        'implementation': {
            'based_on_corrected_source_sha256': CORRECTED_SOURCE_SHA256,
            'source_sha256': {
                'tools/qualify_ocr_typography.py': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'benchmarks/real_world/media.py': hashlib.sha256(Path(media.__file__).read_bytes()).hexdigest(),
            },
            'scope': 'Reusable refactoring with all-profile checks before OCR and explicit new destinations; new runs are distinct observations.',
        },
        'resource_scope': 'At most four stdlib worker threads. No CPU/RSS measurement or enforcement; use a separate supervisor when needed.',
    }
    workspace.mkdir(parents=True, exist_ok=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    with output.open('x', encoding='utf-8') as stream:
        _write_report(stream, report)
        try:
            corpus, report['verified_profile_orders'] = _prepare_corpus(media, image_font, workspace, font_sha256)
            report['status'] = 'running'
            _write_report(stream, report)
            for profile_index, tasks in enumerate(corpus):
                def observe(task):
                    start = time.perf_counter()
                    try:
                        value = media.run_task(task)
                        expected_hash = hashlib.sha256(task['expected_text'].encode()).hexdigest()
                        if value.get('text') != task['expected_text'] or value.get('sha256') != expected_hash:
                            raise ValueError('OCR result differs from exact generated text/digest')
                        error, digest = None, value['sha256']
                    except Exception as exc:
                        error, digest = type(exc).__name__ + ': ' + str(exc), None
                    return {'profile_index': profile_index, 'index': task['index'], 'error': error,
                            'sha256': digest, 'seconds': time.perf_counter() - start}

                # Finite 120-task profile, four threads; no IntelligentExecutor or process pool.
                with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
                    for row in pool.map(observe, tasks):
                        report['rows'].append(row)
                        report['elapsed_seconds'] = time.perf_counter() - started
                        _write_report(stream, report)
            expected_slots = {(profile, index) for profile in range(4) for index in range(120)}
            observed_slots = {(row['profile_index'], row['index']) for row in report['rows']}
            if len(report['rows']) != DOCUMENTS or observed_slots != expected_slots:
                raise AssertionError('Result coverage differs from the declared 480-document corpus')
            report['status'] = 'complete'
            report['accepted'] = not any(row['error'] for row in report['rows'])
        except BaseException as exc:
            report['status'] = 'failed'
            report['failure'] = type(exc).__name__ + ': ' + str(exc)
            report['elapsed_seconds'] = time.perf_counter() - started
            _write_report(stream, report)
            raise
        _write_report(stream, report)
    return report


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, required=True, help='new directory for the development corpus')
    parser.add_argument('--output', type=Path, required=True, help='new JSON report; never overwrite an existing file')
    return parser


def main(argv=None):
    args = argument_parser().parse_args(argv)
    report = qualify(args.workspace, args.output)
    print(json.dumps({'status': report['status'], 'accepted': report['accepted'],
                      'documents': len(report['rows']), 'plan_sha256': report['plan_sha256']}))
    return 0 if report['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
