"""Publish metadata-redacted evidence without changing observations or gates.

Original report bytes stay in the caller's evidence directory. This produces
explicitly labeled derivatives and an original/projected/compressed hash ledger.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import re

PATH = re.compile(r'/(?:workspace|root)/[^\s\"\']+')
PRIVATE_KEYS = {'authorization', 'internal_authorization', 'instruction', 'internal_instructions'}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def numeric_tree(value, prefix=''):
    result = {}
    if isinstance(value, dict):
        for key, item in value.items():
            result.update(numeric_tree(item, prefix + '/' + key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            result.update(numeric_tree(item, prefix + '/' + str(index)))
    elif type(value) in (bool, int, float):
        result[prefix] = value
    return result


def replace_path(match, placeholder):
    original = match.group(0)
    path = original.rstrip('.,;:')
    return placeholder + original[len(path):]


def redact(value, counts):
    if isinstance(value, dict):
        output = {}
        for key, item in value.items():
            if key.lower() in PRIVATE_KEYS:
                counts['nonpublic_bookkeeping_fields_removed'] += 1
            else:
                output[key] = redact(item, counts)
        return output
    if isinstance(value, list):
        return [redact(item, counts) for item in value]
    if isinstance(value, str):
        changed, number = PATH.subn(lambda match: replace_path(match, '[local path]'), value)
        counts['local_path_occurrences_redacted'] += number
        return changed
    return value


def project(evidence_dir, output_dir, protocol):
    evidence_dir, output_dir, protocol = map(Path, (evidence_dir, output_dir, protocol))
    output_dir.mkdir(parents=True, exist_ok=True)
    mapping = (
        ('confirmation-v1/report.json', 'adoption-sustained-v051.json.gz'),
        ('development-v1/report.json', 'adoption-sustained-development-v051.json.gz'),
        ('confirmation-v1-recomputation.json', 'adoption-sustained-recomputation-v051.json'),
        ('confirmation-v1-supervisor.json', 'adoption-sustained-supervisor-v051.json'),
        ('confirmation-v1-summary.json', 'adoption-sustained-summary-v051.json'),
        ('development-v1-supervisor.json', 'adoption-sustained-development-supervisor-v051.json'),
        ('full-campaign-budget-v1.json', 'adoption-sustained-budget-v051.json'),
    )
    ledger = {'schema_version': 1,
        'scope': 'Metadata-redacted public derivatives; original evidence bytes preserved separately. Numerical observations, decisions, source hashes and hardware characteristics are unchanged.',
        'files': []}
    for source_name, target_name in mapping:
        original_raw = (evidence_dir / source_name).read_bytes()
        original = json.loads(original_raw)
        counts = {'local_path_occurrences_redacted': 0, 'nonpublic_bookkeeping_fields_removed': 0}
        projected = redact(original, counts)
        if numeric_tree(original) != numeric_tree(projected):
            raise AssertionError('projection changed numeric or boolean observations')
        if 'comparisons' in original and original['comparisons'] != projected['comparisons']:
            raise AssertionError('projection changed comparison evidence')
        if 'plan' in original and original['plan'] != projected['plan']:
            raise AssertionError('projection changed experimental plan')
        raw = (json.dumps(projected, indent=2, allow_nan=False) + '\n').encode()
        compressed = gzip.compress(raw, compresslevel=6, mtime=0) if target_name.endswith('.gz') else raw
        if target_name.endswith('.gz') and gzip.decompress(compressed) != raw:
            raise AssertionError('public archive byte roundtrip failed')
        target = output_dir / target_name
        with target.open('xb') as stream:
            stream.write(compressed)
        ledger['files'].append({'file': target_name, 'original_sha256': sha(original_raw),
            'original_bytes': len(original_raw), 'public_json_sha256': sha(raw),
            'public_json_bytes': len(raw), 'public_file_sha256': sha(compressed),
            'public_file_bytes': len(compressed), 'redaction_counts': counts,
            'numeric_boolean_tree_exactly_equal': True,
            'comparison_and_plan_exactly_equal_when_present': True})
    original_protocol = protocol.read_bytes()
    public_protocol = PATH.sub(lambda match: replace_path(match, '[source checkout]'), original_protocol.decode())
    public_protocol = ('Public path-redacted projection of the preregistered protocol. '
                       'The original frozen text SHA256 is ' + sha(original_protocol) + '. '
                       'Only the local checkout path and this explanatory note differ.\n\n' + public_protocol)
    public_protocol_raw = public_protocol.encode()
    projected_protocol = output_dir / 'SUSTAINED-PERFORMANCE-PROTOCOL-public.md'
    with projected_protocol.open('xb') as stream:
        stream.write(public_protocol_raw)
    ledger['protocol'] = {'original_frozen_sha256': sha(original_protocol),
                          'public_sha256': sha(public_protocol_raw),
                          'public_bytes': len(public_protocol_raw)}
    with (output_dir / 'adoption-sustained-archives-v051.json').open('x') as stream:
        stream.write(json.dumps(ledger, indent=2) + '\n')
    return ledger


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--protocol', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(project(args.evidence_dir, args.output_dir, args.protocol), indent=2))


if __name__ == '__main__':
    main()
