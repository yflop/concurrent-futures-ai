"""Lossless-numeric, path-redacted operational evidence archives.

Original bytes are never changed. Every input, output and transformation is
recorded in a hash ledger. Interrupted attempts remain separate from completed
attempts; this tool cannot convert a missing terminal result into a pass.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re

MAX_FILE_BYTES=64*1024**2
LOCAL_PATH=re.compile(r'/(?:workspace|root|home|opt)/[^\s\"\']+')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def parse(data):
    def bad(value):
        raise ValueError('nonfinite JSON constant: '+value)
    return json.loads(data,parse_constant=bad)


def nonstring_tree(value,path=''):
    """Preserve structure and exact types of every non-string observation."""
    if isinstance(value,dict):
        return ('dict',tuple((key,nonstring_tree(item,path+'/'+key)) for key,item in sorted(value.items())))
    if isinstance(value,list):
        return ('list',tuple(nonstring_tree(item,path+'/'+str(i)) for i,item in enumerate(value)))
    if isinstance(value,str):
        return ('str',)
    return (type(value).__name__,value)


def redact(value,changes):
    if isinstance(value,dict):
        return {key:redact(item,changes) for key,item in value.items()}
    if isinstance(value,list):
        return [redact(item,changes) for item in value]
    if isinstance(value,str):
        def replace(match):
            original=match.group(0)
            core=original.rstrip('.,;:')
            return '[local path]'+original[len(core):]
        text,count=LOCAL_PATH.subn(replace,value)
        changes['path_occurrences']+=count
        return text
    return value


def project_file(source,target):
    source,target=Path(source),Path(target)
    if source.stat().st_size>MAX_FILE_BYTES:
        raise ValueError('source exceeds declared archive input limit')
    original=source.read_bytes()
    is_jsonl=source.name.endswith('.jsonl')
    records=[parse(line) for line in original.splitlines() if line.strip()] if is_jsonl else [parse(original)]
    changes={'path_occurrences':0}
    projected=[redact(row,changes) for row in records]
    if nonstring_tree(records)!=nonstring_tree(projected):
        raise AssertionError('projection changed structure or numeric/boolean/null observations')
    if is_jsonl:
        public=(''.join(json.dumps(row,sort_keys=True,allow_nan=False)+'\n' for row in projected)).encode()
    else:
        public=(json.dumps(projected[0],sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    compressed=gzip.compress(public,compresslevel=6,mtime=0) if target.name.endswith('.gz') else public
    if target.name.endswith('.gz') and gzip.decompress(compressed)!=public:
        raise AssertionError('gzip round-trip mismatch')
    target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('xb') as stream:stream.write(compressed)
    return {'source_name':source.name,'file':target.name,'format':'jsonl' if is_jsonl else 'json',
        'original_sha256':sha(original),'original_bytes':len(original),
        'public_payload_sha256':sha(public),'public_payload_bytes':len(public),
        'public_file_sha256':sha(compressed),'public_file_bytes':len(compressed),
        'records':len(records),'redaction_counts':changes,
        'structure_and_all_nonstring_observations_exactly_equal':True}


def create_archive(mapping,output,attempt,classification,missing):
    if classification not in ('interrupted','completed','failed'):
        raise ValueError('explicit attempt classification required')
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    names=[target for _,target in mapping]
    if len(names)!=len(set(names)) or any(Path(name).name!=name for name in names):
        raise ValueError('unique plain output names required')
    if classification=='completed' and missing:
        raise ValueError('cannot classify missing terminal evidence as completed')
    ledger={'schema_version':1,'attempt':attempt,'classification':classification,
        'scope':'Metadata-redacted derivatives. Original bytes preserved separately; no numeric/boolean/null/structural changes. This archive classification is not a runtime pass certification.',
        'missing_evidence':list(missing),'files':[]}
    for source,target in mapping:ledger['files'].append(project_file(source,output/target))
    with (output/'archive-ledger.json').open('x') as stream:
        stream.write(json.dumps(ledger,indent=2,sort_keys=True)+'\n')
    return ledger


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',type=Path,required=True,help='explicit JSON input/output mapping and attempt classification')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();plan=parse(args.plan.read_bytes())
    result=create_archive([(item['source'],item['target']) for item in plan['files']],
        args.output,plan['attempt'],plan['classification'],plan.get('missing_evidence',[]))
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
