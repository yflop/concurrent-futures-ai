"""Public-safe metadata projection; never modifies execution/profile APIs or raw input."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex

FINGERPRINT_KEYS={'fingerprint','hardware_fingerprint'}
ABSOLUTE=re.compile(r'(?:/workspace/|/tmp/|/home/|/root/|/opt/)[^\s\"\'<>]*')


def project(value, *, key=None):
    if key in FINGERPRINT_KEYS and isinstance(value,str):
        return None
    if isinstance(value,dict):
        return {k:project(v,key=k) for k,v in value.items()}
    if isinstance(value,list):
        return [project(v) for v in value]
    if isinstance(value,str):
        if key=='command':
            try:
                parts=shlex.split(value)
                if parts and ('python' in Path(parts[0]).name):
                    parts[0]='python'
                parts=[Path(part).name if part.startswith(('/workspace/','/tmp/','/home/','/root/','/opt/'))
                       and Path(part).suffix in ('.json','.py','.log') else
                       '[local-path]' if part.startswith(('/workspace/','/tmp/','/home/','/root/','/opt/')) else part
                       for part in parts]
                value=shlex.join(parts)
            except ValueError:
                pass
        return ABSOLUTE.sub('[redacted local path]',value)
    return value


def convert(source,destination):
    payload=source.read_bytes();raw=json.loads(payload);public=project(raw)
    annotation={'scope':'metadata-only public projection; all numeric measurements, configuration choices, source hashes and hardware characteristics unchanged',
        'redacted':'absolute interpreter/checkout/temporary paths and report-only hardware fingerprint values',
        'original_file_sha256':hashlib.sha256(payload).hexdigest(),
        'saved_configuration_status':'report copies containing redacted fingerprints are intentionally not loadable saved profiles; runtime APIs and raw evidence are unchanged',
        'original_selection_hash_status':'original frozen selection hashes are retained as run provenance; metadata-redacted selection content has a separate projected hash'}
    if isinstance(public,dict):
        if 'selections' in public:
            annotation['projected_selections_sha256']=hashlib.sha256(json.dumps(public['selections'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
        public['public_metadata_projection']=annotation
    else:
        public={'records':public,'public_metadata_projection':annotation}
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_text(json.dumps(public,indent=2,allow_nan=False)+'\n')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.input.resolve()==args.output.resolve():
        parser.error('raw evidence must remain separate; use a different output path')
    if args.input.is_dir():
        for source in sorted(args.input.glob('*.json')):
            convert(source,args.output/source.name)
    else:
        convert(args.input,args.output)

if __name__=='__main__':main()
