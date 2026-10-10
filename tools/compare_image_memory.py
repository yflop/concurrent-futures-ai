import argparse
from concurrent.futures import ThreadPoolExecutor
import gc
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from benchmarks.real_world import media
from benchmarks.real_world.resource_sampling import RSSSampler
import psutil

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--mode',choices=['before','after'],required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--before-source',type=Path,default=Path('benchmarks/results/optimizer/reliability/media_before_cleanup.py'));a=p.parse_args()
 os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:8])
 oldpath=a.before_source
 spec=importlib.util.spec_from_file_location('old_media',oldpath);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
 fn=old.run_task if a.mode=='before' else media.run_task
 with tempfile.TemporaryDirectory() as tmp:
  tasks=old.prepare('images',Path(tmp),12,551237,scale='representative')
  expected=[old.run_task(t) for t in tasks]
  gc.collect();baseline=psutil.Process().memory_info().rss
  correct=[]
  with RSSSampler() as rss:
   for repeat in range(3):
    with ThreadPoolExecutor(max_workers=2) as pool:
     actual=list(pool.map(fn,tasks))
    correct.append(actual==expected)
  gc.collect();final=psutil.Process().memory_info().rss
  r={'mode':a.mode,'source_sha256':hashlib.sha256((oldpath if a.mode=='before' else Path(media.__file__)).read_bytes()).hexdigest(),'scope':'paired fresh-process image adapter memory observation, same12 tasks/all6 profiles,3 pool lifecycles,2 stdlib thread workers; sampled RSS is not allocator or hard memory proof','tasks':36,'correct_per_repeat':correct,'expected_digest':hashlib.sha256(json.dumps(expected,sort_keys=True).encode()).hexdigest(),'baseline_parent_rss_bytes':baseline,'final_parent_rss_bytes':final,'sampled_tree_peak_rss_bytes':rss.peak,'rss_collection':rss.report()}
  a.output.write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
