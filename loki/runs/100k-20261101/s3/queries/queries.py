#!/usr/bin/env python3
"""Query-only 100K run: native SDK GET calls, wall and CPU from the same trials."""
import argparse
from collections import Counter
import copy
import datetime
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path('/mnt/local-ssd/searchbench')
OLD = Path('<home>/loki-s3-validation/s3-100k-20261101T175953Z')
sys.path.insert(0, str(ROOT/'loki'))
sys.path.insert(0, str(ROOT/'lib'))
import adapter
from check_results import parse_log
from prometheus_client.parser import text_string_to_metric_families


def require(value, message):
    if not value: raise ValueError(message)


def canonical(rows):
    return Counter(json.dumps(row,sort_keys=True,separators=(',',':'),ensure_ascii=False) for row in rows)


def scrape(out, name):
    text=adapter.api('/metrics',timeout=10).text
    (out/(name+'.metrics.gz')).write_bytes(gzip.compress(text.encode(),mtime=0))
    counts={}; start=None
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name=='loki_s3_request_duration_seconds_count' and s.labels.get('operation')=='S3.GetObject':
                code=s.labels['status_code'];counts[code]=counts.get(code,0)+s.value
            elif s.name=='process_start_time_seconds':start=s.value
    require(start is not None, 'process identity metric absent')
    return counts,start


def delta(before,after):
    result={k:after.get(k,0)-before.get(k,0) for k in before.keys()|after.keys()}
    require(all(math.isfinite(v) and v>=0 and v==int(v) for v in result.values()),'invalid/reset GET counter')
    return {k:int(v) for k,v in result.items()}


def cpu(scope):
    d=dict(line.split() for line in (scope/'cpu.stat').read_text().splitlines())
    return int(d['usage_usec'])/1e6


def timeout_handler(*_):
    raise TimeoutError('query exceeded 60-second cap')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--out',type=Path,required=True)
    out=parser.parse_args().out;out.mkdir(parents=True,exist_ok=False)
    s=adapter.settings();data=Path(s['data_dir'])
    manifest=json.loads((data/'manifest.json').read_text())
    require(s['storage']=='s3' and s['target']=='read' and manifest['rows']==100000,'expected existing 100K S3 read-only dataset')
    require(s['prefix']=='searchbench/loki/s3-100k-20261101T175953Z/','unexpected source prefix')
    require(s['image']=='grafana/loki@sha256:0db216b630ae1f348a1584c67f3cead8b4c1fc839169a39501db664744158e28','image must remain unchanged')
    require(not any(p.is_file() for p in (data/'chunks').rglob('*')),'unexpected local chunk files')
    running=subprocess.check_output(['docker','ps','--format','{{.Names}}'],text=True).splitlines()
    require(not set(running)-{s['container']},'another container is running')
    adapter.verify_layout()
    spec_lines=(ROOT/'loki/queries.logql').read_text().splitlines()
    specs={v['id']:v for line in spec_lines if line.startswith('{') for v in [json.loads(line)]}
    require(len(specs)==56,'unexpected supported query count')
    ref_path=OLD/'loki-results/loki_3.7.9-m8gd4-100k-s3-validation_otel_logs_100k.json'
    reference=json.loads(ref_path.read_text());sections=parse_log(str(ref_path).replace('.json','.results.log'))
    require(len(sections)==len(reference['query_tags'])==92,'reference log coverage mismatch')
    expected={tag['id']:canonical([json.loads(line) for line in body])
              for tag,(_,body) in zip(reference['query_tags'],sections) if tag['id'] in specs}
    require(set(expected)==set(specs),'reference answers missing')
    system='Loki (3.7.9-m8gd4-100k-s3-gets)'
    result=copy.deepcopy(reference)
    result.update(system=system,date=datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d'),
                  build='3.7.9-m8gd4-100k-s3-validation',
                  result=[[None,None,None] for _ in range(92)],cpu=[[None,None,None] for _ in range(92)])
    positions={tag['id']:i for i,tag in enumerate(result['query_tags'])}
    query_counters={}
    (out/'queries.logql').write_text('\n'.join(spec_lines)+'\n')
    (out/'method.json').write_text(json.dumps({
        'rows':100000,'query_only':True,'supported_queries':56,'tries':3,'timeout_seconds':60,
        'image':s['image'],'source_commit':(ROOT/'SOURCE_COMMIT').read_text().strip(),
        'collector_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'get_metric':'loki_s3_request_duration_seconds_count{operation="S3.GetObject"}',
        'get_semantics':'Completed outer SDK calls in observed process windows. Includes outer retries/background work, not necessarily internal HTTP attempts. Status labels are synthetic nil/error labels.',
        'bytes_collected':False,'cold':'one engine restart and OS page-cache drop per query, then cold/hot/hot',
        'cpu_window':'counter immediately before query_response through its return; metric scrapes outside CPU/window timing',
        'metadata_cache':'seeded once with unmeasured Q01, then retained; data/result caches disabled',
        'idle_tail_seconds':1,'load_metrics':'inherited from original build, not remeasured',
        'reference_result':str(ref_path),'reference_sha256':hashlib.sha256(ref_path.read_bytes()).hexdigest()
    },indent=2))
    # Seed permitted index metadata and prove the selected image/client emits the metric.
    adapter.start()
    _,pid=scrape(out,'seed-before')
    rows,_,_=adapter.query_response(specs['Q01'],manifest,timeout=60)
    calls,pid2=scrape(out,'seed-after')
    require(pid==pid2 and sum(calls.values())>0,'native GET instrumentation not active')
    require(canonical(rows)==expected['Q01'],'seed query answer mismatch')
    adapter.stop()
    signal.signal(signal.SIGALRM,timeout_handler)
    with (out/'measurements.jsonl').open('w',buffering=1) as output:
        for qid in sorted(specs):
            adapter.stop()
            subprocess.run(['sudo','sh','-c','sync; echo 3 > /proc/sys/vm/drop_caches'],check=True)
            adapter.start()
            runtime=json.loads((data/'runtime.json').read_text())
            require(runtime['target']=='read,query-scheduler','wrong runtime target')
            cfg=json.loads((data/'config.json').read_text())
            require(cfg['querier']['query_store_only'],'ingester reads enabled')
            require(not cfg['query_range']['cache_results'] and not cfg['query_range']['cache_instant_metric_results'],'result cache enabled')
            require(all(not cfg['chunk_store_config'][k]['embedded_cache']['enabled'] for k in ('chunk_cache_config','chunk_cache_config_l2')),'chunk cache enabled')
            info=json.loads(subprocess.check_output(['docker','inspect',s['container']],text=True))[0]
            scope=Path('/sys/fs/cgroup/searchbench.slice')/('docker-'+info['Id']+'.scope')
            before,pid=scrape(out,qid+'-idle-before');time.sleep(1)
            after,pid2=scrape(out,qid+'-idle-after');require(pid==pid2,'process changed during idle')
            output.write(json.dumps({'query':qid,'phase':'idle','sdk_getobject_calls_by_status':delta(before,after)})+'\n')
            wall=[];used_cpu=[];gets=[]
            for trial in (1,2,3):
                before,pid2=scrape(out,f'{qid}-{trial}-before');require(pid==pid2,'process changed')
                begin=time.time_ns();c0=cpu(scope)
                signal.alarm(60)
                try: rows,elapsed,stats=adapter.query_response(specs[qid],manifest,timeout=60)
                finally:signal.alarm(0)
                used=cpu(scope)-c0;end=time.time_ns()
                after,pid2=scrape(out,f'{qid}-{trial}-after');require(pid==pid2,'process changed during query')
                require(canonical(rows)==expected[qid],qid+' differs from validated S3 reference')
                require(not stats.get('ingester',{}).get('totalReached',0),'query reached ingesters')
                downloaded=stats.get('querier',{}).get('store',{}).get('totalChunksDownloaded',0)
                require(bool(before or after) or downloaded==0,'chunks downloaded without known GET metric')
                d=delta(before,after);total=sum(d.values())
                if qid=='Q01':require(total>0,'Q01 made no GET SDK calls')
                require(0<=elapsed<60 and used>=0,'invalid timing sample')
                wall.append(elapsed);used_cpu.append(used);gets.append([total,None])
                output.write(json.dumps({'query':qid,'phase':'query','trial':trial,'wall':elapsed,'cpu':used,
                    'gets':total,'bytes':None,'sdk_getobject_calls_by_status':d,
                    'sdk_getobject_before':before,'sdk_getobject_after':after,'process_start_seconds':pid,
                    'container_id':info['Id'],'start_unix_ns':begin,'finish_unix_ns':end,'rows':rows,'stats':stats},separators=(',',':'))+'\n')
            before,pid2=scrape(out,qid+'-tail-before');require(pid==pid2,'process changed')
            time.sleep(1);after,pid2=scrape(out,qid+'-tail-after');require(pid==pid2,'process changed')
            output.write(json.dumps({'query':qid,'phase':'tail','sdk_getobject_calls_by_status':delta(before,after)})+'\n')
            result['result'][positions[qid]]=wall;result['cpu'][positions[qid]]=used_cpu;query_counters[qid]=gets
            (out/'result.partial.json').write_text(json.dumps(result,indent=2))
            print(qid,'wall',','.join(f'{v:.4f}' for v in wall),'GET SDK calls',','.join(str(v[0]) for v in gets),flush=True)
    adapter.stop();adapter.verify_layout()
    require(len(query_counters)==56,'incomplete GET coverage')
    (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    (out/'s3_requests.json').write_text(json.dumps({'otel_logs_100k/'+system:{
        'source':'Native GetObject SDK-call deltas, same trials as wall/CPU; bytes not collected. Internal HTTP retries may not be separately counted.',
        'bytes_available':False,'queries':query_counters}},indent=2)+'\n')
    (out/'checks.json').write_text(json.dumps({'rows':100000,'queries':56,'trials':168,'reference_answers_match':True,
        'layout_unchanged':True,'query_only':True,'bytes_collected':False},indent=2)+'\n')
    print('PASS: 56 queries / 168 trials; native GETs, wall and CPU paired; no bytes or re-ingestion',flush=True)


if __name__=='__main__':
    try:main()
    finally:adapter.stop()
