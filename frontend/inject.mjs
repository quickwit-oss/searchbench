// Replace the results array embedded in SearchBench's self-contained index.html.
// usage: node inject.mjs <template index.html> <results.json> <out.html>
//
// The committed frontend/index.html cannot be rebuilt here (its design kit,
// serenedb/serene-design, is private), so pages for other result sets reuse that
// bundle: only the embedded results literal is swapped, plus the small UI patches
// below. Every step must match exactly once, so a changed bundle fails the build
// instead of producing a broken page. Driven by frontend/build_run.sh.
import fs from 'node:fs';
import { addStorageUnits } from './storage_units.mjs';
import { addRunSelector } from './run_selector.mjs';
import { addCommonGeomean } from './common_geomean.mjs';
const [tpl, resultsPath, out, catalogPath] = process.argv.slice(2);
const catalog = catalogPath ? JSON.parse(fs.readFileSync(catalogPath, 'utf8')) : null;
const s = fs.readFileSync(tpl, 'utf8');
const marker = "JSON.parse(`[{\"system\"";
const at = s.indexOf(marker);
if (at < 0 || s.indexOf(marker, at + 1) >= 0) throw new Error('embedded results literal not found exactly once');
const open = s.indexOf('`', at) + 1;
let i = open;
while (i < s.length) { if (s[i] === '\\') { i += 2; continue; } if (s[i] === '`') break; i++; }
const oldRows = JSON.parse(s.slice(open, i).replace(/\\([\\`$])/g, '$1'));
const rows = JSON.parse(fs.readFileSync(resultsPath, 'utf8'));
// same row shape as the page was built with
const keys = Object.keys(oldRows[0]);
for (const r of rows) for (const k of ['system', 'dataset', 'result', 'query_tags', 'queries'])
  if (!(k in r)) throw new Error(`row ${r.system} lacks ${k}`);
if (catalog) {
  if (!Array.isArray(catalog) || !catalog.length || new Set(catalog.map(r => r.id)).size !== catalog.length)
    throw new Error('run catalog must contain unique run IDs');
  for (const run of catalog) {
    if (typeof run.id !== 'string' || !run.id || typeof run.label !== 'string' ||
        !Array.isArray(run.datasets) || !run.datasets.length || !rows.some(r => r.run_id === run.id))
      throw new Error('invalid or empty run: ' + run.id);
  }
  if (rows.some(row => !catalog.some(run => run.id === row.run_id))) throw new Error('row has no catalog run');
}
const lit = JSON.stringify(rows).replace(/</g, '\\u003c').replace(/[\\`]/g, '\\$&').replace(/\$\{/g, '\\${');
let page = s.slice(0, open) + lit + s.slice(i);
if (catalog) {
  // Wrap exactly the results expression, before any data-dependent app constants.
  if (s.slice(i, i + 2) !== '`)') throw new Error('unexpected end of results expression');
  const catalogJson = JSON.stringify(catalog).replace(/</g, '\\u003c');
  page = s.slice(0, at) + 'sbSelectRun(JSON.parse(`' + lit + '`),' + catalogJson + ')' + s.slice(i + 2);
}

// UI patches, each guarded to match exactly once in the vendored bundle.
function patchOnce(src, from, to, what) {
  const n = src.split(from).length - 1;
  if (n !== 1) throw new Error(`UI patch "${what}": expected 1 match, found ${n}`);
  return src.replace(from, to);
}
// 1) Load-time breakdown rows under "Load time" (load_* fields added by build_run.sh).
const z = 'fmt:y=>y===0?"0":jt(y)';
page = patchOnce(page,
  '{kind:"summary",key:"load",label:"Load time",section:"summary",get:y=>y.load_time,fmt:jt},',
  '{kind:"summary",key:"load",label:"Load time",section:"summary",get:y=>y.load_time,fmt:jt},' +
  // Timeline milestones (seconds from load start; build_run.sh derives them from the phases).
  `{kind:"summary",key:"load_searchable",label:"All data searchable at",section:null,get:y=>y.load_searchable,${z}},` +
  `{kind:"summary",key:"load_merged",label:"Merges finished at",section:null,get:y=>y.load_merged,${z}},` +
  `{kind:"summary",key:"load_ingest",label:"· ingest",section:null,get:y=>y.load_ingest,${z}},` +
  `{kind:"summary",key:"load_merge_after",label:"· merges after ingest",section:null,get:y=>y.load_merge_after,${z}},` +
  `{kind:"summary",key:"load_merge_overlap",label:"· merges during ingest",section:null,get:y=>y.load_merge_overlap,${z}},` +
  `{kind:"summary",key:"load_merge_busy",label:"· merge busy (whole load)",section:null,get:y=>y.load_merge_busy,${z}},` +
  `{kind:"summary",key:"load_check",label:"· settle check (excluded)",section:null,get:y=>y.load_check,${z}},`,
  'load breakdown rows');
// 2) Summary cells: show a measured 0 as "0" instead of "no result recorded".
//    Colouring still scales against the smallest positive value.
page = patchOnce(page,
  'if(p?typeof P!="number":!(typeof P=="number"&&P>0))',
  'if(p?typeof P!="number":!(typeof P=="number"&&(P>0||P===0&&(a.key.startsWith("load_")||a.key.startsWith("s3_")||a.key==="storage_units"))))',
  'zero-valued breakdown cells');
// 3) ...and colour such a 0 like the best value (a ratio of 0 is off the scale).
page = patchOnce(page, 'Ee=p?rl(ie,x):x?ie/x:1', 'Ee=p?rl(ie,x):x?Math.max(1,ie/x):1', 'zero-valued cell colour');
// 4) S3 request rows under "On-disk size" (s3_* fields added by s3_requests.py): GET
//    requests and downloaded bytes, cold and first hot try. Each metric has its own
//    common query set across columns that measured it, and its own coverage label.
//    Unmeasured components show "—". Decimal units (MB = 1e6 bytes), as in the run notes.
const s3n = {gets: {}, bytes: {}};
const datasetKey = r => catalog ? r.run_id + '/' + r.dataset : r.dataset;
const datasetLookup = catalog ? '(globalThis.__searchbenchRun.id+"/"+a)' : 'a';
for (const r of rows) for (const metric of ['gets', 'bytes']) {
  if (typeof r[`s3_${metric}_cold`] !== 'number' || typeof r[`s3_${metric}_hot`] !== 'number') continue;
  const n = r[`s3_${metric}_common_n`] ?? r.s3_common_n;
  if (typeof n === 'number') s3n[metric][datasetKey(r)] = Math.max(s3n[metric][datasetKey(r)] || 0, n);
}
const gfmt = 'fmt:y=>typeof y!="number"?"—":y>=1e6?(y/1e6).toFixed(2)+"M":y>=1e4?(y/1e3).toFixed(0)+"k":y>=1e3?(y/1e3).toFixed(1)+"k":String(y)';
const bfmt = 'fmt:y=>typeof y!="number"?"—":y>=1e12?(y/1e12).toFixed(2)+" TB":y>=1e9?(y/1e9).toFixed(1)+" GB":(y/1e6).toFixed(0)+" MB"';
const lab = (t) => {
  const counts = JSON.stringify(s3n[t.startsWith('S3 GETs') ? 'gets' : 'bytes']);
  return `label:"${t}"+((${counts})[${datasetLookup}]?" ("+(${counts})[${datasetLookup}]+" q.)":"")`;
};
page = patchOnce(page,
  '{kind:"summary",key:"size",label:"On-disk size",section:null,get:y=>y.data_size,fmt:vc},',
  '{kind:"summary",key:"size",label:"On-disk size",section:null,get:y=>y.data_size,fmt:vc},' +
  `{kind:"summary",key:"s3_gets_cold",${lab('S3 GETs · cold')},section:null,get:y=>y.s3_gets_cold,${gfmt}},` +
  `{kind:"summary",key:"s3_gets_hot",${lab('S3 GETs · hot')},section:null,get:y=>y.s3_gets_hot,${gfmt}},` +
  `{kind:"summary",key:"s3_bytes_cold",${lab('S3 downloaded · cold')},section:null,get:y=>y.s3_bytes_cold,${bfmt}},` +
  `{kind:"summary",key:"s3_bytes_hot",${lab('S3 downloaded · hot')},section:null,get:y=>y.s3_bytes_hot,${bfmt}},`,
  'S3 request rows');
// 4b) CPU-time rows under the S3 rows (cpu_* fields added by cpu_time.py): the engine's CPU
//     seconds (user + system, all cores) for the cold and the first hot try, summed over the
//     queries every column with CPU data completed. Columns without CPU data show "—".
// 4c) "CPU time · load" under the CPU-time rows: the load record's load_cpu (ingest + merges until settled).
const ldfmt = 'fmt:y=>typeof y!="number"?"—":y>=100?y.toFixed(0)+" s":y.toFixed(1)+" s"';
const cpun = {};
for (const r of rows) if (typeof r.cpu_common_n === 'number') cpun[datasetKey(r)] = Math.max(cpun[datasetKey(r)] || 0, r.cpu_common_n);
const CPUN = JSON.stringify(cpun);
const cfmt = 'fmt:y=>typeof y!="number"?"—":y>=100?y.toFixed(0)+" s":y.toFixed(1)+" s"';
const clab = (t) => `label:"${t}"+((${CPUN})[${datasetLookup}]?" ("+(${CPUN})[${datasetLookup}]+" q.)":"")`;
page = patchOnce(page,
  `{kind:"summary",key:"s3_bytes_hot",${lab('S3 downloaded · hot')},section:null,get:y=>y.s3_bytes_hot,${bfmt}},`,
  `{kind:"summary",key:"s3_bytes_hot",${lab('S3 downloaded · hot')},section:null,get:y=>y.s3_bytes_hot,${bfmt}},` +
  `{kind:"summary",key:"cpu_cold",${clab('CPU time · cold')},section:null,get:y=>y.cpu_cold,${cfmt}},` +
  `{kind:"summary",key:"cpu_hot",${clab('CPU time · hot')},section:null,get:y=>y.cpu_hot,${cfmt}},` +
  `{kind:"summary",key:"load_cpu",label:"CPU time · load",section:null,get:y=>y.load_cpu,${ldfmt}},`,
  'CPU time rows');
// 5) Query detail panel: that query's S3 GETs and MB per try, when the column has counters.
page = patchOnce(page,
  '` · runs [${x}] · cold ${A}`,Q=',
  '` · runs [${x}] · cold ${A}`+((s.cpu_by_q&&s.cpu_by_q[m])?" · CPU s ["+s.cpu_by_q[m].map(t=>typeof t=="number"?t.toFixed(t>=100?0:1):"—").join(", ")+"]":"")+((s.s3_requests&&s.s3_requests[m])?" · S3 GETs ["+s.s3_requests[m].map(t=>t&&typeof t[0]=="number"?t[0]:"—").join(", ")+"]"+(s.s3_requests[m].some(t=>t&&typeof t[1]=="number")?" · MB ["+s.s3_requests[m].map(t=>t&&typeof t[1]=="number"?(t[1]/1e6).toFixed(1):"—").join(", ")+"]":""):""),Q=',
  'S3 requests in the query panel');
// 10) Query panel: when the column is a median of several rounds (.rounds from rounds.json),
//     the range of that query's cold and hot times across the rounds.
page = patchOnce(page,
  '`+((s.cpu_by_q&&s.cpu_by_q[m])?',
  '`+((s.rounds&&s.rounds.queries&&s.rounds.queries[m])?(()=>{const v=s.rounds.queries[m].filter(t=>t&&typeof t[0]=="number"),c=v.map(t=>t[0]),h=v.map(t=>t[1]);return v.length?" · "+v.length+" rounds: cold "+jt(Math.min(...c))+"–"+jt(Math.max(...c))+" s, hot "+jt(Math.min(...h))+"–"+jt(Math.max(...h))+" s":""})():"")+((s.cpu_by_q&&s.cpu_by_q[m])?',
  'rounds in the query panel');
// 11) Metric switch in the config panel: wall time (as published), CPU time, S3 GET requests, S3 bytes.
//     The choice lives in the URL hash (#m=cpu|gets|bytes; none = wall), so a shared link keeps it; picking
//     one reloads the page. Every per-query cell, sort and the geomean read through Wt(), which now takes
//     that metric's per-try values: cpu_by_q (cpu_time.py) or s3_requests (s3_requests.py), keyed by query
//     id. Columns without that data show "—". The 60 s timeout skull only applies to wall time.
page = patchOnce(page, 'const Mc=.01,E0=60;',
  'const Mc=.01,E0=60,SBM=(typeof location!="undefined"&&(location.hash.match(/(?:^#|&)m=(cpu|gets|bytes)\\b/)||[])[1])||"wall",' +
  'SBG=y=>typeof y!="number"?"—":y>=1e6?(y/1e6).toFixed(2)+"M":y>=1e4?(y/1e3).toFixed(0)+"k":y>=1e3?(y/1e3).toFixed(1)+"k":String(Math.round(y)),' +
  'SBB=y=>typeof y!="number"?"—":y>=1e9?(y/1e9).toFixed(2)+" GB":y>=1e6?(y/1e6).toFixed(1)+" MB":(y/1e3).toFixed(0)+" kB",' +
  'SBU=y=>SBM==="gets"?SBG(y)+" GETs":SBM==="bytes"?SBB(y):jt(y)+" s";',
  'metric: state');
// The timeout test also takes the column's cap: SBC(row) = row.timeout_cap, else E0 (60 s). A column
// run with another cap (e.g. Loki 1B at 600 s) shows 💀 only at its own cap; see 7d below.
page = patchOnce(page, 'function _r(a){return typeof a=="number"&&a>=E0}',
  'function _r(a,c){return SBM==="wall"&&typeof a=="number"&&a>=(c??E0)}function SBC(y){return y&&typeof y.timeout_cap=="number"?y.timeout_cap:E0}',
  'metric: timeout only for wall time, at the column cap');
page = patchOnce(page, 'const m=a.result[s];if(!Array.isArray(m))return null;',
  'const m=SBM==="wall"?a.result[s]:SBM==="cpu"?(a.cpu_by_q?a.cpu_by_q[n]:null):(a.s3_requests&&a.s3_requests[n]?a.s3_requests[n].map(t=>t?t[SBM==="gets"?0:1]:null):null);if(!Array.isArray(m))return null;',
  'metric: cell values');
page = patchOnce(page, 'E==="ms"?oy(ie):jt(ie)', 'SBM==="gets"?SBG(ie):SBM==="bytes"?SBB(ie):E==="ms"?oy(ie):jt(ie)', 'metric: absolute cells');
page = patchOnce(page, 'p?`${jt(ie)} s · ${ci(Ee)} — click for query`', 'p?`${SBU(ie)} · ${ci(Ee)} — click for query`', 'metric: cell tooltip');
page = page.split('"absolute milliseconds":"absolute seconds"').join('"absolute milliseconds":SBM==="gets"?"absolute S3 GET requests":SBM==="bytes"?"absolute S3 bytes":"absolute seconds"');
page = patchOnce(page, 'onPick:m=>n({type:"metric",value:m})}),',
  'onPick:m=>n({type:"metric",value:m})}),R.jsx(T0,{label:"metric",act:"measure",opts:[{label:"Wall",v:"wall"},{label:"CPU",v:"cpu"},{label:"S3 GETs",v:"gets"},{label:"S3 bytes",v:"bytes"}],cur:SBM,flex:!0,pad:"4px 2px",fs:"10.5px",onPick:m=>{location.hash=m==="wall"?"":"m="+m;location.reload()}}),',
  'metric: config control');
// 11b) Relative cells for the S3 metrics: ratios against a floor of 1 GET / 1 MB, so a query that makes
//      no request is the best (1.00x) instead of making every other column infinitely worse.
page = patchOnce(page, 'function rl(a,n){return typeof a!="number"||typeof n!="number"?null:n===0?a===0?1:1/0:a/n}',
  'function rl(a,n){if(typeof a!="number"||typeof n!="number")return null;const f=SBM==="gets"?1:SBM==="bytes"?1e6:0;if(f)return Math.max(a,f)/Math.max(n,f);return n===0?a===0?1:1/0:a/n}',
  'metric: S3 ratio floor');
// 6) Column headers wrap instead of cutting long names with an ellipsis.
page = patchOnce(page,
  'line-height:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}',
  'line-height:14px;white-space:normal;overflow-wrap:anywhere}',
  'column headers wrap');
// 7) Header tooltip: full name, setup line (.details from details.json), version/OS/date,
//    load time, size and result file, one per line.
page = patchOnce(page,
  'Q=[Rc(A).join(" · "),x].filter(Boolean).join("  —  ")+"  —  click: sort queries by this engine · drag: reorder"',
  'Q=[A.system,A.details,[Rc(A).join(" · "),x].filter(Boolean).join("  —  "),typeof A.load_time=="number"?"load "+jt(A.load_time)+" s":null,typeof A.data_size=="number"?"size "+vc(A.data_size):null,A._source?"file "+A._source:null].filter(Boolean).join("\\n")+"\\nclick: sort queries by this engine · drag: reorder"',
  'header tooltip with run details');
// 7b) Version under the engine name in each column header: the engine version as recorded by the
//     run (`version`), without the parenthesised run settings DuckDB appends (those stay in the tooltip).
page = patchOnce(page,
  'R.jsx("span",{className:"grip",children:"⠿"}),A.system,R.jsx("span",{className:"sm",children:w})]',
  'R.jsx("span",{className:"grip",children:"⠿"}),A.system,R.jsx("span",{className:"sm",children:w}),A.version&&A.version!=="unknown"?R.jsx("span",{className:"ver",children:String(A.version).replace(/\\s*\\((?:threads|memory_limit)=[^)]*\\)\\s*$/,"")}):null]',
  'version line in column headers');
page = patchOnce(page,
  '[data-product=searchbench] .engcol .grip{',
  '[data-product=searchbench] .engcol .ver{display:block;font-weight:400;font-size:10px;line-height:13px;color:var(--mut);overflow-wrap:anywhere;margin-top:2px}[data-product=searchbench] .engcol .grip{',
  'version line style');
// 7c) Sparse columns (sparse_columns.py): "finished N/M q. · not in common set" above the version,
//     and their "—" geomean/CPU summary cells say why instead of "no result recorded".
page = patchOnce(page,
  'R.jsx("span",{className:"sm",children:w}),A.version&&',
  'R.jsx("span",{className:"sm",children:w}),A.sparse?R.jsx("span",{className:"ver",title:A.sparse.reason?`run with a ${SBC(A)} s timeout cap instead of ${E0} s: left out of the geomean and CPU common query sets`:"fewer than half of its supported queries finished (no timeout): left out of the geomean and CPU common query sets",children:`finished ${A.sparse.finished}/${A.sparse.supported} q. · ${A.sparse.reason||"not in common set"}`}):null,A.version&&',
  'sparse column note in headers');
page = patchOnce(page,
  'title:"no result recorded",children:"—"},w.system)',
  'title:!p&&w.sparse&&(a.kind==="geomean"||a.key==="cpu_cold"||a.key==="cpu_hot")?`${w.sparse.reason||"sparse column"}: finished ${w.sparse.finished} of ${w.sparse.supported} supported queries, so not in the common query set`:"no result recorded",children:"—"},w.system)',
  'sparse column summary cells');
// 7d) Per-column timeout cap (SBC, defined with _r above): coverage, cells, query panel and legend
//     test and name the column's own cap. Rows without `timeout_cap` keep the 60 s cap (E0).
page = patchOnce(page, 'typeof _=="number"&&(E++,_r(_)||f++)', 'typeof _=="number"&&(E++,_r(_,SBC(m))||f++)', 'column cap: coverage');
page = patchOnce(page, 'de=p&&_r(ie),', 'de=p&&_r(ie,SBC(w)),', 'column cap: cell');
page = patchOnce(page, 'pe=de?`timed out — killed at the ${E0}s cap — click for query`',
  'pe=de?`timed out — killed at the ${SBC(w)}s cap — click for query`', 'column cap: cell tooltip');
page = patchOnce(page,
  'x=f.map(ie=>typeof ie!="number"?"null":(_r(ie)?"💀":"")+jt(ie)).join(", "),F=`${n.metric} ${y==null?"—":(_r(y)?"💀 ":"")+jt(y)+" s"}`+(_r(y)?` (timed out — killed at the ${E0}s cap)`',
  'x=f.map(ie=>typeof ie!="number"?"null":(_r(ie,SBC(s))?"💀":"")+jt(ie)).join(", "),F=`${n.metric} ${y==null?"—":(_r(y,SBC(s))?"💀 ":"")+jt(y)+" s"}`+(_r(y,SBC(s))?` (timed out — killed at the ${SBC(s)}s cap)`',
  'column cap: query panel');
// Legend: when a displayed column has another cap, say that 💀 is at the column's own cap.
page = patchOnce(page, 'function Y_({metric:a,valueMode:n,sortRow:c}){', 'function Y_({metric:a,valueMode:n,sortRow:c,engines:SBe}){', 'column cap: legend engines');
page = patchOnce(page, '"💀 timed out (",E0,"s cap)"]}),',
  '"💀 timed out (",E0,"s cap)"]}),(()=>{const k=[...new Set((SBe||[]).map(SBC).filter(v=>v!==E0))].sort((u,v)=>u-v);' +
  'return k.length?R.jsx("span",{children:`💀 = the column\'s cap: ${E0} s, or ${k.join(" / ")} s where noted`}):null})(),',
  'column cap: legend note');
page = patchOnce(page, 'R.jsx(Y_,{metric:s.metric,valueMode:s.valueMode,sortRow:s.sortRow})',
  'R.jsx(Y_,{metric:s.metric,valueMode:s.valueMode,sortRow:s.sortRow,engines:n})', 'column cap: legend call');
// 8) Participant list wraps long names too.
page = patchOnce(page,
  'overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"},children:p}',
  'overflowWrap:"anywhere",whiteSpace:"normal"},children:p}',
  'participant names wrap');
// 9) Query panel: the column's full name, setup line and version above the query text.
page = patchOnce(page,
  'q="(query text unavailable — re-run ./frontend/build_results)";',
  'q="(query text unavailable — re-run ./frontend/build_results)";s.details&&(q="-- "+s.system+"\\n-- "+s.details+(s.version?"\\n-- "+s.version+(s.date?" · "+s.date:""):"")+"\\n\\n"+q);',
  'run details in the query panel');
// Add informational storage counts without replacing any CPU/S3/rounds patches.
page = addStorageUnits(page, patchOnce);
// Geomean over the queries every displayed engine answered (see common_geomean.mjs).
page = addCommonGeomean(page, patchOnce);
if (catalog) page = addRunSelector(page, patchOnce);
fs.writeFileSync(out, page);
console.log(`replaced ${oldRows.length} rows with ${rows.length}; keys expected: ${keys.join(',')}`);
