// Geomean over the queries every displayed engine answered.
//
// The pinned bundle's il() averaged each engine over the queries *it* answered, against the fastest
// engine on each. An engine that runs extra queries no other engine supports (e.g. ClickHouse joins
// and fuzzy, where it is alone and so x1) then gets a better geomean than on the shared set, and two
// engines are not compared on the same queries. Here the query set is the intersection: every
// displayed engine (after the participants and category filters) has a number that is not a
// timeout. Each engine's geomean is over that set only; the row label shows its size.
//
// Same ratio as before: (Mc + engine) / (Mc + fastest), Mc = 10 ms smoothing, metric-aware via Wt().

export function commonGeomean(engines, ids, valueOf, isTimeout, smoothing = 0.01) {
  const common = ids.filter(id => engines.length > 0 && engines.every(e => {
    const v = valueOf(e, id);
    return typeof v === 'number' && !isTimeout(v);
  }));
  const fastest = {};
  for (const id of common) fastest[id] = Math.min(...engines.map(e => valueOf(e, id)));
  const out = new Map();
  for (const e of engines) {
    let sum = 0, n = 0;
    for (const id of common) {
      const ratio = (smoothing + valueOf(e, id)) / (smoothing + fastest[id]);
      if (ratio > 0) { sum += Math.log(ratio); n++; }
    }
    out.set(e.system, n ? Math.exp(sum / n) : null);
  }
  out.common = common.length;
  out.total = ids.length;
  return out;
}

// Minified twin of commonGeomean() in the bundle's own names: a=engines, n=ids, c=metric,
// Wt=value of (engine, id, metric), _r=timeout test, Mc=smoothing.
const IL_OLD = 'function il(a,n,c){const s={};for(const E of n){let f=1/0;for(const y of a){const _=Wt(y,E,c);typeof _=="number"&&(f=Math.min(f,_))}s[E]=isFinite(f)?f:null}const m=new Map;for(const E of a){let f=0,y=0;for(const _ of n){const T=Wt(E,_,c);if(_r(T)||typeof T!="number")continue;const M=s[_];if(M==null)continue;const p=(Mc+T)/(Mc+M);p>0&&(f+=Math.log(p),y++)}m.set(E.system,y?Math.exp(f/y):null)}return m}';
const IL_NEW = 'function il(a,n,c){const k=n.filter(E=>a.length>0&&a.every(y=>{const T=Wt(y,E,c);return typeof T=="number"&&!_r(T)}));' +
  'const s={};for(const E of k)s[E]=Math.min(...a.map(y=>Wt(y,E,c)));' +
  'const m=new Map;for(const E of a){let f=0,y=0;for(const _ of k){const p=(Mc+Wt(E,_,c))/(Mc+s[_]);p>0&&(f+=Math.log(p),y++)}' +
  'm.set(E.system,y?Math.exp(f/y):null)}m.common=k.length;m.total=n.length;return m}';

export function addCommonGeomean(page, patchOnce) {
  page = patchOnce(page, IL_OLD, IL_NEW, 'common geomean: computation');
  // Row label: "Geomean · N common q." with a tooltip naming the rule.
  page = patchOnce(page,
    'children:[a.label,R.jsx("span",{className:"mk",children:F})',
    'children:[a.kind==="geomean"&&s&&typeof s.common=="number"?' +
      'R.jsx("span",{title:`geometric mean of the ratio to the fastest engine, over the ${s.common} of ${s.total} queries every displayed engine answered (no timeout)`,' +
      'children:`${a.label} · ${s.common} common q.`}):a.label,R.jsx("span",{className:"mk",children:F})',
    'common geomean: label');
  return page;
}
