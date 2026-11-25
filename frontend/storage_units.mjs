// Storage-unit metadata for the pinned run-page bundle. Keep counts neutral:
// splits, active parts, Lucene segments and physical files/objects are not performance scores.
export function sbStorageValid(value) {
  return value !== null && typeof value === 'object' &&
    Number.isSafeInteger(value.count) && value.count >= 0 &&
    ['splits', 'parts', 'segments', 'files', 'objects'].includes(value.unit);
}

export function sbStorageText(value) {
  if (!sbStorageValid(value)) return '—';
  return new Intl.NumberFormat('en-US').format(value.count) + ' ' +
    (value.count === 1 ? value.unit.slice(0, -1) : value.unit);
}

export function addStorageUnits(page, patchOnce) {
  const patch = (from, to, label) => {
    page = patchOnce(page, from, to, 'storage units: ' + label);
  };
  patch('function ad(a,n,c,s){',
    sbStorageValid.toString() + '\n' + sbStorageText.toString() + '\nfunction ad(a,n,c,s){',
    'format helpers');
  const load = '{kind:"summary",key:"load",label:"Load time",section:"summary",get:y=>y.load_time,fmt:jt},';
  patch(load, load +
    '{kind:"summary",key:"storage_units",label:"Splits / parts / files",section:null,' +
    'get:y=>sbStorageValid(y.storage_units)?y.storage_units.count:null,fmt:(v,y)=>sbStorageText(y.storage_units)},',
    'row below load time');
  patch('a.fmt(ie)', 'a.fmt(ie,w)', 'engine-specific unit label');
  patch('className:"trow",style:{gridTemplateColumns:c}',
    'className:a.key==="storage_units"?"trow storage-units-row":"trow",style:{gridTemplateColumns:c}',
    'neutral row marker');
  patch('"data-key":a.key,title:"sort engines by this row"',
    '"data-key":a.key,disabled:a.key==="storage_units",title:a.key==="storage_units"?' +
    '"Persisted data units after loading/settling; not a performance ranking":"sort engines by this row"',
    'disable count ranking');
  patch('className:"cell",style:{background:"var(--na)",color:"var(--mut)"}',
    'className:"cell","data-storage-system":a.key==="storage_units"?w.system:void 0,style:{background:"var(--na)",color:"var(--mut)"}',
    'unknown count cell');
  patch('className:p?"cell q":"cell",...a.kind',
    'className:p?"cell q":"cell","data-storage-system":a.key==="storage_units"?w.system:void 0,...a.kind',
    'measured count cell');
  patch('title:pe,style:{background:de?',
    'title:a.key==="storage_units"?sbStorageText(w.storage_units)+" after loading/settling on disk or object storage; not a performance ranking":pe,style:{background:de?',
    'count tooltip');
  patch('function Cc({engines:a,valueOf:n,fmt:c,log:s}){const m=a.map(T=>({name:T.system,v:n(T)}))',
    'function Cc({engines:a,valueOf:n,fmt:c,log:s,detail}){const m=a.map(T=>({name:T.system,row:T,v:n(T)}))',
    'chart row metadata');
  patch('R.jsx("span",{className:"bartxt",children:c(T.v)})',
    'R.jsx("span",{className:"bartxt",style:detail?{width:100}:void 0,children:detail?' +
    'R.jsxs(R.Fragment,{children:[R.jsx("span",{style:{display:"block"},children:c(T.v)}),' +
    'R.jsx("span",{style:{display:"block",fontSize:10},"data-storage-system":T.name,' +
    'title:"Persisted data units; not a performance ranking",children:detail(T.row)})]}):c(T.v)})',
    'chart count below time');
  patch('valueOf:p=>p.load_time,fmt:p=>jt(p)+" s"',
    'valueOf:p=>p.load_time,fmt:p=>jt(p)+" s",detail:p=>sbStorageText(p.storage_units)',
    'load chart count');
  patch('</head>', '<style>' +
    '[data-product=searchbench] .storage-units-row .rowlab{cursor:default;color:var(--fg)!important;opacity:1}' +
    '[data-product=searchbench] .storage-units-row .cell{background:var(--card)!important;color:var(--fg)!important;white-space:normal}' +
    '</style></head>', 'neutral styles');
  return page;
}
