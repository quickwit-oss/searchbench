// A run set is a data boundary, not a query category. Filter before the pinned
// app derives its datasets, systems, query metadata, rankings or URL defaults.
export function sbSelectRun(rows, catalog) {
  const requested = new URLSearchParams(location.search).get('runSet');
  const direct = catalog.find(run => run.id === requested);
  const migrated = !direct && catalog.find(run => run.aliases?.some(alias => alias.id === requested));
  const selected = direct || migrated || catalog[0];
  if (migrated) {
    const alias = migrated.aliases.find(alias => alias.id === requested);
    const url = new URL(location.href);
    // Preserve packed engine/view choices. Old 100K-only links must not default to 1B.
    const dataset = url.searchParams.get('dataset') ?? sbPackedState(url).d;
    if (!selected.datasets.includes(dataset)) url.searchParams.set('dataset', alias.dataset);
    url.searchParams.set('runSet', selected.id);
    history.replaceState(history.state, '', url.href);
  }
  globalThis.__searchbenchRun = { ...selected, catalog };
  return rows.filter(row => row.run_id === selected.id);
}

// Keep presentation/cache/metric choices; engine selections and ordering belong
// to the previous run. `run=hot|cold` is an existing URL field: do not repurpose it.
function sbPackedState(url) {
  let packed = { v: 1 };
  try {
    const value = url.searchParams.get('s');
    if (value) {
      const raw = value.replace(/-/g, '+').replace(/_/g, '/');
      const decoded = JSON.parse(new TextDecoder().decode(
        Uint8Array.from(atob(raw + '='.repeat((4 - raw.length % 4) % 4)), c => c.charCodeAt(0))));
      if (decoded && decoded.v === 1) packed = decoded;
    }
  } catch { /* A malformed link falls back to normal view defaults. */ }
  return packed;
}

export function sbRunUrl(href, target, currentDataset) {
  const url = new URL(href);
  const packed = sbPackedState(url);
  const dataset = url.searchParams.get('dataset') ?? packed.d ?? currentDataset;
  for (const key of ['i', 'h', 'e', 'ed', 'o']) delete packed[key];
  for (const key of ['include', 'hidden', 'sortEngine', 'sortEngineDir', 'order']) url.searchParams.delete(key);
  packed.d = target.datasets.includes(dataset) ? dataset : target.datasets[0];
  url.searchParams.delete('dataset');
  const bytes = new TextEncoder().encode(JSON.stringify(packed));
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  url.searchParams.set('s', btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''));
  url.searchParams.set('runSet', target.id);
  return url.href;
}

// The always-visible part of a run description: its first ' · ' segments up to about `max`
// characters (machine and RAM: one line of the sidebar), the rest replaced by an ellipsis. The full text is in
// the collapsed details and the tooltip, so long descriptions do not push the sidebar down.
export function sbShortDescription(text, max = 58) {
  if (!text || text.length <= max) return text || '';
  const parts = text.split(' · ');
  let out = parts[0];
  for (const part of parts.slice(1)) {
    if ((out + ' · ' + part).length > max) break;
    out += ' · ' + part;
  }
  if (out.length > max) out = out.slice(0, max).replace(/\s+\S*$/, '');
  return out.replace(/[.;,:]$/, '') + '…';
}

// Serialized into the pinned bundle, where R is its React JSX runtime.
export function SBRunSelector({ dataset }) {
  const run = globalThis.__searchbenchRun;
  return R.jsxs('div', { className: 'run-selector', style: { padding: 9, borderBottom: '1px solid var(--line)' }, children: [
    R.jsx('label', { htmlFor: 'searchbench-run', style: { display: 'block', marginBottom: 7, fontWeight: 700, fontSize: 11 }, children: 'RUN / ENVIRONMENT' }),
    R.jsx('select', { id: 'searchbench-run', value: run.id, style: { width: '100%', font: 'inherit', fontSize: 11, padding: '6px 4px', color: 'var(--fg)', background: 'var(--card)', border: '1px solid var(--line)' },
      onChange: event => {
        const target = run.catalog.find(item => item.id === event.target.value);
        if (target) location.assign(sbRunUrl(location.href, target, dataset));
      }, children: run.catalog.map(item => R.jsx('option', { value: item.id, children: item.label }, item.id)) }),
    R.jsx('div', { className: 'run-desc-short', title: run.description || run.id, style: { marginTop: 7, color: 'var(--mut)', fontSize: 10, lineHeight: 1.4, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' },
      children: sbShortDescription(run.description || run.id) }),
    R.jsxs('details', { className: 'run-desc', style: { marginTop: 4, fontSize: 10, lineHeight: 1.4 }, children: [
      R.jsx('summary', { style: { cursor: 'pointer', color: 'var(--accent)' },
        children: 'details' + ((run.links || []).length ? ` · ${run.links.length} link${run.links.length > 1 ? 's' : ''}` : '') }),
      sbShortDescription(run.description) !== (run.description || '')
        ? R.jsx('div', { className: 'run-desc-full', style: { marginTop: 5, color: 'var(--mut)' }, children: run.description }) : null,
      (run.links || []).length ? R.jsx('div', { className: 'run-links', style: { marginTop: 5, lineHeight: 1.6, display: 'flex', flexWrap: 'wrap', gap: '2px 10px' },
        children: run.links.map(link => R.jsx('a', { href: link.href, target: '_blank', rel: 'noopener', style: { color: 'var(--accent)' }, children: link.label }, link.href)) }) : null,
    ] }),
  ] });
}

export function addRunSelector(page, patchOnce) {
  page = patchOnce(page, 'const cf=sbSelectRun(',
    [sbPackedState, sbSelectRun, sbRunUrl, sbShortDescription, SBRunSelector].map(fn => fn.toString()).join('\n') + '\nconst cf=sbSelectRun(',
    'run selector: runtime');
  page = patchOnce(page, 'className:"dataset-panel",children:[',
    'className:"dataset-panel",children:[R.jsx(SBRunSelector,{dataset:a}),', 'run selector: control');
  page = patchOnce(page, 's.set("s",zT(YT(a,n,c)));',
    's.set("s",zT(YT(a,n,c)));s.set("runSet",globalThis.__searchbenchRun.id);', 'run selector: share URL');
  page = patchOnce(page, 'label:"run",act:"metric"', 'label:"cache",act:"metric"', 'run selector: cache label');
  return page;
}
