// Build ONE self-contained interface, with a run/environment selector.
// Historical paths are small redirects that preserve shared query/hash state.
// Usage: node frontend/build_site.mjs <output-directory>
import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '..');
const out = process.argv[2];
if (!out) throw new Error('usage: node frontend/build_site.mjs <output-directory>');
const work = fs.mkdtempSync(join(tmpdir(), 'searchbench-site-'));
const readJson = path => JSON.parse(fs.readFileSync(path, 'utf8'));
const catalog = [], rows = [];
// Links under the run description point to the repository at the deployed commit
// (GitHub Actions), else to SEARCHBENCH_REPO_URL / this repository at main.
const repoUrl = (process.env.SEARCHBENCH_REPO_URL ||
  (process.env.GITHUB_REPOSITORY ? `${process.env.GITHUB_SERVER_URL || 'https://github.com'}/${process.env.GITHUB_REPOSITORY}`
    : 'https://github.com/quickwit-oss/searchbench')).replace(/\/+$/, '');
const repoRef = process.env.SEARCHBENCH_REPO_REF || process.env.GITHUB_SHA || 'main';
// A run's links: its README, then run.json `links` ([{label, href}]). href is https:// or a
// path relative to the run directory that must stay inside runs/ and exist.
function runLinks(id, metadata) {
  const extra = metadata.links ?? [];
  if (!Array.isArray(extra)) throw new Error('run.json links must be an array: ' + id);
  return [{ label: 'README', href: 'README.md' }, ...extra].map(link => {
    if (!link || typeof link.label !== 'string' || !link.label || typeof link.href !== 'string' || !link.href)
      throw new Error('invalid link in ' + id + ': ' + JSON.stringify(link));
    if (/^https:\/\//.test(link.href)) return { label: link.label, href: link.href };
    const path = resolve(root, 'runs', id, link.href.split('#')[0]);
    const anchor = link.href.includes('#') ? link.href.slice(link.href.indexOf('#')) : '';
    if (!path.startsWith(join(root, 'runs') + '/') || !fs.existsSync(path))
      throw new Error('link outside runs/ or missing in ' + id + ': ' + link.href);
    return { label: link.label, href: `${repoUrl}/blob/${repoRef}/${path.slice(root.length + 1)}${anchor}` };
  });
}
function addRun(id, label, description, data, links = []) {
  if (!data.length || typeof label !== 'string' || typeof description !== 'string')
    throw new Error('invalid run metadata or empty results: ' + id);
  catalog.push({ id, label, description, links, datasets: [...new Set(data.map(row => row.dataset))] });
  rows.push(...data.map(row => ({ ...row, run_id: id })));
}
function redirect(path, relative, id) {
  const dest = join(out, path, 'index.html');
  fs.mkdirSync(dirname(dest), { recursive: true });
  const target = relative + '?runSet=' + encodeURIComponent(id);
  fs.writeFileSync(dest, '<!doctype html><html lang="en"><meta charset="utf-8"><title>SearchBench</title>' +
    '<p><a href="' + target + '">Open SearchBench</a></p><script>' +
    'const u=new URL(' + JSON.stringify(relative) + ',location.href);' +
    'u.search=location.search;u.hash=location.hash;u.searchParams.set("runSet",' + JSON.stringify(id) + ');' +
    'location.replace(u.href);</script></html>');
}
try {
  const dirs = fs.readdirSync(join(root, 'runs'), { withFileTypes: true })
    .filter(entry => entry.isDirectory() && fs.existsSync(join(root, 'runs', entry.name, 'README.md')))
    .map(entry => entry.name).sort().reverse();
  if (!dirs.length) throw new Error('no run directories');
  const meta = Object.fromEntries(dirs.map(id => {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(id)) throw new Error('invalid run directory: ' + id);
    const file = join(root, 'runs', id, 'run.json');
    return [id, fs.existsSync(file) ? readJson(file) : {}];
  }));
  // A run.json `compose` list makes a run set from other run sets' columns (no copied data).
  // Its sources are built first; every column keeps its own enrichment and must have a unique name.
  // A source is a run set ID (all its columns) or {"run": ID, "columns": [display names]}.
  const built = {};
  for (const id of dirs.filter(id => !meta[id].compose && !meta[id].redirect)) {
    const output = join(work, id + '.json');
    execFileSync('bash', [join(here, 'build_run.sh'), join(root, 'runs', id), output, '--rows-only'], { stdio: 'inherit' });
    built[id] = readJson(output);
  }
  for (const id of dirs.filter(id => !meta[id].redirect)) {
    let data = built[id];
    if (meta[id].compose) {
      const raw = meta[id].compose;
      const sources = Array.isArray(raw) ? raw.map(src => typeof src === 'string' ? { run: src } : src) : [];
      if (sources.length < 2 || sources.some(src => !src || typeof src.run !== 'string' || !built[src.run] ||
          (src.columns !== undefined && (!Array.isArray(src.columns) || !src.columns.length || src.columns.some(c => typeof c !== 'string')))))
        throw new Error('compose must list at least two run sets without compose: ' + id);
      data = sources.flatMap(src => {
        if (!src.columns) return built[src.run];
        const missing = src.columns.filter(c => !built[src.run].some(row => row.system === c));
        if (missing.length) throw new Error(`compose ${id}: no column ${missing.join(', ')} in ${src.run}`);
        return built[src.run].filter(row => src.columns.includes(row.system));
      });
      const names = data.map(row => row.dataset + '/' + row.system);
      if (new Set(names).size !== names.length) throw new Error('duplicate column names in composed run ' + id);
    }
    addRun(id, meta[id].label ?? id, meta[id].description ?? id, data, runLinks(id, meta[id]));
  }
  // Retired directories preserve deep links without remaining duplicate selector entries.
  for (const id of dirs.filter(id => meta[id].redirect)) {
    const alias = meta[id].redirect;
    const target = catalog.find(run => run.id === alias.run);
    if (!target || typeof alias.dataset !== 'string' || !target.datasets.includes(alias.dataset))
      throw new Error('redirect must target a visible run and one of its datasets: ' + id);
    (target.aliases ??= []).push({ id, dataset: alias.dataset });
  }
  addRun('upstream', 'Upstream reference', 'Original published results; environments differ.', readJson(join(here, 'results.json')));
  const rowsPath = join(work, 'rows.json'), catalogPath = join(work, 'catalog.json');
  fs.writeFileSync(rowsPath, JSON.stringify(rows));
  fs.writeFileSync(catalogPath, JSON.stringify(catalog));
  const html = join(work, 'index.html');
  execFileSync(process.execPath, [join(here, 'inject.mjs'), join(here, 'index.html'), rowsPath, html, catalogPath], { stdio: 'inherit' });
  fs.mkdirSync(out, { recursive: true });
  fs.copyFileSync(html, join(out, 'index.html'));
  for (const run of catalog.filter(run => run.id !== 'upstream')) {
    redirect('runs/' + run.id, '../../index.html', run.id);
    for (const alias of run.aliases ?? []) redirect('runs/' + alias.id, '../../index.html', alias.id);
  }
  redirect('upstream', '../index.html', 'upstream');
  fs.writeFileSync(join(out, '.nojekyll'), '');
  console.log(`Unified page: ${out}/index.html (${catalog.length} run sets, ${rows.length} result rows; default ${catalog[0].id})`);
} finally {
  fs.rmSync(work, { recursive: true, force: true });
}
