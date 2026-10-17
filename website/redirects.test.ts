import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {redirects} from './redirects';

const docsRoot = path.resolve(__dirname, '../docs');

// Targets whose pages land in a later change; the redirect is written now so the old address
// keeps a home. Once the page exists, its entry here goes.
const awaited = new Set<string>();

const moved = redirects();

// The site address of every page: its `slug` when the front matter names one, otherwise its path
// (`a/b.md` and `a/b/index.md` both serve `a/b/`). A page no locale translated is served from the
// English file, so an address with an English page exists in every language.
function addresses(): Set<string> {
  const found = new Set<string>();
  for (const file of fs.readdirSync(docsRoot, {recursive: true, encoding: 'utf8'})) {
    if (!/\.mdx?$/.test(file)) continue;
    const derived = file.replace(/\.mdx?$/, '').replace(/(^|\/)index$/, '');
    const slug = /^---\n[\s\S]*?^slug:\s*(\S+)\s*$[\s\S]*?^---$/m.exec(
      fs.readFileSync(path.join(docsRoot, file), 'utf8'),
    )?.[1];
    const address = slug?.startsWith('/')
      ? slug
      : slug
        ? path.posix.join(path.posix.dirname(derived), slug)
        : derived;
    found.add(address.replace(/^\/|\/$/g, ''));
  }
  return found;
}
const pages = addresses();
const page = (address: string): boolean => pages.has(address.replace(/^\/|\/$/g, ''));

describe('redirects', () => {
  it('sends the benchmarks page of the old site to its home in Architecture', () => {
    expect(moved.benchmarks).toBe('/architecture/benchmarks/');
  });

  it('sends each guide of the old site to the component page that holds its topic now', () => {
    expect(moved.streams).toBe('/components/streams/');
    expect(moved.lists).toBe('/components/lists/');
    expect(moved.pubsub).toBe('/components/pubsub/');
    expect(moved['dead-letter']).toBe('/components/acknowledgement/dead-letter/');
    expect(moved['auth-tls']).toBe('/components/broker/authentication/');
    expect(moved.transactions).toBe('/components/transactions/');
    expect(moved.asyncapi).toBe('/components/asyncapi/');
    expect(moved.testing).toBe('/components/testing/');
  });

  it('points every old address at a page the build has', () => {
    const targets = Object.entries(moved);
    expect(targets.length).toBeGreaterThan(0);
    for (const [from, to] of targets) {
      expect(to.startsWith('/'), `${from} -> ${to}`).toBe(true);
      if (awaited.has(to)) {
        expect(page(to), `${to} exists now: drop it from the awaited targets`).toBe(false);
      } else {
        expect(page(to), `${from} -> ${to}`).toBe(true);
      }
    }
  });

  it('never writes over a page: an old address has no page of its own', () => {
    for (const from of Object.keys(moved)) {
      expect(page(from), from).toBe(false);
    }
  });
});
