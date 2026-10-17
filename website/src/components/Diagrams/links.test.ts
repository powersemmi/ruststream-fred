import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {describe, expect, it} from 'vitest';
import {LAYOUTS, type DiagramName, type Layout} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The docs are served from the site root, so a route names the page's file under `docs/`.
const DOCS = join(__dirname, '..', '..', '..', '..', 'docs');

const hrefs = (name: DiagramName): Record<string, string> => {
  const draw: (words: Words) => Layout = LAYOUTS[name];
  return Object.fromEntries(
    draw(words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );
};

const page = (href: string): boolean => {
  const route = href.replace(/^\/|\/$/g, '');
  return ['.md', '.mdx', '/index.md', '/index.mdx'].some((ending) =>
    existsSync(join(DOCS, route + ending)),
  );
};

describe('a box that links to a page', () => {
  // One test over every link, so the suite runs from the first diagram on.
  it('names a page of the site', () => {
    for (const name of Object.keys(LAYOUTS) as DiagramName[]) {
      for (const [box, href] of Object.entries(hrefs(name))) {
        // A root-relative route: the link prepends the language's base URL, so one route serves
        // every translation, and the site ends every route with a slash.
        expect(href, `${name}, ${box}`).toMatch(/^\/[a-z0-9/-]+\/$/);
        expect(page(href), `${name}, ${box}: no page under docs/ for ${href}`).toBe(true);
      }
    }
  });
});
