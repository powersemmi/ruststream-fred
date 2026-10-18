import {describe, expect, it} from 'vitest';
import {resolveMarkdownLinks} from './markdownLinks';

// The hook as Docusaurus calls it: the link and the page it is on, relative to the site folder.
const hook = resolveMarkdownLinks() as (params: {url: string; sourceFilePath: string}) => string;
const resolve = (url: string, sourceFilePath: string): string => hook({url, sourceFilePath});

describe('resolveMarkdownLinks', () => {
  it('sends a translated page to an untranslated neighbour in its own language', () => {
    expect(
      resolve(
        '../components/broker/shutdown.md',
        'i18n/zh/docusaurus-plugin-content-docs/current/architecture/lifecycle.md',
      ),
    ).toBe('/components/broker/shutdown/');
  });

  it('sends an untranslated page to a neighbour that has a translation', () => {
    expect(
      resolve('../../architecture/window.md#atomic', '../docs/components/pipelining/flush.md'),
    ).toBe('/architecture/window/#atomic');
  });

  it('refuses a link to a page that does not exist', () => {
    expect(() =>
      resolve('../../architecture/missing.md', '../docs/components/pipelining/flush.md'),
    ).toThrow("Markdown link '../../architecture/missing.md' in");
  });
});
