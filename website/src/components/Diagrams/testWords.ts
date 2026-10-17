import type {Words} from './words';

// Every word is its own key, so a test reads the layout and never a page's wording.
export const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

/** The boxes a link from `id` reaches, in the order the layout draws them. */
export const targets = (links: {from: string; to: string}[], id: string): string[] =>
  links.filter((link) => link.from === id).map((link) => link.to);

/** The boxes with a link into `id`. */
export const sources = (links: {from: string; to: string}[], id: string): string[] =>
  links.filter((link) => link.to === id).map((link) => link.from);
