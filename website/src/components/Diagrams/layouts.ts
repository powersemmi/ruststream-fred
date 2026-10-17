import type {Box, Lane, Link} from './flow';
import {lifecycle} from './lifecycle';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  lifecycle,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
