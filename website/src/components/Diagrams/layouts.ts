import {broadcast} from './broadcast';
import {delay} from './delay';
import type {Box, Lane, Link} from './flow';
import {lifecycle} from './lifecycle';
import {path} from './path';
import {pel} from './pel';
import {queue} from './queue';
import {settlement} from './settlement';
import {transports} from './transports';
import {window} from './window';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  transports,
  pel,
  queue,
  broadcast,
  path,
  lifecycle,
  settlement,
  delay,
  window,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
