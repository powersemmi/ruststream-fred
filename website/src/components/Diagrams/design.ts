import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the fred client, and Redis at the bottom. Every subscription reads on a client of
 * its own; publishes and settlements share the pool.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 214, {kind: 'group'}),
    box('subscribers', left, 226, column, 58, {kind: 'broker'}),
    box('publishers', right, 226, column, 58, {kind: 'broker'}),
    box('descriptors', left, 294, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/subscriptions/',
    }),
    box('window', right, 294, column, 58, {kind: 'broker', href: '/architecture/window/'}),
    box('broker', left, 362, 304, 40, {kind: 'broker', code: true, href: '/components/broker/'}),
    box('client', 0, 438, 360, 72, {kind: 'group'}),
    box('reader', left, 464, column, 36, {kind: 'broker', href: '/architecture/lifecycle/'}),
    box('pool', right, 464, column, 36, {kind: 'broker', href: '/components/broker/pool/'}),
    box('redis', 12, 534, 336, 36, {kind: 'broker'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscribers', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'runtime', to: 'publishers', exitAt: centre(right), label: word(words, 'publishes')},
    {from: 'subscribers', to: 'reader', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'pool', exit: 'right', enter: 'right'},
    {from: 'reader', to: 'redis', enterAt: centre(left)},
    {from: 'pool', to: 'redis', enterAt: centre(right)},
  ];
  return {boxes, links};
}
