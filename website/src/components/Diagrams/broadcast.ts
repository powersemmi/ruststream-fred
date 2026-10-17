import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Pub/Sub: a publish reaches the subscriptions connected at that moment and nobody else, and a
 * subscription holds what its busy handler has not taken in the client's buffer.
 */
export function broadcast(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({
    id,
    x,
    y,
    w,
    h,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    box('publisher', 90, 0, 180, 44, {kind: 'service'}),
    box('channel', 90, 76, 180, 44, {kind: 'broker'}),
    box('subA', 0, 164, 112, 44, {kind: 'core'}),
    box('subB', 124, 164, 112, 44, {kind: 'core'}),
    box('late', 248, 164, 112, 58, {kind: 'bad'}),
    box('buffer', 0, 248, 112, 44, {kind: 'broker'}),
    box('handler', 0, 324, 112, 30, {kind: 'service'}),
    box('overflow', 124, 248, 236, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'publisher', to: 'channel', label: word(words, 'publish')},
    {from: 'channel', to: 'subA', exitAt: 0.2},
    {from: 'channel', to: 'subB', exitAt: 0.8},
    {from: 'subA', to: 'buffer'},
    {from: 'buffer', to: 'handler'},
  ];
  return {boxes, links};
}
