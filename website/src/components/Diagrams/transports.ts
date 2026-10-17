import type {Box, Link} from './flow';
import {lines, type Words} from './words';

// Three columns, one per transport, each the same width with a gutter between them.
const COLUMN = 112;
const X = [4, 124, 244] as const;
const HALF = (COLUMN - 8) / 2;

/*
 * The three transports side by side: a stream keeps its entries and every consumer group reads
 * all of them, a list hands each entry to one consumer, and a channel reaches only the
 * subscriptions connected when the message is published.
 */
export function transports(words: Words): {boxes: Box[]; links: Link[]} {
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
  const [stream, list, channel] = X;
  const boxes: Box[] = [
    box('streamProducer', stream, 0, COLUMN, 30, {kind: 'service', code: true}),
    box('listProducer', list, 0, COLUMN, 30, {kind: 'service', code: true}),
    box('channelProducer', channel, 0, COLUMN, 30, {kind: 'service', code: true}),
    box('stream', stream, 64, COLUMN, 44, {kind: 'broker'}),
    box('list', list, 64, COLUMN, 44, {kind: 'broker'}),
    box('channel', channel, 64, COLUMN, 44, {kind: 'broker'}),
    box('groupA', stream, 148, HALF, 58, {kind: 'core'}),
    box('groupB', stream + HALF + 8, 148, HALF, 58, {kind: 'core'}),
    box('workers', list, 148, COLUMN, 58, {kind: 'core'}),
    box('subA', channel, 148, HALF, 58, {kind: 'core'}),
    box('subB', channel + HALF + 8, 148, HALF, 58, {kind: 'core'}),
    box('late', channel, 226, COLUMN, 44, {kind: 'bad'}),
    box('keeps', stream, 226, COLUMN, 44, {kind: 'note'}),
    box('once', list, 226, COLUMN, 44, {kind: 'note'}),
  ];
  // A link from a store to the left or right half of the column under it.
  const left = HALF / 2 / COLUMN;
  const right = 1 - left;
  const links: Link[] = [
    {from: 'streamProducer', to: 'stream'},
    {from: 'listProducer', to: 'list'},
    {from: 'channelProducer', to: 'channel'},
    {from: 'stream', to: 'groupA', exitAt: left},
    {from: 'stream', to: 'groupB', exitAt: right},
    {from: 'list', to: 'workers'},
    {from: 'channel', to: 'subA', exitAt: left},
    {from: 'channel', to: 'subB', exitAt: right},
  ];
  return {boxes, links};
}
