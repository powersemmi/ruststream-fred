import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

export interface Entry {
  id: string;
  // Acknowledged, delivered and still pending, or not delivered to the group yet.
  state: 'acked' | 'pending' | 'new';
}

// The entries of one stream as one consumer group sees them.
export const ENTRIES: readonly Entry[] = [
  {id: '1-0', state: 'acked'},
  {id: '2-0', state: 'acked'},
  {id: '3-0', state: 'pending'},
  {id: '4-0', state: 'pending'},
  {id: '5-0', state: 'new'},
  {id: '6-0', state: 'new'},
];

/** The group's cursor, `last-delivered-id`: the newest entry it has handed to a consumer. */
export function lastDelivered(entries: readonly Entry[]): string | undefined {
  return entries.filter((entry) => entry.state !== 'new').at(-1)?.id;
}

/** The pending entries list: delivered to a consumer and not acknowledged. */
export const pending = (entries: readonly Entry[]): Entry[] =>
  entries.filter((entry) => entry.state === 'pending');

const KIND = {acked: 'good', pending: 'step', new: 'broker'} as const;
const STEP = 56;

/*
 * One consumer group over a stream: its cursor marks the newest entry delivered, its pending
 * entries list holds what is delivered and not acknowledged, and three commands move entries
 * between the two.
 */
export function pel(words: Words): {boxes: Box[]; links: Link[]} {
  const cursorAt = lastDelivered(ENTRIES);
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
    kind: 'core',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    box('stream', 0, 0, 360, 84, {kind: 'group'}),
    ...ENTRIES.map(
      ({id, state}, index): Box => ({
        id: `e${id}`,
        x: 12 + index * STEP,
        y: 28,
        w: 48,
        h: 44,
        kind: KIND[state],
        lines: [id, word(words, state)],
      }),
    ),
    box('cursor', 12, 124, 156, 58),
    box('list', 184, 124, 164, 58),
    box('reader', 12, 250, 104, 44, {kind: 'service'}),
    box('acker', 128, 250, 104, 44, {kind: 'service'}),
    box('claimer', 244, 250, 104, 44, {kind: 'service'}),
    box('seek', 0, 312, 360, 30, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'cursor', to: `e${cursorAt}`, enterAt: 0.5, label: word(words, 'cursorAt')},
    {from: 'reader', to: 'cursor', enterAt: 52 / 156, label: word(words, 'read')},
    {
      from: 'acker',
      to: 'list',
      exitAt: (200 - 128) / 104,
      enterAt: (200 - 184) / 164,
      label: word(words, 'ack'),
    },
    {from: 'claimer', to: 'list', enterAt: (296 - 184) / 164, label: word(words, 'claim')},
  ];
  return {boxes, links};
}
