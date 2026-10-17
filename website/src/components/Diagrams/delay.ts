import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

// The columns: the runtime's copy, the delay queue in Redis, and the claiming read.
const COLUMNS = {copy: 62, zset: 180, claiming: 298} as const;
const W = 112;

/*
 * The three ways a stream subscription serves retry_after: the runtime holds a copy in the
 * process, a sorted set holds it in Redis, or a claiming read leaves the entry pending.
 */
export function delay(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const step = (
    id: string,
    column: keyof typeof COLUMNS,
    y: number,
    fields: Partial<Box> = {},
  ): Box => ({
    id,
    x: COLUMNS[column] - W / 2,
    y,
    w: W,
    h: 58,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    {id: 'outcome', x: 12, y: 28, w: 336, h: 30, kind: 'service', lines: lines(words, 'outcome')},
    step('copyAck', 'copy', 92),
    step('held', 'copy', 184, {kind: 'bad'}),
    step('republish', 'copy', 276),
    step('zadd', 'zset', 92),
    step('zset', 'zset', 184, {kind: 'broker'}),
    step('sweep', 'zset', 276),
    step('pending', 'claiming', 92, {kind: 'broker'}),
    step('claimed', 'claiming', 184),
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'outcome', to: 'copyAck', exitAt: along(COLUMNS.copy)},
    {from: 'outcome', to: 'zadd', exitAt: along(COLUMNS.zset)},
    {from: 'outcome', to: 'pending', exitAt: along(COLUMNS.claiming)},
    {from: 'copyAck', to: 'held'},
    {from: 'held', to: 'republish', label: word(words, 'timer')},
    {from: 'zadd', to: 'zset'},
    {from: 'zset', to: 'sweep', label: word(words, 'due')},
    {from: 'pending', to: 'claimed', label: word(words, 'idle')},
  ];
  const lanes: Lane[] = (Object.keys(COLUMNS) as (keyof typeof COLUMNS)[]).map((column) => ({
    x: COLUMNS[column],
    label: word(words, `${column}Lane`),
  }));
  return {boxes, links, lanes};
}
