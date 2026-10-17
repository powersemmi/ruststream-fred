import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The queue's column leaves a gutter on its right for the recovery set.
const MAIN = {x: 28, w: 180} as const;
const SIDE = {x: 232, w: 128} as const;

/*
 * A reliable list: a claim moves the entry from the queue to a processing list in one command,
 * the handler's outcome settles it there, and the recovery set returns the entry of a consumer
 * that died before it settled.
 */
export function queue(words: Words): {boxes: Box[]; links: Link[]} {
  const main = (id: string, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: MAIN.x,
    y,
    w: MAIN.w,
    h: 44,
    kind: 'broker',
    lines: lines(words, id),
    ...fields,
  });
  const outcome = (id: string, x: number, kind: Box['kind']): Box => ({
    id,
    x,
    y: 340,
    w: 112,
    h: 44,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    main('producer', 0, {kind: 'service'}),
    main('queue', 80, {code: true}),
    main('processing', 176, {code: true}),
    {
      id: 'recovery',
      x: SIDE.x,
      y: 128,
      w: SIDE.w,
      h: 44,
      kind: 'core',
      lines: lines(words, 'recovery'),
    },
    main('handler', 260, {kind: 'service'}),
    outcome('ack', 0, 'good'),
    outcome('retry', 124, 'step'),
    outcome('drop', 248, 'bad'),
    {id: 'orphan', x: 0, y: 404, w: 360, h: 30, kind: 'note', lines: lines(words, 'orphan')},
  ];
  const links: Link[] = [
    {from: 'producer', to: 'queue', label: word(words, 'push')},
    {from: 'queue', to: 'processing', label: word(words, 'claim')},
    {from: 'processing', to: 'handler', label: word(words, 'delivered')},
    {
      from: 'processing',
      to: 'recovery',
      exit: 'right',
      enter: 'bottom',
      label: word(words, 'tracked'),
    },
    {
      from: 'recovery',
      to: 'queue',
      exit: 'top',
      enter: 'right',
      exitAt: 0.2,
      label: word(words, 'sweep'),
    },
    // The outcomes spread wider than the handler, so each link bends to its own box.
    {from: 'handler', to: 'ack', exitAt: 0.2},
    {from: 'handler', to: 'retry', exitAt: 0.5},
    {from: 'handler', to: 'drop', exitAt: 0.8},
  ];
  return {boxes, links};
}
