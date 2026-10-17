import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

export interface Delivery {
  id: string;
  // Whether the handler acknowledged it: only then does what it queued leave.
  acked: boolean;
}

// The deliveries of one read in the drawing.
export const DELIVERIES: readonly Delivery[] = [
  {id: '1-0', acked: true},
  {id: '2-0', acked: false},
  {id: '3-0', acked: true},
];

/**
 * What one flush of a stream subscription sends, in order: the segment of every acknowledged
 * delivery, then one `XACK` that settles every delivery of the read.
 */
export function flush(deliveries: readonly Delivery[]): string[] {
  return [
    ...deliveries.filter((delivery) => delivery.acked).map((delivery) => `segment-${delivery.id}`),
    `XACK ${deliveries.map((delivery) => delivery.id).join(' ')}`,
  ];
}

const W = 104;
const X = [12, 128, 244] as const;

/*
 * The window of a pipelined stream subscription: each delivery of a read queues its handler's
 * commands into a segment, the outcome keeps or drops the segment, and one flush sends the kept
 * segments and then the settles of the whole read.
 */
export function window(words: Words): {boxes: Box[]; links: Link[]} {
  const sent = flush(DELIVERIES);
  const boxes: Box[] = DELIVERIES.flatMap(({id, acked}, index): Box[] => [
    {
      id: `delivery-${id}`,
      x: X[index]!,
      y: 0,
      w: W,
      h: 44,
      kind: 'service',
      lines: [word(words, 'entry').replace('{id}', id), word(words, 'queues')],
    },
    {
      id: `outcome-${id}`,
      x: X[index]!,
      y: 76,
      w: W,
      h: 44,
      kind: acked ? 'good' : 'bad',
      lines: lines(words, acked ? 'kept' : 'dropped'),
    },
  ]);
  // The flush: a frame around what it sends, the segments side by side over the settles.
  boxes.push({
    id: 'flush',
    x: 0,
    y: 156,
    w: 360,
    h: 150,
    kind: 'group',
    lines: lines(words, 'flush'),
  });
  const segments = sent.slice(0, -1);
  segments.forEach((segment) => {
    const index = DELIVERIES.findIndex((delivery) => `segment-${delivery.id}` === segment);
    const id = DELIVERIES[index]!.id;
    boxes.push({
      id: segment,
      x: X[index]!,
      y: 182,
      w: W,
      h: 44,
      kind: 'step',
      lines: [word(words, 'segment').replace('{id}', id)],
    });
  });
  boxes.push(
    {
      id: 'settles',
      x: 12,
      y: 250,
      w: 336,
      h: 44,
      kind: 'broker',
      code: true,
      lines: [sent.at(-1)!],
    },
    {
      id: 'atomic',
      x: 0,
      y: 324,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'atomic'),
      href: '/components/pipelining/atomic/',
    },
  );
  const links: Link[] = DELIVERIES.flatMap(({id, acked}, index): Link[] => [
    {from: `delivery-${id}`, to: `outcome-${id}`},
    acked
      ? {from: `outcome-${id}`, to: `segment-${id}`}
      : {
          from: `outcome-${id}`,
          to: 'settles',
          enterAt: (X[index]! + W / 2 - 12) / 336,
          label: word(words, 'settleOnly'),
        },
  ]);
  return {boxes, links};
}
