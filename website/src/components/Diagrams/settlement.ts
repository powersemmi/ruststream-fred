import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

export const OUTCOMES = ['ack', 'retry', 'drop'] as const;
// A stream read fresh or reclaimed, a claiming stream, a reliable list, and the forms that
// settle nothing: a simple list and Pub/Sub.
export const FORMS = ['stream', 'claiming', 'reliable', 'none'] as const;

type Outcome = (typeof OUTCOMES)[number];
type Form = (typeof FORMS)[number];
type Effect = 'settled' | 'copy' | 'pending' | 'unsupported';

/** What each outcome does to the entry on each form. */
export const CELLS: Record<Outcome, Record<Form, Effect>> = {
  ack: {stream: 'settled', claiming: 'settled', reliable: 'settled', none: 'unsupported'},
  retry: {stream: 'copy', claiming: 'pending', reliable: 'copy', none: 'unsupported'},
  drop: {stream: 'settled', claiming: 'settled', reliable: 'settled', none: 'unsupported'},
};

const KIND: Record<Effect, Box['kind']> = {
  settled: 'good',
  copy: 'step',
  pending: 'broker',
  unsupported: 'bad',
};

const LABEL = {x: 0, w: 56} as const;
const CELL = 70;
const GAP = 6;
const TOP = 28;
const ROW = 52;

/*
 * Settlement per form as a table: an outcome per row, a form per column, and in each cell the
 * commands the outcome sends. The lanes name the columns, so the text version reads a column
 * at a time.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const column = (index: number): number => LABEL.w + GAP + index * (CELL + GAP);
  const boxes: Box[] = OUTCOMES.flatMap((outcome, row): Box[] => [
    {
      id: outcome,
      x: LABEL.x,
      y: TOP + row * ROW,
      w: LABEL.w,
      h: 44,
      kind: 'core',
      code: true,
      lines: lines(words, outcome),
    },
    ...FORMS.map(
      (form, index): Box => ({
        id: `${outcome}-${form}`,
        x: column(index),
        y: TOP + row * ROW,
        w: CELL,
        h: 44,
        kind: KIND[CELLS[outcome][form]],
        lines: lines(words, `${outcome}-${form}`),
      }),
    ),
  ]);
  const lanes: Lane[] = [
    {x: LABEL.x + LABEL.w / 2, label: word(words, 'outcome')},
    ...FORMS.map((form, index) => ({x: column(index) + CELL / 2, label: word(words, form)})),
  ];
  return {boxes, links: [], lanes};
}
