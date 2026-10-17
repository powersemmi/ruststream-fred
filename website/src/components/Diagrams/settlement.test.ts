import {describe, expect, it} from 'vitest';
import {CELLS, FORMS, OUTCOMES, settlement} from './settlement';
import {words} from './testWords';

describe('settlement per form', () => {
  it('answers Unsupported to every outcome on a form that settles nothing', () => {
    for (const outcome of OUTCOMES) {
      expect(CELLS[outcome].none, outcome).toBe('unsupported');
    }
  });

  it('retries a stream entry with a copy, and a claiming one by leaving it pending', () => {
    expect(CELLS.retry.stream).toBe('copy');
    expect(CELLS.retry.claiming).toBe('pending');
    expect(CELLS.retry.reliable).toBe('copy');
  });

  it('removes the entry on ack and drop wherever the form settles', () => {
    for (const form of ['stream', 'claiming', 'reliable'] as const) {
      expect(CELLS.ack[form]).toBe('settled');
      expect(CELLS.drop[form]).toBe('settled');
    }
  });

  it('draws one cell per outcome and form, in its form column', () => {
    const {boxes, lanes} = settlement(words);
    for (const outcome of OUTCOMES) {
      FORMS.forEach((form, column) => {
        const cell = boxes.find((box) => box.id === `${outcome}-${form}`);
        expect(cell, `${outcome}-${form}`).toBeDefined();
        expect(cell!.x + cell!.w / 2).toBe(lanes[column + 1]!.x);
      });
    }
  });
});
