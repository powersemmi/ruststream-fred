import {describe, expect, it} from 'vitest';
import {DELIVERIES, flush, window} from './window';
import {targets, words} from './testWords';

describe('one flush of a pipelined stream subscription', () => {
  it('sends the segments of acknowledged deliveries, then one XACK for every delivery', () => {
    expect(flush(DELIVERIES)).toEqual(['segment-1-0', 'segment-3-0', 'XACK 1-0 2-0 3-0']);
  });

  it('draws what each outcome sends', () => {
    const {links} = window(words);
    expect(targets(links, 'outcome-1-0')).toEqual(['segment-1-0']);
    expect(targets(links, 'outcome-2-0')).toEqual(['settles']);
    expect(targets(links, 'outcome-3-0')).toEqual(['segment-3-0']);
  });
});
