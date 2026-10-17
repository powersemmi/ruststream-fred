import {describe, expect, it} from 'vitest';
import {ENTRIES, lastDelivered, pel, pending} from './pel';
import {targets, words} from './testWords';

describe('a consumer group over one stream', () => {
  it('keeps its cursor at the newest entry it has delivered', () => {
    expect(lastDelivered(ENTRIES)).toBe('4-0');
  });

  it('holds in its pending list what was delivered and not acknowledged', () => {
    expect(pending(ENTRIES).map((entry) => entry.id)).toEqual(['3-0', '4-0']);
  });

  it('is drawn with the cursor pointing at that entry', () => {
    expect(targets(pel(words).links, 'cursor')).toEqual(['e4-0']);
  });

  it('is read past the cursor, acknowledged and claimed through the pending list', () => {
    const {links} = pel(words);
    expect(targets(links, 'reader')).toEqual(['cursor']);
    expect(targets(links, 'acker')).toEqual(['list']);
    expect(targets(links, 'claimer')).toEqual(['list']);
  });
});
