import {expect, it} from 'vitest';
import {delay} from './delay';
import {targets, words} from './testWords';

const {boxes, links} = delay(words);
const kind = (id: string) => boxes.find((box) => box.id === id)?.kind;

it('serves a delay in one of three ways', () => {
  expect(targets(links, 'outcome')).toEqual(['copyAck', 'zadd', 'pending']);
});

it('holds the runtime copy in the process, where a crash loses it', () => {
  expect(targets(links, 'copyAck')).toEqual(['held']);
  expect(kind('held')).toBe('bad');
});

it('keeps the delayed copy in Redis until a read sweeps it back', () => {
  expect(targets(links, 'zadd')).toEqual(['zset']);
  expect(targets(links, 'zset')).toEqual(['sweep']);
  expect(kind('zset')).toBe('broker');
});

it('leaves a claiming entry in the pending list until min_idle passes', () => {
  expect(targets(links, 'pending')).toEqual(['claimed']);
});
