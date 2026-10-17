import {expect, it} from 'vitest';
import {design} from './design';
import {targets, words} from './testWords';

const {links} = design(words);

it('reads on a client of its own and publishes and settles on the pool', () => {
  expect(targets(links, 'subscribers')).toEqual(['reader']);
  expect(targets(links, 'publishers')).toEqual(['pool']);
  expect(targets(links, 'reader')).toEqual(['redis']);
  expect(targets(links, 'pool')).toEqual(['redis']);
});

it('is driven by the runtime, never by the service directly', () => {
  expect(targets(links, 'handlers')).toEqual(['runtime']);
  expect(targets(links, 'runtime')).toEqual(['subscribers', 'publishers']);
});
