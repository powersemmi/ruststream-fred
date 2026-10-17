import {expect, it} from 'vitest';
import {broadcast} from './broadcast';
import {sources, targets, words} from './testWords';

const {links} = broadcast(words);

it('pushes a message to every subscription connected at the time', () => {
  expect(targets(links, 'publisher')).toEqual(['channel']);
  expect(targets(links, 'channel')).toEqual(['subA', 'subB']);
});

it('never reaches a subscription that joins after the publish', () => {
  expect(sources(links, 'late')).toEqual([]);
});

it('holds what a busy handler has not taken in the client buffer', () => {
  expect(targets(links, 'subA')).toEqual(['buffer']);
  expect(targets(links, 'buffer')).toEqual(['handler']);
});
