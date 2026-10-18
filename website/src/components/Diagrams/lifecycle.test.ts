import {expect, it} from 'vitest';
import {lifecycle} from './lifecycle';
import {targets, words} from './testWords';

const {links} = lifecycle(words);

it('moves the broker through its three states by consuming transitions', () => {
  expect(targets(links, 'unconnected')).toEqual(['connected']);
  expect(targets(links, 'connected')).toEqual(['closed']);
});

it('opens the pool at connect and closes it after every read client', () => {
  expect(targets(links, 'config')).toEqual(['pool']);
  expect(targets(links, 'pool')).toEqual(['subscriptions']);
  expect(targets(links, 'subscriptions')).toEqual(['unblock']);
  expect(targets(links, 'unblock')).toEqual(['quit']);
});
