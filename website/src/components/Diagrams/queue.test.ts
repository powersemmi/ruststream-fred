import {expect, it} from 'vitest';
import {queue} from './queue';
import {sources, targets, words} from './testWords';

const {links} = queue(words);

it('moves an entry to the processing list before the handler sees it', () => {
  expect(targets(links, 'producer')).toEqual(['queue']);
  expect(targets(links, 'queue')).toEqual(['processing']);
  expect(targets(links, 'processing')).toEqual(['handler', 'recovery']);
});

it('settles every outcome from the handler', () => {
  expect(targets(links, 'handler')).toEqual(['ack', 'retry', 'drop']);
});

it('returns an orphan to the queue only through the recovery sweep', () => {
  expect(sources(links, 'queue')).toEqual(['producer', 'recovery']);
});
