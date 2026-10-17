import {expect, it} from 'vitest';
import {sources, targets, words} from './testWords';
import {transports} from './transports';

const {links} = transports(words);

it('lets every consumer group read the whole stream', () => {
  expect(targets(links, 'streamProducer')).toEqual(['stream']);
  expect(targets(links, 'stream')).toEqual(['groupA', 'groupB']);
});

it('hands a list entry to the one pool of consumers that pops it', () => {
  expect(targets(links, 'listProducer')).toEqual(['list']);
  expect(targets(links, 'list')).toEqual(['workers']);
});

it('reaches only the subscribers connected when the message is published', () => {
  expect(targets(links, 'channelProducer')).toEqual(['channel']);
  expect(targets(links, 'channel')).toEqual(['subA', 'subB']);
  expect(sources(links, 'late')).toEqual([]);
});
