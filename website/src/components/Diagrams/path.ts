import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one stream entry that is answered, from the publish to its acknowledgement. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'append', kind: 'service'},
    {key: 'read', kind: 'broker', href: '/architecture/redis/groups/'},
    {key: 'entry', kind: 'broker'},
    {key: 'dispatch', kind: 'core'},
    {key: 'decode'},
    {key: 'body', kind: 'service'},
    {key: 'reply', kind: 'broker'},
    {key: 'settle', kind: 'core', href: '/architecture/settlement/'},
    {key: 'ack', kind: 'broker'},
  ]);
}
