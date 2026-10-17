import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's three states as types on the left, and on the right what each transition does
 * on Redis: the pool at connect, a client of its own per subscription, the unblock when a
 * subscription goes, and the pool's QUIT at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
  });
  const right = (id: string, y: number, h = 58): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 420, kind: 'group', lines: lines(words, 'states')},
    {id: 'redis', x: 184, y: 0, w: 176, h: 420, kind: 'group', lines: lines(words, 'redis')},
    left('unconnected', 28),
    left('connected', 168),
    left('closed', 350),
    right('config', 28),
    right('pool', 98),
    right('subscriptions', 168, 72),
    right('unblock', 262),
    right('quit', 350),
    {
      id: 'afterShutdown',
      x: 0,
      y: 442,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'pool'},
    {from: 'pool', to: 'subscriptions'},
    {from: 'subscriptions', to: 'unblock', dashed: true},
    {from: 'unblock', to: 'quit'},
  ];
  return {boxes, links};
}
