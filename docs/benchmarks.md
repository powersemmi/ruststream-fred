# Benchmarks

A framework between the Redis client and your handler costs time on every message: the read, the
decode, the dispatch, the settle. This page says how much, measured against the same work written
by hand on `fred`.

Two halves in one process run the same scenario: one is a RustStream service, the other a loop on
the client. Everything else is held equal - the pool and its size, the consumer group and the
consumer name, the read commands with their `COUNT` and `BLOCK`, the position of the ack, the
decode into the same type, the payload bytes, the tokio runtime and the build. The procedure is
the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

Medians over eleven interleaved pairs, with the observed spread in parentheses. Higher is better.

| Scenario | Raw client | RustStream | Overhead |
| --- | --- | --- | --- |
| Redis Streams consumer group, 512 B JSON, ack each | 27,700 msg/s (25,275-29,730) | 27,712 msg/s (25,131-29,872) | indistinguishable (broker-bound) |
| Redis list work queue (reliable), 512 B JSON, ack each | 15,843 msg/s (13,932-16,499) | 15,633 msg/s (14,215-16,317) | indistinguishable (broker-bound) |
| Redis Pub/Sub, 512 B JSON, no acknowledgement | 391,599 msg/s (356,408-399,937) | 381,917 msg/s (344,982-393,916) | indistinguishable |

Every row came out indistinguishable: the difference between the two halves is smaller than the
spread between runs of either half, and a figure below the run-to-run noise would read as precision
that was never measured.

The first two rows say why. A settled delivery costs a command of its own - `XACK` for a stream
entry, `LREM` for a list entry - and one command to a Redis on the loopback costs 40 microseconds
on this machine. A stream delivery costs 36 microseconds end to end and a list entry 63, so the
consumer spends its window waiting on the socket. That is what the `broker-bound` mark means: the
framework does its work inside that wait, which for a consumer that settles every message is a real
result, and at the same time a lower bound on the cost of dispatch rather than a measurement of it.

Pub/Sub settles nothing, and it is the row where the framework's own work has room to show: a
delivery there costs 2.6 microseconds, fourteen times less than a stream delivery. Even there the
difference between the halves stays inside the spread, so no percentage is published for it either.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-fred/latest/benchmarks/results.json).

## The machine

| | |
| --- | --- |
| CPU | AMD Ryzen 9 7900X, 12 physical cores, 24 logical |
| Memory | 62.4 GiB |
| OS | Linux 7.2.6 |
| Broker | `redis:7-alpine` in Docker on localhost |
| Rust | 1.98.1, bench profile, no `RUSTFLAGS` |
| Versions | `ruststream-fred` 0.7.0 on `ruststream` 0.7.0-rc.7 |

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one key, a small body and a server on the loopback. It measures what a
delivery costs in this crate, not what Redis can carry, and a row here is not comparable with a row
published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last handler returns, on
both halves alike. The framework settles a delivery after the handler is done, which is a point the
handler itself cannot observe, so one acknowledgement out of a hundred thousand sits outside the
number on both sides.

The run turns the server's append-only file off. What is measured is the cost of a delivery, not
the disk under the server, and an `fsync` that lands inside one half of a pair is noise that
belongs to neither. A service that keeps the append-only file on pays for it, and pays the same on
both sides.

The Pub/Sub figure is taken under a publisher that never waits for the consumer. Redis Pub/Sub
drops what a consumer is not there to take rather than queueing it, so what that row reports is
deliveries handled per second by a saturated consumer.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs every scenario, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about ten minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.
