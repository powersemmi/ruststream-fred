---
title: Methodology
description: "How ruststream-fred measures itself: the three loops, the run on a real Redis server, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-fred` measures itself: the three loops, the run on a real Redis server, the
broker-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on Redis.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** A loop on `fred`: read, decode, acknowledge.
- **Broker crate.** This crate and nothing above it: the broker connects, a descriptor opens a
  subscription, and a loop pulls deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the pool and its size, the consumer group and the
consumer name, the read commands with their `COUNT` and `BLOCK`, the place of the ack, the decode
into the same type, the payload bytes, the tokio runtime with four worker threads, and the binary.
All three are fed by the same pipelined `fred` publisher, so what differs between them is on the
consuming side.

The body is 512 bytes of JSON.

## The run

- **The message count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The window opens at the first delivery and closes when the last one is taken**, in all three
  loops.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `broker-bound` when the raw client spent the run waiting on the socket. A probe times one
`EXISTS` and its answer on each topology, and the round trip is published with the machine, so the
arithmetic can be checked. Settling a stream entry or a list entry costs a command of its own,
`XACK` or `LREM`, and on the loopback that round trip costs more than a delivery spends inside this
crate. Those rows carry the mark. Pub/Sub settles nothing, and it is the row where the work above
the socket shows.

## The code count

Each scenario of the cost chart on [Benchmarks](/architecture/benchmarks/) is the service a user
writes, built without the `testing` feature. Messages are appended from another thread before the measured
region opens, so producing them is not counted.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery,
then 1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

The server is real, so a count moves a little between runs. Over six runs the instruction totals
of a scenario stayed within 0.4 percent of each other, and its allocations within a few blocks.
Each allocation floor is the highest count seen plus a margin of 0.1 percent. `just bench-code`
fails on an allocation above the floor a scenario declares, and, with `--baseline=main`, on more
than two percent more instructions.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs every scenario, stops the stand
and rewrites `docs/benchmarks/results.json`. It takes about ten minutes and wants the machine to
itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on. The build
profile is recorded in the document with the numbers.

```bash
just bench-code
```

The code count starts the same stand, counts the scenarios under valgrind against its standalone
server, stops the stand and rewrites the `code` section of the same document. It needs valgrind,
and installs the benchmark runner itself. `just bench-code --save-baseline=main` on `main` records
a baseline, and `just bench-code --baseline=main` on a branch compares with it.
