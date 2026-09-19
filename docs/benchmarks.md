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

Three scenarios are measured, one per delivery shape this crate offers: a Redis Streams consumer
group acknowledging every entry, a reliable list work queue, and a Pub/Sub channel.

## The numbers

Medians over eleven interleaved pairs, with the observed spread in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "framework": "RustStream", "overhead": "Overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "unavailable": "The published results could not be loaded.", "cpu": "CPU", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "OS", "broker": "Broker", "rustc": "Rust", "profile": "Profile", "features": "Features", "rustflags": "RUSTFLAGS", "versions": "Versions", "measured": "Measured on"}'></div>

The table is read from the document below every time the page is opened, so what it shows is the
last run and nothing else.

Settling a delivery on Redis costs a command of its own - `XACK` for a stream entry, `LREM` for a
list entry - and on a server reached over the loopback that command costs tens of microseconds,
which is an order of magnitude more than a delivery spends inside this crate. The consumer spends
its window waiting on the socket, so those two rows carry the `broker-bound` mark: the framework
does its work inside a wait the consumer was already paying for, which for a consumer that settles
every message is a real result and at the same time a lower bound on the cost of dispatch rather
than a measurement of it.

Pub/Sub settles nothing, and it is the row where the framework's own work has room to show: a
delivery there is a socket read, a decode and a handler call, and it costs an order of magnitude
less than a stream delivery.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-fred/latest/benchmarks/results.json).

## The machine

<div id="benchmark-machine"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one key, a small body and a server on the loopback. It measures what a
delivery costs in this crate, not what Redis can carry, and a row here is not comparable with a row
published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last handler returns, on
both halves alike. The framework settles a delivery after the handler is done, which is a point the
handler itself cannot observe, so one acknowledgement out of a run sits outside the number on both
sides.

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
