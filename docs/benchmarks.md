# Benchmarks

An adapter between the Redis client and a message type costs time on every delivery: the read,
the decode, the settle. This page says how much, measured against the same work written by hand on
`fred`.

A scenario is run three ways in one process. **Raw client** is a loop written by hand on `fred`.
**ruststream-fred** is the same loop written by hand on this crate: the broker, the subscription,
the stream of deliveries and the ack, with no service, no handler and no dispatch.
**RustStream service** is the service a user writes, started through the real runtime.

That gives two differences, and they answer different questions. The adapter against the raw client
is what this crate's own consumer costs over the client it wraps, which is the number this
repository is responsible for. The service against the raw client is what a reader pays end to end.
The gap between them is what the runtime costs on top of this broker in particular; it is published
per broker because every adapter is thin, so a share that differs between brokers lives in how the
two meet - how the stream yields, how deliveries arrive, how back-pressure reaches the consumer.

Everything else is held equal: the pool and its size, the consumer group and the consumer name, the
read commands with their `COUNT` and `BLOCK`, the position of the ack, the decode into the same
type, the payload bytes, the tokio runtime and the build. The procedure is the framework's own and
is described under
[methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology); this page
publishes what it produced here.

Three consumer forms are measured, one per delivery shape this crate offers: a Redis Streams
consumer group acknowledging every entry, a reliable list work queue, and a Pub/Sub channel. Each
is measured against the three server forms this crate connects to: a standalone server, a cluster
and a master behind Sentinel. The subscription is the same on all three; what differs underneath is
the client's routing, so a difference between those rows is a finding about that.

## The numbers

The best of three interleaved rounds, with the slowest round in parentheses. Higher is
better. A difference smaller than the spread between runs is reported as `indistinguishable`
rather than as a percentage.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "ruststream-fred", "framework": "RustStream service", "adapterOverhead": "Adapter over the client", "overhead": "Service over the client", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "unavailable": "The published results could not be loaded.", "cpu": "CPU", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "OS", "broker": "Broker", "rustc": "Rust", "profile": "Profile", "features": "Features", "rustflags": "RUSTFLAGS", "versions": "Versions", "measured": "Measured on"}'></div>

The table is read from the document below every time the page is opened, so what it shows is the
last run and nothing else.

Settling a delivery on Redis costs a command of its own - `XACK` for a stream entry, `LREM` for a
list entry - and on a server reached over the loopback that command costs tens of microseconds,
which is an order of magnitude more than a delivery spends inside this crate. The consumer spends
its window waiting on the socket, so those two rows carry the `broker-bound` mark: everything above
the socket does its work inside a wait the consumer was already paying for, which for a consumer
that settles every message is a real result and at the same time a lower bound on those costs
rather than a measurement of them.

Pub/Sub settles nothing, and it is the row where the work above the socket has room to show: a
delivery there is a socket read, an unframing and a decode, and it costs an order of magnitude less
than a stream delivery.

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
delivery costs in this crate and in the runtime over it, not what Redis can carry, and a row here is
not comparable with a row published for another broker: the transports do different work per
message.

The window a run measures opens at the first delivery and closes when the last one is taken, alike
in all three loops, so one acknowledgement out of a run sits outside the number everywhere.

The publish path is not in these rows. All three loops are fed by the same pipelined `fred`
publisher, so that what differs between them stays on the consuming side; what this crate's own
publisher costs is a measurement of its own.

The stand runs its servers on the host network and without persistence: no port proxy between the
client and the server, no append-only file, no snapshot. What is measured is the cost of a
delivery, not the bridge in front of the server or the disk under it. A service that keeps
persistence on pays for it, and pays the same on both sides.

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
