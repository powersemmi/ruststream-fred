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
the client's routing, so a difference between those rows is a finding about that. The one exception
is deliberate: the Pub/Sub row on the cluster is sharded, `SSUBSCRIBE` and `SPUBLISH`, the form a
cluster is used with, where classic `PUBLISH` is broadcast to every node.

## The numbers

The best of three interleaved rounds, with the median round in parentheses. Higher is
better. A difference smaller than the spread between runs is reported as `indistinguishable`
rather than as a percentage.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "ruststream-fred", "framework": "RustStream service", "adapterOverhead": "Adapter over the client", "overhead": "Service over the client", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "The published results could not be loaded.", "cpu": "CPU", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "OS", "broker": "Broker", "rustc": "Rust", "valgrind": "valgrind", "profile": "Profile", "features": "Features", "rustflags": "RUSTFLAGS", "versions": "Versions", "measured": "Measured on"}'></div>

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

## The crate's own code

<div id="benchmark-code"></div>

The second table is this crate's own cost per message, counted rather than timed: instructions
under callgrind and allocations under DHAT. Each scenario is the service a user writes, built on
`RedisBroker` and started against the standalone server of the stand, so every command in it is
one a service sends: `XREADGROUP` to read a `RedisStream` consumer group, `XACK` to settle, and
`XADD` to reply through `RedisPublish`.

The service runs on a single-threaded tokio runtime, and `fred` drives its connections on the same
thread. Everything on that thread is counted: the framework, this crate, and `fred` writing the
commands and parsing the replies. The server is another process and is not in the number, and
neither is the kernel's side of a socket call. The messages are appended to the stream from
another thread before the measured drain starts, so producing them is not counted either.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what connecting the pool, creating the consumer
group, opening the subscription and taking the first delivery cost once. The numbers are absolute,
the framework's own cost included; the core publishes that cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

The service talks to a real server, so a count moves a little between runs: over five runs the
instruction totals of a scenario stayed within 0.4% of each other and its allocations within seven
blocks of 59,000. Each floor is therefore the highest count seen plus a margin of 0.1%.
`just bench-code` fails on an allocation above the floor a scenario declares, and with
`--baseline=main` on more than two percent more instructions, and a pull request that changes the
cost cites its numbers.

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

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs every scenario, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about ten minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.

```bash
just bench-code
```

The recipe starts the same stand, counts the code table under valgrind against its standalone
server, stops the stand and rewrites the `code` section of the same document. It needs valgrind
and the benchmark runner: `cargo install --locked gungraun-runner --version =0.19.4`.
