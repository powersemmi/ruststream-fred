---
title: Benchmarks
description: "What ruststream-fred costs on every message: throughput against the raw fred client, and instructions and allocations of its code."
---

import {BenchmarkEnvironment, BenchmarkResults, MovedSections} from '@site/src/components/Benchmarks';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The broker", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Broker", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-fred` costs on every message: throughput against the raw fred client, and
instructions and allocations of its code.

## Against a raw client

The first chart runs each scenario three ways on a real Redis server. `Raw client` is a loop
written by hand on `fred`. `Broker crate` is the same loop written on this crate: the broker, the
subscription, the stream of deliveries and the ack, with no runtime. `RustStream` is the service a
user writes, a `#[subscriber]` handler in an app. The first gap is what this crate costs over the
client it wraps, and the second is what the runtime adds on top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

Three forms are measured, one per transport: a stream read through a consumer group that
acknowledges every entry, a [reliable list](../redis/queues.md), and a Pub/Sub channel. Each runs
against a standalone server, a cluster and a primary behind Sentinel. On the cluster the Pub/Sub
row is sharded, `SSUBSCRIBE` and `SPUBLISH`, the form a cluster is used with.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on
[`RedisBroker`](api:ruststream_fred::RedisBroker) against the standalone server of the stand. The
service runs on a single-threaded runtime, and `fred` drives its connections on the same thread,
so the count holds the runtime, this crate, and `fred` writing the commands and parsing the
replies. The server is another process and is not counted. `Cold start` is what connecting the
pool, creating the group, opening the subscription and taking the first delivery cost once.

A stream scenario reads a consumer group with `XREADGROUP`, settles with `XACK` and replies with
`XADD`. A reliable list claims each entry with `BLMOVE` and settles it with `LREM`. A Pub/Sub
scenario reads a channel or a pattern and settles nothing. A `.pipeline()` scenario sends the
settles of a read together and its handler queues one `INCR` per delivery, and
`.pipeline().atomic()` wraps each delivery's segment in `MULTI` and `EXEC`, as
[Window](../window.md) draws.

## Results

<BenchmarkResults labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<BenchmarkEnvironment labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipes, and the build profile they were taken with is the one recorded above.
[Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what they allow
you to conclude.

<MovedSections />
