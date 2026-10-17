---
title: Numbers
description: "What the benchmark numbers of ruststream-fred measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-fred` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, on one server. A row is not comparable with a row another broker crate publishes: the
transports do different work per message.

The server runs on the same machine, on the host network, with persistence off. With no network
latency and no disk, the same cost per message is a larger share of the time than it would be
against a server across a network. Read the percentage as an upper bound on what a deployed
service pays.

A `broker-bound` row is one where the raw client waited on the socket for most of the run. The work
of the crate and the runtime then happens inside that wait, and the difference comes out near zero.
That is a lower bound on the cost, not a measurement of it.

The Pub/Sub row is taken under a publisher that never waits for the consumer. Pub/Sub drops what a
subscription is not there to take, so that row reports the deliveries a saturated subscription
handles per second.

## The cost of the code

An instruction count holds the runtime, this crate and `fred`'s work on the service's thread. It
includes the framework's own cost; the core publishes that cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/). The same count costs
more time where it misses the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Connecting, creating the group and the first allocations behind them are the cold start, paid
once.

## What they do not mean

A row is one consumer reading one key, a small body and a server on the loopback. It measures what
a delivery costs inside this crate and the runtime over it, not what Redis can carry.

The window of a run opens at the first delivery and closes when the last one is taken, alike in
all three loops, so one acknowledgement of a run sits outside the number everywhere.

The publish path is not in the comparison: all three loops share one publisher. The code chart
counts a reply on the same stand.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
