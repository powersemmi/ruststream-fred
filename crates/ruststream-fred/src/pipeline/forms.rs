//! How each subscription form settles inside a window.

use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

use fred::clients::{Client, Pool};
use fred::error::Error;
use fred::interfaces::{KeysInterface, ListInterface, SortedSetsInterface, StreamsInterface};
use fred::types::Value;
use ruststream::AckError;

use super::window::{Form, Segment, Sent};
use crate::convert::fields_for_publish;
use crate::delay::{self, DelayConfig};
use crate::list::RedisListMessage;
use crate::message::RedisMessage;
use crate::pubsub::RedisPubSubMessage;
use crate::stream::RequeueMode;

/// A settle a stream delivery owes: its `XACK`, and on a retry the copy or the schedule that
/// carries it forward.
#[derive(Debug)]
pub enum StreamOp {
    Ack(String),
    Republish(Vec<(String, Vec<u8>)>),
    Schedule { score: f64, member: Vec<u8> },
}

/// The settle side of a [`RedisStream`](crate::RedisStream) subscription.
#[derive(Debug)]
pub struct StreamForm {
    key: Arc<str>,
    group: Arc<str>,
    delay: Option<DelayConfig>,
    requeue: RequeueMode,
}

impl StreamForm {
    pub(crate) fn new(
        key: impl Into<Arc<str>>,
        group: impl Into<Arc<str>>,
        delay: Option<DelayConfig>,
        requeue: RequeueMode,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            delay,
            requeue,
        }
    }

    /// Queues one settle into `sink`, a segment or the flush's settles.
    async fn queue<Sink>(&self, sink: &Sink, op: StreamOp) -> Result<(), Error>
    where
        Sink: StreamsInterface + SortedSetsInterface + KeysInterface + Sync,
    {
        match op {
            StreamOp::Ack(id) => sink.xack::<(), _, _, _>(&*self.key, &*self.group, id).await,
            StreamOp::Republish(fields) => {
                sink.xadd::<(), _, _, _, _>(&*self.key, false, None::<()>, "*", fields)
                    .await
            }
            StreamOp::Schedule { score, member } => {
                let cfg = self
                    .delay
                    .as_ref()
                    .expect("a schedule is owed only where a delay queue is named");
                sink.zadd::<(), _, _>(cfg.zset_key(), None, None, false, false, (score, member))
                    .await?;
                if let Some(ttl) = cfg.ttl_millis() {
                    sink.pexpire::<(), _>(cfg.zset_key(), ttl, None).await?;
                }
                Ok(())
            }
        }
    }
}

impl Form for StreamForm {
    type Message = RedisMessage;
    type Op = StreamOp;

    fn pool(msg: &RedisMessage) -> &Pool {
        msg.seeker().pool()
    }

    fn ack(&self, msg: RedisMessage, ops: &mut Vec<StreamOp>) -> Result<(), AckError> {
        let (id, _, _) = msg.into_settle();
        ops.push(StreamOp::Ack(id));
        Ok(())
    }

    fn nack(
        &self,
        msg: RedisMessage,
        requeue: bool,
        ops: &mut Vec<StreamOp>,
    ) -> Result<(), AckError> {
        // The claiming mode retries through the pending entries list: nothing is owed, and the
        // entry comes back on the subscription's own read.
        if requeue && let RequeueMode::LeavePending { .. } = self.requeue {
            drop(msg.into_settle());
            return Ok(());
        }
        let (id, payload, headers) = msg.into_settle();
        if requeue {
            ops.push(StreamOp::Republish(fields_for_publish(
                payload.to_vec(),
                &headers,
            )));
        }
        ops.push(StreamOp::Ack(id));
        Ok(())
    }

    fn nack_after(
        &self,
        msg: RedisMessage,
        delay_by: Duration,
        ops: &mut Vec<StreamOp>,
    ) -> Result<(), AckError> {
        if self.delay.is_none() {
            if let RequeueMode::LeavePending { .. } = self.requeue {
                drop(msg.into_settle());
                return Ok(());
            }
            return Err(AckError::Unsupported);
        }
        let (id, payload, headers) = msg.into_settle();
        // Scheduled before the `XACK` of the original, in the same order the direct path takes,
        // so a failure between the two leaves a duplicate rather than a loss.
        let (score, member) = delay::scheduled(&id, &payload, &headers, delay_by);
        ops.push(StreamOp::Schedule { score, member });
        ops.push(StreamOp::Ack(id));
        Ok(())
    }

    async fn close_atomic(&self, segment: &Segment, ops: &mut Vec<StreamOp>) -> Result<(), Error> {
        for op in ops.drain(..) {
            match segment {
                Segment::Plain(pipeline) => self.queue(pipeline, op).await?,
                Segment::Atomic(pinned) => self.queue(pinned, op).await?,
            }
        }
        Ok(())
    }

    async fn send_ops(&self, client: &Client, ops: &mut Vec<StreamOp>) -> Sent {
        let mut sent = Sent::default();
        if ops.is_empty() {
            return sent;
        }
        let settles = client.pipeline();
        // Every acknowledgement of the flush goes out as one `XACK`, after the copies and
        // schedules the retries owe.
        let mut acked = Vec::new();
        for op in ops.drain(..) {
            let result = match op {
                StreamOp::Ack(id) => {
                    acked.push(id);
                    Ok(())
                }
                op => self.queue(&settles, op).await,
            };
            if let Err(err) = result {
                sent.failed += 1;
                sent.first_error.get_or_insert(err);
            }
        }
        if !acked.is_empty()
            && let Err(err) = settles
                .xack::<(), _, _, _>(&*self.key, &*self.group, acked)
                .await
        {
            sent.failed += 1;
            sent.first_error.get_or_insert(err);
        }
        sent.record(settles.try_all::<Value>().await);
        sent
    }
}

/// A settle a reliable list delivery owes: its `LREM` off the processing list and the end of its
/// tracking, and on a requeue the `LPUSH` back onto the queue before them.
#[derive(Debug)]
pub struct ListOp {
    value: Vec<u8>,
    member: Option<Vec<u8>>,
    requeue: bool,
}

/// The settle side of a [`RedisList`](crate::RedisList) subscription.
#[derive(Debug)]
pub struct ListForm {
    main: Arc<str>,
    processing: Arc<str>,
    recovery_zset: Option<Arc<str>>,
}

impl ListForm {
    pub(crate) fn new(
        main: impl Into<Arc<str>>,
        processing: impl Into<Arc<str>>,
        recovery_zset: Option<&str>,
    ) -> Self {
        Self {
            main: main.into(),
            processing: processing.into(),
            recovery_zset: recovery_zset.map(Arc::from),
        }
    }

    fn settle(msg: RedisListMessage, requeue: bool, ops: &mut Vec<ListOp>) -> Result<(), AckError> {
        let Some((value, member)) = msg.into_settle() else {
            return Err(AckError::Unsupported);
        };
        ops.push(ListOp {
            value,
            member,
            requeue,
        });
        Ok(())
    }

    async fn queue<Sink>(&self, sink: &Sink, op: ListOp) -> Result<(), Error>
    where
        Sink: ListInterface + SortedSetsInterface + Sync,
    {
        // Back onto the queue before it leaves the processing list, so a failure between the
        // two leaves a duplicate rather than a loss.
        if op.requeue {
            sink.lpush::<(), _, _>(&*self.main, op.value.clone())
                .await?;
        }
        sink.lrem::<(), _, _>(&*self.processing, 1, op.value)
            .await?;
        if let (Some(zset), Some(member)) = (&self.recovery_zset, op.member) {
            sink.zrem::<(), _, _>(&**zset, member).await?;
        }
        Ok(())
    }
}

impl Form for ListForm {
    type Message = RedisListMessage;
    type Op = ListOp;

    fn pool(msg: &RedisListMessage) -> &Pool {
        msg.pool()
    }

    fn ack(&self, msg: RedisListMessage, ops: &mut Vec<ListOp>) -> Result<(), AckError> {
        Self::settle(msg, false, ops)
    }

    fn nack(
        &self,
        msg: RedisListMessage,
        requeue: bool,
        ops: &mut Vec<ListOp>,
    ) -> Result<(), AckError> {
        Self::settle(msg, requeue, ops)
    }

    fn nack_after(
        &self,
        msg: RedisListMessage,
        _delay: Duration,
        _ops: &mut Vec<ListOp>,
    ) -> Result<(), AckError> {
        // A list has no delay of its own: the runtime drops the entry and publishes the copy.
        drop(msg);
        Err(AckError::Unsupported)
    }

    async fn close_atomic(&self, segment: &Segment, ops: &mut Vec<ListOp>) -> Result<(), Error> {
        for op in ops.drain(..) {
            match segment {
                Segment::Plain(pipeline) => self.queue(pipeline, op).await?,
                Segment::Atomic(pinned) => self.queue(pinned, op).await?,
            }
        }
        Ok(())
    }

    async fn send_ops(&self, client: &Client, ops: &mut Vec<ListOp>) -> Sent {
        let mut sent = Sent::default();
        if ops.is_empty() {
            return sent;
        }
        let settles = client.pipeline();
        for op in ops.drain(..) {
            if let Err(err) = self.queue(&settles, op).await {
                sent.failed += 1;
                sent.first_error.get_or_insert(err);
            }
        }
        sent.record(settles.try_all::<Value>().await);
        sent
    }
}

/// The settle side of a [`RedisPubSub`](crate::RedisPubSub) subscription, which settles nothing:
/// its window carries the handlers' commands alone.
#[derive(Debug, Default)]
pub struct PubSubForm;

impl Form for PubSubForm {
    type Message = RedisPubSubMessage;
    type Op = std::convert::Infallible;

    fn pool(msg: &RedisPubSubMessage) -> &Pool {
        msg.pool()
    }

    fn ack(&self, _msg: RedisPubSubMessage, _ops: &mut Vec<Self::Op>) -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }

    fn nack(
        &self,
        _msg: RedisPubSubMessage,
        _requeue: bool,
        _ops: &mut Vec<Self::Op>,
    ) -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }

    fn nack_after(
        &self,
        _msg: RedisPubSubMessage,
        _delay: Duration,
        _ops: &mut Vec<Self::Op>,
    ) -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }

    fn close_atomic(
        &self,
        _segment: &Segment,
        _ops: &mut Vec<Self::Op>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        ready(Ok(()))
    }

    fn send_ops(
        &self,
        _client: &Client,
        _ops: &mut Vec<Self::Op>,
    ) -> impl Future<Output = Sent> + Send {
        ready(Sent::default())
    }
}

/// The in-process broker's settle side, for every form: a delivery settles in memory, at the
/// flush, the way the window settles it on a server.
#[cfg(feature = "testing")]
pub(crate) mod testing {
    use std::future::{Future, ready};
    use std::time::Duration;

    use fred::clients::{Client, Pool};
    use fred::error::Error;
    use ruststream::{AckError, IncomingMessage};

    use crate::pipeline::window::{Form, Segment, Sent};
    use crate::testing::RedisTestMessage;

    /// An outcome held until the window flushes.
    #[derive(Debug)]
    pub enum TestOp {
        Ack(RedisTestMessage),
        Nack(RedisTestMessage, bool),
        NackAfter(RedisTestMessage, Duration),
    }

    /// The settle side of a subscription on the in-process broker.
    #[derive(Debug, Default)]
    pub struct TestForm;

    impl Form for TestForm {
        type Message = RedisTestMessage;
        type Op = TestOp;

        fn pool(msg: &RedisTestMessage) -> &Pool {
            msg.pool()
        }

        fn ack(&self, msg: RedisTestMessage, ops: &mut Vec<TestOp>) -> Result<(), AckError> {
            let answer = msg.settle_answer();
            ops.push(TestOp::Ack(msg));
            answer
        }

        fn nack(
            &self,
            msg: RedisTestMessage,
            requeue: bool,
            ops: &mut Vec<TestOp>,
        ) -> Result<(), AckError> {
            let answer = msg.settle_answer();
            ops.push(TestOp::Nack(msg, requeue));
            answer
        }

        fn nack_after(
            &self,
            msg: RedisTestMessage,
            delay: Duration,
            ops: &mut Vec<TestOp>,
        ) -> Result<(), AckError> {
            if !msg.supports_nack_after() {
                // Held to the flush all the same, so the harness counts it settled there.
                ops.push(TestOp::Nack(msg, false));
                return Err(AckError::Unsupported);
            }
            ops.push(TestOp::NackAfter(msg, delay));
            Ok(())
        }

        /// The in-memory settles run after the segments, which is where a server runs the
        /// `EXEC` that carries them.
        fn close_atomic(
            &self,
            _segment: &Segment,
            _ops: &mut Vec<TestOp>,
        ) -> impl Future<Output = Result<(), Error>> + Send {
            ready(Ok(()))
        }

        async fn send_ops(&self, _client: &Client, ops: &mut Vec<TestOp>) -> Sent {
            for op in ops.drain(..) {
                // The stand-in's answers were given when the delivery settled; what is left is the
                // effect, which in memory cannot fail.
                let _ = match op {
                    TestOp::Ack(msg) => msg.ack().await,
                    TestOp::Nack(msg, requeue) => msg.nack(requeue).await,
                    TestOp::NackAfter(msg, delay) => msg.nack_after(delay).await,
                };
            }
            Sent::default()
        }
    }
}
