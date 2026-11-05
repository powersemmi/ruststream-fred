//! How this crate's native batches fill the slot the runtime lends: a read hands its deliveries
//! over one at a time, each marked with whether the read ends with it.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::Stream;
use pin_project_lite::pin_project;
use ruststream::BatchStream;

/// One delivery of a read, and whether it is the read's last.
///
/// A read is one batch: the stream that yields these decides at the start of a read how many
/// deliveries it holds, so a batch never spans two reads and never exceeds the size it was opened
/// at.
pub(crate) struct Member<Message> {
    message: Message,
    last: bool,
}

impl<Message> Member<Message> {
    /// `message`, followed by `left` more deliveries of the same read.
    pub(crate) const fn new(message: Message, left: usize) -> Self {
        Self {
            message,
            last: left == 0,
        }
    }
}

pin_project! {
    /// The batch stream of a subscription that reads whole batches from the server: it moves the
    /// deliveries of one read into the lent slot and reports the batch when the read's last one
    /// is in.
    ///
    /// The members of a read follow each other without waiting, so a batch is filled within one
    /// poll; a poll that waits leaves the slot as it found it.
    pub(crate) struct ReadBatches<Reads> {
        #[pin]
        reads: Reads,
        ended: bool,
    }
}

impl<Reads> ReadBatches<Reads> {
    pub(crate) const fn new(reads: Reads) -> Self {
        Self {
            reads,
            ended: false,
        }
    }
}

impl<Reads, Message, Error> BatchStream<Vec<Message>> for ReadBatches<Reads>
where
    Reads: Stream<Item = Result<Member<Message>, Error>>,
{
    type Error = Error;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Vec<Message>,
    ) -> Poll<Option<Result<(), Error>>> {
        let mut this = self.project();
        if *this.ended {
            return Poll::Ready(None);
        }
        loop {
            match ready!(this.reads.as_mut().poll_next(cx)) {
                Some(Ok(member)) => {
                    slot.push(member.message);
                    if member.last {
                        return Poll::Ready(Some(Ok(())));
                    }
                }
                Some(Err(error)) => {
                    // A failed read is not handed over in part: what it delivered before the
                    // failure leaves unsettled, as a dropped batch always did.
                    slot.clear();
                    return Poll::Ready(Some(Err(error)));
                }
                None => {
                    *this.ended = true;
                    return Poll::Ready(if slot.is_empty() { None } else { Some(Ok(())) });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::pin::pin;

    use futures::stream;

    use super::*;

    /// Fills one slot from `batches` and reports what the poll answered.
    async fn fill(
        batches: Pin<&mut ReadBatches<impl Stream<Item = Result<Member<u32>, &'static str>>>>,
        slot: &mut Vec<u32>,
    ) -> Option<Result<(), &'static str>> {
        let mut batches = batches;
        poll_fn(|cx| batches.as_mut().poll_fill(cx, slot)).await
    }

    fn read(messages: &[u32]) -> Vec<Result<Member<u32>, &'static str>> {
        let count = messages.len();
        messages
            .iter()
            .enumerate()
            .map(|(at, message)| Ok(Member::new(*message, count - at - 1)))
            .collect()
    }

    #[tokio::test]
    async fn each_read_is_one_batch() {
        let reads = stream::iter(read(&[1, 2, 3]).into_iter().chain(read(&[4])));
        let mut batches = pin!(ReadBatches::new(reads));
        let mut slot = Vec::new();

        assert_eq!(fill(batches.as_mut(), &mut slot).await, Some(Ok(())));
        assert_eq!(slot.drain(..).collect::<Vec<_>>(), [1, 2, 3]);
        assert_eq!(fill(batches.as_mut(), &mut slot).await, Some(Ok(())));
        assert_eq!(slot, [4]);
    }

    #[tokio::test]
    async fn a_failed_read_leaves_the_slot_empty() {
        let reads = stream::iter([Ok(Member::new(1, 2)), Err("decode"), Ok(Member::new(3, 0))]);
        let mut batches = pin!(ReadBatches::new(reads));
        let mut slot = Vec::new();

        assert_eq!(fill(batches.as_mut(), &mut slot).await, Some(Err("decode")));
        assert!(slot.is_empty());
        assert_eq!(fill(batches.as_mut(), &mut slot).await, Some(Ok(())));
        assert_eq!(slot, [3]);
    }

    #[tokio::test]
    async fn the_end_hands_the_partial_batch_over_first() {
        let reads = stream::iter([Ok(Member::new(1, 1))]);
        let mut batches = pin!(ReadBatches::new(reads));
        let mut slot = Vec::new();

        assert_eq!(fill(batches.as_mut(), &mut slot).await, Some(Ok(())));
        assert_eq!(slot.drain(..).collect::<Vec<_>>(), [1]);
        assert_eq!(fill(batches.as_mut(), &mut slot).await, None);
        assert_eq!(fill(batches.as_mut(), &mut slot).await, None);
    }
}
