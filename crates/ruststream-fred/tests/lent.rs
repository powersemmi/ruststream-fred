//! A batch stream read the way the runtime reads it: a slot lent to each fill and emptied after
//! it, so a test takes the batches as a stream of `Vec`s.

use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use ruststream::BatchStream;

/// The batches of `Fill`, each handed out as the `Vec` its fill left in the slot.
pub(crate) struct Lent<Fill, Message> {
    fill: Pin<Box<Fill>>,
    slot: Vec<Message>,
}

/// Reads `fill` through a lent slot.
pub(crate) fn lent<Fill, Message>(fill: Fill) -> Lent<Fill, Message>
where
    Fill: BatchStream<Vec<Message>>,
{
    Lent {
        fill: Box::pin(fill),
        slot: Vec::new(),
    }
}

// The fill is boxed and the slot is only moved; nothing is pinned in place.
impl<Fill, Message> Unpin for Lent<Fill, Message> {}

impl<Fill, Message> Stream for Lent<Fill, Message>
where
    Fill: BatchStream<Vec<Message>>,
{
    type Item = Result<Vec<Message>, Fill::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.fill
            .as_mut()
            .poll_fill(cx, &mut this.slot)
            .map(|fill| fill.map(|filled| filled.map(|()| mem::take(&mut this.slot))))
    }
}
