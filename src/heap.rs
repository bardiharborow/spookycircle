//! The heap-owned endpoints (`alloc` feature).

use crate::raw::{HeapOwner, RawConsumer, RawProducer, typed::TypedLife};

/// The unique producer endpoint of a queue created by [`bounded`](crate::bounded).
///
/// Only the producer can insert values. It cannot be cloned or shared
/// between threads (`!Sync`), but it can be moved to another thread when
/// `T: Send`. Dropping it permanently ends production; the consumer can still
/// drain every value that was published before the drop and can detect the
/// end of the stream with [`Consumer::is_drained`].
///
/// All methods are wait-free scalar operations on certified targets (see the
/// [crate documentation](crate#wait-free-scope)) except `push_slice`, whose
/// work is bounded by its input length, and `Drop`. Whichever endpoint is
/// dropped last drops the values still queued and frees the heap memory.
#[must_use = "dropping the producer permanently ends production"]
pub struct Producer<T> {
    raw: RawProducer<T, usize, TypedLife<usize, HeapOwner>>,
}

/// The unique consumer endpoint of a queue created by [`bounded`](crate::bounded).
///
/// Only the consumer can remove or borrow values. It cannot be cloned or
/// shared between threads (`!Sync`), but it can be moved to another thread
/// when `T: Send`. Dropping it permanently ends consumption; the producer
/// stays usable and can keep pushing until the queue is full, but nothing it
/// pushes will ever be consumed.
///
/// All methods are wait-free scalar operations on certified targets (see the
/// [crate documentation](crate#wait-free-scope)) except `pop_slice`, whose
/// work is bounded by its output length, and `Drop`. Whichever endpoint is
/// dropped last drops the values still queued and frees the heap memory.
#[must_use = "dropping the consumer permanently ends consumption"]
pub struct Consumer<T> {
    raw: RawConsumer<T, usize, TypedLife<usize, HeapOwner>>,
}

/// Wraps a freshly created raw pair.
#[inline]
pub(crate) fn pair_from_raw<T>(
    (producer, consumer): crate::raw::Endpoints<T, usize, TypedLife<usize, HeapOwner>>,
) -> (Producer<T>, Consumer<T>) {
    (Producer { raw: producer }, Consumer { raw: consumer })
}

producer_api! {
    impl[T] Producer<T>,
    elem = T,
    copy_where = [T: Copy],
    name = "Producer",
}

consumer_api! {
    impl[T] Consumer<T>,
    elem = T,
    copy_where = [T: Copy],
    name = "Consumer",
}
