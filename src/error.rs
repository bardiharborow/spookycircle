//! Error types.

use core::{error::Error, fmt};

/// Failure to construct a ring buffer.
///
/// Returned by `bounded` (with the `alloc` feature) and
/// [`BorrowedStorage::new`](crate::BorrowedStorage::new).
/// [`StaticStorage::new`](crate::StaticStorage::new) rejects an invalid
/// capacity at compile time instead. Construction validates
/// in this order: a zero capacity, a capacity above
/// [`MAX_CAPACITY`](crate::MAX_CAPACITY) or whose backing layout cannot be
/// represented on the target, and finally (heap-owned mode only) allocator
/// failure. No endpoint is created and nothing is leaked when an error is
/// returned; caller-owned slots are left untouched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CreateError {
    /// The requested capacity was zero.
    ZeroCapacity,
    /// The requested capacity or backing layout was not representable.
    CapacityTooLarge {
        /// The capacity that was requested.
        requested: usize,
    },
    /// The allocator reported that it could not satisfy the request.
    ///
    /// Only the heap-owned `bounded` constructor can return this. A global
    /// allocator that aborts instead of reporting failure never produces this
    /// variant.
    AllocationFailed,
}

impl fmt::Display for CreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => f.write_str("ring buffer capacity must be at least 1"),
            Self::CapacityTooLarge { requested } => write!(
                f,
                "ring buffer capacity {requested} exceeds the representable maximum"
            ),
            Self::AllocationFailed => f.write_str("ring buffer allocation failed"),
        }
    }
}

impl Error for CreateError {}

/// Failure to split borrowed or static storage into an endpoint pair.
///
/// Returned by [`BorrowedStorage::try_split`](crate::BorrowedStorage::try_split)
/// and [`StaticStorage::try_split`](crate::StaticStorage::try_split). A
/// failed split changes nothing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SplitError {
    /// A pair has already been issued since construction or exclusive reset.
    ///
    /// Dropping both endpoints does not rearm the storage; call `reset`
    /// (which needs exclusive access) to start a new session.
    AlreadySplit,
}

impl fmt::Display for SplitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadySplit => {
                f.write_str("ring buffer storage has already been split since its last reset")
            }
        }
    }
}

impl Error for SplitError {}

/// A value that could not be inserted because the ring was full.
///
/// Returned by every producer's `try_push`. The rejected
/// value is always retained inside, unchanged and never cloned or dropped by
/// the library; recover it with [`into_inner`](Full::into_inner).
///
/// `Display` reports only that the ring was full and never formats the value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[must_use = "a `Full` error owns the rejected value; call `into_inner` to recover it"]
pub struct Full<T> {
    value: T,
}

impl<T> Full<T> {
    #[inline]
    pub(crate) fn new(value: T) -> Self {
        Self { value }
    }

    /// Borrows the rejected value.
    #[inline]
    pub fn get_ref(&self) -> &T {
        &self.value
    }

    /// Mutably borrows the rejected value.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Recovers ownership of the rejected value.
    ///
    /// This is the normative way to get the value back; the crate deliberately
    /// provides no `From<Full<T>> for T`.
    #[inline]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> fmt::Display for Full<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ring buffer is full")
    }
}

impl<T: fmt::Debug> Error for Full<T> {}
