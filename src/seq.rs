//! Sequence-number abstraction.
//!
//! Production code always instantiates the queue with `usize` positions. The
//! trait exists so that test-only code can run the identical state machine
//! with a deliberately narrow counter (`u8`) and force many complete
//! wraparounds in practical test time.

use crate::sync::Ordering;

/// An unsigned wrapping sequence counter with an atomic counterpart.
///
/// Positions are compared only by modular distance
/// (`Sequence::distance`), never by ordinary relational comparison.
pub(crate) trait Sequence: Copy + Eq + core::fmt::Debug {
    /// The shared atomic cell holding a published position.
    type Atomic;

    /// Largest logical capacity for which modular distance is unambiguous:
    /// half of the counter's range.
    const MAX_CAPACITY: usize;

    /// The initial position.
    const ZERO: Self;

    /// Creates an atomic cell holding `value`.
    fn atomic(value: Self) -> Self::Atomic;

    /// Loads the published position with the given ordering.
    fn load(atomic: &Self::Atomic, order: Ordering) -> Self;

    /// Publishes `value` with the given ordering.
    fn store(atomic: &Self::Atomic, value: Self, order: Ordering);

    /// Advances the position by `n` with wrapping arithmetic.
    ///
    /// Callers guarantee `n <= MAX_CAPACITY`, so `n` is representable.
    fn advance(self, n: usize) -> Self;

    /// The wrapping distance `self - from`, as an unsigned count.
    fn distance(self, from: Self) -> usize;
}

impl Sequence for usize {
    type Atomic = crate::sync::AtomicUsize;

    const MAX_CAPACITY: usize = usize::MAX / 2;
    const ZERO: Self = 0;

    #[inline(always)]
    fn atomic(value: Self) -> Self::Atomic {
        crate::sync::AtomicUsize::new(value)
    }

    #[inline(always)]
    fn load(atomic: &Self::Atomic, order: Ordering) -> Self {
        atomic.load(order)
    }

    #[inline(always)]
    fn store(atomic: &Self::Atomic, value: Self, order: Ordering) {
        atomic.store(value, order);
    }

    #[inline(always)]
    fn advance(self, n: usize) -> Self {
        self.wrapping_add(n)
    }

    #[inline(always)]
    fn distance(self, from: Self) -> usize {
        self.wrapping_sub(from)
    }
}

/// Narrow-counter model used only by the wraparound tests.
#[cfg(all(test, not(loom)))]
impl Sequence for u8 {
    type Atomic = crate::sync::AtomicU8;

    const MAX_CAPACITY: usize = u8::MAX as usize / 2;
    const ZERO: Self = 0;

    fn atomic(value: Self) -> Self::Atomic {
        crate::sync::AtomicU8::new(value)
    }

    fn load(atomic: &Self::Atomic, order: Ordering) -> Self {
        atomic.load(order)
    }

    fn store(atomic: &Self::Atomic, value: Self, order: Ordering) {
        atomic.store(value, order);
    }

    fn advance(self, n: usize) -> Self {
        debug_assert!(n <= Self::MAX_CAPACITY);
        self.wrapping_add(n as u8)
    }

    fn distance(self, from: Self) -> usize {
        self.wrapping_sub(from) as usize
    }
}
