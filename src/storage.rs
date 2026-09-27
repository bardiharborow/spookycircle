//! Allocator-free typed storage: caller-borrowed slots and inline/static
//! storage.

use core::{marker::PhantomData, ptr::NonNull};

use crate::{
    MAX_CAPACITY,
    error::{CreateError, SplitError},
    raw::{
        RawConsumer, RawProducer, Slot,
        typed::{CallerEndpoints, CallerOwned, Control, TypedLife, split},
        validate_capacity,
    },
};

/// Control state that borrows an exclusive slice of caller-owned slots.
///
/// The queue's capacity is exactly `slots.len()`. All control state lives
/// inline in this object; nothing is allocated, ever. The slots stay
/// mutably borrowed, and therefore inaccessible to the caller, for as long
/// as the storage object exists.
///
/// # Sessions
///
/// [`try_split`](Self::try_split) hands out one [`BorrowedProducer`] /
/// [`BorrowedConsumer`] pair and claims the storage. The claim survives
/// both endpoints being dropped, so a second `try_split` returns
/// [`SplitError::AlreadySplit`]; [`reset`](Self::reset), which needs
/// exclusive access and therefore proves that no endpoint or peek reference
/// is still usable, starts a new session.
///
/// Whichever endpoint of a pair is dropped last drops the values still
/// queued, exactly once each. The storage itself never drops or frees
/// anything.
///
/// # Forgotten endpoints
///
/// Forgetting an endpoint (for example with [`core::mem::forget`]) is
/// memory-safe but leaks: once exclusive access is regained, `reset` or
/// dropping the storage abandons the values that were still queued, without
/// running their destructors, and treats their bytes as uninitialized.
///
/// # Memory placement
///
/// The slots can live in any memory the caller chooses, so this is the mode
/// to use when placement matters. For large or latency-critical rings,
/// prefault the slots (`MAP_POPULATE`, or write every page once), back them
/// with huge pages to avoid TLB misses (Linux `MADV_HUGEPAGE` or
/// `MAP_HUGETLB`), and `mlock` them so they stay resident. The queue itself
/// never maps, advises, or locks memory.
///
/// # Example
///
/// ```
/// use spookycircle::{BorrowedStorage, Slot};
///
/// let mut slots = [const { Slot::<u32>::new() }; 3];
/// let mut storage = BorrowedStorage::new(&mut slots).unwrap();
/// {
///     let (mut producer, mut consumer) = storage.try_split().unwrap();
///     producer.try_push(7).unwrap();
///     assert_eq!(consumer.try_pop(), Some(7));
///     drop(producer);
///     assert!(consumer.is_drained());
///     drop(consumer);
/// }
///
/// // Both endpoint borrows have ended; exclusive reuse is now legal.
/// storage.reset();
/// let (mut producer, mut consumer) = storage.try_split().unwrap();
/// assert_eq!(producer.capacity(), 3);
/// producer.try_push(9).unwrap();
/// assert_eq!(consumer.try_pop(), Some(9));
/// drop((producer, consumer));
/// ```
#[expect(
    missing_debug_implementations,
    reason = "storage deliberately has no `Debug`; adding one is an API decision, not a lint fix"
)]
pub struct BorrowedStorage<'storage, T> {
    control: Control<usize>,
    slots: &'storage mut [Slot<T>],
    /// The storage logically owns the `T`s its slots may hold, for
    /// drop-check purposes.
    _owns: PhantomData<T>,
}

impl<'storage, T> BorrowedStorage<'storage, T> {
    /// Creates unclaimed storage over `slots`, whose length is the capacity.
    ///
    /// The slots are treated as uninitialized; their bytes are never read or
    /// dropped.
    ///
    /// # Errors
    ///
    /// [`CreateError::ZeroCapacity`] for an empty slice and
    /// [`CreateError::CapacityTooLarge`] above
    /// [`MAX_CAPACITY`](crate::MAX_CAPACITY). Never
    /// [`CreateError::AllocationFailed`]. On error the slots are unchanged.
    pub fn new(slots: &'storage mut [Slot<T>]) -> Result<Self, CreateError> {
        // A slice that exists already has a representable layout, so the
        // capacity checks are the whole validation.
        validate_capacity(slots.len(), MAX_CAPACITY)?;
        Ok(Self {
            control: Control::new(),
            slots,
            _owns: PhantomData,
        })
    }

    /// Returns the fixed capacity, `slots.len()`.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Claims this session and returns its unique endpoint pair.
    ///
    /// Takes `&self`, so the storage can be shared (for example between
    /// threads that race to split it); exactly one caller wins.
    ///
    /// # Errors
    ///
    /// [`SplitError::AlreadySplit`] if a pair has been issued since
    /// construction or the last [`reset`](Self::reset), even if both of its
    /// endpoints have since been dropped. The failure changes nothing.
    ///
    /// # Progress
    ///
    /// Not part of the wait-free guarantee: one compare-and-swap, no loop.
    pub fn try_split(
        &self,
    ) -> Result<(BorrowedProducer<'_, T>, BorrowedConsumer<'_, T>), SplitError> {
        let base = NonNull::from(&*self.slots).cast::<Slot<T>>();
        // SAFETY: `base` is derived from the live, exclusively borrowed slot
        // slice, which (like `self.control`) cannot move, be reset, or be
        // accessed by the owner while `&self` is borrowed by the returned
        // endpoints. `control` is fresh unless already claimed:
        // construction and `reset` are the only ways to make it unclaimed.
        let pair = unsafe { split(&self.control, base, self.slots.len()) };
        pair.map(pair_from_raw).ok_or(SplitError::AlreadySplit)
    }

    /// Ends the previous session and rearms the storage for one new split.
    ///
    /// Exclusive access proves that no endpoint or peek reference from an
    /// earlier session is usable. Normally the final endpoint has already
    /// dropped the queued values; if an endpoint was forgotten, the values
    /// it left queued are abandoned without running their destructors.
    ///
    /// Not part of the wait-free guarantee.
    pub fn reset(&mut self) {
        // Positions, the saved physical index, liveness, shares, and the
        // claim all return to their initial values. The slots are left
        // alone: they are logically uninitialized again.
        self.control = Control::new();
    }
}

/// Inline slots and control state, suitable for stack or `static` placement.
///
/// Holds exactly `N` slots and is `const`-constructible, so it can
/// initialize an ordinary (immutable) `static` without an allocator, a
/// `static mut`, or runtime initialization of any `T`. Session, cleanup,
/// reset, and forgotten-endpoint behaviour are exactly those of
/// [`BorrowedStorage`].
///
/// Rust never runs destructors for `static`s, but a static queue needs none:
/// its final endpoint drops the values still queued. A `const` item is not a
/// substitute for one `static`, since every use of a `const` creates a
/// separate queue.
///
/// # Example
///
/// ```
/// use spookycircle::StaticStorage;
///
/// static STORAGE: StaticStorage<u32, 8> = StaticStorage::new();
///
/// let (mut producer, mut consumer) = STORAGE.try_split().unwrap();
/// producer.try_push(42).unwrap();
/// assert_eq!(consumer.try_pop(), Some(42));
/// drop((producer, consumer));
///
/// // The static is one session: endpoint drop does not rearm it.
/// assert!(STORAGE.try_split().is_err());
/// ```
#[expect(
    missing_debug_implementations,
    reason = "storage deliberately has no `Debug`; adding one is an API decision, not a lint fix"
)]
pub struct StaticStorage<T, const N: usize> {
    control: Control<usize>,
    slots: [Slot<T>; N],
    /// Drop-check ownership of the `T`s the slots may hold.
    _owns: PhantomData<T>,
}

impl<T, const N: usize> StaticStorage<T, N> {
    const_unless_loom! {
        /// Creates unclaimed storage with capacity `N`.
        ///
        /// `const`: needs neither `T: Copy` nor `T: Default`, and never
        /// creates a `T` or allocates. It cannot fail at run time.
        ///
        /// # Compile-time errors
        ///
        /// `N == 0` and `N >` [`MAX_CAPACITY`](crate::MAX_CAPACITY) are
        /// rejected when this function is instantiated for that `N`. The
        /// check is a post-monomorphization error: `cargo build` always
        /// reports it, while `cargo check` reports it only when the call is
        /// evaluated at compile time (for example in a `static`
        /// initializer). An array too large to exist is rejected by the
        /// compiler before this check runs.
        ///
        /// ```compile_fail,E0080
        /// use spookycircle::StaticStorage;
        ///
        /// // Rejected by `cargo build`, even though it runs at run time.
        /// let empty = StaticStorage::<u32, 0>::new();
        /// ```
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            const {
                assert!(N != 0, "`StaticStorage` capacity `N` must be at least 1");
                assert!(
                    N <= MAX_CAPACITY,
                    "`StaticStorage` capacity `N` exceeds `MAX_CAPACITY`"
                );
            }
            Self {
                control: Control::new_const(),
                slots: new_slots(),
                _owns: PhantomData,
            }
        }
    }

    /// Returns the fixed capacity, `N`.
    #[inline]
    pub fn capacity(&self) -> usize {
        N
    }

    /// Claims this session and returns its unique endpoint pair; see
    /// [`BorrowedStorage::try_split`].
    ///
    /// Takes `&self`, so a `static` can be split once through safe code.
    /// The endpoints are `'static` exactly when the storage borrow is.
    ///
    /// # Errors
    ///
    /// [`SplitError::AlreadySplit`] if a pair has been issued since
    /// construction or the last [`reset`](Self::reset).
    pub fn try_split(
        &self,
    ) -> Result<(BorrowedProducer<'_, T>, BorrowedConsumer<'_, T>), SplitError> {
        let base = NonNull::from(&self.slots).cast::<Slot<T>>();
        // SAFETY: as in `BorrowedStorage::try_split`: `base` is derived from
        // the inline slot array, which cannot move or be reset while `&self`
        // is borrowed by the returned endpoints, and `N` passed the
        // compile-time check in `new`, the only constructor.
        let pair = unsafe { split(&self.control, base, N) };
        pair.map(pair_from_raw).ok_or(SplitError::AlreadySplit)
    }

    /// Ends the previous session and rearms the storage; see
    /// [`BorrowedStorage::reset`].
    pub fn reset(&mut self) {
        self.control = Control::new();
    }
}

/// An array of `N` fresh slots, built in constant evaluation.
#[cfg(not(loom))]
const fn new_slots<T, const N: usize>() -> [Slot<T>; N] {
    [const { Slot::new() }; N]
}

/// An array of `N` fresh slots. Loom's `Slot::new` is not `const`, so the
/// array is built at runtime.
#[cfg(loom)]
fn new_slots<T, const N: usize>() -> [Slot<T>; N] {
    core::array::from_fn(|_| Slot::new())
}

// SAFETY: through `&BorrowedStorage` a thread can only read the capacity or
// call `try_split`, whose compare-exchange admits exactly one winner. The
// owner has no slot access while the storage exists, and `reset` needs
// `&mut`. The winning thread may create and destroy `T`s through the
// endpoints, and endpoints move between threads only when `T: Send`, so
// `T: Send` (not `T: Sync`: no `T` is ever shared between the roles) makes
// sharing the storage sound.
unsafe impl<T: Send> Sync for BorrowedStorage<'_, T> {}

// SAFETY: as for `BorrowedStorage`; the slots are inline instead of borrowed.
unsafe impl<T: Send, const N: usize> Sync for StaticStorage<T, N> {}

/// Wraps a freshly split raw pair; the caller ties `'queue` to the storage
/// borrow that keeps the pair's control block and slots alive.
fn pair_from_raw<'queue, T>(
    (producer, consumer): CallerEndpoints<T, usize>,
) -> (BorrowedProducer<'queue, T>, BorrowedConsumer<'queue, T>) {
    (
        BorrowedProducer {
            raw: producer,
            _storage: PhantomData,
        },
        BorrowedConsumer {
            raw: consumer,
            _storage: PhantomData,
        },
    )
}

/// The unique producer endpoint of borrowed or static storage.
///
/// Behaves exactly like the heap-owned `Producer` (same methods, same
/// guarantees) but borrows its storage for `'queue` and never allocates.
/// Dropping it permanently ends this session's production.
#[must_use = "dropping the producer permanently ends this session's production"]
pub struct BorrowedProducer<'queue, T> {
    raw: RawProducer<T, usize, TypedLife<usize, CallerOwned>>,
    /// Ties the endpoint to the storage borrow. Drop check treats
    /// `PhantomData<&'queue ()>` as owning that reference, so `'queue` must
    /// still be live wherever the endpoint is dropped: the raw endpoint's
    /// drop may run final cleanup through pointers into the storage
    /// (`tests/compile_fail_core/storage_dropped_first.rs`).
    _storage: PhantomData<&'queue ()>,
}

/// The unique consumer endpoint of borrowed or static storage.
///
/// Behaves exactly like the heap-owned `Consumer` but borrows its storage
/// for `'queue` and never allocates. Dropping it permanently ends this
/// session's consumption.
#[must_use = "dropping the consumer permanently ends this session's consumption"]
pub struct BorrowedConsumer<'queue, T> {
    raw: RawConsumer<T, usize, TypedLife<usize, CallerOwned>>,
    /// See [`BorrowedProducer`]'s field of the same name.
    _storage: PhantomData<&'queue ()>,
}

producer_api! {
    impl['queue, T] BorrowedProducer<'queue, T>,
    elem = T,
    copy_where = [T: Copy],
    name = "BorrowedProducer",
}

consumer_api! {
    impl['queue, T] BorrowedConsumer<'queue, T>,
    elem = T,
    copy_where = [T: Copy],
    name = "BorrowedConsumer",
}
