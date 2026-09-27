//! Lifecycle of typed queues: the heap-owned, borrowed, and static modes.
//!
//! All three share one private two-endpoint protocol around a [`Control`]
//! block. The block lives inside the heap allocation for the heap-owned mode
//! and inline in the caller's storage object otherwise; the only difference
//! is what the [`Owner`] does once the final endpoint has dropped the queued
//! values.
//!
//! * A successful construction or split creates exactly two ownership
//!   shares, one per endpoint; endpoints are not cloneable.
//! * The consumer's drop first saves its actual physical head index; each
//!   endpoint then release-stores its liveness flag and releases its share
//!   with an acquire/release `fetch_sub`.
//! * The endpoint whose `fetch_sub` observes the final share has acquired the
//!   other endpoint's completed accesses and drops the occupied range once,
//!   starting at the saved physical index, then lets the [`Owner`] release
//!   the backing memory (only the heap owner frees anything).

use core::{marker::PhantomData, ptr::NonNull};

use super::{CachePadded, Lifecycle, Parts, Slot, endpoints, next_index, slot};
use crate::{
    seq::Sequence,
    sync::{AtomicUsize, Ordering},
};

/// A monotonic endpoint-liveness flag.
///
/// Stored as a pointer-width atomic rather than an `AtomicBool`: the
/// liveness queries are wait-free operations, and the crate's wait-free claim
/// and target certification cover only pointer-width acquire loads and
/// release stores. A byte-width flag would also need
/// `target_has_atomic = "8"`, which the crate's compile-time gate does not
/// check.
pub(crate) struct Liveness(AtomicUsize);

impl Liveness {
    const ALIVE: usize = 1;
    const DEAD: usize = 0;

    const_unless_loom! {
        fn new() -> Self {
            Self(AtomicUsize::new(Self::ALIVE))
        }
    }

    /// Acquire-loads the flag. Once `false`, never `true` again.
    #[inline(always)]
    fn is_alive(&self) -> bool {
        self.0.load(Ordering::Acquire) != Self::DEAD
    }

    /// Release-stores the dead state; called once, from the endpoint's `Drop`.
    #[inline(always)]
    fn mark_dead(&self) {
        self.0.store(Self::DEAD, Ordering::Release);
    }
}

/// State shared by the two endpoints of a typed queue.
///
/// The two position atomics live in their own cache-padded blocks so the
/// producer's `tail` stores never contend with the consumer's `head` stores.
/// Every other field is written only on the split and drop paths. Nothing in
/// here is a pointer, so the block can be `const`-initialized and moved
/// freely while no endpoint borrows it.
pub(crate) struct Control<S: Sequence> {
    /// Next sequence position the producer will publish. Producer-owned.
    tail: CachePadded<S::Atomic>,
    /// Next sequence position the consumer will remove. Consumer-owned.
    head: CachePadded<S::Atomic>,
    /// Cleared with `Release` by the producer's `Drop`.
    producer_alive: Liveness,
    /// Cleared with `Release` by the consumer's `Drop`.
    consumer_alive: Liveness,
    /// Outstanding ownership shares: 2 while a pair exists, 0 afterwards.
    shares: AtomicUsize,
    /// Physical index of `head`, recorded by the consumer's `Drop` so that the
    /// final owner can locate the occupied range after a sequence wrap.
    final_head_index: AtomicUsize,
    /// Borrowed/static session claim: `UNCLAIMED` until one `try_split`
    /// wins, then `CLAIMED` until an exclusive reset. Unused by the
    /// heap-owned mode, whose endpoints exist from construction.
    claim: AtomicUsize,
}

const UNCLAIMED: usize = 0;
const CLAIMED: usize = 1;

impl<S: Sequence> Control<S> {
    /// A fresh, unclaimed block: positions zero, both roles alive, two
    /// shares ready to hand out.
    pub(crate) fn new() -> Self {
        Self::with_positions(S::atomic(S::ZERO), S::atomic(S::ZERO))
    }

    const_unless_loom! {
        /// A fresh, unclaimed block around the given position atomics, which
        /// must both hold `S::ZERO`. The single place that lists every
        /// field's initial value, for [`Control::new`] and
        /// [`Control::new_const`].
        fn with_positions(tail: S::Atomic, head: S::Atomic) -> Self {
            Self {
                tail: CachePadded(tail),
                head: CachePadded(head),
                producer_alive: Liveness::new(),
                consumer_alive: Liveness::new(),
                shares: AtomicUsize::new(2),
                final_head_index: AtomicUsize::new(0),
                claim: AtomicUsize::new(UNCLAIMED),
            }
        }
    }

    /// Describes the queue whose positions live in this block and whose
    /// `capacity` slots start at `slots`.
    pub(crate) fn parts<T>(&self, slots: NonNull<Slot<T>>, capacity: usize) -> Parts<T, S> {
        Parts {
            head: NonNull::from(&self.head.0),
            tail: NonNull::from(&self.tail.0),
            slots,
            capacity,
        }
    }
}

impl Control<usize> {
    const_unless_loom! {
        /// [`Control::new`] for production positions, usable in constant
        /// evaluation except under Loom.
        pub(crate) fn new_const() -> Self {
            Self::with_positions(AtomicUsize::new(0), AtomicUsize::new(0))
        }
    }
}

/// Who owns a typed queue's backing memory once its values are gone.
pub(crate) trait Owner {
    /// Releases the backing memory after final cleanup.
    ///
    /// # Safety
    ///
    /// Called exactly once, by the unique final owner, after every queued
    /// value has been dropped (or its destructor has begun); nothing uses
    /// `control` or `slots` afterwards.
    unsafe fn release<T, S: Sequence>(
        control: NonNull<Control<S>>,
        slots: NonNull<Slot<T>>,
        capacity: usize,
    );
}

/// The borrowed and static modes: the caller owns the control block and the
/// slots, so there is nothing to free. Never frees caller memory.
pub(crate) enum CallerOwned {}

impl Owner for CallerOwned {
    #[inline]
    unsafe fn release<T, S: Sequence>(_: NonNull<Control<S>>, _: NonNull<Slot<T>>, _: usize) {}
}

/// The lifecycle handle each typed endpoint holds: one ownership share of a
/// [`Control`] block.
pub(crate) struct TypedLife<S: Sequence, O: Owner> {
    control: NonNull<Control<S>>,
    _owner: PhantomData<fn() -> O>,
}

impl<S: Sequence, O: Owner> Clone for TypedLife<S, O> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: Sequence, O: Owner> Copy for TypedLife<S, O> {}

impl<S: Sequence, O: Owner> TypedLife<S, O> {
    pub(crate) fn new(control: NonNull<Control<S>>) -> Self {
        Self {
            control,
            _owner: PhantomData,
        }
    }

    /// Returns the control block.
    ///
    /// Only valid while the calling endpoint still holds its share, which is
    /// the case in every liveness query and at the start of `close_*`.
    #[inline(always)]
    fn control(&self) -> &Control<S> {
        // SAFETY: this handle's endpoint holds one of the two ownership
        // shares until its `close_*` releases it, so the block is live.
        unsafe { self.control.as_ref() }
    }
}

// SAFETY: the handle is a pointer to a control block that stays live while
// its share is held and that is only accessed through atomics, so moving it
// to another thread with its endpoint is sound. `close_*` release-stores the
// liveness flag read by the matching acquire query, and the queue storage
// stays valid until the final share is released.
unsafe impl<T, S: Sequence, O: Owner> Lifecycle<T, S> for TypedLife<S, O> {
    #[inline(always)]
    fn producer_alive(&self) -> bool {
        self.control().producer_alive.is_alive()
    }

    #[inline(always)]
    fn consumer_alive(&self) -> bool {
        self.control().consumer_alive.is_alive()
    }

    unsafe fn close_producer(&self, slots: NonNull<Slot<T>>, capacity: usize) {
        // Liveness first (after every preceding tail publication in program
        // order), then the ownership share.
        self.control().producer_alive.mark_dead();
        // SAFETY: this endpoint holds one share and releases it exactly once.
        unsafe { release_share::<T, S, O>(self.control, slots, capacity) }
    }

    unsafe fn close_consumer(&self, slots: NonNull<Slot<T>>, capacity: usize, head_index: usize) {
        let control = self.control();
        // Record where `head` physically lives so that the final owner can
        // walk the occupied range even after a sequence wrap has made
        // `head % capacity` meaningless. Ordered before the share release.
        control
            .final_head_index
            .store(head_index, Ordering::Release);
        control.consumer_alive.mark_dead();
        // SAFETY: this endpoint holds one share and releases it exactly once.
        unsafe { release_share::<T, S, O>(self.control, slots, capacity) }
    }
}

/// Endpoints of a borrowed or static queue.
pub(crate) type CallerEndpoints<T, S> = super::Endpoints<T, S, TypedLife<S, CallerOwned>>;

/// Claims a borrowed/static session and creates its endpoint pair.
/// Returns `None` if the session is already claimed,
/// having changed nothing.
///
/// # Safety
///
/// `slots` must point to `capacity` slots that, like `control`, stay live,
/// unmoved, and otherwise unaccessed for as long as either returned endpoint
/// or a reference derived from one is usable. `control` must be fresh
/// (constructed by [`Control::new`] / [`Control::new_const`] and not
/// modified since) unless it is already claimed.
pub(crate) unsafe fn split<T, S: Sequence>(
    control: &Control<S>,
    slots: NonNull<Slot<T>>,
    capacity: usize,
) -> Option<CallerEndpoints<T, S>> {
    // One strong compare-exchange: concurrent callers have exactly one
    // winner, and a loser observes the claim without modifying anything.
    if control
        .claim
        .compare_exchange(UNCLAIMED, CLAIMED, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return None;
    }
    // Nothing fallible or panicking follows the claim.
    let parts = control.parts(slots, capacity);
    // SAFETY: the claim makes this the only pair for the session; a fresh
    // block has zero positions, both roles alive, and two shares, and no
    // slot is initialized; storage validity is the caller's contract.
    Some(unsafe { endpoints(parts, TypedLife::new(NonNull::from(control))) })
}

/// Releases one ownership share; the releaser of the final share drops the
/// remaining values and hands the memory back to its owner.
///
/// # Safety
///
/// The caller must own one outstanding share of `control` and must not use
/// `control` afterwards; `slots` and `capacity` describe its queue.
unsafe fn release_share<T, S: Sequence, O: Owner>(
    control: NonNull<Control<S>>,
    slots: NonNull<Slot<T>>,
    capacity: usize,
) {
    // SAFETY: the caller's share keeps the block live for this access.
    let previous = unsafe { control.as_ref() }
        .shares
        .fetch_sub(1, Ordering::AcqRel);
    if previous == 1 {
        // We observed the other endpoint's release (acquire side of the RMW),
        // so all of its slot accesses, position stores, and its
        // `final_head_index` store happen-before this point, and no endpoint
        // can reach the block any more.
        // SAFETY: exclusive ownership established above.
        unsafe { destroy::<T, S, O>(control, slots, capacity) }
    }
}

/// Drops every value in the occupied range in FIFO order, then lets the
/// owner release the storage.
///
/// # Safety
///
/// `control` must be exclusively owned by the caller: both shares released,
/// no endpoint and no reference derived from one alive.
unsafe fn destroy<T, S: Sequence, O: Owner>(
    control: NonNull<Control<S>>,
    slots: NonNull<Slot<T>>,
    capacity: usize,
) {
    // SAFETY: exclusive ownership (caller contract) keeps the block live.
    let c = unsafe { control.as_ref() };
    let head = S::load(&c.head.0, Ordering::Acquire);
    let tail = S::load(&c.tail.0, Ordering::Acquire);
    let mut guard = CleanupGuard::<T, S, O> {
        control,
        slots,
        capacity,
        index: c.final_head_index.load(Ordering::Acquire),
        remaining: tail.distance(head),
        _owner: PhantomData,
    };
    debug_assert!(guard.index < capacity);
    debug_assert!(guard.remaining <= capacity);
    while guard.remaining != 0 {
        // SAFETY: `remaining != 0`, and the guard holds exclusive ownership.
        unsafe { guard.drop_next() }
    }
    // `guard` drops here and releases the storage. If a `T::drop` above
    // unwinds, the guard's `Drop` still runs during unwinding, drops the
    // elements that have not been visited yet, and then releases.
}

/// Tracks the not-yet-dropped tail of the occupied range so that a panicking
/// destructor cannot cause a double drop, or a leak of heap storage.
struct CleanupGuard<T, S: Sequence, O: Owner> {
    control: NonNull<Control<S>>,
    slots: NonNull<Slot<T>>,
    capacity: usize,
    /// Physical slot of the next value to drop.
    index: usize,
    /// Number of occupied slots not yet dropped.
    remaining: usize,
    _owner: PhantomData<fn() -> O>,
}

impl<T, S: Sequence, O: Owner> CleanupGuard<T, S, O> {
    /// Drops the value at `index`, advancing the bookkeeping *before* the
    /// destructor runs so that an unwinding destructor is never revisited.
    ///
    /// # Safety
    ///
    /// `self.remaining != 0`, and `self` holds exclusive ownership of the
    /// queue.
    unsafe fn drop_next(&mut self) {
        let index = self.index;
        debug_assert!(index < self.capacity);
        self.index = next_index(index, self.capacity);
        debug_assert!(self.remaining != 0);
        // Wrapping, like the rest of the cleanup path: `remaining != 0` is
        // this function's precondition.
        self.remaining = self.remaining.wrapping_sub(1);
        // SAFETY: `index` lies in the occupied range `[head, tail)` fixed by
        // `destroy` after exclusive ownership was established, and every
        // such slot holds an initialized `T` published by the producer and
        // not yet moved out by the consumer. The
        // consumer's release of `head` and the producer's release of `tail`
        // both happen-before the final share release we observed. Each slot
        // is visited exactly once because `index`/`remaining` advance before
        // the drop, and nothing reads the slot afterwards.
        let cell = unsafe { slot(self.slots, index) };
        // SAFETY: as above, `cell` holds an initialized `T` that this guard
        // exclusively owns and never reads again.
        unsafe { cell.drop_value() }
    }
}

impl<T, S: Sequence, O: Owner> Drop for CleanupGuard<T, S, O> {
    fn drop(&mut self) {
        while self.remaining != 0 {
            // SAFETY: `remaining != 0` and exclusive ownership; only reached
            // during unwinding from a panicking `T::drop`.
            unsafe { self.drop_next() }
        }
        // SAFETY: exclusive ownership; every queued value has been dropped
        // (or its destructor has begun and will not be revisited); nothing
        // accesses the queue after this point.
        unsafe { O::release::<T, S>(self.control, self.slots, self.capacity) }
    }
}

/// Narrow-counter wraparound model: the same state
/// machine with `u8` sequence numbers, forced through many complete wraps,
/// in the heap-owned and caller-owned modes.
#[cfg(all(test, not(loom)))]
mod narrow_tests {
    extern crate std;

    use std::{cell::RefCell, collections::VecDeque, vec::Vec};

    use super::*;
    use crate::{
        error::CreateError,
        raw::{RawConsumer, RawProducer, validate_capacity},
    };

    const WRAPS: usize = 6;

    /// Runs `$body(producer, consumer, $args...)` once per typed mode with
    /// `u8` positions: heap-owned (when `alloc` is enabled) and caller-owned
    /// storage whose control block and slots live in this stack frame.
    macro_rules! in_each_mode {
        ($T:ty, $capacity:expr, $body:ident $(, $arg:expr)*) => {{
            let capacity: usize = $capacity;
            #[cfg(feature = "alloc")]
            {
                let (p, c) = crate::raw::create::<$T, u8>(capacity).unwrap();
                $body(p, c $(, $arg)*);
            }
            {
                let control = Control::<u8>::new();
                let slots: Vec<Slot<$T>> = (0..capacity).map(|_| Slot::new()).collect();
                let base = NonNull::new(slots.as_ptr().cast_mut()).unwrap();
                // SAFETY: `control` is fresh and both it and `slots` outlive
                // the endpoints, which `$body` consumes.
                let (p, c) = unsafe { split::<$T, u8>(&control, base, capacity) }.unwrap();
                $body(p, c $(, $arg)*);
                // SAFETY: as above; the session is claimed, so this fails.
                assert!(unsafe { split::<$T, u8>(&control, base, capacity) }.is_none());
            }
        }};
    }

    /// Deterministic xorshift so the tests need no external crate.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    #[test]
    fn rejects_capacity_above_half_range() {
        assert!(validate_capacity(127, u8::MAX_CAPACITY).is_ok());
        assert_eq!(
            validate_capacity(128, u8::MAX_CAPACITY),
            Err(CreateError::CapacityTooLarge { requested: 128 })
        );
        #[cfg(feature = "alloc")]
        {
            assert!(crate::raw::create::<u32, u8>(127).is_ok());
            assert!(matches!(
                crate::raw::create::<u32, u8>(128),
                Err(CreateError::CapacityTooLarge { requested: 128 })
            ));
        }
    }

    fn scalar_fifo<L: Lifecycle<u64, u8>>(
        mut p: RawProducer<u64, u8, L>,
        mut c: RawConsumer<u64, u8, L>,
        capacity: usize,
    ) {
        let mut model = VecDeque::new();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ capacity as u64);
        let mut next = 0u64;
        let mut pushed = 0usize;
        while pushed < 256 * WRAPS {
            if rng.below(2) == 0 {
                match p.try_push(next) {
                    Ok(()) => {
                        model.push_back(next);
                        next += 1;
                        pushed += 1;
                    }
                    Err(full) => {
                        assert_eq!(full.into_inner(), next);
                        assert_eq!(model.len(), capacity);
                    }
                }
            } else {
                let got = c.try_pop();
                assert_eq!(got, model.pop_front());
            }
            assert_eq!(p.len(), model.len());
            assert_eq!(c.len(), model.len());
            assert_eq!(c.peek().copied(), model.front().copied());
        }
        while let Some(expected) = model.pop_front() {
            assert_eq!(c.try_pop(), Some(expected));
        }
        assert_eq!(c.try_pop(), None);
    }

    /// Interleaved scalar pushes and pops through many `u8` wraps, checked
    /// against a `VecDeque` model, for capacities that divide 256 and
    /// capacities that do not (where physical index and sequence number
    /// drift apart).
    #[test]
    fn scalar_fifo_survives_many_wraps() {
        for capacity in [1usize, 2, 3, 5, 7, 16, 100, 127] {
            in_each_mode!(u64, capacity, scalar_fifo, capacity);
        }
    }

    fn bulk_fifo<L: Lifecycle<u32, u8>>(
        mut p: RawProducer<u32, u8, L>,
        mut c: RawConsumer<u32, u8, L>,
        capacity: usize,
    ) {
        let mut model = VecDeque::new();
        let mut rng = Rng(0xD1B5_4A32_D192_ED03 ^ capacity as u64);
        let mut next = 0u32;
        let mut moved = 0usize;
        while moved < 256 * WRAPS {
            let n = rng.below(capacity + 2);
            if rng.below(2) == 0 {
                let source: Vec<u32> = (next..next + n as u32).collect();
                let done = p.push_slice(&source);
                assert_eq!(done, n.min(capacity - model.len()));
                model.extend(&source[..done]);
                next += done as u32;
                moved += done;
            } else {
                let mut dest = std::vec![u32::MAX; n];
                let done = c.pop_slice(&mut dest);
                assert_eq!(done, n.min(model.len()));
                for got in &dest[..done] {
                    assert_eq!(Some(*got), model.pop_front());
                }
                assert!(dest[done..].iter().all(|&x| x == u32::MAX));
            }
            assert_eq!(p.len(), model.len());
            assert_eq!(c.len(), model.len());
        }
    }

    /// Bulk operations with wrap-crossing batches through many `u8` wraps.
    #[test]
    fn bulk_fifo_survives_many_wraps() {
        for capacity in [1usize, 3, 7, 64, 100] {
            in_each_mode!(u32, capacity, bulk_fifo, capacity);
        }
    }

    fn drained<L: Lifecycle<u8, u8>>(mut p: RawProducer<u8, u8, L>, mut c: RawConsumer<u8, u8, L>) {
        for i in 0..(256 * WRAPS) as u32 {
            p.try_push(i as u8).unwrap();
            assert_eq!(c.try_pop(), Some(i as u8));
        }
        p.try_push(1).unwrap();
        p.try_push(2).unwrap();
        drop(p);
        assert!(!c.is_producer_alive());
        assert!(!c.is_drained());
        assert_eq!(c.try_pop(), Some(1));
        assert!(!c.is_drained());
        assert_eq!(c.try_pop(), Some(2));
        assert!(c.is_drained());
        assert!(c.is_drained());
    }

    /// The `is_drained` protocol keeps working after the counter wraps.
    #[test]
    fn drained_after_wrap() {
        in_each_mode!(u8, 3, drained);
    }

    std::thread_local! {
        /// Ids of dropped [`Logged`] values, in drop order.
        static DROP_LOG: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    }

    fn take_log() -> Vec<u64> {
        DROP_LOG.with(|log| core::mem::take(&mut *log.borrow_mut()))
    }

    fn log_len() -> usize {
        DROP_LOG.with(|log| log.borrow().len())
    }

    /// A value with a unique identity whose drop is logged, so that a drop
    /// of the wrong slot (stale bits from an already-consumed value) or a
    /// double drop shows up as a wrong or duplicated id. It owns no
    /// resources, so even a wrong-slot drop in a deliberately broken cleanup
    /// (the narrow-counter negative control run by `cargo xtask loom-mutants`) only
    /// logs a bad id instead of corrupting memory.
    #[derive(Debug)]
    struct Logged(u64);

    impl Drop for Logged {
        fn drop(&mut self) {
            DROP_LOG.with(|log| log.borrow_mut().push(self.0));
        }
    }

    fn cleanup_after_wrap<L: Lifecycle<Logged, u8>>(
        mut p: RawProducer<Logged, u8, L>,
        mut c: RawConsumer<Logged, u8, L>,
        capacity: usize,
        occupied: usize,
        consumer_first: bool,
    ) {
        take_log();
        let mut next_id = 0u64;
        let mut make = || {
            next_id += 1;
            Logged(next_id)
        };
        // Cycle enough elements to wrap the u8 counter a few times, leaving
        // physical index != head % capacity.
        let cycled = 256 * WRAPS + 1;
        for _ in 0..cycled {
            p.try_push(make()).unwrap();
            drop(c.try_pop().unwrap());
        }
        assert_eq!(log_len(), cycled);
        // Leave `occupied` values queued; exactly those must be dropped, once
        // each, in FIFO order, at destruction.
        let queued = occupied.min(capacity);
        let expected: Vec<u64> = (0..queued)
            .map(|_| {
                let value = make();
                let id = value.0;
                p.try_push(value).unwrap();
                id
            })
            .collect();
        if consumer_first {
            drop(c);
            assert_eq!(log_len(), cycled);
            drop(p);
        } else {
            drop(p);
            assert_eq!(log_len(), cycled);
            drop(c);
        }
        let log = take_log();
        assert_eq!(log.len(), cycled + queued);
        assert_eq!(&log[cycled..], &expected[..]);
        // Every id was dropped exactly once overall.
        let mut all = log.clone();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), cycled + queued);
    }

    /// Final cleanup after several wraps with a capacity that does not divide
    /// the counter range: the occupied range must be located by the recorded
    /// physical index, not by `head % capacity`. Capacity 3 crosses complete
    /// sequence wraps and ends nonempty.
    #[test]
    fn cleanup_after_wrap_drops_exactly_the_occupied_range() {
        for capacity in [3usize, 5, 7, 100] {
            for occupied in [1usize, 2, 3] {
                for consumer_first in [false, true] {
                    in_each_mode!(
                        Logged,
                        capacity,
                        cleanup_after_wrap,
                        capacity,
                        occupied,
                        consumer_first
                    );
                }
            }
        }
    }
}

/// Kani proof that final cleanup drops exactly the occupied range, from any
/// final positions (including across the `usize` wrap) and any physical head
/// index, and then releases the storage once. Kani aborts on panic, so the
/// unwinding path through `CleanupGuard::drop` is left to the `narrow_tests`
/// and the allocator tests.
#[cfg(kani)]
// Proof code, like `narrow_tests`: Kani itself fails on any overflow or
// out-of-bounds index.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod proofs {
    use core::{mem::MaybeUninit, ptr::NonNull};

    use super::{Control, Owner, destroy};
    use crate::{
        raw::{Slot, next_index, slot},
        seq::Sequence,
        sync::{AtomicUsize, Ordering},
    };

    const MAX: usize = 4;

    /// Drops of the value created for each physical slot.
    static DROPS: [AtomicUsize; MAX] = [const { AtomicUsize::new(0) }; MAX];
    /// Calls to `Owner::release`, and the total drop count at the last one.
    static RELEASES: AtomicUsize = AtomicUsize::new(0);
    static DROPS_AT_RELEASE: AtomicUsize = AtomicUsize::new(0);

    fn total_drops() -> usize {
        DROPS.iter().map(|d| d.load(Ordering::Relaxed)).sum()
    }

    /// A value that records its drop against the slot it was created for.
    struct Tracked(usize);

    impl Drop for Tracked {
        fn drop(&mut self) {
            DROPS[self.0].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// An owner that records when the storage is released.
    enum Counted {}

    impl Owner for Counted {
        unsafe fn release<T, S: Sequence>(_: NonNull<Control<S>>, _: NonNull<Slot<T>>, _: usize) {
            RELEASES.fetch_add(1, Ordering::Relaxed);
            DROPS_AT_RELEASE.store(total_drops(), Ordering::Relaxed);
        }
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn destroy_drops_exactly_the_occupied_range() {
        let capacity: usize = kani::any();
        kani::assume((1..=MAX).contains(&capacity));
        let head: usize = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= capacity);
        let head_index: usize = kani::any();
        kani::assume(head_index < capacity);

        let slots = [const { Slot::<Tracked>::new() }; MAX];
        let base = NonNull::from(&slots).cast::<Slot<Tracked>>();
        let mut index = head_index;
        for _ in 0..len {
            // SAFETY: `index < capacity <= MAX`, and the slot is free.
            let cell = unsafe { slot(base, index) };
            // SAFETY: as above.
            cell.value
                .with_mut(|p| unsafe { p.write(MaybeUninit::new(Tracked(index))) });
            index = next_index(index, capacity);
        }

        // The state both endpoints leave behind: final positions, and the
        // consumer's physical head index.
        let control = Control::<usize>::new();
        control.head.0.store(head, Ordering::Relaxed);
        control.tail.0.store(head.advance(len), Ordering::Relaxed);
        control
            .final_head_index
            .store(head_index, Ordering::Relaxed);
        // SAFETY: no endpoint exists, so the harness owns the block, and the
        // occupied slots hold initialized values.
        unsafe { destroy::<Tracked, usize, Counted>(NonNull::from(&control), base, capacity) };

        for (i, drops) in DROPS.iter().enumerate() {
            let occupied = i < capacity && (i + capacity - head_index) % capacity < len;
            assert_eq!(drops.load(Ordering::Relaxed), usize::from(occupied));
        }
        assert_eq!(RELEASES.load(Ordering::Relaxed), 1);
        assert_eq!(DROPS_AT_RELEASE.load(Ordering::Relaxed), len);
    }
}
