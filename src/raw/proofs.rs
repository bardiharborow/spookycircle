//! Kani proofs for the queue core.
//!
//! * The physical-index and sequence arithmetic over the full production
//!   `usize` range, which neither the `u8` wraparound model (capacity <= 127)
//!   nor the runtime tests can reach.
//! * The data path itself, run by one thread from any reachable endpoint
//!   state: every operation agrees with a counter model of the queue, keeps
//!   both physical indices in bounds and in step, and keeps both caches
//!   conservative. Kani runs atomics sequentially, so this says nothing about
//!   memory ordering, which the Loom suites cover.

use core::{mem::MaybeUninit, ptr::NonNull};

use super::{Lifecycle, Parts, RawConsumer, RawProducer, Slot, index_after, next_index, slot};
use crate::{
    seq::Sequence,
    sync::{AtomicUsize, Ordering},
};

/// Any capacity the crate accepts, and a physical index within it.
fn any_index() -> (usize, usize) {
    let capacity: usize = kani::any();
    let index: usize = kani::any();
    kani::assume((1..=<usize as Sequence>::MAX_CAPACITY).contains(&capacity));
    kani::assume(index < capacity);
    (capacity, index)
}

#[kani::proof]
fn next_index_stays_in_bounds() {
    let (capacity, index) = any_index();
    let next = next_index(index, capacity);
    assert!(next < capacity);
    assert_eq!(next, if index + 1 == capacity { 0 } else { index + 1 });
}

#[kani::proof]
fn index_after_stays_in_bounds() {
    let (capacity, index) = any_index();
    let count: usize = kani::any();
    kani::assume(count <= capacity);
    let next = index_after(index, count, capacity);
    assert!(next < capacity);
    // `index + count` cannot overflow: both are at most `MAX_CAPACITY`.
    assert_eq!(next, (index + count) % capacity);
}

/// The two-segment split in `push_slice` and `pop_slice`: both runs lie
/// inside the slot array, they never overlap, and the index update
/// lands just past the last slot touched.
#[kani::proof]
fn wrap_split_is_in_bounds_and_disjoint() {
    let (capacity, index) = any_index();
    let count: usize = kani::any();
    kani::assume(count >= 1 && count <= capacity);
    let first = core::cmp::min(count, capacity.wrapping_sub(index));
    let after = count - first;
    assert!(first >= 1);
    assert!(index + first <= capacity);
    // The after-wrap run `0..after` ends at or before the first run's
    // start, so the two runs are disjoint.
    assert!(after <= index);
    let next = index_after(index, count, capacity);
    if after == 0 {
        assert_eq!(next, (index + first) % capacity);
    } else {
        assert_eq!(index + first, capacity);
        assert_eq!(next, after);
    }
}

/// The producer's `full_at` test: `tail == head + capacity` in wrapping
/// arithmetic holds exactly when the queue is full, and `full_at - tail`
/// is the free space, for any positions, including across the `usize`
/// wrap.
#[kani::proof]
fn full_at_matches_occupancy() {
    let capacity: usize = kani::any();
    kani::assume((1..=<usize as Sequence>::MAX_CAPACITY).contains(&capacity));
    let head: usize = kani::any();
    let tail: usize = kani::any();
    let occupancy = tail.distance(head);
    kani::assume(occupancy <= capacity);
    let full_at = head.advance(capacity);
    assert_eq!(tail == full_at, occupancy == capacity);
    assert_eq!(full_at.distance(tail), capacity - occupancy);
    if occupancy < capacity {
        assert_eq!(tail.advance(1).distance(head), occupancy + 1);
    }
}

/// A stale cache is conservative: a producer whose cached head lags the
/// real one by any amount the protocol allows never sees more free space
/// than there is, and a consumer whose cached tail lags never sees more
/// available data than there is.
#[kani::proof]
fn stale_caches_are_conservative() {
    let capacity: usize = kani::any();
    kani::assume((1..=<usize as Sequence>::MAX_CAPACITY).contains(&capacity));
    let cached_head: usize = kani::any();
    let head: usize = kani::any();
    let cached_tail: usize = kani::any();
    let tail: usize = kani::any();
    // Program order on each side: `cached_head <= head <= tail` and
    // `head <= cached_tail <= tail`, as distances within one window of
    // at most `capacity` positions measured from `cached_head`.
    kani::assume(tail.distance(cached_head) <= capacity);
    kani::assume(head.distance(cached_head) <= tail.distance(cached_head));
    kani::assume(cached_tail.distance(head) <= tail.distance(head));

    let free_cached = cached_head.advance(capacity).distance(tail);
    let free_real = head.advance(capacity).distance(tail);
    assert!(free_cached <= free_real);
    assert!(free_real <= capacity);

    let available_cached = cached_tail.distance(head);
    let available_real = tail.distance(head);
    assert!(available_cached <= available_real);
    assert!(available_real <= capacity);
}

/// A lifecycle for single-threaded proofs: both roles stay alive, and
/// closing a role does nothing (the storage is the harness's own locals).
#[derive(Clone, Copy)]
struct Detached;

// SAFETY: a unit handle; the harness keeps the storage alive past both
// endpoints, and the liveness flags never change.
unsafe impl<T, S: Sequence> Lifecycle<T, S> for Detached {
    fn producer_alive(&self) -> bool {
        true
    }

    fn consumer_alive(&self) -> bool {
        true
    }

    unsafe fn close_producer(&self, _: NonNull<Slot<T>>, _: usize) {}

    unsafe fn close_consumer(&self, _: NonNull<Slot<T>>, _: usize, _: usize) {}
}

/// An element type for the data-path proofs: the value stored for the
/// `n`th position pushed, counted from the oldest element initially queued,
/// and the mark `peek_mut` applies to it.
trait Label: Copy + Eq + core::fmt::Debug {
    fn label(n: usize) -> Self;
    fn mark(self) -> Self;
}

/// Single-byte values: Kani 0.68.0 (CBMC 6.11.0) never writes the last
/// element of a `ptr::copy`/`copy_nonoverlapping` of multi-byte elements
/// whose count is symbolic, as in `push_slice` and `pop_slice`, so value
/// checks after one are unsound (bounds are still checked). The data path is
/// generic in `T` and counts in elements, so bytes lose nothing.
impl Label for u8 {
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        reason = "the data proofs queue at most a handful of labels"
    )]
    fn label(n: usize) -> Self {
        // Offset, so that no label is the zero byte of an unwritten slot.
        (n as u8).wrapping_add(0x10)
    }

    fn mark(self) -> Self {
        self ^ 0x80
    }
}

impl Label for () {
    fn label(_: usize) -> Self {}

    fn mark(self) -> Self {}
}

/// The operations of the data path.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    TryPush,
    PushSlice,
    TryPop,
    TryPopInto,
    Peek,
    PeekMut,
    PopSlice,
}

/// Largest slice passed to `push_slice` and `pop_slice`.
const MAX_SLICE: usize = 3;

type Producer<T> = RawProducer<T, usize, Detached>;
type Consumer<T> = RawConsumer<T, usize, Detached>;

/// The queue invariant, in terms of the endpoints' fields and the shared
/// positions, for a queue holding `len` elements:
///
/// * each endpoint's position is the published one;
/// * occupancy `tail - head` is `len`, at most `capacity`;
/// * the physical indices are in bounds and `len` slots apart;
/// * the producer's cached head lags `head` by at most the free space, and
///   the consumer's cached tail lags `tail` by at most `len`, so neither
///   cache can overstate what it proves.
fn assert_invariant<T>(p: &Producer<T>, c: &Consumer<T>, len: usize) {
    let capacity = p.queue.capacity;
    assert_eq!(c.queue.capacity, capacity);
    let (head, tail) = (c.head, p.tail);
    assert_eq!(head, p.queue.shared_head().load(Ordering::Relaxed));
    assert_eq!(tail, c.queue.shared_tail().load(Ordering::Relaxed));
    assert_eq!(tail.distance(head), len);
    assert!(len <= capacity);
    assert!(c.index < capacity);
    assert_eq!(p.index, index_after(c.index, len, capacity));
    let cached_head = p.full_at.wrapping_sub(capacity);
    assert!(head.distance(cached_head) <= capacity - len);
    assert!(tail.distance(c.cached_tail) <= len);
}

/// Proves that `op` agrees with the model and preserves the invariant.
///
/// Builds endpoints in an arbitrary state that satisfies
/// [`assert_invariant`]: any `head` position (so an operation may cross the
/// `usize` wrap), any occupancy up to capacity, any physical head index,
/// caches lagging by any amount the invariant allows, and the queued slots
/// holding labels `0, 1, ...` from the oldest. The operation's result is
/// checked against the model and the invariant re-checked, so by induction
/// every sequence of operations from a fresh queue (which satisfies the
/// invariant) behaves like the model. The data path never inspects the
/// values it moves, so relabelling the queued values after each step
/// loses nothing.
///
/// # Safety
///
/// `slots` must be valid for `max_capacity` slots of `T`, or dangling if `T`
/// is zero-sized.
unsafe fn check_op<T: Label>(op: Op, slots: NonNull<Slot<T>>, max_capacity: usize) {
    let capacity: usize = kani::any();
    kani::assume((1..=max_capacity).contains(&capacity));
    let head: usize = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= capacity);
    let tail = head.advance(len);
    let head_index: usize = kani::any();
    kani::assume(head_index < capacity);
    let head_lag: usize = kani::any();
    kani::assume(head_lag <= capacity - len);
    let tail_lag: usize = kani::any();
    kani::assume(tail_lag <= len);

    if size_of::<T>() != 0 {
        let mut index = head_index;
        for n in 0..len {
            // SAFETY: `index < capacity <= max_capacity` slots, none of them
            // borrowed yet.
            let cell = unsafe { slot(slots, index) };
            // SAFETY: as above; the slot is free for this write.
            cell.value
                .with_mut(|p| unsafe { p.write(MaybeUninit::new(T::label(n))) });
            index = next_index(index, capacity);
        }
    }

    let shared_head = AtomicUsize::new(head);
    let shared_tail = AtomicUsize::new(tail);
    let parts = Parts {
        head: NonNull::from(&shared_head),
        tail: NonNull::from(&shared_tail),
        slots,
        capacity,
    };
    // SAFETY: the storage outlives both endpoints, which are its only ones.
    // Their fields are then moved to the chosen state, which satisfies the
    // invariant, as a fresh queue's does.
    let mut p: Producer<T> = unsafe { RawProducer::new(parts, Detached) };
    // SAFETY: as above.
    let mut c: Consumer<T> = unsafe { RawConsumer::new(parts, Detached) };
    p.tail = tail;
    p.full_at = head.wrapping_sub(head_lag).advance(capacity);
    p.index = index_after(head_index, len, capacity);
    c.head = head;
    c.cached_tail = tail.wrapping_sub(tail_lag);
    c.index = head_index;
    assert_invariant(&p, &c, len);

    let new_len = apply(op, &mut p, &mut c, len);
    assert_invariant(&p, &c, new_len);
    assert!(!c.is_drained());
}

/// Asserts that the `count` positions after the first `len` queued ones,
/// just written by a push, hold labels `len .. len + count`, read straight
/// from the slots the consumer will take them from.
fn assert_queued<T: Label>(c: &Consumer<T>, len: usize, count: usize) {
    for k in len..len + count {
        let index = index_after(c.index, k, c.queue.capacity);
        // SAFETY: `index < capacity`, and position `k` was just published,
        // so the slot holds an initialized `T` that nothing else borrows.
        let cell = unsafe { slot(c.queue.slots, index) };
        // SAFETY: as above.
        let value = cell.value.with(|p| unsafe { p.cast::<T>().read() });
        assert_eq!(value, T::label(k));
    }
}

/// Performs `op` on a queue holding labels `0 .. len`, checks its result
/// against the model, and returns the new length.
fn apply<T: Label>(op: Op, p: &mut Producer<T>, c: &mut Consumer<T>, len: usize) -> usize {
    let capacity = p.queue.capacity;
    match op {
        Op::TryPush => {
            let result = p.try_push(T::label(len));
            assert_eq!(result.is_ok(), len < capacity);
            let pushed = usize::from(result.is_ok());
            assert_queued(c, len, pushed);
            len + pushed
        }
        Op::PushSlice => {
            let n: usize = kani::any();
            kani::assume(n <= MAX_SLICE);
            let source: [T; MAX_SLICE] = core::array::from_fn(|k| T::label(len + k));
            let count = p.push_slice(&source[..n]);
            assert_eq!(count, n.min(capacity - len));
            assert_queued(c, len, count);
            len + count
        }
        Op::TryPop => {
            if let Some(v) = c.try_pop() {
                assert!(len > 0);
                assert_eq!(v, T::label(0));
                len - 1
            } else {
                assert_eq!(len, 0);
                0
            }
        }
        Op::TryPopInto => {
            let mut destination = MaybeUninit::uninit();
            if let Some(v) = c.try_pop_into(&mut destination) {
                assert!(len > 0);
                assert_eq!(*v, T::label(0));
                len - 1
            } else {
                assert_eq!(len, 0);
                0
            }
        }
        Op::Peek => {
            match c.peek() {
                Some(v) => assert!(len > 0 && *v == T::label(0)),
                None => assert_eq!(len, 0),
            }
            len
        }
        Op::PeekMut => {
            match c.peek_mut() {
                Some(v) => {
                    assert!(len > 0 && *v == T::label(0));
                    *v = T::label(0).mark();
                    // The change is to the queued value itself.
                    assert_eq!(c.peek().copied(), Some(T::label(0).mark()));
                }
                None => assert_eq!(len, 0),
            }
            len
        }
        Op::PopSlice => {
            let n: usize = kani::any();
            kani::assume(n <= MAX_SLICE);
            let mut destination = [T::label(usize::MAX); MAX_SLICE];
            let count = c.pop_slice(&mut destination[..n]);
            assert_eq!(count, n.min(len));
            for (k, v) in destination[..count].iter().enumerate() {
                assert_eq!(*v, T::label(k));
            }
            len - count
        }
    }
}

/// For each operation, one proof with real values in a queue of up to four
/// slots, and one with zero-sized values in a queue of any capacity (the
/// index and position arithmetic of the real code at full width).
macro_rules! op_proofs {
    ($($op:ident: $data:ident, $wide:ident;)*) => {$(
        #[kani::proof]
        #[kani::unwind(5)]
        fn $data() {
            let slots = [const { Slot::<u8>::new() }; 4];
            // SAFETY: four slots, alive for the whole proof.
            unsafe { check_op::<u8>(Op::$op, NonNull::from(&slots).cast(), 4) }
        }

        #[kani::proof]
        #[kani::unwind(5)]
        fn $wide() {
            // SAFETY: zero-sized slots need only a dangling, aligned pointer.
            unsafe {
                check_op::<()>(Op::$op, NonNull::dangling(), <usize as Sequence>::MAX_CAPACITY);
            }
        }
    )*};
}

op_proofs! {
    TryPush: try_push_data, try_push_wide;
    PushSlice: push_slice_data, push_slice_wide;
    TryPop: try_pop_data, try_pop_wide;
    TryPopInto: try_pop_into_data, try_pop_into_wide;
    Peek: peek_data, peek_wide;
    PeekMut: peek_mut_data, peek_mut_wide;
    PopSlice: pop_slice_data, pop_slice_wide;
}
