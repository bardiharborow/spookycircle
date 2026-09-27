//! Deterministic unit tests, driven through the public
//! API only. Every applicable body runs against each endpoint family: the
//! heap-owned queue (with `alloc`), borrowed slots, static storage, and
//! (for byte records, with `shared-memory`) a shared region.

// Loom-instrumented atomics only work inside a Loom model; see tests/loom.rs.
#![cfg(not(loom))]

#[macro_use]
mod common;

use std::{
    cell::Cell,
    collections::VecDeque,
    error::Error,
    fmt::Write as _,
    mem::MaybeUninit,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use common::{ConsumerOps, ProducerOps, Rec, rec, unrec};
use spookycircle::{CreateError, Full};

/// Capacities exercised by every parametrised test: 1, 2, a non-power-of-two,
/// and a power-of-two.
const CAPACITIES: [usize; 4] = [1, 2, 3, 8];

/// A non-`Copy`, non-`Clone`, `Send` but `!Sync` element whose drop is logged
/// under a unique id.
struct Tracked {
    id: u64,
    _not_sync: Cell<u8>,
    log: Arc<Mutex<Vec<u64>>>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.log.lock().unwrap().push(self.id);
    }
}

impl std::fmt::Debug for Tracked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tracked({})", self.id)
    }
}

/// Produces `Tracked` values with unique ids.
struct Factory {
    next: u64,
    log: Arc<Mutex<Vec<u64>>>,
}

impl Factory {
    fn new() -> Self {
        Self {
            next: 0,
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn make(&mut self) -> Tracked {
        self.next += 1;
        Tracked {
            id: self.next,
            _not_sync: Cell::new(0),
            log: self.log.clone(),
        }
    }

    fn dropped(&self) -> Vec<u64> {
        self.log.lock().unwrap().clone()
    }

    fn assert_dropped_exactly_once_each(&self, expected: &[u64]) {
        let mut dropped = self.dropped();
        dropped.sort_unstable();
        let mut expected = expected.to_vec();
        expected.sort_unstable();
        assert_eq!(dropped, expected);
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "family runners hand bodies owned endpoints"
)]
fn initial_state<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(p: P, c: C, capacity: usize) {
    assert_eq!(p.capacity(), capacity);
    assert_eq!(c.capacity(), capacity);
    assert_eq!(p.len(), 0);
    assert_eq!(c.len(), 0);
    assert!(p.is_empty());
    assert!(c.is_empty());
    assert!(!p.is_full());
    assert!(!c.is_full());
    assert_eq!(p.remaining_capacity(), capacity);
    assert_eq!(c.remaining_capacity(), capacity);
    assert!(p.is_consumer_alive());
    assert!(c.is_producer_alive());
    assert!(!c.is_drained());
}

#[test]
fn initial_state_and_exact_capacity() {
    for capacity in CAPACITIES {
        each_family!(8, capacity, initial_state, capacity);
    }
}

fn no_sentinel<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C, capacity: usize) {
    for i in 0..capacity {
        p.try_push(rec(i))
            .unwrap_or_else(|_| panic!("slot {i} of {capacity} rejected"));
    }
    assert!(p.is_full());
    assert!(c.is_full());
    assert_eq!(
        p.try_push(rec(capacity)).unwrap_err().into_inner(),
        rec(capacity)
    );
    for i in 0..capacity {
        assert_eq!(c.try_pop(), Some(rec(i)));
    }
    assert_eq!(c.try_pop(), None);
}

#[test]
fn no_hidden_sentinel_slot() {
    for capacity in CAPACITIES.into_iter().chain([5, 17, 1000]) {
        each_family!(8, capacity, no_sentinel, capacity);
    }
}

fn check_snapshots<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(
    p: &P,
    c: &C,
    model: &VecDeque<Rec>,
    capacity: usize,
) {
    assert_eq!(p.len(), model.len());
    assert_eq!(c.len(), model.len());
    assert_eq!(p.is_empty(), model.is_empty());
    assert_eq!(c.is_empty(), model.is_empty());
    assert_eq!(p.is_full(), model.len() == capacity);
    assert_eq!(c.is_full(), model.len() == capacity);
    assert_eq!(p.remaining_capacity(), capacity - model.len());
    assert_eq!(c.remaining_capacity(), capacity - model.len());
}

fn transitions<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C, capacity: usize) {
    let mut model = VecDeque::new();
    let mut next = 0usize;
    // empty -> partial -> full
    for _ in 0..capacity {
        check_snapshots(&p, &c, &model, capacity);
        p.try_push(rec(next)).unwrap();
        model.push_back(rec(next));
        next += 1;
    }
    check_snapshots(&p, &c, &model, capacity);
    assert!(p.is_full());
    assert_eq!(p.try_push(rec(next)).unwrap_err().into_inner(), rec(next));
    // full -> partial (one out, one in) several times
    for _ in 0..3 {
        assert_eq!(c.try_pop(), model.pop_front());
        check_snapshots(&p, &c, &model, capacity);
        p.try_push(rec(next)).unwrap();
        model.push_back(rec(next));
        next += 1;
        check_snapshots(&p, &c, &model, capacity);
    }
    // full -> partial -> empty
    while let Some(expected) = model.pop_front() {
        assert_eq!(c.try_pop(), Some(expected));
        check_snapshots(&p, &c, &model, capacity);
    }
    assert_eq!(c.try_pop(), None);
    assert_eq!(c.peek(), None);
    assert_eq!(c.peek_mut(), None);
    check_snapshots(&p, &c, &model, capacity);
}

#[test]
fn transitions_empty_partial_full_partial_empty() {
    for capacity in CAPACITIES {
        each_family!(8, capacity, transitions, capacity);
    }
}

fn fifo_over_wraps<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C, capacity: usize) {
    let mut next_in = 0usize;
    let mut next_out = 0usize;
    // Alternate batches of varying size so the physical index wraps at
    // every offset.
    for round in 0..(50 * capacity) {
        let batch = 1 + round % capacity;
        for _ in 0..batch {
            if p.try_push(rec(next_in)).is_ok() {
                next_in += 1;
            }
        }
        let take = 1 + (round * 7) % capacity;
        for _ in 0..take {
            if let Some(v) = c.try_pop() {
                assert_eq!(unrec(v), next_out);
                next_out += 1;
            }
        }
    }
    while let Some(v) = c.try_pop() {
        assert_eq!(unrec(v), next_out);
        next_out += 1;
    }
    assert_eq!(next_in, next_out);
    assert!(next_in > 20 * capacity);
}

#[test]
fn fifo_order_over_many_physical_wraps() {
    for capacity in CAPACITIES.into_iter().chain([5, 7]) {
        each_family!(8, capacity, fifo_over_wraps, capacity);
    }
}

fn full_returns_original<P: ProducerOps<Tracked>, C: ConsumerOps<Tracked>>(mut p: P, mut c: C) {
    let mut factory = Factory::new();
    p.try_push(factory.make()).unwrap();
    let rejected = factory.make();
    let rejected_id = rejected.id;
    let full = p.try_push(rejected).unwrap_err();
    assert_eq!(full.get_ref().id, rejected_id);
    assert!(
        factory.dropped().is_empty(),
        "library must not drop the rejected value"
    );
    let recovered = full.into_inner();
    assert_eq!(recovered.id, rejected_id);
    assert_eq!(c.try_pop().unwrap().id, 1);
    p.try_push(recovered).unwrap();
    assert_eq!(c.try_pop().unwrap().id, rejected_id);
}

#[test]
fn full_returns_original_non_copy_value() {
    each_typed!(Tracked, 1, full_returns_original);
}

fn full_string<P: ProducerOps<String>, C: ConsumerOps<String>>(mut p: P, _c: C) {
    p.try_push("a".into()).unwrap();
    let mut full = p.try_push("b".into()).unwrap_err();
    assert_eq!(full.get_ref(), "b");
    full.get_mut().push('!');
    assert_eq!(full.to_string(), "ring buffer is full");
    assert!(
        !full.to_string().contains("b!"),
        "Display must not format T"
    );
    let as_error: &dyn Error = &full;
    assert!(as_error.source().is_none());
    let debug = format!("{full:?}");
    assert!(debug.contains("b!"));
    assert_eq!(full.clone().into_inner(), "b!");
    assert_eq!(full.into_inner(), "b!");
}

fn full_copy<P: ProducerOps<u8>, C: ConsumerOps<u8>>(mut p: P, _c: C) {
    p.try_push(0).unwrap();
    let copy: Full<u8> = p.try_push(7).unwrap_err();
    let copied = copy;
    assert_eq!(copy.into_inner(), copied.into_inner());
    assert_eq!(Full::<u8>::into_inner(copy), 7);
}

#[test]
fn full_accessors_and_traits() {
    each_typed!(String, 1, full_string);
    each_typed!(u8, 1, full_copy);
}

#[cfg(feature = "alloc")]
#[test]
fn create_error_traits_and_validation() {
    use spookycircle::{MAX_CAPACITY, bounded};

    assert_eq!(bounded::<u8>(0).unwrap_err(), CreateError::ZeroCapacity);
    assert_eq!(
        bounded::<u8>(MAX_CAPACITY + 1).unwrap_err(),
        CreateError::CapacityTooLarge {
            requested: MAX_CAPACITY + 1
        }
    );
    assert_eq!(
        bounded::<u8>(usize::MAX).unwrap_err(),
        CreateError::CapacityTooLarge {
            requested: usize::MAX
        }
    );
    // Representable capacity, but the backing array overflows `isize`.
    let too_big = MAX_CAPACITY;
    assert_eq!(
        bounded::<u64>(too_big).unwrap_err(),
        CreateError::CapacityTooLarge { requested: too_big }
    );
    // Zero-sized elements never allocate, so the maximum is accepted.
    assert!(bounded::<()>(MAX_CAPACITY).is_ok());
}

#[test]
fn create_error_traits() {
    for error in [
        CreateError::ZeroCapacity,
        CreateError::CapacityTooLarge { requested: 3 },
        CreateError::AllocationFailed,
    ] {
        let copy = error;
        assert_eq!(copy, error);
        let mut text = String::new();
        write!(text, "{error} / {error:?}").unwrap();
        assert!(!text.is_empty());
        let _: &dyn Error = &error;
    }
    assert!(
        CreateError::CapacityTooLarge { requested: 3 }
            .to_string()
            .contains('3')
    );
}

fn snapshots<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C) {
    p.try_push(rec(1)).unwrap();
    p.try_push(rec(2)).unwrap();
    assert_eq!(p.len(), 2);
    assert_eq!(c.len(), 2);
    assert_eq!(p.remaining_capacity(), 1);
    assert_eq!(c.remaining_capacity(), 1);
    assert!(!p.is_empty() && !c.is_empty());
    assert!(!p.is_full() && !c.is_full());
    c.try_pop();
    assert_eq!(p.len(), 1);
    assert_eq!(c.len(), 1);
    p.try_push(rec(3)).unwrap();
    p.try_push(rec(4)).unwrap();
    assert!(p.is_full() && c.is_full());
    assert_eq!(p.remaining_capacity(), 0);
    assert_eq!(c.remaining_capacity(), 0);
}

#[test]
fn snapshot_methods_track_state() {
    each_family!(8, 3, snapshots);
}

fn peek_string<P: ProducerOps<String>, C: ConsumerOps<String>>(
    mut p: P,
    mut c: C,
    capacity: usize,
) {
    assert_eq!(c.peek(), None);
    assert_eq!(c.peek_mut(), None);
    p.try_push("a".into()).unwrap();
    assert_eq!(c.peek().map(String::as_str), Some("a"));
    assert_eq!(
        c.peek().map(String::as_str),
        Some("a"),
        "peek must not advance"
    );
    assert_eq!(c.len(), 1);
    c.peek_mut().unwrap().push('b');
    assert_eq!(c.peek().map(String::as_str), Some("ab"));
    if capacity > 1 {
        p.try_push("z".into()).unwrap();
        assert_eq!(c.peek().map(String::as_str), Some("ab"));
    }
    assert_eq!(c.try_pop().as_deref(), Some("ab"));
    if capacity > 1 {
        assert_eq!(c.peek().map(String::as_str), Some("z"));
        assert_eq!(c.try_pop().as_deref(), Some("z"));
    }
    assert_eq!(c.peek(), None);
}

fn peek_record<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C, capacity: usize) {
    assert_eq!(c.peek(), None);
    p.try_push(rec(1)).unwrap();
    assert_eq!(c.peek(), Some(&rec(1)));
    c.peek_mut().unwrap()[7] = 0xAB;
    let mut mutated = rec(1);
    mutated[7] = 0xAB;
    if capacity > 1 {
        p.try_push(rec(2)).unwrap();
    }
    assert_eq!(c.peek(), Some(&mutated), "peek must not advance");
    assert_eq!(c.try_pop(), Some(mutated), "mutation is what pop returns");
    if capacity > 1 {
        assert_eq!(c.try_pop(), Some(rec(2)));
    }
    assert_eq!(c.peek_mut(), None);
}

#[test]
fn peek_and_peek_mut_then_pop() {
    for capacity in CAPACITIES {
        each_typed!(String, capacity, peek_string, capacity);
        each_family!(8, capacity, peek_record, capacity);
    }
}

fn pop_into_records<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C, capacity: usize) {
    let sentinel = [0xEE; 8];
    let mut destination = MaybeUninit::new(sentinel);
    assert_eq!(c.try_pop_into(&mut destination), None);
    // SAFETY: initialized above; an empty result writes nothing.
    let after_empty = unsafe { destination.assume_init() };
    assert_eq!(
        after_empty, sentinel,
        "empty result leaves destination untouched"
    );

    // Alternate `try_pop_into` and `try_pop` so both advance the same
    // physical index across wraps at every offset.
    let mut next_in = 0usize;
    let mut next_out = 0usize;
    for round in 0..(20 * capacity) {
        for _ in 0..=(round % capacity) {
            if p.try_push(rec(next_in)).is_ok() {
                next_in += 1;
            }
        }
        for k in 0..=((round * 5) % capacity) {
            let got = if (round + k) % 2 == 0 {
                let place: *const Rec = destination.as_ptr();
                c.try_pop_into(&mut destination).map(|r| {
                    assert!(std::ptr::eq(r, place), "result must point into destination");
                    *r
                })
            } else {
                c.try_pop()
            };
            match got {
                Some(v) => {
                    assert_eq!(unrec(v), next_out);
                    next_out += 1;
                }
                None => assert_eq!(next_out, next_in),
            }
        }
    }
    while let Some(&mut v) = c.try_pop_into(&mut destination) {
        assert_eq!(unrec(v), next_out);
        next_out += 1;
    }
    assert_eq!(next_in, next_out);
    assert!(c.is_empty());

    // The mutation through the returned reference lands in `destination`.
    p.try_push(rec(7)).unwrap();
    c.try_pop_into(&mut destination).unwrap()[0] = 0xAB;
    let mut expected = rec(7);
    expected[0] = 0xAB;
    // SAFETY: initialized by the successful call above.
    assert_eq!(unsafe { destination.assume_init() }, expected);
}

fn pop_into_ownership<P: ProducerOps<Tracked>, C: ConsumerOps<Tracked>>(
    mut p: P,
    mut c: C,
    capacity: usize,
) {
    let mut factory = Factory::new();
    let mut destination = MaybeUninit::uninit();
    assert!(c.try_pop_into(&mut destination).is_none());
    for _ in 0..capacity {
        p.try_push(factory.make()).unwrap();
    }

    let id = c.try_pop_into(&mut destination).unwrap().id;
    assert_eq!(id, 1);
    assert!(factory.dropped().is_empty(), "the queue drops nothing");
    // SAFETY: initialized by the successful call above; read out once.
    drop(unsafe { destination.assume_init_read() });
    assert_eq!(factory.dropped(), vec![1], "the caller owns and drops it");

    if capacity > 2 {
        assert_eq!(c.try_pop_into(&mut destination).unwrap().id, 2);
        // SAFETY: initialized by the call above. The bitwise copy takes
        // ownership; `destination` still holds the same bytes.
        let second = unsafe { destination.assume_init_read() };
        // Overwriting a destination that still holds a value's bytes drops
        // nothing.
        assert_eq!(c.try_pop_into(&mut destination).unwrap().id, 3);
        assert_eq!(factory.dropped(), vec![1], "overwrite drops nothing");
        drop(second);
        // SAFETY: holds value 3, initialized by the last call.
        unsafe { destination.assume_init_drop() };
        assert_eq!(factory.dropped(), vec![1, 2, 3]);
    }

    drop(p);
    drop(c);
    let all: Vec<u64> = (1..=factory.next).collect();
    factory.assert_dropped_exactly_once_each(&all);
}

#[test]
fn pop_into_moves_into_destination() {
    for capacity in CAPACITIES.into_iter().chain([5]) {
        each_family!(8, capacity, pop_into_records, capacity);
        each_typed!(Tracked, capacity, pop_into_ownership, capacity);
    }
}

fn bulk_zero<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C) {
    assert_eq!(p.push_slice(&[]), 0);
    assert_eq!(c.pop_slice(&mut []), 0);
    assert_eq!(p.len(), 0);
    p.try_push(rec(1)).unwrap();
    assert_eq!(c.pop_slice(&mut []), 0);
    assert_eq!(c.len(), 1);
    assert_eq!(p.push_slice(&[]), 0);
    assert_eq!(c.len(), 1);
}

#[test]
fn bulk_zero_length() {
    each_family!(8, 4, bulk_zero);
}

fn bulk_cases<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C, capacity: usize) {
    const NONE: Rec = [0xFF; 8];
    let data: Vec<Rec> = (0..capacity + 3).map(rec).collect();

    // Exact fit.
    assert_eq!(p.push_slice(&data[..capacity]), capacity);
    assert!(p.is_full());
    // Insufficient space: full queue accepts nothing.
    assert_eq!(p.push_slice(&data), 0);
    // Exact drain.
    let mut out = vec![NONE; capacity];
    assert_eq!(c.pop_slice(&mut out), capacity);
    assert_eq!(out, data[..capacity]);
    assert!(c.is_empty());
    // Insufficient data: empty queue yields nothing and leaves dest alone.
    let mut out = vec![NONE; capacity];
    assert_eq!(c.pop_slice(&mut out), 0);
    assert!(out.iter().all(|&x| x == NONE));

    // Partial push: source longer than the space available.
    let filled = capacity.div_ceil(2);
    for i in 0..filled {
        p.try_push(rec(1000 + i)).unwrap();
    }
    let n = p.push_slice(&data);
    assert_eq!(n, capacity - filled);
    assert!(p.is_full());
    // Partial pop: destination longer than the data available.
    let mut out = vec![NONE; capacity + 2];
    assert_eq!(c.pop_slice(&mut out), capacity);
    let mut expected: Vec<Rec> = (0..filled).map(|i| rec(1000 + i)).collect();
    expected.extend_from_slice(&data[..n]);
    assert_eq!(&out[..capacity], &expected[..]);
    assert!(out[capacity..].iter().all(|&x| x == NONE));
    // Destination shorter than the data available.
    for i in 0..capacity {
        p.try_push(rec(i)).unwrap();
    }
    let mut out = [NONE; 1];
    assert_eq!(c.pop_slice(&mut out), 1);
    assert_eq!(out, [rec(0)]);
    assert_eq!(c.len(), capacity - 1);
}

#[test]
fn bulk_exact_partial_insufficient_space_and_data() {
    for capacity in CAPACITIES.into_iter().chain([5]) {
        each_family!(8, capacity, bulk_cases, capacity);
    }
}

fn two_segment<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(
    mut p: P,
    mut c: C,
    capacity: usize,
    offset: usize,
) {
    // Advance the physical index to `offset`.
    for i in 0..offset {
        p.try_push(rec(i)).unwrap();
        assert_eq!(c.try_pop(), Some(rec(i)));
    }
    // A full-capacity batch now crosses the physical end.
    let data: Vec<Rec> = (100..100 + capacity).map(rec).collect();
    assert_eq!(p.push_slice(&data), capacity);
    assert!(c.is_full());
    let mut out = vec![rec(0); capacity];
    assert_eq!(c.pop_slice(&mut out), capacity);
    assert_eq!(out, data);
    // Mixed: scalar pops after a wrapped bulk push.
    assert_eq!(p.push_slice(&data), capacity);
    for &expected in &data {
        assert_eq!(c.try_pop(), Some(expected));
    }
    // Wrapped bulk pop after scalar pushes.
    for &v in &data {
        p.try_push(v).unwrap();
    }
    let mut out = vec![rec(0); capacity];
    assert_eq!(c.pop_slice(&mut out), capacity);
    assert_eq!(out, data);
}

#[test]
fn bulk_two_segment_wrap() {
    for capacity in [2usize, 3, 5, 8] {
        for offset in 1..capacity {
            each_family!(8, capacity, two_segment, capacity, offset);
        }
    }
}

fn drop_either_order<P: ProducerOps<Tracked>, C: ConsumerOps<Tracked>>(
    mut p: P,
    mut c: C,
    capacity: usize,
    producer_first: bool,
) {
    let mut factory = Factory::new();
    // Cycle a few to move the physical indices.
    for _ in 0..=capacity {
        p.try_push(factory.make()).unwrap();
        drop(c.try_pop().unwrap());
    }
    let mut queued = Vec::new();
    for _ in 0..capacity {
        let v = factory.make();
        queued.push(v.id);
        p.try_push(v).unwrap();
    }
    let before = factory.dropped().len();
    if producer_first {
        drop(p);
        assert_eq!(
            factory.dropped().len(),
            before,
            "first drop must not destroy"
        );
        assert!(!c.is_producer_alive());
        drop(c);
    } else {
        drop(c);
        assert_eq!(factory.dropped().len(), before);
        assert!(!p.is_consumer_alive());
        drop(p);
    }
    let dropped = factory.dropped();
    assert_eq!(
        &dropped[before..],
        &queued[..],
        "queued values drop in FIFO order"
    );
    let all: Vec<u64> = (1..=factory.next).collect();
    factory.assert_dropped_exactly_once_each(&all);
}

#[test]
fn drop_endpoints_in_either_order_with_queued_values() {
    for capacity in CAPACITIES {
        for producer_first in [true, false] {
            each_typed!(
                Tracked,
                capacity,
                drop_either_order,
                capacity,
                producer_first
            );
        }
    }
}

fn drain_after_producer_drop<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(
    mut p: P,
    mut c: C,
    capacity: usize,
) {
    for i in 0..capacity {
        p.try_push(rec(i)).unwrap();
    }
    assert!(!c.is_drained());
    drop(p);
    assert!(!c.is_producer_alive());
    assert_eq!(c.len(), capacity);
    assert!(!c.is_drained(), "values remain");
    for i in 0..capacity {
        assert_eq!(c.peek(), Some(&rec(i)));
        assert_eq!(c.try_pop(), Some(rec(i)));
        assert_eq!(c.is_drained(), i + 1 == capacity);
    }
    assert_eq!(c.try_pop(), None);
    assert!(c.is_drained());
    assert!(c.is_drained(), "stays true");
    assert!(c.is_empty());
}

#[test]
fn producer_drop_then_drain_and_is_drained() {
    for capacity in CAPACITIES {
        each_family!(8, capacity, drain_after_producer_drop, capacity);
    }
}

fn fill_after_consumer_drop<P: ProducerOps<Tracked>, C: ConsumerOps<Tracked>>(
    mut p: P,
    c: C,
    capacity: usize,
) {
    let mut factory = Factory::new();
    drop(c);
    assert!(!p.is_consumer_alive());
    let mut ids = Vec::new();
    for _ in 0..capacity {
        let v = factory.make();
        ids.push(v.id);
        p.try_push(v).unwrap();
    }
    assert!(p.is_full());
    let rejected = factory.make();
    let rejected_id = rejected.id;
    let full = p.try_push(rejected).unwrap_err();
    assert_eq!(full.into_inner().id, rejected_id);
    assert_eq!(factory.dropped(), vec![rejected_id]);
    drop(p);
    let dropped = factory.dropped();
    assert_eq!(&dropped[1..], &ids[..]);
}

fn fill_records_after_consumer_drop<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(
    mut p: P,
    c: C,
    capacity: usize,
) {
    drop(c);
    assert!(!p.is_consumer_alive());
    for i in 0..capacity {
        p.try_push(rec(i)).unwrap();
    }
    assert!(p.is_full());
    assert_eq!(p.try_push(rec(99)).unwrap_err().into_inner(), rec(99));
    assert_eq!(p.push_slice(&[rec(1)]), 0);
}

#[test]
fn consumer_drop_then_producer_fills() {
    for capacity in CAPACITIES {
        each_typed!(Tracked, capacity, fill_after_consumer_drop, capacity);
        each_family!(8, capacity, fill_records_after_consumer_drop, capacity);
    }
}

fn simultaneous_drop<P, C>(mut p: P, c: C)
where
    P: ProducerOps<Tracked> + Send,
    C: ConsumerOps<Tracked> + Send,
{
    let log = Arc::new(Mutex::new(Vec::new()));
    for id in 1..=3 {
        p.try_push(Tracked {
            id,
            _not_sync: Cell::new(0),
            log: log.clone(),
        })
        .unwrap();
    }
    let barrier = Barrier::new(2);
    thread::scope(|s| {
        s.spawn(|| {
            barrier.wait();
            drop(p);
        });
        s.spawn(|| {
            barrier.wait();
            drop(c);
        });
    });
    assert_eq!(*log.lock().unwrap(), vec![1, 2, 3]);
}

#[test]
fn simultaneous_endpoint_drop() {
    for _ in 0..if cfg!(miri) { 4 } else { 200 } {
        each_typed!(Tracked, 4, simultaneous_drop);
    }
}

fn send_not_sync<P, C>(mut p: P, c: C)
where
    P: ProducerOps<Tracked> + Send,
    C: ConsumerOps<Tracked> + Send,
{
    let mut factory = Factory::new();
    p.try_push(factory.make()).unwrap();
    let (id, mut c) = thread::scope(|s| {
        s.spawn(move || {
            let mut c = c;
            let first = c.try_pop().unwrap();
            (first.id, c)
        })
        .join()
        .unwrap()
    });
    assert_eq!(id, 1);
    p.try_push(factory.make()).unwrap();
    assert_eq!(c.try_pop().unwrap().id, 2);
    drop((p, c));
    factory.assert_dropped_exactly_once_each(&[1, 2]);
}

#[test]
fn send_but_not_sync_element_type_crosses_threads() {
    each_typed!(Tracked, 2, send_not_sync);
}

#[repr(align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Aligned64([u8; 64]);

#[repr(align(4096))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Aligned4096(u32);

fn aligned64<P: ProducerOps<Aligned64>, C: ConsumerOps<Aligned64>>(
    mut p: P,
    mut c: C,
    capacity: usize,
) {
    for i in 0..capacity {
        p.try_push(Aligned64([u8::try_from(i).unwrap(); 64]))
            .unwrap();
    }
    for i in 0..capacity {
        let r = c.peek().unwrap();
        assert_eq!(std::ptr::from_ref::<Aligned64>(r).addr() % 64, 0);
        assert_eq!(c.try_pop(), Some(Aligned64([u8::try_from(i).unwrap(); 64])));
    }
    let mut out = [Aligned64([0; 64]); 2];
    assert_eq!(p.push_slice(&[Aligned64([9; 64]); 2]), capacity.min(2));
    assert_eq!(c.pop_slice(&mut out), capacity.min(2));
}

fn aligned4096<P: ProducerOps<Aligned4096>, C: ConsumerOps<Aligned4096>>(
    mut p: P,
    mut c: C,
    capacity: usize,
) {
    for i in 0..capacity {
        p.try_push(Aligned4096(u32::try_from(i).unwrap())).unwrap();
    }
    for i in 0..capacity {
        let r = c.peek_mut().unwrap();
        assert_eq!(std::ptr::from_ref::<Aligned4096>(r).addr() % 4096, 0);
        r.0 += 100;
        assert_eq!(
            c.try_pop(),
            Some(Aligned4096(u32::try_from(i).unwrap() + 100))
        );
    }
}

#[test]
fn aligned_element_types() {
    for capacity in CAPACITIES {
        each_typed!(Aligned64, capacity, aligned64, capacity);
        each_typed!(Aligned4096, capacity, aligned4096, capacity);
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct Unit;

fn zst_unit<P: ProducerOps<Unit>, C: ConsumerOps<Unit>>(mut p: P, mut c: C, capacity: usize) {
    assert_eq!(p.capacity(), capacity);
    for _ in 0..capacity {
        p.try_push(Unit).unwrap();
    }
    assert!(p.is_full());
    assert_eq!(p.try_push(Unit).unwrap_err().into_inner(), Unit);
    assert_eq!(c.peek(), Some(&Unit));
    assert_eq!(c.peek_mut(), Some(&mut Unit));
    for _ in 0..capacity {
        assert_eq!(c.try_pop(), Some(Unit));
    }
    assert_eq!(c.try_pop(), None);
    assert_eq!(p.push_slice(&[Unit; 3]), capacity.min(3));
    let mut out = [Unit; 5];
    assert_eq!(c.pop_slice(&mut out), capacity.min(3));
    // Many wraps of the physical index.
    let mut destination = MaybeUninit::uninit();
    for _ in 0..10 * capacity {
        p.try_push(Unit).unwrap();
        assert_eq!(c.try_pop(), Some(Unit));
        p.try_push(Unit).unwrap();
        assert_eq!(c.try_pop_into(&mut destination), Some(&mut Unit));
    }
    assert_eq!(c.try_pop_into(&mut destination), None);
}

#[test]
fn zero_sized_inhabited_type() {
    for capacity in CAPACITIES.into_iter().chain([1000]) {
        each_typed!(Unit, capacity, zst_unit, capacity);
    }
    // Zero-byte shared records use capacity but no payload bytes.
    #[cfg(feature = "shared-memory")]
    for capacity in CAPACITIES {
        let region = common::HeapRegion::new::<0>(capacity);
        let (mut p, mut c) = region.attach::<0>();
        for _ in 0..capacity {
            p.try_push([]).unwrap();
        }
        assert!(p.is_full());
        assert_eq!(p.try_push([]).unwrap_err().into_inner(), []);
        assert_eq!(c.peek(), Some(&[]));
        assert_eq!(c.pop_slice(&mut [[]; 2]), capacity.min(2));
        assert_eq!(c.len(), capacity - capacity.min(2));
    }
}

#[repr(align(4096))]
#[derive(Debug, PartialEq, Eq)]
struct PageAlignedUnit;

static PAGE_DROPS: AtomicUsize = AtomicUsize::new(0);

impl Drop for PageAlignedUnit {
    fn drop(&mut self) {
        assert_eq!(
            std::ptr::from_mut::<Self>(self).addr() % 4096,
            0,
            "misaligned drop"
        );
        PAGE_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

fn zst_over_aligned<P: ProducerOps<PageAlignedUnit>, C: ConsumerOps<PageAlignedUnit>>(
    mut p: P,
    mut c: C,
    capacity: usize,
) {
    let base = PAGE_DROPS.load(Ordering::SeqCst);
    for _ in 0..capacity {
        p.try_push(PageAlignedUnit).unwrap();
    }
    drop(p.try_push(PageAlignedUnit).unwrap_err());
    let r = c.peek().unwrap();
    assert_eq!(std::ptr::from_ref::<PageAlignedUnit>(r).addr() % 4096, 0);
    let m = c.peek_mut().unwrap();
    assert_eq!(std::ptr::from_mut::<PageAlignedUnit>(m).addr() % 4096, 0);
    drop(c.try_pop().unwrap());
    assert_eq!(PAGE_DROPS.load(Ordering::SeqCst) - base, 2);
    // The rest are dropped at final cleanup, through the same pointer.
    drop((p, c));
    assert_eq!(PAGE_DROPS.load(Ordering::SeqCst) - base, capacity + 1);
}

/// Zero-sized slots need an address aligned for `T` even when `T` is more
/// strictly aligned than the storage around it.
#[test]
fn zero_sized_over_aligned_type() {
    for capacity in CAPACITIES {
        each_typed!(PageAlignedUnit, capacity, zst_over_aligned, capacity);
    }
}

static ZST_DROPS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct ZstDrop;

impl Drop for ZstDrop {
    fn drop(&mut self) {
        ZST_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

fn zst_drop_counts<P: ProducerOps<ZstDrop>, C: ConsumerOps<ZstDrop>>(mut p: P, mut c: C) {
    let base = ZST_DROPS.load(Ordering::SeqCst);
    p.try_push(ZstDrop).unwrap();
    p.try_push(ZstDrop).unwrap();
    p.try_push(ZstDrop).unwrap();
    let full = p.try_push(ZstDrop).unwrap_err();
    assert_eq!(ZST_DROPS.load(Ordering::SeqCst) - base, 0);
    drop(full);
    assert_eq!(
        ZST_DROPS.load(Ordering::SeqCst) - base,
        1,
        "rejected value drops once"
    );
    drop(c.try_pop().unwrap());
    assert_eq!(
        ZST_DROPS.load(Ordering::SeqCst) - base,
        2,
        "popped value drops once"
    );
    std::mem::forget(c.try_pop().unwrap());
    assert_eq!(
        ZST_DROPS.load(Ordering::SeqCst) - base,
        2,
        "forgotten value is not dropped"
    );
    drop(p);
    assert_eq!(ZST_DROPS.load(Ordering::SeqCst) - base, 2);
    drop(c);
    assert_eq!(
        ZST_DROPS.load(Ordering::SeqCst) - base,
        3,
        "one queued value drops at cleanup"
    );
}

#[test]
fn zero_sized_type_with_drop_counts() {
    each_typed!(ZstDrop, 3, zst_drop_counts);
}

fn drop_accounting<P: ProducerOps<Tracked>, C: ConsumerOps<Tracked>>(
    mut p: P,
    mut c: C,
    capacity: usize,
) {
    let mut factory = Factory::new();
    // popped
    p.try_push(factory.make()).unwrap();
    let popped = c.try_pop().unwrap();
    let popped_id = popped.id;
    drop(popped);
    assert_eq!(factory.dropped(), vec![popped_id]);
    // queued (fills the queue)
    let mut queued = Vec::new();
    for _ in 0..capacity {
        let v = factory.make();
        queued.push(v.id);
        p.try_push(v).unwrap();
    }
    // returned full
    let rejected = factory.make();
    let rejected_id = rejected.id;
    drop(p.try_push(rejected).unwrap_err());
    assert_eq!(factory.dropped(), vec![popped_id, rejected_id]);
    // never-initialized slots: pop one so a free slot exists at cleanup
    let first_queued = c.try_pop().unwrap();
    drop(first_queued);
    drop(p);
    drop(c);
    let all: Vec<u64> = (1..=factory.next).collect();
    factory.assert_dropped_exactly_once_each(&all);
    assert_eq!(factory.dropped().len(), capacity + 2);
}

#[test]
fn drop_accounting_for_every_slot_kind() {
    for capacity in CAPACITIES.into_iter().chain([5]) {
        each_typed!(Tracked, capacity, drop_accounting, capacity);
    }
}

#[derive(Debug)]
struct Bomb {
    id: u32,
    explode: bool,
    log: Arc<Mutex<Vec<u32>>>,
}

impl Drop for Bomb {
    fn drop(&mut self) {
        self.log.lock().unwrap().push(self.id);
        assert!(!self.explode, "bomb {} exploded", self.id);
    }
}

fn panicking_cleanup<P: ProducerOps<Bomb>, C: ConsumerOps<Bomb>>(mut p: P, c: C) {
    let log = Arc::new(Mutex::new(Vec::new()));
    for (id, explode) in [(1, false), (2, true), (3, false), (4, false)] {
        p.try_push(Bomb {
            id,
            explode,
            log: log.clone(),
        })
        .unwrap();
    }
    drop(p);
    let result = catch_unwind(AssertUnwindSafe(move || drop(c)));
    assert!(result.is_err(), "the panic propagates");
    // 1 dropped normally, 2 began and panicked, 3 and 4 dropped during
    // unwinding, nothing dropped twice.
    assert_eq!(*log.lock().unwrap(), vec![1, 2, 3, 4]);
}

#[test]
fn panicking_destructor_during_cleanup_still_drops_the_rest() {
    each_typed!(Bomb, 4, panicking_cleanup);
}

fn debug_output<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, c: C) {
    p.try_push([0xA5; 8]).unwrap();
    let pd = format!("{p:?}");
    let cd = format!("{c:?}");
    assert!(pd.contains("Producer {"), "{pd}");
    assert!(cd.contains("Consumer {"), "{cd}");
    for text in [&pd, &cd] {
        assert!(text.contains("capacity: 2"), "{text}");
        assert!(text.contains("len: 1"), "{text}");
        assert!(!text.contains("165"), "must not format elements: {text}");
    }
    assert!(pd.contains("consumer_alive: true"));
    assert!(cd.contains("producer_alive: true"));
    drop(c);
    assert!(format!("{p:?}").contains("consumer_alive: false"));
}

#[test]
fn debug_output_reports_role_capacity_len_and_liveness() {
    each_family!(8, 2, debug_output);
    #[cfg(feature = "alloc")]
    {
        let (p, c) = spookycircle::bounded::<Vec<u8>>(2).unwrap();
        assert!(format!("{p:?}").starts_with("Producer {"));
        assert!(format!("{c:?}").starts_with("Consumer {"));
    }
    let mut slots = [const { spookycircle::Slot::<u8>::new() }; 1];
    let storage = spookycircle::BorrowedStorage::new(&mut slots).unwrap();
    let (p, c) = storage.try_split().unwrap();
    assert!(format!("{p:?}").starts_with("BorrowedProducer {"));
    assert!(format!("{c:?}").starts_with("BorrowedConsumer {"));
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "family runners hand bodies owned endpoints"
)]
fn liveness<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(p: P, c: C) {
    assert!(p.is_consumer_alive());
    assert!(c.is_producer_alive());
    drop(p);
    assert!(!c.is_producer_alive());
    assert!(!c.is_producer_alive());
    assert!(c.is_drained());
}

#[test]
fn liveness_flags_are_monotonic_and_independent() {
    each_family!(8, 1, liveness);
}

fn moved_endpoints<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, c: C) {
    // Moving an endpoint (by value) must not disturb its private caches.
    p.try_push(rec(1)).unwrap();
    let mut p2 = p;
    p2.try_push(rec(2)).unwrap();
    let boxed = Box::new(c);
    let mut c = *boxed;
    assert_eq!(c.try_pop(), Some(rec(1)));
    assert_eq!(c.try_pop(), Some(rec(2)));
    assert_eq!(c.try_pop(), None);
    p2.try_push(rec(3)).unwrap();
    assert_eq!(c.try_pop(), Some(rec(3)));
}

#[test]
fn moved_endpoints_keep_cached_state() {
    each_family!(8, 3, moved_endpoints);
}
