//! Reference-model property tests: random sequential
//! operation traces compared against a `VecDeque`, with complete ownership
//! accounting, for every endpoint family.
//!
//! Trace bodies are generic over the endpoint traits and use plain
//! assertions; proptest treats a panic as a failing case and shrinks it.

// Loom-instrumented atomics only work inside a Loom model; see tests/loom.rs.
#![cfg(not(loom))]

#[macro_use]
mod common;

use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    mem::MaybeUninit,
    rc::Rc,
};

use common::{ConsumerOps, ProducerOps, Rec, rec};
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Op {
    Push,
    Pop,
    /// `try_pop_into` into a reused destination.
    PopInto,
    Peek,
    /// Mutate the head element through `peek_mut` by adding this delta.
    PeekMut(u32),
    PushSlice(usize),
    PopSlice(usize),
    /// Every status query at once.
    Status,
}

/// 512 cases unless `PROPTEST_CASES` says otherwise: CI lowers it under Miri
/// and the sanitizers. `ProptestConfig::with_cases` on its own ignores the
/// variable, which only feeds proptest's default configuration.
fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(512)
}

fn op_strategy(max_len: usize) -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => Just(Op::Push),
        4 => Just(Op::Pop),
        2 => Just(Op::PopInto),
        1 => Just(Op::Peek),
        1 => (1..1000u32).prop_map(Op::PeekMut),
        2 => (0..=max_len).prop_map(Op::PushSlice),
        2 => (0..=max_len).prop_map(Op::PopSlice),
        1 => Just(Op::Status),
    ]
}

fn capacity_strategy() -> impl Strategy<Value = usize> {
    prop_oneof![
        3 => Just(1usize),
        2 => Just(2usize),
        3 => Just(3usize),
        2 => Just(5usize),
        2 => Just(7usize),
        1 => Just(8usize),
        1 => 9..40usize,
    ]
}

fn check_status<T, M>(
    p: &impl ProducerOps<T>,
    c: &impl ConsumerOps<T>,
    model: &VecDeque<M>,
    capacity: usize,
) {
    assert_eq!(p.capacity(), capacity);
    assert_eq!(c.capacity(), capacity);
    assert_eq!(p.len(), model.len());
    assert_eq!(c.len(), model.len());
    assert_eq!(p.remaining_capacity(), capacity - model.len());
    assert_eq!(c.remaining_capacity(), capacity - model.len());
    assert_eq!(p.is_empty(), model.is_empty());
    assert_eq!(c.is_empty(), model.is_empty());
    assert_eq!(p.is_full(), model.len() == capacity);
    assert_eq!(c.is_full(), model.len() == capacity);
    assert!(p.is_consumer_alive());
    assert!(c.is_producer_alive());
    assert!(!c.is_drained());
}

/// Adds `delta` to a record's value, as `peek_mut` mutations do in place.
fn bump(record: &mut Rec, delta: u32) {
    let value = u64::from_le_bytes(*record).wrapping_add(u64::from(delta));
    *record = value.to_le_bytes();
}

/// One `Copy` trace: every operation kind, including bulk transfer.
fn copy_trace<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(
    mut p: P,
    mut c: C,
    ops: &[Op],
    capacity: usize,
) {
    const NONE: Rec = [0xFF; 8];
    let mut model: VecDeque<Rec> = VecDeque::new();
    let mut next = 0usize;
    let mut destination = MaybeUninit::uninit();
    for op in ops {
        match *op {
            Op::Push => match p.try_push(rec(next)) {
                Ok(()) => {
                    model.push_back(rec(next));
                    next += 1;
                }
                Err(full) => {
                    assert_eq!(full.into_inner(), rec(next));
                    assert_eq!(model.len(), capacity);
                }
            },
            Op::Pop => assert_eq!(c.try_pop(), model.pop_front()),
            Op::PopInto => {
                let expected = model.pop_front();
                assert_eq!(c.try_pop_into(&mut destination).copied(), expected);
            }
            Op::Peek => assert_eq!(c.peek(), model.front()),
            Op::PeekMut(delta) => {
                let got = c.peek_mut();
                let expected = model.front_mut();
                assert_eq!(got.is_some(), expected.is_some());
                if let (Some(g), Some(e)) = (got, expected) {
                    assert_eq!(*g, *e);
                    bump(g, delta);
                    bump(e, delta);
                }
            }
            Op::PushSlice(len) => {
                let source: Vec<Rec> = (next..next + len).map(rec).collect();
                let done = p.push_slice(&source);
                assert_eq!(done, len.min(capacity - model.len()));
                model.extend(&source[..done]);
                next += done;
            }
            Op::PopSlice(len) => {
                let mut dest = vec![NONE; len];
                let done = c.pop_slice(&mut dest);
                assert_eq!(done, len.min(model.len()));
                for got in &dest[..done] {
                    assert_eq!(Some(*got), model.pop_front());
                }
                assert!(dest[done..].iter().all(|&x| x == NONE));
            }
            Op::Status => check_status(&p, &c, &model, capacity),
        }
        assert_eq!(p.len(), model.len());
        assert_eq!(c.len(), model.len());
    }
    while let Some(expected) = model.pop_front() {
        assert_eq!(c.try_pop(), Some(expected));
    }
    assert_eq!(c.try_pop(), None);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    /// `Copy` records in every family (heap, borrowed, static, shared).
    #[test]
    fn copy_elements_match_vecdeque(
        capacity in capacity_strategy(),
        ops in prop::collection::vec(op_strategy(48), 1..400),
    ) {
        each_family!(8, capacity, copy_trace, &ops, capacity);
    }
}

/// A non-`Copy` element whose every construction and destruction is logged.
#[derive(Debug)]
struct Owned {
    id: u64,
    payload: u32,
    ledger: Rc<RefCell<Ledger>>,
}

#[derive(Default, Debug)]
struct Ledger {
    created: u64,
    /// id -> number of times dropped
    dropped: BTreeMap<u64, u32>,
}

impl Drop for Owned {
    fn drop(&mut self) {
        *self.ledger.borrow_mut().dropped.entry(self.id).or_insert(0) += 1;
    }
}

fn make(ledger: &Rc<RefCell<Ledger>>, payload: u32) -> Owned {
    let mut l = ledger.borrow_mut();
    l.created += 1;
    Owned {
        id: l.created,
        payload,
        ledger: ledger.clone(),
    }
}

#[derive(Clone, Debug)]
enum OwnedOp {
    Push,
    Pop,
    /// Pop and keep the value alive until the end of the trace.
    PopAndHold,
    /// `try_pop_into` a fresh destination, then take the value out of it.
    PopInto,
    Peek,
    PeekMut(u32),
    Status,
}

fn owned_op_strategy() -> impl Strategy<Value = OwnedOp> {
    prop_oneof![
        5 => Just(OwnedOp::Push),
        3 => Just(OwnedOp::Pop),
        1 => Just(OwnedOp::PopAndHold),
        2 => Just(OwnedOp::PopInto),
        1 => Just(OwnedOp::Peek),
        1 => (1..1000u32).prop_map(OwnedOp::PeekMut),
        1 => Just(OwnedOp::Status),
    ]
}

/// One non-`Copy` trace: scalar operations with full ownership accounting.
/// Rejected values are retained by the trace and must be dropped exactly
/// once, like every other value.
fn owned_trace<P: ProducerOps<Owned>, C: ConsumerOps<Owned>>(
    mut p: P,
    mut c: C,
    ops: &[OwnedOp],
    capacity: usize,
    drop_producer_first: bool,
) {
    let ledger = Rc::new(RefCell::new(Ledger::default()));
    // Model holds (id, payload).
    let mut model: VecDeque<(u64, u32)> = VecDeque::new();
    let mut retained: Vec<Owned> = Vec::new();
    let mut payload = 0u32;
    for op in ops {
        match *op {
            OwnedOp::Push => {
                payload += 1;
                let value = make(&ledger, payload);
                let id = value.id;
                match p.try_push(value) {
                    Ok(()) => model.push_back((id, payload)),
                    Err(full) => {
                        assert_eq!(model.len(), capacity);
                        let value = full.into_inner();
                        assert_eq!(value.id, id);
                        retained.push(value);
                    }
                }
            }
            OwnedOp::Pop => {
                let got = c.try_pop();
                let expected = model.pop_front();
                assert_eq!(got.as_ref().map(|v| (v.id, v.payload)), expected);
            }
            OwnedOp::PopInto => {
                let mut destination = MaybeUninit::uninit();
                let got = c.try_pop_into(&mut destination).map(|v| (v.id, v.payload));
                let expected = model.pop_front();
                assert_eq!(got, expected);
                if got.is_some() {
                    // SAFETY: initialized by the successful call; the ledger
                    // then checks that it is dropped exactly once.
                    retained.push(unsafe { destination.assume_init() });
                }
            }
            OwnedOp::PopAndHold => {
                let got = c.try_pop();
                let expected = model.pop_front();
                assert_eq!(got.as_ref().map(|v| (v.id, v.payload)), expected);
                retained.extend(got);
            }
            OwnedOp::Peek => {
                assert_eq!(c.peek().map(|v| (v.id, v.payload)), model.front().copied());
            }
            OwnedOp::PeekMut(delta) => {
                let got = c.peek_mut();
                let expected = model.front_mut();
                assert_eq!(got.is_some(), expected.is_some());
                if let (Some(g), Some(e)) = (got, expected) {
                    assert_eq!((g.id, g.payload), *e);
                    g.payload += delta;
                    e.1 += delta;
                }
            }
            OwnedOp::Status => check_status(&p, &c, &model, capacity),
        }
        // Nothing may have been dropped except through the trace itself.
        let l = ledger.borrow();
        let dropped_total: u32 = l.dropped.values().sum();
        let popped_and_discarded =
            usize::try_from(l.created).unwrap() - model.len() - retained.len();
        assert_eq!(dropped_total as usize, popped_and_discarded);
    }
    let queued = model.len();
    if drop_producer_first {
        drop(p);
        assert_eq!(c.len(), queued);
        drop(c);
    } else {
        drop(c);
        assert!(!p.is_consumer_alive());
        drop(p);
    }
    drop(retained);
    let l = ledger.borrow();
    assert_eq!(l.dropped.len() as u64, l.created, "every value dropped");
    assert!(
        l.dropped.values().all(|&n| n == 1),
        "no double drops: {:?}",
        l.dropped
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    /// Non-`Copy` elements in every typed family (heap, borrowed, static).
    #[test]
    fn owned_elements_match_vecdeque_with_exact_ownership(
        capacity in capacity_strategy(),
        ops in prop::collection::vec(owned_op_strategy(), 1..300),
        drop_producer_first in any::<bool>(),
    ) {
        each_typed!(Owned, capacity, owned_trace, &ops, capacity, drop_producer_first);
    }
}
