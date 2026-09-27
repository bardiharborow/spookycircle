//! Instrumented-allocator tests: zero data-path allocation in every storage
//! mode, zero allocator calls over the whole borrowed, static, and shared
//! lifecycles, and heap constructor failure
//! with partial-allocation cleanup.
//!
//! Counters are thread-local so that concurrently running tests in this
//! binary (and the test harness itself) do not perturb each other.

// Loom-instrumented atomics only work inside a Loom model; see tests/loom.rs.
#![cfg(not(loom))]
// Dropping and forgetting storage (which has no `Drop` impl) is the point of
// several tests here: they show that storage destruction runs no `T` code.
#![allow(clippy::drop_non_drop)]

#[macro_use]
mod common;

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    ptr,
};

use common::{ConsumerOps, ProducerOps, Rec};
use spookycircle::{BorrowedStorage, Slot, StaticStorage};

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static DEALLOCS: Cell<usize> = const { Cell::new(0) };
    /// When nonzero, the n-th allocation from now fails (1 = the next one).
    static FAIL_AT: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: delegates every allocation to `System` and only manipulates
// `const`-initialised thread-locals, which never allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let fail = FAIL_AT.with(|f| {
            let n = f.get();
            if n == 0 {
                false
            } else {
                f.set(n - 1);
                n == 1
            }
        });
        if fail {
            return ptr::null_mut();
        }
        ALLOCS.with(|c| c.set(c.get() + 1));
        // SAFETY: forwarding the caller's contract to `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCS.with(|c| c.set(c.get() + 1));
        // SAFETY: forwarding the caller's contract to `System`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn counts() -> (usize, usize) {
    (ALLOCS.with(Cell::get), DEALLOCS.with(Cell::get))
}

#[cfg_attr(not(feature = "alloc"), allow(dead_code))]
fn fail_allocation_number(n: usize) {
    FAIL_AT.with(|f| f.set(n));
}

/// Every data-path method, measured after construction.
fn data_path<P: ProducerOps<Rec>, C: ConsumerOps<Rec>>(mut p: P, mut c: C, capacity: usize) {
    let source: Vec<Rec> = (0..capacity as u64 + 3).map(u64::to_le_bytes).collect();
    let mut dest = vec![[0u8; 8]; capacity + 3];
    let mut into = std::mem::MaybeUninit::uninit();

    let before = counts();
    for round in 0..3 {
        for i in 0..=capacity {
            let _ = p.try_push((i as u64).to_le_bytes());
        }
        let _ = p.push_slice(&source);
        let _ = p.len();
        let _ = p.remaining_capacity();
        let _ = p.is_empty();
        let _ = p.is_full();
        let _ = p.capacity();
        let _ = p.is_consumer_alive();
        let _ = c.peek();
        if let Some(v) = c.peek_mut() {
            v[0] = v[0].wrapping_add(1);
        }
        let _ = c.len();
        let _ = c.remaining_capacity();
        let _ = c.is_empty();
        let _ = c.is_full();
        let _ = c.capacity();
        let _ = c.is_producer_alive();
        let _ = c.is_drained();
        for _ in 0..(capacity / 2 + round) {
            let _ = c.try_pop();
        }
        let _ = c.pop_slice(&mut dest);
        let _ = c.try_pop();
        let _ = c.try_pop_into(&mut into);
        let _ = c.peek();
    }
    assert_eq!(counts(), before, "data path allocated or deallocated");
}

#[test]
fn data_path_never_allocates() {
    for capacity in [1usize, 2, 3, 8, 1000] {
        each_family!(8, capacity, data_path, capacity);
    }
}

/// Borrowed and static storage make no allocator call at all, from
/// construction through use, reset, and destruction, with elements that are
/// themselves allocator-free.
#[test]
fn borrowed_and_static_lifecycles_never_call_the_allocator() {
    let before = counts();
    {
        let mut slots = [const { Slot::<[u64; 4]>::new() }; 3];
        let mut storage = BorrowedStorage::new(&mut slots).unwrap();
        for session in 0..3u64 {
            let (mut p, mut c) = storage.try_split().unwrap();
            for i in 0..5 {
                let _ = p.try_push([session, i, 0, 0]);
                let _ = c.try_pop();
            }
            p.try_push([9; 4]).unwrap();
            assert!(storage.try_split().is_err());
            drop((p, c));
            storage.reset();
        }
        let (p, c) = storage.try_split().unwrap();
        core::mem::forget((p, c));
        drop(storage);

        let mut storage = StaticStorage::<[u64; 4], 5>::new();
        for _ in 0..2 {
            let (mut p, mut c) = storage.try_split().unwrap();
            assert_eq!(p.push_slice(&[[1; 4]; 7]), 5);
            let mut out = [[0; 4]; 2];
            assert_eq!(c.pop_slice(&mut out), 2);
            drop((c, p));
            storage.reset();
        }
    }
    assert_eq!(
        counts(),
        before,
        "borrowed/static lifecycle called the allocator"
    );
}

/// A shared region in caller memory (here: the stack) makes no allocator
/// call over layout, initialization, attachment, use, and close.
#[cfg(feature = "shared-memory")]
#[test]
fn shared_lifecycle_never_calls_the_allocator() {
    use core::ptr::NonNull;
    use spookycircle::shared_memory as shm;

    #[repr(C, align(64))]
    struct Region([u8; 512]);

    let before = counts();
    {
        let mut region = Region([0; 512]);
        let base = NonNull::from(&mut region).cast::<u8>();
        let layout = shm::layout::<8>(8).unwrap();
        assert!(layout.size() <= 512);
        for generation in 1..=2u64 {
            // SAFETY: `region` is exclusive, writable, 64-byte aligned, and
            // outlives both endpoints; each generation is fresh and the
            // previous one is quiescent.
            unsafe { shm::initialize::<8>(base, 512, 8, generation) }.unwrap();
            // SAFETY: initialized above; the region outlives both endpoints.
            let mut p = unsafe { shm::attach_producer::<8>(base, 512, 8, generation) }.unwrap();
            // SAFETY: as for the producer.
            let mut c = unsafe { shm::attach_consumer::<8>(base, 512, 8, generation) }.unwrap();
            for i in 0..20u64 {
                let _ = p.try_push(i.to_le_bytes());
                let _ = c.try_pop();
            }
            assert_eq!(p.push_slice(&[[1; 8]; 3]), 3);
            let _ = c.peek_mut();
            drop((p, c));
        }
    }
    assert_eq!(counts(), before, "shared lifecycle called the allocator");
}

#[cfg(feature = "alloc")]
#[test]
fn data_path_never_allocates_for_zero_sized_and_boxed_elements() {
    use spookycircle::bounded;

    let (mut p, mut c) = bounded::<()>(3).unwrap();
    let before = counts();
    for _ in 0..10 {
        let _ = p.try_push(());
        let _ = c.try_pop();
    }
    assert_eq!(counts(), before);

    // Elements that own allocations: the queue itself must not add any.
    let (mut p, mut c) = bounded::<Box<u32>>(2).unwrap();
    let a = Box::new(1);
    let b = Box::new(2);
    let d = Box::new(3);
    let before = counts();
    p.try_push(a).unwrap();
    p.try_push(b).unwrap();
    let full = p.try_push(d).unwrap_err();
    let d = full.into_inner();
    let a = c.try_pop().unwrap();
    let _ = c.peek();
    assert_eq!(counts(), before);
    drop((a, d));
}

#[cfg(feature = "alloc")]
#[test]
fn construction_and_destruction_balance() {
    use spookycircle::bounded;

    for capacity in [1usize, 3, 64] {
        let before = counts();
        let (p, c) = bounded::<u64>(capacity).unwrap();
        let (allocs, deallocs) = counts();
        assert_eq!(allocs - before.0, 2, "control block and slot array");
        assert_eq!(deallocs, before.1);
        drop(p);
        assert_eq!(counts().1, before.1, "first drop frees nothing");
        drop(c);
        assert_eq!(counts(), (before.0 + 2, before.1 + 2));
    }
    // Zero-sized elements: only the control block is allocated.
    let before = counts();
    let (p, c) = bounded::<()>(1_000_000).unwrap();
    assert_eq!(counts(), (before.0 + 1, before.1));
    drop((c, p));
    assert_eq!(counts(), (before.0 + 1, before.1 + 1));
}

#[cfg(feature = "alloc")]
#[test]
fn allocation_failure_is_reported_without_leaks() {
    use spookycircle::{CreateError, bounded};

    // First allocation (slot array) fails: nothing to clean up.
    let before = counts();
    fail_allocation_number(1);
    assert_eq!(
        bounded::<u64>(8).unwrap_err(),
        CreateError::AllocationFailed
    );
    assert_eq!(counts(), before);

    // Second allocation (control block) fails: the slot array must be freed.
    let before = counts();
    fail_allocation_number(2);
    assert_eq!(
        bounded::<u64>(8).unwrap_err(),
        CreateError::AllocationFailed
    );
    assert_eq!(counts(), (before.0 + 1, before.1 + 1));

    // Zero-sized elements make one allocation; failing it reports cleanly.
    let before = counts();
    fail_allocation_number(1);
    assert_eq!(bounded::<()>(8).unwrap_err(), CreateError::AllocationFailed);
    assert_eq!(counts(), before);

    // Validation errors come before any allocation.
    let before = counts();
    assert_eq!(bounded::<u64>(0).unwrap_err(), CreateError::ZeroCapacity);
    assert!(matches!(
        bounded::<u64>(usize::MAX / 2).unwrap_err(),
        CreateError::CapacityTooLarge { .. }
    ));
    assert_eq!(counts(), before);

    // Sanity: the injector is disarmed and construction works again.
    let (p, c) = bounded::<u64>(8).unwrap();
    drop((p, c));
}
