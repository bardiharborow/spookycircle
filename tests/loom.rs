//! Concurrency model checking with Loom.
//!
//! Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --release --test loom
//! ```
//!
//! Under `--cfg loom` the crate's `sync` module swaps `core` atomics and
//! `UnsafeCell` for Loom's instrumented versions, so every interleaving of the
//! real producer/consumer code is explored and every slot access is checked
//! for causality (a read before publication, or a write before release, is
//! reported as a data race).
//!
//! The heap-owned models need `alloc` (the default). The borrowed and static
//! models leak their storage per execution with `Box::leak` so that Loom's
//! (`'static`) threads can borrow it; the shared-region protocol models live
//! next to the protocol in `src/shared_memory/protocol.rs` and run with `--lib`.

#![cfg(all(loom, feature = "alloc"))]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering as StdOrdering},
};

use loom::{model::Builder, sync::atomic::Ordering, thread};
use spookycircle::{BorrowedStorage, Slot, SplitError, StaticStorage, bounded};

fn model(preemption_bound: Option<usize>, f: impl Fn() + Sync + Send + 'static) {
    let mut builder = Builder::new();
    builder.preemption_bound = preemption_bound;
    builder.check(f);
}

/// FIFO, at-most-once, at-least-once delivery through a scalar pipeline, with
/// the producer retrying `Full` and the consumer retrying `None`.
fn scalar_pipeline(capacity: usize, count: u32) {
    model(Some(3), move || {
        let (mut p, mut c) = bounded::<u32>(capacity).unwrap();
        let producer = thread::spawn(move || {
            for mut value in 0..count {
                loop {
                    match p.try_push(value) {
                        Ok(()) => break,
                        Err(full) => {
                            value = full.into_inner();
                            thread::yield_now();
                        }
                    }
                }
            }
        });
        let mut expected = 0;
        while expected < count {
            match c.try_pop() {
                Some(value) => {
                    assert_eq!(value, expected);
                    expected += 1;
                }
                None => thread::yield_now(),
            }
        }
        producer.join().unwrap();
        assert_eq!(c.try_pop(), None);
        assert!(c.is_drained());
    });
}

#[test]
fn scalar_pipeline_capacity_1() {
    scalar_pipeline(1, 3);
}

#[test]
fn scalar_pipeline_capacity_2() {
    scalar_pipeline(2, 3);
}

#[test]
fn scalar_pipeline_capacity_3() {
    scalar_pipeline(3, 4);
}

#[derive(Debug)]
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, StdOrdering::SeqCst);
    }
}

/// Endpoint destruction races: both endpoints are dropped concurrently with
/// values still queued; exactly the queued values are dropped, once each,
/// and the block is destroyed exactly once.
#[test]
fn concurrent_endpoint_drop_with_queued_values() {
    model(None, || {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut p, mut c) = bounded::<Counted>(2).unwrap();
        p.try_push(Counted(drops.clone())).unwrap();
        p.try_push(Counted(drops.clone())).unwrap();
        let d = drops.clone();
        let t = thread::spawn(move || {
            // Producer pushes into whatever space appears, then leaves.
            let _ = p.try_push(Counted(d));
            drop(p);
        });
        // Consumer takes at most one value, then leaves.
        let taken = c.try_pop();
        drop(c);
        drop(taken);
        t.join().unwrap();
        // Every value ever created has been dropped exactly once: 2 queued,
        // 1 either queued or rejected-and-dropped.
        assert_eq!(drops.load(StdOrdering::SeqCst), 3);
    });
}

/// The `is_drained` protocol: after the producer is gone, "drained" is only
/// reported once every published value has been consumed.
#[test]
fn drained_never_loses_values() {
    model(None, || {
        let (mut p, mut c) = bounded::<u32>(2).unwrap();
        let producer = thread::spawn(move || {
            for mut value in 0..3u32 {
                loop {
                    match p.try_push(value) {
                        Ok(()) => break,
                        Err(full) => {
                            value = full.into_inner();
                            thread::yield_now();
                        }
                    }
                }
            }
            // `p` drops here: liveness flag released after the last publish.
        });
        let mut received = 0;
        loop {
            match c.try_pop() {
                Some(value) => {
                    assert_eq!(value, received);
                    received += 1;
                }
                None if c.is_drained() => break,
                None => thread::yield_now(),
            }
        }
        assert_eq!(
            received, 3,
            "is_drained returned true before all values arrived"
        );
        assert!(c.is_drained());
        assert!(!c.is_producer_alive());
        producer.join().unwrap();
    });
}

/// `try_pop_into`, interleaved with `try_pop` on the same consumer, reads
/// only published slots, delivers in FIFO order, and hands each non-`Copy`
/// value over exactly once: a slot reused by the producer after the release
/// store must not be visible in the destination.
#[test]
fn pop_into_pipeline() {
    model(Some(3), || {
        let (mut p, mut c) = bounded::<Box<u32>>(2).unwrap();
        let producer = thread::spawn(move || {
            for mut value in (0..3).map(Box::new) {
                loop {
                    match p.try_push(value) {
                        Ok(()) => break,
                        Err(full) => {
                            value = full.into_inner();
                            thread::yield_now();
                        }
                    }
                }
            }
        });
        let mut destination = core::mem::MaybeUninit::uninit();
        let mut expected = 0;
        while expected < 3 {
            let got = if expected % 2 == 0 {
                c.try_pop_into(&mut destination).is_some().then(|| {
                    // SAFETY: initialized by the successful call; taken out
                    // once, so each `Box` is freed exactly once.
                    unsafe { destination.assume_init_read() }
                })
            } else {
                c.try_pop()
            };
            match got {
                Some(value) => {
                    assert_eq!(*value, expected);
                    expected += 1;
                }
                None => thread::yield_now(),
            }
        }
        producer.join().unwrap();
        assert!(c.try_pop_into(&mut destination).is_none());
        assert!(c.is_drained());
    });
}

/// `peek_mut` mutation is what `try_pop` later returns, and the producer
/// cannot reuse the peeked slot while the reference is live.
#[test]
fn peek_mut_then_pop_with_concurrent_producer() {
    model(None, || {
        let (mut p, mut c) = bounded::<u32>(2).unwrap();
        let producer = thread::spawn(move || {
            for mut value in 1..=3u32 {
                loop {
                    match p.try_push(value) {
                        Ok(()) => break,
                        Err(full) => {
                            value = full.into_inner();
                            thread::yield_now();
                        }
                    }
                }
            }
        });
        let mut expected = 1;
        while expected <= 3 {
            match c.peek_mut() {
                Some(head) => {
                    assert_eq!(*head, expected);
                    *head += 10;
                    assert_eq!(c.peek(), Some(&(expected + 10)));
                    assert_eq!(c.try_pop(), Some(expected + 10));
                    expected += 1;
                }
                None => thread::yield_now(),
            }
        }
        producer.join().unwrap();
    });
}

/// Bulk transfers publish and release as single batches.
#[test]
fn bulk_batches() {
    model(Some(3), || {
        let (mut p, mut c) = bounded::<u32>(3).unwrap();
        let producer = thread::spawn(move || {
            let data = [1u32, 2, 3, 4];
            let mut sent = 0;
            while sent < data.len() {
                let n = p.push_slice(&data[sent..]);
                if n == 0 {
                    thread::yield_now();
                }
                sent += n;
            }
        });
        let mut received = Vec::new();
        let mut buffer = [0u32; 2];
        while received.len() < 4 {
            let n = c.pop_slice(&mut buffer);
            if n == 0 {
                thread::yield_now();
            }
            received.extend_from_slice(&buffer[..n]);
        }
        assert_eq!(received, [1, 2, 3, 4]);
        producer.join().unwrap();
    });
}

/// Every snapshot query returns a value within bounds while both endpoints
/// run. Distinct calls are distinct snapshots, so they are not compared with
/// each other.
#[test]
fn snapshots_stay_in_bounds() {
    model(Some(2), || {
        let (mut p, mut c) = bounded::<u32>(2).unwrap();
        let producer = thread::spawn(move || {
            for value in 0..3u32 {
                let _ = p.try_push(value);
                assert!(p.len() <= 2);
                assert!(p.remaining_capacity() <= 2);
                let _ = p.is_full();
                let _ = p.is_empty();
            }
        });
        for _ in 0..3 {
            assert!(c.len() <= 2);
            assert!(c.remaining_capacity() <= 2);
            let _ = c.is_full();
            let _ = c.is_empty();
            let _ = c.try_pop();
        }
        producer.join().unwrap();
    });
}

/// Spins (yielding to the Loom scheduler) until `flag` is set, with an
/// acquire load so the setter's earlier queue operations happen-before
/// whatever the caller does next.
fn wait_for(flag: &loom::sync::atomic::AtomicBool) {
    while !flag.load(Ordering::Acquire) {
        thread::yield_now();
    }
}

/// Full and empty results are valid: neither is ever
/// based solely on a stale cache. Each endpoint first primes its cache with
/// a full or empty observation; the other endpoint then makes progress that
/// happens-before the next call through a release/acquire flag, so that call
/// must refresh and observe the progress. An implementation that trusted
/// its cache would return `Full` or `None` here.
#[test]
fn scalar_full_and_empty_results_are_never_stale() {
    use loom::sync::atomic::AtomicBool;
    model(None, || {
        let popped = Arc::new(AtomicBool::new(false));
        let pushed = Arc::new(AtomicBool::new(false));
        let (mut p, mut c) = bounded::<u32>(1).unwrap();
        p.try_push(0).unwrap();
        // Primes the producer's cached head: the queue is full.
        assert_eq!(p.try_push(1).unwrap_err().into_inner(), 1);

        let (popped2, pushed2) = (popped.clone(), pushed.clone());
        let consumer = thread::spawn(move || {
            assert_eq!(c.try_pop(), Some(0));
            // Primes the consumer's cached tail: the queue is empty. The
            // producer cannot push again before `popped` is set.
            assert_eq!(c.try_pop(), None);
            popped2.store(true, Ordering::Release);
            wait_for(&pushed2);
            assert_eq!(c.peek(), Some(&1), "stale empty result from peek");
            assert_eq!(c.try_pop(), Some(1), "stale empty result from try_pop");
        });

        wait_for(&popped);
        assert!(p.try_push(1).is_ok(), "stale full result from try_push");
        pushed.store(true, Ordering::Release);
        consumer.join().unwrap();
    });
}

/// The bulk counterpart of the test above: after observed
/// progress, `push_slice` and `pop_slice` must refresh and transfer.
#[test]
fn bulk_full_and_empty_results_are_never_stale() {
    use loom::sync::atomic::AtomicBool;
    model(None, || {
        let popped = Arc::new(AtomicBool::new(false));
        let pushed = Arc::new(AtomicBool::new(false));
        let (mut p, mut c) = bounded::<u32>(2).unwrap();
        assert_eq!(p.push_slice(&[1, 2]), 2);
        assert_eq!(p.push_slice(&[3]), 0);

        let (popped2, pushed2) = (popped.clone(), pushed.clone());
        let consumer = thread::spawn(move || {
            let mut out = [0u32; 2];
            assert_eq!(c.pop_slice(&mut out), 2);
            assert_eq!(out, [1, 2]);
            assert_eq!(c.pop_slice(&mut out), 0);
            popped2.store(true, Ordering::Release);
            wait_for(&pushed2);
            assert_eq!(c.pop_slice(&mut out), 2, "stale empty result");
            assert_eq!(out, [3, 4]);
        });

        wait_for(&popped);
        assert_eq!(p.push_slice(&[3, 4]), 2, "stale full result");
        pushed.store(true, Ordering::Release);
        consumer.join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// Borrowed and static storage: the one-winner split
// claim, and the final-share transition with publication of the saved
// physical head index.
// ---------------------------------------------------------------------------

/// Leaks borrowed storage over `capacity` slots for one model execution.
fn leaked_borrowed<T>(capacity: usize) -> &'static BorrowedStorage<'static, T> {
    let slots: &'static mut [Slot<T>] = Box::leak(
        (0..capacity)
            .map(|_| Slot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    Box::leak(Box::new(BorrowedStorage::new(slots).unwrap()))
}

/// Concurrent `try_split` calls: exactly one pair is issued, the loser
/// changes nothing, and the winning pair works.
#[test]
fn split_race_has_one_winner() {
    model(None, || {
        let storage = leaked_borrowed::<u32>(2);
        let t = thread::spawn(move || match storage.try_split() {
            Ok((mut p, mut c)) => {
                p.try_push(7).unwrap();
                assert_eq!(c.try_pop(), Some(7));
                true
            }
            Err(e) => {
                assert_eq!(e, SplitError::AlreadySplit);
                false
            }
        });
        let here = match storage.try_split() {
            Ok((mut p, mut c)) => {
                p.try_push(8).unwrap();
                assert_eq!(c.try_pop(), Some(8));
                true
            }
            Err(_) => false,
        };
        let there = t.join().unwrap();
        assert!(here ^ there, "exactly one split must win");
        assert!(storage.try_split().is_err(), "the claim is never rearmed");
    });
}

/// Racing endpoint drops on borrowed storage after the physical head has
/// moved: the final endpoint must acquire the consumer's saved physical
/// index and drop exactly the queued values (slots 1 and 0 here, in that
/// order), once each.
#[test]
fn borrowed_concurrent_drop_uses_saved_head_index() {
    model(None, || {
        let drops = Arc::new(AtomicUsize::new(0));
        let storage = leaked_borrowed::<Counted>(2);
        let (mut p, mut c) = storage.try_split().unwrap();
        p.try_push(Counted(drops.clone())).unwrap();
        p.try_push(Counted(drops.clone())).unwrap();
        drop(c.try_pop().unwrap()); // Physical head is now 1.
        p.try_push(Counted(drops.clone())).unwrap(); // Wraps into slot 0.
        assert_eq!(drops.load(StdOrdering::SeqCst), 1);
        let t = thread::spawn(move || drop(p));
        drop(c);
        t.join().unwrap();
        assert_eq!(drops.load(StdOrdering::SeqCst), 3);
    });
}

/// A static-storage pipeline with the producer closing: FIFO delivery and a
/// definitive drain, exactly as for heap queues.
#[test]
fn static_pipeline_and_drain() {
    model(Some(3), || {
        let storage: &'static StaticStorage<u32, 2> = Box::leak(Box::new(StaticStorage::new()));
        let (mut p, mut c) = storage.try_split().unwrap();
        let producer = thread::spawn(move || {
            for mut value in 0..3u32 {
                loop {
                    match p.try_push(value) {
                        Ok(()) => break,
                        Err(full) => {
                            value = full.into_inner();
                            thread::yield_now();
                        }
                    }
                }
            }
        });
        let mut received = 0;
        loop {
            match c.try_pop() {
                Some(value) => {
                    assert_eq!(value, received);
                    received += 1;
                }
                None if c.is_drained() => break,
                None => thread::yield_now(),
            }
        }
        assert_eq!(received, 3);
        producer.join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// Negative controls: demonstrate that Loom detects the two broken
// protocols this crate must never use, so that a silent regression of the
// orderings in `src/raw/` would be caught by the tests above.
//
// These are standalone litmus tests. `cargo xtask loom-mutants` complements
// them by weakening each essential acquire and release in `src/raw/` itself,
// one at a time, and requiring this suite to fail for every mutant.
// ---------------------------------------------------------------------------

/// Publishing a slot with a `Relaxed` tail store is a data race: the
/// consumer's acquire load does not synchronize with the write.
#[test]
#[should_panic(expected = "Causality violation")]
fn negative_control_relaxed_tail_publication_is_detected() {
    use loom::{
        cell::UnsafeCell,
        sync::{Arc, atomic::AtomicUsize},
    };
    model(None, || {
        let slot = Arc::new(UnsafeCell::new(0u32));
        let tail = Arc::new(AtomicUsize::new(0));
        let (s, t) = (slot.clone(), tail.clone());
        let producer = thread::spawn(move || {
            // SAFETY: a Loom cell; Loom checks each access for causality
            // before performing it, so the race that this negative control
            // provokes is reported, not executed.
            s.with_mut(|p| unsafe { *p = 42 });
            t.store(1, Ordering::Relaxed); // BROKEN: must be Release.
        });
        while tail.load(Ordering::Acquire) == 0 {
            thread::yield_now();
        }
        // SAFETY: as above; this is the access Loom must reject.
        let value = slot.with(|p| unsafe { *p });
        assert_eq!(value, 42);
        producer.join().unwrap();
    });
}

/// Releasing a slot with a `Relaxed` head store is a data race: the
/// producer's acquire load does not order the consumer's read before the
/// producer's overwrite.
#[test]
#[should_panic(expected = "Causality violation")]
fn negative_control_relaxed_head_release_is_detected() {
    use loom::{
        cell::UnsafeCell,
        sync::{Arc, atomic::AtomicUsize},
    };
    model(None, || {
        let slot = Arc::new(UnsafeCell::new(1u32));
        let head = Arc::new(AtomicUsize::new(0));
        let (s, h) = (slot.clone(), head.clone());
        let consumer = thread::spawn(move || {
            // SAFETY: a Loom cell; Loom checks each access for causality
            // before performing it, so the race that this negative control
            // provokes is reported, not executed.
            let value = s.with(|p| unsafe { *p });
            assert_eq!(value, 1);
            h.store(1, Ordering::Relaxed); // BROKEN: must be Release.
        });
        while head.load(Ordering::Acquire) == 0 {
            thread::yield_now();
        }
        // SAFETY: as above; this is the access Loom must reject.
        slot.with_mut(|p| unsafe { *p = 2 });
        consumer.join().unwrap();
    });
}

/// Control for the controls: the same two patterns with the correct
/// orderings pass, so the panics above are due to the weakened ordering.
#[test]
fn negative_control_baseline_passes_with_release_acquire() {
    use loom::{
        cell::UnsafeCell,
        sync::{Arc, atomic::AtomicUsize},
    };
    model(None, || {
        let slot = Arc::new(UnsafeCell::new(0u32));
        let tail = Arc::new(AtomicUsize::new(0));
        let head = Arc::new(AtomicUsize::new(0));
        let (s, t, h) = (slot.clone(), tail.clone(), head.clone());
        let producer = thread::spawn(move || {
            // SAFETY: the consumer reads only after acquiring the release
            // store below.
            s.with_mut(|p| unsafe { *p = 42 });
            t.store(1, Ordering::Release);
            while h.load(Ordering::Acquire) == 0 {
                thread::yield_now();
            }
            // SAFETY: the consumer's read happens-before its release of
            // `head`, which was just acquired.
            s.with_mut(|p| unsafe { *p = 43 });
        });
        while tail.load(Ordering::Acquire) == 0 {
            thread::yield_now();
        }
        // SAFETY: the producer's write happens-before its release of
        // `tail`, which was just acquired; it writes again only after our
        // release of `head`.
        let value = slot.with(|p| unsafe { *p });
        assert_eq!(value, 42);
        head.store(1, Ordering::Release);
        producer.join().unwrap();
    });
}
