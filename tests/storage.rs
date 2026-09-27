//! Borrowed and static storage: capacity sources, construction without `T` code, the one-winner
//! session claim, final cleanup without a storage drop, exclusive reset, the
//! forgotten-endpoint leak policy, and panicking destructors followed by
//! reset.

// Loom-instrumented atomics only work inside a Loom model; see tests/loom.rs.
#![cfg(not(loom))]
// Dropping and forgetting storage (which has no `Drop` impl) is the point of
// several tests here: they show that storage destruction runs no `T` code.
#![allow(clippy::drop_non_drop, clippy::forget_non_drop)]

use std::{
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use spookycircle::{BorrowedStorage, CreateError, MAX_CAPACITY, Slot, SplitError, StaticStorage};

/// A non-`Copy`, non-`Default`, non-`Clone` element with a global drop log,
/// usable from `static` storage (it is `Send`).
#[derive(Debug)]
struct Logged(u64);

static LOG: Mutex<Vec<u64>> = Mutex::new(Vec::new());

impl Drop for Logged {
    fn drop(&mut self) {
        LOG.lock().unwrap().push(self.0);
    }
}

/// Serializes tests that use `LOG`, and returns the ids logged by `f`.
fn logged(f: impl FnOnce()) -> Vec<u64> {
    static SERIAL: Mutex<()> = Mutex::new(());
    let _guard = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    LOG.lock().unwrap().clear();
    f();
    mem::take(&mut *LOG.lock().unwrap())
}

#[test]
fn capacity_comes_from_slice_length_and_n() {
    for len in [1usize, 2, 3, 8, 100] {
        let mut slots: Vec<Slot<u32>> = (0..len).map(|_| Slot::new()).collect();
        let storage = BorrowedStorage::new(&mut slots).unwrap();
        assert_eq!(storage.capacity(), len);
        let (p, c) = storage.try_split().unwrap();
        assert_eq!(p.capacity(), len);
        assert_eq!(c.capacity(), len);
    }
    let storage = StaticStorage::<u32, 5>::new();
    assert_eq!(storage.capacity(), 5);
    let (p, _c) = storage.try_split().unwrap();
    assert_eq!(p.capacity(), 5);
}

#[test]
fn construction_validates_without_allocating_or_touching_t() {
    let mut none: [Slot<Logged>; 0] = [];
    assert_eq!(
        BorrowedStorage::new(&mut none).err(),
        Some(CreateError::ZeroCapacity)
    );
    // `StaticStorage` rejects `N == 0` and `N > MAX_CAPACITY` at compile
    // time: `compile_fail_core/static_zero_capacity.rs` and
    // `static_capacity_too_large.rs`.
    // Zero-sized elements make a capacity above the maximum representable.
    // SAFETY: a slice of a zero-sized type needs no backing memory, only an
    // aligned non-null pointer; `Slot<()>` has no validity requirement.
    let huge: &mut [Slot<()>] = unsafe {
        std::slice::from_raw_parts_mut(
            std::ptr::NonNull::<Slot<()>>::dangling().as_ptr(),
            MAX_CAPACITY + 1,
        )
    };
    assert_eq!(
        BorrowedStorage::new(huge).err(),
        Some(CreateError::CapacityTooLarge {
            requested: MAX_CAPACITY + 1
        })
    );
    let huge = &mut huge[..MAX_CAPACITY];
    assert!(BorrowedStorage::new(huge).is_ok());
    // Miri does per-element work on the returned `[Slot<()>; N]` array even
    // though its elements are zero-sized, so `N == MAX_CAPACITY` never
    // finishes there.
    #[cfg(not(miri))]
    assert_eq!(
        StaticStorage::<(), MAX_CAPACITY>::new().capacity(),
        MAX_CAPACITY
    );
    // Neither construction nor storage drop runs any `T` code.
    let dropped = logged(|| {
        let mut slots = [const { Slot::<Logged>::new() }; 4];
        drop(BorrowedStorage::new(&mut slots).unwrap());
        drop(StaticStorage::<Logged, 4>::new());
    });
    assert!(dropped.is_empty());
}

/// `StaticStorage::new` is usable in constant evaluation.
const _: () = {
    let storage = StaticStorage::<String, 3>::new();
    mem::forget(storage);
};

#[test]
fn split_once_and_no_rearm_after_drop() {
    let mut slots = [const { Slot::<u8>::new() }; 2];
    let storage = BorrowedStorage::new(&mut slots).unwrap();
    let (mut p, mut c) = storage.try_split().unwrap();
    // A failed split has no side effects on the live pair.
    p.try_push(1).unwrap();
    assert_eq!(storage.try_split().err(), Some(SplitError::AlreadySplit));
    assert_eq!(c.len(), 1);
    assert!(c.is_producer_alive() && p.is_consumer_alive());
    assert_eq!(c.try_pop(), Some(1));
    drop((p, c));
    // Both endpoints gone: still claimed until an exclusive reset.
    assert_eq!(storage.try_split().err(), Some(SplitError::AlreadySplit));

    let storage = StaticStorage::<u8, 2>::new();
    drop(storage.try_split().unwrap());
    assert_eq!(storage.try_split().err(), Some(SplitError::AlreadySplit));
}

#[test]
fn split_error_traits() {
    let error = SplitError::AlreadySplit;
    let copy = error;
    assert_eq!(copy, error);
    assert!(!error.to_string().is_empty());
    assert!(format!("{error:?}").contains("AlreadySplit"));
    let _: &dyn std::error::Error = &error;
}

#[test]
fn simultaneous_split_has_exactly_one_winner() {
    static RACED: StaticStorage<u32, 3> = StaticStorage::new();
    let rounds = if cfg!(miri) { 4 } else { 200 };
    for _ in 0..rounds {
        let mut slots = [const { Slot::<u32>::new() }; 3];
        let storage = BorrowedStorage::new(&mut slots).unwrap();
        let barrier = Barrier::new(4);
        let winners = AtomicUsize::new(0);
        thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    barrier.wait();
                    match storage.try_split() {
                        Ok((mut p, mut c)) => {
                            winners.fetch_add(1, Ordering::Relaxed);
                            // The winning pair works normally.
                            p.try_push(7).unwrap();
                            assert_eq!(c.try_pop(), Some(7));
                        }
                        Err(error) => assert_eq!(error, SplitError::AlreadySplit),
                    }
                });
            }
        });
        assert_eq!(winners.load(Ordering::Relaxed), 1);
    }

    let winners = AtomicUsize::new(0);
    thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                if RACED.try_split().is_ok() {
                    winners.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(winners.load(Ordering::Relaxed), 1);
}

/// A real, immutable `static`: split through safe code, endpoints moved to
/// other threads, and the final endpoint cleans up queued values although
/// the static itself is never dropped.
#[test]
fn static_final_endpoint_cleans_up_without_storage_drop() {
    static QUEUE: StaticStorage<Logged, 4> = StaticStorage::new();
    let dropped = logged(|| {
        let (mut p, mut c) = QUEUE.try_split().unwrap();
        let producer = thread::spawn(move || {
            for id in 1..=4 {
                p.try_push(Logged(id)).unwrap();
            }
            p
        });
        let p = producer.join().unwrap();
        let consumer = thread::spawn(move || {
            assert_eq!(c.try_pop().unwrap().0, 1);
            c
        });
        let c = consumer.join().unwrap();
        drop(p);
        assert_eq!(LOG.lock().unwrap().as_slice(), &[1]);
        drop(c);
    });
    assert_eq!(dropped, [1, 2, 3, 4]);
    assert_eq!(QUEUE.try_split().err(), Some(SplitError::AlreadySplit));
}

/// Reset starts a fresh session; FIFO and drop counts stay exact across
/// sessions that each wrap the physical index many times.
#[test]
fn reset_then_new_session_across_wraps() {
    let mut slots = [const { Slot::<Logged>::new() }; 3];
    let mut storage = BorrowedStorage::new(&mut slots).unwrap();
    let mut static_storage = StaticStorage::<Logged, 3>::new();
    let mut next = 0u64;
    let mut expected_drops = Vec::new();
    let dropped = logged(|| {
        for session in 0..5usize {
            for (p, c) in [
                storage.try_split().unwrap(),
                static_storage.try_split().unwrap(),
            ] {
                let (mut p, mut c) = (p, c);
                // Several full physical wraps, then leave `session % 3 + 1`
                // queued at a different physical offset each session.
                for _ in 0..(10 + session) {
                    next += 1;
                    p.try_push(Logged(next)).unwrap();
                    let got = c.try_pop().unwrap();
                    assert_eq!(got.0, next);
                    expected_drops.push(next);
                }
                for _ in 0..=(session % 3) {
                    next += 1;
                    p.try_push(Logged(next)).unwrap();
                    expected_drops.push(next);
                }
                drop((p, c));
            }
            storage.reset();
            static_storage.reset();
        }
    });
    assert_eq!(dropped, expected_drops);
}

fn forget_scenario(forget_producer: bool, forget_consumer: bool) {
    let mut slots = [const { Slot::<Logged>::new() }; 3];
    let mut storage = BorrowedStorage::new(&mut slots).unwrap();
    let dropped = logged(|| {
        let (mut p, mut c) = storage.try_split().unwrap();
        for id in 1..=3 {
            p.try_push(Logged(id)).unwrap();
        }
        drop(c.try_pop().unwrap());
        match (forget_producer, forget_consumer) {
            (true, true) => mem::forget((p, c)),
            (true, false) => {
                mem::forget(p);
                drop(c);
            }
            (false, true) => {
                drop(p);
                mem::forget(c);
            }
            (false, false) => drop((p, c)),
        }
    });
    if forget_producer || forget_consumer {
        // Only the popped value was dropped; ids 2 and 3 are abandoned.
        assert_eq!(dropped, [1]);
    } else {
        assert_eq!(dropped, [1, 2, 3]);
    }
    // Exclusive access is legal again: reset abandons whatever a forgotten
    // share left behind (running no destructor) and a new session works.
    let dropped = logged(|| {
        storage.reset();
        let (mut p, mut c) = storage.try_split().unwrap();
        for id in 10..13 {
            p.try_push(Logged(id)).unwrap();
        }
        assert_eq!(c.try_pop().unwrap().0, 10);
        drop((p, c));
    });
    assert_eq!(dropped, [10, 11, 12]);
    // And dropping the storage after a forget is equally quiet.
    storage.reset();
    let dropped = logged(|| {
        let (mut p, c) = storage.try_split().unwrap();
        p.try_push(Logged(20)).unwrap();
        mem::forget((p, c));
        drop(storage);
    });
    assert!(dropped.is_empty());
}

#[test]
fn forgotten_endpoints_leak_and_reset_recovers() {
    forget_scenario(true, false);
    forget_scenario(false, true);
    forget_scenario(true, true);
    forget_scenario(false, false);
}

#[test]
fn forgotten_static_endpoints_leak() {
    let mut storage = StaticStorage::<Logged, 2>::new();
    let dropped = logged(|| {
        let (mut p, c) = storage.try_split().unwrap();
        p.try_push(Logged(1)).unwrap();
        mem::forget(c);
        drop(p);
    });
    assert!(dropped.is_empty());
    storage.reset();
    let dropped = logged(|| {
        let (mut p, c) = storage.try_split().unwrap();
        p.try_push(Logged(2)).unwrap();
        drop((p, c));
    });
    assert_eq!(dropped, [2]);
}

#[derive(Debug)]
struct Bomb(u64, bool);

impl Drop for Bomb {
    fn drop(&mut self) {
        LOG.lock().unwrap().push(self.0);
        assert!(!self.1, "bomb {} exploded", self.0);
    }
}

/// A destructor that panics during final cleanup: the remaining values are
/// still dropped once, the unwind can be caught, and reset afterwards never
/// revisits the value whose destructor began.
#[test]
fn panicking_destructor_then_caught_unwind_then_reset() {
    let mut slots = [const { Slot::<Bomb>::new() }; 4];
    let mut storage = BorrowedStorage::new(&mut slots).unwrap();
    let dropped = logged(|| {
        let (mut p, c) = storage.try_split().unwrap();
        for (id, explode) in [(1, false), (2, true), (3, false)] {
            p.try_push(Bomb(id, explode)).unwrap();
        }
        drop(p);
        let result = catch_unwind(AssertUnwindSafe(move || drop(c)));
        assert!(result.is_err());
    });
    assert_eq!(dropped, [1, 2, 3]);
    let dropped = logged(|| {
        storage.reset();
        let (mut p, c) = storage.try_split().unwrap();
        p.try_push(Bomb(4, false)).unwrap();
        drop((p, c));
        drop(storage);
    });
    assert_eq!(dropped, [4], "nothing from the first session is revisited");
}

/// Borrowed elements need not be `'static`, and non-`Sync` (but `Send`)
/// element types work across scoped threads.
#[test]
fn borrowed_elements_and_scoped_threads() {
    let text = String::from("borrowed");
    let mut slots = [const { Slot::<&str>::new() }; 2];
    let storage = BorrowedStorage::new(&mut slots).unwrap();
    let (mut p, mut c) = storage.try_split().unwrap();
    p.try_push(&text[..4]).unwrap();
    assert_eq!(c.try_pop(), Some("borr"));

    let mut slots = [const { Slot::<std::cell::Cell<u32>>::new() }; 2];
    let storage = BorrowedStorage::new(&mut slots).unwrap();
    let (mut p, mut c) = storage.try_split().unwrap();
    thread::scope(|s| {
        s.spawn(move || p.try_push(std::cell::Cell::new(5)).unwrap());
    });
    assert_eq!(c.try_pop().map(|cell| cell.get()), Some(5));
    assert!(c.is_drained());
}
