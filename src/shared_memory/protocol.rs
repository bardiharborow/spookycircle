//! The shared-region protocol above the byte format: readiness, the
//! one-shot role claims, and the endpoints' lifecycle.
//!
//! Everything here works on pointers to the region's words, not on byte
//! offsets, so the Loom model tests at the bottom run exactly this code
//! against model atomics.

// Without `test`, a Loom build has no caller for this layer: the byte-format
// layer that attaches through it is not compiled under Loom.
#![cfg_attr(all(loom, not(test)), allow(dead_code))]

use core::{marker::PhantomData, ptr::NonNull};

use super::{SharedConsumer, SharedError, SharedProducer};
use crate::{
    raw::{Lifecycle, Parts, RawConsumer, RawProducer, Slot},
    seq::Sequence,
    sync::{AtomicUsize, Ordering},
};
/// Readiness value published by `initialize`.
pub(super) const READY: usize = 1;
/// Role state: not yet attached. Counts as alive.
const UNCLAIMED: usize = 0;
/// Role state: attached and not yet dropped.
const LIVE: usize = 1;
/// Role state: dropped; never reopens within the generation.
const CLOSED: usize = 2;

/// Which role an attach call claims.
#[derive(Clone, Copy)]
enum Role {
    Producer,
    Consumer,
}

/// Pointers to one generation's live words and slots, derived from this
/// participant's own mapping (or, under Loom, from a model region).
pub(super) struct RegionPtrs<const RECORD_BYTES: usize> {
    pub(super) ready: NonNull<AtomicUsize>,
    pub(super) producer_role: NonNull<AtomicUsize>,
    pub(super) consumer_role: NonNull<AtomicUsize>,
    pub(super) head: NonNull<AtomicUsize>,
    pub(super) tail: NonNull<AtomicUsize>,
    pub(super) slots: NonNull<Slot<[u8; RECORD_BYTES]>>,
    pub(super) capacity: usize,
}

/// Acquire-loads readiness, then claims `role` with one strong
/// compare-exchange. No retry loop; nothing fallible
/// follows a successful claim.
///
/// # Safety
///
/// `ptrs` must point to live, initialized words of one generation.
unsafe fn claim<const RECORD_BYTES: usize>(
    ptrs: &RegionPtrs<RECORD_BYTES>,
    role: Role,
) -> Result<(), SharedError> {
    // SAFETY: caller contract.
    let ready = unsafe { ptrs.ready.as_ref() };
    if ready.load(Ordering::Acquire) != READY {
        return Err(SharedError::InvalidHeader);
    }
    let word = match role {
        Role::Producer => ptrs.producer_role,
        Role::Consumer => ptrs.consumer_role,
    };
    // SAFETY: caller contract.
    let word = unsafe { word.as_ref() };
    match word.compare_exchange(UNCLAIMED, LIVE, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Ok(()),
        Err(LIVE | CLOSED) => Err(SharedError::RoleAlreadyClaimed),
        Err(_) => Err(SharedError::InvalidHeader),
    }
}

impl<const RECORD_BYTES: usize> RegionPtrs<RECORD_BYTES> {
    fn parts(&self) -> Parts<[u8; RECORD_BYTES], usize> {
        Parts {
            head: self.head,
            tail: self.tail,
            slots: self.slots,
            capacity: self.capacity,
        }
    }

    fn life(&self) -> RegionLife {
        RegionLife {
            producer_role: self.producer_role,
            consumer_role: self.consumer_role,
        }
    }

    /// Claims the producer role and returns its endpoint.
    ///
    /// # Safety
    ///
    /// The words and slots must stay valid, and be accessed only according
    /// to the protocol, for all of `'region`.
    pub(super) unsafe fn attach_producer<'region>(
        &self,
    ) -> Result<SharedProducer<'region, RECORD_BYTES>, SharedError> {
        // SAFETY: forwarded caller contract.
        unsafe { claim(self, Role::Producer)? };
        // SAFETY: the claim made this the generation's only producer, whose
        // role never resumes, so the producer has published nothing and
        // starts at position and physical index zero.
        // The opposite position is discovered by the normal acquire refresh.
        let raw = unsafe { RawProducer::new(self.parts(), self.life()) };
        Ok(SharedProducer {
            raw,
            _region: PhantomData,
        })
    }

    /// Claims the consumer role and returns its endpoint.
    ///
    /// # Safety
    ///
    /// As for [`attach_producer`](Self::attach_producer).
    pub(super) unsafe fn attach_consumer<'region>(
        &self,
    ) -> Result<SharedConsumer<'region, RECORD_BYTES>, SharedError> {
        // SAFETY: forwarded caller contract.
        unsafe { claim(self, Role::Consumer)? };
        // SAFETY: as for the producer, for the unique consumer role.
        let raw = unsafe { RawConsumer::new(self.parts(), self.life()) };
        Ok(SharedConsumer {
            raw,
            _region: PhantomData,
        })
    }
}

/// The lifecycle handle of a shared endpoint: the two role words.
///
/// A role counts as alive until it is closed, including before its first
/// attachment. Closing is a single release store after the role's
/// last publication; nothing is reclaimed.
#[derive(Clone, Copy)]
pub(super) struct RegionLife {
    producer_role: NonNull<AtomicUsize>,
    consumer_role: NonNull<AtomicUsize>,
}

impl RegionLife {
    /// Returns a role word.
    ///
    /// Takes `&self` only to bound the returned reference by the handle's
    /// borrow.
    #[inline(always)]
    #[expect(
        clippy::unused_self,
        reason = "`&self` bounds the lifetime of the returned reference"
    )]
    fn role(&self, word: NonNull<AtomicUsize>) -> &AtomicUsize {
        // SAFETY: the attach caller guarantees the mapping, and so both role
        // words, stay valid for the endpoint's `'region`, which covers every
        // use of this handle.
        unsafe { word.as_ref() }
    }
}

// SAFETY: the handle is two pointers into a mapping that the attach contract
// keeps valid for the endpoint's lifetime, accessed only atomically, so it
// may move to another thread with its endpoint. `close_*` release-stores
// `CLOSED`, which the matching acquire query reads; the storage stays valid
// through `close_*` (and beyond) by the same contract.
unsafe impl<T, S: Sequence> Lifecycle<T, S> for RegionLife {
    #[inline(always)]
    fn producer_alive(&self) -> bool {
        self.role(self.producer_role).load(Ordering::Acquire) != CLOSED
    }

    #[inline(always)]
    fn consumer_alive(&self) -> bool {
        self.role(self.consumer_role).load(Ordering::Acquire) != CLOSED
    }

    unsafe fn close_producer(&self, _: NonNull<Slot<T>>, _: usize) {
        // After every preceding tail publication in program order, so a
        // consumer that acquires `CLOSED` sees the final tail.
        self.role(self.producer_role)
            .store(CLOSED, Ordering::Release);
    }

    unsafe fn close_consumer(&self, _: NonNull<Slot<T>>, _: usize, _: usize) {
        // Records are bytes with no destructor, and roles never resume, so
        // there is no physical index to save and nothing to clean up.
        self.role(self.consumer_role)
            .store(CLOSED, Ordering::Release);
    }
}

/// Loom model of the shared-region protocol: startup
/// handoff, role claims, late first attachment, and close/drain ordering.
///
/// The region's words are Loom atomics in an ordinary struct rather than
/// bytes at fixed offsets; the claim, liveness, close, and data-path code
/// under test is exactly the production code. This models the protocol in
/// one address space only; it is not evidence of cross-process atomic
/// interoperability.
#[cfg(all(test, loom))]
mod loom_tests {
    extern crate std;

    use loom::{sync::Arc, thread};
    use std::vec::Vec;

    use super::*;

    const R: usize = 2;

    struct ModelRegion {
        ready: AtomicUsize,
        producer_role: AtomicUsize,
        consumer_role: AtomicUsize,
        head: AtomicUsize,
        tail: AtomicUsize,
        slots: Vec<Slot<[u8; R]>>,
    }

    impl ModelRegion {
        /// `initialize`: runs before any participant thread is spawned,
        /// which stands in for the synchronized startup handoff.
        fn initialized(capacity: usize) -> Arc<Self> {
            let region = Self {
                ready: AtomicUsize::new(0),
                producer_role: AtomicUsize::new(UNCLAIMED),
                consumer_role: AtomicUsize::new(UNCLAIMED),
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
                slots: (0..capacity).map(|_| Slot::new()).collect(),
            };
            region.ready.store(READY, Ordering::Release);
            Arc::new(region)
        }

        fn ptrs(&self) -> RegionPtrs<R> {
            RegionPtrs {
                ready: NonNull::from(&self.ready),
                producer_role: NonNull::from(&self.producer_role),
                consumer_role: NonNull::from(&self.consumer_role),
                head: NonNull::from(&self.head),
                tail: NonNull::from(&self.tail),
                slots: NonNull::new(self.slots.as_ptr().cast_mut()).unwrap(),
                capacity: self.slots.len(),
            }
        }

        fn producer(&self) -> Result<SharedProducer<'_, R>, SharedError> {
            // SAFETY: the model region outlives the returned borrow.
            unsafe { self.ptrs().attach_producer() }
        }

        fn consumer(&self) -> Result<SharedConsumer<'_, R>, SharedError> {
            // SAFETY: as above.
            unsafe { self.ptrs().attach_consumer() }
        }
    }

    fn record(i: u8) -> [u8; R] {
        [i, !i]
    }

    /// Concurrent attach attempts for one role have exactly one winner, and
    /// the loser consumes nothing.
    #[test]
    fn duplicate_attach_has_one_winner() {
        loom::model(|| {
            let region = ModelRegion::initialized(1);
            let other = region.clone();
            let t = thread::spawn(move || other.producer().map(drop).is_ok());
            let here = region.producer().map(drop).is_ok();
            let there = t.join().unwrap();
            assert!(here ^ there);
            assert_eq!(
                region.producer().map(drop),
                Err(SharedError::RoleAlreadyClaimed)
            );
            // The consumer role is untouched.
            assert!(region.consumer().is_ok());
        });
    }

    /// A consumer attaching concurrently with a producer that pushes and
    /// closes sees every record in order, and `is_drained` is definitive:
    /// never before the last record, including when the consumer attaches
    /// before the producer does.
    #[test]
    fn late_attach_and_close_drain() {
        loom::model(|| {
            let region = ModelRegion::initialized(2);
            let other = region.clone();
            let producer = thread::spawn(move || {
                let mut p = other.producer().unwrap();
                p.try_push(record(1)).unwrap();
                p.try_push(record(2)).unwrap();
                drop(p);
            });

            let mut c = region.consumer().unwrap();
            let mut expected = 1;
            loop {
                match c.try_pop() {
                    Some(got) => {
                        assert_eq!(got, record(expected));
                        expected += 1;
                    }
                    None if c.is_drained() => break,
                    None => thread::yield_now(),
                }
            }
            assert_eq!(expected, 3);
            drop(c);
            producer.join().unwrap();
        });
    }

    /// A producer that closes before the consumer first attaches: the late
    /// consumer still receives everything and then sees a definitive drain.
    #[test]
    fn producer_closes_before_consumer_attaches() {
        loom::model(|| {
            let region = ModelRegion::initialized(2);
            let other = region.clone();
            let producer = thread::spawn(move || {
                let mut p = other.producer().unwrap();
                p.try_push(record(7)).unwrap();
                drop(p);
            });
            producer.join().unwrap();

            let mut c = region.consumer().unwrap();
            assert!(!c.is_producer_alive());
            assert!(!c.is_drained());
            assert_eq!(c.try_pop(), Some(record(7)));
            assert!(c.is_drained());
        });
    }

    /// A consumer that attaches before the producer never reports drained
    /// until the producer has attached, published, and closed.
    #[test]
    fn unattached_producer_is_alive() {
        loom::model(|| {
            let region = ModelRegion::initialized(1);
            let mut c = region.consumer().unwrap();
            assert!(c.is_producer_alive());
            assert!(!c.is_drained());
            assert_eq!(c.try_pop(), None);

            let other = region.clone();
            let producer = thread::spawn(move || {
                let mut p = other.producer().unwrap();
                p.try_push(record(9)).unwrap();
            });
            let mut got = None;
            while got.is_none() {
                got = c.try_pop();
                if got.is_none() {
                    assert!(!c.is_drained());
                    thread::yield_now();
                }
            }
            assert_eq!(got, Some(record(9)));
            producer.join().unwrap();
            assert!(c.is_drained());
        });
    }
}
