//! Test support shared by the integration suites: one trait view over every
//! endpoint family, and runners that build a queue in each storage mode and
//! hand its endpoints to a generic test body.
//!
//! Families:
//!
//! * `heap`: `bounded` (only with the `alloc` feature);
//! * `borrowed`: `BorrowedStorage` over a `Vec<Slot<T>>`;
//! * `static`: `StaticStorage<T, N>` for a set of common capacities;
//! * `shared`: a `shared_memory` region in ordinary heap memory, used from
//!   one process (only with `shared-memory`, and only for `[u8; R]`).
//!
//! Test bodies are generic functions `fn body<P, C>(p: P, c: C, ...)`
//! bounded by [`ProducerOps`] / [`ConsumerOps`]; the `each_typed!` and
//! `each_family!` macros call them once per family.

#![allow(dead_code, unused_macros)]

pub mod affinity;

use std::{fmt::Debug, mem::MaybeUninit};

use spookycircle::{BorrowedConsumer, BorrowedProducer, Full};
#[cfg(feature = "alloc")]
use spookycircle::{Consumer, Producer};

/// Everything a producer endpoint offers, in every family.
pub trait ProducerOps<T>: Debug {
    fn try_push(&mut self, value: T) -> Result<(), Full<T>>;
    fn push_slice(&mut self, source: &[T]) -> usize
    where
        T: Copy;
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
    fn remaining_capacity(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn is_full(&self) -> bool;
    fn is_consumer_alive(&self) -> bool;
}

/// Everything a consumer endpoint offers, in every family.
pub trait ConsumerOps<T>: Debug {
    fn try_pop(&mut self) -> Option<T>;
    fn try_pop_into<'d>(&mut self, destination: &'d mut MaybeUninit<T>) -> Option<&'d mut T>;
    fn peek(&mut self) -> Option<&T>;
    fn peek_mut(&mut self) -> Option<&mut T>;
    fn pop_slice(&mut self, destination: &mut [T]) -> usize
    where
        T: Copy;
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
    fn remaining_capacity(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn is_full(&self) -> bool;
    fn is_producer_alive(&self) -> bool;
    fn is_drained(&self) -> bool;
}

macro_rules! impl_producer_ops {
    (impl[$($gen:tt)*] $ty:ty, $elem:ty) => {
        impl<$($gen)*> ProducerOps<$elem> for $ty {
            fn try_push(&mut self, value: $elem) -> Result<(), Full<$elem>> {
                <$ty>::try_push(self, value)
            }
            fn push_slice(&mut self, source: &[$elem]) -> usize
            where
                $elem: Copy,
            {
                <$ty>::push_slice(self, source)
            }
            fn capacity(&self) -> usize {
                <$ty>::capacity(self)
            }
            fn len(&self) -> usize {
                <$ty>::len(self)
            }
            fn remaining_capacity(&self) -> usize {
                <$ty>::remaining_capacity(self)
            }
            fn is_empty(&self) -> bool {
                <$ty>::is_empty(self)
            }
            fn is_full(&self) -> bool {
                <$ty>::is_full(self)
            }
            fn is_consumer_alive(&self) -> bool {
                <$ty>::is_consumer_alive(self)
            }
        }
    };
}

macro_rules! impl_consumer_ops {
    (impl[$($gen:tt)*] $ty:ty, $elem:ty) => {
        impl<$($gen)*> ConsumerOps<$elem> for $ty {
            fn try_pop(&mut self) -> Option<$elem> {
                <$ty>::try_pop(self)
            }
            fn try_pop_into<'d>(
                &mut self,
                destination: &'d mut MaybeUninit<$elem>,
            ) -> Option<&'d mut $elem> {
                <$ty>::try_pop_into(self, destination)
            }
            fn peek(&mut self) -> Option<&$elem> {
                <$ty>::peek(self)
            }
            fn peek_mut(&mut self) -> Option<&mut $elem> {
                <$ty>::peek_mut(self)
            }
            fn pop_slice(&mut self, destination: &mut [$elem]) -> usize
            where
                $elem: Copy,
            {
                <$ty>::pop_slice(self, destination)
            }
            fn capacity(&self) -> usize {
                <$ty>::capacity(self)
            }
            fn len(&self) -> usize {
                <$ty>::len(self)
            }
            fn remaining_capacity(&self) -> usize {
                <$ty>::remaining_capacity(self)
            }
            fn is_empty(&self) -> bool {
                <$ty>::is_empty(self)
            }
            fn is_full(&self) -> bool {
                <$ty>::is_full(self)
            }
            fn is_producer_alive(&self) -> bool {
                <$ty>::is_producer_alive(self)
            }
            fn is_drained(&self) -> bool {
                <$ty>::is_drained(self)
            }
        }
    };
}

#[cfg(feature = "alloc")]
impl_producer_ops!(impl[T] Producer<T>, T);
#[cfg(feature = "alloc")]
impl_consumer_ops!(impl[T] Consumer<T>, T);
impl_producer_ops!(impl['q, T] BorrowedProducer<'q, T>, T);
impl_consumer_ops!(impl['q, T] BorrowedConsumer<'q, T>, T);
#[cfg(feature = "shared-memory")]
impl_producer_ops!(
    impl['r, const R: usize] spookycircle::shared_memory::SharedProducer<'r, R>,
    [u8; R]
);
#[cfg(feature = "shared-memory")]
impl_consumer_ops!(
    impl['r, const R: usize] spookycircle::shared_memory::SharedConsumer<'r, R>,
    [u8; R]
);

/// An 8-byte record carrying `i`, usable in every family.
pub type Rec = [u8; 8];

pub fn rec(i: usize) -> Rec {
    (i as u64).to_le_bytes()
}

pub fn unrec(r: Rec) -> usize {
    usize::try_from(u64::from_le_bytes(r)).unwrap()
}

/// Runs `$body(producer, consumer, $args...)` once per typed family (heap,
/// borrowed, static) with element type `$T` and capacity `$cap`. The static
/// family runs only for the capacities listed in `each_static!`.
macro_rules! each_typed {
    ($T:ty, $cap:expr, $body:path $(, $arg:expr)* $(,)?) => {{
        let capacity: usize = $cap;
        #[cfg(feature = "alloc")]
        {
            let (p, c) = spookycircle::bounded::<$T>(capacity).unwrap();
            $body(p, c $(, $arg)*);
        }
        {
            let mut slots: Vec<spookycircle::Slot<$T>> =
                (0..capacity).map(|_| spookycircle::Slot::new()).collect();
            let storage = spookycircle::BorrowedStorage::new(&mut slots).unwrap();
            let (p, c) = storage.try_split().unwrap();
            $body(p, c $(, $arg)*);
        }
        each_static!($T, capacity, $body $(, $arg)*);
    }};
}

/// Runs the static family when `$cap` is one of the capacities listed below
/// (each needs its own `StaticStorage<T, N>` type), and does nothing
/// otherwise. This list is the only record of which capacities have static
/// coverage.
macro_rules! each_static {
    ($T:ty, $cap:expr, $body:path $(, $arg:expr)*) => {
        each_static!(@arms $T, $cap, $body, [$($arg),*], 1, 2, 3, 4, 5, 7, 8, 16, 17, 64, 1000)
    };
    (@arms $T:ty, $cap:expr, $body:path, $args:tt, $($n:literal),*) => {
        match $cap {
            $(
                $n => crate::common::with_static::<$T, $n>(|p, c| call_body!($body, p, c, $args)),
            )*
            _ => {}
        }
    };
}

macro_rules! call_body {
    ($body:path, $p:ident, $c:ident, [$($arg:expr),*]) => {
        $body($p, $c $(, $arg)*)
    };
}

/// Builds boxed static storage with capacity `N` and runs `f` on its pair.
///
/// A separate function per `N`, so that the stack frame of a test only holds
/// the storage it actually uses.
pub fn with_static<T, const N: usize>(
    f: impl for<'a> FnOnce(BorrowedProducer<'a, T>, BorrowedConsumer<'a, T>),
) {
    let storage = Box::new(spookycircle::StaticStorage::<T, N>::new());
    let (p, c) = storage.try_split().unwrap();
    f(p, c);
}

/// Runs `$body` once per family, including the shared-region family, with
/// element type `[u8; $R]`.
macro_rules! each_family {
    ($R:literal, $cap:expr, $body:path $(, $arg:expr)* $(,)?) => {{
        let capacity: usize = $cap;
        each_typed!([u8; $R], capacity, $body $(, $arg)*);
        #[cfg(feature = "shared-memory")]
        {
            let region = crate::common::HeapRegion::new::<$R>(capacity);
            let (p, c) = region.attach::<$R>();
            $body(p, c $(, $arg)*);
        }
    }};
}

#[cfg(feature = "shared-memory")]
pub use shared::*;

#[cfg(feature = "shared-memory")]
mod shared {
    use std::{
        alloc::{Layout, alloc_zeroed, dealloc},
        ptr::NonNull,
        sync::atomic::{AtomicU64, Ordering},
    };

    use spookycircle::shared_memory::{self as shm, SharedConsumer, SharedProducer};

    /// Source of fresh generation numbers.
    static GENERATION: AtomicU64 = AtomicU64::new(1);

    pub fn fresh_generation() -> u64 {
        GENERATION.fetch_add(1, Ordering::Relaxed)
    }

    /// A shared region in ordinary heap memory, initialized for one
    /// generation. Stands in for a real mapping in single-process tests
    /// (works under Miri); it is freed only after every endpoint borrowing
    /// it has dropped.
    pub struct HeapRegion {
        pub base: NonNull<u8>,
        pub layout: Layout,
        pub capacity: usize,
        pub generation: u64,
    }

    impl HeapRegion {
        /// Allocates and initializes a region for `capacity` records of
        /// `R` bytes.
        pub fn new<const R: usize>(capacity: usize) -> Self {
            Self::with_generation::<R>(capacity, fresh_generation())
        }

        /// As [`new`](Self::new), with a chosen generation number.
        pub fn with_generation<const R: usize>(capacity: usize, generation: u64) -> Self {
            let mut region = Self::uninitialized::<R>(capacity);
            region.generation = generation;
            // SAFETY: fresh, exclusive, writable memory of `layout.size()`
            // bytes and a fresh generation.
            unsafe {
                shm::initialize::<R>(
                    region.base,
                    region.layout.size(),
                    capacity,
                    region.generation,
                )
            }
            .unwrap();
            region
        }

        /// Allocates zeroed memory with the layout for `capacity` records
        /// of `R` bytes, without initializing it as a region.
        pub fn uninitialized<const R: usize>(capacity: usize) -> Self {
            let layout = shm::layout::<R>(capacity).unwrap();
            // SAFETY: the layout has nonzero size (at least the header).
            let base = NonNull::new(unsafe { alloc_zeroed(layout) }).unwrap();
            Self {
                base,
                layout,
                capacity,
                generation: fresh_generation(),
            }
        }

        pub fn len(&self) -> usize {
            self.layout.size()
        }

        /// Attaches both roles. The endpoints borrow `self`, so the memory
        /// outlives them.
        pub fn attach<const R: usize>(&self) -> (SharedProducer<'_, R>, SharedConsumer<'_, R>) {
            (self.producer::<R>().unwrap(), self.consumer::<R>().unwrap())
        }

        pub fn producer<const R: usize>(&self) -> Result<SharedProducer<'_, R>, shm::SharedError> {
            // SAFETY: the region was initialized for this generation in this
            // process, stays allocated for the borrow, and is only accessed
            // through the queue.
            unsafe {
                shm::attach_producer::<R>(self.base, self.len(), self.capacity, self.generation)
            }
        }

        pub fn consumer<const R: usize>(&self) -> Result<SharedConsumer<'_, R>, shm::SharedError> {
            // SAFETY: as for `producer`.
            unsafe {
                shm::attach_consumer::<R>(self.base, self.len(), self.capacity, self.generation)
            }
        }

        /// Copies the 256 header bytes out, for inspection.
        pub fn header(&self) -> [u8; 256] {
            let mut out = [0; 256];
            // SAFETY: the header lies within the allocation; no endpoint of
            // this test writes non-atomically to it.
            unsafe { std::ptr::copy_nonoverlapping(self.base.as_ptr(), out.as_mut_ptr(), 256) };
            out
        }
    }

    impl Drop for HeapRegion {
        fn drop(&mut self) {
            // SAFETY: allocated in `uninitialized` with this layout; every
            // borrowing endpoint has dropped (enforced by the borrow).
            unsafe { dealloc(self.base.as_ptr(), self.layout) }
        }
    }

    // SAFETY: the region is plain memory accessed through the queue's
    // atomics; tests share it between scoped threads like a real mapping.
    unsafe impl Sync for HeapRegion {}
    // SAFETY: as above.
    unsafe impl Send for HeapRegion {}
}
