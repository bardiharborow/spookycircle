//! A bounded, wait-free, single-producer/single-consumer (SPSC) FIFO ring
//! buffer for `no_std`, with heap-owned, caller-borrowed, `static`, and
//! shared-memory storage.
//!
//! # What SPSC means here
//!
//! Every queue has exactly one producer endpoint and one consumer endpoint.
//! The producer is the only endpoint that can insert; the consumer is the
//! only endpoint that can remove or borrow. Because each role is embodied by
//! a unique, non-cloneable, `!Sync` value that every state-changing method
//! takes by `&mut self`, safe Rust cannot run two producer operations or two
//! consumer operations at the same time. That uniqueness is what lets each
//! position in the queue have a single writer, and the single-writer
//! property is what makes every operation a fixed sequence of plain
//! acquire/release loads and stores with no retry.
//!
//! Values move through the queue by ownership transfer: `try_push` gives a
//! `T` to the queue, `try_pop` gives it back to the caller. Endpoints can be
//! sent to other threads whenever `T: Send`; `T: Sync` is never required.
//!
//! # Storage modes and Cargo features
//!
//! | Storage mode | Construction and endpoints | Backing owner | Allocator |
//! | --- | --- | --- | --- |
//! | Heap-owned | `bounded::<T>` → `Producer<T>`, `Consumer<T>` | The endpoints; the last one frees it | Required for construction only; `alloc` feature |
//! | Borrowed slots | [`BorrowedStorage::new`] + [`try_split`](BorrowedStorage::try_split) → [`BorrowedProducer`], [`BorrowedConsumer`] | Caller owns the [`Slot`]s; the storage object holds the control state | None |
//! | Inline/static | [`StaticStorage::new`] (`const`) + [`try_split`](StaticStorage::try_split) → the same borrowed endpoints | Caller owns the whole storage object, e.g. a `static` | None |
//! | Shared region | `shared_memory::initialize` + one attach per role → `SharedProducer`, `SharedConsumer` | External mapping owner | None; `shared-memory` feature |
//!
//! | Feature | Default | Enables |
//! | --- | --- | --- |
//! | `alloc` | yes | `bounded`, `Producer`, `Consumer` (and `extern crate alloc`) |
//! | `shared-memory` | no | the `shared_memory` module; does not enable `alloc` or `std` |
//!
//! With default features disabled the crate needs only `core`: the borrowed
//! and static modes, [`Slot`], and the error types are always available, and
//! nothing in them allocates, ever. Every endpoint family has the same
//! methods and guarantees; shared-memory endpoints carry fixed-size byte
//! records (`[u8; RECORD_BYTES]`) instead of arbitrary `T`.
//!
//! # Wait-free scope
//!
//! On a *wait-free certified* target (see the tables below), each of the
//! following completes in a bounded number of its own steps regardless of
//! what the other endpoint is doing, with no lock, syscall, allocation,
//! compare-and-swap loop, retry loop, sleep, yield, or callback, in every
//! storage mode:
//!
//! * `try_push` and every producer query (`capacity`, `len`,
//!   `remaining_capacity`, `is_empty`, `is_full`, `is_consumer_alive`);
//! * `try_pop`, `try_pop_into`, `peek`, `peek_mut`, and every consumer
//!   query, including `is_drained`.
//!
//! `push_slice` and `pop_slice` are bounded by `min(slice length, capacity)`
//! own steps and likewise never wait for the other endpoint.
//!
//! The guarantee applies to **one call**. A loop that retries `try_push`
//! until it stops returning [`Full`], or `try_pop` until it returns `Some`,
//! is a caller policy: it may spin forever if the other endpoint never runs.
//! The library provides no blocking, parking, timeouts, wakers, or async
//! integration; layer those outside if you need them.
//!
//! Outside the guarantee entirely: constructors, `try_split`, `reset`,
//! shared-region layout, initialization, and attachment; dropping an
//! endpoint or storage (the last typed endpoint dropped destroys queued
//! values, and frees memory in the heap-owned mode); dropping a [`Full`] or a
//! returned `T`; any `T::drop`; `Debug`/`Display` formatting; and everything
//! the hardware or operating system does to the calling thread (page faults,
//! preemption, interrupts).
//!
//! # Capacity, allocation, and `no_std`
//!
//! The crate is `#![no_std]`. It requires pointer-width atomics
//! (`cfg(target_has_atomic = "ptr")`) and refuses to compile without them;
//! it never emulates atomics with locks, critical sections, or interrupt
//! masking. The `shared-memory` feature additionally requires a 32- or
//! 64-bit target.
//!
//! Capacity is exact: a queue holds exactly the requested number of
//! elements, for any capacity from 1 to [`MAX_CAPACITY`] (or less if the
//! backing array would not fit in memory), including 1 and values that are
//! not powers of two. There is no hidden sentinel slot. Borrowed storage
//! takes its capacity from the slice length, static storage from `N`.
//!
//! Only `bounded` allocates: at most two allocations through the global
//! allocator (one for a zero-sized `T`, whose slot array needs no memory),
//! using fallible paths, returning [`CreateError::AllocationFailed`] if the
//! allocator reports failure. A global allocator that aborts the process on
//! failure instead of returning null is outside the library's control.
//! After construction no data-path method allocates or deallocates, in any
//! mode. The borrowed, static, and shared modes never call an allocator at
//! all, from construction through destruction, so they link in programs
//! that have no global allocator.
//!
//! # Ownership transfer (`alloc`)
//!
//! ```
//! # #[cfg(feature = "alloc")]
//! # fn main() {
//! use spookycircle::bounded;
//!
//! let (mut producer, mut consumer) = bounded::<u32>(2).unwrap();
//!
//! producer.try_push(10).unwrap();
//! producer.try_push(20).unwrap();
//!
//! // Full: the rejected value comes back untouched.
//! let full = producer.try_push(30).unwrap_err();
//! assert_eq!(full.into_inner(), 30);
//!
//! assert_eq!(consumer.peek(), Some(&10));
//! assert_eq!(consumer.try_pop(), Some(10));
//! assert_eq!(consumer.try_pop(), Some(20));
//! assert_eq!(consumer.try_pop(), None);
//! # }
//! # #[cfg(not(feature = "alloc"))]
//! # fn main() {}
//! ```
//!
//! Non-`Copy` values work the same way; a failed push hands back the exact
//! original:
//!
//! ```
//! # #[cfg(feature = "alloc")]
//! # fn main() {
//! use spookycircle::bounded;
//!
//! let (mut producer, mut consumer) = bounded::<String>(1).unwrap();
//! producer.try_push("first".to_owned()).unwrap();
//! let rejected = producer.try_push("second".to_owned()).unwrap_err();
//! assert_eq!(rejected.get_ref(), "second");
//! let second: String = rejected.into_inner();
//!
//! assert_eq!(consumer.try_pop().as_deref(), Some("first"));
//! producer.try_push(second).unwrap();
//! assert_eq!(consumer.try_pop().as_deref(), Some("second"));
//! # }
//! # #[cfg(not(feature = "alloc"))]
//! # fn main() {}
//! ```
//!
//! # Two threads (`alloc`)
//!
//! ```
//! # #[cfg(feature = "alloc")]
//! # fn main() {
//! use std::thread;
//! use spookycircle::bounded;
//!
//! let (mut producer, mut consumer) = bounded::<u64>(1_024).unwrap();
//!
//! let produce = thread::spawn(move || {
//!     for mut value in 0..100_000 {
//!         loop {
//!             match producer.try_push(value) {
//!                 Ok(()) => break,
//!                 Err(full) => {
//!                     value = full.into_inner();
//!                     thread::yield_now(); // Application policy, not queue behavior.
//!                 }
//!             }
//!         }
//!     }
//!     // `producer` drops here, which the consumer detects via `is_drained`.
//! });
//!
//! let consume = thread::spawn(move || {
//!     let mut expected = 0;
//!     loop {
//!         match consumer.try_pop() {
//!             Some(value) => {
//!                 assert_eq!(value, expected);
//!                 expected += 1;
//!             }
//!             None if consumer.is_drained() => break,
//!             None => thread::yield_now(),
//!         }
//!     }
//!     assert_eq!(expected, 100_000);
//! });
//!
//! produce.join().unwrap();
//! consume.join().unwrap();
//! # }
//! # #[cfg(not(feature = "alloc"))]
//! # fn main() {}
//! ```
//!
//! Each individual `try_push` and `try_pop` above is wait-free. The `loop`s
//! that retry them, and the `yield_now` calls, are not: they are the
//! application's back-pressure policy and can wait indefinitely for the other
//! thread.
//!
//! # Borrowed slots, static storage, and reset (no allocator)
//!
//! [`BorrowedStorage`] borrows a caller-provided `&mut [Slot<T>]`;
//! [`StaticStorage`] holds its slots inline and is `const`-constructible, so
//! it can initialize an ordinary `static`. Both keep their control state
//! inline and hand out one endpoint pair per *session* through
//! `try_split(&self)`. The endpoints borrow the storage, so the borrow
//! checker keeps the storage alive, unmoved, and un-reset while either
//! endpoint (or a `peek` reference) is usable.
//!
//! Dropping both endpoints does not rearm the storage: a second `try_split`
//! returns [`SplitError::AlreadySplit`] until `reset(&mut self)`, whose
//! exclusive borrow proves the old session is over. The final endpoint of a
//! session drops any values still queued, so a `static` queue needs no
//! destructor. If an endpoint is *forgotten* (`mem::forget`), the values it
//! left queued are abandoned without running their destructors when the
//! storage is reset or dropped; this leak is memory-safe and is the only
//! case in which queued values are not dropped.
//!
//! Endpoints borrowed from storage cross threads with scoped threads (or, for
//! a `static`, with any threads):
//!
//! ```
//! use std::thread;
//! use spookycircle::{BorrowedStorage, Slot};
//!
//! let mut slots = [const { Slot::<u32>::new() }; 3];
//! let storage = BorrowedStorage::new(&mut slots).unwrap();
//! let (mut producer, mut consumer) = storage.try_split().unwrap();
//!
//! thread::scope(|scope| {
//!     let worker = scope.spawn(move || {
//!         producer.try_push(123).unwrap();
//!         // Producer closes when this scoped thread exits.
//!     });
//!     worker.join().unwrap();
//!     assert_eq!(consumer.try_pop(), Some(123));
//!     assert!(consumer.is_drained());
//!     drop(consumer);
//! });
//! ```
//!
//! The queue and its control state do not allocate; host thread creation
//! and joining belong to `std` and are outside that guarantee. See
//! [`BorrowedStorage`] and [`StaticStorage`] for reset and `static`
//! examples. Typed storage may also connect interrupt handlers and tasks in
//! one program; the queue never masks interrupts or enters a critical
//! section.
//!
//! # Shared memory (`shared-memory`)
//!
//! The `shared_memory` module places a queue of fixed-size byte records in a
//! caller-owned region of coherent memory, which separate processes (or
//! firmware images) may map at different virtual addresses. The region uses
//! a versioned, pointer-free byte format. The application initializes it
//! once, hands the configuration and a generation number to the
//! participants through its own startup protocol, and each participant
//! attaches its one role with an `unsafe` function whose contract covers
//! the mapping's lifetime. Dropping a shared endpoint only closes its role;
//! the library never unmaps, frees, or reinitializes the region. There is
//! no crash recovery or persistence: a peer that dies leaves its role live
//! forever, and the survivor simply keeps seeing full or empty results. See
//! the module documentation for the complete contract.
//!
//! # Full and empty
//!
//! `try_push` returns `Err(Full(value))` only after a fresh acquire load of
//! the consumer's position proves that all `capacity` slots are occupied;
//! `try_pop`, `try_pop_into`, and the peek methods return `None` only after
//! a fresh acquire load of the producer's position proves that nothing is
//! published. Both endpoints cache the other's position to skip that load
//! when the cache already proves success, and a cache can only ever
//! *under*-estimate the space or data available.
//!
//! The query methods (`len`, `is_empty`, `is_full`, `remaining_capacity`,
//! `is_producer_alive`, `is_consumer_alive`) are snapshots. Their results may
//! be stale as soon as they return and are never reservations; treat the
//! result of `try_push` or `try_pop` as the authority.
//!
//! # Disconnection and draining
//!
//! Dropping one endpoint never invalidates the other:
//!
//! * After the producer is dropped, the consumer can still pop every value
//!   that was published. `is_drained` returns `true` once the producer is
//!   gone *and* the queue is empty; that answer is definitive because the
//!   producer's liveness flag is released only after its final publication,
//!   so no value can arrive afterwards.
//! * After the consumer is dropped, the producer can keep pushing until the
//!   queue is full. `is_consumer_alive` lets it notice, but `try_push`
//!   itself never checks liveness, so the data path stays at one load and
//!   one store.
//!
//! For typed queues, whichever endpoint is dropped last drops every value
//! still queued, once each and in FIFO order, and (heap-owned mode only)
//! frees the backing memory. Shared-region producers that have not attached
//! yet count as alive, so a consumer never mistakes a late producer for a
//! finished one.
//!
//! # Panics and destructors
//!
//! No data-path method has an intentional panic path: arithmetic is
//! explicitly wrapping, indices follow proven invariants, and no formatting,
//! cloning, callbacks, or destructors run inside them. If user code panics
//! while holding a reference from `peek_mut`, the element simply stays
//! queued. If a `T::drop` panics during final cleanup, the remaining queued
//! values are still dropped (a second panic during that unwinding aborts, as
//! usual in Rust) and no value is dropped twice, even if the unwind is
//! caught and the storage is later reset. Forgetting an endpoint with
//! `mem::forget` leaks but is not undefined behaviour.
//!
//! # Safety overview
//!
//! The implementation keeps a `tail` position written only by the producer
//! and a `head` position written only by the consumer, each in its own
//! cache-padded atomic. A slot is written only after an acquire load of
//! `head` (possibly cached) shows the slot was released, and it is then
//! published with a release store of `tail`. A slot is read only after an
//! acquire load of `tail` (possibly cached) shows it was published, and then
//! released with a release store of `head`. Those two release/acquire pairs
//! are the entire synchronization protocol, identical in every storage mode;
//! no `SeqCst` and no read-modify-write operations appear on the data path.
//! Typed-queue lifetime uses one acquire/release `fetch_sub` per endpoint
//! drop, and borrowed/static sessions and shared roles are claimed with one
//! compare-and-swap each. The detailed obligations for every `unsafe` block
//! are documented next to the block in the source (`src/raw/`,
//! `src/storage.rs`, `src/shared_memory/`).
//!
//! # Target support
//!
//! `target_has_atomic = "ptr"` governs whether the crate compiles. Whether
//! the *wait-free* claim holds additionally depends on the target's atomic
//! loads and stores being bounded, non-locking instructions, which is
//! verified per target by code-generation inspection and stress testing.
//! Every storage mode runs the same data path, and the inspection covers the
//! heap-owned, static, and shared-region endpoints.
//!
//! ## Same-address-space queues (heap-owned, borrowed, static)
//!
//! | Target | Class |
//! | --- | --- |
//! | `aarch64-apple-darwin` | Wait-free certified. Codegen inspected (`ldapr`/`stlr`, no calls, fences, read-modify-write, or division on scalar paths), stress-tested, `ThreadSanitizer`, Miri, and Loom clean. |
//! | `x86_64-apple-darwin` | Builds, not certified. Codegen inspected (plain `mov`s, no `lock`/`mfence`/calls on scalar paths) but not stress-tested on hardware for this release. |
//! | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-pc-windows-msvc` | Builds, not certified. In the CI matrix (tests, codegen audit, sanitizers) but not inspected on hardware by the release author. |
//! | Any other target with pointer-width atomics (for example `thumbv7em-none-eabihf`) | Builds, not certified. Memory-safe, no wait-free hardware claim. The allocator-free example links for `thumbv7em-none-eabihf` without a global allocator. |
//!
//! ## Shared-memory regions
//!
//! Shared-memory qualification covers the exact compiler, target, operating
//! system, memory type, and participants; a same-process certification is
//! not evidence for it.
//!
//! | Configuration | Status |
//! | --- | --- |
//! | `aarch64-apple-darwin`, macOS, processes mapping one file with `mmap(MAP_SHARED)` at different addresses, same compiler build | Qualified. Two-process tests (numbered transfer, peer unmap after orderly close, peer killed while live, reinitialization) pass on hardware; codegen inspected. |
//! | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, processes mapping one file with `mmap(MAP_SHARED)` | Builds and tested in CI, not certified. |
//! | Windows; `MAP_PRIVATE` or other copy-on-write mappings; any firmware, multi-core, or heterogeneous-processor configuration; DMA, MMIO, or non-coherent memory | Unavailable: no qualification exists, and the `unsafe` contract must not be relied on there. |
//!
//! `push_slice` and `pop_slice` may compile to `memcpy` calls for their
//! contiguous segments; those are bounded by the transfer count and are not
//! part of the scalar claim.
//!
//! Instrumented builds (sanitizers, Miri, Loom, profilers) and virtual
//! machines may replace atomics with slower or blocking machinery and are
//! never certified.
#![no_std]
// docs.rs only (see `[package.metadata.docs.rs]`): label feature-gated items,
// but not the internal `loom` model-checking cfg.
#![cfg_attr(docsrs, feature(doc_cfg), doc(auto_cfg(hide(loom))))]
// Set by `build.rs` when `core::hint`'s prefetches need the feature gate.
#![cfg_attr(spookycircle_hint_prefetch_unstable, feature(hint_prefetch))]
// Workspace-wide lints live in `Cargo.toml`. These apply to the library
// only; tests and benchmarks may use `std`, `unwrap`, and indexing freely.
#![warn(unreachable_pub)]
// `no_std` hygiene: `alloc` only behind the feature, never `std`.
#![warn(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core
)]
// No data-path method has an intentional panic path.
// Every arithmetic operation is explicitly wrapping or checked, every index
// is proven in bounds, and nothing unwraps or panics.
#![warn(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented,
    clippy::as_conversions
)]
// The in-crate unit tests (`narrow_tests`) are test code, like `tests/`.
#![cfg_attr(
    test,
    allow(
        clippy::arithmetic_side_effects,
        clippy::indexing_slicing,
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::std_instead_of_core,
        clippy::std_instead_of_alloc
    )
)]

#[cfg(not(target_has_atomic = "ptr"))]
compile_error!(
    "spookycircle requires pointer-width atomic loads and stores \
     (cfg(target_has_atomic = \"ptr\")) and does not emulate them"
);

#[cfg(all(
    feature = "shared-memory",
    not(any(target_pointer_width = "32", target_pointer_width = "64"))
))]
compile_error!(
    "the spookycircle `shared-memory` feature requires a 32-bit or 64-bit \
     target (its region format stores pointer-width atomic words)"
);

#[cfg(feature = "alloc")]
extern crate alloc;

#[macro_use]
mod endpoint;
#[macro_use]
mod sync;

mod error;
mod prefetch;
#[cfg(feature = "alloc")]
mod heap;
mod raw;
mod seq;
#[cfg(feature = "shared-memory")]
pub mod shared_memory;
mod storage;

pub use error::{CreateError, Full, SplitError};
#[cfg(feature = "alloc")]
pub use heap::{Consumer, Producer};
pub use raw::Slot;
pub use storage::{BorrowedConsumer, BorrowedProducer, BorrowedStorage, StaticStorage};

/// Largest logical capacity allowed by the sequence-number protocol.
///
/// Positions are `usize` counters that wrap; keeping the occupancy at or
/// below half the counter's range makes the wrapping difference
/// `tail - head` unambiguous.
pub const MAX_CAPACITY: usize = usize::MAX / 2;

/// Creates a heap-owned bounded SPSC ring buffer and returns its two unique
/// endpoints (`alloc` feature).
///
/// The queue holds exactly `capacity` elements; capacity 1 and non-power-of-two
/// capacities are fully supported.
///
/// # Errors
///
/// Checked in this order:
///
/// * [`CreateError::ZeroCapacity`] if `capacity == 0`;
/// * [`CreateError::CapacityTooLarge`] if `capacity > MAX_CAPACITY` or the
///   backing array's size would overflow the target's address space;
/// * [`CreateError::AllocationFailed`] if the global allocator reports that
///   it cannot satisfy either allocation. Anything already allocated is
///   released before returning.
///
/// # Progress
///
/// Not wait-free: this function allocates. It is the only place the crate
/// allocates. The endpoints free the memory when the last of them drops.
///
/// # Example
///
/// ```
/// use spookycircle::{bounded, CreateError};
///
/// let (producer, consumer) = bounded::<u8>(3).unwrap();
/// assert_eq!(producer.capacity(), 3);
/// assert!(consumer.is_empty());
///
/// assert_eq!(bounded::<u8>(0).unwrap_err(), CreateError::ZeroCapacity);
/// ```
#[cfg(feature = "alloc")]
pub fn bounded<T>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), CreateError> {
    raw::create::<T, usize>(capacity).map(heap::pair_from_raw)
}

// The public constant must agree with the sequence implementation that the
// public endpoints use.
const _: () = assert!(MAX_CAPACITY == <usize as seq::Sequence>::MAX_CAPACITY);
