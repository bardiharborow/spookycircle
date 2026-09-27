# Rust Wait-Free SPSC Ring Buffer Library

## Normative specification

| Field | Value |
| --- | --- |
| Specification version | 1.0.0-draft.2 |
| Intended crate name | `spookycircle` |
| Intended Rust module name | `spookycircle` |
| Rust edition | 2024 |
| Minimum supported Rust version (MSRV) | 1.88.0 |
| Baseline environment | `#![no_std]` with `core`; optional `alloc` |
| License | MIT OR Apache-2.0 |
| Status | Implementation-ready draft |

## 1. Purpose

This document specifies a bounded, wait-free, single-producer/single-consumer
(SPSC) FIFO ring buffer for Rust. It defines the public API, observable
semantics, progress guarantee, memory-ordering protocol, representation
invariants, destruction behavior, portability boundary, verification plan, and
release acceptance criteria. It includes heap-owned queues, caller-borrowed
slot storage, const-initialized static storage, and an optional protocol for
coherent shared memory, including mappings at different virtual addresses.

Draft 2 promotes allocator-free storage from Section 24 into the normative
contract. Existing `bounded`, `Producer`, and `Consumer` behavior is preserved
with the default `alloc` feature. Shared-memory records are fixed-size byte
arrays; arbitrary Rust values remain supported by the same-address-space
owned, borrowed, and static modes.

The specification is intentionally narrower than a general-purpose channel. It
defines a fast ownership-transfer data structure with non-blocking operations;
it does not define sleeping, waking, timeouts, asynchronous notification,
multi-producer access, or multi-consumer access.

### 1.1 Normative language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
and **MAY** are to be interpreted as described by RFC 2119 and RFC 8174 when
they appear in uppercase.

Code and pseudocode in this document are normative when introduced with
“MUST” or “REQUIRED”; otherwise they illustrate one conforming implementation.
The public API in Section 7 is normative.

### 1.2 Conformance

An implementation conforms to this specification only if:

1. its enabled public API has the signatures, safety contracts, and behavior
   defined here;
2. all safety and state invariants hold for every safe program;
3. every operation identified as wait-free meets Section 14;
4. its atomic protocol is equivalent to Section 11 and establishes the same
   happens-before relationships;
5. it passes the required verification and acceptance work in Sections 19 and
   23; and
6. it labels target support according to Section 6.4 rather than making an
   unqualified platform-independent wait-free claim.

## 2. Executive contract

The library provides one producer role and one consumer role for a
fixed-capacity queue. Same-address-space queues create their unique endpoints
as a pair; shared-memory queues allow each role to attach once per generation.
Endpoints are non-cloneable and may be moved to different threads when their
element type is `Send`.

| Storage mode | Construction and endpoints | Backing owner | Allocator |
| --- | --- | --- | --- |
| Heap-owned | `bounded<T>` → `Producer<T>`, `Consumer<T>` | Endpoint ownership shares | Required for construction; `alloc` feature |
| Borrowed slots | `BorrowedStorage::new` and `try_split` → borrowed endpoints | Caller owns slots; storage object owns control state | None |
| Inline/static | `StaticStorage<T, N>::new` and `try_split` → borrowed endpoints | Caller owns the complete storage object | None |
| Shared region | `shared_memory::initialize` and role attachment → shared endpoints | External region/mapping owner | None; `shared-memory` feature |

In this document, producer/consumer operation requirements apply to every
endpoint family unless a storage-specific exception is explicit. `T` means
`[u8; RECORD_BYTES]` for shared-memory endpoints.

For a successfully constructed queue:

- capacity is fixed and is exactly the value requested by the caller;
- values are returned in FIFO order;
- `try_push` returns the original value when the queue is full;
- `try_pop` returns immediately with `None` when the queue is empty;
- successful scalar operations use no allocation, deallocation, lock, syscall,
  retry loop, compare-and-swap loop, sleep, yield, or callback;
- a full producer and an empty consumer never wait for the other endpoint;
- slots are uninitialized until published; normally terminating typed queues
  drop remaining occupied values exactly once at final endpoint cleanup;
- borrowed/static/shared construction, operation, and library cleanup never
  allocate or deallocate; an application-supplied `T` destructor is outside
  that promise;
- caller-owned storage is never freed, unmapped, or resized by the library;
- counter wraparound is supported and MUST NOT change FIFO or full/empty
  behavior; and
- dropping one endpoint does not invalidate the other endpoint.

The progress guarantee applies to individual queue operations, not to a caller's
loop that repeatedly retries a full push or empty pop. Construction,
destruction, formatting, allocation, and user destructors are outside the
wait-free guarantee.

## 3. Goals

The version 1 implementation MUST optimize for the following, in order:

1. soundness under Rust's aliasing, initialization, ownership, and concurrency
   rules;
2. a defensible wait-free progress claim for scalar data-path operations;
3. predictable latency with bounded work and no hidden allocation;
4. FIFO ownership transfer for arbitrary `T`, requiring only `T: Send` when
   endpoints cross threads;
5. correct operation on weakly ordered architectures through acquire/release
   atomics;
6. a small, stable, idiomatic safe API;
7. efficient steady-state operation through endpoint-local cached counters;
8. usability in allocator-free `no_std` systems with pointer-width atomics;
   and
9. explicitly qualified coherent shared-memory transport without storing
   process-local addresses in the shared region.

## 4. Non-goals

Version 1 does not provide or promise:

- MPSC, SPMC, or MPMC operation;
- cloning either endpoint;
- blocking sends or receives, timeouts, parking, condition variables, wakers,
  futures, or an async runtime integration;
- overwrite-on-full behavior;
- dynamic resizing;
- priority, fairness, or scheduling guarantees;
- zero-copy producer grants or multi-slot reservations;
- iterator-based insertion, callbacks, or operations whose running time is
  controlled by user code;
- DMA/device ownership, MMIO, non-coherent memory requiring cache maintenance,
  or heterogeneous participants with incompatible atomic semantics;
- fallible element moves, automatic serialization, or transporting arbitrary
  Rust object representations between address spaces;
- crash recovery, role replacement within a generation, persistent queues,
  or durability across power loss;
- creating, discovering, mapping, protecting, resizing, or unlinking an OS
  shared-memory object;
- ABI stability or stable sizes for Rust public types; the explicitly
  versioned shared-region format in Section 15.7 is the sole layout exception;
- a hard real-time guarantee for constructors, destructors, error formatting,
  or code outside the library; or
- wait-free behavior on a target whose atomic implementation has not been
  qualified as described in Section 6.4.

These exclusions are scope boundaries, not invitations to expose unsound or
silently blocking fallback behavior.

## 5. Terminology and model

### 5.1 Roles

- **Producer**: the sole endpoint permitted to initialize and publish free
  slots.
- **Consumer**: the sole endpoint permitted to read, move out, and release
  occupied slots.
- **Shared state**: fixed slot storage, atomic positions, immutable metadata,
  endpoint-liveness state, and any lifetime control shared by the endpoints.
- **Storage owner**: the caller or allocation owner responsible for keeping
  the backing memory valid. Shared state does not imply shared heap ownership.
- **Session**: one successful split of borrowed/static storage into a pair.
  A new session requires exclusive `reset` access.
- **Generation**: one initialization of an external shared region, identified
  by an application-supplied `u64`. Each role attaches at most once.
- **Quiescence**: no endpoint operation, reference, attachment attempt, or
  delayed participant can still access the generation being reclaimed.

### 5.2 Positions

- `tail` is the next logical sequence position the producer may publish.
- `head` is the next logical sequence position the consumer may remove.
- `capacity`, written `C`, is the number of logical slots.
- `occupancy = tail.wrapping_sub(head)` in `usize` arithmetic.
- A queue is empty when `occupancy == 0`.
- A queue is full when `occupancy == C`.

Positions are monotonically increasing modulo `usize::MAX + 1`; wrapping is
intentional. “Monotonic” in this document refers to their logical sequence,
not ordinary integer comparison after wraparound.

### 5.3 Progress terms

- **Wait-free operation**: completes in a bounded number of its own steps,
  independently of whether the other endpoint runs.
- **Lock-free operation**: system-wide progress is guaranteed, but a particular
  operation may starve. Lock-free alone is insufficient for this specification.
- **Non-blocking API**: returns a full/empty result instead of parking the
  calling thread. This is necessary but not sufficient for wait-freedom.

### 5.4 Snapshot

A snapshot query returns a value that was true at a linearization point during
the call. The other endpoint may change the state immediately after that point.
A snapshot is not a reservation.

## 6. Package and platform requirements

### 6.1 Crate configuration

The crate MUST:

- use Rust edition 2024 and declare `rust-version = "1.88"`;
- compile as `#![no_std]` with `core` alone when default features are disabled;
- compile `extern crate alloc` and all heap ownership code only with `alloc`;
- have no required runtime dependency outside `core` and optional `alloc`;
- use `#![deny(unsafe_op_in_unsafe_fn)]`;
- document every public item, including each unsafe function's obligations;
  and
- ship both MIT and Apache-2.0 license texts.

The Cargo features MUST be:

```toml
[features]
default = ["alloc"]
alloc = []
shared-memory = []
```

`shared-memory` MUST NOT enable `alloc` or `std`. No feature may silently add
blocking, interrupt masking, or an allocator to a storage mode. The existing
heap-owned API remains available in a default build. Tests and benchmarks MAY
use development-only dependencies such as Loom and property-test generators.
All normative APIs and examples MUST be implementable on the declared MSRV;
newer or nightly-only atomic, allocator, or array initialization APIs are not
permitted requirements.

### 6.2 Allocation contract

Only `bounded` requires a global allocator. It MUST use fallible allocation
paths and return `CreateError::AllocationFailed` when allocation reports
failure, releasing every partial allocation before returning. An allocator
that aborts rather than reports failure is outside the library's control and
MUST be documented.

Borrowed, static, and shared-region paths MUST NOT invoke allocation or
deallocation during construction, split/attachment, data access, reset, or
library-owned destruction. They MUST NOT hide an `Arc`, `Box`, `Vec`, lazy
allocation, or heap-allocated control block. Caller-provided values and user
destructors may independently use an allocator; that is application behavior.
The library MUST link in an allocator-free `no_std` program using only these
modes and allocator-free element types.

All data-path methods in every mode MUST remain allocation-free even when
`alloc` is enabled. Const-initialized storage MUST NOT require a runtime
allocator or initialization of `T`.

### 6.3 Atomic requirement

The crate MUST reject unsupported targets at compile time unless
`cfg(target_has_atomic = "ptr")` is true. Version 1 MUST NOT emulate required
atomics using a mutex, critical section, interrupt masking, operating-system
service, or portable-atomic fallback that weakens the progress guarantee.

Only pointer-width atomic loads and stores are used on the hot path. All
required control atomics, including flags and role claims, MUST be expressible
with `AtomicUsize`; no 8-bit or 64-bit atomic is an additional requirement.
Pointer-width read-modify-write operations MAY be used only for one-time split
or attachment claims and final endpoint lifetime management. These lifecycle
operations are outside the wait-free claim.

The `shared-memory` feature additionally requires a 32-bit or 64-bit pointer
width. It MUST reject other widths at compile time. Same-address-space
borrowed/static queues retain the baseline `target_has_atomic = "ptr"` gate;
allocator-free support does not imply support for targets without that gate.

### 6.4 Target qualification

`target_has_atomic = "ptr"` establishes API availability, not by itself a
universal hard real-time proof. Every release MUST publish a support table with
two target classes:

| Class | Meaning |
| --- | --- |
| Wait-free certified | Pointer-width acquire loads and release stores have been verified to use bounded, non-locking target primitives for the supported compiler/target configuration. The crate may advertise the full wait-free claim. |
| Builds, not certified | The crate compiles and remains memory-safe, but the release does not make a wait-free hardware claim for this target. |

Qualification SHOULD include representative code-generation inspection and
stress testing for each architecture family. Instrumented builds, sanitizers,
profilers, virtual machines, and unusual allocator or runtime environments MAY
alter timing and are not automatically certified.

Shared-memory support requires a separate qualification entry for the exact
compiler, target ABI, OS or firmware, memory attributes, and participating
processors. A same-process certification MUST NOT be presented as proof of
cross-process or cross-firmware atomic interoperability. Qualification MUST
establish coherent ordinary slot accesses and address-independent atomic
loads, stores, and lifecycle RMWs on the same physical memory through different
mappings. Unsupported combinations MUST be documented as unavailable for the
unsafe shared-memory contract, not merely slower.

## 7. Normative public API

The version 1 public surface MUST be equivalent to the following declaration
sketch. Method bodies and private fields are intentionally omitted, so the
sketch is not a standalone compilation unit. Private field layout and module
organization are not stable API. The shared-region byte format is separately
specified in Section 15.7.

### 7.1 Common items and optional heap-owned endpoints

```rust,ignore
#![no_std]

use core::{error::Error, fmt, mem::MaybeUninit};

/// Largest logical capacity allowed by the sequence-number protocol.
pub const MAX_CAPACITY: usize = usize::MAX / 2;

/// Creates a heap-owned bounded SPSC ring buffer and its unique endpoints.
#[cfg(feature = "alloc")]
pub fn bounded<T>(
    capacity: usize,
) -> Result<(Producer<T>, Consumer<T>), CreateError>;

/// Failure to construct a ring buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CreateError {
    /// The requested capacity was zero.
    ZeroCapacity,
    /// The requested capacity or backing layout was not representable.
    CapacityTooLarge { requested: usize },
    /// The allocator reported that it could not satisfy the request.
    AllocationFailed,
}

impl fmt::Display for CreateError;
impl Error for CreateError;

/// A value that could not be inserted because the ring was full.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[must_use]
pub struct Full<T> {
    // Private: the value is always retained here.
}

impl<T> Full<T> {
    pub fn get_ref(&self) -> &T;
    pub fn get_mut(&mut self) -> &mut T;
    pub fn into_inner(self) -> T;
}

impl<T> fmt::Display for Full<T>;
impl<T: fmt::Debug> Error for Full<T>;

/// Unique producer endpoint.
#[must_use = "dropping the producer permanently ends production"]
#[cfg(feature = "alloc")]
pub struct Producer<T> {
    // Private.
}

#[cfg(feature = "alloc")]
impl<T> Producer<T> {
    pub fn try_push(&mut self, value: T) -> Result<(), Full<T>>;
    pub fn capacity(&self) -> usize;
    pub fn len(&self) -> usize;
    pub fn remaining_capacity(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn is_full(&self) -> bool;
    pub fn is_consumer_alive(&self) -> bool;
}

#[cfg(feature = "alloc")]
impl<T: Copy> Producer<T> {
    pub fn push_slice(&mut self, source: &[T]) -> usize;
}

#[cfg(feature = "alloc")]
impl<T> fmt::Debug for Producer<T>;

/// Unique consumer endpoint.
#[must_use = "dropping the consumer permanently ends consumption"]
#[cfg(feature = "alloc")]
pub struct Consumer<T> {
    // Private.
}

#[cfg(feature = "alloc")]
impl<T> Consumer<T> {
    pub fn try_pop(&mut self) -> Option<T>;
    pub fn try_pop_into<'d>(&mut self, destination: &'d mut MaybeUninit<T>) -> Option<&'d mut T>;
    pub fn peek(&mut self) -> Option<&T>;
    pub fn peek_mut(&mut self) -> Option<&mut T>;
    pub fn capacity(&self) -> usize;
    pub fn len(&self) -> usize;
    pub fn remaining_capacity(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn is_full(&self) -> bool;
    pub fn is_producer_alive(&self) -> bool;
    pub fn is_drained(&self) -> bool;
}

#[cfg(feature = "alloc")]
impl<T: Copy> Consumer<T> {
    pub fn pop_slice(&mut self, destination: &mut [T]) -> usize;
}

#[cfg(feature = "alloc")]
impl<T> fmt::Debug for Consumer<T>;
```

The crate MUST NOT attempt to provide `From<Full<T>> for T`, which is not a
permitted generic implementation under Rust's coherence rules.
`Full::into_inner` is the normative recovery mechanism.

### 7.2 Safe borrowed and static storage

These items MUST be available with no Cargo features enabled:

```rust,ignore
/// Opaque, initially uninitialized, caller-owned slot.
pub struct Slot<T> { /* private */ }

impl<T> Slot<T> {
    pub const fn new() -> Self;
}

/// Control state borrowing an exclusive slice of caller-owned slots.
pub struct BorrowedStorage<'storage, T> { /* private */ }

impl<'storage, T> BorrowedStorage<'storage, T> {
    pub fn new(slots: &'storage mut [Slot<T>]) -> Result<Self, CreateError>;
    pub fn capacity(&self) -> usize;
    pub fn try_split(
        &self,
    ) -> Result<(BorrowedProducer<'_, T>, BorrowedConsumer<'_, T>), SplitError>;
    pub fn reset(&mut self);
}

/// Inline slots and control state; suitable for stack or static placement.
pub struct StaticStorage<T, const N: usize> { /* private */ }

impl<T, const N: usize> StaticStorage<T, N> {
    pub const fn new() -> Self;
    pub fn capacity(&self) -> usize;
    pub fn try_split(
        &self,
    ) -> Result<(BorrowedProducer<'_, T>, BorrowedConsumer<'_, T>), SplitError>;
    pub fn reset(&mut self);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SplitError {
    /// A pair has already been issued since construction or exclusive reset.
    AlreadySplit,
}

impl fmt::Display for SplitError;
impl Error for SplitError;

#[must_use = "dropping the producer permanently ends this session's production"]
pub struct BorrowedProducer<'queue, T> { /* private */ }

#[must_use = "dropping the consumer permanently ends this session's consumption"]
pub struct BorrowedConsumer<'queue, T> { /* private */ }

impl<T> fmt::Debug for BorrowedProducer<'_, T>;
impl<T> fmt::Debug for BorrowedConsumer<'_, T>;
```

`BorrowedProducer<'queue, T>` MUST expose every producer method in Section
7.1 with identical arguments, return types, and `T: Copy` bounds.
`BorrowedConsumer<'queue, T>` MUST expose every consumer method in Section
7.1 on the same terms. There is no `alloc` gate on these methods. The endpoint
lifetime is tied to the borrow of the storage object, which in turn cannot
outlive its borrowed slots. It MUST NOT be widened to the slot lifetime if
that would let control state move or disappear while endpoints exist.

### 7.3 Optional shared-memory module

The following public module MUST exist only with `shared-memory` enabled:

```rust,ignore
#[cfg(feature = "shared-memory")]
pub mod shared_memory {
    use core::{alloc::Layout, error::Error, fmt, ptr::NonNull};

    pub const FORMAT_VERSION: u32 = 1;

    pub fn layout<const RECORD_BYTES: usize>(
        capacity: usize,
    ) -> Result<Layout, SharedError>;

    /// Initialize an exclusively owned, quiescent region in place.
    /// Safety: the caller must satisfy Sections 15.7 and 15.8.
    pub unsafe fn initialize<const RECORD_BYTES: usize>(
        base: NonNull<u8>,
        region_len: usize,
        capacity: usize,
        generation: u64,
    ) -> Result<(), SharedError>;

    /// Attach the unique producer to an already initialized generation.
    /// Safety: the mapping must obey Section 15.8 for all of 'region.
    pub unsafe fn attach_producer<'region, const RECORD_BYTES: usize>(
        base: NonNull<u8>,
        region_len: usize,
        capacity: usize,
        generation: u64,
    ) -> Result<SharedProducer<'region, RECORD_BYTES>, SharedError>;

    /// Attach the unique consumer on the same terms.
    pub unsafe fn attach_consumer<'region, const RECORD_BYTES: usize>(
        base: NonNull<u8>,
        region_len: usize,
        capacity: usize,
        generation: u64,
    ) -> Result<SharedConsumer<'region, RECORD_BYTES>, SharedError>;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    #[non_exhaustive]
    pub enum SharedError {
        ZeroCapacity,
        CapacityTooLarge { requested: usize },
        LayoutTooLarge,
        RegionTooSmall { required: usize, provided: usize },
        Misaligned { required: usize },
        InvalidHeader,
        IncompatibleFormat,
        ConfigurationMismatch,
        GenerationMismatch,
        RoleAlreadyClaimed,
    }

    impl fmt::Display for SharedError;
    impl Error for SharedError;

    #[must_use = "dropping the producer permanently closes its shared role"]
    pub struct SharedProducer<'region, const RECORD_BYTES: usize> { /* private */ }
    #[must_use = "dropping the consumer permanently closes its shared role"]
    pub struct SharedConsumer<'region, const RECORD_BYTES: usize> { /* private */ }

    impl<const R: usize> fmt::Debug for SharedProducer<'_, R>;
    impl<const R: usize> fmt::Debug for SharedConsumer<'_, R>;
}
```

Shared producer/consumer types MUST expose the Section 7.1 methods with `T`
replaced by `[u8; RECORD_BYTES]`. For example, `try_push` accepts that array
and returns `Result<(), Full<[u8; RECORD_BYTES]>>`; `push_slice` accepts
`&[[u8; RECORD_BYTES]]`; `pop_slice` accepts `&mut [[u8; RECORD_BYTES]]`.
`peek` and `peek_mut` return references to one array bounded by the mutable
consumer borrow. All data-path methods are safe once attachment succeeds.

Only the three shared-memory raw-region functions above are public unsafe
entry points. No public unsafe trait or public field is permitted. Root items
in Sections 7.1 and 7.2 MUST be re-exported from the crate root; Section 7.3
items live in `shared_memory`. Feature gating MUST apply to all associated
implementations and documentation examples as well as type declarations.

## 8. Construction and capacity

### 8.1 Validation

`bounded::<T>(capacity)` MUST validate in this order:

1. `capacity == 0` returns `CreateError::ZeroCapacity`.
2. `capacity > MAX_CAPACITY` returns
   `CreateError::CapacityTooLarge { requested: capacity }`.
3. Any overflow or target layout limitation for the required backing storage
   returns the same `CapacityTooLarge` variant.
4. A reported allocation failure returns `CreateError::AllocationFailed`.

Validation and allocation MUST NOT construct either endpoint until all shared
state is in a state that can be safely destroyed.

`BorrowedStorage::new` takes capacity from `slots.len()`, applies steps 1–3,
never allocates, and MUST NOT return `AllocationFailed`. On a returned error,
caller-owned slots remain logically uninitialized and unchanged.

`StaticStorage<T, N>::new` takes capacity from `N` and cannot fail at run
time: steps 1–2 for `N` MUST be enforced at compile time, as a
post-monomorphization error in the constructor's instantiation for that `N`
(an unrepresentably large Rust type is rejected by the compiler before that
check). The check needs no `T: Copy`, `T: Default`, or constructed `T`. A
program that evaluates the constructor in a `static` initializer is rejected
by `cargo check`; one that only calls it at run time is rejected by a full
build, since `cargo check` performs no monomorphization.

Shared-region validation is specified in Sections 15.7 and 15.8.

### 8.2 Exact capacity

Subject to sufficient backing storage and a representable layout, all
capacities from 1 through the applicable maximum MUST be accepted, including
capacity 1 and capacities that are not powers of two. The logical capacity MUST equal the
requested value; an implementation MUST NOT reserve a hidden sentinel slot.

### 8.3 Initial state

On successful `bounded` construction or borrowed/static `try_split`:

- `head == 0` and `tail == 0`;
- occupancy is zero;
- no element slot contains a logically initialized `T`;
- both liveness flags are true;
- each endpoint owns one of exactly two session/lifetime shares; and
- producer and consumer local sequence positions and physical indices are zero.

Borrowed/static construction creates an unclaimed storage object with no live
endpoints or initialized values. `try_split` claims it once and creates the
pair. The claim remains set after both endpoints drop; a second split returns
`SplitError::AlreadySplit` until exclusive `reset`. Reset and forget behavior
are specified in Sections 15.5 and 17.4. Shared-region initialization creates
two unclaimed roles; attachment follows Section 15.8.

### 8.4 Zero-sized types

Zero-sized `T` values, including zero-sized types with a destructor, MUST be
supported. The implementation MAY special-case them. It MUST preserve one
logical ownership instance per successful push, produce one instance per pop,
and run the destructor once for every instance left occupied at final cleanup.

An implementation MUST NOT rely on distinct addresses for adjacent zero-sized
array elements. If it fabricates a reference or owned instance for an occupied
zero-sized slot, the safety argument MUST use an aligned, non-null pointer and
the fact that a nonempty queue proves an inhabited `T` was previously accepted.
Zero-sized accesses need no provenance, so a dangling pointer such as
`NonNull::dangling()` is sufficient; an integer-to-pointer cast is not
needed and should not be used. Shared `[u8; 0]` records are also supported: they
consume logical capacity but require no payload bytes.

## 9. Producer semantics

### 9.1 `try_push`

`try_push(value)` MUST behave as follows:

- If occupancy is less than capacity, it initializes the slot at the current
  tail, publishes exactly one new element, and returns `Ok(())`.
- If occupancy equals capacity, it performs a fresh acquire observation of the
  consumer's head before returning `Err(Full { value })`.
- The error MUST retain the exact input value; the library MUST neither clone
  nor drop it.
- It MUST NOT report full solely from a stale cached head.
- It MUST NOT inspect or depend on the consumer-liveness flag. A producer may
  continue to fill available slots after the consumer has been dropped.

FIFO position is determined by successful publication order. Because there is
only one producer, successful calls on that endpoint have an unambiguous total
order.

### 9.2 `push_slice`

`push_slice(source)` is available only for `T: Copy` and MUST:

- copy the longest prefix of `source` that fits at a snapshot during the call;
- return the number of copied elements;
- return zero without publication when `source` is empty or the queue is full;
- preserve source order;
- handle physical wraparound;
- publish the copied prefix with one release store; and
- make no prefix element visible before the batch publication.

It MAY use a cached head when that cache proves that the entire requested slice
fits. If cached free space is insufficient, it MUST refresh head once before
choosing the transfer count. Its loop bound MUST be no greater than
`min(source.len(), capacity)`.

### 9.3 Producer snapshots

`Producer::len` MUST acquire-load the shared head once and compute
`tail.wrapping_sub(head)`. `remaining_capacity` MUST return `capacity - len` for
the same kind of snapshot. `is_empty` and `is_full` are equivalent to comparing
a valid snapshot length with zero and capacity, respectively.

These queries MUST NOT reserve space. A subsequent `try_push` may observe a
different state.

### 9.4 Consumer liveness

`is_consumer_alive` MUST acquire-load a monotonic liveness flag. Once it returns
false for an endpoint, it MUST never later return true. A true result is only a
snapshot: the consumer may be dropped immediately afterward. The method is not
an acknowledgment that any present or future value will be consumed.

## 10. Consumer semantics

### 10.1 `try_pop`

`try_pop()` MUST behave as follows:

- If occupancy is greater than zero, it moves out the element at the current
  head, releases that slot for reuse, and returns `Some(value)`.
- If the queue appears empty from cached state, it performs a fresh acquire
  observation of the producer's tail before returning `None`.
- It MUST NOT report empty solely from a stale cached tail.
- It MUST NOT clone or drop the returned value.
- It MUST NOT interpret producer disconnection as a synthetic element or error.

### 10.2 `peek` and `peek_mut`

Both peek methods MUST use the same empty confirmation rule as `try_pop` and
MUST NOT advance head.

`peek` returns a shared reference to the current head element. `peek_mut`
returns an exclusive reference to that element. The reference lifetime MUST be
bounded by the mutable borrow of the consumer. While either reference is live,
safe Rust must prevent another operation that could advance this consumer.

The producer cannot overwrite the referenced slot because head has not been
published as advanced. A mutation through `peek_mut` becomes the value later
observed by `try_pop` or final destruction. Peek methods are scalar wait-free
operations.

### 10.3 `pop_slice`

`pop_slice(destination)` is available only for `T: Copy` and MUST:

- move the longest available FIFO prefix, up to `destination.len()`, into the
  corresponding prefix of `destination`;
- return the number of destination elements overwritten;
- leave the destination suffix unchanged;
- return zero without publishing head when the destination is empty or the
  queue is empty;
- handle physical wraparound;
- publish all released slots with one release store; and
- expose no partially released batch to the producer.

Because `T: Copy` excludes user-defined destruction, overwriting the destination
does not invoke a destructor. If cached availability is smaller than the
requested length, the method MUST refresh tail once before selecting its final
transfer count. Its loop bound MUST be no greater than
`min(destination.len(), capacity)`.

### 10.4 Consumer snapshots

`Consumer::len` MUST acquire-load the shared tail once and compute
`tail.wrapping_sub(head)`. The other capacity queries have the same meanings as
their producer counterparts and are snapshots rather than reservations.

### 10.5 Producer liveness and drained state

`is_producer_alive` has the same monotonic snapshot semantics as
`is_consumer_alive`.

`is_drained` MUST return true if and only if it observes that:

1. the producer-liveness flag is false using an acquire load; and
2. a fresh tail load performed after that observation equals the consumer's
   current head.

The producer MUST publish its liveness transition only after every preceding
tail publication. Therefore, observing producer-dead and then observing an
empty queue is definitive: no future push can occur. Once `is_drained` returns
true, it remains true for that consumer.

A typical externally waiting consumer may use the following pattern (with
application-defined `process`):

```rust,ignore
loop {
    match consumer.try_pop() {
        Some(value) => process(value),
        None if consumer.is_drained() => break,
        None => core::hint::spin_loop(), // Policy outside this library.
    }
}
```

The loop as a whole is not wait-free, and `process` is outside all library
guarantees.

### 10.6 `try_pop_into`

`try_pop_into(destination)` MUST behave exactly as `try_pop` (Section 10.1),
except for where the removed element goes:

- on success, it moves the head element into `destination`, releases the
  slot, and returns `Some` holding a reference to the element in
  `destination`;
- the move into `destination` MUST happen before the release store that
  publishes the new head, so no copy of the element is made after the slot
  may be reused;
- on `None`, `destination` MUST be left unmodified;
- it MUST NOT read, drop, or otherwise interpret the previous contents of
  `destination`, and MUST NOT run any `T` code; and
- ownership of the element passes to the caller through `destination`.
  Because `destination` is a `MaybeUninit<T>`, dropping the element is the
  caller's responsibility.

`try_pop_into` is a scalar wait-free operation with the same Section 20.1
budget as `try_pop`. It exists so that an element too large to return in
registers can be moved from its slot to caller memory with a single copy: a
by-value `try_pop` must hold the element across the release store and store
it to the caller's place afterwards, and an optimizing compiler may not move
that store above the release.

## 11. Required state machine and atomic protocol

### 11.1 Logical slot state

At every instant, each logical slot is exactly one of:

- **free/uninitialized**: the producer may initialize it after proving space;
  or
- **occupied/initialized**: the consumer may read or move it after observing
  publication.

The half-open logical interval `[head, tail)` contains all and only occupied
slots, interpreted with wrapping sequence arithmetic.

### 11.2 Required atomic ownership

The shared positions MUST have single writers:

| Atomic | Sole writer | Observer | Publication ordering |
| --- | --- | --- | --- |
| `tail` | Producer | Consumer | Producer stores with `Release`; consumer loads with `Acquire` when refreshing or querying. |
| `head` | Consumer | Producer | Consumer stores with `Release`; producer loads with `Acquire` when refreshing or querying. |
| `producer_alive` | Producer drop path | Consumer | Drop stores false with `Release`; observation loads with `Acquire`. |
| `consumer_alive` | Consumer drop path | Producer | Drop stores false with `Release`; observation loads with `Acquire`. |

This table describes established endpoint operation. For shared regions,
`producer_alive` and `consumer_alive` mean the role-state predicates in
Section 15.9; attachment changes an unclaimed role to live without changing
that predicate from true. Initialization, role claims, and readiness are
lifecycle transitions specified separately in Section 15.8.

The scalar hot path MUST NOT use `SeqCst`, compare-and-swap, `fetch_add`,
`fetch_sub`, or another read-modify-write operation for queue positions.

### 11.3 Reference scalar producer algorithm

The following pseudocode is normative in ordering and state transition:

```text
try_push(value):
    t = producer.local_tail
    used_from_cache = t wrapping_sub producer.cached_head

    if used_from_cache == capacity:
        h = shared.head.load(Acquire)
        producer.cached_head = h
        if t wrapping_sub h == capacity:
            return Full(value)

    write value into the free slot at producer.physical_tail
    next_t = t wrapping_add 1
    next_index = wrap_physical_index(producer.physical_tail, capacity)
    producer.local_tail = next_t
    producer.physical_tail = next_index
    shared.tail.store(next_t, Release)
    return success
```

No potentially panicking operation may occur after the slot write and before
the tail publication. Bounds and invariant assertions that can panic MUST be
performed before mutation or limited to test-only checked models.

An implementation MAY store the cached head in the equivalent form
`full_at = cached_head wrapping_add capacity`, the first tail value at which
the cache can no longer prove a free slot. Under wrapping arithmetic
`t wrapping_sub cached_head == capacity` holds exactly when `t == full_at`, so
the fast-path test becomes one equality comparison, mirroring the consumer's
`h == cached_tail` test, and the bulk free-space computation becomes
`full_at wrapping_sub t`. This is a representation choice only: the state,
the refresh rule, and the linearization points are unchanged. Implementations
SHOULD also copy the immutable `capacity` and slot-array base pointer into
each endpoint at construction so that the scalar path never dereferences the
shared control block except for the position atomics (the block contains
atomics, so a compiler cannot assume its other fields are unchanged across a
slot write, which defeats hoisting in the bulk copy loops).

### 11.4 Reference scalar consumer algorithm

```text
try_pop():
    h = consumer.local_head

    if h == consumer.cached_tail:
        t = shared.tail.load(Acquire)
        consumer.cached_tail = t
        if h == t:
            return None

    value = move value out of occupied slot at consumer.physical_head
    next_h = h wrapping_add 1
    next_index = wrap_physical_index(consumer.physical_head, capacity)
    consumer.local_head = next_h
    consumer.physical_head = next_index
    shared.head.store(next_h, Release)
    return Some(value)
```

No potentially panicking operation may occur after moving the value out and
before head publication.

### 11.5 Physical index advancement

The data-path implementation SHOULD keep an endpoint-local physical index and
advance it with a compare-to-capacity branch, avoiding integer division in the
scalar hot path:

```text
next_index = if index + 1 == capacity { 0 } else { index + 1 }
```

The implementation MAY use an equivalent mask for power-of-two capacities but
MUST preserve support for all valid capacities. Every local physical index MUST
equal the unbounded logical operation count modulo capacity. This is NOT
necessarily `wrapped_usize_position % capacity`: after `usize` wrap, those
expressions differ for capacities that do not divide `usize::MAX + 1`.
Local physical indices MUST therefore advance independently of wrapped
sequence counters. Final cleanup MUST preserve an actual physical head index
as specified in Sections 15.2 and 17.2; it MUST NOT reconstruct one using
`head % capacity`. Shared roles never resume mid-generation, so each role's
first attachment starts its own physical index at zero.

### 11.6 Cached position rules

A producer's cached head can only underestimate current free space. A
consumer's cached tail can only underestimate current availability. Therefore:

- a cache MAY prove that an operation can succeed;
- a cache MUST NOT be the sole basis for returning full or empty; and
- every full or empty scalar result requires at most one fresh acquire load.

The initial zero-valued caches are justified by exclusive construction before
the endpoints become reachable. Every later cache update MUST come from the
corresponding acquire load.

## 12. Representation and safety invariants

A conforming implementation MUST maintain all of the following:

1. **Bounded occupancy:** `0 <= tail.wrapping_sub(head) <= capacity` in the
   logical sequence interpretation.
2. **Unambiguous distance:** `capacity <= usize::MAX / 2`.
3. **Single publication:** the producer is the only writer of tail; the
   consumer is the only writer of head.
4. **Initialization:** a slot is read as `T` only after an acquire observation
   of a tail publication that covers it.
5. **Reuse:** a slot is overwritten only after an acquire observation of a head
   publication that releases it.
6. **Exclusive role access:** safe code cannot execute two producer operations
   concurrently or two consumer operations concurrently for the same queue.
7. **Reference validity:** an element reference returned by peek cannot coexist
   with a consumer operation that moves or releases that element.
8. **Lifetime:** backing storage remains valid and unmoved while either
   endpoint or a derived reference can be used. Borrowed lifetimes enforce this
   locally; raw shared mappings require the Section 15.8 safety contract.
9. **Exact ownership:** every successful scalar push transfers one `T` into the
   queue; every successful scalar pop transfers one `T` out; a failed push
   transfers none.
10. **Drop accounting:** every `T` still in the occupied interval at final
    destruction is dropped once, and no free slot is treated as initialized.
11. **Wrap correctness:** ordinary relational comparison of wrapped sequence
    numbers is never used to decide occupancy.
12. **Pointer provenance:** element pointers are derived from their backing
    storage or local mapping and accessed through the `UnsafeCell` API,
    except a documented and separately proven zero-sized special case.
13. **Reinitialization exclusion:** storage is never reset while a former
    endpoint or derived reference can access it. A claim is never rearmed merely
    because liveness observations appear false.
14. **Relocation independence:** a shared region contains no absolute pointer,
    reference, allocator object, destructor, or process-local ownership state.

Violating the SPSC role count requires unsafe code or a library bug. Safe API
users have no separate runtime obligation to promise that the endpoints are
unique; the type design enforces it. A raw-region caller additionally establishes
the mapping, generation, and cooperative-peer conditions in Section 15.8.

## 13. Memory-ordering proof obligations

### 13.1 Producer-to-consumer publication

For an element at sequence position `p`:

1. The producer initializes the slot for `p`.
2. The producer release-stores a tail greater than `p`.
3. The consumer acquire-loads that tail value or a later value.
4. The release/acquire pair makes slot initialization happen-before the
   consumer's read.

The consumer may use a cached tail only if that cache originated from an
earlier qualifying acquire load. The cached value represents already-published
elements and remains safe to consume.

### 13.2 Consumer-to-producer reuse

For the same physical slot in a later lap:

1. The consumer completes its read or move from the old element.
2. The consumer release-stores a head that passes the old sequence position.
3. The producer acquire-loads that head value or a later value before reusing
   the slot.
4. The release/acquire pair makes the prior consumer access happen-before the
   producer's overwrite.

This relationship prevents a data race between consumer access and producer
reuse.

### 13.3 Why relaxed publication is forbidden

A relaxed tail store would not publish the preceding non-atomic slot write to
the consumer. A relaxed head store would not order the consumer's slot access
before producer reuse. Both substitutions are nonconforming even if tests pass
on a strongly ordered machine.

### 13.4 Why sequential consistency is unnecessary

Each direction requires a single release/acquire handoff, and each position has
one writer. No invariant requires one global total order across head, tail, and
liveness atomics. `SeqCst` is therefore unnecessary and is forbidden on the
data path to keep the intended cost and proof surface explicit.

### 13.5 Counter wraparound

The occupancy bound and `capacity <= usize::MAX / 2` ensure that the modular
difference representing live occupancy is always the unique short distance.
Cached observations from a single-writer atomic may lag, but that lag can only
cause a conservative full or empty refresh; it cannot grant access to an
unpublished or unreleased slot.

A model using deliberately narrow counters MUST be part of verification so
that wrap behavior is exercised in practical test time.

### 13.6 Definitive drained-state ordering

The producer's final tail publication is sequenced before its release store of
`producer_alive = false`. A consumer acquire load that reads false therefore
synchronizes with the producer's completed publication history. The subsequent
fresh acquire load of tail cannot legally observe a tail value older than that
history. If it equals local head, the occupied interval is definitively empty
and cannot grow again.

## 14. Progress and complexity guarantees

### 14.1 Wait-free operations

Subject to the platform assumptions below, these methods MUST be wait-free:

- `Producer::try_push`;
- all producer snapshot and liveness queries;
- `Consumer::try_pop` and `try_pop_into`;
- `Consumer::peek` and `peek_mut`;
- all consumer snapshot, liveness, and drained queries.

Each scalar method MUST execute a fixed finite control-flow graph with no
retrying loop. `try_push`, `try_pop`, `try_pop_into`, and the peek methods
perform no more than one opposite-position acquire refresh before deciding
their result. A scalar success performs exactly one position release store.

### 14.2 Bounded bulk operations

`push_slice` and `pop_slice` are wait-free with respect to their finite input
length: each completes in `O(min(slice_length, capacity))` own steps and does
not depend on counterpart progress. They may loop only over the selected finite
prefix and physical wrap segments. Because `T: Copy` and a slot is
layout-identical to `T`, each of the at most two physically contiguous
segments MAY be transferred with a single non-overlapping memory copy instead
of a per-element loop; the cost bound is unchanged.

### 14.3 Assumptions

The wait-free claim assumes:

- the target is wait-free certified under Section 6.4;
- atomic pointer-width loads and stores terminate in bounded target steps;
- the calling thread itself is allowed to execute instructions;
- memory holding the endpoint and queue remains valid;
- the program has not violated Rust's safety rules through unrelated unsafe
  code; and
- compiler instrumentation has not replaced operations with blocking runtime
  services.

The algorithm does not require the counterpart thread to run, but no library
can guarantee wall-clock completion while the caller is indefinitely preempted.

### 14.4 Explicit exclusions

The wait-free claim does not include:

- constructors, `try_split`, `reset`, shared-region layout/initialization or
  attachment, or any allocation;
- endpoint or storage `Drop`;
- dropping a `Full<T>` or a returned `T`;
- execution of `T::drop`;
- formatting or trait code invoked by debugging and error reporting;
- page faults, cache misses, interrupts, preemption, firmware, hypervisor, or OS
  latency;
- caller retry, spin, sleep, or backoff loops; or
- caller work performed before, after, or between queue methods.

## 15. Storage and unsafe implementation requirements

### 15.1 Recommended slot representation

For non-zero-sized `T`, every typed storage mode uses fixed slots logically
equivalent to the public opaque type:

```rust
use core::{cell::UnsafeCell, mem::MaybeUninit};

#[repr(transparent)]
pub struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}
```

`Slot<T>` MUST have the size and alignment of `T`; its private representation
MUST permit contiguous bulk copies and const initialization. `Slot::new`
creates an uninitialized slot and never calls `T` code. A slot exposes no safe
element access, cloning, or standalone occupied-value destruction.

`MaybeUninit<T>` prevents the compiler from assuming a free slot contains a
valid `T`. `UnsafeCell` permits controlled mutation through shared backing
ownership. The implementation MUST obtain the inner pointer through
`UnsafeCell::get` or an equivalently valid API.

Wrapping an already-shared `T` in `UnsafeCell` by pointer cast, creating
overlapping `&mut T`, reading through `assume_init` before publication, or
dropping the entire backing slice as `[T]` is forbidden.

### 15.2 Typed queue lifetime and final cleanup

Heap-owned, borrowed, and static typed queues MUST keep a stable backing address
while endpoints are usable. They MAY share one private two-endpoint lifecycle
implementation:

- a successful construction/split creates exactly two ownership shares;
- each endpoint owns one share and cannot clone it;
- the consumer drop path first saves its actual physical head index into
  private control state; this is a lifecycle write, not a per-pop shared write;
- each endpoint release-stores its liveness flag to false, then releases its
  share;
- exactly one final-share transition acquires the other endpoint's completed
  accesses, including the saved physical head index, before cleanup; and
- the final endpoint drops the remaining occupied values exactly once.

An acquire/release RMW (or release RMW with a proven acquire fence on the final
path) MAY implement the share transition. No endpoint may access shared state
after releasing its share, except the unique final owner performing cleanup.
The final owner deallocates only heap-owned storage. Borrowed/static control
and slots remain owned by the caller, and normal final endpoint cleanup MUST
finish even when that owner is a never-dropped `static`.

The saved physical head index and stable modular occupancy determine the
occupied range after sequence wrap. An equivalent proof-backed representation
is permitted, but it MUST preserve the Section 20.1 scalar shared-write budget.
This lifetime protocol MUST prevent double free, use-after-free, duplicate
cleanup, and lost cleanup under simultaneous endpoint drops. Forgotten
endpoints are the explicit leakage exception in Section 17.4.

### 15.3 Unsafe code policy

Every unsafe block MUST have an adjacent `SAFETY:` comment identifying the
specific invariants that justify it. Unsafe code SHOULD be restricted to:

- fallible allocation and deallocation;
- conversion of a stable shared pointer into endpoint-internal access;
- `Send`/`Sync` implementations for private shared state;
- slot write, read, reference, and drop operations; and
- final occupied-range cleanup;
- initialization and validated offset access within an external shared region;
  and
- conditional trait implementations for public borrowed/static storage.

No unsafe block may use “SPSC” as its entire justification. It must identify
which endpoint owns the slot, how publication or release was observed, why the
slot is initialized or free, why aliasing is legal, and why the storage or
mapping is live. Shared-memory safety documentation MUST distinguish checked
layout errors from caller obligations that cannot be checked at runtime.

### 15.4 Cache placement

Head and tail atomics SHOULD reside in distinct cache-padded blocks, each at
least 64-byte aligned and separated from the other endpoint's frequently
written fields. Target-specific padding MAY be larger. Padding is a performance
requirement, not part of the public layout or correctness proof.

Immutable capacity and slot pointers MAY share a read-mostly block. Liveness
and lifetime-control atomics SHOULD not cause extra writes on the normal data
path.

Each endpoint type itself MUST be cache-padded to the same target-specific
size: every push writes the producer's local tail, cached limit, and physical
index, and every pop writes the consumer's counterparts, so two endpoints
stored side by side (for example the tuple returned by `bounded`, driven from
two threads through mutable borrows) would otherwise share a line and contend
on every operation. The slot array SHOULD be allocated with cache-line
alignment so that its first and last lines are never shared with an unrelated
allocation. Borrowed slot slices require `align_of::<T>()`, not an extra
cache-line alignment promise from the caller; additional slot alignment is
only a performance recommendation. Inline control state remains correctly
aligned by its type. The fixed shared-region format overrides configurable
padding and pointer placement as specified below.

### 15.5 Borrowed slot storage and session reuse

`BorrowedStorage::new` borrows the entire supplied `&mut [Slot<T>]` and stores
all control state inline in the returned storage object. Capacity is exactly
the slice length. The caller cannot inspect, replace, move, or lend the slots
again while that borrow is live. Construction treats slots as uninitialized;
it MUST NOT read or drop their bytes. Previously abandoned bytes covered by
Section 17.4 remain leaked, not implicitly recovered.

`try_split(&self)` MUST use one strong atomic compare-exchange to change an
unclaimed session to claimed, with `AcqRel` on success and `Acquire` on failure.
Success issues one endpoint pair; failure returns
`AlreadySplit` without modifying positions, slots, liveness, or lifetime shares.
Concurrent callers MUST have exactly one winner. The winner initializes the
two endpoint shares and local state before returning; no fallible or panicking
work may follow the claim. No claim or owner reference-count operation occurs
on the data path.

The claim MUST remain consumed after either or both endpoints drop. This avoids
reviving a role or invalidating monotonic disconnection observations. A caller
can start a new session only using `reset(&mut self)` followed by `try_split`.
Exclusive access proves that no former endpoint or peek reference is usable.
Reset MUST set positions, saved physical indices, and cached initialization
state to zero and rearm the claim. Normally, final endpoint cleanup has already
destroyed occupied values. If a share was forgotten, reset instead abandons the
old occupied values under Section 17.4; it MUST NOT try to reconstruct a missing
physical index or invoke a destructor on those bytes. Reset is not a concurrent
close/drain operation and is outside the wait-free guarantee.

Storage objects MUST own the relevant `T` lifetime/drop-check obligations even
when slots are represented by `MaybeUninit<T>`. A storage destructor MUST never
free its borrowed slice or repeat final endpoint cleanup. With forgotten
endpoints, destruction may discard the backing bytes without dropping the
abandoned `T` values. This is a leak, not permission for a usable endpoint to
outlive its storage.

An implementation MUST derive slot pointers only after a successful split,
from the live exclusively borrowed slice. It MUST NOT manufacture a whole-slice
`&mut [T]` or return slot access to the owner while endpoints can use it.

### 15.6 Inline and static storage

`StaticStorage<T, N>` owns both `[Slot<T>; N]` and control state inline. Its
const constructor initializes atomics and uninitialized slots without invoking
`T`, allocating, or installing self-referential pointers. The object may move
before split and after all endpoint borrows end. It need not be pinned; Rust
borrowing MUST prevent movement while endpoints or peek references are usable.

Its `try_split`, final endpoint cleanup, one-session claim, and exclusive reset
semantics are identical to borrowed slot storage. The `try_split` receiver is
`&self`, so a properly declared immutable `static` with interior-mutability
control state can be split once through safe Rust. Endpoint lifetimes may be
`'static` only when the storage borrow actually is `'static`. Neither
`static mut`, an unchecked lifetime extension, nor allocator-backed leaking is
required for static use. A `const` value is not a substitute for one unique
`static` object: separate evaluations create separate queues.

An application MAY place the object or borrowed slots in a linker-selected
RAM section. Startup MUST establish the Rust object's initialized control
state and correct alignment before safe access. A `.noinit` section, a
zero-filled byte region, or retained RAM after reset is not automatically a
constructed `StaticStorage`. Independently booted images or processes MUST use
the shared-region protocol rather than treating those bytes as this Rust type.

Typed storage may connect tasks, threads, or interrupt contexts in one Rust
program if each endpoint remains unique and memory satisfies Section 6.4.
Initialization and endpoint placement are application startup responsibilities.
The queue MUST NOT mask interrupts or enter a critical section internally.

### 15.7 Shared-region records and format

The optional shared-memory mode transfers fixed-size `[u8; RECORD_BYTES]`
records in a caller-owned contiguous region. This deliberately gives the
region a pointer-free payload representation without an unsafe element trait.
`Copy` alone is not a cross-address-space representation contract: Rust values
may contain pointers, padding, or process-local meaning. Typed `T`, references,
`Box`, `Vec`, `String`, trait objects, and allocator ownership MUST NOT be
reinterpreted as shared records by the library. Applications encode and decode
their own byte records outside queue operations. Any address encoded by an
application remains opaque bytes and receives no library validity guarantee.

`layout::<R>(C)` MUST compute the following using checked arithmetic:

1. Reject `C == 0` with `ZeroCapacity`.
2. Reject `C > MAX_CAPACITY` with `CapacityTooLarge { requested: C }`.
3. Compute `payload_bytes = C * R` and `used_bytes = 256 + payload_bytes`.
4. Round `used_bytes` up to a multiple of 64 and return a `Layout` with that
   size and alignment 64. Arithmetic overflow, a result greater than
   `isize::MAX`, or another `Layout` limitation returns `LayoutTooLarge`.

`R == 0` is permitted. The 256-byte header still exists, and capacity still
counts records. Region-length padding never creates additional logical slots.
The function inspects no region and never allocates; `core::alloc::Layout`
is a layout description, not an allocator dependency.

Format version 1 has the following exact offsets. The immutable prefix uses
little-endian integers; atomic words use the participant's native byte order.
`W = size_of::<usize>()` is 4 or 8. Qualified implementations MUST have
`size_of::<AtomicUsize>() == W`, with atomic alignment dividing 64 and suitable
for every listed atomic offset.

| Byte offset | Size | Field and required value |
| ---: | ---: | --- |
| 0 | 8 | Magic bytes `WFSPSC01` |
| 8 | 4 | `FORMAT_VERSION`, little-endian `u32`, equal to 1 |
| 12 | 1 | Atomic word width in bytes, `W` |
| 13 | 1 | Native byte order: 1 = little-endian, 2 = big-endian |
| 14 | 1 | `align_of::<AtomicUsize>()` in bytes |
| 15 | 1 | Reserved, zero |
| 16 | 8 | Required rounded region size, little-endian `u64` |
| 24 | 8 | Exact capacity `C`, little-endian `u64` |
| 32 | 8 | Record size `R`, little-endian `u64` |
| 40 | 8 | Slots offset, little-endian `u64`, equal to 256 |
| 48 | 8 | Application generation identifier, little-endian `u64` |
| 56 | 8 | Reserved, zero |
| 64 | W | Atomic readiness: 0 during exclusive construction, 1 when ready |
| 64 + W | W | Atomic producer role state |
| 64 + 2W | W | Atomic consumer role state |
| 128 | W | Atomic `head`, initially 0 |
| 192 | W | Atomic `tail`, initially 0 |
| 256 | C × R | Contiguous uninitialized record slots |

Unused header bytes through offset 255 MUST be initialized to zero and remain
reserved. Slot bytes and trailing region padding need not be initialized until
used. Header metadata is immutable throughout a generation. Role state values
are `UNCLAIMED = 0`, `LIVE = 1`, and `CLOSED = 2`. No shared pointer, slice fat
pointer, vtable, allocator handle, refcount for memory reclamation, or endpoint
cache appears in the region. Each endpoint derives all local addresses from
its own `base` plus validated offsets and keeps its caches and physical index
in process-local state.

This layout is a versioned transport format, independent of private Rust type
layouts. Implementations MUST read/write its specified fields explicitly or
prove an equivalent internal representation; they MUST NOT serialize a Rust
control struct using its default representation. The same format requires
matching word width, native byte order, atomic alignment, and qualified atomic
semantics. The fixed prefix permits rejecting a mismatch before accessing
native atomic words. Identical header bytes alone do not certify a platform.

### 15.8 Shared-region initialization, attachment, and safety

`initialize` is an unsafe in-place constructor, not a mapping or allocation
function. The caller MUST have exclusive control of the entire region, with
no participant accessing a previous or new generation. The supplied pointer
must have valid provenance for a writable contiguous region of `region_len`
bytes. That region must be coherent normal RAM, not MMIO, copy-on-write private
memory, read-only memory, or memory requiring unimplemented cache maintenance.
Alignment and sufficient reported length are checked conditions, not unchecked
caller promises: a valid byte region that is misaligned or too small returns
an error before access. The library cannot validate the actual mapped extent,
memory attributes, or ownership behind the supplied pointer.

Initialization MUST validate layout, base alignment, and sufficient length,
in that order, before changing any byte. Errors return `Misaligned` or
`RegionTooSmall` as appropriate, following `layout` errors. An error leaves
the region unchanged. Success constructs the prefix and all atomic objects
under exclusive access, initializes positions and role states to zero, leaves
slots logically uninitialized, and finally release-stores readiness to 1.
Only the required layout extent may be written; a larger supplied region is
allowed. No fallible step may remain after initialization starts writing.

Before anyone calls an attach function, the application MUST communicate
completion of initialization through an external synchronized startup handoff.
An attacher MUST NOT poll arbitrary, uninitialized, partially initialized, or
concurrently reformatted memory for a readiness or magic value. Release/acquire
publication cannot by itself make reading an unconstructed atomic valid. Each
attachment additionally acquire-loads readiness after validating the immutable
prefix; the startup handoff and this acquire form the initialization proof.

Both attach functions MUST perform the following, with no retry loop:

1. Compute the expected layout from the supplied capacity and const record
   size; validate base alignment and `region_len` as for initialization.
2. Read the initialized fixed prefix. Reject bad magic or nonzero reserved
   prefix bytes with `InvalidHeader`; a different format version, word width,
   endian marker, or atomic alignment returns `IncompatibleFormat`.
3. Require stored capacity, record size, slots offset, and rounded size to
   equal the independently computed values; otherwise return
   `ConfigurationMismatch`. All conversions and bounds arithmetic are checked.
4. Require the stored generation to equal the supplied generation, or return
   `GenerationMismatch`. The application obtains this expected identifier from
   its startup handoff, not by trusting any region it happens to encounter.
5. Acquire-load readiness and require 1, or return `InvalidHeader`. Validate
   reserved header padding without reading live atomic words non-atomically.
6. Perform one strong compare-exchange of the requested role from `UNCLAIMED`
   to `LIVE`, using `AcqRel` on success and `Acquire` on failure. A `LIVE` or
   `CLOSED` prior value returns `RoleAlreadyClaimed`; an invalid state returns
   `InvalidHeader`. No fallible work may follow a successful claim.
7. Return the unique endpoint with its own local position and physical index
   zero and opposite-position cache zero. The opposite role may already have
   advanced or closed; the normal acquire-refresh rules discover its progress.

Failed attachment MUST NOT reset counters, alter slots, or consume a role.
Roles are single-use for the entire generation; a dropped or forgotten role
cannot reattach. In particular, attachment MUST NOT reconstruct a physical
index from an already advanced wrapped counter. First attachment is valid
even after the opposite role has filled the queue or closed.

An attach caller MUST additionally guarantee, for the full chosen `'region`
lifetime and every derived reference:

- its local mapping remains valid, writable, stable, and at the same address;
  the backing object is not truncated, reclaimed, or reformatted underneath it;
- all participants use a qualified interoperable atomic implementation and
  obey the format, role claims, publication protocol, and exclusive teardown;
- no external code modifies immutable metadata, copies live atomic bytes,
  accesses a slot contrary to its role, or creates conflicting references;
- raw mapping access does not retain a whole-region `&mut` or immutable slice
  reference that conflicts with interior-mutability slot/atomic accesses; and
- fork, DMA, foreign code, or duplicated handles do not create a second active
  owner of an already claimed role.

The raw functions cannot infer mapping lifetime. Their unsafe contract MUST
explicitly forbid choosing `'static` unless those obligations really hold for
that duration. Once established, the returned safe API enforces local endpoint
uniqueness and peek lifetimes. Untrusted peers that corrupt shared memory are
outside this safety contract; validation is not a sandbox or corruption-proof
message parser. No general safe constructor from an arbitrary byte slice is
provided for independently mapped regions.

Malformed-header error cases are permitted inputs when the caller supplies
valid, initialized, race-free bytes for every header read the validation path
performs. Such rejection does not require a live compatible queue. If validation
can reach a native atomic access, the corresponding words must have valid
initialized atomic backing. If attachment succeeds, all full-generation
protocol and mapping obligations above apply. Returning `Err` cannot make a
dangling pointer, an uninitialized header read, or a racing write valid.

### 15.9 Shared-region liveness and reclamation

For shared endpoints, a counterpart is potentially alive while its state is
`UNCLAIMED` or `LIVE`; it is dead only when `CLOSED` is acquired. Thus
`is_producer_alive`/`is_consumer_alive` are true before first attachment as
well as during ordinary operation. This exception is necessary to prevent a
consumer from declaring a not-yet-attached producer definitively drained.
The predicate remains monotonic: role transitions are 0 → 1 → 2 only.

Endpoint drop MUST release-store its role to `CLOSED` after its last position
publication. Shared endpoint drop does not decrement a reclamation refcount,
drop payload records, reset the region, free memory, or unmap anything. All
records are byte arrays with no destructor. The producer's close release and
the consumer's acquire followed by a fresh tail load establish `is_drained`
exactly as in Section 10.5. Once a role is closed, it cannot reopen in that
generation. A producer may fill remaining capacity after consumer closure.

Each participant may release its own mapping only after all local endpoints
and derived references have ceased to be usable. The external owner may
reclaim or reinitialize the shared backing object only after establishing
global quiescence, including in-flight or delayed attachment attempts. Merely
observing both liveness predicates false is not an ownership proof or a
substitute for that coordination.

An abnormal process exit, reset, or forgotten endpoint may leave a role `LIVE`
forever. No liveness query detects process death; the other endpoint continues
to return ordinary full/empty results without waiting. Automatic takeover,
repair of partially completed operations, and reconstruction of local indices
are not supported. After externally establishing quiescence, the owner MAY
discard all records and call `initialize` again with a fresh generation. A
generation identifier MUST NOT be reused while an old participant or delayed
attachment could mistake it for its expected generation. Generation comparison
does not make concurrent reset safe and is not performed on each push/pop.

Memory visibility is not durability. Release/acquire operations do not promise
flushes to persistent storage or recovery after power loss. Creation of shared
objects, memory mapping, page locking/prefaulting, startup synchronization, and
shutdown coordination remain application/platform integration work outside the
queue's allocation-free and wait-free data-path claims.

## 16. Linearizability and observability

### 16.1 Linearization points

| Operation/result | Linearization point |
| --- | --- |
| Successful `try_push` | Release store that publishes the new tail. |
| `try_push` returning `Full` | Fresh acquire head load that observes full occupancy. |
| Successful `try_pop` or `try_pop_into` | Release store that publishes the new head. |
| `try_pop` or `try_pop_into` returning `None` | Fresh acquire tail load that observes equality with local head. |
| Successful peek | The first head-slot access during the call; availability is justified by a cached or freshly acquired tail observation. No queue state changes. |
| Empty peek | Fresh acquire tail load observing equality. |
| `push_slice` transferring `n > 0` | Single release store publishing tail advanced by `n`; the prefix is contiguous with no interleaving. |
| `pop_slice` transferring `n > 0` | Single release store publishing head advanced by `n`; the prefix is contiguous with no interleaving. |
| Length or capacity-state snapshot query | Opposite-position acquire load used for the returned snapshot. `capacity()` is an immutable metadata read and needs no atomic linearization. |
| Liveness query | Acquire load of the corresponding liveness flag. |
| `is_drained == true` | Fresh tail observation after acquiring producer-dead. |

Batch publication is atomic as a batch from the opposite endpoint's point of
view: it sees either the pre-batch published position or a position covering
the entire batch. The consumer may subsequently pop those elements one at a
time.

### 16.2 FIFO

If successful pushes publish values `a` then `b`, no consumer operation may
return `b` before `a`. Peeking does not change order. Bulk insertion preserves
slice order, and bulk removal returns the oldest available prefix.

### 16.3 Snapshot staleness

`len`, `is_empty`, `is_full`, `remaining_capacity`, and liveness methods are
informational. Their return values may be stale as soon as the call completes.
Callers MUST use the result of `try_push` or `try_pop` as the authority for that
operation rather than treating an earlier query as a reservation.

## 17. Destruction, panic, and abnormal termination

### 17.1 Endpoint drop

For typed queues, the consumer first preserves its physical head index for
cleanup. Each endpoint then release-stores its liveness flag to false and
releases its endpoint ownership share as in Section 15.2. Dropping one endpoint MUST leave the
other endpoint memory-safe and usable:

- after producer drop, the consumer may drain all published values;
- after consumer drop, the producer may continue pushing until full, although
  no value is guaranteed to be consumed; and
- final value cleanup occurs only after both endpoint shares are released;
  and
- backing deallocation occurs only in the heap-owned mode, after cleanup.

A typed endpoint drop is normally `O(1)`, but the final endpoint performs
`O(remaining_occupancy)` value cleanup and, for a heap-owned queue, deallocation.
Caller-owned storage remains available for exclusive reset. Shared endpoints
use Section 15.9 instead: drop closes only that role and never owns reclamation.

### 17.2 Final occupied-range cleanup

Typed final cleanup MUST acquire a stable final head and tail after exclusive
endpoint ownership is established. Starting at the preserved physical head
index, it MUST visit exactly `tail.wrapping_sub(head)` slots in FIFO order,
advancing physical indices independently of sequence wrap and dropping each
initialized value once. Free slots MUST not be read or
dropped as `T`.

Cleanup SHOULD use a guard that advances its internal “remaining” state before
calling each `T::drop`. If one destructor unwinds, the guard SHOULD attempt to
drop the remaining initialized elements without revisiting the panicking
element. A second panic during unwinding may abort according to Rust runtime
behavior. Cleanup tracking MUST remain disarmed for an element once its drop
begins, including if the caller catches an unwind and later resets or destroys
the storage. In every case, the implementation MUST remain free of undefined
behavior and double drop; leakage during process abort is acceptable. Allocation
cleanup guards apply only to heap-owned memory; no guard may free caller memory.

### 17.3 Core-operation panic behavior

For a valid endpoint, scalar methods and `T: Copy` bulk methods MUST contain no
intentional panic path. In particular:

- integer wrap uses explicit wrapping arithmetic;
- physical bounds follow proven invariants rather than user indexing;
- no formatting, callback, clone, or destructor runs inside a data-path method;
  and
- an invariant assertion capable of panicking cannot sit between a slot
  ownership transition and its atomic publication.

If user code panics while holding a reference returned by `peek_mut`, the item
remains initialized and occupied; any completed mutation remains part of that
item. Normal unwinding later drops the consumer endpoint if its owner is
unwound.

### 17.4 Forget and abort

Forgetting an endpoint MUST remain memory-safe; its destructor is not a safety
precondition. It may leak heap storage and occupied values in the owned mode.
In borrowed/static modes it can leave the session claimed and its endpoint
share outstanding. Lifetimes still prevent destroying or resetting storage
while the other endpoint or a peek reference remains usable.

Once exclusive storage access is legally regained, `reset` or storage drop
MUST tolerate outstanding forgotten shares. This specification permits, and
the baseline implementation MUST use, abandoning those remaining values
without running their destructors. It then treats the bytes as uninitialized.
It MUST NOT access an abandoned `T`, follow a leaked pointer, reconstruct a
lost physical index using a wrapped counter, or clean up an element twice.
This explicit leak policy avoids making soundness depend on endpoint drop.
Normal completion without forgotten shares MUST NOT leak queued values.

For shared mappings, forgetting an endpoint does not release the caller's
unsafe lifetime/mapping obligations or authorize role reuse. Section 15.9
governs external quiescence and discarding an abandoned generation. Process
abort does not promise cleanup in any storage mode.

## 18. Trait and type behavior

The following traits are normative:

| Type | Required behavior |
| --- | --- |
| `Producer<T>` | `Send` iff `T: Send`; explicitly not `Sync`; not `Clone` or `Copy`; `Debug` without requiring `T: Debug`. |
| `Consumer<T>` | `Send` iff `T: Send`; explicitly not `Sync`; not `Clone` or `Copy`; `Debug` without requiring `T: Debug`. |
| `Full<T>` | Conditional standard value traits as shown in Section 7; `Display` does not format `T`; `Error` when `T: Debug`. |
| `CreateError`, `SplitError`, `shared_memory::SharedError` | `Clone + Copy + Debug + Eq + PartialEq + Display + Error`; shared error exists only with its feature. |
| `BorrowedProducer<'q, T>`, `BorrowedConsumer<'q, T>` | `Send` iff `T: Send`; not `Sync`, `Clone`, or `Copy`; `Debug` without `T: Debug`; cannot outlive the storage borrow. |
| `Slot<T>` | `Send` iff `T: Send`; not `Sync`, `Clone`, or `Copy`; no safe initialized-value access. |
| `BorrowedStorage<'s, T>`, `StaticStorage<T, N>` | `Send` and `Sync` iff `T: Send`; not `Clone` or `Copy`; storage/drop-check ownership of `T` is retained. |
| `SharedProducer<'r, R>`, `SharedConsumer<'r, R>` | `Send`; not `Sync`, `Clone`, or `Copy`; `Debug` without inspecting record contents. |

The endpoint implementations SHOULD contain a private marker such as
`PhantomData<Cell<()>>` to suppress `Sync` while retaining conditional `Send`.
Any unsafe conditional `Send`/`Sync` implementation for private shared state
MUST require `T: Send`, not `T: Sync`: values are transferred between roles but
are never concurrently shared between them as `T`. Public storage's conditional
`Sync` implementation MUST additionally prove that concurrent `try_split` calls
have one winner, that the owner exposes no slot inspection, and that reset
requires exclusive access. Constructor-only markers MUST NOT accidentally
require `T: Sync` or `T: 'static` for ordinary borrowed use.

Typed storage and endpoint representations MUST be invariant in `T`. In
particular, coercing a producer to accept shorter-lived references MUST NOT
permit inserting a value that violates the consumer's or backing owner's
element lifetime. The implementation's marker and pointer choices MUST retain
this property and appropriate drop checking, including for borrowed `T`.

`Debug` for endpoints MUST NOT inspect or format queued elements. It SHOULD
report the role, capacity, snapshot length, and counterpart-liveness snapshot,
and MUST clearly be documented as observational rather than atomic across all
fields.

No guarantee is made for unwind-safety auto traits beyond what Rust derives
from the actual private fields. Adding or removing a positive public auto-trait
implementation is a compatibility change and requires normal semver review.

## 19. Verification and test requirements

### 19.1 Deterministic unit tests

The same applicable semantic tests MUST run against heap-owned (when enabled),
borrowed, static, and shared endpoint families. Shared records exercise byte
array equivalents; non-`Copy` and arbitrary-`T` destruction cases apply to the
three typed modes. Tests MUST cover at least:

- capacities 1, 2, a non-power-of-two value, and a power-of-two value;
- initial empty state and exact capacity;
- transition sequences empty → partial → full → partial → empty;
- FIFO order over multiple physical wraps;
- `Full<T>` returning the original non-`Copy` value;
- no hidden sentinel slot;
- counter and physical-index wrap;
- producer and consumer snapshot methods;
- `peek` and `peek_mut`, including mutation followed by pop;
- `try_pop_into` interleaved with `try_pop` over physical wraps, including an
  untouched destination on an empty result, a result that refers into the
  destination, and exact drop accounting for non-`Copy` elements;
- bulk zero-length, partial, exact, insufficient-space, insufficient-data, and
  two-segment wrap cases;
- endpoint drop in either order;
- producer drop followed by complete draining and `is_drained`;
- consumer drop followed by producer fill-to-full;
- simultaneous endpoint drop;
- non-`Copy`, non-`Clone`, `Send` but non-`Sync` element types;
- aligned element types;
- zero-sized inhabited types;
- zero-sized types with observable drop counts;
- drop counts for popped, returned-full, queued, and never-initialized slots;
  and
- constructor validation and partial-allocation cleanup for heap ownership;
- borrowed capacity from slice length and inline capacity from `N`;
- uninitialized slot construction without `T: Copy` or `T: Default`;
- simultaneous `try_split` calls with one successful pair, failure without
  side effects, and no automatic rearming after both endpoints drop;
- static final endpoint drop cleaning queued values without a storage drop;
- exclusive reset followed by a new session, including preserved FIFO and
  drop counts after repeated counter wraps;
- forgotten producer, forgotten consumer, and both forgotten, followed by
  legal owner drop/reset with the specified leak policy; and
- panicking destructors followed by caught unwind and reset without revisiting
  a value whose destructor began.

### 19.2 Reference-model property tests

Sequential randomized operation traces MUST compare the implementation with a
`VecDeque` model for capacities including 1 and non-powers of two. Operations
must include scalar push/pop, peek mutation, bulk transfer, and endpoint-status
queries. The generator MUST retain failed-push values and validate complete
ownership accounting.

### 19.3 Concurrency model checking

A Loom or equivalent model MUST replace atomics through an internal abstraction
and exhaustively explore small producer/consumer traces. The model MUST check:

- FIFO and at-most-once delivery;
- no read before publication;
- no overwrite before release;
- full and empty result validity;
- endpoint destruction races;
- liveness-flag ordering used by `is_drained`; and
- final drop accounting for owned, borrowed, and static queues;
- the split claim and final-share transition, including publication of the
  saved physical head index; and
- shared startup/role claims, late first attachment, and close/drain ordering.

The shared protocol can be modeled in one address space, but that is not proof
of operating-system mapping or cross-process atomic interoperability.

At least one negative control SHOULD weaken each essential acquire or release
in turn and demonstrate that the model or a dedicated litmus test can detect
the broken protocol.

### 19.4 Wraparound model

Test-only code MUST run the same state machine with a narrow unsigned sequence
type, such as `u8`, and capacities no greater than half its range. It MUST force
multiple complete sequence wraps under interleaved producer/consumer activity.
Production code remains `usize`; the narrow model exists to make ABA and
modular-distance mistakes observable. Capacity 3 MUST cross a complete
sequence wrap and then end with a nonempty queue, verifying final cleanup from
the saved physical head index. A negative control using `wrapped_head % 3`
MUST fail this scenario. Shared tests MUST also cover first consumer attachment
after producer publication and reject any attempt to resume a closed role.

### 19.5 Undefined-behavior tooling

The project MUST run:

- Miri over focused sequential, drop, peek, zero-sized, and wrap tests;
- Miri under the project's chosen aliasing model where supported;
- ThreadSanitizer or an equivalent race detector over long concurrent stress
  tests on at least one certified target; and
- address/leak sanitization over constructor failure and destruction paths where
  the toolchain supports it.

Tool limitations and excluded configurations MUST be documented; passing a
sanitizer is supporting evidence, not a substitute for the safety proof.

### 19.6 Stress testing

Concurrent stress tests MUST transfer unique sequence numbers for sustained
runs at capacities 1, 2, 3, and a larger operational size. The consumer MUST
verify exact order, no gaps, no duplicates, and a final count. Runs SHOULD vary
thread affinity, include deliberate producer/consumer pauses, and cover both
producer-faster and consumer-faster regimes.

### 19.7 Compile-time trait tests

Compile-time assertions or compile-fail tests MUST prove:

- endpoints are `Send` for `T: Send`;
- endpoints are not `Sync`;
- endpoints are not `Clone` or `Copy`;
- a non-`Send` `T` prevents moving an endpoint to another thread; and
- endpoint methods require mutable access for state-changing and peek
  operations;
- borrowed endpoints cannot escape either control-state or slot lifetimes;
- storage cannot move, reset, or be destroyed while an endpoint/peek reference
  can be used, and slots cannot be borrowed again while their owner is live;
- borrowed `T` need not be `'static`; `Send` but non-`Sync` types work;
- variance cannot shorten a producer's element-reference lifetime independently
  of its consumer or backing storage;
- safe static splitting works without `static mut`;
- non-`Send` types cannot make storage `Sync` or send endpoints across threads;
- shared endpoints and constructors are absent without their feature; and
- heap-owned types and `bounded` are absent without `alloc`, while common
  errors, slots, and borrowed/static APIs remain available.

### 19.8 Toolchain and target matrix

Continuous integration MUST include:

- the exact MSRV;
- current stable Rust;
- beta and nightly as advisory jobs;
- `--no-default-features`, default features, `--all-features`, and
  `--no-default-features --features shared-memory`;
- a linked `no_std` example with no global allocator on at least one embedded
  target with pointer-width atomics, exercising borrowed/static lifecycle and
  data-path operations;
- an allocator-free shared-memory build and example on a qualified target;
- an optional `no_std + alloc` build on an embedded-style target;
- a compile-fail check for an unsupported atomic target and for unsupported
  shared-memory pointer widths;
- at least one little-endian 64-bit certified target;
- at least one weakly ordered certified architecture through native or
  emulated execution; and
- rustdoc with warnings denied.

### 19.9 Shared-memory integration and format tests

The `shared-memory` feature MUST additionally pass:

- exact format-offset, alignment, size, checked multiplication/addition/rounding,
  capacity-1, capacity-3, zero-byte-record, and oversized-layout tests;
- alignment and undersized-region failures without mutation or out-of-bounds
  access, including guard-page/canary checks;
- rejection of bad magic, version, word width, endian marker, atomic alignment,
  reserved bytes, inconsistent size/offset/capacity, and wrong generation;
- checks that validation occurs before any native atomic access when ABI fields
  do not match, and that failed attachments consume no role;
- duplicate attachment races with exactly one winner per role, plus permanent
  rejection after that role closes;
- initialized-but-not-attached counterpart liveness, late consumer attachment,
  producer close before consumer attachment, and definitive draining;
- a two-process test using genuinely shared mappings at different virtual
  addresses, transferring numbered records with no gaps or duplicates;
- records and control state surviving removal of a peer's local mapping after
  its orderly endpoint drop, while the surviving peer drains/fills normally;
- forced peer termination yielding ordinary full/empty results without crash
  recovery claims, followed by externally quiescent reinitialization with a
  fresh generation; and
- inspection proving no absolute address, reference, allocator metadata, or
  process-local reclamation state is stored in the region.

For every advertised firmware shared-memory configuration, an actual two-core
or two-image test MUST cover the documented memory attributes and startup
handoff. Thread-only tests, Loom, and Miri are insufficient to certify multiple
address spaces; their scope and platform limitations MUST be stated. Tests MUST
NOT intentionally violate raw-pointer safety preconditions and then treat the
absence of a crash as evidence of error handling.

## 20. Performance requirements and measurement

### 20.1 Data-path budgets

In optimized builds, the reference steady-state scalar path SHOULD have:

| Path | Opposite-position loads | Position stores | Other required shared writes |
| --- | ---: | ---: | ---: |
| Successful push with usable cached head | 0 | 1 release tail store | 0 |
| Successful push requiring refresh | 1 acquire head load | 1 release tail store | 0 |
| Full push | 1 fresh acquire head load | 0 | 0 |
| Successful pop with usable cached tail | 0 | 1 release head store | 0 |
| Successful pop requiring refresh | 1 acquire tail load | 1 release head store | 0 |
| Empty pop | 1 fresh acquire tail load | 0 | 0 |

Implementations MUST NOT add liveness checks to every push or pop. Liveness is
an explicit informational API so the normal data path retains this budget.
The budget applies to every storage backend. Address derivation, generation
validation, split/role claims, and ownership reference counting MUST NOT add
shared writes or RMW operations to ordinary push/pop. Private backend code
sharing is encouraged, but no allocation or dynamic dispatch may be introduced.

### 20.2 Benchmarks

The repository MUST include reproducible benchmarks for:

- single-thread alternating push/pop;
- two-thread throughput with balanced endpoints;
- producer-limited and consumer-limited operation;
- capacities 1, 2, 3, 64, 1,024, and a larger cache-spanning capacity;
- `u8`, `u64`, a cache-line-sized value, and a nontrivially large `Copy` value;
- bulk sizes 1, 4, 16, 64, and wrap-crossing batches;
- pinned and unpinned threads where supported; and
- comparison with at least one maintained SPSC baseline under equivalent
  semantics.

Reports MUST include compiler version, target triple, CPU, optimization flags,
capacity, element size, affinity policy, sample count, throughput, and latency
percentiles. No universal nanosecond threshold is normative because hardware
varies. Release automation SHOULD flag statistically credible regressions above
10% on controlled benchmark hosts for review rather than automatically hiding
them with a new baseline.

### 20.3 Allocation and code-generation audit

An instrumented allocator test MUST show zero allocations and deallocations
inside every data-path method after construction. Borrowed/static/shared tests
with allocator-free elements MUST also observe zero allocator calls over the
entire construction, split/attach, use, reset, and destruction lifecycle.
The allocator-free embedded example MUST link without a global allocator;
successful `cargo check` alone is insufficient. Inspect the resulting symbols
or link map for unexpected allocation/runtime dependencies. Representative
certified targets MUST be inspected to confirm:

- no calls to allocator, mutex, parking, panic-formatting, or unwinding helpers
  on ordinary scalar success/full/empty paths;
- no compare-and-swap retry loop;
- no sequentially consistent fence;
- expected acquire/release instructions or architecture-equivalent compiler
  ordering; and
- no integer division in the recommended scalar physical-index path.

## 21. Documentation requirements

Crate-level documentation MUST include:

- a one-paragraph explanation of SPSC and why endpoints are unique;
- the exact wait-free scope and platform qualification;
- the distinction between an individual non-blocking attempt and a retry loop;
- the storage-mode and Cargo-feature matrix, exact capacities, allocator-free
  operation, and the retained optional `no_std + alloc` API;
- an ownership-transfer example;
- a two-thread example, plus a borrowed scoped-thread example;
- borrowed slots, const-initialized static storage, exclusive reset, and the
  defined forgotten-endpoint leak behavior;
- caller-owned shared-region initialization, attachment, role liveness,
  mapping lifetimes, and orderly shutdown;
- shared-memory byte-record encoding, platform qualification, different-base
  mappings, and the exclusion of crash recovery and persistence;
- full/empty handling;
- disconnection and definitive draining behavior;
- panic and destructor exclusions;
- a safety overview linked to the detailed implementation notes; and
- the certified target table for that release.

Every snapshot method MUST say that its result may be stale immediately. Every
method returning or accepting a value MUST document ownership on success and
failure. Rustdoc examples MUST compile in CI.

### 21.1 Minimal heap-owned example (`alloc`)

```rust
use spookycircle::bounded;

let (mut producer, mut consumer) = bounded::<u32>(2).unwrap();

producer.try_push(10).unwrap();
producer.try_push(20).unwrap();

let full = producer.try_push(30).unwrap_err();
assert_eq!(full.into_inner(), 30);

assert_eq!(consumer.peek(), Some(&10));
assert_eq!(consumer.try_pop(), Some(10));
assert_eq!(consumer.try_pop(), Some(20));
assert_eq!(consumer.try_pop(), None);
```

### 21.2 Cross-thread heap-owned example (`alloc`)

```rust
use std::thread;
use spookycircle::bounded;

let (mut producer, mut consumer) = bounded::<u64>(1_024).unwrap();

let produce = thread::spawn(move || {
    for mut value in 0..100_000 {
        loop {
            match producer.try_push(value) {
                Ok(()) => break,
                Err(full) => {
                    value = full.into_inner();
                    thread::yield_now(); // Application policy, not queue behavior.
                }
            }
        }
    }
});

let consume = thread::spawn(move || {
    let mut expected = 0;
    loop {
        match consumer.try_pop() {
            Some(value) => {
                assert_eq!(value, expected);
                expected += 1;
            }
            None if consumer.is_drained() => break,
            None => thread::yield_now(),
        }
    }
    assert_eq!(expected, 100_000);
});

produce.join().unwrap();
consume.join().unwrap();
```

The example MUST explain that its retry/yield loops are not wait-free as whole
operations.

### 21.3 Borrowed slots and reuse (no allocator)

```rust
use spookycircle::{BorrowedStorage, Slot};

let mut slots = [const { Slot::<u32>::new() }; 3];
let mut storage = BorrowedStorage::new(&mut slots).unwrap();
{
    let (mut producer, mut consumer) = storage.try_split().unwrap();
    producer.try_push(7).unwrap();
    assert_eq!(consumer.try_pop(), Some(7));
    drop(producer);
    assert!(consumer.is_drained());
    drop(consumer);
}

// Both endpoint borrows have ended; exclusive reuse is now legal.
storage.reset();
let (mut producer, mut consumer) = storage.try_split().unwrap();
assert_eq!(producer.capacity(), 3);
producer.try_push(9).unwrap();
assert_eq!(consumer.try_pop(), Some(9));
drop((producer, consumer));
```

The slots and control object must both outlive the pair. Array initialization
uses inline `const` and does not require `Slot<T>: Copy` or initialize a `T`.
No allocator is needed for the queue or the `u32` values in this example.

### 21.4 Const-initialized static storage (no allocator)

```rust
use spookycircle::StaticStorage;

static STORAGE: StaticStorage<u32, 8> = StaticStorage::new();

let (mut producer, mut consumer) = STORAGE.try_split().unwrap();
producer.try_push(42).unwrap();
assert_eq!(consumer.try_pop(), Some(42));
drop((producer, consumer));

// The static is one session: endpoint drop does not rearm it.
assert!(STORAGE.try_split().is_err());
```

Startup may move this unique pair into separate tasks or interrupt contexts.
The storage is a real immutable `static` with interior mutability; its safe API
needs no `static mut` reference. Remaining queued values are cleaned up by the
final endpoint, since Rust does not run destructors for statics at program exit.

### 21.5 Borrowed endpoints across scoped threads

```rust
use std::thread;
use spookycircle::{BorrowedStorage, Slot};

let mut slots = [const { Slot::<u32>::new() }; 3];
let storage = BorrowedStorage::new(&mut slots).unwrap();
let (mut producer, mut consumer) = storage.try_split().unwrap();

thread::scope(|scope| {
    let worker = scope.spawn(move || {
        producer.try_push(123).unwrap();
        // Producer closes when this scoped thread exits.
    });
    worker.join().unwrap();
    assert_eq!(consumer.try_pop(), Some(123));
    assert!(consumer.is_drained());
    drop(consumer);
});
```

The queue and its control state do not allocate; host thread creation and
joining belong to `std` and are outside that guarantee. Non-scoped thread
spawning requires appropriately long-lived storage instead.

### 21.6 Caller-owned shared-region lifecycle (`shared-memory`)

An application first obtains a qualified shared region with the size and
alignment reported by `layout`. The following function illustrates the API on
one local mapping; it does not create a mapping or perform a syscall:

```rust
use core::ptr::NonNull;
use spookycircle::shared_memory as shm;

/// # Safety
/// The caller supplies exclusive, writable, qualified coherent shared memory
/// satisfying Section 15.8 for this call, with no other participant, and a
/// fresh generation. The mapping remains valid until both local endpoints drop.
unsafe fn local_round_trip(
    base: NonNull<u8>,
    region_len: usize,
    generation: u64,
) -> Result<(), shm::SharedError> {
    let required = shm::layout::<4>(3)?;
    assert!(region_len >= required.size());

    // SAFETY: caller provides exclusive quiescent memory and a fresh generation.
    unsafe { shm::initialize::<4>(base, region_len, 3, generation)?; }

    // SAFETY: initialization completed in this thread; the caller guarantees
    // mapping validity through both drops, and each role is attached once.
    let mut producer = unsafe {
        shm::attach_producer::<4>(base, region_len, 3, generation)?
    };
    // SAFETY: same mapping contract; this claims the distinct consumer role.
    let mut consumer = unsafe {
        shm::attach_consumer::<4>(base, region_len, 3, generation)?
    };

    producer.try_push(42_u32.to_le_bytes()).unwrap();
    let record = consumer.try_pop().unwrap();
    assert_eq!(u32::from_le_bytes(record), 42);
    drop(producer);
    assert!(consumer.is_drained());
    drop(consumer);
    Ok(())
}
```

For separate processes or firmware images, one initializer performs exclusive
initialization, then communicates the generation and configuration through
the synchronized startup handoff. Each participant calls only its own attach
function using its local base address. Bases may differ. All local endpoints
and peek references must cease to be usable before their mapping is removed;
reinitializing the backing region additionally requires global quiescence.

## 22. Compatibility and release policy

### 22.1 Semantic versioning

After a 1.0 crate release, these are breaking changes and require a major
version:

- weakening FIFO, ownership, progress, or memory-safety guarantees;
- increasing per-operation requirements on `T`;
- making endpoints cloneable or `Sync`;
- changing exact capacity or full/empty semantics;
- removing supported capacities or a certified target without a documented
  soundness exception;
- adding hidden allocation, blocking, or callbacks to a wait-free method;
- changing a public method signature, storage lifetime, caller-owned memory
  reclamation policy, or error ownership behavior;
- making an allocator-free mode depend on `alloc`/`std` or changing the default
  feature behavior;
- weakening raw mapping safety documentation or silently accepting an
  incompatible shared-region format; or
- raising MSRV outside the published MSRV policy.

Adding an optional adapter or a new method may be minor-version compatible only
if it does not alter existing data-path code or trait behavior.

The shared-region format has its own explicit version. Any change to field
offsets, alignment, encoding, role meanings, atomic access widths, or payload
placement MUST change that format version and be rejected by incompatible
attachers. No migration or reuse of a live generation is implicit in a crate
upgrade. Rust private type layout remains outside the compatibility contract.

### 22.2 MSRV policy

The project MUST test the declared MSRV. An MSRV increase MUST be called out in
release notes and MUST occur only in a semver-minor release before crate 1.0 or
according to a separately published post-1.0 policy.

### 22.3 Unsafe-change review

Every change touching atomics, unsafe code, slot representation, endpoint
traits, sequence arithmetic, storage ownership, shared-region format,
initialization/attachment, or destruction MUST receive a dedicated safety
review. The pull request MUST state which invariants and
model tests were considered. Performance-only reasoning is insufficient.

## 23. Release acceptance criteria

A release candidate is acceptable only when all of the following are true:

- [ ] The public API and feature gates match Section 7; only the three
  documented shared-region raw functions are public unsafe entry points.
- [ ] Capacity 1 and non-power-of-two capacities pass all state-transition and
  stress tests.
- [ ] Scalar methods contain no retry loop, allocation, deallocation, lock,
  syscall, park, yield, callback, or user destructor.
- [ ] Full and empty scalar results each follow a fresh opposite-position
  acquire load.
- [ ] Successful scalar publication uses the required release store.
- [ ] The acquire/release proof has been reviewed against actual code.
- [ ] Counter wrap is exercised with the narrow-counter model.
- [ ] Loom or equivalent model checking passes.
- [ ] Miri and the supported sanitizers pass with documented exceptions.
- [ ] Drop-count tests show neither leaks in normal completion nor double drops,
  including zero-sized and panicking-destructor scenarios.
- [ ] Compile-time tests prove endpoint `Send`/`!Sync`/non-clone behavior.
- [ ] The allocator harness observes no data-path allocation and no allocator
  calls over borrowed/static/shared lifecycles with allocator-free elements.
- [ ] A `no_std` embedded example links with no global allocator.
- [ ] Lifetime/trait tests prove safe borrowed and static ownership, unique
  splitting, and exclusive reset, including forgotten endpoints.
- [ ] Final typed cleanup uses a correct physical head after counter wrap and
  cleans static queued values without requiring static destruction.
- [ ] Shared-format validation, claim races, late attachment, and generation
  mismatch tests pass without altering a live queue on failure.
- [ ] Shared endpoints communicate through mappings at different addresses on
  every advertised process-shared platform; firmware claims have corresponding
  multicore evidence.
- [ ] Shared-memory safety contracts, external startup/teardown, and crash
  limitations are reviewed against the actual integration tests.
- [ ] Code-generation inspection passes for each advertised architecture
  family.
- [ ] Benchmarks show no unexplained material regression.
- [ ] MSRV, stable, all feature combinations, allocator-free `no_std`, optional
  `no_std + alloc`, rustdoc, and target-matrix CI pass.
- [ ] Every unsafe block has a specific `SAFETY:` justification.
- [ ] The release publishes separate same-address-space and shared-memory
  qualification tables and the limits of each wait-free claim.
- [ ] Crate documentation includes ownership, retry-loop, disconnect, panic,
  and destruction caveats.

## 24. Deferred extensions

The following may be designed in later specifications, each behind a separate
soundness and progress review:

- typed portable shared records beyond fixed-size byte arrays;
- non-coherent/DMA transports with separately proved cache and device ordering;
- crash recovery, role replacement, or durable persistent-memory queues;
- producer write grants over `MaybeUninit<T>` with panic-safe commit tracking;
- consumer read grants and two-slice views;
- explicit close semantics distinct from endpoint drop;
- blocking and async adapters layered outside the core queue;
- cache-padding selection by architecture;
- allocator-parameterized construction after the relevant Rust APIs are stable;
  and
- specialized copy paths for byte buffers.

An extension MUST NOT make existing scalar operations conditional on a wakeup,
callback, dynamic dispatch, allocation, or additional contended read-modify-
write operation. Adapter progress guarantees must be stated separately from the
core.

## 25. Normative references and background

The following Rust references define primitives used by this specification.
They are explanatory dependencies; this document's stronger API and progress
requirements remain normative for the library.

- [Rust atomic memory orderings](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html)
- [`UnsafeCell` and aliasing](https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html)
- [`MaybeUninit`](https://doc.rust-lang.org/std/mem/union.MaybeUninit.html)
- [Rust Reference: `target_has_atomic`](https://doc.rust-lang.org/reference/conditional-compilation.html#target_has_atomic)
- [Rustonomicon: atomics](https://doc.rust-lang.org/nomicon/atomics.html)
- [Rustonomicon: `Send` and `Sync`](https://doc.rust-lang.org/nomicon/send-and-sync.html)
- [Core atomic memory model and portability](https://doc.rust-lang.org/core/sync/atomic/index.html)
- [Core `UnsafeCell`](https://doc.rust-lang.org/core/cell/struct.UnsafeCell.html)
- [Core `MaybeUninit`](https://doc.rust-lang.org/core/mem/union.MaybeUninit.html)
- [Rust 1.79: inline const expressions](https://blog.rust-lang.org/2024/06/13/Rust-1.79.0/)
- [Linux `mmap(2)` mapping semantics](https://man7.org/linux/man-pages/man2/mmap.2.html)
- [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119)
- [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)

The shared-region format and lifecycle are this specification's design, not
a claim that Rust's general atomic documentation guarantees cross-process
interoperability. The core atomic reference distinguishes lock-free availability
from wait-free progress, and its access rules constrain overlapping atomic and
non-atomic use. `UnsafeCell` permits interior mutation but does not relax unique
mutable-reference obligations. Those facts motivate the mapping/aliasing
contract and separate qualification requirements in Sections 6.4 and 15.8.
On Linux, `MAP_SHARED` exposes writes through other mappings; `MAP_PRIVATE`
does not provide the required shared-write behavior. Mapping visibility alone
does not establish the queue's ordering or reclamation proof.

## Appendix A. Invariant-to-operation matrix

| Operation | Reads initialized storage | Writes free storage | Publishes tail | Publishes head | May run `T` code | Wait-free scope |
| --- | --- | --- | --- | --- | --- | --- |
| `try_push` success | No | One slot | Yes | No | No | Scalar |
| `try_push` full | No | No | No | No | No | Scalar |
| `push_slice` | No | Up to `min(input, C)` | Once if nonzero | No | No (`T: Copy`) | Input-bounded |
| `try_pop` success | One slot | No | No | Yes | No | Scalar |
| `try_pop` empty | No | No | No | No | No | Scalar |
| `try_pop_into` success | One slot, moved into the destination | No | No | Yes | No | Scalar |
| `try_pop_into` empty | No | No | No | No | No | Scalar |
| `peek` | Shared reference to head | No | No | No | No | Scalar |
| `peek_mut` | Exclusive reference to head | Consumer-only mutation | No | No | User may mutate after return | Method acquisition is scalar |
| `pop_slice` | Up to `min(output, C)` | No | No | Once if nonzero | No (`T: Copy`) | Output-bounded |
| Snapshot query | No | No | No | No | No | Scalar |
| Typed endpoint drop | Possibly during final cleanup | No | No | No | May run `T::drop` | Excluded |
| Shared endpoint drop | No | No | No | No | No; closes one role | Excluded |
| Borrowed/static split or reset | No occupied-value reads | Control initialization only | Initialization only | Initialization only | No | Excluded |
| Shared initialize/attach | Validated header only on attach | Header on initialize only | Initialization only | Initialization only | No | Excluded |

## Appendix B. Required unsafe proof checklist

For each slot access, reviewers MUST be able to answer all of the following:

1. Which endpoint owns this access?
2. Which sequence position maps to this physical slot?
3. Is the slot logically free or occupied?
4. If reading, which acquire observation proves initialization?
5. If writing, which acquire observation proves prior consumer release?
6. Can any reference to the same `T` still be live?
7. Is the `UnsafeCell`-derived pointer aligned, in-bounds for the access, and
   tied to live storage or to this participant's mapping?
8. Will the operation update logical ownership exactly once?
9. Can a panic occur before the corresponding atomic publication?
10. How does the zero-sized case differ, if at all?

For final cleanup, reviewers MUST additionally prove that exactly one endpoint
won the final-share transition, the saved physical head survives sequence wrap,
and cleanup cannot revisit an element after its destructor begins. Deallocation
is legal only for heap-owned backing memory.

For borrowed/static storage, reviewers MUST prove that endpoint lifetimes cover
both control state and slots, split has one winner, reset requires exclusive
access, and forgetting endpoints remains sound without running their drops.

For raw shared regions, reviewers MUST prove checked offsets and atomic
alignment, valid construction before attachment, immutable-prefix validation
before native atomic access, exactly one claim per role and generation, local
pointer provenance without stored addresses, writable coherent mappings, no
conflicting whole-region references, and external quiescence before reuse.
