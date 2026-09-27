//! Generic core of the queue: slot storage, the producer/consumer state
//! machines, and the lifecycle abstraction shared by every storage mode.
//!
//! Everything here is generic over the sequence type `S` so that the
//! production `usize` instantiation and the narrow `u8` wraparound model
//! run byte-for-byte the same algorithm, and over a
//! lifecycle handle `L` so that heap-owned, borrowed, static, and
//! shared-region queues run the same data path. The public endpoint
//! wrappers fix `S = usize` and choose `L`.
//!
//! # Safety model
//!
//! An endpoint holds pointers to the two position atomics, to the slot
//! array, and a lifecycle handle `L`. The handle decides how long those
//! pointers stay valid and what happens when the endpoint is dropped:
//!
//! * typed queues ([`typed`]) keep a two-share control block; the endpoint
//!   whose share release observes the final share drops the queued values
//!   and, for the heap-owned mode only, frees the memory;
//! * shared regions (`crate::shared_memory`) only close the endpoint's
//!   role; the region is owned by the caller's mapping.
//!
//! Slot ownership is split by sequence position:
//!
//! * The producer is the only writer of `tail` and the only endpoint that
//!   initializes a free slot. It may only write slot `p` after an *acquire*
//!   load of `head` proved `p - head < capacity` (or after its cache of such a
//!   load proved it).
//! * The consumer is the only writer of `head` and the only endpoint that
//!   reads, borrows, or moves an occupied slot. It may only read slot `p` after
//!   an *acquire* load of `tail` proved `p < tail` (or its cache did).
//!
//! Every `SAFETY:` comment below refers to those two rules and to the
//! queue's standing invariants:
//!
//! * occupancy `tail - head` (in wrapping arithmetic) never exceeds
//!   `capacity`, and `capacity <= usize::MAX / 2`, so that distance is
//!   unambiguous and wrapped positions are never compared relationally;
//! * a slot is read as `T` only after an acquire observation of a `tail`
//!   publication covering it, and overwritten only after an acquire
//!   observation of a `head` publication releasing it;
//! * every `T` still queued at final destruction is dropped exactly once, and
//!   no free slot is ever treated as initialized; and
//! * slot pointers derive from the backing storage and are accessed only
//!   through `UnsafeCell`, except that zero-sized slots may use a dangling
//!   pointer (see [`Slot`]).

#[cfg(feature = "alloc")]
mod heap;
pub(crate) mod typed;

#[cfg(feature = "alloc")]
pub(crate) use heap::{HeapOwner, create};

use core::{cell::Cell, cmp::min, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use crate::{
    error::{CreateError, Full},
    seq::Sequence,
    sync::{Ordering, UnsafeCell},
};

/// Validates a requested capacity for the
/// heap-owned, borrowed, and shared modes. (`StaticStorage::new` checks `N`
/// at compile time instead.)
///
/// Returns only [`CreateError::ZeroCapacity`] or
/// [`CreateError::CapacityTooLarge`].
#[inline]
pub(crate) fn validate_capacity(capacity: usize, max: usize) -> Result<(), CreateError> {
    if capacity == 0 {
        return Err(CreateError::ZeroCapacity);
    }
    if capacity > max {
        return Err(CreateError::CapacityTooLarge {
            requested: capacity,
        });
    }
    Ok(())
}

/// Pads and aligns a value so that it occupies its own cache line(s).
///
/// 128 bytes on architectures whose prefetchers pull adjacent line pairs, 64
/// bytes elsewhere. Purely a performance measure.
#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64"
    ),
    repr(align(128))
)]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64"
    )),
    repr(align(64))
)]
pub(crate) struct CachePadded<T>(pub(crate) T);

/// One opaque, caller-ownable element slot.
///
/// A `Slot<T>` starts uninitialized and is only ever given meaning by a queue
/// that borrows it: [`BorrowedStorage`](crate::BorrowedStorage) takes a
/// slice of them, and [`StaticStorage`](crate::StaticStorage) holds an array
/// inline. The type offers no way to read, write, clone, or drop an element
/// from safe code; creating one never runs any `T` code, and dropping one
/// never drops a `T` (values still queued are dropped by the queue's final
/// endpoint instead).
///
/// `Slot<T>` has exactly the size and alignment of `T`, so an array of slots
/// is laid out like an array of `T`.
///
/// ```
/// use spookycircle::Slot;
///
/// // Neither `Copy` nor `Default` is needed to build an array of slots.
/// let slots = [const { Slot::<String>::new() }; 4];
/// assert_eq!(core::mem::size_of_val(&slots), 4 * core::mem::size_of::<String>());
/// ```
///
/// # Implementation notes
///
/// `MaybeUninit<T>` stops the compiler from assuming that a free slot holds a
/// valid `T`; `UnsafeCell` allows the two endpoints to mutate through shared
/// access to the backing storage. In production builds
/// the type is `#[repr(transparent)]` over `MaybeUninit<T>` (see `sync.rs`),
/// so `size_of::<Slot<T>>() == 0` exactly when `T` is zero-sized.
///
/// The `UnsafeCell` also makes `Slot<T>`, and through the `NonNull<Slot<T>>`
/// each endpoint keeps, both endpoints *invariant* in `T`. That is
/// load-bearing: a covariant `Producer<&'static str>` could be shortened to
/// `Producer<&'a str>` and used to hand the still-`'static` consumer a
/// dangling reference. `tests/compile_fail/invariant*.rs` check it.
///
/// Zero-sized `T`: accessing a zero-sized value reads and writes no bytes,
/// so it needs no provenance, and every slot may share any one aligned,
/// non-null address. The heap-owned mode allocates nothing and uses
/// [`NonNull::dangling`]; the other modes use whatever address their slot
/// storage has (itself dangling for, say, a `Vec` of zero-sized slots).
/// Materializing a `T` from a
/// zero-sized slot (by `assume_init_read`, `assume_init_ref`, or
/// `assume_init_mut`) is sound only for an occupied position: that `T` was
/// accepted from safe code by a successful push, which proves `T` is
/// inhabited and transfers ownership of that one logical instance.
#[repr(transparent)]
#[expect(
    missing_debug_implementations,
    reason = "`Slot` deliberately has no `Debug`; adding one is an API decision, not a lint fix"
)]
pub struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    const_unless_loom! {
        /// Creates an uninitialized slot. Never runs `T` code or allocates.
        #[allow(clippy::new_without_default)]
        #[inline]
        pub fn new() -> Self {
            Self {
                value: UnsafeCell::new(MaybeUninit::uninit()),
            }
        }
    }
}

/// How an endpoint pair observes liveness and ends its roles.
///
/// # Safety
///
/// Implementors must be plain handles that are sound to move to another
/// thread together with their endpoint whenever `T: Send`, and must keep the
/// position atomics and slot array handed to [`RawProducer::new`] /
/// [`RawConsumer::new`] valid until the corresponding `close_*` returns.
/// `producer_alive` / `consumer_alive` must be monotonic acquire loads
/// released by `close_producer` / `close_consumer` respectively.
pub(crate) unsafe trait Lifecycle<T, S: Sequence> {
    /// Acquire-loads whether the producer role may still publish.
    fn producer_alive(&self) -> bool;

    /// Acquire-loads whether the consumer role may still release.
    fn consumer_alive(&self) -> bool;

    /// Ends the producer role after its last publication.
    ///
    /// # Safety
    ///
    /// Called exactly once, from the producer's `Drop`, with that endpoint's
    /// own slot pointer and capacity. The endpoint is not used afterwards.
    unsafe fn close_producer(&self, slots: NonNull<Slot<T>>, capacity: usize);

    /// Ends the consumer role after its last release.
    ///
    /// # Safety
    ///
    /// As for `close_producer`, from the consumer's `Drop`; `head_index` is
    /// the consumer's actual physical head index.
    unsafe fn close_consumer(&self, slots: NonNull<Slot<T>>, capacity: usize, head_index: usize);
}

/// The fixed geometry of one queue: where its positions and slots live.
pub(crate) struct Parts<T, S: Sequence> {
    /// The consumer-owned position.
    pub(crate) head: NonNull<S::Atomic>,
    /// The producer-owned position.
    pub(crate) tail: NonNull<S::Atomic>,
    /// Start of the `capacity`-slot array (see [`Slot`] for zero-sized `T`).
    pub(crate) slots: NonNull<Slot<T>>,
    /// Number of logical slots, `1..=S::MAX_CAPACITY`.
    pub(crate) capacity: usize,
}

impl<T, S: Sequence> Clone for Parts<T, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, S: Sequence> Copy for Parts<T, S> {}

/// Returns the slot at physical `index` of the array starting at `slots`.
///
/// # Safety
///
/// `slots` must be the slot array of a live queue with at least `index + 1`
/// slots, and the storage must outlive `'a`. Every physical index kept by an
/// endpoint or by the cleanup guard is maintained in `0..capacity` by
/// [`next_index`].
#[inline(always)]
pub(crate) unsafe fn slot<'a, T>(slots: NonNull<Slot<T>>, index: usize) -> &'a Slot<T> {
    // SAFETY: `slots` is either the start of a live array of exactly
    // `capacity` slots and `index < capacity` (caller contract), so the
    // offset stays in bounds of that allocation; or `Slot<T>` is
    // zero-sized, and `add(index)` offsets by zero bytes.
    let ptr = unsafe { slots.as_ptr().add(index) };
    // SAFETY: `ptr` is aligned and points to a slot of the live array, which
    // outlives `'a` (caller contract); or `Slot<T>` is zero-sized and `ptr`
    // is non-null and aligned, which is all a reference to a zero-sized
    // type needs (see [`Slot`]). Slots are only accessed through
    // their `UnsafeCell`, so the shared reference never aliases a `&mut`.
    unsafe { &*ptr }
}

impl<T> Slot<T> {
    /// Drops the value in this slot in place.
    ///
    /// # Safety
    ///
    /// The slot must hold an initialized `T` exclusively owned by the caller,
    /// which must never read it again.
    #[inline]
    pub(crate) unsafe fn drop_value(&self) {
        // SAFETY: caller contract: `p` points to an initialized `T` that the
        // caller exclusively owns and never reads again.
        self.value
            .with_mut(|p| unsafe { core::ptr::drop_in_place(p.cast::<T>()) });
    }
}

/// Returns a pointer to slot `index` as a `*mut T`, for a bulk copy that
/// starts there. The pointer goes through the slot's `UnsafeCell` like every
/// other slot access, but is derived from the storage base, not from a
/// single-slot reference, so it may cover a run of slots.
///
/// # Safety
///
/// As for [`slot`].
#[cfg(not(loom))]
#[inline(always)]
unsafe fn run_start<T>(slots: NonNull<Slot<T>>, index: usize) -> *mut T {
    // SAFETY: `index` is in bounds of the live slot array (caller contract),
    // as in `slot`.
    let slot = unsafe { slots.as_ptr().add(index) };
    // SAFETY: `slot` points into the live slot array. `&raw const` names the
    // field without creating a reference, so the result keeps the storage
    // base's provenance over the whole run.
    let cell = unsafe { &raw const (*slot).value };
    // `MaybeUninit<T>` has the layout of `T`.
    UnsafeCell::raw_get(cell).cast::<T>()
}

/// Copies `source` into the `source.len()` physically contiguous slots
/// starting at `index`, as one `memcpy`.
///
/// # Safety
///
/// As for [`slot`] for every index in `index..index + source.len()`, and the
/// caller must hold producer ownership of those slots.
#[inline(always)]
unsafe fn write_run<T: Copy>(slots: NonNull<Slot<T>>, index: usize, source: &[T]) {
    #[cfg(not(loom))]
    {
        // SAFETY: caller contract, as for `slot`.
        let destination = unsafe { run_start(slots, index) };
        // SAFETY: `Slot<T>` is `repr(transparent)` over `MaybeUninit<T>`, so
        // the slot array is laid out exactly like `[T]`, and `destination`
        // is derived from the storage base, so it covers the whole run of
        // `source.len()` slots, which the caller owns as producer. `source`
        // is a shared slice that cannot overlap a slot we own.
        unsafe { core::ptr::copy_nonoverlapping(source.as_ptr(), destination, source.len()) };
    }
    #[cfg(loom)]
    for (k, &value) in source.iter().enumerate() {
        // SAFETY: caller contract, per slot; `index + k` cannot overflow,
        // as it stays below `capacity`.
        unsafe { slot(slots, index.wrapping_add(k)) }
            .value
            // SAFETY: the caller owns this free slot as producer.
            .with_mut(|p| unsafe { p.write(MaybeUninit::new(value)) });
    }
}

/// Copies the `destination.len()` physically contiguous slots starting at
/// `index` into `destination`, as one `memcpy`.
///
/// # Safety
///
/// As for [`slot`] for every index in `index..index + destination.len()`,
/// and every such slot must hold an initialized `T` that the caller may read.
#[inline(always)]
unsafe fn read_run<T: Copy>(slots: NonNull<Slot<T>>, index: usize, destination: &mut [T]) {
    #[cfg(not(loom))]
    {
        // SAFETY: caller contract, as for `slot`.
        let source = unsafe { run_start(slots, index) };
        // SAFETY: as in `write_run`, `source` covers the run of
        // `destination.len()` initialized slots the caller may read.
        // `destination` is an exclusive slice that cannot overlap the slot
        // array.
        unsafe {
            core::ptr::copy_nonoverlapping(source, destination.as_mut_ptr(), destination.len());
        }
    }
    #[cfg(loom)]
    for (k, out) in destination.iter_mut().enumerate() {
        // SAFETY: caller contract, per slot; `index + k` cannot overflow,
        // as it stays below `capacity`.
        *out = unsafe { slot(slots, index.wrapping_add(k)) }
            .value
            // SAFETY: the slot holds an initialized `T: Copy` the caller may
            // read.
            .with(|p| unsafe { p.cast::<T>().read() });
    }
}

/// Advances a physical index by one with a compare-to-capacity branch.
/// No division, no overflow: `index < capacity`
/// and `capacity <= usize::MAX / 2`.
///
/// Deliberately panic-free, even in debug builds: callers run it between a
/// slot ownership transition and its publication, where nothing may panic.
/// So it has no assertion, and it uses wrapping arithmetic
/// rather than `+`, whose debug-build overflow check would be a (provably
/// unreachable) panic path in that window. The `index < capacity`
/// precondition is checked by the `debug_assert!` that precedes every slot
/// access, before the transition.
#[inline(always)]
pub(crate) fn next_index(index: usize, capacity: usize) -> usize {
    let next = index.wrapping_add(1);
    if next == capacity { 0 } else { next }
}

/// Advances a physical index by `count` slots, `count <= capacity`.
///
/// `index + count < 2 * capacity <= usize::MAX`, so one subtraction wraps.
/// Panic-free for the same reason as [`next_index`]; callers check
/// `count <= capacity` before touching any slot.
#[inline(always)]
fn index_after(index: usize, count: usize, capacity: usize) -> usize {
    let next = index.wrapping_add(count);
    if next >= capacity {
        next.wrapping_sub(capacity)
    } else {
        next
    }
}

/// An endpoint pair in its initial state.
pub(crate) type Endpoints<T, S, L> = (RawProducer<T, S, L>, RawConsumer<T, S, L>);

/// An endpoint's reference to its queue: where the positions and slots
/// live, and the lifecycle handle that keeps them valid.
///
/// Both endpoint types hold one. It is created only by their constructors,
/// whose contract makes `life` keep `head`, `tail`, and `slots` valid until
/// the owning endpoint's `Drop` has run; that invariant is what makes the
/// position accessors safe.
///
/// Every field is an endpoint-local copy, so the scalar path touches shared
/// memory only through the two position atomics: a
/// control block contains atomics, so the compiler could not assume its
/// other fields unchanged across a slot write.
struct QueueRef<T, S: Sequence, L> {
    /// The consumer-owned position.
    head: NonNull<S::Atomic>,
    /// The producer-owned position.
    tail: NonNull<S::Atomic>,
    /// Start of the slot array.
    slots: NonNull<Slot<T>>,
    /// Number of logical slots.
    capacity: usize,
    /// Liveness and end-of-role behaviour for this storage mode.
    life: L,
}

impl<T, S: Sequence, L> QueueRef<T, S, L> {
    /// # Safety
    ///
    /// `life` must keep the storage described by `parts` valid until the
    /// `Drop` of the endpoint that will hold this reference has run.
    #[inline(always)]
    unsafe fn new(parts: Parts<T, S>, life: L) -> Self {
        Self {
            head: parts.head,
            tail: parts.tail,
            slots: parts.slots,
            capacity: parts.capacity,
            life,
        }
    }

    /// Returns the consumer-owned position.
    ///
    /// The reference borrows this `QueueRef`, which lives inside its
    /// endpoint, so it cannot outlive the endpoint. Borrowing only the
    /// `queue` field leaves the endpoint's other fields free to update while
    /// the reference is in use. The one window the borrow checker cannot
    /// rule out is the endpoint's own `Drop` after `close_*`, which calls
    /// neither accessor.
    #[inline(always)]
    fn shared_head(&self) -> &S::Atomic {
        // SAFETY: the lifecycle handle keeps the position atomics valid
        // until the owning endpoint's `Drop` runs (`QueueRef::new` contract),
        // and the returned borrow of `self` ends before then.
        unsafe { self.head.as_ref() }
    }

    /// Returns the producer-owned position; see `shared_head`.
    #[inline(always)]
    fn shared_tail(&self) -> &S::Atomic {
        // SAFETY: as for `shared_head`.
        unsafe { self.tail.as_ref() }
    }
}

/// Producer-side state: the only writer of `tail`.
pub(crate) struct RawProducer<T, S: Sequence, L: Lifecycle<T, S>> {
    /// Positions, slots, and lifecycle. The producer acquire-loads `head`
    /// on refresh and release-stores `tail` on publication.
    queue: QueueRef<T, S, L>,
    /// Next position to publish. Always equals the shared `tail`.
    tail: S,
    /// The cached head stored in the form
    /// `cached_head + capacity`: the first `tail` value at which the cache
    /// can no longer prove a free slot. The fast-path test is then a single
    /// equality comparison, as it is on the consumer side.
    full_at: S,
    /// Physical slot for `tail`: the number of positions this producer has
    /// published, modulo capacity.
    index: usize,
    /// Suppresses `Sync`.
    _not_sync: PhantomData<Cell<()>>,
    /// Zero-sized; aligns, and so pads, the endpoint to its own cache
    /// line(s), so that a producer and a consumer stored side by side (for
    /// example in the tuple returned by `bounded`) never share a line: each
    /// endpoint's local positions are written on every operation by its own
    /// thread.
    _line: CachePadded<()>,
}

/// Consumer-side state: the only writer of `head`.
pub(crate) struct RawConsumer<T, S: Sequence, L: Lifecycle<T, S>> {
    /// Positions, slots, and lifecycle. The consumer acquire-loads `tail`
    /// on refresh and release-stores `head` on release.
    queue: QueueRef<T, S, L>,
    /// Next position to remove. Always equals the shared `head`.
    head: S,
    /// Most recent acquire-loaded `tail`; may lag behind the real value.
    cached_tail: S,
    /// Physical slot for `head`: the number of positions this consumer has
    /// released, modulo capacity.
    index: usize,
    /// Loom only: the tracked access backing the most recent `peek` or
    /// `peek_mut` result. See `PeekAccess`.
    #[cfg(loom)]
    peek_access: PeekAccess<T>,
    /// Suppresses `Sync`.
    _not_sync: PhantomData<Cell<()>>,
    /// Zero-sized; aligns, and so pads, the endpoint to its own cache
    /// line(s). See `RawProducer`'s field of the same name.
    _line: CachePadded<()>,
}

/// Loom's record of a `peek` or `peek_mut` access to the head slot.
///
/// Loom checks a cell access only while its closure or guard is live, and a
/// peek result outlives any closure. The consumer therefore keeps the guard
/// until its next `&mut self` operation (or its `Drop`), which cannot start
/// before the returned reference is dead. The guard may outlive the
/// reference slightly, but that window only covers an occupied slot, which
/// the producer never touches, so it adds no false positives.
#[cfg(loom)]
enum PeekAccess<T> {
    None,
    Shared(
        #[expect(dead_code, reason = "held only for its drop")]
        crate::sync::ConstPtr<MaybeUninit<T>>,
    ),
    Exclusive(
        #[expect(dead_code, reason = "held only for its drop")] crate::sync::MutPtr<MaybeUninit<T>>,
    ),
}

// SAFETY: a producer holds pointers into storage kept live by its lifecycle
// handle (which `Lifecycle` requires to be movable between threads), plus
// plain local integers. Moving it moves the unique producer role; no two
// threads can act as producer at once because the type is neither `Clone`
// nor `Sync`. Values of type `T` move from the producer thread to the
// consumer thread through a slot but are never accessed by both at once, so
// `T: Send` suffices and `T: Sync` is not required. The
// final endpoint may drop queued `T`s on whichever thread it runs on, which
// `T: Send` also covers.
unsafe impl<T: Send, S: Sequence, L: Lifecycle<T, S>> Send for RawProducer<T, S, L> {}

// SAFETY: as for `RawProducer`, for the unique consumer role.
unsafe impl<T: Send, S: Sequence, L: Lifecycle<T, S>> Send for RawConsumer<T, S, L> {}

/// Creates a producer/consumer pair in the initial state.
///
/// # Safety
///
/// `parts` must describe a queue with `head == tail == 0` and no initialized
/// slot, whose storage `life` keeps valid for both endpoints; and no other
/// endpoint of either role may exist for it.
pub(crate) unsafe fn endpoints<T, S: Sequence, L: Lifecycle<T, S> + Copy>(
    parts: Parts<T, S>,
    life: L,
) -> Endpoints<T, S, L> {
    // SAFETY: forwarded caller contract; this is the queue's only producer.
    let producer = unsafe { RawProducer::new(parts, life) };
    // SAFETY: forwarded caller contract; this is the queue's only consumer.
    let consumer = unsafe { RawConsumer::new(parts, life) };
    (producer, consumer)
}

impl<T, S: Sequence, L: Lifecycle<T, S>> RawProducer<T, S, L> {
    /// Creates the producer for a queue whose producer has published nothing.
    ///
    /// # Safety
    ///
    /// `parts` must describe live storage that `life` keeps valid for this
    /// endpoint, with `tail == 0`; and this must be the only producer ever
    /// created for that generation of the queue. The zero-valued cached
    /// head is justified because `head` never exceeds `tail`.
    pub(crate) unsafe fn new(parts: Parts<T, S>, life: L) -> Self {
        Self {
            // SAFETY: forwarded caller contract.
            queue: unsafe { QueueRef::new(parts, life) },
            tail: S::ZERO,
            full_at: S::ZERO.advance(parts.capacity),
            index: 0,
            _not_sync: PhantomData,
            _line: CachePadded(()),
        }
    }

    /// Acquire-loads the consumer's `head`, stores it in the cache as
    /// `full_at`, and returns the new `full_at`.
    ///
    /// This is the only place the data path acquires `head`; every slot
    /// write is justified by a value loaded here, either just now or
    /// earlier and kept in the cache.
    #[inline(always)]
    fn refresh_full_at(&mut self) -> S {
        // Read before the acquire load, which would otherwise force a
        // reload of the field (see `RawConsumer::has_available`).
        let capacity = self.queue.capacity;
        let head = S::load(self.queue.shared_head(), Ordering::Acquire);
        let full_at = head.advance(capacity);
        self.full_at = full_at;
        full_at
    }

    /// Pushes one value, or returns it in `Full` if no slot is free.
    #[inline]
    pub(crate) fn try_push(&mut self, value: T) -> Result<(), Full<T>> {
        let capacity = self.queue.capacity;
        let tail = self.tail;

        // `tail == cached_head + capacity` is `tail - cached_head == capacity`
        // in wrapping arithmetic.
        if tail == self.full_at {
            // The cache cannot prove space; refresh once.
            let full_at = self.refresh_full_at();
            if tail == full_at {
                // Linearization point of the `Full` result.
                return Err(Full::new(value));
            }
        }

        let index = self.index;
        debug_assert!(index < capacity);
        // SAFETY: `index` is this producer's physical tail, kept in
        // `0..capacity` by `next_index`/`index_after`, and the lifecycle
        // handle keeps the slot array live.
        let cell = unsafe { slot(self.queue.slots, index) };
        cell.value.with_mut(|p| {
            // SAFETY: producer write. This endpoint is the sole
            // producer and `index` is the physical slot of position `tail`.
            // The check above proved `tail - head' < capacity` for some `head'`
            // acquire-loaded from the consumer, so the consumer released
            // position `tail - capacity` (the previous occupant of this
            // physical slot) with a release store that our acquire load
            // synchronized with; its access to the old value happens-before
            // this write. Position `tail` is not yet published, so the consumer
            // holds no reference to the slot. The slot is therefore free, and
            // writing a `MaybeUninit<T>` needs no prior initialization.
            unsafe { p.write(MaybeUninit::new(value)) }
        });

        // Nothing below can panic: wrapping arithmetic, a branch, and
        // an atomic store.
        let next_tail = tail.advance(1);
        self.tail = next_tail;
        self.index = next_index(index, capacity);
        // Linearization point of success; publishes the slot write.
        S::store(self.queue.shared_tail(), next_tail, Ordering::Release);
        Ok(())
    }

    /// Copies as many leading elements of `source` as fit; returns the count.
    #[inline]
    pub(crate) fn push_slice(&mut self, source: &[T]) -> usize
    where
        T: Copy,
    {
        if source.is_empty() {
            return 0;
        }
        let capacity = self.queue.capacity;
        let tail = self.tail;

        // `full_at - tail == capacity - (tail - cached_head)`.
        let mut free = self.full_at.distance(tail);
        if free < source.len() {
            // Cache cannot prove the whole slice fits; refresh once.
            free = self.refresh_full_at().distance(tail);
        }
        let count = min(source.len(), free);
        if count == 0 {
            return 0;
        }

        let index = self.index;
        // Checked here, before any slot is written, so that the assertion-free
        // window after the writes holds.
        debug_assert!(index < capacity && count <= capacity);
        // Copy in at most two physically contiguous segments; total loop
        // trips are `count <= min(source.len(), capacity)`.
        let first = min(count, capacity.wrapping_sub(index));
        // Both segments are sliced here, before any slot is written, so no
        // bounds check sits between the writes and the publication.
        #[expect(
            clippy::indexing_slicing,
            reason = "`count <= source.len()` by the `min` above; the codegen audit confirms no panic path"
        )]
        let (before_wrap, after_wrap) = source[..count].split_at(first);
        // SAFETY: as in `try_push`: positions `tail .. tail + count` were
        // proved free by the acquire-loaded (or cached) head, the physical
        // slots `index .. index + first` map to the first `first` of them,
        // and nothing is published until the store below.
        unsafe { write_run(self.queue.slots, index, before_wrap) };
        if !after_wrap.is_empty() {
            // SAFETY: as above; physical slots `0 .. count - first` map to
            // the remaining positions after the wrap.
            unsafe { write_run(self.queue.slots, 0, after_wrap) };
        }

        let next_tail = tail.advance(count);
        self.tail = next_tail;
        self.index = index_after(index, count, capacity);
        // Single release store publishes the whole batch.
        S::store(self.queue.shared_tail(), next_tail, Ordering::Release);
        count
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.queue.capacity
    }

    /// Occupancy snapshot: one acquire load of `head`.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        let head = S::load(self.queue.shared_head(), Ordering::Acquire);
        self.tail.distance(head)
    }

    /// Whether the consumer endpoint has not yet been dropped.
    #[inline]
    pub(crate) fn is_consumer_alive(&self) -> bool {
        self.queue.life.consumer_alive()
    }
}

impl<T, S: Sequence, L: Lifecycle<T, S>> RawConsumer<T, S, L> {
    /// Creates the consumer for a queue whose consumer has released nothing.
    ///
    /// # Safety
    ///
    /// As for [`RawProducer::new`], with `head == 0` and for the consumer
    /// role. The zero-valued cached tail only underestimates availability.
    pub(crate) unsafe fn new(parts: Parts<T, S>, life: L) -> Self {
        Self {
            // SAFETY: forwarded caller contract.
            queue: unsafe { QueueRef::new(parts, life) },
            head: S::ZERO,
            cached_tail: S::ZERO,
            index: 0,
            #[cfg(loom)]
            peek_access: PeekAccess::None,
            _not_sync: PhantomData,
            _line: CachePadded(()),
        }
    }

    /// Ends Loom's tracking of the previous `peek`/`peek_mut` access (see
    /// `PeekAccess`). Every `&mut self` method that touches a slot, and
    /// `Drop`, calls it first. A no-op outside Loom.
    #[inline(always)]
    #[cfg_attr(
        not(loom),
        expect(clippy::unused_self, reason = "the receiver is used only under Loom")
    )]
    fn end_peek(&mut self) {
        #[cfg(loom)]
        {
            self.peek_access = PeekAccess::None;
        }
    }

    /// Acquire-loads the producer's `tail`, stores it in the cache, and
    /// returns it.
    ///
    /// This is the only place the data path acquires `tail` before reading
    /// a slot; every slot read is justified by a value loaded here, either
    /// just now or earlier and kept in the cache. (`len` and `is_drained`
    /// load `tail` too, but only to answer a query.)
    #[inline(always)]
    fn refresh_tail(&mut self) -> S {
        let tail = S::load(self.queue.shared_tail(), Ordering::Acquire);
        self.cached_tail = tail;
        tail
    }

    /// Returns whether position `head` is occupied, refreshing the cached
    /// tail at most once.
    ///
    /// `head` must be `self.head`. It is passed in, rather than read here, so
    /// that callers hold it in a local across the acquire load: the compiler
    /// does not reuse a field load across an acquire, and would otherwise
    /// load `self.head` again after the refresh.
    ///
    /// A `true` result is justified either by the cache (which only ever
    /// holds values acquire-loaded from `tail`) or by the fresh acquire load
    /// performed here; either way the producer's initialization of slot
    /// `head` happens-before any subsequent read of it.
    #[inline(always)]
    fn has_available(&mut self, head: S) -> bool {
        debug_assert_eq!(head, self.head);
        if head == self.cached_tail {
            let tail = self.refresh_tail();
            if head == tail {
                // Linearization point of the empty result.
                return false;
            }
        }
        true
    }

    /// Pops the oldest value, or returns `None` if the queue is empty.
    #[inline]
    pub(crate) fn try_pop(&mut self) -> Option<T> {
        self.end_peek();
        let head = self.head;
        if !self.has_available(head) {
            return None;
        }
        let index = self.index;
        debug_assert!(index < self.queue.capacity);
        // SAFETY: `index` is this consumer's physical head, kept in
        // `0..capacity` by `next_index`/`index_after`, and the lifecycle
        // handle keeps the slot array live.
        let cell = unsafe { slot(self.queue.slots, index) };
        let value = cell.value.with(|p| {
            // SAFETY: consumer read. This endpoint is the sole
            // consumer, `index` is the physical slot of position `head`, and
            // `has_available` proved `head < tail'` for a `tail'`
            // acquire-loaded from the producer, so the producer's write of
            // this slot happens-before this read and the slot holds an
            // initialized `T`. The producer will not touch the slot again
            // until `head` passes it, which only the store below can cause. No
            // `peek` reference can be live because `peek` borrows `&mut self`
            // for its result's lifetime. Reading by value moves logical
            // ownership out exactly once; the slot becomes free at the store
            // below and is never read as `T` again.
            unsafe { p.cast::<T>().read() }
        });
        self.release_one(head, index);
        Some(value)
    }

    /// `try_pop`, but the value is moved
    /// straight into `destination` before the release store.
    ///
    /// `try_pop` has to hold the value in a local across the release store
    /// and move it into the caller's place afterwards; the compiler may not
    /// move that second copy above the store, so a value too large for
    /// registers goes through the stack. Here the only copy is slot to
    /// `destination`.
    #[inline]
    pub(crate) fn try_pop_into<'d>(
        &mut self,
        destination: &'d mut MaybeUninit<T>,
    ) -> Option<&'d mut T> {
        self.end_peek();
        let head = self.head;
        if !self.has_available(head) {
            return None;
        }
        let index = self.index;
        debug_assert!(index < self.queue.capacity);
        // SAFETY: as for the slot lookup in `try_pop`.
        let cell = unsafe { slot(self.queue.slots, index) };
        let out: *mut MaybeUninit<T> = destination;
        cell.value.with(|p| {
            // SAFETY: consumer read, exactly as in `try_pop`: the slot holds
            // an initialized `T` published to this consumer and no reference
            // to it is live, so moving it out by a bitwise copy is sound.
            // `destination` is an exclusive borrow, so it cannot overlap the
            // slot array (which the queue borrows or owns), and a
            // `MaybeUninit<T>` has `T`'s size and alignment. Writing through
            // `MaybeUninit` drops nothing that was in `destination`, so no
            // user code runs before the release store. Logical ownership
            // moves to `destination` exactly once; the slot becomes free at
            // the store below and is never read as `T` again.
            unsafe { core::ptr::copy_nonoverlapping(p, out, 1) }
        });
        self.release_one(head, index);
        // SAFETY: initialized by the copy above.
        Some(unsafe { destination.assume_init_mut() })
    }

    /// Releases position `head` (physical slot `index`) after its value has
    /// been moved out: the tail of `try_pop` and `try_pop_into`.
    ///
    /// `head` and `index` must be `self.head` and `self.index`, passed in so
    /// that callers keep them in registers across the acquire load (see
    /// `has_available`).
    #[inline(always)]
    fn release_one(&mut self, head: S, index: usize) {
        // Nothing below can panic.
        let next_head = head.advance(1);
        self.head = next_head;
        self.index = next_index(index, self.queue.capacity);
        // Linearization point of success; orders our read before
        // any producer reuse of the slot.
        S::store(self.queue.shared_head(), next_head, Ordering::Release);
    }

    /// Returns the slot at the physical head; the shared lookup of `peek`
    /// and `peek_mut`.
    ///
    /// Callers check `has_available` first, so the slot holds a published,
    /// initialized `T` (as in `try_pop`). The emptiness check stays in the
    /// callers: returning `Option<&Slot<T>>` from here stops LLVM from
    /// folding the address arithmetic into the slot load.
    #[inline(always)]
    fn head_slot(&self) -> &Slot<T> {
        debug_assert!(self.index < self.queue.capacity);
        // SAFETY: as for the slot lookup in `try_pop`: `self.index` is the
        // physical head, in `0..capacity`, and the array is live.
        unsafe { slot(self.queue.slots, self.index) }
    }

    /// Borrows the oldest value without removing it.
    #[inline]
    pub(crate) fn peek(&mut self) -> Option<&T> {
        self.end_peek();
        if !self.has_available(self.head) {
            return None;
        }
        #[cfg(not(loom))]
        let p = self.head_slot().value.with(|p| p);
        // Under Loom the pointer comes from a tracked guard, kept until the
        // next consumer operation, so the model sees the whole borrow.
        #[cfg(loom)]
        let p = {
            let access = self.head_slot().value.get();
            let p = access.with(|p| p);
            self.peek_access = PeekAccess::Shared(access);
            p
        };
        // SAFETY: as in `try_pop`, position `head` is published and
        // initialized, so `*p` is a valid `T`. `head` is not advanced here,
        // so the producer cannot reuse the slot while the returned reference
        // is live: the reference borrows `&mut self`, and every operation
        // that could advance `head` needs `&mut self` too. The pointer comes
        // from the slot's `UnsafeCell`, so it is aligned, in bounds, and
        // tied to the live backing storage.
        Some(unsafe { &*p.cast::<T>() })
    }

    /// Mutably borrows the oldest value without removing it.
    #[inline]
    pub(crate) fn peek_mut(&mut self) -> Option<&mut T> {
        self.end_peek();
        if !self.has_available(self.head) {
            return None;
        }
        #[cfg(not(loom))]
        let p = self.head_slot().value.with_mut(|p| p);
        // Tracked for the whole borrow under Loom, as in `peek`.
        #[cfg(loom)]
        let p = {
            let access = self.head_slot().value.get_mut();
            let p = access.with(|p| p);
            self.peek_access = PeekAccess::Exclusive(access);
            p
        };
        // SAFETY: as in `peek`. Exclusivity: the producer never accesses an
        // occupied slot, this is the only consumer, and the returned
        // reference holds the `&mut self` borrow, so no other reference to
        // this `T` can exist while it is live.
        Some(unsafe { &mut *p.cast::<T>() })
    }

    /// Copies as many queued elements into `destination` as fit; returns the
    /// count.
    #[inline]
    pub(crate) fn pop_slice(&mut self, destination: &mut [T]) -> usize
    where
        T: Copy,
    {
        self.end_peek();
        if destination.is_empty() {
            return 0;
        }
        let capacity = self.queue.capacity;
        let head = self.head;

        let mut available = self.cached_tail.distance(head);
        if available < destination.len() {
            // Cache cannot prove enough data; refresh once.
            available = self.refresh_tail().distance(head);
        }
        let count = min(destination.len(), available);
        if count == 0 {
            return 0;
        }

        let index = self.index;
        // Checked here, before any slot is read, so that the assertion-free
        // window after the reads holds.
        debug_assert!(index < capacity && count <= capacity);
        let first = min(count, capacity.wrapping_sub(index));
        // Both segments are sliced here, before any slot is read, so no
        // bounds check sits between the reads and the release.
        #[expect(
            clippy::indexing_slicing,
            reason = "`count <= destination.len()` by the `min` above; the codegen audit confirms no panic path"
        )]
        let (before_wrap, after_wrap) = destination[..count].split_at_mut(first);
        // SAFETY: as in `try_pop`: positions `head .. head + count` are
        // published and initialized per the acquire-loaded (or cached) tail,
        // and physical slots `index .. index + first` hold the first `first`
        // of them. `T: Copy`, so reading by value duplicates nothing that
        // has a destructor and the slots simply become free once the store
        // below releases them.
        unsafe { read_run(self.queue.slots, index, before_wrap) };
        if !after_wrap.is_empty() {
            // SAFETY: as above; physical slots `0 .. count - first` hold the
            // remaining positions after the wrap.
            unsafe { read_run(self.queue.slots, 0, after_wrap) };
        }

        let next_head = head.advance(count);
        self.head = next_head;
        self.index = index_after(index, count, capacity);
        // Single release store releases the whole batch.
        S::store(self.queue.shared_head(), next_head, Ordering::Release);
        count
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.queue.capacity
    }

    /// Occupancy snapshot: one acquire load of `tail`.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        let tail = S::load(self.queue.shared_tail(), Ordering::Acquire);
        tail.distance(self.head)
    }

    /// Whether the producer endpoint has not yet been dropped.
    #[inline]
    pub(crate) fn is_producer_alive(&self) -> bool {
        self.queue.life.producer_alive()
    }

    /// Whether the producer is gone and every value it pushed has been popped.
    ///
    /// The producer's `Drop` release-stores its liveness change after its
    /// final `tail` publication. Acquire-loading "dead" here therefore
    /// synchronizes with the complete publication history, and the acquire
    /// load of `tail` that follows in program order cannot observe an older
    /// value. If it equals `head`, nothing is queued and nothing ever will be.
    #[inline]
    pub(crate) fn is_drained(&self) -> bool {
        if self.queue.life.producer_alive() {
            return false;
        }
        S::load(self.queue.shared_tail(), Ordering::Acquire) == self.head
    }
}

impl<T, S: Sequence, L: Lifecycle<T, S>> Drop for RawProducer<T, S, L> {
    fn drop(&mut self) {
        // SAFETY: called once, from this endpoint's `Drop`, after every
        // preceding tail publication in program order.
        unsafe {
            self.queue
                .life
                .close_producer(self.queue.slots, self.queue.capacity);
        }
    }
}

impl<T, S: Sequence, L: Lifecycle<T, S>> Drop for RawConsumer<T, S, L> {
    fn drop(&mut self) {
        // Final cleanup may drop the peeked value, so Loom's record of the
        // last peek must end first.
        self.end_peek();
        // SAFETY: called once, from this endpoint's `Drop`, with the actual
        // physical head index so that final cleanup can locate the occupied
        // range even after a sequence wrap has made `head % capacity`
        // meaningless.
        unsafe {
            self.queue
                .life
                .close_consumer(self.queue.slots, self.queue.capacity, self.index);
        }
    }
}

#[cfg(kani)]
// Proof code, like `narrow_tests`: Kani itself fails on any overflow or
// out-of-bounds index.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod proofs;
