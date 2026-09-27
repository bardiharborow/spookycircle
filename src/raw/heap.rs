//! The heap-owned mode (`alloc` feature): one fallible allocation for the
//! control block and one for the slot array.

use alloc::alloc::{alloc, dealloc};
use core::{
    alloc::Layout,
    mem::align_of,
    ptr::{self, NonNull},
};

use super::{
    CachePadded, Endpoints, Slot, endpoints,
    typed::{Control, Owner, TypedLife},
    validate_capacity,
};
use crate::{error::CreateError, seq::Sequence};

/// Cache-line alignment used for the slot array allocation.
const SLOT_ARRAY_ALIGN: usize = align_of::<CachePadded<()>>();

/// The heap allocation behind a heap-owned queue's control block.
///
/// `repr(C)` puts `control` at offset 0, so the `NonNull<Control<S>>` held
/// by the endpoints is also a pointer to this whole allocation.
#[repr(C)]
struct HeapShared<S: Sequence> {
    control: Control<S>,
    /// Layout of the slot allocation; `None` when nothing was allocated.
    slots_layout: Option<Layout>,
}

/// The heap-owned mode: the final endpoint frees both allocations.
pub(crate) enum HeapOwner {}

impl Owner for HeapOwner {
    unsafe fn release<T, S: Sequence>(
        control: NonNull<Control<S>>,
        slots: NonNull<Slot<T>>,
        capacity: usize,
    ) {
        let shared = control.cast::<HeapShared<S>>();
        // SAFETY: `control` is the start of a live `HeapShared` allocation
        // made by `create` (repr(C), offset 0), exclusively owned by the
        // caller (`Owner` contract).
        let block = unsafe { shared.as_ref() };
        // Read before `drop_in_place` ends the block's lifetime.
        let slots_layout = block.slots_layout;
        #[cfg(loom)]
        // SAFETY: Loom's cell carries state that should be dropped; the
        // slot array is live, exclusively ours, and never used again. In
        // production a `Slot<T>` has no destructor, so this is deliberately
        // not compiled: the slice is never treated as `[T]`.
        unsafe {
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(slots.as_ptr(), capacity));
        }
        #[cfg(not(loom))]
        let _ = capacity;
        if let Some(layout) = slots_layout {
            // SAFETY: the slot array was allocated by `create` with exactly
            // this layout and is exclusively ours; nothing accesses it after
            // this point.
            unsafe { dealloc(slots.as_ptr().cast(), layout) };
        }
        // SAFETY: the block is live and exclusively ours (see above), and
        // `block` is not used again; this ends its lifetime.
        unsafe { ptr::drop_in_place(shared.as_ptr()) };
        // SAFETY: the block was allocated by `create` with this layout, its
        // contents were dropped just above, and nothing accesses it after
        // this point.
        unsafe { dealloc(shared.as_ptr().cast(), Layout::new::<HeapShared<S>>()) };
    }
}

/// Validates `capacity`, allocates the shared state, and returns the two
/// endpoints.
pub(crate) fn create<T, S: Sequence>(
    capacity: usize,
) -> Result<Endpoints<T, S, TypedLife<S, HeapOwner>>, CreateError> {
    validate_capacity(capacity, S::MAX_CAPACITY)?;
    let too_large = |_| CreateError::CapacityTooLarge {
        requested: capacity,
    };
    // Align the slot array to a cache line, and pad its size to a whole
    // number of lines, so that neither its first nor its last line is ever
    // shared with an unrelated allocation.
    // `pad_to_align` cannot overflow: a `Layout`'s size rounded up to its
    // alignment is guaranteed to fit in `isize`.
    let slots_layout = Layout::array::<Slot<T>>(capacity)
        .and_then(|layout| layout.align_to(SLOT_ARRAY_ALIGN))
        .map(|layout| layout.pad_to_align())
        .map_err(too_large)?;
    // A zero-sized slot array needs no allocation.
    let slots_layout = (slots_layout.size() != 0).then_some(slots_layout);
    let control_layout = Layout::new::<HeapShared<S>>();

    let slot_array: Option<NonNull<Slot<T>>> = match slots_layout {
        // SAFETY: `layout` has nonzero size, as required by `alloc`.
        Some(layout) => Some(
            NonNull::new(unsafe { alloc(layout) }.cast::<Slot<T>>())
                .ok_or(CreateError::AllocationFailed)?,
        ),
        None => None,
    };

    // Production slots need no initialization: a `MaybeUninit<T>` inside a
    // `repr(transparent)` `UnsafeCell` has no validity requirement. Loom's
    // instrumented cell carries bookkeeping that must be constructed (and is
    // never zero-sized, so the array always exists under Loom).
    #[cfg(loom)]
    if let Some(slots) = slot_array {
        for i in 0..capacity {
            // SAFETY: `i < capacity`, so the offset stays in the allocation.
            let slot = unsafe { slots.as_ptr().add(i) };
            // SAFETY: the allocation is live, writable, and aligned for
            // `Slot<T>`, and nothing else refers to it yet.
            unsafe { slot.write(Slot::new()) }
        }
    }

    debug_assert!(control_layout.size() != 0);
    // SAFETY: `HeapShared` contains atomics, so its layout has nonzero size.
    let Some(shared) = NonNull::new(unsafe { alloc(control_layout) }.cast::<HeapShared<S>>())
    else {
        if let (Some(slots), Some(layout)) = (slot_array, slots_layout) {
            // SAFETY: `slots` was allocated above with exactly this layout and
            // has not been handed to anyone.
            unsafe { dealloc(slots.as_ptr().cast(), layout) }
        }
        return Err(CreateError::AllocationFailed);
    };
    // Zero-sized slots need no provenance, only a non-null, aligned address
    // (see `Slot`).
    let slots = slot_array.unwrap_or(NonNull::dangling());

    // SAFETY: `shared` is a fresh, aligned, writable allocation sized for
    // `HeapShared<S>`, written exactly once before either endpoint exists.
    unsafe {
        shared.as_ptr().write(HeapShared {
            control: Control::new(),
            slots_layout,
        });
    }
    let control = shared.cast::<Control<S>>();
    // SAFETY: the block was just initialized and is not yet shared.
    let parts = unsafe { control.as_ref() }.parts(slots, capacity);
    // SAFETY: a fresh control block has zero positions, both roles alive,
    // and two shares; no slot is initialized; the allocation stays live
    // until the final share is released, and these are the only endpoints.
    Ok(unsafe { endpoints(parts, TypedLife::new(control)) })
}
