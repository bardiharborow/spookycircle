//! The version-1 byte format at fixed offsets: initialization, header
//! validation, and the public attach functions.
//!
//! Every atomic word here is a real `AtomicUsize` at a fixed byte offset,
//! which Loom cannot model, so the parent module compiles this layer only
//! without `--cfg loom`. The claim and lifecycle logic it hands off to lives
//! in `protocol`, which the Loom tests exercise directly.

use core::{
    alloc::Layout,
    mem::{align_of, offset_of, size_of},
    ptr::{self, NonNull},
};

use super::{
    FORMAT_VERSION, HEADER_BYTES, REGION_ALIGN, SharedConsumer, SharedError, SharedProducer,
    layout,
    protocol::{READY, RegionPtrs},
};
use crate::sync::{AtomicUsize, Ordering};

/// Magic bytes at offset 0.
const MAGIC: [u8; 8] = *b"WFSPSC01";
/// Width of every atomic word in the region.
const W: usize = size_of::<usize>();

/// The version-1 header, as a layout description (the table in the parent
/// module's documentation).
///
/// Never instantiated, read, or referenced: the region is only ever
/// accessed through byte copies of the immutable prefix and through
/// pointers to individual atomic words, so that no native word is touched
/// before the prefix proves the ABI matches, and no plain read races a
/// peer's atomic store. The struct exists so that the compiler computes the
/// offsets below; the assertions after them pin those offsets to the
/// documented format.
///
/// The prefix fields are byte arrays holding little-endian integers, so
/// they have alignment 1 and no implicit padding.
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: [u8; 4],
    word_width: u8,
    byte_order: u8,
    atomic_align: u8,
    reserved_byte: u8,
    region_size: [u8; 8],
    capacity: [u8; 8],
    record_bytes: [u8; 8],
    slots_offset: [u8; 8],
    generation: [u8; 8],
    reserved_word: [u8; 8],
    ready: AtomicUsize,
    producer_role: AtomicUsize,
    consumer_role: AtomicUsize,
    reserved_after_roles: [u8; RESERVED_AFTER_ROLES],
    head: AtomicUsize,
    reserved_after_head: [u8; RESERVED_AFTER_POSITION],
    tail: AtomicUsize,
    reserved_after_tail: [u8; RESERVED_AFTER_POSITION],
}

/// Reserved bytes that put `head` on the header's third 64-byte line.
const RESERVED_AFTER_ROLES: usize = 64 - 3 * W;
/// Reserved bytes that fill out the line of `head`, and of `tail`.
const RESERVED_AFTER_POSITION: usize = 64 - W;

const OFF_MAGIC: usize = offset_of!(Header, magic);
const OFF_VERSION: usize = offset_of!(Header, version);
const OFF_WORD_WIDTH: usize = offset_of!(Header, word_width);
const OFF_BYTE_ORDER: usize = offset_of!(Header, byte_order);
const OFF_ATOMIC_ALIGN: usize = offset_of!(Header, atomic_align);
const OFF_RESERVED_BYTE: usize = offset_of!(Header, reserved_byte);
const OFF_REGION_SIZE: usize = offset_of!(Header, region_size);
const OFF_CAPACITY: usize = offset_of!(Header, capacity);
const OFF_RECORD_BYTES: usize = offset_of!(Header, record_bytes);
const OFF_SLOTS_OFFSET: usize = offset_of!(Header, slots_offset);
const OFF_GENERATION: usize = offset_of!(Header, generation);
const OFF_RESERVED_WORD: usize = offset_of!(Header, reserved_word);
/// End of the immutable, non-atomic prefix.
const PREFIX_BYTES: usize = offset_of!(Header, ready);
const OFF_READY: usize = offset_of!(Header, ready);
const OFF_PRODUCER_ROLE: usize = offset_of!(Header, producer_role);
const OFF_CONSUMER_ROLE: usize = offset_of!(Header, consumer_role);
const OFF_RESERVED_AFTER_ROLES: usize = offset_of!(Header, reserved_after_roles);
const OFF_HEAD: usize = offset_of!(Header, head);
const OFF_RESERVED_AFTER_HEAD: usize = offset_of!(Header, reserved_after_head);
const OFF_TAIL: usize = offset_of!(Header, tail);
const OFF_RESERVED_AFTER_TAIL: usize = offset_of!(Header, reserved_after_tail);

/// Byte-order marker for this target.
const BYTE_ORDER: u8 = if cfg!(target_endian = "little") { 1 } else { 2 };

// The format is an external contract: the compiler-computed offsets must
// equal the documented table, whatever the field order above says. `core`'s
// `AtomicUsize` is documented to have the size of `usize` and alignment
// equal to its size, so a `repr(C)` struct inserts no padding before the
// words (every offset is a multiple of 64 or of `W`), and `W`-byte words at
// these offsets are aligned whenever the base is `REGION_ALIGN`-aligned.
const _: () = {
    assert!(W == 4 || W == 8);
    assert!(usize::BITS <= u64::BITS);
    assert!(size_of::<AtomicUsize>() == W);
    assert!(align_of::<AtomicUsize>() == W);
    assert!(REGION_ALIGN.is_multiple_of(align_of::<Header>()));
    assert!(size_of::<Header>() == HEADER_BYTES);

    assert!(OFF_MAGIC == 0);
    assert!(OFF_VERSION == 8);
    assert!(OFF_WORD_WIDTH == 12);
    assert!(OFF_BYTE_ORDER == 13);
    assert!(OFF_ATOMIC_ALIGN == 14);
    assert!(OFF_RESERVED_BYTE == 15);
    assert!(OFF_REGION_SIZE == 16);
    assert!(OFF_CAPACITY == 24);
    assert!(OFF_RECORD_BYTES == 32);
    assert!(OFF_SLOTS_OFFSET == 40);
    assert!(OFF_GENERATION == 48);
    assert!(OFF_RESERVED_WORD == 56);
    assert!(PREFIX_BYTES == 64);
    assert!(OFF_READY == 64);
    assert!(OFF_PRODUCER_ROLE == 64 + W);
    assert!(OFF_CONSUMER_ROLE == 64 + 2 * W);
    assert!(OFF_HEAD == 128);
    assert!(OFF_TAIL == 192);
};

/// The one-byte word-width and atomic-alignment fields. Both are 4 or 8
/// (asserted above), so the narrowing cannot truncate.
#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "the value is 4 or 8, asserted at compile time"
)]
const WORD_WIDTH_BYTE: u8 = W as u8;
#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "the value is 4 or 8, asserted at compile time"
)]
const ATOMIC_ALIGN_BYTE: u8 = align_of::<AtomicUsize>() as u8;

/// Widens a `usize` to the format's `u64` fields; lossless because
/// `usize::BITS <= 64` is asserted at compile time.
#[expect(
    clippy::as_conversions,
    reason = "lossless: `usize::BITS <= 64` is asserted above"
)]
const fn wide(n: usize) -> u64 {
    n as u64
}

/// Checks the base alignment, then the reported length.
fn check_region(base: NonNull<u8>, region_len: usize, layout: Layout) -> Result<(), SharedError> {
    if !base.as_ptr().addr().is_multiple_of(REGION_ALIGN) {
        return Err(SharedError::Misaligned {
            required: REGION_ALIGN,
        });
    }
    if region_len < layout.size() {
        return Err(SharedError::RegionTooSmall {
            required: layout.size(),
            provided: region_len,
        });
    }
    Ok(())
}

/// Initializes a region in place for one new generation.
///
/// Validates, in order, the [`layout`] for `capacity` and `RECORD_BYTES`,
/// the 64-byte alignment of `base`, and that `region_len` covers the
/// layout. Only then does it write the header (zeroing all 256 header
/// bytes, writing the prefix, constructing the atomic words with both roles
/// unclaimed and both positions zero) and finally release-store the
/// readiness word. Record slots and any bytes past the layout size are not
/// touched; slots are logically uninitialized.
///
/// Not part of the wait-free guarantee. Never allocates.
///
/// # Errors
///
/// Any [`layout`] error, then [`SharedError::Misaligned`], then
/// [`SharedError::RegionTooSmall`]. An error leaves the region unchanged.
///
/// # Safety
///
/// The caller must guarantee that:
///
/// * `base` has provenance for, and points to, `region_len` contiguous
///   bytes of writable, coherent, normal memory (not MMIO, not read-only,
///   not a private copy-on-write mapping, and not memory that needs cache
///   maintenance the platform does not perform) that stay valid for the
///   duration of this call;
/// * the caller has exclusive access to the whole region for this call: no
///   participant is using a previous generation or can observe this one
///   until the caller's startup handoff says so, and no attach call on the
///   region is in flight or can be delayed into this call (global
///   quiescence); and
/// * `generation` is not one that any old participant or delayed attach
///   could mistake for the generation it expects.
///
/// Misalignment and a too-small `region_len` are checked, not assumed; the
/// actual mapped extent behind `base` cannot be checked.
pub unsafe fn initialize<const RECORD_BYTES: usize>(
    base: NonNull<u8>,
    region_len: usize,
    capacity: usize,
    generation: u64,
) -> Result<(), SharedError> {
    let layout = layout::<RECORD_BYTES>(capacity)?;
    check_region(base, region_len, layout)?;
    // Nothing fallible remains: every write below completes.
    let p = base.as_ptr();
    #[expect(
        clippy::multiple_unsafe_ops_per_block,
        reason = "every write is an in-bounds header write covered by the one proof below"
    )]
    // SAFETY: `base` is valid for writes of `region_len >= layout.size() >=
    // HEADER_BYTES` bytes, 64-byte aligned (checked), and exclusively ours
    // (caller contract). Only header bytes are written. The atomic words
    // are constructed in place at offsets that the const assertions above
    // prove aligned for `AtomicUsize`, before anyone can access them.
    unsafe {
        ptr::write_bytes(p, 0, HEADER_BYTES);
        write_bytes_at(p, OFF_MAGIC, &MAGIC);
        write_bytes_at(p, OFF_VERSION, &FORMAT_VERSION.to_le_bytes());
        write_bytes_at(p, OFF_WORD_WIDTH, &[WORD_WIDTH_BYTE]);
        write_bytes_at(p, OFF_BYTE_ORDER, &[BYTE_ORDER]);
        write_bytes_at(p, OFF_ATOMIC_ALIGN, &[ATOMIC_ALIGN_BYTE]);
        write_bytes_at(p, OFF_REGION_SIZE, &wide(layout.size()).to_le_bytes());
        write_bytes_at(p, OFF_CAPACITY, &wide(capacity).to_le_bytes());
        write_bytes_at(p, OFF_RECORD_BYTES, &wide(RECORD_BYTES).to_le_bytes());
        write_bytes_at(p, OFF_SLOTS_OFFSET, &wide(HEADER_BYTES).to_le_bytes());
        write_bytes_at(p, OFF_GENERATION, &generation.to_le_bytes());
        for offset in [
            OFF_READY,
            OFF_PRODUCER_ROLE,
            OFF_CONSUMER_ROLE,
            OFF_HEAD,
            OFF_TAIL,
        ] {
            #[expect(
                clippy::cast_ptr_alignment,
                reason = "atomic-word offsets are `W`-aligned from a `REGION_ALIGN`-aligned base (asserted above)"
            )]
            p.add(offset)
                .cast::<AtomicUsize>()
                .write(AtomicUsize::new(0));
        }
        // Publish the completed header; attachers acquire this after their
        // startup handoff.
        word(base, OFF_READY)
            .as_ref()
            .store(READY, Ordering::Release);
    }
    Ok(())
}

/// Copies `bytes` to `p + offset`.
///
/// # Safety
///
/// `p + offset .. p + offset + bytes.len()` must be valid for writes.
unsafe fn write_bytes_at(p: *mut u8, offset: usize, bytes: &[u8]) {
    // SAFETY: `p + offset` is in bounds of the writable range (caller
    // contract).
    let destination = unsafe { p.add(offset) };
    // SAFETY: `destination .. destination + bytes.len()` is valid for writes
    // (caller contract); a local slice cannot overlap the region.
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), destination, bytes.len()) }
}

/// Returns the atomic word at `offset` in the region.
///
/// # Safety
///
/// `offset` must be one of the header's atomic-word offsets, and the region
/// must be valid, with that word initialized by `initialize`, for as long as
/// the returned pointer is used.
unsafe fn word(base: NonNull<u8>, offset: usize) -> NonNull<AtomicUsize> {
    // SAFETY: in bounds of the header (caller contract), derived from the
    // caller's `base`, so it carries this participant's mapping provenance.
    unsafe { base.add(offset).cast::<AtomicUsize>() }
}

/// Copies the `N`-byte field at `offset` out of the prefix copy.
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "callers pass the documented field offsets, all within the prefix (asserted above)"
)]
fn prefix_bytes<const N: usize>(prefix: &[u8; PREFIX_BYTES], offset: usize) -> [u8; N] {
    let mut bytes = [0; N];
    bytes.copy_from_slice(&prefix[offset..offset + N]);
    bytes
}

/// Reads a little-endian `u64` from the prefix copy.
fn prefix_u64(prefix: &[u8; PREFIX_BYTES], offset: usize) -> u64 {
    u64::from_le_bytes(prefix_bytes(prefix, offset))
}

/// Validates the header and derives the region pointers: every attach check
/// except the readiness acquire and role claim, which `protocol::claim`
/// performs.
///
/// # Safety
///
/// As for [`attach_producer`]: in particular every header byte read here
/// must be valid, initialized, and not concurrently written.
unsafe fn validate<const RECORD_BYTES: usize>(
    base: NonNull<u8>,
    region_len: usize,
    capacity: usize,
    generation: u64,
) -> Result<RegionPtrs<RECORD_BYTES>, SharedError> {
    // Step 1: independently computed layout, alignment, length.
    let layout = layout::<RECORD_BYTES>(capacity)?;
    check_region(base, region_len, layout)?;
    let p = base.as_ptr();

    // Step 2: the immutable prefix. It is read into a local copy with plain
    // byte reads; no native atomic word is touched until the ABI fields
    // (width, byte order, alignment) are known to match.
    let mut prefix = [0u8; PREFIX_BYTES];
    // SAFETY: `region_len >= layout.size() >= HEADER_BYTES > PREFIX_BYTES`
    // readable bytes (caller contract), and the prefix is immutable
    // throughout a generation, so no write races with this read.
    unsafe { ptr::copy_nonoverlapping(p, prefix.as_mut_ptr(), PREFIX_BYTES) };
    if prefix_bytes(&prefix, OFF_MAGIC) != MAGIC
        || prefix[OFF_RESERVED_BYTE] != 0
        || prefix_bytes::<8>(&prefix, OFF_RESERVED_WORD) != [0; 8]
    {
        return Err(SharedError::InvalidHeader);
    }
    if u32::from_le_bytes(prefix_bytes(&prefix, OFF_VERSION)) != FORMAT_VERSION
        || prefix[OFF_WORD_WIDTH] != WORD_WIDTH_BYTE
        || prefix[OFF_BYTE_ORDER] != BYTE_ORDER
        || prefix[OFF_ATOMIC_ALIGN] != ATOMIC_ALIGN_BYTE
    {
        return Err(SharedError::IncompatibleFormat);
    }

    // Step 3: configuration, compared as `u64` so that no conversion can
    // truncate.
    if prefix_u64(&prefix, OFF_CAPACITY) != wide(capacity)
        || prefix_u64(&prefix, OFF_RECORD_BYTES) != wide(RECORD_BYTES)
        || prefix_u64(&prefix, OFF_SLOTS_OFFSET) != wide(HEADER_BYTES)
        || prefix_u64(&prefix, OFF_REGION_SIZE) != wide(layout.size())
    {
        return Err(SharedError::ConfigurationMismatch);
    }

    // Step 4: generation.
    if prefix_u64(&prefix, OFF_GENERATION) != generation {
        return Err(SharedError::GenerationMismatch);
    }

    // Step 5 (padding part): the reserved bytes between and after the
    // atomic words, read without touching the words themselves.
    for range in [
        OFF_RESERVED_AFTER_ROLES..OFF_RESERVED_AFTER_ROLES + RESERVED_AFTER_ROLES,
        OFF_RESERVED_AFTER_HEAD..OFF_RESERVED_AFTER_HEAD + RESERVED_AFTER_POSITION,
        OFF_RESERVED_AFTER_TAIL..OFF_RESERVED_AFTER_TAIL + RESERVED_AFTER_POSITION,
    ] {
        for offset in range {
            // SAFETY: `offset` is a reserved-byte offset, in bounds of the
            // header.
            let byte = unsafe { p.add(offset) };
            // SAFETY: reserved bytes are written only by `initialize`, which
            // the handoff ordered before us, and never concurrently.
            if unsafe { byte.read() } != 0 {
                return Err(SharedError::InvalidHeader);
            }
        }
    }

    #[expect(
        clippy::multiple_unsafe_ops_per_block,
        reason = "six offsets into the validated region, all covered by the one proof below"
    )]
    // SAFETY: the offsets are the header's atomic words, all in bounds and
    // aligned (const assertions above; base alignment checked). The slot
    // array starts at `HEADER_BYTES` and `capacity * RECORD_BYTES` bytes
    // follow within `layout.size() <= region_len`; `[u8; R]` slots have
    // alignment 1. For `RECORD_BYTES == 0` the slot pointer may be one past
    // the header's end, which is valid for zero-sized accesses.
    unsafe {
        Ok(RegionPtrs {
            ready: word(base, OFF_READY),
            producer_role: word(base, OFF_PRODUCER_ROLE),
            consumer_role: word(base, OFF_CONSUMER_ROLE),
            head: word(base, OFF_HEAD),
            tail: word(base, OFF_TAIL),
            slots: base.add(HEADER_BYTES).cast(),
            capacity,
        })
    }
}

/// Attaches the unique producer of an initialized generation.
///
/// Validates the region in this order: the
/// layout for `capacity` and `RECORD_BYTES`; base alignment and
/// `region_len`; magic and reserved prefix bytes; format version, word
/// width, byte order, and atomic alignment; stored capacity, record size,
/// slot offset, and region size; the generation; then (acquire) readiness
/// and the reserved padding; and finally claims the producer role with one
/// compare-and-swap. A failed attach changes nothing and consumes no role.
///
/// The consumer may already have attached, or even closed; the producer
/// starts at the beginning of the generation either way.
///
/// Not part of the wait-free guarantee. Never allocates.
///
/// # Errors
///
/// The [`SharedError`] variant for the first check that fails, in the order
/// above; [`SharedError::RoleAlreadyClaimed`] if a producer has already
/// attached in this generation, even if it has since been dropped.
///
/// # Safety
///
/// Before the call, the region must have been initialized by [`initialize`]
/// for this generation, and the caller must have learned that (and the
/// expected configuration and `generation`) through a synchronized startup
/// handoff, not by polling the region. Every header byte this function reads
/// must be valid, initialized, and not concurrently written; returning an
/// error does not excuse a dangling pointer or a racing write.
///
/// For all of the caller-chosen `'region`, and for every reference derived
/// from the returned endpoint, the caller must additionally guarantee that:
///
/// * `base` has provenance for, and its mapping stays valid, writable,
///   coherent, and at the same address over, at least `region_len` bytes;
///   the backing object is not truncated, reclaimed, or reinitialized
///   underneath it (do not choose `'static` unless that is really true);
/// * every participant uses a qualified, interoperable atomic
///   implementation (see the crate-level qualification table) and follows
///   the format, the role claims, the publication protocol, and exclusive
///   teardown;
/// * no other code modifies the immutable header, copies live atomic words,
///   accesses a record slot contrary to its role, or holds a whole-region
///   `&mut` or `&[u8]` reference that conflicts with the queue's
///   interior-mutable accesses; and
/// * fork, DMA, foreign code, or duplicated handles do not create a second
///   active owner of the role.
pub unsafe fn attach_producer<'region, const RECORD_BYTES: usize>(
    base: NonNull<u8>,
    region_len: usize,
    capacity: usize,
    generation: u64,
) -> Result<SharedProducer<'region, RECORD_BYTES>, SharedError> {
    // SAFETY: forwarded caller contract.
    let region = unsafe { validate::<RECORD_BYTES>(base, region_len, capacity, generation)? };
    // SAFETY: `region` was just validated, and the caller's contract keeps
    // its words and slots valid, and accessed only according to the
    // protocol, for all of `'region`. The role claim happens inside.
    unsafe { region.attach_producer() }
}

/// Attaches the unique consumer of an initialized generation.
///
/// Identical to [`attach_producer`], for the consumer role. The producer may
/// not have attached yet (it then counts as alive, so `is_drained` stays
/// `false`), may have filled the queue, or may already have closed.
///
/// # Errors
///
/// As for [`attach_producer`].
///
/// # Safety
///
/// Exactly the obligations of [`attach_producer`].
pub unsafe fn attach_consumer<'region, const RECORD_BYTES: usize>(
    base: NonNull<u8>,
    region_len: usize,
    capacity: usize,
    generation: u64,
) -> Result<SharedConsumer<'region, RECORD_BYTES>, SharedError> {
    // SAFETY: forwarded caller contract.
    let region = unsafe { validate::<RECORD_BYTES>(base, region_len, capacity, generation)? };
    // SAFETY: as in `attach_producer`, for the consumer role.
    unsafe { region.attach_consumer() }
}

/// Kani proofs that `layout` and header validation are total and sound over
/// every input: any capacity, any region length, any header bytes a peer
/// could have written, and a base of any alignment.
#[cfg(kani)]
// Proof code, like `tests/`: Kani itself fails on any overflow, unwrap, or
// panic.
#[allow(clippy::arithmetic_side_effects, clippy::panic, clippy::unwrap_used)]
mod proofs {
    use core::ptr::NonNull;

    use super::{HEADER_BYTES, REGION_ALIGN, SharedError, layout, validate};
    use crate::MAX_CAPACITY;

    /// `layout` never panics (so its `unreachable!` is unreachable), reports
    /// errors in the documented order, and on success returns a
    /// `REGION_ALIGN`-aligned size that covers the header and every record.
    fn layout_is_sound<const R: usize>() {
        let capacity: usize = kani::any();
        match layout::<R>(capacity) {
            Ok(l) => {
                assert!((1..=MAX_CAPACITY).contains(&capacity));
                assert_eq!(l.align(), REGION_ALIGN);
                assert!(l.size().is_multiple_of(REGION_ALIGN));
                assert!(isize::try_from(l.size()).is_ok());
                let used = capacity
                    .checked_mul(R)
                    .and_then(|payload| payload.checked_add(HEADER_BYTES));
                assert!(
                    matches!(used, Some(used) if used <= l.size() && l.size() - used < REGION_ALIGN)
                );
            }
            Err(SharedError::ZeroCapacity) => assert_eq!(capacity, 0),
            Err(SharedError::CapacityTooLarge { requested }) => {
                assert!(requested == capacity && capacity > MAX_CAPACITY);
            }
            Err(SharedError::LayoutTooLarge) => {
                assert!((1..=MAX_CAPACITY).contains(&capacity));
                // Too large exactly when the padded size overflows `isize`.
                let fits = capacity
                    .checked_mul(R)
                    .and_then(|payload| payload.checked_add(HEADER_BYTES + REGION_ALIGN - 1))
                    .is_some_and(|padded| isize::try_from(padded & !(REGION_ALIGN - 1)).is_ok());
                assert!(!fits);
            }
            Err(error) => panic!("`layout` returned an undocumented error: {error:?}"),
        }
    }

    #[kani::proof]
    fn layout_is_sound_0() {
        layout_is_sound::<0>();
    }

    #[kani::proof]
    fn layout_is_sound_1() {
        layout_is_sound::<1>();
    }

    #[kani::proof]
    fn layout_is_sound_24() {
        layout_is_sound::<24>();
    }

    #[kani::proof]
    fn layout_is_sound_4096() {
        layout_is_sound::<4096>();
    }

    #[kani::proof]
    fn layout_is_sound_max() {
        layout_is_sound::<{ usize::MAX }>();
    }

    /// Record size and largest capacity for the validation proof, and a
    /// buffer big enough for that layout at any of the 64 base offsets.
    const R: usize = 8;
    const MAX_CAP: usize = 4;
    const BUFFER: usize = 2 * REGION_ALIGN + HEADER_BYTES + MAX_CAP * R;

    #[repr(C, align(64))]
    struct Buffer([u8; BUFFER]);

    /// Whatever bytes the region holds and whatever the caller claims about
    /// it, `validate` reads only within `region_len`, and success implies
    /// every derived pointer lies in the region, at the documented offsets.
    #[kani::proof]
    #[kani::unwind(65)]
    fn validate_is_sound_for_any_header() {
        let mut buffer = Buffer(kani::any());
        let offset: usize = kani::any();
        kani::assume(offset < REGION_ALIGN);
        let capacity: usize = kani::any();
        let region_len: usize = kani::any();
        kani::assume(region_len <= BUFFER - offset);
        let generation: u64 = kani::any();

        let region = buffer.0.as_mut_ptr().wrapping_add(offset);
        let base = NonNull::new(region).unwrap();
        // SAFETY: `base` is valid for `region_len` bytes of reads, all
        // initialized, and nothing else accesses the buffer.
        let result = unsafe { validate::<R>(base, region_len, capacity, generation) };
        kani::cover!(result.is_ok(), "an arbitrary region can validate");
        if let Ok(ptrs) = result {
            assert_eq!(offset, 0);
            assert_eq!(ptrs.capacity, capacity);
            let end = region.addr() + region_len;
            let slots = ptrs.slots.as_ptr().addr();
            assert_eq!(slots, region.addr() + HEADER_BYTES);
            assert!(slots + capacity * R <= end);
            for word in [
                ptrs.ready,
                ptrs.producer_role,
                ptrs.consumer_role,
                ptrs.head,
                ptrs.tail,
            ] {
                let word = word.as_ptr().addr();
                assert!(word.is_multiple_of(align_of::<usize>()));
                assert!(word + size_of::<usize>() <= region.addr() + HEADER_BYTES);
            }
        }
    }

    /// Reachability check: `initialize` followed by `validate` succeeds, so
    /// the success branch above is not vacuous.
    #[kani::proof]
    #[kani::unwind(65)]
    fn initialized_region_validates() {
        let mut buffer = Buffer([0; BUFFER]);
        let capacity: usize = kani::any();
        kani::assume((1..=MAX_CAP).contains(&capacity));
        let generation: u64 = kani::any();
        let base = NonNull::new(buffer.0.as_mut_ptr()).unwrap();
        // SAFETY: the buffer is 64-byte aligned, writable for `BUFFER`
        // bytes, and exclusively ours.
        unsafe { super::initialize::<R>(base, BUFFER, capacity, generation) }.unwrap();
        // SAFETY: as above; `initialize` has completed.
        let ptrs = unsafe { validate::<R>(base, BUFFER, capacity, generation) }.unwrap();
        assert_eq!(ptrs.capacity, capacity);
    }
}
