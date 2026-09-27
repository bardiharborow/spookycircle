//! Queues of fixed-size byte records in caller-owned shared memory
//! (`shared-memory` feature).
//!
//! A shared region is a contiguous block of coherent memory that one or
//! more address spaces (processes, or firmware images on cores that share
//! coherent RAM) map, possibly at *different* virtual addresses. The region
//! holds a 256-byte header followed by `capacity` slots of `RECORD_BYTES`
//! bytes each. It contains no pointer, reference, allocator state, or
//! destructor, so its bytes mean the same thing in every mapping. Each
//! participant derives its addresses from its own `base` pointer.
//!
//! Records are plain byte arrays. `Copy` alone does not make a Rust value
//! meaningful in another address space (it may contain pointers, padding,
//! or process-local meaning), so the queue never reinterprets typed values:
//! applications encode and decode their own records outside queue
//! operations. An address encoded into a record is just bytes to the queue.
//!
//! # Lifecycle
//!
//! 1. Obtain a region of at least [`layout`]`::<R>(capacity)` bytes, aligned
//!    to 64 bytes, by whatever means the platform provides (for example a
//!    file or shared-memory object mapped with `MAP_SHARED`). Creating,
//!    mapping, protecting, resizing, and unlinking the region are the
//!    application's job; this module never does any of them.
//! 2. With exclusive access and no other participant, call [`initialize`]
//!    with the capacity and a fresh *generation* number.
//! 3. Tell the participants that initialization is complete, and which
//!    capacity, record size, and generation to expect, through a
//!    synchronized startup handoff of the application's choosing. An
//!    attacher must never poll arbitrary or not-yet-initialized memory for a
//!    magic number or readiness flag.
//! 4. Each participant calls [`attach_producer`] or [`attach_consumer`]
//!    once, with its own local base address. The attach functions validate
//!    the header, then claim the role; each role can be claimed once per
//!    generation. Either role may attach first, even after the other has
//!    filled the queue or closed.
//! 5. Use the [`SharedProducer`] / [`SharedConsumer`] like any other
//!    endpoint. Their methods are safe and wait-free.
//! 6. Dropping an endpoint *closes* its role, after its last publication.
//!    Nothing is freed, unmapped, or reset. A participant may remove its
//!    own mapping once its local endpoint and every reference from `peek`
//!    are gone.
//! 7. To reuse the region, the owner must first establish *global
//!    quiescence*: no participant can still access the old generation, and
//!    no attach call is in flight or delayed. Then it may call
//!    [`initialize`] again with a generation number that no old participant
//!    could mistake for its own.
//!
//! # Memory placement
//!
//! The region's pages are the application's to prepare, like the mapping
//! itself. For large or latency-critical regions, prefault them
//! (`MAP_POPULATE`, or write every page once before step 2) so the first
//! pass around the ring takes no page faults; back them with huge pages
//! (on Linux, for example `memfd_create(MFD_HUGETLB)` or `MADV_HUGEPAGE`)
//! so a large ring does not miss the TLB on every new page; and `mlock`
//! them in each participant so they stay resident.
//!
//! # Liveness
//!
//! A counterpart role counts as alive until it is closed, *including before
//! it first attaches*. Otherwise a consumer that attached first could see an
//! empty queue and a "dead" producer and wrongly conclude that the stream
//! had ended. `is_drained` is definitive exactly as for the other modes: the
//! producer closes its role only after its last publication.
//!
//! There is no crash recovery. A participant that is killed, or that forgets
//! its endpoint, leaves its role live forever; the survivor keeps receiving
//! ordinary full or empty results without waiting. Nothing detects process
//! death, takes over a role, or repairs a half-finished operation. Memory
//! visibility is not durability: nothing here flushes to persistent storage.
//!
//! # Safety contract summary
//!
//! The three raw functions are this crate's only public `unsafe` entry
//! points. They check what can be checked: the layout arithmetic, the base
//! alignment, the reported region length, and every header field. They
//! return an error, having changed nothing, for any mismatch. They cannot
//! check that `base` really points to `region_len` mapped, writable,
//! coherent bytes; that the mapping outlives the chosen `'region`; that all
//! participants run a qualified atomic implementation (see the crate-level
//! qualification table); or that no one else scribbles on the region. Those
//! are the caller's obligations, listed on each function. Peers that corrupt
//! the region are outside the contract: validation is not a sandbox.
//!
//! # Format (version 1)
//!
//! The immutable prefix uses little-endian integers; the atomic words use
//! the participants' native byte order and width `W = size_of::<usize>()`
//! (4 or 8), which the prefix records so that mismatched participants are
//! rejected before any atomic word is touched.
//!
//! | Offset | Size | Field |
//! | ---: | ---: | --- |
//! | 0 | 8 | Magic bytes `WFSPSC01` |
//! | 8 | 4 | [`FORMAT_VERSION`], `u32` |
//! | 12 | 1 | Atomic word width `W` |
//! | 13 | 1 | Byte order: 1 = little-endian, 2 = big-endian |
//! | 14 | 1 | `align_of::<AtomicUsize>()` |
//! | 15 | 1 | Reserved, zero |
//! | 16 | 8 | Rounded region size, `u64` |
//! | 24 | 8 | Capacity, `u64` |
//! | 32 | 8 | Record size, `u64` |
//! | 40 | 8 | Slots offset (256), `u64` |
//! | 48 | 8 | Generation, `u64` |
//! | 56 | 8 | Reserved, zero |
//! | 64 | W | Readiness: 0 while initializing, 1 when ready |
//! | 64 + W | W | Producer role: 0 unclaimed, 1 live, 2 closed |
//! | 64 + 2W | W | Consumer role, likewise |
//! | 128 | W | `head` |
//! | 192 | W | `tail` |
//! | 256 | C × R | Record slots |
//!
//! All other header bytes are zero. The region size is `256 + C × R`
//! rounded up to a multiple of 64.
//!
//! # Example
//!
//! One local mapping, both roles in one thread; a real deployment attaches
//! each role in its own process with that process's base address.
//!
//! ```
//! use core::ptr::NonNull;
//! use spookycircle::shared_memory as shm;
//!
//! /// # Safety
//! /// The caller supplies exclusive, writable, qualified coherent shared
//! /// memory satisfying the `initialize` and attach contracts for this call,
//! /// with no other participant, and a fresh generation. The mapping remains
//! /// valid until both local endpoints drop.
//! unsafe fn local_round_trip(
//!     base: NonNull<u8>,
//!     region_len: usize,
//!     generation: u64,
//! ) -> Result<(), shm::SharedError> {
//!     let required = shm::layout::<4>(3)?;
//!     assert!(region_len >= required.size());
//!
//!     // SAFETY: caller provides exclusive quiescent memory and a fresh generation.
//!     unsafe { shm::initialize::<4>(base, region_len, 3, generation)?; }
//!
//!     // SAFETY: initialization completed in this thread; the caller guarantees
//!     // mapping validity through both drops, and each role is attached once.
//!     let mut producer = unsafe {
//!         shm::attach_producer::<4>(base, region_len, 3, generation)?
//!     };
//!     // SAFETY: same mapping contract; this claims the distinct consumer role.
//!     let mut consumer = unsafe {
//!         shm::attach_consumer::<4>(base, region_len, 3, generation)?
//!     };
//!
//!     producer.try_push(42_u32.to_le_bytes()).unwrap();
//!     let record = consumer.try_pop().unwrap();
//!     assert_eq!(u32::from_le_bytes(record), 42);
//!     drop(producer);
//!     assert!(consumer.is_drained());
//!     drop(consumer);
//!     Ok(())
//! }
//! # // Stand-in for a real shared mapping: ordinary heap memory with the
//! # // required layout, used from one thread.
//! # let layout = shm::layout::<4>(3).unwrap();
//! # let base = NonNull::new(unsafe { std::alloc::alloc(layout) }).unwrap();
//! # unsafe { local_round_trip(base, layout.size(), 7).unwrap() };
//! # unsafe { std::alloc::dealloc(base.as_ptr(), layout) };
//! ```
//!
//! For separate processes or firmware images, one initializer performs
//! exclusive initialization, then communicates the generation and
//! configuration through the synchronized startup handoff. Each participant
//! calls only its own attach function using its local base address. Bases
//! may differ. All local endpoints and peek references must cease to be
//! usable before their mapping is removed; reinitializing the backing region
//! additionally requires global quiescence.

// The module has three layers:
//
// * this file: the public error type, `layout`, and the endpoint types;
// * `region`: the byte format at fixed offsets (initialization, header
//   validation, the public attach functions), which needs real fixed-size
//   atomics and so is not compiled under Loom;
// * `protocol`: readiness, role claims, and the endpoints' lifecycle, which
//   the Loom model tests exercise directly.
mod protocol;
#[cfg(not(loom))]
mod region;

use core::{alloc::Layout, error::Error, fmt, marker::PhantomData};

use crate::{
    MAX_CAPACITY,
    error::CreateError,
    raw::{RawConsumer, RawProducer, validate_capacity},
};
use protocol::RegionLife;
#[cfg(not(loom))]
pub use region::{attach_consumer, attach_producer, initialize};

/// The shared-region format version this build reads and writes.
///
/// Any change to the region's offsets, encodings, role meanings, atomic
/// widths, or payload placement changes this number, and attachers reject
/// regions of any other version.
pub const FORMAT_VERSION: u32 = 1;

/// Size of the header, and offset of the first record slot.
const HEADER_BYTES: usize = 256;
/// Required alignment of the region base, and granularity of its size.
const REGION_ALIGN: usize = 64;

/// Failure to lay out, initialize, or attach to a shared region.
///
/// Every error is returned before the region is modified: no counter,
/// record, or role changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SharedError {
    /// The requested capacity was zero.
    ZeroCapacity,
    /// The requested capacity exceeds [`MAX_CAPACITY`].
    CapacityTooLarge {
        /// The capacity that was requested.
        requested: usize,
    },
    /// The region size overflowed or cannot be described by a `Layout`.
    LayoutTooLarge,
    /// The supplied region is shorter than [`layout`] requires.
    RegionTooSmall {
        /// Bytes the configuration requires.
        required: usize,
        /// Bytes the caller supplied.
        provided: usize,
    },
    /// The base address is not aligned to `required` bytes.
    Misaligned {
        /// The required alignment of the base address.
        required: usize,
    },
    /// The header is not a ready version-1 region header: bad magic,
    /// nonzero reserved bytes, readiness not set, or an invalid role state.
    InvalidHeader,
    /// The region was written by an incompatible participant: another
    /// format version, atomic word width, byte order, or atomic alignment.
    IncompatibleFormat,
    /// The region's capacity, record size, slot offset, or size differs from
    /// the configuration supplied to the attach call.
    ConfigurationMismatch,
    /// The region holds a different generation than the one expected.
    GenerationMismatch,
    /// The requested role is already live or closed in this generation.
    RoleAlreadyClaimed,
}

impl fmt::Display for SharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => f.write_str("shared ring buffer capacity must be at least 1"),
            Self::CapacityTooLarge { requested } => write!(
                f,
                "shared ring buffer capacity {requested} exceeds the representable maximum"
            ),
            Self::LayoutTooLarge => f.write_str("shared region size is not representable"),
            Self::RegionTooSmall { required, provided } => write!(
                f,
                "shared region is {provided} bytes but {required} are required"
            ),
            Self::Misaligned { required } => {
                write!(f, "shared region base is not {required}-byte aligned")
            }
            Self::InvalidHeader => f.write_str("shared region header is invalid or not ready"),
            Self::IncompatibleFormat => {
                f.write_str("shared region was written in an incompatible format")
            }
            Self::ConfigurationMismatch => {
                f.write_str("shared region configuration does not match the expected one")
            }
            Self::GenerationMismatch => f.write_str("shared region holds a different generation"),
            Self::RoleAlreadyClaimed => {
                f.write_str("shared region role has already been claimed in this generation")
            }
        }
    }
}

impl Error for SharedError {}

/// Returns the size and alignment of a region holding `capacity` records of
/// `RECORD_BYTES` bytes.
///
/// The size is `256 + capacity × RECORD_BYTES` rounded up to a multiple of
/// 64, and the alignment is 64. `RECORD_BYTES == 0` is allowed: such
/// records use capacity but no payload bytes. Padding never adds capacity.
///
/// Pure arithmetic: inspects no memory and never allocates.
///
/// # Errors
///
/// In this order: [`SharedError::ZeroCapacity`],
/// [`SharedError::CapacityTooLarge`] above
/// [`MAX_CAPACITY`], and [`SharedError::LayoutTooLarge`]
/// if the size overflows or exceeds `isize::MAX`.
pub fn layout<const RECORD_BYTES: usize>(capacity: usize) -> Result<Layout, SharedError> {
    // Exhaustive (the crate may match its own `#[non_exhaustive]` enum), so a
    // new `CreateError` variant must be mapped here deliberately.
    validate_capacity(capacity, MAX_CAPACITY).map_err(|error| match error {
        CreateError::ZeroCapacity => SharedError::ZeroCapacity,
        CreateError::CapacityTooLarge { requested } => SharedError::CapacityTooLarge { requested },
        #[expect(
            clippy::unreachable,
            reason = "`validate_capacity` never reports allocation failure; construction, not the data path"
        )]
        CreateError::AllocationFailed => {
            unreachable!("`validate_capacity` never reports allocation failure")
        }
    })?;
    let size = capacity
        .checked_mul(RECORD_BYTES)
        .and_then(|payload| payload.checked_add(HEADER_BYTES))
        .and_then(|used| used.checked_add(REGION_ALIGN - 1))
        .map(|padded| padded & !(REGION_ALIGN - 1))
        .ok_or(SharedError::LayoutTooLarge)?;
    Layout::from_size_align(size, REGION_ALIGN).map_err(|_| SharedError::LayoutTooLarge)
}

/// The unique producer of a shared region's generation.
///
/// Created by [`attach_producer`]. Has the same methods and guarantees as
/// every other producer, with `T = [u8; RECORD_BYTES]`: `try_push` takes a
/// record by value and returns it inside [`Full`](crate::Full) when the
/// queue is full, and `push_slice` copies a slice of records. All methods
/// are safe once attachment has succeeded.
///
/// `Send` (records are plain bytes) but not `Sync`, `Clone`, or `Copy`.
/// Dropping it closes the producer role for the rest of the generation; it
/// does not unmap or reset anything.
#[must_use = "dropping the producer permanently closes its shared role"]
pub struct SharedProducer<'region, const RECORD_BYTES: usize> {
    raw: RawProducer<[u8; RECORD_BYTES], usize, RegionLife>,
    /// Ties the endpoint to the mapping's lifetime. Drop check treats it as
    /// owning a `&'region ()`, so `'region` must be live wherever the
    /// endpoint is dropped: the drop writes the role word in the mapping.
    _region: PhantomData<&'region ()>,
}

/// The unique consumer of a shared region's generation.
///
/// Created by [`attach_consumer`]. Has the same methods and guarantees as
/// every other consumer, with `T = [u8; RECORD_BYTES]`; `peek` and
/// `peek_mut` borrow one record in place for as long as the consumer is
/// mutably borrowed.
///
/// `Send` but not `Sync`, `Clone`, or `Copy`. Dropping it closes the
/// consumer role for the rest of the generation.
#[must_use = "dropping the consumer permanently closes its shared role"]
pub struct SharedConsumer<'region, const RECORD_BYTES: usize> {
    raw: RawConsumer<[u8; RECORD_BYTES], usize, RegionLife>,
    /// See [`SharedProducer`]'s field of the same name.
    _region: PhantomData<&'region ()>,
}

producer_api! {
    impl['region, const RECORD_BYTES: usize] SharedProducer<'region, RECORD_BYTES>,
    elem = [u8; RECORD_BYTES],
    copy_where = [],
    name = "SharedProducer",
}

consumer_api! {
    impl['region, const RECORD_BYTES: usize] SharedConsumer<'region, RECORD_BYTES>,
    elem = [u8; RECORD_BYTES],
    copy_where = [],
    name = "SharedConsumer",
}
