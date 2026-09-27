//! Shared-region format and validation, in one process. The regions here are ordinary heap memory; the
//! cross-process tests are in `shared_process.rs`.
#![cfg(all(feature = "shared-memory", not(loom)))]

#[macro_use]
mod common;

use std::{
    alloc::{Layout, alloc, dealloc},
    mem::{align_of, size_of},
    ptr::NonNull,
    sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use common::HeapRegion;
use spookycircle::{
    MAX_CAPACITY,
    shared_memory::{self as shm, FORMAT_VERSION, SharedError},
};

const W: usize = size_of::<usize>();

fn read_u64(header: &[u8; 256], offset: usize) -> u64 {
    u64::from_le_bytes(header[offset..offset + 8].try_into().unwrap())
}

fn read_word(header: &[u8; 256], offset: usize) -> usize {
    usize::from_ne_bytes(header[offset..offset + W].try_into().unwrap())
}

#[test]
fn layout_arithmetic() {
    let cases: [(usize, Result<usize, SharedError>); 4] = [
        (1, Ok(320)),  // 256 + 4 → 320
        (3, Ok(320)),  // 256 + 12 → 320
        (16, Ok(320)), // 256 + 64 = 320 exactly
        (17, Ok(384)),
    ];
    for (capacity, expected) in cases {
        let layout = shm::layout::<4>(capacity).map(|l| {
            assert_eq!(l.align(), 64);
            l.size()
        });
        assert_eq!(layout, expected, "capacity {capacity}");
    }
    // Zero-byte records: capacity counts, payload is empty.
    assert_eq!(shm::layout::<0>(1).unwrap().size(), 256);
    assert_eq!(shm::layout::<0>(MAX_CAPACITY).unwrap().size(), 256);
    assert_eq!(shm::layout::<1>(1).unwrap().size(), 320);
    assert_eq!(shm::layout::<64>(1).unwrap().size(), 320);
    assert_eq!(shm::layout::<65>(1).unwrap().size(), 384);

    assert_eq!(shm::layout::<4>(0), Err(SharedError::ZeroCapacity));
    assert_eq!(
        shm::layout::<4>(MAX_CAPACITY + 1),
        Err(SharedError::CapacityTooLarge {
            requested: MAX_CAPACITY + 1
        })
    );
    // Multiplication overflow.
    assert_eq!(
        shm::layout::<{ usize::MAX }>(2),
        Err(SharedError::LayoutTooLarge)
    );
    // Addition overflow after a representable product.
    assert_eq!(
        shm::layout::<{ usize::MAX }>(1),
        Err(SharedError::LayoutTooLarge)
    );
    // No overflow, but larger than `isize::MAX` after rounding.
    assert_eq!(
        shm::layout::<2>(MAX_CAPACITY),
        Err(SharedError::LayoutTooLarge)
    );
    assert_eq!(
        shm::layout::<1>(MAX_CAPACITY),
        Err(SharedError::LayoutTooLarge)
    );
}

#[test]
fn format_offsets_and_initial_values() {
    for capacity in [1usize, 3] {
        let region = HeapRegion::new::<4>(capacity);
        let h = region.header();
        assert_eq!(&h[0..8], b"WFSPSC01");
        assert_eq!(
            u32::from_le_bytes(h[8..12].try_into().unwrap()),
            FORMAT_VERSION
        );
        assert_eq!(FORMAT_VERSION, 1);
        assert_eq!(usize::from(h[12]), W);
        assert_eq!(h[13], if cfg!(target_endian = "little") { 1 } else { 2 });
        assert_eq!(usize::from(h[14]), align_of::<AtomicUsize>());
        assert_eq!(h[15], 0);
        assert_eq!(read_u64(&h, 16), 320);
        assert_eq!(read_u64(&h, 24), capacity as u64);
        assert_eq!(read_u64(&h, 32), 4);
        assert_eq!(read_u64(&h, 40), 256);
        assert_eq!(read_u64(&h, 48), region.generation);
        assert_eq!(read_u64(&h, 56), 0);
        assert_eq!(read_word(&h, 64), 1, "ready");
        assert_eq!(read_word(&h, 64 + W), 0, "producer unclaimed");
        assert_eq!(read_word(&h, 64 + 2 * W), 0, "consumer unclaimed");
        assert_eq!(read_word(&h, 128), 0, "head");
        assert_eq!(read_word(&h, 192), 0, "tail");
        // Every other header byte is zero.
        for (offset, &byte) in h.iter().enumerate() {
            let in_word = |start: usize| (start..start + W).contains(&offset);
            if offset < 64 || in_word(64) || in_word(128) || in_word(192) {
                continue;
            }
            assert_eq!(byte, 0, "reserved byte {offset}");
        }

        // Role states after attach and drop, positions after traffic.
        let (mut p, mut c) = region.attach::<4>();
        let h = region.header();
        assert_eq!(read_word(&h, 64 + W), 1, "producer live");
        assert_eq!(read_word(&h, 64 + 2 * W), 1, "consumer live");
        p.try_push(*b"abcd").unwrap();
        assert_eq!(c.try_pop(), Some(*b"abcd"));
        p.try_push(*b"efgh").unwrap();
        drop(p);
        let h = region.header();
        assert_eq!(read_word(&h, 64 + W), 2, "producer closed");
        assert_eq!(read_word(&h, 128), 1, "head");
        assert_eq!(read_word(&h, 192), 2, "tail");
        // Records are contiguous from offset 256, in physical order. The
        // region is quiescent for these reads (the consumer is idle and the
        // producer has closed).
        let slot = |i: usize| {
            // SAFETY: record `i` starts at `256 + 4 * i`, inside the region.
            let record = unsafe { region.base.as_ptr().add(256 + 4 * i) };
            // SAFETY: the 4 record bytes are initialized and, with the
            // region quiescent, not written while the copy is taken.
            unsafe { std::slice::from_raw_parts(record, 4) }.to_vec()
        };
        if capacity > 1 {
            assert_eq!(slot(0), b"abcd");
            assert_eq!(slot(1), b"efgh");
        } else {
            assert_eq!(slot(0), b"efgh", "capacity 1 reuses slot 0");
        }
        drop(c);
        assert_eq!(
            read_word(&region.header(), 64 + 2 * W),
            2,
            "consumer closed"
        );
    }
}

/// A byte buffer with canary bytes around and inside the would-be region.
struct Canary {
    base: NonNull<u8>,
    layout: Layout,
}

const CANARY: u8 = 0xCD;

impl Canary {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size + 128, 64).unwrap();
        // SAFETY: nonzero size.
        let base = NonNull::new(unsafe { alloc(layout) }).unwrap();
        // SAFETY: fresh allocation of `layout.size()` bytes.
        unsafe { base.as_ptr().write_bytes(CANARY, layout.size()) };
        Self { base, layout }
    }

    fn at(&self, offset: usize) -> NonNull<u8> {
        // SAFETY: callers stay within the allocation.
        unsafe { self.base.add(offset) }
    }

    fn untouched(&self) -> bool {
        // SAFETY: all bytes were initialized in `new`.
        unsafe { std::slice::from_raw_parts(self.base.as_ptr(), self.layout.size()) }
            .iter()
            .all(|&b| b == CANARY)
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this layout.
        unsafe { dealloc(self.base.as_ptr(), self.layout) }
    }
}

#[test]
fn misaligned_and_short_regions_are_rejected_before_any_write() {
    let size = shm::layout::<4>(3).unwrap().size();
    let buffer = Canary::new(size);
    for offset in [1usize, 8, 32] {
        // SAFETY: the pointer is valid for `size` bytes; the call is expected
        // to fail its alignment check before touching them.
        let result = unsafe { shm::initialize::<4>(buffer.at(offset), size, 3, 1) };
        assert_eq!(result, Err(SharedError::Misaligned { required: 64 }));
        // SAFETY: as above; attach checks alignment before reading.
        let result = unsafe { shm::attach_producer::<4>(buffer.at(offset), size, 3, 1) };
        assert_eq!(result.err(), Some(SharedError::Misaligned { required: 64 }));
    }
    for short in [0usize, 1, 255, 256, size - 1] {
        // SAFETY: the reported length is valid (and smaller than the
        // buffer); the call is expected to fail its length check.
        let result = unsafe { shm::initialize::<4>(buffer.at(0), short, 3, 1) };
        assert_eq!(
            result,
            Err(SharedError::RegionTooSmall {
                required: size,
                provided: short
            })
        );
        // SAFETY: as above.
        let result = unsafe { shm::attach_consumer::<4>(buffer.at(0), short, 3, 1) };
        assert_eq!(
            result.err(),
            Some(SharedError::RegionTooSmall {
                required: size,
                provided: short
            })
        );
    }
    // Layout errors come first of all.
    // SAFETY: rejected before any access.
    let result = unsafe { shm::initialize::<4>(buffer.at(1), 0, 0, 1) };
    assert_eq!(result, Err(SharedError::ZeroCapacity));
    assert!(buffer.untouched(), "a failed call wrote to the region");

    // A larger region is fine; only the layout extent is written.
    // SAFETY: exclusive, valid for `size + 64` bytes.
    unsafe { shm::initialize::<4>(buffer.at(0), size + 64, 3, 1) }.unwrap();
    // SAFETY: the trailing bytes are within the allocation.
    let tail = unsafe { std::slice::from_raw_parts(buffer.at(256).as_ptr(), size + 128 - 256) };
    assert!(
        tail.iter().all(|&b| b == CANARY),
        "slots and padding must not be written by initialize"
    );
}

/// Undersized regions ending right at an inaccessible guard page: a stray
/// access past `region_len` would fault instead of silently passing.
#[cfg(all(unix, not(miri)))]
#[test]
fn short_region_before_guard_page() {
    // SAFETY: `sysconf` has no preconditions.
    let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap();
    // SAFETY: a plain anonymous private mapping of two pages; no existing
    // memory is affected.
    let map = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            2 * page,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };
    assert_ne!(map, libc::MAP_FAILED);
    let map = map.cast::<u8>();
    // SAFETY: `page` bytes into the two-page mapping.
    let guard = unsafe { map.add(page) };
    // SAFETY: the second page belongs to this mapping and nothing refers to
    // it; making it inaccessible turns any stray access into a fault.
    let protected = unsafe { libc::mprotect(guard.cast(), page, libc::PROT_NONE) };
    assert_eq!(protected, 0);
    let capacity = 2 * page / 64;
    let size = shm::layout::<64>(capacity).unwrap().size();
    assert!(size > page);
    for short in [256usize, 320, page / 2, page] {
        // The region's reported end coincides with the guard page.
        // SAFETY: `base .. base + short` is mapped; everything after faults.
        let base = NonNull::new(unsafe { map.add(page - short) }).unwrap();
        // SAFETY: valid for `short` bytes; expected to fail before access.
        let result = unsafe { shm::initialize::<64>(base, short, capacity, 1) };
        assert!(matches!(result, Err(SharedError::RegionTooSmall { .. })));
        // SAFETY: as above.
        let result = unsafe { shm::attach_consumer::<64>(base, short, capacity, 1) };
        assert!(matches!(result, Err(SharedError::RegionTooSmall { .. })));
    }
    // An exactly sized region right before the guard page works end to end.
    let size = shm::layout::<4>(16).unwrap().size();
    // SAFETY: `size` bytes before the guard page are mapped and ours.
    let base = NonNull::new(unsafe { map.add(page - size) }).unwrap();
    // SAFETY: as above, exclusive, fresh generation.
    unsafe { shm::initialize::<4>(base, size, 16, 9) }.unwrap();
    // SAFETY: initialized above; the mapping outlives the endpoints.
    let mut p = unsafe { shm::attach_producer::<4>(base, size, 16, 9) }.unwrap();
    // SAFETY: as for the producer.
    let mut c = unsafe { shm::attach_consumer::<4>(base, size, 16, 9) }.unwrap();
    for round in 0..3u8 {
        for i in 0..16u8 {
            p.try_push([round, i, 0, 0]).unwrap();
        }
        let mut out = [[0; 4]; 16];
        assert_eq!(c.pop_slice(&mut out), 16);
        assert_eq!(out[15], [round, 15, 0, 0]);
    }
    drop((p, c));
    // SAFETY: mapped above; no endpoint remains.
    assert_eq!(unsafe { libc::munmap(map.cast(), 2 * page) }, 0);
}

/// Writes `bytes` into a quiescent region's header at `offset`.
fn poke(region: &HeapRegion, offset: usize, bytes: &[u8]) {
    // SAFETY: `offset` is within the header.
    let destination = unsafe { region.base.as_ptr().add(offset) };
    // SAFETY: `offset + bytes.len()` is within the header, and no endpoint
    // exists or runs concurrently.
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), destination, bytes.len()) };
}

fn attach_both(region: &HeapRegion) -> (Option<SharedError>, Option<SharedError>) {
    (region.producer::<4>().err(), region.consumer::<4>().err())
}

/// Every header corruption maps to its error, for both roles, and a failed
/// attach consumes no role: after restoring the byte, both roles attach.
#[test]
fn header_corruption_is_rejected_without_consuming_roles() {
    let word_width = u8::try_from(W).unwrap();
    let byte_order = if cfg!(target_endian = "little") {
        1u8
    } else {
        2
    };
    let cases: Vec<(&str, usize, Vec<u8>, SharedError)> = vec![
        ("magic", 0, b"XFSPSC01".to_vec(), SharedError::InvalidHeader),
        ("magic tail", 7, vec![b'2'], SharedError::InvalidHeader),
        ("reserved byte", 15, vec![1], SharedError::InvalidHeader),
        ("reserved word", 60, vec![1], SharedError::InvalidHeader),
        (
            "version",
            8,
            2u32.to_le_bytes().to_vec(),
            SharedError::IncompatibleFormat,
        ),
        (
            "word width",
            12,
            vec![word_width ^ 0b1100],
            SharedError::IncompatibleFormat,
        ),
        (
            "byte order",
            13,
            vec![3 - byte_order],
            SharedError::IncompatibleFormat,
        ),
        ("atomic align", 14, vec![1], SharedError::IncompatibleFormat),
        (
            "region size",
            16,
            384u64.to_le_bytes().to_vec(),
            SharedError::ConfigurationMismatch,
        ),
        (
            "capacity",
            24,
            4u64.to_le_bytes().to_vec(),
            SharedError::ConfigurationMismatch,
        ),
        (
            "record size",
            32,
            5u64.to_le_bytes().to_vec(),
            SharedError::ConfigurationMismatch,
        ),
        (
            "slots offset",
            40,
            320u64.to_le_bytes().to_vec(),
            SharedError::ConfigurationMismatch,
        ),
        (
            "generation",
            48,
            [0xEE; 8].to_vec(),
            SharedError::GenerationMismatch,
        ),
        (
            "padding after roles",
            64 + 3 * W,
            vec![1],
            SharedError::InvalidHeader,
        ),
        (
            "padding before tail",
            128 + W,
            vec![1],
            SharedError::InvalidHeader,
        ),
        ("padding at end", 255, vec![1], SharedError::InvalidHeader),
        (
            "ready",
            64,
            0usize.to_ne_bytes().to_vec(),
            SharedError::InvalidHeader,
        ),
    ];
    for (name, offset, bad, expected) in cases {
        let region = HeapRegion::new::<4>(3);
        let original = region.header()[offset..offset + bad.len()].to_vec();
        poke(&region, offset, &bad);
        assert_eq!(
            attach_both(&region),
            (Some(expected), Some(expected)),
            "{name}"
        );
        poke(&region, offset, &original);
        assert_eq!(attach_both(&region), (None, None), "{name}: role consumed");
    }

    // An invalid role state is a header error; the other role is unaffected.
    let region = HeapRegion::new::<4>(3);
    poke(&region, 64 + W, &7usize.to_ne_bytes());
    assert_eq!(
        region.producer::<4>().err(),
        Some(SharedError::InvalidHeader)
    );
    assert!(region.consumer::<4>().is_ok());
}

/// The attach call's own configuration must match the region.
#[test]
fn configuration_and_generation_mismatch() {
    let region = HeapRegion::new::<4>(3);
    let len = region.len();
    // Same layout size (320), different capacity.
    // SAFETY: valid initialized region; failures access only the header.
    let result = unsafe { shm::attach_producer::<4>(region.base, len, 4, region.generation) };
    assert_eq!(result.err(), Some(SharedError::ConfigurationMismatch));
    // Different record size with the same layout size.
    // SAFETY: as above.
    let result = unsafe { shm::attach_producer::<5>(region.base, len, 3, region.generation) };
    assert_eq!(result.err(), Some(SharedError::ConfigurationMismatch));
    // Different generation.
    // SAFETY: as above.
    let result = unsafe { shm::attach_consumer::<4>(region.base, len, 3, region.generation + 1) };
    assert_eq!(result.err(), Some(SharedError::GenerationMismatch));
    // A larger reported length is fine.
    let (p, c) = region.attach::<4>();
    drop((p, c));
}

/// With an ABI mismatch in the prefix, validation must stop before reading
/// any atomic word: here the words (and everything after the prefix) are
/// left uninitialized, which Miri reports if they are read.
#[test]
fn abi_mismatch_is_detected_before_touching_atomic_words() {
    let size = shm::layout::<4>(3).unwrap().size();
    let layout = Layout::from_size_align(size, 64).unwrap();
    let word_width = u8::try_from(W).unwrap();
    let byte_order = if cfg!(target_endian = "little") {
        1u8
    } else {
        2
    };
    let mut prefix = [0u8; 64];
    prefix[..8].copy_from_slice(b"WFSPSC01");
    prefix[8..12].copy_from_slice(&1u32.to_le_bytes());
    prefix[12] = word_width;
    prefix[13] = byte_order;
    prefix[14] = u8::try_from(align_of::<AtomicUsize>()).unwrap();
    prefix[16..24].copy_from_slice(&(size as u64).to_le_bytes());
    prefix[24..32].copy_from_slice(&3u64.to_le_bytes());
    prefix[32..40].copy_from_slice(&4u64.to_le_bytes());
    prefix[40..48].copy_from_slice(&256u64.to_le_bytes());
    prefix[48..56].copy_from_slice(&5u64.to_le_bytes());
    let cases: [(usize, u8, SharedError); 6] = [
        (0, b'x', SharedError::InvalidHeader),
        (12, word_width ^ 0b1100, SharedError::IncompatibleFormat),
        (13, 3 - byte_order, SharedError::IncompatibleFormat),
        (14, 3, SharedError::IncompatibleFormat),
        (24, 9, SharedError::ConfigurationMismatch),
        (48, 6, SharedError::GenerationMismatch),
    ];
    for (offset, value, expected) in cases {
        // SAFETY: nonzero size.
        let base = NonNull::new(unsafe { alloc(layout) }).unwrap();
        let mut bytes = prefix;
        bytes[offset] = value;
        // SAFETY: only the 64-byte prefix is initialized.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), base.as_ptr(), 64) };
        // SAFETY: every byte the validation path may read before rejecting
        // the prefix is initialized; the atomic words are not.
        let result = unsafe { shm::attach_producer::<4>(base, size, 3, 5) };
        assert_eq!(result.err(), Some(expected));
        // SAFETY: allocated above with this layout.
        unsafe { dealloc(base.as_ptr(), layout) };
    }
}

#[test]
fn roles_attach_once_per_generation() {
    let region = HeapRegion::new::<4>(2);
    let p = region.producer::<4>().unwrap();
    assert_eq!(
        region.producer::<4>().err(),
        Some(SharedError::RoleAlreadyClaimed)
    );
    drop(p);
    // Closed roles never reopen.
    assert_eq!(
        region.producer::<4>().err(),
        Some(SharedError::RoleAlreadyClaimed)
    );
    let c = region.consumer::<4>().unwrap();
    assert!(!c.is_producer_alive());
    drop(c);
    assert_eq!(
        region.consumer::<4>().err(),
        Some(SharedError::RoleAlreadyClaimed)
    );
}

#[test]
fn duplicate_attach_race_has_one_winner_per_role() {
    let rounds = if cfg!(miri) { 2 } else { 100 };
    for _ in 0..rounds {
        let region = HeapRegion::new::<4>(2);
        let barrier = Barrier::new(6);
        let producers = AtomicUsize::new(0);
        let consumers = AtomicUsize::new(0);
        thread::scope(|s| {
            for i in 0..6 {
                let (region, barrier, producers, consumers) =
                    (&region, &barrier, &producers, &consumers);
                s.spawn(move || {
                    barrier.wait();
                    if i % 2 == 0 {
                        match region.producer::<4>() {
                            Ok(p) => {
                                producers.fetch_add(1, Ordering::Relaxed);
                                drop(p);
                            }
                            Err(e) => assert_eq!(e, SharedError::RoleAlreadyClaimed),
                        }
                    } else {
                        match region.consumer::<4>() {
                            Ok(c) => {
                                consumers.fetch_add(1, Ordering::Relaxed);
                                drop(c);
                            }
                            Err(e) => assert_eq!(e, SharedError::RoleAlreadyClaimed),
                        }
                    }
                });
            }
        });
        assert_eq!(producers.load(Ordering::Relaxed), 1);
        assert_eq!(consumers.load(Ordering::Relaxed), 1);
    }
}

/// Before the producer attaches it counts as alive, so a consumer that
/// attached first never reports the stream drained.
#[test]
fn unattached_counterpart_is_alive_and_late_attach_works() {
    let region = HeapRegion::new::<4>(3);
    let mut c = region.consumer::<4>().unwrap();
    assert!(c.is_producer_alive());
    assert!(!c.is_drained());
    assert_eq!(c.try_pop(), None);
    assert!(!c.is_drained());

    let mut p = region.producer::<4>().unwrap();
    assert!(p.is_consumer_alive());
    p.try_push(*b"late").unwrap();
    drop(p);
    assert!(!c.is_drained());
    assert_eq!(c.try_pop(), Some(*b"late"));
    assert!(c.is_drained());

    // Late consumer: the producer has already filled the queue and closed.
    let region = HeapRegion::new::<4>(3);
    let mut p = region.producer::<4>().unwrap();
    assert!(p.is_consumer_alive(), "unattached consumer counts as alive");
    assert_eq!(p.push_slice(&[*b"one.", *b"two.", *b"thre", *b"four"]), 3);
    drop(p);
    let mut c = region.consumer::<4>().unwrap();
    assert!(!c.is_producer_alive());
    assert!(!c.is_drained());
    assert!(c.is_full());
    let mut out = [[0; 4]; 4];
    assert_eq!(c.pop_slice(&mut out), 3);
    assert_eq!(&out[..3], &[*b"one.", *b"two.", *b"thre"]);
    assert!(c.is_drained());
}

/// Reinitializing a quiescent region with a fresh generation starts over;
/// the old generation number no longer attaches.
#[test]
fn reinitialize_with_fresh_generation() {
    let region = HeapRegion::new::<4>(3);
    {
        let (mut p, c) = region.attach::<4>();
        p.try_push(*b"old.").unwrap();
        // The consumer "dies" holding its role: forget it.
        std::mem::forget(c);
    }
    let old = region.generation;
    let new = common::fresh_generation();
    // SAFETY: quiescent (no endpoint is usable), exclusive, fresh generation.
    unsafe { shm::initialize::<4>(region.base, region.len(), 3, new) }.unwrap();
    // SAFETY: valid region; rejected on the generation.
    let stale = unsafe { shm::attach_producer::<4>(region.base, region.len(), 3, old) };
    assert_eq!(stale.err(), Some(SharedError::GenerationMismatch));
    // SAFETY: freshly initialized for `new`.
    let mut p = unsafe { shm::attach_producer::<4>(region.base, region.len(), 3, new) }.unwrap();
    // SAFETY: as for the producer.
    let mut c = unsafe { shm::attach_consumer::<4>(region.base, region.len(), 3, new) }.unwrap();
    assert_eq!(c.try_pop(), None, "old records are discarded");
    p.try_push(*b"new.").unwrap();
    assert_eq!(c.try_pop(), Some(*b"new."));
}

/// The region stores no process-local state: two regions at different
/// addresses, driven through the same operations, have byte-identical
/// headers, and no header word looks like an address inside either region.
#[test]
fn region_contains_no_addresses() {
    let generation = common::fresh_generation();
    let make = || HeapRegion::with_generation::<8>(5, generation);
    let (a, b) = (make(), make());
    assert_ne!(a.base, b.base);
    for region in [&a, &b] {
        let (mut p, mut c) = region.attach::<8>();
        for i in 0..7u64 {
            p.try_push(i.to_le_bytes()).unwrap();
            if i % 2 == 0 {
                c.try_pop().unwrap();
            }
        }
        let _ = c.peek();
        drop((p, c));
    }
    let (ha, hb) = (a.header(), b.header());
    assert_eq!(ha, hb);
    let ranges = [&a, &b].map(|r| r.base.as_ptr().addr()..r.base.as_ptr().addr() + r.len());
    for chunk in ha.as_chunks::<W>().0 {
        let word = usize::from_ne_bytes(*chunk);
        for range in &ranges {
            assert!(
                !range.contains(&word),
                "header word {word:#x} is an address"
            );
        }
    }
}

#[test]
fn shared_error_traits() {
    let all = [
        SharedError::ZeroCapacity,
        SharedError::CapacityTooLarge { requested: 3 },
        SharedError::LayoutTooLarge,
        SharedError::RegionTooSmall {
            required: 320,
            provided: 64,
        },
        SharedError::Misaligned { required: 64 },
        SharedError::InvalidHeader,
        SharedError::IncompatibleFormat,
        SharedError::ConfigurationMismatch,
        SharedError::GenerationMismatch,
        SharedError::RoleAlreadyClaimed,
    ];
    for error in all {
        let copy = error;
        assert_eq!(copy, error);
        assert!(!error.to_string().is_empty());
        assert!(!format!("{error:?}").is_empty());
        let _: &dyn std::error::Error = &error;
    }
    assert!(all[3].to_string().contains("320"));
}
