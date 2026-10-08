//! Experimental software prefetch of upcoming slots (`prefetch` feature).
//!
//! The single-element push path prefetches, for store, the slot
//! [`DISTANCE`] bytes ahead of the current one, once per [`INTERVAL`] bytes,
//! so that the producer owns the line before the write that needs it.
//!
//! There is no consumer-side (load) hint. In the balanced steady state the
//! queue is nearly empty, so a slot ahead of `head` is usually one the
//! producer has not written yet: hinting it pulls the line away from the
//! producer mid-write, and benchmarks on Apple M1 measured that as 1.35 to
//! 1.46 times slower, never faster.
//!
//! Where the compiler provides `core::hint`'s prefetches, the hints go
//! through them on every target. `build.rs` detects this and sets
//! `spookycircle_hint_prefetch`: today that means nightly, under
//! `hint_prefetch` (rust-lang/rust#146941), and the same probe picks them up
//! without the gate once they are stable. Otherwise they are a raw aarch64
//! `PRFM` and compile to nothing elsewhere. Once `hint_prefetch` is stable
//! and within the MSRV, the `asm!` path can go.
//!
//! A prefetch is only a hint: it neither reads nor writes memory as far as
//! the program can observe, never faults, and so has no bearing on the
//! queue's safety argument. It can still move a cache line between cores
//! early, which is the whole point and also the risk, so whether it pays
//! off is a benchmark question:
//!
//! ```text
//! cargo bench --features prefetch                 # stable: aarch64 `asm!`
//! cargo +nightly bench --features prefetch        # nightly: `core::hint`
//! RUSTFLAGS="-C target-feature=+prfchw" \
//!     cargo +nightly bench --features prefetch    # nightly, x86 and x86_64
//! ```
//!
//! On x86 the hint is issued only with `prfchw` (see [`ENABLED`]), so that
//! it lowers to `prefetchw`. Every x86 CPU should either implement
//! `prefetchw` or execute it as a no-op, but the flag applies to the whole
//! build, so it belongs on the command line, not in this crate.

use core::{mem::size_of, ptr::NonNull};

/// Bytes ahead of the current slot to prefetch. On Apple M1, two 64-byte
/// slots ahead beat 1024 bytes (3.5 to 5 times faster than no hint, against
/// 2.6 to 3).
const DISTANCE: usize = 128;

/// Bytes between hints: a slot is hinted only if it is the first to start
/// in its `INTERVAL`-aligned block. 64 even on Apple silicon, whose lines
/// are 128 bytes; measurements that favoured 64 over 128 there also had
/// load hints on, so the choice is not settled. Only decides how often a
/// hint is issued; a poor value costs redundant or missed hints, never
/// correctness.
const INTERVAL: usize = 64;

/// Whether this build issues hints: with `prefetch` on, every target where
/// `build.rs` found `core::hint`'s prefetches, and aarch64 otherwise
/// (through `asm!`), except on x86 without `prfchw`. There
/// `prefetch_write` can only lower to the read prefetch `prefetcht0`, which
/// brings the line in shared while the consumer still holds its copy from
/// the previous lap, so the store has to request ownership regardless: a
/// second coherence transaction rather than an early one. Loom and Kani
/// model the protocol, not the cache, so they never see a hint; nor does
/// Miri on the `asm!` path, which it cannot run.
const ENABLED: bool = cfg!(all(
    feature = "prefetch",
    not(any(loom, kani)),
    any(
        spookycircle_hint_prefetch,
        all(target_arch = "aarch64", not(miri))
    ),
    not(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        not(target_feature = "prfchw")
    ))
));

/// Returns the physical slot `DISTANCE` bytes past `index`, wrapped into
/// `0..capacity`, or `None` when there is nothing useful to prefetch (hints
/// disabled, zero-sized `T`, or a ring no longer than the distance, where
/// the target would wrap onto or behind the current slot).
#[inline(always)]
fn ahead<T>(index: usize, capacity: usize) -> Option<usize> {
    if !ENABLED || size_of::<T>() == 0 {
        return None;
    }
    // Constant per `T`, so this folds to an immediate.
    let step = DISTANCE.checked_div(size_of::<T>()).map_or(1, |s| s.max(1));
    if step >= capacity {
        return None;
    }
    // `index < capacity` and `step < capacity`, so one subtraction wraps,
    // and the target stays inside the slot array: off neighbouring
    // allocations' lines, which a prefetch-for-store would otherwise steal
    // from whichever core owns them.
    let target = index.wrapping_add(step);
    Some(if target >= capacity {
        target.wrapping_sub(capacity)
    } else {
        target
    })
}

/// Whether slot `index` of the array at `slots` is the first slot to start
/// in its [`INTERVAL`]-aligned block, so that one hint per block suffices:
/// with several slots to a block, hinting every slot would repeat the same
/// prefetch.
///
/// Uses the slot's address, not its index, because a borrowed slot array
/// need not be aligned.
#[inline(always)]
fn starts_interval<T>(slots: *mut T, index: usize) -> bool {
    if size_of::<T>() >= INTERVAL {
        // Every slot starts in a block of its own.
        return true;
    }
    // `INTERVAL` is a power of two, so this is the offset within the block.
    let offset = slots.wrapping_add(index).addr() & (INTERVAL - 1);
    offset < size_of::<T>()
}

/// Hints that the producer will soon write the slot ahead of `index`.
#[inline(always)]
pub(crate) fn for_store<T>(slots: NonNull<T>, index: usize, capacity: usize) {
    if let Some(target) = ahead::<T>(index, capacity)
        && starts_interval(slots.as_ptr(), target)
    {
        hint(slots.as_ptr(), target);
    }
}

/// Issues the hint for slot `index` of the array at `slots` through
/// `core::hint`.
#[cfg(all(
    feature = "prefetch",
    not(any(loom, kani)),
    spookycircle_hint_prefetch
))]
#[inline(always)]
fn hint<T>(slots: *mut T, index: usize) {
    core::hint::prefetch_write(slots.wrapping_add(index), core::hint::Locality::L1);
}

/// Issues the hint for slot `index` of the array at `slots` as a raw
/// `PRFM PSTL1KEEP`, for stable toolchains, which lack `core::hint`'s prefetches.
///
/// Chooses operands so that the compiler emits what LLVM does for the
/// intrinsic. Slots smaller than [`INTERVAL`] pass the finished address,
/// which `starts_interval` has already computed. Larger slots pass base
/// and offset as separate registers, so the addition happens in the
/// addressing mode rather than in a separate `add`. `PRFM` can scale its
/// register offset only by its access size, 8, so a slot size that is a
/// multiple of 8 is passed as a count of 8-byte words and scaled there
/// (`[base, words, lsl #3]`); any other size is passed as a byte offset
/// (`[base, offset]`).
#[cfg(all(
    feature = "prefetch",
    not(any(loom, kani, miri, spookycircle_hint_prefetch)),
    target_arch = "aarch64"
))]
#[inline(always)]
#[expect(
    clippy::pointers_in_nomem_asm_block,
    reason = "`PRFM` only names an address; it accesses no memory"
)]
fn hint<T>(slots: *mut T, index: usize) {
    if size_of::<T>() < INTERVAL {
        // `starts_interval` has just computed this address, so passing it
        // whole lets the compiler reuse it rather than recompute an offset.
        let address = slots.wrapping_add(index);
        // SAFETY: `PRFM` is a hint with no architecturally visible effect
        // on memory or registers and never faults, whatever the address;
        // `nomem` is accurate because it does not access memory as far as
        // the abstract machine is concerned.
        unsafe {
            core::arch::asm!(
                "prfm pstl1keep, [{address}]",
                address = in(reg) address,
                options(nomem, nostack, preserves_flags),
            );
        }
    } else if size_of::<T>().is_multiple_of(8) {
        // `index < capacity`, so neither product here or below exceeds the
        // slot array's size in bytes and neither can overflow.
        let words = index.wrapping_mul(size_of::<T>() / 8);
        // SAFETY: as above.
        unsafe {
            core::arch::asm!(
                "prfm pstl1keep, [{base}, {words}, lsl #3]",
                base = in(reg) slots,
                words = in(reg) words,
                options(nomem, nostack, preserves_flags),
            );
        }
    } else {
        let offset = index.wrapping_mul(size_of::<T>());
        // SAFETY: as above.
        unsafe {
            core::arch::asm!(
                "prfm pstl1keep, [{base}, {offset}]",
                base = in(reg) slots,
                offset = in(reg) offset,
                options(nomem, nostack, preserves_flags),
            );
        }
    }
}

/// No hint: `prefetch` is off, or neither path applies to this build.
#[cfg(not(all(
    feature = "prefetch",
    not(any(loom, kani)),
    any(
        spookycircle_hint_prefetch,
        all(target_arch = "aarch64", not(miri))
    )
)))]
#[inline(always)]
fn hint<T>(_slots: *mut T, _index: usize) {}
