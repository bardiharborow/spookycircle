//! Code-generation inspection target.
//!
//! Each data-path method is wrapped in an `#[inline(never)]` function so that
//! its machine code can be located in the assembly output:
//!
//! ```text
//! cargo xtask inspect-codegen            # host target
//! cargo xtask inspect-codegen x86_64-unknown-linux-gnu
//! ```
//!
//! The audit reports, per function, any calls (there must be none on the
//! success/full/empty paths), any fences or read-modify-write instructions
//! (there must be none), and any division (there must be none).
//!
//! The unprefixed wrappers cover the heap-owned endpoints; `codegen_static_*`
//! cover borrowed/static endpoints and `codegen_shared_*` shared-region
//! endpoints, which run the same data path and
//! must meet the same budget. `codegen_wide_*` (a 32-byte `Copy`
//! element) and `codegen_owned_*` (a `String`, which has drop glue) check that
//! moving larger and non-`Copy` values through a slot adds no calls.
//! `codegen_wide_try_pop_into` checks that `try_pop_into` moves a large
//! element from its slot straight to the destination.
//!
//! The wrappers return the endpoint's result unchanged rather than
//! discarding it: dropping a returned `Full<String>` would call the
//! deallocator inside the wrapper, which is the caller's cost, not the
//! queue's.

#![allow(missing_docs, clippy::missing_errors_doc)]

use std::{hint::black_box, mem::MaybeUninit};

use spookycircle::{
    BorrowedConsumer, BorrowedProducer, Consumer, Full, Producer, StaticStorage, bounded,
};

type Wide = [u64; 4];

#[inline(never)]
pub fn codegen_try_push(p: &mut Producer<u64>, value: u64) -> bool {
    p.try_push(value).is_ok()
}

#[inline(never)]
pub fn codegen_try_pop(c: &mut Consumer<u64>) -> Option<u64> {
    c.try_pop()
}

#[inline(never)]
pub fn codegen_try_pop_into(c: &mut Consumer<u64>, out: &mut MaybeUninit<u64>) -> bool {
    c.try_pop_into(out).is_some()
}

#[inline(never)]
pub fn codegen_peek(c: &mut Consumer<u64>) -> Option<u64> {
    c.peek().copied()
}

#[inline(never)]
pub fn codegen_peek_mut(c: &mut Consumer<u64>) -> bool {
    match c.peek_mut() {
        Some(v) => {
            *v += 1;
            true
        }
        None => false,
    }
}

#[inline(never)]
#[must_use]
pub fn codegen_producer_len(p: &Producer<u64>) -> usize {
    p.len()
}

#[inline(never)]
#[must_use]
pub fn codegen_consumer_len(c: &Consumer<u64>) -> usize {
    c.len()
}

#[inline(never)]
#[must_use]
pub fn codegen_is_drained(c: &Consumer<u64>) -> bool {
    c.is_drained()
}

#[inline(never)]
#[must_use]
pub fn codegen_is_consumer_alive(p: &Producer<u64>) -> bool {
    p.is_consumer_alive()
}

#[inline(never)]
pub fn codegen_push_slice(p: &mut Producer<u64>, source: &[u64]) -> usize {
    p.push_slice(source)
}

#[inline(never)]
pub fn codegen_pop_slice(c: &mut Consumer<u64>, destination: &mut [u64]) -> usize {
    c.pop_slice(destination)
}

#[inline(never)]
pub fn codegen_wide_try_push(p: &mut Producer<Wide>, value: Wide) -> Result<(), Full<Wide>> {
    p.try_push(value)
}

#[inline(never)]
pub fn codegen_wide_try_pop(c: &mut Consumer<Wide>) -> Option<Wide> {
    c.try_pop()
}

// `try_pop` must return the element by value, which for a 32-byte element
// goes through a stack temporary; `try_pop_into` copies the slot straight to
// `out` and must not.
#[inline(never)]
pub fn codegen_wide_try_pop_into(c: &mut Consumer<Wide>, out: &mut MaybeUninit<Wide>) -> bool {
    c.try_pop_into(out).is_some()
}

#[inline(never)]
pub fn codegen_owned_try_push(p: &mut Producer<String>, value: String) -> Result<(), Full<String>> {
    p.try_push(value)
}

#[inline(never)]
pub fn codegen_owned_try_pop(c: &mut Consumer<String>) -> Option<String> {
    c.try_pop()
}

// The static wrappers use `u32` elements: with `u64` their machine code is
// byte-identical to the heap wrappers', and LLVM merges them away. (The
// static `is_drained` never touches an element, so it is still merged into
// `codegen_is_drained`; identical code needs no separate audit.)
#[inline(never)]
pub fn codegen_static_try_push(p: &mut BorrowedProducer<'_, u32>, value: u32) -> bool {
    p.try_push(value).is_ok()
}

#[inline(never)]
pub fn codegen_static_try_pop(c: &mut BorrowedConsumer<'_, u32>) -> Option<u32> {
    c.try_pop()
}

#[inline(never)]
#[must_use]
pub fn codegen_static_is_drained(c: &BorrowedConsumer<'_, u32>) -> bool {
    c.is_drained()
}

mod shared {
    use spookycircle::shared_memory::{SharedConsumer, SharedProducer};

    #[inline(never)]
    pub fn codegen_shared_try_push(p: &mut SharedProducer<'_, 8>, value: [u8; 8]) -> bool {
        p.try_push(value).is_ok()
    }

    // Returned as `u64` so the result comes back in registers like the other
    // `try_pop` wrappers'; `Option<[u8; 8]>` is 9 bytes and would be returned
    // through memory, inflating the instruction count for reasons that have
    // nothing to do with the queue.
    #[inline(never)]
    pub fn codegen_shared_try_pop(c: &mut SharedConsumer<'_, 8>) -> Option<u64> {
        c.try_pop().map(u64::from_ne_bytes)
    }

    #[inline(never)]
    pub fn codegen_shared_is_drained(c: &SharedConsumer<'_, 8>) -> bool {
        c.is_drained()
    }

    pub fn exercise() {
        use spookycircle::shared_memory as shm;
        use std::{alloc, hint::black_box, ptr::NonNull};

        let layout = shm::layout::<8>(3).unwrap();
        // SAFETY: nonzero-size layout.
        let base = NonNull::new(unsafe { alloc::alloc(layout) }).unwrap();
        // SAFETY: fresh, exclusive memory of the required layout.
        unsafe { shm::initialize::<8>(base, layout.size(), 3, 1) }.unwrap();
        // SAFETY: initialized above; the endpoints drop before it is freed.
        let mut p = unsafe { shm::attach_producer::<8>(base, layout.size(), 3, 1) }.unwrap();
        // SAFETY: as for the producer.
        let mut c = unsafe { shm::attach_consumer::<8>(base, layout.size(), 3, 1) }.unwrap();
        black_box(codegen_shared_try_push(&mut p, black_box([1; 8])));
        black_box(codegen_shared_try_pop(&mut c));
        black_box(codegen_shared_is_drained(&c));
        drop((p, c));
        // SAFETY: allocated above with `layout`; both endpoints have dropped.
        unsafe { alloc::dealloc(base.as_ptr(), layout) };
    }
}

fn main() {
    static STORAGE: StaticStorage<u32, 3> = StaticStorage::new();
    let (mut sp, mut sc) = STORAGE.try_split().unwrap();
    black_box(codegen_static_try_push(&mut sp, black_box(1)));
    black_box(codegen_static_try_pop(&mut sc));
    black_box(codegen_static_is_drained(&sc));
    shared::exercise();

    let (mut p, mut c) = bounded::<u64>(black_box(3)).unwrap();
    black_box(codegen_try_push(&mut p, black_box(1)));
    black_box(codegen_producer_len(&p));
    black_box(codegen_is_consumer_alive(&p));
    black_box(codegen_peek(&mut c));
    black_box(codegen_peek_mut(&mut c));
    black_box(codegen_try_pop(&mut c));
    let mut into = MaybeUninit::uninit();
    black_box(codegen_try_push(&mut p, black_box(2)));
    black_box(codegen_try_pop_into(&mut c, black_box(&mut into)));
    black_box(codegen_consumer_len(&c));
    black_box(codegen_is_drained(&c));
    black_box(codegen_push_slice(&mut p, black_box(&[1, 2, 3, 4])));
    let mut out = [0u64; 4];
    black_box(codegen_pop_slice(&mut c, black_box(&mut out)));

    let (mut wp, mut wc) = bounded::<Wide>(black_box(3)).unwrap();
    black_box(codegen_wide_try_push(&mut wp, black_box([1, 2, 3, 4])).is_ok());
    black_box(codegen_wide_try_pop(&mut wc));
    let mut wide_into = MaybeUninit::uninit();
    black_box(codegen_wide_try_push(&mut wp, black_box([5, 6, 7, 8])).is_ok());
    black_box(codegen_wide_try_pop_into(
        &mut wc,
        black_box(&mut wide_into),
    ));

    let (mut op, mut oc) = bounded::<String>(black_box(3)).unwrap();
    black_box(codegen_owned_try_push(&mut op, black_box(String::from("x"))).is_ok());
    black_box(codegen_owned_try_pop(&mut oc));
}
