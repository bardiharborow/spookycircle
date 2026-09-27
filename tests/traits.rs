//! Compile-time trait tests.
//!
//! Positive properties are checked with bound-constrained functions; negative
//! properties (`!Sync`, `!Clone`, `!Copy`) use the autoref-ambiguity trick so
//! that a spurious implementation turns into a compile error. Cases that must
//! fail to compile (a non-`Send` `T` crossing a thread, calling a `&mut self`
//! method through a shared reference) are `trybuild` tests under
//! `tests/compile_fail/`; they run when `SPOOKYCIRCLE_COMPILE_FAIL=1` is set
//! because their expected diagnostics are tied to a compiler version.
//!
//! Directories, selected by the enabled features:
//!
//! * `compile_fail_core/`: borrowed and static storage (always);
//! * `compile_fail/`: heap-owned endpoints (`alloc`);
//! * `compile_fail_no_alloc/`: heap items are absent without `alloc`;
//! * `compile_fail_no_shm/`: the shared-memory module is absent without
//!   `shared-memory`.

// Loom-instrumented atomics only work inside a Loom model; see tests/loom.rs.
#![cfg(not(loom))]

use std::{cell::Cell, rc::Rc, sync::Mutex};

use spookycircle::{
    BorrowedConsumer, BorrowedProducer, BorrowedStorage, CreateError, Full, Slot, SplitError,
    StaticStorage,
};
#[cfg(feature = "alloc")]
use spookycircle::{Consumer, Producer};

/// Fails to compile if `$ty` implements `$trait`.
macro_rules! assert_not_impl {
    ($ty:ty: $($trait:tt)+) => {
        const _: () = {
            trait AmbiguousIfImpl<A> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            struct Invalid;
            impl<T: ?Sized + $($trait)+> AmbiguousIfImpl<Invalid> for T {}
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        };
    };
}

// The helpers are only named inside `const _` blocks below; Rust 1.88 (the
// MSRV) does not count that as a use and would reject the file under
// `-D warnings` without the allow.
#[allow(dead_code)]
fn assert_send<T: Send>() {}
#[allow(dead_code)]
fn assert_sync<T: Sync>() {}
#[allow(dead_code)]
fn assert_copy<T: Copy>() {}
#[allow(dead_code)]
fn assert_clone<T: Clone>() {}
#[allow(dead_code)]
fn assert_error<T: std::error::Error>() {}
#[allow(dead_code)]
fn assert_eq_hash_ord<T: Eq + std::hash::Hash + Ord>() {}

// Endpoints are `Send` for `T: Send`, including `T: !Sync`.
#[cfg(feature = "alloc")]
const _: () = {
    let _ = assert_send::<Producer<u8>>;
    let _ = assert_send::<Consumer<u8>>;
    let _ = assert_send::<Producer<Cell<u8>>>;
    let _ = assert_send::<Consumer<Cell<u8>>>;
    let _ = assert_send::<Producer<Box<dyn Send>>>;
    let _ = assert_send::<Consumer<Box<dyn Send>>>;
};

// Endpoints are never `Sync`, even for `T: Sync`.
#[cfg(feature = "alloc")]
assert_not_impl!(Producer<u8>: Sync);
#[cfg(feature = "alloc")]
assert_not_impl!(Consumer<u8>: Sync);
#[cfg(feature = "alloc")]
assert_not_impl!(Producer<Cell<u8>>: Sync);
#[cfg(feature = "alloc")]
assert_not_impl!(Consumer<Cell<u8>>: Sync);

// A non-`Send` `T` makes the endpoints non-`Send`.
#[cfg(feature = "alloc")]
assert_not_impl!(Producer<Rc<u8>>: Send);
#[cfg(feature = "alloc")]
assert_not_impl!(Consumer<Rc<u8>>: Send);

// Endpoints are never `Clone` or `Copy`.
#[cfg(feature = "alloc")]
assert_not_impl!(Producer<u8>: Clone);
#[cfg(feature = "alloc")]
assert_not_impl!(Consumer<u8>: Clone);
#[cfg(feature = "alloc")]
assert_not_impl!(Producer<u8>: Copy);
#[cfg(feature = "alloc")]
assert_not_impl!(Consumer<u8>: Copy);

// `Full<T>` follows `T` for the standard value traits.
const _: () = {
    let _ = assert_copy::<Full<u8>>;
    let _ = assert_clone::<Full<String>>;
    let _ = assert_eq_hash_ord::<Full<u32>>;
    let _ = assert_error::<Full<u32>>;
    let _ = assert_send::<Full<u32>>;
    let _ = assert_sync::<Full<u32>>;
};
assert_not_impl!(Full<String>: Copy);
assert_not_impl!(Full<Mutex<u8>>: Clone);
assert_not_impl!(Full<Cell<u8>>: Sync);

// `CreateError` traits.
const _: () = {
    let _ = assert_copy::<CreateError>;
    let _ = assert_clone::<CreateError>;
    let _ = assert_error::<CreateError>;
    let _ = assert_send::<CreateError>;
    let _ = assert_sync::<CreateError>;
};

// Queries take `&self`; state-changing and peek operations take `&mut self`.
#[cfg(feature = "alloc")]
#[allow(dead_code)]
fn shared_reference_queries(p: &Producer<u8>, c: &Consumer<u8>) {
    let _ = p.capacity();
    let _ = p.len();
    let _ = p.remaining_capacity();
    let _ = p.is_empty();
    let _ = p.is_full();
    let _ = p.is_consumer_alive();
    let _ = c.capacity();
    let _ = c.len();
    let _ = c.remaining_capacity();
    let _ = c.is_empty();
    let _ = c.is_full();
    let _ = c.is_producer_alive();
    let _ = c.is_drained();
}

/// A `peek` reference must keep the consumer borrowed.
#[cfg(feature = "alloc")]
#[allow(dead_code)]
fn peek_borrows_consumer(c: &mut Consumer<String>) -> Option<&str> {
    c.peek().map(String::as_str)
}

// Borrowed endpoints: `Send` iff `T: Send` (including `T: !Sync`), never
// `Sync`, `Clone`, or `Copy`.
const _: () = {
    let _ = assert_send::<BorrowedProducer<'static, u8>>;
    let _ = assert_send::<BorrowedConsumer<'static, u8>>;
    let _ = assert_send::<BorrowedProducer<'static, Cell<u8>>>;
    let _ = assert_send::<BorrowedConsumer<'static, Cell<u8>>>;
};
assert_not_impl!(BorrowedProducer<'static, u8>: Sync);
assert_not_impl!(BorrowedConsumer<'static, u8>: Sync);
assert_not_impl!(BorrowedProducer<'static, Rc<u8>>: Send);
assert_not_impl!(BorrowedConsumer<'static, Rc<u8>>: Send);
assert_not_impl!(BorrowedProducer<'static, u8>: Clone);
assert_not_impl!(BorrowedConsumer<'static, u8>: Clone);
assert_not_impl!(BorrowedProducer<'static, u8>: Copy);
assert_not_impl!(BorrowedConsumer<'static, u8>: Copy);

// `Slot<T>`: `Send` iff `T: Send`; never `Sync`, `Clone`, or `Copy`.
const _: () = {
    let _ = assert_send::<Slot<u8>>;
    let _ = assert_send::<Slot<Cell<u8>>>;
};
assert_not_impl!(Slot<Rc<u8>>: Send);
assert_not_impl!(Slot<u8>: Sync);
assert_not_impl!(Slot<u8>: Clone);
assert_not_impl!(Slot<u8>: Copy);

// Storage: `Send` and `Sync` iff `T: Send` (not `T: Sync`); never `Clone`
// or `Copy`.
const _: () = {
    let _ = assert_send::<BorrowedStorage<'static, u8>>;
    let _ = assert_sync::<BorrowedStorage<'static, u8>>;
    let _ = assert_send::<BorrowedStorage<'static, Cell<u8>>>;
    let _ = assert_sync::<BorrowedStorage<'static, Cell<u8>>>;
    let _ = assert_send::<StaticStorage<u8, 4>>;
    let _ = assert_sync::<StaticStorage<u8, 4>>;
    let _ = assert_sync::<StaticStorage<Cell<u8>, 4>>;
};
assert_not_impl!(BorrowedStorage<'static, Rc<u8>>: Sync);
assert_not_impl!(BorrowedStorage<'static, Rc<u8>>: Send);
assert_not_impl!(StaticStorage<Rc<u8>, 4>: Sync);
assert_not_impl!(StaticStorage<Rc<u8>, 4>: Send);
assert_not_impl!(BorrowedStorage<'static, u8>: Clone);
assert_not_impl!(StaticStorage<u8, 4>: Clone);
assert_not_impl!(StaticStorage<u8, 4>: Copy);

// `SplitError` traits.
const _: () = {
    let _ = assert_copy::<SplitError>;
    let _ = assert_error::<SplitError>;
    let _ = assert_send::<SplitError>;
    let _ = assert_sync::<SplitError>;
};

// Borrowed element types need not be `'static`.
#[allow(dead_code)]
fn borrowed_elements<'a>(slots: &'a mut [Slot<&'a str>], text: &'a str) {
    let storage = BorrowedStorage::new(slots).unwrap();
    let (mut p, _c) = storage.try_split().unwrap();
    let _ = p.try_push(text);
}

// Safe static splitting without `static mut`; the endpoints are `'static`.
#[allow(dead_code)]
fn static_split() -> (BorrowedProducer<'static, u8>, BorrowedConsumer<'static, u8>) {
    static STORAGE: StaticStorage<u8, 2> = StaticStorage::new();
    STORAGE.try_split().unwrap()
}

// Borrowed queries take `&self`; state-changing and peek operations take
// `&mut self` (the negative case is `compile_fail_core/borrowed_needs_mut.rs`).
#[allow(dead_code)]
fn borrowed_shared_reference_queries(p: &BorrowedProducer<'_, u8>, c: &BorrowedConsumer<'_, u8>) {
    let _ = (p.capacity(), p.len(), p.remaining_capacity(), p.is_empty());
    let _ = (p.is_full(), p.is_consumer_alive());
    let _ = (c.capacity(), c.len(), c.remaining_capacity(), c.is_empty());
    let _ = (c.is_full(), c.is_producer_alive(), c.is_drained());
}

#[cfg(feature = "shared-memory")]
mod shared {
    use super::*;
    use spookycircle::shared_memory::{SharedConsumer, SharedError, SharedProducer};

    // Shared endpoints: `Send`; never `Sync`, `Clone`, or `Copy`.
    const _: () = {
        let _ = assert_send::<SharedProducer<'static, 8>>;
        let _ = assert_send::<SharedConsumer<'static, 8>>;
        let _ = assert_copy::<SharedError>;
        let _ = assert_error::<SharedError>;
        let _ = assert_send::<SharedError>;
        let _ = assert_sync::<SharedError>;
    };
    assert_not_impl!(SharedProducer<'static, 8>: Sync);
    assert_not_impl!(SharedConsumer<'static, 8>: Sync);
    assert_not_impl!(SharedProducer<'static, 8>: Clone);
    assert_not_impl!(SharedConsumer<'static, 8>: Clone);
    assert_not_impl!(SharedProducer<'static, 8>: Copy);
    assert_not_impl!(SharedConsumer<'static, 8>: Copy);
}

/// Every endpoint is aligned, and so padded, to its own cache line(s), so a
/// producer and a consumer stored side by side never share one.
#[test]
fn endpoints_are_cache_line_aligned() {
    use std::mem::{align_of, size_of};

    fn check<E>(line: usize) {
        assert_eq!(align_of::<E>(), line, "{}", std::any::type_name::<E>());
        assert_eq!(size_of::<E>() % line, 0, "{}", std::any::type_name::<E>());
    }
    let line = if cfg!(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64"
    )) {
        128
    } else {
        64
    };
    #[cfg(feature = "alloc")]
    {
        check::<Producer<u64>>(line);
        check::<Consumer<u64>>(line);
    }
    check::<BorrowedProducer<'static, u64>>(line);
    check::<BorrowedConsumer<'static, u64>>(line);
    #[cfg(feature = "shared-memory")]
    {
        use spookycircle::shared_memory::{SharedConsumer, SharedProducer};
        check::<SharedProducer<'static, 8>>(line);
        check::<SharedConsumer<'static, 8>>(line);
    }
}

#[test]
fn compile_fail_cases() {
    if std::env::var_os("SPOOKYCIRCLE_COMPILE_FAIL").is_none() {
        eprintln!("skipping trybuild cases; set SPOOKYCIRCLE_COMPILE_FAIL=1 to run them");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail_core/*.rs");
    #[cfg(feature = "alloc")]
    t.compile_fail("tests/compile_fail/*.rs");
    #[cfg(not(feature = "alloc"))]
    t.compile_fail("tests/compile_fail_no_alloc/*.rs");
    #[cfg(not(feature = "shared-memory"))]
    t.compile_fail("tests/compile_fail_no_shm/*.rs");
}
