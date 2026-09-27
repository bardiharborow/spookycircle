//! Atomic and interior-mutability primitives behind one internal abstraction.
//!
//! Production builds re-export the `core` primitives directly, so this layer
//! costs nothing. Builds compiled with `--cfg loom` swap in Loom's instrumented
//! equivalents so the model checker can explore every interleaving of the
//! queue protocol.

/// Defines a constructor that is a `const fn` in production builds and a
/// plain `fn` under Loom, whose instrumented atomics and cells must be
/// registered with the model at runtime and so cannot be built in constant
/// evaluation.
macro_rules! const_unless_loom {
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident($($param:ident: $param_ty:ty),* $(,)?) -> $ret:ty $body:block
    ) => {
        #[cfg(not(loom))]
        $(#[$attr])*
        $vis const fn $name($($param: $param_ty),*) -> $ret $body

        #[cfg(loom)]
        $(#[$attr])*
        $vis fn $name($($param: $param_ty),*) -> $ret $body
    };
}

#[cfg(not(loom))]
pub(crate) use self::real::*;

#[cfg(loom)]
pub(crate) use self::model::*;

#[cfg(not(loom))]
mod real {
    #[cfg(test)]
    pub(crate) use core::sync::atomic::AtomicU8;
    pub(crate) use core::sync::atomic::{AtomicUsize, Ordering};

    /// Thin wrapper over [`core::cell::UnsafeCell`] with a closure-based API
    /// that matches `loom::cell::UnsafeCell`.
    ///
    /// `#[repr(transparent)]` keeps a `Slot<T>` layout-identical to
    /// `MaybeUninit<T>`, which is what makes the zero-sized-type reasoning
    /// in `raw/mod.rs` valid.
    #[repr(transparent)]
    pub(crate) struct UnsafeCell<T>(core::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        /// Creates a cell holding `value`; usable in constant evaluation.
        #[inline(always)]
        pub(crate) const fn new(value: T) -> Self {
            Self(core::cell::UnsafeCell::new(value))
        }

        /// Runs `f` with a shared-access raw pointer to the contents.
        ///
        /// The pointer is obtained through `core::cell::UnsafeCell::get`, so it
        /// carries the provenance of the cell's allocation. The caller is
        /// responsible for proving that no exclusive access is live.
        #[inline(always)]
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        /// Runs `f` with an exclusive-access raw pointer to the contents.
        ///
        /// The pointer is obtained through `core::cell::UnsafeCell::get`. The
        /// caller is responsible for proving that no other access is live.
        #[inline(always)]
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }

        /// Returns a pointer to the contents of the cell at `this`, without
        /// creating a reference, as [`core::cell::UnsafeCell::raw_get`]
        /// does. The result keeps `this`'s provenance, so a pointer into an
        /// array of cells can reach past this cell.
        ///
        /// Not available under Loom, which cannot track such an access; see
        /// the bulk copies in `raw/mod.rs`.
        #[inline(always)]
        pub(crate) const fn raw_get(this: *const Self) -> *mut T {
            // `repr(transparent)`: the wrapper and the inner cell share one
            // address and layout.
            core::cell::UnsafeCell::raw_get(this.cast::<core::cell::UnsafeCell<T>>())
        }
    }
}

#[cfg(loom)]
mod model {
    pub(crate) use loom::cell::{ConstPtr, MutPtr, UnsafeCell};
    pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};
}
