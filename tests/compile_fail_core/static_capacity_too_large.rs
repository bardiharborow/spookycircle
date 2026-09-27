// `StaticStorage` rejects a capacity above `MAX_CAPACITY` at compile time.
// (Run-time construction is covered by the `compile_fail` doctest on
// `StaticStorage::new`: `cargo check`, which trybuild uses, does not report
// post-monomorphization errors there.)
use spookycircle::{MAX_CAPACITY, StaticStorage};

static HUGE: StaticStorage<(), { MAX_CAPACITY + 1 }> = StaticStorage::new();

fn main() {
    let _ = HUGE.capacity();
}
