// Without the `alloc` feature the heap-owned API does not exist.
use spookycircle::{Consumer, Producer, bounded};

fn main() {
    let _: Option<(Producer<u8>, Consumer<u8>)> = bounded(1).ok();
}
