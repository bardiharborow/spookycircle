// Borrowed endpoints must be invariant in `T`, like the heap-owned ones:
// shortening a producer's element lifetime independently of its consumer or
// storage would let a short-lived reference escape.
use spookycircle::{BorrowedConsumer, BorrowedProducer};

fn shorten_producer<'q, 'a>(p: BorrowedProducer<'q, &'static str>) -> BorrowedProducer<'q, &'a str> {
    p
}

fn lengthen_consumer<'q, 'a>(c: BorrowedConsumer<'q, &'a str>) -> BorrowedConsumer<'q, &'static str> {
    c
}

fn main() {}
