// Endpoints must be invariant in `T`. If `Producer<T>` were covariant, safe
// code could shorten `Producer<&'static str>` to `Producer<&'a str>`, push a
// short-lived reference, and pop it from the still-`'static` consumer after
// the referent is freed. Contravariance would allow the mirror-image attack
// through the consumer. Invariance currently comes from the `UnsafeCell`
// inside the private slot type; this test keeps a refactor from losing it.
use spookycircle::{Consumer, Producer};

fn shorten_producer<'a>(p: Producer<&'static str>) -> Producer<&'a str> {
    p
}

fn lengthen_producer<'a>(p: Producer<&'a str>) -> Producer<&'static str> {
    p
}

fn shorten_consumer<'a>(c: Consumer<&'static str>) -> Consumer<&'a str> {
    c
}

fn lengthen_consumer<'a>(c: Consumer<&'a str>) -> Consumer<&'static str> {
    c
}

fn main() {}
