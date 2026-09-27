// Endpoints cannot be cloned or copied.
use spookycircle::bounded;

fn main() {
    let (producer, consumer) = bounded::<u8>(4).unwrap();
    let _p2 = producer.clone();
    let _c2 = consumer.clone();
}
