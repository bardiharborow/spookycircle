// Endpoints are not `Sync`: a shared reference cannot cross a thread boundary.
use spookycircle::bounded;

fn main() {
    let (producer, consumer) = bounded::<u8>(4).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| producer.len());
        s.spawn(|| consumer.len());
    });
}
