// Borrowed endpoints are not `Sync`: a shared reference cannot cross a
// thread boundary.
use spookycircle::StaticStorage;

fn main() {
    let storage = StaticStorage::<u8, 4>::new();
    let (producer, consumer) = storage.try_split().unwrap();
    std::thread::scope(|s| {
        s.spawn(|| producer.len());
        s.spawn(|| consumer.len());
    });
}
