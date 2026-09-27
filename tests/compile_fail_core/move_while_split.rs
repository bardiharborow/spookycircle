// Storage cannot move while its endpoints borrow it.
use spookycircle::StaticStorage;

fn main() {
    let storage = StaticStorage::<u8, 2>::new();
    let (mut producer, _consumer) = storage.try_split().unwrap();
    let moved = Box::new(storage);
    producer.try_push(1).unwrap();
    drop(moved);
}
