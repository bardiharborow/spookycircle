// `reset` needs exclusive access: not while an endpoint or a peek
// reference from the current session is usable.
use spookycircle::{BorrowedStorage, Slot};

fn main() {
    let mut slots = [const { Slot::<String>::new() }; 2];
    let mut storage = BorrowedStorage::new(&mut slots).unwrap();
    let (mut producer, mut consumer) = storage.try_split().unwrap();
    producer.try_push("a".to_owned()).unwrap();
    storage.reset();
    let head = consumer.peek().unwrap();
    println!("{head}");
}
