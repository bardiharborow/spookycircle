// Neither storage nor its endpoints can outlive the borrowed slots.
use spookycircle::{BorrowedStorage, Slot};

fn main() {
    let storage;
    {
        let mut slots = [const { Slot::<u8>::new() }; 2];
        storage = BorrowedStorage::new(&mut slots).unwrap();
    }
    let (mut producer, _consumer) = storage.try_split().unwrap();
    producer.try_push(1).unwrap();
}
