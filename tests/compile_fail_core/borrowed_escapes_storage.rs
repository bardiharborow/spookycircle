// Borrowed endpoints cannot outlive the storage object that holds their
// control state.
use spookycircle::{BorrowedProducer, BorrowedStorage, Slot};

fn escape<'s>(slots: &'s mut [Slot<u8>]) -> BorrowedProducer<'s, u8> {
    let storage = BorrowedStorage::new(slots).unwrap();
    let (producer, _consumer) = storage.try_split().unwrap();
    producer
}

fn main() {
    let mut slots = [const { Slot::<u8>::new() }; 2];
    let _ = escape(&mut slots);
}
