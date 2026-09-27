// The slots stay exclusively borrowed while the storage object exists.
use spookycircle::{BorrowedStorage, Slot};

fn main() {
    let mut slots = [const { Slot::<u8>::new() }; 2];
    let storage = BorrowedStorage::new(&mut slots).unwrap();
    let again = &slots[0];
    let _ = storage.try_split();
    let _ = again;
}
