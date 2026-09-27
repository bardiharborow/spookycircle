// A non-`Send` element type makes storage non-`Sync` (so it cannot be a
// `static`) and keeps borrowed endpoints on their thread.
use std::rc::Rc;
use spookycircle::{BorrowedStorage, Slot, StaticStorage};

static STORAGE: StaticStorage<Rc<u8>, 2> = StaticStorage::new();

fn main() {
    let mut slots = [const { Slot::<Rc<u8>>::new() }; 2];
    let storage = BorrowedStorage::new(&mut slots).unwrap();
    let (mut producer, _consumer) = storage.try_split().unwrap();
    std::thread::scope(|s| {
        s.spawn(move || producer.try_push(Rc::new(1)).ok());
    });
}
