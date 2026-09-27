// An endpoint's drop may run final cleanup through the storage, so the
// storage cannot be dropped (or go out of scope) while an endpoint that
// will still be dropped exists, even if the endpoint is never used again.
use spookycircle::StaticStorage;

fn main() {
    let (producer, consumer);
    {
        let storage = StaticStorage::<String, 2>::new();
        (producer, consumer) = storage.try_split().unwrap();
    }
    drop((producer, consumer));
}
