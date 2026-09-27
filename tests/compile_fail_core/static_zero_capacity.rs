// `StaticStorage` rejects a zero capacity at compile time.
use spookycircle::StaticStorage;

static EMPTY: StaticStorage<u32, 0> = StaticStorage::new();

fn main() {
    let _ = EMPTY.capacity();
}
