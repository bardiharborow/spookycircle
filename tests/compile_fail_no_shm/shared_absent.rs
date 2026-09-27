// Without the `shared-memory` feature the shared-region module does not
// exist.
use spookycircle::shared_memory;

fn main() {
    let _ = shared_memory::layout::<8>(1);
}
