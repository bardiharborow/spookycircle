// A non-`Send` element type must prevent moving an endpoint to another thread.
use std::rc::Rc;
use spookycircle::bounded;

fn main() {
    let (mut producer, mut consumer) = bounded::<Rc<u8>>(4).unwrap();
    std::thread::spawn(move || {
        producer.try_push(Rc::new(1)).ok();
    });
    std::thread::spawn(move || {
        consumer.try_pop();
    });
}
