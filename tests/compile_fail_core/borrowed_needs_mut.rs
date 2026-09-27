// Borrowed state-changing and peek operations require `&mut`.
use spookycircle::StaticStorage;

fn main() {
    let storage = StaticStorage::<u8, 4>::new();
    let (producer, consumer) = storage.try_split().unwrap();
    let p = &producer;
    let c = &consumer;
    p.try_push(1).ok();
    p.push_slice(&[1, 2]);
    c.try_pop();
    c.peek();
    c.peek_mut();
    c.pop_slice(&mut [0; 2]);
}
