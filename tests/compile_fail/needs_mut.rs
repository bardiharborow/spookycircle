// State-changing and peek operations require `&mut`.
use spookycircle::bounded;

fn main() {
    let (producer, consumer) = bounded::<u8>(4).unwrap();
    let p = &producer;
    let c = &consumer;
    p.try_push(1).ok();
    p.push_slice(&[1, 2]);
    c.try_pop();
    c.peek();
    c.peek_mut();
    c.pop_slice(&mut [0; 2]);
}
