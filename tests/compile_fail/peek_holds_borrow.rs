// A reference returned by `peek` / `peek_mut` keeps the consumer mutably
// borrowed, so no operation that could remove the element can run.
use spookycircle::bounded;

fn main() {
    let (mut producer, mut consumer) = bounded::<String>(4).unwrap();
    producer.try_push("a".to_owned()).unwrap();

    let head = consumer.peek().unwrap();
    consumer.try_pop();
    println!("{head}");

    let head_mut = consumer.peek_mut().unwrap();
    consumer.try_pop();
    head_mut.push('x');
}
