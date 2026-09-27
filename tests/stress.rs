//! Concurrent stress tests: unique sequence numbers
//! transferred between two threads, verified for exact order, no gaps, no
//! duplicates, and a final count. Every scenario runs against each endpoint
//! family (heap, borrowed, static, and in-process shared region), with the
//! threads scoped so that borrowed endpoints can cross them.
//!
//! Set `SPOOKYCIRCLE_STRESS_LONG=1` for longer runs and
//! `SPOOKYCIRCLE_STRESS_PIN=1` to pin the two threads to distinct cores
//! where the platform supports it (on macOS, to the performance cores only;
//! see `common/affinity.rs`).

// Loom-instrumented atomics only work inside a Loom model; see tests/loom.rs.
#![cfg(not(loom))]

#[macro_use]
mod common;

use std::{hint, thread};

use common::{ConsumerOps, ProducerOps, Rec, rec, unrec};

fn long() -> bool {
    std::env::var_os("SPOOKYCIRCLE_STRESS_LONG").is_some()
}

fn pinning() -> bool {
    std::env::var_os("SPOOKYCIRCLE_STRESS_PIN").is_some()
}

fn count() -> u64 {
    if long() {
        20_000_000
    } else if cfg!(debug_assertions) {
        300_000
    } else {
        2_000_000
    }
}

fn pin(index: usize) {
    if pinning() {
        common::affinity::pin_current_thread(index);
    }
}

/// Deliberate pause every `every` operations, to shift the interleaving.
#[derive(Clone, Copy)]
struct Pause {
    every: u64,
    spins: u32,
}

impl Pause {
    const NONE: Pause = Pause { every: 0, spins: 0 };

    #[inline]
    fn maybe(self, i: u64) {
        if self.every != 0 && i.is_multiple_of(self.every) {
            for _ in 0..self.spins {
                hint::spin_loop();
            }
        }
    }
}

fn run_scalar<P, C>(mut p: P, mut c: C, total: u64, producer_pause: Pause, consumer_pause: Pause)
where
    P: ProducerOps<Rec> + Send,
    C: ConsumerOps<Rec> + Send,
{
    let capacity = p.capacity();
    let (full_results, empty_results) = thread::scope(|s| {
        let producer = s.spawn(move || {
            pin(0);
            let mut full_results = 0u64;
            for i in 0..total {
                producer_pause.maybe(i);
                let mut value = rec(usize::try_from(i).unwrap());
                loop {
                    match p.try_push(value) {
                        Ok(()) => break,
                        Err(full) => {
                            full_results += 1;
                            value = full.into_inner();
                            hint::spin_loop();
                        }
                    }
                }
            }
            full_results
        });

        let consumer = s.spawn(move || {
            pin(1);
            let mut expected = 0u64;
            let mut empty_results = 0u64;
            loop {
                match c.try_pop() {
                    Some(value) => {
                        assert_eq!(
                            unrec(value) as u64,
                            expected,
                            "order violated or value duplicated/skipped"
                        );
                        expected += 1;
                        consumer_pause.maybe(expected);
                    }
                    None if c.is_drained() => break,
                    None => {
                        empty_results += 1;
                        hint::spin_loop();
                    }
                }
            }
            assert_eq!(expected, total, "final count");
            assert!(c.is_drained());
            assert_eq!(c.try_pop(), None);
            empty_results
        });
        (producer.join().unwrap(), consumer.join().unwrap())
    });
    eprintln!(
        "scalar capacity={capacity} total={total}: {full_results} full results, {empty_results} empty results"
    );
}

fn run_bulk<P, C>(
    mut p: P,
    mut c: C,
    total: u64,
    batch: usize,
    producer_pause: Pause,
    consumer_pause: Pause,
) where
    P: ProducerOps<Rec> + Send,
    C: ConsumerOps<Rec> + Send,
{
    thread::scope(|s| {
        s.spawn(move || {
            pin(0);
            let source: Vec<Rec> = (0..usize::try_from(total).unwrap()).map(rec).collect();
            let mut sent = 0usize;
            let mut i = 0u64;
            while sent < source.len() {
                let end = (sent + batch).min(source.len());
                let n = p.push_slice(&source[sent..end]);
                sent += n;
                if n == 0 {
                    hint::spin_loop();
                }
                i += 1;
                producer_pause.maybe(i);
            }
        });

        s.spawn(move || {
            pin(1);
            let mut expected = 0u64;
            let mut buffer = vec![[0xFF; 8]; batch];
            let mut i = 0u64;
            loop {
                let n = c.pop_slice(&mut buffer);
                for &v in &buffer[..n] {
                    assert_eq!(unrec(v) as u64, expected);
                    expected += 1;
                }
                if n == 0 {
                    if c.is_drained() {
                        break;
                    }
                    hint::spin_loop();
                }
                i += 1;
                consumer_pause.maybe(i);
            }
            assert_eq!(expected, total);
        });
    });
}

/// Balanced: both sides run flat out.
#[test]
fn scalar_balanced() {
    for capacity in [1, 2, 3, 1000] {
        each_family!(8, capacity, run_scalar, count(), Pause::NONE, Pause::NONE);
    }
}

/// Producer-faster regime: the consumer pauses regularly, so the queue is
/// usually full and the producer sees many `Full` results.
#[test]
fn scalar_producer_faster() {
    for capacity in [1, 2, 3, 1000] {
        each_family!(
            8,
            capacity,
            run_scalar,
            count() / 4,
            Pause::NONE,
            Pause {
                every: 64,
                spins: 200,
            },
        );
    }
}

/// Consumer-faster regime: the producer pauses regularly, so the queue is
/// usually empty and the consumer sees many `None` results.
#[test]
fn scalar_consumer_faster() {
    for capacity in [1, 2, 3, 1000] {
        each_family!(
            8,
            capacity,
            run_scalar,
            count() / 4,
            Pause {
                every: 64,
                spins: 200,
            },
            Pause::NONE,
        );
    }
}

/// Both sides pause at different, coprime intervals.
#[test]
fn scalar_irregular_pauses() {
    for capacity in [1, 2, 3, 7, 1000] {
        each_family!(
            8,
            capacity,
            run_scalar,
            count() / 4,
            Pause {
                every: 97,
                spins: 50,
            },
            Pause {
                every: 89,
                spins: 70,
            },
        );
    }
}

#[test]
fn bulk_wrap_crossing_batches() {
    for (capacity, batch) in [(1, 1), (2, 3), (3, 2), (64, 17), (1000, 16), (1000, 1024)] {
        each_family!(
            8,
            capacity,
            run_bulk,
            count() / 2,
            batch,
            Pause::NONE,
            Pause::NONE
        );
        each_family!(
            8,
            capacity,
            run_bulk,
            count() / 8,
            batch,
            Pause {
                every: 13,
                spins: 100,
            },
            Pause::NONE,
        );
        each_family!(
            8,
            capacity,
            run_bulk,
            count() / 8,
            batch,
            Pause::NONE,
            Pause {
                every: 11,
                spins: 100,
            },
        );
    }
}

fn mixed<P, C>(mut p: P, mut c: C, total: u64)
where
    P: ProducerOps<Rec> + Send,
    C: ConsumerOps<Rec> + Send,
{
    let total = usize::try_from(total).unwrap();
    thread::scope(|s| {
        s.spawn(move || {
            pin(0);
            let mut next = 0usize;
            while next < total {
                if next.is_multiple_of(3) {
                    let end = (next + 5).min(total);
                    let batch: Vec<Rec> = (next..end).map(rec).collect();
                    next += p.push_slice(&batch);
                } else {
                    match p.try_push(rec(next)) {
                        Ok(()) => next += 1,
                        Err(_) => hint::spin_loop(),
                    }
                }
            }
        });
        s.spawn(move || {
            pin(1);
            let mut expected = 0usize;
            let mut buf = [[0u8; 8]; 4];
            loop {
                if expected.is_multiple_of(5)
                    && let Some(&v) = c.peek()
                {
                    assert_eq!(unrec(v), expected);
                }
                if expected.is_multiple_of(2) {
                    let n = c.pop_slice(&mut buf);
                    for &v in &buf[..n] {
                        assert_eq!(unrec(v), expected);
                        expected += 1;
                    }
                    if n == 0 && c.is_drained() {
                        break;
                    }
                } else {
                    match c.try_pop() {
                        Some(v) => {
                            assert_eq!(unrec(v), expected);
                            expected += 1;
                        }
                        None if c.is_drained() => break,
                        None => hint::spin_loop(),
                    }
                }
            }
            assert_eq!(expected, total);
        });
    });
}

/// Mixed scalar and bulk on each side, with `peek` used by the consumer.
#[test]
fn mixed_scalar_and_bulk_with_peek() {
    for capacity in [1usize, 3, 64] {
        each_family!(8, capacity, mixed, count() / 4);
    }
}

/// Repeated construction and racing destruction with values still queued,
/// in every typed family.
#[test]
fn racing_drop_with_queued_values() {
    let rounds = if long() { 20_000 } else { 2_000 };
    for round in 0..rounds {
        let capacity = 1 + round % 5;
        each_typed!(Counted, capacity, racing_drop, round);
    }
}

#[derive(Debug)]
struct Counted(std::sync::Arc<std::sync::atomic::AtomicU64>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn racing_drop<P, C>(mut p: P, mut c: C, round: usize)
where
    P: ProducerOps<Counted> + Send,
    C: ConsumerOps<Counted> + Send,
{
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    };

    let capacity = p.capacity();
    let drops = Arc::new(AtomicU64::new(0));
    let queued = round % (capacity + 1);
    let popped = round % 2;
    for _ in 0..queued {
        p.try_push(Counted(drops.clone())).unwrap();
    }
    for _ in 0..popped.min(queued) {
        drop(c.try_pop().unwrap());
    }
    let barrier = Barrier::new(2);
    thread::scope(|s| {
        s.spawn(|| {
            barrier.wait();
            drop(p);
        });
        barrier.wait();
        drop(c);
    });
    assert_eq!(
        usize::try_from(drops.load(Ordering::Relaxed)).unwrap(),
        queued
    );
    assert_eq!(Arc::strong_count(&drops), 1);
}
