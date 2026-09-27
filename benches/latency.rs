//! Latency percentiles.
//!
//! Criterion (`benches/spsc.rs`) reports throughput and mean/median times,
//! but not latency percentiles. This harness bounces one element between
//! two threads through a pair of queues (ping-pong). Each sample is the
//! round-trip time of one individually timed transfer in each direction,
//! with at most one element in flight, so queueing delay never inflates it.
//!
//! ```text
//! cargo bench --bench latency
//! SPOOKYCIRCLE_BENCH_PIN=1 cargo bench --bench latency
//! SPOOKYCIRCLE_LATENCY_SAMPLES=1000000 cargo bench --bench latency
//! ```
//!
//! Every sample includes one `Instant::now()` pair; the measured timer
//! overhead is printed so it can be subtracted.

mod common;

use std::{
    hint, thread,
    time::{Duration, Instant},
};

use common::{Element, Line};
use spookycircle::bounded;

const CAPACITIES: [usize; 3] = [1, 64, 1_024];
const WARMUP: usize = 10_000;

fn samples() -> usize {
    std::env::var("SPOOKYCIRCLE_LATENCY_SAMPLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000)
}

/// Returns `samples` round-trip times through two `spookycircle` queues.
fn round_trips<T: Element>(capacity: usize, samples: usize) -> Vec<Duration> {
    let (mut ping_p, mut ping_c) = bounded::<T>(capacity).unwrap();
    let (mut pong_p, mut pong_c) = bounded::<T>(capacity).unwrap();
    let echo = thread::spawn(move || {
        common::pin(0);
        loop {
            match ping_c.try_pop() {
                Some(mut value) => loop {
                    match pong_p.try_push(value) {
                        Ok(()) => break,
                        Err(full) => {
                            value = full.into_inner();
                            hint::spin_loop();
                        }
                    }
                },
                None if ping_c.is_drained() => break,
                None => hint::spin_loop(),
            }
        }
    });
    common::pin(1);
    let mut results = Vec::with_capacity(samples);
    for i in 0..WARMUP + samples {
        let mut value = T::make(i as u64);
        let start = Instant::now();
        loop {
            match ping_p.try_push(value) {
                Ok(()) => break,
                Err(full) => {
                    value = full.into_inner();
                    hint::spin_loop();
                }
            }
        }
        let echoed = loop {
            if let Some(v) = pong_c.try_pop() {
                break v;
            }
            hint::spin_loop();
        };
        let elapsed = start.elapsed();
        hint::black_box(echoed);
        if i >= WARMUP {
            results.push(elapsed);
        }
    }
    drop(ping_p);
    echo.join().unwrap();
    results
}

/// `rtrb` counterpart of [`round_trips`] for `u64`, as a baseline.
fn rtrb_round_trips(capacity: usize, samples: usize) -> Vec<Duration> {
    let (mut ping_p, mut ping_c) = rtrb::RingBuffer::<u64>::new(capacity);
    let (mut pong_p, mut pong_c) = rtrb::RingBuffer::<u64>::new(capacity);
    let echo = thread::spawn(move || {
        common::pin(0);
        loop {
            match ping_c.pop() {
                Ok(value) => {
                    while pong_p.push(value).is_err() {
                        hint::spin_loop();
                    }
                }
                Err(_) if ping_c.is_abandoned() => break,
                Err(_) => hint::spin_loop(),
            }
        }
    });
    common::pin(1);
    let mut results = Vec::with_capacity(samples);
    for i in 0..(WARMUP + samples) as u64 {
        let start = Instant::now();
        while ping_p.push(i).is_err() {
            hint::spin_loop();
        }
        let echoed = loop {
            if let Ok(v) = pong_c.pop() {
                break v;
            }
            hint::spin_loop();
        };
        let elapsed = start.elapsed();
        hint::black_box(echoed);
        if usize::try_from(i).unwrap() >= WARMUP {
            results.push(elapsed);
        }
    }
    drop(ping_p);
    echo.join().unwrap();
    results
}

/// Median cost of one `Instant::now()` pair, which every sample includes,
/// and the smallest nonzero step the clock was seen to take. Samples are
/// quantized to that step (about 42 ns on Apple silicon, for example).
fn timer_characteristics() -> (Duration, Option<Duration>) {
    let mut pairs: Vec<Duration> = (0..100_000)
        .map(|_| {
            let start = Instant::now();
            start.elapsed()
        })
        .collect();
    pairs.sort_unstable();
    let step = pairs.iter().copied().find(|d| !d.is_zero());
    (pairs[pairs.len() / 2], step)
}

#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a rank in `0..len` rounded from a non-negative float is the intent"
)]
fn percentile(sorted: &[Duration], p: f64) -> u128 {
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank].as_nanos()
}

fn report(name: &str, element_size: usize, capacity: usize, mut rtts: Vec<Duration>) {
    rtts.sort_unstable();
    let mean = rtts.iter().sum::<Duration>().as_nanos() / rtts.len() as u128;
    println!(
        "{name:<24} {element_size:>5} {capacity:>6} {:>8} {mean:>7} {:>7} {:>7} {:>7} {:>8} {:>9} {:>9}",
        rtts.len(),
        percentile(&rtts, 50.0),
        percentile(&rtts, 90.0),
        percentile(&rtts, 99.0),
        percentile(&rtts, 99.9),
        percentile(&rtts, 99.99),
        rtts.last().unwrap().as_nanos(),
    );
}

fn run<T: Element>(samples: usize) {
    for capacity in CAPACITIES {
        let rtts = round_trips::<T>(capacity, samples);
        report(
            &format!("spookycircle/{}", T::NAME),
            size_of::<T>(),
            capacity,
            rtts,
        );
    }
}

fn main() {
    common::print_environment("latency");
    let samples = samples();
    let (overhead, step) = timer_characteristics();
    eprintln!(
        "timer: overhead (median Instant pair) {overhead:?}, smallest observed step {step:?}"
    );
    eprintln!("round-trip latency in nanoseconds; {WARMUP} warm-up round trips discarded per row");
    println!(
        "{:<24} {:>5} {:>6} {:>8} {:>7} {:>7} {:>7} {:>7} {:>8} {:>9} {:>9}",
        "queue/element",
        "bytes",
        "cap",
        "samples",
        "mean",
        "p50",
        "p90",
        "p99",
        "p99.9",
        "p99.99",
        "max"
    );
    run::<u64>(samples);
    run::<Line>(samples);
    for capacity in CAPACITIES {
        report("rtrb/u64", 8, capacity, rtrb_round_trips(capacity, samples));
    }
}
