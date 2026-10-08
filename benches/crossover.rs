//! Producer/consumer speed-balance sweep with regime reporting.
//!
//! A balanced two-thread transfer settles into one of two regimes, and which
//! one depends on which side is faster, not on the queue: if the consumer is
//! faster it keeps catching the producer, the queue sits near empty, and the
//! line at `tail` crosses cores several times per line's worth of elements
//! ("lockstep"); if the producer is faster it runs ahead, the consumer reads
//! lines the producer has finished with, and each line crosses about once
//! ("stream"). A consumer-limited queue sits near full instead ("full").
//! Small per-element costs (code layout, a branch, a prefetch hint) can flip
//! an unbalanced benchmark between regimes, so a single throughput number
//! for it is not meaningful. This harness sweeps the balance explicitly and
//! reports, per point, the throughput and the occupancy the consumer saw.
//!
//! Extra work is a dependent multiply-add chain, one "unit" per step
//! (calibrated and printed in nanoseconds), added before each push (positive
//! `p-c`) or after each pop (negative `p-c`). `spin_loop` would be far too
//! coarse: one `PAUSE` is about 15 ns on Zen 3.
//!
//! ```text
//! SPOOKYCIRCLE_BENCH_PIN=1 cargo bench --bench crossover
//! SPOOKYCIRCLE_BENCH_PIN=1 cargo bench --features prefetch --bench crossover
//! SPOOKYCIRCLE_CROSSOVER_MAX=12 SPOOKYCIRCLE_CROSSOVER_ITERS=10000000 \
//!     SPOOKYCIRCLE_CROSSOVER_REPS=7 cargo bench --bench crossover
//! ```
//!
//! Columns: `ns/elt` is the median over repetitions; `lag` percentiles are
//! the bytes between `head` and `tail` the consumer observed every
//! `LAG_EVERY` pops, pooled over repetitions; `empty` is the fraction of
//! pops that found the queue empty, `full` of pushes that found it full;
//! `near0` and `near1` are the fractions of lag samples under two cache
//! lines and over 95% of capacity. Each repetition uses a fresh queue whose
//! pages are faulted in before timing, so the first lap does not pay page
//! faults.

#![expect(
    clippy::cast_precision_loss,
    reason = "statistics for a report: element counts and nanoseconds are far below 2^52"
)]

mod common;

use std::{
    hint::{self, black_box},
    mem::size_of,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use common::{Element, Line, TransferStart};
use spookycircle::bounded;

const CAPACITIES: [usize; 2] = [1_024, 1 << 20];
/// Pops between occupancy samples.
const LAG_EVERY: u64 = 64;
/// "Near empty": less than this many bytes between `head` and `tail`.
const NEAR_EMPTY_BYTES: usize = 128;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// `units` steps of a dependent multiply-add chain: each step needs the
/// previous result, so the chain cannot overlap. The empty `asm!` hides the
/// value from the optimizer each step (without it LLVM rewrites the loop to
/// well under a cycle per step) and emits no instruction.
#[inline(always)]
fn work(units: u32, seed: u64) -> u64 {
    let mut x = seed;
    for _ in 0..units {
        x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        // SAFETY: the template is a comment naming `x`; it executes nothing.
        unsafe {
            std::arch::asm!("/* {x} */", x = inout(reg) x, options(pure, nomem, nostack, preserves_flags));
        }
    }
    x
}

/// Nanoseconds per `work` unit.
fn calibrate() -> f64 {
    let units = 50_000_000;
    let start = Instant::now();
    black_box(work(black_box(units), black_box(1)));
    start.elapsed().as_nanos() as f64 / f64::from(units)
}

struct Run {
    elapsed: Duration,
    empty_pops: u64,
    full_pushes: u64,
    /// Elements between `head` and `tail`, as seen by the consumer.
    lag: Vec<u32>,
}

/// Transfers `iters` elements with `producer_work` units before each push
/// and `consumer_work` units after each pop.
fn transfer<T: Element>(
    capacity: usize,
    iters: u64,
    producer_work: u32,
    consumer_work: u32,
) -> Run {
    let (mut p, mut c) = bounded::<T>(capacity).unwrap();
    // Fault the slots in: one full lap before timing.
    for i in 0..capacity {
        assert!(p.try_push(T::make(i as u64)).is_ok());
    }
    while c.try_pop().is_some() {}

    let barrier = Arc::new(TransferStart::new());
    let producer_barrier = Arc::clone(&barrier);
    let producer = thread::spawn(move || {
        common::pin(0);
        producer_barrier.wait();
        let mut full = 0;
        for i in 0..iters {
            let mut value = T::make(black_box(work(producer_work, i)));
            while let Err(rejected) = p.try_push(value) {
                value = rejected.into_inner();
                full += 1;
                hint::spin_loop();
            }
        }
        full
    });
    common::pin(1);
    let mut lag = Vec::with_capacity(usize::try_from(iters / LAG_EVERY + 1).unwrap_or(0));
    let mut empty = 0;
    let start = barrier.start();
    let mut received = 0;
    while received < iters {
        if let Some(v) = c.try_pop() {
            black_box(v);
            received += 1;
            black_box(work(consumer_work, received));
            if received % LAG_EVERY == 0 {
                lag.push(u32::try_from(c.len()).unwrap_or(u32::MAX));
            }
        } else {
            empty += 1;
            hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();
    let full_pushes = producer.join().unwrap();
    Run {
        elapsed,
        empty_pops: empty,
        full_pushes,
        lag,
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a rank in `0..len` rounded from a non-negative float is the intent"
)]
fn percentile(sorted: &[u32], q: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

fn sweep<T: Element>(iters: u64, reps: u64, max: i64, ns_per_unit: f64) {
    let size = size_of::<T>();
    for capacity in CAPACITIES {
        for delta in -max..=max {
            let units = u32::try_from(delta.unsigned_abs()).expect("sweep bound fits u32");
            let (pw, cw) = if delta >= 0 { (units, 0) } else { (0, units) };
            let mut ns = Vec::new();
            let mut lag = Vec::new();
            let (mut empty, mut full) = (0, 0);
            for _ in 0..reps {
                let run = transfer::<T>(capacity, iters, pw, cw);
                ns.push(run.elapsed.as_nanos() as f64 / iters as f64);
                empty += run.empty_pops;
                full += run.full_pushes;
                lag.extend(run.lag);
            }
            ns.sort_by(f64::total_cmp);
            lag.sort_unstable();
            let samples = lag.len().max(1) as f64;
            let near0 =
                lag.partition_point(|&l| (l as usize) * size < NEAR_EMPTY_BYTES) as f64 / samples;
            let near1 = (lag.len() - lag.partition_point(|&l| (l as usize) * 20 <= capacity * 19))
                as f64
                / samples;
            let regime = if near0 > 0.5 {
                "lockstep"
            } else if near1 > 0.5 {
                "full"
            } else {
                "stream"
            };
            let total = (iters * reps) as f64;
            let bytes = |q| u64::from(percentile(&lag, q)) * size as u64;
            println!(
                "{:<8} {:>8} {:>4} {:>6.1} {:>7.2} {:>6.2} {:>6.2} {:>10} {:>10} {:>10} {:>6.3} {:>6.3} {:>6.3} {:>6.3}  {}",
                T::NAME,
                capacity,
                delta,
                delta as f64 * ns_per_unit,
                ns[ns.len() / 2],
                ns[0],
                ns[ns.len() - 1],
                bytes(0.1),
                bytes(0.5),
                bytes(0.9),
                empty as f64 / total,
                full as f64 / total,
                near0,
                near1,
                regime,
            );
        }
    }
}

fn main() {
    common::print_environment("crossover");
    let iters = env("SPOOKYCIRCLE_CROSSOVER_ITERS", 4_000_000);
    let reps = env("SPOOKYCIRCLE_CROSSOVER_REPS", 5).max(1);
    let max = i64::try_from(env("SPOOKYCIRCLE_CROSSOVER_MAX", 6)).unwrap_or(6);
    let ns_per_unit = calibrate();
    eprintln!(
        "work unit: {ns_per_unit:.3} ns; prefetch feature: {}; {iters} elements x {reps} repetitions per point",
        cfg!(feature = "prefetch")
    );
    println!(
        "{:<8} {:>8} {:>4} {:>6} {:>7} {:>6} {:>6} {:>10} {:>10} {:>10} {:>6} {:>6} {:>6} {:>6}  regime",
        "element",
        "cap",
        "p-c",
        "ns",
        "ns/elt",
        "min",
        "max",
        "lag p10 B",
        "lag p50 B",
        "lag p90 B",
        "empty",
        "full",
        "near0",
        "near1"
    );
    sweep::<u8>(iters, reps, max, ns_per_unit);
    sweep::<u64>(iters, reps, max, ns_per_unit);
    sweep::<Line>(iters, reps, max, ns_per_unit);
}
