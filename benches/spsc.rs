//! Reproducible benchmarks.
//!
//! ```text
//! cargo bench --bench spsc                      # everything (long)
//! cargo bench --bench spsc -- two_thread/u64    # filter by name
//! SPOOKYCIRCLE_BENCH_PIN=1 cargo bench ...     # pin the two threads
//! ```
//!
//! Each run prints the compiler version, target, CPU model, build flags, and
//! affinity policy (see `common/mod.rs`); benchmark IDs carry the capacity
//! and element type, and Criterion writes sample counts and throughput to
//! `target/criterion/`. The `round_trip/*` groups time a one-element
//! ping-pong between two threads, so mean latency is covered by
//! `cargo xtask bench-regress`, which flags regressions against a saved
//! baseline; latency percentiles come from `benches/latency.rs`.
//!
//! The `backends/*` groups run the same 8-byte-record workloads on the
//! heap-owned, static, and (with `--features shared-memory`) shared-region
//! endpoints, whose data paths must share one budget.

mod common;

use std::{
    hint,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use common::{Big, Element, Line, TransferStart};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use spookycircle::bounded;
use std::hint::black_box;

const CAPACITIES: [usize; 6] = [1, 2, 3, 64, 1_024, 1 << 20];
const BULK_SIZES: [usize; 4] = [1, 4, 16, 64];
/// Matches `benches/latency.rs`, so the two reports line up.
const LATENCY_CAPACITIES: [usize; 3] = [1, 64, 1_024];

fn spin(work: u32) {
    for _ in 0..work {
        hint::spin_loop();
    }
}

fn single_thread_alternating<T: Element>(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("single_thread_alternating/{}", T::NAME));
    group.throughput(Throughput::Elements(1));
    for capacity in CAPACITIES {
        group.bench_with_input(
            BenchmarkId::from_parameter(capacity),
            &capacity,
            |b, &cap| {
                let (mut p, mut c) = bounded::<T>(cap).unwrap();
                let mut i = 0u64;
                b.iter(|| {
                    i += 1;
                    p.try_push(T::make(i)).ok().unwrap();
                    black_box(c.try_pop().unwrap());
                });
            },
        );
    }
    group.finish();
}

/// Transfers exactly `iters` elements from a producer thread to a consumer
/// thread and returns the wall time of the transfer alone.
///
/// The producer thread is spawned and pinned before the timer starts; both
/// sides rendezvous before the timer releases the producer. The measured
/// duration includes the start signal, but excludes thread creation and joining.
/// Criterion divides the returned duration by `iters`, so the closure must
/// run exactly that many transfers and nothing else.
fn two_thread_transfer<T: Element>(
    capacity: usize,
    iters: u64,
    producer_work: u32,
    consumer_work: u32,
) -> Duration {
    let (mut p, mut c) = bounded::<T>(capacity).unwrap();
    let barrier = Arc::new(TransferStart::new());
    let producer_barrier = Arc::clone(&barrier);
    let producer = thread::spawn(move || {
        common::pin(0);
        producer_barrier.wait();
        for i in 0..iters {
            let mut value = T::make(i);
            spin(producer_work);
            loop {
                match p.try_push(value) {
                    Ok(()) => break,
                    Err(full) => {
                        value = full.into_inner();
                        hint::spin_loop();
                    }
                }
            }
        }
    });
    common::pin(1);
    let start = barrier.start();
    let mut received = 0;
    while received < iters {
        match c.try_pop() {
            Some(v) => {
                black_box(v);
                received += 1;
                spin(consumer_work);
            }
            None => hint::spin_loop(),
        }
    }
    let elapsed = start.elapsed();
    producer.join().unwrap();
    elapsed
}

fn two_thread_balanced<T: Element>(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("two_thread_balanced/{}", T::NAME));
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);
    for capacity in CAPACITIES {
        group.bench_with_input(
            BenchmarkId::from_parameter(capacity),
            &capacity,
            |b, &cap| {
                b.iter_custom(|iters| two_thread_transfer::<T>(cap, iters, 0, 0));
            },
        );
    }
    group.finish();
}

fn two_thread_limited(c: &mut Criterion) {
    let mut group = c.benchmark_group("two_thread_limited/u64");
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);
    for capacity in [1usize, 3, 1_024] {
        group.bench_with_input(
            BenchmarkId::new("producer_limited", capacity),
            &capacity,
            |b, &cap| b.iter_custom(|iters| two_thread_transfer::<u64>(cap, iters, 40, 0)),
        );
        group.bench_with_input(
            BenchmarkId::new("consumer_limited", capacity),
            &capacity,
            |b, &cap| b.iter_custom(|iters| two_thread_transfer::<u64>(cap, iters, 0, 40)),
        );
    }
    group.finish();
}

/// Bounces one element between two threads through a pair of queues,
/// `iters` times, and returns the wall time of the round trips alone.
///
/// At most one element is in flight, so each round trip measures
/// cross-thread handoff latency rather than queueing delay. The echo thread
/// is spawned, pinned, and parked on a barrier before the timer starts.
fn round_trip_transfer<T: Element>(capacity: usize, iters: u64) -> Duration {
    let (mut ping_p, mut ping_c) = bounded::<T>(capacity).unwrap();
    let (mut pong_p, mut pong_c) = bounded::<T>(capacity).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let echo_barrier = Arc::clone(&barrier);
    let echo = thread::spawn(move || {
        common::pin(0);
        echo_barrier.wait();
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
    barrier.wait();
    let start = Instant::now();
    for i in 0..iters {
        let mut value = T::make(i);
        loop {
            match ping_p.try_push(value) {
                Ok(()) => break,
                Err(full) => {
                    value = full.into_inner();
                    hint::spin_loop();
                }
            }
        }
        loop {
            if let Some(v) = pong_c.try_pop() {
                black_box(v);
                break;
            }
            hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();
    drop(ping_p);
    echo.join().unwrap();
    elapsed
}

/// Mean round-trip latency; the reported time is one full round trip.
fn round_trip<T: Element>(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("round_trip/{}", T::NAME));
    group.sample_size(20);
    for capacity in LATENCY_CAPACITIES {
        group.bench_with_input(
            BenchmarkId::from_parameter(capacity),
            &capacity,
            |b, &cap| b.iter_custom(|iters| round_trip_transfer::<T>(cap, iters)),
        );
    }
    group.finish();
}

/// `rtrb` counterpart of [`two_thread_transfer`], timed the same way.
fn rtrb_two_thread_transfer(capacity: usize, iters: u64) -> Duration {
    let (mut p, mut c) = rtrb::RingBuffer::<u64>::new(capacity);
    let barrier = Arc::new(TransferStart::new());
    let producer_barrier = Arc::clone(&barrier);
    let producer = thread::spawn(move || {
        common::pin(0);
        producer_barrier.wait();
        for i in 0..iters {
            let mut value = i;
            loop {
                match p.push(value) {
                    Ok(()) => break,
                    Err(rtrb::PushError::Full(v)) => {
                        value = v;
                        hint::spin_loop();
                    }
                }
            }
        }
    });
    common::pin(1);
    let start = barrier.start();
    let mut received = 0;
    while received < iters {
        match c.pop() {
            Ok(v) => {
                black_box(v);
                received += 1;
            }
            Err(rtrb::PopError::Empty) => hint::spin_loop(),
        }
    }
    let elapsed = start.elapsed();
    producer.join().unwrap();
    elapsed
}

/// `rtrb` counterpart of [`round_trip_transfer`], timed the same way.
fn rtrb_round_trip_transfer(capacity: usize, iters: u64) -> Duration {
    let (mut ping_p, mut ping_c) = rtrb::RingBuffer::<u64>::new(capacity);
    let (mut pong_p, mut pong_c) = rtrb::RingBuffer::<u64>::new(capacity);
    let barrier = Arc::new(Barrier::new(2));
    let echo_barrier = Arc::clone(&barrier);
    let echo = thread::spawn(move || {
        common::pin(0);
        echo_barrier.wait();
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
    barrier.wait();
    let start = Instant::now();
    for i in 0..iters {
        while ping_p.push(i).is_err() {
            hint::spin_loop();
        }
        loop {
            if let Ok(v) = pong_c.pop() {
                black_box(v);
                break;
            }
            hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();
    drop(ping_p);
    echo.join().unwrap();
    elapsed
}

/// Baseline comparison against `rtrb`, a maintained SPSC ring buffer with
/// equivalent non-blocking push/pop semantics.
fn baseline_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline/u64");
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);
    for capacity in CAPACITIES {
        group.bench_with_input(
            BenchmarkId::new("spookycircle", capacity),
            &capacity,
            |b, &cap| {
                b.iter_custom(|iters| two_thread_transfer::<u64>(cap, iters, 0, 0));
            },
        );
        group.bench_with_input(BenchmarkId::new("rtrb", capacity), &capacity, |b, &cap| {
            b.iter_custom(|iters| rtrb_two_thread_transfer(cap, iters));
        });
    }
    group.finish();
    let mut group2 = c.benchmark_group("baseline_single_thread/u64");
    group2.throughput(Throughput::Elements(1));
    for capacity in [1usize, 64, 1_024] {
        group2.bench_with_input(
            BenchmarkId::new("spookycircle", capacity),
            &capacity,
            |b, &cap| {
                let (mut p, mut c) = bounded::<u64>(cap).unwrap();
                let mut i = 0u64;
                b.iter(|| {
                    i += 1;
                    p.try_push(i).ok().unwrap();
                    black_box(c.try_pop().unwrap());
                });
            },
        );
        group2.bench_with_input(BenchmarkId::new("rtrb", capacity), &capacity, |b, &cap| {
            let (mut p, mut c) = rtrb::RingBuffer::<u64>::new(cap);
            let mut i = 0u64;
            b.iter(|| {
                i += 1;
                p.push(i).ok().unwrap();
                black_box(c.pop().unwrap());
            });
        });
    }
    group2.finish();
    let mut group3 = c.benchmark_group("baseline_round_trip/u64");
    group3.sample_size(20);
    for capacity in LATENCY_CAPACITIES {
        group3.bench_with_input(
            BenchmarkId::new("spookycircle", capacity),
            &capacity,
            |b, &cap| b.iter_custom(|iters| round_trip_transfer::<u64>(cap, iters)),
        );
        group3.bench_with_input(BenchmarkId::new("rtrb", capacity), &capacity, |b, &cap| {
            b.iter_custom(|iters| rtrb_round_trip_transfer(cap, iters));
        });
    }
    group3.finish();
}

fn bulk(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_single_thread/u64");
    for batch in BULK_SIZES {
        group.throughput(Throughput::Elements(batch as u64));
        let source: Vec<u64> = (0..batch as u64).collect();
        let mut dest = vec![0u64; batch];
        group.bench_with_input(BenchmarkId::new("cap1024", batch), &batch, |b, _| {
            let (mut p, mut c) = bounded::<u64>(1_024).unwrap();
            b.iter(|| {
                assert_eq!(p.push_slice(&source), batch);
                assert_eq!(c.pop_slice(&mut dest), batch);
                black_box(&dest);
            });
        });
        // Capacity 100 with batch 64 crosses the physical end on most calls;
        // batch 1/4/16 cross periodically.
        group.bench_with_input(
            BenchmarkId::new("cap100_wrap_crossing", batch),
            &batch,
            |b, _| {
                let (mut p, mut c) = bounded::<u64>(100).unwrap();
                b.iter(|| {
                    assert_eq!(p.push_slice(&source), batch);
                    assert_eq!(c.pop_slice(&mut dest), batch);
                    black_box(&dest);
                });
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("bulk_two_thread/u64");
    group.sample_size(20);
    for batch in BULK_SIZES {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("cap1024", batch), &batch, |b, &batch| {
            b.iter_custom(|total| {
                let (mut p, mut c) = bounded::<u64>(1_024).unwrap();
                let barrier = Arc::new(TransferStart::new());
                let producer_barrier = Arc::clone(&barrier);
                let producer = thread::spawn(move || {
                    common::pin(0);
                    let source: Vec<u64> = (0..batch as u64).collect();
                    producer_barrier.wait();
                    let mut sent = 0u64;
                    while sent < total {
                        let want = usize::try_from(total - sent).unwrap().min(batch);
                        let n = p.push_slice(&source[..want]) as u64;
                        if n == 0 {
                            hint::spin_loop();
                        }
                        sent += n;
                    }
                });
                common::pin(1);
                let mut dest = vec![0u64; batch];
                let start = barrier.start();
                let mut received = 0u64;
                while received < total {
                    let n = c.pop_slice(&mut dest) as u64;
                    if n == 0 {
                        hint::spin_loop();
                    }
                    black_box(&dest);
                    received += n;
                }
                let elapsed = start.elapsed();
                producer.join().unwrap();
                elapsed
            });
        });
    }
    group.finish();
}

/// The producer operation the backend comparison needs.
trait Push {
    fn push(&mut self, value: [u8; 8]) -> Result<(), [u8; 8]>;
}

/// The consumer operation the backend comparison needs.
trait Pop {
    fn pop(&mut self) -> Option<[u8; 8]>;
}

macro_rules! impl_ends {
    ($p:ty, $c:ty) => {
        impl Push for $p {
            #[inline]
            fn push(&mut self, value: [u8; 8]) -> Result<(), [u8; 8]> {
                self.try_push(value).map_err(|full| full.into_inner())
            }
        }
        impl Pop for $c {
            #[inline]
            fn pop(&mut self) -> Option<[u8; 8]> {
                self.try_pop()
            }
        }
    };
}

impl_ends!(
    spookycircle::Producer<[u8; 8]>,
    spookycircle::Consumer<[u8; 8]>
);
impl_ends!(
    spookycircle::BorrowedProducer<'_, [u8; 8]>,
    spookycircle::BorrowedConsumer<'_, [u8; 8]>
);
#[cfg(feature = "shared-memory")]
impl_ends!(
    spookycircle::shared_memory::SharedProducer<'_, 8>,
    spookycircle::shared_memory::SharedConsumer<'_, 8>
);

const BACKEND_CAPACITY: usize = 1_024;

fn single_thread_generic<P: Push, C: Pop>(b: &mut criterion::Bencher<'_>, p: &mut P, c: &mut C) {
    let mut i = 0u64;
    b.iter(|| {
        i += 1;
        p.push(i.to_le_bytes()).unwrap();
        black_box(c.pop().unwrap());
    });
}

fn two_thread_generic<P: Push + Send, C: Pop>(p: &mut P, c: &mut C, iters: u64) -> Duration {
    let barrier = TransferStart::new();
    thread::scope(|s| {
        let barrier = &barrier;
        s.spawn(move || {
            common::pin(0);
            barrier.wait();
            for i in 0..iters {
                let mut value = i.to_le_bytes();
                while let Err(back) = p.push(value) {
                    value = back;
                    hint::spin_loop();
                }
            }
        });
        common::pin(1);
        let start = barrier.start();
        let mut received = 0;
        while received < iters {
            match c.pop() {
                Some(v) => {
                    black_box(v);
                    received += 1;
                }
                None => hint::spin_loop(),
            }
        }
        start.elapsed()
    })
}

/// Times one record in flight through two queues, excluding setup and joining.
fn round_trip_generic<P: Push + Send, C: Pop + Send>(
    ping_p: &mut P,
    ping_c: &mut C,
    pong_p: &mut P,
    pong_c: &mut C,
    iters: u64,
) -> Duration {
    let ready = Barrier::new(2);
    thread::scope(|s| {
        let ready = &ready;
        s.spawn(move || {
            common::pin(0);
            ready.wait();
            for _ in 0..iters {
                let mut value = loop {
                    if let Some(value) = ping_c.pop() {
                        break value;
                    }
                    hint::spin_loop();
                };
                while let Err(back) = pong_p.push(value) {
                    value = back;
                    hint::spin_loop();
                }
            }
        });
        common::pin(1);
        ready.wait();
        let start = Instant::now();
        for i in 0..iters {
            let mut value = i.to_le_bytes();
            while let Err(back) = ping_p.push(value) {
                value = back;
                hint::spin_loop();
            }
            loop {
                if let Some(value) = pong_c.pop() {
                    black_box(value);
                    break;
                }
                hint::spin_loop();
            }
        }
        start.elapsed()
    })
}

/// Runs `$run(&mut producer, &mut consumer)` on a fresh pair of the named
/// backend. Every backend gets its own monomorphic copy of the loop.
macro_rules! with_backend {
    ("heap", |$p:ident, $c:ident| $run:expr) => {{
        let (mut $p, mut $c) = bounded::<[u8; 8]>(BACKEND_CAPACITY).unwrap();
        $run
    }};
    ("static", |$p:ident, $c:ident| $run:expr) => {{
        let storage = Box::new(spookycircle::StaticStorage::<[u8; 8], BACKEND_CAPACITY>::new());
        let (mut $p, mut $c) = storage.try_split().unwrap();
        $run
    }};
    ("shared", |$p:ident, $c:ident| $run:expr) => {{
        use spookycircle::shared_memory as shm;
        use std::{alloc, ptr::NonNull};
        let layout = shm::layout::<8>(BACKEND_CAPACITY).unwrap();
        // SAFETY: nonzero-size layout.
        let base = NonNull::new(unsafe { alloc::alloc(layout) }).unwrap();
        let out = {
            // SAFETY: fresh exclusive memory standing in for a mapping.
            unsafe { shm::initialize::<8>(base, layout.size(), BACKEND_CAPACITY, 1) }.unwrap();
            let mut $p =
                // SAFETY: initialized above; endpoints drop before deallocation.
                unsafe { shm::attach_producer::<8>(base, layout.size(), BACKEND_CAPACITY, 1) }
                    .unwrap();
            let mut $c =
                // SAFETY: same lifetime, and this claims the distinct consumer role.
                unsafe { shm::attach_consumer::<8>(base, layout.size(), BACKEND_CAPACITY, 1) }
                    .unwrap();
            $run
        };
        // SAFETY: allocated above; the endpoints have dropped.
        unsafe { alloc::dealloc(base.as_ptr(), layout) };
        out
    }};
}

/// Backend comparison at capacity 1024 with 8-byte records.
fn backends(c: &mut Criterion) {
    let mut group = c.benchmark_group("backends/single_thread_alternating");
    group.throughput(Throughput::Elements(1));
    group.bench_function("heap", |b| {
        with_backend!("heap", |p, c| single_thread_generic(b, &mut p, &mut c));
    });
    group.bench_function("static", |b| {
        with_backend!("static", |p, c| single_thread_generic(b, &mut p, &mut c));
    });
    #[cfg(feature = "shared-memory")]
    group.bench_function("shared", |b| {
        with_backend!("shared", |p, c| single_thread_generic(b, &mut p, &mut c));
    });
    group.finish();

    let mut group = c.benchmark_group("backends/two_thread");
    group.throughput(Throughput::Elements(1));
    group.bench_function("heap", |b| {
        b.iter_custom(|n| with_backend!("heap", |p, c| two_thread_generic(&mut p, &mut c, n)));
    });
    group.bench_function("static", |b| {
        b.iter_custom(|n| with_backend!("static", |p, c| two_thread_generic(&mut p, &mut c, n)));
    });
    #[cfg(feature = "shared-memory")]
    group.bench_function("shared", |b| {
        b.iter_custom(|n| with_backend!("shared", |p, c| two_thread_generic(&mut p, &mut c, n)));
    });
    group.finish();

    let mut group = c.benchmark_group("backends/round_trip");
    macro_rules! round_trip_backend {
        ($backend:tt) => {
            group.bench_function($backend, |b| {
                b.iter_custom(|n| {
                    with_backend!($backend, |ping_p, ping_c| {
                        with_backend!($backend, |pong_p, pong_c| {
                            round_trip_generic(
                                &mut ping_p,
                                &mut ping_c,
                                &mut pong_p,
                                &mut pong_c,
                                n,
                            )
                        })
                    })
                });
            });
        };
    }
    round_trip_backend!("heap");
    round_trip_backend!("static");
    #[cfg(feature = "shared-memory")]
    round_trip_backend!("shared");
    group.finish();
}

fn all(c: &mut Criterion) {
    common::print_environment("throughput");
    single_thread_alternating::<u8>(c);
    single_thread_alternating::<u64>(c);
    single_thread_alternating::<Line>(c);
    single_thread_alternating::<Big>(c);
    two_thread_balanced::<u8>(c);
    two_thread_balanced::<u64>(c);
    two_thread_balanced::<Line>(c);
    two_thread_balanced::<Big>(c);
    two_thread_limited(c);
    round_trip::<u64>(c);
    round_trip::<Line>(c);
    baseline_comparison(c);
    bulk(c);
    backends(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(2)).warm_up_time(Duration::from_millis(500));
    targets = all
}
criterion_main!(benches);
