# spookycircle

A bounded, wait-free, single-producer/single-consumer (SPSC) FIFO ring buffer
for `#![no_std]`, with heap-owned, caller-borrowed, `static`, and
shared-memory storage.

* Exactly one producer and one consumer endpoint per queue (per session,
  or per shared-region generation). Both are `Send` when `T: Send`, never
  `Sync`, never `Clone`.
* Exact capacity: any capacity `>= 1`, no hidden sentinel slot, no
  power-of-two requirement.
* `try_push` / `try_pop` / `try_pop_into` / `peek` / `peek_mut` and every
  query are wait-free: at most one acquire load and one release store, no
  retry loop, no CAS, no allocation, no lock, no callback.
* `push_slice` / `pop_slice` for `T: Copy`, bounded by the slice length and
  published as a single batch.
* Dropping either endpoint leaves the other usable. `Consumer::is_drained`
  is a definitive end-of-stream test.
* Allocator-free operation: borrowed slots, `const`-initialized `static`
  storage, and shared regions never call an allocator, from construction to
  destruction. Only `bounded` (feature `alloc`) allocates.
* No dependencies. Edition 2024, MSRV 1.88, `MIT OR Apache-2.0`.

## Storage modes and features

| Storage mode | Construction and endpoints | Backing owner | Allocator |
| --- | --- | --- | --- |
| Heap-owned | `bounded::<T>(n)` → `Producer<T>`, `Consumer<T>` | Endpoints; the last one frees it | Construction only; feature `alloc` |
| Borrowed slots | `BorrowedStorage::new(&mut [Slot<T>])` + `try_split` → `BorrowedProducer`, `BorrowedConsumer` | Caller owns the slots | None |
| Inline/static | `StaticStorage::<T, N>::new()` (`const`) + `try_split` → the same borrowed endpoints | Caller owns the storage, e.g. a `static` | None |
| Shared region | `shared_memory::initialize` + `attach_producer` / `attach_consumer` → `SharedProducer`, `SharedConsumer` (records are `[u8; R]`) | External mapping owner | None; feature `shared-memory` |

| Feature | Default | Enables |
| --- | --- | --- |
| `alloc` | yes | `bounded`, `Producer`, `Consumer` |
| `shared-memory` | no | `shared_memory` module (does not enable `alloc` or `std`; needs 32- or 64-bit pointers) |

With `default-features = false` the crate needs only `core`. Borrowed and
static storage hand out one endpoint pair per session: dropping both
endpoints does not rearm the storage; `reset(&mut self)` does. The final
endpoint of a session drops the values still queued, so a `static` queue
needs no destructor. Forgetting an endpoint leaks its queued values (memory
safe); `reset` then abandons them without running destructors.

### Memory placement

The crate never maps, advises, or locks memory. For large or
latency-critical rings, place the memory yourself and hand it to
`BorrowedStorage::new` (or map the shared region that way):

- **Prefault** the slots (`MAP_POPULATE`, or write every page once) so the
  first pass around the ring does not take one page fault per page.
- **Huge pages** (Linux `madvise(MADV_HUGEPAGE)`, `MAP_HUGETLB`, or
  `memfd_create(MFD_HUGETLB)` for shared regions) remove most TLB misses
  on rings much larger than the TLB covers. They make no difference to
  small rings.
- **`mlock`** keeps the ring resident so it is never paged out.

The `posix_madvise` hints do not help: the ring is not read from a file,
so `SEQUENTIAL`, `RANDOM`, and `WILLNEED` change nothing, and `DONTNEED`
throws away queued data.

The crate documentation covers the wait-free scope, ownership rules, disconnection,
panic behaviour, and the safety argument.

## Example

```rust
use spookycircle::bounded;

let (mut producer, mut consumer) = bounded::<u32>(2).unwrap();

producer.try_push(10).unwrap();
producer.try_push(20).unwrap();

let full = producer.try_push(30).unwrap_err();
assert_eq!(full.into_inner(), 30);          // the value comes back untouched

assert_eq!(consumer.peek(), Some(&10));
assert_eq!(consumer.try_pop(), Some(10));
assert_eq!(consumer.try_pop(), Some(20));
assert_eq!(consumer.try_pop(), None);
```

Across threads, retry loops belong to the application; each individual
attempt is wait-free, the loop is not:

```rust
use std::thread;
use spookycircle::bounded;

let (mut producer, mut consumer) = bounded::<u64>(1_024).unwrap();

let produce = thread::spawn(move || {
    for mut value in 0..100_000 {
        loop {
            match producer.try_push(value) {
                Ok(()) => break,
                Err(full) => { value = full.into_inner(); thread::yield_now(); }
            }
        }
    }
});

let consume = thread::spawn(move || {
    let mut expected = 0;
    loop {
        match consumer.try_pop() {
            Some(value) => { assert_eq!(value, expected); expected += 1; }
            None if consumer.is_drained() => break,
            None => thread::yield_now(),
        }
    }
    assert_eq!(expected, 100_000);
});

produce.join().unwrap();
consume.join().unwrap();
```

Allocator-free, with a `static` (no `static mut`) or with borrowed slots:

```rust
use spookycircle::{BorrowedStorage, Slot, StaticStorage};

static QUEUE: StaticStorage<u32, 8> = StaticStorage::new();

let (mut producer, mut consumer) = QUEUE.try_split().unwrap();
producer.try_push(42).unwrap();
assert_eq!(consumer.try_pop(), Some(42));

let mut slots = [const { Slot::<String>::new() }; 3];
let mut storage = BorrowedStorage::new(&mut slots).unwrap();
let (mut producer, mut consumer) = storage.try_split().unwrap();
producer.try_push("hello".to_owned()).unwrap();
assert_eq!(consumer.try_pop().as_deref(), Some("hello"));
drop((producer, consumer));
storage.reset(); // exclusive access: start a new session
```

Shared memory between processes (or firmware images on coherent RAM) uses
the `unsafe` `shared_memory::initialize` / `attach_*` functions on a
caller-provided mapping; see the module documentation and
[`examples/shared_memory.rs`](examples/shared_memory.rs), which runs a
producer and a consumer in two processes over one `mmap(MAP_SHARED)` file.
There is no crash recovery: a dead peer's role stays live and the survivor
keeps seeing ordinary full/empty results.

## Wait-free claim and target support

`cfg(target_has_atomic = "ptr")` decides whether the crate compiles. The
wait-free claim additionally needs the target's pointer-width acquire loads
and release stores to be bounded, non-locking instructions, which is checked
per target by code-generation inspection (`cargo xtask inspect-codegen`) and
stress testing.

All storage modes run the same data path, and the codegen audit inspects
the heap-owned, static, and shared-region endpoints, plus wide (32-byte)
and owned (`String`) elements.

### Same-address-space queues (heap-owned, borrowed, static)

| Target | Class (this release) |
| --- | --- |
| `aarch64-apple-darwin` | **Wait-free certified.** Codegen inspected (`ldapr`/`stlr`; `try_push` and `try_pop` 24 instructions each, with no calls, fences, read-modify-write, or division on scalar paths, identically for heap, static, and shared endpoints), stress-tested, ThreadSanitizer, Miri (Stacked and Tree Borrows), and Loom clean. |
| `x86_64-apple-darwin` | Builds, not certified. Codegen inspected for the previous release (plain `mov`s, no `lock`/`mfence`/calls on scalar paths) but not stress-tested on hardware. |
| `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` | Builds, not certified. In the CI matrix (tests, codegen audit; sanitizers on `x86_64` only) but not yet inspected on hardware by the release author. |
| `x86_64-pc-windows-msvc` | Builds, not certified. Tested in CI; no codegen audit. |
| Any other target with pointer-width atomics (e.g. `thumbv7em-none-eabihf`) | Builds, not certified. Memory-safe; no wait-free hardware claim. An allocator-free program links for `thumbv7em-none-eabihf` with no global allocator (`ci/embedded`). |

### Shared-memory regions

Qualification is per compiler, target, OS, memory type, and set of
participants; same-process certification is not evidence for it.

| Configuration | Status |
| --- | --- |
| `aarch64-apple-darwin`, macOS, processes mapping one file with `mmap(MAP_SHARED)` at different addresses, same compiler build | **Qualified.** Two-process tests (numbered transfer, peer unmap after orderly close, peer killed while live, reinitialization) and the two-process example pass on hardware; codegen inspected. |
| `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, processes mapping one file with `mmap(MAP_SHARED)` | Builds and tested in CI, not certified. |
| Windows; `MAP_PRIVATE`/copy-on-write mappings; any firmware, multi-core, or heterogeneous-processor configuration; DMA, MMIO, or non-coherent memory | **Unavailable**: not qualified; do not rely on the `unsafe` contract there. |

Instrumented builds (Miri, Loom, sanitizers, profilers) and virtual machines
are never certified: they may replace atomics with slower or blocking
machinery.

## Verification

| What | Command |
| --- | --- |
| Deterministic, storage, shared-format, two-process, model, allocator, and trait tests (every endpoint family) | `cargo test --all-features` |
| The same for each feature set | `cargo test --no-default-features`, `cargo test`, `cargo test --no-default-features --features shared-memory` |
| Concurrent stress and two-process tests, optimized (longer: `SPOOKYCIRCLE_STRESS_LONG=1`) | `cargo test --release --all-features --test stress --test shared_process` |
| Narrow `u8` sequence-counter wrap model | `cargo test --lib` |
| Compile-fail cases (stable toolchain; per feature set) | `SPOOKYCIRCLE_COMPILE_FAIL=1 cargo test --all-features --test traits` |
| Loom model checking with negative controls, including the shared-region protocol | `RUSTFLAGS="--cfg loom" cargo test --release --all-features --lib --test loom` |
| Mutants: each essential acquire/release in `src/raw/` and `src/shared_memory/` weakened, each full/empty decision made to trust a stale cache, each claim broken, and the narrow-counter `head % capacity` cleanup, one at a time; every mutant must be detected | `cargo xtask loom-mutants` |
| Miri, Stacked Borrows | `cargo +nightly miri test --all-features --lib --test deterministic --test allocator --test storage --test shared_format` |
| Miri, Tree Borrows | the same with `MIRIFLAGS=-Zmiri-tree-borrows` |
| Miri over the property model (proptest needs `getcwd`, hence no isolation) | `MIRIFLAGS=-Zmiri-disable-isolation PROPTEST_CASES=24 cargo +nightly miri test --all-features --test model` |
| ThreadSanitizer (`<triple>` = host triple) | `RUSTFLAGS=-Zsanitizer=thread cargo +nightly test -Zbuild-std --target <triple> --release --all-features --test stress --test deterministic --test storage` |
| Codegen audit: no calls, fences, RMW, or division; every wrapper present; instruction counts within the `ci/codegen-baseline/` budget; on aarch64 exactly the expected pointer-width acquire loads and release stores | `cargo xtask inspect-codegen [--update-baseline] [target]` |
| `no_std` builds | `cargo build --lib --target thumbv7em-none-eabihf --no-default-features [--features shared-memory]` |
| Allocator-free link test and symbol audit | `cargo xtask check-symbols` |
| Two-process shared-memory example | `cargo run --release --features shared-memory --example shared_memory` |

Known tool limits: the Loom model runs the bulk and pipeline scenarios with a
preemption bound of 3 (the tiny scenarios are exhaustive); the Loom
shared-region models use one address space and prove the protocol, not
cross-process atomic interoperability (that is what the two-process tests on
real mappings are for); Miri does not run the multi-threaded stress suite
(covered by ThreadSanitizer) or the two-process tests (they need `mmap` and
`fork`/`exec`); references
returned by `peek` are not causality-tracked by Loom once they leave the
accessor closure (the corresponding `try_pop` read is); `push_slice` and
`pop_slice` may compile to bounded `memcpy` calls for their contiguous
segments. The `loom` crate is a `cfg(loom)`-gated dependency that is never
part of a normal build.

## Benchmarks

```text
cargo bench --all-features --bench spsc         # throughput, all groups
cargo bench --bench spsc -- baseline            # vs. rtrb
cargo bench --bench latency                     # round-trip latency percentiles
SPOOKYCIRCLE_BENCH_PIN=1 cargo bench --bench spsc -- two_thread
SPOOKYCIRCLE_BENCH_PIN=1 cargo bench --bench crossover  # speed-balance sweep with regimes
```

Groups: single-thread alternating push/pop, two-thread balanced,
producer-limited and consumer-limited, `rtrb` baseline, bulk sizes 1/4/16/64
including wrap-crossing batches; capacities 1, 2, 3, 64, 1024, 2^20; elements
`u8`, `u64`, a 64-byte line, and a 512-byte `Copy` value. The `backends/*`
groups run the same 8-byte-record workloads on heap, static, and (with
`--all-features`) shared-region endpoints. These groups include single-thread
transfers, two-thread transfers, and round-trip latency at capacity 1,024.
Criterion writes results to `target/criterion/`.

`benches/latency.rs` bounces one element between two threads through a pair
of queues and reports mean, p50, p90, p99, p99.9, p99.99, and max round-trip
time (with an `rtrb` baseline), plus the timer's overhead and granularity.

A balanced two-thread transfer settles into one of two regimes depending on
which side is faster: near empty, with the `tail` line crossing cores
several times per line of elements ("lockstep"), or with the producer ahead
and each line crossing about once ("stream"); a consumer-limited queue sits
near full. The regimes differ by up to about 3 times in throughput, and
small per-element costs (code layout, a branch, a prefetch hint) can move an
unbalanced benchmark between them, so treat single two-thread numbers with
care. `benches/crossover.rs` adds calibrated work to one side at a time and
reports, for each producer-minus-consumer cost, the throughput, the
occupancy the consumer observed, and the regime.

All three harnesses print a report header: compiler
version (`rustc -vV`), target, CPU model, build flags, and affinity policy;
capacity, element size, and sample count appear per result.

To flag regressions, record a baseline on a controlled host and
compare a candidate against it on the same host:

```text
cargo xtask bench-regress save main          # on the reference build
cargo xtask bench-regress check main         # on the candidate; exits 1 on regression
```

`check` flags a benchmark only when the whole 95% confidence interval of its
mean change exceeds 10% (`THRESHOLD=0.05` to tighten), so noise alone does not
trip it. A flagged regression calls for review, not a new baseline. It is not
run in CI, whose shared runners are not controlled hosts.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
