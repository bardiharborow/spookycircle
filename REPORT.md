# x86 test report: spookycircle

Date: 2026-10-08. Scope: everything the aarch64 (Apple M1) development
machine could not check — the x86 build and test matrix, the x86
`prefetch` path, and x86 prefetch tuning — plus what that work turned up
about the two-thread benchmarks themselves.

Status: all changes are uncommitted in the working tree (see
[Changes in the working tree](#changes-in-the-working-tree)).

## Contents

1. [Environment](#environment)
2. [Summary](#summary)
3. [Baseline verification on x86](#baseline-verification-on-x86)
4. [The x86 prefetch path](#the-x86-prefetch-path)
5. [Thread pinning bug (fixed)](#thread-pinning-bug-fixed)
6. [The two-thread benchmarks are bistable](#the-two-thread-benchmarks-are-bistable)
7. [`benches/crossover.rs`](#benchescrossoverrs)
8. [Prefetch tuning on x86, chronologically](#prefetch-tuning-on-x86-chronologically)
9. [Changes in the working tree](#changes-in-the-working-tree)
10. [Open questions and next experiments](#open-questions-and-next-experiments)
11. [Reproducing](#reproducing)

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 5 5600X (Zen 3), 6 cores / 12 threads, one CCX, 32 MB L3; has `prfchw` |
| OS | Windows 11 Pro 10.0.26200 |
| Stable | rustc 1.93.0 (254b59607 2026-01-19), LLVM 21.1.8 |
| Nightly | rustc 1.101.0-nightly (1d81eb4ad 2026-10-07), installed during this session |
| Not available | WSL distro, Linux cross C toolchain, AMD uProf |

Note: `ci/codegen-baseline/*.txt` were generated with rustc 1.98.1; the local
stable (1.93.0) is older than that.

Windows numbers logical processors so that SMT siblings are adjacent:
`GetLogicalProcessorInformation` reports core masks `0b11`, `0b1100`, …, i.e.
logical CPUs 0 and 1 share physical core 0, 2 and 3 share core 1, and so on.

## Summary

- **Correctness on x86 is fine.** Every test, the release stress suite, and
  the host codegen audit pass on stable; tests and stress also pass with
  `prefetchw` live (nightly, `+prfchw`, `--features prefetch`).
- **The x86 prefetch path works as designed.** `build.rs` detects
  `core::hint::prefetch_write` on nightly, and `prefetchw` is emitted only
  with nightly plus `+prfchw`, in exactly the five `try_push` paths.
- **Bug fixed:** benchmark/stress pinning put both threads on SMT siblings of
  one physical core on Windows. Pinning now uses one logical CPU per core.
- **The two-thread benchmarks are bistable.** A balanced transfer settles
  into a slow "lockstep" regime (queue near empty, the `tail` line bouncing
  between cores) or a fast "stream"/"full" regime, about 2–4× apart.
  Which one it lands in depends on tiny per-element cost differences —
  code layout alone moved one benchmark between 5.5 ns and 1.6 ns per
  element. Single Criterion numbers for unbalanced two-thread transfers are
  therefore not trustworthy, and the earlier "prefetch makes `u8` 2× slower"
  result was mostly a regime flip, not the hint's own cost.
- **New benchmark `benches/crossover.rs`** sweeps the producer/consumer cost
  balance with a calibrated work knob and reports the regime per point.
- **Prefetch on x86:** its main benefit is moving the lockstep→stream
  crossover so `u64` rings escape lockstep (≈0.61–0.65 of the unhinted time
  at capacity 1024, ≈0.75–0.85 at 2^20). It is neutral for `u8` and costs
  7–21% for 64-byte slots. The shipped x86 setting is now: hint every push
  (no interval test), 128 bytes ahead, no free-space gate.
- **The "writer laps the reader" hypothesis** is mechanically real in a
  full queue, but gating hints on known-free space was measured *slower*,
  not faster, on Zen 3, so it is not in the code.
- **Pre-existing:** the committed prefetch code (`build.rs`, `src/lib.rs`,
  `src/prefetch.rs`) is not rustfmt-clean, so CI's `cargo fmt --all --
  --check` would fail. Left untouched.

## Baseline verification on x86

| Check | Result |
| --- | --- |
| `cargo test --all-features` (stable) | Pass |
| `cargo test --release --all-features --test stress` | Pass (7/7) |
| `cargo clippy --all-features --all-targets -- -D warnings` (stable) | Clean |
| `cargo +nightly clippy --all-features --all-targets` | Library clean; tests hit a new nightly lint ("used `assert!` to check that a value is (not) empty") in `deterministic`, `shared_format`, `storage` — unrelated to prefetch |
| `cargo xtask inspect-codegen` (host `x86_64-pc-windows-msvc`, stable 1.93) | Pass. Plain `mov`s for acquire/release; no calls, fences, RMW, or division on scalar paths; `memcpy` only on bulk paths. `codegen_owned_try_pop` 28 and `codegen_owned_try_push` 40 instructions vs baseline 27/39 (within tolerance) |
| Linux-target clippy of the test crate | Not possible: criterion's `alloca` dependency needs `x86_64-linux-gnu-gcc`. The pinning module was instead checked standalone for `x86_64-unknown-linux-gnu` and `x86_64-pc-windows-msvc` |

The README's "Wait-free certified" claims were not changed: this session
did not run ThreadSanitizer, Miri, or Loom on x86, and the codegen audit ran
on an older compiler than the baselines.

## The x86 prefetch path

`build.rs` probe output (`spookycircle` build-script stdout):

| Toolchain | cfgs emitted |
| --- | --- |
| stable 1.93 | none |
| nightly 1.101 | `spookycircle_hint_prefetch`, `spookycircle_hint_prefetch_unstable` |

`prefetchw` instructions in the codegen crate with `--features prefetch`:

| Toolchain | `+prfchw` | `prefetchw` count |
| --- | --- | --- |
| stable | no | 0 |
| stable | yes | 0 |
| nightly | no | 0 (`ENABLED` false: `prefetcht0` would be wrong) |
| nightly | yes | 5 — `try_push`, `wide_try_push`, `owned_try_push`, `static_try_push`, `shared_try_push` |

Codegen of the committed version for `u64` slots (step = 128 / 8 = 16
slots): wrap the target with `cmp`/`cmov`/`sub`, then `testb $56` (offset
within the 64-byte block < 8, the interval test), then `prefetchw`, issued
before the release store. Branch-bounded; no calls or fences.

## Thread pinning bug (fixed)

`tests/common/affinity.rs` (used by `tests/stress.rs` and, via `#[path]`,
`benches/common`) pinned index *i* to the *i*-th logical CPU from
`core_affinity`. On Windows that put producer (0) and consumer (1) on SMT
siblings of one physical core, sharing L1/L2: the cross-core transfer the
benchmarks are meant to measure never happened, and a cross-core prefetch
study is meaningless there.

Fix: the allowed CPU list is reduced to the first logical CPU of each
physical core:

- Windows: `GetLogicalProcessorInformation` (declared directly; no new
  dependency), `RelationProcessorCore` masks, processor group 0 only (as
  `core_affinity` itself).
- Linux: `/sys/devices/system/cpu/cpuN/topology/core_cpus_list`, falling
  back to `thread_siblings_list` (Linux < 5.6).
- Elsewhere, or topology unreadable: every logical CPU counts as a core (old
  behaviour). If fewer than two cores remain, all logical CPUs are used.
- macOS unchanged (QoS class only).

`pin_current_thread` now returns the CPU it chose; `tests/affinity.rs`
(Linux only) uses that instead of assuming `ids[1]`, and also asserts the
parent and child land on different CPUs.

On this machine indices 0–5 now map to logical CPUs 0, 2, 4, 6, 8, 10.

Side effect: pinned stress (`SPOOKYCIRCLE_STRESS_PIN=1`) takes 40.4 s
instead of 30.9 s (unpinned: about 2.7 s). That is the expected price of a
real cross-core transfer; pinned results before and after the fix are not
comparable.

## The two-thread benchmarks are bistable

### Evidence

1. **Occupancy probe** (standalone binary; producer samples `len()` every
   1024 pushes; CPUs 0 and 2): in the slow runs the queue is under 5% full
   in 98–100% of samples, pushes almost never see `Full` (≤ 0.3%), and
   6–28% of pops see empty.

2. **The same loop, different builds, 4× apart** (`u8`, capacity 2^20,
   20 M elements, CPUs 0 and 2):

   | Build | ns/element |
   | --- | --- |
   | Criterion `two_thread_balanced/u8/1048576` | 1.35–1.44 (reproduced twice) |
   | Verbatim copy of `two_thread_transfer` + `TransferStart` in a separate crate, work counts constant 0 | 5.1–6.1 (first run 25.9: page faults) |
   | The same copy with work counts read from argv (still 0) | 1.56–1.70 |

   Repeating the transfer in one process, and matching the start protocol,
   did not change the slow result; only the build did.

3. **Criterion's own samples** for `u8`/2^20 without prefetch fall from
   ~4.5 ns at 3 M iterations to ~1.35 ns at 60 M+: runs start slow and
   flip to fast. With the old prefetch build they stayed at 2.5–5.7 ns.

4. **SMT siblings vs separate cores** (occupancy probe, slow build):

   | CPUs | `u8`/2^20 | `u64`/1024 |
   | --- | --- | --- |
   | 0,1 (siblings) | 4.36 ns | 3.61 ns |
   | 0,2 | 8.93 ns | 5.27 ns |
   | 0,6 | 7.31 ns | 5.97 ns |

### Model

- **Lockstep (slow):** if the consumer is faster per element, it keeps
  catching the producer; the queue sits near empty and the line holding
  `tail` crosses cores several times per line's worth of elements.
- **Stream (fast):** if the producer is faster, it runs ahead; the consumer
  reads lines the producer has finished with and each line crosses about
  once.
- **Full:** a consumer-limited queue sits near capacity.

Lockstep is self-reinforcing: the line ping-pong slows both sides about
equally, so the consumer must be 2–3 ns per element slower before the queue
leaves it (see the crossover results below). Any small per-push cost —
layout, a branch, a prefetch hint — can therefore move an unbalanced
benchmark across the crossover.

### Other benchmark-hygiene findings

- `spin_loop()` (`PAUSE`) costs about 15 ns on Zen 3: `producer_work = 1`
  turns 1.6 ns into 15.6 ns. Far too coarse to explore the crossover.
- Each Criterion sample allocates a fresh ring, so for 2^20 capacities the
  first lap pays page faults; small-iteration samples (~4.5 ns) drag the
  mean up.
- At 10 samples, run-to-run noise on two-thread benchmarks is about ±15%:
  capacities 1–3 (where the hint never fires) and `rtrb` (untouched by the
  feature) moved that much between runs.
- Build-to-build variance is ±10% even in the stable "full" regime: the same
  configuration measured in different sessions differs by that much.

## `benches/crossover.rs`

A harness-less benchmark (registered in `Cargo.toml`, described in the
README) that sweeps producer-minus-consumer cost and reports regimes:

- **Work knob:** a dependent multiply-add chain, `units` steps, with an empty
  `asm!` (`/* {x} */`, emits nothing) hiding the value each step. Without it
  LLVM rewrote the loop to ~0.115 ns per unit; with it the calibration is
  0.86–0.90 ns per unit (≈4 cycles), printed at startup.
- **Sweep:** `p-c` from `-MAX` to `+MAX` units; positive adds work before
  each push, negative after each pop.
- **Per point:** median ns/element over repetitions; consumer-observed lag
  (`head`→`tail` bytes, sampled every 64 pops) p10/p50/p90; fraction of pops
  that found the queue empty and of pushes that found it full; `near0`
  (lag < 128 B) and `near1` (lag > 95% of capacity); regime = lockstep if
  `near0 > 0.5`, full if `near1 > 0.5`, else stream.
- Each repetition prefaults the ring (one full lap) before timing.
- Elements `u8`, `u64`, 64-byte `Line`; capacities 1024 and 2^20.
- Env: `SPOOKYCIRCLE_CROSSOVER_ITERS` (default 4 M),
  `SPOOKYCIRCLE_CROSSOVER_REPS` (5), `SPOOKYCIRCLE_CROSSOVER_MAX` (6);
  honours `SPOOKYCIRCLE_BENCH_PIN`.

The instrumentation lives only in this binary, so the existing Criterion
groups' codegen is untouched.

## Prefetch tuning on x86, chronologically

All runs: nightly 1.101, `RUSTFLAGS="-C target-feature=+prfchw"`, so the only
difference between "off" and "on" is `--features prefetch`. From step 2 on,
pinned to separate physical cores.

### 1. First Criterion comparison (old code: interval test, 128 B)

10 samples, two runs. Only results that repeated:

| Benchmark | Run 1 | Run 2 |
| --- | --- | --- |
| `two_thread_balanced/line64B/1024` | −16.5% | −24.6% |
| `two_thread_balanced/u8/1024` | +19.0% | +18.1% |
| `two_thread_balanced/u8/1048576` | +101% | +76% |

(`single_thread_alternating/u64/1048576` +67% did not repeat: +4.4%.)
In hindsight the `u8` "regressions" are regime flips (see above).

### 2. Criterion matrix: interval vs every push × distance

Temporary `option_env!` knobs for distance and interval/every-push.
Change vs prefetch off, run 1 / run 2 (negative = faster). `i` = interval
test, `u` = every push; number = distance in bytes.

| Ring | off (ns) | i64 | i128 | i256 | i512 | i1024 | u64 | u128 | u256 | u512 | u1024 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2t u8 1K | 4.20/4.53 | +25/+25 | +46/+16 | +9/+3 | −12/−18 | +43/+52 | +6/−0 | −3/−23 | −12/−31 | −8/−31 | +49/+5 |
| 2t u8 1M | 1.74/1.66 | +132/+95 | +143/+100 | +132/+127 | +151/+161 | +249/+254 | −5/−9 | +109/+146 | +129/+143 | +24/+169 | +21/+188 |
| 2t u64 1K | 5.02/5.20 | −6/−7 | −18/−10 | −16/−20 | −15/−26 | −9/−13 | −3/−7 | −22/−36 | −27/−48 | −25/−45 | −34/−47 |
| 2t u64 1M | 5.03/4.63 | −7/+2 | −15/−15 | −8/−13 | −1/−3 | −13/+14 | −8/+6 | −2/+4 | −4/+2 | −24/+11 | −20/+8 |
| 2t line64B 1K | 12.12/12.06 | −5/+1 | −29/−24 | −8/−4 | −4/−5 | −14/−14 | −2/−1 | −29/−36 | −5/−4 | +1/−4 | −20/−18 |
| 2t line64B 1M | 15.30/14.66 | −3/+3 | −3/+0 | −5/−3 | −17/−8 | −16/−12 | −4/+2 | −4/+0 | −5/−2 | −29/−11 | −14/−10 |
| 2t big512B 1K | 37.75/39.98 | ±5 | | | | | | | | | |
| 2t big512B 1M | 114.7/107.4 | ±10 | | | | | | | | | |
| 1t u64 1K | 2.23/2.27 | +1/+0 | +12/+9 | +9/+6 | +9/+8 | −0/−1 | ≈0 | ≈0 | ≈0 | ≈0 | ≈0 |
| 1t u64 1M | 2.38/2.38 | ±5 | | | | | | | | | |

Takeaways that survived later testing: every push ≥ interval almost
everywhere; the interval test itself costs 6–12% single-threaded. The `u8`
rows were later shown to be regime effects.

### 3. Occupancy probe

Answered "is the writer catching up to the reader?" for the balanced
benchmark: **no** — the queue sits near empty (see bistability evidence).
The hinted line at `tail + 128 B` is free space well away from `head`.

### 4. Crossover sweep: off vs old code (2 runs each, ±8 units)

ns/element; L = lockstep, S = stream, F = full; consumer slower on the left.

| Ring | Prefetch off | Prefetch on (old code) |
| --- | --- | --- |
| `u8` 1K/1M | Lockstep 7–8 ns unless the consumer is ≥ 2–3 ns slower (then S/F at 2.5–4 ns) | Still lockstep, about 10% faster (6.7–7.5 ns) |
| `u64` 1K | Lockstep 5–6 ns from p-c ≈ −1 to +8 | **Escapes lockstep**: F/S at 2.0–2.6 ns from −2 to +3 (≈2.7×), both runs |
| `u64` 1M | Stream up to ≈ +1, then lockstep 5–6 ns | Stream extends to +3…+5 |
| line64B 1K | Always full, 9.5–10.5 ns (consumer copies 64 B) | 10–20% slower, 11–12.5 ns |
| line64B 1M | Stream, 13.5–15 ns | ≈10% slower, 15.5–17 ns |

In the full regime (line64B/1K: 13–15% of pushes see `Full`, lag ≈ 64 KB)
the hint's target is ≈ `step` past `head` — the lines the consumer reads
next. That is the "writer catches the reader" mechanism, and it motivated
step 5. line64B/1M slowing down is *not* that (stream, small lag); an
untested guess is that 128 B is far too short to cover DRAM latency on a
64 MB ring (> 32 MB L3) and the hint interferes with the hardware
prefetcher.

### 5. Free-space gate

Gate: skip the hint unless `step <= full_at - next_tail` (slots known free
from the producer's cached `head`). Cost: `sub`, `cmp`, `jb`; since
`free < capacity` it also subsumes the `step >= capacity` test.

Geometric-mean time vs off across `p-c` −6…+6 (3 reps per point), two
passes; < 1 is faster. `old` = committed code (no gate, interval test);
`g-` = gated; `n-u128` = no gate, every push, 128 B (measured in a later
session against its own fresh off baseline).

| Ring | old | g-i128 | g-u64 | g-u128 | g-u256 | g-u512 | n-u128 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| u8/1K | 0.98/1.33* | 0.96/1.35* | 1.03/1.41* | 0.96/1.36* | 0.96/1.37* | 0.96/1.39* | 0.94/1.01 |
| u8/2^20 | 0.93/0.88 | 0.95/0.95 | 0.92/1.01 | 0.92/0.93 | 0.95/0.89 | 0.94/0.92 | 0.99/0.96 |
| u64/1K | 0.71/0.68 | 0.65/0.68 | 0.67/0.67 | 0.65/0.66 | 0.64/0.68 | 0.66/0.65 | **0.61/0.61** |
| u64/2^20 | 0.90/0.86 | 0.78/0.83 | 0.80/0.83 | 0.77/0.79 | 0.84/0.89 | 1.10/1.14 | **0.75/0.75** |
| line64B/1K | 1.15/0.95 | 1.35/1.09 | 1.27/1.04 | 1.31/1.09 | 1.28/1.11 | 1.27/1.09 | 1.13/1.21 |
| line64B/2^20 | 0.91/1.10 | 1.04/1.10 | 1.09/1.05 | 1.22/1.08 | 1.18/1.04 | 1.07/0.95 | 1.07/1.00 |

\* pass 2's off baseline for u8/1K happened to land in stream at several
middle points, inflating every ratio in that column; treat u8 as neutral.

Findings:

- For line64B (interval test always true), `old` vs `g-i128` differ only by
  the gate, and the gate was ≈10% **slower** in the full regime in both
  passes — the opposite of the prediction. Mechanism unknown; in a
  consumer-limited queue the producer's polling cadence on `head` may matter
  more than the hint. Needs hardware counters.
- 512 B loses the benefit at 2^20 (falls into lockstep from p-c ≈ −2).
- Best overall: no gate, every push, 128 B.

### 6. Shipped configuration, re-measured

Final code (x86: every push, 128 B, no gate) vs off, two passes:

| Ring | final |
| --- | --- |
| u8/1K | 0.95/0.98 |
| u8/2^20 | 0.94/0.98 |
| u64/1K | 0.65/0.63 |
| u64/2^20 | 0.84/0.85 |
| line64B/1K | 1.19/1.07 |
| line64B/2^20 | 1.09/1.08 |

Codegen of the final `try_push` (u64): `cmpq $17` (ring longer than the
step), wrap with `addq`/`cmp`/`cmov`/`sub`, `prefetchw` — no address test.
Tests (86) and release stress pass with hints live.

## Changes in the working tree

| File | Change |
| --- | --- |
| `tests/common/affinity.rs` | One logical CPU per physical core (Windows/Linux topology); returns the chosen CPU |
| `tests/affinity.rs` | Uses the returned CPU; asserts parent/child differ |
| `benches/crossover.rs` | New regime-reporting speed-balance sweep |
| `Cargo.toml` | `[[bench]] crossover` |
| `README.md` | Crossover command; paragraph on the two regimes |
| `src/prefetch.rs` | `EVERY_PUSH` (x86/x86_64): skip the interval test; docs record the x86 distance and gate results |
| `REPORT.md` | This report |

`src/raw/mod.rs` is unchanged (the gate was removed). All temporary tuning
knobs are removed.

## Open questions and next experiments

**Prefetch (x86)**

1. Skip the hint for slots ≥ 64 B on x86: they lost 7–21% in every
   configuration and session, including the streaming 2^20 ring. One-line
   `const` condition plus a crossover run.
2. Why the free-space gate is slower in the full regime, and why line64B/2^20
   slows down: needs counters (AMD uProf on Windows; Linux `perf` with Zen 3
   `ls_any_fills_from_sys.*` to split fills by source, plus prefetch and
   branch-mispredict counts).
3. Distance scaled per slot size, re-measured per regime (u64: 64–256 B
   alike; 512 B too far at 2^20).
4. Hint only while streaming (producer's cached view says it is several
   lines ahead).
5. Hints in `push_slice` (currently only `try_push`).
6. `prefetchw` vs `prefetcht0` vs none, per regime.
7. Intel-only: consumer-side `CLDEMOTE` after reading a line, so the
   producer's RFO hits LLC rather than the consumer's L1/L2.

**Modelling**

8. Measure cross-core line transfers per element directly (the model's
   quantity) and the single-line ping-pong latency on its own; check the
   model cost/elt ≈ op cost + transfers-per-line × transfer latency /
   elements-per-line against each regime.
9. Map the crossover point (p-c where lockstep ends) per element size and
   per prefetch variant; report regime plateaus rather than means.
10. Code-layout sensitivity: rebuild with different alignments (e.g.
    `-C llvm-args=-align-all-nofallthru-blocks=6`); report both regimes if
    the result flips.

**Benchmark hygiene**

11. Criterion two-thread groups: prefault or reuse the ring, or use a fixed
    large iteration count per sample; report regime alongside time.
12. More processes × fewer samples: process/build variance dominates.

**Coverage**

13. Topology: cross-CCX/CCD (5900X/5950X, EPYC), Intel mesh/ring, SMT
    siblings deliberately.
14. Realistic workloads: bursty producer, variable consumer work, tail
    latency (`benches/latency.rs` p99/p99.9) with prefetch on.
15. Linux x86: tests, ThreadSanitizer, Miri, Loom, and the codegen audit
    with a compiler matching the baselines (1.98.1 or newer), which this
    session could not run.

**Housekeeping**

16. Run `cargo fmt` on the "Add prefetch" commit's files (`build.rs`,
    `src/lib.rs`, `src/prefetch.rs`); CI's fmt check would fail on them.
17. Note that `cargo fmt -- <file>` formats the whole crate, not just that
    file.

## Reproducing

```text
# Correctness with hints live (nightly)
RUSTFLAGS="-C target-feature=+prfchw" cargo +nightly test --all-features
RUSTFLAGS="-C target-feature=+prfchw" cargo +nightly test --release --all-features --test stress

# Codegen with hints
RUSTFLAGS="-C target-feature=+prfchw" cargo +nightly rustc --release \
    --manifest-path ci/codegen/Cargo.toml --features spookycircle/prefetch \
    -- --emit asm=out.s -C debuginfo=0 -C codegen-units=1

# Crossover sweep, off vs on
export RUSTFLAGS="-C target-feature=+prfchw" SPOOKYCIRCLE_BENCH_PIN=1
cargo +nightly bench --bench crossover
cargo +nightly bench --features prefetch --bench crossover
```

Raw outputs from this session (local, under `target/`, not committed):
`crossover-{off,prefetch}-{1,2}.txt` (step 4), `gate-*-p{1..6}.txt`
(steps 5–6), `matrix-*.log` and `bench-x86/criterion/` (step 2).
