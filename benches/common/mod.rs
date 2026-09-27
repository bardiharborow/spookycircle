//! Helpers shared by the benchmark binaries: thread pinning and the
//! environment block that every benchmark report must carry (compiler version, target triple, CPU, optimization flags, and
//! affinity policy).

#![allow(dead_code)]

use std::process::Command;
use std::{
    hint,
    sync::{
        Barrier,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

/// Keeps production behind the timer while excluding worker setup.
pub struct TransferStart {
    ready: Barrier,
    go: AtomicBool,
}

impl TransferStart {
    /// Creates a gate for one producer and the timing thread.
    pub fn new() -> Self {
        Self {
            ready: Barrier::new(2),
            go: AtomicBool::new(false),
        }
    }

    /// Reports that setup is complete, then waits for the timer to start.
    pub fn wait(&self) {
        self.ready.wait();
        while !self.go.load(Ordering::Acquire) {
            hint::spin_loop();
        }
    }

    /// Waits for worker setup, starts timing, and releases the producer.
    pub fn start(&self) -> Instant {
        self.ready.wait();
        let start = Instant::now();
        self.go.store(true, Ordering::Release);
        start
    }
}

#[path = "../../tests/common/affinity.rs"]
mod affinity;

/// Element types the benchmarks transfer: 1 byte, 8 bytes, one 64-byte
/// cache line, 512 bytes.
pub trait Element: Copy + Send + 'static {
    const NAME: &'static str;
    fn make(i: u64) -> Self;
}

impl Element for u8 {
    const NAME: &'static str = "u8";
    fn make(i: u64) -> Self {
        // Truncating on purpose, and branch-free inside the timed loop.
        i.to_le_bytes()[0]
    }
}

impl Element for u64 {
    const NAME: &'static str = "u64";
    fn make(i: u64) -> Self {
        i
    }
}

#[derive(Clone, Copy)]
#[repr(align(64))]
pub struct Line([u64; 8]);

impl Element for Line {
    const NAME: &'static str = "line64B";
    fn make(i: u64) -> Self {
        Line([i; 8])
    }
}

#[derive(Clone, Copy)]
pub struct Big([u64; 64]);

impl Element for Big {
    const NAME: &'static str = "big512B";
    fn make(i: u64) -> Self {
        Big([i; 64])
    }
}

/// Whether `SPOOKYCIRCLE_BENCH_PIN` asks for pinned threads.
pub fn pinning() -> bool {
    std::env::var_os("SPOOKYCIRCLE_BENCH_PIN").is_some()
}

/// Pins the calling thread to core `index` when pinning is enabled; see
/// `tests/common/affinity.rs` for what that means on each platform.
pub fn pin(index: usize) {
    if pinning() {
        affinity::pin_current_thread(index);
    }
}

/// Runs a command and returns its trimmed stdout, if it succeeded.
fn capture(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Best-effort CPU model string.
fn cpu_model() -> String {
    if cfg!(target_os = "macos")
        && let Some(model) = capture("sysctl", &["-n", "machdep.cpu.brand_string"])
    {
        return model;
    }
    if let Ok(info) = std::fs::read_to_string("/proc/cpuinfo") {
        // x86 reports "model name"; many aarch64 kernels only report the
        // implementer/part codes.
        for key in ["model name", "Model", "CPU part"] {
            if let Some(line) = info.lines().find(|l| l.starts_with(key))
                && let Some((_, value)) = line.split_once(':')
            {
                return format!("{key}: {}", value.trim());
            }
        }
    }
    "unknown".to_owned()
}

/// Prints the benchmark report header to stderr.
///
/// The compiler is queried at run time (`$RUSTC`, else `rustc` on `PATH`),
/// which matches the build compiler in the usual `cargo bench` setting; the
/// target is the one this binary was built for.
pub fn print_environment(harness: &str) {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let version = capture(&rustc, &["-vV"]).unwrap_or_else(|| "unavailable".to_owned());
    eprintln!("== spookycircle {harness} benchmark environment");
    eprintln!(
        "arch/os: {}-{} (family {}, pointer width {} bits; triple: see `host` below)",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY,
        usize::BITS
    );
    eprintln!("cpu: {}", cpu_model());
    eprintln!(
        "cores: {:?}",
        core_affinity::get_core_ids().map(|ids| ids.len())
    );
    eprintln!(
        "build: cargo `bench` profile, debug_assertions={}, RUSTFLAGS={:?}",
        cfg!(debug_assertions),
        std::env::var("RUSTFLAGS").unwrap_or_default()
    );
    eprintln!(
        "affinity: {}",
        if pinning() && cfg!(target_os = "macos") {
            "QoS user-interactive: performance cores, core and L2 cluster not controlled \
             (SPOOKYCIRCLE_BENCH_PIN set)"
        } else if pinning() {
            "pinned (SPOOKYCIRCLE_BENCH_PIN set)"
        } else {
            "unpinned (set SPOOKYCIRCLE_BENCH_PIN=1 to pin)"
        }
    );
    eprintln!("compiler:");
    for line in version.lines() {
        eprintln!("  {line}");
    }
}
