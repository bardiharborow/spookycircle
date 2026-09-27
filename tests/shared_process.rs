//! Two-process shared-memory tests.
//!
//! The parent and a child process (this test binary, re-executed with a
//! role in `SPOOKYCIRCLE_CHILD`) each map one temporary file with
//! `mmap(MAP_SHARED)` at *different* virtual addresses and attach one role
//! each. The startup handoff is the child's command line: the parent
//! initializes the region before spawning the child.
#![cfg(all(unix, feature = "shared-memory", not(miri), not(loom)))]

use std::{
    env,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

use spookycircle::shared_memory::{self as shm, SharedError};

const R: usize = 8;
const CHILD_ENV: &str = "SPOOKYCIRCLE_CHILD";

/// One `MAP_SHARED` mapping of the region file.
struct Mapping {
    base: NonNull<u8>,
    len: usize,
}

impl Mapping {
    /// Maps `path`, asking (as a hint) for `hint` so that parent and child
    /// use different addresses.
    fn open(path: &Path, len: usize, hint: usize) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        // SAFETY: a fresh shared mapping of an open file of at least `len`
        // bytes; the file descriptor may be closed after mapping.
        let map = unsafe {
            libc::mmap(
                std::ptr::without_provenance_mut::<libc::c_void>(hint),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        assert_ne!(map, libc::MAP_FAILED, "mmap failed");
        Self {
            base: NonNull::new(map.cast()).unwrap(),
            len,
        }
    }

    fn addr(&self) -> usize {
        self.base.as_ptr().addr()
    }

    fn producer(&self, capacity: usize, generation: u64) -> shm::SharedProducer<'_, R> {
        // SAFETY: the region was initialized before this process learned the
        // generation (startup handoff), the mapping outlives the borrow, and
        // this process claims the role once.
        unsafe { shm::attach_producer::<R>(self.base, self.len, capacity, generation) }.unwrap()
    }

    fn consumer(&self, capacity: usize, generation: u64) -> shm::SharedConsumer<'_, R> {
        // SAFETY: as for `producer`.
        unsafe { shm::attach_consumer::<R>(self.base, self.len, capacity, generation) }.unwrap()
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: mapped in `open`; every endpoint borrowing it has dropped.
        let unmapped = unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
        assert_eq!(unmapped, 0);
    }
}

/// A temporary region file, removed on drop.
struct RegionFile {
    path: PathBuf,
    len: usize,
    capacity: usize,
    generation: u64,
}

impl RegionFile {
    fn create(capacity: usize) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("spookycircle-{}-{n}.region", std::process::id()));
        let len = shm::layout::<R>(capacity).unwrap().size();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.set_len(len as u64).unwrap();
        let generation = (u64::from(std::process::id()) << 20) | n as u64;
        Self {
            path,
            len,
            capacity,
            generation,
        }
    }

    fn map(&self) -> Mapping {
        Mapping::open(&self.path, self.len, 0)
    }

    /// Initializes the region through `mapping`: exclusive, since no child
    /// has been told about it yet.
    fn initialize(&self, mapping: &Mapping, generation: u64) {
        // SAFETY: coherent shared file mapping, no other participant.
        unsafe { shm::initialize::<R>(mapping.base, mapping.len, self.capacity, generation) }
            .unwrap();
    }

    /// Spawns the child in `mode`; it maps the file near `hint`.
    fn spawn(&self, mode: &str, hint: usize) -> ChildGuard {
        let spec = format!(
            "{mode} {} {} {} {} {hint}",
            self.path.display(),
            self.len,
            self.capacity,
            self.generation
        );
        ChildGuard::new(
            Command::new(env::current_exe().unwrap())
                .args(["child_entry", "--exact", "--nocapture", "--test-threads=1"])
                .env(CHILD_ENV, spec)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        )
    }
}

impl Drop for RegionFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// A spawned child that is killed and reaped when dropped, so a failing
/// assertion in the parent never leaves a child running.
struct ChildGuard {
    child: Child,
    out: std::io::Lines<BufReader<std::process::ChildStdout>>,
}

impl ChildGuard {
    fn new(mut child: Child) -> Self {
        let out = BufReader::new(child.stdout.take().unwrap()).lines();
        Self { child, out }
    }

    /// Reads the child's stdout until its `KEY=value` report for `key`.
    fn read_value(&mut self, key: &str) -> String {
        read_value(&mut self.out, key)
    }

    /// Drains the child's remaining output (so it never writes to a closed
    /// pipe), then waits for a normal exit and asserts success.
    fn finish(mut self) {
        for line in self.out.by_ref() {
            line.unwrap();
        }
        let status = self.child.wait().unwrap();
        assert!(status.success(), "child failed: {status}");
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Harmless if the child already exited.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Reads the child's stdout until a `KEY=value` report for `key`.
///
/// The report may share a line with libtest's `test child_entry ... `
/// prefix, so it is matched anywhere in the line; values are the rest of the
/// line up to whitespace.
fn read_value(lines: &mut impl Iterator<Item = std::io::Result<String>>, key: &str) -> String {
    let marker = format!("{key}=");
    for line in lines {
        let line = line.unwrap();
        if let Some(start) = line.find(&marker) {
            let rest = &line[start + marker.len()..];
            return rest.split_whitespace().next().unwrap_or("").to_owned();
        }
    }
    panic!("child exited without reporting {key}");
}

fn record(i: u64) -> [u8; R] {
    i.to_le_bytes()
}

/// An address hint well away from `mapping`, so the child's mapping lands
/// elsewhere.
fn hint_away_from(mapping: &Mapping) -> usize {
    let distance = if usize::BITS > 32 { 34 } else { 28 };
    mapping.addr().wrapping_add(1usize << distance) & !0xFFFF
}

/// Retries with a yield until `f` succeeds or 10 s pass. Test policy only.
fn spin<T>(mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(value) = f() {
            return value;
        }
        assert!(Instant::now() < deadline, "peer made no progress");
        thread::yield_now();
    }
}

/// Records moved by the transfer test: fewer in unoptimized builds, where
/// the yield-and-retry loops dominate the run time.
const TRANSFER: u64 = if cfg!(debug_assertions) {
    20_000
} else {
    200_000
};

/// The child side. Does nothing unless spawned by one of the tests below.
#[test]
fn child_entry() {
    let Ok(spec) = env::var(CHILD_ENV) else {
        return;
    };
    let fields: Vec<&str> = spec.split(' ').collect();
    let [mode, path, len, capacity, generation, hint] = fields[..] else {
        panic!("bad child spec {spec:?}");
    };
    let len: usize = len.parse().unwrap();
    let capacity: usize = capacity.parse().unwrap();
    let generation: u64 = generation.parse().unwrap();
    let hint: usize = hint.parse().unwrap();
    let mapping = Mapping::open(Path::new(path), len, hint);
    println!("\nBASE={}", mapping.addr());
    match mode {
        // Consume numbered records until the producer is definitively done.
        "consume" => {
            let mut c = mapping.consumer(capacity, generation);
            let mut expected = 0u64;
            let mut batch = [[0u8; R]; 16];
            let mut idle_since = Instant::now();
            loop {
                let n = c.pop_slice(&mut batch);
                for got in &batch[..n] {
                    assert_eq!(u64::from_le_bytes(*got), expected, "gap or duplicate");
                    expected += 1;
                }
                if n == 0 {
                    if c.is_drained() {
                        break;
                    }
                    // Fail rather than hang if the producer stops for good.
                    assert!(
                        idle_since.elapsed() < Duration::from_secs(10),
                        "producer made no progress"
                    );
                    thread::yield_now();
                } else {
                    idle_since = Instant::now();
                }
            }
            println!("\nCOUNT={expected}");
        }
        // Publish `capacity` records, close, unmap, and exit.
        "produce_and_leave" => {
            let mut p = mapping.producer(capacity, generation);
            for i in 0..capacity as u64 {
                p.try_push(record(i)).unwrap();
            }
            drop(p);
            drop(mapping);
            println!("DONE=1");
        }
        // Consume a few records, close, unmap, and exit.
        "consume_some_and_leave" => {
            let mut c = mapping.consumer(capacity, generation);
            for i in 0..2 {
                let got = spin(|| c.try_pop());
                assert_eq!(got, record(i));
            }
            drop(c);
            drop(mapping);
            println!("DONE=1");
        }
        // Attach the consumer and then hang, holding the role live, until
        // killed.
        "attach_and_hang" => {
            let c = mapping.consumer(capacity, generation);
            println!("READY=1");
            loop {
                thread::sleep(Duration::from_secs(1));
                let _ = &c;
            }
        }
        _ => panic!("unknown mode {mode}"),
    }
}

/// Numbered records cross from this process to the child with no gaps or
/// duplicates, through mappings at different addresses.
#[test]
fn numbered_transfer_between_processes() {
    let file = RegionFile::create(64);
    let mapping = file.map();
    file.initialize(&mapping, file.generation);
    let mut child = file.spawn("consume", hint_away_from(&mapping));
    let child_base: usize = child.read_value("BASE").parse().unwrap();
    assert_ne!(child_base, mapping.addr(), "mappings must differ");

    let mut p = mapping.producer(file.capacity, file.generation);
    let mut next = 0u64;
    let mut batch = [[0u8; R]; 16];
    while next < TRANSFER {
        let n = (TRANSFER - next).min(16) as usize;
        for (k, slot) in batch[..n].iter_mut().enumerate() {
            *slot = record(next + k as u64);
        }
        let done = spin(|| match p.push_slice(&batch[..n]) {
            0 => None,
            done => Some(done),
        });
        next += done as u64;
    }
    drop(p);
    let count: u64 = child.read_value("COUNT").parse().unwrap();
    assert_eq!(count, TRANSFER);
    child.finish();
}

/// A peer's orderly drop and unmap leaves the records and control state in
/// place: the surviving peer drains (or fills) normally.
#[test]
fn records_survive_peer_unmapping_after_orderly_drop() {
    // Producer leaves; this process attaches late and drains.
    let file = RegionFile::create(5);
    let mapping = file.map();
    file.initialize(&mapping, file.generation);
    let mut child = file.spawn("produce_and_leave", hint_away_from(&mapping));
    child.read_value("DONE");
    child.finish();
    let mut c = mapping.consumer(file.capacity, file.generation);
    assert!(!c.is_producer_alive());
    for i in 0..5 {
        assert_eq!(c.try_pop(), Some(record(i)));
    }
    assert!(c.is_drained());
    drop(c);

    // Consumer leaves; this process keeps filling to capacity.
    let file = RegionFile::create(4);
    let mapping = file.map();
    file.initialize(&mapping, file.generation);
    let mut p = mapping.producer(file.capacity, file.generation);
    let mut child = file.spawn("consume_some_and_leave", hint_away_from(&mapping));
    p.try_push(record(0)).unwrap();
    p.try_push(record(1)).unwrap();
    child.read_value("DONE");
    child.finish();
    assert!(!p.is_consumer_alive());
    assert!(p.is_empty());
    for i in 0..4 {
        p.try_push(record(10 + i)).unwrap();
    }
    assert_eq!(p.try_push(record(99)).unwrap_err().into_inner(), record(99));
}

/// A peer killed while its role is live: no crash is detected, the
/// survivor keeps getting ordinary full results without waiting, and after
/// external quiescence the region is reinitialized with a fresh generation.
#[test]
fn killed_peer_then_reinitialization() {
    let file = RegionFile::create(3);
    let mapping = file.map();
    file.initialize(&mapping, file.generation);
    let mut child = file.spawn("attach_and_hang", hint_away_from(&mapping));
    child.read_value("READY");
    let mut p = mapping.producer(file.capacity, file.generation);
    for i in 0..3 {
        p.try_push(record(i)).unwrap();
    }
    // Dropping the guard sends SIGKILL and reaps the child.
    drop(child);
    // No crash recovery: the dead consumer's role stays live forever and
    // pushes simply report full.
    assert!(p.is_consumer_alive());
    for _ in 0..3 {
        assert!(p.try_push(record(9)).is_err());
    }
    drop(p);

    // The child is gone and this process's endpoint dropped: quiescent.
    let fresh = file.generation + 1;
    file.initialize(&mapping, fresh);
    // SAFETY: valid mapping; the stale generation is rejected.
    let stale = unsafe {
        shm::attach_consumer::<R>(mapping.base, mapping.len, file.capacity, file.generation)
    };
    assert_eq!(stale.err(), Some(SharedError::GenerationMismatch));
    let mut p = mapping.producer(file.capacity, fresh);
    let mut c = mapping.consumer(file.capacity, fresh);
    assert_eq!(c.try_pop(), None, "old records are gone");
    p.try_push(record(42)).unwrap();
    assert_eq!(c.try_pop(), Some(record(42)));
}
