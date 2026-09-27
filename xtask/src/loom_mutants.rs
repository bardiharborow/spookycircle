//! Negative controls against the real code.
//!
//! ```text
//! cargo xtask loom-mutants
//! ```
//!
//! `tests/loom.rs` contains litmus-test negative controls, but those exercise
//! standalone code. This task checks the crate's *own* protocol: it weakens
//! each essential acquire or release in `src/raw/` and `src/shared_memory/`,
//! makes each full/empty decision trust a stale cache, and breaks the session
//! and role claims, one mutation at a time, in a scratch copy of the crate.
//! It requires the Loom suite (`--lib` shared-region models plus
//! `tests/loom.rs`) to fail for every mutant. It also runs the
//! narrow-counter negative control: final cleanup starting at
//! `wrapped_head % capacity` instead of the saved physical index must fail
//! the `u8` wraparound tests.
//!
//! A surviving mutant means either the ordering is not actually needed (and
//! the proof in the source should say so) or the suite has a blind spot.
//!
//! Mutations are located by file, enclosing function, and code text, not by
//! line number, so ordinary edits do not break the task; a mutation whose
//! anchor text disappears is reported as an error rather than skipped.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};

use regex::Regex;

use crate::{Result, cargo, root};

const RAW: &str = "src/raw/mod.rs";
const TYPED: &str = "src/raw/typed.rs";
const SHM: &str = "src/shared_memory/protocol.rs";

#[derive(Clone, Copy)]
enum Suite {
    /// The shared-region protocol models (`--lib`) and tests/loom.rs.
    Loom,
    /// The narrow `u8` wraparound model.
    Narrow,
}

impl Suite {
    const ALL: [Suite; 2] = [Suite::Loom, Suite::Narrow];

    fn name(self) -> &'static str {
        match self {
            Suite::Loom => "loom",
            Suite::Narrow => "narrow",
        }
    }

    fn args(self) -> &'static [&'static str] {
        match self {
            Suite::Loom => &[
                "test",
                "--quiet",
                "--release",
                "--all-features",
                "--lib",
                "--test",
                "loom",
            ],
            Suite::Narrow => &["test", "--quiet", "--lib", "narrow_tests"],
        }
    }

    fn env(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Suite::Loom => &[("RUSTFLAGS", "--cfg loom")],
            Suite::Narrow => &[],
        }
    }
}

struct Mutant {
    label: &'static str,
    file: &'static str,
    /// The enclosing function.
    function: &'static str,
    /// Exact code text, which must occur once in `function`.
    old: &'static str,
    new: &'static str,
    suite: Suite,
}

const fn mutant(
    label: &'static str,
    file: &'static str,
    function: &'static str,
    old: &'static str,
    new: &'static str,
    suite: Suite,
) -> Mutant {
    Mutant {
        label,
        file,
        function,
        old,
        new,
        suite,
    }
}

const MUTANTS: &[Mutant] = &[
    // `refresh_full_at` / `refresh_tail` are the data path's only acquires
    // of the opposite position; the scalar and bulk operations share them.
    mutant(
        "refresh_full_at: head refresh Acquire -> Relaxed",
        RAW,
        "refresh_full_at",
        "S::load(self.queue.shared_head(), Ordering::Acquire)",
        "S::load(self.queue.shared_head(), Ordering::Relaxed)",
        Suite::Loom,
    ),
    mutant(
        "try_push: tail publish Release -> Relaxed",
        RAW,
        "try_push",
        "S::store(self.queue.shared_tail(), next_tail, Ordering::Release)",
        "S::store(self.queue.shared_tail(), next_tail, Ordering::Relaxed)",
        Suite::Loom,
    ),
    mutant(
        "push_slice: tail publish Release -> Relaxed",
        RAW,
        "push_slice",
        "S::store(self.queue.shared_tail(), next_tail, Ordering::Release)",
        "S::store(self.queue.shared_tail(), next_tail, Ordering::Relaxed)",
        Suite::Loom,
    ),
    mutant(
        "refresh_tail: tail refresh Acquire -> Relaxed",
        RAW,
        "refresh_tail",
        "S::load(self.queue.shared_tail(), Ordering::Acquire)",
        "S::load(self.queue.shared_tail(), Ordering::Relaxed)",
        Suite::Loom,
    ),
    // `release_one` is the release store of both `try_pop` and `try_pop_into`.
    mutant(
        "release_one: head release Release -> Relaxed",
        RAW,
        "release_one",
        "S::store(self.queue.shared_head(), next_head, Ordering::Release)",
        "S::store(self.queue.shared_head(), next_head, Ordering::Relaxed)",
        Suite::Loom,
    ),
    // `try_pop_into`: the move into the destination must precede the release; after
    // it, the producer may already be reusing the slot.
    mutant(
        "try_pop_into: copy after the release store",
        RAW,
        "try_pop_into",
        "unsafe { core::ptr::copy_nonoverlapping(p, out, 1) }\n        });\n        self.release_one(head, index);",
        "});\n        self.release_one(head, index);\n        cell.value.with(|p| unsafe { core::ptr::copy_nonoverlapping(p, out, 1) });",
        Suite::Loom,
    ),
    mutant(
        "pop_slice: head release Release -> Relaxed",
        RAW,
        "pop_slice",
        "S::store(self.queue.shared_head(), next_head, Ordering::Release)",
        "S::store(self.queue.shared_head(), next_head, Ordering::Relaxed)",
        Suite::Loom,
    ),
    mutant(
        "typed liveness: dead flag Release -> Relaxed",
        TYPED,
        "mark_dead",
        "self.0.store(Self::DEAD, Ordering::Release)",
        "self.0.store(Self::DEAD, Ordering::Relaxed)",
        Suite::Loom,
    ),
    mutant(
        "typed liveness: flag load Acquire -> Relaxed",
        TYPED,
        "is_alive",
        "self.0.load(Ordering::Acquire)",
        "self.0.load(Ordering::Relaxed)",
        Suite::Loom,
    ),
    mutant(
        "release_share: final-share RMW AcqRel -> Release",
        TYPED,
        "release_share",
        ".fetch_sub(1, Ordering::AcqRel)",
        ".fetch_sub(1, Ordering::Release)",
        Suite::Loom,
    ),
    mutant(
        "release_share: final-share RMW AcqRel -> Acquire",
        TYPED,
        "release_share",
        ".fetch_sub(1, Ordering::AcqRel)",
        ".fetch_sub(1, Ordering::Acquire)",
        Suite::Loom,
    ),
    mutant(
        "split: claim ignored (every caller wins)",
        TYPED,
        "split",
        ".is_err()\n    {",
        ".is_err() && false\n    {",
        Suite::Loom,
    ),
    mutant(
        "shared close: CLOSED store Release -> Relaxed",
        SHM,
        "close_producer",
        ".store(CLOSED, Ordering::Release)",
        ".store(CLOSED, Ordering::Relaxed)",
        Suite::Loom,
    ),
    mutant(
        "shared liveness: role load Acquire -> Relaxed",
        SHM,
        "producer_alive",
        ".load(Ordering::Acquire) != CLOSED",
        ".load(Ordering::Relaxed) != CLOSED",
        Suite::Loom,
    ),
    mutant(
        "shared claim: claimed role accepted again",
        SHM,
        "claim",
        "Err(LIVE | CLOSED) => Err(SharedError::RoleAlreadyClaimed)",
        "Err(LIVE | CLOSED) => Ok(())",
        Suite::Loom,
    ),
    // Stale-cache mutants: full/empty decided from the cache alone, ignoring
    // the fresh load. Memory-safe, but a spurious `Full` or empty result that
    // the `*_are_never_stale` tests must catch.
    mutant(
        "try_push: Full from stale cache",
        RAW,
        "try_push",
        "if tail == full_at {",
        "if true {",
        Suite::Loom,
    ),
    mutant(
        "has_available: empty from stale cache",
        RAW,
        "has_available",
        "if head == tail {",
        "if true {",
        Suite::Loom,
    ),
    mutant(
        "push_slice: never refresh cached head",
        RAW,
        "push_slice",
        "if free < source.len() {",
        "if false {",
        Suite::Loom,
    ),
    mutant(
        "pop_slice: never refresh cached tail",
        RAW,
        "pop_slice",
        "if available < destination.len() {",
        "if false {",
        Suite::Loom,
    ),
    // Narrow-counter negative control: reconstructing the physical head from the
    // wrapped counter must fail the narrow-counter cleanup test.
    mutant(
        "destroy: cleanup starts at wrapped_head % capacity",
        TYPED,
        "destroy",
        "index: c.final_head_index.load(Ordering::Acquire),",
        "index: head.distance(S::ZERO) % capacity,",
        Suite::Narrow,
    ),
];

/// Orderings deliberately not mutated, with the reason. Weakening these does
/// not break an invariant, so no model can (or should) detect it.
const NOT_MUTATED: &str = "
  is_drained tail load   after acquiring producer-dead, coherence already
                         forces the final tail; Relaxed would be equivalent
  len() loads            snapshot only; no slot access depends on them
  destroy() loads        run after the AcqRel final-share RMW, which already
                         orders every earlier access
  final_head_index store published by the consumer's AcqRel fetch_sub
  split claim orderings  the claim only arbitrates between callers (its
                         atomicity is what matters); the fresh control state
                         was established by construction or by `reset`
                         under `&mut`, which happens-before any sharing
  shared readiness load  the required external startup handoff already
                         orders initialization before every attach
  shared role CAS        arbitration only, as for the split claim; roles
                         never resume, so no earlier role state is inherited
  close_consumer store   nothing is published after a consumer closes; the
                         producer only reads it as a liveness snapshot";

const FN_START: &str = r"\n\s*(?:pub(?:\((?:crate|super)\))? )?(?:const )?(?:unsafe )?fn (\w+)";

/// Replaces the single occurrence of `old` inside function `function`.
fn mutate(fn_start: &Regex, source: &str, function: &str, old: &str, new: &str) -> Result<String> {
    let starts: Vec<(usize, &str)> = fn_start
        .captures_iter(source)
        .map(|c| {
            (
                c.get(0).expect("whole match").start(),
                c.get(1).expect("name").as_str(),
            )
        })
        .collect();
    let spans: Vec<(usize, usize)> = starts
        .iter()
        .enumerate()
        .filter(|(_, (_, name))| *name == function)
        .map(|(i, &(begin, _))| (begin, starts.get(i + 1).map_or(source.len(), |s| s.0)))
        .collect();
    let &[(begin, end)] = spans.as_slice() else {
        return Err(format!("expected one `fn {function}`, found {}", spans.len()).into());
    };
    let body = &source[begin..end];
    let occurrences = body.matches(old).count();
    if occurrences != 1 {
        return Err(format!("`{old}` occurs {occurrences} times in `fn {function}`").into());
    }
    Ok([&source[..begin], &body.replace(old, new), &source[end..]].concat())
}

fn suite_passes(crate_dir: &Path, suite: Suite) -> Result<bool> {
    let target_dir = root().join("target/loom-mutants").join(suite.name());
    let status = cargo()
        .current_dir(crate_dir)
        .args(suite.args())
        .env("CARGO_TARGET_DIR", target_dir)
        .envs(suite.env().iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    Ok(status.success())
}

/// A scratch directory, removed on drop.
struct ScratchDir(PathBuf);

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Copies the tree at `from` to `to`, skipping `target` and `.git`
/// directories at every level.
fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "target" || name == ".git" {
            continue;
        }
        let (source, destination) = (entry.path(), to.join(&name));
        if fs::metadata(&source)?.is_dir() {
            copy_tree(&source, &destination)?;
        } else {
            fs::copy(&source, &destination)?;
        }
    }
    Ok(())
}

fn progress(message: &str) {
    print!("{message} ... ");
    let _ = std::io::stdout().flush();
}

pub fn run() -> Result<ExitCode> {
    let fn_start = Regex::new(FN_START)?;
    let scratch =
        ScratchDir(std::env::temp_dir().join(format!("loom-mutants-{}", std::process::id())));
    let crate_dir = scratch.0.join("crate");
    copy_tree(root(), &crate_dir)?;
    let mut originals = BTreeMap::new();
    for m in MUTANTS {
        if !originals.contains_key(m.file) {
            originals.insert(m.file, fs::read_to_string(crate_dir.join(m.file))?);
        }
    }

    // Validate every anchor before spending time on model runs.
    for m in MUTANTS {
        mutate(&fn_start, &originals[m.file], m.function, m.old, m.new)?;
    }

    for suite in Suite::ALL {
        progress(&format!("control (unmutated, {})", suite.name()));
        if !suite_passes(&crate_dir, suite)? {
            println!("FAILED");
            return Err(format!("the {} suite fails on unmodified code", suite.name()).into());
        }
        println!("passes");
    }

    let mut survivors = Vec::new();
    for m in MUTANTS {
        let original = &originals[m.file];
        let path = crate_dir.join(m.file);
        fs::write(
            &path,
            mutate(&fn_start, original, m.function, m.old, m.new)?,
        )?;
        progress(m.label);
        if suite_passes(&crate_dir, m.suite)? {
            println!("SURVIVED");
            survivors.push(m.label);
        } else {
            println!("detected");
        }
        fs::write(&path, original)?;
    }
    drop(scratch);

    println!("\nNot mutated (weakening is harmless):{NOT_MUTATED}");
    if !survivors.is_empty() {
        println!(
            "\n{} mutant(s) survived: the suite has a blind spot",
            survivors.len()
        );
        return Ok(ExitCode::FAILURE);
    }
    println!("\nall {} mutants detected", MUTANTS.len());
    Ok(ExitCode::SUCCESS)
}
