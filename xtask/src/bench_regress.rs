//! Benchmark regression flagging.
//!
//! ```text
//! cargo xtask bench-regress save  <baseline> [criterion filter]   # reference build
//! cargo xtask bench-regress check <baseline> [criterion filter]   # candidate build
//! ```
//!
//! `save` records a named Criterion baseline for `benches/spsc.rs`. `check`
//! re-runs the benchmarks against it and exits 1, listing the benchmarks, if
//! any regressed by more than `THRESHOLD` (default 0.10, i.e. 10%) with the
//! *entire* 95% confidence interval of the mean change above the threshold,
//! so noise alone does not trip it. A flagged regression is a prompt for
//! review: do not paper over it by saving a new baseline.
//!
//! Run both steps on the same controlled host (same CPU, power settings,
//! affinity policy, and background load); cross-host comparisons are
//! meaningless. Not run in CI, whose shared runners are not controlled hosts.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::SystemTime;

use serde_json::Value;

use crate::{Result, cargo, root, run_command, usage};

const USAGE: &str = "usage: cargo xtask bench-regress save|check <baseline> [criterion filter]\n";

pub fn run(args: &[String]) -> Result<ExitCode> {
    let (mode, baseline, filter) = match args {
        [mode, baseline] => (mode, baseline, None),
        [mode, baseline, filter] => (mode, baseline, Some(filter)),
        _ => return Ok(usage(USAGE)),
    };
    if baseline.is_empty() || (mode != "save" && mode != "check") {
        return Ok(usage(USAGE));
    }
    let threshold = match std::env::var("THRESHOLD") {
        Ok(t) => t
            .parse::<f64>()
            .map_err(|_| format!("THRESHOLD `{t}` is not a number"))?,
        Err(_) => 0.10,
    };

    let mut bench = cargo();
    bench.args(["bench", "--all-features", "--bench", "spsc", "--"]);
    bench.args(filter.filter(|f| !f.is_empty()));

    if mode == "save" {
        run_command(bench.args(["--save-baseline", baseline]))?;
        return Ok(ExitCode::SUCCESS);
    }

    // Only change files written by this run count; earlier comparisons may
    // have left stale ones for benchmarks outside the current filter. Take
    // the reference time from the filesystem, which stamps those files.
    let stamp = stamp()?;
    // `--baseline-lenient`: benchmarks added since the baseline was saved
    // (for example new storage backends) are measured but not compared,
    // instead of aborting the whole run.
    run_command(bench.args(["--baseline-lenient", baseline]))?;

    let criterion = root().join("target/criterion");
    let mut changes = Vec::new();
    find_changes(&criterion, &mut changes)?;
    let (mut flagged, mut checked) = (Vec::new(), 0);
    for path in changes {
        if fs::metadata(&path)?.modified()? < stamp {
            continue;
        }
        checked += 1;
        let estimates: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
        let mean = &estimates["mean"];
        let number = |v: &Value| {
            v.as_f64()
                .ok_or_else(|| format!("{}: malformed mean estimate", path.display()))
        };
        let point = number(&mean["point_estimate"])?;
        let lower = number(&mean["confidence_interval"]["lower_bound"])?;
        if lower > threshold {
            // `<name>/change/estimates.json`
            let dir = path
                .parent()
                .and_then(Path::parent)
                .expect("found under `change/`");
            let name = dir.strip_prefix(&criterion)?.display().to_string();
            flagged.push((name, point, lower));
        }
    }

    println!(
        "\ncompared {checked} benchmark(s) against the baseline; threshold {:.0}%",
        threshold * 100.0
    );
    if checked == 0 {
        return Err("no comparisons were produced; does the baseline exist?".into());
    }
    flagged.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, point, lower) in &flagged {
        println!(
            "REGRESSION {name}: {:+.1}% (95% CI lower bound {:+.1}%)",
            point * 100.0,
            lower * 100.0
        );
    }
    Ok(if flagged.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// The modification time of a freshly written file.
fn stamp() -> Result<SystemTime> {
    let path = std::env::temp_dir().join(format!("bench-regress-{}.stamp", std::process::id()));
    fs::write(&path, "")?;
    let time = fs::metadata(&path)?.modified();
    fs::remove_file(&path)?;
    Ok(time?)
}

/// Collects every `change/estimates.json` below `dir`.
fn find_changes(dir: &Path, found: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            find_changes(&path, found)?;
        } else if path.ends_with("change/estimates.json") {
            found.push(path);
        }
    }
    Ok(())
}
