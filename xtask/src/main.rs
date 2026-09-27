//! Development tasks for spookycircle.
//!
//! ```text
//! cargo xtask bench-regress save|check <baseline> [criterion filter]
//! cargo xtask inspect-codegen [--update-baseline] [target-triple]
//! cargo xtask loom-mutants
//! cargo xtask check-symbols
//! cargo xtask clippy-kani
//! ```
//!
//! Each task is documented in its module.

mod bench_regress;
mod check_symbols;
mod clippy_kani;
mod inspect_codegen;
mod loom_mutants;

use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const USAGE: &str = "\
usage: cargo xtask <task> [args]

tasks:
  bench-regress save|check <baseline> [criterion filter]
  inspect-codegen [--update-baseline] [target-triple]
  loom-mutants
  check-symbols
  clippy-kani
";

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some((task, rest)) = args.split_first() else {
        return usage(USAGE);
    };
    let result = match task.as_str() {
        "bench-regress" => bench_regress::run(rest),
        "inspect-codegen" => inspect_codegen::run(rest),
        "loom-mutants" if rest.is_empty() => loom_mutants::run(),
        "check-symbols" if rest.is_empty() => check_symbols::run(),
        "clippy-kani" if rest.is_empty() => clippy_kani::run(),
        _ => return usage(USAGE),
    };
    result.unwrap_or_else(|e| {
        eprintln!("error: {e}");
        ExitCode::FAILURE
    })
}

/// Prints `message` to stderr and returns the usage-error exit code.
fn usage(message: &str) -> ExitCode {
    eprint!("{message}");
    ExitCode::from(2)
}

/// The repository root (the parent of this crate).
fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in a subdirectory of the repository")
}

/// A `cargo` command in the repository root, using the Cargo that runs this
/// task when there is one.
fn cargo() -> Command {
    let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo")));
    command.current_dir(root());
    command
}

/// Runs `command`, failing unless it exits successfully.
fn run_command(command: &mut Command) -> Result<()> {
    let status = command
        .status()
        .map_err(|e| format!("cannot run {command:?}: {e}"))?;
    if !status.success() {
        return Err(format!("{command:?} failed ({status})").into());
    }
    Ok(())
}

/// Runs `command` and returns its standard output, failing unless it exits
/// successfully. Standard error passes through.
fn output(command: &mut Command) -> Result<String> {
    let out = command
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| format!("cannot run {command:?}: {e}"))?;
    if !out.status.success() {
        return Err(format!("{command:?} failed ({})", out.status).into());
    }
    Ok(String::from_utf8(out.stdout)?)
}

/// The host target triple, from `rustc -vV`.
fn host_triple() -> Result<String> {
    let info = output(Command::new("rustc").arg("-vV"))?;
    info.lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(str::to_owned)
        .ok_or_else(|| "`rustc -vV` printed no host triple".into())
}

/// Searches `PATH` for an executable called `name`.
fn which(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .flat_map(|dir| {
            [
                dir.join(name),
                dir.join(format!("{name}{}", env::consts::EXE_SUFFIX)),
            ]
        })
        .find(|candidate| candidate.is_file())
}

/// Joins `lines` with single spaces, collapsing every run of whitespace.
fn squeeze<'a>(lines: impl IntoIterator<Item = &'a str>) -> String {
    lines
        .into_iter()
        .flat_map(str::split_whitespace)
        .collect::<Vec<_>>()
        .join(" ")
}
