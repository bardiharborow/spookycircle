//! Clippy on the Kani proof harnesses.
//!
//! ```text
//! cargo xtask clippy-kani
//! ```
//!
//! The `#[cfg(kani)]` proof modules use the `kani` crate, which ships with
//! Kani's own pinned nightly toolchain rather than on crates.io, so an
//! ordinary `cargo clippy` never compiles them. This task runs that
//! toolchain's Clippy with the configuration and library flags that
//! `cargo kani` passes to its compiler, with `-D warnings`, in a separate
//! target directory. Requires an installed Kani (`cargo install --locked
//! kani-verifier && cargo kani setup`); honours `KANI_HOME`. Kani installs
//! its toolchain with rustup's minimal profile, which has no Clippy, so
//! this task adds the `clippy` component to that toolchain when missing.

use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use crate::{Result, output, root, run_command};

pub fn run() -> Result<ExitCode> {
    let kani = kani_dir()?;
    let bin = kani.join("toolchain/bin");
    let lib = kani.join("lib");
    ensure_clippy(&kani)?;
    let flags = [
        "-Zunstable-options".to_owned(),
        "--cfg=kani".to_owned(),
        "-Zcrate-attr=feature(register_tool)".to_owned(),
        "-Zcrate-attr=register_tool(kanitool)".to_owned(),
        format!("--sysroot={}", kani.display()),
        format!("-L{}", lib.display()),
        "--extern=force:kani".to_owned(),
        format!(
            "--extern=noprelude,nounused:std={}",
            lib.join("libstd.rlib").display()
        ),
        "-Cpanic=abort".to_owned(),
        "-Dwarnings".to_owned(),
    ];
    let mut path = OsString::from(bin.as_os_str());
    if let Some(rest) = env::var_os("PATH") {
        path.push(if cfg!(windows) { ";" } else { ":" });
        path.push(rest);
    }
    run_command(
        Command::new(bin.join("cargo-clippy"))
            .current_dir(root())
            .args([
                "clippy",
                "--locked",
                "--lib",
                "--features",
                "shared-memory",
                "--target-dir",
                "target/kani-clippy",
            ])
            // Kani's toolchain throughout, not the one running this task.
            .env("PATH", path)
            .env("CARGO", bin.join("cargo"))
            .env("RUSTC", bin.join("rustc"))
            .env_remove("RUSTC_WRAPPER")
            .env_remove("RUSTFLAGS")
            .env("CARGO_ENCODED_RUSTFLAGS", flags.join("\u{1f}"))
            .env("RUSTC_BOOTSTRAP", "1"),
    )?;
    println!("ok: proof harnesses are Clippy-clean");
    Ok(ExitCode::SUCCESS)
}

/// Adds the `clippy` component to Kani's toolchain unless it is already
/// there. `cargo kani setup` installs the toolchain with `--profile
/// minimal`, so a fresh install (such as in CI) has no `cargo-clippy`.
fn ensure_clippy(kani: &Path) -> Result<()> {
    let bin = kani.join("toolchain/bin");
    if bin.join("cargo-clippy").is_file() || bin.join("cargo-clippy.exe").is_file() {
        return Ok(());
    }
    let version_file = kani.join("rust-toolchain-version");
    let toolchain = fs::read_to_string(&version_file)
        .map_err(|e| format!("cannot read {}: {e}", version_file.display()))?;
    let toolchain = toolchain.trim();
    println!("adding clippy to Kani's toolchain {toolchain}");
    run_command(
        Command::new("rustup").args(["component", "add", "clippy", "--toolchain", toolchain]),
    )
    .map_err(|e| format!("{e}; install Clippy on Kani's toolchain with `rustup component add clippy --toolchain {toolchain}`"))?;
    Ok(())
}

/// The installed Kani release directory: `$KANI_HOME` (default `~/.kani`)
/// joined with `kani-<version>`, the version reported by `cargo kani`.
fn kani_dir() -> Result<PathBuf> {
    let version = output(Command::new("cargo").args(["kani", "--version"]))
        .map_err(|e| format!("{e}; install Kani with `cargo install --locked kani-verifier`"))?;
    let version = version
        .split_whitespace()
        .find(|word| word.starts_with(|c: char| c.is_ascii_digit()))
        .ok_or_else(|| format!("cannot find a version in `cargo kani --version`: {version}"))?;
    let home = match env::var_os("KANI_HOME") {
        Some(home) => PathBuf::from(home),
        None => Path::new(&env::var_os("HOME").ok_or("HOME is not set")?).join(".kani"),
    };
    let dir = home.join(format!("kani-{version}"));
    if !dir.join("toolchain/bin").is_dir() {
        return Err(format!("{} is missing; run `cargo kani setup`", dir.display()).into());
    }
    Ok(dir)
}
