//! Detects `core::hint::prefetch_write` for the `prefetch` feature (see
//! `src/prefetch.rs`).
//!
//! Emits `spookycircle_hint_prefetch` when the target compiler accepts it,
//! and additionally `spookycircle_hint_prefetch_unstable` when it only does
//! so under `#![feature(hint_prefetch)]` (nightly, until
//! rust-lang/rust#146941 is stabilized). A probe that fails for any reason,
//! including a nightly whose API has since changed, emits nothing and the
//! crate falls back to its stable `asm!` path, so the probe can never break
//! the build.

use std::{env, ffi::OsString, fs, path::PathBuf, process::Command};

/// The call `src/prefetch.rs` makes, with the same argument types.
const PROBE: &str = "
pub fn probe(write: *mut u8) {
    core::hint::prefetch_write(write, core::hint::Locality::L1);
}
";

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(spookycircle_hint_prefetch)");
    println!("cargo::rustc-check-cfg=cfg(spookycircle_hint_prefetch_unstable)");
    if env::var_os("CARGO_FEATURE_PREFETCH").is_none() {
        return;
    }
    if compiles("#![no_std]") {
        println!("cargo::rustc-cfg=spookycircle_hint_prefetch");
    } else if compiles("#![no_std]\n#![feature(hint_prefetch)]") {
        println!("cargo::rustc-cfg=spookycircle_hint_prefetch");
        println!("cargo::rustc-cfg=spookycircle_hint_prefetch_unstable");
    }
}

/// Whether `PROBE`, preceded by `attributes`, type-checks with the compiler,
/// target, and flags Cargo uses for this crate.
fn compiles(attributes: &str) -> bool {
    let (Some(rustc), Some(out_dir), Ok(target)) = (
        env::var_os("RUSTC"),
        env::var_os("OUT_DIR").map(PathBuf::from),
        env::var("TARGET"),
    ) else {
        return false;
    };
    let source = out_dir.join("probe_hint_prefetch.rs");
    if fs::write(&source, format!("{attributes}\n{PROBE}")).is_err() {
        return false;
    }
    let mut command = Command::new(rustc);
    command
        .args(["--edition", "2024", "--crate-type", "lib", "--emit", "metadata"])
        .args(["--crate-name", "probe_hint_prefetch", "--target", &target])
        .arg("--out-dir")
        .arg(&out_dir)
        .arg(&source);
    // The crate's own flags, so that, for example, `-Zallow-features` or a
    // custom sysroot governs the probe as it governs the crate.
    if let Some(flags) = env::var_os("CARGO_ENCODED_RUSTFLAGS") {
        let flags = flags.to_string_lossy().into_owned();
        command.args(
            flags
                .split('\u{1f}')
                .filter(|flag| !flag.is_empty())
                .map(OsString::from),
        );
    }
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}
