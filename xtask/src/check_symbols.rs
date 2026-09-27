//! Allocator-free link test and symbol audit (a
//! successful `cargo check` alone is insufficient).
//!
//! ```text
//! cargo xtask check-symbols
//! ```
//!
//! Builds `ci/embedded` and checks the linked image for allocator,
//! unwinding, and other unexpected runtime dependencies.

use std::fs;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

use regex::Regex;

use crate::{Result, cargo, output, root, run_command, which};

const IMAGE: &str = "target/thumbv7em-none-eabihf/release/spookycircle-embedded";

pub fn run() -> Result<ExitCode> {
    let crate_dir = root().join("ci/embedded");
    // In the crate's own directory, so that its `.cargo/config.toml` selects
    // the target and linker script.
    run_command(cargo().current_dir(&crate_dir).args(["build", "--release"]))?;
    let elf = crate_dir.join(IMAGE);
    let nm = nm()?;
    let symbols = output(Command::new(&nm).arg(&elf))?;
    if symbols.trim().is_empty() {
        return Err(format!(
            "{} printed no symbols for {} (cannot read ARM ELF?); set NM",
            nm.display(),
            elf.display()
        )
        .into());
    }

    // Whole symbol names, case-sensitive: the global-allocator shims, libc
    // heap functions, and the unwinding runtime. (`rust_begin_unwind` is the
    // panic handler entry point, not the unwinder, and is expected.)
    let forbidden = Regex::new(
        r"^__rust_(alloc|dealloc|realloc|alloc_zeroed|alloc_error_handler|no_alloc_shim_is_unstable.*)$|^__rg_|^(malloc|calloc|realloc|free|posix_memalign)$|^_Unwind_|^rust_eh_personality$",
    )?;
    let bad: Vec<&str> = symbols
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .filter(|name| forbidden.is_match(name))
        .collect();
    // An `nm` that rejects `-u` prints nothing here, as before.
    let undefined = Command::new(&nm).arg("-u").arg(&elf).output()?;
    let undefined = String::from_utf8(undefined.stdout)?;
    let undefined = undefined.trim_end();

    println!(
        "image: ci/embedded/{IMAGE} ({} bytes)",
        fs::metadata(&elf)?.len()
    );
    if !bad.is_empty() {
        println!("FORBIDDEN symbols in allocator-free image:");
        println!("{}", bad.join("\n"));
        return Ok(ExitCode::FAILURE);
    }
    if !undefined.is_empty() {
        println!("undefined symbols (the image must link standalone):\n{undefined}");
        return Ok(ExitCode::FAILURE);
    }
    for want in ["Reset"] {
        if !symbols
            .lines()
            .any(|line| line.ends_with(&format!(" {want}")))
        {
            println!("missing symbol {want}");
            return Ok(ExitCode::FAILURE);
        }
    }
    println!("ok: no allocator, unwinding, or undefined symbols");
    Ok(ExitCode::SUCCESS)
}

/// An ELF-capable `nm`: `$NM`, the Rust toolchain's `llvm-nm` (rustup
/// component `llvm-tools`), then any `llvm-nm`, then (on Linux CI) GNU `nm`.
fn nm() -> Result<PathBuf> {
    if let Some(nm) = std::env::var_os("NM").filter(|nm| !nm.is_empty()) {
        return Ok(nm.into());
    }
    let sysroot = output(Command::new("rustc").args(["--print", "sysroot"]))?;
    let rustlib = PathBuf::from(sysroot.trim()).join("lib/rustlib");
    if let Ok(entries) = fs::read_dir(&rustlib) {
        let mut hosts: Vec<PathBuf> = entries.filter_map(|e| Some(e.ok()?.path())).collect();
        hosts.sort();
        let exe = format!("llvm-nm{}", std::env::consts::EXE_SUFFIX);
        if let Some(nm) = hosts
            .iter()
            .map(|h| h.join("bin").join(&exe))
            .find(|p| p.is_file())
        {
            return Ok(nm);
        }
    }
    if let Some(nm) = which("llvm-nm") {
        return Ok(nm);
    }
    if let Ok(out) = Command::new("xcrun").args(["--find", "llvm-nm"]).output() {
        let found = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        if out.status.success() && !found.is_empty() {
            return Ok(found.into());
        }
    }
    which("nm").ok_or_else(|| "no nm found; install llvm-tools or set NM".into())
}
