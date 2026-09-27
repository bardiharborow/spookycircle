//! Code-generation audit for the data path.
//!
//! ```text
//! cargo xtask inspect-codegen [--update-baseline] [target-triple]
//! ```
//!
//! Emits optimised assembly for ci/codegen (a standalone crate, so a cross
//! target builds only the library, not the root dev-dependencies; it covers
//! heap, static, and shared-region endpoints) and checks every `codegen_*`
//! function:
//!
//! - Every expected wrapper is present. A wrapper that disappears (renamed,
//!   or merged by LLVM into an identical twin) would otherwise go unchecked.
//!   Only the merges listed in `MERGEABLE` are accepted.
//! - No calls, tail calls, or indirect branches; no fences; no
//!   read-modify-write/CAS; no integer division. The bulk paths may call
//!   `memcpy`/`memmove` (after vectorisation), and nothing else.
//! - The instruction count does not exceed the per-target baseline in
//!   `ci/codegen-baseline/<triple>.txt` by more than the tolerance. With
//!   `--update-baseline`, the baseline is rewritten from this build instead.
//!
//! On aarch64 it also requires exactly the acquire loads and release stores
//! each function's protocol needs, and rejects byte- or halfword-width
//! acquire/release (the crate uses pointer-width atomics only). On `x86_64`, acquire
//! loads and release stores are plain `mov`s under TSO, so only the forbidden-instruction check applies there; a store
//! strengthened to `SeqCst` shows up as a forbidden `xchg`.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::process::{Command, ExitCode, Stdio};
use std::time::SystemTime;

use regex::Regex;

use crate::{Result, cargo, host_triple, output, root, run_command, squeeze, usage, which};

const USAGE: &str = "usage: cargo xtask inspect-codegen [--update-baseline] [target-triple]\n";

/// Every wrapper in ci/codegen/src/main.rs. Keep in sync with it.
const REQUIRED: &[&str] = &[
    "codegen_try_push",
    "codegen_try_pop",
    "codegen_try_pop_into",
    "codegen_peek",
    "codegen_peek_mut",
    "codegen_producer_len",
    "codegen_consumer_len",
    "codegen_is_drained",
    "codegen_is_consumer_alive",
    "codegen_push_slice",
    "codegen_pop_slice",
    "codegen_wide_try_push",
    "codegen_wide_try_pop",
    "codegen_wide_try_pop_into",
    "codegen_owned_try_push",
    "codegen_owned_try_pop",
    "codegen_static_try_push",
    "codegen_static_try_pop",
    "codegen_static_is_drained",
    "codegen_shared_try_push",
    "codegen_shared_try_pop",
    "codegen_shared_is_drained",
];

/// `(wrapper, twin)`: `wrapper` may be absent because LLVM merged it into
/// `twin`. The static `is_drained` never touches an element, so its machine
/// code is identical to the heap one's; identical code needs no separate
/// audit.
const MERGEABLE: &[(&str, &str)] = &[("codegen_static_is_drained", "codegen_is_drained")];

/// The prefixed families wrap the same methods as the unprefixed functions
/// and share their expectations.
fn base_name(function: &str) -> String {
    for family in ["static", "shared", "wide", "owned"] {
        if let Some(rest) = function.strip_prefix(&format!("codegen_{family}_")) {
            return format!("codegen_{rest}");
        }
    }
    function.to_owned()
}

fn is_bulk(function: &str) -> bool {
    matches!(
        base_name(function).as_str(),
        "codegen_push_slice" | "codegen_pop_slice"
    )
}

/// Exact (acquire loads, release stores) each function contains on aarch64.
/// Counts are static: a refresh load on a cold branch still counts.
fn expect(function: &str) -> Option<(usize, usize)> {
    match base_name(function).as_str() {
        "codegen_try_push"
        | "codegen_try_pop"
        | "codegen_try_pop_into"
        | "codegen_push_slice"
        | "codegen_pop_slice" => Some((1, 1)),
        "codegen_peek" | "codegen_peek_mut" => Some((1, 0)),
        "codegen_producer_len" | "codegen_consumer_len" | "codegen_is_consumer_alive" => {
            Some((1, 0))
        }
        "codegen_is_drained" => Some((2, 0)),
        _ => None,
    }
}

const ACQUIRE: &str = r"^\s*(ldar|ldapr|ldapur)\s";
const RELEASE: &str = r"^\s*(stlr|stlur)\s";
const NARROW: &str = r"^\s*(ldar|ldapr|ldapur|stlr|stlur)[bh]\s";

/// Instructions forbidden on the data path:
/// calls: aarch64 `bl`/`blr`, x86 `call` (except to a local label: i686 PIC
///   code reads the program counter with `calll .L1$pb; .L1$pb: popl %ebx`);
/// tail calls to another symbol: aarch64 `b sym`, x86 `jmp sym` (local labels
///   start with `L` on Mach-O and `.L` on ELF/COFF, and are allowed);
/// indirect branches: aarch64 `br`, x86 `jmp *`;
/// fences: `dmb`/`dsb`/`mfence`/`lfence`/`sfence`;
/// RMW/CAS: `ldaxr`/`stlxr`/`ldxr`/`stxr`/`cas`/`swp`/`ldadd`, x86
///   `lock`/`cmpxchg`/`xchg`/`xadd`;
/// division: `udiv`/`sdiv`/`div`/`idiv`.
const FORBIDDEN: &str = r"^\s*((bl|blr|br|dmb|dsb|mfence|lfence|sfence|ldaxr|stlxr|ldxr|stxr|cas[a-z]*|swp[a-z]*|ldadd[a-z]*|lock|cmpxchg[a-z]*|xchg[a-z]*|xadd[a-z]*|udiv|sdiv|divq|divl|idivq|idivl)\b|b\s+[^L.[:space:]]|(call|jmp)[lq]?\s+([^L.[:space:]]|\*))";
/// The one exemption: a call or tail call to memcpy/memmove on a bulk path,
/// direct, through the PLT, or through the GOT.
const BULK_COPY: &str = r"^\s*(bl|b|call[lq]?|jmp[lq]?)\s+\*?_*(memcpy|memmove)\b";
const ORDERING: &str =
    r"^\s*(ldar|ldarb|ldapr|ldaprb|ldapur|ldapurb|stlr|stlrb|stlur|stlurb|dmb|mfence|xchg)";

/// Function labels are mangled (legacy `__ZN7codegen16codegen_try_push17h...E:`
/// or v0 `__RNvCs..._7codegen16codegen_try_push:`); pick every label
/// containing a `codegen_*` name.
const LABEL: &str = r"^[A-Za-z_.$][A-Za-z0-9_.$]*codegen_[a-z_]+[A-Za-z0-9_.$]*:";
/// The end of a function body. COFF leaf functions have no unwind info and so
/// no `.seh_endproc` or `.Lfunc_end`; there, the next `.def` or non-local
/// label ends the body (local labels start with `L` on Mach-O and `.L` on
/// ELF/COFF).
const BODY_END: &str = r"\.cfi_endproc|\.seh_endproc|^Lfunc_end|^\.Lfunc_end|^[ \t]*\.def[ \t]|^[A-KM-Za-z_$][A-Za-z0-9_.$]*:";

pub fn run(args: &[String]) -> Result<ExitCode> {
    let (update, target) = match args {
        [] => (false, None),
        [flag] if flag == "--update-baseline" => (true, None),
        [flag, target] if flag == "--update-baseline" => (true, Some(target.as_str())),
        [target] if !target.starts_with('-') => (false, Some(target.as_str())),
        _ => return Ok(usage(USAGE)),
    };
    let host = host_triple()?;
    let triple = target.unwrap_or(&host);
    let arch = triple.split('-').next().unwrap_or_default();

    // Emit to a fixed path so a stale file from an earlier build is never
    // read. Cargo skips rustc when nothing changed, so remove the old output
    // and touch the source to force it to run.
    let asm = root().join(format!("target/codegen/{triple}.s"));
    fs::create_dir_all(asm.parent().expect("has a parent"))?;
    match fs::remove_file(&asm) {
        Err(e) if e.kind() != ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    fs::File::options()
        .write(true)
        .open(root().join("ci/codegen/src/main.rs"))?
        .set_modified(SystemTime::now())?;
    let mut build = cargo();
    build.args([
        "rustc",
        "--locked",
        "--release",
        "--manifest-path",
        "ci/codegen/Cargo.toml",
        "--target-dir",
        "target/codegen-build",
    ]);
    if let Some(target) = target {
        build.args(["--target", target]);
    }
    build
        .arg("--")
        .arg("--emit")
        .arg(format!("asm={}", asm.display()));
    build.args(["-C", "debuginfo=0", "-C", "codegen-units=1"]);
    // Cargo still links the binary. Only the assembly is needed, and a
    // cross-compiling host usually has no linker for the target, so hand
    // rustc a no-op one; the assembly does not depend on the host.
    if triple != host {
        let noop = which("true").ok_or("no `true` executable to use as a no-op linker")?;
        build.arg("-C").arg(format!("linker={}", noop.display()));
    }
    run_command(build.stdout(Stdio::null()))?;
    let text = fs::read_to_string(&asm).unwrap_or_default();
    if text.is_empty() {
        return Err(format!("no assembly written to {}", asm.display()).into());
    }
    println!("assembly: {}", asm.display());
    let lines: Vec<&str> = text.lines().collect();

    let acquire = Regex::new(ACQUIRE)?;
    let release = Regex::new(RELEASE)?;
    let narrow = Regex::new(NARROW)?;
    let forbidden = Regex::new(FORBIDDEN)?;
    let bulk_copy = Regex::new(BULK_COPY)?;
    let ordering = Regex::new(ORDERING)?;
    let label_re = Regex::new(LABEL)?;
    let name_re = Regex::new(r"codegen_[a-z_]+")?;
    let body_end = Regex::new(BODY_END)?;
    let instruction = Regex::new(r"^\s+[a-z]")?;

    // Baseline instruction counts: `name count` per line. A function may grow
    // by at most max(2, 10%) before the audit fails, which absorbs minor
    // compiler drift but not an added bounds check, panic path, or loop.
    let baseline_file = format!("ci/codegen-baseline/{triple}.txt");
    let baseline_path = root().join(&baseline_file);
    let baseline = if update || !baseline_path.is_file() {
        None
    } else {
        Some(fs::read_to_string(&baseline_path)?)
    };
    let baseline_of = |function: &str| -> Result<Option<usize>> {
        let Some(baseline) = &baseline else {
            return Ok(None);
        };
        for line in baseline.lines() {
            let mut fields = line.split_whitespace();
            if fields.next() == Some(function)
                && let Some(count) = fields.next()
            {
                return Ok(Some(count.parse().map_err(|_| {
                    format!("{baseline_file}: bad count `{count}` for {function}")
                })?));
            }
        }
        Ok(None)
    };

    let mut failed = false;
    let mut fail = |message: String| {
        println!("   FAIL: {message}");
        failed = true;
    };
    let mut counts = Vec::new();
    let mut found = BTreeSet::new();

    let labels: BTreeSet<&str> = lines
        .iter()
        .filter_map(|line| label_re.find(line))
        .map(|m| m.as_str().trim_end_matches(':'))
        .collect();
    for label in labels {
        let function = name_re.find(label).expect("the label matched").as_str();
        found.insert(function);
        // The function body: from its label to the end of the function.
        let body: Vec<&str> = lines
            .iter()
            .skip_while(|line| line.strip_suffix(':') != Some(label))
            .skip(1)
            .take_while(|line| !body_end.is_match(line))
            .copied()
            .collect();
        let count = body
            .iter()
            .filter(|line| instruction.is_match(line))
            .count();
        counts.push(format!("{function} {count}"));
        println!("== {function} ({count} instructions)");

        if !REQUIRED.contains(&function) {
            fail(format!(
                "{function} is not in REQUIRED; add it (and its expectations) to xtask/src/inspect_codegen.rs"
            ));
        }

        let mut bad: Vec<&str> = body
            .iter()
            .copied()
            .filter(|l| forbidden.is_match(l))
            .collect();
        if is_bulk(function) {
            let (copies, rest): (Vec<&str>, Vec<&str>) =
                bad.into_iter().partition(|l| bulk_copy.is_match(l));
            if !copies.is_empty() {
                println!("   bulk copy: {}", squeeze(copies));
            }
            bad = rest;
        }
        if !bad.is_empty() {
            fail(format!("forbidden: {}", squeeze(bad)));
        }

        match baseline_of(function)? {
            Some(base) => {
                let slack = (base / 10).max(2);
                if count > base + slack {
                    fail(format!(
                        "{count} instructions exceeds baseline {base} (+{slack}); if intended, rerun with --update-baseline"
                    ));
                } else if count < base {
                    println!("   note: below baseline {base}; consider --update-baseline");
                }
            }
            None if baseline.is_some() => fail(format!(
                "no baseline for {function} in {baseline_file}; rerun with --update-baseline"
            )),
            None => {}
        }

        if arch == "aarch64" {
            match expect(function) {
                None => fail(format!(
                    "no acquire/release expectation recorded for {function}"
                )),
                Some((want_acq, want_rel)) => {
                    let acq = body.iter().filter(|l| acquire.is_match(l)).count();
                    let rel = body.iter().filter(|l| release.is_match(l)).count();
                    if acq != want_acq || rel != want_rel {
                        fail(format!(
                            "ordering: {acq} acquire load(s), {rel} release store(s); expected exactly {want_acq} and {want_rel}"
                        ));
                    }
                    let narrowed: Vec<&str> = body
                        .iter()
                        .copied()
                        .filter(|l| narrow.is_match(l))
                        .collect();
                    if !narrowed.is_empty() {
                        fail(format!("not pointer-width: {}", squeeze(narrowed)));
                    }
                }
            }
        }

        let ordered: Vec<&str> = body
            .iter()
            .copied()
            .filter(|l| ordering.is_match(l))
            .collect();
        if ordered.is_empty() {
            println!("   ordering: (plain loads/stores; expected on x86 for acquire/release)");
        } else {
            println!("   ordering: {}", squeeze(ordered));
        }
    }

    for &function in REQUIRED {
        if found.contains(function) {
            continue;
        }
        let twin = MERGEABLE
            .iter()
            .find(|(f, _)| *f == function)
            .map(|(_, twin)| *twin);
        match twin {
            Some(twin) if found.contains(twin) => {
                println!("== {function}: merged into {twin} (identical code)");
            }
            _ => {
                println!("== {function}");
                fail(
                    "not found in the assembly (renamed, inlined, or merged into a twin not listed in MERGEABLE)"
                        .to_owned(),
                );
            }
        }
    }

    if update {
        counts.sort();
        let rustc = output(Command::new("rustc").arg("-V"))?;
        let mut contents = format!(
            "# Instruction counts from `cargo xtask inspect-codegen --update-baseline`.\n# {}\n",
            rustc.trim()
        );
        for line in &counts {
            contents.extend([line.as_str(), "\n"]);
        }
        fs::create_dir_all(baseline_path.parent().expect("has a parent"))?;
        fs::write(&baseline_path, contents)?;
        println!("baseline written: {baseline_file}");
    } else if baseline.is_none() {
        println!(
            "note: no baseline for {triple}; instruction counts not gated (create one with --update-baseline)"
        );
    }

    Ok(if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
