//! Puts `link.x` on the linker search path and links with it.
//!
//! The `-Tlink.x` argument is emitted here rather than set as `rustflags` in
//! `.cargo/config.toml`: a `RUSTFLAGS` environment variable (CI sets
//! `-D warnings`) replaces config `rustflags` entirely, which would drop it.
fn main() {
    let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("cargo:rustc-link-search={dir}");
    println!("cargo:rustc-link-arg=-Tlink.x");
    println!("cargo:rerun-if-changed=link.x");
}
