//! Thread placement for the two-thread stress tests and benchmarks. Also
//! compiled into `benches/common` through a `#[path]` attribute.

/// Pins the calling thread to core `index` (modulo the core count) where the
/// platform supports it. Callers decide whether pinning is enabled.
///
/// macOS has no thread-to-core affinity (Apple Silicon ignores
/// `THREAD_AFFINITY_POLICY`), so there the thread only gets the
/// user-interactive `QoS` class, which keeps it on the performance cores. The
/// scheduler still chooses the core, and so whether the two threads share an
/// L2 cluster.
pub fn pin_current_thread(index: usize) {
    #[cfg(target_os = "macos")]
    {
        let _ = index;
        // SAFETY: sets the QoS class of the calling thread only; no pointers.
        unsafe {
            libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0);
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        // Capture the allowed CPUs before this helper pins any thread.
        // On Linux, later workers inherit their parent's restricted mask.
        static IDS: std::sync::OnceLock<Option<Vec<core_affinity::CoreId>>> =
            std::sync::OnceLock::new();
        if let Some(ids) = IDS.get_or_init(core_affinity::get_core_ids)
            && ids.len() > 1
        {
            assert!(
                core_affinity::set_for_current(ids[index % ids.len()]),
                "failed to pin thread to the selected CPU"
            );
        }
    }
}
