//! Thread placement for the two-thread stress tests and benchmarks. Also
//! compiled into `benches/common` through a `#[path]` attribute.

/// Pins the calling thread to physical core `index` (modulo the core count)
/// where the platform supports it, and returns the logical CPU it chose.
/// Callers decide whether pinning is enabled.
///
/// Only one logical CPU per physical core is used, so that two threads given
/// different indices never share a core's L1 and L2 as SMT siblings would:
/// the transfer under test should cross cores. Windows numbers siblings
/// adjacently (0 and 1 share a core), and some Linux hosts do too. If the
/// topology cannot be read, or the allowed CPUs span a single physical core,
/// every allowed logical CPU is used instead.
///
/// macOS has no thread-to-core affinity (Apple Silicon ignores
/// `THREAD_AFFINITY_POLICY`), so there the thread only gets the
/// user-interactive `QoS` class, which keeps it on the performance cores. The
/// scheduler still chooses the core, and so whether the two threads share an
/// L2 cluster.
pub fn pin_current_thread(index: usize) -> Option<core_affinity::CoreId> {
    #[cfg(target_os = "macos")]
    {
        let _ = index;
        // SAFETY: sets the QoS class of the calling thread only; no pointers.
        unsafe {
            libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0);
        }
        None
    }
    #[cfg(not(target_os = "macos"))]
    {
        // Capture the allowed CPUs before this helper pins any thread.
        // On Linux, later workers inherit their parent's restricted mask.
        static IDS: std::sync::OnceLock<Option<Vec<core_affinity::CoreId>>> =
            std::sync::OnceLock::new();
        let ids = IDS
            .get_or_init(|| core_affinity::get_core_ids().map(one_per_core))
            .as_ref()?;
        if ids.len() < 2 {
            return None;
        }
        let id = ids[index % ids.len()];
        assert!(
            core_affinity::set_for_current(id),
            "failed to pin thread to the selected CPU"
        );
        Some(id)
    }
}

/// The first allowed logical CPU of each physical core, in order; all of
/// `ids` when that leaves fewer than two.
#[cfg(not(target_os = "macos"))]
fn one_per_core(ids: Vec<core_affinity::CoreId>) -> Vec<core_affinity::CoreId> {
    let mut seen = Vec::new();
    let primaries: Vec<_> = ids
        .iter()
        .copied()
        .filter(|cpu| {
            // A CPU of unknown topology counts as a core of its own.
            let Some(core) = physical_core(cpu.id) else {
                return true;
            };
            let first = !seen.contains(&core);
            seen.push(core);
            first
        })
        .collect();
    if primaries.len() < 2 { ids } else { primaries }
}

/// An identifier shared by exactly the logical CPUs of `cpu`'s physical core:
/// the kernel's list of them.
#[cfg(target_os = "linux")]
fn physical_core(cpu: usize) -> Option<String> {
    let topology = format!("/sys/devices/system/cpu/cpu{cpu}/topology");
    // `core_cpus_list` since Linux 5.6; `thread_siblings_list` before.
    ["core_cpus_list", "thread_siblings_list"]
        .iter()
        .find_map(|file| std::fs::read_to_string(format!("{topology}/{file}")).ok())
        .map(|list| list.trim().to_owned())
}

/// An identifier shared by exactly the logical CPUs of `cpu`'s physical core:
/// the core's processor mask. Covers processor group 0 only, as
/// `core_affinity` does on Windows.
#[cfg(windows)]
fn physical_core(cpu: usize) -> Option<usize> {
    /// `SYSTEM_LOGICAL_PROCESSOR_INFORMATION`; the trailing union is 16
    /// bytes, 8-byte aligned.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Info {
        processor_mask: usize,
        relationship: u32,
        union: [u64; 2],
    }
    /// `RelationProcessorCore`.
    const RELATION_PROCESSOR_CORE: u32 = 0;
    unsafe extern "system" {
        fn GetLogicalProcessorInformation(buffer: *mut Info, length: *mut u32) -> i32;
    }

    static CORES: std::sync::OnceLock<Vec<usize>> = std::sync::OnceLock::new();
    let cores = CORES.get_or_init(|| {
        let mut length = 0u32;
        // SAFETY: a null buffer with a zero length only asks for the
        // required length, written to `length`.
        unsafe { GetLogicalProcessorInformation(std::ptr::null_mut(), &raw mut length) };
        let empty = Info {
            processor_mask: 0,
            relationship: u32::MAX,
            union: [0; 2],
        };
        let mut buffer = vec![empty; (length as usize).div_ceil(size_of::<Info>())];
        let mut length = u32::try_from(buffer.len() * size_of::<Info>()).unwrap_or(0);
        // SAFETY: `buffer` holds `length` writable bytes of `Info`, which
        // has the layout the function writes.
        let ok = unsafe { GetLogicalProcessorInformation(buffer.as_mut_ptr(), &raw mut length) };
        if ok == 0 {
            return Vec::new();
        }
        buffer.truncate(length as usize / size_of::<Info>());
        buffer
            .iter()
            .filter(|info| info.relationship == RELATION_PROCESSOR_CORE)
            .map(|info| info.processor_mask)
            .collect()
    });
    let bit = 1usize.checked_shl(u32::try_from(cpu).ok()?)?;
    cores.iter().copied().find(|mask| mask & bit != 0)
}

/// Topology unknown: every logical CPU counts as a core of its own.
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn physical_core(_cpu: usize) -> Option<()> {
    None
}
