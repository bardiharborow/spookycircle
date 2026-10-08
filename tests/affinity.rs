//! Regression coverage for workers inheriting a pinned parent's CPU mask.

#![cfg(all(target_os = "linux", not(loom)))]

#[path = "common/affinity.rs"]
mod affinity;

#[test]
fn repeated_workers_can_leave_the_parents_cpu() {
    // Keep the test harness thread's affinity unchanged.
    std::thread::spawn(|| {
        let Some(parent) = affinity::pin_current_thread(1) else {
            return;
        };
        assert_eq!(core_affinity::get_core_ids().unwrap(), vec![parent]);
        for _ in 0..3 {
            std::thread::spawn(move || {
                let cpu = affinity::pin_current_thread(0).expect("pinned");
                assert_ne!(cpu, parent);
                assert_eq!(core_affinity::get_core_ids().unwrap(), vec![cpu]);
            })
            .join()
            .unwrap();
        }
    })
    .join()
    .unwrap();
}
