//! Regression coverage for workers inheriting a pinned parent's CPU mask.

#![cfg(all(target_os = "linux", not(loom)))]

#[path = "common/affinity.rs"]
mod affinity;

#[test]
fn repeated_workers_can_leave_the_parents_cpu() {
    // Keep the test harness thread's affinity unchanged.
    std::thread::spawn(|| {
        let ids = core_affinity::get_core_ids().expect("read CPU affinity");
        if ids.len() < 2 {
            return;
        }
        affinity::pin_current_thread(1);
        assert_eq!(core_affinity::get_core_ids().unwrap(), vec![ids[1]]);
        for _ in 0..3 {
            let cpu = ids[0];
            std::thread::spawn(move || {
                affinity::pin_current_thread(0);
                assert_eq!(core_affinity::get_core_ids().unwrap(), vec![cpu]);
            })
            .join()
            .unwrap();
        }
    })
    .join()
    .unwrap();
}
