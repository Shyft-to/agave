//! Helpers for pinning pipeline threads to dedicated CPU cores.

/// Pins the calling thread to `cpu_core`. Failures are logged rather than
/// fatal so a misconfigured core degrades to an unpinned thread.
///
/// On Linux, threads spawned by the calling thread after this point inherit
/// the affinity mask, so call it only after any child thread pools are built.
pub(crate) fn pin_current_thread(cpu_core: usize, thread_desc: &str) {
    #[cfg(target_os = "linux")]
    {
        use agave_cpu_utils::{CpuId, set_cpu_affinity};

        match CpuId::new(cpu_core).and_then(|cpu| set_cpu_affinity(None, [cpu])) {
            Ok(()) => info!("Pinned {thread_desc} to CPU {cpu_core}"),
            Err(e) => error!("Failed to pin {thread_desc} to CPU {cpu_core}: {e:?}"),
        }
    }
    #[cfg(not(target_os = "linux"))]
    warn!("CPU pinning is only supported on Linux; not pinning {thread_desc} to CPU {cpu_core}");
}
