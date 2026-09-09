// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cgroup CPU and memory-limit monitoring from `pkg/util/cgmon`.

#[cfg(any(target_os = "linux", test))]
use std::io;
#[cfg(any(target_os = "linux", test))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(target_os = "linux")]
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
#[cfg(target_os = "linux")]
use std::sync::Mutex;
#[cfg(any(target_os = "linux", test))]
use std::sync::OnceLock;
#[cfg(target_os = "linux")]
use std::thread::JoinHandle;
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(any(target_os = "linux", test))]
use prometheus::{Gauge, Opts};

#[cfg(target_os = "linux")]
const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

#[cfg(any(target_os = "linux", test))]
static LAST_CPU: AtomicUsize = AtomicUsize::new(0);
#[cfg(any(target_os = "linux", test))]
static LAST_MEMORY_LIMIT: AtomicU64 = AtomicU64::new(0);

#[cfg(any(target_os = "linux", test))]
fn registered_gauge(name: &str, help: &str) -> Gauge {
    let gauge = Gauge::with_opts(Opts::new(name, help).namespace("tidb").subsystem("server"))
        .expect("cgroup monitor gauge definition must be valid");
    prometheus::default_registry()
        .register(Box::new(gauge.clone()))
        .expect("cgroup monitor gauge must register once");
    gauge
}

#[cfg(any(target_os = "linux", test))]
fn max_procs_gauge() -> &'static Gauge {
    static GAUGE: OnceLock<Gauge> = OnceLock::new();
    GAUGE.get_or_init(|| registered_gauge("maxprocs", "The value of GOMAXPROCS."))
}

#[cfg(any(target_os = "linux", test))]
fn memory_limit_gauge() -> &'static Gauge {
    static GAUGE: OnceLock<Gauge> = OnceLock::new();
    GAUGE.get_or_init(|| registered_gauge("memory_quota_bytes", "The value of memory quota bytes."))
}

#[cfg(any(target_os = "linux", test))]
fn refresh_cpu(get_period_and_quota: impl FnOnce() -> io::Result<(i64, i64)>) -> io::Result<()> {
    let mut quota = super::cgroup::logical_cpu_count()?;
    let result = get_period_and_quota();
    if let Ok((period, cgroup_quota)) = result {
        if period > 0 && cgroup_quota > 0 {
            let ratio = cgroup_quota as f64 / period as f64;
            if ratio < quota as f64 {
                quota = ratio.ceil() as usize;
            }
        }
    }
    if quota != LAST_CPU.load(Ordering::Relaxed) {
        tracing::info!(quota, "set the maxprocs");
        max_procs_gauge().set(quota as f64);
        LAST_CPU.store(quota, Ordering::Relaxed);
    }
    result.map(|_| ())
}

#[cfg(any(target_os = "linux", test))]
fn refresh_memory(get_cgroup_limit: impl FnOnce() -> io::Result<u64>) -> io::Result<()> {
    let mut memory_limit = super::memory::host_memory_total()?;
    let result = get_cgroup_limit();
    if let Ok(cgroup_limit) = result {
        if cgroup_limit < memory_limit {
            memory_limit = cgroup_limit;
        }
    }
    if memory_limit != LAST_MEMORY_LIMIT.load(Ordering::Relaxed) {
        tracing::info!(memory_limit, "set the memory limit");
        memory_limit_gauge().set(memory_limit as f64);
        LAST_MEMORY_LIMIT.store(memory_limit, Ordering::Relaxed);
    }
    result.map(|_| ())
}

#[cfg(target_os = "linux")]
fn run(exit: std::sync::mpsc::Receiver<()>) {
    if let Err(error) = refresh_cpu(super::cgroup::get_cpu_period_and_quota) {
        tracing::warn!(%error, "failed to get cgroup cpu quota");
    }
    if let Err(error) = refresh_memory(super::cgroup::get_memory_limit) {
        tracing::warn!(%error, "failed to get cgroup memory limit");
    }
    loop {
        match exit.recv_timeout(REFRESH_INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {
                if let Err(error) = refresh_cpu(super::cgroup::get_cpu_period_and_quota) {
                    tracing::debug!(%error, "failed to get cgroup cpu quota");
                }
                if let Err(error) = refresh_memory(super::cgroup::get_memory_limit) {
                    tracing::debug!(%error, "failed to get cgroup memory limit");
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
struct MonitorThread {
    exit: Sender<()>,
    thread: JoinHandle<()>,
}

#[cfg(target_os = "linux")]
fn monitor_thread() -> &'static Mutex<Option<MonitorThread>> {
    static MONITOR: OnceLock<Mutex<Option<MonitorThread>>> = OnceLock::new();
    MONITOR.get_or_init(|| Mutex::new(None))
}

/// Starts cgroup monitoring. Repeated starts and non-Linux calls are no-ops.
pub fn start_cgroup_monitor() {
    #[cfg(target_os = "linux")]
    {
        let mut monitor = monitor_thread()
            .lock()
            .expect("cgroup monitor state poisoned");
        if monitor.is_some() {
            return;
        }
        let (exit, receiver) = mpsc::channel();
        let thread = std::thread::spawn(move || run(receiver));
        *monitor = Some(MonitorThread { exit, thread });
        tracing::info!("cgroup monitor started");
    }
}

/// Stops cgroup monitoring. Repeated stops and non-Linux calls are no-ops.
pub fn stop_cgroup_monitor() {
    #[cfg(target_os = "linux")]
    {
        let monitor = monitor_thread()
            .lock()
            .expect("cgroup monitor state poisoned")
            .take();
        let Some(MonitorThread { exit, thread }) = monitor else {
            return;
        };
        let _ = exit.send(());
        let _ = thread.join();
        tracing::info!("cgroup monitor stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_default_value_without_cgroup() {
        LAST_CPU.store(0, Ordering::Relaxed);
        LAST_MEMORY_LIMIT.store(0, Ordering::Relaxed);
        assert!(refresh_cpu(|| Err(io::Error::other("mock error"))).is_err());
        assert!(refresh_memory(|| Err(io::Error::other("mock error"))).is_err());
        assert_eq!(
            super::super::cgroup::logical_cpu_count().unwrap(),
            LAST_CPU.load(Ordering::Relaxed)
        );
        assert_eq!(
            super::super::memory::host_memory_total().unwrap(),
            LAST_MEMORY_LIMIT.load(Ordering::Relaxed)
        );
    }
}
