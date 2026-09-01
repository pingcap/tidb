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

//! Process CPU observation from `pkg/util/cpu`.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prometheus::{Gauge, Opts};

use crate::mathutil::ExponentialMovingAverage;

const OBSERVE_INTERVAL: Duration = Duration::from_millis(100);

static CPU_USAGE: AtomicU64 = AtomicU64::new(0);
static UNSUPPORTED: AtomicBool = AtomicBool::new(false);
static CPU_COUNT: AtomicUsize = AtomicUsize::new(0);

fn ema_cpu_usage_gauge() -> &'static Gauge {
    static GAUGE: OnceLock<Gauge> = OnceLock::new();
    GAUGE.get_or_init(|| {
        let gauge = Gauge::with_opts(
            Opts::new("ema_cpu_usage", "exponential moving average of CPU usage")
                .namespace("tidb")
                .subsystem("rm"),
        )
        .expect("resource-manager CPU gauge definition must be valid");
        prometheus::default_registry()
            .register(Box::new(gauge.clone()))
            .expect("resource-manager CPU gauge must register once");
        gauge
    })
}

/// Returns the observed process CPU usage and whether cgroup observation is
/// unsupported.
pub fn get_cpu_usage() -> (f64, bool) {
    (
        f64::from_bits(CPU_USAGE.load(Ordering::Relaxed)),
        UNSUPPORTED.load(Ordering::Relaxed),
    )
}

/// Installs the scheduler CPU count established by startup configuration,
/// automaxprocs, and the optional affinity list.
#[doc(hidden)]
pub fn install_cpu_count(configured: usize, affinity_count: usize) {
    let mut count = if configured > 0 {
        configured
    } else if let Some(environment) = std::env::var_os("GOMAXPROCS")
        .and_then(|value| value.to_str().and_then(|value| value.parse().ok()))
        .filter(|value: &usize| *value > 0)
    {
        environment
    } else {
        crate::cgroup::cpu_quota_to_gomaxprocs(1)
            .ok()
            .and_then(|(value, status)| {
                (status != crate::cgroup::CpuQuotaStatus::Undefined && value > 0)
                    .then_some(value as usize)
            })
            .or_else(|| crate::cgroup::logical_cpu_count().ok())
            .unwrap_or(1)
    };
    if affinity_count > 0 && affinity_count < count {
        count = affinity_count;
    }
    CPU_COUNT.store(count, Ordering::Relaxed);
}

/// Returns the scheduler parallelism visible to the current process.
pub fn get_cpu_count() -> usize {
    #[cfg(feature = "failpoints")]
    fail::fail_point!("mockNumCpu", |value| {
        return value
            .expect("mockNumCpu requires an integer return value")
            .parse()
            .expect("mockNumCpu return value must be an integer");
    });
    let count = CPU_COUNT.load(Ordering::Relaxed);
    if count > 0 {
        count
    } else {
        std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
    }
}

/// Observes process CPU usage every 100 milliseconds.
pub struct Observer {
    observation: Arc<Mutex<Observation>>,
    lifecycle: Mutex<ObserverLifecycle>,
}

struct ObserverLifecycle {
    stopped: bool,
    threads: Vec<ObserverThread>,
}

struct ObserverThread {
    stop: crossbeam_channel::Sender<()>,
    thread: JoinHandle<()>,
}

impl Observer {
    /// Creates a CPU observer.
    pub fn new() -> Self {
        Self {
            observation: Arc::new(Mutex::new(Observation {
                user_time: 0,
                system_time: 0,
                now: now_nanos(),
                cpu: ExponentialMovingAverage::new(0.95, 10),
            })),
            lifecycle: Mutex::new(ObserverLifecycle {
                stopped: false,
                threads: Vec::new(),
            }),
        }
    }

    /// Starts CPU observation.
    pub fn start(&self) {
        self.start_with(crate::cgroup::get_cgroup_cpu);
    }

    fn start_with(&self, get_cgroup_cpu: impl FnOnce() -> io::Result<crate::cgroup::CpuUsage>) {
        let _ = ema_cpu_usage_gauge();
        if let Err(error) = get_cgroup_cpu() {
            UNSUPPORTED.store(true, Ordering::Relaxed);
            tracing::error!(%error, "GetCgroupCPU");
            return;
        }
        let (stop, receiver) = crossbeam_channel::bounded(1);
        let keepalive = stop.clone();
        let observation = Arc::clone(&self.observation);
        let mut state = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped {
            return;
        }
        let thread = std::thread::spawn(move || observe_loop(receiver, keepalive, observation));
        state.threads.push(ObserverThread { stop, thread });
    }

    /// Stops CPU observation and waits for every observer thread.
    pub fn stop(&self) {
        let threads = {
            let mut state = self
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!state.stopped, "close of closed CPU observer");
            state.stopped = true;
            std::mem::take(&mut state.threads)
        };
        for ObserverThread { stop, thread } in threads {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }
}

fn observe_loop(
    stop: crossbeam_channel::Receiver<()>,
    _keepalive: crossbeam_channel::Sender<()>,
    observation: Arc<Mutex<Observation>>,
) {
    let ticker = crossbeam_channel::tick(OBSERVE_INTERVAL);
    loop {
        crossbeam_channel::select! {
            recv(stop) -> _ => return,
            recv(ticker) -> _ => {
                let mut observer = observation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let current = observer.observe();
                observer.cpu.add(current);
                let value = observer.cpu.get();
                CPU_USAGE.store(value.to_bits(), Ordering::Relaxed);
                ema_cpu_usage_gauge().set(value);
            }
        }
    }
}

struct Observation {
    user_time: i64,
    system_time: i64,
    now: i64,
    cpu: ExponentialMovingAverage,
}

impl Observation {
    fn observe(&mut self) -> f64 {
        let (user, system) = match get_cpu_time() {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(%error, "getCPUTime");
                (0, 0)
            }
        };
        let cgroup_cpu = crate::cgroup::get_cgroup_cpu().unwrap_or_default();
        let cpu_share = cgroup_cpu.cpu_shares();
        let now = now_nanos();
        let duration = (now - self.now) as f64;
        let user_time = user * 1_000_000;
        let system_time = system * 1_000_000;
        let user_rate = (user_time - self.user_time) as f64 / duration;
        let system_rate = (system_time - self.system_time) as f64 / duration;
        self.now = now;
        self.user_time = user_time;
        self.system_time = system_time;
        (system_rate + user_rate) / cpu_share
    }
}

fn now_nanos() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the Unix epoch");
    duration.as_nanos() as i64
}

/// Returns cumulative user and system CPU time in milliseconds.
#[cfg(unix)]
fn get_cpu_time() -> io::Result<(i64, i64)> {
    use nix::sys::resource::{getrusage, UsageWho};
    use nix::sys::time::TimeValLike;

    let usage = getrusage(UsageWho::RUSAGE_SELF).map_err(io::Error::from)?;
    Ok((
        usage.user_time().num_milliseconds(),
        usage.system_time().num_milliseconds(),
    ))
}

#[cfg(windows)]
fn get_cpu_time() -> io::Result<(i64, i64)> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    let success = unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    };
    if success == 0 {
        return Err(io::Error::last_os_error());
    }
    let millis = |value: FILETIME| {
        (((value.dwHighDateTime as u64) << 32) | value.dwLowDateTime as u64) / 10_000
    };
    Ok((millis(user) as i64, millis(kernel) as i64))
}

#[cfg(not(any(unix, windows)))]
fn get_cpu_time() -> io::Result<(i64, i64)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process CPU time is unavailable on this platform",
    ))
}
