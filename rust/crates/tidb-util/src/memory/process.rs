use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[cfg(target_os = "linux")]
use std::fs;
use std::io;
#[cfg(any(target_os = "linux", test))]
use std::io::ErrorKind;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::process::Command;

use super::{parse_soft_limit_text, parse_work_mode_text, MemArbitrator, MemStats};

/// Process-wide `tidb_server_memory_limit` value.
pub static SERVER_MEMORY_LIMIT: AtomicU64 = AtomicU64::new(0);

/// Minimum session memory consumption eligible for server-limit killing.
pub static SERVER_MEMORY_LIMIT_SESS_MIN_SIZE: AtomicU64 = AtomicU64::new(128 << 20);

static USING_GLOBAL_MEM_ARBITRATION: AtomicBool = AtomicBool::new(false);

static PROCESS_ARBITRATOR: OnceLock<Mutex<Weak<MemArbitrator>>> = OnceLock::new();

fn process_arbitrator() -> &'static Mutex<Weak<MemArbitrator>> {
    PROCESS_ARBITRATOR.get_or_init(|| Mutex::new(Weak::new()))
}

/// Reads physical RAM through the same operating-system boundary Go's
/// `gopsutil.VirtualMemory().Total` uses for its memory authority.
#[cfg(target_os = "linux")]
fn host_memory_total() -> io::Result<u64> {
    let content = fs::read_to_string("/proc/meminfo")?;
    let kib = content
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "missing MemTotal"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "MemTotal overflows bytes"))
}

#[cfg(target_os = "macos")]
fn host_memory_total() -> io::Result<u64> {
    let output = Command::new("sysctl").args(["-n", "hw.memsize"]).output()?;
    if !output.status.success() {
        return Err(io::Error::other("sysctl hw.memsize failed"));
    }
    std::str::from_utf8(&output.stdout)
        .ok()
        .map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid hw.memsize"))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_memory_total() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "host memory discovery is not available on this platform",
    ))
}

/// Returns the RAM total visible to TiDB: a finite cgroup limit wins over
/// physical RAM, exactly as Go's memory hook does.
pub fn effective_memory_limit() -> io::Result<u64> {
    let physical = host_memory_total()?;
    Ok(select_effective_memory_limit(
        physical,
        crate::cgroup::get_memory_limit().unwrap_or_default(),
    ))
}

fn select_effective_memory_limit(physical: u64, cgroup_limit: u64) -> u64 {
    if cgroup_limit > 0 && cgroup_limit < physical {
        cgroup_limit
    } else {
        physical
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_process_rss_kib(status: &str) -> io::Result<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|kib| kib.checked_mul(1024))
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "missing VmRSS"))
}

/// Returns the current resident memory of this TiDB process.
///
/// Go feeds its memory controller from `runtime.ReadMemStats`. This RSS seam
/// is the native fallback used when Rust cannot read an allocator-specific
/// live heap counter; it intentionally measures the process rather than the
/// whole cgroup.
#[cfg(target_os = "linux")]
fn current_process_memory_usage() -> io::Result<u64> {
    parse_process_rss_kib(&fs::read_to_string("/proc/self/status")?)
}

#[cfg(target_os = "macos")]
fn current_process_memory_usage() -> io::Result<u64> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("ps failed to read process memory"));
    }
    std::str::from_utf8(&output.stdout)
        .ok()
        .map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|kib| kib.checked_mul(1024))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid process RSS"))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn current_process_memory_usage() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process memory discovery is not available on this platform",
    ))
}

/// Keeps this process's running memory arbitrator eligible for SQL runtime
/// setting updates.
pub struct ProcessArbitratorRegistration {
    installed: Weak<MemArbitrator>,
}

/// Registers the one arbitrator a TiDB server process owns.
pub fn install_process_arbitrator(
    arbitrator: &Arc<MemArbitrator>,
) -> ProcessArbitratorRegistration {
    SERVER_MEMORY_LIMIT.store(arbitrator.limit_u64(), SeqCst);
    USING_GLOBAL_MEM_ARBITRATION.store(
        arbitrator.work_mode() != super::ArbitratorWorkMode::Disable,
        SeqCst,
    );
    let installed = Arc::downgrade(arbitrator);
    *process_arbitrator()
        .lock()
        .expect("process memory arbitrator lock poisoned") = installed.clone();
    ProcessArbitratorRegistration { installed }
}

impl Drop for ProcessArbitratorRegistration {
    fn drop(&mut self) {
        let mut current = process_arbitrator()
            .lock()
            .expect("process memory arbitrator lock poisoned");
        if current.ptr_eq(&self.installed) {
            *current = Weak::new();
            USING_GLOBAL_MEM_ARBITRATION.store(false, SeqCst);
        }
    }
}

/// Applies one SQL-visible memory setting to the running process authority.
///
/// An absent authority is normal for library-only sessions and is a no-op.
pub fn apply_process_memory_setting(name: &str, value: &str) -> Result<(), String> {
    validate_process_memory_setting(name, value)?;
    if name.eq_ignore_ascii_case("tidb_server_memory_limit_sess_min_size") {
        let value = value
            .parse::<u64>()
            .map_err(|_| format!("invalid tidb_server_memory_limit_sess_min_size {value:?}"))?;
        SERVER_MEMORY_LIMIT_SESS_MIN_SIZE.store(value, SeqCst);
        return Ok(());
    }
    let server_memory_limit = name
        .eq_ignore_ascii_case("tidb_server_memory_limit")
        .then(|| parse_server_memory_limit(value))
        .transpose()?;
    if let Some(limit) = server_memory_limit {
        SERVER_MEMORY_LIMIT.store(limit, SeqCst);
    }
    let arbitrator = process_arbitrator()
        .lock()
        .expect("process memory arbitrator lock poisoned")
        .upgrade();
    let Some(arbitrator) = arbitrator else {
        return Ok(());
    };
    match name.to_ascii_lowercase().as_str() {
        "tidb_mem_arbitrator_mode" => {
            let mode = parse_work_mode_text(value);
            arbitrator.set_work_mode(mode);
            USING_GLOBAL_MEM_ARBITRATION.store(mode != super::ArbitratorWorkMode::Disable, SeqCst);
        }
        "tidb_mem_arbitrator_soft_limit" => {
            let (bytes, ratio, mode) = parse_soft_limit_text(value);
            arbitrator.set_soft_limit(bytes, ratio, mode);
        }
        "tidb_server_memory_limit" => {
            arbitrator.set_limit(server_memory_limit.expect("parsed above"));
        }
        _ => return Ok(()),
    }
    Ok(())
}

/// Validates a SQL-visible process-memory setting before the sysvar table
/// publishes it. This keeps an invalid value from becoming visible when a
/// server authority is not currently installed (for example in a library
/// session test).
pub fn validate_process_memory_setting(name: &str, value: &str) -> Result<(), String> {
    if name.eq_ignore_ascii_case("tidb_server_memory_limit") {
        parse_server_memory_limit(value)?;
    }
    if name.eq_ignore_ascii_case("tidb_server_memory_limit_sess_min_size") {
        value
            .parse::<u64>()
            .map_err(|_| format!("invalid tidb_server_memory_limit_sess_min_size {value:?}"))?;
    }
    Ok(())
}

/// Parses the SQL/config text form of `tidb_server_memory_limit`.
pub fn parse_server_memory_limit(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let total = (text.ends_with('%') || text == "0")
        .then(effective_memory_limit)
        .transpose()
        .map_err(|error| error.to_string())?;
    let parsed = if let Some(percent) = text.strip_suffix('%') {
        let percent = percent
            .parse::<u64>()
            .map_err(|_| format!("invalid tidb_server_memory_limit {text:?}"))?;
        if !(1..100).contains(&percent) {
            return Err(format!("invalid tidb_server_memory_limit {text:?}"));
        }
        total
            .ok_or_else(|| "host memory is required for a percentage limit".to_owned())?
            .saturating_mul(percent)
            / 100
    } else {
        let suffixes = [
            ("KiB", 1_u64 << 10),
            ("MiB", 1_u64 << 20),
            ("GiB", 1_u64 << 30),
            ("TiB", 1_u64 << 40),
            ("KB", 1_u64 << 10),
            ("MB", 1_u64 << 20),
            ("GB", 1_u64 << 30),
            ("TB", 1_u64 << 40),
        ];
        let (digits, scale) = suffixes
            .iter()
            .find_map(|(suffix, scale)| text.strip_suffix(suffix).map(|digits| (digits, *scale)))
            .unwrap_or((text, 1));
        digits
            .parse::<u64>()
            .ok()
            .and_then(|value| value.checked_mul(scale))
            .ok_or_else(|| format!("invalid tidb_server_memory_limit {text:?}"))?
    };
    let parsed = if parsed == 0 {
        total.ok_or_else(|| "host memory is required for a zero limit".to_owned())?
    } else {
        parsed.max(512 << 20)
    };
    i64::try_from(parsed)
        .map_err(|_| format!("tidb_server_memory_limit is too large: {text:?}"))?;
    Ok(parsed)
}

/// One live-heap sample from the running allocator:
/// `(allocated, active, resident)` in bytes.
///
/// Go feeds `HandleRuntimeStats` from `runtime.ReadMemStats`, whose
/// `HeapAlloc` is the live application allocation; on the jemalloc build the
/// equivalent counters come from this seam (through `tidb-hack`, the only
/// crate allowed to touch raw pointers). Returns `None` on builds without
/// the `jemalloc` feature, where callers keep their RSS-based fallback.
#[cfg(feature = "jemalloc")]
#[must_use]
pub fn allocator_live_heap_sample() -> Option<(i64, i64, i64)> {
    tidb_hack::allocator_stats::sample()
}

/// Non-jemalloc builds have no allocator-statistics seam: there is no finer
/// source than process RSS, so callers keep their legacy behaviour.
#[cfg(not(feature = "jemalloc"))]
#[must_use]
pub fn allocator_live_heap_sample() -> Option<(i64, i64, i64)> {
    None
}

/// Returns the process memory counters consumed by TiDB's memory controllers.
#[must_use]
pub fn read_mem_stats() -> MemStats {
    let rss = current_process_memory_usage()
        .ok()
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or(0);
    if let Some((allocated, active, resident)) = allocator_live_heap_sample() {
        return MemStats {
            heap_alloc: allocated,
            heap_inuse: active,
            total_free: (resident - active).max(0),
            mem_off_heap: (rss - resident).max(0),
            last_gc: 0,
        };
    }
    MemStats {
        heap_alloc: rss,
        heap_inuse: rss,
        ..MemStats::default()
    }
}

/// Reports whether the process is using the global memory arbitrator.
#[must_use]
pub fn using_global_mem_arbitration() -> bool {
    USING_GLOBAL_MEM_ARBITRATION.load(SeqCst)
}

/// Refreshes the installed global arbitrator from current process memory.
pub fn handle_global_mem_arbitrator_runtime() {
    let arbitrator = process_arbitrator()
        .lock()
        .expect("process memory arbitrator lock poisoned")
        .upgrade();
    if let Some(arbitrator) = arbitrator {
        arbitrator.handle_runtime_stats(read_mem_stats());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_limit_uses_the_smaller_finite_cgroup_value() {
        assert_eq!(select_effective_memory_limit(16, 0), 16);
        assert_eq!(select_effective_memory_limit(16, 32), 16);
        assert_eq!(select_effective_memory_limit(16, 8), 8);
    }

    #[test]
    fn process_rss_parser_uses_the_status_value_in_bytes() {
        assert_eq!(
            parse_process_rss_kib("Name:\ttidb-server\nVmRSS:\t  1234 kB\n").unwrap(),
            1234 << 10
        );
        assert!(parse_process_rss_kib("Name:\ttidb-server\n").is_err());
    }

    #[test]
    fn explicit_server_memory_limit_observes_the_source_minimum() {
        assert_eq!(parse_server_memory_limit("1GiB").unwrap(), 1 << 30);
        assert_eq!(parse_server_memory_limit("1").unwrap(), 512 << 20);
        assert!(parse_server_memory_limit("100%").is_err());
    }
}
