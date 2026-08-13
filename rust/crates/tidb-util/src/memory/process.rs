use std::sync::{Arc, Mutex, OnceLock, Weak};

use super::{parse_soft_limit_text, parse_work_mode_text, MemArbitrator};

static PROCESS_ARBITRATOR: OnceLock<Mutex<Weak<MemArbitrator>>> = OnceLock::new();

fn process_arbitrator() -> &'static Mutex<Weak<MemArbitrator>> {
    PROCESS_ARBITRATOR.get_or_init(|| Mutex::new(Weak::new()))
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
        }
    }
}

/// Applies one SQL-visible memory setting to the running process authority.
///
/// An absent authority is normal for library-only sessions and is a no-op.
pub fn apply_process_memory_setting(name: &str, value: &str) -> Result<(), String> {
    validate_process_memory_setting(name, value)?;
    let arbitrator = process_arbitrator()
        .lock()
        .expect("process memory arbitrator lock poisoned")
        .upgrade();
    let Some(arbitrator) = arbitrator else {
        return Ok(());
    };
    match name.to_ascii_lowercase().as_str() {
        "tidb_mem_arbitrator_mode" => {
            arbitrator.set_work_mode(parse_work_mode_text(value));
        }
        "tidb_mem_arbitrator_soft_limit" => {
            let (bytes, ratio, mode) = parse_soft_limit_text(value);
            arbitrator.set_soft_limit(bytes, ratio, mode);
        }
        "tidb_server_memory_limit" => {
            arbitrator.set_limit(parse_server_memory_limit(value)?);
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
    Ok(())
}

/// Parses the SQL/config text form of `tidb_server_memory_limit`.
pub fn parse_server_memory_limit(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let total = (text.ends_with('%') || text == "0")
        .then(crate::cgroup::effective_memory_limit)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_server_memory_limit_observes_the_source_minimum() {
        assert_eq!(parse_server_memory_limit("1GiB").unwrap(), 1 << 30);
        assert_eq!(parse_server_memory_limit("1").unwrap(), 512 << 20);
        assert!(parse_server_memory_limit("100%").is_err());
    }
}
