// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transcreation of the behavioral core of Go
//! `pkg/util/memory/global_arbitrator.go`: the JSON file recorder that
//! persists [`RuntimeMemStateV1`] across restarts, plus the work-mode /
//! soft-limit text parsing.
//!
//! Not ported (per the rewrite's scope decisions): the Prometheus metric
//! reporters and the Go-runtime heap sampling (`SampleRuntimeMemStats`) —
//! observability glue with no behavioral contract — and the process-global
//! singleton wiring, which binds to server config (`ServerMemoryLimit`,
//! `config.GetGlobalConfig`) and belongs to the server crate's integration
//! layer.

use std::path::{Path, PathBuf};

use super::arbitrator::{
    ArbitratorWorkMode, RecordMemState, RuntimeMemStateV1, SoftLimitMode,
    ARBITRATOR_MODE_PRIORITY_NAME, ARBITRATOR_MODE_STANDARD_NAME,
    ARBITRATOR_SOFT_LIMIT_MODE_AUTO_NAME, ARBITRATOR_SOFT_LIMIT_MODE_DISABLE_NAME,
};

const MEM_STATE_VER: &str = "v1";
const MEM_STATE_STORE_NAME_PREFIX: &str = "mem-state.";
const MEM_STATE_STORE_NAME_SUFFIX: &str = ".json";

/// File-backed recorder of the runtime memory state (Go
/// `runtimeMemStateRecorder`).
pub struct RuntimeMemStateRecorder {
    base_dir: PathBuf,
    file_path: PathBuf,
}

/// Go `runtimeMemStateRecorderFilePath`.
pub fn runtime_mem_state_recorder_file_path(base_dir: &Path) -> PathBuf {
    base_dir.join(format!(
        "{MEM_STATE_STORE_NAME_PREFIX}{MEM_STATE_VER}{MEM_STATE_STORE_NAME_SUFFIX}"
    ))
}

impl RuntimeMemStateRecorder {
    /// Go `newMemStateRecorder`.
    pub fn new(base_dir: &Path) -> RuntimeMemStateRecorder {
        RuntimeMemStateRecorder {
            base_dir: base_dir.to_path_buf(),
            file_path: runtime_mem_state_recorder_file_path(base_dir),
        }
    }
}

impl RecordMemState for RuntimeMemStateRecorder {
    /// Go `Store`: write to a temp file, then rename atomically.
    fn store(&self, mem_state: &RuntimeMemStateV1) -> Result<(), String> {
        if !self.base_dir.exists() {
            std::fs::create_dir_all(&self.base_dir).map_err(|e| {
                format!(
                    "failed to create dir `{}`, err: {e}",
                    self.base_dir.display()
                )
            })?;
        }
        let buff = serde_json::to_vec(mem_state).map_err(|e| e.to_string())?;
        let tmp = self.base_dir.join(format!(
            ".mem_state.{}.json",
            std::process::id() as u64
                ^ (mem_state.magnif as u64) << 16
                ^ mem_state.pool_medium_cap as u64
        ));
        std::fs::write(&tmp, &buff).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &self.file_path).map_err(|e| e.to_string())
    }

    /// Go `Load`: scan the dir for a `mem-state.v1*.json` file.
    fn load(&self) -> Result<Option<RuntimeMemStateV1>, String> {
        let entries = std::fs::read_dir(&self.base_dir)
            .map_err(|e| format!("failed to read dir `{}`: {e}", self.base_dir.display()))?;
        let mut real_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(suffix) = name.strip_prefix(MEM_STATE_STORE_NAME_PREFIX) {
                let suffixes: Vec<&str> = suffix.split('.').collect();
                if suffixes.len() < 2 {
                    continue;
                }
                if suffixes[0] == MEM_STATE_VER {
                    real_path = Some(entry.path());
                    break;
                }
            }
        }
        let Some(real_path) = real_path else {
            return Ok(None);
        };
        let buff = std::fs::read(&real_path)
            .map_err(|e| format!("failed to read file `{}`: {e}", real_path.display()))?;
        let mem_state: RuntimeMemStateV1 = serde_json::from_slice(&buff)
            .map_err(|e| format!("failed to unmarshal mem state: {e}"))?;
        Ok(Some(mem_state))
    }
}

/// Parses the arbitrator soft-limit text (the switch in Go
/// `doSetGlobalMemArbitratorSoftLimit`): returns `(bytes, ratio, mode)`.
pub fn parse_soft_limit_text(text: &str) -> (i64, f64, SoftLimitMode) {
    match text {
        ARBITRATOR_SOFT_LIMIT_MODE_DISABLE_NAME => (0, 0.0, SoftLimitMode::Disable),
        ARBITRATOR_SOFT_LIMIT_MODE_AUTO_NAME => (0, 0.0, SoftLimitMode::Auto),
        _ => {
            if let Ok(int_value) = text.parse::<u64>() {
                if int_value as i64 > 1 {
                    return (int_value as i64, 0.0, SoftLimitMode::Specified);
                }
            }
            if let Ok(float_value) = text.parse::<f64>() {
                if float_value > 0.0 && float_value <= 1.0 {
                    return (0, float_value, SoftLimitMode::Specified);
                }
            }
            (0, 0.0, SoftLimitMode::Disable)
        }
    }
}

/// Parses the arbitrator work-mode text (Go
/// `SetGlobalMemArbitratorWorkMode`'s switch).
pub fn parse_work_mode_text(text: &str) -> ArbitratorWorkMode {
    match text {
        ARBITRATOR_MODE_STANDARD_NAME => ArbitratorWorkMode::Standard,
        ARBITRATOR_MODE_PRIORITY_NAME => ArbitratorWorkMode::Priority,
        _ => ArbitratorWorkMode::Disable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_round_trip() {
        let dir = std::env::temp_dir().join(format!("mem_arbitrator_test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let r = RuntimeMemStateRecorder::new(&dir);
        assert!(r.load().is_err()); // dir does not exist yet

        let s = RuntimeMemStateV1 {
            version: 1,
            last_risk: crate::memory::LastRisk {
                heap_alloc: 123,
                quota_alloc: 45,
            },
            magnif: 6100,
            pool_medium_cap: 400,
        };
        r.store(&s).unwrap();
        assert_eq!(r.load().unwrap(), Some(s));

        // Field names must match the Go JSON contract.
        let raw = std::fs::read_to_string(runtime_mem_state_recorder_file_path(&dir)).unwrap();
        for key in [
            "version",
            "last-risk",
            "heap",
            "quota",
            "magnif",
            "pool-medium-cap",
        ] {
            assert!(
                raw.contains(&format!("\"{key}\"")),
                "missing key {key} in {raw}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_texts() {
        assert_eq!(parse_soft_limit_text("0"), (0, 0.0, SoftLimitMode::Disable));
        assert_eq!(parse_soft_limit_text("auto"), (0, 0.0, SoftLimitMode::Auto));
        assert_eq!(
            parse_soft_limit_text("1024"),
            (1024, 0.0, SoftLimitMode::Specified)
        );
        assert_eq!(
            parse_soft_limit_text("0.8"),
            (0, 0.8, SoftLimitMode::Specified)
        );
        // 1 is not > 1 as an int, but parses as a valid ratio (0,1].
        assert_eq!(
            parse_soft_limit_text("1"),
            (0, 1.0, SoftLimitMode::Specified)
        );
        assert_eq!(
            parse_soft_limit_text("bogus"),
            (0, 0.0, SoftLimitMode::Disable)
        );

        assert_eq!(
            parse_work_mode_text("standard"),
            ArbitratorWorkMode::Standard
        );
        assert_eq!(
            parse_work_mode_text("priority"),
            ArbitratorWorkMode::Priority
        );
        assert_eq!(parse_work_mode_text("disable"), ArbitratorWorkMode::Disable);
        assert_eq!(parse_work_mode_text("?"), ArbitratorWorkMode::Disable);
    }
}
