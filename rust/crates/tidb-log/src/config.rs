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

//! `pingcap/log`'s `Config`/`FileLogConfig` with the same TOML/JSON field
//! names.

use serde::{Deserialize, Serialize};

/// Default max size of a log file in MB (Go `defaultLogMaxSize`).
pub const DEFAULT_LOG_MAX_SIZE: i64 = 300;

/// File log related config (Go `FileLogConfig`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FileLogConfig {
    /// Log filename; empty disables file log.
    #[serde(rename = "filename")]
    pub filename: String,
    /// Max size for a single file, in MB.
    #[serde(rename = "max-size")]
    pub max_size: i64,
    /// Max log keep days; default never deletes.
    #[serde(rename = "max-days")]
    pub max_days: i64,
    /// Maximum number of old log files to retain.
    #[serde(rename = "max-backups")]
    pub max_backups: i64,
    /// Compression for rotated files: `gzip` or empty (disabled).
    #[serde(rename = "compression")]
    pub compression: String,
    /// Whether to use a buffered logger.
    #[serde(rename = "is-buffered")]
    pub is_buffered: bool,
    /// Buffer size when buffered.
    #[serde(rename = "buffer-size")]
    pub buffer_size: i64,
    /// Buffer flush interval (nanoseconds, Go `time.Duration`).
    #[serde(rename = "buffer-flush-interval")]
    pub buffer_flush_interval: i64,
}

/// Log related config (Go `Config`).
#[derive(Clone, PartialEq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Log level.
    #[serde(rename = "level")]
    pub level: String,
    /// Log format: `text` (default) or `json`.
    #[serde(rename = "format")]
    pub format: String,
    /// Disable automatic timestamps in output.
    #[serde(rename = "disable-timestamp")]
    pub disable_timestamp: bool,
    /// File log config.
    #[serde(rename = "file")]
    pub file: FileLogConfig,
    /// Development mode.
    #[serde(rename = "development")]
    pub development: bool,
    /// Stop annotating logs with file:line.
    #[serde(rename = "disable-caller")]
    pub disable_caller: bool,
    /// Disable automatic stacktrace capturing.
    #[serde(rename = "disable-stacktrace")]
    pub disable_stacktrace: bool,
    /// Stop annotating logs with the full verbose error message.
    #[serde(rename = "disable-error-verbose")]
    pub disable_error_verbose: bool,
    /// Per-second sampling: initial/thereafter (Go `*zap.SamplingConfig`).
    #[serde(rename = "sampling", skip_serializing_if = "Option::is_none")]
    pub sampling: Option<SamplingConfig>,
    /// Path for internal logger errors (`stderr` supported).
    #[serde(rename = "error-output-path")]
    pub error_output_path: String,
    /// Panic when a log write hangs this many seconds (0 = no timeout).
    #[serde(rename = "timeout")]
    pub timeout: i64,
}

/// Sampling strategy (the fields of `zap.SamplingConfig` the config uses).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SamplingConfig {
    /// Log every entry until `initial` per second.
    pub initial: i64,
    /// Then log every `thereafter`-th entry.
    pub thereafter: i64,
}
