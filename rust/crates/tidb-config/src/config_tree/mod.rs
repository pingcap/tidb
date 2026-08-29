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

//! Transcreation of Go `pkg/config/config.go`'s top-level `Config` tree.
//!
//! The tikvcfg-embedded fields use [`crate::tikvcfg`]; the actual TiKV
//! client is `tikv/client-rust`.

pub mod big_sections;
pub mod config;
mod errmsg;
pub mod helpers;
pub mod load;
pub mod log_instance;
pub mod marshal;
pub mod sections;

pub use big_sections::{Performance, Security, Status};
pub use config::{new_config, Config};
pub use helpers::{
    clone_conf, flatten_config_items, prepare_error_message_extensions, valid_max_allowed_packet,
    ConfReloadFunc, Cse, ErrorMessageExtension, TrxSummary, DEF_MAX_ALLOWED_PACKET,
};
pub use load::{
    initialize_config, is_all_removed_config_items, is_defined, InstanceConfigSection, LoadError,
};
pub use log_instance::{FileLogConfig, Instance, Log, RuntimeLogConfig};
pub use marshal::{AtomicBool, NullableBool, NB_FALSE, NB_TRUE, NB_UNSET};
pub use sections::{
    Experimental, IsolationRead, OpenTracing, OpenTracingReporter, OpenTracingSampler,
    PessimisticTxn, PlanCache, Plugin, PreparedPlanCache, ProxyProtocol, RuV2Config, Standby,
    StarterParams, TopSql, TracingConfiguration,
};

/// Validates a zap log level string (Go's final `Config.Valid` check via
/// `zap.AtomicLevel.UnmarshalText`).
pub fn parse_log_level(level: &str) -> Result<(), String> {
    match level.to_lowercase().as_str() {
        "debug" | "info" | "warn" | "warning" | "error" | "dpanic" | "panic" | "fatal" | "" => {
            Ok(())
        }
        other => Err(format!("unrecognized level: {other:?}")),
    }
}
