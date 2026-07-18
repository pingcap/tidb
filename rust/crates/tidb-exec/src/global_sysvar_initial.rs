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

//! Dynamic initial values for TiDB system variables.
//!
//! This is the dependency-closed policy from
//! `pkg/sessionctx/variable/sysvar.go`. The live global config, test-mode
//! singleton, and kernel-type selector are represented by explicit facts so
//! this leaf does not reach into session or process state.

/// Source names whose initial values have environment-sensitive overrides.
pub const ENABLE_ASYNC_COMMIT: &str = "tidb_enable_async_commit";
/// System variable controlling one-phase commit.
pub const ENABLE_1PC: &str = "tidb_enable_1pc";
/// System variable controlling out-of-memory action.
pub const MEM_OOM_ACTION: &str = "tidb_mem_oom_action";
/// System variable controlling automatic analysis.
pub const ENABLE_AUTO_ANALYZE: &str = "tidb_enable_auto_analyze";
/// System variable controlling row-format version.
pub const ROW_FORMAT_VERSION: &str = "tidb_row_format_version";
/// System variable controlling transaction assertion level.
pub const TXN_ASSERTION_LEVEL: &str = "tidb_txn_assertion_level";
/// System variable controlling the mutation checker.
pub const ENABLE_MUTATION_CHECKER: &str = "tidb_enable_mutation_checker";
/// System variable controlling pessimistic fair locking.
pub const PESSIMISTIC_TRANSACTION_FAIR_LOCKING: &str = "tidb_pessimistic_txn_fair_locking";

/// Source facts used to select a dynamic system-variable initial value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GlobalSysvarEnvironment {
    /// Whether the configured store is TiKV.
    pub store_is_tikv: bool,
    /// Whether TiDB's process-wide test flag is enabled.
    pub in_test: bool,
    /// Whether the next-generation kernel is active.
    pub next_gen: bool,
}

/// TiDB's source constants used by the override table.
pub const ON: &str = "ON";
/// Canonical disabled value.
pub const OFF: &str = "OFF";
/// Canonical out-of-memory logging action.
pub const OOM_ACTION_LOG: &str = "LOG";
/// Canonical row-format v2 value.
pub const ROW_FORMAT_V2: &str = "2";
/// Canonical fast assertion value.
pub const ASSERTION_FAST: &str = "FAST";
/// Canonical strict assertion value.
pub const ASSERTION_STRICT: &str = "STRICT";

/// Returns the environment-adjusted initial value for one system variable.
///
/// Unknown names and variables without an override retain `var_val` exactly,
/// matching the Go function's final `return varVal` path. The override order
/// is intentionally one switch arm per source case: TiKV store, test mode,
/// row-format install default, kernel assertion/fair-locking defaults, and
/// mutation-checker install default.
#[must_use]
pub fn global_system_variable_initial_value(
    var_name: &str,
    var_val: &str,
    environment: GlobalSysvarEnvironment,
) -> String {
    let mut value = var_val.to_owned();
    match var_name {
        ENABLE_ASYNC_COMMIT | ENABLE_1PC => {
            if environment.store_is_tikv {
                value = ON.to_owned();
            }
        }
        MEM_OOM_ACTION => {
            if environment.in_test {
                value = OOM_ACTION_LOG.to_owned();
            }
        }
        ENABLE_AUTO_ANALYZE => {
            if environment.in_test {
                value = OFF.to_owned();
            }
        }
        ROW_FORMAT_VERSION => value = ROW_FORMAT_V2.to_owned(),
        TXN_ASSERTION_LEVEL => {
            value = if environment.next_gen {
                ASSERTION_STRICT.to_owned()
            } else {
                ASSERTION_FAST.to_owned()
            };
        }
        ENABLE_MUTATION_CHECKER => value = ON.to_owned(),
        PESSIMISTIC_TRANSACTION_FAIR_LOCKING => {
            value = if environment.next_gen {
                OFF.to_owned()
            } else {
                ON.to_owned()
            };
        }
        _ => {}
    }
    value
}
