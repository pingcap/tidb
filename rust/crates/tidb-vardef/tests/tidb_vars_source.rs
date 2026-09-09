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

//! Source tests for `pkg/sessionctx/vardef/tidb_vars_test.go`.

use tidb_vardef::{
    is_mdl_enabled, is_read_only_var_in_next_gen,
    modes::{tidb_opt_enable_clustered, ExchangeCompressionMode},
    plan_replayer_file_retention_time, set_enable_mdl, set_plan_replayer_file_retention_time,
    tidb_vars::TIDB_DDL_DISK_QUOTA,
    tidb_vars::TIDB_DDL_ENABLE_FAST_REORG,
    tidb_vars::TIDB_DDL_REORG_MAX_WRITE_SPEED,
    tidb_vars::TIDB_ENABLE_DIST_TASK,
    tidb_vars::TIDB_ENABLE_MDL,
    tidb_vars::TIDB_MAX_DIST_TASK_NODES,
};

/// Direct Go `pkg/sessionctx/vardef` returns may be ignored without adding a
/// Rust-only discarded-return diagnostic.
#[test]
#[deny(unused_must_use)]
fn direct_vardef_source_returns_may_be_ignored() {
    plan_replayer_file_retention_time();
    is_mdl_enabled(false);
    is_read_only_var_in_next_gen("ordinary_variable");
    tidb_opt_enable_clustered("INT_ONLY");
    ExchangeCompressionMode::FAST.name();
    ExchangeCompressionMode::FAST.to_tipb_compression_value();
}

struct RestoreMdl(bool);

impl Drop for RestoreMdl {
    fn drop(&mut self) {
        set_enable_mdl(self.0);
    }
}

/// Source: `pkg/sessionctx/vardef/tidb_vars_test.go::TestIsMDLEnabledInNextGen`.
#[test]
fn mdl_is_always_enabled_in_nextgen_source() {
    let original = is_mdl_enabled(false);
    let _restore = RestoreMdl(original);

    set_enable_mdl(false);
    assert!(is_mdl_enabled(true));
    set_enable_mdl(true);
    assert!(is_mdl_enabled(true));
}

/// Source: `pkg/sessionctx/vardef/runtime_test.go::TestIsReadOnlyVarInNextGen`.
#[test]
fn read_only_vars_are_detected_in_nextgen_source() {
    assert!(!is_read_only_var_in_next_gen("abc"));
    assert!(is_read_only_var_in_next_gen(TIDB_ENABLE_MDL));
    assert!(is_read_only_var_in_next_gen("TIDB_ENABLE_METADATA_LOCK"));
    assert!(is_read_only_var_in_next_gen(TIDB_MAX_DIST_TASK_NODES));
    assert!(is_read_only_var_in_next_gen(TIDB_DDL_REORG_MAX_WRITE_SPEED));
    assert!(is_read_only_var_in_next_gen(TIDB_DDL_DISK_QUOTA));
    assert!(is_read_only_var_in_next_gen(TIDB_ENABLE_DIST_TASK));
    assert!(is_read_only_var_in_next_gen(TIDB_DDL_ENABLE_FAST_REORG));
}

struct RestorePlanReplayerRetention(i64);

impl Drop for RestorePlanReplayerRetention {
    fn drop(&mut self) {
        set_plan_replayer_file_retention_time(self.0);
    }
}

/// Source: `pkg/sessionctx/vardef/runtime.go::Set/GetPlanReplayerFileRetentionTime`.
#[test]
fn plan_replayer_file_retention_time_is_process_global_source() {
    let original = plan_replayer_file_retention_time();
    let _restore = RestorePlanReplayerRetention(original);
    assert_eq!(
        original,
        tidb_vardef::defaults::DEF_TIDB_PLAN_REPLAYER_FILE_RETENTION_TIME
    );

    let retention = 2 * 60 * 60 * 1_000_000_000_i64;
    set_plan_replayer_file_retention_time(retention);
    assert_eq!(plan_replayer_file_retention_time(), retention);
}
