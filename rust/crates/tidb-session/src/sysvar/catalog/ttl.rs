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

//! The `tidb_ttl_*` background row-expiry jobs.
//!
//! `log_throttle_queries_not_using_indexes` .. `tidb_ttl_scan_worker_count` -- 11 entries, name-ordered.
//!
//! Captured from Go `pkg/sessionctx/variable/sysvar.go`'s `sysVars`. The
//! registry is one flat name-ordered slice because lookup binary-searches
//! it, so each subject file keeps its own entries sorted and `catalog.rs`
//! MERGES them; see the `sysvar` module doc.

use super::super::{SysVarDef, VarType};

pub(super) static ENTRIES: [SysVarDef; 11] = [
    SysVarDef {
        name: "log_throttle_queries_not_using_indexes",
        scope: 1,
        value: "0",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_mpp_store_fail_ttl",
        scope: 3,
        value: "0s",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_delete_batch_size",
        scope: 1,
        value: "100",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 1,
        max_value: 10240,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_delete_rate_limit",
        scope: 1,
        value: "0",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 9223372036854775807,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_delete_worker_count",
        scope: 1,
        value: "4",
        var_type: VarType::Unsigned,
        read_only: false,
        allow_auto_value: false,
        min_value: 1,
        max_value: 256,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_job_enable",
        scope: 1,
        value: "ON",
        var_type: VarType::Bool,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_job_schedule_window_end_time",
        scope: 1,
        value: "23:59 +0000",
        var_type: VarType::Time,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_job_schedule_window_start_time",
        scope: 1,
        value: "00:00 +0000",
        var_type: VarType::Time,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_running_tasks",
        scope: 1,
        value: "-1",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: true,
        min_value: 1,
        max_value: 256,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_scan_batch_size",
        scope: 1,
        value: "500",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 1,
        max_value: 10240,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_ttl_scan_worker_count",
        scope: 1,
        value: "4",
        var_type: VarType::Unsigned,
        read_only: false,
        allow_auto_value: false,
        min_value: 1,
        max_value: 256,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
];
