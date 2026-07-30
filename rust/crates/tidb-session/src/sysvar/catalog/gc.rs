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

//! Garbage collection of MVCC versions.
//!
//! `tidb_enable_gc_aware_memory_track` .. `tidb_server_memory_limit_gc_trigger` -- 12 entries, name-ordered.
//!
//! Captured from Go `pkg/sessionctx/variable/sysvar.go`'s `sysVars`. The
//! registry is one flat name-ordered slice because lookup binary-searches
//! it, so each subject file keeps its own entries sorted and `catalog.rs`
//! MERGES them; see the `sysvar` module doc.

use super::super::{SysVarDef, VarType};

pub(super) static ENTRIES: [SysVarDef; 12] = [
    SysVarDef {
        name: "tidb_enable_gc_aware_memory_track",
        scope: 1,
        value: "OFF",
        var_type: VarType::Bool,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_enable_gogc_tuner",
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
        name: "tidb_gc_concurrency",
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
        name: "tidb_gc_enable",
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
        name: "tidb_gc_life_time",
        scope: 1,
        value: "10m0s",
        var_type: VarType::Duration,
        read_only: false,
        allow_auto_value: false,
        min_value: 600000000000,
        max_value: 31536000000000000,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_gc_max_wait_time",
        scope: 1,
        value: "86400",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 600,
        max_value: 31536000,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_gc_run_interval",
        scope: 1,
        value: "10m0s",
        var_type: VarType::Duration,
        read_only: false,
        allow_auto_value: false,
        min_value: 600000000000,
        max_value: 31536000000000000,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_gc_scan_lock_mode",
        scope: 1,
        value: "LEGACY",
        var_type: VarType::Enum,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &["PHYSICAL", "LEGACY"],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_gogc_tuner_max_value",
        scope: 1,
        value: "500",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 10,
        max_value: 2147483647,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_gogc_tuner_min_value",
        scope: 1,
        value: "100",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 10,
        max_value: 2147483647,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_gogc_tuner_threshold",
        scope: 1,
        value: "0.6",
        var_type: VarType::Float,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 18446744073709551615,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_server_memory_limit_gc_trigger",
        scope: 5,
        value: "0.7",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
];
