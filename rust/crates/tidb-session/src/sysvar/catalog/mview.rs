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

//! Materialized-view session variables (`pkg/sessionctx/variable/sysvar.go`,
//! Go master `94a9cbedab`).

use super::super::{SysVarDef, VarType};

/// The materialized-view subject slice, sorted by name.
pub(crate) const ENTRIES: &[SysVarDef] = &[
    SysVarDef {
        name: "tidb_mview_enable",
        scope: 3,
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
        name: "tidb_mview_maintain_import_disk_quota",
        scope: 3,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_mview_maintain_import_threads",
        scope: 3,
        value: "0",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 256,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_mview_maintain_isolation_read_engines",
        scope: 3,
        value: "tikv,tiflash,tidb",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_mview_maintain_mem_quota",
        scope: 3,
        value: "2147483648",
        var_type: VarType::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: -1,
        max_value: 9223372036854775807,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
];
