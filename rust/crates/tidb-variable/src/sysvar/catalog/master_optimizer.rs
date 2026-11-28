// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Go master's optimizer variables beyond this branch's Go (`sysvar.go`'s
//! `TiDBOptRangeMaxCount`, from Go master `93a01d31f6d`), ported with
//! `compareEqOrIn`'s range-count threshold, and `TiDBEnableAdaptiveLimitScan`
//! (Go master `0b505ecc58b`), ported with its adaptive LIMIT controller.

use super::super::{SysVarDef, TypeFlag};

pub(super) static ENTRIES: [SysVarDef; 2] = [
    SysVarDef {
        name: "tidb_enable_adaptive_limit_scan",
        global_config_name: "",
        scope: 3,
        value: "OFF",
        var_type: TypeFlag::Bool,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
        validation: None,
        set_session: None,
        set_global: None,
        get_session: None,
        get_global: None,
        get_state_value: None,
    },
    SysVarDef {
        name: "tidb_opt_range_max_count",
        global_config_name: "",
        scope: 3,
        value: "1000",
        var_type: TypeFlag::Int,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 9223372036854775807,
        possible_values: &[],
        auto_convert_negative_bool: false,
        validation: None,
        set_session: Some(crate::vars::SessionVars::set_session_range_max_count),
        set_global: None,
        get_session: None,
        get_global: None,
        get_state_value: None,
    },
];
