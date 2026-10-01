// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Current Go-master additions not present in the original Rust catalog.

use super::super::{SysVarDef, VarType};

pub(super) static ENTRIES: [SysVarDef; 6] = [
    SysVarDef {
        name: "tidb_analyze_store_batch_size",
        global_config_name: "",
        scope: 3,
        value: "4",
        var_type: VarType::Unsigned,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 8,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_enable_connection_event_log",
        global_config_name: "",
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
        name: "tidb_enable_full_outer_join",
        global_config_name: "",
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
        name: "tidb_enable_txn_file",
        global_config_name: "",
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
        name: "tidb_plan_replayer_file_retention_time",
        global_config_name: "",
        scope: 1,
        value: "168h0m0s",
        var_type: VarType::Duration,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 31536000000000000,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_txn_file_min_mutation_size",
        global_config_name: "",
        scope: 3,
        value: "0",
        var_type: VarType::Unsigned,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 9223372036854775807,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
];
