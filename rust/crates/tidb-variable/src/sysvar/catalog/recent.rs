// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Variables this branch's Go registers that the original Rust catalog lacked.

use super::super::{SysVarDef, VarType};

pub(super) static ENTRIES: [SysVarDef; 2] = [
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
];
