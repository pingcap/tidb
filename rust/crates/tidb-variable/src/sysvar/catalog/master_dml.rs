// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Go master's transactional DML timeout beyond this branch's Go
//! (`sysvar.go`'s `TiDBDMLMaxExecutionTime`, from Go master `b82bed1eca2`).

use super::super::{SysVarDef, TypeFlag};

pub(super) static ENTRIES: [SysVarDef; 1] = [
    SysVarDef {
        name: "tidb_dml_max_execution_time",
        global_config_name: "",
        scope: 3,
        value: "0",
        var_type: TypeFlag::Unsigned,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 2147483647,
        possible_values: &[],
        auto_convert_negative_bool: false,
        validation: None,
        set_session: Some(crate::vars::SessionVars::set_session_tidb_dml_max_execution_time),
        set_global: None,
        get_session: None,
        get_global: None,
        get_state_value: None,
    },
];
