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

//! Source-backed tests for charset-variable groups.

use tidb_exec::charset_variable_groups::{
    is_set_charset_variable, is_set_names_variable, SET_CHARSET_VARIABLES, SET_NAMES_VARIABLES,
};

#[test]
fn charset_variable_groups_preserve_source_order_and_membership() {
    // Source: pkg/sessionctx/vardef/sysvar.go:20-36 and
    // pkg/executor/set_test.go:993-1035 (TestSetCollationAndCharset).
    assert_eq!(
        SET_NAMES_VARIABLES,
        [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ]
    );
    assert_eq!(
        SET_CHARSET_VARIABLES,
        ["character_set_client", "character_set_results"]
    );

    for name in SET_NAMES_VARIABLES {
        assert!(is_set_names_variable(name));
    }
    for name in SET_CHARSET_VARIABLES {
        assert!(is_set_charset_variable(name));
    }
    assert!(!is_set_names_variable("character_set_server"));
    assert!(!is_set_charset_variable("character_set_connection"));
}

#[test]
fn charset_variable_groups_keep_shared_client_results_boundary() {
    // Source: pkg/sessionctx/vardef/sysvar.go:20-36.
    for name in ["character_set_client", "character_set_results"] {
        assert!(is_set_names_variable(name));
        assert!(is_set_charset_variable(name));
    }
}
