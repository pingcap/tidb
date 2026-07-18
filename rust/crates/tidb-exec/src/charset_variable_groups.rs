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

//! Charset-variable groups from `pkg/sessionctx/vardef/sysvar.go`.
//!
//! TiDB's SET NAMES and SET CHARSET paths share source-owned ordered lists of
//! system-variable names. This leaf ports those lists and membership checks
//! only; SQL parsing/execution, collation validation, SessionVars mutation,
//! and charset conversion remain external.

/// Variables assigned by `SET NAMES`.
pub const SET_NAMES_VARIABLES: [&str; 3] = [
    "character_set_client",
    "character_set_connection",
    "character_set_results",
];

/// Variables assigned by `SET CHARSET`.
pub const SET_CHARSET_VARIABLES: [&str; 2] = ["character_set_client", "character_set_results"];

/// Returns whether `name` is one of the source `SET NAMES` variables.
#[must_use]
pub fn is_set_names_variable(name: &str) -> bool {
    SET_NAMES_VARIABLES.contains(&name)
}

/// Returns whether `name` is one of the source `SET CHARSET` variables.
#[must_use]
pub fn is_set_charset_variable(name: &str) -> bool {
    SET_CHARSET_VARIABLES.contains(&name)
}
