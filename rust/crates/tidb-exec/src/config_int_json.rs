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

//! Integer SET CONFIG JSON rendering from `pkg/executor/set_config.go`.
//!
//! The source's ETInt branch renders boolean-flag values as JSON booleans and
//! other integers as decimal JSON numbers. Expression evaluation, key
//! validation/escaping, string/real/decimal branches, and HTTP config mutation
//! remain outside this dependency-closed scalar helper.

/// Renders an already-evaluated integer config value as a JSON object.
///
/// This preserves the source's key interpolation exactly: the key is inserted
/// without additional escaping, while boolean mode treats zero as false and
/// every non-zero value as true.
#[must_use]
pub fn integer_config_json(key: &str, value: i64, boolean_flag: bool) -> String {
    let rendered = if boolean_flag {
        if value == 0 {
            "false".to_owned()
        } else {
            "true".to_owned()
        }
    } else {
        value.to_string()
    };
    format!(r#"{{"{key}":{rendered}}}"#)
}
