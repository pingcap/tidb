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

//! Configured SQL-error suffixes from Go `pkg/util/errmsg`.

use tidb_error::mysql::SqlError;

/// Go `Extend`: append the first matching configured suffix to an SQL error.
/// `None` is the safe Rust representation of Go's nil `*mysql.SQLError`.
pub fn extend(error: Option<&mut SqlError>) {
    let Some(error) = error else {
        return;
    };
    for extension in tidb_config::config_tree::config::get_error_message_extensions() {
        if extension.suffix.is_empty() {
            continue;
        }
        let Some(matcher) = &extension.regexp else {
            continue;
        };
        if matcher.is_match(&error.message) {
            error.message = format!(
                "{}, {}.",
                error.message.trim_end_matches('.'),
                extension.suffix.trim_end_matches('.')
            );
            return;
        }
    }
}
