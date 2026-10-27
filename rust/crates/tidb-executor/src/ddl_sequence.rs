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

//! `SHOW CREATE SEQUENCE`; the sequence DDL is `tidb_ddl_build::sequence`.

use crate::driver::SequenceDef;

/// The `SHOW CREATE SEQUENCE` / `SHOW CREATE TABLE` text for a sequence.
///
/// Captured verbatim from real TiDB for an option-free sequence:
/// ``CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806
/// increment by 1 cache 1000 nocycle ENGINE=InnoDB`` -- note the fixed clause
/// ORDER (start, minvalue, maxvalue, increment, cache, cycle), the lowercase
/// keywords, and that `SHOW CREATE TABLE` over a sequence prints this same
/// text rather than a `CREATE TABLE`.
#[must_use]
pub fn show_create_sequence(sequence: &SequenceDef) -> String {
    let info = sequence.allocator.info();
    let cache = if info.cache {
        format!("cache {}", info.cache_value)
    } else {
        "nocache".to_owned()
    };
    let cycle = if info.cycle { "cycle" } else { "nocycle" };
    let mut text = format!(
        "CREATE SEQUENCE `{}` start with {} minvalue {} maxvalue {} increment by {} {} {} ENGINE=InnoDB",
        sequence.name, info.start, info.min_value, info.max_value, info.increment, cache, cycle
    );
    if !sequence.comment.is_empty() {
        let mut escaped = String::with_capacity(sequence.comment.len());
        for character in sequence.comment.chars() {
            match character {
                '\0' => escaped.push_str("\\0"),
                '\'' => escaped.push_str("''"),
                '\n' => escaped.push_str("\\n"),
                '\r' => escaped.push_str("\\r"),
                other => escaped.push(other),
            }
        }
        text.push_str(" COMMENT='");
        text.push_str(&escaped);
        text.push('\'');
    }
    text
}
