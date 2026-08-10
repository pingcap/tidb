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

//! `pkg/util/format`: stateful indentation and SQL display escaping.
//!
//! The formatter state machine is identical to `pkg/parser/format`, so this
//! package reuses its native Rust owner. `OutputFormat` is intentionally owned
//! here because the util package additionally doubles backslashes.

pub use tidb_datatype::{FlatFormatter, FormatFragment, Formatter, IndentFormatter};

/// Applies `pkg/util/format.OutputFormat` to text.
#[must_use]
pub fn output_format(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '\0' => output.push_str("\\0"),
            '\'' => output.push_str("''"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\\' => output.push_str("\\\\"),
            _ => output.push(character),
        }
    }
    output
}
