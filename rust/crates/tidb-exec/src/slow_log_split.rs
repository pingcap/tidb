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

//! Slow-log `field: value` splitting from `pkg/executor/slow_query.go`.
//!
//! The source parser treats ASCII letters/digits as key starts, keeps spaces
//! inside same-type brackets, recognizes nested `{}`/`[]` values, and rejects
//! malformed or unbalanced bracket sequences. Logging, slow-log I/O, time
//! conversion, and row/datums remain outside this string-only leaf.

fn is_ascii_letter_or_numeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

fn find_matched_right_bracket(line: &[u8], left_bracket_idx: usize) -> Option<usize> {
    let left_bracket = *line.get(left_bracket_idx)?;
    let right_bracket = match left_bracket {
        b'{' => b'}',
        b'[' => b']',
        _ => return None,
    };

    let mut current = left_bracket_idx;
    let mut left_bracket_count = 0usize;
    while current < line.len() {
        match line[current] {
            byte if byte == left_bracket => {
                left_bracket_count += 1;
                current += 1;
            }
            byte if byte == right_bracket => {
                left_bracket_count = left_bracket_count.checked_sub(1)?;
                if left_bracket_count > 0 {
                    current += 1;
                } else {
                    if current + 1 < line.len() && line[current + 1] != b' ' {
                        return None;
                    }
                    return Some(current);
                }
            }
            _ => current += 1,
        }
    }
    None
}

fn byte_slice_to_string(line: &[u8], start: usize, end: usize) -> String {
    String::from_utf8_lossy(&line[start..end]).into_owned()
}

/// Splits a slow-log line into fields and values.
///
/// `Some((fields, values))` mirrors the source's successful return. Malformed
/// brackets or unequal field/value counts return `None`, matching its `nil,
/// nil` error boundary after logging externally.
#[must_use]
pub fn split_by_colon(line: &str) -> Option<(Vec<String>, Vec<String>)> {
    let bytes = line.as_bytes();
    let mut fields = Vec::with_capacity(1);
    let mut values = Vec::with_capacity(1);
    let mut parse_key = true;
    let mut current = 0usize;

    while current < bytes.len() {
        if parse_key {
            while current < bytes.len() && !is_ascii_letter_or_numeric(bytes[current]) {
                current += 1;
            }
            let start = current;
            if current >= bytes.len() {
                break;
            }
            while current < bytes.len() && bytes[current] != b':' {
                current += 1;
            }
            fields.push(byte_slice_to_string(bytes, start, current));
            parse_key = false;
            current += 2; // source bypasses ": ".
            if current >= bytes.len() {
                values.push(String::new());
            }
        } else {
            let start = current;
            if current < bytes.len() && matches!(bytes[current], b'{' | b'[') {
                let right_bracket = find_matched_right_bracket(bytes, current)?;
                current = right_bracket + 1;
            } else {
                while current < bytes.len() && bytes[current] != b' ' {
                    current += 1;
                }
                // Empty value boundary: `Key: Key:`.
                if current > 0 && bytes[current - 1] == b':' {
                    values.push(String::new());
                    current = start;
                    parse_key = true;
                    continue;
                }
            }
            values.push(byte_slice_to_string(bytes, start, current.min(bytes.len())));
            parse_key = true;
        }
    }

    (fields.len() == values.len()).then_some((fields, values))
}
