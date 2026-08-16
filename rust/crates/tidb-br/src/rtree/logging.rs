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

//! Go `br/pkg/rtree/logging.go`: how a range and a slice of ranges print.

use tidb_util::redact;

use super::rtree::KeyRange;

impl std::fmt::Display for KeyRange {
    /// Go `(KeyRange).String`:
    /// `fmt.Sprintf("[%s, %s)", redact.Key(start), redact.Key(end))`.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "[{}, {})",
            redact::key(&self.start_key),
            redact::key(&self.end_key)
        )
    }
}

/// Go `logutil.AbbreviatedStringers`: fewer than four elements print in full;
/// otherwise only the first and last survive, with a `(skip n)` marker between.
fn abbreviated_stringers<T: std::fmt::Display>(stringers: &[T]) -> Vec<String> {
    if stringers.len() < 4 {
        return stringers.iter().map(ToString::to_string).collect();
    }
    vec![
        stringers[0].to_string(),
        format!("(skip {})", stringers.len() - 2),
        stringers[stringers.len() - 1].to_string(),
    ]
}

/// Go `ZapRanges`: the zap field for logging a [`KeyRange`] slice.
///
/// boundary: Rust has no zap. The only thing that is ever observed about this
/// field — and the only thing `TestLogRanges` asserts — is the text a
/// `zapcore` console encoder renders for it, so that text is what this
/// function returns.
#[must_use]
pub fn zap_ranges(ranges: &[KeyRange]) -> String {
    let elements = abbreviated_stringers(ranges);
    let rendered: Vec<String> = elements
        .iter()
        .map(|element| format!("\"{}\"", element.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    format!("{{\"ranges\": [{}]}}", rendered.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestLogRanges` (`logging_test.go`).
    #[test]
    fn log_ranges() {
        let cases: &[(usize, &str)] = &[
            (0, r#"{"ranges": []}"#),
            (1, r#"{"ranges": ["[30, 31)"]}"#),
            (2, r#"{"ranges": ["[30, 31)", "[31, 32)"]}"#),
            (3, r#"{"ranges": ["[30, 31)", "[31, 32)", "[32, 33)"]}"#),
            (4, r#"{"ranges": ["[30, 31)", "(skip 2)", "[33, 34)"]}"#),
            (5, r#"{"ranges": ["[30, 31)", "(skip 3)", "[34, 35)"]}"#),
            (6, r#"{"ranges": ["[30, 31)", "(skip 4)", "[35, 36)"]}"#),
            (
                1024,
                r#"{"ranges": ["[30, 31)", "(skip 1022)", "[31303233, 31303234)"]}"#,
            ),
        ];

        for (count, expect) in cases {
            let ranges: Vec<KeyRange> = (0..*count)
                .map(|j| {
                    KeyRange::new(j.to_string().into_bytes(), (j + 1).to_string().into_bytes())
                })
                .collect();
            assert_eq!(*expect, zap_ranges(&ranges), "count={count}");
        }
    }
}
