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

//! Go `br/pkg/streamhelper/spans/utils.go`: overlap and collapse predicates
//! over spans, plus the order-insensitive equality used by the package tests.

use std::cmp::Ordering;

use tidb_util::br_key_utils::compare_bytes_ext;
use tidb_util::redact;

use super::sorted::{Span, Valued};

/// Go `Overlaps`: whether two spans share any part.
#[must_use]
pub fn overlaps(a: &Span, b: &Span) -> bool {
    if b.end_key.is_empty() {
        return a.end_key.is_empty() || a.end_key.as_slice() > b.start_key.as_slice();
    }
    if a.end_key.is_empty() {
        return b.end_key.is_empty() || b.end_key.as_slice() > a.start_key.as_slice();
    }
    a.start_key < b.end_key && b.start_key < a.end_key
}

/// Go `logutil.StringifyRange(rng).String()`.
///
/// boundary: `br/pkg/logutil` is a zap-field package; this package uses only
/// this one rendering, so it is reproduced here instead of pulling the logging
/// helper across.
#[must_use]
pub fn stringify_range(rng: &Span) -> String {
    let end = if rng.end_key.is_empty() {
        "inf".to_owned()
    } else {
        redact::key(&rng.end_key)
    };
    format!("[{}, {})", redact::key(&rng.start_key), redact::value(&end))
}

/// Go `Collapse`: collapses overlapping or adjacent ranges.
///
/// ```text
/// collapse({[1, 4], [2, 8], [3, 9]}) == {[1, 9]}
/// collapse({[1, 3], [4, 7], [2, 3]}) == {[1, 3], [4, 7]}
/// ```
///
/// Go takes `(length int, getRange func(int) Span)` because its callers hold
/// spans inside heterogeneous slices; Rust takes the materialized slice that
/// Go's very first statement builds anyway.
#[must_use]
pub fn collapse(ranges: &[Span]) -> Vec<Span> {
    let mut frs = ranges.to_vec();
    frs.sort_by(|left, right| match left.start_key.cmp(&right.start_key) {
        Ordering::Equal => compare_bytes_ext(&left.end_key, true, &right.end_key, true),
        other => other,
    });

    let mut result = Vec::with_capacity(frs.len());
    let mut i = 0;
    while i < frs.len() {
        let mut item = frs[i].clone();
        loop {
            i += 1;
            if i >= frs.len() || (!item.end_key.is_empty() && frs[i].start_key > item.end_key) {
                break;
            }
            // Go: `len(item.EndKey) != 0 && item.EndKey < frs[i].EndKey || len(frs[i].EndKey) == 0`
            // (`&&` binds tighter than `||`).
            if (!item.end_key.is_empty() && item.end_key < frs[i].end_key)
                || frs[i].end_key.is_empty()
            {
                item.end_key = frs[i].end_key.clone();
            }
        }
        result.push(item);
    }
    result
}

/// Go `Full`: one span crossing the whole key space.
#[must_use]
pub fn full() -> Vec<Span> {
    vec![Span::default()]
}

/// Go `ValuedSetEquals`: whether two valued sets describe the same mapping,
/// regardless of how each side happens to be cut into spans.
///
/// Go sorts both argument slices in place; Rust sorts private copies, which no
/// caller can distinguish.
#[must_use]
pub fn valued_set_equals(xs: &[Valued], ys: &[Valued]) -> bool {
    if xs.is_empty() || ys.is_empty() {
        return xs.len() == ys.len();
    }

    let sort_key =
        |left: &Valued, right: &Valued| match left.key.start_key.cmp(&right.key.start_key) {
            Ordering::Equal => compare_bytes_ext(&left.key.end_key, true, &right.key.end_key, true),
            other => other,
        };
    let mut xs = xs.to_vec();
    let mut ys = ys.to_vec();
    xs.sort_by(sort_key);
    ys.sort_by(sort_key);

    let mut xi = 0usize;
    let mut yi = 0usize;

    loop {
        if xi >= xs.len() || yi >= ys.len() {
            return (xi >= xs.len()) == (yi >= ys.len());
        }
        if xs[xi].key.start_key != ys[yi].key.start_key {
            return false;
        }

        loop {
            if xi >= xs.len() || yi >= ys.len() {
                return (xi >= xs.len()) == (yi >= ys.len());
            }
            let x = xs[xi].clone();
            let y = ys[yi].clone();

            if x.value != y.value {
                return false;
            }

            let c = compare_bytes_ext(&x.key.end_key, true, &y.key.end_key, true);
            if c == Ordering::Equal {
                xi += 1;
                yi += 1;
                break;
            }
            if c == Ordering::Less {
                xi += 1;
                // If not an adjacent key, return false directly.
                if xi < xs.len()
                    && compare_bytes_ext(&x.key.end_key, true, &xs[xi].key.start_key, false)
                        != Ordering::Equal
                {
                    return false;
                }
            }
            if c == Ordering::Greater {
                yi += 1;
                if yi < ys.len()
                    && compare_bytes_ext(&y.key.end_key, true, &ys[yi].key.start_key, false)
                        != Ordering::Equal
                {
                    return false;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(start: &str, end: &str, value: u64) -> Valued {
        Valued::new(Span::new(start.as_bytes(), end.as_bytes()), value)
    }

    /// Go `TestValuedEquals` (`utils_test.go`).
    #[test]
    fn valued_equals() {
        struct Case {
            input_a: Vec<Valued>,
            input_b: Vec<Valued>,
            required: bool,
        }
        let cases = vec![
            Case {
                input_a: vec![s("0001", "0002", 3)],
                input_b: vec![s("0001", "0003", 3)],
                required: false,
            },
            Case {
                input_a: vec![s("0001", "0002", 3)],
                input_b: vec![s("0001", "0002", 3)],
                required: true,
            },
            Case {
                input_a: vec![s("0001", "0003", 3)],
                input_b: vec![s("0001", "0002", 3), s("0002", "0003", 3)],
                required: true,
            },
            Case {
                input_a: vec![s("0001", "0003", 4)],
                input_b: vec![s("0001", "0002", 3), s("0002", "0003", 3)],
                required: false,
            },
            Case {
                input_a: vec![s("0001", "0003", 3)],
                input_b: vec![s("0001", "0002", 4), s("0002", "0003", 3)],
                required: false,
            },
            Case {
                input_a: vec![s("0001", "0003", 3)],
                input_b: vec![s("0001", "0002", 3), s("0002", "0004", 3)],
                required: false,
            },
            Case {
                input_a: vec![s("", "0003", 3)],
                input_b: vec![s("0001", "0002", 3), s("0002", "0003", 3)],
                required: false,
            },
            Case {
                input_a: vec![s("0001", "", 1)],
                input_b: vec![s("0001", "0003", 1), s("0004", "", 1)],
                required: false,
            },
            Case {
                input_a: vec![s("0001", "0004", 1), s("0001", "0002", 1)],
                input_b: vec![s("0001", "0002", 1), s("0001", "0004", 1)],
                required: true,
            },
        ];

        for (index, case) in cases.iter().enumerate() {
            assert_eq!(
                case.required,
                valued_set_equals(&case.input_a, &case.input_b),
                "#{}",
                index + 1
            );
            assert_eq!(
                case.required,
                valued_set_equals(&case.input_b, &case.input_a),
                "#{}",
                index + 1
            );
        }
    }
}
