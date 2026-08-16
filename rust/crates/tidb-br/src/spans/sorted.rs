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

//! Go `br/pkg/streamhelper/spans/sorted.go`: the start-key-ordered interval map
//! and its value-joining merge.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use tidb_util::br_key_utils::compare_bytes_ext;

use super::utils::{collapse, overlaps, stringify_range};

/// Go `spans.Value`: the value stored in the span tree.
pub type Value = u64;

/// Go `spans.Span = kv.KeyRange`.
///
/// boundary: `pkg/kv`'s `KeyRange` is a two-field byte-range struct. This crate
/// is a BR leaf, so it uses the identical range already landed in
/// `tidb-util`'s `br/pkg/utils/key.go` port rather than depending upward on
/// `tidb-txnkv`.
pub type Span = tidb_util::br_key_utils::KeyRange;

/// Go `join`: the upper bound of two values.
#[must_use]
pub const fn join(a: Value, b: Value) -> Value {
    if a > b {
        a
    } else {
        b
    }
}

/// Go `spans.Valued`: a span bound to a value, the entry type of the span tree.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Valued {
    /// Go `Valued.Key`.
    pub key: Span,
    /// Go `Valued.Value`.
    pub value: Value,
}

impl Valued {
    /// Builds one entry.
    #[must_use]
    pub fn new(key: Span, value: Value) -> Self {
        Self { key, value }
    }

    /// Go `Valued.Less`: ordering is by start key alone.
    #[must_use]
    pub fn less(&self, other: &Self) -> bool {
        self.key.start_key < other.key.start_key
    }

    /// Go `(Valued).Equals` (declared in `utils.go`).
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.value == other.value
            && self.key.start_key == other.key.start_key
            && self.key.end_key == other.key.end_key
    }
}

impl std::fmt::Display for Valued {
    /// Go `Valued.String`: `fmt.Sprintf("(%s, %d)", logutil.StringifyRange(...), v)`.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "({}, {})",
            stringify_range(&self.key),
            self.value
        )
    }
}

/// Go `spans.ValuedFull`: a set of non-overlapping valued ranges whose union is
/// the full key space it was created with.
#[derive(Clone, Debug, Default)]
pub struct ValuedFull {
    /// Go's `*btree.BTree` keyed by `Valued.Less`, i.e. by start key.
    inner: BTreeMap<Vec<u8>, Valued>,
}

impl ValuedFull {
    /// Go `NewFullWith`: creates a set over a subset of spans.
    #[must_use]
    pub fn new_full_with(init_spans: &[Span], init: Value) -> Self {
        let mut inner = BTreeMap::new();
        for r in collapse(init_spans) {
            inner.insert(
                r.start_key.clone(),
                Valued {
                    key: r,
                    value: init,
                },
            );
        }
        Self { inner }
    }

    /// Go `(*ValuedFull).Merge`: merges a new interval, joining the values of
    /// the overlapped parts.
    ///
    /// ```text
    /// |___________________________________________________________________________|
    /// ^-----------------^-----------------^-----------------^---------------------^
    /// |      c = 42     |      c = 43     |     c = 45      |      c = 41         |
    ///                        ^--------------------------^
    ///                  merge(|          c = 44          |)
    /// Would Give:
    /// |___________________________________________________________________________|
    /// ^-----------------^----^------------^-------------^---^---------------------^
    /// |      c = 42     | 43 |   c = 44   |     c = 45      |      c = 41         |
    ///                                     |-------------|
    ///                                     Unchanged, because 44 < 45.
    /// ```
    pub fn merge(&mut self, val: &Valued) {
        let mut overlaps_buf = Vec::with_capacity(16);
        self.overlapped(&val.key, &mut overlaps_buf);
        self.merge_with_overlap(val, &mut overlaps_buf, None);
    }

    /// Go `(*ValuedFull).Traverse`: visits every range in key order until the
    /// callback returns `false`.
    pub fn traverse(&self, mut visit: impl FnMut(&Valued) -> bool) {
        for item in self.inner.values() {
            if !visit(item) {
                return;
            }
        }
    }

    /// Go `(*ValuedFull).mergeWithOverlap`.
    pub(super) fn merge_with_overlap(
        &mut self,
        val: &Valued,
        overlapped: &mut [Valued],
        new_items: Option<&mut Vec<Valued>>,
    ) {
        // There isn't any range overlapping with the input range, perhaps the
        // input range is empty. Do nothing for this case.
        if overlapped.is_empty() {
            return;
        }

        for r in overlapped.iter() {
            // Assert all overlapped ranges are deleted.
            self.inner.remove(&r.key.start_key);
        }

        let mut collector = Collector {
            inner: &mut self.inner,
            new_items,
            initialized: false,
            collected: Valued::default(),
            merged_with: val.value,
        };

        let leftmost = overlapped[0].clone();
        if leftmost.key.start_key < val.key.start_key {
            collector.emit(
                Valued {
                    key: Span::new(leftmost.key.start_key, val.key.start_key.clone()),
                    value: leftmost.value,
                },
                true,
            );
            overlapped[0].key.start_key = val.key.start_key.clone();
        }

        let last = overlapped.len() - 1;
        let rightmost = overlapped[last].clone();
        let mut right_trail = None;
        if compare_bytes_ext(&rightmost.key.end_key, true, &val.key.end_key, true)
            == Ordering::Greater
        {
            right_trail = Some(Valued {
                key: Span::new(val.key.end_key.clone(), rightmost.key.end_key),
                value: rightmost.value,
            });
            overlapped[last].key.end_key = val.key.end_key.clone();
        }

        for rng in overlapped.iter() {
            collector.emit(rng.clone(), false);
        }

        if let Some(trail) = right_trail {
            collector.emit(trail, true);
        }

        collector.flush();
    }

    /// Go `(*ValuedFull).overlapped`: appends the ranges overlapping `k`.
    pub(super) fn overlapped(&self, k: &Span, result: &mut Vec<Valued>) {
        // Firstly, find whether there is an overlapped region with a lesser
        // start key.
        let first = self
            .inner
            .range(..=k.start_key.clone())
            .next_back()
            .map(|(_, item)| item.key.clone());
        let from = match first {
            Some(first) if overlaps(&first, k) => first.start_key,
            _ => k.start_key.clone(),
        };

        for item in self.inner.range(from..).map(|(_, item)| item) {
            if !overlaps(&item.key, k) {
                return;
            }
            result.push(item.clone());
        }
    }
}

/// The `collected`/`rightTrail`/`emitToCollected`/`flushCollected` closure set
/// of Go's `mergeWithOverlap`, expressed as one borrow-checkable value.
struct Collector<'a> {
    inner: &'a mut BTreeMap<Vec<u8>, Valued>,
    new_items: Option<&'a mut Vec<Valued>>,
    initialized: bool,
    collected: Valued,
    merged_with: Value,
}

impl Collector<'_> {
    /// Go `flushCollected`.
    fn flush(&mut self) {
        if self.initialized {
            self.inner
                .insert(self.collected.key.start_key.clone(), self.collected.clone());
            if let Some(items) = self.new_items.as_mut() {
                items.push(self.collected.clone());
            }
        }
    }

    /// Go `emitToCollected`.
    fn emit(&mut self, rng: Valued, standalone: bool) {
        let merged = if standalone {
            rng.value
        } else {
            join(self.merged_with, rng.value)
        };
        if !self.initialized {
            self.collected = rng;
            self.collected.value = merged;
            self.initialized = true;
            return;
        }
        if merged == self.collected.value
            && compare_bytes_ext(&self.collected.key.end_key, true, &rng.key.start_key, false)
                == Ordering::Equal
        {
            self.collected.key.end_key = rng.key.end_key;
        } else {
            self.flush();
            self.collected = Valued {
                key: rng.key,
                value: merged,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::utils::{full, valued_set_equals};
    use super::*;

    fn s(a: &str, b: &str) -> Span {
        Span::new(a.as_bytes(), b.as_bytes())
    }

    fn kv(span: Span, value: Value) -> Valued {
        Valued::new(span, value)
    }

    fn run(index: usize, ranges: &[Span], input_sequence: &[Valued], expect: &[Valued]) {
        let mut tree = ValuedFull::new_full_with(ranges, 0);
        for item in input_sequence {
            tree.merge(item);
            let mut step = Vec::new();
            tree.traverse(|v| {
                step.push(v.to_string());
                true
            });
            println!("{item} -> [{}]", step.join(" "));
        }

        let mut result = Vec::new();
        tree.traverse(|v| {
            result.push(v.clone());
            true
        });

        assert!(
            valued_set_equals(&result, expect),
            "#{}: {:?}\nvs\n{:?}",
            index + 1,
            result,
            expect
        );
    }

    /// Go `TestBasic` (`sorted_test.go`).
    #[test]
    fn basic() {
        let cases: Vec<(Vec<Valued>, Vec<Valued>)> = vec![
            (
                vec![kv(s("0001", "0002"), 1), kv(s("0002", "0003"), 2)],
                vec![
                    kv(s("", "0001"), 0),
                    kv(s("0001", "0002"), 1),
                    kv(s("0002", "0003"), 2),
                    kv(s("0003", ""), 0),
                ],
            ),
            (
                vec![
                    kv(s("0001", "0002"), 1),
                    kv(s("0002", "0003"), 2),
                    kv(s("0001", "0003"), 4),
                ],
                vec![
                    kv(s("", "0001"), 0),
                    kv(s("0001", "0003"), 4),
                    kv(s("0003", ""), 0),
                ],
            ),
            (
                vec![
                    kv(s("0001", "0004"), 3),
                    kv(s("0004", "0008"), 5),
                    kv(s("0001", "0007"), 4),
                    kv(s("", "0002"), 2),
                ],
                vec![
                    kv(s("", "0001"), 2),
                    kv(s("0001", "0004"), 4),
                    kv(s("0004", "0008"), 5),
                    kv(s("0008", ""), 0),
                ],
            ),
            (
                vec![
                    kv(s("0001", "0004"), 3),
                    kv(s("0004", "0008"), 5),
                    kv(s("0001", "0009"), 4),
                ],
                vec![
                    kv(s("", "0001"), 0),
                    kv(s("0001", "0004"), 4),
                    kv(s("0004", "0008"), 5),
                    kv(s("0008", "0009"), 4),
                    kv(s("0009", ""), 0),
                ],
            ),
        ];

        for (index, (input, expect)) in cases.iter().enumerate() {
            run(index, &full(), input, expect);
        }
    }

    /// Go `TestSubRange` (`sorted_test.go`).
    #[test]
    fn sub_range() {
        let cases: Vec<(Vec<Span>, Vec<Valued>, Vec<Valued>)> = vec![
            (
                vec![s("0001", "0004"), s("0008", "")],
                vec![
                    kv(s("0001", "0007"), 42),
                    kv(s("0000", "0009"), 41),
                    kv(s("0002", "0005"), 43),
                ],
                vec![
                    kv(s("0001", "0002"), 42),
                    kv(s("0002", "0004"), 43),
                    kv(s("0008", "0009"), 41),
                    kv(s("0009", ""), 0),
                ],
            ),
            (
                vec![s("0001", "0004"), s("0008", "")],
                vec![kv(s("", ""), 42)],
                vec![kv(s("0001", "0004"), 42), kv(s("0008", ""), 42)],
            ),
            (
                vec![s("0001", "0004"), s("0005", "0008")],
                vec![
                    kv(s("0001", "0002"), 42),
                    kv(s("0002", "0008"), 43),
                    kv(s("0004", "0007"), 45),
                    kv(s("0000", "00015"), 48),
                ],
                vec![
                    kv(s("0001", "00015"), 48),
                    kv(s("00015", "0002"), 42),
                    kv(s("0002", "0004"), 43),
                    kv(s("0005", "0007"), 45),
                    kv(s("0007", "0008"), 43),
                ],
            ),
            (
                vec![s("0001", "0004"), s("0005", "0008")],
                vec![
                    kv(s("0004", "0008"), 32),
                    kv(s("00041", "0007"), 33),
                    kv(s("0004", "00041"), 99999),
                    kv(s("0005", "0006"), 34),
                ],
                vec![
                    kv(s("0001", "0004"), 0),
                    kv(s("0005", "0006"), 34),
                    kv(s("0006", "0007"), 33),
                    kv(s("0007", "0008"), 32),
                ],
            ),
        ];

        for (index, (ranges, input, expect)) in cases.iter().enumerate() {
            run(index, ranges, input, expect);
        }
    }
}
