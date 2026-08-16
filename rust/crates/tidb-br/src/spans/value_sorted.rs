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

//! Go `br/pkg/streamhelper/spans/value_sorted.go`: the same interval map with a
//! second index ordered by value, so the laggard spans can be found directly.

use std::collections::BTreeMap;

use super::sorted::{Value, Valued, ValuedFull};

/// Go `sortedByValueThenStartKey`'s ordering, expressed as the key of the
/// second map: `(Value, StartKey)`.
type ValueKey = (Value, Vec<u8>);

fn value_key(item: &Valued) -> ValueKey {
    (item.value, item.key.start_key.clone())
}

/// Go `spans.ValueSortedFull`: a [`ValuedFull`] with an extra index that
/// enables querying ranges by their value.
#[derive(Clone, Debug, Default)]
pub struct ValueSortedFull {
    /// Go's embedded `*ValuedFull`.
    inner: ValuedFull,
    /// Go's `valueIdx *btree.BTree` ordered by `(Value, StartKey)`.
    value_idx: BTreeMap<ValueKey, Valued>,
}

/// Go `Sorted`: takes ownership of a raw [`ValuedFull`] and wraps it.
#[must_use]
pub fn sorted(full: ValuedFull) -> ValueSortedFull {
    let mut value_idx = BTreeMap::new();
    full.traverse(|item| {
        value_idx.insert(value_key(item), item.clone());
        true
    });
    ValueSortedFull {
        inner: full,
        value_idx,
    }
}

impl ValueSortedFull {
    /// Go's promoted `(*ValuedFull).Traverse`.
    pub fn traverse(&self, visit: impl FnMut(&Valued) -> bool) {
        self.inner.traverse(visit);
    }

    /// Go `(*ValueSortedFull).Merge`.
    pub fn merge(&mut self, new_item: Valued) {
        self.merge_all(&[new_item]);
    }

    /// Go `(*ValueSortedFull).MergeAll`.
    pub fn merge_all(&mut self, new_items: &[Valued]) {
        let mut overlapped: Vec<Valued> = Vec::new();
        let mut inserted: Vec<Valued> = Vec::new();

        for item in new_items {
            overlapped.clear();
            inserted.clear();

            self.inner.overlapped(&item.key, &mut overlapped);
            self.inner
                .merge_with_overlap(item, &mut overlapped, Some(&mut inserted));

            for o in &overlapped {
                self.value_idx.remove(&value_key(o));
            }
            for i in &inserted {
                self.value_idx.insert(value_key(i), i.clone());
            }
        }
    }

    /// Go `(*ValueSortedFull).TraverseValuesLessThan`.
    pub fn traverse_values_less_than(&self, n: Value, mut action: impl FnMut(&Valued) -> bool) {
        for item in self.value_idx.range(..(n, Vec::new())).map(|(_, v)| v) {
            if !action(item) {
                return;
            }
        }
    }

    /// Go `(*ValueSortedFull).Min`.
    ///
    /// # Panics
    ///
    /// Panics when the index is empty, exactly as Go's `valueIdx.Min()` type
    /// assertion panics on the `nil` item it returns for an empty tree.
    #[must_use]
    pub fn min(&self) -> Valued {
        self.value_idx
            .values()
            .next()
            .expect("Min on an empty span tree")
            .clone()
    }

    /// Go `(*ValueSortedFull).MinValue`.
    ///
    /// # Panics
    ///
    /// See [`ValueSortedFull::min`].
    #[must_use]
    pub fn min_value(&self) -> Value {
        self.min().value
    }
}

/// Go `Debug` (declared in `utils.go`): prints the tree and its value index.
///
/// Go renders `[]Valued` through `%s`, which prints the elements separated by
/// single spaces inside square brackets; the Rust rendering matches.
pub fn debug(full: &ValueSortedFull) {
    let mut result = Vec::new();
    full.traverse(|v| {
        result.push(v.to_string());
        true
    });
    let mut idx = Vec::new();
    full.traverse_values_less_than(Value::MAX, |v| {
        idx.push(v.to_string());
        true
    });
    println!("[{}]\n\tidx = [{}]", result.join(" "), idx.join(" "));
}

#[cfg(test)]
mod tests {
    use super::super::sorted::Span;
    use super::super::utils::{full as full_span, valued_set_equals};
    use super::*;

    fn s(a: &str, b: &str) -> Span {
        Span::new(a.as_bytes(), b.as_bytes())
    }

    fn kv(span: Span, value: Value) -> Valued {
        Valued::new(span, value)
    }

    struct Case {
        input_sequence: Vec<Valued>,
        retain_less_than: Value,
        result: Vec<Valued>,
    }

    /// Go `TestSortedBasic` (`value_sorted_test.go`).
    #[test]
    fn sorted_basic() {
        let cases = [
            Case {
                input_sequence: vec![kv(s("0001", "0002"), 1), kv(s("0002", "0003"), 2)],
                retain_less_than: 10,
                result: vec![
                    kv(s("", "0001"), 0),
                    kv(s("0001", "0002"), 1),
                    kv(s("0002", "0003"), 2),
                    kv(s("0003", ""), 0),
                ],
            },
            Case {
                input_sequence: vec![
                    kv(s("0001", "0002"), 1),
                    kv(s("0002", "0003"), 2),
                    kv(s("0001", "0003"), 4),
                ],
                retain_less_than: 1,
                result: vec![kv(s("", "0001"), 0), kv(s("0003", ""), 0)],
            },
            Case {
                input_sequence: vec![
                    kv(s("0001", "0004"), 3),
                    kv(s("0004", "0008"), 5),
                    kv(s("0001", "0007"), 4),
                    kv(s("", "0002"), 2),
                ],
                retain_less_than: 5,
                result: vec![
                    kv(s("", "0001"), 2),
                    kv(s("0001", "0004"), 4),
                    kv(s("0008", ""), 0),
                ],
            },
            Case {
                input_sequence: vec![
                    kv(s("0001", "0004"), 3),
                    kv(s("0004", "0008"), 5),
                    kv(s("0001", "0007"), 4),
                    kv(s("", "0002"), 2),
                    kv(s("0001", "0004"), 5),
                    kv(s("0008", ""), 10),
                    kv(s("", "0001"), 20),
                ],
                retain_less_than: 11,
                result: vec![kv(s("0001", "0008"), 5), kv(s("0008", ""), 10)],
            },
        ];

        for (index, case) in cases.iter().enumerate() {
            let mut tree = sorted(ValuedFull::new_full_with(&full_span(), 0));
            for item in &case.input_sequence {
                tree.merge(item.clone());
                debug(&tree);
            }

            let mut result = Vec::new();
            tree.traverse_values_less_than(case.retain_less_than, |v| {
                result.push(v.clone());
                true
            });

            assert!(
                valued_set_equals(&result, &case.result),
                "#{}: {:?}\nvs\n{:?}",
                index + 1,
                result,
                case.result
            );
        }
    }
}
