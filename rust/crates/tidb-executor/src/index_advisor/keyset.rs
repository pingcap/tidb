// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/util/set`'s keyed set: items identified by `Key()`, listed in key
//! order so every walk of a set is deterministic.

use std::collections::BTreeMap;

/// Go `set.Key`.
pub(crate) trait Keyed {
    fn key(&self) -> String;
}

/// Go `set.Set[T]`.
#[derive(Clone, Debug)]
pub(crate) struct KeySet<T: Keyed + Clone> {
    items: BTreeMap<String, T>,
}

impl<T: Keyed + Clone> Default for KeySet<T> {
    fn default() -> Self {
        Self {
            items: BTreeMap::new(),
        }
    }
}

impl<T: Keyed + Clone> KeySet<T> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Go `ListToSet`.
    pub(crate) fn from_items(items: impl IntoIterator<Item = T>) -> Self {
        let mut set = Self::new();
        for item in items {
            set.add(item);
        }
        set
    }

    /// Go `Add`: an item with the same key replaces the stored one.
    pub(crate) fn add(&mut self, item: T) {
        self.items.insert(item.key(), item);
    }

    pub(crate) fn contains(&self, item: &T) -> bool {
        self.items.contains_key(&item.key())
    }

    pub(crate) fn remove(&mut self, item: &T) {
        self.items.remove(&item.key());
    }

    /// Go `ToList`: sorted by key.
    pub(crate) fn to_list(&self) -> Vec<T> {
        self.items.values().cloned().collect()
    }

    pub(crate) fn size(&self) -> usize {
        self.items.len()
    }

    /// Go `UnionSet`.
    pub(crate) fn union(sets: &[&Self]) -> Self {
        let mut union = Self::new();
        for set in sets {
            for item in set.items.values() {
                union.add(item.clone());
            }
        }
        union
    }

    /// Go `AndSet` of two sets.
    pub(crate) fn intersection(left: &Self, right: &Self) -> Self {
        let mut set = Self::new();
        for item in left.items.values() {
            if right.contains(item) {
                set.add(item.clone());
            }
        }
        set
    }

    /// Go `DiffSet`: the items of `left` that `right` lacks.
    pub(crate) fn difference(left: &Self, right: &Self) -> Self {
        let mut set = Self::new();
        for item in left.items.values() {
            if !right.contains(item) {
                set.add(item.clone());
            }
        }
        set
    }

    /// Go `CombSet`: every combination of `number_of_items` items, in the
    /// order the include-then-exclude recursion yields them.
    pub(crate) fn combinations(&self, number_of_items: usize) -> Vec<Self> {
        let list = self.to_list();
        let mut current = Self::new();
        comb_iterate(&list, &mut current, 0, number_of_items)
    }
}

fn comb_iterate<T: Keyed + Clone>(
    list: &[T],
    current: &mut KeySet<T>,
    depth: usize,
    number_of_items: usize,
) -> Vec<KeySet<T>> {
    if current.size() == number_of_items {
        return vec![current.clone()];
    }
    if depth == list.len() || current.size() > number_of_items {
        return Vec::new();
    }
    let mut result = Vec::new();
    current.add(list[depth].clone());
    result.extend(comb_iterate(list, current, depth + 1, number_of_items));
    current.remove(&list[depth]);
    result.extend(comb_iterate(list, current, depth + 1, number_of_items));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct Item(&'static str);

    impl Keyed for Item {
        fn key(&self) -> String {
            self.0.to_owned()
        }
    }

    fn keys(set: &KeySet<Item>) -> Vec<&'static str> {
        set.to_list().into_iter().map(|item| item.0).collect()
    }

    /// Go `set_test.go`: lists are key-ordered, and `CombSet({a,b,c}, 2)` is
    /// {ab, ac, bc}.
    #[test]
    fn sets_list_in_key_order_and_combine_like_go() {
        let set = KeySet::from_items([Item("c"), Item("a"), Item("b"), Item("a")]);
        assert_eq!(keys(&set), vec!["a", "b", "c"]);
        let combos: Vec<_> = set.combinations(2).iter().map(keys).collect();
        assert_eq!(combos, vec![vec!["a", "b"], vec!["a", "c"], vec!["b", "c"]]);
        assert_eq!(set.combinations(0).len(), 1);
        let other = KeySet::from_items([Item("b")]);
        assert_eq!(keys(&KeySet::difference(&set, &other)), vec!["a", "c"]);
        assert_eq!(keys(&KeySet::intersection(&set, &other)), vec!["b"]);
        assert_eq!(keys(&KeySet::union(&[&set, &other])), vec!["a", "b", "c"]);
    }
}
