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
pub trait Keyed {
    /// Go `Key`: the item's stable identity within the set.
    fn key(&self) -> String;
}

/// Go `set.Set[T]`.
#[derive(Clone, Debug)]
pub struct KeySet<T: Keyed + Clone> {
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `ListToSet`.
    pub fn from_items(items: impl IntoIterator<Item = T>) -> Self {
        let mut set = Self::new();
        for item in items {
            set.add(item);
        }
        set
    }

    /// Go `Add`: an item with the same key replaces the stored one.
    pub fn add(&mut self, item: T) {
        self.items.insert(item.key(), item);
    }

    pub fn contains(&self, item: &T) -> bool {
        self.items.contains_key(&item.key())
    }

    pub fn remove(&mut self, item: &T) {
        self.items.remove(&item.key());
    }

    /// Go `ToList`: sorted by key.
    pub fn to_list(&self) -> Vec<T> {
        self.iter().cloned().collect()
    }

    /// Borrows the items in the same key order as [`Self::to_list`].
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &T> {
        self.items.values()
    }

    pub fn size(&self) -> usize {
        self.items.len()
    }

    /// Go `UnionSet`.
    pub fn union(sets: &[&Self]) -> Self {
        let Some((last, earlier)) = sets.split_last() else {
            return Self::new();
        };
        // Visit the last representative first so that overwritten items are never cloned.
        let mut union = (*last).clone();
        for set in earlier.iter().rev() {
            for (key, item) in &set.items {
                if !union.items.contains_key(key) {
                    union.items.insert(key.clone(), item.clone());
                }
            }
        }
        union
    }

    /// Go `AndSet` of two sets.
    pub fn intersection(left: &Self, right: &Self) -> Self {
        let items = left
            .items
            .iter()
            .filter(|(key, _)| right.items.contains_key(*key))
            .map(|(key, item)| (key.clone(), item.clone()))
            .collect();
        Self { items }
    }

    /// Go `DiffSet`: the items of `left` that `right` lacks.
    pub fn difference(left: &Self, right: &Self) -> Self {
        let items = left
            .items
            .iter()
            .filter(|(key, _)| !right.items.contains_key(*key))
            .map(|(key, item)| (key.clone(), item.clone()))
            .collect();
        Self { items }
    }

    /// Go `CombSet`: every combination of `number_of_items` items, in the
    /// order the include-then-exclude recursion yields them.
    pub fn comb_set(&self, number_of_items: usize) -> Vec<Self> {
        match number_of_items {
            0 => return vec![Self::new()],
            count if count > self.size() => return Vec::new(),
            count if count == self.size() => return vec![self.clone()],
            _ => {}
        }
        let entries: Vec<_> = self.items.iter().collect();
        let mut current = Vec::with_capacity(number_of_items);
        let mut result = Vec::new();
        comb_iterate(&entries, &mut current, number_of_items, &mut result);
        result
    }
}

fn comb_iterate<'a, T: Keyed + Clone>(
    entries: &[(&'a String, &'a T)],
    current: &mut Vec<(&'a String, &'a T)>,
    remaining: usize,
    result: &mut Vec<KeySet<T>>,
) {
    if remaining == 0 {
        let items = current
            .iter()
            .map(|&(key, item)| (key.clone(), item.clone()))
            .collect();
        result.push(KeySet { items });
        return;
    }
    // This call always has enough entries to fill the remaining positions.
    for i in 0..=entries.len() - remaining {
        current.push(entries[i]);
        comb_iterate(&entries[i + 1..], current, remaining - 1, result);
        current.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};

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
        let combos: Vec<_> = set.comb_set(2).iter().map(keys).collect();
        assert_eq!(combos, vec![vec!["a", "b"], vec!["a", "c"], vec!["b", "c"]]);
        assert_eq!(set.comb_set(0).len(), 1);
        let other = KeySet::from_items([Item("b")]);
        assert_eq!(keys(&KeySet::difference(&set, &other)), vec!["a", "c"]);
        assert_eq!(keys(&KeySet::intersection(&set, &other)), vec!["b"]);
        assert_eq!(keys(&KeySet::union(&[&set, &other])), vec!["a", "b", "c"]);
    }

    #[test]
    fn set_operations_reuse_stored_keys_and_preserve_representatives() {
        #[derive(Clone)]
        struct Counted {
            name: &'static str,
            tag: u8,
            calls: Rc<Cell<usize>>,
        }

        impl Keyed for Counted {
            fn key(&self) -> String {
                self.calls.set(self.calls.get() + 1);
                self.name.to_owned()
            }
        }

        let calls = Rc::new(Cell::new(0));
        let item = |name, tag| Counted {
            name,
            tag,
            calls: calls.clone(),
        };
        let left = KeySet::from_items([item("b", 2), item("a", 1), item("c", 3)]);
        let right = KeySet::from_items([item("d", 5), item("b", 4)]);
        let tags = |set: &KeySet<Counted>| {
            set.to_list()
                .into_iter()
                .map(|item| item.tag)
                .collect::<Vec<_>>()
        };
        calls.set(0);

        assert_eq!(tags(&KeySet::union(&[&left, &right])), [1, 4, 3, 5]);
        assert_eq!(tags(&KeySet::intersection(&left, &right)), [2]);
        assert_eq!(tags(&KeySet::difference(&left, &right)), [1, 3]);
        assert_eq!(
            left.comb_set(2).iter().map(tags).collect::<Vec<_>>(),
            [vec![1, 2], vec![1, 3], vec![2, 3]]
        );
        assert_eq!(tags(&left.comb_set(3)[0]), [1, 2, 3]);
        assert!(left.comb_set(4).is_empty());
        assert_eq!(calls.get(), 0);
    }
}
