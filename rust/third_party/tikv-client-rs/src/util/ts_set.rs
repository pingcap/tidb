// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::sync::RwLock;

/// Lazily allocated concurrent set of transaction timestamps.
#[derive(Default)]
pub struct TimestampSet {
    timestamps: RwLock<Option<HashSet<u64>>>,
}

impl TimestampSet {
    pub fn put(&self, timestamps: impl IntoIterator<Item = u64>) {
        let mut values = self.timestamps.write().unwrap();
        let values = values.get_or_insert_with(|| HashSet::with_capacity(5));
        values.extend(timestamps);
    }

    pub fn get_all(&self) -> Vec<u64> {
        self.timestamps
            .read()
            .unwrap()
            .as_ref()
            .map(|values| values.iter().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_set_deduplicates_and_returns_all_timestamps() {
        let set = TimestampSet::default();
        assert!(set.get_all().is_empty());
        set.put([3, 1, 3, 2]);
        let mut values = set.get_all();
        values.sort_unstable();
        assert_eq!(values, [1, 2, 3]);
    }
}
