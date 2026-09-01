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

//! Go `pkg/statistics/handle/lockstats`: the complete lock-state policy over
//! one storage transaction.
//!
//! The Go package reaches `mysql.stats_table_locked` and `mysql.stats_meta`
//! through a restricted SQL session. Rust has two physical stores (the
//! in-process catalog and TiKV), so [`StatsLockTransaction`] is that narrow
//! storage boundary. All branching, ordering, warning text, and delta merge
//! behavior remains here and is shared by both stores.

use std::collections::{BTreeMap, BTreeSet};

use crate::StatsLockTable;

/// The storage operations Go performs inside one `FlagWrapTxn` transaction.
pub trait StatsLockTransaction {
    /// Storage error returned without translation by the policy layer.
    type Error;

    /// `SELECT table_id FROM mysql.stats_table_locked`.
    fn query_locked_tables(&mut self) -> Result<BTreeSet<i64>, Self::Error>;

    /// Inserts the lock row and updates the matching `stats_meta.version` to
    /// this transaction's start timestamp. The meta update may affect no row.
    fn insert_lock_and_update_meta_version(&mut self, table_id: i64) -> Result<(), Self::Error>;

    /// Reads `(count, modify_count)` from one lock row; a missing row is
    /// `(0, 0)` in Go.
    fn lock_delta(&mut self, table_id: i64) -> Result<(i64, i64), Self::Error>;

    /// Merges a lock delta into `stats_meta`, setting its version to this
    /// transaction's start timestamp and clamping count at zero.
    fn update_meta_delta(
        &mut self,
        table_id: i64,
        count_delta: i64,
        modify_count_delta: i64,
    ) -> Result<(), Self::Error>;

    /// Deletes one row from `stats_table_locked`.
    fn delete_lock(&mut self, table_id: i64) -> Result<(), Self::Error>;
}

/// Go `GetLockedTables`: intersects a stored lock set with requested IDs.
#[must_use]
pub fn get_locked_tables(
    locked: &BTreeSet<i64>,
    table_ids: impl IntoIterator<Item = i64>,
) -> BTreeSet<i64> {
    table_ids
        .into_iter()
        .filter(|table_id| locked.contains(table_id))
        .collect()
}

/// Go `AddLockedTables`.
pub fn add_locked_tables<T: StatsLockTransaction>(
    transaction: &mut T,
    tables: &BTreeMap<i64, StatsLockTable>,
) -> Result<String, T::Error> {
    let locked = transaction.query_locked_tables()?;
    let ids = tables.iter().flat_map(|(&table_id, table)| {
        std::iter::once(table_id).chain(
            table
                .partition_info
                .iter()
                .flat_map(|partitions| partitions.keys().copied()),
        )
    });
    let locked = get_locked_tables(&locked, ids);
    let mut skipped = Vec::new();

    for (&table_id, table) in tables {
        if locked.contains(&table_id) {
            skipped.push(table.full_name.clone());
        } else {
            transaction.insert_lock_and_update_meta_version(table_id)?;
        }
        if let Some(partitions) = &table.partition_info {
            for &partition_id in partitions.keys() {
                if !locked.contains(&partition_id) {
                    transaction.insert_lock_and_update_meta_version(partition_id)?;
                }
            }
        }
    }
    Ok(skipped_tables_message(
        tables.len(),
        skipped,
        "locking",
        "locked",
    ))
}

/// Go `AddLockedPartitions`.
pub fn add_locked_partitions<T: StatsLockTransaction>(
    transaction: &mut T,
    table_id: i64,
    table_name: &str,
    partitions: &BTreeMap<i64, String>,
) -> Result<String, T::Error> {
    let locked = transaction.query_locked_tables()?;
    if locked.contains(&table_id) {
        return Ok(format!(
            "skip locking partitions of locked table: {table_name}"
        ));
    }
    let mut skipped = Vec::new();
    for (&partition_id, partition_name) in partitions {
        if locked.contains(&partition_id) {
            skipped.push(partition_name.clone());
        } else {
            transaction.insert_lock_and_update_meta_version(partition_id)?;
        }
    }
    Ok(skipped_partitions_message(
        partitions.len(),
        table_name,
        skipped,
        "locking",
        "locked",
    ))
}

/// Go `RemoveLockedTables`.
pub fn remove_locked_tables<T: StatsLockTransaction>(
    transaction: &mut T,
    tables: &BTreeMap<i64, StatsLockTable>,
) -> Result<String, T::Error> {
    let locked = transaction.query_locked_tables()?;
    let mut skipped = Vec::new();
    for (&table_id, table) in tables {
        // Go skips the WHOLE table target when its logical row is unlocked,
        // even if one of the target's partition rows is locked.
        if !locked.contains(&table_id) {
            skipped.push(table.full_name.clone());
            continue;
        }
        update_stats_and_unlock_table(transaction, table_id)?;
        if let Some(partitions) = &table.partition_info {
            for &partition_id in partitions.keys() {
                if locked.contains(&partition_id) {
                    update_stats_and_unlock_partition(transaction, partition_id, table_id)?;
                }
            }
        }
    }
    Ok(skipped_tables_message(
        tables.len(),
        skipped,
        "unlocking",
        "unlocked",
    ))
}

/// Go `RemoveLockedPartitions`.
pub fn remove_locked_partitions<T: StatsLockTransaction>(
    transaction: &mut T,
    table_id: i64,
    table_name: &str,
    partitions: &BTreeMap<i64, String>,
) -> Result<String, T::Error> {
    let locked = transaction.query_locked_tables()?;
    if locked.contains(&table_id) {
        return Ok(format!(
            "skip unlocking partitions of locked table: {table_name}"
        ));
    }
    let mut skipped = Vec::new();
    for (&partition_id, partition_name) in partitions {
        if locked.contains(&partition_id) {
            update_stats_and_unlock_partition(transaction, partition_id, table_id)?;
        } else {
            skipped.push(partition_name.clone());
        }
    }
    Ok(skipped_partitions_message(
        partitions.len(),
        table_name,
        skipped,
        "unlocking",
        "unlocked",
    ))
}

fn update_stats_and_unlock_table<T: StatsLockTransaction>(
    transaction: &mut T,
    table_id: i64,
) -> Result<(), T::Error> {
    let (count, modify_count) = transaction.lock_delta(table_id)?;
    transaction.update_meta_delta(table_id, count, modify_count)?;
    transaction.delete_lock(table_id)
}

fn update_stats_and_unlock_partition<T: StatsLockTransaction>(
    transaction: &mut T,
    partition_id: i64,
    table_id: i64,
) -> Result<(), T::Error> {
    let (count, modify_count) = transaction.lock_delta(partition_id)?;
    transaction.update_meta_delta(partition_id, count, modify_count)?;
    transaction.update_meta_delta(table_id, count, modify_count)?;
    transaction.delete_lock(partition_id)
}

fn skipped_tables_message(
    total: usize,
    mut skipped: Vec<String>,
    action: &str,
    status: &str,
) -> String {
    skipped.sort();
    if skipped.is_empty() {
        return String::new();
    }
    let names = skipped.join(", ");
    if total == 1 {
        return format!("skip {action} {status} table: {names}");
    }
    if total > skipped.len() {
        format!("skip {action} {status} tables: {names}, other tables {status} successfully")
    } else {
        format!("skip {action} {status} tables: {names}")
    }
}

fn skipped_partitions_message(
    total: usize,
    table_name: &str,
    mut skipped: Vec<String>,
    action: &str,
    status: &str,
) -> String {
    skipped.sort();
    if skipped.is_empty() {
        return String::new();
    }
    let names = skipped.join(", ");
    if total == 1 {
        return format!("skip {action} {status} partition of table {table_name}: {names}");
    }
    if total > skipped.len() {
        format!(
            "skip {action} {status} partitions of table {table_name}: {names}, other partitions {status} successfully"
        )
    } else {
        format!("skip {action} {status} partitions of table {table_name}: {names}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MemoryTransaction {
        locks: BTreeMap<i64, (i64, i64)>,
        meta: BTreeMap<i64, (u64, i64, i64)>,
        start_ts: u64,
    }

    impl StatsLockTransaction for MemoryTransaction {
        type Error = std::convert::Infallible;

        fn query_locked_tables(&mut self) -> Result<BTreeSet<i64>, Self::Error> {
            Ok(self.locks.keys().copied().collect())
        }

        fn insert_lock_and_update_meta_version(
            &mut self,
            table_id: i64,
        ) -> Result<(), Self::Error> {
            self.locks.entry(table_id).or_default();
            if let Some(meta) = self.meta.get_mut(&table_id) {
                meta.0 = self.start_ts;
            }
            Ok(())
        }

        fn lock_delta(&mut self, table_id: i64) -> Result<(i64, i64), Self::Error> {
            Ok(self.locks.get(&table_id).copied().unwrap_or_default())
        }

        fn update_meta_delta(
            &mut self,
            table_id: i64,
            count_delta: i64,
            modify_count_delta: i64,
        ) -> Result<(), Self::Error> {
            if let Some((version, count, modify_count)) = self.meta.get_mut(&table_id) {
                *version = self.start_ts;
                *count = (*count + count_delta).max(0);
                *modify_count += modify_count_delta;
            }
            Ok(())
        }

        fn delete_lock(&mut self, table_id: i64) -> Result<(), Self::Error> {
            self.locks.remove(&table_id);
            Ok(())
        }
    }

    fn table(name: &str, partitions: &[(i64, &str)]) -> StatsLockTable {
        StatsLockTable {
            full_name: name.to_owned(),
            partition_info: Some(
                partitions
                    .iter()
                    .map(|(id, name)| (*id, (*name).to_owned()))
                    .collect(),
            ),
        }
    }

    #[test]
    fn skipped_message_matrices_match_go() {
        assert_eq!(skipped_tables_message(3, vec![], "locking", "locked"), "");
        assert_eq!(
            skipped_tables_message(1, vec!["t1".to_owned()], "locking", "locked"),
            "skip locking locked table: t1"
        );
        assert_eq!(
            skipped_tables_message(
                4,
                vec!["t3".to_owned(), "t1".to_owned(), "t2".to_owned()],
                "locking",
                "locked",
            ),
            "skip locking locked tables: t1, t2, t3, other tables locked successfully"
        );
        assert_eq!(
            skipped_tables_message(
                4,
                vec![
                    "t4".to_owned(),
                    "t2".to_owned(),
                    "t1".to_owned(),
                    "t3".to_owned(),
                ],
                "unlocking",
                "unlocked",
            ),
            "skip unlocking unlocked tables: t1, t2, t3, t4"
        );

        assert_eq!(
            skipped_partitions_message(3, "test.t", vec![], "locking", "locked"),
            ""
        );
        assert_eq!(
            skipped_partitions_message(1, "test.t", vec!["p1".to_owned()], "locking", "locked",),
            "skip locking locked partition of table test.t: p1"
        );
        assert_eq!(
            skipped_partitions_message(
                4,
                "test.t",
                vec!["p3".to_owned(), "p1".to_owned(), "p2".to_owned()],
                "locking",
                "locked",
            ),
            "skip locking locked partitions of table test.t: p1, p2, p3, other partitions locked successfully"
        );
        assert_eq!(
            skipped_partitions_message(
                4,
                "test.t",
                vec![
                    "p4".to_owned(),
                    "p2".to_owned(),
                    "p1".to_owned(),
                    "p3".to_owned(),
                ],
                "unlocking",
                "unlocked",
            ),
            "skip unlocking unlocked partitions of table test.t: p1, p2, p3, p4"
        );
    }

    #[test]
    fn table_lock_and_unlock_match_go_skip_and_delta_rules() {
        let mut transaction = MemoryTransaction {
            locks: BTreeMap::from([(1, (0, 0))]),
            meta: BTreeMap::from([(1, (1, 10, 2)), (2, (1, 4, 1)), (4, (1, 3, 1))]),
            start_ts: 1000,
        };
        let tables = BTreeMap::from([
            (1, table("test.t1", &[(4, "p1")])),
            (2, table("test.t2", &[])),
        ]);
        assert_eq!(
            add_locked_tables(&mut transaction, &tables).unwrap(),
            "skip locking locked tables: test.t1, other tables locked successfully"
        );
        assert_eq!(
            transaction.locks.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 4]
        );
        transaction.locks.insert(4, (2, 5));
        assert_eq!(remove_locked_tables(&mut transaction, &tables).unwrap(), "");
        assert!(transaction.locks.is_empty());
        assert_eq!(transaction.meta[&1], (1000, 12, 7));
        assert_eq!(transaction.meta[&4], (1000, 5, 6));
    }

    #[test]
    fn partition_operations_match_go_whole_table_gate_and_stable_messages() {
        let partitions = BTreeMap::from([(2, "p2".to_owned()), (3, "p1".to_owned())]);
        let mut transaction = MemoryTransaction {
            locks: BTreeMap::from([(1, (0, 0))]),
            ..MemoryTransaction::default()
        };
        assert_eq!(
            add_locked_partitions(&mut transaction, 1, "test.t", &partitions).unwrap(),
            "skip locking partitions of locked table: test.t"
        );
        assert_eq!(
            remove_locked_partitions(&mut transaction, 1, "test.t", &partitions).unwrap(),
            "skip unlocking partitions of locked table: test.t"
        );
        transaction.locks = BTreeMap::from([(2, (0, 0))]);
        assert_eq!(
            add_locked_partitions(&mut transaction, 1, "test.t", &partitions).unwrap(),
            "skip locking locked partitions of table test.t: p2, other partitions locked successfully"
        );
        transaction.locks.clear();
        assert_eq!(
            remove_locked_partitions(&mut transaction, 1, "test.t", &partitions).unwrap(),
            "skip unlocking unlocked partitions of table test.t: p1, p2"
        );
    }

    #[test]
    fn unlocked_logical_table_skips_its_locked_partition() {
        let tables = BTreeMap::from([(1, table("test.t", &[(2, "p0")]))]);
        let mut transaction = MemoryTransaction {
            locks: BTreeMap::from([(2, (7, 9))]),
            ..MemoryTransaction::default()
        };
        assert_eq!(
            remove_locked_tables(&mut transaction, &tables).unwrap(),
            "skip unlocking unlocked table: test.t"
        );
        assert_eq!(transaction.locks[&2], (7, 9));
    }
}
