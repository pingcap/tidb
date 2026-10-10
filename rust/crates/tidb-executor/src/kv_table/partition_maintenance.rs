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

use super::*;
use crate::partition_routing::{PartitionDef, PartitionKind};

impl KvTable {
    fn clear_partition_data(
        &mut self,
        physical_ids: &[i64],
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        let previous_read_partitions = self.read_partitions.replace(physical_ids.to_vec());
        let rows = self.scan_rows_with_handles_recomputed(&RowDecodeContext::for_write(ctx));
        self.read_partitions = previous_read_partitions;
        let rows = rows?;
        let zone = ctx.session_zone();
        for (handle, row) in rows {
            let physical_id = self.record_physical_id(&row, ctx)?;
            self.delete_index_entries(&row, &handle, physical_id, &zone)?;
        }

        for physical_id in physical_ids.iter().copied() {
            let (low, high) = get_table_handle_key_range(physical_id);
            let mut upper = high;
            upper.push(0);
            let mut iterator = self
                .store
                .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(upper)))
                .map_err(KvTableError::from)?;
            let mut keys = Vec::new();
            while iterator.valid() {
                keys.push(iterator.key().clone());
                iterator.next().map_err(KvTableError::from)?;
            }
            iterator.close();
            for key in keys {
                self.store.delete(key).map_err(KvTableError::from)?;
            }
        }
        Ok(())
    }

    /// Replaces selected physical partitions with empty ones while preserving
    /// the logical table and every unselected partition.
    pub(crate) fn truncate_partitions(
        &mut self,
        ordinals: &[usize],
        replacement_ids: &[i64],
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        debug_assert_eq!(ordinals.len(), replacement_ids.len());
        let old_ids = {
            let partition = self.partition.as_ref().expect("validated by DDL");
            ordinals
                .iter()
                .map(|ordinal| partition.definitions[*ordinal].id)
                .collect::<Vec<_>>()
        };

        self.clear_partition_data(&old_ids, ctx)?;

        let partition = self.partition.as_mut().expect("validated by DDL");
        for (ordinal, replacement_id) in ordinals.iter().zip(replacement_ids) {
            partition.definitions[*ordinal].id = *replacement_id;
        }
        // Catalog-owned tables normally have no read restriction, but keeping
        // a restriction coherent costs nothing and prevents a narrowed clone
        // from retaining retired physical IDs if this operation is reused.
        self.read_partitions = self.read_partitions.take().map(|ids| {
            ids.into_iter()
                .map(|id| {
                    old_ids
                        .iter()
                        .position(|old_id| *old_id == id)
                        .map_or(id, |index| replacement_ids[index])
                })
                .collect()
        });
        ctx.staged_writes().mark_dirty(self.table_id);
        Ok(())
    }

    pub(crate) fn drop_partitions(
        &mut self,
        ordinals: &[usize],
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        let old_ids = {
            let partition = self.partition.as_ref().expect("validated by DDL");
            ordinals
                .iter()
                .map(|ordinal| partition.definitions[*ordinal].id)
                .collect::<Vec<_>>()
        };
        self.clear_partition_data(&old_ids, ctx)?;

        let partition = self.partition.as_mut().expect("validated by DDL");
        let mut next = 0usize;
        let remap = (0..partition.definitions.len())
            .map(|old| {
                if ordinals.contains(&old) {
                    None
                } else {
                    let new = next;
                    next += 1;
                    Some(new)
                }
            })
            .collect::<Vec<_>>();
        partition.definitions = partition
            .definitions
            .drain(..)
            .enumerate()
            .filter_map(|(old, definition)| remap[old].map(|_| definition))
            .collect();
        match &mut partition.kind {
            // NONE keeps no per-partition structure beside the definitions
            // that were just remapped.
            PartitionKind::None => {}
            PartitionKind::Range { less_than, .. } => {
                *less_than = less_than
                    .drain(..)
                    .enumerate()
                    .filter_map(|(old, bound)| remap[old].map(|_| bound))
                    .collect();
            }
            PartitionKind::RangeColumns { less_than, .. } => {
                *less_than = less_than
                    .drain(..)
                    .enumerate()
                    .filter_map(|(old, bound)| remap[old].map(|_| bound))
                    .collect();
            }
            PartitionKind::List {
                values,
                null_partition,
                default_partition,
                ..
            } => {
                *values = values
                    .drain(..)
                    .filter_map(|(value, old)| remap[old].map(|new| (value, new)))
                    .collect();
                *null_partition = null_partition.and_then(|old| remap[old]);
                *default_partition = default_partition.and_then(|old| remap[old]);
            }
            PartitionKind::ListColumns {
                values,
                keys,
                default_partition,
                ..
            } => {
                *values = values
                    .drain(..)
                    .filter_map(|(value, old)| remap[old].map(|new| (value, new)))
                    .collect();
                keys.retain(|_, old| {
                    let Some(new) = remap[*old] else {
                        return false;
                    };
                    *old = new;
                    true
                });
                *default_partition = default_partition.and_then(|old| remap[old]);
            }
            PartitionKind::Hash | PartitionKind::Key => {
                unreachable!("DDL allows DROP PARTITION only for RANGE/LIST")
            }
        }
        if let Some(ids) = &mut self.read_partitions {
            ids.retain(|id| !old_ids.contains(id));
        }
        ctx.staged_writes().mark_dirty(self.table_id);
        Ok(())
    }

    pub(crate) fn append_partitions(
        &mut self,
        definitions: Vec<PartitionDef>,
        added_kind: PartitionKind,
        ctx: &crate::StmtContext,
    ) {
        let partition = self.partition.as_mut().expect("validated by DDL");
        let offset = partition.definitions.len();
        match (&mut partition.kind, added_kind) {
            (
                PartitionKind::Range {
                    less_than,
                    unsigned,
                },
                PartitionKind::Range {
                    less_than: added_bounds,
                    unsigned: added_unsigned,
                },
            ) => {
                debug_assert_eq!(*unsigned, added_unsigned);
                less_than.extend(added_bounds);
            }
            (
                PartitionKind::RangeColumns {
                    less_than,
                    field_types,
                },
                PartitionKind::RangeColumns {
                    less_than: added_bounds,
                    field_types: added_types,
                },
            ) => {
                debug_assert_eq!(*field_types, added_types);
                less_than.extend(added_bounds);
            }
            (
                PartitionKind::List {
                    values,
                    null_partition,
                    default_partition,
                    unsigned,
                },
                PartitionKind::List {
                    values: added_values,
                    null_partition: added_null,
                    default_partition: added_default,
                    unsigned: added_unsigned,
                },
            ) => {
                debug_assert_eq!(*unsigned, added_unsigned);
                values.extend(
                    added_values
                        .into_iter()
                        .map(|(value, owner)| (value, owner + offset)),
                );
                if let Some(owner) = added_null {
                    *null_partition = Some(owner + offset);
                }
                if let Some(owner) = added_default {
                    *default_partition = Some(owner + offset);
                }
            }
            (
                PartitionKind::ListColumns {
                    values,
                    keys,
                    default_partition,
                    field_types,
                },
                PartitionKind::ListColumns {
                    values: added_values,
                    keys: added_keys,
                    default_partition: added_default,
                    field_types: added_types,
                },
            ) => {
                debug_assert_eq!(*field_types, added_types);
                values.extend(
                    added_values
                        .into_iter()
                        .map(|(value, owner)| (value, owner + offset)),
                );
                keys.extend(
                    added_keys
                        .into_iter()
                        .map(|(key, owner)| (key, owner + offset)),
                );
                if let Some(owner) = added_default {
                    *default_partition = Some(owner + offset);
                }
            }
            _ => unreachable!("DDL folds added definitions with the existing partition method"),
        }
        partition.definitions.extend(definitions);
        ctx.staged_writes().mark_dirty(self.table_id);
    }
}

impl KvTable {
    /// Rebuilds a HASH table to `new_ids.len()` partitions, redistributing
    /// every row by the new modulus. Go `hashPartitionManagement`
    /// (`pkg/ddl/executor.go:2782-2814`) reaches the same observable state
    /// through `ReorganizePartitions`: all rows are re-hashed, the
    /// definitions are renumbered `p0..`, and the old physical tables
    /// retire. COALESCE shrinks to this count; ADD PARTITION PARTITIONS n
    /// grows to it.
    pub(crate) fn rehash_hash_partitions(
        &mut self,
        definitions: Vec<PartitionDef>,
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        let old_ids: Vec<i64> = {
            let partition = self.partition.as_ref().expect("validated by DDL");
            partition.definitions.iter().map(|d| d.id).collect()
        };

        // Every row, with the handle it must keep.
        let previous_read_partitions = self.read_partitions.replace(old_ids.clone());
        let rows = self.scan_rows_with_handles_recomputed(&RowDecodeContext::for_write(ctx));
        self.read_partitions = previous_read_partitions;
        let rows = rows?;

        // Retire every old physical table: data and index entries.
        self.clear_partition_data(&old_ids, ctx)?;

        // The definitions Go's `buildHashPartitionDefinitions` built.
        self.partition
            .as_mut()
            .expect("validated by DDL")
            .definitions = definitions;
        self.read_partitions = None;

        // Re-insert every row through the normal write path, which routes it
        // to `handle % new_count` and rebuilds the index entries. Int
        // handles are preserved (`insert_row_with_row_id`); a clustered
        // common handle is recomputed from the row itself.
        for (handle, row) in rows {
            match handle.int_value() {
                Some(row_id) => {
                    self.insert_row_with_row_id(&row, Some(row_id), 0, ctx)?;
                }
                None => {
                    self.insert_row(&row, ctx)?;
                }
            }
        }
        ctx.staged_writes().mark_dirty(self.table_id);
        Ok(())
    }

    /// Go `checkExchangePartitionRecordValidation`'s partition condition for
    /// one row of the table being exchanged in: whether the row does NOT
    /// belong to the partition at `ordinal`.
    ///
    /// HASH is Go's literal `mod(expr, num) != index`, which refuses a
    /// negative value that routing would place by its absolute value, and a
    /// NULL anywhere but the first partition. The other methods compare the
    /// row's route, and a row no partition accepts does not match.
    pub(crate) fn exchange_row_mismatches(
        &self,
        ordinal: usize,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<bool, KvTableError> {
        let partition = self.partition.as_ref().expect("validated by DDL");
        if let PartitionKind::Hash = partition.kind {
            let num = partition.num();
            if num == 1 {
                return Ok(false);
            }
            let value = crate::generated_column::eval_over_dependencies(
                &partition.expr,
                &partition.dependencies,
                &*self.columns,
                row,
                ctx,
            )
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
            let remainder = match value {
                Datum::Null => return Ok(ordinal != 0),
                Datum::UInt(value) => (value % num) as i64,
                Datum::Int(value) => value % num as i64,
                other => {
                    let index = crate::partition_routing::hash_partition_index(&other, num)
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    index as i64
                }
            };
            return Ok(remainder != ordinal as i64);
        }
        match self.record_physical_id(row, ctx) {
            Ok(physical_id) => Ok(physical_id != partition.definitions[ordinal].id),
            Err(KvTableError::NoPartitionForValue(_)) => Ok(true),
            Err(error) => Err(error),
        }
    }

    /// Go `onExchangeTablePartition`'s counters: each of the row id, the
    /// separate auto-increment and the auto-random counter moves to the
    /// larger of the two tables', as Go puts `max(ptAutoIDs, ntAutoIDs)`
    /// under both ids.
    pub(crate) fn exchange_auto_ids(&self, other: &KvTable) -> Result<(), AutoIdStoreError> {
        let groups = [
            (self.row_id_allocator(), other.row_id_allocator()),
            (&self.auto_random_id, &other.auto_random_id),
        ];
        let increment = (self.row_id.is_some() || other.row_id.is_some())
            .then_some((&self.auto_id, &other.auto_id));
        for (left, right) in groups.into_iter().chain(increment) {
            let next = (left.next_global()? as i64).max(right.next_global()? as i64);
            for allocator in [left, right] {
                allocator.rebase_to_next(next as u64)?;
                allocator.forget_reservation();
            }
        }
        Ok(())
    }

    /// The rows stored under one of this table's physical ids.
    pub(crate) fn physical_rows(
        &mut self,
        physical_id: i64,
        ctx: &crate::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, KvTableError> {
        Ok(self
            .rows_of_physical_table(physical_id, &RowDecodeContext::for_write(ctx))?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    /// The rows of each physical table, with their handles and the physical
    /// id they are stored under: Go's per-partition reorg and check workers.
    /// Two partitions may hold the same `_tidb_rowid` once a partition has
    /// been exchanged, so a handle alone does not name a row.
    pub(crate) fn scan_physical_rows_with_handles(
        &mut self,
        decode_context: &RowDecodeContext,
    ) -> Result<Vec<(i64, TableHandle, Vec<Datum>)>, KvTableError> {
        let mut rows = Vec::new();
        for physical_id in self.record_physical_ids() {
            rows.extend(
                self.rows_of_physical_table(physical_id, decode_context)?
                    .into_iter()
                    .map(|(handle, row)| (physical_id, handle, row)),
            );
        }
        Ok(rows)
    }

    fn rows_of_physical_table(
        &mut self,
        physical_id: i64,
        decode_context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        let previous_read_partitions = self
            .partition
            .is_some()
            .then(|| self.read_partitions.replace(vec![physical_id]));
        let rows = self.scan_rows_with_handles_recomputed(decode_context);
        if let Some(previous) = previous_read_partitions {
            self.read_partitions = previous;
        }
        rows
    }

    /// Go `onExchangeTablePartition`'s swap: the partition at `ordinal` and
    /// `standalone` trade physical ids, so every record and index key stays
    /// as written and only changes hands. Global indexes are refused before
    /// this runs, so each table keeps every key it holds under its physical
    /// id.
    pub(crate) fn exchange_partition(
        &mut self,
        ordinal: usize,
        standalone: &mut KvTable,
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        let partition_id = self
            .partition
            .as_ref()
            .expect("validated by DDL")
            .definitions[ordinal]
            .id;
        let standalone_id = standalone.table_id;
        let leaving = take_physical_keys(&mut *self.store, partition_id)?;
        let arriving = take_physical_keys(&mut *standalone.store, standalone_id)?;
        for (key, value) in arriving {
            self.store.set(key, value).map_err(KvTableError::from)?;
        }
        for (key, value) in leaving {
            standalone
                .store
                .set(key, value)
                .map_err(KvTableError::from)?;
        }
        self.partition
            .as_mut()
            .expect("validated by DDL")
            .definitions[ordinal]
            .id = standalone_id;
        standalone.table_id = partition_id;
        if let Some(replica) = self.tiflash_replica.as_mut() {
            for id in replica.available_partition_ids.iter_mut() {
                if *id == partition_id {
                    *id = standalone_id;
                    break;
                }
            }
        }
        ctx.staged_writes().mark_dirty(self.table_id);
        ctx.staged_writes().mark_dirty(standalone_id);
        ctx.staged_writes().mark_dirty(partition_id);
        Ok(())
    }
}

/// Removes and returns every key `store` holds under `physical_id`: its
/// records and its local index entries.
fn take_physical_keys(
    store: &mut dyn TableStorage,
    physical_id: i64,
) -> Result<Vec<(Key, Vec<u8>)>, KvTableError> {
    let low = tidb_codec::table_key::encode_table_prefix(physical_id);
    let high = tidb_codec::table_key::encode_table_prefix(physical_id + 1);
    let mut iterator = store
        .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(high)))
        .map_err(KvTableError::from)?;
    let mut entries = Vec::new();
    while iterator.valid() {
        entries.push((iterator.key().clone(), iterator.value().to_vec()));
        iterator.next().map_err(KvTableError::from)?;
    }
    iterator.close();
    for (key, _) in &entries {
        store.delete(key.clone()).map_err(KvTableError::from)?;
    }
    Ok(entries)
}
