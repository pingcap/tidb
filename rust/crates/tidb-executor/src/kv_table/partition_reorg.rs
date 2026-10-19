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

//! DML during a partition reorganization: the double write of Go's
//! `partitionedTable` (`pkg/table/tables/partition.go`,
//! `newPartitionedTable`'s reorg setup, `partitionedTableAddRecord`,
//! `RemoveRecord`, `partitionedTableUpdateRecord`, `locateReorgPartition`,
//! `dataEqRec`).
//!
//! While a DDL runs REORGANIZE PARTITION, REMOVE PARTITIONING or ALTER TABLE
//! ... PARTITION BY, every row written into a partition being reorganized is
//! written again into the partition the new scheme routes it to, so the
//! reorganization's copy stays complete. Each side keeps its own index set:
//! the side that is not yet (or no longer) public writes its indexes in a
//! non-public state, without assertions.

use std::collections::{BTreeMap, BTreeSet};

use tidb_datatype::{Datum, SessionTimeZone};
use tidb_txnkv::Key;

use super::{KvIndex, KvTable, KvTableError, NonPublicIndex, RowDecodeContext, TableHandle};
use crate::partition_routing::PartitionSpec;

/// One table's ongoing partition reorganization, as Go's
/// `partitionedTable` holds it.
#[derive(Clone, Debug)]
pub struct PartitionDoubleWrite {
    /// Go `reorgPartitionExpr` over the reorganization's other definition
    /// set: where a double write goes.
    pub spec: PartitionSpec,
    /// Go `reorganizePartitions`: writes to these partitions are written
    /// again through [`Self::spec`].
    pub reorganize: BTreeSet<i64>,
    /// Go `doubleWritePartitions`: the copies' partitions, which no read
    /// sees.
    pub double_write: BTreeSet<i64>,
    /// Go `DDLState == StateDeleteOnly || StatePublic`: only deletions are
    /// mirrored.
    pub delete_only: bool,
    /// Go's per-partition `tblInfo.Indices` (`DroppingDefinitionIndices` /
    /// `AddingDefinitionIndices`), by physical partition id.
    pub index_modes: BTreeMap<i64, PartitionIndexMode>,
}

/// How one partition's copy of the indexes differs from the table's.
#[derive(Clone, Debug, Default)]
pub struct PartitionIndexMode {
    /// The state Go's clone gives every untouched index here (`DDLState`
    /// for an adding partition before the change is public, `WriteOnly`
    /// for a dropping one after); `None` keeps them as they are.
    pub state: Option<tidb_model::SchemaState>,
    /// Indexes this partition does not carry (Go `DDLChangedIndex`: a new
    /// index only on adding partitions, a replaced one only on dropping
    /// ones).
    pub excluded: BTreeSet<i64>,
    /// `DDLChangedIndex` entries this partition carries as they are, in
    /// their own state rather than [`Self::state`].
    pub changed: BTreeSet<i64>,
}

/// What an UPDATE does beyond its own partition during a reorganization.
pub(in crate::kv_table) struct ReorgUpdate {
    /// Remove the old row's copy from this partition.
    remove_from: Option<i64>,
    /// Add the new row's copy to this partition, under `copy_handle`.
    add_to: Option<i64>,
    copy_handle: TableHandle,
}

impl KvTable {
    /// Installs (or clears) the table's ongoing partition reorganization.
    pub fn set_partition_reorg(&mut self, reorg: Option<PartitionDoubleWrite>) {
        self.partition_reorg = reorg.map(Box::new);
    }

    /// The table's ongoing partition reorganization, if any.
    #[must_use]
    pub fn partition_reorg(&self) -> Option<&PartitionDoubleWrite> {
        self.partition_reorg.as_deref()
    }

    /// The indexes a write into `physical_id` maintains: the table's, or the
    /// partition's own set during a reorganization.
    pub(in crate::kv_table) fn partition_index_sets(
        &self,
        physical_id: i64,
    ) -> (Vec<KvIndex>, Vec<NonPublicIndex>) {
        let mode = self
            .partition_reorg
            .as_deref()
            .and_then(|reorg| reorg.index_modes.get(&physical_id));
        let Some(mode) = mode else {
            return (self.indexes.to_vec(), self.nonpublic_indexes.to_vec());
        };
        let mut normal = Vec::new();
        let mut stateful: Vec<NonPublicIndex> = self
            .nonpublic_indexes
            .iter()
            .filter(|index| !mode.excluded.contains(&index.index.id))
            .cloned()
            .collect();
        for index in self.indexes.iter() {
            if mode.excluded.contains(&index.id) {
                continue;
            }
            match mode.state {
                Some(state) if !index.clustered_primary && !mode.changed.contains(&index.id) => {
                    stateful.push(NonPublicIndex {
                        index: index.clone(),
                        state,
                        backfill_state: tidb_model::BackfillState::INAPPLICABLE,
                    });
                }
                _ => normal.push(index.clone()),
            }
        }
        (normal, stateful)
    }

    /// Go `locateReorgPartition`: the partition the reorganization's scheme
    /// routes `row` to.
    pub(in crate::kv_table) fn locate_reorg_partition(
        &self,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<Option<i64>, KvTableError> {
        let Some(reorg) = self.partition_reorg.as_deref() else {
            return Ok(None);
        };
        reorg
            .spec
            .locate(row, &self.columns, ctx)
            .map(Some)
            .map_err(super::routing_error)
    }

    /// Whether writes to `physical_id` are mirrored.
    fn reorganizes(&self, physical_id: i64) -> bool {
        self.partition_reorg
            .as_deref()
            .is_some_and(|reorg| reorg.reorganize.contains(&physical_id))
    }

    fn reorg_delete_only(&self) -> bool {
        self.partition_reorg
            .as_deref()
            .is_some_and(|reorg| reorg.delete_only)
    }

    fn clustered(&self) -> bool {
        self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty()
    }

    /// Go `addRecord` on a double-write partition (`skipAssert`): the
    /// record under the same handle, and the partition's index set.
    #[allow(clippy::too_many_arguments)]
    fn add_partition_copy(
        &mut self,
        physical_id: i64,
        row: &[Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
        lazy_dup_check: bool,
        pessimistic: bool,
        check_in_prewrite: bool,
    ) -> Result<(), KvTableError> {
        let value = self.encode_row_value(row, zone)?;
        let key = Key::from_bytes(super::encode_row_key_with_handle(
            physical_id,
            &handle.record_handle(),
        ));
        let duplicate_value = if self.clustered() {
            super::clustered_key_text(self, row)
        } else {
            match handle {
                TableHandle::Int(value) => value.to_string(),
                TableHandle::Common(_) => unreachable!("a heap table has an integer handle"),
            }
        };
        self.check_insert_key(
            &key,
            &duplicate_value,
            &self.qualified_key("PRIMARY"),
            lazy_dup_check,
        )?;
        self.write_index_entries(
            row,
            handle,
            physical_id,
            zone,
            lazy_dup_check,
            pessimistic,
            check_in_prewrite,
        )?;
        self.store.set(key, value).map_err(KvTableError::from)
    }

    /// Go `removeRecord` on the other side of a reorganization.
    fn remove_partition_copy(
        &mut self,
        physical_id: i64,
        row: &[Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        self.delete_index_entries(row, handle, physical_id, zone)?;
        let key = Key::from_bytes(super::encode_row_key_with_handle(
            physical_id,
            &handle.record_handle(),
        ));
        self.store.delete(key).map_err(KvTableError::from)
    }

    /// The stored copy under `handle` in `physical_id`, if any.
    fn partition_copy(
        &mut self,
        physical_id: i64,
        handle: &TableHandle,
    ) -> Result<Option<Vec<u8>>, KvTableError> {
        let key = Key::from_bytes(super::encode_row_key_with_handle(
            physical_id,
            &handle.record_handle(),
        ));
        match self.store.get(&key) {
            Ok(value) if value.is_empty() => Ok(None),
            Ok(value) => Ok(Some(value)),
            Err(crate::storage::StorageError::NotFound) => Ok(None),
            Err(error) => Err(KvTableError::from(error)),
        }
    }

    /// Go `dataEqRec`: whether a stored copy holds exactly `row`'s values
    /// for every column it stores.
    fn copy_equals_row(
        &self,
        handle: &TableHandle,
        copy: &[u8],
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<bool, KvTableError> {
        let stored = self.decode_row_entry(handle, copy, &RowDecodeContext::for_write(ctx))?;
        Ok(stored.iter().zip(row).all(|(left, right)| left == right))
    }

    /// `partitionedTableAddRecord`'s tail: a row added to a partition being
    /// reorganized is added again where the new scheme routes it, unless the
    /// reorganization only mirrors deletions by now.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::kv_table) fn double_write_insert(
        &mut self,
        physical_id: i64,
        row: &[Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
        lazy_dup_check: bool,
        pessimistic: bool,
        check_in_prewrite: bool,
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        if self.reorg_delete_only() || !self.reorganizes(physical_id) {
            return Ok(());
        }
        let Some(target) = self.locate_reorg_partition(row, ctx)? else {
            return Ok(());
        };
        self.add_partition_copy(
            target,
            row,
            handle,
            zone,
            lazy_dup_check,
            pessimistic,
            check_in_prewrite,
        )
    }

    /// `partitionedTable.RemoveRecord`'s tail: the copy goes too -- a heap
    /// table's only while it still holds this row's values.
    pub(in crate::kv_table) fn double_write_remove(
        &mut self,
        physical_id: i64,
        row: &[Datum],
        handle: &TableHandle,
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        if !self.reorganizes(physical_id) {
            return Ok(());
        }
        let Some(target) = self.locate_reorg_partition(row, ctx)? else {
            return Ok(());
        };
        let zone = ctx.session_zone();
        if self.clustered() {
            return self.remove_partition_copy(target, row, handle, &zone);
        }
        if let Some(copy) = self.partition_copy(target, handle)? {
            if self.copy_equals_row(handle, &copy, row, ctx)? {
                return self.remove_partition_copy(target, row, handle, &zone);
            }
        }
        Ok(())
    }

    /// `partitionedTableUpdateRecord`'s reorganization half, decided before
    /// the update's own write: the copies to remove and add, and -- for a
    /// heap row staying in its partition whose copy diverged -- the fresh
    /// handle (`AllocHandle`) the row then moves to in BOTH partitions.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::kv_table) fn plan_reorg_update(
        &mut self,
        old_physical_id: i64,
        new_physical_id: i64,
        handle: &TableHandle,
        new_handle: &TableHandle,
        old_row: &[Datum],
        new_row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<Option<ReorgUpdate>, KvTableError> {
        let new_to = if self.reorganizes(new_physical_id) {
            self.locate_reorg_partition(new_row, ctx)?
        } else {
            None
        };
        let new_from = if self.reorganizes(old_physical_id) {
            self.locate_reorg_partition(old_row, ctx)?
        } else {
            None
        };
        if new_from.is_none() && new_to.is_none() {
            return Ok(None);
        }
        let delete_only = self.reorg_delete_only();
        if self.clustered() {
            // Always remove and add, keeping the copy's indexes in sync even
            // when they are not backfilled yet.
            return Ok(Some(ReorgUpdate {
                remove_from: new_from,
                add_to: new_to.filter(|_| !delete_only),
                copy_handle: new_handle.clone(),
            }));
        }
        let mut copy_handle = new_handle.clone();
        let mut remove_from = None;
        let mut same_as_from = None;
        if let Some(from) = new_from {
            if let Some(copy) = self.partition_copy(from, handle)? {
                let same = self.copy_equals_row(handle, &copy, old_row, ctx)?;
                if same {
                    remove_from = Some(from);
                }
                if new_to == Some(from) {
                    same_as_from = Some(same);
                }
            }
        }
        let Some(to) = new_to.filter(|_| !delete_only) else {
            return Ok(Some(ReorgUpdate {
                remove_from,
                add_to: None,
                copy_handle,
            }));
        };
        // Only a copy under the SAME handle can collide; a row that already
        // moved partitions has a fresh handle no copy carries.
        let collides = if Some(to) == new_from {
            same_as_from.map(|same| !same)
        } else if new_handle == handle {
            match self.partition_copy(to, handle)? {
                Some(copy) => Some(!self.copy_equals_row(handle, &copy, old_row, ctx)?),
                None => None,
            }
        } else {
            None
        };
        if collides == Some(true) {
            let shard = if self.shard_row_id_bits > 0 {
                ctx.next_row_id_shard(1) as i64
            } else {
                0
            };
            copy_handle = self.handle_of_row(new_row, &ctx.session_zone(), shard)?;
        }
        Ok(Some(ReorgUpdate {
            remove_from,
            add_to: Some(to),
            copy_handle,
        }))
    }

    /// Applies a [`ReorgUpdate`] after the update's own write.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::kv_table) fn apply_reorg_update(
        &mut self,
        plan: ReorgUpdate,
        handle: &TableHandle,
        old_row: &[Datum],
        new_row: &[Datum],
        lazy_dup_check: bool,
        pessimistic: bool,
        check_in_prewrite: bool,
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        let zone = ctx.session_zone();
        if let Some(from) = plan.remove_from {
            self.remove_partition_copy(from, old_row, handle, &zone)?;
        }
        if let Some(to) = plan.add_to {
            self.add_partition_copy(
                to,
                new_row,
                &plan.copy_handle,
                &zone,
                lazy_dup_check,
                pessimistic,
                check_in_prewrite,
            )?;
        }
        Ok(())
    }

    /// The handle a heap row moves to when its reorganized copy diverged,
    /// which the update's own write must use too.
    pub(in crate::kv_table) fn reorg_moved_handle<'a>(
        &self,
        plan: Option<&'a ReorgUpdate>,
        handle: &TableHandle,
    ) -> Option<&'a TableHandle> {
        plan.filter(|plan| {
            !self.clustered() && plan.add_to.is_some() && &plan.copy_handle != handle
        })
        .map(|plan| &plan.copy_handle)
    }
}
