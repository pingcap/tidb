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
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            let mut keys = Vec::new();
            while iterator.valid() {
                keys.push(iterator.key().clone());
                iterator
                    .next()
                    .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            }
            iterator.close();
            for key in keys {
                self.store
                    .delete(key)
                    .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
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
        self.dirty_content
            .0
            .store(true, std::sync::atomic::Ordering::Relaxed);
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
        self.dirty_content
            .0
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn append_partitions(
        &mut self,
        definitions: Vec<PartitionDef>,
        added_kind: PartitionKind,
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
        self.dirty_content
            .0
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
