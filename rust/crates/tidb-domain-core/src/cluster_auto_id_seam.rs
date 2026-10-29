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

//! The node's auto-increment allocators: one per table, over the cluster's
//! own meta keys, kept alive for as long as the node is.
//!
//! Go puts the allocator on the domain's table cache -- one per
//! `tidb-server`, not one per session and not one per schema reload -- and
//! that placement is a correctness property, not a cache. A reserved range
//! belongs to whoever reserved it, so an allocator rebuilt alongside its
//! `KvTable` would abandon the ids it still held and reserve a fresh range
//! from the meta key, leaving a `DEFAULT_AUTO_ID_STEP`-sized hole in the ids
//! every time the schema version moved. This registry is where the allocator
//! outlives the table objects, and `Domain` holds exactly
//! one.
//!
//! [`DEFAULT_AUTO_ID_STEP`]: tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_txnkv::transaction::StoreClient;

use tidb_cluster::cluster_auto_id::ClusterAutoIdStore;
use tidb_exec::cluster_sequence::ClusterSequenceCounter;
use tidb_executor::driver::SequenceDef;
use tidb_executor::kv_table::{TableAutoId, DEFAULT_AUTO_ID_STEP};
use tidb_executor::sequence::{SequenceAllocator, SequenceInfo as SeqCounterInfo};
use tidb_model::TableInfo;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;

use tidb_ddl_build::table_from_meta::{AutoIdKind, TableAutoIds};

/// This node's allocators, keyed by table id.
///
/// A table id is unique cluster-wide and is REPLACED rather than reused when
/// a table is truncated or recreated, which is what makes it the right key:
/// the entry a dropped table leaves behind can never be handed to a different
/// table, and a truncated table gets a new id and so a new allocator whose
/// counter key has never been written.
pub struct ClusterTableAutoIds<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    opener: RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    service: Option<Arc<tidb_cluster::auto_id_client::AutoIdClient>>,
    allocators: Mutex<HashMap<(i64, AutoIdKind), (AllocatorMark, TableAutoId)>>,
    /// The same lifetime rule for SEQUENCE allocators: one per sequence table
    /// id and published `TableInfo` (its `update_ts`), so a reserved cache
    /// batch is never abandoned by a catalog rebuild, and is abandoned by an
    /// `ALTER SEQUENCE`.
    sequences: Mutex<HashMap<i64, (u64, SequenceDef)>>,
}

impl<S> std::fmt::Debug for ClusterTableAutoIds<S>
where
    S: StoreClient,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held = self.allocators.lock().map_or(0, |map| map.len());
        formatter
            .debug_struct("ClusterTableAutoIds")
            .field("tables", &held)
            .finish_non_exhaustive()
    }
}

impl<S> ClusterTableAutoIds<S>
where
    S: StoreClient,
{
    /// Allocators reserving through `opener`, giving each meta-key
    /// transaction `timeout` to finish.
    #[must_use]
    pub fn new(opener: RealOptimisticTransactionOpener<S>, timeout: Duration) -> Self {
        ClusterTableAutoIds {
            opener,
            timeout,
            service: None,
            allocators: Mutex::new(HashMap::new()),
            sequences: Mutex::new(HashMap::new()),
        }
    }

    /// Compose the process-wide AutoID service transport for cluster tables.
    pub fn with_service(mut self, service: Arc<tidb_cluster::auto_id_client::AutoIdClient>) -> Self {
        self.service = Some(service);
        self
    }

    /// The live allocator for one stored SEQUENCE.
    ///
    /// Go `NewSequenceAllocator` is built from the stored `SequenceInfo` and
    /// reserves through the meta keys [`ClusterSequenceCounter`] owns. The
    /// FIRST call reads nothing: the counter's whole state -- batch end and
    /// cycle round -- is read inside each reservation transaction, which is
    /// what keeps this node consistent with its peers even though the
    /// allocator object itself never re-reads between reservations.
    fn sequence(&self, db_id: i64, table: &TableInfo) -> SequenceDef {
        let mut sequences = self.sequences.lock().expect("sequence registry poisoned");
        // Go builds a new table object -- and with it an empty sequence
        // cache -- for every published `TableInfo`: an `ALTER SEQUENCE` that
        // changes the options or `RESTART`s the counter must not keep issuing
        // from the batch reserved before it.
        if let Some((update_ts, held)) = sequences.get(&table.id) {
            if *update_ts == table.update_ts {
                return held.clone();
            }
        }
        let stored_sequence = table
            .sequence
            .as_ref()
            .map(|shared| shared.read().clone())
            .unwrap_or_default();
        let info = SeqCounterInfo {
            start: stored_sequence.start,
            increment: stored_sequence.increment,
            min_value: stored_sequence.min_value,
            max_value: stored_sequence.max_value,
            cache_value: stored_sequence.cache_value,
            cache: stored_sequence.cache,
            cycle: stored_sequence.cycle,
        };
        let counter =
            ClusterSequenceCounter::new(self.opener.clone(), db_id, table.id, self.timeout);
        let def = SequenceDef {
            id: table.id,
            name: table.name.original().to_owned(),
            comment: stored_sequence.comment,
            allocator: SequenceAllocator::over_counter(info, counter.shared()),
            update_ts: table.update_ts,
        };
        sequences.insert(table.id, (table.update_ts, def.clone()));
        def
    }
}

impl<S> TableAutoIds for ClusterTableAutoIds<S>
where
    S: StoreClient,
{
    fn allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId {
        self.allocator(db_id, table, AutoIdKind::AutoIncrement)
    }

    fn row_allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId {
        self.allocator(
            db_id,
            table,
            if table.sep_auto_inc() {
                AutoIdKind::RowId
            } else {
                AutoIdKind::AutoIncrement
            },
        )
    }

    fn random_allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId {
        self.allocator(db_id, table, AutoIdKind::AutoRandom)
    }

    fn sequence_allocator_for(&self, db_id: i64, table: &TableInfo) -> Option<SequenceDef> {
        Some(self.sequence(db_id, table))
    }

    fn rebuild_rebased_allocators(&self, table_id: i64, action: tidb_model::ActionType) {
        let kinds: &[AutoIdKind] =
            if action == tidb_model::ActionType::ACTION_REBASE_AUTO_RANDOM_BASE {
                &[AutoIdKind::AutoRandom]
            } else {
                &[AutoIdKind::AutoIncrement, AutoIdKind::RowId]
            };
        let allocators = self
            .allocators
            .lock()
            .expect("cluster auto id registry poisoned");
        for kind in kinds {
            if let Some((_, allocator)) = allocators.get(&(table_id, *kind)) {
                allocator.forget_reservation();
            }
        }
    }
}

impl<S> ClusterTableAutoIds<S>
where
    S: StoreClient,
{
    fn allocator(&self, db_id: i64, table: &TableInfo, kind: AutoIdKind) -> TableAutoId {
        let db_id = if table.auto_id_schema_id == 0 {
            db_id
        } else {
            table.auto_id_schema_id
        };
        let mut allocators = self
            .allocators
            .lock()
            .expect("cluster auto id registry poisoned");
        let mark = AllocatorMark::of(db_id, table, kind);
        if let Some((held, allocator)) = allocators.get(&(table.id, kind)) {
            if *held == mark {
                return allocator.clone();
            }
            // A single-point allocator has no reserved range to preserve. Rebuild
            // its table-local Base after DDL while retaining process discovery.
            if held.single_point != mark.single_point
                || held.schema_id != mark.schema_id
                || mark.single_point
            {
                allocators.remove(&(table.id, kind));
            } else {
                // The recorded base moved, which only a DDL does: `ALTER TABLE
                // ... AUTO_INCREMENT` publishes a new `TableInfo` AND a new
                // counter. A range reserved before that change would keep
                // issuing the old ids, so this node drops it and re-reads --
                // Go reaches the same state by rebuilding the table's
                // allocators alongside the new `InfoSchema`.
                if held.base != mark.base {
                    allocator.forget_reservation();
                }
                let allocator = if held.cache == mark.cache {
                    allocator.clone()
                } else {
                    allocator.with_step(step_for(table))
                };
                allocators.insert((table.id, kind), (mark, allocator.clone()));
                return allocator;
            }
        }
        let step = if kind == AutoIdKind::RowId {
            DEFAULT_AUTO_ID_STEP
        } else if kind == AutoIdKind::AutoIncrement && table.sep_auto_inc() {
            1
        } else {
            step_for(table)
        };
        if kind == AutoIdKind::AutoIncrement && table.sep_auto_inc() {
            if let Some(service) = &self.service {
                let unsigned = table.is_auto_inc_col_unsigned();
                let allocator = TableAutoId::service(Arc::new(
                    tidb_cluster::cluster_auto_id::AutoIdServiceAllocator::new(
                        service.clone(),
                        db_id,
                        table.id,
                        unsigned,
                        u32::MAX,
                    ),
                ));
                allocators.insert((table.id, kind), (mark, allocator.clone()));
                return allocator;
            }
        }
        let store = if kind == AutoIdKind::RowId {
            ClusterAutoIdStore::over_key(
                self.opener.clone(),
                tidb_meta::key::auto_table_id_kv_key(db_id, table.id),
                self.timeout,
            )
            .shared()
        } else if kind == AutoIdKind::AutoRandom {
            ClusterAutoIdStore::new_random(self.opener.clone(), db_id, table, self.timeout).shared()
        } else {
            ClusterAutoIdStore::new(self.opener.clone(), db_id, table, self.timeout).shared()
        };
        let allocator = TableAutoId::over(store, step);
        allocators.insert((table.id, kind), (mark, allocator.clone()));
        allocator
    }
}

/// What the registry remembers about the `TableInfo` an allocator was built
/// for, so it can tell a re-ask apart from a change.
#[derive(Clone, Copy, PartialEq, Eq)]
struct AllocatorMark {
    schema_id: i64,
    single_point: bool,
    /// `TableInfo.AutoIDCache`, which sets the reservation step.
    cache: i64,
    /// `TableInfo.AutoIncID` / `AutoRandID`: the recorded base, which a
    /// rebase DDL moves and nothing else does.
    base: i64,
}

impl AllocatorMark {
    fn of(schema_id: i64, table: &TableInfo, kind: AutoIdKind) -> Self {
        AllocatorMark {
            schema_id,
            single_point: kind == AutoIdKind::AutoIncrement && table.sep_auto_inc(),
            cache: table.auto_id_cache,
            base: if kind == AutoIdKind::AutoRandom {
                table.auto_rand_id
            } else {
                table.auto_inc_id
            },
        }
    }
}

fn step_for(table: &TableInfo) -> u64 {
    if table.auto_id_cache > 0 {
        table.auto_id_cache as u64
    } else {
        DEFAULT_AUTO_ID_STEP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
    use tidb_executor::KvTable;
    use tidb_model::ColumnInfo;
    struct RefusedLeader;
    impl tidb_cluster::auto_id_client::AutoIdLeader for RefusedLeader {
        fn leader(
            &self,
        ) -> Result<Option<String>, tidb_cluster::cluster_auto_id::AutoIdServiceRpcError> {
            Err(tidb_cluster::cluster_auto_id::AutoIdServiceRpcError::Other(
                "service discovery reached".into(),
            ))
        }
    }
    fn model(version: u16, cache: i64) -> TableInfo {
        let mut field = FieldType::new(FieldTypeCode::LongLong);
        field.add_flags(FieldTypeFlags::AUTO_INCREMENT);
        TableInfo {
            id: 402,
            version,
            auto_id_cache: cache,
            name: CiString::new("t"),
            columns: vec![ColumnInfo::new(1, "id", field)].into(),
            ..TableInfo::default()
        }
    }
    fn next(
        allocator: TableAutoId,
    ) -> Result<Option<i64>, tidb_executor::kv_table::AutoIdStoreError> {
        let mut table = KvTable::new(402, vec![]);
        table.set_auto_increment_offset(0);
        table.set_auto_id(allocator);
        table.next_auto_increment_for_show()
    }
    #[test]
    fn auto_id_owner_registry_selects_service_and_keeps_row_random_legacy_counters() {
        let (_pd, opener) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        let service = Arc::new(
            tidb_cluster::auto_id_client::AutoIdClient::new(
                Arc::new(RefusedLeader),
                tidb_pd_client::ClusterSecurity::plaintext(),
            )
            .unwrap(),
        );
        let registry =
            ClusterTableAutoIds::new(opener, Duration::from_secs(2)).with_service(service);
        let table = model(5, 1);
        let first = registry.allocator_for(1, &table);
        assert!(
            first.same_allocator_as(&registry.allocator_for(1, &table)),
            "reload retains the owner"
        );
        assert!(next(first.clone())
            .unwrap_err()
            .0
            .contains("service discovery reached"));
        let mut rebased = table.clone();
        rebased.auto_inc_id = 100;
        let after_ddl = registry.allocator_for(1, &rebased);
        assert!(!first.same_allocator_as(&after_ddl));
        assert!(next(after_ddl)
            .unwrap_err()
            .0
            .contains("service discovery reached"));
        let mut renamed = table.clone();
        renamed.auto_id_schema_id = 1;
        assert!(registry
            .allocator_for(2, &renamed)
            .same_allocator_as(&registry.allocator_for(1, &table)));
        let row = registry.row_allocator_for(1, &table);
        assert!(!row.same_allocator_as(&first));
        assert_eq!(next(row).unwrap(), Some(1));
        assert_eq!(
            next(registry.random_allocator_for(1, &table)).unwrap(),
            Some(1)
        );
        // Other table identities avoid intentional registry reuse across incompatible metadata.
        let mut legacy = model(4, 1);
        legacy.id = 403;
        assert_eq!(next(registry.allocator_for(1, &legacy)).unwrap(), Some(1));
        let mut cached = model(5, 10);
        cached.id = 404;
        let ordinary = registry.allocator_for(1, &cached);
        assert!(ordinary.same_allocator_as(&registry.row_allocator_for(1, &cached)));
        assert_eq!(next(ordinary).unwrap(), Some(1));
    }
}
