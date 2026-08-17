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

//! Statistics-metadata maintenance for schema-change events, transcreated from
//! `pkg/statistics/handle/ddl/subscriber.go` and
//! `pkg/statistics/handle/ddl/ddl.go`.
//!
//! Status: complete for the event dispatch, the per-branch statement/effect
//! ordering, and the global-stats delta arithmetic those two files own. The
//! crate already owned two leaves of the same package -- `ddl_physical_ids`
//! (`getPhysicalIDs`) and `ddl_stats_delta` (the three delta statements of
//! `updateStatsWithCountDeltaAndModifyCountDelta`) -- and this module composes
//! them rather than restating them.
//!
//! Narrowings, each named:
//!
//! * `// boundary: session-pool SQL execution` -- every `mysql.stats_*` read
//!   and write in the Go file runs through `util.Exec` / `storage.*` on a
//!   pooled `sessionctx.Context`. That is not portable here, so the statements
//!   sit behind [`DdlStatsStorePort`] and this module owns only which call
//!   happens, with which arguments, in which order.
//! * `// boundary: global system variables` -- `tidb_partition_prune_mode` and
//!   `tidb_enable_historical_stats` are read through
//!   `GlobalVarsAccessor.GetGlobalSysVar`; [`DdlSessionPort`] projects them to
//!   two fallible booleans.
//! * `// boundary: infoschema` -- `infoschema.SchemaByTable` becomes
//!   [`DdlSessionPort::schema_name_by_table`], preserving the
//!   [`SCHEMA_NOT_FOUND`] placeholder on a miss.
//! * `notifier.SchemaChangeEvent` decoding stays external:
//!   [`StatsSchemaChangeEvent`] is the already-decoded projection, so
//!   `GetCreateTableInfo`-style accessors have no Rust counterpart.
//! * zap logging is narrowed to data. The two Go field builders
//!   (`exchangePartitionLogFields`, `truncatePartitionsLogFields`) return
//!   [`ExchangePartitionAudit`] / [`TruncatePartitionAudit`] so the exact field
//!   set survives without a logging dependency.
//! * `intest.Assert` is debug-only in Go and has no runtime effect; an
//!   unhandled event is [`StatsSchemaChangeEvent::Unhandled`] and is a no-op.

use std::collections::BTreeSet;

use crate::ddl_physical_ids::physical_ids_for_stats_ddl;
use crate::ddl_stats_delta::{ddl_stats_delta_update, DdlStatsDeltaUpdate};

/// Port errors are deliberately opaque strings; this layer does not invent
/// TiDB session or storage error identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlError(pub String);

pub type DdlResult<T> = Result<T, DdlError>;

/// Buffer size of `ddlHandlerImpl.ddlEventCh` (`ddl.go:47`).
pub const DDL_EVENT_CHANNEL_CAPACITY: usize = 1000;

/// Placeholder used when the global table's schema is gone
/// (`subscriber.go:439`).
pub const SCHEMA_NOT_FOUND: &str = "Not Found";

/// Duration Go treats as "not analyzed yet" is unrelated here; this constant
/// is the source of the `HandleDDLEvent` swallow list (`ddl.go:59-71`).
const IGNORABLE_DDL_ERRORS: [&str; 3] = [
    "context canceled",
    "mock handleTaskOnce error",
    "session pool closed",
];

/// Whether `HandleDDLEvent`'s `intest.Assert` accepts the failure as expected.
///
/// Go asserts on `context.Canceled` equality plus two substring checks; the
/// assert is debug-only, and the surrounding code warns and returns `nil`
/// either way. Exposed so callers can reproduce the classification.
#[must_use]
pub fn is_expected_ddl_event_error(message: &str) -> bool {
    IGNORABLE_DDL_ERRORS
        .iter()
        .any(|expected| message.contains(expected))
}

/// A partition definition as consumed by the subscriber: only its ID and name
/// reach `mysql.stats_*` or the log fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionDef {
    pub id: i64,
    pub name: String,
}

impl PartitionDef {
    #[must_use]
    pub fn new(id: i64, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }
}

/// The `model.TableInfo` fields the subscriber actually reads.
///
/// `partitions == None` is `GetPartitionInfo() == nil` and must stay distinct
/// from `Some(vec![])`, which is a partitioned table with no definitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlTableInfo {
    pub id: i64,
    pub name: String,
    pub partitions: Option<Vec<PartitionDef>>,
}

impl DdlTableInfo {
    #[must_use]
    pub fn non_partitioned(id: i64, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            partitions: None,
        }
    }

    #[must_use]
    pub fn partitioned(id: i64, name: impl Into<String>, partitions: Vec<PartitionDef>) -> Self {
        Self {
            id,
            name: name.into(),
            partitions: Some(partitions),
        }
    }

    fn partition_ids(&self) -> Option<Vec<i64>> {
        self.partitions
            .as_ref()
            .map(|defs| defs.iter().map(|def| def.id).collect())
    }
}

/// One table of `miniDBInfo.Tables` in the drop-schema branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedSchemaTable {
    pub id: i64,
    pub partition_ids: Vec<i64>,
}

/// `notifier.SchemaChangeEvent` after the accessor calls the Go switch makes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatsSchemaChangeEvent {
    CreateTable {
        table: DdlTableInfo,
    },
    TruncateTable {
        new_table: DdlTableInfo,
        dropped_table: DdlTableInfo,
    },
    DropTable {
        dropped_table: DdlTableInfo,
    },
    AddColumn {
        new_table: DdlTableInfo,
        column_ids: Vec<i64>,
    },
    ModifyColumn {
        new_table: DdlTableInfo,
        column_ids: Vec<i64>,
        /// `tidb_stats_update_during_ddl` already analyzed the column.
        analyzed: bool,
    },
    AddTablePartition {
        global_table: DdlTableInfo,
        added_partitions: Vec<PartitionDef>,
    },
    TruncateTablePartition {
        global_table: DdlTableInfo,
        added_partitions: Vec<PartitionDef>,
        dropped_partitions: Vec<PartitionDef>,
    },
    DropTablePartition {
        global_table: DdlTableInfo,
        dropped_partitions: Vec<PartitionDef>,
    },
    ExchangeTablePartition {
        global_table: DdlTableInfo,
        /// `originalPartInfo.Definitions[0]`; the source indexes it directly.
        original_partition: PartitionDef,
        original_table: DdlTableInfo,
    },
    ReorganizePartition {
        global_table: DdlTableInfo,
        added_partitions: Vec<PartitionDef>,
        dropped_partitions: Vec<PartitionDef>,
    },
    AlterTablePartitioning {
        old_single_table_id: i64,
        global_table: DdlTableInfo,
        added_partitions: Vec<PartitionDef>,
    },
    RemovePartitioning {
        old_table_id: i64,
        new_single_table: DdlTableInfo,
        dropped_partitions: Vec<PartitionDef>,
    },
    FlashbackCluster,
    /// `model.ActionAddIndex`: explicitly nothing to do (`subscriber.go:251`).
    AddIndex,
    DropSchema {
        tables: Vec<DroppedSchemaTable>,
    },
    /// Anything reaching the Go `default` arm: assert in tests, log, no effect.
    Unhandled,
}

/// `storage.NewDeltaUpdate` payload for the global stats-meta writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeltaUpdate {
    pub table_id: i64,
    pub count: i64,
    pub delta: i64,
    pub is_locked: bool,
}

/// Field set of `exchangePartitionLogFields` (`subscriber.go:542-566`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangePartitionAudit {
    pub global_table_schema: String,
    pub global_table_id: i64,
    pub global_table_name: String,
    pub count_delta: i64,
    pub modify_count_delta: i64,
    pub partition_id: i64,
    pub partition_name: String,
    pub partition_count: i64,
    pub partition_modify_count: i64,
    pub table_id: i64,
    pub table_name: String,
    pub table_count: i64,
    pub table_modify_count: i64,
}

/// Field set of `truncatePartitionsLogFields` (`subscriber.go:661-682`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncatePartitionAudit {
    pub schema: String,
    pub table_id: i64,
    pub table_name: String,
    pub partition_ids: Vec<i64>,
    pub partition_names: Vec<String>,
    pub count: i64,
    pub delta: i64,
    pub start_ts: u64,
    pub is_locked: bool,
}

/// Exchange-partition global deltas (`subscriber.go:462-469`).
///
/// `total_count = original_table_count - original_partition_count +
/// new_table_count`, so the count delta is `table_count - part_count`. The
/// modify-count delta additionally charges the delete-then-add of both sides.
/// Go's signed additions wrap, so this keeps wrapping arithmetic.
#[must_use]
pub const fn exchange_partition_deltas(
    part_count: i64,
    part_modify_count: i64,
    table_count: i64,
    table_modify_count: i64,
) -> (i64, i64) {
    let count_delta = table_count.wrapping_sub(part_count);
    let modify_count_delta = table_count
        .wrapping_add(part_count)
        .wrapping_sub(part_modify_count)
        .wrapping_add(table_modify_count);
    (count_delta, modify_count_delta)
}

// boundary: global system variables and infoschema lookups.
/// Session-scoped reads the subscriber performs.
pub trait DdlSessionPort {
    /// `getCurrentPruneMode(sctx) == variable.Dynamic`.
    fn dynamic_prune_mode(&self) -> DdlResult<bool>;
    /// `getEnableHistoricalStats(sctx)`.
    fn enable_historical_stats(&self) -> DdlResult<bool>;
    /// `util.GetStartTS(sctx)`.
    fn start_ts(&self) -> DdlResult<u64>;
    /// `infoschema.SchemaByTable`; `None` keeps [`SCHEMA_NOT_FOUND`].
    fn schema_name_by_table(&self, table_id: i64) -> Option<String>;
}

// boundary: session-pool SQL execution against `mysql.stats_*`.
/// Every storage call the subscriber makes, in source terms.
pub trait DdlStatsStorePort {
    /// `storage.InsertTableStats2KV`; returns the write's start TS, `0` when
    /// the source performed no write.
    fn insert_table_stats(&mut self, table: &DdlTableInfo, physical_id: i64) -> DdlResult<u64>;
    /// `storage.InsertColStats2KV`.
    fn insert_column_stats(&mut self, physical_id: i64, column_ids: &[i64]) -> DdlResult<u64>;
    /// `storage.UpdateStatsMetaVerAndLastHistUpdateVer`.
    fn bump_stats_meta_and_hist_version(&mut self, physical_id: i64) -> DdlResult<u64>;
    /// `storage.StatsMetaCountAndModifyCount`; the flag is the `isNull` result.
    fn stats_meta_count_and_modify_count(
        &mut self,
        physical_id: i64,
    ) -> DdlResult<(i64, i64, bool)>;
    /// `storage.StatsMetaCountAndModifyCountForUpdate`.
    fn stats_meta_count_and_modify_count_for_update(
        &mut self,
        physical_id: i64,
    ) -> DdlResult<(i64, i64, bool)>;
    /// `lockstats.QueryLockedTables`.
    fn query_locked_tables(&mut self) -> DdlResult<BTreeSet<i64>>;
    /// `storage.UpdateStatsMeta`.
    fn update_stats_meta(&mut self, start_ts: u64, update: DeltaUpdate) -> DdlResult<()>;
    /// The `util.Exec` of `updateStatsWithCountDeltaAndModifyCountDelta`.
    fn execute_stats_delta(&mut self, update: DdlStatsDeltaUpdate) -> DdlResult<()>;
    /// `storage.ChangeGlobalStatsID`.
    fn change_global_stats_id(&mut self, old_id: i64, new_id: i64) -> DdlResult<()>;
    /// `storage.UpdateStatsVersion`.
    fn update_stats_version(&mut self) -> DdlResult<()>;
    /// `history.RecordHistoricalStatsMeta` with
    /// `util.StatsMetaHistorySourceSchemaChange`.
    fn record_historical_stats_meta(&mut self, physical_id: i64, start_ts: u64) -> DdlResult<()>;
}

/// `types.StatsCache.Get(id)` reduced to the predicate the subscriber uses.
pub trait DdlStatsCachePort {
    /// `ok && tbl.IsInitialized()`.
    fn is_initialized(&self, physical_id: i64) -> bool;
}

/// `subscriber` from `pkg/statistics/handle/ddl/subscriber.go`.
pub struct DdlSubscriber<'a, S, T, C> {
    pub session: &'a S,
    pub store: &'a mut T,
    pub stats_cache: &'a C,
    /// Audit records the Go code would have emitted through zap, in order.
    pub exchange_audits: Vec<ExchangePartitionAudit>,
    pub truncate_audits: Vec<TruncatePartitionAudit>,
    /// Errors the drop-schema branch logs and deliberately does not return.
    pub suppressed_errors: Vec<DdlError>,
}

impl<'a, S, T, C> DdlSubscriber<'a, S, T, C>
where
    S: DdlSessionPort,
    T: DdlStatsStorePort,
    C: DdlStatsCachePort,
{
    pub fn new(session: &'a S, store: &'a mut T, stats_cache: &'a C) -> Self {
        Self {
            session,
            store,
            stats_cache,
            exchange_audits: Vec::new(),
            truncate_audits: Vec::new(),
            suppressed_errors: Vec::new(),
        }
    }

    /// `ddlHandlerImpl.HandleDDLEvent` (`ddl.go:55`): run [`Self::handle`] and
    /// swallow its error after classification, always returning `Ok`.
    ///
    /// Returns the classified failure so a caller can log it, matching the Go
    /// warn-and-continue contract without a logging dependency.
    pub fn handle_ddl_event(&mut self, event: &StatsSchemaChangeEvent) -> (Option<DdlError>, bool) {
        match self.handle(event) {
            Ok(()) => (None, true),
            Err(error) => {
                let expected = is_expected_ddl_event_error(&error.0);
                (Some(error), expected)
            }
        }
    }

    /// `subscriber.handle` (`subscriber.go:49`).
    pub fn handle(&mut self, event: &StatsSchemaChangeEvent) -> DdlResult<()> {
        match event {
            StatsSchemaChangeEvent::CreateTable { table } => {
                for id in self.physical_ids(table)? {
                    self.insert_stats_for_physical_id(table, id)?;
                }
            }
            StatsSchemaChangeEvent::TruncateTable {
                new_table,
                dropped_table,
            } => {
                for id in self.physical_ids(new_table)? {
                    self.insert_stats_for_physical_id(new_table, id)?;
                }
                for id in self.physical_ids(dropped_table)? {
                    self.delayed_delete_stats_for_physical_id(id)?;
                }
            }
            StatsSchemaChangeEvent::DropTable { dropped_table } => {
                for id in self.physical_ids(dropped_table)? {
                    self.delayed_delete_stats_for_physical_id(id)?;
                }
            }
            StatsSchemaChangeEvent::AddColumn {
                new_table,
                column_ids,
            } => {
                for id in self.physical_ids(new_table)? {
                    self.insert_stats_for_columns(id, column_ids)?;
                }
            }
            StatsSchemaChangeEvent::ModifyColumn {
                new_table,
                column_ids,
                analyzed,
            } => {
                // DDL already analyzed the column, so skip column init here.
                if *analyzed {
                    return Ok(());
                }
                for id in self.physical_ids(new_table)? {
                    self.insert_stats_for_columns(id, column_ids)?;
                }
            }
            StatsSchemaChangeEvent::AddTablePartition {
                global_table,
                added_partitions,
            } => {
                for def in added_partitions {
                    self.insert_stats_for_physical_id(global_table, def.id)?;
                }
            }
            StatsSchemaChangeEvent::TruncateTablePartition {
                global_table,
                added_partitions,
                dropped_partitions,
            } => {
                for def in added_partitions {
                    self.insert_stats_for_physical_id(global_table, def.id)?;
                }
                self.update_global_stats_for_truncate_partition(global_table, dropped_partitions)?;
                for def in dropped_partitions {
                    self.delayed_delete_stats_for_physical_id(def.id)?;
                }
            }
            StatsSchemaChangeEvent::DropTablePartition {
                global_table,
                dropped_partitions,
            } => {
                self.update_global_stats_for_drop_partition(global_table, dropped_partitions)?;
                for def in dropped_partitions {
                    self.delayed_delete_stats_for_physical_id(def.id)?;
                }
            }
            // Exchanging with a system table cannot be fully reconciled, so the
            // source ignores system-table events entirely before this point.
            StatsSchemaChangeEvent::ExchangeTablePartition {
                global_table,
                original_partition,
                original_table,
            } => {
                self.update_global_stats_for_exchange_partition(
                    global_table,
                    original_partition,
                    original_table,
                )?;
            }
            // Global stats are untouched: the data is unchanged, and the new
            // partitions have no statistics, so auto-analyze will pick them up.
            StatsSchemaChangeEvent::ReorganizePartition {
                global_table,
                added_partitions,
                dropped_partitions,
            } => {
                for def in added_partitions {
                    self.insert_stats_for_physical_id(global_table, def.id)?;
                }
                for def in dropped_partitions {
                    self.delayed_delete_stats_for_physical_id(def.id)?;
                }
            }
            StatsSchemaChangeEvent::AlterTablePartitioning {
                old_single_table_id,
                global_table,
                added_partitions,
            } => {
                for def in added_partitions {
                    self.insert_stats_for_physical_id(global_table, def.id)?;
                }
                // The data did not change, so the global stats only change ID.
                self.store
                    .change_global_stats_id(*old_single_table_id, global_table.id)?;
            }
            StatsSchemaChangeEvent::RemovePartitioning {
                old_table_id,
                new_single_table,
                dropped_partitions,
            } => {
                self.store
                    .change_global_stats_id(*old_table_id, new_single_table.id)?;
                for def in dropped_partitions {
                    self.delayed_delete_stats_for_physical_id(def.id)?;
                }
            }
            StatsSchemaChangeEvent::FlashbackCluster => {
                self.store.update_stats_version()?;
            }
            StatsSchemaChangeEvent::AddIndex | StatsSchemaChangeEvent::Unhandled => {}
            StatsSchemaChangeEvent::DropSchema { tables } => {
                for table in tables {
                    // Best effort for stats GC: keep going past every failure.
                    for partition_id in &table.partition_ids {
                        if let Err(error) = self.delayed_delete_stats_for_physical_id(*partition_id)
                        {
                            self.suppressed_errors.push(error);
                        }
                    }
                    if let Err(error) = self.delayed_delete_stats_for_physical_id(table.id) {
                        self.suppressed_errors.push(error);
                    }
                }
            }
        }
        Ok(())
    }

    /// `getPhysicalIDs` (`subscriber.go:356`), composed over the existing leaf.
    fn physical_ids(&self, table: &DdlTableInfo) -> DdlResult<Vec<i64>> {
        let partition_ids = table.partition_ids();
        if partition_ids.is_none() {
            return Ok(physical_ids_for_stats_ddl(table.id, None, false));
        }
        let dynamic = self.session.dynamic_prune_mode()?;
        Ok(physical_ids_for_stats_ddl(
            table.id,
            partition_ids.as_deref(),
            dynamic,
        ))
    }

    /// `insertStats4PhysicalID` (`subscriber.go:287`).
    fn insert_stats_for_physical_id(
        &mut self,
        table: &DdlTableInfo,
        physical_id: i64,
    ) -> DdlResult<()> {
        let start_ts = self.store.insert_table_stats(table, physical_id)?;
        self.record_historical_stats_meta(physical_id, start_ts)
    }

    /// `insertStats4Col` (`subscriber.go:343`).
    fn insert_stats_for_columns(&mut self, physical_id: i64, column_ids: &[i64]) -> DdlResult<()> {
        let start_ts = self.store.insert_column_stats(physical_id, column_ids)?;
        self.record_historical_stats_meta(physical_id, start_ts)
    }

    /// `delayedDeleteStats4PhysicalID` (`subscriber.go:331`).
    fn delayed_delete_stats_for_physical_id(&mut self, physical_id: i64) -> DdlResult<()> {
        let start_ts = self.store.bump_stats_meta_and_hist_version(physical_id)?;
        self.record_historical_stats_meta(physical_id, start_ts)
    }

    /// `recordHistoricalStatsMeta` (`subscriber.go:300`).
    ///
    /// A zero start TS means the caller wrote nothing, the global variable
    /// gates the record, and an uncached or uninitialized table is skipped.
    fn record_historical_stats_meta(&mut self, physical_id: i64, start_ts: u64) -> DdlResult<()> {
        if start_ts == 0 {
            return Ok(());
        }
        if !self.session.enable_historical_stats()? {
            return Ok(());
        }
        if !self.stats_cache.is_initialized(physical_id) {
            return Ok(());
        }
        self.store
            .record_historical_stats_meta(physical_id, start_ts)
    }

    /// Sum of `StatsMetaCountAndModifyCount` counts over dropped definitions.
    fn dropped_partition_count(&mut self, dropped: &[PartitionDef]) -> DdlResult<i64> {
        let mut count = 0i64;
        for def in dropped {
            let (table_count, _, _) = self.store.stats_meta_count_and_modify_count(def.id)?;
            count = count.wrapping_add(table_count);
        }
        Ok(count)
    }

    /// `getCountsAndModifyCounts` (`subscriber.go:524`).
    fn counts_and_modify_counts(
        &mut self,
        partition_id: i64,
        table_id: i64,
    ) -> DdlResult<(i64, i64, i64, i64)> {
        let (part_count, part_modify_count, _) =
            self.store.stats_meta_count_and_modify_count(partition_id)?;
        let (table_count, table_modify_count, _) =
            self.store.stats_meta_count_and_modify_count(table_id)?;
        Ok((
            part_count,
            part_modify_count,
            table_count,
            table_modify_count,
        ))
    }

    fn schema_name(&self, table_id: i64) -> String {
        self.session
            .schema_name_by_table(table_id)
            .unwrap_or_else(|| SCHEMA_NOT_FOUND.to_owned())
    }

    /// `updateGlobalTableStats4DropPartition` (`subscriber.go:396`).
    ///
    /// A zero total count short-circuits before the lock query, so no start TS
    /// is consumed. The dropped rows leave the global table, hence `-count`.
    pub fn update_global_stats_for_drop_partition(
        &mut self,
        global_table: &DdlTableInfo,
        dropped: &[PartitionDef],
    ) -> DdlResult<()> {
        let count = self.dropped_partition_count(dropped)?;
        if count == 0 {
            return Ok(());
        }
        let locked = self.store.query_locked_tables()?;
        let is_locked = locked.contains(&global_table.id);
        let start_ts = self.session.start_ts()?;
        self.store.update_stats_meta(
            start_ts,
            DeltaUpdate {
                table_id: global_table.id,
                count,
                delta: -count,
                is_locked,
            },
        )
    }

    /// `updateGlobalTableStats4TruncatePartition` (`subscriber.go:568`).
    ///
    /// Modify count is deliberately not subtracted: the deletes that produced
    /// it still happened from the global table's point of view.
    pub fn update_global_stats_for_truncate_partition(
        &mut self,
        global_table: &DdlTableInfo,
        dropped: &[PartitionDef],
    ) -> DdlResult<()> {
        let mut count = 0i64;
        let mut partition_ids = Vec::with_capacity(dropped.len());
        let mut partition_names = Vec::with_capacity(dropped.len());
        for def in dropped {
            let (table_count, _, _) = self.store.stats_meta_count_and_modify_count(def.id)?;
            count = count.wrapping_add(table_count);
            partition_ids.push(def.id);
            partition_names.push(def.name.clone());
        }
        if count == 0 {
            return Ok(());
        }
        let schema = self.schema_name(global_table.id);
        let locked = self.store.query_locked_tables()?;
        let is_locked = locked.contains(&global_table.id);
        let start_ts = self.session.start_ts()?;
        let delta = -count;
        let audit = TruncatePartitionAudit {
            schema,
            table_id: global_table.id,
            table_name: global_table.name.clone(),
            partition_ids,
            partition_names,
            count,
            delta,
            start_ts,
            is_locked,
        };
        let result = self.store.update_stats_meta(
            start_ts,
            DeltaUpdate {
                table_id: global_table.id,
                count,
                delta,
                is_locked,
            },
        );
        self.truncate_audits.push(audit);
        result
    }

    /// `updateGlobalTableStats4ExchangePartition` (`subscriber.go:442`).
    pub fn update_global_stats_for_exchange_partition(
        &mut self,
        global_table: &DdlTableInfo,
        original_partition: &PartitionDef,
        original_table: &DdlTableInfo,
    ) -> DdlResult<()> {
        let (part_count, part_modify_count, table_count, table_modify_count) =
            self.counts_and_modify_counts(original_partition.id, original_table.id)?;
        let (count_delta, modify_count_delta) = exchange_partition_deltas(
            part_count,
            part_modify_count,
            table_count,
            table_modify_count,
        );
        if count_delta == 0 && modify_count_delta == 0 {
            return Ok(());
        }
        let global_table_schema = self.schema_name(global_table.id);
        let audit = ExchangePartitionAudit {
            global_table_schema,
            global_table_id: global_table.id,
            global_table_name: global_table.name.clone(),
            count_delta,
            modify_count_delta,
            partition_id: original_partition.id,
            partition_name: original_partition.name.clone(),
            partition_count: part_count,
            partition_modify_count: part_modify_count,
            table_id: original_table.id,
            table_name: original_table.name.clone(),
            table_count,
            table_modify_count,
        };
        let result = self.update_stats_with_count_delta_and_modify_count_delta(
            global_table.id,
            count_delta,
            modify_count_delta,
        );
        // Go logs the same field set on both the success and failure paths.
        self.exchange_audits.push(audit);
        result
    }

    /// `updateStatsWithCountDeltaAndModifyCountDelta` (`ddl.go:98`).
    ///
    /// Locked tables take the `stats_table_locked` upsert and never read the
    /// current row, so their counts may go negative; unlocked tables read
    /// `... FOR UPDATE` first and clamp at zero in SQL.
    pub fn update_stats_with_count_delta_and_modify_count_delta(
        &mut self,
        table_id: i64,
        count_delta: i64,
        modify_count_delta: i64,
    ) -> DdlResult<()> {
        let locked = self.store.query_locked_tables()?;
        let is_locked = locked.contains(&table_id);
        let start_ts = self.session.start_ts()?;
        let existing = if is_locked {
            None
        } else {
            let (count, modify_count, is_null) = self
                .store
                .stats_meta_count_and_modify_count_for_update(table_id)?;
            if is_null {
                None
            } else {
                Some((count, modify_count))
            }
        };
        let update = ddl_stats_delta_update(
            is_locked,
            existing,
            start_ts,
            table_id,
            count_delta,
            modify_count_delta,
        );
        self.store.execute_stats_delta(update)
    }
}

#[cfg(test)]
mod tests {
    // New coverage: the Go tests for this package
    // (`pkg/statistics/handle/ddl/ddl_test.go`) drive a real cluster through
    // `testkit`, so they cannot be ported. These tests instead pin the
    // dispatch, ordering, short-circuits, and delta arithmetic against a
    // recording store.
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Effect {
        InsertTableStats(i64),
        InsertColumnStats(i64, Vec<i64>),
        BumpVersion(i64),
        CountAndModify(i64),
        CountAndModifyForUpdate(i64),
        QueryLocked,
        UpdateStatsMeta(u64, DeltaUpdate),
        StatsDelta(DdlStatsDeltaUpdate),
        ChangeGlobalStatsId(i64, i64),
        UpdateStatsVersion,
        RecordHistorical(i64, u64),
    }

    struct Session {
        dynamic: bool,
        historical: bool,
        start_ts: u64,
        schema: Option<String>,
    }

    impl Default for Session {
        fn default() -> Self {
            Self {
                dynamic: true,
                historical: false,
                start_ts: 700,
                schema: Some("test".to_owned()),
            }
        }
    }

    impl DdlSessionPort for Session {
        fn dynamic_prune_mode(&self) -> DdlResult<bool> {
            Ok(self.dynamic)
        }
        fn enable_historical_stats(&self) -> DdlResult<bool> {
            Ok(self.historical)
        }
        fn start_ts(&self) -> DdlResult<u64> {
            Ok(self.start_ts)
        }
        fn schema_name_by_table(&self, _table_id: i64) -> Option<String> {
            self.schema.clone()
        }
    }

    #[derive(Default)]
    struct Store {
        effects: Vec<Effect>,
        write_ts: u64,
        counts: std::collections::BTreeMap<i64, (i64, i64, bool)>,
        locked: BTreeSet<i64>,
        fail_bump: BTreeSet<i64>,
    }

    impl Store {
        fn count_of(&self, id: i64) -> (i64, i64, bool) {
            self.counts.get(&id).copied().unwrap_or((0, 0, true))
        }
    }

    impl DdlStatsStorePort for Store {
        fn insert_table_stats(&mut self, _table: &DdlTableInfo, id: i64) -> DdlResult<u64> {
            self.effects.push(Effect::InsertTableStats(id));
            Ok(self.write_ts)
        }
        fn insert_column_stats(&mut self, id: i64, column_ids: &[i64]) -> DdlResult<u64> {
            self.effects
                .push(Effect::InsertColumnStats(id, column_ids.to_vec()));
            Ok(self.write_ts)
        }
        fn bump_stats_meta_and_hist_version(&mut self, id: i64) -> DdlResult<u64> {
            self.effects.push(Effect::BumpVersion(id));
            if self.fail_bump.contains(&id) {
                return Err(DdlError(format!("bump failed for {id}")));
            }
            Ok(self.write_ts)
        }
        fn stats_meta_count_and_modify_count(&mut self, id: i64) -> DdlResult<(i64, i64, bool)> {
            self.effects.push(Effect::CountAndModify(id));
            Ok(self.count_of(id))
        }
        fn stats_meta_count_and_modify_count_for_update(
            &mut self,
            id: i64,
        ) -> DdlResult<(i64, i64, bool)> {
            self.effects.push(Effect::CountAndModifyForUpdate(id));
            Ok(self.count_of(id))
        }
        fn query_locked_tables(&mut self) -> DdlResult<BTreeSet<i64>> {
            self.effects.push(Effect::QueryLocked);
            Ok(self.locked.clone())
        }
        fn update_stats_meta(&mut self, start_ts: u64, update: DeltaUpdate) -> DdlResult<()> {
            self.effects.push(Effect::UpdateStatsMeta(start_ts, update));
            Ok(())
        }
        fn execute_stats_delta(&mut self, update: DdlStatsDeltaUpdate) -> DdlResult<()> {
            self.effects.push(Effect::StatsDelta(update));
            Ok(())
        }
        fn change_global_stats_id(&mut self, old_id: i64, new_id: i64) -> DdlResult<()> {
            self.effects
                .push(Effect::ChangeGlobalStatsId(old_id, new_id));
            Ok(())
        }
        fn update_stats_version(&mut self) -> DdlResult<()> {
            self.effects.push(Effect::UpdateStatsVersion);
            Ok(())
        }
        fn record_historical_stats_meta(&mut self, id: i64, start_ts: u64) -> DdlResult<()> {
            self.effects.push(Effect::RecordHistorical(id, start_ts));
            Ok(())
        }
    }

    struct Cache {
        initialized: BTreeSet<i64>,
    }

    impl DdlStatsCachePort for Cache {
        fn is_initialized(&self, physical_id: i64) -> bool {
            self.initialized.contains(&physical_id)
        }
    }

    fn cache(ids: impl IntoIterator<Item = i64>) -> Cache {
        Cache {
            initialized: ids.into_iter().collect(),
        }
    }

    fn run(session: &Session, store: &mut Store, cache: &Cache, event: &StatsSchemaChangeEvent) {
        let mut subscriber = DdlSubscriber::new(session, store, cache);
        subscriber.handle(event).expect("handle must succeed");
    }

    fn partitioned() -> DdlTableInfo {
        DdlTableInfo::partitioned(
            10,
            "t",
            vec![PartitionDef::new(11, "p0"), PartitionDef::new(12, "p1")],
        )
    }

    #[test]
    fn create_table_dynamic_mode_appends_global_id_after_partitions() {
        let session = Session::default();
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::CreateTable {
                table: partitioned(),
            },
        );
        assert_eq!(
            store.effects,
            vec![
                Effect::InsertTableStats(11),
                Effect::InsertTableStats(12),
                Effect::InsertTableStats(10),
            ]
        );
    }

    #[test]
    fn create_table_static_mode_omits_global_id() {
        let session = Session {
            dynamic: false,
            ..Session::default()
        };
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::CreateTable {
                table: partitioned(),
            },
        );
        assert_eq!(
            store.effects,
            vec![Effect::InsertTableStats(11), Effect::InsertTableStats(12)]
        );
    }

    #[test]
    fn non_partitioned_table_never_reads_the_prune_mode() {
        let session = Session::default();
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::CreateTable {
                table: DdlTableInfo::non_partitioned(5, "t"),
            },
        );
        assert_eq!(store.effects, vec![Effect::InsertTableStats(5)]);
    }

    #[test]
    fn partitioned_table_with_no_definitions_still_differs_from_nil() {
        let session = Session {
            dynamic: false,
            ..Session::default()
        };
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::CreateTable {
                table: DdlTableInfo::partitioned(7, "t", vec![]),
            },
        );
        assert!(store.effects.is_empty());
    }

    #[test]
    fn truncate_table_inserts_new_then_retires_dropped() {
        let session = Session {
            dynamic: false,
            ..Session::default()
        };
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::TruncateTable {
                new_table: DdlTableInfo::non_partitioned(21, "t"),
                dropped_table: DdlTableInfo::non_partitioned(20, "t"),
            },
        );
        assert_eq!(
            store.effects,
            vec![Effect::InsertTableStats(21), Effect::BumpVersion(20)]
        );
    }

    #[test]
    fn historical_stats_recorded_only_when_enabled_cached_and_written() {
        for (historical, write_ts, cached, expect) in [
            (false, 900u64, true, false),
            (true, 0, true, false),
            (true, 900, false, false),
            (true, 900, true, true),
        ] {
            let session = Session {
                historical,
                ..Session::default()
            };
            let mut store = Store {
                write_ts,
                ..Store::default()
            };
            let cache = cache(if cached { vec![5] } else { vec![] });
            run(
                &session,
                &mut store,
                &cache,
                &StatsSchemaChangeEvent::CreateTable {
                    table: DdlTableInfo::non_partitioned(5, "t"),
                },
            );
            assert_eq!(
                store.effects.contains(&Effect::RecordHistorical(5, 900)),
                expect,
                "historical={historical} write_ts={write_ts} cached={cached}"
            );
        }
    }

    #[test]
    fn modify_column_skips_when_ddl_already_analyzed() {
        let session = Session::default();
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::ModifyColumn {
                new_table: DdlTableInfo::non_partitioned(5, "t"),
                column_ids: vec![1],
                analyzed: true,
            },
        );
        assert!(store.effects.is_empty());

        let mut store = Store::default();
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::ModifyColumn {
                new_table: DdlTableInfo::non_partitioned(5, "t"),
                column_ids: vec![1],
                analyzed: false,
            },
        );
        assert_eq!(store.effects, vec![Effect::InsertColumnStats(5, vec![1])]);
    }

    #[test]
    fn drop_partition_subtracts_the_summed_count_from_global_stats() {
        let session = Session::default();
        let mut store = Store {
            counts: [(11, (30, 4, false)), (12, (12, 1, false))]
                .into_iter()
                .collect(),
            ..Store::default()
        };
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::DropTablePartition {
                global_table: partitioned(),
                dropped_partitions: vec![PartitionDef::new(11, "p0"), PartitionDef::new(12, "p1")],
            },
        );
        assert_eq!(
            store.effects,
            vec![
                Effect::CountAndModify(11),
                Effect::CountAndModify(12),
                Effect::QueryLocked,
                Effect::UpdateStatsMeta(
                    700,
                    DeltaUpdate {
                        table_id: 10,
                        count: 42,
                        delta: -42,
                        is_locked: false,
                    }
                ),
                Effect::BumpVersion(11),
                Effect::BumpVersion(12),
            ]
        );
    }

    #[test]
    fn zero_total_count_short_circuits_before_the_lock_query() {
        let session = Session::default();
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::DropTablePartition {
                global_table: partitioned(),
                dropped_partitions: vec![PartitionDef::new(11, "p0")],
            },
        );
        assert_eq!(
            store.effects,
            vec![Effect::CountAndModify(11), Effect::BumpVersion(11)]
        );
    }

    #[test]
    fn truncate_partition_inserts_new_updates_global_then_retires_old() {
        let session = Session::default();
        let mut store = Store {
            counts: [(11, (25, 3, false))].into_iter().collect(),
            locked: [10].into_iter().collect(),
            ..Store::default()
        };
        let cache = cache([]);
        let mut subscriber = DdlSubscriber::new(&session, &mut store, &cache);
        subscriber
            .handle(&StatsSchemaChangeEvent::TruncateTablePartition {
                global_table: partitioned(),
                added_partitions: vec![PartitionDef::new(13, "p2")],
                dropped_partitions: vec![PartitionDef::new(11, "p0")],
            })
            .expect("handle");
        let audit = subscriber.truncate_audits.first().cloned().expect("audit");
        assert_eq!(
            store.effects,
            vec![
                Effect::InsertTableStats(13),
                Effect::CountAndModify(11),
                Effect::QueryLocked,
                Effect::UpdateStatsMeta(
                    700,
                    DeltaUpdate {
                        table_id: 10,
                        count: 25,
                        delta: -25,
                        is_locked: true,
                    }
                ),
                Effect::BumpVersion(11),
            ]
        );
        assert_eq!(
            audit,
            TruncatePartitionAudit {
                schema: "test".to_owned(),
                table_id: 10,
                table_name: "t".to_owned(),
                partition_ids: vec![11],
                partition_names: vec!["p0".to_owned()],
                count: 25,
                delta: -25,
                start_ts: 700,
                is_locked: true,
            }
        );
    }

    #[test]
    fn missing_schema_falls_back_to_the_not_found_placeholder() {
        let session = Session {
            schema: None,
            ..Session::default()
        };
        let mut store = Store {
            counts: [(11, (25, 3, false))].into_iter().collect(),
            ..Store::default()
        };
        let cache = cache([]);
        let mut subscriber = DdlSubscriber::new(&session, &mut store, &cache);
        subscriber
            .update_global_stats_for_truncate_partition(
                &partitioned(),
                &[PartitionDef::new(11, "p0")],
            )
            .expect("truncate");
        assert_eq!(subscriber.truncate_audits[0].schema, SCHEMA_NOT_FOUND);
    }

    #[test]
    fn exchange_partition_delta_formula_matches_the_source() {
        // total = table_count - part_count + new_table_count, and the modify
        // delta charges both the delete and the add.
        assert_eq!(exchange_partition_deltas(10, 2, 30, 5), (20, 43));
        assert_eq!(exchange_partition_deltas(0, 0, 0, 0), (0, 0));
        // A pure swap of equal counts still records modify-count churn.
        assert_eq!(exchange_partition_deltas(7, 0, 7, 0), (0, 14));
    }

    #[test]
    fn exchange_partition_takes_the_unlocked_update_branch() {
        let session = Session::default();
        let mut store = Store {
            counts: [
                (11, (10, 2, false)),
                (99, (30, 5, false)),
                (10, (4, 1, false)),
            ]
            .into_iter()
            .collect(),
            ..Store::default()
        };
        let cache = cache([]);
        let mut subscriber = DdlSubscriber::new(&session, &mut store, &cache);
        subscriber
            .handle(&StatsSchemaChangeEvent::ExchangeTablePartition {
                global_table: partitioned(),
                original_partition: PartitionDef::new(11, "p0"),
                original_table: DdlTableInfo::non_partitioned(99, "nt"),
            })
            .expect("handle");
        let audit = subscriber.exchange_audits.first().cloned().expect("audit");
        let Some(Effect::StatsDelta(delta)) = store.effects.last().cloned() else {
            panic!("expected a stats-delta execution, got {:?}", store.effects);
        };
        assert_eq!(
            delta,
            DdlStatsDeltaUpdate {
                query: crate::ddl_stats_delta::EXISTING_STATS_META_DELTA_QUERY,
                start_ts: 700,
                count_value: 4 + 20,
                modify_count_value: 1 + 43,
                table_id: 10,
            }
        );
        assert_eq!(audit.count_delta, 20);
        assert_eq!(audit.modify_count_delta, 43);
        assert_eq!(audit.partition_name, "p0");
        assert_eq!(audit.table_name, "nt");
    }

    #[test]
    fn exchange_partition_locked_table_skips_the_for_update_read() {
        let session = Session::default();
        let mut store = Store {
            counts: [(11, (10, 2, false)), (99, (30, 5, false))]
                .into_iter()
                .collect(),
            locked: [10].into_iter().collect(),
            ..Store::default()
        };
        let cache = cache([]);
        let mut subscriber = DdlSubscriber::new(&session, &mut store, &cache);
        subscriber
            .handle(&StatsSchemaChangeEvent::ExchangeTablePartition {
                global_table: partitioned(),
                original_partition: PartitionDef::new(11, "p0"),
                original_table: DdlTableInfo::non_partitioned(99, "nt"),
            })
            .expect("handle");
        assert!(!store.effects.contains(&Effect::CountAndModifyForUpdate(10)));
        let Some(Effect::StatsDelta(delta)) = store.effects.last().cloned() else {
            panic!("expected a stats-delta execution");
        };
        assert_eq!(
            delta.query,
            crate::ddl_stats_delta::LOCKED_STATS_DELTA_QUERY
        );
        assert_eq!(delta.count_value, 20);
    }

    #[test]
    fn exchange_partition_with_zero_deltas_writes_nothing() {
        let session = Session::default();
        let mut store = Store {
            counts: [(11, (0, 0, false)), (99, (0, 0, false))]
                .into_iter()
                .collect(),
            ..Store::default()
        };
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::ExchangeTablePartition {
                global_table: partitioned(),
                original_partition: PartitionDef::new(11, "p0"),
                original_table: DdlTableInfo::non_partitioned(99, "nt"),
            },
        );
        assert_eq!(
            store.effects,
            vec![Effect::CountAndModify(11), Effect::CountAndModify(99)]
        );
    }

    #[test]
    fn reorganize_partition_leaves_global_stats_alone() {
        let session = Session::default();
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::ReorganizePartition {
                global_table: partitioned(),
                added_partitions: vec![PartitionDef::new(13, "p2")],
                dropped_partitions: vec![PartitionDef::new(11, "p0")],
            },
        );
        assert_eq!(
            store.effects,
            vec![Effect::InsertTableStats(13), Effect::BumpVersion(11)]
        );
    }

    #[test]
    fn alter_table_partitioning_changes_the_global_id_last() {
        let session = Session::default();
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::AlterTablePartitioning {
                old_single_table_id: 3,
                global_table: partitioned(),
                added_partitions: vec![PartitionDef::new(11, "p0")],
            },
        );
        assert_eq!(
            store.effects,
            vec![
                Effect::InsertTableStats(11),
                Effect::ChangeGlobalStatsId(3, 10),
            ]
        );
    }

    #[test]
    fn remove_partitioning_changes_the_global_id_first() {
        let session = Session::default();
        let mut store = Store::default();
        let cache = cache([]);
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::RemovePartitioning {
                old_table_id: 10,
                new_single_table: DdlTableInfo::non_partitioned(40, "t"),
                dropped_partitions: vec![PartitionDef::new(11, "p0")],
            },
        );
        assert_eq!(
            store.effects,
            vec![Effect::ChangeGlobalStatsId(10, 40), Effect::BumpVersion(11),]
        );
    }

    #[test]
    fn flashback_and_add_index_branches() {
        let session = Session::default();
        let cache = cache([]);
        let mut store = Store::default();
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::FlashbackCluster,
        );
        assert_eq!(store.effects, vec![Effect::UpdateStatsVersion]);

        let mut store = Store::default();
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::AddIndex,
        );
        assert!(store.effects.is_empty());

        let mut store = Store::default();
        run(
            &session,
            &mut store,
            &cache,
            &StatsSchemaChangeEvent::Unhandled,
        );
        assert!(store.effects.is_empty());
    }

    #[test]
    fn drop_schema_visits_partitions_before_tables_and_survives_failures() {
        let session = Session::default();
        let mut store = Store {
            fail_bump: [11].into_iter().collect(),
            ..Store::default()
        };
        let cache = cache([]);
        let mut subscriber = DdlSubscriber::new(&session, &mut store, &cache);
        subscriber
            .handle(&StatsSchemaChangeEvent::DropSchema {
                tables: vec![
                    DroppedSchemaTable {
                        id: 10,
                        partition_ids: vec![11, 12],
                    },
                    DroppedSchemaTable {
                        id: 20,
                        partition_ids: vec![],
                    },
                ],
            })
            .expect("drop schema is best effort");
        assert_eq!(subscriber.suppressed_errors.len(), 1);
        assert_eq!(
            store.effects,
            vec![
                Effect::BumpVersion(11),
                Effect::BumpVersion(12),
                Effect::BumpVersion(10),
                Effect::BumpVersion(20),
            ]
        );
    }

    #[test]
    fn handle_ddl_event_swallows_errors_and_classifies_them() {
        let session = Session::default();
        let cache = cache([]);
        let mut store = Store {
            fail_bump: [5].into_iter().collect(),
            ..Store::default()
        };
        let mut subscriber = DdlSubscriber::new(&session, &mut store, &cache);
        let (error, expected) = subscriber.handle_ddl_event(&StatsSchemaChangeEvent::DropTable {
            dropped_table: DdlTableInfo::non_partitioned(5, "t"),
        });
        assert!(error.is_some());
        assert!(!expected, "an arbitrary storage failure is not expected");
    }

    #[test]
    fn expected_ddl_event_errors_match_the_source_allow_list() {
        assert!(is_expected_ddl_event_error("context canceled"));
        assert!(is_expected_ddl_event_error(
            "injected: mock handleTaskOnce error"
        ));
        assert!(is_expected_ddl_event_error("session pool closed"));
        assert!(!is_expected_ddl_event_error("table doesn't exist"));
    }

    #[test]
    fn ddl_event_channel_capacity_matches_the_source() {
        assert_eq!(DDL_EVENT_CHANNEL_CAPACITY, 1000);
    }
}
