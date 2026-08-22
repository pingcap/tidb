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

//! `LOAD STATS 'file.json'`: the `AdminStmt::LoadStats` arm of
//! [`crate::Session::dispatch_admin_stmt`].
//!
//! What the dump's bytes MEAN is [`tidb_executor::load_stats`]'s, mirroring
//! `pkg/statistics/handle/storage/json.go`. This arm is Go's OUTER layers:
//! `LoadStatsExec` + `LoadStatsInfo.Update` (`pkg/executor/load_stats.go`)
//! and the physical-table routing of
//! `statsReadWriter.LoadStatsFromJSONNoUpdate`
//! (`pkg/statistics/handle/storage/stats_read_writer.go`).
//!
//! # Where the file is read, and why the path is taken as given
//!
//! Go's executor never opens the file: `LoadStatsExec.Next` parks a
//! `LoadStatsInfo` under `LoadStatsVarKey`, and the server's connection layer
//! (`FileTransInConnHandlers`) fetches the bytes FROM THE CLIENT over the
//! local-infile protocol -- so a relative path resolves against the CLIENT's
//! working directory (mysql-tester runs from `tests/integrationtest/`, which
//! is how `load stats 's/...'` scripts work). This tier has no client to ask,
//! so it reads the path from the local filesystem exactly as written; a
//! harness that replays those scripts resolves the relative path before the
//! statement reaches the engine, because only the harness knows what
//! directory the recording's client ran in.
//!
//! # Which table, and which physical ids
//!
//! The dump names its own target: `LoadStatsFromJSONNoUpdate` looks up
//! `jsonTbl.DatabaseName`.`jsonTbl.TableName` in the infoschema, NOT the
//! session's current database. `tests/integrationtest/t/explain_complex.test`
//! depends on that: it runs `use test` before `load stats
//! 's/issue_50080.json'` because the dump says `"database_name": "test"`,
//! while the tpch fixtures name `tpch50` and load fine from whatever database
//! the session sits in.
//!
//! For a partitioned table WITH a `partitions` object, each entry loads into
//! the partition of the same name and the `global` entry into the logical
//! table id; a dump with a nil `partitions` loads into the logical table
//! directly even when the table is partitioned. Go tests `Partitions == nil`,
//! so present-but-empty takes the partition branch and installs NOTHING --
//! preserved here because changing it would make this tier accept dumps Go
//! rejects silently.
//!
//! # What is deliberately not here
//!
//! * **`mysql.stats_*` writes.** Go's `loadStatsFromJSON` persists each
//!   histogram through `SaveColOrIdxStatsToStorage` and the meta through
//!   `SaveMetaToStorage`, then `statsHandler.Update` republishes the cache.
//!   This tier's catalog IS its statistics store
//!   ([`tidb_executor::driver::Catalog::set_table_statistics`]), the same
//!   single publish `ANALYZE` uses (`crate::analyze_arm`) -- a second copy in
//!   table form would have no reader.
//! * **The `LoadStatsVarKey` handshake.** "previous load stats option isn't
//!   closed normally" guards Go's two-phase executor/connection split, which
//!   does not exist when the same call both parses and applies the file.

use std::sync::Arc;

use tidb_executor::load_stats::{
    parse_stats_json, table_statistics_from_json, JsonTable, TIDB_GLOBAL_STATS,
};
use tidb_executor::{DriverError, SchemaErrorKind, TableEntry};

use crate::{Session, StmtOutput};

impl Session {
    /// Runs `LOAD STATS 'path'`, Go `LoadStatsExec` + `handleLoadStats`.
    pub(crate) fn load_stats_stmt(
        &mut self,
        stmt: &tidb_ast::LoadStatsStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        // Go `LoadStatsExec.Next`: the one refusal the executor itself owns.
        if stmt.path.is_empty() {
            return Err(DriverError::unsupported("Load Stats: file path is empty"));
        }
        // Go `handleLoadStats`: an empty FILE (not an empty path) is a no-op,
        // not an error -- the recording's own scripts rely on a missing
        // optional dump loading nothing rather than failing the script.
        let data = std::fs::read_to_string(&stmt.path).map_err(|error| {
            DriverError::unsupported(format!("Load Stats: read {}: {error}", stmt.path))
        })?;
        if data.is_empty() {
            return Ok(Some(StmtOutput::Affected(0)));
        }
        let json = parse_stats_json(&data)
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        // Go `LoadStatsInfo.Update`'s guard, comment and all: "Check the
        // `jsonTbl` in cases where the stats file with `null`."
        if json.table_name.is_empty() && json.version == 0 {
            return Ok(Some(StmtOutput::Affected(0)));
        }
        self.install_stats_dump(&json)?;
        // Go answers `LOAD STATS` with an OK packet carrying no rows.
        Ok(Some(StmtOutput::Affected(0)))
    }

    /// Go `LoadStatsFromJSONNoUpdate`: resolve the named table, decide the
    /// physical ids, and publish one [`TableStatistics`] per id.
    ///
    /// [`TableStatistics`]: tidb_executor::access_cost::TableStatistics
    fn install_stats_dump(&mut self, json: &JsonTable) -> Result<(), DriverError> {
        let database = json.database_name.clone();
        let name = json.table_name.clone();
        self.with_catalog_mut(|catalog| {
            let Some(TableEntry::Kv(table)) = catalog.table_in(&database, &name) else {
                // Go's `is.TableByName` failure: `ErrTableNotExists` for the
                // dump's own database/table pair.
                return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(
                    format!("{database}.{name}"),
                )));
            };
            let table = table.clone();
            let partition_definitions: Vec<(i64, String)> = table
                .partition()
                .map(|partition| {
                    partition
                        .definitions
                        .iter()
                        .map(|definition| (definition.id, definition.name.clone()))
                        .collect()
                })
                .unwrap_or_default();

            // Go: `if pi == nil || jsonTbl.Partitions == nil` -- an
            // unpartitioned table, or a dump without a partitions object,
            // loads the top-level histograms into the table's own id.
            if partition_definitions.is_empty() || json.partitions.is_none() {
                let statistics = table_statistics_from_json(&table, json)
                    .map_err(|error| DriverError::unsupported(error.to_string()))?;
                catalog.set_table_statistics(table.table_id, Arc::new(statistics));
                return Ok(());
            }

            // The partition branch: Go queues one load task per partition
            // whose name has an entry (`jsonTbl.Partitions[def.Name.L]`),
            // plus the `global` entry for the logical table id. A partition
            // without an entry keeps whatever statistics it had.
            let partitions = json.partitions.as_ref().expect("checked above");
            for (physical_id, definition_name) in &partition_definitions {
                let Some(Some(entry)) = partitions.get(&definition_name.to_lowercase()) else {
                    continue;
                };
                let statistics = table_statistics_from_json(&table, entry)
                    .map_err(|error| DriverError::unsupported(error.to_string()))?;
                catalog.set_table_statistics(*physical_id, Arc::new(statistics));
            }
            if let Some(Some(global)) = partitions.get(TIDB_GLOBAL_STATS) {
                let statistics = table_statistics_from_json(&table, global)
                    .map_err(|error| DriverError::unsupported(error.to_string()))?;
                catalog.set_table_statistics(table.table_id, Arc::new(statistics));
            }
            Ok(())
        })
    }
}
