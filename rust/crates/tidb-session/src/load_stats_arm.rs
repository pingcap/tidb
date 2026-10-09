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

//! `LOAD STATS 'file.json'` over this session's own catalog.
//!
//! Go splits the statement in three: `LoadStatsExec.Next` only records the
//! path in the session, the server's `handleLoadStats` asks the CLIENT for
//! the file's bytes over the protocol's local-file transfer, and
//! `LoadStatsInfo.Update` parses them and hands the table to
//! `LoadStatsFromJSON`. The front end that owns this session supplies the
//! client half through [`Session::set_client_local_file_reader`]; without one
//! the statement is refused, as a server whose client cannot send files
//! refuses it.

use std::sync::Arc;

use tidb_executor::load_stats::{
    parse_stats_json, statistics_table_from_json, table_statistics_from_table, JsonTable,
    TIDB_GLOBAL_STATS,
};
use tidb_executor::{DriverError, SchemaErrorKind, TableEntry};

use crate::{Session, StmtOutput};

/// The client side of Go's local-file transfer: the bytes of the file the
/// statement names, as the connection's client reads them.
pub type ClientLocalFileReader = Arc<dyn Fn(&str) -> std::io::Result<Vec<u8>> + Send + Sync>;

impl Session {
    /// Installs the client half of the protocol's local-file transfer, which
    /// `LOAD STATS` reads its dump through.
    pub fn set_client_local_file_reader(&mut self, reader: ClientLocalFileReader) {
        self.client_local_file_reader = Some(reader);
    }

    /// Go `LoadStatsExec.Next`, `clientConn.handleLoadStats` and
    /// `LoadStatsInfo.Update` (`executor/load_stats.go`).
    pub(crate) fn load_stats_stmt(&mut self, path: &str) -> Result<StmtOutput, DriverError> {
        if path.is_empty() {
            return Err(DriverError::unsupported("Load Stats: file path is empty"));
        }
        let Some(reader) = self.client_local_file_reader.clone() else {
            return Err(DriverError::unsupported(
                "LOAD STATS requires client-local file transfer",
            ));
        };
        let data = reader(path).map_err(|error| {
            DriverError::unsupported(format!("Load Stats: read {path}: {error}"))
        })?;
        let json = parse_stats_json(&String::from_utf8_lossy(&data))
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        // Go skips a dump of `null`: no table name and no version.
        if json.table_name.is_empty() && json.version == 0 {
            return Ok(StmtOutput::Affected(0));
        }
        self.with_catalog_mut(|catalog| load_stats_from_json(catalog, &json))?;
        Ok(StmtOutput::Affected(0))
    }
}

/// Go `statsReadWriter.LoadStatsFromJSON` (`stats_read_writer.go:558`): the
/// dump's OWN database and table name select the target; a partitioned
/// table's dump feeds each partition it names plus its `global` entry.
///
/// Go writes the loaded items over the stored ones and reloads the table, so
/// an item the dump omits keeps its earlier statistics, while the dump's
/// count and modify count replace the table's meta row.
fn load_stats_from_json(
    catalog: &mut tidb_executor::Catalog,
    json: &JsonTable,
) -> Result<(), DriverError> {
    let table = match catalog.table_in(&json.database_name, &json.table_name) {
        Some(TableEntry::Kv(table)) => table.clone(),
        _ => {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                "{}.{}",
                json.database_name, json.table_name
            ))));
        }
    };
    let mut targets: Vec<(i64, &JsonTable)> = Vec::new();
    match (table.partition(), json.partitions.as_ref()) {
        (Some(partition), Some(partitions)) => {
            for definition in &partition.definitions {
                if let Some(Some(partition_json)) =
                    partitions.get(&definition.name.to_lowercase())
                {
                    targets.push((definition.id, partition_json));
                }
            }
            if let Some(global) = partitions.get(TIDB_GLOBAL_STATS) {
                let global = global.as_ref().ok_or_else(|| {
                    DriverError::unsupported("Load Stats: the global statistics are null")
                })?;
                targets.push((table.table_id, global));
            }
        }
        _ => targets.push((table.table_id, json)),
    }
    for (physical_id, target) in targets {
        let stats = statistics_table_from_json(&table, physical_id, target)
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        let loaded = table_statistics_from_table(&stats, &table);
        let merged = match catalog.table_statistics(physical_id) {
            Some(existing) => merge_loaded_statistics(&existing, loaded),
            None => loaded,
        };
        catalog.set_table_statistics(physical_id, Arc::new(merged));
    }
    Ok(())
}

/// Overlays the dump's items on the table's earlier statistics: Go's
/// `SaveColOrIdxStatsToStorage` replaces only the histograms the dump
/// carries, and the reload reads every stored item back.
fn merge_loaded_statistics(
    existing: &tidb_executor::access_cost::TableStatistics,
    loaded: tidb_executor::access_cost::TableStatistics,
) -> tidb_executor::access_cost::TableStatistics {
    let mut merged = existing.clone();
    merged.pseudo = loaded.pseudo && existing.pseudo;
    merged.cache_pseudo = loaded.cache_pseudo && existing.cache_pseudo;
    merged.row_count = loaded.row_count;
    merged.modify_count = loaded.modify_count;
    if loaded.stats_ver != 0 {
        merged.stats_ver = loaded.stats_ver;
    }
    merged.columns.extend(loaded.columns);
    merged.indexes.extend(loaded.indexes);
    merged.column_fm_sketches.extend(loaded.column_fm_sketches);
    merged.index_fm_sketches.extend(loaded.index_fm_sketches);
    merged.column_load_status.extend(loaded.column_load_status);
    merged.index_load_status.extend(loaded.index_load_status);
    merged.column_stats_existence.extend(loaded.column_stats_existence);
    merged.index_stats_existence.extend(loaded.index_stats_existence);
    merged
}
