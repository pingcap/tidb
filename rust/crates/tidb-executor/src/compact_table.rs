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

//! Go `pkg/executor/compact_table.go`: `ALTER TABLE ... COMPACT`.

use tidb_ast::{CompactReplicaKind, CompactTableStmt};
use tidb_hack::GoCaseCompare;

use crate::{Catalog, DriverError, StmtContext, TableEntry};

/// Go `executorBuilder.buildCompactTable` and `CompactTableTiFlashExec.Next`
/// over the `CompactTable` plan: the replica kind and the partition list are
/// checked, and a table with no TiFlash replica is skipped with a warning. A
/// replica is compacted on every TiFlash store PD reports, and this tier has
/// none, so there is nothing to send.
///
/// # Errors
///
/// A missing table (the preprocessor's 1146), a TiKV replica kind, a
/// partition list over an unpartitioned table, or an unknown partition.
pub fn execute_compact_table(
    compact: &CompactTableStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<(), DriverError> {
    let (database, name) = crate::driver::split_table_path_pub(&compact.table, current_db)?;
    let Some(entry) = catalog.table_in(database, name) else {
        return Err(DriverError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
        ));
    };
    if compact.replica_kind == CompactReplicaKind::TiKv {
        return Err(DriverError::unsupported("compact tikv replica is not supported".to_owned()));
    }
    let table = match entry {
        TableEntry::Kv(table) => Some(table),
        _ => None,
    };
    let table_name = table.map_or_else(|| name.to_owned(), |table| table.name());
    if !compact.partition_names.is_empty() {
        let Some(partition) = table.and_then(|table| table.partition()) else {
            return Err(DriverError::unsupported(format!(
                "table:{table_name} is not a partition table, but user specify partition name list:[{}]",
                compact.partition_names.join(" ")
            )));
        };
        if let Some(unknown) = compact.partition_names.iter().find(|written| {
            !partition
                .definitions
                .iter()
                .any(|definition| definition.name.go_lower_eq(written))
        }) {
            return Err(DriverError::from(
                tidb_error::catalog::table::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(&[
                    unknown.clone().into(),
                    table_name.to_owned().into(),
                ]),
            ));
        }
    }
    if table
        .and_then(|table| table.tiflash_replica())
        .is_none_or(|replica| replica.count == 0)
    {
        ctx.append_warning_parts(1105, "compact skipped: no tiflash replica in the table");
    }
    Ok(())
}
