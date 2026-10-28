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

//! The rows of `SHOW PLACEMENT [FOR DATABASE | TABLE | PARTITION]`, from Go's
//! `executor/show_placement.go`.
//!
//! The scheduling state of every object is what `infosync.GetReplicationState`
//! answers without a PD HTTP client: `PENDING`. A database with no table at
//! all never asks, so it stays at the accumulator's start, `SCHEDULED`.

use tidb_datatype::Datum;
use crate::show_regions::policy_placement;
use crate::{Catalog, TableEntry};
use tidb_hack::GoCaseCompare;
use tidb_model::PlacementSettings;

use crate::DriverError;

pub(crate) const PENDING: &str = "PENDING";
const SCHEDULED: &str = "SCHEDULED";

/// One `Target | Placement | Scheduling_State` row.
pub fn row(target: String, placement: &PlacementSettings, state: &str) -> Vec<Datum> {
    vec![
        Datum::Bytes(target.into_bytes()),
        Datum::Bytes(placement.to_string().into_bytes()),
        Datum::Bytes(state.as_bytes().to_vec()),
    ]
}

/// One table's placement rows, which Go builds table first and its
/// partitions after (`fetchAllTablePlacements`): the table's own row, then
/// each partition's.
pub fn table_rows(
    catalog: &Catalog,
    database: &str,
    entry: &TableEntry,
    table_name: &str,
) -> Result<(Option<Vec<Datum>>, Vec<Vec<Datum>>), DriverError> {
    let TableEntry::Kv(table) = entry else {
        return Ok((None, Vec::new()));
    };
    let table_placement = policy_placement(catalog, table.placement_policy().as_ref())?;
    let own = table_placement
        .as_ref()
        .map(|placement| row(format!("TABLE {database}.{table_name}"), placement, PENDING));
    let mut rows = Vec::new();
    if let Some(partitioning) = table.partition() {
        for definition in &partitioning.definitions {
            let placement = policy_placement(catalog, definition.placement_policy.as_ref())?
                .or_else(|| table_placement.clone());
            if let Some(placement) = placement {
                rows.push(row(
                    format!(
                        "TABLE {database}.{table_name} PARTITION {}",
                        definition.name
                    ),
                    &placement,
                    PENDING,
                ));
            }
        }
    }
    Ok((own, rows))
}

/// The schema's own placement row, if it has a policy. Its state is
/// `PENDING` while any table exists.
pub fn database_rows(catalog: &Catalog, database: &str) -> Result<Option<Vec<Datum>>, DriverError> {
    let Some(placement) = policy_placement(catalog, catalog.database_placement_policy(database))?
    else {
        return Ok(None);
    };
    let has_tables = catalog
        .table_names(database)
        .is_some_and(|names| !names.is_empty());
    Ok(Some(row(
        format!("DATABASE {database}"),
        &placement,
        if has_tables { PENDING } else { SCHEDULED },
    )))
}

/// The table `SHOW PLACEMENT FOR TABLE | PARTITION` names, with the schema and
/// table names as the catalog stores them.
pub fn placement_table<'a>(
    catalog: &'a Catalog,
    database: &str,
    table_name: &str,
) -> Result<(String, String, &'a TableEntry), DriverError> {
    let missing = || {
        DriverError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&table_name).into()]),
        )
    };
    let Some(entry) = catalog.table_in(database, table_name) else {
        return Err(missing());
    };
    let stored_database = catalog
        .database_names()
        .into_iter()
        .find(|name| name.go_lower_eq(database))
        .unwrap_or_else(|| database.to_owned());
    let stored_table = match entry {
        TableEntry::Kv(table) if !table.name().is_empty() => table.name().to_owned(),
        _ => table_name.to_owned(),
    };
    Ok((stored_database, stored_table, entry))
}
