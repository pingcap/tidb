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
use tidb_executor::show_regions::policy_placement;
use tidb_executor::{Catalog, TableEntry};
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_model::PlacementSettings;

use crate::{DriverError, SchemaErrorKind, Session};

const PENDING: &str = "PENDING";
const SCHEDULED: &str = "SCHEDULED";

fn row(target: String, placement: &PlacementSettings, state: &str) -> Vec<Datum> {
    vec![
        Datum::Bytes(target.into_bytes()),
        Datum::Bytes(placement.to_string().into_bytes()),
        Datum::Bytes(state.as_bytes().to_vec()),
    ]
}

/// One table's placement rows, which Go builds table first and its
/// partitions after (`fetchAllTablePlacements`): the table's own row, then
/// each partition's.
fn table_rows(
    catalog: &Catalog,
    database: &str,
    entry: &TableEntry,
    table_name: &str,
) -> Result<(Option<Vec<Datum>>, Vec<Vec<Datum>>), DriverError> {
    let TableEntry::Kv(table) = entry else {
        return Ok((None, Vec::new()));
    };
    let table_placement = policy_placement(catalog, table.placement_policy())?;
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

impl Session {
    /// The unfiltered rows of one SHOW PLACEMENT form.
    pub(crate) fn placement_rows(
        &mut self,
        target: &tidb_ast::ShowPlacementTarget,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        match target {
            tidb_ast::ShowPlacementTarget::All => self.all_placement_rows(),
            tidb_ast::ShowPlacementTarget::Database(database) => {
                self.require_visible_database(database)?;
                self.with_catalog_mut(|catalog| {
                    let Some(name) = catalog
                        .database_names()
                        .into_iter()
                        .find(|name| name.go_lower_eq(database))
                    else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                            database.clone(),
                        )));
                    };
                    Ok(database_rows(catalog, &name)?.into_iter().collect())
                })
            }
            tidb_ast::ShowPlacementTarget::Table(path) => {
                let (database, table_name) = self.placement_table_name(path)?;
                self.require_show_create_privilege(&database, &table_name)?;
                self.with_catalog_mut(|catalog| {
                    let (database, entry_name, entry) =
                        placement_table(catalog, &database, &table_name)?;
                    table_rows(catalog, &database, entry, &entry_name)
                        .map(|(own, _)| own.into_iter().collect())
                })
            }
            tidb_ast::ShowPlacementTarget::Partition { table, partition } => {
                let (database, table_name) = self.placement_table_name(table)?;
                self.require_show_create_privilege(&database, &table_name)?;
                self.with_catalog_mut(|catalog| {
                    let (database, entry_name, entry) =
                        placement_table(catalog, &database, &table_name)?;
                    let unknown = || DriverError::UnknownPartition {
                        partition: partition.clone(),
                        table: entry_name.clone(),
                    };
                    let TableEntry::Kv(table) = entry else {
                        return Err(unknown());
                    };
                    let Some(partitioning) = table.partition() else {
                        return Err(unknown());
                    };
                    let Some(definition) = partitioning
                        .definitions
                        .iter()
                        .find(|definition| definition.name.go_lower_eq(partition))
                    else {
                        return Err(unknown());
                    };
                    let table_placement = policy_placement(catalog, table.placement_policy())?;
                    let placement =
                        policy_placement(catalog, definition.placement_policy.as_ref())?
                            .or(table_placement);
                    Ok(placement
                        .map(|placement| {
                            row(
                                format!(
                                    "TABLE {database}.{entry_name} PARTITION {}",
                                    definition.name
                                ),
                                &placement,
                                PENDING,
                            )
                        })
                        .into_iter()
                        .collect())
                })
            }
            // Go `fetchShowPlacementLabels`, whose restricted SQL fails with
            // the store-status retriever's plain error.
            tidb_ast::ShowPlacementTarget::Labels => {
                let store = self.with_catalog_mut(|catalog| Ok(catalog.region_store()))?;
                crate::memtable_retrievers::show_placement_labels_rows(store.as_ref()).map_err(
                    |message| DriverError::Mysql(tidb_executor::MysqlError::unknown(message)),
                )
            }
        }
    }

    fn placement_table_name(&self, path: &[String]) -> Result<(String, String), DriverError> {
        match path {
            [table] => Ok((self.require_current_database()?.to_owned(), table.clone())),
            [database, table] => Ok((database.clone(), table.clone())),
            _ => Err(DriverError::unsupported("empty table name")),
        }
    }

    /// Go `fetchShowPlacement`: policies, then databases, then tables.
    fn all_placement_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        struct TableSet {
            database: String,
            name: String,
            rows: Vec<Vec<Datum>>,
        }
        let (mut rows, databases, tables) = self.with_catalog_mut(|catalog| {
            let mut policies: Vec<_> = catalog.policies().collect();
            policies.sort_by(|a, b| a.name.original().cmp(b.name.original()));
            let rows: Vec<Vec<Datum>> = policies
                .into_iter()
                .map(|policy| {
                    vec![
                        Datum::Bytes(format!("POLICY {}", policy.name.original()).into_bytes()),
                        Datum::Bytes(
                            policy
                                .placement_settings
                                .as_ref()
                                .map(|settings| settings.read().to_string())
                                .unwrap_or_default()
                                .into_bytes(),
                        ),
                        Datum::Bytes(b"NULL".to_vec()),
                    ]
                })
                .collect();
            let mut names = catalog.database_names();
            names.sort();
            let mut databases = Vec::new();
            for name in &names {
                databases.push((name.clone(), database_rows(catalog, name)?));
            }
            // The v2 infoschema walks its table btree downwards, so the
            // schemas come out by lower-cased name, last first.
            let mut by_lower = names.clone();
            by_lower.sort_by_key(|name| std::cmp::Reverse(name.go_to_lower()));
            let mut tables = Vec::new();
            for database in by_lower {
                let mut sets = Vec::new();
                for table_name in catalog.table_names(&database).unwrap_or_default() {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        continue;
                    };
                    let (own, partitions) = table_rows(catalog, &database, entry, &table_name)?;
                    let rows: Vec<_> = own.into_iter().chain(partitions).collect();
                    if !rows.is_empty() {
                        sets.push(TableSet {
                            database: database.clone(),
                            name: table_name,
                            rows,
                        });
                    }
                }
                sets.sort_by(|a, b| a.name.cmp(&b.name));
                tables.extend(sets);
            }
            Ok((rows, databases, tables))
        })?;
        for (database, database_rows) in databases {
            if self.database_is_visible(&database) {
                rows.extend(database_rows);
            }
        }
        for set in tables {
            if self.has_any_scoped_privilege(
                &set.database,
                &set.name,
                crate::privilege::any_priv_mask(),
            ) {
                rows.extend(set.rows);
            }
        }
        Ok(rows)
    }
}

/// The schema's own placement row, if it has a policy. Its state is
/// `PENDING` while any table exists.
fn database_rows(catalog: &Catalog, database: &str) -> Result<Option<Vec<Datum>>, DriverError> {
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
fn placement_table<'a>(
    catalog: &'a Catalog,
    database: &str,
    table_name: &str,
) -> Result<(String, String, &'a TableEntry), DriverError> {
    let missing = || {
        DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
            "{database}.{table_name}"
        )))
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
