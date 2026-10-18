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

//! Hypothetical indexes and TiFlash replicas for the Index Advisor
//! (`pkg/ddl/executor.go` `addHypoIndexIntoCtx` / `dropHypoIndexFromCtx` /
//! `setHypoTiFlashReplica`).
//!
//! Neither is schema: Go keeps them in `SessionVars.HypoIndexes` and
//! `SessionVars.HypoTiFlashReplicas`, keyed by the lowercase database and
//! table NAMES, so they outlive a dropped table and apply to a new table of
//! the same name. Only `EXPLAIN` planning (`getPossibleAccessPaths`,
//! `UsedHypoTiFlashReplicas`) and `SHOW CREATE TABLE` read them.

use std::collections::{BTreeMap, BTreeSet};

use tidb_hack::GoToLower;

use super::indexes::{
    build_index_definition, is_visible, unsupported_partial_index, BuiltIndex, IndexAdmission,
    IndexSpec,
};
use crate::{Catalog, DriverError, KvIndex};

/// Go `SessionVars.HypoIndexes`: `dbName -> tblName -> idxName -> idxInfo`,
/// every key lowercase.
#[derive(Clone, Debug, Default)]
pub struct HypoIndexes {
    by_table: BTreeMap<String, BTreeMap<String, BTreeMap<String, KvIndex>>>,
    /// How many ids [`Self::add`] has handed out.
    allocated_ids: i64,
}

impl HypoIndexes {
    /// Whether no hypothetical index is defined anywhere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_table
            .values()
            .all(|tables| tables.values().all(BTreeMap::is_empty))
    }

    /// The hypothetical indexes of `database`.`table`, ordered by Go's
    /// `Name.O` -- the order `SHOW CREATE TABLE` sorts them into "to make the
    /// result stable" (`show.go`). Go's access-path loop walks the map in
    /// random order; this one order serves both.
    #[must_use]
    pub fn table_indexes(&self, database: &str, table: &str) -> Vec<&KvIndex> {
        let mut indexes: Vec<&KvIndex> = self
            .by_table
            .get(&database.go_to_lower())
            .and_then(|tables| tables.get(&table.go_to_lower()))
            .map(|indexes| indexes.values().collect())
            .unwrap_or_default();
        indexes.sort_by(|left, right| left.name.cmp(&right.name));
        indexes
    }

    /// Go `addHypoIndexIntoCtx`.
    ///
    /// Go leaves a hypothetical index's id 0 and every plan carries the
    /// `*IndexInfo` itself. Plans here name their index by id, so each one
    /// takes its own negative id, which no stored index can have; ids never
    /// reach EXPLAIN's text.
    fn add(&mut self, database: &str, table: &str, mut index: KvIndex) -> Result<(), DriverError> {
        let indexes = self
            .by_table
            .entry(database.go_to_lower())
            .or_default()
            .entry(table.go_to_lower())
            .or_default();
        let name = index.name.go_to_lower();
        if indexes.contains_key(&name) {
            // Go `errors.Errorf("conflict hypo index name %s", indexName.L)`.
            return Err(DriverError::Mysql(crate::driver::MysqlError::new(
                1105,
                format!("conflict hypo index name {name}"),
            )));
        }
        self.allocated_ids += 1;
        index.id = -self.allocated_ids;
        indexes.insert(name, index);
        Ok(())
    }

    /// Go `dropHypoIndexFromCtx`.
    fn remove(
        &mut self,
        database: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), DriverError> {
        let removed = self
            .by_table
            .get_mut(&database.go_to_lower())
            .and_then(|tables| tables.get_mut(&table.go_to_lower()))
            .and_then(|indexes| indexes.remove(&index.go_to_lower()));
        if removed.is_none() && !if_exists {
            return Err(DriverError::UnknownIndex(index.to_owned()));
        }
        Ok(())
    }
}

/// Go `SessionVars.HypoTiFlashReplicas`: `dbName -> tblName`, lowercase.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HypoTiFlashReplicas {
    by_database: BTreeMap<String, BTreeSet<String>>,
}

impl HypoTiFlashReplicas {
    /// Whether no hypothetical replica is defined anywhere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_database.values().all(BTreeSet::is_empty)
    }

    /// The membership test of Go `UsedHypoTiFlashReplicas`.
    #[must_use]
    pub fn contains(&self, database: &str, table: &str) -> bool {
        self.by_database
            .get(&database.go_to_lower())
            .is_some_and(|tables| tables.contains(&table.go_to_lower()))
    }
}

/// Go `createIndex` for an `IndexOption.Tp == IndexTypeHypo` statement: every
/// check up to the hypo branch, then `BuildIndexInfo` (no id is allocated)
/// and `addHypoIndexIntoCtx`. No job is submitted and the table is untouched.
pub fn create_hypo_index(
    catalog: &Catalog,
    current_db: &str,
    create: &tidb_ast::CreateIndexStmt,
    ctx: &crate::StmtContext,
    hypo_indexes: &mut HypoIndexes,
) -> Result<(), DriverError> {
    let (database, table_name) = crate::driver::split_table_path_pub(&create.table, current_db)?;
    super::refuse_local_temporary_table_ddl(catalog, database, table_name, "CREATE INDEX")?;
    let unique = match create.kind {
        tidb_ast::IndexKind::Ordinary => false,
        tidb_ast::IndexKind::Unique => true,
        _ => {
            return Err(DriverError::unsupported(
                "SPATIAL, FULLTEXT, VECTOR and COLUMNAR indexes are not supported yet",
            ))
        }
    };
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    if table.is_cache_table() {
        return Err(DriverError::OperationOnCachedTable("Create Index"));
    }
    if create.options.condition.is_some() && table.partition().is_some() {
        return Err(unsupported_partial_index(
            "partial index on partitioned table is not supported",
        ));
    }
    let spec = IndexSpec {
        name: &create.name,
        comment: create.options.comment.as_deref().unwrap_or(""),
        unique,
        parts: &create.parts,
        visible: is_visible(&create.options),
        global: create.options.global,
        if_not_exists: create.if_not_exists,
        condition: create.options.condition.as_ref(),
    };
    let BuiltIndex { index, hidden } =
        match build_index_definition(table, spec, ctx, catalog.max_index_length())? {
            IndexAdmission::Change(built) => built,
            IndexAdmission::Note(note) => {
                ctx.append_suppressed(&note);
                return Ok(());
            }
        };
    // Go builds a hypothetical expression index over `finalColumns`, a COPY
    // of the table's columns with the hidden ones appended, and keeps only
    // the `IndexInfo`. A `KvIndex` names its columns by offset into the
    // table itself, so it cannot name a column the table does not hold.
    if !hidden.is_empty() {
        return Err(DriverError::unsupported(
            "a hypothetical expression index is not supported",
        ));
    }
    hypo_indexes.add(database, table_name, index)
}

/// The Index Advisor's `addHypoIndex` (`pkg/planner/indexadvisor/optimizer.go`):
/// a public, non-unique hypothetical index over `columns`, built straight
/// from the table's columns with no key-length admission and replacing any
/// hypothetical index of the same name, as the advisor's own map write does.
pub fn set_advisor_hypo_index(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    index_name: &str,
    columns: &[String],
    hypo_indexes: &mut HypoIndexes,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let mut offsets = Vec::with_capacity(columns.len());
    for column in columns {
        let wanted = column.go_to_lower();
        let Some(offset) = table
            .columns()
            .iter()
            .position(|stored| stored.name.go_to_lower() == wanted)
        else {
            return Err(DriverError::Mysql(crate::driver::MysqlError::new(
                1105,
                format!("column {column} not found in table {database}.{table_name}"),
            )));
        };
        offsets.push(offset);
    }
    let index = KvIndex {
        id: 0,
        name: index_name.to_owned(),
        comment: String::new(),
        unique: false,
        prefix_lengths: vec![super::index_prefix::UNSPECIFIED_LENGTH; offsets.len()],
        column_offsets: offsets,
        visible: true,
        global: false,
        global_index_version: 0,
        clustered_primary: false,
    };
    let name = index.name.go_to_lower();
    let _ = hypo_indexes.remove(database, table_name, &name, true);
    hypo_indexes.add(database, table_name, index)
}

/// Go `dropIndex` with `isHypo`: the table must exist and not be cached, and
/// then `dropHypoIndexFromCtx`.
pub fn drop_hypo_index(
    catalog: &Catalog,
    current_db: &str,
    drop: &tidb_ast::DropIndexStmt,
    hypo_indexes: &mut HypoIndexes,
) -> Result<(), DriverError> {
    let (database, table_name) = crate::driver::split_table_path_pub(&drop.table, current_db)?;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        if drop.if_exists {
            return Ok(());
        }
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    if table.is_cache_table() {
        return Err(DriverError::OperationOnCachedTable("Drop Index"));
    }
    hypo_indexes.remove(database, table_name, &drop.name, drop.if_exists)
}

/// Go `AlterTableSetTiFlashReplica` with `replicaInfo.Hypo`:
/// `isTableTiFlashSupported`, `shouldModifyTiFlashReplica`, then
/// `setHypoTiFlashReplica`.
pub fn set_hypo_tiflash_replica(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    count: u64,
    labels: &[String],
    replicas: &mut HypoTiFlashReplicas,
) -> Result<(), DriverError> {
    let table = match catalog.table_in(database, table_name) {
        None => {
            return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                format!("{database}.{table_name}"),
            )))
        }
        Some(crate::TableEntry::Kv(table)) => Some(table),
        Some(_) => None,
    };
    is_table_tiflash_supported(database, table_name, table.map(|table| &**table))?;
    let table = table.expect("isTableTiFlashSupported refuses every non-table entry");
    if !should_modify_tiflash_replica(table.tiflash_replica(), count, labels) {
        return Ok(());
    }
    let tables = replicas
        .by_database
        .entry(database.go_to_lower())
        .or_default();
    if count > 0 {
        tables.insert(table_name.go_to_lower());
    } else {
        tables.remove(&table_name.go_to_lower());
    }
    Ok(())
}

/// Go `isTableTiFlashSupported`. `table` is `None` for a view or a sequence.
fn is_table_tiflash_supported(
    database: &str,
    table_name: &str,
    table: Option<&crate::KvTable>,
) -> Result<(), DriverError> {
    let unsupported = |what: String| {
        DriverError::Mysql(crate::driver::MysqlError::new(
            tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
            format!("Unsupported {what}"),
        ))
    };
    if tidb_metadef::db::is_mem_or_sys_db(&database.go_to_lower()) {
        return Err(unsupported(
            "`set TiFlash replica` settings for system table and memory table".to_owned(),
        ));
    }
    let Some(table) = table else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::WrongObject {
            name: format!("{database}.{table_name}"),
            expected: "BASE TABLE",
        }));
    };
    if table.temp_table_type() != tidb_model::TempTableType::NONE {
        return Err(DriverError::OptOnTemporaryTable("set TiFlash replica"));
    }
    for column in table.columns() {
        let charset = column.field_type.charset();
        let charset = charset.name();
        if !tidb_datatype::TIFLASH_SUPPORTED_CHARSETS.contains(&charset) {
            return Err(unsupported(format!(
                "`set TiFlash replica` settings for table contains {charset} charset"
            )));
        }
    }
    Ok(())
}

/// Go `shouldModifyTiFlashReplica`.
fn should_modify_tiflash_replica(
    current: Option<&tidb_model::table::TiFlashReplicaInfo>,
    count: u64,
    labels: &[String],
) -> bool {
    match current {
        Some(current)
            if current.count == count && current.location_labels.len() == labels.len() =>
        {
            current
                .location_labels
                .iter()
                .zip(labels)
                .any(|(current, wanted)| current != wanted)
        }
        _ => true,
    }
}
