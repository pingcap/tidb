// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `br/pkg/metautil/load.go`.
use super::{MetaReader, ReadSchemaConfig, Table};
use std::collections::BTreeMap;
use tidb_gocontext::Context;

/// Go `Database`.
pub struct Database {
    /// Go `Info`.
    pub info: tidb_model::DBInfo,
    /// Go `Tables`.
    pub tables: Vec<Table>,
    reused_by_pitr: bool,
}
impl Database {
    /// Go `Database.SetReusedByPITR`.
    pub fn set_reused_by_pitr(&mut self) {
        self.reused_by_pitr = true;
    }
    /// Go `Database.IsReusedByPITR`.
    pub fn is_reused_by_pitr(&self) -> bool {
        self.reused_by_pitr
    }
    /// Go `Database.GetTable`: case-sensitive original name comparison.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|table| {
            table
                .info
                .as_ref()
                .is_some_and(|info| info.name.original() == name)
        })
    }
}
/// Go `LoadBackupTables`.
pub fn load_backup_tables(
    ctx: &Context,
    reader: &MetaReader,
    load_stats: bool,
) -> Result<BTreeMap<String, Database>, String> {
    let mut databases = BTreeMap::new();
    for table in reader.read_schemas_files_with_options(
        ctx,
        ReadSchemaConfig {
            skip_stats: !load_stats,
            skip_files: false,
        },
    )? {
        let db = databases
            .entry(table.db.name.original().to_owned())
            .or_insert_with(|| Database {
                info: table.db.clone(),
                tables: Vec::new(),
                reused_by_pitr: false,
            });
        db.tables.push(table);
    }
    Ok(databases)
}

#[cfg(test)]
mod tests;
