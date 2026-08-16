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

//! SEED of Go `pkg/ttl/cache/infoschema.go`: the map from physical table id to
//! the TTL view of that table, rebuilt whenever the schema version moves.
//!
//! `InfoSchemaCache`, `NewInfoSchemaCache`, `Update` and `newTable` all come
//! across, including the version short-circuit, the enable/state filter, the
//! per-partition fan-out, the identity reuse of an unchanged entry, and the
//! skip-and-log behaviour on a table that fails to build. The one narrowing is
//! [`TtlInfoSchema`], which stands in for the info schema itself.

use std::collections::HashMap;
use std::time::Duration;

use tidb_ast::CiString;
use tidb_model::{GoShared, PartitionDefinition, SchemaState, TableInfo};

use super::base::BaseCache;
use super::table::{new_physical_table, PhysicalTable};
use super::Result;
use crate::session::Session;

/// `// boundary:` `pkg/infoschema.InfoSchema` reached through
/// `pkg/infoschema/context.MetaOnlyInfoSchema`, plus the
/// `infoschemacontext.TTLAttribute` listing it is asked for.
///
/// `infoschema.go` calls exactly two things on the info schema —
/// `SchemaMetaVersion()` and `ListTablesWithSpecialAttribute(TTLAttribute)` —
/// so exactly those are declared. The listing's Go shape is a slice of
/// `(DBName, TableInfos)` groups, which is what
/// [`list_ttl_tables`](TtlInfoSchema::list_ttl_tables) returns.
pub trait TtlInfoSchema {
    /// Go `InfoSchema.SchemaMetaVersion`.
    fn schema_meta_version(&self) -> i64;

    /// Go `InfoSchema.ListTablesWithSpecialAttribute(infoschemacontext.TTLAttribute)`.
    fn list_ttl_tables(&self) -> Vec<(CiString, Vec<GoShared<TableInfo>>)>;
}

/// Go `InfoSchemaCache`.
pub struct InfoSchemaCache {
    base: BaseCache,
    schema_ver: i64,
    /// Go `Tables`.
    pub tables: HashMap<i64, PhysicalTable>,
}

impl InfoSchemaCache {
    /// Go `NewInfoSchemaCache`.
    #[must_use]
    pub fn new(update_interval: Duration) -> Self {
        Self {
            base: BaseCache::new(update_interval),
            schema_ver: 0,
            tables: HashMap::new(),
        }
    }

    /// Go's embedded `baseCache.ShouldUpdate`.
    #[must_use]
    pub fn should_update(&self) -> bool {
        self.base.should_update()
    }

    /// Go's embedded `baseCache.SetInterval`.
    pub fn set_interval(&mut self, interval: Duration) {
        self.base.set_interval(interval);
    }

    /// Go's embedded `baseCache.GetInterval`.
    #[must_use]
    pub fn get_interval(&self) -> Duration {
        self.base.get_interval()
    }

    /// Go `(*InfoSchemaCache).Update`.
    ///
    /// Go logs a warning and skips the table when `newTable` fails; the log
    /// sink is not part of this crate's dependency set, so the skip stays and
    /// the warning does not.
    pub fn update<S>(&mut self, se: &S) -> Result<()>
    where
        S: Session,
        S::InfoSchema: TtlInfoSchema,
    {
        let is = se.get_latest_info_schema();

        if self.schema_ver == is.schema_meta_version() {
            return Ok(());
        }

        let mut new_tables = HashMap::with_capacity(self.tables.len());

        for (db_name, table_infos) in is.list_ttl_tables() {
            for tbl_info in table_infos {
                {
                    let info = tbl_info.read();
                    let enabled = info
                        .ttl_info
                        .as_ref()
                        .is_some_and(|ttl_info| ttl_info.read().enable);
                    if !enabled || info.state != SchemaState::PUBLIC {
                        continue;
                    }
                }

                let partitions: Vec<PartitionDefinition> = {
                    let info = tbl_info.read();
                    match info.partition.as_ref() {
                        None => Vec::new(),
                        Some(partition) => partition.read().definitions.snapshot(),
                    }
                };

                if partitions.is_empty() && tbl_info.read().partition.is_none() {
                    let id = tbl_info.read().id;
                    if let Ok(ttl_table) = self.new_table(&db_name, &tbl_info, None) {
                        new_tables.insert(id, ttl_table);
                    }
                    continue;
                }

                for par in partitions {
                    let id = par.id;
                    if let Ok(ttl_table) = self.new_table(&db_name, &tbl_info, Some(&par)) {
                        new_tables.insert(id, ttl_table);
                    }
                }
            }
        }

        self.schema_ver = is.schema_meta_version();
        self.tables = new_tables;
        self.base.mark_updated();
        Ok(())
    }

    /// Go `(*InfoSchemaCache).newTable`.
    ///
    /// Go's fast path returns the cached entry when it points at the very same
    /// `*model.TableInfo`; `GoShared::ptr_eq` is that pointer comparison.
    fn new_table(
        &self,
        schema: &CiString,
        tbl_info: &GoShared<TableInfo>,
        par: Option<&PartitionDefinition>,
    ) -> Result<PhysicalTable> {
        let id = match par {
            Some(par) => par.id,
            None => tbl_info.read().id,
        };

        if let Some(ttl_table) = self.tables.get(&id) {
            if ttl_table.table_info_ptr_eq(tbl_info) {
                return Ok(ttl_table.clone());
            }
        }

        let partition_name = match par {
            Some(par) => par.name.clone(),
            None => CiString::new(""),
        };
        new_physical_table(schema.clone(), tbl_info, partition_name)
    }
}
