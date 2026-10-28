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

//! Go `keydecoder.DecodeKey`'s lookups over an `infoschema.InfoSchema`
//! (`TIDB_DECODE_KEY`): the statement's catalog image.

use tidb_infoschema::{Catalog, TableEntry};
use tidb_table::kv_table::KvTable;

use crate::{KeyInfoCatalog, KeyInfoIndex, KeyInfoTable, KeyInfoTableLookup};

impl KeyInfoCatalog for Catalog {
    fn resolve_physical_table(&self, physical_id: i64) -> Option<KeyInfoTableLookup> {
        for (database, registered_name, entry) in self.table_entries() {
            let TableEntry::Kv(table) = entry else {
                continue;
            };
            if table.table_id() == physical_id {
                return Some(KeyInfoTableLookup::Resolved(key_info_table(
                    self,
                    database,
                    registered_name,
                    table,
                    0,
                    String::new(),
                )));
            }
        }
        for (database, registered_name, entry) in self.table_entries() {
            let TableEntry::Kv(table) = entry else {
                continue;
            };
            let Some(partition) = table.partition() else {
                continue;
            };
            if let Some(definition) = partition
                .definitions
                .iter()
                .find(|definition| definition.id == physical_id)
            {
                return Some(KeyInfoTableLookup::Resolved(key_info_table(
                    self,
                    database,
                    registered_name,
                    table,
                    definition.id,
                    definition.name.clone(),
                )));
            }
        }
        None
    }
}

fn key_info_table(
    catalog: &Catalog,
    database: &str,
    registered_name: &str,
    table: &KvTable,
    partition_id: i64,
    partition_name: String,
) -> KeyInfoTable {
    KeyInfoTable {
        db_name: database.to_owned(),
        db_id: catalog.database_id(database).unwrap_or_default(),
        table_name: if table.name().is_empty() {
            registered_name.to_owned()
        } else {
            table.name().clone()
        },
        table_id: table.table_id(),
        partition_name,
        partition_id,
        indexes: table
            .indexes()
            .iter()
            .map(|index| KeyInfoIndex {
                id: index.id,
                name: index.name.clone(),
            })
            .collect(),
    }
}
