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

//! Go `keydecoder`'s schema lookups over the image (`TIDB_DECODE_KEY`).

use crate::*;

impl tidb_keydecoder::KeyInfoCatalog for Catalog {
    fn resolve_physical_table(
        &self,
        physical_id: i64,
    ) -> Option<tidb_keydecoder::KeyInfoTableLookup> {
        for database in self.schema.databases.values() {
            for (registered_name, entry) in &database.tables {
                let TableEntry::Kv(table) = entry.as_ref() else {
                    continue;
                };
                if table.table_id() == physical_id {
                    return Some(tidb_keydecoder::KeyInfoTableLookup::Resolved(
                        key_info_table(database, registered_name, table, 0, String::new()),
                    ));
                }
            }
        }
        for database in self.schema.databases.values() {
            for (registered_name, entry) in &database.tables {
                let TableEntry::Kv(table) = entry.as_ref() else {
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
                    return Some(tidb_keydecoder::KeyInfoTableLookup::Resolved(
                        key_info_table(
                            database,
                            registered_name,
                            table,
                            definition.id,
                            definition.name.clone(),
                        ),
                    ));
                }
            }
        }
        None
    }
}

fn key_info_table(
    database: &Database,
    registered_name: &str,
    table: &KvTable,
    partition_id: i64,
    partition_name: String,
) -> tidb_keydecoder::KeyInfoTable {
    tidb_keydecoder::KeyInfoTable {
        db_name: database.name.clone(),
        db_id: database.id,
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
            .map(|index| tidb_keydecoder::KeyInfoIndex {
                id: index.id,
                name: index.name.clone(),
            })
            .collect(),
    }
}
