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

//! Persisted-cluster catalog adapter for `pkg/util/keydecoder`.

pub use tidb_executor::keydecoder::{
    decode_key, DecodedKey, HandleType, KeyDecoderError, KeyDecoderFailure, KeyInfoCatalog,
    KeyInfoIndex, KeyInfoTable, KeyInfoTableLookup,
};

use crate::cluster_catalog::ClusterCatalog;

impl KeyInfoCatalog for ClusterCatalog {
    fn resolve_physical_table(&self, physical_id: i64) -> Option<KeyInfoTableLookup> {
        for database in &self.databases {
            if let Some(table) = database.tables.iter().find(|table| table.id == physical_id) {
                return Some(KeyInfoTableLookup::Resolved(table_info(
                    database,
                    table,
                    0,
                    String::new(),
                )));
            }
        }
        for database in &self.databases {
            for table in &database.tables {
                let Some(partition) = table.partition.as_ref() else {
                    continue;
                };
                let partition = partition.read();
                if let Some(definition) = partition
                    .definitions
                    .snapshot()
                    .into_iter()
                    .find(|definition| definition.id == physical_id)
                {
                    return Some(KeyInfoTableLookup::Resolved(table_info(
                        database,
                        table,
                        definition.id,
                        definition.name.original().to_owned(),
                    )));
                }
            }
        }
        None
    }
}

fn table_info(
    database: &crate::cluster_catalog::LoadedDatabase,
    table: &tidb_model::table_info::TableInfo,
    partition_id: i64,
    partition_name: String,
) -> KeyInfoTable {
    KeyInfoTable {
        db_name: database.info.name.original().to_owned(),
        db_id: database.info.id,
        table_name: table.name.original().to_owned(),
        table_id: table.id,
        partition_name,
        partition_id,
        indexes: table
            .indices
            .iter_deref()
            .map(|index| {
                let index = index.read();
                KeyInfoIndex {
                    id: index.id,
                    name: index.name.original().to_owned(),
                }
            })
            .collect(),
    }
}
