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

use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_table::tables::TableCommon;

use crate::{KeyInfoCatalog, KeyInfoIndex, KeyInfoTable, KeyInfoTableLookup};

impl KeyInfoCatalog for SessionExtendedInfoSchema {
    fn resolve_physical_table(&self, physical_id: i64) -> Option<KeyInfoTableLookup> {
        resolve_table(self, physical_id, |table| {
            table
                .indexes()
                .iter()
                .map(|index| {
                    let info = index.read();
                    KeyInfoIndex {
                        id: info.id,
                        name: info.name.original().to_owned(),
                    }
                })
                .collect()
        })
    }

    fn resolve_physical_table_for_key(
        &self,
        physical_id: i64,
        index_id: Option<i64>,
    ) -> Option<KeyInfoTableLookup> {
        resolve_table(self, physical_id, |table| {
            let Some(index_id) = index_id else {
                return Vec::new();
            };
            table
                .indexes()
                .iter()
                .find_map(|index| {
                    let info = index.read();
                    (info.id == index_id).then(|| KeyInfoIndex {
                        id: index_id,
                        name: info.name.original().to_owned(),
                    })
                })
                .into_iter()
                .collect()
        })
    }
}

fn resolve_table(
    catalog: &SessionExtendedInfoSchema,
    physical_id: i64,
    indexes: impl FnOnce(&TableCommon) -> Vec<KeyInfoIndex>,
) -> Option<KeyInfoTableLookup> {
    let (database, table) = catalog.table_by_id(physical_id)?;
    let table_id = table.table_id();
    let table_name = table.name();
    let (partition_id, partition_name) = if table_id == physical_id {
        (0, String::new())
    } else {
        let partition = table.meta().read().get_partition_info()?;
        let name = partition.read().definitions.with_visible(|definitions| {
            definitions
                .iter()
                .find(|definition| definition.id == physical_id)
                .map(|definition| definition.name.original().to_owned())
        })?;
        (physical_id, name)
    };
    let db = catalog
        .schema_by_name(database)
        .or_else(|| catalog.v2()?.schema_by_name(database));
    let (db_id, db_name) = match db {
        Some(db) => {
            let info = db.read();
            (info.id, info.name.original().to_owned())
        }
        None if partition_id == 0 => {
            return Some(KeyInfoTableLookup::TableWithoutSchema {
                table_name,
                table_id,
            });
        }
        None => (0, String::new()),
    };
    Some(KeyInfoTableLookup::Resolved(KeyInfoTable {
        db_name,
        db_id,
        table_name,
        table_id,
        partition_name,
        partition_id,
        indexes: indexes(table),
    }))
}
