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

//! Go `pkg/infoschema/builder.go`'s `drivers`: the virtual schemas
//! (`information_schema`, `performance_schema`, `metrics_schema`) each
//! package registers once (`RegisterVirtualTable`), which every infoschema
//! then serves beside the stored ones (`initVirtualTables`).

use std::sync::{Mutex, OnceLock};

use tidb_hack::GoToLower;
use tidb_model::db::DBInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::GoShared;

fn drivers() -> &'static Mutex<Vec<&'static DBInfo>> {
    static DRIVERS: OnceLock<Mutex<Vec<&'static DBInfo>>> = OnceLock::new();
    DRIVERS.get_or_init(Mutex::default)
}

/// Go `RegisterVirtualTable`: adds the virtual driver's `DBInfo`, whose
/// `Deprecated.Tables` are its tables, to every infoschema. A schema
/// registered again (by name) keeps its first registration.
pub fn register_virtual_table(db_info: DBInfo) {
    let mut drivers = drivers().lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let name = db_info.name.lowercase().to_owned();
    if drivers
        .iter()
        .any(|registered| registered.name.lowercase() == name)
    {
        return;
    }
    drivers.push(Box::leak(Box::new(db_info)));
}

/// The registered virtual schema named `schema` (case-insensitive) with its
/// tables, the part of Go `InfoSchema.SchemaByName` and
/// `SchemaTableInfos` the virtual drivers answer.
#[must_use]
pub fn virtual_schema(schema: &str) -> Option<&'static DBInfo> {
    let schema = schema.go_to_lower();
    let drivers = drivers().lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    drivers
        .iter()
        .find(|registered| registered.name.lowercase() == schema)
        .copied()
}

/// The registered virtual table `schema`.`table` (case-insensitive), the
/// part of Go `InfoSchema.TableByName` the virtual drivers answer.
#[must_use]
pub fn virtual_table(schema: &str, table: &str) -> Option<(&'static DBInfo, GoShared<TableInfo>)> {
    let db_info = virtual_schema(schema)?;
    let table = table.go_to_lower();
    db_info
        .deprecated_tables
        .iter_deref()
        .find(|stored| stored.read().name.lowercase() == table)
        .map(|stored| (db_info, stored))
}
