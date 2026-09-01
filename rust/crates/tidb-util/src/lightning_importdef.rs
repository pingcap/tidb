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

//! Target database metadata from Go `pkg/lightning/importdef`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tidb_datatype::GoString;
use tidb_model::TableInfo as ModelTableInfo;

/// Go `DBInfo`: a target TiDB database for import.
#[derive(Default)]
pub struct DbInfo {
    /// Go `DBInfo.ID`.
    pub id: i64,
    /// Go `DBInfo.Name`.
    pub name: GoString,
    /// Go `DBInfo.Tables`; `None` is a nil map.
    pub tables: Option<HashMap<GoString, Arc<RwLock<TableInfo>>>>,
}

/// Go `TableInfo`: target and desired TiDB table metadata for import.
#[derive(Clone, Default)]
pub struct TableInfo {
    /// Go `TableInfo.ID`.
    pub id: i64,
    /// Go `TableInfo.DB`.
    pub db: GoString,
    /// Go `TableInfo.Name`.
    pub name: GoString,
    /// Go `TableInfo.Core`: the current table metadata in TiDB.
    pub core: Option<Arc<RwLock<ModelTableInfo>>>,
    /// Go `TableInfo.Desired`: the table metadata to migrate to.
    pub desired: Option<Arc<RwLock<ModelTableInfo>>>,
}

impl PartialEq for TableInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.db == other.db
            && self.name == other.name
            && same_pointer(&self.core, &other.core)
            && same_pointer(&self.desired, &other.desired)
    }
}

impl Eq for TableInfo {}

fn same_pointer<T>(left: &Option<Arc<T>>, right: &Option<Arc<T>>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        _ => false,
    }
}
