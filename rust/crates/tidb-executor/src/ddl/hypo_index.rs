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

//! The Index Advisor's `addHypoIndex`; the hypothetical-index state itself is
//! `tidb_table::hypo_index` (Go `SessionVars.HypoIndexes`).

use tidb_hack::GoToLower;
use tidb_model::index::{IndexColumn, IndexInfo};

use crate::{Catalog, DriverError};
pub use tidb_table::hypo_index::{HypoIndexMap, HypoIndexes, HypoTiFlashReplicas};

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
        return Err(DriverError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&table_name).into()]),
        ));
    };
    // Go: each column's offset in `tInfo.Cols()`, the public columns.
    let public = table.meta().read().cols();
    let mut parts = Vec::with_capacity(columns.len());
    for column in columns {
        let Some(offset) = public
            .iter_deref()
            .position(|stored| stored.read().name.lowercase() == column.go_to_lower())
        else {
            return Err(DriverError::Mysql(crate::driver::MysqlError::unknown(
                format!("column {column} not found in table {database}.{table_name}"),
            )));
        };
        parts.push(IndexColumn {
            name: tidb_ast::CiString::new(column.clone()),
            offset: i64::try_from(offset).unwrap_or(0),
            length: tidb_datatype::UNSPECIFIED_LENGTH,
            ..IndexColumn::default()
        });
    }
    let index = IndexInfo {
        name: tidb_ast::CiString::new(index_name),
        columns: parts.into(),
        state: tidb_model::SchemaState::PUBLIC,
        tp: tidb_ast::IndexType::HYPO,
        ..IndexInfo::default()
    };
    let name = index.name.lowercase().to_owned();
    let _ = hypo_indexes.remove(database, table_name, &name, true);
    hypo_indexes
        .add(database, table_name, index)
        .map_err(DriverError::Mysql)
}



