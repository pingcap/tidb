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

//! Compatibility helpers from `pkg/meta/model/job_args.go` whose source
//! shapes are independent of the typed `JobArgs` receiver codec.

use serde::{Deserialize, Serialize};
use tidb_ast::CiString;

use crate::ColumnarIndexType;

/// Go `IndexOp` (a byte), used by version-1 index arguments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IndexOp(pub u8);

impl IndexOp {
    /// Go `OpAddIndex`.
    pub const ADD_INDEX: Self = Self(0);
    /// Go `OpDropIndex`.
    pub const DROP_INDEX: Self = Self(1);
    /// Go `OpRollbackAddIndex`.
    pub const ROLLBACK_ADD_INDEX: Self = Self(2);
}

/// Go `IndexArg.GetColumnarIndexType`, expressed over the only two fields the
/// source rule reads so that it does not fabricate the still-deferred AST
/// argument surface.
#[must_use]
pub fn index_arg_columnar_index_type(
    columnar_index_type: ColumnarIndexType,
    is_columnar: bool,
) -> ColumnarIndexType {
    if columnar_index_type == ColumnarIndexType::NA && is_columnar {
        ColumnarIndexType::VECTOR
    } else {
        columnar_index_type
    }
}

/// Go `RenameTableArgs`, the data boundary consumed by
/// `GetRenameTablesArgsFromV1`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameTableArgs {
    /// Original schema identifier.
    #[serde(rename = "old_schema_id", default, skip_serializing_if = "is_zero_i64")]
    pub old_schema_id: i64,
    /// Original schema name.
    #[serde(rename = "old_schema_name")]
    pub old_schema_name: CiString,
    /// Destination table name.
    #[serde(rename = "new_table_name")]
    pub new_table_name: CiString,
    /// Original table name (used by multi-table rename).
    #[serde(rename = "old_table_name")]
    pub old_table_name: CiString,
    /// Destination schema identifier.
    #[serde(rename = "new_schema_id", default, skip_serializing_if = "is_zero_i64")]
    pub new_schema_id: i64,
    /// Table identifier.
    #[serde(rename = "table_id", default, skip_serializing_if = "is_zero_i64")]
    pub table_id: i64,
    /// Runtime-only Go field (`json:"-"`).
    #[serde(skip)]
    pub old_schema_id_for_schema_diff: i64,
}

const fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

/// Go `GetRenameTablesArgsFromV1`.
///
/// Length mismatches intentionally panic at the first missing parallel entry,
/// exactly like Go's unchecked slice indexing. Extra entries are ignored
/// because the source iterates only `oldSchemaIDs`.
#[must_use]
pub fn rename_tables_args_from_v1(
    old_schema_ids: &[i64],
    old_schema_names: &[CiString],
    old_table_names: &[CiString],
    new_schema_ids: &[i64],
    new_table_names: &[CiString],
    table_ids: &[i64],
) -> Vec<RenameTableArgs> {
    old_schema_ids
        .iter()
        .enumerate()
        .map(|(index, old_schema_id)| RenameTableArgs {
            old_schema_id: *old_schema_id,
            old_schema_name: old_schema_names[index].clone(),
            old_table_name: old_table_names[index].clone(),
            new_schema_id: new_schema_ids[index],
            new_table_name: new_table_names[index].clone(),
            table_id: table_ids[index],
            old_schema_id_for_schema_diff: 0,
        })
        .collect()
}
