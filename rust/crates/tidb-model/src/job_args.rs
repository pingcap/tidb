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

//! The independently reachable pieces of `pkg/meta/model/job_args.go`.
//!
//! The accepted Rust architecture deliberately does not yet expose Go's
//! `Job`, `RawArgs`, `Encode`, `Decode`, or `FillArgs` boundary.  The adjacent
//! lockdown ledger therefore ports the source rules that do not cross that
//! boundary and classifies every boundary-dependent obligation explicitly.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columnar_index_type_preserves_all_source_boundaries() {
        assert_eq!(
            index_arg_columnar_index_type(ColumnarIndexType::NA, false),
            ColumnarIndexType::NA
        );
        assert_eq!(
            index_arg_columnar_index_type(ColumnarIndexType::NA, true),
            ColumnarIndexType::VECTOR
        );
        for explicit in [
            ColumnarIndexType::INVERTED,
            ColumnarIndexType::VECTOR,
            ColumnarIndexType::FULLTEXT,
            ColumnarIndexType(255),
        ] {
            assert_eq!(index_arg_columnar_index_type(explicit, false), explicit);
            assert_eq!(index_arg_columnar_index_type(explicit, true), explicit);
        }
    }

    #[test]
    fn rename_parallel_slices_keep_source_order_and_json_boundaries() {
        let args = rename_tables_args_from_v1(
            &[11, i64::MIN],
            &[CiString::new("OldDB"), CiString::new("Σ")],
            &[CiString::new("OldT"), CiString::new("İ")],
            &[22, i64::MAX],
            &[CiString::new("NewT"), CiString::new("")],
            &[33, -1],
        );
        assert_eq!(args.len(), 2);
        assert_eq!(args[0].old_schema_name.original(), "OldDB");
        assert_eq!(args[1].old_schema_name.lowercase(), "σ");
        assert_eq!(args[1].old_table_name.lowercase(), "i");
        assert_eq!(args[1].new_schema_id, i64::MAX);
        assert_eq!(args[1].table_id, -1);

        let encoded = serde_json::to_value(&args[0]).expect("rename args serialize");
        assert_eq!(encoded["old_schema_id"], 11);
        assert_eq!(encoded["old_schema_name"]["O"], "OldDB");
        assert_eq!(encoded["old_schema_name"]["L"], "olddb");
        assert!(encoded.get("old_schema_id_for_schema_diff").is_none());

        let zero =
            serde_json::to_value(RenameTableArgs::default()).expect("zero rename args serialize");
        assert!(zero.get("old_schema_id").is_none());
        assert_eq!(zero["old_schema_name"], serde_json::json!({"O":"","L":""}));
    }

    #[test]
    fn rename_empty_and_mismatched_parallel_slices_match_go() {
        assert!(rename_tables_args_from_v1(&[], &[], &[], &[], &[], &[]).is_empty());
        let name = CiString::new("x");
        for missing in 0..5 {
            let old_schema_names = (missing != 0)
                .then_some(name.clone())
                .into_iter()
                .collect::<Vec<_>>();
            let old_table_names = (missing != 1)
                .then_some(name.clone())
                .into_iter()
                .collect::<Vec<_>>();
            let new_schema_ids = (missing != 2).then_some(2).into_iter().collect::<Vec<_>>();
            let new_table_names = (missing != 3)
                .then_some(name.clone())
                .into_iter()
                .collect::<Vec<_>>();
            let table_ids = (missing != 4).then_some(3).into_iter().collect::<Vec<_>>();
            let missing_parallel = std::panic::catch_unwind(|| {
                rename_tables_args_from_v1(
                    &[1],
                    &old_schema_names,
                    &old_table_names,
                    &new_schema_ids,
                    &new_table_names,
                    &table_ids,
                )
            });
            assert!(missing_parallel.is_err(), "parallel slice {missing}");
        }
    }

    #[test]
    fn index_operation_values_keep_go_iota_and_byte_width() {
        assert_eq!(IndexOp::ADD_INDEX.0, 0);
        assert_eq!(IndexOp::DROP_INDEX.0, 1);
        assert_eq!(IndexOp::ROLLBACK_ADD_INDEX.0, 2);
        assert_eq!(serde_json::to_string(&IndexOp(255)).unwrap(), "255");
    }
}
