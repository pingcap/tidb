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

//! Exact executor-side point-get helpers from `pkg/executor/point_get.go`.
//!
//! This module is deliberately smaller than Go's `PointGetExecutor`. The
//! current native executor has table metadata and partition definitions, so
//! the five pure metadata rules below are reachable and ported. It does not
//! expose Go's `sessionctx.Context`, pessimistic lock cache, `kv.Snapshot`,
//! chunk decoder, row-checksum writer, or snapshot runtime-stat interfaces.
//! The compact semantic package specification records the remaining boundary;
//! this module itself contains only executable behavior and focused tests.

use crate::{kv_table::KvColumn, PartitionDef};
use tidb_model::column::EXTRA_ROW_CHECKSUM_ID;

/// Go `GetPhysID` for every representable Rust partition ordinal.
///
/// `None` selects the logical table id. A supplied ordinal selects that
/// partition when partition metadata exists, otherwise it also falls back to
/// the logical table id. Indexing intentionally panics for an out-of-range
/// ordinal, as Go's direct `Definitions[*idx]` access does. Go's negative
/// `*int` assertion is structurally absent because Rust uses `usize`.
#[must_use]
pub fn physical_table_id(
    table_id: i64,
    definitions: Option<&[PartitionDef]>,
    partition_ordinal: Option<usize>,
) -> i64 {
    if let Some(ordinal) = partition_ordinal {
        if let Some(definitions) = definitions {
            return definitions[ordinal].id;
        }
    }
    table_id
}

/// Go `matchPartitionNames`.
///
/// An empty requested-name list accepts every physical id without touching
/// partition metadata. Otherwise only the first definition with `pid` is
/// considered, and names compare through the same Unicode-lowercase shape as
/// Go `ast.CIStr.L`.
#[must_use]
pub fn partition_name_matches(
    pid: i64,
    partition_names: &[String],
    definitions: Option<&[PartitionDef]>,
) -> bool {
    if partition_names.is_empty() {
        return true;
    }
    let definitions = definitions.expect("non-empty partition names require partition metadata");
    for definition in definitions {
        if definition.id == pid {
            let definition_name = definition.name.to_lowercase();
            for name in partition_names {
                if definition_name == name.to_lowercase() {
                    return true;
                }
            }
            return false;
        }
    }
    false
}

/// Go `shouldFillRowChecksum`: the first checksum pseudo-column, if present.
#[must_use]
pub fn row_checksum_column(columns: &[KvColumn]) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.id == EXTRA_ROW_CHECKSUM_ID)
}

/// Go `notPKPrefixCol`.
#[must_use]
pub fn not_primary_prefix_column(column_id: i64, prefix_column_ids: &[i64]) -> bool {
    !prefix_column_ids.contains(&column_id)
}

/// Go `getColInfoByID`: the first column with the requested id.
#[must_use]
pub fn column_by_id(columns: &[KvColumn], column_id: i64) -> Option<&KvColumn> {
    columns.iter().find(|column| column.id == column_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn definitions() -> Vec<PartitionDef> {
        vec![
            PartitionDef {
                id: 101,
                name: "P0".to_owned(),
            },
            PartitionDef {
                id: 202,
                name: "PÜNICODE".to_owned(),
            },
        ]
    }

    fn column(id: i64, name: &str) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    #[test]
    fn physical_id_uses_the_logical_or_selected_partition_id() {
        let definitions = definitions();
        assert_eq!(physical_table_id(7, Some(&definitions), None), 7);
        assert_eq!(physical_table_id(7, None, Some(0)), 7);
        assert_eq!(physical_table_id(7, Some(&definitions), Some(0)), 101);
        assert_eq!(physical_table_id(7, Some(&definitions), Some(1)), 202);
    }

    #[test]
    #[should_panic]
    fn physical_id_preserves_the_out_of_range_definition_panic() {
        let definitions = definitions();
        let _ = physical_table_id(7, Some(&definitions), Some(2));
    }

    #[test]
    fn partition_names_preserve_empty_casefold_and_first_pid_rules() {
        assert!(partition_name_matches(9, &[], None));
        let mut definitions = definitions();
        definitions.push(PartitionDef {
            id: 202,
            name: "later".to_owned(),
        });
        assert!(partition_name_matches(
            202,
            &["pünicode".to_owned()],
            Some(&definitions)
        ));
        assert!(!partition_name_matches(
            202,
            &["later".to_owned()],
            Some(&definitions)
        ));
        assert!(!partition_name_matches(
            303,
            &["p0".to_owned()],
            Some(&definitions)
        ));
    }

    #[test]
    #[should_panic(expected = "non-empty partition names require partition metadata")]
    fn nonempty_partition_names_require_metadata() {
        let _ = partition_name_matches(101, &["p0".to_owned()], None);
    }

    #[test]
    fn checksum_and_column_lookup_choose_the_first_matching_id() {
        let columns = vec![
            column(1, "a"),
            column(EXTRA_ROW_CHECKSUM_ID, "first"),
            column(EXTRA_ROW_CHECKSUM_ID, "second"),
        ];
        assert_eq!(row_checksum_column(&[]), None);
        assert_eq!(row_checksum_column(&columns), Some(1));
        assert_eq!(
            column_by_id(&[], 1).map(|column| column.name.as_str()),
            None
        );
        assert_eq!(
            column_by_id(&columns, EXTRA_ROW_CHECKSUM_ID).map(|column| column.name.as_str()),
            Some("first")
        );
        assert_eq!(
            column_by_id(&columns, 99).map(|column| column.name.as_str()),
            None
        );
    }

    #[test]
    fn primary_prefix_membership_covers_empty_hit_and_miss() {
        assert!(not_primary_prefix_column(7, &[]));
        assert!(!not_primary_prefix_column(7, &[5, 7, 9]));
        assert!(not_primary_prefix_column(8, &[5, 7, 9]));
    }
}
