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

use tidb_datatype::{Datum, FieldTypeFlags};

/// The `SHOW INDEX` header, with the columns Go reports as numbers marked.
pub(crate) const SHOW_INDEX_COLUMNS: &[(&str, bool)] = &[
    ("Table", false),
    ("Non_unique", true),
    ("Key_name", false),
    ("Seq_in_index", true),
    ("Column_name", false),
    ("Collation", false),
    ("Cardinality", true),
    ("Sub_part", true),
    ("Packed", false),
    ("Null", false),
    ("Index_type", false),
    ("Comment", false),
    ("Index_comment", false),
    ("Visible", false),
    ("Expression", false),
    ("Clustered", false),
    ("Global", false),
];

/// One `SHOW INDEX` row per index column, in Go's own order: the clustered
/// primary key first, then each index in definition order.
pub(crate) fn show_index_rows(table_name: &str, table: &tidb_executor::KvTable) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut push = |key_name: &str,
                    unique: bool,
                    clustered: bool,
                    sequence: usize,
                    column: Option<&str>,
                    expression: Option<&str>,
                    nullable: bool,
                    comment: &str,
                    visible: bool| {
        rows.push(vec![
            text(table_name),
            Datum::Int(i64::from(!unique)),
            text(key_name),
            Datum::Int(sequence as i64),
            column.map_or(Datum::Null, text),
            text("A"),
            Datum::Int(0),
            Datum::Null,
            Datum::Null,
            text(if nullable { "YES" } else { "" }),
            text("BTREE"),
            text(""),
            text(comment),
            text(if visible { "YES" } else { "NO" }),
            expression.map_or(Datum::Null, text),
            text(if clustered { "YES" } else { "NO" }),
            text("NO"),
        ]);
    };
    if let Some(offset) = table.pk_handle_offset() {
        push(
            "PRIMARY",
            true,
            true,
            1,
            Some(&table.columns[offset].name),
            None,
            false,
            "",
            true,
        );
    }
    for index in table.indexes() {
        let clustered =
            index.name.eq_ignore_ascii_case("PRIMARY") && !table.common_handle_offsets().is_empty();
        for (position, offset) in index.column_offsets.iter().enumerate() {
            let column = &table.columns[*offset];
            let nullable = column.field_type.flags() & FieldTypeFlags::NOT_NULL == 0;
            let expression = column
                .generated
                .as_ref()
                .filter(|_| table.is_hidden(*offset))
                .map(|generated| generated.expr_text.as_str());
            push(
                &index.name,
                index.unique,
                clustered,
                position + 1,
                expression.is_none().then_some(column.name.as_str()),
                expression,
                nullable,
                &index.comment,
                index.visible,
            );
        }
    }
    rows
}
