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

/// One `SHOW INDEX` row per index column, in Go's own order: the clustered
/// primary key first, then each index in definition order. `column_ndv` is
/// Go `fetchShowIndex`'s Cardinality: the column's NDV in the table's stats
/// (`statsTbl.GetCol(col.ID).NDV`, 0 without them).
pub fn show_index_rows(
    table_name: &str,
    table: &crate::KvTable,
    column_ndv: impl Fn(i64) -> i64,
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut push = |key_name: &str,
                    unique: bool,
                    clustered: bool,
                    sequence: usize,
                    column: Option<&str>,
                    cardinality: i64,
                    expression: Option<&str>,
                    nullable: bool,
                    comment: &str,
                    visible: bool,
                    global: bool,
                    sub_part: Option<i64>| {
        rows.push(vec![
            text(table_name),
            Datum::Int(i64::from(!unique)),
            text(key_name),
            Datum::Int(sequence as i64),
            column.map_or(Datum::Null, text),
            text("A"),
            Datum::Int(cardinality),
            // go `fetchShowIndex`'s Sub_part: the declared prefix length a
            // key part stores (`IndexColumn.Length`), NULL for a whole-column
            // part.
            sub_part.map_or(Datum::Null, |length| Datum::Int(length)),
            Datum::Null,
            text(if nullable { "YES" } else { "" }),
            text("BTREE"),
            text(""),
            text(comment),
            text(if visible { "YES" } else { "NO" }),
            expression.map_or(Datum::Null, text),
            text(if clustered { "YES" } else { "NO" }),
            text(if global { "YES" } else { "NO" }),
        ]);
    };
    if let Some(offset) = table.pk_handle_offset() {
        push(
            "PRIMARY",
            true,
            true,
            1,
            Some(&table.columns()[offset].name),
            column_ndv(table.columns()[offset].id),
            None,
            false,
            "",
            true,
            false,
            None,
        );
    }
    for index in table.indexes() {
        // Go `tb.Meta().IsCommonHandle && idxInfo.Primary`.
        let clustered = index.primary && !table.common_handle_offsets().is_empty();
        for (position, offset) in index.column_offsets.iter().enumerate() {
            let column = &table.columns()[*offset];
            let nullable = column.field_type.flags() & FieldTypeFlags::NOT_NULL == 0;
            let expression = column
                .generated
                .as_ref()
                .filter(|_| table.is_hidden(*offset))
                .map(|generated| generated.expr_text.as_str());
            let sub_part = index
                .prefix_lengths
                .get(position)
                .copied()
                .filter(|length| *length != tidb_datatype::UNSPECIFIED_LENGTH);
            push(
                &index.name,
                index.unique,
                clustered,
                position + 1,
                // Go `fetchShowIndex`: a hidden expression-index column is
                // named by the STRING "NULL" (not SQL NULL).
                Some(if expression.is_none() { column.name.as_str() } else { "NULL" }),
                column_ndv(column.id),
                expression,
                nullable,
                &index.comment,
                index.visible,
                index.global,
                sub_part,
            );
        }
    }
    rows
}
