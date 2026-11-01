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

//! The generated-column assignments Go's planner appends to a statement's
//! write list: `buildUpdateLists`' virtual assignments (`Update.OrderedList`)
//! and `resolveGeneratedColumns`' `OnDuplicates` (`Insert.OnDuplicate`).
//! The foreign-key triggers read the columns of those lists
//! (`buildTbl2UpdateColumns`, `buildOnDuplicateUpdateColumns`).

use tidb_hack::GoCaseCompare;
use tidb_model::{ColumnInfo, GoShared};

fn depends_on(column: &ColumnInfo, mut matches: impl FnMut(&str) -> bool) -> bool {
    column
        .dependences
        .snapshot()
        .iter()
        .any(|dependency| matches(&dependency.to_utf8_lossy_go()))
}

/// Go `buildUpdateLists`: the generated columns of `columns` (the table's
/// `Cols()`, in order) whose value may change when `assigned` columns are
/// SET. A generated column may change when a column it depends on was
/// assigned, was itself appended earlier (`dependentColumnsModified`), or
/// carries ON UPDATE NOW.
#[must_use]
pub fn update_list_generated_columns(
    columns: &[GoShared<ColumnInfo>],
    assigned: &[String],
) -> Vec<String> {
    let on_update_now: Vec<String> = columns
        .iter()
        .map(|column| column.read())
        .filter(|column| {
            column
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW)
        })
        .map(|column| column.name.original().to_owned())
        .collect();
    let mut modified: Vec<String> = assigned.to_vec();
    let mut appended = Vec::new();
    for column in columns {
        let column = column.read();
        if !column.is_generated() {
            continue;
        }
        let may_modified = depends_on(&column, |dependency| {
            on_update_now.iter().any(|name| name.go_lower_eq(dependency))
                || modified.iter().any(|name| name.go_lower_eq(dependency))
        });
        if may_modified {
            let name = column.name.original().to_owned();
            modified.push(name.clone());
            appended.push(name);
        }
    }
    appended
}

/// Go `resolveGeneratedColumns` with `onDups`: the generated columns an
/// `INSERT ... ON DUPLICATE KEY UPDATE` of `assigned` columns appends to
/// `Insert.OnDuplicate`. Walking `columns` (the table's `Cols()`) in order,
/// a column with ON UPDATE NOW joins the updated set where it stands, so a
/// generated column placed before it does not see it -- Go's order.
#[must_use]
pub fn on_duplicate_generated_columns(
    columns: &[GoShared<ColumnInfo>],
    assigned: &[String],
) -> Vec<String> {
    let mut on_dups: Vec<String> = assigned.to_vec();
    let mut appended = Vec::new();
    for column in columns {
        let column = column.read();
        if !column.is_generated() {
            if column
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW)
            {
                on_dups.push(column.name.original().to_owned());
            }
            continue;
        }
        if depends_on(&column, |dependency| {
            on_dups.iter().any(|name| name.go_lower_eq(dependency))
        }) {
            let name = column.name.original().to_owned();
            on_dups.push(name.clone());
            appended.push(name);
        }
    }
    appended
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, generated_from: &[&str], on_update_now: bool) -> GoShared<ColumnInfo> {
        let mut field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long);
        if on_update_now {
            field_type.add_flags(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW);
        }
        let mut info = ColumnInfo {
            name: tidb_ast::CiString::new(name),
            field_type,
            generated_expr_string: if generated_from.is_empty() {
                String::new()
            } else {
                generated_from.join(" + ")
            },
            ..ColumnInfo::default()
        };
        for dependency in generated_from {
            info.dependences.insert(*dependency);
        }
        GoShared::new(info)
    }

    /// A generated column placed BEFORE the ON UPDATE NOW column it reads:
    /// Go's UPDATE list appends it (the dependency's flag is read directly),
    /// while Go's ON DUPLICATE list does not (the timestamp joins `onDups`
    /// only when the walk reaches it).
    #[test]
    fn on_update_now_dependencies_follow_go_order_per_statement_kind() {
        let columns = vec![
            column("g", &["ts"], false),
            column("ts", &[], true),
            column("a", &[], false),
            column("h", &["g"], false),
            column("k", &["ts"], false),
        ];
        let assigned = vec!["a".to_owned()];
        assert_eq!(update_list_generated_columns(&columns, &assigned), ["g", "h", "k"]);
        assert_eq!(on_duplicate_generated_columns(&columns, &assigned), ["k"]);
    }

    /// The chain: a generated column over an appended one is appended too,
    /// in both lists; an unrelated generated column is not.
    #[test]
    fn generated_chains_follow_assignments() {
        let columns = vec![
            column("a", &[], false),
            column("g1", &["a"], false),
            column("g2", &["g1"], false),
            column("b", &[], false),
            column("g3", &["b"], false),
        ];
        let assigned = vec!["A".to_owned()];
        assert_eq!(update_list_generated_columns(&columns, &assigned), ["g1", "g2"]);
        assert_eq!(on_duplicate_generated_columns(&columns, &assigned), ["g1", "g2"]);
    }
}
