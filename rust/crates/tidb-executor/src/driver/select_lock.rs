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

//! Bind physical record-key expressions to the selected source layout.

use super::*;
use crate::select_lock::{SelectLockExec, SelectedRecordKey};
use crate::StmtContext;

pub(super) fn wrap(
    source: Box<dyn Executor>,
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let Some(selected) = ctx.selected_lock_keys() else {
        return Ok(source);
    };
    let mut expressions: Vec<SelectedRecordKey> = Vec::new();
    let mut tables = Vec::new();
    if let Some(from) = &select.from {
        base_tables(from, &mut tables);
    }
    for relation in &scope.tables {
        let Some(table_ref) = tables.iter().find(|table_ref| {
            table_ref
                .alias
                .as_deref()
                .or_else(|| table_ref.name.last().map(String::as_str))
                .is_some_and(|name| name.eq_ignore_ascii_case(&relation.name))
        }) else {
            continue;
        };
        let (database, name) = match table_ref.name.as_slice() {
            [name] => (current_db, name.as_str()),
            [database, name] => (database.as_str(), name.as_str()),
            _ => {
                return Err(DriverError::Exec(ExecError::internal(
                    "invalid locking table reference",
                )))
            }
        };
        let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
            continue;
        };
        let table = table.clone();
        let offset_of = |name: &str| {
            relation
                .columns
                .iter()
                .position(|(column, _)| column.eq_ignore_ascii_case(name))
                .map(|offset| relation.offset + offset)
                .ok_or_else(|| {
                    DriverError::Exec(ExecError::internal(format!(
                        "locking plan discarded record identity column {}.{name}",
                        relation.name
                    )))
                })
        };
        let handle_offsets = if !table.common_handle_offsets().is_empty() {
            table
                .common_handle_offsets()
                .iter()
                .map(|offset| offset_of(&table.columns[*offset].name))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![offset_of(
                table
                    .pk_handle_offset()
                    .map_or("_tidb_rowid", |offset| table.columns[offset].name.as_str()),
            )?]
        };
        let partition_offsets = if table.partition().is_some() {
            table
                .columns
                .iter()
                .map(|column| offset_of(&column.name))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let types = source.ret_field_types().to_vec();
        let context = ctx.clone();
        expressions.push(Box::new(move |row| {
            let values = handle_offsets
                .iter()
                .map(|offset| row.get_datum(*offset, &types[*offset]))
                .collect::<Vec<_>>();
            // A primary handle is non-null in storage; NULL here denotes an
            // absent side of an outer join, which owns no record lock.
            if values.iter().all(|value| matches!(value, Datum::Null)) {
                return Ok(None);
            }
            let handle = if table.common_handle_offsets().is_empty() {
                match &values[0] {
                    Datum::Int(id) => TableHandle::Int(*id),
                    Datum::UInt(id) => TableHandle::Int(*id as i64),
                    _ => return Err(ExecError::internal("invalid integer lock handle")),
                }
            } else {
                table
                    .common_handle_from_values(&values, &context.session_zone())
                    .map_err(|error| ExecError::internal(format!("{error:?}")))?
            };
            let physical_id = if partition_offsets.is_empty() {
                table.table_id
            } else {
                let values = partition_offsets
                    .iter()
                    .map(|offset| row.get_datum(*offset, &types[*offset]))
                    .collect::<Vec<_>>();
                table
                    .row_partition_route(&values, &context)
                    .ok_or_else(|| ExecError::internal("selected row has no physical partition"))?
                    .1
            };
            Ok(Some(tidb_codec::table_key::encode_row_key_with_handle(
                physical_id,
                &handle.record_handle(),
            )))
        }));
    }
    Ok(Box::new(SelectLockExec::new(source, expressions, selected)))
}

pub(super) fn base_tables<'a>(join: &'a tidb_ast::Join, tables: &mut Vec<&'a tidb_ast::TableRef>) {
    fn visit<'a>(node: &'a tidb_ast::JoinNode, tables: &mut Vec<&'a tidb_ast::TableRef>) {
        match node {
            tidb_ast::JoinNode::Table(table) => tables.push(table),
            tidb_ast::JoinNode::Join(join) => base_tables(join, tables),
            tidb_ast::JoinNode::Derived { .. } => {}
        }
    }
    visit(&join.left, tables);
    if let Some(right) = &join.right {
        visit(right, tables);
    }
}
