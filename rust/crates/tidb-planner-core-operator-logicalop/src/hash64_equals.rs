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

//! Go `logicalop/hash64_equals_generated.go`: the `Hash64` / `Equals` pair
//! every logical operator offers the cascades memo, which hashes a group
//! expression as its operator plus its input groups and folds two group
//! expressions together when their operators are `Equals`.
//!
//! Each operator writes its plan-codec type name and then its fields in the
//! generated order into the caller's hasher. An expression contributes its
//! canonical `HashCode` bytes and compares by them
//! ([`schema_producer::expressions_equal`]), the context-free structural
//! identity the crate's other `Hash64` ports use. A Go slice that is nil
//! hashes `NilFlag` where an empty one hashes `NotNilFlag` and a zero length;
//! a Rust `Vec` has no nil, so every list hashes as the non-nil form.
//!
//! The operators Go does not generate (`LogicalCTE`, `LogicalCTETable`,
//! `TiKVSingleGather`, `LogicalTableScan`, `LogicalIndexScan`) inherit
//! `LogicalSchemaProducer.Hash64` (the schema alone) and an `Equals` whose
//! `other.(*LogicalSchemaProducer)` assertion never holds for a whole
//! operator, so they are never equal to anything.

use tidb_expr::expression::Expression;
use tidb_expression_aggregation::ByItems;
use tidb_planner_cascades_base::{Hasher, NOT_NIL_FLAG, NIL_FLAG};
use tidb_planner_property::{ColumnSortItem, SortItem};
use tidb_plancodec as plancodec;

use crate::schema_producer::{self, expression_lists_equal, expressions_equal};
use crate::LogicalPlan;

fn hash_expr(h: &mut dyn Hasher, expr: &Expression) {
    let mut expr = expr.clone();
    h.hash_bytes(expr.hash_code());
}

fn hash_exprs(h: &mut dyn Hasher, exprs: &[Expression]) {
    h.hash_byte(NOT_NIL_FLAG);
    h.hash_int(exprs.len() as i64);
    for expr in exprs {
        hash_expr(h, expr);
    }
}

/// Go `LogicalSchemaProducer.Hash64(h)`, written into the caller's hasher.
fn hash_schema(h: &mut dyn Hasher, plan: &LogicalPlan) {
    h.hash_uint64(schema_producer::schema_hash64(plan.base().base.schema()));
}

fn schemas_equal(left: &LogicalPlan, right: &LogicalPlan) -> bool {
    schema_producer::schema_equals(left.base().base.schema(), right.base().base.schema())
}

/// Go `util.ByItems.Hash64`: the expression, then `Desc`.
fn hash_by_items(h: &mut dyn Hasher, items: &[ByItems]) {
    h.hash_byte(NOT_NIL_FLAG);
    h.hash_int(items.len() as i64);
    for item in items {
        hash_expr(h, &item.expr);
        h.hash_bool(item.desc);
    }
}

fn by_items_equal(left: &[ByItems], right: &[ByItems]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.desc == right.desc && expressions_equal(&left.expr, &right.expr))
}

/// Go `property.SortItem.Hash64`: the column, then `Desc`.
fn hash_sort_items(h: &mut dyn Hasher, items: &[SortItem]) {
    h.hash_byte(NOT_NIL_FLAG);
    h.hash_int(items.len() as i64);
    for item in items {
        h.hash_uint64(item.col.hash64());
        h.hash_bool(item.desc);
    }
}

fn sort_items_equal(left: &[SortItem], right: &[SortItem]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.desc == right.desc && left.col.equals(&right.col))
}

fn hash_column_sort_items(h: &mut dyn Hasher, items: &[ColumnSortItem]) {
    h.hash_byte(NOT_NIL_FLAG);
    h.hash_int(items.len() as i64);
    for item in items {
        h.hash_uint64(item.col.hash64());
        h.hash_bool(item.desc);
    }
}

fn column_sort_items_equal(left: &[ColumnSortItem], right: &[ColumnSortItem]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.desc == right.desc && left.col.equals(&right.col))
}

impl LogicalPlan {
    /// Go `LogicalPlan.Hash64(h)`: the operator's generated hash, written into
    /// `h` (`hash64_equals_generated.go`).
    pub fn hash64(&self, h: &mut dyn Hasher) {
        match self {
            Self::Join(op) => {
                h.hash_string(plancodec::TYPE_JOIN);
                h.hash_uint64(op.hash64(op.base.base.schema()));
            }
            Self::Aggregation(op) => {
                h.hash_string(plancodec::TYPE_AGGREGATION);
                h.hash_uint64(op.hash64(op.base.base.schema()));
            }
            Self::Apply(op) => {
                h.hash_string(plancodec::TYPE_APPLY);
                h.hash_string(plancodec::TYPE_JOIN);
                h.hash_uint64(op.join.hash64(op.join.base.base.schema()));
                h.hash_byte(NOT_NIL_FLAG);
                h.hash_int(op.cor_cols.len() as i64);
                for column in &op.cor_cols {
                    h.hash_uint64(column.column.hash64());
                }
                h.hash_bool(op.no_decorrelate);
                h.hash_bool(op.is_lateral);
            }
            Self::Expand(op) => {
                h.hash_string(plancodec::TYPE_EXPAND);
                h.hash_uint64(op.hash64(op.base.base.schema()));
            }
            Self::Limit(op) => {
                h.hash_string(plancodec::TYPE_LIMIT);
                hash_schema(h, self);
                hash_sort_items(h, &op.partition_by);
                h.hash_uint64(op.offset);
                h.hash_uint64(op.count);
            }
            Self::MaxOneRow(op) => {
                h.hash_string(plancodec::TYPE_MAX_ONE_ROW);
                // Go `BaseLogicalPlan.Hash64`: the plan id.
                h.hash_int(i64::from(op.base.base.id()));
            }
            Self::DataSource(op) => {
                h.hash_string(plancodec::TYPE_DATA_SOURCE);
                hash_schema(h, self);
                match &op.table_info {
                    // Go `TableInfo.Hash64`: the table id.
                    Some(table) => {
                        h.hash_byte(NOT_NIL_FLAG);
                        h.hash_int64(table.read().id);
                    }
                    None => h.hash_byte(NIL_FLAG),
                }
                match &op.table_as_name {
                    // Go `CIStr.Hash64`: the lower-case name.
                    Some(name) => {
                        h.hash_byte(NOT_NIL_FLAG);
                        h.hash_string(&name.to_lowercase());
                    }
                    None => h.hash_byte(NIL_FLAG),
                }
                hash_exprs(h, &op.pushed_down_conds);
                hash_exprs(h, &op.all_conds);
                h.hash_int64(op.physical_table_id);
                h.hash_int64(i64::from(op.prefer_store_type));
                h.hash_bool(op.is_for_update_read);
            }
            Self::MemTable(op) => {
                h.hash_string(plancodec::TYPE_MEM_TABLE_SCAN);
                hash_schema(h, self);
                h.hash_string(&op.db_name.to_lowercase());
                // Go `TableInfo.Hash64`: the table id.
                h.hash_byte(NOT_NIL_FLAG);
                h.hash_int64(op.table_info.read().id);
            }
            Self::UnionAll(_) => {
                h.hash_string(plancodec::TYPE_UNION);
                hash_schema(h, self);
            }
            Self::PartitionUnionAll(_) => {
                h.hash_string(plancodec::TYPE_PARTITION_UNION);
                h.hash_string(plancodec::TYPE_UNION);
                hash_schema(h, self);
            }
            Self::Projection(op) => {
                h.hash_string(plancodec::TYPE_PROJECTION);
                h.hash_uint64(op.hash64(op.base.base.schema()));
            }
            Self::Selection(op) => {
                h.hash_string(plancodec::TYPE_SELECTION);
                hash_exprs(h, &op.conditions);
            }
            Self::Sequence(op) => {
                h.hash_string(plancodec::TYPE_SEQUENCE);
                h.hash_int(i64::from(op.base.base.id()));
            }
            Self::Show(_) => {
                h.hash_string(plancodec::TYPE_SHOW);
                hash_schema(h, self);
            }
            Self::ShowDDLJobs(_) => {
                h.hash_string(plancodec::TYPE_SHOW_DDL_JOBS);
                hash_schema(h, self);
            }
            Self::Sort(op) => {
                h.hash_string(plancodec::TYPE_SORT);
                hash_by_items(h, &op.by_items);
            }
            Self::TableDual(op) => {
                h.hash_string(plancodec::TYPE_TABLE_DUAL);
                hash_schema(h, self);
                h.hash_int64(op.row_count as i64);
            }
            Self::TopN(op) => {
                h.hash_string(plancodec::TYPE_TOP_N);
                hash_schema(h, self);
                hash_by_items(h, &op.by_items);
                hash_sort_items(h, &op.partition_by);
                h.hash_uint64(op.offset);
                h.hash_uint64(op.count);
                h.hash_bool(op.prefer_limit_to_cop);
            }
            Self::UnionScan(op) => {
                h.hash_string(plancodec::TYPE_UNION_SCAN);
                hash_exprs(h, &op.conditions);
                for column in &op.handle_cols {
                    h.hash_uint64(column.hash64());
                }
            }
            Self::Window(op) => {
                h.hash_string(plancodec::TYPE_WINDOW);
                hash_schema(h, self);
                h.hash_byte(NOT_NIL_FLAG);
                h.hash_int(op.window_func_descs.len() as i64);
                for desc in &op.window_func_descs {
                    h.hash_string(&desc.base.name);
                    hash_exprs(h, &desc.base.args);
                }
                hash_column_sort_items(h, &op.partition_by);
                hash_column_sort_items(h, &op.order_by);
                match &op.frame {
                    Some(frame) => {
                        h.hash_byte(NOT_NIL_FLAG);
                        h.hash_uint64(frame.hash64());
                    }
                    None => h.hash_byte(NIL_FLAG),
                }
            }
            Self::Lock(op) => {
                h.hash_string(plancodec::TYPE_SELECT_LOCK);
                h.hash_int(i64::from(op.base.base.id()));
                // Go `SelectLockInfo.Hash64`.
                h.hash_byte(NOT_NIL_FLAG);
                h.hash_int(op.lock.lock_type as i64);
                h.hash_uint64(op.lock.wait_sec);
                h.hash_int(op.lock.tables.len() as i64);
                for table in &op.lock.tables {
                    for name in table {
                        h.hash_string(name);
                    }
                }
            }
            Self::CTE(_)
            | Self::CTETable(_)
            | Self::TiKVSingleGather(_)
            | Self::TableScan(_)
            | Self::IndexScan(_) => hash_schema(h, self),
        }
    }

    /// Go `LogicalPlan.Equals(other)`: the generated field-wise equality of
    /// two operators of the same type.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Join(left), Self::Join(right)) => left.equals(
                left.base.base.schema(),
                right,
                right.base.base.schema(),
            ),
            (Self::Aggregation(left), Self::Aggregation(right)) => left.equals(
                left.base.base.schema(),
                right,
                right.base.base.schema(),
            ),
            (Self::Apply(left), Self::Apply(right)) => {
                left.join.equals(left.join.base.base.schema(), &right.join, right.join.base.base.schema())
                    && left.cor_cols.len() == right.cor_cols.len()
                    && left
                        .cor_cols
                        .iter()
                        .zip(&right.cor_cols)
                        .all(|(left, right)| left.column.equals(&right.column))
                    && left.no_decorrelate == right.no_decorrelate
                    && left.is_lateral == right.is_lateral
            }
            (Self::Expand(left), Self::Expand(right)) => left.equals(
                left.base.base.schema(),
                right,
                right.base.base.schema(),
            ),
            (Self::Limit(left), Self::Limit(right)) => {
                schemas_equal(self, other)
                    && sort_items_equal(&left.partition_by, &right.partition_by)
                    && left.offset == right.offset
                    && left.count == right.count
            }
            // Go `BaseLogicalPlan.Equals`: the same plan id.
            (Self::MaxOneRow(left), Self::MaxOneRow(right)) => left.base.base.id() == right.base.base.id(),
            (Self::Sequence(left), Self::Sequence(right)) => left.base.base.id() == right.base.base.id(),
            (Self::DataSource(left), Self::DataSource(right)) => {
                schemas_equal(self, other)
                    && match (&left.table_info, &right.table_info) {
                        (None, None) => true,
                        (Some(left), Some(right)) => left.read().id == right.read().id,
                        _ => false,
                    }
                    && match (&left.table_as_name, &right.table_as_name) {
                        (None, None) => true,
                        (Some(left), Some(right)) => left.to_lowercase() == right.to_lowercase(),
                        _ => false,
                    }
                    && expression_lists_equal(&left.pushed_down_conds, &right.pushed_down_conds)
                    && expression_lists_equal(&left.all_conds, &right.all_conds)
                    && left.physical_table_id == right.physical_table_id
                    && left.prefer_store_type == right.prefer_store_type
                    && left.is_for_update_read == right.is_for_update_read
            }
            (Self::MemTable(left), Self::MemTable(right)) => {
                schemas_equal(self, other)
                    && left.db_name.to_lowercase() == right.db_name.to_lowercase()
                    && left.table_info.read().id == right.table_info.read().id
            }
            (Self::UnionAll(_), Self::UnionAll(_))
            | (Self::PartitionUnionAll(_), Self::PartitionUnionAll(_))
            | (Self::Show(_), Self::Show(_))
            | (Self::ShowDDLJobs(_), Self::ShowDDLJobs(_)) => schemas_equal(self, other),
            (Self::Projection(left), Self::Projection(right)) => left.equals(
                left.base.base.schema(),
                right,
                right.base.base.schema(),
            ),
            (Self::Selection(left), Self::Selection(right)) => {
                expression_lists_equal(&left.conditions, &right.conditions)
            }
            (Self::Sort(left), Self::Sort(right)) => by_items_equal(&left.by_items, &right.by_items),
            (Self::TableDual(left), Self::TableDual(right)) => {
                schemas_equal(self, other) && left.row_count == right.row_count
            }
            (Self::TopN(left), Self::TopN(right)) => {
                schemas_equal(self, other)
                    && by_items_equal(&left.by_items, &right.by_items)
                    && sort_items_equal(&left.partition_by, &right.partition_by)
                    && left.offset == right.offset
                    && left.count == right.count
                    && left.prefer_limit_to_cop == right.prefer_limit_to_cop
            }
            (Self::UnionScan(left), Self::UnionScan(right)) => {
                expression_lists_equal(&left.conditions, &right.conditions)
                    && left.handle_cols.len() == right.handle_cols.len()
                    && left
                        .handle_cols
                        .iter()
                        .zip(&right.handle_cols)
                        .all(|(left, right)| left.equals(right))
            }
            (Self::Window(left), Self::Window(right)) => {
                schemas_equal(self, other)
                    && left.window_func_descs.len() == right.window_func_descs.len()
                    && left
                        .window_func_descs
                        .iter()
                        .zip(&right.window_func_descs)
                        .all(|(left, right)| left.base.equals(&right.base))
                    && column_sort_items_equal(&left.partition_by, &right.partition_by)
                    && column_sort_items_equal(&left.order_by, &right.order_by)
                    && match (&left.frame, &right.frame) {
                        (None, None) => true,
                        (Some(left), Some(right)) => left.equals(right),
                        _ => false,
                    }
            }
            (Self::Lock(left), Self::Lock(right)) => {
                left.base.base.id() == right.base.base.id() && left.lock == right.lock
            }
            _ => false,
        }
    }
}
