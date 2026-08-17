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

//! Go `pkg/planner/core/operator/logicalop/logical_schema_producer.go`: the
//! schema-producing behaviour that `LogicalJoin`, `LogicalProjection`,
//! `LogicalAggregation` and `DataSource` all embed.
//!
//! SEED of `pkg/planner/core`. Go models this as a struct
//! (`LogicalSchemaProducer`) embedded between the operator and
//! `BaseLogicalPlan`, carrying `schema` and `names`. Those two fields already
//! live on [`crate::plan_base::BasePlan`] in this port (see that module's
//! header for why), so what is left of the Go type is BEHAVIOUR, and it lands
//! here as free functions over a [`Schema`] rather than as a third base
//! struct. Nothing is lost: every Go member is reproduced below.
//!
//! # Narrowings, by name
//!
//! * `LogicalSchemaProducer.Schema()` lazily MEMOISES the child's schema into
//!   `s.schema`. [`crate::logical::LogicalPlan::schema`] answers the same
//!   question without the write, so [`materialized_schema`] is the explicit
//!   materialising form for callers that need an owned schema to mutate.
//! * `InlineProjection` reads `col.GetType(evalCtx).GetFlen()`. There is no
//!   `EvalCtx` here; [`Column::ret_type`] carries the same `Flen`, and a
//!   column with no type is treated as `MaxInt` wide, which is Go's behaviour
//!   for an unset `Flen` only by coincidence — see the guard in the body.
//! * `Hash64`/`Equals` are the halves this crate's `logical_schema_producer`
//!   module already modelled over a normalised column adapter. Here they run
//!   on the REAL [`Schema`], which is why they are the merged form; that
//!   module stays for its out-of-crate consumers.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Go `expression.GetUsedList(ctx, usedCols, schema)` (`schema.go:338`).
///
/// One flag per column of `schema`: whether `used_cols` references it. The
/// second pass is Go's generated-column rule — when a used column is virtual,
/// every other column with an EQUAL virtual expression and an equal result
/// type is used too, because they are the same generated value.
#[must_use]
pub fn get_used_list(used_cols: &[Column], schema: &Schema) -> Vec<bool> {
    let mut used = vec![false; schema.len()];
    for i in 0..schema.columns.len() {
        if used[i] {
            continue;
        }
        let column = &schema.columns[i];
        used[i] = used_cols
            .iter()
            .any(|candidate| candidate.unique_id == column.unique_id);
        if !used[i] {
            continue;
        }
        let Some(Expression::ScalarFunction(_)) = column.virtual_expr.as_deref() else {
            continue;
        };
        let virtual_expr = column.virtual_expr.as_deref();
        let ret_type = column.ret_type.clone();
        for (j, other) in schema.columns.iter().enumerate() {
            if used[j] || j == i {
                continue;
            }
            let same_expr = match (virtual_expr, other.virtual_expr.as_deref()) {
                (Some(left), Some(right)) => expressions_equal(left, right),
                _ => false,
            };
            if same_expr && ret_type == other.ret_type {
                used[j] = true;
            }
        }
    }
    used
}

/// Go `LogicalSchemaProducer.Schema()` (`logical_schema_producer.go:80`): the
/// operator's own schema, or a clone of its single child's, or an empty one.
///
/// Go memoises the result into the operator; the caller here owns the value
/// instead, which is the same answer without the hidden write.
#[must_use]
pub fn materialized_schema(own: Option<&Schema>, children: &[&Schema]) -> Schema {
    match own {
        Some(schema) => schema.clone(),
        None if children.len() == 1 => children[0].clone(),
        None => Schema::default(),
    }
}

/// Go `LogicalSchemaProducer.InlineProjection(parentUsedCols)`
/// (`logical_schema_producer.go:120`): drops every column the parent does not
/// use, keeping at least one.
///
/// Returns the pruned columns in Go's order, which is BACK TO FRONT — the
/// source appends while walking the schema in reverse.
pub fn inline_projection(schema: &mut Schema, parent_used_cols: &[Column]) -> Vec<Column> {
    let mut used = get_used_list(parent_used_cols, schema);
    if parent_used_cols.is_empty() && !used.is_empty() {
        // Go: "When this operator output no columns, we return its smallest
        // column for safety."
        let mut min_col_len = i64::MAX;
        let mut chosen_pos = 0;
        for (i, column) in schema.columns.iter().enumerate() {
            let flen = column.ret_type.as_ref().map_or(i64::MAX, |ty| ty.flen());
            if flen < min_col_len {
                chosen_pos = i;
                min_col_len = flen;
            }
        }
        used[chosen_pos] = true;
    }
    let mut pruned = Vec::new();
    for i in (0..used.len()).rev() {
        if !used[i] {
            pruned.push(schema.columns.remove(i));
        }
    }
    pruned
}

/// Go `LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)`
/// (`logical_schema_producer.go:148`): clear this operator's keys, then carry
/// forward every single child key whose columns all survive into `self`.
///
/// The `BaseLogicalPlan.BuildKeyInfo` half Go calls first is the `maxOneRow`
/// propagation, which stays on [`crate::logical::LogicalPlan::build_key_info`].
pub fn propagate_child_keys(self_schema: &mut Schema, child_schema: &[Schema]) {
    self_schema.pk_or_uk.clear();
    if child_schema.len() != 1 {
        return;
    }
    let mut carried = Vec::new();
    for key in &child_schema[0].pk_or_uk {
        let Some(indices) = self_schema.columns_indices(key) else {
            continue;
        };
        carried.push(
            indices
                .into_iter()
                .map(|i| self_schema.columns[i].clone())
                .collect::<Vec<_>>(),
        );
    }
    self_schema.pk_or_uk = carried;
}

/// Go `LogicalSchemaProducer.Hash64(h)` (`logical_schema_producer.go:36`):
/// a nil/not-nil marker followed by every output column's own `Hash64`.
///
/// As the source comment says, the NAMES are deliberately excluded — TiDB does
/// not maintain them strictly, so only the schema's column identity counts.
#[must_use]
pub fn schema_hash64(schema: Option<&Schema>) -> u64 {
    let mut hasher = new_hash_equaler();
    match schema {
        Some(schema) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            for column in &schema.columns {
                hash_column(&mut hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.sum64()
}

/// Go `LogicalSchemaProducer.Equals(other)`
/// (`logical_schema_producer.go:51`): both nil, or the same length with
/// column-for-column equality.
#[must_use]
pub fn schema_equals(left: Option<&Schema>, right: Option<&Schema>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.columns.len() == right.columns.len()
                && left
                    .columns
                    .iter()
                    .zip(&right.columns)
                    .all(|(a, b)| a.unique_id == b.unique_id && a.id == b.id && a.index == b.index)
        }
        _ => false,
    }
}

/// Go `base.HashEquals`' structural expression equality, which the generated
/// `Equals` bodies use: the canonical `HashCode` encoding, not
/// `Expression.Equal(ctx, other)`.
///
/// [`Expression::equal`] is the CONTEXT-FREE half of the latter and reports
/// `false` for a constant or a scalar function by design, so it cannot answer
/// operator identity. `HashCode` is structural and context-free, which is
/// exactly what the identity needs.
#[must_use]
pub fn expressions_equal(left: &Expression, right: &Expression) -> bool {
    let (mut left, mut right) = (left.clone(), right.clone());
    left.hash_code() == right.hash_code()
}

/// [`expressions_equal`] over two lists, in order.
#[must_use]
pub fn expression_lists_equal(left: &[Expression], right: &[Expression]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| expressions_equal(left, right))
}

/// Go `Column.Hash64` as the schema hash consumes it: the identity triple.
fn hash_column(hasher: &mut impl Hasher, column: &Column) {
    hasher.hash_int64(column.id);
    hasher.hash_int64(column.unique_id);
    hasher.hash_int64(column.index);
}
