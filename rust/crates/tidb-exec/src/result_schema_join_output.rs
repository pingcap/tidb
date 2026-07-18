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

//! Planner-owned output metadata for a bounded join tree.
//!
//! TiDB's planner builds a join schema before the executor can produce rows.
//! In particular, `pkg/planner/core/logical_plan_builder.go` resets the
//! not-null flag on the nullable side of an outer join and
//! `buildUsingClause` orders coalesced columns before the remaining outer and
//! inner columns. A plain RIGHT join retains its syntactic left-then-right
//! visible schema while mirroring `FullSchema` to right-then-left; RIGHT
//! USING/NATURAL coalescing then follows that mirrored outer-child order.
//! This leaf ports only those declared metadata decisions. It
//! also preserves the source planner's `FullSchema` order and
//! redundant-column-to-visible index mapping, so a later expression-rewriter
//! leaf can resolve qualified USING columns without widening the executable
//! row.
//!
//! It does not evaluate `ON`/`USING` predicates or manufacture null rows. The
//! result records those execution responsibilities explicitly so a later
//! executor integration cannot mistake metadata for completed join behavior.

use tidb_ast::{Join, JoinType};

use crate::result_field_resolver::ResolvedResultField;
use crate::result_metadata::NOT_NULL_FLAG;

/// A field already declared by a planner child schema.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinOutputField {
    /// Source-shaped field names and type metadata.
    pub field: ResolvedResultField,
    /// Whether the declared join output may contain SQL `NULL`.
    pub nullable: bool,
    /// How this field is exposed by the join schema.
    pub origin: JoinOutputOrigin,
}

impl JoinOutputField {
    /// Creates a base field from a planner child schema.
    pub fn new(field: ResolvedResultField, nullable: bool) -> Self {
        Self {
            field,
            nullable,
            origin: JoinOutputOrigin::Base,
        }
    }

    fn null_extended(&self) -> Self {
        let mut field = self.clone();
        field.nullable = true;
        field.field.field_type.flags &= !NOT_NULL_FLAG;
        field
    }

    fn coalesced_with(&self, redundant: &Self) -> Self {
        let mut field = self.clone();
        field.origin = JoinOutputOrigin::UsingCoalesced {
            redundant_table: redundant.field.names.table.original.clone(),
            redundant_column: redundant.field.names.column.original.clone(),
        };
        field
    }
}

/// Whether a visible output field is a direct child field or the canonical
/// outer-side field retained for `JOIN ... USING (...)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinOutputOrigin {
    /// A field copied from one planner child.
    Base,
    /// The outer field retained after the inner common field was coalesced.
    UsingCoalesced {
        /// The relation qualifier whose inner-side field was hidden.
        redundant_table: String,
        /// The inner-side field hidden from the visible output schema.
        redundant_column: String,
    },
}

/// A child relation supplied by the planner/output-schema owner.
#[derive(Clone, Debug, PartialEq)]
pub enum JoinOutputChild {
    /// Declared fields from a table or already-resolved nested join.
    Fields(Vec<JoinOutputField>),
    /// A derived relation whose output schema has not crossed this boundary.
    Derived,
}

/// Execution responsibilities intentionally left to the join executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOutputUnsupported {
    /// The `ON` expression has not been evaluated by this metadata leaf.
    OnExpressionEvaluation,
    /// The `USING` equality predicates have not been evaluated by this leaf.
    UsingPredicateEvaluation,
    /// Null-extended rows are not manufactured by this metadata leaf.
    RowNullExtension,
}

/// Planner-owned join output metadata plus explicit execution gaps.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinOutputMetadata {
    /// Visible output fields in planner declaration order.
    pub fields: Vec<JoinOutputField>,
    /// All source fields retained by the planner's `FullSchema` contract.
    ///
    /// Unlike [`Self::fields`], this preserves the redundant right-side
    /// fields hidden by `JOIN ... USING (...)`. The order is always the
    /// source child order (`left full schema`, then `right full schema`),
    /// matching `LogicalJoin.FullSchema` rather than the coalesced visible
    /// output order.
    pub full_fields: Vec<JoinOutputField>,
    /// Maps every [`Self::full_fields`] position to its executable visible
    /// output position. A redundant USING field maps to the canonical
    /// coalesced field on the left; this is the source planner's
    /// `RedundantColsToOutputIdx` boundary without pretending the executor
    /// can widen a join row.
    pub full_to_output_indices: Vec<usize>,
    /// Execution behaviors that remain outside this metadata contract.
    pub unsupported: Vec<JoinOutputUnsupported>,
}

/// Explicit failures for join output metadata derivation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinOutputSchemaError {
    /// A join child was a derived table without a planner output schema.
    DerivedRelation,
    /// A join shape is outside this bounded leaf.
    UnsupportedJoin {
        /// The unsupported source join shape.
        reason: &'static str,
    },
    /// A USING column occurs more than once in one child schema.
    AmbiguousUsingColumn {
        /// Which side contained duplicate visible names.
        side: &'static str,
        /// The duplicate column name.
        column: String,
    },
    /// A USING column was not exposed by one child schema.
    MissingUsingColumn {
        /// Which side was missing the name.
        side: &'static str,
        /// The requested column name.
        column: String,
    },
    /// A USING column was declared more than once.
    DuplicateUsingColumn {
        /// The repeated declaration.
        column: String,
    },
}

/// Derives one join node's visible output metadata from already-declared child
/// fields.
///
/// This is intentionally one level deep: callers recursively derive nested
/// children and pass their metadata back as [`JoinOutputChild::Fields`]. That
/// keeps catalog lookup, expression typing, and row execution in their own
/// workstreams while preserving the source planner's output contract.
pub fn derive_join_output_metadata(
    join: &Join,
    left: JoinOutputChild,
    right: JoinOutputChild,
) -> Result<JoinOutputMetadata, JoinOutputSchemaError> {
    let mut left = declared_child(left)?;
    let mut right = declared_child(right)?;

    if join.right.is_none() {
        return Err(JoinOutputSchemaError::UnsupportedJoin {
            reason: "single-relation wrapper is not a binary join",
        });
    }
    if join.straight {
        return Err(JoinOutputSchemaError::UnsupportedJoin {
            reason: "STRAIGHT_JOIN is outside the bounded output-schema leaf",
        });
    }
    let mut unsupported = Vec::new();
    if join.on.is_some() {
        unsupported.push(JoinOutputUnsupported::OnExpressionEvaluation);
    }

    let right_join = matches!(join.tp, JoinType::Right);
    if matches!(join.tp, JoinType::Left) {
        // The planner clears the not-null flag on the inner/right child before
        // any physical join is built. Mark those declared fields now; the
        // executor still owns manufacturing the corresponding NULL rows.
        for field in &mut right {
            *field = field.null_extended();
        }
    } else if right_join {
        // RIGHT JOIN preserves the syntactic left+right visible schema, but
        // the syntactic left child is the nullable inner side.
        for field in &mut left {
            *field = field.null_extended();
        }
    }

    // Go mirrors RIGHT JOIN's FullSchema before USING/NATURAL coalescing:
    // original right (outer) first, original left (inner) second. Ordinary
    // RIGHT output itself remains syntactic left+right and is mapped below.
    let (outer, inner) = if right_join {
        (&right, &left)
    } else {
        (&left, &right)
    };
    let mut full_fields = outer.clone();
    full_fields.extend(inner.iter().cloned());

    let using = if join.natural {
        natural_common_columns(outer, inner)
    } else {
        join.using.clone()
    };
    if !using.is_empty() {
        unsupported.push(JoinOutputUnsupported::UsingPredicateEvaluation);
    }

    let left_len = left.len();
    let right_len = right.len();
    let (fields, full_to_output_indices) = if using.is_empty() {
        let mut fields = Vec::with_capacity(left.len() + right.len());
        fields.append(&mut left);
        fields.append(&mut right);
        let full_to_output_indices = if right_join {
            // FullSchema is right+left, while the executable row is left+right.
            (0..right_len)
                .map(|index| left_len + index)
                .chain(0..left_len)
                .collect()
        } else {
            (0..fields.len()).collect()
        };
        (fields, full_to_output_indices)
    } else {
        let mut declared_using: Vec<&String> = Vec::with_capacity(using.len());
        for column in &using {
            if declared_using
                .iter()
                .any(|declared| (*declared).as_str().eq_ignore_ascii_case(column))
            {
                return Err(JoinOutputSchemaError::DuplicateUsingColumn {
                    column: column.clone(),
                });
            }
            declared_using.push(column);
        }

        let mut outer_matches = vec![None; outer.len()];
        let mut inner_matches = vec![None; inner.len()];
        for (using_index, column) in declared_using.iter().enumerate() {
            let outer_indexes = matching_indexes(outer, (*column).as_str());
            if outer_indexes.len() != 1 {
                return Err(using_error(
                    if right_join { "right" } else { "left" },
                    (*column).as_str(),
                    outer_indexes.len(),
                ));
            }
            let inner_indexes = matching_indexes(inner, (*column).as_str());
            if inner_indexes.len() != 1 {
                return Err(using_error(
                    if right_join { "left" } else { "right" },
                    (*column).as_str(),
                    inner_indexes.len(),
                ));
            }
            outer_matches[outer_indexes[0]] = Some((using_index, inner_indexes[0]));
            inner_matches[inner_indexes[0]] = Some(using_index);
        }

        // `coalesceCommonColumns` documents this exact order: common fields
        // occur in outer-child declaration order, followed by outer remainder
        // and inner remainder. RIGHT JOIN's outer child is original right.
        let mut fields = Vec::with_capacity(outer.len() + inner.len() - using.len());
        let mut full_to_output_indices = vec![usize::MAX; full_fields.len()];
        for (index, field) in outer.iter().enumerate() {
            if let Some((_, inner_index)) = outer_matches[index] {
                let output_index = fields.len();
                fields.push(field.coalesced_with(&inner[inner_index]));
                full_to_output_indices[index] = output_index;
                full_to_output_indices[outer.len() + inner_index] = output_index;
            }
        }
        for (index, field) in outer.iter().cloned().enumerate() {
            if outer_matches[index].is_none() {
                let output_index = fields.len();
                fields.push(field);
                full_to_output_indices[index] = output_index;
            }
        }
        for (index, field) in inner.iter().cloned().enumerate() {
            if inner_matches[index].is_none() {
                let output_index = fields.len();
                fields.push(field);
                full_to_output_indices[outer_matches.len() + index] = output_index;
            }
        }
        // Every source column is either visible directly or mapped to its
        // canonical coalesced field. Keep this an invariant of the metadata
        // boundary so callers never need a special-case lookup.
        debug_assert!(full_to_output_indices
            .iter()
            .all(|index| *index != usize::MAX));
        (fields, full_to_output_indices)
    };

    if matches!(join.tp, JoinType::Left | JoinType::Right) {
        // The planner clears the not-null flag on the inner/right side before
        // any physical join is built. We copy that declaration into metadata;
        // row fabrication remains the executor's explicit gap.
        unsupported.push(JoinOutputUnsupported::RowNullExtension);
    }

    Ok(JoinOutputMetadata {
        fields,
        full_fields,
        full_to_output_indices,
        unsupported,
    })
}

/// Finds NATURAL JOIN's implicit USING names in planner outer-child order.
/// Duplicate names are retained once here and rejected by the same exact-one
/// matching check as explicit USING below.
fn natural_common_columns(outer: &[JoinOutputField], inner: &[JoinOutputField]) -> Vec<String> {
    let mut common = Vec::new();
    for field in outer {
        let name = &field.field.names.column.lower;
        if matching_indexes(inner, name).is_empty()
            || common
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(name))
        {
            continue;
        }
        common.push(name.clone());
    }
    common
}

fn declared_child(child: JoinOutputChild) -> Result<Vec<JoinOutputField>, JoinOutputSchemaError> {
    match child {
        JoinOutputChild::Fields(fields) => Ok(fields),
        JoinOutputChild::Derived => Err(JoinOutputSchemaError::DerivedRelation),
    }
}

fn matching_indexes(fields: &[JoinOutputField], column: &str) -> Vec<usize> {
    fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            field
                .field
                .names
                .column
                .lower
                .eq_ignore_ascii_case(column)
                .then_some(index)
        })
        .collect()
}

fn using_error(side: &'static str, column: &str, match_count: usize) -> JoinOutputSchemaError {
    if match_count == 0 {
        JoinOutputSchemaError::MissingUsingColumn {
            side,
            column: column.to_owned(),
        }
    } else {
        JoinOutputSchemaError::AmbiguousUsingColumn {
            side,
            column: column.to_owned(),
        }
    }
}
