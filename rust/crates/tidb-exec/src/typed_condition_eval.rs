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

//! The first real consumer of the planner's typed-condition request.
//!
//! This adapter evaluates the existing scalar `tidb_ast::Expr` domain over a
//! borrowed source-shaped `FullSchema` row using `tidb-expr`.  It returns a
//! tri-state truth value and deliberately leaves filtering policy, outer-row
//! mutation, and join result construction to the caller.

use tidb_ast::{BinaryOp, Expr};
use tidb_datatype::Datum;
use tidb_expr::{apply_binary, eval_in, truthy_of, Columns, EvalError};
use tidb_planner::condition_binding::ConditionBindingError;
use tidb_planner::join_condition::{
    EqualitySemantics, JoinCondition, JoinEquality, JoinSchema, UnsupportedJoinCondition,
};
use tidb_planner::predicate_partition::partition_predicates;
use tidb_planner::typed_condition::{ConditionEvaluationMode, TypedConditionRequest};

/// One planner-bound condition consumed by the live join executor.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ExecutableJoinCondition {
    Equality(JoinEquality),
    Residual(TypedConditionRequest),
}

impl ExecutableJoinCondition {
    pub(crate) fn equality_indices(&self) -> Option<(usize, usize)> {
        match self {
            Self::Equality(equality) => {
                Some((equality.left().side_index(), equality.right().side_index()))
            }
            Self::Residual(_) => None,
        }
    }
}

pub(crate) fn compile_join_condition(
    expression: &Expr,
    schema: &JoinSchema,
    mode: ConditionEvaluationMode,
) -> Result<ExecutableJoinCondition, TypedConditionEvalError> {
    match schema.classify_on(expression) {
        JoinCondition::Equality(equality) => Ok(ExecutableJoinCondition::Equality(equality)),
        JoinCondition::Unsupported(reason) if binding_failure(&reason) => {
            Err(TypedConditionEvalError::Binding(reason.into()))
        }
        JoinCondition::Unsupported(_) => {
            let plan = partition_predicates([expression.clone()], schema)?;
            let predicate = plan
                .predicates()
                .first()
                .expect("one input expression produces one planned predicate");
            if let Some(shape) = predicate.plan().opaque_shapes().first() {
                return Err(TypedConditionEvalError::UnsupportedShape(*shape));
            }
            Ok(ExecutableJoinCondition::Residual(
                predicate.typed_request(mode),
            ))
        }
    }
}

pub(crate) fn compile_using_conditions(
    names: &[String],
    schema: &JoinSchema,
) -> Result<Vec<ExecutableJoinCondition>, TypedConditionEvalError> {
    schema
        .bind_using(names.iter().cloned())
        .into_iter()
        .map(|condition| match condition {
            JoinCondition::Equality(equality) => Ok(ExecutableJoinCondition::Equality(equality)),
            JoinCondition::Unsupported(reason) => {
                Err(TypedConditionEvalError::Binding(reason.into()))
            }
        })
        .collect()
}

fn binding_failure(reason: &UnsupportedJoinCondition) -> bool {
    matches!(
        reason,
        UnsupportedJoinCondition::InvalidColumnPath
            | UnsupportedJoinCondition::UnknownColumn { .. }
            | UnsupportedJoinCondition::AmbiguousColumn { .. }
    )
}

/// SQL truth produced by one typed predicate evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateTruth {
    /// The predicate evaluated to TRUE.
    True,
    /// The predicate evaluated to FALSE.
    False,
    /// The predicate evaluated to SQL UNKNOWN (`NULL`).
    Unknown,
}

/// Row-indexed masks returned by the batch condition boundary.
///
/// `selected[i]` is true only when row `i` evaluated to SQL TRUE.  `unknown[i]`
/// is true only when row `i` evaluated to SQL UNKNOWN (`NULL`).  The masks are
/// deliberately kept separate: a WHERE-style owner can consume `selected`,
/// while an outer-join owner can consume `unknown` to preserve its
/// `has-null` status.  Neither mask mutates rows or applies a filtering policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicateBatchMask {
    selected: Vec<bool>,
    unknown: Vec<bool>,
}

impl PredicateBatchMask {
    /// Returns the row-indexed TRUE mask.
    pub fn selected(&self) -> &[bool] {
        &self.selected
    }

    /// Returns the row-indexed UNKNOWN mask.
    pub fn unknown(&self) -> &[bool] {
        &self.unknown
    }

    /// Returns the number of rows represented by both masks.
    pub fn len(&self) -> usize {
        self.selected.len()
    }

    /// Returns whether no rows were evaluated.
    pub fn is_empty(&self) -> bool {
        self.selected.is_empty()
    }
}

/// Why a reusable predicate buffer could not satisfy a requested batch size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateBatchBufferError {
    /// The caller's expected row count differs from the reusable buffer.
    LengthMismatch {
        /// Row count requested by the lifecycle owner.
        expected: usize,
        /// Row count currently held by the buffer.
        actual: usize,
    },
}

/// Reusable TRUE/UNKNOWN slices for one executor filter lifecycle.
///
/// `reset` mirrors the source row-based filter's new-batch initialization:
/// selected rows start TRUE and the nullable-result slice starts FALSE.
/// `replace` copies a completed [`PredicateBatchMask`] into the same buffers,
/// retaining capacity and preserving index alignment without copying rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicateBatchBuffer {
    selected: Vec<bool>,
    unknown: Vec<bool>,
}

impl PredicateBatchBuffer {
    /// Creates empty reusable slices with the requested initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            selected: Vec::with_capacity(capacity),
            unknown: Vec::with_capacity(capacity),
        }
    }

    /// Resets the slices for a new batch while retaining allocated capacity.
    pub fn reset(&mut self, len: usize) {
        self.selected.clear();
        self.selected.resize(len, true);
        self.unknown.clear();
        self.unknown.resize(len, false);
    }

    /// Replaces the current slices with a mask while retaining capacity.
    pub fn replace(&mut self, mask: &PredicateBatchMask) {
        self.reset(mask.len());
        self.selected.copy_from_slice(mask.selected());
        self.unknown.copy_from_slice(mask.unknown());
    }

    /// Returns the TRUE slice in source row order.
    pub fn selected(&self) -> &[bool] {
        &self.selected
    }

    /// Returns the UNKNOWN slice in source row order.
    pub fn unknown(&self) -> &[bool] {
        &self.unknown
    }

    /// Returns the current logical row count.
    pub fn len(&self) -> usize {
        self.selected.len()
    }

    /// Returns whether no rows are currently represented.
    pub fn is_empty(&self) -> bool {
        self.selected.is_empty()
    }

    /// Returns the shared capacity retained by both slices.
    pub fn capacity(&self) -> usize {
        self.selected.capacity().min(self.unknown.capacity())
    }

    /// Verifies that this buffer is aligned with an executor-owned row count.
    pub fn validate_len(&self, expected: usize) -> Result<(), PredicateBatchBufferError> {
        if self.len() != expected {
            return Err(PredicateBatchBufferError::LengthMismatch {
                expected,
                actual: self.len(),
            });
        }
        Ok(())
    }
}

/// Status of one outer row while an outer join consumes candidate matches.
///
/// These values mirror the source joiner's `outerRowStatusFlag`.  A row starts
/// as [`OuterRowStatus::Matched`] for a candidate batch; a FALSE predicate
/// changes it to [`OuterRowStatus::Unmatched`], while SQL UNKNOWN changes it
/// to [`OuterRowStatus::HasNull`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OuterRowStatus {
    /// No candidate satisfied the condition.
    Unmatched,
    /// At least one candidate satisfied the condition.
    Matched,
    /// A candidate evaluated to UNKNOWN and no TRUE candidate superseded it.
    HasNull,
}

/// Why a batch could not be applied to an outer-row status slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OuterRowStatusError {
    /// The status slice and predicate masks represent different row counts.
    LengthMismatch {
        /// Number of statuses supplied by the join owner.
        statuses: usize,
        /// Number of rows represented by the predicate masks.
        mask: usize,
    },
    /// The accumulated and current batch statuses represent different rows.
    MergeLengthMismatch {
        /// Number of statuses accumulated from earlier candidate batches.
        accumulated: usize,
        /// Number of statuses in the current candidate batch.
        batch: usize,
    },
}

/// Logical indexes and statuses that survive a predicate batch's selection.
///
/// The index vector keeps the source chunk's row positions explicit while the
/// status vector remains positionally aligned.  This is the status-only
/// counterpart of `CopySelectedJoinRowsDirect`; it does not copy or construct
/// any row values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedOuterRowStatuses {
    indices: Vec<usize>,
    statuses: Vec<OuterRowStatus>,
}

impl SelectedOuterRowStatuses {
    /// Returns selected logical row indexes in source order.
    pub fn indices(&self) -> &[usize] {
        &self.indices
    }

    /// Returns statuses aligned one-for-one with [`Self::indices`].
    pub fn statuses(&self) -> &[OuterRowStatus] {
        &self.statuses
    }

    /// Returns the number of selected rows.
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns whether no rows survived selection.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

/// One outer row that requires finalization after all candidate batches.
///
/// A finalization event is emitted in source-row order for `Unmatched` and
/// `HasNull` statuses only.  The caller decides how to append its default
/// inner/null-extension row; this value carries no row data or chunk state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OuterRowFinalization {
    index: usize,
    status: OuterRowStatus,
}

impl OuterRowFinalization {
    /// Returns the logical outer-row index to finalize.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns the cumulative status that triggered finalization.
    pub fn status(&self) -> OuterRowStatus {
        self.status
    }

    /// Returns whether a default-inner/null-extension row is required for this
    /// non-matched outer row.
    pub fn needs_default_inner(&self) -> bool {
        true
    }

    /// Returns whether the condition was UNKNOWN for this outer row.
    pub fn has_null(&self) -> bool {
        self.status == OuterRowStatus::HasNull
    }
}

/// Applies the source `filterAndCheckOuterRowStatus` transition without
/// materializing rows or coupling this boundary to a join algorithm.
///
/// The source joiner preinitializes every status to [`OuterRowStatus::Matched`]
/// for the candidate batch.  This pure function preserves TRUE statuses,
/// changes FALSE rows to [`OuterRowStatus::Unmatched`], and changes UNKNOWN
/// rows to [`OuterRowStatus::HasNull`].  Returning a new vector keeps status
/// ownership with the caller and leaves selection/chunk lifecycle outside the
/// typed-condition contract.
pub fn transition_outer_row_status(
    statuses: &[OuterRowStatus],
    mask: &PredicateBatchMask,
) -> Result<Vec<OuterRowStatus>, OuterRowStatusError> {
    if statuses.len() != mask.len() {
        return Err(OuterRowStatusError::LengthMismatch {
            statuses: statuses.len(),
            mask: mask.len(),
        });
    }
    let mut transitioned = statuses.to_vec();
    for (index, status) in transitioned.iter_mut().enumerate() {
        if mask.unknown[index] {
            *status = OuterRowStatus::HasNull;
        } else if !mask.selected[index] {
            *status = OuterRowStatus::Unmatched;
        }
    }
    Ok(transitioned)
}

/// Aligns selected logical indexes with their outer-row statuses.
///
/// This preserves the source order used by chunk selection: UNKNOWN and FALSE
/// rows remain represented by the full status slice for later outer-row
/// handling, while only TRUE rows are returned for selected-row copying.
pub fn select_outer_row_statuses(
    statuses: &[OuterRowStatus],
    mask: &PredicateBatchMask,
) -> Result<SelectedOuterRowStatuses, OuterRowStatusError> {
    if statuses.len() != mask.len() {
        return Err(OuterRowStatusError::LengthMismatch {
            statuses: statuses.len(),
            mask: mask.len(),
        });
    }
    let selected_len = mask.selected.iter().filter(|selected| **selected).count();
    let mut indices = Vec::with_capacity(selected_len);
    let mut selected_statuses = Vec::with_capacity(selected_len);
    for (index, selected) in mask.selected.iter().copied().enumerate() {
        if selected {
            indices.push(index);
            selected_statuses.push(statuses[index]);
        }
    }
    Ok(SelectedOuterRowStatuses {
        indices,
        statuses: selected_statuses,
    })
}

/// Produces source-order finalization events for cumulative outer statuses.
///
/// This is the pure status portion of the source joiner's final
/// `OnMissMatch(hasNull, outerRow, ...)` loop: matched rows are omitted,
/// unmatched rows carry `has_null = false`, and UNKNOWN rows carry
/// `has_null = true`.  Physical row lookup, default-inner construction, and
/// null-extension remain with the join owner.
pub fn finalize_outer_row_statuses(statuses: &[OuterRowStatus]) -> Vec<OuterRowFinalization> {
    statuses
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, status)| match status {
            OuterRowStatus::Matched => None,
            OuterRowStatus::Unmatched | OuterRowStatus::HasNull => {
                Some(OuterRowFinalization { index, status })
            }
        })
        .collect()
}

/// Merges one candidate batch into the cumulative outer-row status.
///
/// This is the pure status-only portion of the source index-lookup joiner:
/// [`OuterRowStatus::Matched`] always wins, [`OuterRowStatus::HasNull`] wins
/// over an unmatched row, and a FALSE/`Unmatched` candidate never erases an
/// earlier UNKNOWN or TRUE result.  The caller owns batch ordering and any
/// final null-extension/materialization step.
pub fn merge_outer_row_status(
    accumulated: &[OuterRowStatus],
    batch: &PredicateBatchMask,
) -> Result<Vec<OuterRowStatus>, OuterRowStatusError> {
    if accumulated.len() != batch.len() {
        return Err(OuterRowStatusError::MergeLengthMismatch {
            accumulated: accumulated.len(),
            batch: batch.len(),
        });
    }
    let mut merged = accumulated.to_vec();
    for (index, status) in merged.iter_mut().enumerate() {
        let current = if batch.selected[index] {
            OuterRowStatus::Matched
        } else if batch.unknown[index] {
            OuterRowStatus::HasNull
        } else {
            OuterRowStatus::Unmatched
        };
        if current == OuterRowStatus::Matched || *status == OuterRowStatus::Unmatched {
            *status = current;
        }
    }
    Ok(merged)
}

/// Merges one scalar candidate truth into an outer row's cumulative status.
///
/// The live row-vector join uses the same priority rule as the batch owner:
/// TRUE wins permanently, UNKNOWN survives FALSE, and FALSE alone remains
/// unmatched.
#[must_use]
pub(crate) fn merge_outer_row_truth(
    accumulated: OuterRowStatus,
    truth: PredicateTruth,
) -> OuterRowStatus {
    match (accumulated, truth) {
        (OuterRowStatus::Matched, _) | (_, PredicateTruth::True) => OuterRowStatus::Matched,
        (OuterRowStatus::HasNull, _) | (_, PredicateTruth::Unknown) => OuterRowStatus::HasNull,
        (OuterRowStatus::Unmatched, PredicateTruth::False) => OuterRowStatus::Unmatched,
    }
}

/// Why a typed condition could not be consumed at this boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypedConditionEvalError {
    /// Planner name binding failed; the executor must not guess a column.
    Binding(ConditionBindingError),
    /// The row-only join executor cannot consume this AST shape. Reject it at
    /// compilation rather than evaluating a partially bound expression.
    UnsupportedShape(tidb_planner::condition_binding::OpaqueConditionShape),
    /// The caller supplied a row that does not match the planner FullSchema
    /// width captured in the request.
    RowWidth {
        /// The FullSchema width captured by the planner request.
        expected: usize,
        /// The row width supplied by the executor caller.
        actual: usize,
    },
    /// The scalar expression evaluator rejected the expression or value
    /// domain; no fallback truth value is invented.
    Evaluation(EvalError),
    /// One row in a batch failed scalar evaluation.  The index keeps the
    /// source batch position visible to the executor caller.
    Batch {
        /// The zero-based row index in the input batch.
        index: usize,
        /// The scalar error produced while evaluating that row.
        source: Box<Self>,
    },
}

impl From<ConditionBindingError> for TypedConditionEvalError {
    fn from(value: ConditionBindingError) -> Self {
        Self::Binding(value)
    }
}

impl From<EvalError> for TypedConditionEvalError {
    fn from(value: EvalError) -> Self {
        Self::Evaluation(value)
    }
}

/// Evaluates one planner request against a source-shaped FullSchema row.
///
/// This is intentionally tri-state even for a request whose [`TruthPolicy`]
/// asks the caller to discard UNKNOWN.  The policy remains visible on the
/// request so outer/semi join owners can choose how to consume the result.
///
/// [`TruthPolicy`]: tidb_planner::typed_condition::TruthPolicy
pub fn evaluate_typed_condition(
    request: &TypedConditionRequest,
    row: &[Datum],
) -> Result<PredicateTruth, TypedConditionEvalError> {
    if row.len() != request.full_schema_width() {
        return Err(TypedConditionEvalError::RowWidth {
            expected: request.full_schema_width(),
            actual: row.len(),
        });
    }
    let columns = FullSchemaColumns { request, row };
    let value = eval_in(request.expression(), &columns)?;
    if value.is_null() {
        return Ok(PredicateTruth::Unknown);
    }
    match truthy_of(&value)? {
        Some(true) => Ok(PredicateTruth::True),
        Some(false) => Ok(PredicateTruth::False),
        None => Err(TypedConditionEvalError::Evaluation(EvalError::Unsupported(
            "string predicate",
        ))),
    }
}

pub(crate) fn evaluate_join_condition(
    condition: &ExecutableJoinCondition,
    full_row: &[Datum],
) -> Result<PredicateTruth, TypedConditionEvalError> {
    let equality = match condition {
        ExecutableJoinCondition::Residual(request) => {
            return evaluate_typed_condition(request, full_row)
        }
        ExecutableJoinCondition::Equality(equality) => equality,
    };
    let left =
        full_row
            .get(equality.left().full_index())
            .ok_or(TypedConditionEvalError::RowWidth {
                expected: equality.left().full_index() + 1,
                actual: full_row.len(),
            })?;
    let right =
        full_row
            .get(equality.right().full_index())
            .ok_or(TypedConditionEvalError::RowWidth {
                expected: equality.right().full_index() + 1,
                actual: full_row.len(),
            })?;
    let operator = match equality.semantics() {
        EqualitySemantics::ThreeValued => BinaryOp::Eq,
        EqualitySemantics::NullSafe => BinaryOp::NullEq,
    };
    let value = apply_binary(operator, left.clone(), right.clone())?;
    if value.is_null() {
        return Ok(PredicateTruth::Unknown);
    }
    Ok(if truthy_of(&value)? == Some(true) {
        PredicateTruth::True
    } else {
        PredicateTruth::False
    })
}

pub(crate) fn evaluate_join_conditions(
    conditions: &[ExecutableJoinCondition],
    full_row: &[Datum],
) -> Result<PredicateTruth, TypedConditionEvalError> {
    let mut unknown = false;
    for condition in conditions {
        match evaluate_join_condition(condition, full_row)? {
            PredicateTruth::True => {}
            PredicateTruth::False => return Ok(PredicateTruth::False),
            PredicateTruth::Unknown => unknown = true,
        }
    }
    Ok(if unknown {
        PredicateTruth::Unknown
    } else {
        PredicateTruth::True
    })
}

/// Evaluates one typed request over a source-shaped batch without materializing
/// joined rows or choosing a consumer policy.
///
/// This is the dependency-closed portion of TiDB's
/// `VectorizedFilterConsiderNull` contract: each row is evaluated through the
/// existing scalar Datum evaluator and represented by disjoint TRUE and
/// UNKNOWN masks.  Callers decide whether UNKNOWN is discarded (ordinary
/// filtering) or recorded as an outer-row status; selection slices, chunk
/// reuse, row null-extension, and join result construction remain outside this
/// boundary.
pub fn evaluate_typed_condition_batch<R>(
    request: &TypedConditionRequest,
    rows: &[R],
) -> Result<PredicateBatchMask, TypedConditionEvalError>
where
    R: AsRef<[Datum]>,
{
    let mut selected = Vec::with_capacity(rows.len());
    let mut unknown = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        match evaluate_typed_condition(request, row.as_ref()) {
            Ok(PredicateTruth::True) => {
                selected.push(true);
                unknown.push(false);
            }
            Ok(PredicateTruth::False) => {
                selected.push(false);
                unknown.push(false);
            }
            Ok(PredicateTruth::Unknown) => {
                selected.push(false);
                unknown.push(true);
            }
            Err(source) => {
                return Err(TypedConditionEvalError::Batch {
                    index,
                    source: Box::new(source),
                });
            }
        }
    }
    Ok(PredicateBatchMask { selected, unknown })
}

struct FullSchemaColumns<'a> {
    request: &'a TypedConditionRequest,
    row: &'a [Datum],
}

impl Columns for FullSchemaColumns<'_> {
    fn get(&self, path: &[String]) -> Option<Datum> {
        self.request
            .plan()
            .bindings()
            .iter()
            .find(|binding| binding.path() == path)
            .and_then(|binding| self.row.get(binding.column().full_index()))
            .cloned()
    }
}
