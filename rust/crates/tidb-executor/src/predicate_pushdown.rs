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

//! Coprocessor Selection lowering for planner-owned physical conditions.
//!
//! Go decides predicate placement before `executorBuilder.build`. This module
//! converts those retained expressions into the bounded scan description used
//! by local/staged-row evaluation and TiKV request construction; it does not
//! perform a second AST predicate-pushdown decision.
use std::collections::HashSet;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::{truthy_of, Columns};

use crate::executor::ExecError;

/// The comparison operators a scan filter accepts, which are exactly the ones
/// the bounded TiKV Selection lowering speaks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanComparisonOp {
    /// `=`
    Eq,
    /// `!=` / `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

/// One pushed conjunct, described independently of how it is evaluated.
///
/// A description is what a coprocessor lowering reads; the paired
/// [`Expression`] is what an in-process source evaluates. Both halves describe
/// the same conjunct, so a lowering may refuse any subset of them without
/// changing an answer -- see [`PushedScanFilter`].
///
/// # The `collation` a string comparison runs in
///
/// It is the FUNCTION's derived collation, not the column's declared one. Go
/// derives it over ALL the arguments (`expression/collation.go:290` for
/// `ast.In`, `:282` for `ast.Like`) and writes it onto the function's own
/// `FieldType`, which is what `ExprToPB` sends and what TiKV picks its
/// collator from. The two differ whenever an argument carries an explicit
/// `COLLATE`: that is EXPLICIT coercibility, and it outranks the column's
/// IMPLICIT.
///
/// Every variant that compares strings therefore CARRIES that collation
/// rather than letting each consumer re-derive it from `column_type`. Five
/// consumers had re-derived it, and each got a different answer than the
/// expression beside it; carrying it is what makes "the description and the
/// expression agree" true by construction instead of by repetition.
///
/// Each description is derived from the already-built physical expression,
/// so this collation is the function collation the planner retained.
#[derive(Clone, Debug, PartialEq)]
pub enum ScanPredicate {
    /// A column compared with a constant, in either operand order.
    Compare(ScanComparison),
    /// A comparison between two columns of the scanned table.
    ///
    /// Go's `scalarFuncToPBExpr` sends both `ColumnRef` children when the
    /// comparison is supported by TiKV. Keeping this separate from
    /// [`ScanComparison`] preserves the latter's literal-oriented contract
    /// for callers that build range predicates.
    ColumnCompare(ScanColumnComparison),
    /// `column IS NULL`, or `column IS NOT NULL` when `negated`.
    IsNull {
        /// Zero-based offset of the column in the scan's output row.
        column_offset: u32,
        /// The column's declared type.
        column_type: FieldType,
        /// `true` for the `IS NOT NULL` spelling.
        negated: bool,
    },
    /// `column IN (constants)`, or `NOT IN` when `negated`. The list is never
    /// empty and never holds [`Datum::Null`]: a NULL member makes the whole
    /// predicate's three-valued result depend on the non-matching case, which
    /// is not a shape this description promises, so such a conjunct stays
    /// residual.
    In {
        /// Zero-based offset of the column in the scan's output row.
        column_offset: u32,
        /// The column's declared type.
        column_type: FieldType,
        /// The constant list, in source order.
        literals: Vec<Datum>,
        /// `true` for the `NOT IN` spelling.
        negated: bool,
        /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s
        /// note on it.
        collation: tidb_datatype::Collation,
    },
    /// A pushable string scalar tested against non-NULL string constants.
    ///
    /// Go chooses `IN`'s signature from the tested expression's evaluation
    /// type, so this is not limited to a bare column: calls such as
    /// `SUBSTRING(column, 1, 2)` retain their complete TiPB scalar tree.
    ScalarIn {
        /// The string expression being tested.
        tested: tidb_expr::pushdown_catalog::PbScalar,
        /// The non-empty constant list, in source order.
        literals: Vec<Datum>,
        /// `true` for the `NOT IN` spelling.
        negated: bool,
        /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s
        /// note on it.
        collation: tidb_datatype::Collation,
    },
    /// `string_column LIKE constant_pattern` with a constant escape byte.
    /// `NOT LIKE` is represented by the existing [`Self::Not`] wrapper, as
    /// Go rewrites it to `UnaryNotInt(LikeSig(...))` before protobuf lowering.
    Like {
        /// Zero-based offset of the tested column in the scan output.
        column_offset: u32,
        /// The column's declared string type and collation.
        column_type: FieldType,
        /// Pattern bytes exactly as parsed from the SQL literal.
        pattern: Vec<u8>,
        /// Explicit escape byte, or the session default supplied by rewrite.
        escape: u8,
        /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s
        /// note on it.
        collation: tidb_datatype::Collation,
    },
    /// A builtin function call whose `tipb.ScalarFuncSig` the push-down
    /// catalog resolved and whose signature TiKV evaluates.
    ///
    /// The whole call -- name, signature, and the operand tree -- lives in
    /// [`tidb_expr::pushdown_catalog`], which is also what decides whether
    /// TiKV may evaluate it; nothing here holds a second opinion about
    /// either. A call reaching this variant has already been admitted;
    /// whether it can also be *encoded* is the lowering's own question
    /// (`pushdown_catalog::to_pb`), and refusing there costs network only.
    Builtin(tidb_expr::pushdown_catalog::PbScalar),
    /// One source conjunct that Go expands into multiple Selection
    /// conditions. The current producer is non-negated `BETWEEN`, whose
    /// lower and upper comparisons remain one local filter.
    And(Vec<ScanPredicate>),
    /// A disjunction of two or more descriptions, flattened out of the source
    /// `OR` chain in source order.
    Or(Vec<ScanPredicate>),
    /// `NOT description`.
    Not(Box<ScanPredicate>),
}

impl ScanPredicate {
    /// Remaps this predicate from an input schema into the projected schema
    /// described by `keep`. The returned predicate is otherwise identical.
    pub(crate) fn remapped_columns(&self, keep: &[usize]) -> Option<Self> {
        let mut predicate = self.clone();
        remap_scan_predicate(&mut predicate, keep)?;
        Some(predicate)
    }
}

/// A column-versus-constant comparison.
///
/// This names the scan input offset, the operator, the constant, and which
/// side the column was written on, which is everything
/// `PhysicalSelection.ToPB` needs and nothing that ties the description to
/// in-process evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanComparison {
    /// Zero-based offset of the column in the scan's output row.
    pub column_offset: u32,
    /// The column's declared type, which decides whether a lowering may
    /// treat the comparison as the signed-BIGINT shape TiKV accepts.
    pub column_type: FieldType,
    /// The constant's type after Go-equivalent constant folding and any
    /// comparison-domain cast. TiPB carries this metadata on the literal
    /// leaf, so retaining only the [`Datum`] is not sufficient for DECIMAL
    /// precision/scale or temporal FSP.
    pub literal_type: FieldType,
    /// The comparison operator, as written.
    pub op: ScanComparisonOp,
    /// The already-evaluated constant operand. Never [`Datum::Null`]: a NULL
    /// comparison is unknown for every row, which is not the "filter" shape
    /// this split describes, so such a conjunct stays residual.
    pub literal: Datum,
    /// `true` when the column was written on the left (`a > 5`), `false` for
    /// the flipped spelling (`5 < a`). The lowering preserves operand order
    /// rather than canonicalizing it, as the source protobuf does.
    pub column_on_left: bool,
    /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s note
    /// on it. Meaningful only for a string comparison.
    pub collation: tidb_datatype::Collation,
}

/// A source-ordered column-to-column comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanColumnComparison {
    /// Zero-based offset of the left operand in the scan output.
    pub left_offset: u32,
    /// Declared type of the left operand.
    pub left_type: FieldType,
    /// Zero-based offset of the right operand in the scan output.
    pub right_offset: u32,
    /// Declared type of the right operand.
    pub right_type: FieldType,
    /// The comparison operator, as written.
    pub op: ScanComparisonOp,
}

/// The conjuncts a scan agreed to apply itself, with both the description a
/// lowering reads and the expressions an in-process source evaluates.
#[derive(Clone, Debug)]
pub struct PushedScanFilter {
    predicates: Vec<ScanPredicate>,
    filters: Vec<Expression>,
    fast_paths: Vec<Option<FastScanFilter>>,
}

/// A pushed predicate whose per-row work can be reduced without changing the
/// expression evaluator's SQL semantics. The key is built once when the scan
/// is accepted, rather than once for every row and every `IN` literal.
#[derive(Clone, Debug)]
enum FastScanFilter {
    StringIn {
        column_offset: usize,
        collator: tidb_datatype::Collator,
        /// Go's `builtinInStringSig` builds a `set.StringSet` once and does
        /// hash membership per row. Keep the same O(1)-average lookup shape;
        /// sorting the keys and binary-searching them makes large `IN` lists
        /// (such as hbx-web3's maker list) needlessly O(log n) per row.
        keys: HashSet<Vec<u8>>,
        negated: bool,
    },
    Like {
        column_offset: usize,
        pattern: Vec<u8>,
        escape: u8,
        collation: tidb_datatype::Collation,
        negated: bool,
    },
}

impl PushedScanFilter {
    /// Builds the exact conditions already selected by a physical reader.
    ///
    /// These expressions need no second AST pushdown decision.  They are
    /// evaluated by the source for every local/staged row and translated to
    /// the remote request when the expression has a TiKV representation.
    #[must_use]
    pub(crate) fn from_physical_conditions(filters: Vec<Expression>) -> Self {
        let predicates = filters
            .iter()
            .filter_map(scan_predicate_from_expression)
            .collect();
        let fast_paths = vec![None; filters.len()];
        Self {
            predicates,
            filters,
            fast_paths,
        }
    }

    /// Whether every locally evaluated physical condition also has a remote
    /// description. A backend can only report on the descriptions it was
    /// sent; this receipt prevents an un-described condition from being
    /// mistaken for one the coprocessor applied.
    #[must_use]
    pub(crate) fn fully_described(&self) -> bool {
        self.predicates.len() == self.filters.len()
    }

    /// The pushed conjuncts in `WHERE` order, for a coprocessor lowering.
    #[must_use]
    pub fn predicates(&self) -> &[ScanPredicate] {
        &self.predicates
    }

    /// The built expressions the accepting source evaluates.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn filters(&self) -> &[Expression] {
        &self.filters
    }

    /// Adds predicates from a later offer without dropping any conjunct this
    /// source already accepted. Identical descriptions are retained once: the
    /// paired expression has the same semantics, and evaluating it twice can
    /// duplicate statement warnings.
    fn conjoin(&mut self, additional: &Self) {
        if additional.predicates.len() != additional.filters.len() {
            for filter in &additional.filters {
                self.filters.push(filter.clone());
                self.fast_paths.push(None);
            }
            return;
        }
        for (predicate, filter) in additional.predicates.iter().zip(&additional.filters) {
            if self.predicates.contains(predicate) {
                continue;
            }
            self.fast_paths
                .push(FastScanFilter::from_predicate(predicate));
            self.predicates.push(predicate.clone());
            self.filters.push(filter.clone());
        }
    }

    /// Whether anything was pushed at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Whether `row` satisfies every pushed conjunct.
    ///
    /// The row is evaluated by the same expression evaluator `SelectionExec`
    /// uses, including MySQL's three-valued logic, so moving a conjunct into
    /// the scan cannot change what it means.
    pub fn matches<C: Columns>(
        &self,
        ctx: &C,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<bool, ExecError> {
        for (filter, fast_path) in self.filters.iter().zip(&self.fast_paths) {
            if let Some(fast_path) = fast_path {
                if !fast_path.matches(row) {
                    return Ok(false);
                }
                continue;
            }
            if truthy_of(&filter.eval(ctx, row)?)? != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn remapped_columns(&self, keep: &[usize]) -> Option<Self> {
        let mut predicates = self.predicates.clone();
        for predicate in &mut predicates {
            remap_scan_predicate(predicate, keep)?;
        }
        let mut filters = self.filters.clone();
        for filter in &mut filters {
            remap_expression(filter, keep)?;
        }
        Some(Self {
            fast_paths: if predicates.len() == filters.len() {
                predicates
                    .iter()
                    .map(FastScanFilter::from_predicate)
                    .collect()
            } else {
                vec![None; filters.len()]
            },
            predicates,
            filters,
        })
    }
}

impl FastScanFilter {
    fn from_predicate(predicate: &ScanPredicate) -> Option<Self> {
        let (like, negated) = match predicate {
            ScanPredicate::Like { .. } => (predicate, false),
            ScanPredicate::Not(inner) if matches!(&**inner, ScanPredicate::Like { .. }) => {
                (&**inner, true)
            }
            _ => (predicate, false),
        };
        if let ScanPredicate::Like {
            column_offset,
            pattern,
            escape,
            collation,
            ..
        } = like
        {
            return Some(Self::Like {
                column_offset: usize::try_from(*column_offset).ok()?,
                pattern: pattern.clone(),
                escape: *escape,
                collation: *collation,
                negated,
            });
        }
        let (column_offset, column_type, literals, negated, collation) = match predicate {
            ScanPredicate::In {
                column_offset,
                column_type,
                literals,
                negated,
                collation,
            } => (*column_offset, column_type, literals, *negated, *collation),
            ScanPredicate::ScalarIn {
                tested: tidb_expr::pushdown_catalog::PbScalar::Column { offset, field_type },
                literals,
                negated,
                collation,
            } => (*offset, field_type, literals, *negated, *collation),
            _ => return None,
        };
        if !column_type.is_string() || literals.is_empty() {
            return None;
        }
        let collation = collation.name().to_owned();
        let keys = literals
            .iter()
            .map(|literal| literal.as_raw_bytes().map(|bytes| (literal, bytes)))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|(_, bytes)| tidb_datatype::get_collator(&collation).key(bytes))
            .collect::<HashSet<_>>();
        Some(Self::StringIn {
            column_offset: usize::try_from(column_offset).ok()?,
            // The PROBE has to use the same collator the keys were built
            // with, or the set is searched in a collation nothing was
            // inserted under.
            collator: tidb_datatype::get_collator(&collation),
            keys,
            negated,
        })
    }

    fn matches(&self, row: tidb_chunk::row::Row<'_>) -> bool {
        match self {
            Self::StringIn {
                column_offset,
                collator,
                keys,
                negated,
            } => {
                if row.is_null(*column_offset) {
                    // `NULL IN (...)` and `NULL NOT IN (...)` are both NULL,
                    // which a scan filter must reject just like `truthy_of`.
                    return false;
                }
                let key = collator.key(row.get_string(*column_offset).as_bytes());
                let found = keys.contains(&key);
                if *negated {
                    !found
                } else {
                    found
                }
            }
            Self::Like {
                column_offset,
                pattern,
                escape,
                collation,
                negated,
            } => {
                if row.is_null(*column_offset) {
                    return false;
                }
                let matched = tidb_expr::like_match_with_collation(
                    row.get_string(*column_offset).as_bytes(),
                    pattern,
                    Some(*escape),
                    *collation,
                );
                if *negated {
                    !matched
                } else {
                    matched
                }
            }
        }
    }
}

fn remapped_offset(offset: u32, keep: &[usize]) -> Option<u32> {
    keep.iter()
        .position(|kept| *kept == offset as usize)
        .and_then(|offset| u32::try_from(offset).ok())
}

pub(crate) fn remap_scan_predicate(predicate: &mut ScanPredicate, keep: &[usize]) -> Option<()> {
    match predicate {
        ScanPredicate::Compare(comparison) => {
            comparison.column_offset = remapped_offset(comparison.column_offset, keep)?;
        }
        ScanPredicate::ColumnCompare(comparison) => {
            comparison.left_offset = remapped_offset(comparison.left_offset, keep)?;
            comparison.right_offset = remapped_offset(comparison.right_offset, keep)?;
        }
        ScanPredicate::IsNull { column_offset, .. } | ScanPredicate::In { column_offset, .. } => {
            *column_offset = remapped_offset(*column_offset, keep)?;
        }
        ScanPredicate::ScalarIn { tested, .. } => remap_pb_scalar(tested, keep)?,
        ScanPredicate::Like { column_offset, .. } => {
            *column_offset = remapped_offset(*column_offset, keep)?;
        }
        ScanPredicate::Builtin(scalar) => remap_pb_scalar(scalar, keep)?,
        ScanPredicate::And(branches) | ScanPredicate::Or(branches) => {
            for branch in branches {
                remap_scan_predicate(branch, keep)?;
            }
        }
        ScanPredicate::Not(inner) => remap_scan_predicate(inner, keep)?,
    }
    Some(())
}

fn remap_pb_scalar(
    scalar: &mut tidb_expr::pushdown_catalog::PbScalar,
    keep: &[usize],
) -> Option<()> {
    match scalar {
        tidb_expr::pushdown_catalog::PbScalar::Column { offset, .. } => {
            *offset = remapped_offset(*offset, keep)?;
        }
        tidb_expr::pushdown_catalog::PbScalar::Call { args, .. } => {
            for argument in args {
                remap_pb_scalar(argument, keep)?;
            }
        }
        tidb_expr::pushdown_catalog::PbScalar::IntLiteral(_)
        | tidb_expr::pushdown_catalog::PbScalar::NullLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::UIntLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::DecimalLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::RealLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::StringLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::BytesLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::BitLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::EnumLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::TimeLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::DurationLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::JsonLiteral { .. }
        | tidb_expr::pushdown_catalog::PbScalar::VectorLiteral { .. } => {}
    }
    Some(())
}

pub(crate) fn remap_expression(expression: &mut Expression, keep: &[usize]) -> Option<()> {
    match expression {
        Expression::Column(column) => {
            let old = usize::try_from(column.index).ok()?;
            column.index = i64::try_from(keep.iter().position(|kept| *kept == old)?).ok()?;
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                remap_expression(argument, keep)?;
            }
        }
        Expression::Constant(_) => {}
        Expression::CorrelatedColumn(_) => return None,
    }
    Some(())
}

/// A one-row staging area an in-process source filters through.
///
/// A source holds its rows as `Datum`s but the evaluator reads chunk rows, so
/// each candidate row is appended here, tested, and only then copied into the
/// output chunk.
pub struct ScanFilterProbe {
    filter: PushedScanFilter,
    ctx: crate::StmtContext,
    scratch: Chunk,
}

impl ScanFilterProbe {
    pub(crate) fn new(filter: PushedScanFilter, ctx: crate::StmtContext, scratch: Chunk) -> Self {
        Self {
            filter,
            ctx,
            scratch,
        }
    }

    /// Adds a later accepted offer to the existing conjunction.
    ///
    /// The additional filter is compiled against the source's current row
    /// space, which is also the row space described by `scratch`.
    pub(crate) fn conjoin(&mut self, additional: &PushedScanFilter) {
        self.filter.conjoin(additional);
    }

    /// Whether `row` passes every pushed conjunct.
    pub(crate) fn admits(&mut self, row: &[Datum]) -> Result<bool, ExecError> {
        self.scratch.reset();
        for (column, value) in row.iter().enumerate() {
            self.scratch.append_datum(column, value);
        }
        // The filter evaluates in the coprocessor's seat, so its warnings
        // follow TiKV's reporting contract: distinct messages per response,
        // not one per row (see `StmtContext::enter_cop_eval`).
        let _cop = self.ctx.enter_cop_eval();
        self.filter.matches(&self.ctx, self.scratch.get_row(0))
    }

    pub(crate) fn remapped_columns(&self, keep: &[usize], scratch: Chunk) -> Option<Self> {
        Some(Self {
            filter: self.filter.remapped_columns(keep)?,
            ctx: self.ctx.clone(),
            scratch,
        })
    }

    pub(crate) fn predicates(&self) -> &[ScanPredicate] {
        self.filter.predicates()
    }

    pub(crate) fn fully_described(&self) -> bool {
        self.filter.fully_described()
    }
}

/// Describes one already-resolved physical condition for TiKV.
///
/// Go performs this conversion after `findBestTask` has selected the reader;
/// the executor receives that exact `expression.Expression`, not the original
/// SQL AST. Keep comparisons in the explicit scan representation used by the
/// local fakes, and use the shared scalar-signature catalog for every other
/// expression family. A refusal leaves the physical expression in the local
/// filter and is therefore semantic, not heuristic.
pub(crate) fn scan_predicate_from_expression(expression: &Expression) -> Option<ScanPredicate> {
    let Expression::ScalarFunction(function) = expression else {
        return tidb_expr::pushdown_catalog::from_expression(expression)
            .map(ScanPredicate::Builtin);
    };
    let operation = match function.func_name.lowercase().as_ref() {
        "eq" => Some(ScanComparisonOp::Eq),
        "ne" => Some(ScanComparisonOp::Ne),
        "lt" => Some(ScanComparisonOp::Lt),
        "le" => Some(ScanComparisonOp::Le),
        "gt" => Some(ScanComparisonOp::Gt),
        "ge" => Some(ScanComparisonOp::Ge),
        _ => None,
    };
    if let (Some(operation), [left, right]) = (operation, function.args.as_slice()) {
        if let (Expression::Column(left), Expression::Column(right)) = (left, right) {
            return Some(ScanPredicate::ColumnCompare(ScanColumnComparison {
                left_offset: u32::try_from(left.index).ok()?,
                left_type: left.get_static_type()?.clone(),
                right_offset: u32::try_from(right.index).ok()?,
                right_type: right.get_static_type()?.clone(),
                op: operation,
            }));
        }
        let (column, constant, column_on_left) = match (left, right) {
            (Expression::Column(column), Expression::Constant(constant)) => {
                (column, constant, true)
            }
            (Expression::Constant(constant), Expression::Column(column)) => {
                (column, constant, false)
            }
            _ => {
                return tidb_expr::pushdown_catalog::from_expression(expression)
                    .map(ScanPredicate::Builtin);
            }
        };
        if constant.value != Datum::Null {
            return Some(ScanPredicate::Compare(ScanComparison {
                column_offset: u32::try_from(column.index).ok()?,
                collation: column.get_static_type()?.collation(),
                column_type: column.get_static_type()?.clone(),
                literal_type: constant.get_static_type()?.clone(),
                op: operation,
                literal: constant.value.clone(),
                column_on_left,
            }));
        }
    }
    tidb_expr::pushdown_catalog::from_expression(expression).map(ScanPredicate::Builtin)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_txnkv::Key;

    use crate::cluster_storage::{
        ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
    };
    use crate::driver::{run_select_on, Catalog};
    use crate::kv_table::{KvColumn, KvTable};
    use crate::storage::StorageError;

    /// A snapshot over a fixed map: the committed half of a cluster read.
    #[derive(Debug, Default)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for MockSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: long(),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    /// Publishes everything the buffer stages into the snapshot and empties
    /// it: what COMMIT does to the two halves of a cluster read.
    fn commit(buffer: &MutationBuffer, snapshot: &Arc<Mutex<MockSnapshot>>) {
        let mut snapshot = snapshot.lock().unwrap();
        for (key, value) in buffer.snapshot() {
            match value {
                Some(value) => snapshot.data.insert(key.as_bytes().to_vec(), value),
                None => snapshot.data.remove(key.as_bytes()),
            };
        }
        buffer.reset();
    }

    /// A pushed predicate must filter the transaction's own staged rows too.
    ///
    /// Over `ClusterTableStorage` a scan produces the snapshot merged with the
    /// session's staged mutation buffer. Pushing a conjunct into the scan
    /// removes it from the `Selection` above, so the scan becomes the only
    /// place it is applied: a staged row that fails it must not be returned,
    /// and a staged row that satisfies it must not be lost -- including when
    /// a staged UPDATE moves a row across the predicate's boundary and when a
    /// staged DELETE removes a row the committed half still holds.
    #[test]
    fn a_pushed_predicate_filters_staged_rows_as_well_as_committed_ones() {
        let snapshot = Arc::new(Mutex::new(MockSnapshot::default()));
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let buffer = MutationBuffer::new();
        let mut table = KvTable::with_storage(
            91,
            vec![column("a", 1), column("b", 2)],
            Box::new(ClusterTableStorage::new(buffer.clone(), handle)),
        );

        // Committed half: one row above the predicate, one below.
        let committed_low = table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(9), Datum::Int(90)], &tidb_expr::NoColumns)
            .unwrap();
        let committed_moved = table
            .insert_row(&[Datum::Int(2), Datum::Int(20)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&buffer, &snapshot);
        assert!(buffer.is_empty(), "nothing is staged after the commit");

        // Staged half, all inside one open transaction:
        //   * an INSERT that satisfies `a > 5`,
        //   * an INSERT that does not,
        //   * an UPDATE that lifts a committed row across the boundary,
        //   * a DELETE of a committed row that satisfies it.
        table
            .insert_row(&[Datum::Int(7), Datum::Int(70)], &tidb_expr::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(3), Datum::Int(30)], &tidb_expr::NoColumns)
            .unwrap();
        table
            .update_row_with_context(
                &committed_moved,
                &[Datum::Int(8), Datum::Int(80)],
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        table
            .delete_row_with_context(
                &committed_low,
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        assert!(!buffer.is_empty(), "the writes are staged, not committed");

        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a, b FROM t WHERE a > 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(7), Datum::Int(70)],
                vec![Datum::Int(8), Datum::Int(80)],
                vec![Datum::Int(9), Datum::Int(90)],
            ],
            "staged inserts and updates are filtered by the pushed predicate, \
             not waved through it"
        );
        // The residual half of a split predicate still runs above the scan.
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM t WHERE a > 5 AND b + 1 < 80 ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![vec![Datum::Int(7), Datum::Int(70)]]
        );
        // A staged row the predicate excludes is not reachable by any spelling.
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a = 3", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(3)]],
            "and it is still there when the predicate selects it"
        );
        // The composed descriptions carry the same obligation, because they are
        // removed from the `Selection` in exactly the same way: a staged row
        // must be tested by the pushed `IN` and the pushed `OR` too.
        assert_eq!(
            run_select_on(
                "SELECT a FROM t WHERE a IN (3, 7, 8) ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(8)],
            ],
            "the staged insert, the staged update's new value, and nothing else"
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM t WHERE a = 1 OR a = 8 ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![vec![Datum::Int(8)]],
            "the staged DELETE removed the row `a = 1` matched, and the staged \
             UPDATE created the row `a = 8` matches"
        );
        // A pushed BUILTIN carries the same obligation, and widening what may
        // be pushed widens what the obligation covers -- so it is re-proved
        // here rather than assumed. `MOD(a, 4)` is 0 (false) for `a = 8`, the
        // value a staged UPDATE created, and truthy for the staged INSERTs; a
        // source that filtered the snapshot half only would return `a = 8`.
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE mod(a, 4) ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(9)],
            ],
            "the staged UPDATE's new value `a = 8` fails the pushed builtin and \
             must not be returned, while the staged inserts that pass it are"
        );
    }

    /// The whole predicate must survive when nothing is pushed, and the split
    /// must not change any answer a `Selection` alone produced.
    #[test]
    fn splitting_a_where_does_not_change_its_result() {
        let mut table = KvTable::new(92, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [(1, 10), (5, 50), (7, 70), (9, 90)] {
            table
                .insert_row(&[Datum::Int(a), Datum::Int(b)], &tidb_expr::NoColumns)
                .unwrap();
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let cases: [(&str, Vec<Vec<Datum>>); 5] = [
            (
                "SELECT a FROM t WHERE a > 5",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
            (
                "SELECT a FROM t WHERE 5 < a",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
            (
                "SELECT a FROM t WHERE a > 5 AND b < 80",
                vec![vec![Datum::Int(7)]],
            ),
            // A disjunction now pushes as one conjunct, and must answer the
            // same rows it did as a residual `Selection`.
            (
                "SELECT a FROM t WHERE a = 1 OR a = 9",
                vec![vec![Datum::Int(1)], vec![Datum::Int(9)]],
            ),
            // Mixed: `a > 1` pushes, the arithmetic stays above.
            (
                "SELECT a FROM t WHERE a > 1 AND b + 1 > 60",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// The composed predicates, over a table with NULLs, against the answer
    /// MySQL's three-valued logic gives.
    ///
    /// This is the pushed form and the local form agreeing: each `WHERE` below
    /// moves into the scan as one described conjunct, so the scan is the only
    /// place it is applied, and the expected rows are the ones the same
    /// predicate produces as a `Selection` -- including the cases where
    /// UNKNOWN, not FALSE, is the reason a row is absent.
    #[test]
    fn the_composed_predicates_keep_mysqls_three_valued_answers() {
        let mut table = KvTable::new(93, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [
            (Datum::Int(1), Datum::Int(10)),
            (Datum::Int(2), Datum::Null),
            (Datum::Null, Datum::Int(30)),
            (Datum::Int(4), Datum::Int(40)),
        ] {
            table.insert_row(&[a, b], &tidb_expr::NoColumns).unwrap();
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let int = |value: i64| vec![Datum::Int(value)];
        let cases: [(&str, Vec<Vec<Datum>>); 10] = [
            ("SELECT a FROM t WHERE a IS NULL", vec![vec![Datum::Null]]),
            (
                "SELECT a FROM t WHERE a IS NOT NULL",
                vec![int(1), int(2), int(4)],
            ),
            (
                "SELECT a FROM t WHERE b IS NULL",
                // The row whose `b` is NULL, whatever its `a` is.
                vec![int(2)],
            ),
            ("SELECT a FROM t WHERE a IN (1, 4)", vec![int(1), int(4)]),
            // `NOT IN` is UNKNOWN for the NULL row, so it is absent -- and it
            // would be present if `NOT IN` were pushed as a plain negation of
            // membership over a NULL-blind test.
            ("SELECT a FROM t WHERE a NOT IN (1, 4)", vec![int(2)]),
            ("SELECT a FROM t WHERE a = 1 OR a = 4", vec![int(1), int(4)]),
            // `OR` over a branch that is UNKNOWN for the NULL row: TRUE wins.
            (
                "SELECT a FROM t WHERE a = 1 OR a IS NULL",
                vec![int(1), vec![Datum::Null]],
            ),
            ("SELECT a FROM t WHERE NOT a = 1", vec![int(2), int(4)]),
            (
                "SELECT a FROM t WHERE a > -1 AND a < 3",
                vec![int(1), int(2)],
            ),
            // A pushed disjunction beside a residual conjunct: both apply.
            (
                "SELECT a FROM t WHERE (a = 1 OR a = 4) AND b + 1 > 20",
                vec![int(4)],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// A pushed builtin call answers the rows the same call answers as a
    /// `Selection`, including where the answer is NULL rather than false.
    ///
    /// This is the local half of the newly pushed math family: the conjunct is
    /// removed from the `Selection` above the scan, so the scan is the only
    /// place it is applied, and every expectation below is the row set MySQL's
    /// three-valued logic gives for the same predicate.
    #[test]
    fn a_pushed_math_builtin_answers_what_the_same_selection_answered() {
        let mut table = KvTable::new(94, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [
            (Datum::Int(0), Datum::Int(3)),
            (Datum::Int(1), Datum::Int(0)),
            (Datum::Int(2), Datum::Int(7)),
            (Datum::Int(7), Datum::Int(7)),
            (Datum::Null, Datum::Int(1)),
        ] {
            table.insert_row(&[a, b], &tidb_expr::NoColumns).unwrap();
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let int = |value: i64| vec![Datum::Int(value)];
        let cases: [(&str, Vec<Vec<Datum>>); 8] = [
            // `SIN(0)` is 0, which is false; the NULL row is UNKNOWN.
            ("SELECT a FROM t WHERE sin(a)", vec![int(1), int(2), int(7)]),
            // `MOD(a, 3)` is 0 for a = 0 and a = 3, and NULL for the NULL row.
            (
                "SELECT a FROM t WHERE mod(a, 3)",
                vec![int(1), int(2), int(7)],
            ),
            // A zero divisor is NULL in MySQL, so no row qualifies -- and it
            // must not be an error either, on the scan or above it.
            ("SELECT a FROM t WHERE mod(a, 0)", vec![]),
            // `ACOS` of anything outside [-1, 1] is NULL, not an error, and
            // `ACOS(1)` is exactly 0, which is false -- so only `a = 0`
            // (`ACOS(0)` = pi/2) qualifies.
            ("SELECT a FROM t WHERE acos(a)", vec![int(0)]),
            // `ROUND` over an integer column keeps the integer domain.
            (
                "SELECT a FROM t WHERE round(a)",
                vec![int(1), int(2), int(7)],
            ),
            // `PI()` is a nonzero constant, so it selects every row -- the NULL
            // one included, since the predicate does not read the column.
            (
                "SELECT a FROM t WHERE pi()",
                vec![int(0), int(1), int(2), int(7), vec![Datum::Null]],
            ),
            // `POW(a, 2)` is 0 only for a = 0.
            (
                "SELECT a FROM t WHERE pow(a, 2)",
                vec![int(1), int(2), int(7)],
            ),
            // A pushed builtin beside a pushed comparison and a residual
            // conjunct: all three still apply.
            (
                "SELECT a FROM t WHERE sin(a) AND a > 1 AND b + 1 > 7",
                vec![int(2), int(7)],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// A call the catalog does not hold stays above the scan, and so does one
    /// whose argument is not describable -- neither may quietly become a
    /// pushed conjunct, because the store would then evaluate what only this
    /// engine evaluates.
    #[test]
    fn a_call_outside_the_catalog_stays_above_the_scan() {
        let mut table = KvTable::new(95, vec![column("a", 1), column("b", 2)]);
        for a in [1_i64, 2, 3] {
            table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        // `TAN` is deliberately absent from Go's TiKV whitelist (the comment
        // cites Rust's LLVM math precision), `ABS` is on it but has no catalog
        // row yet, and `SIN(a + 1)` has an argument the description cannot
        // carry. All three must still answer correctly.
        for (sql, expected) in [
            ("SELECT a FROM t WHERE tan(a)", vec![1_i64, 2, 3]),
            ("SELECT a FROM t WHERE abs(a)", vec![1, 2, 3]),
            ("SELECT a FROM t WHERE sin(a + 1)", vec![1, 2, 3]),
            ("SELECT a FROM t WHERE mod(a, b)", vec![1, 2, 3]),
        ] {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected
                    .into_iter()
                    .map(|a| vec![Datum::Int(a)])
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        }
    }
}
