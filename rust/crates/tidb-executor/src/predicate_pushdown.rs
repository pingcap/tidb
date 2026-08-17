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

//! Predicate pushdown into a base-table scan: Go's
//! `rule_predicate_push_down.go` split, expressed for this tier's sources.
//!
//! # The split
//!
//! Go flattens the `WHERE` into conjuncts and gives each one to the deepest
//! plan node whose schema covers its columns; `expression.PushDownExprs` then
//! decides which of those the coprocessor can actually evaluate, and the rest
//! stay in a root `Selection`. [`split_scan_predicates`] performs the same two
//! steps at once, over the *predicate* shapes TiKV's own whitelist admits
//! unconditionally (`infer_pushdown.go`'s `scalarExprSupportedByTiKV`):
//!
//! * a comparison between one column of the scanned table and one constant, in
//!   either operand order;
//! * `column IS [NOT] NULL`;
//! * `column [NOT] IN (constants)`;
//! * `OR` and `NOT` over any of the above, to any depth;
//! * a **builtin function call** whose `tipb.ScalarFuncSig` the push-down
//!   catalog resolves and whose signature that catalog says TiKV evaluates
//!   ([`tidb_expr::pushdown_catalog`]) -- `sin(a)`, `mod(a, 7)`, `round(a)`
//!   and the rest of the math family today. The catalog is the *only* thing
//!   that answers either question, so widening the set is adding a row to its
//!   table rather than a branch here.
//!
//! Every other conjunct -- an expression over a column (`b + 1 < 10`), a
//! column-to-column comparison, `IS TRUE`, NULL-safe equality, a subquery, a
//! call the catalog does not hold, anything referring to a second table -- is
//! residual and is left for the `Selection` above the scan. A nested `AND`
//! inside an `OR` branch is residual too: the top-level `AND` is what this
//! split flattens, and describing an inner one would need a connective the
//! description does not carry.
//!
//! Note that the split is deliberately **type-agnostic** for the predicate
//! shapes: it describes the conjunct, and the coprocessor lowering
//! (`tidb_exec::wide_scan_selection`) applies the type gate that decides
//! whether the description can actually travel. Keeping those two decisions
//! apart is what lets an in-process source take a conjunct the wire cannot
//! carry. A builtin call is not type-agnostic and cannot be: Go picks the
//! signature *from* the argument types, so the description carries the
//! resolved signature and the lowering re-checks that the scan descriptor's own
//! declared column types are the ones it was resolved from.
//!
//! # Why pushing a builtin cannot make a query fail that used to work
//!
//! A pushed conjunct is evaluated by [`PushedScanFilter::matches`], which is
//! the same [`Expression::eval`] and the same [`truthy_of`] that
//! [`SelectionExec`](crate::selection::SelectionExec) applies to a residual
//! conjunct, with the same [`crate::StmtContext`]. Moving a conjunct from the
//! `Selection` into the scan therefore changes *where* it runs and nothing
//! about what it means, including which values are NULL and which statement
//! warnings it raises.
//!
//! Being a strict subset of what Go pushes is safe in the only direction that
//! matters: a conjunct that stays above the scan is still applied, so the
//! result set cannot change. Widening the set is a separate, verifiable step.
//!
//! # The staged-buffer obligation
//!
//! A pushed conjunct is *removed* from the `Selection` above the scan, so the
//! scan becomes the only place it is ever applied. Over
//! [`ClusterTableStorage`](crate::cluster_storage::ClusterTableStorage) the
//! rows a scan produces are not only the snapshot's: the session's staged
//! mutation buffer is merged into the same key-ordered stream, so a row this
//! statement's own transaction wrote appears there and *never passed through
//! any coprocessor*. If a source applied a pushed predicate to the snapshot
//! half only, a staged row that fails the predicate would be returned and a
//! staged row that satisfies it could be dropped.
//!
//! That is why [`TableAccess::accept_scan_filter`] is opt-in and defaults to
//! refusing: a source may only return `true` when it applies every pushed
//! conjunct to *every* row it emits, merged rows included. A future
//! coprocessor-backed source that filters only the snapshot half must either
//! keep applying the predicate to the merged staged rows itself, or refuse --
//! in which case the driver leaves the whole `WHERE` in the `Selection` and
//! nothing changes.
//!
//! [`TableAccess::accept_scan_filter`]: crate::table_access::TableAccess::accept_scan_filter

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

impl ScanComparisonOp {
    /// The operator of a binary AST node, when it is one this filter accepts.
    #[must_use]
    pub const fn from_ast(op: tidb_ast::BinaryOp) -> Option<Self> {
        Some(match op {
            tidb_ast::BinaryOp::Eq => Self::Eq,
            tidb_ast::BinaryOp::Ne => Self::Ne,
            tidb_ast::BinaryOp::Lt => Self::Lt,
            tidb_ast::BinaryOp::Le => Self::Le,
            tidb_ast::BinaryOp::Gt => Self::Gt,
            tidb_ast::BinaryOp::Ge => Self::Ge,
            _ => return None,
        })
    }
}

/// One pushed conjunct, described independently of how it is evaluated.
///
/// A description is what a coprocessor lowering reads; the paired
/// [`Expression`] is what an in-process source evaluates. Both halves describe
/// the same conjunct, so a lowering may refuse any subset of them without
/// changing an answer -- see [`PushedScanFilter`].
#[derive(Clone, Debug, PartialEq)]
pub enum ScanPredicate {
    /// A column compared with a constant, in either operand order.
    Compare(ScanComparison),
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
    /// A disjunction of two or more descriptions, flattened out of the source
    /// `OR` chain in source order.
    Or(Vec<ScanPredicate>),
    /// `NOT description`.
    Not(Box<ScanPredicate>),
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
}

/// The conjuncts a scan agreed to apply itself, with both the description a
/// lowering reads and the expressions an in-process source evaluates.
#[derive(Clone, Debug)]
pub struct PushedScanFilter {
    predicates: Vec<ScanPredicate>,
    filters: Vec<Expression>,
}

impl PushedScanFilter {
    /// Pairs each described comparison with the expression that evaluates it.
    ///
    /// # Panics
    /// If the two halves differ in length -- they describe the same conjuncts,
    /// so a mismatch is a construction bug rather than a runtime condition.
    #[must_use]
    pub fn new(predicates: Vec<ScanPredicate>, filters: Vec<Expression>) -> Self {
        assert_eq!(
            predicates.len(),
            filters.len(),
            "every pushed conjunct has one description and one expression"
        );
        Self {
            predicates,
            filters,
        }
    }

    /// The pushed conjuncts in `WHERE` order, for a coprocessor lowering.
    #[must_use]
    pub fn predicates(&self) -> &[ScanPredicate] {
        &self.predicates
    }

    /// The built expressions the accepting source evaluates.
    #[must_use]
    pub(crate) fn filters(&self) -> &[Expression] {
        &self.filters
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
        for filter in &self.filters {
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
            predicates,
            filters,
        })
    }
}

fn remapped_offset(offset: u32, keep: &[usize]) -> Option<u32> {
    keep.iter()
        .position(|kept| *kept == offset as usize)
        .and_then(|offset| u32::try_from(offset).ok())
}

fn remap_scan_predicate(predicate: &mut ScanPredicate, keep: &[usize]) -> Option<()> {
    match predicate {
        ScanPredicate::Compare(comparison) => {
            comparison.column_offset = remapped_offset(comparison.column_offset, keep)?;
        }
        ScanPredicate::IsNull { column_offset, .. } | ScanPredicate::In { column_offset, .. } => {
            *column_offset = remapped_offset(*column_offset, keep)?;
        }
        ScanPredicate::Builtin(scalar) => remap_pb_scalar(scalar, keep)?,
        ScanPredicate::Or(branches) => {
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
        tidb_expr::pushdown_catalog::PbScalar::IntLiteral(_) => {}
    }
    Some(())
}

fn remap_expression(expression: &mut Expression, keep: &[usize]) -> Option<()> {
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

    /// Whether `row` passes every pushed conjunct.
    pub(crate) fn admits(&mut self, row: &[Datum]) -> Result<bool, ExecError> {
        self.scratch.reset();
        for (column, value) in row.iter().enumerate() {
            self.scratch.append_datum(column, value);
        }
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
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_txnkv::Key;

    use super::ScanComparisonOp;
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
            generated: None,
        }
    }

    /// Publishes everything the buffer stages into the snapshot and empties
    /// it: what COMMIT does to the two halves of a cluster read.
    fn commit(buffer: &MutationBuffer, snapshot: &Arc<Mutex<MockSnapshot>>) {
        let mut snapshot = snapshot.lock().unwrap();
        for (key, value) in buffer.staged() {
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

    #[test]
    fn only_the_lowerable_comparison_operators_are_accepted() {
        assert_eq!(
            ScanComparisonOp::from_ast(tidb_ast::BinaryOp::Ge),
            Some(ScanComparisonOp::Ge)
        );
        assert_eq!(ScanComparisonOp::from_ast(tidb_ast::BinaryOp::Plus), None);
        // NULL-safe equality is not the same function as `eq` on the
        // coprocessor side, so it stays residual.
        assert_eq!(ScanComparisonOp::from_ast(tidb_ast::BinaryOp::NullEq), None);
    }
}

/// The port of Go `TestExprPushDownToTiKV`
/// (`pkg/expression/expr_to_pb_test.go:1547`): which expressions may be
/// evaluated by the coprocessor.
///
/// # What the port can and cannot assert
///
/// Go's table is a list of scalar functions built over typed columns, each
/// checked with `PushDownExprs(..., kv.TiKV)`. **Every row of it is a builtin
/// function call** -- there is not one comparison, `AND`, `OR`, `IS NULL` or
/// `IN` row in the table, which was confirmed against the Go source rather
/// than assumed.
///
/// Reaching those rows needs a name-to-`ScalarFuncSig` resolution catalog and
/// the cast-inserting type inference Go's `getFunction` performs, because
/// [`ScanPredicate`] describes a *shape* and `ScalarFunction` in this tree
/// carries no resolved signature at all. That catalog now exists
/// ([`tidb_expr::pushdown_catalog`]) and is the single owner of both the
/// signature and TiKV's verdict on it, so a row of Go's table moves here
/// exactly when the catalog holds its family.
///
/// The table is therefore split in two, and the split is the honest statement
/// of where this engine stands:
///
/// * [`GO_PUSHES_HERE_TOO`] -- the families the catalog holds, asserted
///   *running* against Go's verdict, with the signature each resolves to
///   pinned separately in [`tidb_expr::pushdown_catalog`]'s own tests and the
///   rows-returned agreement proved against a real cluster by
///   `rust/scripts/run-realtikv-scan-pushdown.sh`.
/// * [`GO_PUSHES_NOT_HERE_YET`] -- the families it does not, each for a stated
///   reason. Those keep Go's verdict in the `#[ignore]`d
///   [`tikv_pushes_what_go_pushes`] and are pinned as still-refused by
///   [`every_not_yet_pushable_expression_is_still_refused_here`], so a
///   widening that starts pushing one of them fails a test rather than
///   drifting silently.
///
/// The two directions of a disagreement with Go are not equally serious:
///
/// * **Correctness.** For the rows Go *refuses*, a push here would be a bug:
///   the coprocessor would evaluate an expression TiDB deliberately keeps in
///   the TiDB layer (`INET_ATON` and friends; `CONV` over a BIT column, Go
///   issue 51877), and rows would silently differ. Those rows are asserted
///   unconditionally and are live coverage for any future widening.
/// * **Performance.** For the rows Go *pushes* and this engine does not,
///   refusing them costs network and CPU but cannot change an answer, because
///   the scan source applies every pushed conjunct itself regardless.
#[cfg(test)]
mod tests_push_down_verdict {
    use tidb_datatype::{FieldType, FieldTypeCode};

    use crate::driver::{split_scan_predicates, FromScope, FromTable, ScopeResolver};

    /// Go's `genColumn` set from the test, one column per type it builds an
    /// expression over.
    fn scope() -> FromScope {
        let column = |name: &str, code: FieldTypeCode| (name.to_owned(), FieldType::new(code));
        FromScope {
            tables: vec![FromTable {
                name: "t".to_owned(),
                database: None,
                columns: vec![
                    column("j", FieldTypeCode::Json),
                    column("i", FieldTypeCode::LongLong),
                    column("r", FieldTypeCode::Double),
                    column("dec", FieldTypeCode::NewDecimal),
                    column("s", FieldTypeCode::String),
                    column("dt", FieldTypeCode::Datetime),
                    // Go's `binaryStringColumn`, whose very next line is
                    // `RetType.SetCollate(charset.CollationBin)` -- the only
                    // thing that distinguishes it from `s`, and the whole
                    // selector of the string family's binary spelling.
                    (
                        "bs".to_owned(),
                        FieldType::new(FieldTypeCode::String).with_collation_name("binary"),
                    ),
                    column("d", FieldTypeCode::Date),
                    column("bt", FieldTypeCode::Bit),
                    column("tm", FieldTypeCode::Duration),
                ],
                offset: 0,
                func_deps: Default::default(),
            }],
            ..FromScope::default()
        }
    }

    /// Whether this engine pushes the single conjunct of `where_expr` into the
    /// scan. `None` means the expression does not parse here at all, which is
    /// a different (and larger) gap than a refused push.
    fn pushes(where_expr: &str) -> Option<bool> {
        let sql = format!("SELECT 1 FROM t WHERE {where_expr}");
        let stmt = tidb_parser::parse(&sql).ok()?;
        let tidb_ast::Stmt::Query(query) = &stmt else {
            return None;
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            return None;
        };
        let where_clause = select.where_clause.clone()?;
        let scope = scope();
        let (pushed, _) = split_scan_predicates(
            &where_clause,
            &ScopeResolver { scope: &scope },
            &crate::StmtContext::default(),
        );
        Some(!pushed.is_empty())
    }

    /// The single builtin-call description `where_expr` pushes, when it pushes
    /// one, so a test can read the signature the catalog resolved.
    fn described_call(where_expr: &str) -> Option<tidb_expr::pushdown_catalog::PbScalar> {
        let sql = format!("SELECT 1 FROM t WHERE {where_expr}");
        let tidb_ast::Stmt::Query(query) = tidb_parser::parse(&sql).ok()? else {
            return None;
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            return None;
        };
        let where_clause = select.where_clause.clone()?;
        let scope = scope();
        let (pushed, _) = split_scan_predicates(
            &where_clause,
            &ScopeResolver { scope: &scope },
            &crate::StmtContext::default(),
        );
        match pushed.predicates() {
            [super::ScanPredicate::Builtin(call)] => Some(call.clone()),
            _ => None,
        }
    }

    /// Every expression Go REFUSES to push to TiKV, in Go's order.
    ///
    /// The IP family is TiDB-only; `CONV` over a BIT column is refused
    /// because the BIT-to-binary-string cast TiDB inserts is only handled in
    /// TiDB (Go issue 51877) -- note that `CONV` over a plain string column
    /// *is* pushed, and is in the pushed table below.
    const GO_REFUSES: &[&str] = &[
        "inet_aton(s)",
        "inet_ntoa(s)",
        "inet6_aton(s)",
        "inet6_ntoa(s)",
        "is_ipv4(s)",
        "is_ipv6(s)",
        "is_ipv4_compat(s)",
        "is_ipv4_mapped(s)",
        "conv(cast(bt as binary), i, i)",
    ];

    /// The rows of Go's pushed table this engine pushes too: the math family
    /// whose signatures `tidb_expr::pushdown_catalog` holds.
    ///
    /// All twelve resolve one of Go's `ETReal`/`ETInt`/`ETDecimal` signatures
    /// from the argument types alone, with no collation to derive and no
    /// metadata to encode, which is why this is the family the catalog could
    /// be completed for first.
    const GO_PUSHES_HERE_TOO: &[&str] = &[
        "sin(i)",
        "asin(i)",
        "cos(i)",
        "acos(i)",
        "atan(i)",
        "cot(i)",
        "atan2(i, i)",
        "pi()",
        "round(i)",
        "mod(i, i)",
        "pow(r, r)",
        "power(r, r)",
        // The string family. Each resolves its signature from
        // `types.IsBinaryStr(args[0])` and its result collation from that same
        // single argument (`deriveCollation`'s `ast.Upper`/`ast.Substr`
        // cases), or -- for `CONV` -- from the connection charset the family's
        // own `getFunction` sets. `s` is Go's non-binary `stringColumn`, so
        // every row below is the UTF-8 spelling; the binary spellings over Go's
        // `bs` are pinned by
        // [`the_binary_spelling_travels_for_a_binary_collation`].
        "conv(s, i, i)",
        "substr(s, i, i)",
        "substring(s, i, i)",
        "mid(s, i, i)",
        "char_length(s)",
        "upper(s)",
        "lower(s)",
    ];

    /// The rows of Go's pushed table this engine does not push yet, in Go's
    /// order.
    ///
    /// Go's table has five further rows commented out in the source
    /// (`TRUNCATE`, and four `STR_TO_DATE` spellings), so they pin no verdict
    /// and are deliberately absent here too.
    ///
    /// Each of these needs something neither the math nor the string family
    /// did. The DATE family is the largest block, and it is a genuinely
    /// separate seam rather than more of the same work: every one of its rows
    /// takes a `d`, `dt` or `tm` argument, and Go's `getFunction` for it calls
    /// `newBaseBuiltinFuncWithTp(..., types.ETDatetime)` or `ETDuration`, whose
    /// implicit wrapper is `WrapWithCastAsTime`/`WrapWithCastAsDuration`.
    /// Unlike `WrapWithCastAsReal`, that wrapper's target `FieldType` is not
    /// fixed -- it carries an FSP the wrapper computes from the SOURCE type
    /// (`builtin_cast.go`: `tp.SetDecimal(arg.GetType().GetDecimal())` and the
    /// `MaxDatetimeWidthWithFsp` width that follows from it), and a `MysqlTime`
    /// constant is encoded with `codec.EncodeMySQLTime` against the SESSION
    /// TIME ZONE, which this scan path does not put in the DAG request at all.
    /// Both are their own units; neither is unlocked by the collation seam the
    /// string family needed.
    ///
    /// The remainder:
    ///
    /// * `DATE_FORMAT` additionally takes a string format argument, so it needs
    ///   the temporal seam AND the string one;
    /// * the `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE` family additionally
    ///   sends the INTERVAL unit as a third string argument, and picks among
    ///   more than twenty signatures by unit *and* argument type;
    /// * the JSON family needs the `ETJson` TiPB field type and the implicit
    ///   `CAST(... AS JSON)` wrappers;
    /// * `FROM_UNIXTIME`, `UNIX_TIMESTAMP` and `TIMESTAMPDIFF` need the
    ///   session time zone in the DAG request, which this scan path does not
    ///   yet send.
    const GO_PUSHES_NOT_HERE_YET: &[&str] = &[
        // The `testcases` table, row for row.
        "date_format(d, s)",
        "hour(d)",
        "minute(d)",
        "second(d)",
        "month(d)",
        "microsecond(d)",
        "date(d)",
        "week(d)",
        "datediff(d, d)",
        "json_replace(j, s, j, s, j)",
        "json_array_append(j, s, j, s, j)",
        "json_merge_patch(j, j, j)",
        "date_add(s, interval s second)",
        "date_add(dec, interval r day)",
        "date_add(dt, interval i year)",
        "date_add(tm, interval s minute)",
        "date_add(tm, interval s year_month)",
        "date_sub(s, interval i microsecond)",
        "date_sub(i, interval r day)",
        "date_sub(dt, interval i quarter)",
        "date_sub(tm, interval s hour)",
        "date_sub(tm, interval s year_month)",
        "adddate(tm, interval s week)",
        "subdate(s, interval i hour)",
        "from_unixtime(dec)",
        "from_unixtime(dec, s)",
        "timestampdiff(second, dt, dt)",
        "timestampdiff(day, dt, dt)",
        "timestampdiff(year, dt, dt)",
        "unix_timestamp(dt)",
        "unix_timestamp(s)",
    ];

    /// CORRECTNESS: nothing Go keeps in TiDB may be handed to the store.
    #[test]
    fn tikv_refuses_what_go_refuses() {
        for expr in GO_REFUSES {
            // `None` (unparsable here) is also a refusal to push, which is the
            // safe direction; the missing builtin is a separate gap.
            if let Some(pushed) = pushes(expr) {
                assert!(
                    !pushed,
                    "{expr}: TiDB refuses to push this to TiKV, so pushing it here \
                     would let the store evaluate what only TiDB evaluates correctly"
                );
            }
        }
    }

    /// The predicate shapes this engine *does* push, over Go's own column set,
    /// so the blanket refusal below cannot be mistaken for pushdown being off.
    ///
    /// These are the rows Go's table does not contain; they are on the same
    /// TiKV whitelist (`infer_pushdown.go`'s `scalarExprSupportedByTiKV` lists
    /// every comparison operator, `LogicAnd`/`LogicOr`/`UnaryNot`, `In` and
    /// `IsNull` unconditionally), which is why widening to them was possible
    /// without a function catalog and why it moves none of `GO_PUSHES`.
    #[test]
    fn the_integer_predicate_shapes_push_over_gos_own_columns() {
        for expr in [
            "i > 5",
            "5 < i",
            "i = 1",
            "i <> 1",
            "i >= -7",
            "i IS NULL",
            "i IS NOT NULL",
            "i IN (1, 2, 3)",
            "i NOT IN (4)",
            "i = 1 OR i = 2",
            "i = 1 OR i IS NULL",
            "NOT i = 1",
            "NOT (i IN (1, 2))",
        ] {
            assert_eq!(pushes(expr), Some(true), "{expr} is a pushed predicate");
        }
        // And the shapes the *split* keeps above the scan: a second column
        // reference, and functions with their own NULL semantics and their own
        // signatures. Note what is deliberately not in this list -- `s = 'x'`
        // and `dec > 1` DO pass the split, because the split is type-agnostic
        // by design: it hands the source a description, and the coprocessor
        // lowering applies the type gate (`tidb_exec::wide_scan_selection`).
        // Refusing there costs wire volume only, because the source evaluates
        // every pushed conjunct itself regardless.
        for expr in ["i = r", "i IS TRUE", "i <=> 1", "i + 1 = 2"] {
            assert_eq!(pushes(expr), Some(false), "{expr} stays above the scan");
        }
    }

    /// PERFORMANCE, the part already reached: every row of Go's pushed table
    /// whose family the catalog holds pushes here, with Go's own verdict.
    ///
    /// This runs, unignored: these nineteen are a live claim, not a plan.
    #[test]
    fn tikv_pushes_the_math_family_go_pushes() {
        for expr in GO_PUSHES_HERE_TOO {
            assert_eq!(pushes(expr), Some(true), "{expr}: TiDB pushes this to TiKV");
        }
    }

    /// And the resolved signature is Go's own, not merely *some* signature: the
    /// description each of the twelve produces lowers to the `ScalarFuncSig`
    /// Go's `getFunction` sets, over Go's own column set.
    ///
    /// A push with the wrong signature is the one failure mode that returns
    /// wrong rows rather than slow ones, so it is pinned by name here rather
    /// than left to the shape assertion above.
    #[test]
    fn the_lowered_signature_is_the_one_gos_get_function_resolves() {
        use tidb_expr::pushdown_catalog::ScalarFuncSig;
        let cases: [(&str, ScalarFuncSig); 19] = [
            ("sin(i)", ScalarFuncSig::Sin),
            ("asin(i)", ScalarFuncSig::Asin),
            ("cos(i)", ScalarFuncSig::Cos),
            ("acos(i)", ScalarFuncSig::Acos),
            ("atan(i)", ScalarFuncSig::Atan1Arg),
            ("cot(i)", ScalarFuncSig::Cot),
            ("atan2(i, i)", ScalarFuncSig::Atan2Args),
            ("pi()", ScalarFuncSig::Pi),
            // The argument is a signed BIGINT column, so `ROUND` keeps the
            // integer domain and `MOD` takes the signed/signed signature.
            ("round(i)", ScalarFuncSig::RoundInt),
            ("mod(i, i)", ScalarFuncSig::ModIntSignedSigned),
            ("pow(r, r)", ScalarFuncSig::Pow),
            ("power(r, r)", ScalarFuncSig::Pow),
            // `s` is Go's non-binary `stringColumn`, so each string family
            // takes its UTF-8 spelling -- the answer that differs from the
            // binary one by case-folding rules and by counting characters
            // rather than bytes.
            ("conv(s, i, i)", ScalarFuncSig::Conv),
            ("substr(s, i, i)", ScalarFuncSig::Substring3ArgsUtf8),
            ("substring(s, i, i)", ScalarFuncSig::Substring3ArgsUtf8),
            ("mid(s, i, i)", ScalarFuncSig::Substring3ArgsUtf8),
            ("char_length(s)", ScalarFuncSig::CharLengthUtf8),
            ("upper(s)", ScalarFuncSig::UpperUtf8),
            ("lower(s)", ScalarFuncSig::LowerUtf8),
        ];
        for (expr, expected) in cases {
            let described = described_call(expr)
                .unwrap_or_else(|| panic!("{expr} is described as a builtin call"));
            let tidb_expr::pushdown_catalog::PbScalar::Call { signature, .. } = &described else {
                panic!("{expr} describes a call");
            };
            assert_eq!(signature.sig, expected, "{expr}");
        }
    }

    /// CORRECTNESS: the binary spelling travels for a binary-collation column
    /// and the UTF-8 one for every other collation -- over Go's OWN two string
    /// columns, which differ in nothing but `SetCollate(charset.CollationBin)`.
    ///
    /// This is the trap the string widening creates and the only reason it can
    /// be trusted: `UpperUTF8` sent against binary bytes case-folds them as
    /// UTF-8, and `CharLengthUTF8` counts characters where `CharLength` counts
    /// bytes. Both return a WRONG answer rather than a slow one, and no local
    /// pass afterwards can detect it, so the choice is pinned by signature.
    #[test]
    fn the_binary_spelling_travels_for_a_binary_collation() {
        use tidb_expr::pushdown_catalog::ScalarFuncSig;
        let cases: [(&str, &str, ScalarFuncSig, ScalarFuncSig); 6] = [
            (
                "char_length({})",
                "char_length",
                ScalarFuncSig::CharLengthUtf8,
                ScalarFuncSig::CharLength,
            ),
            (
                "upper({})",
                "upper",
                ScalarFuncSig::UpperUtf8,
                ScalarFuncSig::Upper,
            ),
            (
                "lower({})",
                "lower",
                ScalarFuncSig::LowerUtf8,
                ScalarFuncSig::Lower,
            ),
            (
                "substr({}, i, i)",
                "substr/3",
                ScalarFuncSig::Substring3ArgsUtf8,
                ScalarFuncSig::Substring3Args,
            ),
            (
                "substring({}, i)",
                "substring/2",
                ScalarFuncSig::Substring2ArgsUtf8,
                ScalarFuncSig::Substring2Args,
            ),
            (
                "mid({}, i, i)",
                "mid/3",
                ScalarFuncSig::Substring3ArgsUtf8,
                ScalarFuncSig::Substring3Args,
            ),
        ];
        let resolved = |expr: &str| {
            let described =
                described_call(expr).unwrap_or_else(|| panic!("{expr} describes a call"));
            let tidb_expr::pushdown_catalog::PbScalar::Call { signature, .. } = described else {
                panic!("{expr} describes a call");
            };
            signature.sig
        };
        for (template, label, utf8, binary) in cases {
            assert_eq!(
                resolved(&template.replace("{}", "s")),
                utf8,
                "{label} over Go's non-binary stringColumn"
            );
            assert_eq!(
                resolved(&template.replace("{}", "bs")),
                binary,
                "{label} over Go's binaryStringColumn"
            );
        }
    }

    /// CORRECTNESS: `CONV` alone is collation-blind, so both of Go's string
    /// columns resolve the single `Conv` signature -- a second spelling here
    /// would be an invention.
    #[test]
    fn conv_resolves_one_signature_for_either_string_column() {
        use tidb_expr::pushdown_catalog::ScalarFuncSig;
        for column in ["s", "bs"] {
            let described = described_call(&format!("conv({column}, i, i)")).unwrap();
            let tidb_expr::pushdown_catalog::PbScalar::Call { signature, .. } = described else {
                panic!("conv({column}, i, i) describes a call");
            };
            assert_eq!(signature.sig, ScalarFuncSig::Conv);
        }
    }

    /// The string slot admits only an argument that is ALREADY `ETString`.
    /// Go would insert `WrapWithCastAsString`; this tier does not build that
    /// cast, so the whole conjunct stays above the scan -- a refusal, which
    /// costs network and never an answer.
    #[test]
    fn a_string_family_over_a_non_string_column_stays_above_the_scan() {
        for expr in [
            "char_length(i)",
            "upper(i)",
            "lower(r)",
            "substr(dec, i, i)",
            "conv(i, i, i)",
            // `j` is JSON and `bt` is BIT: Go casts both into the string slot,
            // and `bt` is exactly the shape `scalarExprSupportedByTiKV`'s
            // `ast.Conv` case refuses outright (Go issue 51877).
            "upper(j)",
            "char_length(bt)",
        ] {
            assert_eq!(
                pushes(expr),
                Some(false),
                "{expr}: the implicit CAST into the string slot is not built here"
            );
        }
    }

    /// PERFORMANCE, the part not reached: Go's verdict on the families the
    /// catalog does not hold, kept as the assertion it must eventually become.
    #[test]
    #[ignore = "the date, INTERVAL and JSON families need temporal cast targets with a source-derived FSP, the session time zone in the DAG request, INTERVAL metadata and the ETJson field type -- see GO_PUSHES_NOT_HERE_YET"]
    fn tikv_pushes_what_go_pushes() {
        for expr in GO_PUSHES_NOT_HERE_YET {
            assert_eq!(pushes(expr), Some(true), "{expr}: TiDB pushes this to TiKV");
        }
    }

    /// The gap the `#[ignore]`d test above would otherwise hide: TODAY every
    /// row of the not-yet half is refused. Pinning that keeps the count honest
    /// -- if a widening starts pushing some of them, this test fails and the
    /// ignored one must be re-checked, along with the live differential.
    #[test]
    fn every_not_yet_pushable_expression_is_still_refused_here() {
        let pushed_here: Vec<&&str> = GO_PUSHES_NOT_HERE_YET
            .iter()
            .filter(|expr| pushes(expr) == Some(true))
            .collect();
        assert!(
            pushed_here.is_empty(),
            "these now push -- re-check them against Go's verdict: {pushed_here:?}"
        );
    }

    /// The two halves of Go's pushed table are the whole of it, with no row
    /// counted twice or lost while moving one across.
    ///
    /// Fifty is the row count of Go's pushed table as ported: the four
    /// `CONV`/substring rows plus the forty-six live rows of `testcases`,
    /// with Go's five commented-out rows excluded because they pin no verdict.
    #[test]
    fn the_two_halves_reconstruct_gos_pushed_table() {
        assert_eq!(GO_PUSHES_HERE_TOO.len() + GO_PUSHES_NOT_HERE_YET.len(), 50);
        for expr in GO_PUSHES_HERE_TOO {
            assert!(
                !GO_PUSHES_NOT_HERE_YET.contains(expr),
                "{expr} is in both halves"
            );
        }
    }

    /// Every expression in Go's table parses here, in all three halves, so a
    /// regression that lost one of these spellings from the grammar would fail
    /// here rather than silently shrinking a table above.
    #[test]
    fn every_expression_in_gos_table_parses() {
        let unparsable: Vec<&&str> = GO_REFUSES
            .iter()
            .chain(GO_PUSHES_HERE_TOO)
            .chain(GO_PUSHES_NOT_HERE_YET)
            .filter(|expr| pushes(expr).is_none())
            .collect();
        assert!(
            unparsable.is_empty(),
            "these rows of Go's push-down table do not parse here: {unparsable:?}"
        );
    }
}
