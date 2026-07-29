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
//! steps at once, with the narrow accepted shape the bounded TiKV lowering
//! already speaks: **a comparison between one column of the scanned table and
//! one constant**, in either operand order. Every other conjunct -- an
//! expression over a column (`b + 1 < 10`), a disjunction, `IS NULL`, a
//! subquery, anything referring to a second table -- is residual and is left
//! for the `Selection` above the scan.
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
//! That is why [`Executor::accept_scan_filter`] is opt-in and defaults to
//! refusing: a source may only return `true` when it applies every pushed
//! conjunct to *every* row it emits, merged rows included. A future
//! coprocessor-backed source that filters only the snapshot half must either
//! keep applying the predicate to the merged staged rows itself, or refuse --
//! in which case the driver leaves the whole `WHERE` in the `Selection` and
//! nothing changes.
//!
//! [`Executor::accept_scan_filter`]: crate::executor::Executor::accept_scan_filter

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
/// This is the hand-off shape for a coprocessor lowering: it names the scan
/// input offset, the operator, the constant, and which side the column was
/// written on, which is everything `PhysicalSelection.ToPB` needs and nothing
/// that ties the description to in-process evaluation.
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
    comparisons: Vec<ScanComparison>,
    filters: Vec<Expression>,
}

impl PushedScanFilter {
    /// Pairs each described comparison with the expression that evaluates it.
    ///
    /// # Panics
    /// If the two halves differ in length -- they describe the same conjuncts,
    /// so a mismatch is a construction bug rather than a runtime condition.
    #[must_use]
    pub fn new(comparisons: Vec<ScanComparison>, filters: Vec<Expression>) -> Self {
        assert_eq!(
            comparisons.len(),
            filters.len(),
            "every pushed conjunct has one description and one expression"
        );
        Self {
            comparisons,
            filters,
        }
    }

    /// The pushed conjuncts in `WHERE` order, for a coprocessor lowering.
    #[must_use]
    pub fn comparisons(&self) -> &[ScanComparison] {
        &self.comparisons
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
}

/// A one-row staging area an in-process source filters through.
///
/// A source holds its rows as `Datum`s but the evaluator reads chunk rows, so
/// each candidate row is appended here, tested, and only then copied into the
/// output chunk.
pub(crate) struct ScanFilterProbe {
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

        fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
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
            default_value: None,
            origin_default: None,
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
        let committed_low = table.insert_row(&[Datum::Int(1), Datum::Int(10)]).unwrap();
        table.insert_row(&[Datum::Int(9), Datum::Int(90)]).unwrap();
        let committed_moved = table.insert_row(&[Datum::Int(2), Datum::Int(20)]).unwrap();
        commit(&buffer, &snapshot);
        assert!(buffer.is_empty(), "nothing is staged after the commit");

        // Staged half, all inside one open transaction:
        //   * an INSERT that satisfies `a > 5`,
        //   * an INSERT that does not,
        //   * an UPDATE that lifts a committed row across the boundary,
        //   * a DELETE of a committed row that satisfies it.
        table.insert_row(&[Datum::Int(7), Datum::Int(70)]).unwrap();
        table.insert_row(&[Datum::Int(3), Datum::Int(30)]).unwrap();
        table
            .update_row(&committed_moved, &[Datum::Int(8), Datum::Int(80)])
            .unwrap();
        table.delete_row(&committed_low).unwrap();
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
    }

    /// The whole predicate must survive when nothing is pushed, and the split
    /// must not change any answer a `Selection` alone produced.
    #[test]
    fn splitting_a_where_does_not_change_its_result() {
        let mut table = KvTable::new(92, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [(1, 10), (5, 50), (7, 70), (9, 90)] {
            table.insert_row(&[Datum::Int(a), Datum::Int(b)]).unwrap();
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
            // Fully residual: a disjunction pushes nothing.
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
