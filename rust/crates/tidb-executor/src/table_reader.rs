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

//! `pkg/executor/table_reader.go`: `TableReaderExecutor` -- the operator that
//! turns a table's ranges into a coprocessor read and hands the rows up.
//!
//! # What is here and what is deliberately not
//!
//! Most of this Go file is request construction: `buildKVReq` (:579),
//! `buildKVReqSeparately` (:504), `buildKVReqForPartitionTableScan` (:540),
//! `buildRespForGroupedRanges` (:401) and the `selectResultHook` (:65) all
//! build a `kv.Request` from a `tipb.DAGRequest` and dispatch it through
//! `distsql.Select`. That is the coprocessor round-trip, which this tier
//! reaches through [`crate::remote_scan`] instead, and it is NOT restated
//! here.
//!
//! What *is* here is everything the Go file computes on its own:
//!
//! * [`TableResultHandler`] -- Go `tableResultHandler` (:651), the two-half
//!   result sequencing that exists because an UNSIGNED clustered handle stores
//!   `(MaxInt64, MaxUint64]` physically *before* `[0, MaxInt64]`. This is the
//!   part of a table read whose behavior is pure ordering logic, and it is
//!   where the whole unsigned-primary-key ordering correctness lives.
//! * [`build_virtual_column_index`] / [`build_virtual_column_info`] -- Go
//!   :620 / :640, the definition-order sort that makes a virtual column
//!   computable in terms of earlier ones.
//! * [`sort_and_get_kv_ranges_from_reqs`] -- Go `sortAndGetKVRangesFromReqs`
//!   (:490).
//! * [`TableReaderExec`] -- Go's `Open`/`Next`/`Close` (:232/:356/:383)
//!   *shape*: the `dummy` short-circuit, the two-half open, the virtual-column
//!   fill after every chunk, and `Close` clearing `kvRanges` before deciding
//!   whether the handler's error matters.
//!
//! # The unsigned-handle split, stated once
//!
//! `Open` calls `distsql.SplitRangesAcrossInt64Boundary(ranges, keepOrder,
//! desc, isCommonHandle)` and gets back a *signed* half and an *unsigned*
//! half. Which one is read first is the whole point:
//!
//! * ascending: signed first, then unsigned -- `optionalResult` = signed;
//! * descending: the split function itself returns them the other way round,
//!   so `optionalResult` is whichever half must be read first;
//! * no split needed: `optionalResult` is nil and `optionalFinished` starts
//!   `true`.
//!
//! [`TableResultHandler`] reproduces the consumption rule exactly, including
//! the subtlety in `nextChunk` (:675) that a first half is only finished once
//! it returns an EMPTY chunk -- one empty chunk from the first half does not
//! end the read, it switches halves and pulls again.
//!
//! # boundary: `distsql.SplitRangesAcrossInt64Boundary`
//!
//! The split itself lives in `pkg/distsql/request_builder.go`, outside this
//! Go package. [`TableReaderExec`] therefore takes the two halves already
//! separated, as `Open` has them by the time `buildRespForGroupedRanges` runs,
//! and the `dummy` branch's descending SWAP of the two halves (:309) is
//! expressed as [`TableReaderExec::with_dummy_kv_ranges`] taking them in the
//! order `UnionScan` needs.
//!
//! # Narrowings, all named
//!
//! * `memUsage` (:217), `memTracker`, `indexUsageReporter`
//!   (`ReportCopIndexUsageForHandle`), `netDataSize`, `paging`, `batchCop`
//!   and the runtime-stats collection flag have no counterpart in this tier.
//! * `corColInFilter`/`corColInAccess` DAG rebuilding (:247, :268) requires
//!   `ResolveCorrelatedColumns` and `ConstructListBasedDistExec`; not ported.
//! * `nextRaw` (:689) exists only for `checksum`/`analyze` callers that read
//!   the raw coprocessor bytes; it has no chunk-level meaning here. The
//!   handler's `next_chunk` is ported, `nextRaw` is not.
//! * `groupedRanges`/`groupByColIdxs` merge-sort access is request building
//!   (`buildKVReqSeparatelyForGroupedRanges` :470) and is folded into the
//!   caller-supplied halves.

use std::collections::BTreeMap;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;
use tidb_txnkv::KeyRange;

use crate::executor::{ExecError, Executor, ExecutorMeta};

/// Go `tableResultHandler` (:651): sequences an OPTIONAL first result and a
/// mandatory second one.
///
/// The two results are the two sides of the int64 boundary for an unsigned
/// clustered handle. `optional_finished` starting `true` is the ordinary
/// single-result case.
pub struct TableResultHandler {
    /// Go `optionalResult`.
    optional_result: Option<Box<dyn Executor>>,
    /// Go `result`.
    result: Option<Box<dyn Executor>>,
    /// Go `optionalFinished`.
    optional_finished: bool,
}

impl Default for TableResultHandler {
    fn default() -> Self {
        TableResultHandler {
            optional_result: None,
            result: None,
            optional_finished: true,
        }
    }
}

impl TableResultHandler {
    /// Go `tableResultHandler.open` (:664): a nil `optionalResult` marks the
    /// optional half finished before it is ever read.
    pub fn open(&mut self, optional_result: Option<Box<dyn Executor>>, result: Box<dyn Executor>) {
        self.optional_finished = optional_result.is_none();
        self.optional_result = optional_result;
        self.result = Some(result);
    }

    /// Go `tableResultHandler.nextChunk` (:675).
    ///
    /// The optional half is finished only by an EMPTY chunk, and the same call
    /// then pulls from the second half -- so a caller never observes the
    /// switch as a spurious end of results.
    pub fn next_chunk(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        if !self.optional_finished {
            if let Some(optional) = self.optional_result.as_mut() {
                optional.next(chk)?;
                if chk.num_rows() > 0 {
                    return Ok(());
                }
            }
            self.optional_finished = true;
        }
        match self.result.as_mut() {
            Some(result) => result.next(chk),
            None => {
                chk.reset();
                Ok(())
            }
        }
    }

    /// Go `tableResultHandler.Close` (:707): `closeAll` on both, then both
    /// fields are dropped. `closeAll` closes EVERY non-nil result and returns
    /// the first error, so a failure on the first half does not leak the
    /// second.
    pub fn close(&mut self) -> Result<(), ExecError> {
        let mut first_error = None;
        for result in [self.optional_result.as_mut(), self.result.as_mut()]
            .into_iter()
            .flatten()
        {
            if let Err(err) = result.close() {
                first_error.get_or_insert(err);
            }
        }
        self.optional_result = None;
        self.result = None;
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// One output-schema column, reduced to what
/// [`build_virtual_column_index`] reads off it.
#[derive(Clone, Copy, Debug)]
pub struct SchemaColumnRef {
    /// Go `expression.Column.ID`.
    pub id: i64,
    /// Go `expression.Column.VirtualExpr != nil`.
    pub is_virtual: bool,
}

/// Go `buildVirtualColumnIndex` (:620): the schema offsets of the virtual
/// columns, sorted by their DEFINITION order in the table.
///
/// The sort is the load-bearing part. A virtual column may reference an
/// earlier virtual column, and evaluating them in definition order is what
/// guarantees the referenced value is already filled in. Schema order is the
/// query's projection order and carries no such guarantee.
///
/// `definition_offsets` is Go's `model.FindColumnInfoByID(columns, id).Offset`
/// lookup. A schema column absent from it sorts last; Go would nil-panic
/// there, which only a malformed plan can produce.
#[must_use]
pub fn build_virtual_column_index(
    schema: &[SchemaColumnRef],
    definition_offsets: &BTreeMap<i64, usize>,
) -> Vec<usize> {
    let mut index: Vec<usize> = schema
        .iter()
        .enumerate()
        .filter(|(_, column)| column.is_virtual)
        .map(|(offset, _)| offset)
        .collect();
    index.sort_by_key(|offset| {
        definition_offsets
            .get(&schema[*offset].id)
            .copied()
            .unwrap_or(usize::MAX)
    });
    index
}

/// Go `buildVirtualColumnInfo` (:640): the indexes plus the matching return
/// types, which Go leaves nil when there are no virtual columns at all.
#[must_use]
pub fn build_virtual_column_info(
    schema: &[SchemaColumnRef],
    ret_types: &[FieldType],
    definition_offsets: &BTreeMap<i64, usize>,
) -> (Vec<usize>, Vec<FieldType>) {
    let index = build_virtual_column_index(schema, definition_offsets);
    if index.is_empty() {
        return (index, Vec::new());
    }
    let types = index
        .iter()
        .map(|offset| ret_types[*offset].clone())
        .collect();
    (index, types)
}

/// Go `sortAndGetKVRangesFromReqs` (:490): each request's ranges are sorted by
/// start key, concatenated, then the whole list is sorted again.
///
/// The double sort is not redundant in Go -- the first mutates each request's
/// own `KeyRanges` (which the request then dispatches in that order), the
/// second orders the flattened view `UnionScan` reads.
#[must_use]
pub fn sort_and_get_kv_ranges_from_reqs(requests: &mut [Vec<KeyRange>]) -> Vec<KeyRange> {
    let mut flattened = Vec::new();
    for request in requests.iter_mut() {
        request.sort_by(|a, b| a.start_key.as_bytes().cmp(b.start_key.as_bytes()));
        flattened.extend(request.iter().cloned());
    }
    flattened.sort_by(|a, b| a.start_key.as_bytes().cmp(b.start_key.as_bytes()));
    flattened
}

/// The two results `Open` (:337) hands [`TableResultHandler::open`]: the
/// optional first half (present only when the ranges straddled the int64
/// boundary) and the mandatory second one.
type ResultHalves = (Option<Box<dyn Executor>>, Box<dyn Executor>);

/// Go `TableReaderExecutor` (:139).
pub struct TableReaderExec {
    meta: ExecutorMeta,
    /// Go `resultHandler`.
    result_handler: TableResultHandler,
    /// The two halves `Open` (:337) hands the handler, kept until `open` runs
    /// because Go builds its results inside `Open` too.
    halves: Option<ResultHalves>,
    /// Go `dummy`: a temporary or cached table, where this operator exists
    /// only to publish `kvRanges` for the `UnionScan` above it and must send
    /// no request at all.
    dummy: bool,
    /// Go `kvRanges`, in the order `UnionScan` consumes them.
    kv_ranges: Vec<KeyRange>,
    /// Go `virtualColumnIndex`.
    virtual_column_index: Vec<usize>,
    /// Go `virtualColumnRetFieldTypes`.
    virtual_column_ret_field_types: Vec<FieldType>,
}

impl TableReaderExec {
    /// Builds the reader over the two range halves `Open` (:293) produced.
    ///
    /// `optional` is Go's `firstResult` when a second half exists -- i.e. the
    /// half that must be READ FIRST, which for a descending unsigned scan is
    /// the one above the int64 boundary.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        optional: Option<Box<dyn Executor>>,
        result: Box<dyn Executor>,
    ) -> Self {
        TableReaderExec {
            meta,
            result_handler: TableResultHandler::default(),
            halves: Some((optional, result)),
            dummy: false,
            kv_ranges: Vec::new(),
            virtual_column_index: Vec::new(),
            virtual_column_ret_field_types: Vec::new(),
        }
    }

    /// Go `setDummy` (:213) together with the `dummy` branch of `Open` (:305),
    /// which computes `kvRanges` and returns without building any result.
    ///
    /// The caller passes the ranges already in `UnionScan` order, which is the
    /// descending SWAP Go performs at :309: TiKV reverse-scans, `UnionScan`
    /// does not, so a descending read must hand `UnionScan` the halves the
    /// other way round and let it reverse the rows itself.
    #[must_use]
    pub fn with_dummy_kv_ranges(mut self, kv_ranges: Vec<KeyRange>) -> Self {
        self.dummy = true;
        self.kv_ranges = kv_ranges;
        self
    }

    /// Go `buildVirtualColumnInfo` (:635) applied to this reader.
    #[must_use]
    pub fn with_virtual_columns(mut self, index: Vec<usize>, ret_types: Vec<FieldType>) -> Self {
        self.virtual_column_index = index;
        self.virtual_column_ret_field_types = ret_types;
        self
    }

    /// Go `kvRanges`, which `UnionScan` reads after `Open`.
    #[must_use]
    pub fn kv_ranges(&self) -> &[KeyRange] {
        &self.kv_ranges
    }

    /// Go `table.FillVirtualColumnValue` (called from `Next` :371).
    ///
    /// boundary: `pkg/table/column.go` `FillVirtualColumnValue`. Filling
    /// requires evaluating each virtual column's `GeneratedExpr` against the
    /// chunk row, which this tier does through
    /// [`crate::generated_column::materialize`] over a `Vec<Datum>` row rather
    /// than in place over a chunk column. Wiring that here would restate the
    /// materialization; until a chunk-level evaluator exists, a reader with
    /// virtual columns refuses instead of returning unfilled NULLs.
    fn fill_virtual_column_value(&self) -> Result<(), ExecError> {
        if self.virtual_column_index.is_empty() {
            return Ok(());
        }
        Err(ExecError::unsupported(
            "table reader cannot fill virtual columns yet (Go: table.FillVirtualColumnValue)",
        ))
    }
}

impl Executor for TableReaderExec {
    /// Go `Open` (:232), reduced to the handler wiring: a dummy reader opens
    /// no result at all, and the handler decides the two-half sequencing.
    fn open(&mut self) -> Result<(), ExecError> {
        if self.dummy {
            return Ok(());
        }
        let Some((optional, mut result)) = self.halves.take() else {
            return Err(ExecError::internal("table reader was opened twice"));
        };
        let optional = match optional {
            Some(mut optional) => {
                optional.open()?;
                Some(optional)
            }
            None => None,
        };
        result.open()?;
        self.result_handler.open(optional, result);
        Ok(())
    }

    /// Go `Next` (:356).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.dummy {
            // Go: "Treat temporary table as dummy table, avoid sending
            // distsql request to TiKV." The rows come from the `UnionScan`
            // above, which reads `kvRanges` directly.
            req.reset();
            return Ok(());
        }
        self.result_handler.next_chunk(req)?;
        self.fill_virtual_column_value()?;
        Ok(())
    }

    /// Go `Close` (:383): `kvRanges` is truncated unconditionally, and a dummy
    /// reader discards the handler's error because it never opened one.
    fn close(&mut self) -> Result<(), ExecError> {
        let result = self.result_handler.close();
        self.kv_ranges.clear();
        if self.dummy {
            return Ok(());
        }
        result
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem_table::MemTableSourceExec;
    use tidb_datatype::{Datum, FieldTypeCode};
    use tidb_expr::column::Column;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn int_schema() -> Schema {
        let mut column = Column::new(1, long());
        column.index = 0;
        Schema::new(vec![column])
    }

    fn meta() -> ExecutorMeta {
        ExecutorMeta::new(int_schema(), 1, 32, 1024)
    }

    fn source(values: &[i64]) -> Box<dyn Executor> {
        Box::new(MemTableSourceExec::new(
            meta(),
            values.iter().map(|v| vec![Datum::Int(*v)]).collect(),
        ))
    }

    fn drain(exec: &mut dyn Executor) -> Vec<i64> {
        let mut out = Vec::new();
        loop {
            let mut chunk = exec.new_chunk();
            exec.next(&mut chunk).expect("next");
            if chunk.num_rows() == 0 {
                return out;
            }
            for row in 0..chunk.num_rows() {
                match chunk.get_row(row).get_datum(0, &long()) {
                    Datum::Int(value) => out.push(value),
                    other => panic!("expected an int column, got {other:?}"),
                }
            }
        }
    }

    /// WRITTEN test (Go covers this through `testkit` unsigned-PK queries):
    /// the handler reads the whole optional half before the mandatory one.
    #[test]
    fn the_optional_half_is_fully_drained_before_the_second() {
        let mut exec = TableReaderExec::new(meta(), Some(source(&[9, 10])), source(&[1, 2]));
        exec.open().expect("open");
        assert_eq!(drain(&mut exec), vec![9, 10, 1, 2]);
    }

    /// The single-result case: `optionalFinished` starts true.
    #[test]
    fn a_reader_with_no_optional_half_reads_only_the_second() {
        let mut exec = TableReaderExec::new(meta(), None, source(&[1, 2, 3]));
        exec.open().expect("open");
        assert_eq!(drain(&mut exec), vec![1, 2, 3]);
    }

    /// An EMPTY optional half must not be mistaken for end of results: Go
    /// switches halves inside the same `nextChunk` call.
    #[test]
    fn an_empty_optional_half_does_not_end_the_read() {
        let mut exec = TableReaderExec::new(meta(), Some(source(&[])), source(&[4, 5]));
        exec.open().expect("open");
        assert_eq!(drain(&mut exec), vec![4, 5]);
    }

    /// Go's `dummy` reader sends no request and returns no rows, but keeps its
    /// `kvRanges` available for the `UnionScan` above it until `Close`.
    #[test]
    fn a_dummy_reader_returns_no_rows_but_publishes_its_ranges() {
        let ranges = vec![KeyRange::new(vec![1u8].into(), vec![2u8].into())];
        let mut exec =
            TableReaderExec::new(meta(), None, source(&[1, 2])).with_dummy_kv_ranges(ranges);
        exec.open().expect("open");
        assert_eq!(exec.kv_ranges().len(), 1);
        assert_eq!(drain(&mut exec), Vec::<i64>::new());
        exec.close().expect("close");
        assert!(exec.kv_ranges().is_empty());
    }

    /// Go sorts the virtual columns into table-definition order, not schema
    /// order, so a virtual column can depend on an earlier one.
    #[test]
    fn virtual_columns_sort_into_definition_order() {
        let schema = [
            SchemaColumnRef {
                id: 7,
                is_virtual: true,
            },
            SchemaColumnRef {
                id: 3,
                is_virtual: false,
            },
            SchemaColumnRef {
                id: 5,
                is_virtual: true,
            },
        ];
        // Column 5 is defined before column 7 in the table.
        let offsets = BTreeMap::from([(3i64, 0usize), (5, 1), (7, 2)]);
        assert_eq!(build_virtual_column_index(&schema, &offsets), vec![2, 0]);
    }

    #[test]
    fn a_schema_with_no_virtual_columns_yields_no_ret_types() {
        let schema = [SchemaColumnRef {
            id: 1,
            is_virtual: false,
        }];
        let types = [long()];
        let offsets = BTreeMap::from([(1i64, 0usize)]);
        let (index, ret_types) = build_virtual_column_info(&schema, &types, &offsets);
        assert!(index.is_empty());
        assert!(ret_types.is_empty());
    }

    #[test]
    fn virtual_column_ret_types_follow_the_sorted_index() {
        let schema = [
            SchemaColumnRef {
                id: 7,
                is_virtual: true,
            },
            SchemaColumnRef {
                id: 5,
                is_virtual: true,
            },
        ];
        let types = [FieldType::new(FieldTypeCode::Varchar), long()];
        let offsets = BTreeMap::from([(5i64, 0usize), (7, 1)]);
        let (index, ret_types) = build_virtual_column_info(&schema, &types, &offsets);
        assert_eq!(index, vec![1, 0]);
        assert_eq!(ret_types[0].code(), FieldTypeCode::Long);
        assert_eq!(ret_types[1].code(), FieldTypeCode::Varchar);
    }

    /// A reader that must fill virtual columns refuses rather than emitting
    /// unfilled NULLs; see the boundary note on `fill_virtual_column_value`.
    #[test]
    fn a_reader_with_virtual_columns_refuses() {
        let mut exec = TableReaderExec::new(meta(), None, source(&[1]))
            .with_virtual_columns(vec![0], vec![long()]);
        exec.open().expect("open");
        let mut chunk = exec.new_chunk();
        assert!(matches!(
            exec.next(&mut chunk),
            Err(ExecError::Unsupported(_))
        ));
    }

    #[test]
    fn kv_ranges_are_sorted_within_and_across_requests() {
        let range = |start: u8, end: u8| KeyRange::new(vec![start].into(), vec![end].into());
        let mut requests = vec![vec![range(5, 6), range(1, 2)], vec![range(3, 4)]];
        let sorted = sort_and_get_kv_ranges_from_reqs(&mut requests);
        // Each request is sorted in place ...
        assert_eq!(requests[0][0].start_key.as_bytes(), &[1u8]);
        // ... and the flattened view is sorted across requests.
        assert_eq!(
            sorted
                .iter()
                .map(|r| r.start_key.as_bytes()[0])
                .collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
    }
}
