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

//! The SPILL tests: an over-quota `GROUP BY` must answer exactly what the same
//! aggregation answers with room to spare.
//!
//! The oracle is a DUAL RUN (the pattern the join's spill tests use): the same
//! rows through the same operator twice, once under a quota it fits inside and
//! once under a quota it cannot, compared group by group. Nothing here trusts
//! a hand-written expectation about what a spilled aggregation "should" say.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, HashAggExec};
use crate::mem_quota::{OomAction, StatementMemory};
use crate::test_temp_storage::{scratch_dir as scratch_temp_dir, storage as test_storage};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::NoColumns;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn col(index: i64) -> Expression {
    let mut c = Column::new(index + 1, long());
    c.index = index;
    Expression::Column(c)
}

fn schema_of(n: usize) -> Schema {
    Schema::new(
        (0..n)
            .map(|i| {
                let mut c = Column::new(i as i64 + 1, long());
                c.index = i as i64;
                c
            })
            .collect(),
    )
}

/// The aggregation's OUTPUT schema: `SUM` over an integer column is a
/// DECIMAL in MySQL, so its cell has to be one.
fn out_schema() -> Schema {
    let types = [
        long(),
        long(),
        FieldType::new(FieldTypeCode::NewDecimal),
        long(),
        long(),
        FieldType::new(FieldTypeCode::VarString),
    ];
    Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let mut c = Column::new(i as i64 + 1, t.clone());
                c.index = i as i64;
                c
            })
            .collect(),
    )
}

/// A source that emits its rows `batch` at a time, so the aggregation sees
/// SEVERAL child chunks -- which is what lets a spill fire part way in and
/// leave the rest of the input still to come.
struct ManyChunkSource {
    meta: ExecutorMeta,
    rows: Vec<(i64, i64)>,
    cursor: usize,
    batch: usize,
}

impl Executor for ManyChunkSource {
    fn open(&mut self) -> Result<(), ExecError> {
        self.cursor = 0;
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let end = (self.cursor + self.batch).min(self.rows.len());
        for (g, v) in &self.rows[self.cursor..end] {
            req.append_int64(0, *g);
            req.append_int64(1, *v);
        }
        self.cursor = end;
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
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

/// `SELECT g, COUNT(v), SUM(v), MIN(v), MAX(v), GROUP_CONCAT(v) FROM t
/// GROUP BY g`.
///
/// GROUP_CONCAT is here to make ROW ORDER WITHIN A GROUP observable: the other
/// five are order-insensitive, so they cannot tell a spill that preserved a
/// group's row order from one that reversed it. Go's rounds do preserve it --
/// a group is either OPEN this round (and every one of its rows folds in) or
/// deferred whole, so no group is ever fed its rows out of order.
fn grouped(rows: &[(i64, i64)], batch: usize, memory: StatementMemory) -> HashAggExec<NoColumns> {
    let source = ManyChunkSource {
        meta: ExecutorMeta::new(schema_of(2), 0, batch, batch),
        rows: rows.to_vec(),
        cursor: 0,
        batch,
    };
    HashAggExec::new(
        ExecutorMeta::new(out_schema(), 1, 32, 1024),
        vec![col(0)],
        vec![
            AggFunc::new(AggKind::FirstRow, Some(col(0))),
            AggFunc::new(AggKind::Count, Some(col(1))),
            AggFunc::new(AggKind::Sum, Some(col(1))),
            AggFunc::new(AggKind::Min, Some(col(1))),
            AggFunc::new(AggKind::Max, Some(col(1))),
            AggFunc::new(
                AggKind::GroupConcat {
                    separator: ",".to_owned(),
                },
                Some(col(1)),
            ),
        ],
        Box::new(source),
        NoColumns,
        memory,
    )
}

/// Drains the aggregation into `group -> the four aggregate values`.
/// A map, not a list: the ROUND structure reorders the groups of a spilled
/// aggregation, and neither engine promises an order for `GROUP BY`.
fn drain(exec: &mut HashAggExec<NoColumns>) -> BTreeMap<i64, Vec<Datum>> {
    exec.open().unwrap();
    let mut out = BTreeMap::new();
    let mut req = exec.new_chunk();
    loop {
        exec.next(&mut req).expect("the aggregation must not fail");
        if req.num_rows() == 0 {
            return out;
        }
        for r in 0..req.num_rows() {
            let row = req.get_row(r);
            let previous = out.insert(row.get_int64(0), agg_values(&row));
            assert!(
                previous.is_none(),
                "group {} was emitted TWICE -- a round leaked a group",
                row.get_int64(0)
            );
        }
    }
}

fn spill_files_in(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.contains("ChunkDataInDiskByChunks"))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The five aggregate cells of one output row, as datums: COUNT, SUM
/// (decimal), MIN, MAX, GROUP_CONCAT.
fn agg_values(row: &tidb_chunk::row::Row<'_>) -> Vec<Datum> {
    let types = [
        long(),
        FieldType::new(FieldTypeCode::NewDecimal),
        long(),
        long(),
        FieldType::new(FieldTypeCode::VarString),
    ];
    (1..6).map(|c| row.get_datum(c, &types[c - 1])).collect()
}

/// The number of distinct groups the spill tests aggregate over. It is large
/// enough that the group table cannot fit the quota below, and the round it
/// forces has to happen several times over.
const GROUPS: i64 = 2000;

/// `GROUPS` groups of 3 rows each, in TWO layouts, because the two mistakes a
/// spill can make need different inputs to show up:
///
/// * The first half is INTERLEAVED -- pass after pass over every group -- so a
///   group's rows are spread across many chunks and many rounds. That is what
///   catches a round that loses, duplicates or re-opens a group.
/// * The second half is BLOCKED -- a group's three rows adjacent -- so ONE
///   chunk holds several rows of the SAME group. That is what makes the order
///   in which deferred rows are written to disk observable at all: with the
///   interleaved layout alone, no two rows in a chunk share a group, and a
///   spill that reversed them would change nothing.
fn interleaved_rows() -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    let split = GROUPS / 2;
    for pass in 0..3i64 {
        for g in 0..split {
            rows.push((g, g * 10 + pass));
        }
    }
    for g in split..GROUPS {
        for pass in 0..3i64 {
            rows.push((g, g * 10 + pass));
        }
    }
    rows
}

/// A quota the group table cannot fit inside. It has to leave headroom
/// between the SOFT limit that starts a spill (80%) and the HARD limit that
/// cancels: the groups a round has already opened keep growing after the
/// spill starts, because their remaining rows still fold in.
///
/// The bar is set by how many groups a round must hold: with a group table
/// entry of ~1.3KB (six aggregate states) and `GROUPS` groups, a quota this
/// size gives a round a few hundred groups, so the aggregation finishes in a
/// handful of rounds -- well inside Go's `maxSpillTimes` of 10, past which the
/// action stops spilling and the statement fails at the hard limit instead.
fn tight_quota() -> i64 {
    1 << 20
}

/// Source: `pkg/executor/aggregate/agg_spill_test.go::TestGetCorrectResult`.
///
/// The Go test runs the same grouped aggregation before and after forcing the
/// parallel executor to spill. This serial implementation preserves the same
/// observable oracle: every aggregate cell must equal the unspilled run, the
/// spill must really reach disk, and close must remove every spill file.
#[test]
fn test_get_correct_result() {
    let dir = scratch_temp_dir("hashagg");

    let rows = interleaved_rows();

    // The reference: a quota this aggregation fits inside.
    let mut reference = grouped(&rows, 64, StatementMemory::default());
    let expected = drain(&mut reference);
    assert_eq!(expected.len(), GROUPS as usize);
    assert_eq!(reference.spill_times(), 0, "the reference must not spill");
    assert_eq!(reference.bytes_in_disk(), 0);
    reference.close().unwrap();

    // The same aggregation under a quota it cannot hold its groups within.
    let memory = StatementMemory::new(tight_quota(), OomAction::Cancel, 42)
        .with_spill_storage(test_storage(&dir));
    let mut exec = grouped(&rows, 64, memory);
    exec.open().unwrap();
    let mut got = BTreeMap::new();
    let mut saw_spill_file = false;
    let mut req = exec.new_chunk();
    loop {
        exec.next(&mut req)
            .expect("a spilling aggregation must not fail");
        if req.num_rows() == 0 {
            break;
        }
        saw_spill_file |= !spill_files_in(&dir).is_empty();
        for r in 0..req.num_rows() {
            let row = req.get_row(r);
            let previous = got.insert(row.get_int64(0), agg_values(&row));
            assert!(
                previous.is_none(),
                "group {} came out TWICE: a round re-opened a group another \
                 round had already completed, so its aggregate is a partial",
                row.get_int64(0)
            );
        }
    }

    assert!(
        exec.spill_times() > 0,
        "spill never triggered -- this test proved nothing"
    );
    assert!(
        saw_spill_file,
        "no spill file existed while the aggregation ran"
    );
    assert!(
        exec.bytes_in_disk() > 0,
        "the disk tracker must have counted the deferred rows"
    );
    assert_eq!(got, expected, "a spilled GROUP BY changed its answer");
    exec.close().unwrap();
    assert!(
        spill_files_in(&dir).is_empty(),
        "close must remove every spill file"
    );
    drop(exec);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Source: `pkg/executor/aggregate/agg_spill_test.go::TestFallBackAction`.
///
/// Go observes that an aggregation unable to spill reaches the previously
/// installed root exceed action. Rust's root fallback is the typed CANCEL
/// action: disabling temporary storage under the same quota must therefore
/// reach 8175, rather than swallowing the overrun, and must create no file.
#[test]
fn test_fall_back_action() {
    let dir = scratch_temp_dir("hashagggate");

    let memory = StatementMemory::new(tight_quota(), OomAction::Cancel, 42)
        .with_spill_storage(test_storage(&dir))
        .with_tmp_storage_on_oom(false);
    let mut exec = grouped(&interleaved_rows(), 64, memory);
    exec.open().unwrap();
    let mut req = exec.new_chunk();
    match exec.next(&mut req) {
        Err(ExecError::MemoryExceedForQuery { conn_id }) => assert_eq!(conn_id, 42),
        other => panic!("expected 8175 with tmp storage disabled, got {other:?}"),
    }
    assert!(spill_files_in(&dir).is_empty(), "no file may be written");
    drop(exec);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Source: `pkg/executor/aggregate/agg_spill_test.go::TestRandomFail`.
///
/// Go injects a disk failure while racing `Close` against its parallel
/// workers and asserts the executor never hangs. Rust owns the spill loop and
/// `Close` through one mutable executor, so the type system removes that data
/// race; the equivalent failure boundary is a real write attempt below a
/// regular file. It must return promptly, close successfully, and release the
/// complete statement charge.
#[test]
fn test_random_fail() {
    let dir = scratch_temp_dir("hashaggfail");
    let storage_path = dir.join("storage");
    let displaced_storage_path = dir.join("leased-storage");
    let storage = test_storage(&storage_path);

    // Keep the immutable authority alive while replacing only its pathname.
    // Its next create_file call therefore reaches a real ENOTDIR boundary,
    // without restoring the deleted process-global temp-directory switch.
    std::fs::rename(&storage_path, &displaced_storage_path).unwrap();
    std::fs::write(&storage_path, b"block spill file creation").unwrap();

    let memory =
        StatementMemory::new(tight_quota(), OomAction::Cancel, 42).with_spill_storage(storage);
    let mut exec = grouped(&interleaved_rows(), 64, memory.clone());
    exec.open().unwrap();
    let mut req = exec.new_chunk();
    let started = Instant::now();
    let error = exec.next(&mut req).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a failed spill must not strand the executor"
    );
    match error {
        ExecError::SpillFailed(message) => assert!(!message.is_empty()),
        other => panic!("expected a spill failure, got {other:?}"),
    }
    exec.close().unwrap();
    assert_eq!(
        memory.bytes_consumed(),
        0,
        "failure followed by close must release every tracked byte"
    );

    drop(exec);
    drop(memory);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A spilled aggregation must release the round's memory: without
/// `resetSpillMode`'s `ReplaceBytesUsed`, round two would start already
/// over the quota and the statement would die at 8175 anyway.
#[test]
fn each_round_gives_the_statements_budget_back() {
    let dir = scratch_temp_dir("hashaggbudget");

    let memory = StatementMemory::new(tight_quota(), OomAction::Cancel, 42)
        .with_spill_storage(test_storage(&dir));
    let mut exec = grouped(&interleaved_rows(), 64, memory.clone());
    let got = drain(&mut exec);
    assert_eq!(got.len(), GROUPS as usize);
    assert!(exec.spill_times() > 1, "this test needs several rounds");
    exec.close().unwrap();
    assert_eq!(
        memory.bytes_consumed(),
        0,
        "close must return every byte to the statement"
    );
    drop(exec);
    drop(memory);
    let _ = std::fs::remove_dir_all(&dir);
}
