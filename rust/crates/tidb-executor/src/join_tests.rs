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

use super::*;
use tidb_ast::CiString;
use tidb_datatype::FieldTypeCode;
use tidb_expr::column::Column;
use tidb_expr::scalar_function::ScalarFunction;
pub(super) use tidb_expr::NoColumns;

const CHUNK: usize = 1024;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

fn schema_of(width: usize) -> Schema {
    Schema::new(
        (0..width)
            .map(|i| {
                let mut column = Column::new(i as i64 + 1, long());
                column.index = i as i64;
                column
            })
            .collect(),
    )
}

/// A source that hands out prebuilt rows in `max_chunk_size` batches, so
/// the probe side really is pulled incrementally rather than in one go.
struct RowSource {
    meta: ExecutorMeta,
    rows: Vec<Vec<Datum>>,
    cursor: usize,
}

impl RowSource {
    fn new(rows: Vec<Vec<Datum>>, width: usize) -> Self {
        RowSource {
            meta: ExecutorMeta::new(schema_of(width), 0, CHUNK, CHUNK),
            rows,
            cursor: 0,
        }
    }
}

impl Executor for RowSource {
    fn open(&mut self) -> Result<(), ExecError> {
        self.cursor = 0;
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let end = (self.cursor + CHUNK).min(self.rows.len());
        for row in &self.rows[self.cursor..end] {
            for (c, value) in row.iter().enumerate() {
                req.append_datum(c, value);
            }
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

/// `left.<lhs> = right.<rhs>`, addressed against the joined schema.
pub(super) fn eq_on(lhs: usize, rhs: usize, left_width: usize) -> Expression {
    let column = |index: usize| {
        let mut column = Column::new(index as i64 + 1, long());
        column.index = index as i64;
        Expression::Column(column)
    };
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        long(),
        vec![column(lhs), column(left_width + rhs)],
    ))
}

pub(super) fn join_of(
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
    width: usize,
) -> JoinExec<NoColumns> {
    join_with_memory(
        kind,
        conditions,
        left,
        right,
        width,
        StatementMemory::default(),
    )
}

pub(super) fn join_with_memory(
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
    width: usize,
    memory: StatementMemory,
) -> JoinExec<NoColumns> {
    let output_width = if matches!(kind, JoinKind::Semi | JoinKind::AntiSemi) {
        width
    } else {
        2 * width
    };
    JoinExec::new(
        ExecutorMeta::new(schema_of(output_width), 1, CHUNK, CHUNK),
        kind,
        conditions,
        Box::new(RowSource::new(left, width)),
        Box::new(RowSource::new(right, width)),
        NoColumns,
        memory,
    )
}

/// Drains a join to completion, exactly as a caller does: repeated
/// `next()` until an empty chunk.
pub(super) fn run(join: &mut JoinExec<NoColumns>) -> Vec<Vec<i64>> {
    join.open().unwrap();
    let types = join.ret_field_types().to_vec();
    let mut out = Vec::new();
    let mut req = join.new_chunk();
    loop {
        join.next(&mut req).unwrap();
        if req.num_rows() == 0 {
            break;
        }
        for r in 0..req.num_rows() {
            let row = req.get_row(r);
            out.push(
                (0..types.len())
                    .map(|c| match row.get_datum(c, &types[c]) {
                        Datum::Int(value) => value,
                        // NULL padding, distinguishable from any test
                        // value because every fixture value is >= 0.
                        Datum::Null => -1,
                        other => panic!("unexpected datum {other:?}"),
                    })
                    .collect(),
            );
        }
    }
    join.close().unwrap();
    out
}

/// Left rows: key `i % 7` (so keys repeat and both sides fan out), value
/// `i`, with every 11th key NULL. Right rows: key `i % 5`, so some keys
/// match nothing on either side.
fn fixture(n: i64, modulus: i64) -> Vec<Vec<Datum>> {
    (0..n)
        .map(|i| {
            let key = if i % 11 == 10 {
                Datum::Null
            } else {
                Datum::Int(i % modulus)
            };
            vec![key, Datum::Int(i)]
        })
        .collect()
}

/// The hash path must reproduce the nested loop ROW FOR ROW -- same
/// rows, same order -- for every join kind, over data with duplicate
/// keys, unmatched keys on both sides, and NULL keys on both sides.
///
/// The NULL rows are the point of the fixture: a NULL key matches
/// nothing (not even another NULL), so an inner join must drop those
/// rows and an outer join must still emit them NULL-padded. Getting that
/// wrong is exactly the failure a bucket-based key can introduce.
#[test]
fn hash_path_matches_the_nested_loop_row_for_row() {
    for kind in [
        JoinKind::Inner,
        JoinKind::Left,
        JoinKind::Right,
        JoinKind::Semi,
        JoinKind::AntiSemi,
    ] {
        let left = fixture(200, 7);
        let right = fixture(200, 5);
        let mut hashed = join_of(kind, vec![eq_on(0, 0, 2)], left.clone(), right.clone(), 2);
        assert!(hashed.is_hash_join());
        let mut looped = join_of(kind, vec![eq_on(0, 0, 2)], left, right, 2);
        looped.force_nested_loop();
        assert_eq!(run(&mut hashed), run(&mut looped), "{kind:?}");
    }
}

/// The same, with a non-equi conjunct riding along: the hash table
/// selects candidates on the equal condition, and the residue still has
/// to reject the pairs it rejects.
#[test]
fn residual_conditions_still_filter_hashed_candidates() {
    let left = fixture(150, 7);
    let right = fixture(150, 5);
    // `l.key = r.key AND l.value = r.value` -- the second conjunct is
    // also an equal condition, so both become keys; the composite key is
    // what must not let one column borrow the other's bytes.
    let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];
    let mut hashed = join_of(
        JoinKind::Left,
        conditions.clone(),
        left.clone(),
        right.clone(),
        2,
    );
    let mut looped = join_of(JoinKind::Left, conditions, left, right, 2);
    looped.force_nested_loop();
    assert_eq!(run(&mut hashed), run(&mut looped));
}

/// A join with no equal condition keeps the nested loop, as documented.
#[test]
fn cross_join_falls_back_to_the_nested_loop() {
    let mut join = join_of(JoinKind::Inner, Vec::new(), fixture(4, 7), fixture(4, 5), 2);
    assert!(!join.is_hash_join());
    assert_eq!(run(&mut join).len(), 16);
}

#[test]
fn cartesian_join_follows_the_costed_build_side_and_bucket_order() {
    let left = vec![
        vec![Datum::Int(1), Datum::Int(1)],
        vec![Datum::Int(2), Datum::Int(2)],
    ];
    let right = vec![
        vec![Datum::Int(7), Datum::Int(7)],
        vec![Datum::Int(8), Datum::Int(8)],
    ];
    let mut join = join_of(JoinKind::Inner, Vec::new(), left, right, 2);
    assert_eq!(
        run(&mut join),
        vec![
            vec![1, 1, 8, 8],
            vec![1, 1, 7, 7],
            vec![2, 2, 8, 8],
            vec![2, 2, 7, 7],
        ],
    );

    let left = vec![vec![Datum::Int(1), Datum::Int(1)]];
    let right = vec![
        vec![Datum::Int(7), Datum::Int(7)],
        vec![Datum::Int(8), Datum::Int(8)],
    ];
    let mut join = join_of(JoinKind::Inner, Vec::new(), left, right, 2);
    join.set_cartesian_build_side(true);
    assert_eq!(run(&mut join), vec![vec![1, 1, 7, 7], vec![1, 1, 8, 8]]);
}

/// The scaling claim, asserted on the cost the hash table exists to
/// remove rather than on the wall clock.
///
/// 10k x 10k over 10k distinct keys: the nested loop would evaluate the
/// `ON` clause 100_000_000 times. The hash join evaluates it once per
/// candidate pair a bucket produces -- here exactly once per matching
/// row, because the keys are distinct.
#[test]
fn ten_thousand_by_ten_thousand_is_linear_not_quadratic() {
    let rows = 10_000i64;
    let side: Vec<Vec<Datum>> = (0..rows)
        .map(|i| vec![Datum::Int(i), Datum::Int(i * 2)])
        .collect();
    let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], side.clone(), side, 2);
    assert!(join.is_hash_join());
    let out = run(&mut join);
    assert_eq!(out.len(), rows as usize);
    // Every output row is the key joined to itself.
    assert_eq!(out[0], vec![0, 0, 0, 0]);
    assert_eq!(out[9_999], vec![9_999, 19_998, 9_999, 19_998]);

    let evals = join.condition_evals();
    let nested_loop_evals = (rows * rows) as u64;
    assert_eq!(evals, rows as u64, "one candidate pair per probe row");
    // Stated as a ratio so the assertion says what it means: at least
    // four orders of magnitude fewer, not a tuned constant.
    assert!(
        evals * 10_000 <= nested_loop_evals,
        "{evals} evaluations vs the nested loop's {nested_loop_evals}"
    );
}
