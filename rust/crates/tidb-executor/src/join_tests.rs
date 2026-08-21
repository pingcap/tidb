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
use tidb_expr::constant::Constant;
use tidb_expr::scalar_function::ScalarFunction;
pub(super) use tidb_expr::NoColumns;

const CHUNK: usize = 1024;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

fn decimal(text: &str) -> Decimal {
    let (value, error) = Decimal::parse_mysql(text);
    assert_eq!(error, None, "invalid decimal fixture {text}");
    value
}

#[test]
fn decimal_residual_multiply_preserves_mysql_hidden_scale() {
    let average = Decimal::from_int(100)
        .div_mysql(&Decimal::from_int(4), 4)
        .expect("nonzero divisor");
    let factor = decimal("0.2");

    // `div_mysql` retains whole base-1e9 fraction words behind the visible
    // scale. The fast predicate must still use the bounded SQL multiplication
    // API and compare the same value as the expression evaluator.
    assert!(decimal_mul_lt_mysql(&decimal("4.99999"), &factor, &average,).unwrap());
    assert!(!decimal_mul_lt_mysql(&decimal("5.00000"), &factor, &average,).unwrap());
}

fn schema_of(width: usize) -> Schema {
    schema_with_types(&vec![long(); width])
}

fn schema_with_types(types: &[FieldType]) -> Schema {
    Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(i, field_type)| {
                let mut column = Column::new(i as i64 + 1, field_type.clone());
                column.index = i as i64;
                column
            })
            .collect(),
    )
}

#[test]
fn grouped_index_lookup_count_and_max_match_go_null_semantics() {
    let aggregation = IndexLookupAggregation {
        group_offsets: vec![0],
        input_offsets: vec![1],
        outputs: vec![
            IndexLookupAggregateOutput::Column(0),
            IndexLookupAggregateOutput::Count(None),
            IndexLookupAggregateOutput::Count(Some(1)),
            IndexLookupAggregateOutput::Max {
                offset: 1,
                collation: Collation::Binary,
            },
        ],
        pruned_row_count: false,
    };
    let rows = vec![
        vec![Datum::Int(1), Datum::Null],
        vec![Datum::Int(1), Datum::Int(3)],
        vec![Datum::Int(1), Datum::Int(2)],
        vec![Datum::Int(2), Datum::Null],
    ];
    assert_eq!(
        aggregation.apply(rows.clone(), false).unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(3), Datum::Int(2), Datum::Int(3)],
            vec![Datum::Int(2), Datum::Int(1), Datum::Int(0), Datum::Null],
        ]
    );
    assert_eq!(
        aggregation.apply(rows, true).unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(3), Datum::Int(2), Datum::Int(3)],
            vec![Datum::Int(2), Datum::Int(1), Datum::Int(0), Datum::Null],
        ]
    );
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
        Self::with_types(rows, &vec![long(); width])
    }

    fn with_types(rows: Vec<Vec<Datum>>, types: &[FieldType]) -> Self {
        RowSource {
            meta: ExecutorMeta::new(schema_with_types(types), 0, CHUNK, CHUNK),
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

fn join_with_types(
    conditions: Vec<Expression>,
    left: Vec<Vec<Datum>>,
    left_types: &[FieldType],
    right: Vec<Vec<Datum>>,
    right_types: &[FieldType],
) -> JoinExec<NoColumns> {
    let output_types = left_types
        .iter()
        .chain(right_types)
        .cloned()
        .collect::<Vec<_>>();
    JoinExec::new(
        ExecutorMeta::new(schema_with_types(&output_types), 1, CHUNK, CHUNK),
        JoinKind::Inner,
        conditions,
        Box::new(RowSource::with_types(left, left_types)),
        Box::new(RowSource::with_types(right, right_types)),
        NoColumns,
        StatementMemory::default(),
    )
}

fn run_datums(join: &mut JoinExec<NoColumns>) -> Vec<Vec<Datum>> {
    join.open().unwrap();
    let types = join.ret_field_types().to_vec();
    let mut out = Vec::new();
    let mut req = join.new_chunk();
    loop {
        join.next(&mut req).unwrap();
        if req.num_rows() == 0 {
            break;
        }
        for row in 0..req.num_rows() {
            out.push(req.get_row(row).get_datum_row(&types));
        }
    }
    join.close().unwrap();
    out
}

#[test]
fn decimal_residual_fast_path_matches_general_join_evaluator() {
    let decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    let column = |index: usize, field_type: FieldType| {
        let mut column = Column::new(index as i64 + 1, field_type);
        column.index = index as i64;
        Expression::Column(column)
    };
    let residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("lt"),
        long(),
        vec![
            column(1, decimal_type.clone()),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("mul"),
                decimal_type.clone(),
                vec![
                    Expression::Constant(Constant::new(
                        Datum::Decimal(decimal("0.2")),
                        decimal_type.clone(),
                    )),
                    column(3, decimal_type.clone()),
                ],
            )),
        ],
    ));
    let conditions = vec![eq_on(0, 0, 2), residual];
    let left = vec![
        vec![Datum::Int(1), Datum::Decimal(decimal("3.00"))],
        vec![Datum::Int(1), Datum::Decimal(decimal("12.00"))],
    ];
    let right = vec![vec![Datum::Int(1), Datum::Decimal(decimal("25.00"))]];
    let types = [long(), decimal_type];

    let mut fast = join_with_types(
        conditions.clone(),
        left.clone(),
        &types,
        right.clone(),
        &types,
    );
    assert!(fast.residual_decimal_mul_lt.is_some());
    let fast_rows = run_datums(&mut fast);

    let mut general = join_with_types(conditions, left, &types, right, &types);
    general.residual_decimal_mul_lt = None;
    let general_rows = run_datums(&mut general);

    assert_eq!(fast_rows, general_rows);
    assert_eq!(
        fast_rows,
        vec![vec![
            Datum::Int(1),
            Datum::Decimal(decimal("3.00")),
            Datum::Int(1),
            Datum::Decimal(decimal("25.00")),
        ]]
    );
}

#[test]
fn decimal_residual_unique_integer_join_uses_parallel_probe_window() {
    let decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    let column = |index: usize, field_type: FieldType| {
        let mut column = Column::new(index as i64 + 1, field_type);
        column.index = index as i64;
        Expression::Column(column)
    };
    let residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("lt"),
        long(),
        vec![
            column(1, decimal_type.clone()),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("mul"),
                decimal_type.clone(),
                vec![
                    Expression::Constant(Constant::new(
                        Datum::Decimal(decimal("0.2")),
                        decimal_type.clone(),
                    )),
                    column(3, decimal_type.clone()),
                ],
            )),
        ],
    ));
    let left = (0..10_000)
        .map(|row| {
            vec![
                Datum::Int(1),
                Datum::Decimal(decimal(if row % 2 == 0 { "3.00" } else { "12.00" })),
            ]
        })
        .collect();
    let right = vec![vec![Datum::Int(1), Datum::Decimal(decimal("25.00"))]];
    let types = [long(), decimal_type];
    let mut join = join_with_types(vec![eq_on(0, 0, 2), residual], left, &types, right, &types);

    assert_eq!(run_datums(&mut join).len(), 5_000);
    assert!(
        join.parallel_probe_windows() > 0,
        "q17-shaped decimal residual joins must use the parallel probe path"
    );
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

#[test]
fn exact_integer_build_table_detects_duplicate_buckets() {
    let types = vec![long(), long()];
    let rows = fixture(200, 5);
    let mut chunk = Chunk::new_with_capacity(&types, rows.len());
    for row in &rows {
        for (column, datum) in row.iter().enumerate() {
            chunk.append_datum(column, datum);
        }
    }
    let key = EquiKey {
        left: 0,
        right: 0,
        class: KeyClass::Int,
        null_safe: false,
    };
    let mut table = BuildTable::new(
        &types,
        CHUNK,
        StatementMemory::default().spill_storage(),
        false,
        true,
    );
    table
        .index_chunk(chunk, &[key], &types, false)
        .expect("build table");

    assert_eq!(table.probe_exact_int(0).len(), 36);
    assert!(!table.exact_int_is_unique());
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
        let hashed_rows = run(&mut hashed);
        assert!(
            !hashed.parallel_exact_int_enabled(),
            "duplicate build keys must not be classified as unique for {kind:?}"
        );
        assert_eq!(
            hashed.parallel_probe_windows(),
            0,
            "duplicate build keys must keep the bounded unique-key path disabled for {kind:?}"
        );
        assert_eq!(hashed_rows, run(&mut looped), "{kind:?}");
    }
}

#[test]
fn duplicate_integer_left_join_matches_loop_in_isolation() {
    let left = fixture(200, 7);
    let right = fixture(200, 5);
    let mut hashed = join_of(
        JoinKind::Left,
        vec![eq_on(0, 0, 2)],
        left.clone(),
        right.clone(),
        2,
    );
    let mut looped = join_of(JoinKind::Left, vec![eq_on(0, 0, 2)], left, right, 2);
    looped.force_nested_loop();
    assert_eq!(run(&mut hashed), run(&mut looped));
}

/// Go hash join v2 may build the preserved side of an outer join. Matches are
/// emitted while the non-preserved side probes, then unmatched build rows are
/// emitted by scanning the row table. Both orientations must still produce
/// the same SQL result as the nested-loop reference.
#[test]
fn outer_hash_join_can_build_the_preserved_side() {
    let cases = [
        (
            JoinKind::Left,
            true,
            vec![
                vec![1, 10, 1, 100],
                vec![1, 11, 1, 100],
                vec![1, 10, 1, 101],
                vec![1, 11, 1, 101],
                vec![2, 20, -1, -1],
                vec![-1, 30, -1, -1],
            ],
        ),
        (
            JoinKind::Right,
            false,
            vec![
                vec![1, 10, 1, 100],
                vec![1, 10, 1, 101],
                vec![1, 11, 1, 100],
                vec![1, 11, 1, 101],
                vec![-1, -1, 3, 300],
                vec![-1, -1, -1, 400],
            ],
        ),
    ];

    for (kind, build_is_left, expected) in cases {
        let left = vec![
            vec![Datum::Int(1), Datum::Int(10)],
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Null, Datum::Int(30)],
            vec![Datum::Int(1), Datum::Int(11)],
        ];
        let right = vec![
            vec![Datum::Int(1), Datum::Int(100)],
            vec![Datum::Int(3), Datum::Int(300)],
            vec![Datum::Int(1), Datum::Int(101)],
            vec![Datum::Null, Datum::Int(400)],
        ];
        let conditions = vec![eq_on(0, 0, 2)];
        let mut hashed = join_of(kind, conditions.clone(), left.clone(), right.clone(), 2);
        hashed.set_hash_build_is_left(build_is_left);
        let actual = run(&mut hashed);
        assert_eq!(actual, expected, "{kind:?} build_is_left={build_is_left}");

        let mut looped = join_of(kind, conditions, left, right, 2);
        looped.force_nested_loop();
        let mut actual_set = actual;
        let mut reference_set = run(&mut looped);
        actual_set.sort();
        reference_set.sort();
        assert_eq!(
            actual_set, reference_set,
            "{kind:?} build_is_left={build_is_left}"
        );
    }
}

/// Go hash join v2 may also build the preserved left side of a semi or
/// anti-semi join. The build rows are emitted only after the right probe has
/// marked them, once for semi and only when unmarked for anti-semi.
#[test]
fn semi_hash_join_can_build_the_preserved_left_side() {
    for kind in [JoinKind::Semi, JoinKind::AntiSemi] {
        let left = fixture(200, 7);
        let right = fixture(200, 5);
        let conditions = vec![eq_on(0, 0, 2)];
        let mut hashed = join_of(kind, conditions.clone(), left.clone(), right.clone(), 2);
        hashed.set_hash_build_is_left(true);
        assert!(hashed.hash_build_is_left());
        let mut actual = run(&mut hashed);

        let mut looped = join_of(kind, conditions, left, right, 2);
        looped.force_nested_loop();
        let mut expected = run(&mut looped);
        actual.sort();
        expected.sort();
        assert_eq!(
            actual, expected,
            "{kind:?} with the preserved left side built"
        );
    }
}

#[test]
fn preserved_build_row_is_matched_only_after_every_on_condition_passes() {
    for (kind, build_is_left) in [(JoinKind::Left, true), (JoinKind::Right, false)] {
        let left = vec![vec![Datum::Int(1), Datum::Int(10)]];
        let right = vec![vec![Datum::Int(1), Datum::Int(20)]];
        let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];

        let mut hashed = join_of(kind, conditions.clone(), left.clone(), right.clone(), 2);
        hashed.set_hash_build_is_left(build_is_left);
        let actual = run(&mut hashed);

        let mut looped = join_of(kind, conditions, left, right, 2);
        looped.force_nested_loop();
        assert_eq!(actual, run(&mut looped), "{kind:?}");
        assert_eq!(
            actual,
            if kind == JoinKind::Left {
                vec![vec![1, 10, -1, -1]]
            } else {
                vec![vec![-1, -1, 1, 20]]
            },
            "the equal hash key alone must not mark a preserved row"
        );
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
    assert_eq!(
        evals, 0,
        "pure equal conditions are enforced by the hash key"
    );
    // Stated as a ratio so the assertion says what it means: at least
    // four orders of magnitude fewer, not a tuned constant.
    assert!(
        evals * 10_000 <= nested_loop_evals,
        "{evals} evaluations vs the nested loop's {nested_loop_evals}"
    );
}

/// Go HashJoin hands probe chunks to five workers. A large pure integer
/// equality join must take the corresponding bounded parallel path instead of
/// running every probe chunk on the session thread.
#[test]
fn exact_integer_hash_join_uses_parallel_probe_window() {
    let rows = 10_000i64;
    let side: Vec<Vec<Datum>> = (0..rows)
        .map(|i| vec![Datum::Int(i), Datum::Int(i * 2)])
        .collect();
    let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], side.clone(), side, 2);

    assert_eq!(run(&mut join).len(), rows as usize);
    assert!(
        join.parallel_probe_windows() > 0,
        "large exact-integer probes must use the parallel worker path"
    );
}

/// TPC-H q13 builds the preserved customer side and probes orders. Parallel
/// workers must report matches back to the session thread so the post-probe
/// scan emits only truly unmatched build rows.
#[test]
fn parallel_exact_integer_probe_marks_preserved_build_rows() {
    let left = (0..6_000i64)
        .map(|key| vec![Datum::Int(key), Datum::Int(key * 10)])
        .collect::<Vec<_>>();
    let right = (0..7_000i64)
        .map(|value| vec![Datum::Int(value % 5_000), Datum::Int(value)])
        .collect::<Vec<_>>();
    let mut join = join_of(JoinKind::Left, vec![eq_on(0, 0, 2)], left, right, 2);
    join.set_hash_build_is_left(true);

    let mut actual = run(&mut join);
    assert!(join.parallel_probe_windows() > 0);
    let mut expected = (0..7_000i64)
        .map(|value| {
            let key = value % 5_000;
            vec![key, key * 10, key, value]
        })
        .chain((5_000..6_000i64).map(|key| vec![key, key * 10, -1, -1]))
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}
