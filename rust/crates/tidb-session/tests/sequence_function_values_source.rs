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


//! SQL nextval/lastval/setval value cases from Go `TestSequenceFunction`
//! (`pkg/ddl/sequence_test.go:95`), each `tk` a session over one mock store
//! through the Go testkit port.

use crate::support::testkit::TestKit;

/// Go `tk.MustExec("use test")` + `tk.MustExec(create)`: a kit holding a
/// freshly created `test.seq`.
fn seq_kit(create: &str) -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec(create);
    tk
}

/// Go's runs of `tk.MustQuery(sql).Check(testkit.Rows(want))`.
fn check_ladder(tk: &mut TestKit, ladder: &[(&str, &str)]) {
    for (sql, want) in ladder {
        tk.must_query(sql).check([*want]);
    }
}

/// Go `tc.GetSequenceCommon().GetSequenceBaseEndRound()` on
/// `external.GetTableByName(t, tk, "test", "seq")`: the table-local cache
/// triple, read without reserving another batch.
fn sequence_cache(tk: &TestKit) -> (i64, i64, i64) {
    tk.info_schema()
        .sequence_in("test", "seq")
        .expect("test.seq sequence exists")
        .allocator
        .base_end_round()
}

/// Go rows `pkg/ddl/sequence_test.go:100-108`: the four spellings -- unqualified
/// `nextval(seq)`, qualified `nextval(test.seq)`, `next value for seq`,
/// `next value for test.seq` -- share one counter and hand out 1, 2, 3, 4.
#[test]
fn nextval_spellings_share_one_counter() {
    let mut tk = seq_kit("create sequence seq");
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "1"),
            ("select nextval(test.seq)", "2"),
            ("select next value for seq", "3"),
            ("select next value for test.seq", "4"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:110-118`: `nextval(seq1)` over a missing
/// sequence is `[schema:1146]Table 'test.seq1' doesn't exist`; after
/// `use test2` the QUALIFIED name still resolves while the unqualified name
/// reports `'test2.seq'` (1146).
#[test]
fn nextval_resolves_unqualified_names_against_the_current_database() {
    let mut tk = seq_kit("create sequence seq");
    assert_eq!(
        tk.must_get_err_code("select nextval(seq1)", 1146).message,
        "Table 'test.seq1' doesn't exist"
    );

    tk.must_exec("create database test2");
    tk.must_exec("use test2");
    tk.must_query("select nextval(test.seq)").check(["1"]);
    tk.must_query("select next value for test.seq").check(["2"]);
    assert_eq!(
        tk.must_get_err_code("select nextval(seq)", 1146).message,
        "Table 'test2.seq' doesn't exist"
    );
    assert_eq!(
        tk.must_get_err_code("select next value for seq", 1146).message,
        "Table 'test2.seq' doesn't exist"
    );
    tk.must_exec("use test");
}

/// Go rows `pkg/ddl/sequence_test.go:117-138`: `NOCACHE` hands out 1, 2, 3
/// one at a time; `INCREMENT = 5` starts at 1 and steps 1, 6, 11;
/// `INCREMENT = 5 START = 3` gives 3, 8, 13.
#[test]
fn sequence_option_ladders_match_go() {
    let ladders: [(&str, [&str; 3]); 3] = [
        ("create sequence seq nocache", ["1", "2", "3"]),
        ("create sequence seq increment = 5", ["1", "6", "11"]),
        ("create sequence seq increment = 5 start = 3", ["3", "8", "13"]),
    ];
    for (create, expected) in ladders {
        let mut tk = seq_kit(create);
        for want in expected {
            tk.must_query("select nextval(seq)").check([want]);
        }
    }
}

/// Go row `pkg/ddl/sequence_test.go:140-145`: `minvalue -5 start = -2
/// increment = 5` (the "minvalue should be specified lower than start"
/// negative-start case) yields -2, 3, 8 -- the cache-batch size computation
/// `maxv - nr` overflows int64 there and Go PROCEEDS on the wrapped value
/// (`CalcSequenceBatchSize`, `pkg/meta/autoid/autoid.go:802`; Go's signed
/// overflow wraps silently).
#[test]
fn negative_minvalue_ladder_with_default_maxvalue_matches_go() {
    let mut tk = seq_kit("create sequence seq minvalue -5 start = -2 increment = 5");
    // Go's CalcSequenceBatchSize wraps signed overflow and keeps the ladder
    // alive.
    for want in ["-2", "3", "8"] {
        tk.must_query("select nextval(seq)").check([want]);
    }
}

/// Go rows `pkg/ddl/sequence_test.go:147-166`: `CYCLE` wraps to the
/// seek-offset bound -- `increment = 5 start = 3 maxvalue = 12` gives
/// 3, 8, 1, 6, 11, 1 and `increment = 4 start = 2 maxvalue = 10` gives
/// 2, 6, 10, 1, 5, 9, 1 (the wrapped-to value is the first value congruent
/// to START's offset, not MAXVALUE).
#[test]
fn cycle_wraps_to_the_offset_bound() {
    let ladders: [(&str, &[&str]); 2] = [
        (
            "create sequence seq increment = 5 start = 3 maxvalue = 12 cycle",
            &["3", "8", "1", "6", "11", "1"],
        ),
        (
            "create sequence seq increment = 4 start = 2 maxvalue = 10 cycle",
            &["2", "6", "10", "1", "5", "9", "1"],
        ),
    ];
    for (create, expected) in ladders {
        let mut tk = seq_kit(create);
        for want in expected {
            tk.must_query("select nextval(seq)").check([*want]);
        }
    }
}

/// Go rows `pkg/ddl/sequence_test.go:168-217`: a `NOCYCLE` sequence whose
/// next step would pass MAXVALUE (or fall below MINVALUE) reports
/// `[table:4135]Sequence 'test.seq' has run out`, for both growth directions.
#[test]
fn nocycle_sequences_run_out_with_4135() {
    let runouts: [&str; 4] = [
        "create sequence seq increment = 5 start = 3 maxvalue = 12 nocycle",
        "create sequence seq increment = 3 start = 3 maxvalue = 9 nocycle",
        "create sequence seq increment = -4 start = 6 minvalue -6 maxvalue = 11",
        "create sequence seq increment = -3 start = 2 minvalue -2 maxvalue 10",
    ];
    for create in runouts {
        let mut tk = seq_kit(create);
        // Drain until exhaustion; every step before it must succeed.
        loop {
            match tk.exec("select nextval(seq)") {
                Ok(_) => continue,
                Err(error) => {
                    assert_eq!(error.code, 4135, "create: {create}");
                    assert_eq!(
                        error.message, "Sequence 'test.seq' has run out",
                        "create: {create}"
                    );
                    break;
                }
            }
        }
    }
    // Spot-check the positive-growth ladder Go spells out (3, 8 then out).
    let mut tk = seq_kit("create sequence seq increment = 5 start = 3 maxvalue = 12 nocycle");
    tk.must_query("select nextval(seq)").check(["3"]);
    tk.must_query("select nextval(seq)").check(["8"]);
    tk.must_get_err_code("select nextval(seq)", 4135);
}

/// Go rows `pkg/ddl/sequence_test.go:184-201`: negative-growth CYCLE ladders
/// -- `increment = -2 start = 3 minvalue -5 maxvalue = 12` gives
/// 3, 1, -1, -3, -5, 12, 10 and `increment = -3 start = 2 minvalue -6
/// maxvalue = 11` gives 2, -1, -4, 11, 8.
#[test]
fn negative_growth_cycle_ladders_match_go() {
    let ladders: [(&str, &[&str]); 2] = [
        (
            "create sequence seq increment = -2 start = 3 minvalue -5 maxvalue = 12 cycle",
            &["3", "1", "-1", "-3", "-5", "12", "10"],
        ),
        (
            "create sequence seq increment = -3 start = 2 minvalue -6 maxvalue = 11 cycle",
            &["2", "-1", "-4", "11", "8"],
        ),
    ];
    for (create, expected) in ladders {
        let mut tk = seq_kit(create);
        for want in expected {
            tk.must_query("select nextval(seq)").check([*want]);
        }
    }
}

/// Go rows `pkg/ddl/sequence_test.go:221-230`: `setval` to an already-used
/// value reports NULL (a sequence never moves backwards); to an unused value
/// it reports the value itself, and the NEXT value is base+digits from the
/// setval, not from the old position (1, 2, setval 2 -> NULL, next 3,
/// setval 5 -> 5, next 6).
#[test]
fn setval_skips_used_values_and_rebases_the_ladder() {
    let mut tk = seq_kit("create sequence seq");
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "1"),
            ("select nextval(seq)", "2"),
            ("select setval(seq, 2)", "<nil>"),
            ("select nextval(seq)", "3"),
            ("select setval(seq, 5)", "5"),
            ("select nextval(seq)", "6"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:232-250`: SETVAL against a cached batch
/// -- inside the batch it moves the position (`setval 5` after 1, 4 reports 5
/// and the next value is 7), below the position it reports NULL, past the
/// batch end it rebases (`setval 8` -> 8, next 10), and once exhausted every
/// `nextval` is 4135 while `setval 11` (== maxvalue) and even
/// `setval 100` (beyond maxvalue) still report their argument without
/// reviving the sequence.
#[test]
fn setval_within_and_past_the_cached_batch() {
    let mut tk = seq_kit("create sequence seq increment 3 maxvalue 11");
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "1"),
            ("select nextval(seq)", "4"),
            ("select setval(seq, 3)", "<nil>"),
            ("select setval(seq, 4)", "<nil>"),
            ("select setval(seq, 5)", "5"),
            ("select nextval(seq)", "7"),
            ("select setval(seq, 8)", "8"),
            ("select nextval(seq)", "10"),
        ],
    );
    tk.must_get_err_code("select nextval(seq)", 4135);
    tk.must_query("select setval(seq, 11)").check(["11"]);
    tk.must_get_err_code("select nextval(seq)", 4135);
    // set value can be bigger than maxvalue.
    tk.must_query("select setval(seq, 100)").check(["100"]);
    tk.must_get_err_code("select nextval(seq)", 4135);
}

/// Go rows `pkg/ddl/sequence_test.go:252-278` minus the
/// `GetSequenceBaseEndRound` internals (asserted in the base/end/round test
/// below): `increment 10 start 5 maxvalue 100 cache 10 cycle` batches
/// 5..95, `setval(seq, 95)` exhausts the round, the next `nextval` wraps to
/// 1 (round 1), and a `setval(seq, 15)` inside the NEW batch is honored
/// (next 21, 31).
#[test]
fn setval_across_cycle_rounds_honors_the_new_batch() {
    let mut tk = seq_kit("create sequence seq increment 10 start 5 maxvalue 100 cache 10 cycle");
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "5"),
            ("select nextval(seq)", "15"),
            ("select setval(seq, 20)", "20"),
            ("select nextval(seq)", "25"),
            ("select setval(seq, 95)", "95"),
            ("select nextval(seq)", "1"),
            ("select setval(seq, 15)", "15"),
            ("select nextval(seq)", "21"),
            ("select nextval(seq)", "31"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:263,273,288,299,308,317,334,362,373,384,
/// 397,407,420` (the `GetSequenceBaseEndRound` assertions interleaved through
/// `TestSequenceFunction`): after each batch the table instance's
/// `(base, end, round)` cache triple is exactly Go's batch arithmetic --
/// e.g. `(5, 95, 0)` then `(1, 91, 1)` for
/// `increment 10 start 5 maxvalue 100 cache 10 cycle`, `(0, -6, 1)` for
/// `increment 2 start 0 maxvalue 10 minvalue -10 cache 3 cycle` after a
/// `setval(seq, 20)`, and `(-1, -10, 0)` / `(-10, 4, 1)` for the
/// negative-growth ladders.
#[test]
fn sequence_cache_base_end_round_bounds_match_go_batches() {
    let mut tk = seq_kit("create sequence seq increment 10 start 5 maxvalue 100 cache 10 cycle");
    tk.must_query("select nextval(seq)").check(["5"]);
    assert_eq!(sequence_cache(&tk), (5, 95, 0));
    tk.must_query("select setval(seq, 95)").check(["95"]);
    tk.must_query("select nextval(seq)").check(["1"]);
    assert_eq!(sequence_cache(&tk), (1, 91, 1));

    let mut tk =
        seq_kit("create sequence seq increment 2 start 0 maxvalue 10 minvalue -10 cache 3 cycle");
    tk.must_query("select setval(seq, -20)").check(["<nil>"]);
    tk.must_query("select setval(seq, 20)").check(["20"]);
    tk.must_query("select nextval(seq)").check(["-10"]);
    assert_eq!(sequence_cache(&tk), (-10, -6, 1));

    let mut tk =
        seq_kit("create sequence seq increment -3 start 5 maxvalue 10 minvalue -10 cache 3 cycle");
    tk.must_query("select nextval(seq)").check(["5"]);
    assert_eq!(sequence_cache(&tk), (5, -1, 0));
    tk.must_query("select setval(seq, -2)").check(["-2"]);
    tk.must_query("select nextval(seq)").check(["-4"]);
    assert_eq!(sequence_cache(&tk), (-4, -10, 0));
    tk.must_query("select setval(seq, -10)").check(["-10"]);
    tk.must_query("select nextval(seq)").check(["10"]);
    assert_eq!(sequence_cache(&tk), (10, 4, 1));

    let mut tk =
        seq_kit("create sequence seq increment -2 start 0 maxvalue 10 minvalue -10 cache 3 cycle");
    tk.must_query("select setval(seq, 20)").check(["<nil>"]);
    tk.must_query("select setval(seq, -20)").check(["-20"]);
    tk.must_query("select nextval(seq)").check(["10"]);
    assert_eq!(sequence_cache(&tk), (10, 6, 1));
}

/// Go rows `pkg/ddl/sequence_test.go:279-290` minus the end/round internals:
/// `setval(seq, -20)` below MINVALUE reports NULL, `setval(seq, 20)` above
/// MAXVALUE rebases, and the next `nextval` is the wrapped -10.
#[test]
fn setval_beyond_bounds_then_wraps_to_the_negative_bound() {
    let mut tk =
        seq_kit("create sequence seq increment 2 start 0 maxvalue 10 minvalue -10 cache 3 cycle");
    check_ladder(
        &mut tk,
        &[
            ("select setval(seq, -20)", "<nil>"),
            ("select setval(seq, 20)", "20"),
            ("select nextval(seq)", "-10"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:292-334` minus the end/round internals:
/// the negative-growth SETVAL ladder -- `setval(-2)` inside the first batch,
/// `setval(-10)` == MINVALUE wrapping to 10, `nextval` 7, 4, then
/// `setval(seq, 0)` rebasing downward (`nextval` -2), and the
/// `setval(20)`-rejected / `setval(-20)`-rebase pair.
#[test]
fn negative_growth_setval_ladder_matches_go() {
    let mut tk =
        seq_kit("create sequence seq increment -3 start 5 maxvalue 10 minvalue -10 cache 3 cycle");
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "5"),
            ("select setval(seq, -2)", "-2"),
            ("select nextval(seq)", "-4"),
            ("select setval(seq, -10)", "-10"),
            ("select nextval(seq)", "10"),
            ("select nextval(seq)", "7"),
            ("select nextval(seq)", "4"),
            ("select setval(seq, 0)", "0"),
            ("select nextval(seq)", "-2"),
        ],
    );

    let mut tk =
        seq_kit("create sequence seq increment -2 start 0 maxvalue 10 minvalue -10 cache 3 cycle");
    check_ladder(
        &mut tk,
        &[
            ("select setval(seq, 20)", "<nil>"),
            ("select setval(seq, -20)", "-20"),
            ("select nextval(seq)", "10"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:336-352`: `LASTVAL` is SESSION state --
/// NULL before the first `nextval`, tracks only `NEXTVAL`/`NEXT VALUE FOR`
/// (2 after `next value for`), and `setval` never changes it (still 2 after
/// both the NULL-reporting and value-reporting `setval`s; 6, 7 follow).
#[test]
fn lastval_tracks_nextval_but_not_setval() {
    let mut tk = seq_kit("create sequence seq");
    check_ladder(
        &mut tk,
        &[
            ("select lastval(seq)", "<nil>"),
            ("select nextval(seq)", "1"),
            ("select lastval(seq)", "1"),
            ("select next value for seq", "2"),
            ("select lastval(seq)", "2"),
            ("select setval(seq, -1)", "<nil>"),
            ("select lastval(seq)", "2"),
            ("select setval(seq, 5)", "5"),
            ("select lastval(seq)", "2"),
            ("select nextval(seq)", "6"),
            ("select nextval(seq)", "7"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:354-410` minus the end/round internals:
/// LASTVAL across CYCLE + CACHE invalidation -- positive growth
/// (`increment 3 start 3 maxvalue 14 cache 3 cycle`): lastval 3 after the
/// first draw, still 3 after `setval(10)` invalidated the batch, 12 after the
/// next draw, 1 after the wrap, and NULL->value transitions around it;
/// negative growth (`increment -3 start -2 maxvalue 10 minvalue -10 cache 3
/// cycle`): -2, still -2 after `setval(-8)`, then 10 after the wrap;
/// and `increment -1 start 1` ending at -9 after `setval(-8)`.
#[test]
fn lastval_across_cache_invalidation_and_cycles() {
    let mut tk = seq_kit("create sequence seq increment 3 start 3 maxvalue 14 cache 3 cycle");
    check_ladder(
        &mut tk,
        &[
            ("select lastval(seq)", "<nil>"),
            ("select nextval(seq)", "3"),
            ("select setval(seq, 10)", "10"),
            ("select lastval(seq)", "3"),
            ("select nextval(seq)", "12"),
            ("select setval(seq, 13)", "13"),
            ("select lastval(seq)", "12"),
            ("select nextval(seq)", "1"),
            ("select lastval(seq)", "1"),
        ],
    );

    let mut tk =
        seq_kit("create sequence seq increment -3 start -2 maxvalue 10 minvalue -10 cache 3 cycle");
    check_ladder(
        &mut tk,
        &[
            ("select lastval(seq)", "<nil>"),
            ("select nextval(seq)", "-2"),
            ("select setval(seq, -8)", "-8"),
            ("select lastval(seq)", "-2"),
            ("select nextval(seq)", "10"),
            ("select lastval(seq)", "10"),
        ],
    );

    let mut tk =
        seq_kit("create sequence seq increment -1 start 1 maxvalue 10 minvalue -10 cache 3 cycle");
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "1"),
            ("select setval(seq, -8)", "-8"),
            ("select nextval(seq)", "-9"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:422-435`: the seek formula must not
/// overflow i64 at the boundaries -- an ascending sequence seeded at
/// `i64::MIN + 1` steps to `setval`'s 9223372036854775800 and then ...801,
/// and a descending one seeded at 9223372036854775806 descends to
/// -9223372036854775800 and then ...802. Go survives because its signed
/// arithmetic WRAPS (`CalcSequenceBatchSize`, `pkg/meta/autoid/autoid.go:802`,
/// and the uint64-domain seek it delegates to).
#[test]
fn i64_boundary_seek_does_not_overflow() {
    let mut tk = seq_kit(
        "create sequence seq increment 2 start -9223372036854775807 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle",
    );
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "-9223372036854775807"),
            ("select setval(seq, 9223372036854775800)", "9223372036854775800"),
            ("select nextval(seq)", "9223372036854775801"),
        ],
    );

    let mut tk = seq_kit(
        "create sequence seq increment -2 start 9223372036854775806 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle",
    );
    check_ladder(
        &mut tk,
        &[
            ("select nextval(seq)", "9223372036854775806"),
            ("select setval(seq, -9223372036854775800)", "-9223372036854775800"),
            ("select nextval(seq)", "-9223372036854775802"),
        ],
    );
}

/// Go rows `pkg/ddl/sequence_test.go:466-479`: the ticase regression -- after
/// `setval(seq, 10)` a LOWER `setval(seq, 5)` reports NULL (the collapsed
/// batch base/end keeps the second, lower SETVAL honest); same for a
/// descending sequence at -10/-5.
#[test]
fn setval_regression_lower_value_reports_null() {
    let mut tk = seq_kit("create sequence seq");
    tk.must_query("select setval(seq, 10)").check(["10"]);
    tk.must_query("select setval(seq, 5)").check(["<nil>"]);
    tk.must_exec("drop sequence seq");
    tk.must_exec("create sequence seq increment=-1");
    tk.must_query("select setval(seq, -10)").check(["-10"]);
    tk.must_query("select setval(seq, -5)").check(["<nil>"]);
}

/// Go rows `pkg/ddl/sequence_test.go:481-496`: a SECOND session sees the
/// shared stored counter, not this session's cache -- after session 1's
/// `setval(seq, 100)`, session 2's `setval(seq, 50)` is NULL, its
/// `nextval` is 101, and 100/101 report NULL while 102 reports 102; the
/// descending `increment=-1` mirror ends at -101/-102.
#[test]
fn setval_and_nextval_are_visible_across_sessions() {
    let mut tk = seq_kit("create sequence seq");
    tk.must_query("select setval(seq, 100)").check(["100"]);
    let mut tk1 = tk.new_tk();
    tk1.must_exec("use test");
    check_ladder(
        &mut tk1,
        &[
            ("select setval(seq, 50)", "<nil>"),
            ("select nextval(seq)", "101"),
            ("select setval(seq, 100)", "<nil>"),
            ("select setval(seq, 101)", "<nil>"),
            ("select setval(seq, 102)", "102"),
        ],
    );
    tk.must_exec("drop sequence seq");

    tk.must_exec("create sequence seq increment=-1");
    tk.must_query("select setval(seq, -100)").check(["-100"]);
    check_ladder(
        &mut tk1,
        &[
            ("select setval(seq, -50)", "<nil>"),
            ("select nextval(seq)", "-101"),
            ("select setval(seq, -100)", "<nil>"),
            ("select setval(seq, -101)", "<nil>"),
            ("select setval(seq, -102)", "-102"),
        ],
    );
    tk.must_exec("drop sequence seq");
}

/// Go rows `pkg/ddl/sequence_test.go:498-505`: `nextval(seq)` in a projection
/// over a two-row table consumes one value PER ROW (1 1, 2 2), and
/// `nextval(t)` over a TABLE name errors before any row is produced.
#[test]
fn nextval_in_a_projection_consumes_one_value_per_row() {
    let mut tk = TestKit::new();
    tk.must_exec("create sequence seq");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t values(1),(2)");
    tk.must_query("select nextval(seq), t.a from t").check(["1 1", "2 2"]);
    // A statement that ERRORS over `nextval(t)` fails before producing rows.
    tk.must_exec_to_err("select nextval(t), t.a from t");
    tk.must_exec_to_err("select nextval(seq), nextval(t), t.a from t");
}

/// Go rows `pkg/ddl/sequence_test.go:496-506`: a statement that ERRORS must
/// consume NOTHING -- after `select nextval(seq), nextval(t), t.a from t`
/// fails with 1347, the next bare `select nextval(seq)` draws 3. Go fails at
/// PLAN-BUILD time (`buildSimpleExpression`'s sequence resolution), before
/// any row is evaluated.
#[test]
fn an_errored_projection_consumes_no_sequence_values() {
    let mut tk = TestKit::new();
    tk.must_exec("create sequence seq");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t values(1),(2)");

    tk.must_query("select nextval(seq), t.a from t").check(["1 1", "2 2"]);

    assert_eq!(
        tk.must_get_err_code("select nextval(seq), nextval(t), t.a from t", 1347).message,
        "'test.t' is not SEQUENCE"
    );
    tk.must_query("select nextval(seq)").check(["3"]);
}

/// Go rows `pkg/ddl/sequence_test.go:437-464`: `nextval`/`lastval`/`setval`
/// over an existing TABLE (`test.seq`) or VIEW (`test.seq1`) report
/// `[schema:1347]'test.seq' is not SEQUENCE` (Go's `WrongObjectType`).
#[test]
fn sequence_functions_over_a_table_or_view_report_1347() {
    let mut tk = TestKit::new();
    tk.must_exec("create table seq(a int)");
    tk.must_exec("create view seq1 as select * from seq");
    for name in ["seq", "seq1"] {
        for function in ["nextval", "lastval"] {
            assert_eq!(
                tk.must_get_err_code(&format!("select {function}({name})"), 1347).message,
                format!("'test.{name}' is not SEQUENCE"),
                "{function} over {name}"
            );
        }
        assert_eq!(
            tk.must_get_err_code(&format!("select setval({name}, 10)"), 1347).message,
            format!("'test.{name}' is not SEQUENCE"),
            "setval over {name}"
        );
    }
}

/// Go `BenchmarkInsertCacheDefaultExpr` (`pkg/ddl/sequence_test.go:527`):
/// the benchmark's SETUP is a table whose column default is
/// `next value for seq`; the behavior it exercises is that INSERTs with an
/// empty values list consume the sequence allocator through the column
/// default, one value per row. (The timed b.N loop is Go's measurement
/// harness, not behavior; 1000 rows are inserted and the assigned values
/// are verified.)
// go-parity-gap: the multi-row empty-list form `values (),(),...` that Go's
// benchmark statement uses does not parse in this tier (the parser refuses
// an empty `()` row list after the first), so the 1000 rows are inserted one
// `values ()` statement at a time -- the per-row consumption contract and
// the assigned values are the same.
#[test]
fn insert_with_a_sequence_default_consumes_the_allocator_per_row() {
    let mut tk = TestKit::new();
    tk.must_exec("create sequence seq");
    tk.must_exec("create table t(a int default next value for seq)");
    for _ in 0..1000 {
        tk.must_exec("insert into t values ()");
        assert_eq!(tk.affected_rows(), 1);
    }
    // count 1000; the first row drew 1, the last row drew 1000.
    tk.must_query("select count(*), min(a), max(a) from t").check(["1000 1 1000"]);
}
