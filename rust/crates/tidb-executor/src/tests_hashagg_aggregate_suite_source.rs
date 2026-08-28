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

//! Ports of Go `pkg/executor/test/aggregate/aggregate_test.go` items 730–739
//! (the parallel/stream/spill aggregate slice) plus that package's
//! `main_test.go` bootstrap.
//!
//! SCOPE NOTE. Go drives the mock-TiKV SQL suite with
//! `tidb_streamagg_concurrency`/`tidb_executor_concurrency` settings, Shuffle
//! plan-shape checks, and failpoints that panic the worker pipelines; the
//! comparison targets are PARALLEL-vs-SERIAL agreement (and, for
//! `TestAggInDisk`, spill-to-disk observability). This tier pins the SAME
//! aggregate semantics through absolute values over fixed data (Go's random
//! draws replaced by fixed rows, the established pattern of
//! `tests_partition_table_sql_source`), and records the parallel/Shuffle/
//! failpoint arms as `#[ignore]` gap tests.

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog)
        .unwrap_or_else(|error| panic!("create {sql:?} failed: {error:?}"));
}

fn insert(catalog: &mut Catalog, sql: &str) {
    run_insert_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("insert {sql:?} failed: {error:?}"));
}

fn select(catalog: &Catalog, sql: &str) -> Vec<Vec<Datum>> {
    run_select_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("select {sql:?} failed: {error:?}"))
}

/// Renders one datum the way Go's testkit prints a TEXT cell
/// (`fmt.Sprintf("%v")` semantics for the values these queries return).
fn cell(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => {
            let rendered = format!("{value}");
            if rendered.contains('.') { rendered } else { format!("{rendered}") }
        }
        Datum::Decimal(value) => value.to_string(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        other => format!("{other:?}"),
    }
}

/// Go's `reconstructParallelGroupConcatResult` (aggregate_test.go:147): each
/// group's `group_concat(a, b)` is split on `,`, the tokens are sorted, and
/// the token lists are sorted — so only the MULTISET of per-row `a||b`
/// concatenations matters, never the row order within a group or the group
/// order.
fn reconstructed(catalog: &Catalog, sql: &str) -> Vec<String> {
    let mut data = Vec::new();
    for row in select(catalog, sql) {
        let cell = cell(&row[0]);
        let mut tokens: Vec<&str> = cell.split(',').collect();
        tokens.sort_unstable();
        data.push(tokens.join(","));
    }
    data.sort();
    data
}

/// Go `aggregate_test.go:128::TestParallelStreamAggGroupConcat`, data-level:
/// `select group_concat(a, b) from t group by b` joins each row's `a` and `b`
/// with no separator and groups with `,`, and the reconstructed (sorted)
/// multiset of group results is what Go requires the 1/2/4/8-concurrency
/// runs to agree on. The fixed 12-row fixture replaces Go's
/// `rand.Intn(100)` draws; the expected multiset is that same concatenation
/// rule applied by hand.
#[test]
fn group_concat_result_multiset_is_row_order_insensitive() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "CREATE TABLE t(a bigint, b bigint)");
    insert(
        &mut catalog,
        "insert into t values (3, 1), (17, 2), (0, 1), (99, 2), (42, 7), (8, 1), \
         (55, 7), (1, 3), (26, 2), (7, 3), (90, 1), (64, 3)",
    );

    let got = reconstructed(&catalog, "select group_concat(a, b) from t group by b");
    // Group b=1 holds a ∈ {3, 0, 8, 90}: tokens "31","01","81","901" sorted.
    // Group b=2 holds a ∈ {17, 99, 26}: tokens "172","992","262" sorted.
    // Group b=3 holds a ∈ {1, 7, 64}: tokens "13","73","643" sorted.
    // Group b=7 holds a ∈ {42, 55}: tokens "427","557" sorted.
    let expected = vec![
        "01,31,81,901".to_owned(),
        "13,643,73".to_owned(),
        "172,262,992".to_owned(),
        "427,557".to_owned(),
    ];
    assert_eq!(got, expected, "group_concat(a, b) multiset per group");
}

/// Go `aggregate_test.go:174::TestIssue20658`, data-level: the aggregate
/// family Go streams (`count/sum/avg/max/min/bit_or/bit_xor/bit_and/
/// var_pop/var_samp/stddev_pop/stddev_samp/approx_count_distinct/
/// approx_percentile`) over `group by b`. Go requires the parallel streams to
/// reproduce the serial values within 1e-3; this tier pins the serial values
/// themselves as absolutes over a fixed 6-row fixture, hand-derived from the
/// Go definitions (`func_count.go`, `func_sum.go`, `func_avg.go`,
/// `func_max_min.go`, `func_bitfuncs.go`, `func_varpop.go`,
/// `func_percentile.go`).
///
/// Group b=1 reads a ∈ {1, 3, NULL}; group b=2 reads a ∈ {2, 2, 6}.
#[test]
fn aggregate_family_group_values_match_the_go_definitions() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "CREATE TABLE t(a bigint, b bigint)");
    insert(&mut catalog, "insert into t values (1,1),(3,1),(null,1),(2,2),(2,2),(6,2)");

    // count skips NULLs (Go `countPartial`): b=1 holds {1,3,NULL} -> 2,
    // b=2 holds {2,2,6} -> 3.
    assert_eq!(select(&catalog, "select count(a) from t group by b"), [[Datum::Int(2)], [Datum::Int(3)]]);
    // SUM over an integer argument returns a widened DECIMAL (Go `typeInfer4Sum`).
    let rows = select(&catalog, "select sum(a) from t group by b");
    assert_eq!(cell(&rows[0][0]), "4");
    assert_eq!(cell(&rows[1][0]), "10");
    // AVG returns DECIMAL scale 4 for integer inputs (Go `typeInfer4Avg`).
    let rows = select(&catalog, "select avg(a) from t group by b");
    assert_eq!(cell(&rows[0][0]), "2.0000");
    assert_eq!(cell(&rows[1][0]), "3.3333");
    assert_eq!(select(&catalog, "select max(a) from t group by b"), [[Datum::Int(3)], [Datum::Int(6)]]);
    assert_eq!(select(&catalog, "select min(a) from t group by b"), [[Datum::Int(1)], [Datum::Int(2)]]);
    // The bit family folds in the unsigned 64-bit domain (Go `func_bitfuncs.go`):
    // b=1: OR(1,3)=3, XOR(1,3)=2, AND(1,3)=1; b=2: OR(2,2,6)=6, XOR=6, AND=2.
    assert_eq!(select(&catalog, "select bit_or(a) from t group by b"), [[Datum::UInt(3)], [Datum::UInt(6)]]);
    assert_eq!(select(&catalog, "select bit_xor(a) from t group by b"), [[Datum::UInt(2)], [Datum::UInt(6)]]);
    assert_eq!(select(&catalog, "select bit_and(a) from t group by b"), [[Datum::UInt(1)], [Datum::UInt(2)]]);
    // b=1 mean 2: var_pop = ((1-2)^2+(3-2)^2)/2 = 1; var_samp = 2/1 = 2;
    // stddev_pop = 1; stddev_samp = sqrt(2). b=2 mean 10/3: var_pop = 32/9;
    // var_samp = 16/3; stddev_pop = sqrt(32/9); stddev_samp = sqrt(16/3)
    // (Go `calculateIntermediate`, func_varpop.go).
    let approx = |sql: &str| -> Vec<f64> {
        select(&catalog, sql)
            .into_iter()
            .map(|row| match &row[0] {
                Datum::Real(value) => *value,
                other => panic!("real expected, got {other:?}"),
            })
            .collect()
    };
    let values = approx("select var_pop(a) from t group by b");
    assert!((values[0] - 1.0).abs() < 1e-12 && (values[1] - 32.0 / 9.0).abs() < 1e-12, "{values:?}");
    let values = approx("select var_samp(a) from t group by b");
    assert!((values[0] - 2.0).abs() < 1e-12 && (values[1] - 16.0 / 3.0).abs() < 1e-12, "{values:?}");
    let values = approx("select stddev_pop(a) from t group by b");
    assert!((values[0] - 1.0).abs() < 1e-12 && (values[1] - (32.0f64 / 9.0).sqrt()).abs() < 1e-12, "{values:?}");
    let values = approx("select stddev_samp(a) from t group by b");
    assert!((values[0] - 2.0f64.sqrt()).abs() < 1e-12 && (values[1] - (16.0f64 / 3.0).sqrt()).abs() < 1e-12, "{values:?}");
    // BJKST sketch (Go `partialResult4ApproxCountDistinct`): 2 distinct per group.
    assert_eq!(
        select(&catalog, "select approx_count_distinct(a) from t group by b"),
        [[Datum::Int(2)], [Datum::Int(2)]]
    );
    // Ordinal rank k = min(ceil(N * P/100), N), 1-indexed
    // (pkg/executor/aggfuncs/func_percentile.go:41). b=1: N=2 -> k=1 -> 1;
    // b=2: N=3 -> k=1 -> 2.
    assert_eq!(
        select(&catalog, "select approx_percentile(a, 7) from t group by b"),
        [[Datum::Int(1)], [Datum::Int(2)]]
    );
}

/// Go `aggregate_test.go:249::TestAggInDisk`, numeric halves: over a 201-row
/// `t` (0..200 plus one extra 0, Go's own data), the cross join
/// `avg(t1.a) from t t1 join t t2 group by t1.a, t2.a` sums to exactly
/// 4040100.0000 (Go's asserted value), and the no-groupby/no-data tails
/// answer `[0]` and no rows for `count(c)`.
///
/// go-parity-gap: the `desc analyze` DISK-column assertions (the HashAgg
/// plan lines must show a non-zero disk usage under the 4MB query quota) and
/// the two `sum_int` result queries live in the gaps below
/// (`agg_in_disk_reports_disk_usage_in_plan` and
/// `sum_int_distinct_semantics`).
#[test]
fn cross_join_grouped_avg_sums_to_the_go_value() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t(a int)");
    let mut sql = String::from("insert into t values (0)");
    for i in 1..=200 {
        sql.push_str(&format!(",({i})"));
    }
    insert(&mut catalog, &sql);

    let rows = select(
        &catalog,
        "select sum(tt.b) from (select avg(t1.a) as b from t t1 join t t2 group by t1.a, t2.a) as tt",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(cell(&rows[0][0]), "4040100.0000");

    // "Add code cover" tail: one extra row so the spill chunk is not always
    // full — the joined aggregate's sum is unchanged by a duplicate 0 row's
    // group because Go asserts different queries there (sum_int ones live in
    // the gap rows); the no-groupby/no-data tails are pinned directly:
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t(c int, c1 int)");
    assert_eq!(select(&catalog, "select count(c) from t"), [[Datum::Int(0)]]);
    assert!(select(&catalog, "select count(c) from t group by c1").is_empty());
}

/// Go `aggregate_test.go:426::TestParallelHashAgg`, first half: twenty
/// case-variant spellings of `aa`–`ee` are TWENTY DISTINCT group keys
/// (`aa`/`AA`/`aA`/`Aa` …), each summing 20 over the repeated inserts — the
/// group keys must never collide case-insensitively (Go builds the group key
/// from the raw bytes; this fixture is Go's own table content).
///
/// go-parity-gap: the second half (list-partitioned `tlist` vs `tnormal`
/// aggregate equality under `tidb_partition_prune_mode` dynamic vs static)
/// lives in `parallel_hash_agg_partition_prune_parity` below: the prune-mode
/// switch has no SQL surface on this tier.
#[test]
fn parallel_hash_agg_group_keys_stay_case_sensitive() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table test_parallel_hash_agg(k varchar(30), v int)");
    for _ in 0..20 {
        insert(
            &mut catalog,
            "insert into test_parallel_hash_agg (k, v) values \
             ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), \
             ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), \
             ('dD', 1), ('Dd', 1), ('ee', 1), ('EE', 1), ('eE', 1), ('Ee', 1)",
        );
    }
    let rows = select(
        &catalog,
        "select k, sum(v) from test_parallel_hash_agg group by k",
    );
    assert_eq!(rows.len(), 20, "twenty distinct case-variant keys");
    for row in rows {
        assert_eq!(cell(&row[1]), "20", "each key sums the twenty ones");
    }
}

/// Go `aggregate_test.go:128::TestParallelStreamAggGroupConcat` and
/// `aggregate_test.go:174::TestIssue20658`, parallel arms: with
/// `tidb_streamagg_concurrency` 2/4/8 the explain plan shows a `Shuffle`
/// above the stream aggregate and the parallel results must agree with the
/// serial run (multiset or within 1e-3).
///
/// go-parity-gap: the shuffle/parallel stream-agg driver, the concurrency
/// variables and explain output are unported on this tier; the data-level
/// halves are the two running tests above.
#[test]
#[ignore = "go-parity-gap: parallel stream-agg (tidb_streamagg_concurrency + Shuffle plan) is unported on this tier"]
fn stream_agg_parallel_concurrency_plan_and_agreement() {}

/// Go `aggregate_test.go:249::TestAggInDisk`, observability arm: under
/// `tidb_mem_quota_query = 4194304` the `desc analyze` plan's HashAgg lines
/// must report non-zero DISK usage, and the `sum_int` result queries must
/// answer 4060200.
///
/// go-parity-gap: `desc analyze` output, the query-quota spill wiring on the
/// SQL path, and `sum_int` (see `sum_int_distinct_semantics`) are unported.
#[test]
#[ignore = "go-parity-gap: desc-analyze disk usage column + SQL-path query quota spill + sum_int are unported"]
fn agg_in_disk_reports_disk_usage_in_plan() {}

/// Go `aggregate_test.go:39::TestHashAggRuntimeStat`: the
/// `HashAggRuntimeStats` string (`partial_worker:{wall_time:20s, ...},
/// final_worker:{...}`), `Clone`, and `Merge` doubling both halves.
///
/// go-parity-gap: the executor runtime-stats surface (Go
/// `pkg/executor/aggregate/hash_agg_worker.go`'s `AggWorkerStat` and
/// `HashAggRuntimeStats.String/Clone/Merge`) is unported, the same gap the
/// join-side stats tests record in `tests_join_probe_source_gaps`.
#[test]
#[ignore = "go-parity-gap: HashAggRuntimeStats/AggWorkerStat (aggregate pkg runtime stats, String/Clone/Merge) unported; Executor carries no runtime-stats surface in this workspace"]
fn hash_agg_runtime_stats_format_clone_and_merge() {}

/// Go `aggregate_test.go:76::TestSumIntDistinct`: `sum_int(distinct a)` over
/// signed/unsigned bigint pairs answers 6/6 globally and 3/3 per group, and
/// an all-NULL table answers `<nil> <nil>`.
///
/// go-parity-gap: measured — `sum_int` parses as a keyword but has no
/// aggregate arm in this tier's driver (`agg_build.rs` maps
/// COUNT/SUM/MIN/MAX/AVG/bit/variance/JSON aggregates only), so the probe
/// `select sum_int(a) from t` errors "no such function".
#[test]
#[ignore = "go-parity-gap: sum_int has no aggregate arm in this tier's driver (agg_build.rs), so the sum_int(distinct ...) SQL shape cannot run"]
fn sum_int_distinct_semantics() {}

/// Go `aggregate_test.go:96::TestSumIntMockCopPushDown`: with
/// `agg_to_cop(), hash_agg()` the `explain format='brief'` plan shows
/// `HashAgg` under `cop[tikv]` with `sum_int(test.t.a)`/`sum_int(test.t.b)`
/// pushdown, and the query answers `3 3`.
///
/// go-parity-gap: no coprocessor pushdown plan/explain text on this tier,
/// and `sum_int` is unported (see `sum_int_distinct_semantics`).
#[test]
#[ignore = "go-parity-gap: cop[tikv] pushdown plan text and sum_int aggregate are unported"]
fn sum_int_mock_cop_push_down_plan_and_result() {}

/// Go `aggregate_test.go:293::TestRandomPanicConsume`: with the
/// `ConsumeRandomPanic` failpoints armed over the aggregate/join/copr
/// consume sites, twenty query shapes are driven to a random panic ten times
/// each and every failure renders `failpoint panic: ERROR 1105 (HY000): Out
/// Of Memory Quota![conn=1]` or `context canceled`.
///
/// go-parity-gap: the percentage-panic failpoints and the random concurrency
/// driver are unported.
#[test]
#[ignore = "go-parity-gap: ConsumeRandomPanic failpoints (aggregate/copr/join consume sites) and the random-concurrency loop are unported"]
fn random_panic_consume_renders_the_oom_error() {}

/// Go `aggregate_test.go:426::TestParallelHashAgg`, second half: a
/// list-partitioned `tlist` and an unpartitioned `tnormal` aggregate
/// identically under `tidb_partition_prune_mode` `dynamic` and `static`,
/// for min/max/sum/count.
///
/// go-parity-gap: `set tidb_partition_prune_mode` has no SQL surface on this
/// tier, so the static/dynamic comparison cannot run.
#[test]
#[ignore = "go-parity-gap: tidb_partition_prune_mode static/dynamic switch has no SQL surface on this tier"]
fn parallel_hash_agg_partition_prune_parity() {}

/// Go `aggregate_test.go:501::TestIssue50849`: with the
/// `injectHashAggClosePanic` failpoint armed, closing the hash-agg result set
/// of `select /*+hash_agg()*/ sum(t1.a) from t t1 join t t2` returns an error
/// that carries a stack (`errors.HasStack`).
///
/// go-parity-gap: the close-path panic failpoint and Go's stack-carrying
/// error wrapper are unported.
#[test]
#[ignore = "go-parity-gap: injectHashAggClosePanic failpoint and the errors.HasStack surface are unported"]
fn hash_agg_close_panic_carries_stack() {}

/// Go `aggregate_test.go:529::TestStreamAggPendingMemDeltaBatching`: with
/// `streamAggMemDeltaFlushForTest` armed as a panic, small per-group
/// GROUP_CONCAT deltas must never cross the flush threshold while
/// large-string groups must trigger the pendingMemDelta flush mid-group.
///
/// go-parity-gap: the pendingMemDelta batching optimization and its
/// `streamAggMemDeltaFlushForTest` failpoint are unported.
#[test]
#[ignore = "go-parity-gap: stream-agg pendingMemDelta batching + streamAggMemDeltaFlushForTest failpoint are unported"]
fn stream_agg_pending_mem_delta_batching_thresholds() {}
