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

//! Ports of Go `pkg/executor/test/analyzetest` items 741–780
//! (`analyze_bench_test.go::BenchmarkAnalyzePartition` and the 742–780 slice
//! of `analyze_test.go`).
//!
//! The tests exercise the statistics behavior implemented by this crate's
//! analyze engine ([`crate::analyze::kv::analyze_kv_table`]).

use std::sync::Arc;

use tidb_datatype::Datum;

use crate::analyze::{kv::analyze_kv_table, AnalyzeOptions};
use crate::{run_create_table_on, run_insert_on, Catalog, StmtContext};

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

fn kv_table_of(catalog: &Catalog, name: &str) -> crate::kv_table::KvTable {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in("test", name) else {
        panic!("table {name} is not stored as bytes");
    };
    table.clone()
}

/// Analyzes one table image and publishes the result under `physical_id`,
/// the exact publication step the in-process session's analyze arm runs
/// (`tidb-session/src/analyze_arm.rs`).
fn analyze_and_publish(
    catalog: &mut Catalog,
    table: &mut crate::kv_table::KvTable,
    physical_id: i64,
    options: &AnalyzeOptions,
) {
    let statistics = analyze_kv_table(table, options, None, &ctx())
        .unwrap_or_else(|error| panic!("analyze failed: {error:?}"));
    catalog.set_table_statistics(physical_id, Arc::new(statistics));
}

/// Renders one datum the way Go's testkit prints a TEXT cell.
fn cell(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => format!("{value}"),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    }
}

/// Decodes one stored INT histogram bound: the bytes carry the value-kind
/// prefix and the sign-flipped big-endian integer (Go's `SHOW STATS_BUCKETS`
/// prints the decoded value).
fn int_of_encoded(datum: &Datum) -> i64 {
    let Datum::Bytes(bytes) = datum else {
        panic!("encoded bound expected, got {datum:?}");
    };
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[1..9]);
    (u64::from_be_bytes(raw) ^ 0x8000_0000_0000_0000) as i64
}

/// Decodes one stored VARCHAR histogram bound. Column histograms keep the
/// plain value bytes; index histograms keep the kind-prefixed key form with
/// zero padding and a `0xf?` flag tail (the same stored form the cluster
/// tier round-trips through `show stats_buckets`).
fn text_of_encoded(datum: &Datum) -> String {
    let Datum::Bytes(bytes) = datum else {
        panic!("encoded bound expected, got {datum:?}");
    };
    let mut body: &[u8] = if bytes.first() == Some(&0x01) {
        &bytes[1..]
    } else {
        bytes
    };
    while let Some((&last, rest)) = body.split_last() {
        if last == 0 || last >= 0xf8 {
            body = rest;
        } else {
            break;
        }
    }
    String::from_utf8(body.to_vec()).expect("utf8 bound")
}

fn topn_counts(topn: Option<&tidb_stats::cmsketch::TopN>) -> Vec<u64> {
    let mut counts = topn
        .map(|topn| {
            topn.entries()
                .iter()
                .map(|entry| entry.count)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    counts.sort_unstable();
    counts
}

/// Go `analyze_test.go:63::TestAnalyzePartition`: a RANGE-partitioned table
/// (`a` pk, `idx(b)`, v2) is analyzed; EVERY partition's statistics are
/// non-pseudo with 3 columns and 1 index, and every column/index histogram
/// has content (`col.Len()+col.TopN.Num() > 0` — Go's own bound, which
/// accepts the all-values-in-TopN shape). Then `alter table t analyze
/// partition p0` (this tier's named-partition analysis runs through the same
/// `restrict_read_to_partitions` + per-partition analyze the session arm
/// uses) leaves p0 non-pseudo with the same coverage while the untouched
/// partitions stay pseudo (no statistics at all — Go's `PseudoTable`).
///
/// Go's `TriggerPredicateColumnsCollection` does not change the port: this
/// tier's analyze covers every column by construction.
#[test]
fn analyze_partition_publishes_per_partition_then_partition_scoped_statistics() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), \
         PARTITION p3 VALUES LESS THAN (21))",
    );
    for i in 1..21 {
        insert(
            &mut catalog,
            &format!("insert into t values ({i}, {i}, 'hello')"),
        );
    }
    let (table_id, partition_ids) = {
        let table = kv_table_of(&catalog, "t");
        let ids = table
            .partition()
            .map(|partition| {
                partition
                    .definitions
                    .iter()
                    .map(|d| d.id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        (table.table_id, ids)
    };
    assert_eq!(partition_ids.len(), 4);

    // Whole-table analyze publishes non-pseudo statistics per partition.
    let options = AnalyzeOptions::default();
    let mut table = kv_table_of(&catalog, "t");
    for physical_id in &partition_ids {
        let mut partition = table.clone();
        partition.restrict_read_to_partitions(&[*physical_id]);
        let statistics = analyze_kv_table(&mut partition, &options, None, &ctx())
            .unwrap_or_else(|error| panic!("partition analyze failed: {error:?}"));
        assert!(!statistics.pseudo, "analyzed partition is not pseudo");
        assert_eq!(statistics.columns.len(), 3, "ColNum");
        assert_eq!(statistics.indexes.len(), 1, "IdxNum");
        for column in statistics.columns.values() {
            assert!(
                column.histogram.len()
                    + column
                        .topn
                        .as_ref()
                        .map_or(0, tidb_stats::cmsketch::TopN::num)
                    > 0,
                "column histogram has content"
            );
        }
        for index in statistics.indexes.values() {
            assert!(
                index.histogram.len()
                    + index
                        .topn
                        .as_ref()
                        .map_or(0, tidb_stats::cmsketch::TopN::num)
                    > 0,
                "index histogram has content"
            );
        }
        catalog.set_table_statistics(*physical_id, Arc::new(statistics));
    }
    let global = analyze_kv_table(&mut table, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("global analyze failed: {error:?}"));
    catalog.set_table_statistics(table_id, Arc::new(global));

    // Second act: only p0 is analyzed. p0 keeps full coverage; the other
    // partitions have no statistics, which is Go's pseudo table.
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), \
         PARTITION p3 VALUES LESS THAN (21))",
    );
    for i in 1..21 {
        insert(
            &mut catalog,
            &format!("insert into t values ({i}, {i}, 'hello')"),
        );
    }
    let mut table = kv_table_of(&catalog, "t");
    let mut p0 = table.clone();
    p0.restrict_read_to_partitions(&[partition_ids[0]]);
    let p0_stats = analyze_kv_table(&mut p0, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("p0 analyze failed: {error:?}"));
    assert!(!p0_stats.pseudo);
    assert_eq!(p0_stats.columns.len(), 3);
    assert_eq!(p0_stats.indexes.len(), 1);
    catalog.set_table_statistics(partition_ids[0], Arc::new(p0_stats));
    for physical_id in &partition_ids[1..] {
        assert!(
            catalog.table_statistics(*physical_id).is_none(),
            "un-analyzed partition {} is Go's pseudo table",
            physical_id
        );
    }
}

/// Go `analyze_test.go:306::TestExtractTopN`: after a v2 analyze of
/// `test_extract_topn(a int primary key, b int, index index_b(b))` where `b`
/// holds 0..9 and ten more 0s, the column's TopN holds exactly 10 entries
/// whose top entry counts 11, the index's likewise, and the count MULTISET
/// is one 11 plus nine 1s (Go's `show stats_topn` rows, sorted).
#[test]
fn analyze_extract_topn_entries_and_counts_from_index_and_column() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table te(a int primary key, b int, index index_b(b))",
    );
    for i in 0..10 {
        insert(&mut catalog, &format!("insert into te values ({i}, {i})"));
    }
    for i in 10..20 {
        insert(&mut catalog, &format!("insert into te values ({i}, 0)"));
    }
    let mut table = kv_table_of(&catalog, "te");
    let options = AnalyzeOptions {
        num_buckets: 256,
        num_topn: 20,
        ..AnalyzeOptions::default()
    };
    let statistics = analyze_kv_table(&mut table, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("analyze failed: {error:?}"));

    let column_b = statistics
        .columns
        .values()
        .find(|column| column.histogram.ndv == 10)
        .expect("column b statistics");
    let topn = column_b.topn.as_ref().expect("v2 keeps a TopN");
    assert_eq!(topn.num(), 10, "colStats.TopN.TopN length");
    assert_eq!(topn.entries()[0].count, 11, "the zero value's count");
    assert_eq!(topn_counts(Some(topn)), vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 11]);

    let index_b = statistics
        .indexes
        .values()
        .find(|index| index.histogram.ndv == 10)
        .expect("index_b statistics");
    let topn = index_b.topn.as_ref().expect("v2 keeps a TopN");
    assert_eq!(topn.num(), 10, "idxStats.TopN.TopN length");
    assert_eq!(topn.entries()[0].count, 11, "the zero value's count");
    assert_eq!(topn_counts(Some(topn)), vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 11]);
}

/// Go `analyze_test.go:359::TestAnalyzeFullSamplingOnIndexWithVirtualColumnOrPrefixColumn`:
/// `idx(b)` over `b int as (a+1)` with data
/// (1, 2, NULL, 3, 4, NULL, 5, 5, 5, 5) and `with 1 topn` builds, for the
/// index histogram: NDV 5, NULLs 2, buckets [2,2]/[3,3]/[4,4]/[5,5], and a
/// single TopN entry 6 counting 4 — Go's `show stats_buckets`/
/// `show stats_topn`/NDV/NULL rows verbatim. Its prefix-index half cuts
/// `idx(a(1))` before collecting statistics: aa/ab/ac become `a`, so the
/// index has NDV 2, TopN `a` count 3, and one remaining bucket for `b`.
#[test]
fn analyze_full_sampling_on_virtual_or_prefix_column_index() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table sampling_index_virtual_col(a int, b int as (a+1), index idx(b))",
    );
    insert(
        &mut catalog,
        "insert into sampling_index_virtual_col (a) values (1), (2), (null), (3), (4), (null), (5), (5), (5), (5)",
    );
    let mut table = kv_table_of(&catalog, "sampling_index_virtual_col");
    let options = AnalyzeOptions {
        num_topn: 1,
        ..AnalyzeOptions::default()
    };
    let statistics = analyze_kv_table(&mut table, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("analyze failed: {error:?}"));

    let index = statistics.indexes.values().next().expect("idx statistics");
    assert_eq!(index.histogram.ndv, 5, "the NDV");
    assert_eq!(index.histogram.null_count, 2, "the NULLs");
    let bounds: Vec<_> = index
        .histogram
        .buckets
        .iter()
        .map(|bucket| {
            (
                int_of_encoded(&bucket.lower_bound),
                int_of_encoded(&bucket.upper_bound),
            )
        })
        .collect();
    assert_eq!(
        bounds,
        vec![(2, 2), (3, 3), (4, 4), (5, 5),],
        "show stats_buckets rows"
    );

    let topn = index.topn.as_ref().expect("topn kept with 1 topn");
    assert_eq!(topn.num(), 1);
    assert_eq!(topn.entries()[0].count, 4, "the value 6 counts 4");

    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table sampling_index_prefix_col(a varchar(3), index idx(a(1)))",
    );
    insert(
        &mut catalog,
        "insert into sampling_index_prefix_col values ('aa'), ('ab'), ('ac'), ('bb')",
    );
    let mut table = kv_table_of(&catalog, "sampling_index_prefix_col");
    let statistics = analyze_kv_table(&mut table, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("prefix-index analyze failed: {error:?}"));

    let index = statistics.indexes.values().next().expect("idx statistics");
    assert_eq!(index.histogram.ndv, 2, "prefix-index NDV");
    let topn = index.topn.as_ref().expect("prefix index keeps a TopN");
    assert_eq!(topn.num(), 1);
    assert_eq!(topn.entries()[0].count, 3);
    assert_eq!(
        text_of_encoded(&Datum::new_bytes(topn.entries()[0].encoded.clone())),
        "a"
    );
    assert_eq!(index.histogram.buckets.len(), 1);
    let bucket = &index.histogram.buckets[0];
    assert_eq!(text_of_encoded(&bucket.lower_bound), "b");
    assert_eq!(text_of_encoded(&bucket.upper_bound), "b");
    assert_eq!(bucket.count, 1);
}

/// Go `analyze_test.go:458::TestAdjustSampleRateNote` and
/// `analyze_test.go:583::TestSmallTableAnalyzeV2`, sample-rate halves: the
/// auto-adjusted rate is `min(1, 110000/realtime_count)`
/// (`DEF_ROWS_FOR_SAMPLE_RATE`, `tidb-stats/src/row_sample_collector.rs`),
/// which is the number Go's Note rows print — 220000 stored rows give the
/// 0.500000 of `use min(1, 110000/220000) as the sample-rate=0.5`, and a
/// small table's storage count 10000 gives the 1.000000 of
/// `use min(1, 110000/10000)`; a table with no stats row at all reads all of
/// it (rate 1).
#[test]
fn analyze_auto_adjusted_sample_rate_boundaries() {
    use tidb_stats::row_sample_collector::adjusted_sample_rate;
    assert_eq!(adjusted_sample_rate(Some(220_000), None), 0.5);
    assert_eq!(adjusted_sample_rate(Some(10_000), None), 1.0);
    assert_eq!(adjusted_sample_rate(Some(3), None), 1.0);
    assert_eq!(adjusted_sample_rate(None, None), 1.0);
}

/// Go `analyze_test.go:509::TestIssue20874`: utf8mb4_unicode_ci /
/// utf8mb4_general_ci collations shape the analysis — the TopN values are
/// the collation SORT KEYS (`#`→\x02\xd2, `$`→\x0e\x0f, a→\x0e3 under
/// unicode_ci; C/c→\x00C and a→\x00A under general_ci) with the exact
/// counts, and the histograms carry Go's mysql.stats_histograms rows
/// exactly: (ndv, nulls, tot_col_size, stats_ver, correlation) = a:(3,0,6,2,1),
/// b:(2,0,6,2,-0.5), idxa:(3,0,6,2,0), idxb:(2,0,6,2,0).
#[test]
fn analyze_collation_sort_keys_shape_topn_and_histograms() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t (a char(10) collate utf8mb4_unicode_ci not null, \
         b char(20) collate utf8mb4_general_ci not null, key idxa(a), key idxb(b))",
    );
    insert(
        &mut catalog,
        "insert into t values ('#', 'C'), ('$', 'c'), ('a', 'a')",
    );
    let mut table = kv_table_of(&catalog, "t");
    let options = AnalyzeOptions {
        num_buckets: 2,
        num_topn: 3,
        ..AnalyzeOptions::default()
    };
    let statistics = analyze_kv_table(&mut table, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("analyze failed: {error:?}"));

    // TopN encoded values are the collation sort keys. Go's SHOW rows decode
    // them as "\x02\xd2" etc.; the stored bytes carry the same key with the
    // value-kind prefix (0x01) and the bytes-value tail (…f9).
    let key_bytes = |topn: Option<&tidb_stats::cmsketch::TopN>| -> Vec<(String, u64)> {
        topn.map(|topn| {
            topn.entries()
                .iter()
                .map(|entry| {
                    (
                        entry
                            .encoded
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>(),
                        entry.count,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
    };
    let column_a = &statistics.columns[&1];
    assert_eq!(
        key_bytes(column_a.topn.as_ref()),
        vec![
            ("0102d2000000000000f9".to_owned(), 1),
            ("010e0f000000000000f9".to_owned(), 1),
            ("010e33000000000000f9".to_owned(), 1),
        ],
        "unicode_ci keys for #, $, a"
    );
    let column_b = &statistics.columns[&2];
    assert_eq!(
        key_bytes(column_b.topn.as_ref()),
        vec![
            ("010041000000000000f9".to_owned(), 1),
            ("010043000000000000f9".to_owned(), 2),
        ],
        "general_ci keys: a → \\x00A (1), C and c → \\x00C (2)"
    );
    // The indexes repeat the column keys.
    assert_eq!(
        key_bytes(statistics.indexes[&1].topn.as_ref()),
        key_bytes(column_a.topn.as_ref())
    );
    assert_eq!(
        key_bytes(statistics.indexes[&2].topn.as_ref()),
        key_bytes(column_b.topn.as_ref())
    );

    // Histogram rows, Go's `select is_index, hist_id, distinct_count,
    // null_count, tot_col_size, stats_ver, correlation from
    // mysql.stats_histograms` sorted.
    let shape = |ndv: i64, nulls: i64, tot: i64, ver: i64, correlation: f64| {
        (ndv, nulls, tot, ver, correlation)
    };
    assert_eq!(
        shape(3, 0, 6, 2, 1.0),
        shape(
            column_a.histogram.ndv,
            column_a.histogram.null_count,
            column_a.histogram.tot_col_size,
            column_a.stats_ver,
            column_a.histogram.correlation
        )
    );
    assert_eq!(
        shape(2, 0, 6, 2, -0.5),
        shape(
            column_b.histogram.ndv,
            column_b.histogram.null_count,
            column_b.histogram.tot_col_size,
            column_b.stats_ver,
            column_b.histogram.correlation
        )
    );
    assert_eq!(
        shape(3, 0, 6, 2, 0.0),
        shape(
            statistics.indexes[&1].histogram.ndv,
            statistics.indexes[&1].histogram.null_count,
            statistics.indexes[&1].histogram.tot_col_size,
            statistics.indexes[&1].stats_ver,
            statistics.indexes[&1].histogram.correlation
        )
    );
    assert_eq!(
        shape(2, 0, 6, 2, 0.0),
        shape(
            statistics.indexes[&2].histogram.ndv,
            statistics.indexes[&2].histogram.null_count,
            statistics.indexes[&2].histogram.tot_col_size,
            statistics.indexes[&2].stats_ver,
            statistics.indexes[&2].histogram.correlation
        )
    );
}

/// Go `analyze_test.go:540::TestAnalyzeClusteredIndexPrimary`: a clustered
/// varchar primary key (`t0 ... primary key(a) clustered` and its default
/// twin `t1`) analyzes to buckets on BOTH the PRIMARY index and the column
/// with bounds "1111", and — unique single-column values never reaching
/// TopN (max count 1, pingcap/tidb#66221) — `show stats_topn` stays EMPTY.
#[test]
fn analyze_clustered_varchar_primary_key_buckets_without_topn() {
    for name in ["t0", "t1"] {
        let mut catalog = Catalog::default();
        let spelling = if name == "t0" {
            "create table t0(a varchar(20), primary key(a) clustered)"
        } else {
            "create table t1(a varchar(20), primary key(a))"
        };
        create(&mut catalog, spelling);
        insert(&mut catalog, &format!("insert into {name} values('1111')"));
        let mut table = kv_table_of(&catalog, name);
        let statistics = analyze_kv_table(&mut table, &AnalyzeOptions::default(), None, &ctx())
            .unwrap_or_else(|error| panic!("analyze failed: {error:?}"));

        // show stats_topn is empty: no TopN entries anywhere.
        for column in statistics.columns.values() {
            assert_eq!(
                column
                    .topn
                    .as_ref()
                    .map_or(0, tidb_stats::cmsketch::TopN::num),
                0,
                "no column TopN"
            );
        }
        for index in statistics.indexes.values() {
            assert_eq!(
                index
                    .topn
                    .as_ref()
                    .map_or(0, tidb_stats::cmsketch::TopN::num),
                0,
                "no index TopN"
            );
        }

        // show stats_buckets: PRIMARY and a both carry one [1111, 1111] bucket.
        let bounds_of = |buckets: &[tidb_stats::Bucket]| -> Vec<(String, String)> {
            buckets
                .iter()
                .map(|bucket| {
                    (
                        text_of_encoded(&bucket.lower_bound),
                        text_of_encoded(&bucket.upper_bound),
                    )
                })
                .collect()
        };
        // Column a's histogram.
        let column_a = statistics.columns.values().next().expect("column a");
        assert_eq!(
            bounds_of(&column_a.histogram.buckets),
            vec![("1111".to_owned(), "1111".to_owned())],
            "column a bucket bounds"
        );
        // The PRIMARY index histogram.
        let primary = statistics.indexes.values().next().expect("PRIMARY index");
        assert_eq!(
            bounds_of(&primary.histogram.buckets),
            vec![("1111".to_owned(), "1111".to_owned())],
            "PRIMARY bucket bounds"
        );
    }
}

/// Go `analyze_test.go:1948::TestAnalyzePartitionVerify`: a 10-partition
/// RANGE table (a int, b varchar, c int, INDEX idx_c(c)) with 1000 rows
/// ('abc' everywhere) analyzes to per-partition NDVs (b: 1, a/c/idx_c: 100)
/// and global NDVs (b: 1, a/c/idx_c: 1000) — Go's `show stats_histograms`
/// row count (4 + 4×10) and per-row distinct counts.
#[test]
fn analyze_partition_verify_per_partition_and_global_ndv() {
    let mut catalog = Catalog::default();
    let mut sql = String::from(
        "create table t(a int, b varchar(100), c int, INDEX idx_c(c)) PARTITION BY RANGE (a) (",
    );
    for n in (100..1000).step_by(100) {
        sql.push_str(&format!("PARTITION p{n} VALUES LESS THAN ({n}),"));
    }
    sql.push_str("PARTITION p1000 VALUES LESS THAN MAXVALUE)");
    create(&mut catalog, &sql);
    let mut rows_sql = String::from("insert into t (a,b,c) values(0, 'abc', 0)");
    for i in 1..1000 {
        rows_sql.push_str(&format!(" ,({i}, 'abc', {i})"));
    }
    insert(&mut catalog, &rows_sql);

    let options = AnalyzeOptions {
        num_buckets: 256,
        num_topn: 20,
        ..AnalyzeOptions::default()
    };
    let mut table = kv_table_of(&catalog, "t");
    let partition_ids: Vec<i64> = table
        .partition()
        .map(|partition| partition.definitions.iter().map(|d| d.id).collect())
        .unwrap_or_default();
    assert_eq!(partition_ids.len(), 10, "ten partitions");

    for physical_id in &partition_ids {
        let mut partition = table.clone();
        partition.restrict_read_to_partitions(&[*physical_id]);
        let statistics = analyze_kv_table(&mut partition, &options, None, &ctx())
            .unwrap_or_else(|error| panic!("partition analyze failed: {error:?}"));
        assert_eq!(statistics.row_count, 100);
        let columns: Vec<_> = statistics.columns.values().collect();
        assert_eq!(columns.len(), 3);
        assert_eq!(
            columns[1].histogram.ndv, 1,
            "partition b: one distinct value"
        );
        assert_eq!(columns[0].histogram.ndv, 100, "partition a");
        assert_eq!(columns[2].histogram.ndv, 100, "partition c");
        assert_eq!(
            statistics
                .indexes
                .values()
                .next()
                .expect("idx_c")
                .histogram
                .ndv,
            100
        );
        catalog.set_table_statistics(*physical_id, Arc::new(statistics));
    }

    let global = analyze_kv_table(&mut table, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("global analyze failed: {error:?}"));
    assert_eq!(global.row_count, 1000);
    let columns: Vec<_> = global.columns.values().collect();
    assert_eq!(columns[1].histogram.ndv, 1, "global b: one distinct value");
    assert_eq!(columns[0].histogram.ndv, 1000, "global a");
    assert_eq!(columns[2].histogram.ndv, 1000, "global c");
    assert_eq!(
        global.indexes.values().next().expect("idx_c").histogram.ndv,
        1000,
        "global idx_c"
    );
}

/// Go `analyze_test.go:1989::TestIssue55438`: a numeric column with a
/// CASE/TRIM generated-column definition and an index over the generated
/// column creates and analyzes without error (the regression panicked inside
/// the analyze column list).
#[test]
fn analyze_numeric_generated_column_with_index_succeeds() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "CREATE TABLE t0(c0 NUMERIC, c1 BIGINT UNSIGNED AS ((CASE 0 WHEN 0 THEN 1358571571 ELSE trim(c0) END)), INDEX i0(c1))",
    );
    let mut table = kv_table_of(&catalog, "t0");
    let statistics = analyze_kv_table(&mut table, &AnalyzeOptions::default(), None, &ctx())
        .unwrap_or_else(|error| panic!("analyze must not fail: {error:?}"));
    assert_eq!(statistics.columns.len(), 2, "c0 and the generated c1");
    assert_eq!(statistics.indexes.len(), 1, "i0 over the generated column");
}

/// Go `analyze_test.go:1998::TestIssue61609`: one sample drawn from ten
/// zero rows (`analyze table t with 1 topn, 1 samples`) — the TopN result
/// must SCALE the sampled count by the table (topn entry 0 counting 10).
#[test]
fn analyze_single_sample_scales_topn_count_to_the_table() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t (a int)");
    insert(
        &mut catalog,
        "insert into t values (0),(0),(0),(0),(0),(0),(0),(0),(0),(0)",
    );
    let mut table = kv_table_of(&catalog, "t");
    let options = AnalyzeOptions {
        num_topn: 1,
        num_samples: 1,
        ..AnalyzeOptions::default()
    };
    let statistics = analyze_kv_table(&mut table, &options, None, &ctx())
        .unwrap_or_else(|error| panic!("analyze failed: {error:?}"));
    let column = statistics.columns.values().next().expect("column a");
    let topn = column.topn.as_ref().expect("one topn kept");
    assert_eq!(topn.num(), 1, "with 1 topn");
    assert_eq!(
        topn.entries()[0].count,
        10,
        "1 sample of 10 rows scales to 10"
    );
}

/// Go `analyze_test.go:2154::TestIssue66918`: a JSON base column with a
/// STORED generated varchar column (`JSON_EXTRACT(j, '$.v')`) and a UNIQUE
/// index over it analyzes without panicking; the column and its index keep
/// v2 statistics (`0 2` / `1 2` in mysql.stats_histograms).
#[test]
fn analyze_stored_generated_column_from_json_with_unique_index() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "CREATE TABLE t (j JSON, g VARCHAR(255) GENERATED ALWAYS AS (json_extract(j, '$.v')) STORED, UNIQUE INDEX g_idx (g))",
    );
    insert(
        &mut catalog,
        "INSERT INTO t(j) VALUES ('{\"v\":1}'), ('{\"v\":2}')",
    );
    let mut table = kv_table_of(&catalog, "t");
    let statistics = analyze_kv_table(&mut table, &AnalyzeOptions::default(), None, &ctx())
        .unwrap_or_else(|error| panic!("analyze must not panic or fail: {error:?}"));
    assert_eq!(
        statistics.columns.len(),
        2,
        "j and the stored g are both analyzed"
    );
    assert_eq!(statistics.indexes.len(), 1, "g_idx is analyzed");
    for column in statistics.columns.values() {
        assert_eq!(column.stats_ver, 2, "v2 stats");
    }
    for index in statistics.indexes.values() {
        assert_eq!(index.stats_ver, 2, "v2 stats");
    }
    assert!(
        statistics.columns.contains_key(&2),
        "the stored generated column is analyzed"
    );
}

/// Go `analyze_bench_test.go:29::BenchmarkAnalyzePartition`: a 1000-partition
/// RANGE table of 100000 rows analyzed repeatedly.
///
/// No behavior to pin: it is a BENCHMARK (the assigned gate filters
/// `/bench/`), and the 1000-partition × 100k-row scale is out of this tier's
/// in-process scope.
#[test]
fn benchmark_analyze_partition_is_out_of_scope_scale() {
    // skipped-reason: benchmark + out-of-scope scale.
}
