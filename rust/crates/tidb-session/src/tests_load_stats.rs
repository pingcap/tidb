#![cfg(test)]

//! `LOAD STATS 'file.json'` end to end: a dump written by hand, the
//! histograms it installs, and the `EXPLAIN` numbers they change.
//!
//! The point of asserting EST-ROWS rather than only the `stats:pseudo`
//! marker: a loader that decoded the bounds wrongly (kept an INT bound as the
//! ASCII bytes `"10"`, forgot the TopN sort, mis-read a cumulative bucket
//! count as a per-bucket one) would still make the marker disappear. The
//! numbers below are hand-derived from Go's own arithmetic
//! (`pkg/statistics/histogram.go` `equalRowCount`/`BetweenRowCount`,
//! `pkg/planner/cardinality`), so they only come out right if the loaded
//! histogram IS the dumped one.
//!
//! The dump format is Go's `statsutil.JSONTable`
//! (`pkg/statistics/util/json_objects.go`), loaded by
//! `storage.TableStatsFromJSON` (`pkg/statistics/handle/storage/json.go`);
//! see `tidb_executor::load_stats` for the byte-level contract.

use crate::tests_support::*;
use crate::*;

/// Writes one dump to a scratch file and returns the `LOAD STATS` statement
/// text for it. The file lives under the OS scratch directory: the engine
/// takes the path exactly as written (Go reads the file through the CLIENT's
/// working directory, so relative paths belong to harnesses, not the engine).
fn load_stats_sql(name: &str, json: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "tidb_rust_load_stats_{}_{name}.json",
        std::process::id()
    ));
    std::fs::write(&path, json).expect("write stats fixture");
    format!("LOAD STATS '{}'", path.display())
}

/// The scan row's `estRows` and `operator info` (`tests_analyze::scan_row`'s
/// twin, kept local so the two suites stay independently readable).
fn scan_row(session: &mut Session, sql: &str) -> (String, String) {
    let rows = row_text(session.run(sql));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("Scan") || row[0].contains("Get"))
        .unwrap_or_else(|| panic!("no scan operator in the plan of `{sql}`: {rows:?}"));
    (scan[1].clone(), scan[4].clone())
}

/// A version-1 dump (`stats_ver: 1`, counters-only era): two buckets over an
/// INT column and the SAME two buckets in index-key form, exactly the shape
/// of the pre-2021 fixtures under `tests/integrationtest/s/`.
///
/// The column bounds are the STRING FORM of the values (`dumpJSONCol` runs
/// `ConvertTo(TypeBlob)` before marshalling, so `10` dumps as the two ASCII
/// bytes `"10"`, base64 `MTA=`), while the index bounds are `codec.EncodeKey`
/// bytes (`0x03` int flag + sign-flipped big-endian, so `10` is
/// `A4AAAAAAAAAK`). Loading must treat the two encodings differently -- a
/// loader that key-decoded the column bounds or string-parsed the index
/// bounds gets every estimate below wrong.
///
/// Distribution: bucket 0 holds values 1..10 (cumulative count 10, upper
/// bound repeats once), bucket 1 holds 11..20 (cumulative count 30, upper
/// bound repeats 3 times); NDV 20, 30 rows, no NULLs.
const V1_DUMP: &str = r#"{
  "database_name": "test",
  "table_name": "t",
  "count": 30,
  "modify_count": 0,
  "columns": {
    "a": {
      "histogram": {
        "ndv": 20,
        "buckets": [
          {"count": 10, "lower_bound": "MQ==", "upper_bound": "MTA=", "repeats": 1},
          {"count": 30, "lower_bound": "MTE=", "upper_bound": "MjA=", "repeats": 3}
        ]
      },
      "null_count": 0,
      "tot_col_size": 30,
      "correlation": 1,
      "stats_ver": 1
    }
  },
  "indices": {
    "ia": {
      "histogram": {
        "ndv": 20,
        "buckets": [
          {"count": 10, "lower_bound": "A4AAAAAAAAAB", "upper_bound": "A4AAAAAAAAAK", "repeats": 1},
          {"count": 30, "lower_bound": "A4AAAAAAAAAL", "upper_bound": "A4AAAAAAAAAU", "repeats": 3}
        ]
      },
      "null_count": 0,
      "stats_ver": 1
    }
  }
}"#;

/// Loading a v1 dump installs the histogram, and the estimates come out of
/// ITS buckets through the ordinary estimator paths:
///
/// * the full scan reads the dump's `count` (30), not the pseudo 10000;
/// * `a = 10` hits bucket 0's upper bound, whose `repeats` is 1 -> 1.00
///   (Go `Histogram.equalRowCount`'s `matchLastValue` arm);
/// * `a = 15` falls INSIDE bucket 1 without matching its upper bound, and a
///   v1 column with no CMSketch answers `NotNullCount()/NDV` = 30/20 -> 1.50
///   (the same function's uniform fallback);
/// * neither plan says `stats:pseudo` any more.
///
/// 1.00 vs 1.50 is the load-bearing pair: both queries are equalities on the
/// same column, so ONLY the bucket geometry -- which value is an upper bound,
/// what its repeat count is -- can tell them apart. A dump loaded with the
/// bounds left as ASCII bytes would order `"15"` between `"10"` and `"11"`
/// lexically and answer the wrong arm.
#[test]
fn v1_dump_est_rows_come_from_the_loaded_buckets() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY ia(a))").unwrap();
    assert_eq!(
        scan_row(&mut session, "EXPLAIN SELECT * FROM t"),
        (
            "10000.00".to_owned(),
            "keep order:false, stats:pseudo".to_owned()
        )
    );

    session
        .run(&load_stats_sql("v1_two_buckets", V1_DUMP))
        .unwrap();

    assert_eq!(
        scan_row(&mut session, "EXPLAIN SELECT * FROM t"),
        ("30.00".to_owned(), "keep order:false".to_owned())
    );
    let (est, info) = scan_row(&mut session, "EXPLAIN SELECT * FROM t WHERE a = 10");
    assert!(
        !info.contains("stats:pseudo"),
        "the loaded statistics must replace the pseudo ones: {info}"
    );
    assert_eq!(est, "1.00", "a = 10 is bucket 0's upper bound, repeats 1");
    let (est, _) = scan_row(&mut session, "EXPLAIN SELECT * FROM t WHERE a = 15");
    assert_eq!(
        est, "1.50",
        "a = 15 is inside bucket 1: v1 without a CMSketch answers NotNullCount/NDV = 30/20"
    );
}

/// The dumped INDEX histogram estimates through the index path: a covering
/// read of `a = 10` scans the index, and its `estRows` must come from the
/// index histogram's key-encoded bucket bounds (upper bound of bucket 0,
/// `repeats` 1), not from the column histogram that happens to agree.
#[test]
fn v1_dump_feeds_the_index_histogram_too() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY ia(a))").unwrap();
    session
        .run(&load_stats_sql("v1_index_path", V1_DUMP))
        .unwrap();
    let rows = row_text(session.run("EXPLAIN SELECT a FROM t WHERE a = 10"));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("a covering equality should scan index ia: {rows:?}"));
    assert_eq!(scan[1], "1.00");
    assert!(!scan[4].contains("stats:pseudo"), "{}", scan[4]);
}

/// A version-2 dump: no counter rows, the TopN riding inside `cm_sketch` as
/// Go's `CMSketchToProto` writes it. The TopN entry's `data` is
/// `codec.EncodeKey` of the value (`0x03` + sign-flipped `7`), the encoding
/// `TopNFromProto` round-trips untouched and the estimator queries with.
///
/// `a = 7` must answer EXACTLY 7.00 -- a TopN hit is a count, not an
/// interpolation (Go `equalRowCount`'s `QueryTopN` arm under stats version
/// 2). If the loader forgot `topN.Sort()` (Go sorts inside `TopNFromProto`
/// because dumps store pruning order), the binary search misses the entry
/// and the estimate collapses to the uniform rate instead.
#[test]
fn v2_dump_topn_answers_exact_counts() {
    let mut session = Session::new();
    session.run("CREATE TABLE t2 (a INT)").unwrap();
    // Deliberately UNSORTED entries: 7 (encoded A4AAAAAAAAAH) after 20
    // (A4AAAAAAAAAU) would break an unsorted binary search.
    let dump = r#"{
      "database_name": "test",
      "table_name": "t2",
      "count": 10,
      "modify_count": 0,
      "columns": {
        "a": {
          "histogram": {"ndv": 3},
          "cm_sketch": {
            "top_n": [
              {"data": "A4AAAAAAAAAU", "count": 2},
              {"data": "A4AAAAAAAAAH", "count": 7},
              {"data": "A4AAAAAAAAAB", "count": 1}
            ]
          },
          "null_count": 0,
          "tot_col_size": 10,
          "stats_ver": 2
        }
      },
      "indices": {}
    }"#;
    session.run(&load_stats_sql("v2_topn", dump)).unwrap();
    // `t2` has no index, so the plan is TableReader over Selection over a
    // full scan: the scan row carries the table's 10 rows and the FILTERED
    // estimate is the tree's top -- the same split `tests_analyze` captures
    // from Go, where `WHERE a > 2` shows 10.00 on the scan and 7.00 above it.
    let (est, info) = scan_row(&mut session, "EXPLAIN SELECT * FROM t2 WHERE a = 7");
    assert!(!info.contains("stats:pseudo"), "{info}");
    assert_eq!(est, "10.00", "the full scan reads the dump's count");
    let rows = row_text(session.run("EXPLAIN SELECT * FROM t2 WHERE a = 7"));
    assert_eq!(
        rows[0][1], "7.00",
        "a TopN hit is an exact count under version 2: {rows:?}"
    );
}

/// Go `LoadStatsInfo.Update`: a file holding JSON `null` unmarshals to the
/// zero `JSONTable`, and the `TableName == "" && Version == 0` guard makes
/// the statement a successful no-op -- nothing installed, no error.
#[test]
fn a_null_stats_file_is_a_successful_no_op() {
    let mut session = Session::new();
    session.run("CREATE TABLE t3 (a INT)").unwrap();
    session.run(&load_stats_sql("null_file", "null")).unwrap();
    let (_, info) = scan_row(&mut session, "EXPLAIN SELECT * FROM t3 WHERE a > 1");
    assert!(
        info.contains("stats:pseudo"),
        "a null dump must not have installed anything: {info}"
    );
}

/// The dump names its own target: `LoadStatsFromJSONNoUpdate` resolves
/// `database_name`.`table_name` in the schema, NOT the session's current
/// database. `explain_complex.test` depends on this (`use test` before
/// loading a dump that says `"database_name": "test"`), and a dump naming a
/// table that does not exist is Go's 1146.
#[test]
fn the_dump_names_its_own_database_and_table() {
    let mut session = Session::new();
    session.run("CREATE DATABASE other_db").unwrap();
    session.run("CREATE TABLE other_db.t4 (a INT)").unwrap();
    // The session sits in `test`; the dump installs into `other_db.t4`.
    let dump = r#"{
      "database_name": "other_db",
      "table_name": "t4",
      "count": 30,
      "columns": {
        "a": {
          "histogram": {
            "ndv": 20,
            "buckets": [
              {"count": 30, "lower_bound": "MQ==", "upper_bound": "MjA=", "repeats": 3}
            ]
          },
          "stats_ver": 1
        }
      }
    }"#;
    session.run(&load_stats_sql("other_db", dump)).unwrap();
    let (est, info) = scan_row(&mut session, "EXPLAIN SELECT * FROM other_db.t4");
    assert_eq!(est, "30.00");
    assert!(!info.contains("stats:pseudo"), "{info}");

    let missing =
        r#"{"database_name": "test", "table_name": "no_such_table", "count": 1, "version": 5}"#;
    let error = session
        .run(&load_stats_sql("missing_table", missing))
        .unwrap_err();
    let text = format!("{error:?}");
    assert!(
        text.contains("no_such_table"),
        "the refusal names the dump's own target: {text}"
    );
}
