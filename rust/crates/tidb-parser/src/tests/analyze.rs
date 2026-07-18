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

//! `ANALYZE TABLE` parser/restore tests mapped from Go's `TestAnalyze`.

use super::*;

#[test]
fn analyze_table_and_index_targets_match_go() {
    for (sql, restored) in [
        ("analyze table t1", "ANALYZE TABLE `t1`"),
        ("analyze table t,t1", "ANALYZE TABLE `t`,`t1`"),
        ("analyze table t1 index", "ANALYZE TABLE `t1` INDEX"),
        (
            "analyze table t1 index a,b",
            "ANALYZE TABLE `t1` INDEX `a`,`b`",
        ),
        (
            "analyze table t0 index primary",
            "ANALYZE TABLE `t0` INDEX `primary`",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
    assert!(parse("analyze table t1.*").is_err());
}

#[test]
fn analyze_partitions_and_column_targets_match_go() {
    for (sql, restored) in [
        (
            "analyze table t partition p0,p1 index",
            "ANALYZE TABLE `t` PARTITION `p0`,`p1` INDEX",
        ),
        (
            "analyze table t partition p0 index PRIMARY",
            "ANALYZE TABLE `t` PARTITION `p0` INDEX `PRIMARY`",
        ),
        (
            "analyze table t1,t2 all columns",
            "ANALYZE TABLE `t1`,`t2` ALL COLUMNS",
        ),
        (
            "analyze table t partition p0 all columns",
            "ANALYZE TABLE `t` PARTITION `p0` ALL COLUMNS",
        ),
        (
            "analyze table t columns c1,c2",
            "ANALYZE TABLE `t` COLUMNS `c1`,`c2`",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
    assert!(parse("analyze table t columns t.c1,t.c2").is_err());
    assert!(parse("analyze table t index a columns c").is_err());
    assert!(parse("analyze table t index a all columns").is_err());
    assert!(parse("analyze table t index a predicate columns").is_err());
}

#[test]
fn analyze_topn_and_bucket_options_preserve_order() {
    for (sql, restored) in [
        (
            "analyze table t with 4 buckets",
            "ANALYZE TABLE `t` WITH 4 BUCKETS",
        ),
        (
            "analyze table t with 4 topn",
            "ANALYZE TABLE `t` WITH 4 TOPN",
        ),
        (
            "analyze table t all columns with 2 topn, 3 buckets",
            "ANALYZE TABLE `t` ALL COLUMNS WITH 2 TOPN, 3 BUCKETS",
        ),
        (
            "analyze table t partition p0 index i with 4 buckets",
            "ANALYZE TABLE `t` PARTITION `p0` INDEX `i` WITH 4 BUCKETS",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
}

#[test]
fn unrepresented_go_analyze_payloads_remain_explicit_gaps() {
    for sql in [
        "analyze no_write_to_binlog table t",
        "analyze local table t",
        "analyze table t update histogram on c1",
        "analyze table t drop histogram on c1",
        "analyze table t predicate columns",
        "analyze table t with 4 cmsketch width",
        "analyze table t with 4 cmsketch depth",
        "analyze table t with 4 samples",
        "analyze table t with 0.1 samplerate",
        "analyze table t with 0.05 ndvrate",
    ] {
        assert!(parse(sql).is_err(), "unrepresented payload accepted: {sql}");
    }
}
