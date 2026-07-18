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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Standalone and shared index-parser regression tests.

use super::*;

#[test]
fn create_index_kinds_and_types_match_go() {
    for (sql, expected) in [
        (
            "create unique index if not exists i type hypo on t (a(3), (b+1) desc)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `i` ON `t` (`a`(3), (`b`+1) DESC) USING HYPO",
        ),
        (
            "create unique index ident on d_n.t_n (ident, ident asc) type rtree",
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING RTREE",
        ),
        (
            "create spatial index if not exists i on t(a)",
            "CREATE SPATIAL INDEX IF NOT EXISTS `i` ON `t` (`a`)",
        ),
        (
            "create fulltext index i on t(a)",
            "CREATE FULLTEXT INDEX `i` ON `t` (`a`)",
        ),
        (
            "create vector index i using hnsw on t ((vec_cosine_distance(a)))",
            "CREATE VECTOR INDEX `i` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "create columnar index i on t(a)",
            "CREATE COLUMNAR INDEX `i` ON `t` (`a`)",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn create_index_options_merge_repeat_and_restore_like_go() {
    for (sql, expected) in [
        (
            "create fulltext index i on t(a) with parser p comment 'x'",
            "CREATE FULLTEXT INDEX `i` ON `t` (`a`) WITH PARSER `p` COMMENT 'x'",
        ),
        (
            "create index i using btree on t(a) using hash comment 'foo'",
            "CREATE INDEX `i` ON `t` (`a`) USING HASH COMMENT 'foo'",
        ),
        (
            "create index i type inverted on t(a)",
            "CREATE INDEX `i` ON `t` (`a`) USING INVERTED",
        ),
        (
            "create index i on t(a) key_block_size = 32 using hash comment 'hello'",
            "CREATE INDEX `i` ON `t` (`a`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'",
        ),
        (
            "create index i on t(a) invisible visible global local",
            "CREATE INDEX `i` ON `t` (`a`) VISIBLE",
        ),
        (
            "create index i on t(a) clustered nonclustered add_columnar_replica_on_demand",
            "CREATE INDEX `i` ON `t` (`a`) ADD_COLUMNAR_REPLICA_ON_DEMAND NONCLUSTERED",
        ),
        (
            "create index i on t(a) secondary_engine_attribute = '{\"engine\":\"x\"}'",
            "CREATE INDEX `i` ON `t` (`a`) SECONDARY_ENGINE_ATTRIBUTE = '{\"engine\":\"x\"}'",
        ),
        (
            "create index i on t(a) where a > 1",
            "CREATE INDEX `i` ON `t` (`a`) WHERE `a`>1",
        ),
        (
            "create index i on t(a, b) pre_split_regions = 100",
            "CREATE INDEX `i` ON `t` (`a`, `b`) PRE_SPLIT_REGIONS = 100",
        ),
        (
            "create index i on t(a) key_block_size=16 using inverted with parser p comment 'x' invisible clustered global pre_split_regions=2 secondary_engine_attribute='{}' add_columnar_replica_on_demand where a > 1",
            "CREATE INDEX `i` ON `t` (`a`) ADD_COLUMNAR_REPLICA_ON_DEMAND CLUSTERED KEY_BLOCK_SIZE=16 USING INVERTED WITH PARSER `p` COMMENT 'x' GLOBAL INVISIBLE PRE_SPLIT_REGIONS = 2 SECONDARY_ENGINE_ATTRIBUTE = '{}' WHERE `a`>1",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn create_index_online_ddl_matches_go() {
    for (sql, expected) in [
        (
            "create index i on t(a) algorithm = default lock = default",
            "CREATE INDEX `i` ON `t` (`a`)",
        ),
        (
            "create index i on t(a) algorithm inplace lock exclusive",
            "CREATE INDEX `i` ON `t` (`a`) ALGORITHM = INPLACE LOCK = EXCLUSIVE",
        ),
        (
            "create index i on t(a) lock none algorithm copy",
            "CREATE INDEX `i` ON `t` (`a`) ALGORITHM = COPY LOCK = NONE",
        ),
        (
            "create index i on t(a) algorithm instant",
            "CREATE INDEX `i` ON `t` (`a`) ALGORITHM = INSTANT",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

/// Direct rows from `pkg/parser/parser_test.go`'s CREATE INDEX block.  Keep
/// this list source-shaped: it catches parser/restore regressions that a
/// hand-selected feature test could accidentally skip.
#[test]
fn go_parser_create_index_rows_match() {
    for (sql, expected) in [
        ("create index idx on t (a)", "CREATE INDEX `idx` ON `t` (`a`)"),
        (
            "create index if not exists idx on t (a)",
            "CREATE INDEX IF NOT EXISTS `idx` ON `t` (`a`)",
        ),
        (
            "create unique index idx on t (a)",
            "CREATE UNIQUE INDEX `idx` ON `t` (`a`)",
        ),
        (
            "create unique index if not exists idx on t (a)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `idx` ON `t` (`a`)",
        ),
        (
            "create unique index ident on d_n.t_n ( ident , ident asc ) type btree",
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE",
        ),
        (
            "create unique index ident on d_n.t_n ( ident , ident asc ) type hash",
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING HASH",
        ),
        (
            "create unique index ident on d_n.t_n ( ident , ident asc ) type rtree",
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING RTREE",
        ),
        (
            "create unique index ident type btree on d_n.t_n ( ident , ident asc )",
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE",
        ),
        (
            "create unique index ident using btree on d_n.t_n ( ident , ident asc )",
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE",
        ),
        (
            "create fulltext index if not exists idx on t (a)",
            "CREATE FULLTEXT INDEX IF NOT EXISTS `idx` ON `t` (`a`)",
        ),
        (
            "create fulltext index idx on t (a) with parser ident comment 'string' lock default",
            "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident` COMMENT 'string'",
        ),
        (
            "create index idx on t (a) lock=none",
            "CREATE INDEX `idx` ON `t` (`a`) LOCK = NONE",
        ),
        (
            "create index idx using btree on t (a) using hash comment 'foo'",
            "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'",
        ),
        (
            "create index idx on t (a) invisible visible",
            "CREATE INDEX `idx` ON `t` (`a`) VISIBLE",
        ),
        (
            "create vector index idx on t (a, b) using hnsw",
            "CREATE VECTOR INDEX `idx` ON `t` (`a`, `b`) USING HNSW",
        ),
        (
            "create vector index idx on t ((vec_cosine_distance(a)), a) using hnsw",
            "CREATE VECTOR INDEX `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`)), `a`) USING HNSW",
        ),
        (
            "create vector index if not exists idx on t ((vec_cosine_distance(a))) type hnsw",
            "CREATE VECTOR INDEX IF NOT EXISTS `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "create vector index ident type hnsw on d_n.t_n ((vec_cosine_distance(a)))",
            "CREATE VECTOR INDEX `ident` ON `d_n`.`t_n` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "create index idx on t (a) algorithm copy",
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = COPY",
        ),
        (
            "create index idx on t (a) lock exclusive algorithm inplace",
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE LOCK = EXCLUSIVE",
        ),
        (
            "create index idx on t (a) algorithm default lock default",
            "CREATE INDEX `idx` ON `t` (`a`)",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn create_index_rejects_invalid_source_forms() {
    assert_eq!(
        r("create index i on t(a) pre_split_regions = (between (1, 'a') and (2, 'b') regions 4)"),
        "CREATE INDEX `i` ON `t` (`a`) PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4)"
    );
    for sql in [
        "create vector i on t((vec_cosine_distance(a))) using hnsw",
        "create vector key i on t((vec_cosine_distance(a))) using hnsw",
        "create index i on t(a) algorithm = ident",
        "create index i on t(a) lock = invalid",
        "create index i on t(a) secondary_engine_attribute",
        "create index i on t(a) key_block_size = 'x'",
        "create index i on t(a) using vector",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
