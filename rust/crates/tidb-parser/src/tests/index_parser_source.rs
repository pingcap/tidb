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

//! Direct index/FK vectors from Go `pkg/parser/parser_test.go`.
//!
//! Keep this separate from `index_source`: that file transcribes AST-owned
//! tests, while this one is the parser-test source leaf.  It intentionally
//! checks Go's ordinary parser restore, not `RestoreTiDBSpecialComment`.

use super::*;

type Case = (&'static str, bool, &'static str);

fn assert_cases(cases: &[Case]) {
    for (sql, accepted, expected) in cases {
        match (parse(sql), accepted) {
            (Ok(statement), true) => {
                assert_eq!(statement.restore(), *expected, "source SQL: {sql}")
            }
            (Err(_), false) => {}
            (Ok(statement), false) => {
                panic!(
                    "Go rejects but Rust accepted {sql}: {}",
                    statement.restore()
                )
            }
            (Err(error), true) => panic!("Go accepts but Rust rejected {sql}: {error:?}"),
        }
    }
}

fn assert_source_cases(cases: &[Case], expected_count: usize) {
    assert_eq!(cases.len(), expected_count, "source-row count changed");
    assert_cases(cases);
}

/// `pkg/parser/parser_test.go:3016-3025`: column-level REFERENCES accepts
/// every source reference shape and rejects a repeated referential action.
#[test]
fn go_parser_column_references_rows_match() {
    assert_source_cases(&[
        ("create table a (id int REFERENCES a (id) ON delete NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE NO ACTION)"),
        ("create table a (id int REFERENCES a (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"),
        ("create table a (id int REFERENCES a (id) ON delete set null on update CASCADE)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE SET NULL ON UPDATE CASCADE)"),
        ("create table a (id int REFERENCES a (id) ON update set default on delete RESTRICT)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE RESTRICT ON UPDATE SET DEFAULT)"),
        ("create table a (id int REFERENCES a (id) MATCH FULL ON delete NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH FULL ON DELETE NO ACTION)"),
        ("create table a (id int REFERENCES a (id) MATCH PARTIAL ON update NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH PARTIAL ON UPDATE NO ACTION)"),
        ("create table a (id int REFERENCES a (id) MATCH SIMPLE ON update NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH SIMPLE ON UPDATE NO ACTION)"),
        ("create table a (id int REFERENCES a (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"),
        ("create table a (id int REFERENCES a (id) ON update set default on update CURRENT_TIMESTAMP)", false, ""),
        ("create table a (id int REFERENCES a (id) ON delete set default on update CURRENT_TIMESTAMP)", false, ""),
    ], 10);
}

/// Go `parseReferenceDef` consumes a bare MATCH token and leaves the
/// following reference actions intact.  This is a source-fidelity regression:
/// the clause is ignored when no FULL/PARTIAL/SIMPLE mode follows it.
#[test]
fn go_parser_bare_match_reference_clause_is_ignored() {
    assert_source_cases(
        &[
            (
                "create table a (id int REFERENCES a (id) MATCH)",
                true,
                "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`))",
            ),
            (
                "create table a (id int REFERENCES a (id) MATCH ON update NO ACTION)",
                true,
                "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE NO ACTION)",
            ),
        ],
        2,
    );
}

/// `pkg/parser/parser_test.go:3288-3310`: ALTER index options, including
/// PRE_SPLIT, GLOBAL/LOCAL, and canonical INDEX/UNIQUE restore.
#[test]
fn go_parser_alter_index_options_rows_match() {
    assert_source_cases(&[
        ("ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = 4", true, "ALTER TABLE `t` ADD INDEX(`a`) PRE_SPLIT_REGIONS = 4"),
        ("ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS 4", true, "ALTER TABLE `t` ADD INDEX(`a`) PRE_SPLIT_REGIONS = 4"),
        ("ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = 'a'", false, ""),
        ("ALTER TABLE t ADD PRIMARY KEY (a) CLUSTERED PRE_SPLIT_REGIONS = 4", true, "ALTER TABLE `t` ADD PRIMARY KEY(`a`) CLUSTERED PRE_SPLIT_REGIONS = 4"),
        ("ALTER TABLE t ADD PRIMARY KEY (a) PRE_SPLIT_REGIONS = 4 NONCLUSTERED", true, "ALTER TABLE `t` ADD PRIMARY KEY(`a`) NONCLUSTERED PRE_SPLIT_REGIONS = 4"),
        ("ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = (between (1, 'a') and (2, 'b') regions 4);", true, "ALTER TABLE `t` ADD INDEX(`a`) PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4)"),
        ("ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = (by (1, 'a'), (2, 'b'), (3, 'c'));", true, "ALTER TABLE `t` ADD INDEX(`a`) PRE_SPLIT_REGIONS = (BY (1,_UTF8MB4'a'),(2,_UTF8MB4'b'),(3,_UTF8MB4'c'))"),
        ("ALTER TABLE t ADD INDEX (a) comment 'a' PRE_SPLIT_REGIONS = (between (1, 'a') and (2, 'b') regions 4);", true, "ALTER TABLE `t` ADD INDEX(`a`) COMMENT 'a' PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4)"),
        ("CREATE INDEX idx ON t (a, b) pre_split_regions = 100", true, "CREATE INDEX `idx` ON `t` (`a`, `b`) PRE_SPLIT_REGIONS = 100"),
        ("CREATE INDEX idx ON t (a, b) PRE_SPLIT_REGIONS = (between (1, 'a') and (2, 'b') regions 4);", true, "CREATE INDEX `idx` ON `t` (`a`, `b`) PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4)"),
        ("ALTER TABLE t ADD INDEX idx(a) pre_split_regions = 100, ADD INDEX idx2(b) pre_split_regions = (by(1),(2),(3))", true, "ALTER TABLE `t` ADD INDEX `idx`(`a`) PRE_SPLIT_REGIONS = 100, ADD INDEX `idx2`(`b`) PRE_SPLIT_REGIONS = (BY (1),(2),(3))"),
        ("ALTER TABLE t ADD KEY (a) USING HASH COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX(`a`) USING HASH COMMENT 'a'"),
        ("ALTER TABLE t ADD INDEX (a) USING BTREE /*T![global_index] GLOBAL */ COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX(`a`) USING BTREE COMMENT 'a' GLOBAL"),
        ("ALTER TABLE t ADD UNIQUE INDEX (a) /*T![global_index] GLOBAL */", true, "ALTER TABLE `t` ADD UNIQUE(`a`) GLOBAL"),
        ("ALTER TABLE t ADD UNIQUE INDEX (a) LOCAL", true, "ALTER TABLE `t` ADD UNIQUE(`a`)"),
        ("ALTER TABLE t ADD KEY IF NOT EXISTS (a) USING HASH COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX IF NOT EXISTS(`a`) USING HASH COMMENT 'a'"),
        ("ALTER TABLE t ADD PRIMARY KEY ident USING RTREE ( a DESC , b   )", true, "ALTER TABLE `t` ADD PRIMARY KEY `ident`(`a` DESC, `b`) USING RTREE"),
        ("ALTER TABLE t ADD KEY USING RTREE   ( a ) ", true, "ALTER TABLE `t` ADD INDEX(`a`) USING RTREE"),
        ("ALTER TABLE t ADD KEY USING RTREE ( ident ASC , ident ( 123 ) )", true, "ALTER TABLE `t` ADD INDEX(`ident`, `ident`(123)) USING RTREE"),
        ("ALTER TABLE t ADD PRIMARY KEY (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD PRIMARY KEY(`a`) COMMENT 'a'"),
        ("ALTER TABLE t ADD UNIQUE (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"),
        ("ALTER TABLE t ADD UNIQUE KEY (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"),
        ("ALTER TABLE t ADD UNIQUE INDEX (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"),
    ], 23);
}

/// `pkg/parser/parser_test.go:3311-3334`: vector and columnar ALTER INDEX
/// grammar, including the rejected shorthand spellings.
#[test]
fn go_parser_alter_vector_and_columnar_index_rows_match() {
    assert_source_cases(&[
        ("ALTER TABLE t ADD VECTOR (a) USING HNSW COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD VECTOR (VEC_COSINE_DISTANCE(a)) USING HNSW COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD VECTOR ((VEC_COSINE_DISTANCE(a))) USING HASH COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD VECTOR ((VEC_COSINE_DISTANCE(a, b))) USING HNSW COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD VECTOR KEY ((VEC_COSINE_DISTANCE(a, b))) USING HNSW COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD VECTOR INDEX ((VEC_COSINE_DISTANCE(a, b))) USING HNSW COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX((VEC_COSINE_DISTANCE(`a`, `b`))) USING HNSW COMMENT 'a'"),
        ("ALTER TABLE t ADD VECTOR INDEX ((lower(a))) USING HNSW COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX((LOWER(`a`))) USING HNSW COMMENT 'a'"),
        ("ALTER TABLE t ADD VECTOR INDEX ((VEC_COSINE_DISTANCE(a), a)) USING HNSW COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD VECTOR INDEX (a, (VEC_COSINE_DISTANCE(a))) USING HNSW COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX(`a`, (VEC_COSINE_DISTANCE(`a`))) USING HNSW COMMENT 'a'"),
        ("ALTER TABLE t ADD VECTOR INDEX ((VEC_COSINE_DISTANCE(a))) USING HYPO COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX((VEC_COSINE_DISTANCE(`a`))) USING HYPO COMMENT 'a'"),
        ("ALTER TABLE t ADD VECTOR INDEX ((VEC_COSINE_DISTANCE(a))) COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX((VEC_COSINE_DISTANCE(`a`))) COMMENT 'a'"),
        ("ALTER TABLE t ADD VECTOR INDEX ((VEC_COSINE_DISTANCE(a))) USING HNSW COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX((VEC_COSINE_DISTANCE(`a`))) USING HNSW COMMENT 'a'"),
        ("ALTER TABLE t ADD VECTOR INDEX IF NOT EXISTS ((VEC_COSINE_DISTANCE(a))) USING HNSW COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX IF NOT EXISTS((VEC_COSINE_DISTANCE(`a`))) USING HNSW COMMENT 'a'"),
        ("ALTER TABLE t ADD VECTOR INDEX IF NOT EXISTS ((VEC_COSINE_DISTANCE(a))) ADD_COLUMNAR_REPLICA_ON_DEMAND USING HNSW COMMENT 'a'", true, "ALTER TABLE `t` ADD VECTOR INDEX IF NOT EXISTS((VEC_COSINE_DISTANCE(`a`))) ADD_COLUMNAR_REPLICA_ON_DEMAND USING HNSW COMMENT 'a'"),
        ("ALTER TABLE t ADD COLUMNAR (a) USING INVERTED COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD COLUMNAR ((a - 1)) USING INVERTED COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD COLUMNAR (a) USING HASH COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD COLUMNAR (a, b) USING INVERTED COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD COLUMNAR KEY (a, b) USING INVERTED COMMENT 'a'", false, ""),
        ("ALTER TABLE t ADD COLUMNAR INDEX (a, b) USING INVERTED COMMENT 'a'", true, "ALTER TABLE `t` ADD COLUMNAR INDEX(`a`, `b`) USING INVERTED COMMENT 'a'"),
        ("ALTER TABLE t ADD COLUMNAR INDEX (a) USING INVERTED COMMENT 'a'", true, "ALTER TABLE `t` ADD COLUMNAR INDEX(`a`) USING INVERTED COMMENT 'a'"),
        ("ALTER TABLE t ADD COLUMNAR INDEX (a) USING HYPO COMMENT 'a'", true, "ALTER TABLE `t` ADD COLUMNAR INDEX(`a`) USING HYPO COMMENT 'a'"),
        ("ALTER TABLE t ADD COLUMNAR INDEX ((a - 1)) USING HYPO COMMENT 'a'", true, "ALTER TABLE `t` ADD COLUMNAR INDEX((`a`-1)) USING HYPO COMMENT 'a'"),
        ("ALTER TABLE t ADD COLUMNAR INDEX IF NOT EXISTS (a) USING INVERTED COMMENT 'a'", true, "ALTER TABLE `t` ADD COLUMNAR INDEX IF NOT EXISTS(`a`) USING INVERTED COMMENT 'a'"),
    ], 24);
}

/// `pkg/parser/parser_test.go:3335-3336,3675-3680`: ALTER foreign-key and
/// column-reference forms whose target column list is optional.
#[test]
fn go_parser_alter_foreign_key_and_reference_rows_match() {
    assert_source_cases(&[
        ("ALTER TABLE t ADD CONSTRAINT fk_t2_id FOREIGN KEY (t2_id) REFERENCES t(id)", true, "ALTER TABLE `t` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY (`t2_id`) REFERENCES `t`(`id`)"),
        ("ALTER TABLE t ADD CONSTRAINT fk_t2_id FOREIGN KEY IF NOT EXISTS (t2_id) REFERENCES t(id)", true, "ALTER TABLE `t` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY IF NOT EXISTS (`t2_id`) REFERENCES `t`(`id`)"),
        ("alter table t add column a double (4,2) zerofill references b match full on update set null first", true, "ALTER TABLE `t` ADD COLUMN `a` DOUBLE(4,2) UNSIGNED ZEROFILL REFERENCES `b` MATCH FULL ON UPDATE SET NULL FIRST"),
        ("alter table d_n.t_n add constraint foreign key ident (ident(1)) references d_n.t_n match full on delete set null", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`(1)) REFERENCES `d_n`.`t_n` MATCH FULL ON DELETE SET NULL"),
        ("alter table t_n add constraint ident foreign key (ident,ident(1)) references t_n match full on update set null on delete restrict", true, "ALTER TABLE `t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`, `ident`(1)) REFERENCES `t_n` MATCH FULL ON DELETE RESTRICT ON UPDATE SET NULL"),
        ("alter table d_n.t_n add foreign key ident (ident, ident(1) asc) references t_n match partial on delete cascade remove partitioning", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`, `ident`(1)) REFERENCES `t_n` MATCH PARTIAL ON DELETE CASCADE REMOVE PARTITIONING"),
        ("alter table d_n.t_n add constraint foreign key (ident asc) references d_n.t_n match simple on update cascade on delete cascade", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT FOREIGN KEY (`ident`) REFERENCES `d_n`.`t_n` MATCH SIMPLE ON DELETE CASCADE ON UPDATE CASCADE"),
    ], 7);
}
