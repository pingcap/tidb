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

//! Source-checked table-level `CONSTRAINT name` index routing.
//!
//! Go's `pkg/parser/ddl_index_parser.go::parseConstraint` consumes one
//! optional `CONSTRAINT name` prefix before switching over every index class.
//! It overwrites any inline index name with that constraint name before the
//! common `ast.Constraint` restore path runs. These rows were checked with
//! `godump restore`; keep them separate from the source-coverage ledger so
//! parser ownership remains one leaf per grammar concern.

use super::*;
use tidb_ast::{DdlStmt, IndexConstraintKind};

#[test]
fn named_table_index_constraints_use_go_constraint_name_for_every_class() {
    for (sql, expected, kind, expected_name) in [
        (
            "create table t (a int, constraint cn_primary primary key inline_primary (a))",
            "CREATE TABLE `t` (`a` INT,PRIMARY KEY `cn_primary`(`a`))",
            IndexConstraintKind::PrimaryKey,
            "cn_primary",
        ),
        (
            "create table t (a int, constraint cn_unique unique key inline_unique (a))",
            "CREATE TABLE `t` (`a` INT,UNIQUE `cn_unique`(`a`))",
            IndexConstraintKind::Unique,
            "cn_unique",
        ),
        (
            "create table t (a int, constraint cn_index index inline_index (a))",
            "CREATE TABLE `t` (`a` INT,INDEX `cn_index`(`a`))",
            IndexConstraintKind::Index,
            "cn_index",
        ),
        (
            "create table t (a int, constraint cn_key key inline_key (a))",
            "CREATE TABLE `t` (`a` INT,INDEX `cn_key`(`a`))",
            IndexConstraintKind::Index,
            "cn_key",
        ),
        (
            "create table t (a int, constraint cn_full fulltext key inline_full (a))",
            "CREATE TABLE `t` (`a` INT,FULLTEXT `cn_full`(`a`))",
            IndexConstraintKind::Fulltext,
            "cn_full",
        ),
        (
            "create table t (a int, constraint cn_vector vector index inline_vector (a))",
            "CREATE TABLE `t` (`a` INT,VECTOR INDEX `cn_vector`(`a`))",
            IndexConstraintKind::Vector,
            "cn_vector",
        ),
        (
            "create table t (a int, constraint cn_columnar columnar index inline_columnar (a))",
            "CREATE TABLE `t` (`a` INT,COLUMNAR INDEX `cn_columnar`(`a`))",
            IndexConstraintKind::Columnar,
            "cn_columnar",
        ),
    ] {
        assert_eq!(r(sql), expected, "restore: {sql}");

        let statement = parse(sql).expect("Go accepts the named table constraint");
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected DDL envelope for {sql}");
        };
        let DdlStmt::CreateTable(table) = ddl.as_ref() else {
            panic!("expected CREATE TABLE payload for {sql}");
        };
        let [TableConstraint::Index(index)] = table.table_constraints.as_slice() else {
            panic!("expected one index constraint for {sql}");
        };
        assert_eq!(index.kind, kind, "kind: {sql}");
        assert_eq!(index.name.as_deref(), Some(expected_name), "name: {sql}");
    }
}

#[test]
fn unnamed_constraint_prefix_leaves_inline_index_name_intact() {
    // Go's `ConstraintKeywordOpt` returns nil for this spelling. Without a
    // separate prefix name, the shared AST payload retains the inline name.
    assert_eq!(
        r("create table t (a int, constraint fulltext index inline_full (a))"),
        "CREATE TABLE `t` (`a` INT,FULLTEXT `inline_full`(`a`))"
    );
}
