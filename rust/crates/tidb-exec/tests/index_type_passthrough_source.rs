// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `CREATE TABLE` index USING clauses must reach the stored `TableInfo`
//! unchanged.
//!
//! Go `BuildIndexInfo` (pkg/ddl/index.go, `buildIndexInfo`/`BuildIndexInfo`
//! path invoked from `pkg/ddl/create_table.go`'s `BuildTableInfo`):
//!
//! ```go
//! if indexOption.Tp == ast.IndexTypeInvalid {
//!     // Use btree as default index type.
//!     idxInfo.Tp = ast.IndexTypeBtree
//! } else {
//!     idxInfo.Tp = indexOption.Tp
//! }
//! ```
//!
//! `ast.IndexTypeInvalid` is the only value Go special-cases; every other
//! declared type is copied through verbatim, including ones this node has
//! no special handling for (HYPO, HNSW, VECTOR, INVERTED, FULLTEXT).
//! `pkg/parser/ast/model.go`'s `IndexTypes` const block fixes the ordinals:
//! `Invalid=0, Btree=1, Hash=2, Rtree=3, Hypo=4, Vector=5, Inverted=6,
//! HNSW=7, Fulltext=8`.

use tidb_exec::table_info_build::{build_table_info, ClusteredIndexDefMode};
use tidb_model::table_info::TableInfo;

fn build(sql: &str) -> TableInfo {
    let statement = tidb_parser::parse(sql).expect("the fixture parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("the fixture is a CREATE TABLE");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("the fixture is a CREATE TABLE");
    };
    build_table_info(create, "utf8mb4", "utf8mb4_bin", ClusteredIndexDefMode::On)
        .expect("the fixture is a table this node can express")
}

/// `USING HYPO` is not BTREE/HASH/RTREE -- it must reach the stored
/// `TableInfo` as HYPO (ordinal 4, Go `ast.IndexTypeHypo`), not collapse to
/// BTREE the way a wildcard `_ => Btree` match would.
#[test]
fn using_hypo_stores_the_hypo_ordinal_not_btree() {
    let table = build("CREATE TABLE t (a INT, INDEX i(a) USING HYPO)");
    let index = table
        .indices
        .iter()
        .find(|idx| idx.name.lowercase() == "i")
        .expect("the fixture declares index i");
    assert_eq!(
        index.tp,
        tidb_ast::IndexType::HYPO,
        "USING HYPO must be stored verbatim, matching Go's non-Invalid passthrough"
    );
    assert_eq!(index.tp.0, 4, "HYPO's ordinal per pkg/parser/ast/model.go's IndexTypes block");
}

/// No `USING` clause at all: Go's `indexOption.Tp == ast.IndexTypeInvalid`
/// branch fires and stores BTREE.
#[test]
fn no_using_clause_defaults_to_btree() {
    let table = build("CREATE TABLE t (a INT, INDEX i(a))");
    let index = table
        .indices
        .iter()
        .find(|idx| idx.name.lowercase() == "i")
        .expect("the fixture declares index i");
    assert_eq!(
        index.tp,
        tidb_ast::IndexType::BTREE,
        "no USING clause must default to BTREE, matching Go's Invalid->Btree branch"
    );
}
