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

//! Go `pkg/executor/brie_utils_test.go`'s restored DDL queries: the
//! `SHOW CREATE` text each restored object's job records.

use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_model::table::SequenceInfo;
use tidb_model::{ColumnInfo, GoShared, SchemaState, ViewInfo};

use super::*;

/// Go `mysql.PriKeyFlag | mysql.AutoIncrementFlag | mysql.NotNullFlag`.
const PRI_AUTO_NOT_NULL: u32 = (1 << 1) | (1 << 9) | 1;

fn vars() -> StmtContext {
    tidb_ddl_build::table_info_build::default_ddl_statement_context()
}

fn auto_increment_table() -> TableInfo {
    let mut field_type = FieldType::new(FieldTypeCode::Long);
    field_type.add_flags(PRI_AUTO_NOT_NULL);
    let mut column = ColumnInfo::new(1, "id", field_type);
    column.state = SchemaState::PUBLIC;
    TableInfo {
        id: 10,
        name: CiString::new("t"),
        charset: "utf8mb4".into(),
        collate: "utf8mb4_bin".into(),
        columns: vec![column].into(),
        pk_is_handle: true,
        auto_inc_id: 30001,
        state: SchemaState::PUBLIC,
        ..Default::default()
    }
}

/// Go passes no allocators, so a restored table's query has no
/// `AUTO_INCREMENT=` even when its counter has moved.
#[test]
fn restored_table_query_has_no_auto_increment() {
    let query =
        show_restored_create_table(&vars(), "test", &auto_increment_table(), "/*from(br)*/")
            .unwrap();
    assert!(
        query.starts_with("/*from(br)*/CREATE TABLE `t` ("),
        "{query}"
    );
    assert!(
        query.contains("AUTO_INCREMENT"),
        "the column keeps its attribute: {query}"
    );
    assert!(!query.contains("AUTO_INCREMENT="), "{query}");
}

/// A view and a sequence show their own `CREATE`, as Go's
/// `ConstructResultOfShowCreateTable` does.
#[test]
fn restored_view_and_sequence_queries() {
    let view = TableInfo {
        id: 11,
        name: CiString::new("v"),
        charset: "utf8mb4".into(),
        collate: "utf8mb4_bin".into(),
        state: SchemaState::PUBLIC,
        view: Some(GoShared::new(ViewInfo {
            select_stmt: "SELECT `id` FROM `test`.`t`".into(),
            ..Default::default()
        })),
        ..Default::default()
    };
    let query = show_restored_create_table(&vars(), "test", &view, "").unwrap();
    assert!(query.starts_with("CREATE ALGORITHM="), "{query}");
    assert!(
        query.contains("VIEW `v`") && query.ends_with("SELECT `id` FROM `test`.`t`"),
        "{query}"
    );

    let sequence = TableInfo {
        id: 12,
        name: CiString::new("s"),
        state: SchemaState::PUBLIC,
        sequence: Some(GoShared::new(SequenceInfo {
            start: 100,
            increment: 5,
            min_value: 1,
            max_value: 1000,
            cache: true,
            cache_value: 1000,
            ..Default::default()
        })),
        ..Default::default()
    };
    let query = show_restored_create_table(&vars(), "test", &sequence, "").unwrap();
    assert_eq!(query, "CREATE SEQUENCE `s` start with 100 minvalue 1 maxvalue 1000 increment by 5 cache 1000 nocycle ENGINE=InnoDB");
}

/// Go `showRestoredCreateDatabase`: `IF NOT EXISTS` with the charset.
#[test]
fn restored_database_query() {
    let db = DBInfo {
        id: 2,
        name: CiString::new("e07b"),
        charset: "utf8mb4".into(),
        collate: "utf8mb4_bin".into(),
        ..Default::default()
    };
    assert_eq!(
        show_restored_create_database(&vars(), &db, ""),
        "CREATE DATABASE IF NOT EXISTS `e07b` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"
    );
}

/// Go `mergeQuerys`.
#[test]
fn merged_batch_query() {
    assert_eq!(merge_queries(&["a".into(), "b".into()]), "a;b;");
}
