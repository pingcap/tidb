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

//! A common-handle `mysql.*` row, as the system-table writers store it.
//!
//! The expected bytes are what a classic Go server stored for the same row of
//! a table declared like `mysql.tables_priv` under its next-gen layout
//! (`PRIMARY KEY (Host, DB, User, Table_name) CLUSTERED` over
//! `utf8mb4_general_ci` columns, `KEY i_user (User)`), read back through its
//! `/mvcc/key` and `/mvcc/index` status endpoints: the record key holds the
//! handle columns' SORT KEYS, the record value restores the original bytes of
//! the columns whose sort key loses them (`DB` and `Table_name`, not the
//! `CHAR` binary `Host` and `User`), and the secondary index entry carries
//! the handle in its key and the restored data in its value.

use tidb_datatype::Datum;
use tidb_cluster::system_row_write::{decode_stored_row, store_clustered_row, RowValues};
use tidb_ddl_build::table_info_build::build_table_info;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn table() -> tidb_model::TableInfo {
    let sql = "create table tp (Host char(255), DB char(64) collate utf8mb4_general_ci, \
               User char(32), Table_name char(64) collate utf8mb4_general_ci, Grantor char(77), \
               primary key (Host,DB,User,Table_name) clustered, key i_user(User))";
    let tidb_ast::Stmt::Ddl(ddl) = tidb_parser::parse(sql).unwrap() else {
        panic!("a DDL");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("a CREATE TABLE");
    };
    let mut info = build_table_info(create, "utf8mb4", "utf8mb4_bin")
        .expect("the table builds");
    info.id = 125;
    info
}

fn row(info: &tidb_model::TableInfo) -> RowValues {
    let mut values = RowValues::new();
    for column in info.cols().iter_deref() {
        let column = column.read();
        let text = match column.name.lowercase() {
            "host" => "%",
            "db" => "test",
            "user" => "gtu",
            "table_name" => "gtab",
            _ => "root@%",
        };
        values.insert(column.id, Datum::Bytes(text.as_bytes().to_vec()));
    }
    values
}

#[test]
fn a_common_handle_row_is_what_go_stores() {
    tidb_datatype::set_new_collation_enabled(true);
    let info = table();
    let mutations = store_clustered_row(&info, None, &row(&info)).expect("the row encodes");
    let stored: Vec<(String, String)> = mutations
        .iter()
        .map(|mutation| (hex(mutation.key()), hex(mutation.value())))
        .collect();
    // Go `addRecord` writes the index entries (`addIndices`) before the
    // record.
    assert_eq!(
        stored,
        [
            (
                "74800000000000007D5F698000000000000002016774750000000000FA\
                 012500000000000000F8010054004500530054FF0000000000000000F7\
                 016774750000000000FA010047005400410042FF0000000000000000F7"
                    .to_owned(),
                "007D018000020000000204040008007465737467746162".to_owned()
            ),
            (
                "74800000000000007D5F72012500000000000000F8010054004500530054FF0000000000000000F7\
                 016774750000000000FA010047005400410042FF0000000000000000F7"
                    .to_owned(),
                "800003000000020405040008000E007465737467746162726F6F744025".to_owned()
            ),
        ]
    );
}

/// Reading it back restores `DB` and `Table_name` from the value, and the
/// other handle columns from the key.
#[test]
fn a_common_handle_row_reads_back_whole() {
    tidb_datatype::set_new_collation_enabled(true);
    let info = table();
    let wanted = row(&info);
    let mutations = store_clustered_row(&info, None, &wanted).expect("the row encodes");
    let record_prefix = tidb_codec::gen_table_record_prefix(info.id);
    let record = mutations
        .iter()
        .find(|mutation| mutation.key().starts_with(&record_prefix))
        .expect("the record is written");
    let read = decode_stored_row(&info, record.key(), record.value(), None).expect("it decodes");
    let text = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => bytes.clone(),
        Datum::String(string) => string.bytes().to_vec(),
        other => panic!("not text: {other:?}"),
    };
    for (id, datum) in &wanted {
        assert_eq!(text(&read[id]), text(datum), "column {id}");
    }
}
