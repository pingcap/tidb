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

//! Go `pkg/ttl/sqlbuilder/sql_test.go` (`package sqlbuilder_test`).
//!
//! Every expected statement is the upstream Go string literal verbatim; Go and
//! Rust spell these escapes identically.
//!
//! Two adaptations:
//! - Go re-parses each generated statement with `parser.New()` and walks the
//!   resulting AST to confirm the escaping round-trips. This workspace's parser
//!   is a separate crate whose AST has no `TableName`/`ValueExpr` nodes to walk,
//!   so `test_escape` keeps the golden-string half, which is what pins the
//!   escaping.
//! - `TestFormatSQLDatum` drives `testkit` to round-trip every value through a
//!   live TiDB session. That oracle has no counterpart here, so the ported test
//!   asserts `format_sql_datum`'s output directly for the same field types.

use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeBuilder, FieldTypeCode, FieldTypeFlags};
use tidb_model::{ColumnInfo, PartitionDefinition, TableInfo};
use tidb_ttl::sql_builder::{
    build_delete_sql, format_sql_datum, PhysicalTable, ScanQueryGenerator, SqlBuilder,
};

/// Go's `d(vs ...any)` helper.
enum Val {
    Str(String),
    Int(i64),
    Bytes(Vec<u8>),
}

impl From<&str> for Val {
    fn from(value: &str) -> Self {
        Self::Str(value.to_string())
    }
}

impl From<i32> for Val {
    fn from(value: i32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<Vec<u8>> for Val {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

macro_rules! d {
    ($($value:expr),* $(,)?) => {
        // `Vec::from` rather than `vec![]`: at a borrowing call site clippy
        // reads the macro's `vec![]` as a needless allocation, but several
        // callers here do need an owned `Vec<Datum>`.
        Vec::from([$(match Val::from($value) {
            Val::Str(text) => Datum::new_string(text.into_bytes()),
            Val::Int(number) => Datum::new_int(number),
            Val::Bytes(bytes) => Datum::new_bytes(bytes),
        }),*])
    };
}

fn col(name: &str, field_type: FieldType) -> ColumnInfo {
    ColumnInfo {
        name: CiString::new(name),
        field_type,
        ..Default::default()
    }
}

fn table(name: &str) -> TableInfo {
    TableInfo {
        name: CiString::new(name),
        ..Default::default()
    }
}

fn datetime() -> FieldType {
    FieldType::new(FieldTypeCode::Datetime)
}

/// Go's `result(last, n)`: `n` rows of which only the last carries a key.
fn result(last: Vec<Datum>, n: usize) -> Vec<Vec<Datum>> {
    let mut rows = vec![Vec::new(); n];
    rows[n - 1] = last;
    rows
}

/// Go `TestEscape`.
#[test]
fn test_escape() {
    let tb = PhysicalTable {
        schema: CiString::new("testp;\"';123`456"),
        table_info: table("tp\"';123`456"),
        key_columns: vec![col(
            "col1\"';123`456",
            FieldType::new(FieldTypeCode::String),
        )],
        time_column: Some(col("time\"';123`456", datetime())),
        partition_def: Some(PartitionDefinition {
            name: CiString::new("p1\"';123`456"),
            ..Default::default()
        }),
        ..Default::default()
    };

    let build_select = |datums: &[Datum]| -> String {
        let mut b = SqlBuilder::new(&tb);
        b.write_select().unwrap();
        b.write_common_condition(&tb.key_columns, ">", datums)
            .unwrap();
        b.write_expire_condition(0).unwrap();
        b.build().unwrap()
    };

    let build_delete = |rows: &[Vec<Datum>]| -> String {
        let mut b = SqlBuilder::new(&tb);
        b.write_delete().unwrap();
        b.write_in_condition(&tb.key_columns, rows).unwrap();
        b.write_expire_condition(0).unwrap();
        b.build().unwrap()
    };

    assert_eq!(
        build_select(&d!["key1'\";123`456\t\n\r"]),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `col1\"';123``456` FROM `testp;\"';123``456`.`tp\"';123``456` PARTITION(`p1\"';123``456`) WHERE `col1\"';123``456` > 'key1\\'\\\";123`456\t\\n\\r' AND `time\"';123``456` < FROM_UNIXTIME(0)"
    );

    assert_eq!(
        build_delete(&[d!["key2'\";123`456\t\n\r"]]),
        "DELETE LOW_PRIORITY FROM `testp;\"';123``456`.`tp\"';123``456` PARTITION(`p1\"';123``456`) WHERE `col1\"';123``456` IN ('key2\\'\\\";123`456\t\\n\\r') AND `time\"';123``456` < FROM_UNIXTIME(0)"
    );

    assert_eq!(
        build_delete(&[d!["key3'\";123`456\t\n\r"], d!["key4'`\""]]),
        "DELETE LOW_PRIORITY FROM `testp;\"';123``456`.`tp\"';123``456` PARTITION(`p1\"';123``456`) WHERE `col1\"';123``456` IN ('key3\\'\\\";123`456\t\\n\\r', 'key4\\'`\\\"') AND `time\"';123``456` < FROM_UNIXTIME(0)"
    );
}

/// Go `TestSQLBuilder`.
#[test]
fn test_sql_builder() {
    let t1 = PhysicalTable {
        schema: CiString::new("test"),
        table_info: table("t1"),
        key_columns: vec![col("id", FieldType::new(FieldTypeCode::Varchar))],
        time_column: Some(col("time", datetime())),
        ..Default::default()
    };

    let t2 = PhysicalTable {
        schema: CiString::new("test2"),
        table_info: table("t2"),
        key_columns: vec![
            col("a", FieldType::new(FieldTypeCode::Varchar)),
            col("b", FieldType::new(FieldTypeCode::Int24)),
        ],
        time_column: Some(col("time", datetime())),
        ..Default::default()
    };

    let tp = PhysicalTable {
        schema: CiString::new("testp"),
        table_info: table("tp"),
        key_columns: t1.key_columns.clone(),
        time_column: t1.time_column.clone(),
        partition_def: Some(PartitionDefinition {
            name: CiString::new("p1"),
            ..Default::default()
        }),
        ..Default::default()
    };

    // test build select queries
    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1`"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_common_condition(&t1.key_columns, ">", &d!["a1"])
        .unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1'"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_common_condition(&t1.key_columns, ">", &d!["a1"])
        .unwrap();
    b.write_common_condition(&t1.key_columns, "<=", &d!["c3"])
        .unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1' AND `id` <= 'c3'"
    );

    // Go passes `time.UnixMilli(0).In(shLoc)`; only the Unix second is read,
    // so the zone drops out here as it does there.
    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_expire_condition(0).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0)"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_common_condition(&t1.key_columns, ">", &d!["a1"])
        .unwrap();
    b.write_common_condition(&t1.key_columns, "<=", &d!["c3"])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1' AND `id` <= 'c3' AND `time` < FROM_UNIXTIME(0)"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_order_by(&t1.key_columns, false).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` ORDER BY `id` ASC"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_order_by(&t1.key_columns, true).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` ORDER BY `id` DESC"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_order_by(&t1.key_columns, false).unwrap();
    b.write_limit(128).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` ORDER BY `id` ASC LIMIT 128"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_common_condition(&t1.key_columns, ">", &d!["';``~?%\"\n"])
        .unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > '\\';``~?%\\\"\\n'"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_select().unwrap();
    b.write_common_condition(&t1.key_columns, ">", &d!["a1';'"])
        .unwrap();
    b.write_common_condition(&t1.key_columns, "<=", &d!["a2\""])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    b.write_order_by(&t1.key_columns, false).unwrap();
    b.write_limit(128).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1\\';\\'' AND `id` <= 'a2\\\"' AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 128"
    );

    let mut b = SqlBuilder::new(&t2);
    b.write_select().unwrap();
    b.write_common_condition(&t2.key_columns, ">", &d!["x1", 20])
        .unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b` FROM `test2`.`t2` WHERE (`a`, `b`) > ('x1', 20)"
    );

    let mut b = SqlBuilder::new(&t2);
    b.write_select().unwrap();
    b.write_common_condition(&t2.key_columns, "<=", &d!["x2", 21])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    b.write_order_by(&t2.key_columns, false).unwrap();
    b.write_limit(100).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b` FROM `test2`.`t2` WHERE (`a`, `b`) <= ('x2', 21) AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b` ASC LIMIT 100"
    );

    let mut b = SqlBuilder::new(&t2);
    b.write_select().unwrap();
    b.write_common_condition(&t2.key_columns[0..1], "=", &d!["x3"])
        .unwrap();
    b.write_common_condition(&t2.key_columns[1..2], ">", &d![31])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    b.write_order_by(&t2.key_columns, false).unwrap();
    b.write_limit(100).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b` FROM `test2`.`t2` WHERE `a` = 'x3' AND `b` > 31 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b` ASC LIMIT 100"
    );

    // test build delete queries
    let mut b = SqlBuilder::new(&t1);
    b.write_delete().unwrap();
    assert_eq!(
        b.build().unwrap_err().to_string(),
        "expire condition not write"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_delete().unwrap();
    b.write_in_condition(&t1.key_columns, &[d!["a"]]).unwrap();
    b.write_expire_condition(0).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN ('a') AND `time` < FROM_UNIXTIME(0)"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_delete().unwrap();
    b.write_in_condition(&t1.key_columns, &[d!["a"], d!["b"]])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN ('a', 'b') AND `time` < FROM_UNIXTIME(0)"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_delete().unwrap();
    b.write_in_condition(&t2.key_columns, &[d!["a", 1]])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    b.write_limit(100).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1)) AND `time` < FROM_UNIXTIME(0) LIMIT 100"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_delete().unwrap();
    b.write_in_condition(&t2.key_columns, &[d!["a", 1], d!["b", 2]])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    b.write_limit(100).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1), ('b', 2)) AND `time` < FROM_UNIXTIME(0) LIMIT 100"
    );

    let mut b = SqlBuilder::new(&t1);
    b.write_delete().unwrap();
    b.write_in_condition(&t2.key_columns, &[d!["a", 1], d!["b", 2]])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1), ('b', 2)) AND `time` < FROM_UNIXTIME(0)"
    );

    // test select partition table
    let mut b = SqlBuilder::new(&tp);
    b.write_select().unwrap();
    b.write_common_condition(&tp.key_columns, ">", &d!["a1"])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `testp`.`tp` PARTITION(`p1`) WHERE `id` > 'a1' AND `time` < FROM_UNIXTIME(0)"
    );

    let mut b = SqlBuilder::new(&tp);
    b.write_delete().unwrap();
    b.write_in_condition(&tp.key_columns, &[d!["a"], d!["b"]])
        .unwrap();
    b.write_expire_condition(0).unwrap();
    assert_eq!(
        b.build().unwrap(),
        "DELETE LOW_PRIORITY FROM `testp`.`tp` PARTITION(`p1`) WHERE `id` IN ('a', 'b') AND `time` < FROM_UNIXTIME(0)"
    );
}

/// Go `TestScanQueryGenerator`.
#[test]
fn test_scan_query_generator() {
    let t1 = PhysicalTable {
        schema: CiString::new("test"),
        table_info: table("t1"),
        key_columns: vec![col("id", FieldType::new(FieldTypeCode::Int24))],
        time_column: Some(col("time", datetime())),
        ..Default::default()
    };

    let t2 = PhysicalTable {
        schema: CiString::new("test2"),
        table_info: table("t2"),
        key_columns: vec![
            col("a", FieldType::new(FieldTypeCode::Int24)),
            col("b", FieldType::new(FieldTypeCode::Varchar)),
            col(
                "c",
                FieldTypeBuilder::new()
                    .with_code(FieldTypeCode::String)
                    .flags_set(FieldTypeFlags::BINARY)
                    .build(),
            ),
        ],
        time_column: Some(col("time", datetime())),
        ..Default::default()
    };

    struct Step {
        result: Option<Vec<Vec<Datum>>>,
        limit: i64,
        sql: &'static str,
    }

    struct Case {
        tbl: PhysicalTable,
        range_start: Vec<Datum>,
        range_end: Vec<Datum>,
        path: Vec<Step>,
    }

    let cases = vec![
        Case {
            tbl: t1.clone(),
            range_start: Vec::new(),
            range_end: Vec::new(),
            path: vec![
                Step { result: None, limit: 3, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3" },
                Step { result: None, limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t1.clone(),
            range_start: Vec::new(),
            range_end: Vec::new(),
            path: vec![
                Step { result: None, limit: 3, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3" },
                Step { result: Some(Vec::new()), limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t1.clone(),
            range_start: d![1],
            range_end: d![100],
            path: vec![
                Step { result: None, limit: 3, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` >= 1 AND `id` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3" },
                Step { result: Some(result(d![10], 3)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 10 AND `id` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 5" },
                Step { result: Some(result(d![15], 4)), limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t1.clone(),
            range_start: Vec::new(),
            range_end: Vec::new(),
            path: vec![
                Step { result: None, limit: 3, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3" },
                Step { result: Some(result(d![2], 3)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 2 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 5" },
                Step { result: Some(result(d![4], 5)), limit: 6, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 4 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 6" },
                Step { result: Some(result(d![7], 5)), limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t2.clone(),
            range_start: Vec::new(),
            range_end: Vec::new(),
            path: vec![
                Step { result: None, limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: None, limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t2.clone(),
            range_start: Vec::new(),
            range_end: Vec::new(),
            path: vec![
                Step { result: None, limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: None, limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t2.clone(),
            range_start: Vec::new(),
            range_end: Vec::new(),
            path: vec![
                Step { result: None, limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(Vec::new()), limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t2.clone(),
            range_start: Vec::new(),
            range_end: Vec::new(),
            path: vec![
                Step { result: None, limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "x", vec![0xf0]], 4)), limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t2.clone(),
            range_start: d![1, "x", vec![0x0e]],
            range_end: d![100, "z", vec![0xff]],
            path: vec![
                Step { result: None, limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` >= x'0e' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "x", vec![0x1a]], 5)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'1a' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "x", vec![0x20]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'x' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "y", vec![0x0a]], 5)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'y' AND `c` > x'0a' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "y", vec![0x11]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'y' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "z", vec![0x02]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 1 AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![3, "a", vec![0x01]], 5)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 3 AND `b` = 'a' AND `c` > x'01' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![3, "a", vec![0x11]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 3 AND `b` > 'a' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![3, "c", vec![0x12]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 3 AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![5, "e", vec![0xa1]], 4)), limit: 5, sql: "" },
            ],
        },
        Case {
            tbl: t2.clone(),
            range_start: d![1],
            range_end: d![100],
            path: vec![
                Step { result: None, limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` >= 1 AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "x", vec![0x1a]], 5)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'1a' AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "x", vec![0x20]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'x' AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "y", vec![0x0a]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 1 AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
            ],
        },
        Case {
            tbl: t2.clone(),
            range_start: d![1, "x"],
            range_end: d![100, "z"],
            path: vec![
                Step { result: None, limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` >= 'x' AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "x", vec![0x1a]], 5)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'1a' AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "x", vec![0x20]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'x' AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
                Step { result: Some(result(d![1, "y", vec![0x0a]], 4)), limit: 5, sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 1 AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5" },
            ],
        },
    ];

    for (i, case) in cases.iter().enumerate() {
        let mut generator =
            ScanQueryGenerator::new(&case.tbl, 0, &case.range_start, &case.range_end).unwrap();
        for (j, step) in case.path.iter().enumerate() {
            let message = format!("{i}-{j}");
            let rows = step.result.clone().unwrap_or_default();
            let sql = generator.next_sql(&rows, step.limit).unwrap();
            assert_eq!(sql, step.sql, "{message}");
            assert_eq!(sql.is_empty(), generator.is_exhausted(), "{message}");
        }
    }
}

/// Go `TestBuildDeleteSQL`.
#[test]
fn test_build_delete_sql() {
    let t1 = PhysicalTable {
        schema: CiString::new("test"),
        table_info: table("t1"),
        key_columns: vec![col("id", FieldType::new(FieldTypeCode::Int24))],
        time_column: Some(col("time", datetime())),
        ..Default::default()
    };

    let t2 = PhysicalTable {
        schema: CiString::new("test2"),
        table_info: table("t2"),
        key_columns: vec![
            col("a", FieldType::new(FieldTypeCode::Int24)),
            col("b", FieldType::new(FieldTypeCode::Varchar)),
        ],
        time_column: Some(col("time", datetime())),
        ..Default::default()
    };

    let cases: Vec<(&PhysicalTable, Vec<Vec<Datum>>, &str)> = vec![
        (
            &t1,
            vec![d![1]],
            "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN (1) AND `time` < FROM_UNIXTIME(0) LIMIT 1",
        ),
        (
            &t1,
            vec![d![2], d![3], d![4]],
            "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN (2, 3, 4) AND `time` < FROM_UNIXTIME(0) LIMIT 3",
        ),
        (
            &t2,
            vec![d![1, "a"]],
            "DELETE LOW_PRIORITY FROM `test2`.`t2` WHERE (`a`, `b`) IN ((1, 'a')) AND `time` < FROM_UNIXTIME(0) LIMIT 1",
        ),
        (
            &t2,
            vec![d![1, "a"], d![2, "b"]],
            "DELETE LOW_PRIORITY FROM `test2`.`t2` WHERE (`a`, `b`) IN ((1, 'a'), (2, 'b')) AND `time` < FROM_UNIXTIME(0) LIMIT 2",
        ),
    ];

    for (tbl, rows, expected) in cases {
        assert_eq!(build_delete_sql(tbl, &rows, 0).unwrap(), expected);
    }
}

/// Go `TestFormatSQLDatum`, minus its live-session oracle.
///
/// Go inserts each value through `testkit`, reads it back as a `Datum`, formats
/// it, and re-queries with the literal. Without a session this asserts the
/// formatter's output directly over the same field types: the escaped-text
/// branch, the hex branch for binary-flagged and blob columns, and the
/// value-expression fallback for numbers.
#[test]
fn test_format_sql_datum() {
    // varchar / char: escaped text, never hex.
    assert_eq!(
        format_sql_datum(
            &Datum::new_string(b"aa';delete from t where 1;".to_vec()),
            &FieldType::new(FieldTypeCode::Varchar)
        )
        .unwrap(),
        "'aa\\';delete from t where 1;'"
    );
    assert_eq!(
        format_sql_datum(
            &Datum::new_string("\n123".as_bytes().to_vec()),
            &FieldType::new(FieldTypeCode::String)
        )
        .unwrap(),
        "'\\n123'"
    );
    assert_eq!(
        format_sql_datum(
            &Datum::new_string("你好👋".as_bytes().to_vec()),
            &FieldType::new(FieldTypeCode::Varchar)
        )
        .unwrap(),
        "'你好👋'"
    );

    // A binary-flagged string column takes the hex branch, so Go's test
    // asserts the `x'` prefix for it.
    let binary = FieldTypeBuilder::new()
        .with_code(FieldTypeCode::String)
        .flags_set(FieldTypeFlags::BINARY)
        .build();
    let formatted =
        format_sql_datum(&Datum::new_bytes(vec![0xf1, 0xf2, 0xf3, 0xf4]), &binary).unwrap();
    assert!(formatted.starts_with("x'"));
    assert_eq!(formatted, "x'f1f2f3f4'");
    assert_eq!(
        format_sql_datum(&Datum::new_bytes("你好👋".as_bytes().to_vec()), &binary).unwrap(),
        "x'e4bda0e5a5bdf09f918b'"
    );
    assert_eq!(
        format_sql_datum(&Datum::new_bytes(b"abcdef".to_vec()), &binary).unwrap(),
        "x'616263646566'"
    );

    // blob and bit likewise.
    assert_eq!(
        format_sql_datum(
            &Datum::new_bytes(vec![0xf1, 0xf2]),
            &FieldType::new(FieldTypeCode::Blob)
        )
        .unwrap(),
        "x'f1f2'"
    );
    assert_eq!(
        format_sql_datum(
            &Datum::new_bytes(vec![0x01]),
            &FieldType::new(FieldTypeCode::Bit)
        )
        .unwrap(),
        "x'01'"
    );

    // Numbers reach the value-expression fallback.
    assert_eq!(
        format_sql_datum(&Datum::new_int(-12), &FieldType::new(FieldTypeCode::Long)).unwrap(),
        "-12"
    );
    assert_eq!(
        format_sql_datum(&Datum::new_int(1), &FieldType::new(FieldTypeCode::Int24)).unwrap(),
        "1"
    );
}
