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

use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;
#[allow(unused_imports)]
use tidb_table::kv_table::row_decoder::*;

#[test]
fn legacy_generated_enum_matching_is_binary_on_new_collation_process() {
    check_generated_matching(tidb_datatype::FieldTypeCode::Enum, false, 2);
}

#[test]
fn legacy_generated_set_matching_is_binary_on_new_collation_process() {
    check_generated_matching(tidb_datatype::FieldTypeCode::Set, false, 2);
}

#[test]
fn new_collation_generated_enum_set_matching_remains_case_insensitive() {
    for code in [
        tidb_datatype::FieldTypeCode::Enum,
        tidb_datatype::FieldTypeCode::Set,
    ] {
        check_generated_matching(code, true, 1);
    }
}

fn check_generated_matching(code: tidb_datatype::FieldTypeCode, mode: bool, expected: u64) {
    assert!(new_collation_enabled());
    {
        let mut field_type = FieldType::new(code).with_collation_name("utf8mb4_general_ci");
        field_type.set_elems(vec!["A".into(), "a".into(), "B".into()]);
        let mut column = KvColumn {
            id: 1,
            name: "v".to_owned(),
            field_type,
            column_info_version: 1,
            comment: String::new(),
            generated: None,
            default_value: None,
            origin_default: None,
        };
        let tidb_ast::Stmt::Ddl(ddl) =
            tidb_parser::parse("create table t (v int as ('a'))").unwrap()
        else {
            panic!()
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
            panic!()
        };
        let expr = create.columns[0]
            .options
            .iter()
            .find_map(|option| match option {
                tidb_ast::ColumnOption::Generated { expression, .. } => Some(expression),
                _ => None,
            })
            .unwrap();
        column.generated = Some(
            tidb_ddl_build::generated_column::build_added_generated_column(
                "v",
                expr,
                false,
                &[],
                &[],
                &SessionTimeZone::utc(),
            )
            .unwrap(),
        );
        let decoder = RowDecoder::for_table_read(
            Arc::new(vec![column]),
            None,
            vec![],
            vec![],
            None,
            mode,
            RowDecodeContext::for_query(&crate::StmtContext::for_query()),
        )
        .unwrap();
        let mut row = DecodedRow {
            values: vec![Datum::Null],
            by_id: BTreeMap::new(),
        };
        decoder.eval_remaining(&mut row).unwrap();
        match &row.values[0] {
            Datum::Enum(value, collation) => {
                assert_eq!(value.value(), expected);
                assert_eq!(*collation, tidb_datatype::Collation::Utf8Mb4GeneralCi);
            }
            Datum::Set(value, collation) => {
                assert_eq!(value.value(), expected);
                assert_eq!(*collation, tidb_datatype::Collation::Utf8Mb4GeneralCi);
            }
            other => panic!("unexpected converted datum: {other:?}"),
        }
    }
}
