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

#![allow(missing_docs)]

use tidb_datatype::Collation;
use tidb_exec::{
    derive_tableless_select_columns, derive_tableless_select_result, AutomaticResultResponseError,
};

fn utf8() -> Collation {
    Collation::Utf8Mb4GeneralCi
}

#[test]
fn tableless_literals_and_aliases_produce_protocol_columns() {
    let result = derive_tableless_select_result("SELECT 1, 'hello' AS greeting", utf8(), "app")
        .expect("table-less select metadata");

    assert_eq!(result.resolved_fields.len(), 2);
    assert_eq!(result.adapted_fields[0].column_as_name.original, "1");
    assert_eq!(result.adapted_fields[1].column_as_name.original, "greeting");
    assert_eq!(result.columns[0].name, "1");
    assert_eq!(result.columns[1].name, "greeting");
    assert!(result.columns[1].org_name.is_empty());
}

#[test]
fn tableless_operator_and_function_metadata_is_derived() {
    let columns = derive_tableless_select_columns(
        "SELECT 1 + 2 AS total, CONCAT('a', 'b') AS text_value",
        utf8(),
        "",
    )
    .expect("operator/function metadata");

    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].name, "total");
    assert_eq!(columns[1].name, "text_value");
    assert_eq!(columns[0].type_code, 8); // MYSQL_TYPE_LONGLONG
    assert_eq!(columns[1].type_code, 253); // MYSQL_TYPE_VAR_STRING
    assert_eq!(columns[1].charset, 45); // utf8mb4_general_ci
}

#[test]
fn schema_dependent_and_unsupported_shapes_fail_explicitly() {
    let cases = [
        (
            "SELECT a FROM t",
            AutomaticResultResponseError::FromRequiresSchema,
        ),
        (
            "SELECT *",
            AutomaticResultResponseError::Resolve(
                tidb_exec::ResultFieldResolveError::WildcardRequiresSchema,
            ),
        ),
        (
            "SELECT 1 UNION SELECT 2",
            AutomaticResultResponseError::SetOperationRequiresSchema,
        ),
        ("TABLE t", AutomaticResultResponseError::NonPlainSelect),
    ];

    for (sql, expected) in cases {
        let error = derive_tableless_select_result(sql, utf8(), "").expect_err(sql);
        assert_eq!(error, expected, "source SQL: {sql}");
    }
}
