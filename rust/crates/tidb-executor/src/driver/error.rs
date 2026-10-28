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

//! Driver failures: [`tidb_driver_error`] owns the types and their MySQL
//! wire rendering, below the executor; this module keeps the paths the
//! executor and its callers name them by.

pub use tidb_driver_error::*;

#[cfg(test)]
mod source_tests {
    #[test]
    fn raw_incorrect_string_value_matches_table_cast_value() {
        // pkg/table/column_test.go::TestCastValue expects the raw table error,
        // before a statement-level caller has attached a row number.
        let ctx = crate::StmtContext::for_dml(true, true, false);
        let field = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar)
            .with_flen(10)
            .with_collation_name("utf8mb4_bin");
        let error = tidb_table::column::cast_table_value(
            tidb_datatype::Datum::new_bytes([0x81]),
            &field,
            "",
            &ctx,
            false,
        )
        .expect_err("an invalid character group is refused")
        .to_mysql_error();

        assert_eq!(error.code, 1366);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(
            error.message,
            "Incorrect string value '\\x81' for column ''"
        );
    }
}
