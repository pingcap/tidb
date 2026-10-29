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

//! Go-pinned identity of an error that crosses the storage layer.

use crate::*;
use tidb_kv::storage::StorageError;
use tidb_table::kv_table::KvTableError;

#[test]
fn coprocessor_cot_error_matches_go() {
    // pkg/expression/builtin_math_test.go: TestCot.
    let storage = StorageError::Sql(MysqlError::from_terror(
        &tidb_datatype::types_errors::ERR_OVERFLOW
            .generate_with_stack("DOUBLE value is out of range in 'cot(0)'"),
    ));
    let error = crate::DriverError::Exec(ExecError::from(KvTableError::from(storage)))
        .to_mysql_error();
    assert_eq!(error.code, 1690);
    assert_eq!(error.state, *b"22003");
    assert_eq!(error.message, "DOUBLE value is out of range in 'cot(0)'");
}
