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

//! Each helper renders exactly what the `DriverError` variant its raise sites
//! used to build renders: code, SQLSTATE, message, identity and the
//! evaluation-origin flag (`MysqlError`'s `PartialEq` covers all five).

use crate::{DriverError, ExecError, MysqlError};
use tidb_expr::EvalError;

fn s(text: &str) -> String {
    text.to_owned()
}

#[test]
fn evaluation_errors_render_as_the_driver_did() {
    for error in [
        EvalError::IntOverflow,
        EvalError::DivisionByZero,
        EvalError::WrongParameterCount("now"),
        EvalError::Unsupported("a window frame in RANGE units"),
        EvalError::Unsupported("Unknown charset 'x'"),
    ] {
        let label = format!("{error:?}");
        assert_eq!(
            DriverError::Exec(ExecError::Eval(error.clone())).to_mysql_error(),
            error.to_mysql_error(),
            "{label}"
        );
        assert_eq!(
            DriverError::from(ExecError::Eval(error.clone())).to_mysql_error(),
            error.to_mysql_error(),
            "{label}"
        );
    }
}

/// Go's table-layer write errors (`KvTableError::into_mysql_error`) render
/// what the write path's `kv_write_error` and `index_backfill_error` render
/// through `DriverError`, for every variant shape.
#[test]
fn table_write_errors_match_the_driver_rendering() {
    use tidb_kv::storage::StorageError;
    use tidb_table::kv_table::KvTableError;
    let cases: Vec<fn() -> KvTableError> = vec![
        || KvTableError::Storage(StorageError::Sql(MysqlError::unknown("sql"))),
        || KvTableError::Storage(StorageError::Retryable(s("region"))),
        || KvTableError::Storage(StorageError::NotFound),
        || KvTableError::Storage(StorageError::Backend(s("backend"))),
        || KvTableError::ColumnCast(MysqlError::unknown("cast")),
        || KvTableError::DuplicateEntry {
            value: s("1"),
            key: s("t.PRIMARY"),
        },
        || KvTableError::Generation {
            column: s("g"),
            detail: s("d"),
            eval: Some(EvalError::DivisionByZero),
        },
        || KvTableError::Generation {
            column: s("g"),
            detail: s("d"),
            eval: None,
        },
        || KvTableError::CheckConstraint {
            name: s("c"),
            detail: s("d"),
            eval: Some(EvalError::IntOverflow),
        },
        || KvTableError::CheckConstraint {
            name: s("c"),
            detail: s("d"),
            eval: None,
        },
        || KvTableError::PartitionEval(EvalError::DivisionByZero),
        || KvTableError::CheckConstraintViolated(s("c")),
        || KvTableError::NoPartitionForValue(s("7")),
        || KvTableError::RowDoesNotMatchGivenPartitionSet,
        || KvTableError::PartitionValueOverflowsBigint(s("18446744073709551615")),
        || KvTableError::AutoIdExhausted,
        || KvTableError::Encode(s("e")),
        || KvTableError::Decode(s("d")),
        || KvTableError::InvalidUseOfNull,
        || KvTableError::CacheTableUnsupported("Alter Table"),
    ];
    for case in cases {
        let label = format!("{:?}", case());
        assert_eq!(
            crate::driver::kv_write_error(case()).to_mysql_error(),
            case().into_mysql_error(),
            "{label}"
        );
        assert_eq!(
            crate::ddl::index_backfill_error(case()).to_mysql_error(),
            tidb_ddl_build::table_backfill::index_backfill_error(case()),
            "{label}"
        );
    }
    assert_eq!(
        crate::ddl::index_backfill_error(KvTableError::DuplicateKeyName(s("k"))).to_mysql_error(),
        tidb_ddl_build::table_backfill::index_backfill_error(KvTableError::DuplicateKeyName(s("k"))),
    );
}
