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

//! Parity of the lower layers' errors with the driver variants they
//! replaced: code, SQLSTATE, message, identity and the evaluation flag.

mod storage {
    #[allow(unused_imports)]
    use tidb_table::column::*;
    #[allow(unused_imports)]
    use tidb_table::kv_table::KvTableError;
    #[allow(unused_imports)]
    use tidb_kv::storage::StorageError;
    use crate::{DriverError, ExecError};
    use tidb_error::mysql::MysqlError;

    /// The SQL layer's rendering of each storage failure, pinned before the
    /// conversions left `tidb-dbterror`.
    #[test]
    fn storage_error_renders_as_before() {
        let sql = MysqlError::unknown("coprocessor said no");
        assert_eq!(
            DriverError::from(StorageError::Sql(sql.clone())).to_mysql_error(),
            sql
        );
        assert_eq!(
            DriverError::from(StorageError::Retryable("epoch not match".to_owned()))
                .to_mysql_error(),
            DriverError::from(tidb_error::catalog::tikv::ERR_REGION_UNAVAILABLE.generate_with_stack_by_args(&[])).to_mysql_error()
        );
        for (error, text) in [
            (StorageError::NotFound, "key not found"),
            (StorageError::InvalidIterator, "iterator is exhausted"),
            (StorageError::Backend("disk".to_owned()), "storage error: disk"),
        ] {
            assert_eq!(
                DriverError::from(error.clone()).to_mysql_error(),
                MysqlError::unknown(text)
            );
            let ExecError::Unsupported(message) = ExecError::from(error) else {
                panic!("{text}")
            };
            assert_eq!(message, text);
        }
        assert!(matches!(
            ExecError::from(StorageError::Sql(sql.clone())),
            ExecError::Mysql(error) if error == sql
        ));
        assert_eq!(
            StorageError::from(tidb_txnkv::MemStorageError::NotFound),
            StorageError::NotFound
        );
    }
}

mod kv_table {
    use crate::driver::error::*;
    #[allow(unused_imports)]
    use tidb_table::column::*;
    #[allow(unused_imports)]
    use tidb_table::kv_table::KvTableError;
    #[allow(unused_imports)]
    use tidb_kv::storage::StorageError;

    #[test]
    fn coprocessor_cot_error_matches_go() {
        // pkg/expression/builtin_math_test.go: TestCot.
        let storage = StorageError::Sql(MysqlError::terror(
            &tidb_error::catalog::types::ERR_OVERFLOW,
            "DOUBLE value is out of range in 'cot(0)'",
        ));
        let error = crate::DriverError::Exec(ExecError::from(KvTableError::from(storage)))
            .to_mysql_error();
        assert_eq!(error.code, 1690);
        assert_eq!(error.state, *b"22003");
        assert_eq!(error.message, "DOUBLE value is out of range in 'cot(0)'");
    }

    /// The catch-all arm formats the variant with `Debug`, so the variant
    /// name is part of the message.
    #[test]
    fn an_unmapped_table_error_keeps_its_variant_name_in_the_message() {
        let error = ExecError::from(KvTableError::DuplicateKeyName("k".to_owned()));
        let ExecError::Unsupported(message) = error else { panic!("{error:?}") };
        assert_eq!(message, "table bytes failed to decode: DuplicateKeyName(\"k\")");
    }
}

mod cast {
    use crate::driver::error::*;
    #[allow(unused_imports)]
    use tidb_table::column::*;
    #[allow(unused_imports)]
    use tidb_table::kv_table::KvTableError;
    #[allow(unused_imports)]
    use tidb_kv::storage::StorageError;
    use crate::DriverError;

    /// Each cast-family helper renders exactly what the `DriverError` variant
    /// the cast used to return renders: code, SQLSTATE, message, identity and
    /// the evaluation-origin flag.
    #[test]
    fn cast_family_helpers_match_the_driver_variants() {
        let column = || "c".to_owned();
        for (old, new) in [
            (
                DriverError::from(
                    tidb_util::dbterror::exeerrors::ERR_TRUNCATE_WRONG_INSERT_VALUE
                        .fast_generate_by_args(&[
                            "datetime".into(),
                            "x".into(),
                            column().into(),
                            2.into(),
                        ]),
                ),
                incorrect_temporal_value("datetime", "x", "c", 2),
            ),
            (
                DriverError::from(
                    tidb_error::catalog::types::ERR_WARN_DATA_OUT_OF_RANGE
                        .generate_with_stack_by_args(&[column().into(), 1.into()]),
                ),
                data_out_of_range("c", 1),
            ),
            (
                DriverError::from(
                    tidb_error::catalog::types::ERR_DATA_TOO_LONG
                        .generate_with_stack_by_args(&[column().into(), 1.into()]),
                ),
                data_too_long("c", 1),
            ),
            (
                DriverError::from(
                    tidb_error::catalog::types::ERR_TRUNCATED
                        .generate_with_stack_by_args(&[column().into(), 4.into()]),
                ),
                data_truncated_at_row("c", 4),
            ),
            (
                DriverError::from(
                    tidb_error::catalog::types::ERR_TRUNCATED_WRONG_VAL
                        .generate_with_stack_by_args(&["DOUBLE".into(), "abc".into()]),
                ),
                truncated_incorrect_value("DOUBLE", "abc"),
            ),
            (
                DriverError::from(
                    tidb_error::catalog::types::ERR_WRONG_VALUE
                        .generate_with_stack_by_args(&["date".into(), "x".into()]),
                ),
                incorrect_value_raw("date", "x"),
            ),
            (
                DriverError::from(
                    tidb_error::catalog::types::ERR_DATA_TOO_LONG.generate_with_stack(format!(
                        "Data Too Long, field len {}, data len {}",
                        3, 7
                    )),
                ),
                data_too_long_raw(3, 7),
            ),
            (
                DriverError::from(
                    tidb_error::catalog::types::ERR_TRUNCATED.generate_with_stack_by_args(&[]),
                ),
                data_truncated_unformatted(),
            ),
        ] {
            let old = old.to_mysql_error();
            assert_eq!(old, new);
            assert_eq!(old.identity, new.identity);
            assert_eq!(old.is_from_evaluation(), new.is_from_evaluation());
        }
    }

    /// A rendered cast error and a typed evaluation error reach the driver
    /// as they did when the cast returned `DriverError` itself.
    #[test]
    fn cast_error_reaches_the_driver_as_before() {
        let json = tidb_expr::EvalError::Json(tidb_expr::JsonError::InvalidCharset);
        let old = DriverError::Exec(crate::ExecError::Eval(json.clone()));
        let new = DriverError::from(CastError::Eval(json.clone()));
        assert!(matches!(
            &new,
            DriverError::Exec(crate::ExecError::Eval(tidb_expr::EvalError::Json(
                tidb_expr::JsonError::InvalidCharset
            )))
        ));
        assert_eq!(old.to_mysql_error(), new.to_mysql_error());
        assert_eq!(
            CastError::Eval(json.clone()).to_mysql_error(),
            DriverError::Exec(crate::ExecError::Eval(json)).to_mysql_error()
        );

        let mysql = data_too_long("c", 1);
        assert_eq!(
            DriverError::from(CastError::Column(mysql.clone())).to_mysql_error(),
            DriverError::from(
                tidb_error::catalog::types::ERR_DATA_TOO_LONG
                    .generate_with_stack_by_args(&["c".into(), 1.into()])
            )
            .to_mysql_error()
        );
        assert_eq!(CastError::Column(mysql.clone()).to_string(), mysql.message);
        assert_eq!(
            json_write_error(&tidb_datatype::DatumValueError::InvalidJsonCharset).map(|error| {
                DriverError::from(CastError::Eval(error)).to_mysql_error()
            }),
            Some(
                DriverError::Exec(crate::ExecError::Eval(tidb_expr::EvalError::Json(
                    tidb_expr::JsonError::InvalidCharset
                )))
                .to_mysql_error()
            )
        );
    }
}

mod autoinc {
    /// The allocator's 1467 renders as the driver variant it replaced.
    #[test]
    fn autoinc_read_failed_matches_the_driver_variant() {
        let old = crate::DriverError::from(
            tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED.generate_with_stack_by_args(&[]),
        )
        .to_mysql_error();
        let new = tidb_table::kv_table::autoinc_read_failed();
        assert_eq!(old, new);
        assert_eq!(old.identity, new.identity);
    }
}

mod kill_stop {
    use crate::{DriverError, ExecError};
    use tidb_kv::mem_quota::KillStop;

    fn rendered(error: ExecError) -> crate::MysqlError {
        DriverError::from(error).to_mysql_error()
    }

    /// The memory check's stop renders exactly as the `ExecError` variant it
    /// maps to, and as `KillStop::to_mysql_error` renders itself.
    #[test]
    fn kill_stop_maps_one_to_one_onto_exec_error() {
        assert_eq!(
            rendered(KillStop::MemoryExceedForQuery { conn_id: 7 }.into()),
            rendered(ExecError::MemoryExceedForQuery { conn_id: 7 })
        );
        assert_eq!(
            KillStop::MemoryExceedForQuery { conn_id: 7 }.to_mysql_error(),
            rendered(ExecError::MemoryExceedForQuery { conn_id: 7 })
        );
        let killed = tidb_util::dbterror::exeerrors::ERR_QUERY_INTERRUPTED.to_sql_error();
        assert_eq!(
            rendered(KillStop::Killed(killed.clone()).into()),
            rendered(ExecError::Killed(killed.clone()))
        );
        assert_eq!(
            KillStop::Killed(killed.clone()).to_mysql_error(),
            rendered(ExecError::Killed(killed))
        );
    }
}
