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

//! Go `pkg/ddl`'s reorganization diagnostics: `updateColumnWorker`'s
//! `reformatErrors` (`pkg/ddl/column.go`), for the column reorganization
//! ([`crate::table_backfill::KvTableBackfill::update_column_batch`]).

use tidb_datatype::Datum;

/// Go `updateColumnWorker.reformatErrors`: a truncation or too-long value
/// names the OLD column and the old value, as does an out-of-range one; other
/// errors keep their own text.
pub fn reformat_reorg_error(
    error: tidb_error::mysql::MysqlError,
    old_column: &str,
    old_value: &Datum,
) -> tidb_error::mysql::MysqlError {
    let value = || {
        old_value
            .sql_string()
            .unwrap_or_else(|_| format!("{old_value:?}"))
    };
    match error.code {
        tidb_error::tidb::errcode::WarnDataTruncated
        | tidb_error::tidb::errcode::ErrDataTooLong => tidb_error::mysql::MysqlError::terror(
            &tidb_error::catalog::types::ERR_TRUNCATED,
            format!(
                "Data truncated for column '{old_column}', value is '{}'",
                value()
            ),
        ),
        tidb_error::tidb::errcode::ErrWarnDataOutOfRange => tidb_error::mysql::MysqlError::terror(
            &tidb_error::catalog::types::ERR_WARN_DATA_OUT_OF_RANGE,
            format!(
                "Out of range value for column '{old_column}', the value is '{}'",
                value()
            ),
        ),
        _ => error,
    }
}
