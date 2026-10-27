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

//! Go `pkg/ddl/executor.go`'s session-interruption and cancellation
//! helpers, and its TiFlash replica admission.

use tidb_error::terror::TerrorError;
use tidb_util::dbterror::{self, exeerrors};
use tidb_util::sqlkiller::{KillSignal, SqlKiller};

/// Reports whether a DDL statement's session has been interrupted, together
/// with the Go-compatible kill flag.
///
/// This is Go `isSessionDone` (`pkg/ddl/executor.go:461`). The failpoint-only
/// `BatchAddTiFlashSendDone` override belongs to the unported batch wait loop;
/// this pure helper covers the SQL-killer contract shared by that loop and
/// other DDL callers.
#[must_use]
pub fn is_session_done(killer: &SqlKiller) -> (bool, u32) {
    let killed = killer
        .handle_signal()
        .is_some_and(|error| exeerrors::ERR_QUERY_INTERRUPTED.equal(Some(&error)));
    if killed {
        (true, KillSignal::QueryInterrupted.raw())
    } else {
        (false, 0)
    }
}

/// Converts the DDL wait loop's numeric kill flag into the canonical
/// interrupted-query error.
///
/// This is Go `convertKillFlag` (`pkg/ddl/executor.go:475`): zero is the
/// failpoint-only abort and therefore succeeds, while any non-zero flag is a
/// query interruption.
pub fn convert_kill_flag(killed: u32) -> Result<(), TerrorError> {
    if killed == 0 {
        return Ok(());
    }
    Err(exeerrors::ERR_QUERY_INTERRUPTED
        .generate_with_stack(exeerrors::ERR_QUERY_INTERRUPTED.message()))
}

/// Reports whether a failed system DDL cancellation should be retried.
///
/// This is Go `isRetryableDDLCancelErr` (`pkg/ddl/executor.go:7227`). The
/// three terminal cancellation errors are rejected through terror identity
/// equality, including wrapped errors; every other error remains retryable.
#[must_use]
pub fn is_retryable_ddl_cancel_err(error: &(dyn std::error::Error + 'static)) -> bool {
    !dbterror::ERR_CANCEL_FINISHED_DDL_JOB.equal(Some(error))
        && !dbterror::ERR_CANNOT_CANCEL_DDL_JOB.equal(Some(error))
        && !dbterror::ERR_DDL_JOB_NOT_FOUND.equal(Some(error))
}

/// Go `isTableTiFlashSupported`: memory and system tables, temporary tables,
/// views and sequences, and a table with a column in a charset TiFlash
/// lacks, cannot have a TiFlash replica.
pub fn is_table_tiflash_supported(
    db_name: &str,
    table: &tidb_model::table_info::TableInfo,
) -> Result<(), tidb_error::mysql::MysqlError> {
    use tidb_error::mysql::MysqlError;
    use tidb_hack::GoToLower;
    if tidb_metadef::db::is_mem_or_sys_db(&db_name.go_to_lower()) {
        return Err(MysqlError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_SYS_OR_MEM_TABLE
                .generate_with_stack_by_args(&[]),
        ));
    }
    if table.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(MysqlError::from_terror(
            &tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE
                .generate_with_stack_by_args(&["set TiFlash replica".into()]),
        ));
    }
    if table.is_view() || table.is_sequence() {
        return Err(MysqlError::from_terror(
            &tidb_util::dbterror::ERR_WRONG_OBJECT.generate_with_stack(format!(
                "'{db_name}.{}' is not BASE TABLE",
                table.name.original()
            )),
        ));
    }
    for column in table.cols().iter_deref() {
        let column = column.read();
        let charset = column.field_type.charset_name();
        if !tidb_datatype::TIFLASH_SUPPORTED_CHARSETS.contains(&charset) {
            return Err(MysqlError::from_terror(&tidb_util::dbterror::ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_UNSUPPORTED_CHARSET_TABLE.generate_with_stack_by_args(&[charset.into()])));
        }
    }
    Ok(())
}

/// Go `shouldModifyTiFlashReplica`: whether the request differs from the
/// table's replica.
#[must_use]
pub fn should_modify_tiflash_replica(
    current: Option<&tidb_model::table::TiFlashReplicaInfo>,
    count: u64,
    labels: &[String],
) -> bool {
    match current {
        Some(current)
            if current.count == count && current.location_labels.len() == labels.len() =>
        {
            current
                .location_labels
                .iter()
                .zip(labels)
                .any(|(current, wanted)| current != wanted)
        }
        _ => true,
    }
}
