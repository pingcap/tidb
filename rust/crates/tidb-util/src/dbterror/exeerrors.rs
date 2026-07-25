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

//! Complete transcreation of Go `pkg/util/dbterror/exeerrors` (`errors.go`):
//! the executor error table (82 prototypes across the Executor, Privilege,
//! DDL, and Table classes).
//!
//! GENERATED mechanically from the Go source and verified entry-by-entry —
//! code, RFC identity, and message template — against
//! `exeerrors_go_fixture.txt`, a dump produced by executing the REAL Go
//! package, exactly like the sibling DDL table. `NewStdErr` entries carry
//! `Message(text, nil)` composed literals with no redaction positions.

use std::sync::LazyLock;

use tidb_error::terror::TerrorError;
use tidb_error::tidb::errcode;

use super::{CLASS_DDL, CLASS_EXECUTOR, CLASS_PRIVILEGE, CLASS_TABLE};

/// Source `exeerrors.ErrGetStartTS`.
pub static ERR_GET_START_TS: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrGetStartTS));
/// Source `exeerrors.ErrUnknownPlan`.
pub static ERR_UNKNOWN_PLAN: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrUnknownPlan));
/// Source `exeerrors.ErrPrepareMulti`.
pub static ERR_PREPARE_MULTI: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPrepareMulti));
/// Source `exeerrors.ErrPrepareDDL`.
pub static ERR_PREPARE_DDL: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPrepareDDL));
/// Source `exeerrors.ErrResultIsEmpty`.
pub static ERR_RESULT_IS_EMPTY: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrResultIsEmpty));
/// Source `exeerrors.ErrBuildExecutor`.
pub static ERR_BUILD_EXECUTOR: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBuildExecutor));
/// Source `exeerrors.ErrBatchInsertFail`.
pub static ERR_BATCH_INSERT_FAIL: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBatchInsertFail));
/// Source `exeerrors.ErrUnsupportedPs`.
pub static ERR_UNSUPPORTED_PS: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrUnsupportedPs));
/// Source `exeerrors.ErrSubqueryMoreThan1Row`.
pub static ERR_SUBQUERY_MORE_THAN1_ROW: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrSubqueryNo1Row));
/// Source `exeerrors.ErrIllegalGrantForTable`.
pub static ERR_ILLEGAL_GRANT_FOR_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrIllegalGrantForTable));
/// Source `exeerrors.ErrColumnsNotMatched`.
pub static ERR_COLUMNS_NOT_MATCHED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrColumnNotMatched));
/// Source `exeerrors.ErrCantCreateUserWithGrant`.
pub static ERR_CANT_CREATE_USER_WITH_GRANT: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrCantCreateUserWithGrant));
/// Source `exeerrors.ErrPasswordNoMatch`.
pub static ERR_PASSWORD_NO_MATCH: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPasswordNoMatch));
/// Source `exeerrors.ErrCannotUser`.
pub static ERR_CANNOT_USER: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrCannotUser));
/// Source `exeerrors.ErrGrantRole`.
pub static ERR_GRANT_ROLE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrGrantRole));
/// Source `exeerrors.ErrPasswordFormat`.
pub static ERR_PASSWORD_FORMAT: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPasswordFormat));
/// Source `exeerrors.ErrCantChangeTxCharacteristics`.
pub static ERR_CANT_CHANGE_TX_CHARACTERISTICS: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrCantChangeTxCharacteristics));
/// Source `exeerrors.ErrPsManyParam`.
pub static ERR_PS_MANY_PARAM: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPsManyParam));
/// Source `exeerrors.ErrAdminCheckTable`.
pub static ERR_ADMIN_CHECK_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrAdminCheckTable));
/// Source `exeerrors.ErrDBaccessDenied`.
pub static ERR_D_BACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrDBaccessDenied));
/// Source `exeerrors.ErrTableaccessDenied`.
pub static ERR_TABLEACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrTableaccessDenied));
/// Source `exeerrors.ErrBadDB`.
pub static ERR_BAD_DB: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBadDB));
/// Source `exeerrors.ErrWrongObject`.
pub static ERR_WRONG_OBJECT: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrWrongObject));
/// Source `exeerrors.ErrWrongUsage`.
pub static ERR_WRONG_USAGE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrWrongUsage));
/// Source `exeerrors.ErrRoleNotGranted`.
pub static ERR_ROLE_NOT_GRANTED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_PRIVILEGE.new_std(errcode::ErrRoleNotGranted));
/// Source `exeerrors.ErrDeadlock`.
pub static ERR_DEADLOCK: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLockDeadlock));
/// Source `exeerrors.ErrQueryInterrupted`.
pub static ERR_QUERY_INTERRUPTED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrQueryInterrupted));
/// Source `exeerrors.ErrMaxExecTimeExceeded`.
pub static ERR_MAX_EXEC_TIME_EXCEEDED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrMaxExecTimeExceeded));
/// Source `exeerrors.ErrResourceGroupQueryRunawayInterrupted`.
pub static ERR_RESOURCE_GROUP_QUERY_RUNAWAY_INTERRUPTED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrResourceGroupQueryRunawayInterrupted));
/// Source `exeerrors.ErrQueryExecStopped`.
pub static ERR_QUERY_EXEC_STOPPED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrQueryExecStopped));
/// Source `exeerrors.ErrResourceGroupQueryRunawayQuarantine`.
pub static ERR_RESOURCE_GROUP_QUERY_RUNAWAY_QUARANTINE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrResourceGroupQueryRunawayQuarantine));
/// Source `exeerrors.ErrDynamicPrivilegeNotRegistered`.
pub static ERR_DYNAMIC_PRIVILEGE_NOT_REGISTERED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrDynamicPrivilegeNotRegistered));
/// Source `exeerrors.ErrIllegalPrivilegeLevel`.
pub static ERR_ILLEGAL_PRIVILEGE_LEVEL: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrIllegalPrivilegeLevel));
/// Source `exeerrors.ErrInvalidSplitRegionRanges`.
pub static ERR_INVALID_SPLIT_REGION_RANGES: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrInvalidSplitRegionRanges));
/// Source `exeerrors.ErrViewInvalid`.
pub static ERR_VIEW_INVALID: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrViewInvalid));
/// Source `exeerrors.ErrInstanceScope`.
pub static ERR_INSTANCE_SCOPE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrInstanceScope));
/// Source `exeerrors.ErrSettingNoopVariable`.
pub static ERR_SETTING_NOOP_VARIABLE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrSettingNoopVariable));
/// Source `exeerrors.ErrLazyUniquenessCheckFailure`.
pub static ERR_LAZY_UNIQUENESS_CHECK_FAILURE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLazyUniquenessCheckFailure));
/// Source `exeerrors.ErrMemoryExceedForQuery`.
pub static ERR_MEMORY_EXCEED_FOR_QUERY: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrMemoryExceedForQuery));
/// Source `exeerrors.ErrMemoryExceedForInstance`.
pub static ERR_MEMORY_EXCEED_FOR_INSTANCE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrMemoryExceedForInstance));
/// Source `exeerrors.ErrDeleteNotFoundColumn`.
pub static ERR_DELETE_NOT_FOUND_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrDeleteNotFoundColumn));
/// Source `exeerrors.ErrBRIEBackupFailed`.
pub static ERR_BRIE_BACKUP_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBRIEBackupFailed));
/// Source `exeerrors.ErrBRIERestoreFailed`.
pub static ERR_BRIE_RESTORE_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBRIERestoreFailed));
/// Source `exeerrors.ErrBRIEImportFailed`.
pub static ERR_BRIE_IMPORT_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBRIEImportFailed));
/// Source `exeerrors.ErrBRIEExportFailed`.
pub static ERR_BRIE_EXPORT_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBRIEExportFailed));
/// Source `exeerrors.ErrBRJobNotFound`.
pub static ERR_BR_JOB_NOT_FOUND: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrBRJobNotFound));
/// Source `exeerrors.ErrCTEMaxRecursionDepth`.
pub static ERR_CTE_MAX_RECURSION_DEPTH: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrCTEMaxRecursionDepth));
/// Source `exeerrors.ErrPluginIsNotLoaded`.
pub static ERR_PLUGIN_IS_NOT_LOADED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPluginIsNotLoaded));
/// Source `exeerrors.ErrSetPasswordAuthPlugin`.
pub static ERR_SET_PASSWORD_AUTH_PLUGIN: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrSetPasswordAuthPlugin));
/// Source `exeerrors.ErrFuncNotEnabled`.
pub static ERR_FUNC_NOT_ENABLED: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_EXECUTOR.new_plain_err(errcode::ErrNotSupportedYet, "%-.32s is not supported. To enable this experimental feature, set '%-.32s' in the configuration file.")
});
/// Source `exeerrors.ErrSavepointNotExists`.
pub static ERR_SAVEPOINT_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrSpDoesNotExist));
/// Source `exeerrors.ErrForeignKeyCascadeDepthExceeded`.
pub static ERR_FOREIGN_KEY_CASCADE_DEPTH_EXCEEDED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrForeignKeyCascadeDepthExceeded));
/// Source `exeerrors.ErrPasswordExpireAnonymousUser`.
pub static ERR_PASSWORD_EXPIRE_ANONYMOUS_USER: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPasswordExpireAnonymousUser));
/// Source `exeerrors.ErrMustChangePassword`.
pub static ERR_MUST_CHANGE_PASSWORD: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrMustChangePassword));
/// Source `exeerrors.ErrSecondPasswordCannotBeEmpty`.
pub static ERR_SECOND_PASSWORD_CANNOT_BE_EMPTY: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrSecondPasswordCannotBeEmpty));
/// Source `exeerrors.ErrPasswordCannotBeRetainedOnPluginChange`.
pub static ERR_PASSWORD_CANNOT_BE_RETAINED_ON_PLUGIN_CHANGE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrPasswordCannotBeRetainedOnPluginChange));
/// Source `exeerrors.ErrCurrentPasswordCannotBeRetained`.
pub static ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrCurrentPasswordCannotBeRetained));
/// Source `exeerrors.ErrWrongStringLength`.
pub static ERR_WRONG_STRING_LENGTH: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_DDL.new_std(errcode::ErrWrongStringLength));
/// Source `exeerrors.ErrUnsupportedFlashbackTmpTable`.
pub static ERR_UNSUPPORTED_FLASHBACK_TMP_TABLE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Recover/flashback table is not supported on temporary tables",
    )
});
/// Source `exeerrors.ErrTruncateWrongInsertValue`.
pub static ERR_TRUNCATE_WRONG_INSERT_VALUE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_TABLE.new_plain_err(
        errcode::ErrTruncatedWrongValue,
        "Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d",
    )
});
/// Source `exeerrors.ErrExistsInHistoryPassword`.
pub static ERR_EXISTS_IN_HISTORY_PASSWORD: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrExistsInHistoryPassword));
/// Source `exeerrors.ErrUserNameNeedPrefix`.
pub static ERR_USER_NAME_NEED_PREFIX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUsername,
        "User name must start with `%s.` (use `%s.%s` instead)",
    )
});
/// Source `exeerrors.ErrWarnTooFewRecords`.
pub static ERR_WARN_TOO_FEW_RECORDS: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrWarnTooFewRecords));
/// Source `exeerrors.ErrWarnTooManyRecords`.
pub static ERR_WARN_TOO_MANY_RECORDS: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrWarnTooManyRecords));
/// Source `exeerrors.ErrLoadDataFromServerDisk`.
pub static ERR_LOAD_DATA_FROM_SERVER_DISK: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataFromServerDisk));
/// Source `exeerrors.ErrLoadParquetFromLocal`.
pub static ERR_LOAD_PARQUET_FROM_LOCAL: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadParquetFromLocal));
/// Source `exeerrors.ErrLoadDataEmptyPath`.
pub static ERR_LOAD_DATA_EMPTY_PATH: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataEmptyPath));
/// Source `exeerrors.ErrLoadDataUnsupportedFormat`.
pub static ERR_LOAD_DATA_UNSUPPORTED_FORMAT: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataUnsupportedFormat));
/// Source `exeerrors.ErrLoadDataInvalidURI`.
pub static ERR_LOAD_DATA_INVALID_URI: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataInvalidURI));
/// Source `exeerrors.ErrLoadDataCantAccess`.
pub static ERR_LOAD_DATA_CANT_ACCESS: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataCantAccess));
/// Source `exeerrors.ErrLoadDataCantRead`.
pub static ERR_LOAD_DATA_CANT_READ: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataCantRead));
/// Source `exeerrors.ErrLoadDataWrongFormatConfig`.
pub static ERR_LOAD_DATA_WRONG_FORMAT_CONFIG: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataWrongFormatConfig));
/// Source `exeerrors.ErrUnknownOption`.
pub static ERR_UNKNOWN_OPTION: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrUnknownOption));
/// Source `exeerrors.ErrInvalidOptionVal`.
pub static ERR_INVALID_OPTION_VAL: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrInvalidOptionVal));
/// Source `exeerrors.ErrDuplicateOption`.
pub static ERR_DUPLICATE_OPTION: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrDuplicateOption));
/// Source `exeerrors.ErrLoadDataUnsupportedOption`.
pub static ERR_LOAD_DATA_UNSUPPORTED_OPTION: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataUnsupportedOption));
/// Source `exeerrors.ErrLoadDataDuplicateKeyConflict`.
pub static ERR_LOAD_DATA_DUPLICATE_KEY_CONFLICT: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataDuplicateKeyConflict));
/// Source `exeerrors.ErrLoadDataJobNotFound`.
pub static ERR_LOAD_DATA_JOB_NOT_FOUND: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataJobNotFound));
/// Source `exeerrors.ErrLoadDataInvalidOperation`.
pub static ERR_LOAD_DATA_INVALID_OPERATION: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataInvalidOperation));
/// Source `exeerrors.ErrLoadDataLocalUnsupportedOption`.
pub static ERR_LOAD_DATA_LOCAL_UNSUPPORTED_OPTION: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataLocalUnsupportedOption));
/// Source `exeerrors.ErrLoadDataPreCheckFailed`.
pub static ERR_LOAD_DATA_PRE_CHECK_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrLoadDataPreCheckFailed));
/// Source `exeerrors.ErrMaxKeysReadExceeded`.
pub static ERR_MAX_KEYS_READ_EXCEEDED: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_EXECUTOR.new_std(errcode::ErrMaxKeysReadExceeded));

/// Every executor error paired with its Go variable name, for the fixture test.
#[cfg(test)]
pub(crate) fn fixture_entries() -> Vec<(&'static str, &'static TerrorError)> {
    vec![
        ("ErrGetStartTS", &*ERR_GET_START_TS),
        ("ErrUnknownPlan", &*ERR_UNKNOWN_PLAN),
        ("ErrPrepareMulti", &*ERR_PREPARE_MULTI),
        ("ErrPrepareDDL", &*ERR_PREPARE_DDL),
        ("ErrResultIsEmpty", &*ERR_RESULT_IS_EMPTY),
        ("ErrBuildExecutor", &*ERR_BUILD_EXECUTOR),
        ("ErrBatchInsertFail", &*ERR_BATCH_INSERT_FAIL),
        ("ErrUnsupportedPs", &*ERR_UNSUPPORTED_PS),
        ("ErrSubqueryMoreThan1Row", &*ERR_SUBQUERY_MORE_THAN1_ROW),
        ("ErrIllegalGrantForTable", &*ERR_ILLEGAL_GRANT_FOR_TABLE),
        ("ErrColumnsNotMatched", &*ERR_COLUMNS_NOT_MATCHED),
        (
            "ErrCantCreateUserWithGrant",
            &*ERR_CANT_CREATE_USER_WITH_GRANT,
        ),
        ("ErrPasswordNoMatch", &*ERR_PASSWORD_NO_MATCH),
        ("ErrCannotUser", &*ERR_CANNOT_USER),
        ("ErrGrantRole", &*ERR_GRANT_ROLE),
        ("ErrPasswordFormat", &*ERR_PASSWORD_FORMAT),
        (
            "ErrCantChangeTxCharacteristics",
            &*ERR_CANT_CHANGE_TX_CHARACTERISTICS,
        ),
        ("ErrPsManyParam", &*ERR_PS_MANY_PARAM),
        ("ErrAdminCheckTable", &*ERR_ADMIN_CHECK_TABLE),
        ("ErrDBaccessDenied", &*ERR_D_BACCESS_DENIED),
        ("ErrTableaccessDenied", &*ERR_TABLEACCESS_DENIED),
        ("ErrBadDB", &*ERR_BAD_DB),
        ("ErrWrongObject", &*ERR_WRONG_OBJECT),
        ("ErrWrongUsage", &*ERR_WRONG_USAGE),
        ("ErrRoleNotGranted", &*ERR_ROLE_NOT_GRANTED),
        ("ErrDeadlock", &*ERR_DEADLOCK),
        ("ErrQueryInterrupted", &*ERR_QUERY_INTERRUPTED),
        ("ErrMaxExecTimeExceeded", &*ERR_MAX_EXEC_TIME_EXCEEDED),
        (
            "ErrResourceGroupQueryRunawayInterrupted",
            &*ERR_RESOURCE_GROUP_QUERY_RUNAWAY_INTERRUPTED,
        ),
        ("ErrQueryExecStopped", &*ERR_QUERY_EXEC_STOPPED),
        (
            "ErrResourceGroupQueryRunawayQuarantine",
            &*ERR_RESOURCE_GROUP_QUERY_RUNAWAY_QUARANTINE,
        ),
        (
            "ErrDynamicPrivilegeNotRegistered",
            &*ERR_DYNAMIC_PRIVILEGE_NOT_REGISTERED,
        ),
        ("ErrIllegalPrivilegeLevel", &*ERR_ILLEGAL_PRIVILEGE_LEVEL),
        (
            "ErrInvalidSplitRegionRanges",
            &*ERR_INVALID_SPLIT_REGION_RANGES,
        ),
        ("ErrViewInvalid", &*ERR_VIEW_INVALID),
        ("ErrInstanceScope", &*ERR_INSTANCE_SCOPE),
        ("ErrSettingNoopVariable", &*ERR_SETTING_NOOP_VARIABLE),
        (
            "ErrLazyUniquenessCheckFailure",
            &*ERR_LAZY_UNIQUENESS_CHECK_FAILURE,
        ),
        ("ErrMemoryExceedForQuery", &*ERR_MEMORY_EXCEED_FOR_QUERY),
        (
            "ErrMemoryExceedForInstance",
            &*ERR_MEMORY_EXCEED_FOR_INSTANCE,
        ),
        ("ErrDeleteNotFoundColumn", &*ERR_DELETE_NOT_FOUND_COLUMN),
        ("ErrBRIEBackupFailed", &*ERR_BRIE_BACKUP_FAILED),
        ("ErrBRIERestoreFailed", &*ERR_BRIE_RESTORE_FAILED),
        ("ErrBRIEImportFailed", &*ERR_BRIE_IMPORT_FAILED),
        ("ErrBRIEExportFailed", &*ERR_BRIE_EXPORT_FAILED),
        ("ErrBRJobNotFound", &*ERR_BR_JOB_NOT_FOUND),
        ("ErrCTEMaxRecursionDepth", &*ERR_CTE_MAX_RECURSION_DEPTH),
        ("ErrPluginIsNotLoaded", &*ERR_PLUGIN_IS_NOT_LOADED),
        ("ErrSetPasswordAuthPlugin", &*ERR_SET_PASSWORD_AUTH_PLUGIN),
        ("ErrFuncNotEnabled", &*ERR_FUNC_NOT_ENABLED),
        ("ErrSavepointNotExists", &*ERR_SAVEPOINT_NOT_EXISTS),
        (
            "ErrForeignKeyCascadeDepthExceeded",
            &*ERR_FOREIGN_KEY_CASCADE_DEPTH_EXCEEDED,
        ),
        (
            "ErrPasswordExpireAnonymousUser",
            &*ERR_PASSWORD_EXPIRE_ANONYMOUS_USER,
        ),
        ("ErrMustChangePassword", &*ERR_MUST_CHANGE_PASSWORD),
        (
            "ErrSecondPasswordCannotBeEmpty",
            &*ERR_SECOND_PASSWORD_CANNOT_BE_EMPTY,
        ),
        (
            "ErrPasswordCannotBeRetainedOnPluginChange",
            &*ERR_PASSWORD_CANNOT_BE_RETAINED_ON_PLUGIN_CHANGE,
        ),
        (
            "ErrCurrentPasswordCannotBeRetained",
            &*ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED,
        ),
        ("ErrWrongStringLength", &*ERR_WRONG_STRING_LENGTH),
        (
            "ErrUnsupportedFlashbackTmpTable",
            &*ERR_UNSUPPORTED_FLASHBACK_TMP_TABLE,
        ),
        (
            "ErrTruncateWrongInsertValue",
            &*ERR_TRUNCATE_WRONG_INSERT_VALUE,
        ),
        (
            "ErrExistsInHistoryPassword",
            &*ERR_EXISTS_IN_HISTORY_PASSWORD,
        ),
        ("ErrUserNameNeedPrefix", &*ERR_USER_NAME_NEED_PREFIX),
        ("ErrWarnTooFewRecords", &*ERR_WARN_TOO_FEW_RECORDS),
        ("ErrWarnTooManyRecords", &*ERR_WARN_TOO_MANY_RECORDS),
        (
            "ErrLoadDataFromServerDisk",
            &*ERR_LOAD_DATA_FROM_SERVER_DISK,
        ),
        ("ErrLoadParquetFromLocal", &*ERR_LOAD_PARQUET_FROM_LOCAL),
        ("ErrLoadDataEmptyPath", &*ERR_LOAD_DATA_EMPTY_PATH),
        (
            "ErrLoadDataUnsupportedFormat",
            &*ERR_LOAD_DATA_UNSUPPORTED_FORMAT,
        ),
        ("ErrLoadDataInvalidURI", &*ERR_LOAD_DATA_INVALID_URI),
        ("ErrLoadDataCantAccess", &*ERR_LOAD_DATA_CANT_ACCESS),
        ("ErrLoadDataCantRead", &*ERR_LOAD_DATA_CANT_READ),
        (
            "ErrLoadDataWrongFormatConfig",
            &*ERR_LOAD_DATA_WRONG_FORMAT_CONFIG,
        ),
        ("ErrUnknownOption", &*ERR_UNKNOWN_OPTION),
        ("ErrInvalidOptionVal", &*ERR_INVALID_OPTION_VAL),
        ("ErrDuplicateOption", &*ERR_DUPLICATE_OPTION),
        (
            "ErrLoadDataUnsupportedOption",
            &*ERR_LOAD_DATA_UNSUPPORTED_OPTION,
        ),
        (
            "ErrLoadDataDuplicateKeyConflict",
            &*ERR_LOAD_DATA_DUPLICATE_KEY_CONFLICT,
        ),
        ("ErrLoadDataJobNotFound", &*ERR_LOAD_DATA_JOB_NOT_FOUND),
        (
            "ErrLoadDataInvalidOperation",
            &*ERR_LOAD_DATA_INVALID_OPERATION,
        ),
        (
            "ErrLoadDataLocalUnsupportedOption",
            &*ERR_LOAD_DATA_LOCAL_UNSUPPORTED_OPTION,
        ),
        (
            "ErrLoadDataPreCheckFailed",
            &*ERR_LOAD_DATA_PRE_CHECK_FAILED,
        ),
        ("ErrMaxKeysReadExceeded", &*ERR_MAX_KEYS_READ_EXCEEDED),
    ]
}
