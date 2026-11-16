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

//! Go `br/pkg/errors`: BR's normalized errors, `Is` and
//! `IsContextCanceled`, plus the error type BR code returns (Go `error`)
//! and Go's `errors.Annotate` / `Annotatef` / `Cause` over it.

use std::error::Error as StdError;
use std::sync::{Arc, LazyLock};

use tidb_terror::{TerrorCode, TerrorError};

/// Go `error` in BR code.
pub type Error = Arc<dyn StdError + Send + Sync>;
/// A BR result.
pub type Result<T> = std::result::Result<T, Error>;

fn normalize(message: &str, rfc_code: &str) -> TerrorError {
    TerrorError::normalize(message, rfc_code, TerrorCode::new(0))
}

/// The error of a normalized BR error (Go returning `berrors.ErrX`).
#[must_use]
pub fn new(err: &TerrorError) -> Error {
    Arc::new(err.clone())
}

/// A plain error with Go's `errors.New` / `errors.Errorf` text.
#[must_use]
pub fn errorf(message: impl Into<String>) -> Error {
    Arc::new(Plain(message.into()))
}

/// Go `errors.Annotate(err, message)`.
#[must_use]
pub fn annotate(err: Error, message: impl Into<String>) -> Error {
    Arc::new(tidb_terror::annotate(err, message))
}

/// Go `errors.Annotatef(berrors.ErrX, message)`.
#[must_use]
pub fn annotatef(err: &TerrorError, message: impl Into<String>) -> Error {
    annotate(new(err), message)
}

/// Go `errors.Cause`: the innermost error of the chain.
#[must_use]
pub fn cause<'a>(err: &'a (dyn StdError + 'static)) -> &'a (dyn StdError + 'static) {
    let mut e = err;
    while let Some(next) = e.source() {
        e = next;
    }
    e
}

/// Go `Is`: whether a normalized error with `is`'s ID is in `err`'s chain.
#[must_use]
pub fn is(err: &(dyn StdError + 'static), is: &TerrorError) -> bool {
    let mut e = Some(err);
    while let Some(cur) = e {
        if cur.downcast_ref::<TerrorError>().is_some_and(|t| t.id() == is.id()) {
            return true;
        }
        e = cur.source();
    }
    false
}

/// Go `IsContextCanceled`: `context.Canceled` or `DeadlineExceeded` in the
/// chain.
#[must_use]
pub fn is_context_canceled(err: &(dyn StdError + 'static)) -> bool {
    let mut e = Some(err);
    while let Some(cur) = e {
        if cur.downcast_ref::<tidb_gocontext::CtxError>().is_some() {
            return true;
        }
        e = cur.source();
    }
    false
}

/// Go `status.Code(err)`: the gRPC code of the first gRPC status in the
/// chain, `Unknown` without one.
#[must_use]
pub fn grpc_code(err: &(dyn StdError + 'static)) -> tonic::Code {
    let mut e = Some(err);
    while let Some(cur) = e {
        if let Some(status) = cur.downcast_ref::<GrpcStatus>() {
            return status.0.code();
        }
        if let Some(status) = cur.downcast_ref::<tonic::Status>() {
            return status.code();
        }
        e = cur.source();
    }
    tonic::Code::Unknown
}

/// A gRPC failure, shown as grpc-go's `status.Error` shows it
/// (`rpc error: code = Unavailable desc = ...`): BR decides on retries by
/// the message text too.
#[derive(Debug, Clone)]
pub struct GrpcStatus(pub tonic::Status);

impl std::fmt::Display for GrpcStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rpc error: code = {} desc = {}", go_code_name(self.0.code()), self.0.message())
    }
}

impl StdError for GrpcStatus {}

/// grpc-go's `codes.Code.String`.
#[must_use]
pub fn go_code_name(code: tonic::Code) -> &'static str {
    use tonic::Code;
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "Canceled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    }
}

/// A gRPC status as a BR error.
#[must_use]
pub fn grpc(status: tonic::Status) -> Error {
    Arc::new(GrpcStatus(status))
}

/// Go `errors.New` text.
#[derive(Debug, Clone)]
pub struct Plain(pub String);

impl std::fmt::Display for Plain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for Plain {}



/// Go `ErrUnknown`.
pub static ERR_UNKNOWN: LazyLock<TerrorError> = LazyLock::new(|| normalize("internal error", "BR:Common:ErrUnknown"));

/// Go `ErrInvalidArgument`.
pub static ERR_INVALID_ARGUMENT: LazyLock<TerrorError> = LazyLock::new(|| normalize("invalid argument", "BR:Common:ErrInvalidArgument"));

/// Go `ErrOperationAborted`.
pub static ERR_OPERATION_ABORTED: LazyLock<TerrorError> = LazyLock::new(|| normalize("operation aborted by user", "BR:Common:ErrOperationAborted"));

/// Go `ErrUndefinedRestoreDbOrTable`.
pub static ERR_UNDEFINED_RESTORE_DB_OR_TABLE: LazyLock<TerrorError> = LazyLock::new(|| normalize("undefined restore databases or tables", "BR:Common:ErrUndefinedDbOrTable"));

/// Go `ErrVersionMismatch`.
pub static ERR_VERSION_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("version mismatch", "BR:Common:ErrVersionMismatch"));

/// Go `ErrFailedToConnect`.
pub static ERR_FAILED_TO_CONNECT: LazyLock<TerrorError> = LazyLock::new(|| normalize("failed to make gRPC channels", "BR:Common:ErrFailedToConnect"));

/// Go `ErrInvalidMetaFile`.
pub static ERR_INVALID_META_FILE: LazyLock<TerrorError> = LazyLock::new(|| normalize("invalid metafile: %s", "BR:Common:ErrInvalidMetaFile"));

/// Go `ErrEnvNotSpecified`.
pub static ERR_ENV_NOT_SPECIFIED: LazyLock<TerrorError> = LazyLock::new(|| normalize("environment variable not found", "BR:Common:ErrEnvNotSpecified"));

/// Go `ErrUnsupportedOperation`.
pub static ERR_UNSUPPORTED_OPERATION: LazyLock<TerrorError> = LazyLock::new(|| normalize("the operation is not supported", "BR:Common:ErrUnsupportedOperation"));

/// Go `ErrInvalidRange`.
pub static ERR_INVALID_RANGE: LazyLock<TerrorError> = LazyLock::new(|| normalize("invalid restore range", "BR:Common:ErrInvalidRange"));

/// Go `ErrMigrationNotFound`.
pub static ERR_MIGRATION_NOT_FOUND: LazyLock<TerrorError> = LazyLock::new(|| normalize("no migration found", "BR:Common:ErrMigrationNotFound"));

/// Go `ErrMigrationVersionNotSupported`.
pub static ERR_MIGRATION_VERSION_NOT_SUPPORTED: LazyLock<TerrorError> = LazyLock::new(|| normalize("the migration version isn't supported", "BR:Common:ErrMigrationVersionNotSupported"));

/// Go `ErrPDUpdateFailed`.
pub static ERR_PD_UPDATE_FAILED: LazyLock<TerrorError> = LazyLock::new(|| normalize("failed to update PD", "BR:PD:ErrPDUpdateFailed"));

/// Go `ErrPDLeaderNotFound`.
pub static ERR_PD_LEADER_NOT_FOUND: LazyLock<TerrorError> = LazyLock::new(|| normalize("PD leader not found", "BR:PD:ErrPDLeaderNotFound"));

/// Go `ErrPDInvalidResponse`.
pub static ERR_PD_INVALID_RESPONSE: LazyLock<TerrorError> = LazyLock::new(|| normalize("PD invalid response", "BR:PD:ErrPDInvalidResponse"));

/// Go `ErrPDBatchScanRegion`.
pub static ERR_PD_BATCH_SCAN_REGION: LazyLock<TerrorError> = LazyLock::new(|| normalize("batch scan region", "BR:PD:ErrPDBatchScanRegion"));

/// Go `ErrPDUnknownScatterResult`.
pub static ERR_PD_UNKNOWN_SCATTER_RESULT: LazyLock<TerrorError> = LazyLock::new(|| normalize("failed to wait region scattered", "BR:PD:ErrPDUknownScatterResult"));

/// Go `ErrPDNotFullyScatter`.
pub static ERR_PD_NOT_FULLY_SCATTER: LazyLock<TerrorError> = LazyLock::new(|| normalize("pd not fully scattered", "BR:PD:ErrPDNotFullyScatter"));

/// Go `ErrPDSplitFailed`.
pub static ERR_PD_SPLIT_FAILED: LazyLock<TerrorError> = LazyLock::new(|| normalize("failed to wait region split", "BR:PD:ErrPDUknownScatterResult"));

/// Go `ErrPDRegionsNotFullyScatter`.
pub static ERR_PD_REGIONS_NOT_FULLY_SCATTER: LazyLock<TerrorError> = LazyLock::new(|| normalize("regions not fully scattered", "BR:PD:ErrPDRegionsNotFullyScatter"));

/// Go `ErrBackupChecksumMismatch`.
pub static ERR_BACKUP_CHECKSUM_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("backup checksum mismatch", "BR:Backup:ErrBackupChecksumMismatch"));

/// Go `ErrBackupInvalidRange`.
pub static ERR_BACKUP_INVALID_RANGE: LazyLock<TerrorError> = LazyLock::new(|| normalize("backup range invalid", "BR:Backup:ErrBackupInvalidRange"));

/// Go `ErrBackupNoLeader`.
pub static ERR_BACKUP_NO_LEADER: LazyLock<TerrorError> = LazyLock::new(|| normalize("backup no leader", "BR:Backup:ErrBackupNoLeader"));

/// Go `ErrBackupGCSafepointExceeded`.
pub static ERR_BACKUP_GC_SAFEPOINT_EXCEEDED: LazyLock<TerrorError> = LazyLock::new(|| normalize("backup GC safepoint exceeded", "BR:Backup:ErrBackupGCSafepointExceeded"));

/// Go `ErrBackupKeyIsLocked`.
pub static ERR_BACKUP_KEY_IS_LOCKED: LazyLock<TerrorError> = LazyLock::new(|| normalize("backup key is locked", "BR:Backup:ErrBackupKeyIsLocked"));

/// Go `ErrBackupRegion`.
pub static ERR_BACKUP_REGION: LazyLock<TerrorError> = LazyLock::new(|| normalize("backup region error", "BR:Backup:ErrBackupRegion"));

/// Go `ErrRestoreModeMismatch`.
pub static ERR_RESTORE_MODE_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore mode mismatch", "BR:Restore:ErrRestoreModeMismatch"));

/// Go `ErrRestoreRangeMismatch`.
pub static ERR_RESTORE_RANGE_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore range mismatch", "BR:Restore:ErrRestoreRangeMismatch"));

/// Go `ErrRestoreCheckpointMismatch`.
pub static ERR_RESTORE_CHECKPOINT_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore checkpoint mismatch", "BR:Restore:ErrRestoreCheckpointMismatch"));

/// Go `ErrRestoreChecksumMismatch`.
pub static ERR_RESTORE_CHECKSUM_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore checksum mismatch", "BR:Restore:ErrRestoreChecksumMismatch"));

/// Go `ErrRestoreTableIDMismatch`.
pub static ERR_RESTORE_TABLE_ID_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore table ID mismatch", "BR:Restore:ErrRestoreTableIDMismatch"));

/// Go `ErrRestoreRejectStore`.
pub static ERR_RESTORE_REJECT_STORE: LazyLock<TerrorError> = LazyLock::new(|| normalize("failed to restore remove rejected store", "BR:Restore:ErrRestoreRejectStore"));

/// Go `ErrRestoreNoPeer`.
pub static ERR_RESTORE_NO_PEER: LazyLock<TerrorError> = LazyLock::new(|| normalize("region does not have peer", "BR:Restore:ErrRestoreNoPeer"));

/// Go `ErrRestoreSplitFailed`.
pub static ERR_RESTORE_SPLIT_FAILED: LazyLock<TerrorError> = LazyLock::new(|| normalize("fail to split region", "BR:Restore:ErrRestoreSplitFailed"));

/// Go `ErrRestoreInvalidRewrite`.
pub static ERR_RESTORE_INVALID_REWRITE: LazyLock<TerrorError> = LazyLock::new(|| normalize("invalid rewrite rule", "BR:Restore:ErrRestoreInvalidRewrite"));

/// Go `ErrRestoreInvalidBackup`.
pub static ERR_RESTORE_INVALID_BACKUP: LazyLock<TerrorError> = LazyLock::new(|| normalize("invalid backup", "BR:Restore:ErrRestoreInvalidBackup"));

/// Go `ErrRestoreWriteAndIngest`.
pub static ERR_RESTORE_WRITE_AND_INGEST: LazyLock<TerrorError> = LazyLock::new(|| normalize("failed to write and ingest", "BR:Restore:ErrRestoreWriteAndIngest"));

/// Go `ErrRestoreSchemaNotExists`.
pub static ERR_RESTORE_SCHEMA_NOT_EXISTS: LazyLock<TerrorError> = LazyLock::new(|| normalize("schema not exists", "BR:Restore:ErrRestoreSchemaNotExists"));

/// Go `ErrRestoreNotFreshCluster`.
pub static ERR_RESTORE_NOT_FRESH_CLUSTER: LazyLock<TerrorError> = LazyLock::new(|| normalize("cluster is not fresh", "BR:Restore:ErrRestoreNotFreshCluster"));

/// Go `ErrRestoreIncompatibleSys`.
pub static ERR_RESTORE_INCOMPATIBLE_SYS: LazyLock<TerrorError> = LazyLock::new(|| normalize("incompatible system table", "BR:Restore:ErrRestoreIncompatibleSys"));

/// Go `ErrUnsupportedSystemTable`.
pub static ERR_UNSUPPORTED_SYSTEM_TABLE: LazyLock<TerrorError> = LazyLock::new(|| normalize("the system table isn't supported for restoring yet", "BR:Restore:ErrUnsupportedSysTable"));

/// Go `ErrDatabasesAlreadyExisted`.
pub static ERR_DATABASES_ALREADY_EXISTED: LazyLock<TerrorError> = LazyLock::new(|| normalize("databases already existed in restored cluster", "BR:Restore:ErrDatabasesAlreadyExisted"));

/// Go `ErrTablesAlreadyExisted`.
pub static ERR_TABLES_ALREADY_EXISTED: LazyLock<TerrorError> = LazyLock::new(|| normalize("tables already existed in restored cluster", "BR:Restore:ErrTablesAlreadyExisted"));

/// Go `ErrStreamLogTaskExist`.
pub static ERR_STREAM_LOG_TASK_EXIST: LazyLock<TerrorError> = LazyLock::new(|| normalize("stream task already exists", "BR:Stream:ErrStreamLogTaskExist"));

/// Go `ErrStreamLogTaskHasNoStorage`.
pub static ERR_STREAM_LOG_TASK_HAS_NO_STORAGE: LazyLock<TerrorError> = LazyLock::new(|| normalize("stream task has no storage", "BR:Stream:ErrStreamLogTaskHasNoStorage"));

/// Go `ErrRestoreRTsConstrain`.
pub static ERR_RESTORE_RTS_CONSTRAIN: LazyLock<TerrorError> = LazyLock::new(|| normalize("resolved ts constrain violation", "BR:Restore:ErrRestoreResolvedTsConstrain"));

/// Go `ErrPiTRInvalidCDCLogFormat`.
pub static ERR_PITR_INVALID_CDC_LOG_FORMAT: LazyLock<TerrorError> = LazyLock::new(|| normalize("invalid cdc log format", "BR:PiTR:ErrPiTRInvalidCDCLogFormat"));

/// Go `ErrPiTRTaskNotFound`.
pub static ERR_PITR_TASK_NOT_FOUND: LazyLock<TerrorError> = LazyLock::new(|| normalize("task not found", "BR:PiTR:ErrTaskNotFound"));

/// Go `ErrPiTRInvalidTaskInfo`.
pub static ERR_PITR_INVALID_TASK_INFO: LazyLock<TerrorError> = LazyLock::new(|| normalize("task info is invalid", "BR:PiTR:ErrInvalidTaskInfo"));

/// Go `ErrPiTRMalformedMetadata`.
pub static ERR_PITR_MALFORMED_METADATA: LazyLock<TerrorError> = LazyLock::new(|| normalize("malformed metadata", "BR:PiTR:ErrMalformedMetadata"));

/// Go `ErrPiTRCheckpointWatchRestart`.
pub static ERR_PITR_CHECKPOINT_WATCH_RESTART: LazyLock<TerrorError> = LazyLock::new(|| normalize("checkpoint watch needs restart", "BR:PiTR:ErrCheckpointWatchRestart"));

/// Go `ErrStorageUnknown`.
pub static ERR_STORAGE_UNKNOWN: LazyLock<TerrorError> = LazyLock::new(|| normalize("unknown external storage error", "BR:ExternalStorage:ErrStorageUnknown"));

/// Go `ErrStorageInvalidConfig`.
pub static ERR_STORAGE_INVALID_CONFIG: LazyLock<TerrorError> = LazyLock::new(|| normalize("invalid external storage config", "BR:ExternalStorage:ErrStorageInvalidConfig"));

/// Go `ErrStorageInvalidPermission`.
pub static ERR_STORAGE_INVALID_PERMISSION: LazyLock<TerrorError> = LazyLock::new(|| normalize("external storage permission", "BR:ExternalStorage:ErrStorageInvalidPermission"));

/// Go `ErrRestoreTotalKVMismatch`.
pub static ERR_RESTORE_TOTAL_KV_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore total tikvs mismatch", "BR:EBS:ErrRestoreTotalKVMismatch"));

/// Go `ErrRestoreInvalidPeer`.
pub static ERR_RESTORE_INVALID_PEER: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore met a invalid peer", "BR:EBS:ErrRestoreInvalidPeer"));

/// Go `ErrRestoreRegionWithoutPeer`.
pub static ERR_RESTORE_REGION_WITHOUT_PEER: LazyLock<TerrorError> = LazyLock::new(|| normalize("restore met a region without any peer", "BR:EBS:ErrRestoreRegionWithoutPeer"));

/// Go `ErrKVStorage`.
pub static ERR_KV_STORAGE: LazyLock<TerrorError> = LazyLock::new(|| normalize("tikv storage occur I/O error", "BR:KV:ErrKVStorage"));

/// Go `ErrKVUnknown`.
pub static ERR_KV_UNKNOWN: LazyLock<TerrorError> = LazyLock::new(|| normalize("unknown error occur on tikv", "BR:KV:ErrKVUnknown"));

/// Go `ErrKVClusterIDMismatch`.
pub static ERR_KV_CLUSTER_ID_MISMATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("tikv cluster ID mismatch", "BR:KV:ErrKVClusterIDMismatch"));

/// Go `ErrKVNotLeader`.
pub static ERR_KV_NOT_LEADER: LazyLock<TerrorError> = LazyLock::new(|| normalize("not leader", "BR:KV:ErrKVNotLeader"));

/// Go `ErrKVNotTiKV`.
pub static ERR_KV_NOT_TIKV: LazyLock<TerrorError> = LazyLock::new(|| normalize("storage is not tikv", "BR:KV:ErrNotTiKVStorage"));

/// Go `ErrKVDiskFull`.
pub static ERR_KV_DISK_FULL: LazyLock<TerrorError> = LazyLock::new(|| normalize("disk is full", "BR:KV:ErrKVDiskFull"));

/// Go `ErrKVEpochNotMatch`.
pub static ERR_KV_EPOCH_NOT_MATCH: LazyLock<TerrorError> = LazyLock::new(|| normalize("epoch not match", "BR:KV:ErrKVEpochNotMatch"));

/// Go `ErrKVKeyNotInRegion`.
pub static ERR_KV_KEY_NOT_IN_REGION: LazyLock<TerrorError> = LazyLock::new(|| normalize("key not in region", "BR:KV:ErrKVKeyNotInRegion"));

/// Go `ErrKVRewriteRuleNotFound`.
pub static ERR_KV_REWRITE_RULE_NOT_FOUND: LazyLock<TerrorError> = LazyLock::new(|| normalize("rewrite rule not found", "BR:KV:ErrKVRewriteRuleNotFound"));

/// Go `ErrKVRangeIsEmpty`.
pub static ERR_KV_RANGE_IS_EMPTY: LazyLock<TerrorError> = LazyLock::new(|| normalize("range is empty", "BR:KV:ErrKVRangeIsEmpty"));

/// Go `ErrKVDownloadFailed`.
pub static ERR_KV_DOWNLOAD_FAILED: LazyLock<TerrorError> = LazyLock::new(|| normalize("download sst failed", "BR:KV:ErrKVDownloadFailed"));

/// Go `ErrKVIngestFailed`.
pub static ERR_KV_INGEST_FAILED: LazyLock<TerrorError> = LazyLock::new(|| normalize("ingest sst failed", "BR:KV:ErrKVIngestFailed"));

/// Go `ErrPossibleInconsistency`.
pub static ERR_POSSIBLE_INCONSISTENCY: LazyLock<TerrorError> = LazyLock::new(|| normalize("the cluster state might be inconsistent", "BR:KV:ErrPossibleInconsistency"));
