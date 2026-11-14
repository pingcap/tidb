// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/lightning/common/retry.go`, on native transport and TiDB errors.
use std::error::Error;
use tidb_ingestor_errdef::*;
use tidb_terror::{MysqlError, TerrorError};
/// Go `ErrWriteTooSlow`.
#[derive(Debug)]
pub struct WriteTooSlow;
impl std::fmt::Display for WriteTooSlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("write too slow, maybe gRPC is blocked forever")
    }
}
impl Error for WriteTooSlow {}
/// Go `IsRetryableError` / `isSingleRetryableError` for native errors.
pub fn is_retryable_error(error: &(dyn Error + 'static)) -> bool {
    let mut cause = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    if cause.is::<WriteTooSlow>() {
        return true;
    }
    if let Some(error) = cause.downcast_ref::<tidb_gocontext::CtxError>() {
        return *error == tidb_gocontext::CtxError::DeadlineExceeded;
    }
    if let Some(error) = cause.downcast_ref::<std::io::Error>() {
        return matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
        );
    }
    if let Some(error) = cause.downcast_ref::<MysqlError>() {
        return retryable_mysql_code(error.code);
    }
    if let Some(error) = cause.downcast_ref::<TerrorError>() {
        if [
            &*ERR_KV_EPOCH_NOT_MATCH,
            &*ERR_KV_NOT_LEADER,
            &*ERR_NO_LEADER,
            &*ERR_KV_REGION_NOT_FOUND,
            &*ERR_KV_SERVER_IS_BUSY,
            &*ERR_KV_READ_INDEX_NOT_READY,
            &*ERR_KV_INGEST_FAILED,
            &*ERR_KV_RAFT_PROPOSAL_DROPPED,
        ]
        .iter()
        .any(|template| template.id() == error.id())
        {
            return true;
        }
        if error.id() == "Lightning:PD:ErrCreatePDClient" {
            return true;
        }
        // Go's driver errors retain the TiKV RFC class after SQL conversion.
        return matches!(
            error.rfc_code(),
            "tikv:9001" | "tikv:9002" | "tikv:9003" | "tikv:9005" | "tikv:9010" | "tikv:1105"
        );
    }
    if let Some(error) = cause.downcast_ref::<HttpStatusError>() {
        return error.status_code != 400 && error.status_code != 404;
    }
    if let Some(error) = cause.downcast_ref::<tonic::Status>() {
        use tonic::Code;
        return match error.code() {
            Code::DeadlineExceeded
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::OutOfRange
            | Code::Unavailable
            | Code::DataLoss => true,
            Code::Unknown => {
                !error.message().contains("DiskSpaceNotEnough")
                    && !error
                        .message()
                        .contains("Keys must be added in strict ascending order")
            }
            _ => false,
        };
    }
    let message = cause.to_string().to_lowercase();
    [
        "coprocessor task terminated due to exceeding the deadline",
        "rate: wait",
        "injected random error",
    ]
    .iter()
    .any(|part| message.contains(part))
}
fn retryable_mysql_code(code: u16) -> bool {
    use tidb_errno::errcode::*;
    matches!(
        code,
        ErrUnknown
            | ErrLockDeadlock
            | ErrLockWaitTimeout
            | ErrWriteConflict
            | ErrWriteConflictInTiDB
            | ErrPDServerTimeout
            | ErrTiKVServerTimeout
            | ErrTiKVServerBusy
            | ErrResolveLockTimeout
            | ErrRegionUnavailable
            | ErrInfoSchemaExpired
            | ErrInfoSchemaChanged
            | ErrTxnRetryable
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `retry_test.go:TestIsRetryableError`, native error representations
    /// exercised by ImportSST and distributed backfill.
    #[test]
    fn test_is_retryable_error() {
        assert!(!is_retryable_error(&tidb_gocontext::CtxError::Canceled));
        assert!(is_retryable_error(
            &tidb_gocontext::CtxError::DeadlineExceeded
        ));
        assert!(is_retryable_error(&WriteTooSlow));
        for (kind, expected) in [
            (std::io::ErrorKind::ConnectionRefused, true),
            (std::io::ErrorKind::BrokenPipe, true),
            (std::io::ErrorKind::ConnectionReset, true),
            (std::io::ErrorKind::TimedOut, true),
            (std::io::ErrorKind::UnexpectedEof, false),
        ] {
            assert_eq!(is_retryable_error(&std::io::Error::from(kind)), expected);
        }
        for error in [
            &*ERR_NO_LEADER,
            &*ERR_KV_NOT_LEADER,
            &*ERR_KV_EPOCH_NOT_MATCH,
            &*ERR_KV_SERVER_IS_BUSY,
            &*ERR_KV_REGION_NOT_FOUND,
            &*ERR_KV_READ_INDEX_NOT_READY,
            &*ERR_KV_INGEST_FAILED,
            &*ERR_KV_RAFT_PROPOSAL_DROPPED,
        ] {
            assert!(is_retryable_error(error));
            assert!(is_retryable_error(&error.gen_with_stack("test")));
        }
        assert!(!is_retryable_error(
            &ERR_KV_DISK_FULL.gen_with_stack("test")
        ));
        assert!(!is_retryable_error(&MysqlError::sql_error(0, "")));
        use tidb_errno::errcode::*;
        for code in [
            ErrUnknown,
            ErrLockDeadlock,
            ErrPDServerTimeout,
            ErrTiKVServerTimeout,
            ErrTiKVServerBusy,
            ErrResolveLockTimeout,
            ErrRegionUnavailable,
            ErrWriteConflictInTiDB,
            ErrWriteConflict,
            ErrInfoSchemaExpired,
            ErrInfoSchemaChanged,
            ErrTxnRetryable,
        ] {
            assert!(is_retryable_error(&MysqlError::sql_error(code, "")));
        }
        use tonic::Code;
        assert!(!is_retryable_error(&tonic::Status::new(
            Code::Cancelled,
            ""
        )));
        for code in [
            Code::DeadlineExceeded,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::Aborted,
            Code::OutOfRange,
            Code::Unavailable,
            Code::DataLoss,
        ] {
            assert!(is_retryable_error(&tonic::Status::new(code, "")));
        }
        for (message, expected) in [
            ("region 1234 is not fully replicated", true),
            (
                "No such file or directory: while stat a file for size",
                true,
            ),
            ("DiskSpaceNotEnough", false),
            ("Keys must be added in strict ascending order", false),
        ] {
            assert_eq!(
                is_retryable_error(&tonic::Status::new(Code::Unknown, message)),
                expected
            );
        }
    }
}
