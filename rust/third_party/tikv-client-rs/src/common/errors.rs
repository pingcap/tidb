// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;

use thiserror::Error;

use crate::proto::kvrpcpb;
use crate::region::RegionVerId;
use crate::BoundRange;

/// Protobuf-generated region-level error returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::errorpb::Error as ProtoRegionError;

/// Protobuf-generated per-key error returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::KeyError as ProtoKeyError;

/// An error originating from the TiKV client or dependencies.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    /// Feature is not implemented.
    #[error("Unimplemented feature")]
    Unimplemented,
    /// Duplicate key insertion happens.
    #[error("Duplicate key insertion")]
    DuplicateKeyInsertion,
    /// Failed to resolve a lock
    #[error("Failed to resolve lock")]
    ResolveLockError(Vec<kvrpcpb::LockInfo>),
    /// Will raise this error when using a pessimistic txn only operation on an optimistic txn
    #[error("Invalid operation for this type of transaction")]
    InvalidTransactionType,
    /// It's not allowed to perform operations in a transaction after it has been committed or rolled back.
    #[error("Cannot read or write data after any attempt to commit or roll back the transaction")]
    OperationAfterCommitError,
    /// An optimistic transaction became stale while waiting for local latches.
    #[error(transparent)]
    WriteConflictInLatch(#[from] crate::error::WriteConflictInLatchError),
    /// We tried to use 1pc for a transaction, but it didn't work. Probably should have used 2pc.
    #[error("1PC transaction could not be committed.")]
    OnePcFailure,
    /// An operation requires a primary key, but the transaction was empty.
    #[error("transaction has no primary key")]
    NoPrimaryKey,
    /// For raw client, operation is not supported in atomic/non-atomic mode.
    #[error(
        "The operation is not supported in current mode, please consider using RawClient with or without atomic mode"
    )]
    UnsupportedMode,
    /// A logical TiKV store has reached the configured in-flight request limit.
    #[error(transparent)]
    TokenLimit(#[from] crate::error::TokenLimitError),
    /// PD's region-metadata circuit breaker is open and rejects this request
    /// without contacting PD.
    #[error("circuit breaker is open")]
    CircuitBreakerOpen,
    #[error("There is no current_regions in the EpochNotMatch error")]
    NoCurrentRegions,
    #[error("The specified entry is not found in the region cache")]
    EntryNotFoundInRegionCache,
    /// Wraps a `std::io::Error`.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Wraps a `std::io::Error`.
    #[error("tokio channel error: {0}")]
    Channel(#[from] tokio::sync::oneshot::error::RecvError),
    /// Wraps a `grpcio::Error`.
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::transport::Error),
    /// Wraps a `reqwest::Error`.
    /// Wraps a `grpcio::Error`.
    #[error("gRPC api error: {0}")]
    GrpcAPI(#[from] tonic::Status),
    /// A transport failure annotated with the TiKV connection-pool identity
    /// that produced it. This is the native counterpart of client-go's
    /// `internal/client.ErrConn` and lets retry paths retire only the stale
    /// pool generation.
    #[error("[{address}]({version}) {source}")]
    Connection {
        #[source]
        source: Box<Error>,
        address: String,
        version: u64,
    },
    /// Wraps a `grpcio::Error`.
    #[error("url error: {0}")]
    Url(#[from] tonic::codegen::http::uri::InvalidUri),
    /// Represents that a futures oneshot channel was cancelled.
    #[error("A futures oneshot channel was canceled. {0}")]
    Canceled(#[from] futures::channel::oneshot::Canceled),
    /// Errors caused by changes of region information
    #[error("Region error: {0:?}")]
    RegionError(Box<ProtoRegionError>),
    /// Whether the transaction is committed or not is undetermined
    #[error("Whether the transaction is committed or not is undetermined")]
    UndeterminedError(Box<Error>),
    /// Wraps a per-key error returned by TiKV.
    #[error("{0:?}")]
    KeyError(Box<ProtoKeyError>),
    /// A malformed memcomparable region key returned by PD or TiKV.
    ///
    /// This preserves client-go's `internal/apicodec.decodeError`
    /// classification while displaying the underlying codec failure unchanged.
    #[error(transparent)]
    ApiCodecDecode(Box<Error>),
    /// Multiple errors generated from the ExtractError plan.
    #[error("Multiple errors: {0:?}")]
    ExtractedErrors(Vec<Error>),
    /// Multiple key errors
    #[error("Multiple key errors: {0:?}")]
    MultipleKeyErrors(Vec<Error>),
    /// Invalid ColumnFamily
    #[error("Unsupported column family {}", _0)]
    ColumnFamilyError(String),
    /// Can't join tokio tasks
    #[error("Failed to join tokio tasks")]
    JoinError(#[from] tokio::task::JoinError),
    /// No region is found for the given key.
    #[error("Region is not found for key: {:?}", key)]
    RegionForKeyNotFound { key: Vec<u8> },
    #[error("Region is not found for range: {:?}", range)]
    RegionForRangeNotFound { range: BoundRange },
    /// No region is found for the given id. note: distinguish it with the RegionNotFound error in errorpb.
    #[error("Region {} is not found in the response", region_id)]
    RegionNotFoundInResponse { region_id: u64 },
    /// No leader is found for the given id.
    #[error("Leader of region {} is not found", region.id)]
    LeaderNotFound { region: RegionVerId },
    /// Scan limit exceeds the maximum
    #[error("Limit {} exceeds max scan limit {}", limit, max_limit)]
    MaxScanLimitExceeded { limit: u32, max_limit: u32 },
    #[error("Invalid Semver string: {0:?}")]
    InvalidSemver(#[from] semver::Error),
    /// A string error returned by TiKV server
    #[error("Kv error. {}", message)]
    KvError { message: String },
    #[error("{}", message)]
    InternalError { message: String },
    #[error("{0}")]
    StringError(String),
    #[error("PessimisticLock error: {:?}", inner)]
    PessimisticLockError {
        inner: Box<Error>,
        success_keys: Vec<Vec<u8>>,
    },
    #[error("Keyspace not found: {0}")]
    KeyspaceNotFound(String),
    #[error("Transaction not found error: {:?}", _0)]
    TxnNotFound(kvrpcpb::TxnNotFound),
    /// Attempted to create or use the sync client (including calling its methods) from within a Tokio async runtime context
    #[error(
        "Nested Tokio runtime detected: cannot use SyncTransactionClient from within an async context. \
Use the async TransactionClient instead, or create and use SyncTransactionClient outside of any Tokio runtime.{0}"
    )]
    NestedRuntimeError(String),
}

impl Error {
    /// Returns the target and generation carried by a transport error.
    pub(crate) fn connection_info(&self) -> Option<(&str, u64)> {
        match self {
            Error::Connection {
                address, version, ..
            } => Some((address, *version)),
            _ => None,
        }
    }
}

impl From<ProtoRegionError> for Error {
    fn from(e: ProtoRegionError) -> Error {
        Error::RegionError(Box::new(e))
    }
}

impl From<ProtoKeyError> for Error {
    fn from(e: ProtoKeyError) -> Error {
        Error::KeyError(Box::new(e))
    }
}

/// A result holding an [`Error`](enum@Error).
pub type Result<T> = result::Result<T, Error>;

#[doc(hidden)]
#[macro_export]
macro_rules! internal_err {
    ($e:expr) => ({
        $crate::Error::InternalError {
            message: format!("[{}:{}]: {}", file!(), line!(),  $e)
        }
    });
    ($f:tt, $($arg:expr),+) => ({
        internal_err!(format!($f, $($arg),+))
    });
}
