// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl`.
pub mod compress;
pub mod disk_quota;
pub mod localhelper;
pub mod rate_limiter;
pub mod rate_limiter_param;

pub mod engine;

/// Go backend errors, preserving typed disk-full and duplicate-key causes.
#[derive(Debug, Clone)]
pub enum Error {
    /// Go IO, transport or iterator error.
    Other(String),
    /// Go errors preserving the transport cause for retry classification.
    Source(std::sync::Arc<dyn std::error::Error + Send + Sync>),
    /// Go `ingestcli.IngestAPIError.NewRegion`.
    IngestRegion {
        /// Go `ingestcli.IngestAPIError` cause.
        error: tidb_terror::TerrorError,
        /// Go `ingestcli.IngestAPIError.NewRegion`.
        region: crate::region_job::RegionInfo,
    },
    /// Go `common.ErrFoundDuplicateKeys`, retaining the original index KV.
    Duplicate {
        /// Go `ErrFoundDuplicateKeys` key argument.
        key: Vec<u8>,
        /// Go `ErrFoundDuplicateKeys` value argument.
        value: Vec<u8>,
    },
    /// Go ingest error identity.
    Ingest(tidb_terror::TerrorError),
}
impl std::fmt::Display for Error {
    /// Go `error.Error`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(e) => f.write_str(e),
            Self::Source(e) => e.fmt(f),
            Self::IngestRegion { error, .. } => error.fmt(f),
            Self::Duplicate { .. } => f.write_str("found duplicate keys"),
            Self::Ingest(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for Error {
    /// Go `errors.Cause`.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Ingest(e) => Some(e),
            Self::Source(e) => Some(e.as_ref()),
            Self::IngestRegion { error, .. } => Some(error),
            _ => None,
        }
    }
}
impl From<std::io::Error> for Error {
    /// Go `errors.Trace`.
    fn from(e: std::io::Error) -> Self {
        Self::Source(std::sync::Arc::new(e))
    }
}
impl From<tidb_extsort::Error> for Error {
    /// Go `errors.Trace`.
    fn from(e: tidb_extsort::Error) -> Self {
        Self::Other(e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    /// Go `errors.Trace`.
    fn from(e: serde_json::Error) -> Self {
        Self::Other(e.to_string())
    }
}
/// Go ingest operation result.
pub type Result<T> = std::result::Result<T, Error>;

pub mod local;
pub mod region_job;

pub mod duplicate;

pub mod engine_mgr;
pub mod iterator;

pub mod checksum;
pub mod job_worker;
pub mod tikv_mode;
