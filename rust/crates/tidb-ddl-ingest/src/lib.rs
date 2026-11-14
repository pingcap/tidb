// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest`.

pub mod backend;
pub mod checkpoint;
pub mod config;
pub mod disk_root;
pub mod engine;
pub mod env;
pub mod mem_root;

/// Errors retain Go's distinction between retryable IO/probe failures and
/// `dbterror.ErrIngestCheckEnvFailed` admission failures.
#[derive(Debug)]
pub enum Error {
    /// Go filesystem, PD, session, or backend error.
    Other(String),
    /// Go `terror.Error`.
    Database(tidb_terror::TerrorError),
    /// Go local backend failure.
    Backend(tidb_ingestor_ingestctrl::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(e) => f.write_str(e),
            Self::Database(e) => e.fmt(f),
            Self::Backend(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(e) => Some(e),
            Self::Backend(e) => Some(e),
            _ => None,
        }
    }
}
impl From<tidb_ingestor_ingestctrl::Error> for Error {
    fn from(e: tidb_ingestor_ingestctrl::Error) -> Self {
        Self::Backend(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Other(e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Other(e.to_string())
    }
}
/// Go ingest operation result.
pub type Result<T> = std::result::Result<T, Error>;

/// Go `dbterror.ErrIngestCheckEnvFailed.FastGenByArgs`.
fn check_env_error(message: impl Into<String>) -> Error {
    Error::Database(
        tidb_dbterror::ERR_INGEST_CHECK_ENV_FAILED.gen_with_stack_by_args(&[message.into().into()]),
    )
}

pub mod backend_mgr;

pub mod util;

pub mod mock;
