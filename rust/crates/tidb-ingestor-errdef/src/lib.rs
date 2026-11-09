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

//! Go `pkg/ingestor/errdef`: the errors of the ingest API.

use std::error::Error;
use std::sync::LazyLock;

use tidb_terror::{TerrorCode, TerrorError};

fn normalize(message: &str, rfc_code: &str) -> TerrorError {
    TerrorError::normalize(message, rfc_code, TerrorCode::new(0))
}

/// Go `ErrNoLeader`.
pub static ERR_NO_LEADER: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("region has no leader, region '%d'", "KV:ErrNoLeader"));
/// Go `ErrKVEpochNotMatch`.
pub static ERR_KV_EPOCH_NOT_MATCH: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("epoch not match", "Ingest:EpochNotMatch"));
/// Go `ErrKVNotLeader`.
pub static ERR_KV_NOT_LEADER: LazyLock<TerrorError> = LazyLock::new(|| normalize("not leader", "Ingest:NotLeader"));
/// Go `ErrKVServerIsBusy`.
pub static ERR_KV_SERVER_IS_BUSY: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("server is busy", "Ingest:ServerIsBusy"));
/// Go `ErrKVRegionNotFound`.
pub static ERR_KV_REGION_NOT_FOUND: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("region not found", "Ingest:RegionNotFound"));
/// Go `ErrKVReadIndexNotReady`.
pub static ERR_KV_READ_INDEX_NOT_READY: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("read index not ready", "Ingest:ReadIndexNotReady"));
/// Go `ErrKVDiskFull`.
pub static ERR_KV_DISK_FULL: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("store disk full", "Ingest:StoreDiskFull"));
/// Go `ErrKVIngestFailed`.
pub static ERR_KV_INGEST_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("ingest tikv failed", "Ingest:ErrKVIngestFailed"));
/// Go `ErrKVRaftProposalDropped`.
pub static ERR_KV_RAFT_PROPOSAL_DROPPED: LazyLock<TerrorError> =
    LazyLock::new(|| normalize("raft proposal dropped", "Ingest:ErrKVRaftProposalDropped"));
/// Go `ErrTooManyDataFiles`.
pub static ERR_TOO_MANY_DATA_FILES: LazyLock<TerrorError> = LazyLock::new(|| {
    normalize(
        "cannot merge %d data files with concurrency %d into at most %d target files",
        "GlobalSort:TooManyDataFiles",
    )
});

/// Go `IsKVDiskFullError`: whether TiKV reporting disk full caused the
/// error.
#[must_use]
pub fn is_kv_disk_full_error(err: &(dyn Error + 'static)) -> bool {
    // Go `goerrors.Is(err, ErrKVDiskFull)`, then `goerrors.As(err, &tErr)` and
    // `errors.Cause`: the first `*errors.Error` along the chain decides.
    let mut cause = Some(err);
    while let Some(error) = cause {
        if let Some(t_err) = error.downcast_ref::<TerrorError>() {
            return t_err.id() == ERR_KV_DISK_FULL.id();
        }
        cause = error.source();
    }
    false
}

/// Go `HTTPStatusError`: a nextgen write or ingest request failed with a
/// non-200 status and no response body to tell the error, which helps decide
/// whether to retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpStatusError {
    /// Go `StatusCode`.
    pub status_code: isize,
    /// Go `Message`.
    pub message: String,
}

impl std::fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request failed with status code {}: {}", self.status_code, self.message)
    }
}

impl Error for HttpStatusError {}

#[cfg(test)]
mod tests {
    use super::{is_kv_disk_full_error, ERR_KV_DISK_FULL, ERR_TOO_MANY_DATA_FILES};

    /// Go `TestTooManyDataFilesErrorContract`.
    #[test]
    fn test_too_many_data_files_error_contract() {
        let err = ERR_TOO_MANY_DATA_FILES.gen_with_stack_by_args(&[10.into(), 2.into(), 3.into()]);
        assert_eq!(
            err.to_string(),
            "[GlobalSort:TooManyDataFiles]cannot merge 10 data files with concurrency 2 into at most 3 target files"
        );
        assert_eq!(ERR_TOO_MANY_DATA_FILES.id(), "GlobalSort:TooManyDataFiles");
    }

    /// Go `IsKVDiskFullError` over the errors the DXF tests build: the error
    /// itself, a generated copy, and one wrapped under an annotation.
    #[test]
    fn is_kv_disk_full_error_follows_the_rfc_code() {
        let generated = ERR_KV_DISK_FULL.gen_with_stack("store 1 disk full");
        assert!(is_kv_disk_full_error(&*ERR_KV_DISK_FULL));
        assert!(is_kv_disk_full_error(&generated));
        assert!(is_kv_disk_full_error(&tidb_terror::annotate(std::sync::Arc::new(generated), "ctx")));
        assert!(!is_kv_disk_full_error(&tidb_terror::TerrorError::normalize("other", "Ingest:Other", tidb_terror::TerrorCode::new(0))));
        assert!(!is_kv_disk_full_error(&std::io::Error::other("store disk full")));
    }
}
