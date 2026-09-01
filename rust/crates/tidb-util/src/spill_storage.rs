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

//! Native integration seam for Go's startup temp-directory, quota, global
//! disk-tracker, spilled-file encryption, and `os.CreateTemp` behavior.
//!
//! Directory locking and stale-file cleanup remain in [`crate::disk`], exactly
//! where Go owns them.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::{
    memory::{Tracker, LABEL_FOR_GLOBAL_STORAGE},
    sys::storage::get_target_directory_capacity,
};

/// The accepted MySQL-visible error text for local temporary-space exhaustion.
pub const LOCAL_TEMPORARY_SPACE_QUOTA_ERROR: &str = "Out Of Quota For Local Temporary Space!";

/// The configured spill-file encryption method.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SpillEncryptionMethod {
    /// Checksum framing over plaintext bytes.
    #[default]
    Plaintext,
    /// Checksum framing over AES-128-CTR encrypted bytes.
    Aes128Ctr,
}

impl SpillEncryptionMethod {
    /// The normalized TiDB config spelling.
    #[must_use]
    pub const fn as_config_value(self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::Aes128Ctr => "aes128-ctr",
        }
    }
}

/// An invalid `security.spilled-file-encryption-method` value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillEncryptionParseError {
    value: String,
}

impl std::fmt::Display for SpillEncryptionParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "unsupported [security]spilled-file-encryption-method {}",
            self.value
        )
    }
}

impl std::error::Error for SpillEncryptionParseError {}

impl FromStr for SpillEncryptionMethod {
    type Err = SpillEncryptionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("plaintext") {
            Ok(Self::Plaintext)
        } else if value.eq_ignore_ascii_case("aes128-ctr") {
            Ok(Self::Aes128Ctr)
        } else {
            Err(SpillEncryptionParseError {
                value: value.to_owned(),
            })
        }
    }
}

/// Fully resolved immutable startup policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillStorageSpec {
    /// Final directory after endpoint/UID path derivation.
    pub path: PathBuf,
    /// Process-wide disk quota. Values `<= 0` are unlimited at query time;
    /// every nonnegative value is still checked against filesystem capacity.
    pub quota_bytes: i64,
    /// Encryption applied to every file created by this authority.
    pub encryption: SpillEncryptionMethod,
}

/// Failure to validate the configured temporary-storage quota.
#[derive(Debug)]
pub enum SpillStorageOpenError {
    /// The configured quota is larger than space available to this process.
    QuotaExceedsAvailable {
        /// Configured storage path.
        path: PathBuf,
        /// Requested startup quota.
        quota_bytes: i64,
        /// Bytes available to an unprivileged process.
        available_bytes: u64,
    },
    /// Filesystem operation failure with its exact target.
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Filesystem target of the operation.
        path: PathBuf,
        /// Underlying operating-system error.
        source: io::Error,
    },
}

impl std::fmt::Display for SpillStorageOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QuotaExceedsAvailable {
                path,
                quota_bytes,
                available_bytes,
            } => write!(
                formatter,
                "tmp-storage-quota {quota_bytes} exceeds available space {available_bytes} at {}",
                path.display()
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl std::error::Error for SpillStorageOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::QuotaExceedsAvailable { .. } => None,
        }
    }
}

/// One process-wide spill-storage authority.
pub struct SpillStorage {
    path: PathBuf,
    quota_bytes: i64,
    encryption: SpillEncryptionMethod,
    global_tracker: Arc<Tracker>,
}

impl SpillStorage {
    /// Applies Go's startup capacity check and constructs the global tracker.
    /// Directory initialization is owned by [`crate::disk::initialize_temp_dir`].
    pub fn open(spec: SpillStorageSpec) -> Result<Self, SpillStorageOpenError> {
        if spec.quota_bytes >= 0 {
            let available_bytes = get_target_directory_capacity(&spec.path).map_err(|source| {
                SpillStorageOpenError::Io {
                    operation: "read temporary storage capacity",
                    path: spec.path.clone(),
                    source,
                }
            })?;
            if u64::try_from(spec.quota_bytes).is_ok_and(|quota| quota > available_bytes) {
                return Err(SpillStorageOpenError::QuotaExceedsAvailable {
                    path: spec.path,
                    quota_bytes: spec.quota_bytes,
                    available_bytes,
                });
            }
        }

        let global_tracker =
            crate::disk::new_global_tracker(LABEL_FOR_GLOBAL_STORAGE, spec.quota_bytes);
        global_tracker.set_bytes_limit(spec.quota_bytes);
        Ok(Self {
            path: spec.path,
            quota_bytes: spec.quota_bytes,
            encryption: spec.encryption,
            global_tracker,
        })
    }

    /// The final configured spill directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The configured process quota.
    #[must_use]
    pub const fn quota_bytes(&self) -> i64 {
        self.quota_bytes
    }

    /// The immutable file-encryption policy.
    #[must_use]
    pub const fn encryption(&self) -> SpillEncryptionMethod {
        self.encryption
    }

    /// Process-global disk tracker to which statement disk roots attach.
    #[must_use]
    pub fn global_tracker(&self) -> &Arc<Tracker> {
        &self.global_tracker
    }

    /// Creates one private, atomically unique spill file in this authority.
    pub fn create_file(&self, prefix: &str) -> io::Result<(File, PathBuf)> {
        tempfile::Builder::new()
            .prefix(prefix)
            .tempfile_in(&self.path)?
            .keep()
            .map_err(Into::into)
    }
}
