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

//! Immutable startup user store for the deployable loopback SQL node.

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::auth_identity::{
    IdentityCatalog, IdentityLookupRequest, IdentityLookupResult, MatchedIdentity,
    DEFAULT_AUTH_PLUGIN,
};
use crate::native_password::{verify_candidate, NativePasswordHash};

/// Canonical identity established only after password verification succeeds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedIdentity {
    identity: MatchedIdentity,
}

impl AuthenticatedIdentity {
    /// Canonical username selected by TiDB-compatible host matching.
    #[must_use]
    pub fn username(&self) -> &str {
        self.identity.username()
    }

    /// Canonical host pattern selected by TiDB-compatible host matching.
    #[must_use]
    pub fn host(&self) -> &str {
        self.identity.host()
    }

    /// Configured plugin verified for this milestone.
    #[must_use]
    pub const fn auth_plugin(&self) -> &'static str {
        DEFAULT_AUTH_PLUGIN
    }

    /// Canonical identity for session metadata owners.
    #[must_use]
    pub const fn matched_identity(&self) -> &MatchedIdentity {
        &self.identity
    }
}

struct ConfiguredUser {
    identity: MatchedIdentity,
    password_hash: NativePasswordHash,
}

/// Host-aware native-password account catalog loaded once at startup.
pub struct ConfiguredUserStore {
    identities: IdentityCatalog,
    users: Vec<ConfiguredUser>,
}

impl ConfiguredUserStore {
    /// Opens and parses one strict TSV auth file.
    ///
    /// Each record is
    /// `username<TAB>host<TAB>mysql_native_password<TAB>*40HEX`. On Unix the
    /// already-opened descriptor must be a regular file with mode exactly
    /// `0600`; validation and reading use the same descriptor.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfiguredUserStoreError> {
        let mut file = File::open(path).map_err(ConfiguredUserStoreError::Io)?;
        let metadata = file.metadata().map_err(ConfiguredUserStoreError::Io)?;
        if !metadata.file_type().is_file() {
            return Err(ConfiguredUserStoreError::NotRegularFile);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if metadata.permissions().mode() & 0o7777 != 0o600 {
                return Err(ConfiguredUserStoreError::InvalidPermissions);
            }
            if metadata.uid() != rustix::process::getuid().as_raw() {
                return Err(ConfiguredUserStoreError::InvalidOwner);
            }
        }

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(ConfiguredUserStoreError::Io)?;
        Self::parse(&contents)
    }

    /// Parses strict TSV contents after the file owner validates its metadata.
    pub fn parse(contents: &str) -> Result<Self, ConfiguredUserStoreError> {
        if contents.is_empty() {
            return Err(ConfiguredUserStoreError::EmptyStore);
        }

        let mut users = Vec::new();
        let mut identities = Vec::new();
        let mut seen = HashSet::new();
        for (index, line) in contents.lines().enumerate() {
            let line_number = index + 1;
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 4 || fields[0].is_empty() || fields[1].is_empty() {
                return Err(ConfiguredUserStoreError::MalformedRecord { line: line_number });
            }
            if fields[2] != DEFAULT_AUTH_PLUGIN {
                return Err(ConfiguredUserStoreError::UnsupportedPlugin { line: line_number });
            }
            if fields.iter().any(|field| {
                field
                    .as_bytes()
                    .iter()
                    .any(|byte| matches!(byte, b'\r' | b'\n' | b'\0'))
            }) {
                return Err(ConfiguredUserStoreError::MalformedRecord { line: line_number });
            }

            let duplicate_key = (fields[0].to_owned(), fields[1].to_owned());
            if !seen.insert(duplicate_key) {
                return Err(ConfiguredUserStoreError::DuplicateIdentity { line: line_number });
            }
            let password_hash = NativePasswordHash::parse(fields[3])
                .map_err(|_| ConfiguredUserStoreError::InvalidPasswordHash { line: line_number })?;
            let identity = MatchedIdentity::new(fields[0], fields[1]);
            identities.push(identity.clone());
            users.push(ConfiguredUser {
                identity,
                password_hash,
            });
        }
        if users.is_empty() {
            return Err(ConfiguredUserStoreError::EmptyStore);
        }

        Ok(Self {
            identities: IdentityCatalog::new(identities),
            users,
        })
    }

    /// Returns the number of immutable configured account rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.users.len()
    }

    /// Returns whether the catalog has no account rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    /// Resolves and verifies one native-password response.
    ///
    /// An unknown user or host still executes the native verifier against a
    /// dummy hash before returning `None`. The successful result contains the
    /// canonical configured host pattern, not the client-supplied host.
    #[must_use]
    pub fn authenticate_native(
        &self,
        username: &str,
        remote_host: &str,
        salt: &[u8],
        response: &[u8],
    ) -> Option<AuthenticatedIdentity> {
        let request = IdentityLookupRequest::new(username, remote_host, true);
        let identity = match self.identities.resolve(&request, &[]) {
            IdentityLookupResult::Matched(identity) => Some(identity),
            IdentityLookupResult::Bypassed(_) | IdentityLookupResult::NotFound => None,
        };
        let user = identity.as_ref().and_then(|identity| {
            self.users.iter().find(|user| {
                user.identity.username() == identity.username()
                    && user.identity.host() == identity.host()
            })
        });

        if verify_candidate(user.map(|user| &user.password_hash), salt, response) {
            identity.map(|identity| AuthenticatedIdentity { identity })
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ConfiguredUserStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfiguredUserStore")
            .field("account_count", &self.users.len())
            .finish_non_exhaustive()
    }
}

/// Safe startup auth-file failure categories.
#[derive(Debug)]
pub enum ConfiguredUserStoreError {
    /// The file could not be opened, inspected, or read.
    Io(std::io::Error),
    /// The opened descriptor is not a regular file.
    NotRegularFile,
    /// Unix secret-file permissions are not exactly `0600`.
    InvalidPermissions,
    /// The Unix secret file is not owned by the effective process user.
    InvalidOwner,
    /// No account rows were present.
    EmptyStore,
    /// A record does not have four nonempty required fields.
    MalformedRecord {
        /// One-based record line.
        line: usize,
    },
    /// A record selects a plugin outside this milestone.
    UnsupportedPlugin {
        /// One-based record line.
        line: usize,
    },
    /// An exact username/host row occurs more than once.
    DuplicateIdentity {
        /// One-based record line.
        line: usize,
    },
    /// A stored password is not a strict native stage-two hash.
    InvalidPasswordHash {
        /// One-based record line.
        line: usize,
    },
}

impl std::fmt::Display for ConfiguredUserStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "could not read authentication file: {error}"),
            Self::NotRegularFile => formatter.write_str("authentication file is not regular"),
            Self::InvalidPermissions => {
                formatter.write_str("authentication file permissions must be 0600")
            }
            Self::InvalidOwner => {
                formatter.write_str("authentication file must be owned by the process user")
            }
            Self::EmptyStore => formatter.write_str("authentication file contains no accounts"),
            Self::MalformedRecord { line } => {
                write!(formatter, "authentication file record {line} is malformed")
            }
            Self::UnsupportedPlugin { line } => write!(
                formatter,
                "authentication file record {line} uses an unsupported plugin"
            ),
            Self::DuplicateIdentity { line } => write!(
                formatter,
                "authentication file record {line} duplicates an identity"
            ),
            Self::InvalidPasswordHash { line } => write!(
                formatter,
                "authentication file record {line} has an invalid password hash"
            ),
        }
    }
}

impl std::error::Error for ConfiguredUserStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
