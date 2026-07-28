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

//! The deployable SQL node's `mysql.user` table: startup account
//! provisioning plus the login path that verifies against it.
//!
//! The account rows themselves live in the shared
//! [`PrivilegeRegistry`][tidb_session::privilege::PrivilegeRegistry] -- the
//! same one `CREATE USER`/`GRANT`/`DROP USER` write and `SHOW GRANTS` reads,
//! exactly as Go has ONE `mysql.user` holding both the
//! `authentication_string` and the privilege columns. This file's strict TSV
//! is therefore only a *provisioning* format (the operator's initial rows),
//! not a separate, immutable catalog: an account `CREATE USER ... IDENTIFIED
//! BY` adds at runtime can log in immediately, and one `DROP USER` removes
//! can no longer log in at all.

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use tidb_session::privilege::PrivilegeRegistry;

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

/// The live `mysql.user` table, plus the login verifier that reads it.
///
/// Cheaply cloneable: every clone is the SAME table (the registry is an
/// `Arc`-shared map), which is what lets the wire front end's authenticator
/// and the session factory's `CREATE USER`/`GRANT` executor be one store.
#[derive(Clone)]
pub struct ConfiguredUserStore {
    accounts: PrivilegeRegistry,
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

    /// Parses strict TSV contents after the file owner validates its
    /// metadata, provisioning one `mysql.user` row per record.
    ///
    /// A record's stored hash must be a strict `*40HEX` stage-two value or
    /// EMPTY, the latter provisioning a passwordless account exactly as
    /// `CREATE USER 'u'@'%'` without `IDENTIFIED BY` does (captured: Go
    /// leaves `authentication_string` empty for such an account).
    pub fn parse(contents: &str) -> Result<Self, ConfiguredUserStoreError> {
        if contents.is_empty() {
            return Err(ConfiguredUserStoreError::EmptyStore);
        }

        let mut seen = HashSet::new();
        let mut provisioned = Vec::new();
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
            if !fields[3].is_empty() && NativePasswordHash::parse(fields[3]).is_err() {
                return Err(ConfiguredUserStoreError::InvalidPasswordHash { line: line_number });
            }
            provisioned.push((
                fields[0].to_owned(),
                fields[1].to_owned(),
                fields[3].to_owned(),
            ));
        }
        if provisioned.is_empty() {
            return Err(ConfiguredUserStoreError::EmptyStore);
        }

        // The file IS this node's bootstrap, so the table holds exactly the
        // accounts it lists: a node whose file omits `root`@`%` has no root
        // row, and therefore no implicitly passwordless root login.
        Ok(Self {
            accounts: PrivilegeRegistry::bootstrapped_from(provisioned),
        })
    }

    /// The live account table, to be shared with the session factory so that
    /// `CREATE USER`/`DROP USER` and the login path see one set of rows.
    #[must_use]
    pub fn accounts(&self) -> PrivilegeRegistry {
        self.accounts.clone()
    }

    /// Returns the number of account rows currently in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        self.accounts.accounts().len()
    }

    /// Returns whether the table has no account rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resolves and verifies one native-password response against the LIVE
    /// account table.
    ///
    /// An unknown user or host still executes the native verifier against a
    /// dummy hash before returning `None`. The successful result contains the
    /// canonical stored host pattern, not the client-supplied host.
    ///
    /// A passwordless account (empty `authentication_string`) authenticates
    /// only on an empty auth response, which is what a client sends when the
    /// user supplies no password.
    #[must_use]
    pub fn authenticate_native(
        &self,
        username: &str,
        remote_host: &str,
        salt: &[u8],
        response: &[u8],
    ) -> Option<AuthenticatedIdentity> {
        let catalog = IdentityCatalog::new(
            self.accounts
                .accounts()
                .into_iter()
                .map(|(user, host)| MatchedIdentity::new(&user, &host)),
        );
        let request = IdentityLookupRequest::new(username, remote_host, true);
        let identity = match catalog.resolve(&request, &[]) {
            IdentityLookupResult::Matched(identity) => Some(identity),
            IdentityLookupResult::Bypassed(_) | IdentityLookupResult::NotFound => None,
        };
        // A ROLE is a `mysql.user` row with `account_locked = 'Y'` and an
        // empty password, so without this it would be the most loginable
        // account on the server. Go refuses a locked account at the same
        // point, before any password comparison.
        if identity
            .as_ref()
            .is_some_and(|identity| self.accounts.is_role(identity.username(), identity.host()))
        {
            return None;
        }
        let stored = identity.as_ref().and_then(|identity| {
            self.accounts
                .auth_string(identity.username(), identity.host())
        });

        // Three outcomes, not two: no such account, an account with no
        // password, and an account with a stored hash.
        let verified = match stored.as_deref() {
            None => verify_candidate(None, salt, response),
            Some("") => response.is_empty(),
            Some(encoded) => verify_candidate(
                NativePasswordHash::parse(encoded).ok().as_ref(),
                salt,
                response,
            ),
        };
        if verified {
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
            .field("account_count", &self.len())
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
    /// A stored password is neither empty nor a strict native stage-two
    /// hash.
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
