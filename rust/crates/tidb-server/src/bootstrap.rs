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

//! Source-shaped bootstrap decisions for the session/server boundary.
//!
//! This module deliberately stops at pure metadata.  TiDB's Go bootstrap
//! path (`pkg/session/session.go:4312-4657` and
//! `pkg/session/bootstrap.go:64-97`) performs the actual work through KV,
//! domain/DDL ownership, system-table mutation, privilege loading, and many
//! background services.  None of those side effects can be represented by a
//! dependency-free Rust helper without inventing a storage or cluster
//! contract.  The types here therefore own only the decisions that are
//! stable before those services are constructed: the start mode, the
//! user-keyspace/system-keyspace version guard, feature-gate outcomes, and a
//! coarse source order for the later owner to implement.

/// The version stored in an unbootstrapped keyspace.
pub const NOT_BOOTSTRAPPED: i64 = 0;

/// The bootstrap mode selected from a keyspace's current and target versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapMode {
    /// The keyspace has no bootstrap version and needs initial system setup.
    Bootstrap,
    /// The keyspace is older than the target and needs rolling-upgrade work.
    Upgrade,
    /// The keyspace is already at (or beyond) the target version.
    Normal,
}

/// Selects the mode used by Go's `getStartMode` without touching storage.
#[must_use]
pub const fn start_mode(current_version: i64, target_version: i64) -> BootstrapMode {
    if current_version == NOT_BOOTSTRAPPED {
        BootstrapMode::Bootstrap
    } else if current_version < target_version {
        BootstrapMode::Upgrade
    } else {
        BootstrapMode::Normal
    }
}

/// Why a user-keyspace bootstrap decision could not be admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapDecisionError {
    /// The system keyspace has not been bootstrapped yet.
    SystemKeyspaceNotBootstrapped,
    /// The system keyspace is behind the target version required by the user
    /// keyspace.
    SystemKeyspaceBehind {
        /// The target version the user keyspace is trying to reach.
        target_version: i64,
        /// The version currently recorded by the system keyspace.
        system_version: i64,
    },
}

/// Applies the user-keyspace guard from `bootstrapSessionImpl` and selects a
/// start mode.  `system_version` is `None` for a normal/local keyspace, where
/// Go does not perform the cross-keyspace check.
pub const fn decide_start_mode(
    current_version: i64,
    target_version: i64,
    system_version: Option<i64>,
) -> Result<BootstrapMode, BootstrapDecisionError> {
    if let Some(system_version) = system_version {
        if system_version == NOT_BOOTSTRAPPED {
            return Err(BootstrapDecisionError::SystemKeyspaceNotBootstrapped);
        }
        if target_version > system_version {
            return Err(BootstrapDecisionError::SystemKeyspaceBehind {
                target_version,
                system_version,
            });
        }
    }
    Ok(start_mode(current_version, target_version))
}

/// Process-wide switches that affect bootstrap-time service admission.
///
/// These booleans are intentionally facts supplied by the caller.  They do
/// not load plugins, inspect configuration files, read the current OS user,
/// or start any service.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BootstrapFeatureGates {
    next_gen_kernel: bool,
    skip_grant_table: bool,
    secure_bootstrap: bool,
    plugins_requested: bool,
    telemetry_enabled: bool,
    bootstrap_sql_file_requested: bool,
    disaggregated_tiflash: bool,
    use_autoscaler: bool,
    etcd_backend: bool,
}

impl BootstrapFeatureGates {
    /// Creates feature facts corresponding to the gates in
    /// `bootstrapSessionImpl`.
    // Keep the source's independent process gates in one constructor rather
    // than hiding them behind a loosely typed map or a fake global config.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        next_gen_kernel: bool,
        skip_grant_table: bool,
        plugins_requested: bool,
        telemetry_enabled: bool,
        bootstrap_sql_file_requested: bool,
        disaggregated_tiflash: bool,
        use_autoscaler: bool,
        etcd_backend: bool,
    ) -> Self {
        Self {
            next_gen_kernel,
            skip_grant_table,
            secure_bootstrap: false,
            plugins_requested,
            telemetry_enabled,
            bootstrap_sql_file_requested,
            disaggregated_tiflash,
            use_autoscaler,
            etcd_backend,
        }
    }

    /// Returns whether next-generation system schemas are prepared through the
    /// metadata path before DDL tables.
    #[must_use]
    pub const fn next_gen_kernel(self) -> bool {
        self.next_gen_kernel
    }

    /// Returns whether the privilege-loading loop is skipped.
    #[must_use]
    pub const fn skip_grant_table(self) -> bool {
        self.skip_grant_table
    }

    /// Returns whether the secure-bootstrap root-account branch is selected.
    /// The current user lookup and `auth_socket` row write remain external.
    #[must_use]
    pub const fn secure_bootstrap(self) -> bool {
        self.secure_bootstrap
    }

    /// Returns these gates with the secure-bootstrap root-account branch
    /// selected or cleared.
    #[must_use]
    pub const fn with_secure_bootstrap(mut self, secure_bootstrap: bool) -> Self {
        self.secure_bootstrap = secure_bootstrap;
        self
    }

    /// Returns whether configured plugins need loading/initialization.
    #[must_use]
    pub const fn plugins_requested(self) -> bool {
        self.plugins_requested
    }

    /// Returns whether the telemetry loop is admitted after domain startup.
    #[must_use]
    pub const fn telemetry_enabled(self) -> bool {
        self.telemetry_enabled
    }

    /// Returns whether the post-bootstrap SQL file stage was requested.
    #[must_use]
    pub const fn bootstrap_sql_file_requested(self) -> bool {
        self.bootstrap_sql_file_requested
    }

    /// Returns whether the TiFlash compute-node watcher is admitted.
    #[must_use]
    pub const fn watch_disaggregated_tiflash(self) -> bool {
        self.disaggregated_tiflash && !self.use_autoscaler
    }

    /// Returns whether the external-store GC worker is admitted.
    #[must_use]
    pub const fn etcd_backend(self) -> bool {
        self.etcd_backend
    }

    /// Returns whether the privilege-loading loop is admitted.
    #[must_use]
    pub const fn load_privileges(self) -> bool {
        !self.skip_grant_table
    }
}

/// Coarse source order for bootstrap decisions and service owners.
///
/// This is an audit/decomposition contract, not an executable scheduler.  A
/// future storage/domain owner may attach effects to these markers only after
/// it has ported the corresponding Go dependency and tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapPhase {
    /// Read the current bootstrap version from the store.
    ReadStoreVersion,
    /// Validate user-keyspace versus system-keyspace version ordering.
    ValidateKeyspaceVersion,
    /// Load configured plugins before schema/session construction.
    LoadPlugins,
    /// Prepare next-generation system schemas when that kernel is enabled.
    PrepareNextGenSchemas,
    /// Prepare DDL tables and schema-cache metadata.
    PrepareDdlTables,
    /// Run bootstrap, upgrade, or normal MDL initialization for the version.
    RunStartMode,
    /// Persist the bootstrap version and release the temporary bootstrap
    /// domain/session resources. This marker applies to bootstrap/upgrade;
    /// normal startup performs MDL initialization in the preceding stage.
    FinishBootstrap,
    /// Initialize global variables needed by sessions.
    InitializeGlobalVariables,
    /// Create restricted bootstrap sessions and start the domain.
    CreateSessionsAndStartDomain,
    /// Load privilege/sysvar/binding services after domain startup.
    LoadDomainServices,
    /// Start optional workers, including the bootstrap SQL-file stage.
    StartOptionalWorkers,
}

/// Stable coarse order copied from `bootstrapSessionImpl` and
/// `runInBootstrapSession`.
pub const BOOTSTRAP_PHASE_ORDER: &[BootstrapPhase] = &[
    BootstrapPhase::ReadStoreVersion,
    BootstrapPhase::ValidateKeyspaceVersion,
    BootstrapPhase::LoadPlugins,
    BootstrapPhase::PrepareNextGenSchemas,
    BootstrapPhase::PrepareDdlTables,
    BootstrapPhase::RunStartMode,
    BootstrapPhase::FinishBootstrap,
    BootstrapPhase::InitializeGlobalVariables,
    BootstrapPhase::CreateSessionsAndStartDomain,
    BootstrapPhase::LoadDomainServices,
    BootstrapPhase::StartOptionalWorkers,
];
