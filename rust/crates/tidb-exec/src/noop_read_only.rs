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

//! Read-only/no-op system-variable policy from `pkg/sessionctx/variable/noop.go`.
//!
//! TiDB keeps a small group of MySQL compatibility variables writable even
//! though their implementation is a no-op. Enabling one requires
//! `tidb_enable_noop_functions` in the same scope. This leaf preserves the
//! registration names, aliases, scope metadata, ON/1 gate, OFF/ON/WARN
//! behavior, and the distinct OFFLINE MODE diagnostic. SysVar registration,
//! SessionVars mutation, global-variable access, and warning/error plumbing
//! remain external.

use crate::sysvar_scope::ScopeFlag;

/// The scope in which a no-op variable is being validated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoopScope {
    /// Validate a session-local assignment.
    Session,
    /// Validate a global assignment.
    Global,
}

/// Effective value of `tidb_enable_noop_functions`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoopFuncsMode {
    /// Reject enabling the no-op variable.
    Off,
    /// Accept enabling the no-op variable.
    On,
    /// Accept it while publishing a warning.
    Warn,
}

/// Compatibility metadata for one of the read-only/no-op variables.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NoopReadOnlyVariable {
    /// Canonical system-variable name.
    pub name: &'static str,
    /// The alternate spelling, when the source registration has one.
    pub alias: Option<&'static str>,
    /// Dynamic scopes accepted by the source registration.
    pub scope: ScopeFlag,
    /// Whether diagnostics name OFFLINE MODE instead of READ ONLY.
    pub offline_mode: bool,
}

/// The five read-only/no-op registrations at the head of `noopSysVars`.
pub const READ_ONLY_NOOP_VARIABLES: [NoopReadOnlyVariable; 5] = [
    NoopReadOnlyVariable {
        name: "tx_read_only",
        alias: Some("transaction_read_only"),
        scope: ScopeFlag::from_bits(ScopeFlag::GLOBAL.bits() | ScopeFlag::SESSION.bits()),
        offline_mode: false,
    },
    NoopReadOnlyVariable {
        name: "transaction_read_only",
        alias: Some("tx_read_only"),
        scope: ScopeFlag::from_bits(ScopeFlag::GLOBAL.bits() | ScopeFlag::SESSION.bits()),
        offline_mode: false,
    },
    NoopReadOnlyVariable {
        name: "offline_mode",
        alias: None,
        scope: ScopeFlag::GLOBAL,
        offline_mode: true,
    },
    NoopReadOnlyVariable {
        name: "super_read_only",
        alias: None,
        scope: ScopeFlag::GLOBAL,
        offline_mode: false,
    },
    NoopReadOnlyVariable {
        name: "read_only",
        alias: None,
        scope: ScopeFlag::GLOBAL,
        offline_mode: false,
    },
];

/// Diagnostic emitted by the no-op read-only policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoopDiagnostic {
    /// The variable is accepted but its implementation is a no-op.
    FunctionsNoopImplementation {
        /// Whether the diagnostic uses the OFFLINE MODE wording.
        offline_mode: bool,
    },
    /// The global accessor did not expose `tidb_enable_noop_functions`.
    UnknownSystemVariable,
}

impl NoopDiagnostic {
    /// Returns the source-compatible diagnostic text.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::FunctionsNoopImplementation { offline_mode: true } => {
                "function OFFLINE MODE has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions"
            }
            Self::FunctionsNoopImplementation { offline_mode: false } => {
                "function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions"
            }
            Self::UnknownSystemVariable => {
                "Unknown system variable 'tidb_enable_noop_functions'"
            }
        }
    }
}

/// Result of validating a read-only/no-op assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NoopValidation {
    /// The normalized value is accepted, optionally with a warning.
    Accepted {
        /// The normalized assignment value.
        value: String,
        /// The optional compatibility warning.
        warning: Option<NoopDiagnostic>,
    },
    /// The value is rejected; `value` is the value returned by the Go helper.
    Rejected {
        /// The normalized value returned alongside the error.
        value: String,
        /// The source-compatible rejection diagnostic.
        error: NoopDiagnostic,
    },
}

/// Applies the `checkReadOnly` ON/1 and OFF/ON/WARN policy.
///
/// `global_mode == None` models a global accessor error. For a session scope,
/// the `session_mode` is consulted and `global_mode` is ignored. Values other
/// than ON (case-insensitive) or `1` bypass the no-op gate unchanged, matching
/// `TiDBOptOn` in the source.
#[must_use]
pub fn validate_read_only(
    normalized_value: &str,
    original_value: &str,
    scope: NoopScope,
    session_mode: NoopFuncsMode,
    global_mode: Option<NoopFuncsMode>,
    offline_mode: bool,
) -> NoopValidation {
    if !(normalized_value.eq_ignore_ascii_case("ON") || normalized_value == "1") {
        return NoopValidation::Accepted {
            value: normalized_value.to_owned(),
            warning: None,
        };
    }

    let diagnostic = NoopDiagnostic::FunctionsNoopImplementation { offline_mode };
    match scope {
        NoopScope::Session => match session_mode {
            NoopFuncsMode::Off => NoopValidation::Rejected {
                value: "OFF".to_owned(),
                error: diagnostic,
            },
            NoopFuncsMode::On => NoopValidation::Accepted {
                value: normalized_value.to_owned(),
                warning: None,
            },
            NoopFuncsMode::Warn => NoopValidation::Accepted {
                value: normalized_value.to_owned(),
                warning: Some(diagnostic),
            },
        },
        NoopScope::Global => match global_mode {
            None => NoopValidation::Rejected {
                value: original_value.to_owned(),
                error: NoopDiagnostic::UnknownSystemVariable,
            },
            Some(NoopFuncsMode::Off) => NoopValidation::Rejected {
                value: "OFF".to_owned(),
                error: diagnostic,
            },
            Some(NoopFuncsMode::On) => NoopValidation::Accepted {
                value: normalized_value.to_owned(),
                warning: None,
            },
            Some(NoopFuncsMode::Warn) => NoopValidation::Accepted {
                value: normalized_value.to_owned(),
                warning: Some(diagnostic),
            },
        },
    }
}
