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

//! Source-backed tests for read-only/no-op variable policy.

use tidb_exec::noop_read_only::{
    validate_read_only, NoopDiagnostic, NoopFuncsMode, NoopScope, NoopValidation,
    READ_ONLY_NOOP_VARIABLES,
};
use tidb_exec::sysvar_scope::ScopeFlag;

#[test]
fn read_only_noop_registry_preserves_names_aliases_and_scope() {
    // Source: pkg/sessionctx/variable/noop.go:29-46 and
    // pkg/sessionctx/variable/sysvar_test.go:411-448 (TestReadOnlyNoop).
    assert_eq!(
        READ_ONLY_NOOP_VARIABLES
            .iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec![
            "tx_read_only",
            "transaction_read_only",
            "offline_mode",
            "super_read_only",
            "read_only",
        ]
    );
    assert_eq!(
        READ_ONLY_NOOP_VARIABLES[0].alias,
        Some("transaction_read_only")
    );
    assert_eq!(READ_ONLY_NOOP_VARIABLES[1].alias, Some("tx_read_only"));
    assert_eq!(
        READ_ONLY_NOOP_VARIABLES[0].scope,
        ScopeFlag::GLOBAL | ScopeFlag::SESSION
    );
    assert_eq!(READ_ONLY_NOOP_VARIABLES[2].scope, ScopeFlag::GLOBAL);
    assert!(READ_ONLY_NOOP_VARIABLES[2].offline_mode);
    assert!(!READ_ONLY_NOOP_VARIABLES[4].offline_mode);
}

#[test]
fn read_only_noop_policy_matches_session_and_global_modes() {
    let rejected = validate_read_only(
        "ON",
        "on",
        NoopScope::Session,
        NoopFuncsMode::Off,
        None,
        false,
    );
    assert_eq!(
        rejected,
        NoopValidation::Rejected {
            value: "OFF".to_owned(),
            error: NoopDiagnostic::FunctionsNoopImplementation {
                offline_mode: false,
            },
        }
    );
    assert_eq!(
        NoopDiagnostic::FunctionsNoopImplementation {
            offline_mode: false,
        }
        .message(),
        "function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions"
    );

    assert_eq!(
        validate_read_only(
            "1",
            "1",
            NoopScope::Session,
            NoopFuncsMode::Warn,
            None,
            true,
        ),
        NoopValidation::Accepted {
            value: "1".to_owned(),
            warning: Some(NoopDiagnostic::FunctionsNoopImplementation { offline_mode: true }),
        }
    );
    assert_eq!(
        validate_read_only(
            "ON",
            "ON",
            NoopScope::Global,
            NoopFuncsMode::Off,
            Some(NoopFuncsMode::Warn),
            false,
        ),
        NoopValidation::Accepted {
            value: "ON".to_owned(),
            warning: Some(NoopDiagnostic::FunctionsNoopImplementation {
                offline_mode: false,
            }),
        }
    );
    assert_eq!(
        validate_read_only(
            "ON",
            "ON",
            NoopScope::Global,
            NoopFuncsMode::Off,
            Some(NoopFuncsMode::Off),
            false,
        ),
        NoopValidation::Rejected {
            value: "OFF".to_owned(),
            error: NoopDiagnostic::FunctionsNoopImplementation {
                offline_mode: false,
            },
        }
    );
    assert_eq!(
        validate_read_only(
            "ON",
            "ON",
            NoopScope::Global,
            NoopFuncsMode::Off,
            Some(NoopFuncsMode::Off),
            true,
        ),
        NoopValidation::Rejected {
            value: "OFF".to_owned(),
            error: NoopDiagnostic::FunctionsNoopImplementation { offline_mode: true },
        }
    );
    assert_eq!(
        validate_read_only(
            "ON",
            "ON",
            NoopScope::Global,
            NoopFuncsMode::Off,
            Some(NoopFuncsMode::On),
            false,
        ),
        NoopValidation::Accepted {
            value: "ON".to_owned(),
            warning: None,
        }
    );
    assert_eq!(
        validate_read_only(
            "ON",
            "original",
            NoopScope::Global,
            NoopFuncsMode::Off,
            None,
            false,
        ),
        NoopValidation::Rejected {
            value: "original".to_owned(),
            error: NoopDiagnostic::UnknownSystemVariable,
        }
    );
    assert_eq!(
        validate_read_only(
            "off",
            "off",
            NoopScope::Global,
            NoopFuncsMode::Off,
            Some(NoopFuncsMode::Off),
            false,
        ),
        NoopValidation::Accepted {
            value: "off".to_owned(),
            warning: None,
        }
    );
}
