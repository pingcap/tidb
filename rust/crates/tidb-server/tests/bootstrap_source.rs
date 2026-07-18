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

#![allow(missing_docs)]

use tidb_server::{
    decide_start_mode, start_mode, BootstrapDecisionError, BootstrapFeatureGates, BootstrapMode,
    BootstrapPhase, BOOTSTRAP_PHASE_ORDER, NOT_BOOTSTRAPPED,
};

#[test]
fn start_mode_preserves_go_version_boundaries() {
    // Source: pkg/session/session.go:4576-4584 and
    // pkg/session/bootstrap_test.go:329-426. The zero version is the only
    // bootstrap sentinel; positive versions below target upgrade, while the
    // target and future versions use normal startup.
    assert_eq!(start_mode(NOT_BOOTSTRAPPED, 263), BootstrapMode::Bootstrap);
    assert_eq!(start_mode(1, 263), BootstrapMode::Upgrade);
    assert_eq!(start_mode(263, 263), BootstrapMode::Normal);
    assert_eq!(start_mode(264, 263), BootstrapMode::Normal);
}

#[test]
fn user_keyspace_guard_matches_bootstrap_session_ordering() {
    // Source: pkg/session/session.go:4313-4324. User keyspaces require an
    // already bootstrapped SYSTEM keyspace and may not target a version newer
    // than SYSTEM. Local keyspaces skip this cross-keyspace check.
    assert_eq!(decide_start_mode(3, 263, None), Ok(BootstrapMode::Upgrade));
    assert_eq!(
        decide_start_mode(3, 263, Some(NOT_BOOTSTRAPPED)),
        Err(BootstrapDecisionError::SystemKeyspaceNotBootstrapped)
    );
    assert_eq!(
        decide_start_mode(3, 263, Some(262)),
        Err(BootstrapDecisionError::SystemKeyspaceBehind {
            target_version: 263,
            system_version: 262,
        })
    );
    assert_eq!(
        decide_start_mode(3, 263, Some(263)),
        Ok(BootstrapMode::Upgrade)
    );
}

#[test]
fn feature_gates_preserve_source_conjunctions() {
    // Source: pkg/session/session.go:4328-4341, 4439-4514, and
    // 4470-4550. This is only the pure admission result: plugin loading,
    // privilege/sysvar/background workers, and SQL-file execution still need
    // their storage/domain owners.
    let gates = BootstrapFeatureGates::new(true, false, true, true, true, true, false, true)
        .with_secure_bootstrap(true);
    assert!(gates.next_gen_kernel());
    assert!(gates.plugins_requested());
    assert!(gates.secure_bootstrap());
    assert!(gates.load_privileges());
    assert!(gates.telemetry_enabled());
    assert!(gates.bootstrap_sql_file_requested());
    assert!(gates.watch_disaggregated_tiflash());
    assert!(gates.etcd_backend());

    let autoscaled = BootstrapFeatureGates::new(true, true, false, false, false, true, true, false);
    assert!(!autoscaled.load_privileges());
    assert!(!autoscaled.watch_disaggregated_tiflash());
    assert!(!autoscaled.secure_bootstrap());
}

#[test]
fn coarse_phase_order_keeps_storage_effects_out_of_the_decision_leaf() {
    // Source: pkg/session/session.go:4312-4501 and 4590-4657. The phase list
    // is an audit contract for parallel ownership, not an executable bootstrap
    // loop; effects remain external until KV/domain/DDL seams are ported.
    assert_eq!(
        BOOTSTRAP_PHASE_ORDER.first(),
        Some(&BootstrapPhase::ReadStoreVersion)
    );
    assert_eq!(
        BOOTSTRAP_PHASE_ORDER.last(),
        Some(&BootstrapPhase::StartOptionalWorkers)
    );
    let plugins = BOOTSTRAP_PHASE_ORDER
        .iter()
        .position(|phase| *phase == BootstrapPhase::LoadPlugins)
        .unwrap();
    let schemas = BOOTSTRAP_PHASE_ORDER
        .iter()
        .position(|phase| *phase == BootstrapPhase::PrepareNextGenSchemas)
        .unwrap();
    let mode = BOOTSTRAP_PHASE_ORDER
        .iter()
        .position(|phase| *phase == BootstrapPhase::RunStartMode)
        .unwrap();
    let finish = BOOTSTRAP_PHASE_ORDER
        .iter()
        .position(|phase| *phase == BootstrapPhase::FinishBootstrap)
        .unwrap();
    let globals = BOOTSTRAP_PHASE_ORDER
        .iter()
        .position(|phase| *phase == BootstrapPhase::InitializeGlobalVariables)
        .unwrap();
    assert!(plugins < schemas && schemas < mode && mode < finish && finish < globals);
}
