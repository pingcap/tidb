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

//! Source-backed tests for dynamic system-variable initial values.

use tidb_exec::global_sysvar_initial::{
    global_system_variable_initial_value, GlobalSysvarEnvironment, ASSERTION_FAST,
    ASSERTION_STRICT, ENABLE_1PC, ENABLE_ASYNC_COMMIT, ENABLE_AUTO_ANALYZE,
    ENABLE_MUTATION_CHECKER, MEM_OOM_ACTION, OFF, ON, OOM_ACTION_LOG,
    PESSIMISTIC_TRANSACTION_FAIR_LOCKING, ROW_FORMAT_V2, ROW_FORMAT_VERSION, TXN_ASSERTION_LEVEL,
};

#[test]
fn global_initial_values_match_source_table() {
    // Source: pkg/sessionctx/variable/sysvar.go:4047-4082.
    // Direct Go coverage: pkg/sessionctx/variable/sysvar_test.go:1575
    // (TestGlobalSystemVariableInitialValue).
    let test_classic = GlobalSysvarEnvironment {
        in_test: true,
        ..GlobalSysvarEnvironment::default()
    };
    assert_eq!(
        global_system_variable_initial_value("tidb_txn_mode", "pessimistic", test_classic),
        "pessimistic"
    );
    assert_eq!(
        global_system_variable_initial_value(ENABLE_ASYNC_COMMIT, OFF, test_classic),
        OFF
    );
    assert_eq!(
        global_system_variable_initial_value(ENABLE_1PC, OFF, test_classic),
        OFF
    );
    assert_eq!(
        global_system_variable_initial_value(MEM_OOM_ACTION, "CANCEL", test_classic),
        OOM_ACTION_LOG
    );
    assert_eq!(
        global_system_variable_initial_value(ENABLE_AUTO_ANALYZE, ON, test_classic),
        OFF
    );
    assert_eq!(
        global_system_variable_initial_value(ROW_FORMAT_VERSION, "1", test_classic),
        ROW_FORMAT_V2
    );
    assert_eq!(
        global_system_variable_initial_value(TXN_ASSERTION_LEVEL, "OFF", test_classic),
        ASSERTION_FAST
    );
    assert_eq!(
        global_system_variable_initial_value(ENABLE_MUTATION_CHECKER, OFF, test_classic),
        ON
    );
    assert_eq!(
        global_system_variable_initial_value(
            PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
            OFF,
            test_classic
        ),
        ON
    );
}

#[test]
fn global_initial_values_preserve_environment_branches() {
    let tikv = GlobalSysvarEnvironment {
        store_is_tikv: true,
        ..GlobalSysvarEnvironment::default()
    };
    assert_eq!(
        global_system_variable_initial_value(ENABLE_ASYNC_COMMIT, OFF, tikv),
        ON
    );
    assert_eq!(
        global_system_variable_initial_value(ENABLE_1PC, OFF, tikv),
        ON
    );

    let classic = GlobalSysvarEnvironment::default();
    assert_eq!(
        global_system_variable_initial_value(MEM_OOM_ACTION, "CANCEL", classic),
        "CANCEL"
    );
    assert_eq!(
        global_system_variable_initial_value(ENABLE_AUTO_ANALYZE, ON, classic),
        ON
    );
    assert_eq!(
        global_system_variable_initial_value(TXN_ASSERTION_LEVEL, "OFF", classic),
        ASSERTION_FAST
    );
    assert_eq!(
        global_system_variable_initial_value(PESSIMISTIC_TRANSACTION_FAIR_LOCKING, OFF, classic),
        ON
    );

    let next_gen = GlobalSysvarEnvironment {
        next_gen: true,
        ..classic
    };
    assert_eq!(
        global_system_variable_initial_value(TXN_ASSERTION_LEVEL, "OFF", next_gen),
        ASSERTION_STRICT
    );
    assert_eq!(
        global_system_variable_initial_value(PESSIMISTIC_TRANSACTION_FAIR_LOCKING, ON, next_gen),
        OFF
    );
}

#[test]
fn global_initial_values_leave_unknown_names_unchanged() {
    assert_eq!(
        global_system_variable_initial_value(
            "unknown_sysvar",
            "keep-me",
            GlobalSysvarEnvironment {
                store_is_tikv: true,
                in_test: true,
                next_gen: true,
            }
        ),
        "keep-me"
    );
}
