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

//! The session values a store transaction opens with.
//!
//! Go captures them once when a transaction activates
//! (`SetOptionsOnTxnActive`, `pkg/sessiontxn/isolation/base.go`). The
//! explicit-transaction state itself lives in the cluster node
//! (`cluster_session_node`), over Go's `BEGIN` activating at once and the
//! `autocommit = 0` transaction staying pending until a statement needs it;
//! this file used to hold a second, unused state machine that assumed `BEGIN`
//! is lazy, which Go's `OnInitialize` (`activeNow = true`) refutes.

use tidb_store_driver::session_commit_protocol::SessionTransactionOptions;

/// A restricted operation without a retained SQL session starts with the
/// global values a new system session would inherit.
pub fn restricted_transaction_options(
    globals: &tidb_domain::global_sysvars::GlobalSysvars,
) -> tidb_store_driver::session_commit_protocol::SessionTransactionOptions {
    options_from_getter(|name| {
        globals
            .get(name)
            .expect("transaction options are registered")
    })
}

fn options_from_getter<V: AsRef<str>>(
    get: impl Fn(&str) -> V,
) -> tidb_store_driver::session_commit_protocol::SessionTransactionOptions {
    use tidb_variable::varsutil::{tidb_opt_assertion_level, AssertionLevel};
    use tidb_vardef::tidb_vars::{
        TIDB_ENABLE1_PC, TIDB_ENABLE_ASYNC_COMMIT, TIDB_TXN_ASSERTION_LEVEL,
    };
    options_from_values(
        get(TIDB_ENABLE_ASYNC_COMMIT).as_ref(),
        get(TIDB_ENABLE1_PC).as_ref(),
        match tidb_opt_assertion_level(get(TIDB_TXN_ASSERTION_LEVEL).as_ref()) {
            AssertionLevel::Off => tidb_proto::KvrpcAssertionLevel::Off,
            AssertionLevel::Fast => tidb_proto::KvrpcAssertionLevel::Fast,
            AssertionLevel::Strict => tidb_proto::KvrpcAssertionLevel::Strict,
        },
    )
}

/// Go `SetOptionsOnTxnActive`: capture the validated session values once.
pub fn transaction_options(vars: &tidb_variable::SessionVars) -> SessionTransactionOptions {
    use tidb_variable::varsutil::{tidb_opt_assertion_level, AssertionLevel};
    use tidb_vardef::tidb_vars::{
        TIDB_ENABLE1_PC, TIDB_ENABLE_ASYNC_COMMIT, TIDB_TXN_ASSERTION_LEVEL,
    };
    let get = |name: &str| {
        vars.system_value(name)
            .expect("transaction options are registered")
    };
    options_from_values(
        get(TIDB_ENABLE_ASYNC_COMMIT).as_ref(),
        get(TIDB_ENABLE1_PC).as_ref(),
        match tidb_opt_assertion_level(get(TIDB_TXN_ASSERTION_LEVEL).as_ref()) {
            AssertionLevel::Off => tidb_proto::KvrpcAssertionLevel::Off,
            AssertionLevel::Fast => tidb_proto::KvrpcAssertionLevel::Fast,
            AssertionLevel::Strict => tidb_proto::KvrpcAssertionLevel::Strict,
        },
    )
}

/// The options built from the three session values a transaction captures;
/// shared with the restricted operations that have no session.
pub fn options_from_values(
    async_commit: &str,
    one_pc: &str,
    assertion_level: tidb_proto::KvrpcAssertionLevel,
) -> SessionTransactionOptions {
    SessionTransactionOptions {
        commit_protocol: tidb_txnkv::transaction::CommitProtocol {
            async_commit: tidb_vardef::option_values::tidb_opt_on(async_commit),
            one_pc: tidb_vardef::option_values::tidb_opt_on(one_pc),
        },
        assertion_level,
        kv_context: None,
    }
}

pub fn global_lock_wait_timeout(vars: &tidb_domain::global_sysvars::GlobalSysvars) -> std::time::Duration {
    let seconds = vars
        .get("innodb_lock_wait_timeout")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(tidb_vardef::defaults::DEF_INNODB_LOCK_WAIT_TIMEOUT as u64);
    std::time::Duration::from_secs(seconds)
}
