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

/// Go `SetOptionsOnTxnActive`: capture the validated session values once.
pub(crate) use tidb_session::txn_manager::transaction_options;

/// A restricted operation without a retained SQL session starts with the
/// global values a new system session would inherit.
pub(crate) fn restricted_transaction_options(
    globals: &tidb_session::GlobalSysvars,
) -> tidb_exec::session_commit_protocol::SessionTransactionOptions {
    options_from_values(|name| {
        globals
            .get(name)
            .expect("transaction options are registered")
    })
}

fn options_from_values<V: AsRef<str>>(
    get: impl Fn(&str) -> V,
) -> tidb_exec::session_commit_protocol::SessionTransactionOptions {
    use tidb_session::varsutil::{tidb_opt_assertion_level, AssertionLevel};
    use tidb_vardef::tidb_vars::{
        TIDB_ENABLE1_PC, TIDB_ENABLE_ASYNC_COMMIT, TIDB_TXN_ASSERTION_LEVEL,
    };
    tidb_session::txn_manager::options_from_values(
        get(TIDB_ENABLE_ASYNC_COMMIT).as_ref(),
        get(TIDB_ENABLE1_PC).as_ref(),
        match tidb_opt_assertion_level(get(TIDB_TXN_ASSERTION_LEVEL).as_ref()) {
            AssertionLevel::Off => tidb_proto::KvrpcAssertionLevel::Off,
            AssertionLevel::Fast => tidb_proto::KvrpcAssertionLevel::Fast,
            AssertionLevel::Strict => tidb_proto::KvrpcAssertionLevel::Strict,
        },
    )
}
