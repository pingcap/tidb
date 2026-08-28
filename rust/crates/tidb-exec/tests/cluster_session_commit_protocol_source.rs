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

//! Cluster-session transactions must carry `@@tidb_enable_async_commit` /
//! `@@tidb_enable_1pc` into every writable transaction they open.
//!
//! Go resolves both variables per transaction (`pkg/session` builds them into
//! `txnVars`; client-go's committer reads them in `checkAsyncCommit` /
//! `checkOnePC`). The Rust cluster path resolved them on one node flavor but
//! dropped them on the `--cluster-session` path: transactions opened through
//! [`SessionTransaction::begin_pessimistic`] and autocommit publications
//! through [`commit_staged_buffer`] fell back to classic 2PC, paying one extra
//! round trip per transaction than Go — measured as ~40% of TPC-C NEW_ORDER's
//! per-transaction latency gap.
//!
//! The protocol machinery itself is proven end to end by
//! `tidb-txnkv/tests/async_commit_one_pc_realtikv_source.rs` against a live
//! cluster; what this contract pins is that the cluster-session layer actually
//! hands the resolved protocol down.

#![allow(missing_docs)]

use std::path::Path;

fn source_of(path: &str) -> String {
    // CARGO_MANIFEST_DIR = <workspace>/crates/tidb-exec; the workspace root is
    // two levels up.
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate sits under <workspace>/crates/");
    let full = workspace.join(path);
    std::fs::read_to_string(&full)
        .unwrap_or_else(|error| panic!("read {}: {error}", full.display()))
}

#[test]
fn both_explicit_transaction_constructors_apply_the_protocol() {
    let source = source_of("crates/tidb-exec/src/cluster_table_storage.rs");
    let optimistic = source
        .split("pub fn begin(")
        .nth(1)
        .and_then(|tail| tail.split("pub fn begin_pessimistic(").next())
        .expect("SessionTransaction::begin implementation");
    let pessimistic = source
        .split("pub fn begin_pessimistic(")
        .nth(1)
        .and_then(|tail| tail.split("pub fn lock_keys(").next())
        .expect("SessionTransaction::begin_pessimistic implementation");

    for (name, constructor) in [("optimistic", optimistic), ("pessimistic", pessimistic)] {
        assert!(
            constructor.contains("transaction.set_commit_protocol(commit_protocol);"),
            "the {name} explicit-transaction constructor must apply the resolved \
             commit protocol before publishing start_ts"
        );
    }
}

#[test]
fn explicit_transaction_state_stays_on_the_session_worker() {
    let source = source_of("crates/tidb-exec/src/cluster_table_storage.rs");
    let session = source
        .split("pub struct SessionTransaction")
        .nth(1)
        .and_then(|tail| tail.split("/// One statement's view").next())
        .expect("the SessionTransaction implementation");

    assert!(
        session.contains("state: Arc<Mutex<SessionTransactionState"),
        "an explicit transaction must share its owned state directly with \
         statement snapshots"
    );
    assert!(
        !session.contains("thread: TransactionThread"),
        "an explicit transaction must not rendezvous with a pinned OS thread"
    );
}

#[test]
fn read_only_pessimistic_transaction_promotes_only_when_it_locks() {
    let source = source_of("crates/tidb-exec/src/cluster_table_storage.rs");
    let constructor = source
        .split("pub fn begin_pessimistic(")
        .nth(1)
        .and_then(|tail| tail.split("pub fn lock_keys(").next())
        .expect("SessionTransaction::begin_pessimistic implementation");
    let lock_path = source
        .split("pub fn lock_keys_with_assertions(")
        .nth(1)
        .and_then(|tail| tail.split("pub fn release_keys(").next())
        .expect("SessionTransaction::lock_keys_with_assertions implementation");

    assert!(
        constructor.contains("SessionTransactionState::PessimisticPending"),
        "BEGIN in pessimistic mode must retain the ordinary transaction until a locking statement"
    );
    assert!(
        lock_path.contains("promote_pessimistic_state(&mut state)"),
        "the first locking statement must promote the retained transaction before acquiring locks"
    );
    assert!(
        source
            .matches("SessionTransactionState::PessimisticPending")
            .count()
            >= 5,
        "reads, commit, and cleanup must preserve the lazy pessimistic state"
    );
}

#[test]
fn sql_snapshot_reads_carry_the_statement_resource_group() {
    let server = source_of("crates/tidb-server/src/cluster_session_node/transactions.rs");
    assert!(
        server.contains("opener_for_resource_group")
            && server.contains(".with_resource_group_name(Arc::<str>::from(resource_group))")
            && !server.contains("opener.with_resource_group_name(\"default\")"),
        "the SQL transaction tier must stamp the group resolved for this statement, not a process-wide default"
    );

    let session = source_of("crates/tidb-session/src/variables.rs");
    assert!(
        session.contains("SessionStmt::SetResourceGroup(resource_group)")
            && session.contains("pub fn statement_resource_group")
            && session.contains("RESOURCE_GROUP"),
        "SET RESOURCE GROUP and the one-statement hint must share the session resolver"
    );

    let opener = source_of("crates/tidb-txnkv/src/transaction/coordinator/opener.rs");
    assert!(
        opener.contains("TxnResourceGroup::set_resource_group_name(")
            && opener
                .matches("self.resource_group_name.as_deref()")
                .count()
                >= 3,
        "ordinary transactions and both direct MaxTS paths must inherit the configured group"
    );

    let reads = source_of("crates/tidb-txnkv/src/transaction/coordinator/snapshot_read.rs");
    assert!(
        reads.matches("self.write_context(").count() >= 2,
        "transactional point and batch reads must attach the transaction resource group"
    );
    assert!(
        reads.matches("context_with_resource_group(").count() >= 3,
        "direct MaxTS point and range reads must attach the opener resource group"
    );
}

#[test]
fn session_transactions_and_autocommit_commits_carry_it() {
    let source = source_of("crates/tidb-exec/src/cluster_table_storage.rs");
    for signature in [
        "timeout: Duration,\n        commit_protocol: CommitProtocol,\n    ) -> Result<Self, OptimisticCoordinatorError>",
        "timeout: Duration,\n    commit_protocol: CommitProtocol,\n) -> Result<Option<OptimisticCommitOutcome>, LockSqlError>",
    ] {
        assert!(
            source.contains(signature),
            "SessionTransaction::begin/begin_pessimistic and commit_staged_buffer \
             must take the resolved commit protocol"
        );
    }
    assert!(
        source.contains("transaction.set_commit_protocol(commit_protocol);".trim_end())
            && source
                .contains("// Go's autocommit committer checks `@@tidb_enable_async_commit` /",),
        "commit_staged_buffer must apply the protocol to the autocommit transaction"
    );
    assert!(
        source.contains("staged_mutations_from_entries(buffer, buffer.take_snapshot())"),
        "commit_staged_buffer must move the Go-shaped autocommit MemBuffer entries"
    );
}

#[test]
fn every_cluster_session_caller_resolves_go_defaults() {
    // Go defaults both variables ON for a TiKV-backed cluster
    // (`GlobalSystemVariableInitialValue`); the node-level resolver encodes
    // exactly that, so every caller must go through it rather than passing a
    // hand-built `two_phase_only()`.
    let server_source = source_of("crates/tidb-server/src/cluster_session_node/transactions.rs");
    let count = server_source
        .matches("session_commit_protocol::session_commit_protocol()")
        .count();
    assert!(
        count >= 3,
        "the cluster session node must resolve the protocol for begin, \
         begin_pessimistic, and commit_staged_buffer; found {count} resolutions"
    );
}
