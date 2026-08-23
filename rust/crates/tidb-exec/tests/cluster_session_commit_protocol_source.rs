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
    std::fs::read_to_string(&full).unwrap_or_else(|error| panic!("read {}: {error}", full.display()))
}

#[test]
fn the_cluster_thread_applies_the_protocol_in_both_writable_arms() {
    let source = source_of("crates/tidb-exec/src/cluster_table_storage.rs");

    // The thread constructor takes the resolved protocol.
    assert!(
        source.contains("name: &str,\n        commit_protocol: CommitProtocol,"),
        "TransactionThread::open/open_with/prepare_with must accept the resolved \
         commit protocol"
    );

    // Both writable arms apply it before serving the transaction. Counting
    // matters: dropping either arm silently reverts one transaction shape to
    // classic 2PC.
    let pessimistic_arm = format!(
        "{}{}",
        "transaction.set_commit_protocol(commit_protocol);",
        "\n                                if opened.send(Ok(transaction.start_ts())).is_err()"
    );
    assert!(
        source.contains(&pessimistic_arm),
        "the pessimistic arm must apply the commit protocol before publishing start_ts"
    );
    let optimistic_arm = "if open == TransactionOpen::Writable {\n                                transaction.set_commit_protocol(commit_protocol);\n                            }";
    assert!(
        source.contains(optimistic_arm),
        "the optimistic arm must apply the commit protocol too"
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
            && source.contains(
                "// Go's autocommit committer checks `@@tidb_enable_async_commit` /",
            ),
        "commit_staged_buffer must apply the protocol to the autocommit transaction"
    );
}

#[test]
fn every_cluster_session_caller_resolves_go_defaults() {
    // Go defaults both variables ON for a TiKV-backed cluster
    // (`GlobalSystemVariableInitialValue`); the node-level resolver encodes
    // exactly that, so every caller must go through it rather than passing a
    // hand-built `two_phase_only()`.
    let server_source =
        source_of("crates/tidb-server/src/cluster_session_node/transactions.rs");
    let count = server_source.matches("session_commit_protocol::session_commit_protocol()")
        .count();
    assert!(
        count >= 3,
        "the cluster session node must resolve the protocol for begin, \
         begin_pessimistic, and commit_staged_buffer; found {count} resolutions"
    );
}
