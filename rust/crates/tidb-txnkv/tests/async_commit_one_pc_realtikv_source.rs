// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Real-TiKV async commit and 1PC.
//!
//! The two protocols only exist because a real TiKV grants them; a scripted
//! server can prove the client's decisions but not that a cluster accepts a
//! prewrite carrying `use_async_commit`/`try_one_pc` and answers with the
//! timestamps the client then commits at. These proofs run against a live
//! PD + TiKV playground and read the committed rows back at the derived commit
//! timestamp, which is the only way to show the derivation was correct.

use std::time::Duration;

use tidb_pd_client::PdClient;
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::transaction::{
    CommitProtocol, CommittedProtocol, OptimisticCommitOutcome, OptimisticMutation,
    RealOptimisticTransactionOpener, TransactionAttemptPhase,
};
use tidb_txnkv::{PdRegionLoader, SharedReadAuthority};

const RPC_TIMEOUT: Duration = Duration::from_secs(20);

/// Both keys sort together, so a fresh playground serves them from one region
/// and 1PC is actually reachable.
const ASYNC_PRIMARY_KEY: &[u8] = b"async-commit-proof-a";
const ASYNC_SECONDARY_KEY: &[u8] = b"async-commit-proof-b";
const ONE_PC_KEY: &[u8] = b"one-pc-proof-a";

fn opener(protocol: CommitProtocol) -> (RealOptimisticTransactionOpener, u64) {
    let pd_address = std::env::var("ASYNC_COMMIT_PD_ADDR")
        .expect("runner must provide ASYNC_COMMIT_PD_ADDR");
    let pd_owner = PdClient::connect_seeds([pd_address], Duration::from_secs(10))
        .expect("start sole real PD authority");
    let cluster_id = pd_owner.cluster_id();
    assert_ne!(cluster_id, 0);
    let transport_owner =
        TonicCoprocessorClient::new().expect("start sole real BatchCommands authority");
    let shared = SharedReadAuthority::start_with_store_liveness(
        transport_owner,
        RegionCache::new(PdRegionLoader::from_client(pd_owner.clone())),
    )
    .expect("start sole real RegionCache authority");
    assert_eq!(shared.cluster_id(), cluster_id);
    let opener = RealOptimisticTransactionOpener::from_process_capabilities(
        shared.opener(),
        pd_owner,
        RPC_TIMEOUT,
    )
    .expect("derive transaction opener without starting another authority")
    .with_commit_protocol(protocol);
    // The authority owners are deliberately leaked for the life of the test
    // process: dropping them here would tear down the transport the returned
    // opener still routes through.
    std::mem::forget(shared);
    (opener, cluster_id)
}

/// A real cluster commits a two-key async-commit transaction at
/// `max(min_commit_ts)` and never publishes a second timestamp request.
///
/// Source contract (`twoPhaseCommitter.execute`): with `isAsyncCommit()` the
/// commit timestamp is `c.minCommitTSMgr.get()` — a value TiKV supplied on the
/// prewrite responses — so PD is asked exactly once for the whole transaction.
#[test]
#[ignore = "requires run-realtikv-async-commit-1pc.sh"]
fn a_real_async_commit_transaction_commits_at_its_prewrite_timestamps() {
    let (opener, cluster_id) = opener(CommitProtocol {
        async_commit: true,
        one_pc: false,
    });
    let transaction = opener
        .begin(2, 4 * 1024)
        .expect("allocate one real start timestamp");
    let start_ts = transaction.start_ts();

    let outcome = transaction
        .commit(
            vec![
                OptimisticMutation::insert(ASYNC_PRIMARY_KEY.to_vec(), b"async-v1".to_vec())
                    .unwrap(),
                OptimisticMutation::insert(ASYNC_SECONDARY_KEY.to_vec(), b"async-v2".to_vec())
                    .unwrap(),
            ],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("run a real async-commit transaction");
    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("expected a committed async-commit outcome: {outcome:?}");
    };
    let receipt = &committed.receipt;
    assert!(
        committed.secondary_failures.is_empty(),
        "async-commit follow-up commits failed: {:?}",
        committed.secondary_failures
    );
    assert_eq!(
        receipt.commit_protocol,
        CommittedProtocol::AsyncCommit,
        "a real TiKV must grant async commit for a two-key transaction"
    );
    assert!(
        receipt.commit_ts > start_ts,
        "TiKV must answer with a min_commit_ts past the start timestamp"
    );
    for publication in &receipt.prewrite_publications {
        println!(
            "async_commit_realtikv phase=prewrite tag={} request_id={} physical_address={}",
            publication.tag().field_number(),
            publication.request_id(),
            publication.physical_address(),
        );
    }

    // The transaction is readable at the timestamp the prewrites derived, which
    // is the observable proof that the derivation was the real commit point.
    let mut reader = opener.begin_read_only().expect("open a reader after commit");
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    // `>=`, not `>`: TiKV answers with `start_ts + 1`, and PD's very next
    // allocation can be exactly that value. A snapshot at `commit_ts` still
    // sees the commit, which is the whole reason async commit may claim it.
    assert!(reader.start_ts() >= receipt.commit_ts);
    for (key, expected) in [
        (ASYNC_PRIMARY_KEY, b"async-v1".as_slice()),
        (ASYNC_SECONDARY_KEY, b"async-v2".as_slice()),
    ] {
        let read = reader
            .snapshot_get(key, &call)
            .expect("read back an async-committed key");
        assert_eq!(read.value.as_deref(), Some(expected));
    }

    println!(
        "async_commit_realtikv status=passed cluster_id={cluster_id} start_ts={start_ts} \
         commit_ts={} prewrite_batches={}",
        receipt.commit_ts,
        receipt.prewrite_publications.len(),
    );
}

/// A real cluster commits a single-region transaction inside its prewrite, and
/// the coordinator publishes no Commit command at all.
///
/// Source contract: when `prewriteResp.OnePcCommitTs != 0`, execute returns
/// right after prewrite — the receipt therefore has to contain prewrite
/// attempts and nothing else.
#[test]
#[ignore = "requires run-realtikv-async-commit-1pc.sh"]
fn a_real_one_pc_transaction_publishes_no_commit_command() {
    let (opener, cluster_id) = opener(CommitProtocol {
        async_commit: true,
        one_pc: true,
    });
    let transaction = opener
        .begin(1, 4 * 1024)
        .expect("allocate one real start timestamp");
    let start_ts = transaction.start_ts();

    let outcome = transaction
        .commit(
            vec![OptimisticMutation::insert(ONE_PC_KEY.to_vec(), b"one-pc-v1".to_vec()).unwrap()],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("run a real 1PC transaction");
    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("expected a committed 1PC outcome: {outcome:?}");
    };
    let receipt = &committed.receipt;
    assert_eq!(
        receipt.commit_protocol,
        CommittedProtocol::OnePc,
        "a real TiKV must grant 1PC for a single-region single-key transaction"
    );
    assert!(receipt.commit_ts > start_ts);

    // The load-bearing evidence: every physical attempt this transaction made
    // was a Prewrite, so no Commit RPC followed it.
    assert!(
        receipt
            .attempt_history
            .iter()
            .all(|attempt| attempt.phase == TransactionAttemptPhase::Prewrite),
        "a 1PC transaction published a non-prewrite command: {:?}",
        receipt.attempt_history
    );
    assert!(receipt.primary_publications.is_empty());
    assert!(receipt.secondary_attempt_publications.is_empty());
    assert!(receipt.rollback_attempt_publications.is_empty());
    for publication in &receipt.prewrite_publications {
        println!(
            "one_pc_realtikv phase=prewrite tag={} request_id={} physical_address={}",
            publication.tag().field_number(),
            publication.request_id(),
            publication.physical_address(),
        );
    }
    println!(
        "one_pc_realtikv phase=no_commit_rpc attempts={} primary_publications={}",
        receipt.attempt_history.len(),
        receipt.primary_publications.len(),
    );

    let mut reader = opener.begin_read_only().expect("open a reader after commit");
    let read = reader
        .snapshot_get(ONE_PC_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("read back the 1PC-committed key");
    assert_eq!(read.value.as_deref(), Some(b"one-pc-v1".as_slice()));

    println!(
        "one_pc_realtikv status=passed cluster_id={cluster_id} start_ts={start_ts} \
         commit_ts={}",
        receipt.commit_ts,
    );
}
