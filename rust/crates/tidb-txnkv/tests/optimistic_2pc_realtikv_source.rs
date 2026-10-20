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

//! Real multi-region normal optimistic 2PC and rollback proof.

use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use tidb_codec::encode_bytes;
use tidb_proto::{
    KvrpcAssertionLevel, KvrpcCommitRequest, KvrpcCommitResponse, KvrpcCommitRole, KvrpcMutation,
    KvrpcOp, KvrpcPrewriteRequest, KvrpcPrewriteResponse,
};
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    BufferMutation, OptimisticCommitOutcome, OptimisticTransactionState,
    RealOptimisticTransactionOpener, TransactionCause,
};

use crate::realtikv_fixture::{connect, route, send, timestamp};

const RPC_TIMEOUT: Duration = Duration::from_secs(20);
const PHASE_TIMEOUT: Duration = Duration::from_secs(120);
const LOW_KEY: &[u8] = b"c28-stage-b-a";
const LOW_SIBLING_KEY: &[u8] = b"c28-stage-b-c";
const SPLIT_KEY: &[u8] = b"c28-stage-b-m";
const HIGH_KEY: &[u8] = b"c28-stage-b-z";
const ROLLBACK_KEY: &[u8] = b"c28-stage-b-b";
const HIGH_ROLLBACK_KEY: &[u8] = b"c28-stage-b-y";
const OLDER_LOCK_KEY: &[u8] = b"c28-stage-b-d";
const NEWER_LOCK_KEY: &[u8] = b"c28-stage-b-x";

fn prewrite_real_lock(
    store: &TikvStore,
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    lock_ttl_ms: u64,
) {
    let (address, context) = route(store, key);
    let request = KvrpcPrewriteRequest {
        context: Some(context),
        mutations: vec![KvrpcMutation {
            op: KvrpcOp::Put as i32,
            key: key.to_vec(),
            value: value.to_vec(),
            ..KvrpcMutation::default()
        }],
        primary_lock: key.to_vec(),
        start_version: start_ts,
        lock_ttl: lock_ttl_ms,
        txn_size: 1,
        assertion_level: KvrpcAssertionLevel::Off as i32,
        ..KvrpcPrewriteRequest::default()
    };
    let response: KvrpcPrewriteResponse = send(store, &address, request, RPC_TIMEOUT);
    assert!(response.region_error.is_none());
    assert!(response.errors.is_empty());
}

fn commit_real_lock(store: &TikvStore, key: &[u8], start_ts: u64, commit_ts: u64) {
    let (address, context) = route(store, key);
    let request = KvrpcCommitRequest {
        context: Some(context),
        start_version: start_ts,
        keys: vec![key.to_vec()],
        commit_version: commit_ts,
        commit_role: KvrpcCommitRole::Primary as i32,
        primary_key: key.to_vec(),
        ..KvrpcCommitRequest::default()
    };
    let response: KvrpcCommitResponse = send(store, &address, request, RPC_TIMEOUT);
    assert!(response.region_error.is_none());
    assert!(response.error.is_none());
}

fn phase_dir() -> PathBuf {
    std::env::var("OPTIMISTIC_2PC_PHASE_DIR")
        .map(PathBuf::from)
        .expect("runner must provide OPTIMISTIC_2PC_PHASE_DIR")
}

fn publish_phase(directory: &Path, name: &str, body: &str) {
    let temporary = directory.join(format!("{name}.tmp"));
    let final_path = directory.join(name);
    fs::write(&temporary, body).expect("write owned phase file");
    fs::rename(temporary, final_path).expect("publish owned phase file atomically");
}

fn wait_for_phase(directory: &Path, name: &str) {
    let path = directory.join(name);
    let deadline = Instant::now() + PHASE_TIMEOUT;
    while !path.is_file() {
        assert!(Instant::now() < deadline, "timed out waiting for {name}");
        thread::sleep(Duration::from_millis(100));
    }
}

#[test]
#[ignore = "requires run-realtikv-optimistic-2pc.sh"]
fn normal_optimistic_2pc_commits_two_regions_and_cleans_conflict() {
    let pd_address = std::env::var("OPTIMISTIC_2PC_PD_ADDR")
        .expect("runner must provide OPTIMISTIC_2PC_PD_ADDR");
    let phase_dir = phase_dir();
    let fixture_store = connect(&pd_address);
    let cluster_id = fixture_store.cluster_id();
    assert_ne!(cluster_id, 0);
    let opener = RealOptimisticTransactionOpener::from_process_capabilities(
        fixture_store.clone(),
        RPC_TIMEOUT,
    )
    .expect("derive transaction opener without starting another authority");
    assert_ne!(opener.authority_id(), 0);

    // Locate before the external split. The later commit therefore proves a
    // stale epoch response invalidates and regroups the original two-key batch.
    let mut transaction = opener
        .begin()
        .expect("allocate real start timestamp before split");
    assert_eq!(transaction.authority_id(), opener.authority_id());
    let before_split = transaction
        .snapshot_get(LOW_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("locate source region through real BatchCommands Get");
    assert!(before_split.value.is_none());
    let (stale_address, source_context) = route(&fixture_store, LOW_KEY);
    let mut encoded_split = Vec::new();
    encode_bytes(&mut encoded_split, SPLIT_KEY);
    let split_hex = encoded_split
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    publish_phase(
        &phase_dir,
        "split-source",
        &format!(
            "region_id={}\nsplit_key_hex={split_hex}\nstale_address={stale_address}\n",
            source_context.region_id,
        ),
    );
    wait_for_phase(&phase_dir, "split-complete");

    let mutations = vec![
        BufferMutation::set_with_flags(
            LOW_KEY.to_vec(),
            b"low-v1".to_vec(),
            true,
            tidb_txnkv::AssertionOp::AssertNotExist,
        )
        .unwrap(),
        BufferMutation::set_with_flags(
            LOW_SIBLING_KEY.to_vec(),
            b"low-sibling-v1".to_vec(),
            true,
            tidb_txnkv::AssertionOp::AssertNotExist,
        )
        .unwrap(),
        BufferMutation::set_with_flags(
            HIGH_KEY.to_vec(),
            b"high-v1".to_vec(),
            true,
            tidb_txnkv::AssertionOp::AssertNotExist,
        )
        .unwrap(),
    ];
    let committed = transaction
        .commit(mutations, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("run normal optimistic 2PC");
    let OptimisticCommitOutcome::Committed(committed) = committed else {
        panic!("expected committed multi-region outcome: {committed:?}");
    };
    assert!(committed.secondary_failures.is_empty());
    assert!(committed.receipt.commit_ts > committed.receipt.start_ts);

    let mut readback = opener.begin().expect("allocate readback snapshot");
    let low = readback
        .snapshot_get(LOW_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("read committed low key");
    let high = readback
        .snapshot_get(HIGH_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("read committed high key");
    let low_sibling = readback
        .snapshot_get(
            LOW_SIBLING_KEY,
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("read committed same-region primary sibling");
    assert_eq!(low.value.as_deref(), Some(b"low-v1".as_slice()));
    assert_eq!(high.value.as_deref(), Some(b"high-v1".as_slice()));
    assert_eq!(
        low_sibling.value.as_deref(),
        Some(b"low-sibling-v1".as_slice())
    );
    // The split put the primary's region and the secondary's apart.
    let low_region = route(&fixture_store, LOW_KEY).1.region_id;
    let high_region = route(&fixture_store, HIGH_KEY).1.region_id;
    assert_eq!(
        low_region,
        route(&fixture_store, LOW_SIBLING_KEY).1.region_id
    );
    assert_ne!(low_region, high_region);
    let read_only = readback
        .finish_without_writes()
        .expect("finish readback without publishing writes");
    assert_eq!(read_only.state, OptimisticTransactionState::ReadOnly);
    assert_eq!(read_only.authority_id, opener.authority_id());

    // The low-region Insert prewrites first; the high-region existing Insert
    // fails. The coordinator must synchronously rollback every published batch.
    let (rollback_key, duplicate_key) = if low_region < high_region {
        (ROLLBACK_KEY, HIGH_KEY)
    } else {
        (HIGH_ROLLBACK_KEY, LOW_KEY)
    };
    let conflict = opener.begin().expect("allocate conflict transaction");
    let outcome = conflict
        .commit(
            vec![
                BufferMutation::set_with_flags(
                    rollback_key.to_vec(),
                    b"rollback".to_vec(),
                    true,
                    tidb_txnkv::AssertionOp::AssertNotExist,
                )
                .unwrap(),
                BufferMutation::set_with_flags(
                    duplicate_key.to_vec(),
                    b"duplicate".to_vec(),
                    true,
                    tidb_txnkv::AssertionOp::AssertNotExist,
                )
                .unwrap(),
            ],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("return typed conflict outcome");
    let OptimisticCommitOutcome::RolledBack(rolled_back) = outcome else {
        panic!("expected determinate rollback: {outcome:?}");
    };
    match &rolled_back.cause {
        TransactionCause::AlreadyExists { key, .. }
        | TransactionCause::AssertionFailed { key, .. } => assert_eq!(key, duplicate_key),
        cause => panic!("expected typed existing-key cause, got {cause:?}"),
    }
    let mut after_rollback = opener.begin().expect("allocate cleanup snapshot");
    assert!(after_rollback
        .snapshot_get(rollback_key, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("read exact rolled-back key")
        .value
        .is_none());
    after_rollback
        .finish_without_writes()
        .expect("finish cleanup read without writes");

    let post_cleanup = opener.begin().expect("allocate post-cleanup write");
    let post_cleanup = post_cleanup
        .commit(
            vec![BufferMutation::set_with_flags(
                rollback_key.to_vec(),
                b"post-cleanup".to_vec(),
                true,
                tidb_txnkv::AssertionOp::AssertNotExist,
            )
            .unwrap()],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("write rolled-back key immediately");
    assert!(matches!(
        post_cleanup,
        OptimisticCommitOutcome::Committed(_)
    ));

    let missing_update_key = b"c28-stage-b-missing".to_vec();
    let missing_update = opener.begin().expect("allocate missing UPDATE transaction");
    let missing_update = missing_update
        .commit(
            vec![BufferMutation::put_existing(
                missing_update_key.clone(),
                b"must-not-appear".to_vec(),
            )
            .unwrap()],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("return strict missing-key assertion");
    let OptimisticCommitOutcome::RolledBack(missing_update) = missing_update else {
        panic!("missing PutExisting must roll back: {missing_update:?}");
    };
    match missing_update.cause {
        TransactionCause::AssertionFailed { key, .. } => {
            assert_eq!(key, missing_update_key);
        }
        cause => panic!("missing PutExisting lost assertion identity: {cause:?}"),
    }

    // A strict PutExisting uses one snapshot and one normal transaction.
    let mut update = opener.begin().expect("allocate update snapshot");
    assert_eq!(
        update
            .snapshot_get(LOW_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
            .expect("read UPDATE row at own start_ts")
            .value
            .as_deref(),
        Some(b"low-v1".as_slice())
    );
    let update = update
        .commit(
            vec![BufferMutation::put_existing(LOW_KEY.to_vec(), b"low-v2".to_vec()).unwrap()],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("commit strict existing-key update");
    assert!(matches!(update, OptimisticCommitOutcome::Committed(_)));

    let mut final_read = opener.begin().expect("allocate final snapshot");
    assert_eq!(
        final_read
            .snapshot_get(LOW_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
            .expect("read updated persisted value")
            .value
            .as_deref(),
        Some(b"low-v2".as_slice())
    );
    final_read
        .finish_without_writes()
        .expect("finish final read without writes");

    // A real older lock must first be observed alive, waited to expiry,
    // resolved, then retried at the transaction's original start_ts.
    let older_lock_start_ts = timestamp(&fixture_store);
    prewrite_real_lock(
        &fixture_store,
        OLDER_LOCK_KEY,
        b"expired-holder",
        older_lock_start_ts,
        500,
    );
    let older_lock_writer = opener
        .begin()
        .expect("allocate transaction newer than the real expired lock");
    assert!(older_lock_writer.start_ts() > older_lock_start_ts);
    let older_lock_wait_started = Instant::now();
    let older_lock_outcome = older_lock_writer
        .commit(
            vec![BufferMutation::set_with_flags(
                OLDER_LOCK_KEY.to_vec(),
                b"resolved-writer".to_vec(),
                true,
                tidb_txnkv::AssertionOp::AssertNotExist,
            )
            .unwrap()],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("resolve older live fixture and commit at the same start_ts");
    assert!(
        older_lock_wait_started.elapsed() >= Duration::from_millis(100),
        "real older lock must remain alive long enough to exercise cancellation-aware waiting"
    );
    let OptimisticCommitOutcome::Committed(_) = older_lock_outcome else {
        panic!("expired older lock was not resolved: {older_lock_outcome:?}");
    };
    // A transaction must classify a real later-started lock as a write
    // conflict without asking the resolver to roll it back. Committing the
    // fixture afterward proves that the coordinator left that newer lock live.
    let newer_conflict = opener
        .begin()
        .expect("allocate transaction before the newer lock fixture");
    let newer_lock_start_ts = timestamp(&fixture_store);
    assert!(newer_lock_start_ts > newer_conflict.start_ts());
    prewrite_real_lock(
        &fixture_store,
        NEWER_LOCK_KEY,
        b"newer-holder",
        newer_lock_start_ts,
        20_000,
    );
    let newer_conflict_outcome = newer_conflict
        .commit(
            vec![BufferMutation::set_with_flags(
                NEWER_LOCK_KEY.to_vec(),
                b"must-not-win".to_vec(),
                true,
                tidb_txnkv::AssertionOp::AssertNotExist,
            )
            .unwrap()],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("return typed newer-lock conflict");
    let OptimisticCommitOutcome::RolledBack(newer_conflict) = newer_conflict_outcome else {
        panic!("newer lock did not produce determinate conflict: {newer_conflict_outcome:?}");
    };
    assert!(matches!(
        newer_conflict.cause,
        TransactionCause::WriteConflict { .. }
    ));
    let newer_lock_commit_ts = timestamp(&fixture_store);
    assert!(newer_lock_commit_ts > newer_lock_start_ts);
    commit_real_lock(
        &fixture_store,
        NEWER_LOCK_KEY,
        newer_lock_start_ts,
        newer_lock_commit_ts,
    );
    let mut lock_readback = opener.begin().expect("allocate lock readback");
    assert_eq!(
        lock_readback
            .snapshot_get(NEWER_LOCK_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
            .expect("read fixture committed after conflict")
            .value
            .as_deref(),
        Some(b"newer-holder".as_slice())
    );
    lock_readback
        .finish_without_writes()
        .expect("finish lock readback without writes");

    println!(
        "campaign28_optimistic_2pc status=passed cluster_id={cluster_id} start_ts={} commit_ts={} primary_region={} secondary_region={} rollback_start_ts={} older_lock_start_ts={} newer_lock_start_ts={} newer_lock_commit_ts={}",
        committed.receipt.start_ts,
        committed.receipt.commit_ts,
        low_region,
        high_region,
        rolled_back.receipt.start_ts,
        older_lock_start_ts,
        newer_lock_start_ts,
        newer_lock_commit_ts,
    );
}
