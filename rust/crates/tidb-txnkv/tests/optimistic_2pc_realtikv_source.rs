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
use tidb_pd_client::PdClient;
use tidb_proto::{
    KvrpcAssertionLevel, KvrpcCommitRequest, KvrpcCommitRole, KvrpcContext, KvrpcMutation, KvrpcOp,
    KvrpcPeer, KvrpcPrewriteRequest, KvrpcRegionEpoch, KvrpcRequestOrigin,
};
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::rpc::{TonicCoprocessorClient, TransactionBatchPublication, UnaryCallContext};
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, OptimisticTransactionState,
    RealOptimisticTransactionOpener, TransactionAttemptPhase, TransactionAttemptResult,
    TransactionCause,
};
use tidb_txnkv::{DirectUnaryClient, PdRegionLoader, SharedReadAuthority, SharedReadRuntime};

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

type RealRuntime = SharedReadRuntime<TonicCoprocessorClient, PdRegionLoader>;

fn route(runtime: &RealRuntime, key: &[u8]) -> (String, KvrpcContext) {
    let location = runtime
        .locate_key(key)
        .expect("RegionCache lifecycle remains live")
        .expect("locate real fixture key");
    let leader_id = location.leader_peer_id.expect("real region has a leader");
    let leader = location
        .peers
        .iter()
        .find(|peer| peer.id == leader_id)
        .expect("leader peer is in region metadata");
    let store = location
        .stores
        .iter()
        .find(|store| store.id == leader.store_id)
        .expect("leader store is hydrated");
    (
        store.address.clone(),
        KvrpcContext {
            region_id: location.region.id,
            region_epoch: Some(KvrpcRegionEpoch {
                conf_ver: location.region.epoch.conf_ver,
                version: location.region.epoch.version,
            }),
            peer: Some(KvrpcPeer {
                id: leader.id,
                store_id: leader.store_id,
                role: leader.role.as_i32(),
                is_witness: leader.is_witness,
            }),
            request_source: "campaign28_real_lock_fixture".to_owned(),
            request_origin: KvrpcRequestOrigin::TiDb as i32,
            cluster_id: runtime.cluster_id(),
            ..KvrpcContext::default()
        },
    )
}

fn prewrite_real_lock(
    runtime: &RealRuntime,
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    lock_ttl_ms: u64,
) {
    let (address, context) = route(runtime, key);
    let request = KvrpcPrewriteRequest {
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
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = runtime
        .client()
        .try_borrow_mut()
        .expect("fixture client is not borrowed")
        .begin_transaction_prewrite(&address, None, &request, &context, &call)
        .expect("publish real lock fixture");
    let response = pending
        .complete(&call)
        .expect("complete real lock fixture")
        .expect("decode real lock fixture response");
    assert!(response.response.region_error.is_none());
    assert!(response.response.errors.is_empty());
}

fn commit_real_lock(runtime: &RealRuntime, key: &[u8], start_ts: u64, commit_ts: u64) {
    let (address, context) = route(runtime, key);
    let request = KvrpcCommitRequest {
        start_version: start_ts,
        keys: vec![key.to_vec()],
        commit_version: commit_ts,
        commit_role: KvrpcCommitRole::Primary as i32,
        primary_key: key.to_vec(),
        ..KvrpcCommitRequest::default()
    };
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = runtime
        .client()
        .try_borrow_mut()
        .expect("fixture client is not borrowed")
        .begin_transaction_commit(&address, None, &request, &context, &call)
        .expect("publish real fixture Commit");
    let response = pending
        .complete(&call)
        .expect("complete real fixture Commit")
        .expect("decode real fixture Commit response");
    assert!(response.response.region_error.is_none());
    assert!(response.response.error.is_none());
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

fn print_publication(
    phase: &str,
    publication: &TransactionBatchPublication,
    start_ts: u64,
    commit_ts: u64,
) {
    println!(
        "campaign28_optimistic_2pc phase={phase} tag={} request_id={} physical_address={} channel_version={} stream_generation={} start_ts={start_ts} commit_ts={commit_ts}",
        publication.tag().field_number(),
        publication.request_id(),
        publication.physical_address(),
        publication.physical_channel_version(),
        publication.batch_stream_generation(),
    );
}

#[test]
#[ignore = "requires run-realtikv-optimistic-2pc.sh"]
fn normal_optimistic_2pc_commits_two_regions_and_cleans_conflict() {
    let pd_address =
        std::env::var("OPTIMISTIC_2PC_PD_ADDR").expect("runner must provide OPTIMISTIC_2PC_PD_ADDR");
    let phase_dir = phase_dir();
    let pd_owner = PdClient::connect_seeds([pd_address], Duration::from_secs(10))
        .expect("start sole real PD authority");
    let cluster_id = pd_owner.cluster_id();
    assert_ne!(cluster_id, 0);
    let loader = PdRegionLoader::from_client(pd_owner.clone());
    let mut transport_owner =
        TonicCoprocessorClient::new().expect("start sole real BatchCommands authority");
    let shared = SharedReadAuthority::start_with_store_liveness(
        transport_owner.clone(),
        RegionCache::new(loader),
    )
    .expect("start sole real RegionCache authority");
    assert_eq!(shared.cluster_id(), cluster_id);
    let shared_read_opener = shared.opener();
    let opener = RealOptimisticTransactionOpener::from_process_capabilities(
        shared_read_opener.clone(),
        pd_owner.clone(),
        RPC_TIMEOUT,
    )
    .expect("derive transaction opener without starting another authority");
    assert_eq!(opener.authority_id(), shared_read_opener.authority_id());
    assert_ne!(opener.authority_id(), 0);

    // Locate before the external split. The later commit therefore proves a
    // stale epoch response invalidates and regroups the original two-key batch.
    let mut transaction = opener
        .begin(3, 192)
        .expect("allocate real start timestamp before split");
    assert_eq!(
        transaction.authority_id(),
        shared_read_opener.authority_id()
    );
    let before_split = transaction
        .snapshot_get(LOW_KEY, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("locate source region through real BatchCommands Get");
    assert!(before_split.value.is_none());
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
            "region_id={}\nsplit_key_hex={split_hex}\nstale_address={}\n",
            before_split.region.id,
            before_split.publication.physical_address(),
        ),
    );
    wait_for_phase(&phase_dir, "split-complete");

    let mutations = vec![
        OptimisticMutation::insert(LOW_KEY.to_vec(), b"low-v1".to_vec()).unwrap(),
        OptimisticMutation::insert(LOW_SIBLING_KEY.to_vec(), b"low-sibling-v1".to_vec()).unwrap(),
        OptimisticMutation::insert(HIGH_KEY.to_vec(), b"high-v1".to_vec()).unwrap(),
    ];
    let committed = transaction
        .commit(mutations, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("run normal optimistic 2PC");
    let OptimisticCommitOutcome::Committed(committed) = committed else {
        panic!("expected committed multi-region outcome: {committed:?}");
    };
    assert!(committed.secondary_failures.is_empty());
    assert_eq!(committed.receipt.mutation_count, 3);
    assert_eq!(committed.receipt.primary_key, LOW_KEY);
    assert_eq!(committed.receipt.prewrite_publications.len(), 2);
    assert_eq!(committed.receipt.primary_publications.len(), 1);
    assert_eq!(committed.receipt.secondary_publications.len(), 1);
    assert!(
        committed.receipt.prewrite_attempt_publications.len()
            > committed.receipt.prewrite_publications.len(),
        "forced split/leader transfer must produce a stale Prewrite attempt"
    );
    let prewrite_attempts = committed
        .receipt
        .attempt_history
        .iter()
        .filter(|attempt| attempt.phase == TransactionAttemptPhase::Prewrite)
        .collect::<Vec<_>>();
    let stale_source_attempt = prewrite_attempts
        .iter()
        .copied()
        .find(|attempt| {
            attempt.region == before_split.region
                && matches!(attempt.result, TransactionAttemptResult::Retry(_))
        })
        .expect("typed receipt must retain the stale source-region publication");
    let confirmed_source_attempt = prewrite_attempts
        .iter()
        .copied()
        .find(|attempt| {
            attempt.region.id == before_split.region.id
                && matches!(attempt.result, TransactionAttemptResult::Confirmed)
        })
        .expect("typed receipt must retain the confirmed source-region retry");
    assert!(
        confirmed_source_attempt
            .keys
            .iter()
            .all(|key| stale_source_attempt.keys.contains(key)),
        "regrouped source batch must descend from the exact immutable stale key set"
    );
    assert_ne!(
        stale_source_attempt.address,
        confirmed_source_attempt.address
    );
    assert_ne!(stale_source_attempt.region, confirmed_source_attempt.region);
    let correlated_key = confirmed_source_attempt
        .keys
        .first()
        .expect("confirmed source batch is nonempty");
    println!(
        "campaign28_optimistic_2pc phase=prewrite_regroup key={} stale_region={} stale_epoch_conf_ver={} stale_epoch_version={} stale_address={} confirmed_region={} confirmed_epoch_conf_ver={} confirmed_epoch_version={} confirmed_address={}",
        String::from_utf8_lossy(correlated_key),
        stale_source_attempt.region.id,
        stale_source_attempt.region.epoch.conf_ver,
        stale_source_attempt.region.epoch.version,
        stale_source_attempt.address,
        confirmed_source_attempt.region.id,
        confirmed_source_attempt.region.epoch.conf_ver,
        confirmed_source_attempt.region.epoch.version,
        confirmed_source_attempt.address,
    );
    assert!(committed.receipt.commit_ts > committed.receipt.start_ts);
    let mut regions = committed
        .receipt
        .region_attempts
        .iter()
        .map(|region| region.id)
        .collect::<Vec<_>>();
    regions.sort_unstable();
    regions.dedup();
    assert!(
        regions.len() >= 2,
        "receipt must contain distinct real regions"
    );
    for publication in &committed.receipt.prewrite_publications {
        print_publication(
            "prewrite",
            publication,
            committed.receipt.start_ts,
            committed.receipt.commit_ts,
        );
    }
    for publication in &committed.receipt.prewrite_attempt_publications {
        print_publication(
            "prewrite_attempt",
            publication,
            committed.receipt.start_ts,
            committed.receipt.commit_ts,
        );
    }
    for publication in &committed.receipt.primary_publications {
        print_publication(
            "primary_commit",
            publication,
            committed.receipt.start_ts,
            committed.receipt.commit_ts,
        );
    }
    for publication in &committed.receipt.secondary_publications {
        print_publication(
            "secondary_commit",
            publication,
            committed.receipt.start_ts,
            committed.receipt.commit_ts,
        );
    }

    let mut readback = opener.begin(1, 128).expect("allocate readback snapshot");
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
    assert_eq!(low.region.id, low_sibling.region.id);
    assert_ne!(low.region.id, high.region.id);
    let read_only = readback
        .finish_without_writes()
        .expect("finish readback without publishing writes");
    assert_eq!(read_only.state, OptimisticTransactionState::ReadOnly);
    assert_eq!(read_only.authority_id, shared_read_opener.authority_id());
    assert_eq!(read_only.snapshot_reads.len(), 3);

    // The low-region Insert prewrites first; the high-region existing Insert
    // fails. The coordinator must synchronously rollback every published batch.
    let (rollback_key, duplicate_key) = if low.region.id < high.region.id {
        (ROLLBACK_KEY, HIGH_KEY)
    } else {
        (HIGH_ROLLBACK_KEY, LOW_KEY)
    };
    let conflict = opener.begin(2, 128).expect("allocate conflict transaction");
    let outcome = conflict
        .commit(
            vec![
                OptimisticMutation::insert(rollback_key.to_vec(), b"rollback".to_vec()).unwrap(),
                OptimisticMutation::insert(duplicate_key.to_vec(), b"duplicate".to_vec()).unwrap(),
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
    assert!(!rolled_back.receipt.prewrite_publications.is_empty());
    assert!(!rolled_back.receipt.rollback_publications.is_empty());
    assert!(rolled_back.receipt.primary_publications.is_empty());
    for publication in &rolled_back.receipt.rollback_publications {
        print_publication("rollback", publication, rolled_back.receipt.start_ts, 0);
    }

    let mut after_rollback = opener.begin(1, 128).expect("allocate cleanup snapshot");
    assert!(after_rollback
        .snapshot_get(rollback_key, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("read exact rolled-back key")
        .value
        .is_none());
    after_rollback
        .finish_without_writes()
        .expect("finish cleanup read without writes");

    let post_cleanup = opener.begin(1, 128).expect("allocate post-cleanup write");
    let post_cleanup = post_cleanup
        .commit(
            vec![
                OptimisticMutation::insert(rollback_key.to_vec(), b"post-cleanup".to_vec())
                    .unwrap(),
            ],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("write rolled-back key immediately");
    assert!(matches!(
        post_cleanup,
        OptimisticCommitOutcome::Committed(_)
    ));

    let missing_update_key = b"c28-stage-b-missing".to_vec();
    let missing_update = opener
        .begin(1, 128)
        .expect("allocate missing UPDATE transaction");
    let missing_update = missing_update
        .commit(
            vec![OptimisticMutation::put_existing(
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
    assert!(missing_update.receipt.primary_publications.is_empty());

    // A strict PutExisting uses one snapshot and one normal transaction.
    let mut update = opener.begin(1, 128).expect("allocate update snapshot");
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
            vec![OptimisticMutation::put_existing(LOW_KEY.to_vec(), b"low-v2".to_vec()).unwrap()],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("commit strict existing-key update");
    assert!(matches!(update, OptimisticCommitOutcome::Committed(_)));

    let mut final_read = opener.begin(1, 128).expect("allocate final snapshot");
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
    let fixture_runtime = shared_read_opener
        .open_session()
        .expect("open real lock-fixture session from the same authority");
    let older_lock_start_ts = pd_owner
        .get_timestamp()
        .expect("allocate older fixture lock timestamp");
    prewrite_real_lock(
        &fixture_runtime,
        OLDER_LOCK_KEY,
        b"expired-holder",
        older_lock_start_ts,
        500,
    );
    let older_lock_writer = opener
        .begin(1, 128)
        .expect("allocate transaction newer than the real expired lock");
    assert!(older_lock_writer.start_ts() > older_lock_start_ts);
    let older_lock_wait_started = Instant::now();
    let older_lock_outcome = older_lock_writer
        .commit(
            vec![
                OptimisticMutation::insert(OLDER_LOCK_KEY.to_vec(), b"resolved-writer".to_vec())
                    .unwrap(),
            ],
            &UnaryCallContext::with_timeout(RPC_TIMEOUT),
        )
        .expect("resolve older live fixture and commit at the same start_ts");
    assert!(
        older_lock_wait_started.elapsed() >= Duration::from_millis(100),
        "real older lock must remain alive long enough to exercise cancellation-aware waiting"
    );
    let OptimisticCommitOutcome::Committed(older_lock_committed) = older_lock_outcome else {
        panic!("expired older lock was not resolved: {older_lock_outcome:?}");
    };
    assert!(older_lock_committed
        .receipt
        .attempt_history
        .iter()
        .any(|attempt| {
            attempt.phase == TransactionAttemptPhase::Prewrite
                && matches!(
                    attempt.result,
                    TransactionAttemptResult::Retry(TransactionCause::Lock { .. })
                )
        }));

    // A transaction must classify a real later-started lock as a write
    // conflict without asking the resolver to roll it back. Committing the
    // fixture afterward proves that the coordinator left that newer lock live.
    let newer_conflict = opener
        .begin(1, 128)
        .expect("allocate transaction before the newer lock fixture");
    let newer_lock_start_ts = pd_owner
        .get_timestamp()
        .expect("allocate newer fixture lock timestamp");
    assert!(newer_lock_start_ts > newer_conflict.start_ts());
    prewrite_real_lock(
        &fixture_runtime,
        NEWER_LOCK_KEY,
        b"newer-holder",
        newer_lock_start_ts,
        20_000,
    );
    let newer_conflict_outcome = newer_conflict
        .commit(
            vec![
                OptimisticMutation::insert(NEWER_LOCK_KEY.to_vec(), b"must-not-win".to_vec())
                    .unwrap(),
            ],
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
    let newer_lock_commit_ts = pd_owner
        .get_timestamp()
        .expect("allocate newer fixture commit timestamp");
    assert!(newer_lock_commit_ts > newer_lock_start_ts);
    commit_real_lock(
        &fixture_runtime,
        NEWER_LOCK_KEY,
        newer_lock_start_ts,
        newer_lock_commit_ts,
    );
    let mut lock_readback = opener.begin(1, 128).expect("allocate lock readback");
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

    drop(fixture_runtime);
    drop(opener);
    drop(shared_read_opener);
    shared.shutdown().expect("stop sole RegionCache authority");
    drop(shared);
    transport_owner
        .close()
        .expect("stop sole BatchCommands authority");
    drop(transport_owner);
    pd_owner.shutdown().expect("stop sole PD authority");
    println!(
        "campaign28_optimistic_2pc status=passed cluster_id={cluster_id} start_ts={} commit_ts={} primary_region={} secondary_region={} rollback_start_ts={} older_lock_start_ts={} newer_lock_start_ts={} newer_lock_commit_ts={}",
        committed.receipt.start_ts,
        committed.receipt.commit_ts,
        low.region.id,
        high.region.id,
        rolled_back.receipt.start_ts,
        older_lock_start_ts,
        newer_lock_start_ts,
        newer_lock_commit_ts,
    );
}
