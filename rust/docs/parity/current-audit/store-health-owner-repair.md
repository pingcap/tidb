# Store-health owner repair

## Reference and scope

Both repositories were pulled before editing: TiDB hparser-integration a4ef2faed3c5ed2eb01d55e69911d1e92bc86bb9 and client-rust master 5777c01c03bd1474d61da0b7793999c92908c017 were current. Fresh Go master 93a01d31f6da205ae4bf376825293903a6899fdb pins client-go v2.0.8-0.20260928031501-8edb23f6c7ee. Its internal/locate/store_cache.go retains a health pointer per store, uses atomic score reads and skips contended feedback/decay writes. slow_score.go owns the trend calculation. This is maintenance of existing ports, not complete-package acceptance.

## Removed implementations and repaired behavior

Deleted tidb-txnkv/src/region/slow_score.rs and removed the parallel client/TiKV score, threshold, rate-limit, decay and queue-estimate implementations from store_health.rs. Existing exported names alias native types through client-rust's tikv facade. StoreRoutingHealth now retains Arc<StoreHealthStatus>; topology copies preserve that same health object. Its metadata equality uses the health handle's identity and load snapshot, so asynchronous health updates do not spuriously advance store revisions.

The native StoreHealthStatus now reads the TiKV score atomically without acquiring its feedback mutex. Feedback and decay use try_lock; a concurrent update is skipped, with later ticks retaining the same decay rules. Active feedback checks defer while the feedback metadata is being updated. Slow-score counters and window arithmetic remain in the native atomic owner.

The existing native StoreLoadStats now provides the shared default/update/estimated_wait operations. TiDB health/load observations use Instant, including DistSQL's ServerIsBusy observation, so elapsed queue decay is independent of wall-clock adjustments. DistSQL's response-statistics observation callback is unchanged. This changes internal Rust health APIs; no SQL or wire-format change is introduced.

Production files changed: tidb-txnkv/src/region/{mod.rs,store_state.rs,store_health.rs,health_policy.rs,cache/replica_routing.rs}, deleted region/slow_score.rs, tidb-distsql/src/cop_paging/direct_unary_query_transport.rs, and synchronized native src/{locate.rs,region_cache.rs,tikv.rs}. Existing replica_health_scoring_source.rs and region_topology_source.rs provide TiDB regression coverage. Other changes are native source notes, sync metadata, this receipt, living plans and the structural register.

## Regression evidence

Before the fix, topology_copy_retains_the_same_store_health_owner failed because marking one copy slow left the other copy healthy. After replacement both observe the same native state. A second integration case proves loading another region preserves the canonical store's health identity. Existing busy-selection tests still assert the 500/800/150 ms sequence and the strict threshold comparison.

Native health_feedback_and_tick_skip_a_concurrent_update failed before the fix: its worker could not finish while the update mutex was held. It now finishes immediately, leaves score 80 unchanged during contention, and decays it to 60 after release. The test releases the guard before joining even when the timeout fails, avoiding hangs. Logs: /private/tmp/native-store-health-owner-{red,green}.log and /private/tmp/tidb-store-health-owner-{red,green}.log.

## Validation and publication

From /Users/qiliu/projects/client-rust:

    cargo test --locked --lib health_feedback_and_tick_skip_a_concurrent_update
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

Native 6f663b396552eec6d1bfad76b65f813e317884a4 is committed and pushed to ngaut/client-rust master. The full library passes 1,406 tests with two ignored; strict Clippy passes. Logs: /private/tmp/native-store-health-owner-{all,clippy}.log.

From TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh
    make lint
    git diff --check

Source synchronization applied four maintained patches and regenerated protobuf outputs without a generated-file diff. Root lint passed, including protocol freshness/input checks. No Go or Bazel inputs changed, so no bazel_prepare or Go failpoint mutation was needed.

From TiDB rust/:

    cargo test --locked -p tidb-txnkv --test all replica_health_scoring_source
    cargo test --locked -p tidb-txnkv --lib --test all --test region_error_recovery_source
    cargo test --locked -p tidb-distsql --lib --test all

The focused health suite passes all 12 tests. Broader results: transaction library 141 passed/one ignored; transaction integration 426 passed/ten ignored; standalone region recovery 26 passed; DistSQL library 37 passed/one ignored; DistSQL integration 256 passed/two ignored. Total 886 passed/14 ignored, excluding the repeated focused cases. Logs: /private/tmp/tidb-store-health-owner-{txnkv,distsql,lint,sync}.log. Formatting uses rustfmt --check --edition 2021 --config skip_children=true on changed Rust files.

Publication must use the actual hook from TiDB root:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: share native store health and monotonic load state'

The hook must pass cd rust && cargo build --locked -p tidb-server. After the final commit, rerun cargo build --locked -p tidb-server from rust/ and only then git push origin HEAD:hparser-integration. Gate logs: /private/tmp/tidb-store-health-owner-{commit,prepush}.log. The actual hook passed its locked server build. This receipt amendment also runs through that hook. Final publication uses cargo build --locked -p tidb-server followed by git push only on success; the final post-amend build log is /private/tmp/tidb-store-health-owner-prepush-final.log.

## Remaining work and risks

The shared health algorithms and per-store handle do not close T02's competing cache/RPC/request-state owners. TiDB's removed latency-recording, feedback and tick methods had only test callers; connecting the corresponding production events and periodic owner remains necessary. Native client-rust's cache already drives those native methods. This change does not claim that all TiDB routing health signals are now wired.

No original Go suite, full Rust workspace suite, real TiKV deployment or sysbench/TPC-C/TPC-H/YCSB measurement was run. Shared handles avoid copying score histories and atomic score reads avoid the feedback mutex, but throughput and contention improvements are unmeasured.

## Atomic-publication follow-up, 2026-10-02

The earlier statement that active-feedback checks defer while metadata is being
updated is superseded by the [T04 repair](../../health-feedback-publication-execplan.md).
Go's read-side presence and timestamp also remain atomic. Native c97dafb now
preserves that admission contract and the client-score/callback/decay order;
TiDB consumes the synchronized owner. This closes that recorded scheduling gap,
while the T02 integration limits above remain open. The follow-up runs the
original Go health cases under the race detector and retains red/green native
contention/callback evidence; no benchmark improvement is claimed.
