# Replica candidate owner repair

## Scope and source

Both repositories were pulled first: TiDB hparser-integration 2ba4ea91e6400968690fcadcd24589879b23f2dd and native client-rust b23c6d37b92312ca34bad76b0bfac7e847cd3d6f were current. Go master 93a01d31f6da205ae4bf376825293903a6899fdb pins client-go v2.0.8-0.20260928031501-8edb23f6c7ee. The reference is internal/locate/replica_selector.go, especially ReplicaSelectMixedStrategy.next, isCandidate and calculateScore. This maintenance repair does not accept the complete internal/locate package or close T02.

Go shares one candidate predicate between mixed and idle selection. It allows one second attempt for a nonleader with DataIsNotReady, even when the busy threshold is positive. Busy-reported peers, the leader and peers with estimated wait strictly greater than that threshold remain excluded from idle selection. The five-bit score ranks eligible peers; equal scores use randomness for each selection.

## Removed duplicate policies

Native region_cache.rs had an additional attempts == 0 filter in select_idle_replica, bypassing the shared DataIsNotReady exception. The existing candidate snapshot now includes reported-busy and decayed wait facts, and idle selection consumes the same predicate and score as ordinary mixed selection.

TiDB health_policy.rs delegates candidate eligibility and score to the public native tikv facade. cache/replica_routing.rs no longer duplicates the attempt budget or highest-score/tie loop. Its remaining adapter supplies validated peer/store metadata and request-local facts. DistSQL no longer creates, advances, captures or propagates a per-query replica selection seed. ReadPolicy.selection_seed is removed. Read-byte estimation state remains because it is unrelated to replica selection.

Changed production files: tidb-txnkv/src/region/{health_policy.rs,replica_selector.rs,cache/replica_routing.rs}, tidb-distsql/src/cop_paging/direct_unary_query_transport.rs, and synchronized native src/{locate.rs,region_cache.rs,pd/client.rs,tikv.rs}. Test changes cover the existing native region-cache fixture and TiDB replica-health, request-selector, cache-TTL, region-error, query-dispatch and query-state suites. Remaining files are synchronization metadata, native source notes, the living plans, structural register and this receipt. No generated output was edited by hand.

## Regression and validation

The extended source_go_region_request3_TestSendReqWithReplicaSelector failed before the fix: an idle follower with one attempt and DataIsNotReady returned None instead of follower 12. It passes after repair and checks exhaustion after the allowed second attempt. Existing busy and unknown-liveness controls remain. TiDB's regression additionally distinguishes ordinary exhaustion, the one permitted retry, a busy peer and final exhaustion.

Tests formerly asserting one fixed tied peer now check the eligible set, unique attempts and exact request flags. Fixtures requiring a particular first peer use Go's store/label preference. Stale reads remain Mixed; switching them to Follower is unsupported and was rejected during fixture validation. All affected suites are green after these fixture corrections; no test failure is classified as pre-existing.

From /Users/qiliu/projects/client-rust:

    cargo test --locked --lib source_go_region_request3_TestSendReqWithReplicaSelector
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

Native results: 1,405 passed, two ignored; strict Clippy and formatting passed. Commits 568a68d9002abdb1f1c70b370cab7e86b32af6c6 and 5777c01c03bd1474d61da0b7793999c92908c017 are published to ngaut/client-rust master. The second commit exposes the types through tikv because region_cache is private; targeted tests and strict Clippy were rerun for that visibility correction. Logs: /private/tmp/native-replica-owner-{red,green,all,clippy,facade,clippy-facade}.log.

From TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh
    make lint
    git diff --check

The source synchronization applied four maintained compatibility patches and regenerated protobuf outputs without a generated-file diff. Latest native revision is 5777c01c. Lint passed, including protocol freshness/input checks. Formatting passed with rustfmt --check --edition 2021 --config skip_children=true on every changed Rust source file. This Rust-only change needs neither bazel_prepare nor Go failpoint toggling.

From TiDB rust/:

    cargo test --locked -p tidb-txnkv --lib --test all --test region_error_recovery_source
    cargo test --locked -p tidb-distsql --lib --test all

TiDB results: transaction library 141 passed/one ignored; transaction integration 424 passed/ten ignored; standalone region recovery 26 passed; DistSQL library 37 passed/one ignored; DistSQL integration 256 passed/two ignored. Total 884 passed/14 ignored. Logs: /private/tmp/tidb-replica-owner-{txnkv,distsql,lint,sync}.log. The selected scope covers all changed routing consumers, request flags, retry transitions and query dispatch.

## Publication gates

Use the actual hook from TiDB root:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: share native Go replica candidate selection'

The hook must pass cd rust && cargo build --locked -p tidb-server. After commit, rerun cargo build --locked -p tidb-server from rust/ and only then git push origin HEAD:hparser-integration. Logs: /private/tmp/tidb-replica-owner-{commit,prepush}.log. The actual hook passed its locked build (22.88 seconds), and the separate post-commit locked build passed (10.38 seconds). This receipt amendment runs through the hook again; its final post-amend locked build must pass before push. Existing warnings remain, with no build errors.

## Remaining boundaries and risks

T02 remains open: TiDB still owns cache metadata, health state, request transitions and other RPC/recovery logic also present in client-rust. This change removes one duplicated candidate algorithm; it is not a complete client-go cache/sender migration. T01's INSERT assertion ownership also requires a coordinated caller migration, not a blanket flag substitution.

Random tied-peer selection intentionally replaces deterministic query rotation. No stable peer order is a supported compatibility contract. Native candidate construction now snapshots estimated wait; its hot-path cost and end-to-end throughput were not benchmarked. No complete Rust workspace, original Go test suite, real TiKV deployment, sysbench/TPC-C/TPC-H/YCSB run or full package acceptance is claimed.
