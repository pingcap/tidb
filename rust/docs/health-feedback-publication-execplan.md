# Publish shared store-health metadata independently of update locking

This living ExecPlan follows root PLANS.md and continues store-health-owner-execplan.md.

## Purpose and source boundary


A periodic health tick must still request overdue feedback when another update
holds the mutation lock. Go checks atomic feedback presence, score and timestamp
before calling TiKV; only the update/decay phase uses TryLock. Native Rust keeps
presence/time inside that mutex and silently skips the request on contention.
Fix the existing native owner and synchronize every TiDB consumer through the
maintained dependency workflow. Do not add another TiDB health implementation.

Fresh references: integration 1e5b7e65a494111a4a044f1ec162a66a174171a3,
Go master 93a01d31f6da205ae4bf376825293903a6899fdb, native client-rust master
6163ecfc587b248dcbf0e30c1c9d905b4bc5a665. Master pins client-go
v2.0.8-0.20260928031501-8edb23f6c7ee. Its internal/locate has no doc.go.
store_cache.go::StoreHealthStatus and updateTiKVServerSideSlowScoreOnTick own
the contract. This is existing-package maintenance and closure of recorded T04,
not acceptance of the entire locate package or T02's routing/runtime scope.

## Progress


- [x] Fetch both repositories and inspect the source fields, admission, update, callback and decay paths.
- [x] Reproduce a suppressed overdue request under a held update lock (expected false/true admission, observed false/false).
- [x] Publish read-side metadata atomically, retain nonblocking updates and source rate-limit/recheck ordering.
- [x] Validate native state and cache callers, upstream source cases and static checks; publish native master c97dafb89883312deb526dc8d8f36cc7f7001f47 (remote SHA verified).
- [x] Synchronize TiDB through its generator/patch workflow and validate shared health/routing consumers.
- [x] Reconcile T04 evidence, run lint and the actual locked server pre-commit hook; publish through the mandatory fresh-build/push/remote-verification chain below.

## Design and milestones


Extend the existing native contention regression before editing production. It
must observe no request before the 15-second boundary and a due request exactly
at the boundary while the update lock remains held. Retain the checks proving
feedback writes/decay skip contention and resume after unlock.

Use an atomic boolean for feedback presence and ArcSwapOption<Instant> for the
immutable last-update timestamp, alongside the existing atomic score. ArcSwap
provides safe pointer publication and reclamation without an invented integer
clock range or unsafe raw-pointer ownership. Keep one Mutex<()> only for writers.
Follow Go's pre-lock and in-lock timestamp checks and unchanged-score refresh.
Existing cache callbacks still execute before attempted decay; verify successful
feedback refresh prevents decay and failed/ineligible callbacks retain it.
Read/write field selection must never depend on try_lock success for admission.

Review the full health lifecycle and all native/TiDB call sites. Keep public
StoreHealthStatus methods and shared handle identity stable. Add no public
feature, SQL behavior or protocol field. Native Cargo.toml/Cargo.lock may add
arc-swap as the Rust implementation of Go's atomic timestamp pointer. Update
the existing native source receipt and publish before copying the dependency.

Then run rust/scripts/sync-tikv-client-rs.sh. Review every resulting diff,
including the lockfile and generated output. Close only T04 after its regression
and cache caller checks pass; keep T02 and all other unresolved findings open.

## Validation


In /Users/qiliu/projects/client-rust run:

    cargo test --locked --lib health_feedback_and_tick_skip_a_concurrent_update
    cargo test --locked --lib locate::tests
    cargo test --locked --lib region_cache::test
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --all-targets -- -D warnings
    cargo fmt --all --check
    git diff --check

Run the pinned Go health source tests with race detection from a disposable
copy, enabling/disabling its package failpoints without touching cached source.
Retain exact commands and results in this receipt. No original assertion may be
weakened to make a candidate pass.

The disposable copy is /private/tmp/client-go-health-publication.JBOpRN, copied
from the pinned module path above. Its package has failpoint references. From
that directory the executed command was:

    bash -c 'set -e; trap '\''/Users/qiliu/projects/tidb/tools/bin/failpoint-ctl disable . '\'' EXIT; /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl enable .; GOTOOLCHAIN=go1.25.14 go test -race ./internal/locate -run "^TestRegionCache$/(TestTiKVSideSlowScore|TestStoreHealthStatus|TestRegionCacheHandleHealthStatus)$" -count=1 -v'

All three original cases passed; /private/tmp/go-health-publication.log retains
the output. An additional disposable TestHealthPublicationWhileWriterHeld
case holds the source writer lock while tick requests feedback, verifies the
callback observes client score 1 and unchanged TiKV score 80, then verifies
decay to 75 after unlock. The same command with -run
'^TestHealthPublicationWhileWriterHeld$' passed under -race; output is
/private/tmp/go-health-publication-oracle.log. Both traps disabled failpoints.

In TiDB rust/ run:

    cargo test --locked -p tidb-txnkv --test all replica_health_scoring_source
    cargo test --locked -p tidb-txnkv --test all region_topology_source
    cargo test --locked -p tidb-txnkv --test region_error_recovery_source
    cargo test --locked -p tidb-distsql --lib --test all
    cargo check --locked -p tidb-txnkv -p tidb-distsql -p tidb-server --all-targets

At root run make lint and git diff --check. Rust-only changes require no
bazel_prepare. These checks cover the native owner, real cache caller and TiDB
routing handle contract. Real TiKV failover, ThreadSanitizer, other platforms
and sysbench/TPC-C/TPC-H/YCSB are not covered or claimed by local unit tests.

## Publication and recovery


Native changes are authorized for /Users/qiliu/projects/client-rust master;
check for concurrent remote commits before publishing. Synchronize only after
that source commit is remotely available. Never edit generated vendor outputs
by hand or force-push. TiDB commits use TERM=xterm git -c core.hooksPath=hooks
commit; the actual hook must pass cd rust && cargo build --locked -p tidb-server.
Repeat that locked build after the final commit/amend immediately before push
to hparser-integration. Verify both remote SHAs and clean trees. Store logs
under /private/tmp/native-health-publication-* and /private/tmp/tidb-health-publication-*.

## Surprises & Discoveries


The earlier health receipt explicitly described deferring active feedback while
the metadata mutex was held. Source review shows that policy belongs only to
writes and decay; the read-side metadata deliberately remains atomic in Go.

## Decision Log


On 2026-10-02 choose independent atomic publication with safe Rust ownership.
A blocking read lock or a second fallback timer would introduce extra policy.
Use the same native object for every caller and preserve monotonic Instant
values without converting to wall time or lossy numeric ticks.

The safe timestamp pointer uses [arc-swap 1.9.2](https://docs.rs/arc-swap/1.9.2/arc_swap/).
The native repository ignores Cargo.lock; Cargo resolved the added dependency in
its local lockfile. TiDB tracks its workspace lockfile and must retain that resolution.
Review also found that the cache invoked its callback before updating the client
score. Move the asynchronous tick lifecycle into StoreHealthStatus and keep the
public no-callback tick adapter; both share admission, mutation and decay policy.

The first cache test filter used region_cache::tests and matched zero tests;
the actual module is region_cache::test. Corrected invocation runs 89 cases.
The new cache regression initially needed a qualified std::time::Instant import;
the production regression passed before adding that cache test.

The full native library passes 1,524 tests with two existing ignored tests.
All-target strict Clippy stops on two existing redundant_closure warnings in
src/pd/circuitbreaker_tests.rs:369 and :371. They are outside this change;
retain that failed check and run strict library Clippy separately, without
loosening lints or changing unrelated tests.

Strict library Clippy and formatting pass. The two cache regressions were also
run against pre-fix production with only test support retained: the held-writer
callback count was zero instead of one, and the successful callback observed
client score zero instead of one. Restoring the fix makes both pass. Their
red/green logs are /private/tmp/native-health-publication-{red-*,cache-green,order-green}.log.

The maintained sync script applied all four patches and regenerated protobuf
outputs without a generated-code diff. Cargo check --offline -p tidb-txnkv
-p tidb-distsql -p tidb-server --all-targets resolved just arc-swap 1.9.2 into
the tracked TiDB lockfile, before the locked validation commands above.
The first sandboxed make lint could not resolve proxy.golang.org to install its
pinned revive tool; rerun with network access rather than skipping the gate.

## Outcomes & Retrospective


Native c97dafb89883312deb526dc8d8f36cc7f7001f47 is published on master and
the maintained sync reproduces it with all four patches. T04's read/admission
boundary and shared tick ordering are repaired. Native public methods and
TiDB's shared Arc handle retain their identity; the locked consumer check passes.
No second health implementation was added. T02 and complete internal/locate
acceptance remain open; not all TiDB production health events are wired.

TiDB validation passes 12 replica-health, seven topology and 26 region-recovery
cases. DistSQL's library/all targets pass 37 and 256 cases respectively (one and
two existing ignored cases; /private/tmp/tidb-health-publication-distsql.log).
The locked all-target check
and required make lint pass. Existing unused-import/mut warnings remain outside
this change. The first sandbox lint attempt failed only on dependency DNS; the
authorized network-enabled rerun passes. No Go/Bazel source changed, so no
bazel_prepare was required. Historical audit hashes are unchanged; the JSON's
latest_repair links this native source and T04 receipt. The register totals are
85 tracked, 74 unresolved (68 open, six partial), eleven repaired.

After native validation, removed its target/debug/incremental cache (du reported
13 GiB). Available filesystem space increased from 7.4 GiB to 14 GiB; APFS
sharing/allocation means logical cache size is not the reclaimed physical size.
Source, commits, lockfiles, final binaries and validation logs are preserved.

The actual pre-commit hook passed cd rust && cargo build --locked -p tidb-server;
the commit log is /private/tmp/tidb-health-publication-commit.log. This receipt
amendment runs through the same hook, with its output in
/private/tmp/tidb-health-publication-amend.log. Final publication is enforced by
the following command chain from the repository root after the final amendment:

    (cd rust && cargo build --locked -p tidb-server > /private/tmp/tidb-health-publication-prepush.log 2>&1) && git push origin HEAD:hparser-integration && git rev-parse HEAD && git ls-remote origin refs/heads/hparser-integration && git status --short

Verify equal local/remote SHAs and clean trees for both repositories. No force
push is used. The final response retains the published TiDB hash independently
of this document, which cannot embed its own commit hash.

No live cluster, mixed-node failover, other platform, ThreadSanitizer or
sysbench/TPC-C/TPC-H/YCSB throughput run was performed. Safe timestamp publication
allocates like Go's timestamp pointer; performance impact is not measured.
