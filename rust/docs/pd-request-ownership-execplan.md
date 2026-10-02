# Retain PD request connections without holding the cluster lock

This living ExecPlan follows PLANS.md and the W01 native-client sequence.

## Purpose and scope


A stalled PD metadata RPC currently blocks unrelated metadata and timestamp
requests. RetryClient's retry_mut macro keeps the cluster write lock across
the RPC; the timestamp path keeps its read lock while waiting for a timestamp.
Go takes a service connection and lets each RPC retain it independently.
Repair recorded P07 across every existing native PD request and reconnect
caller, then synchronize TiDB. This is maintenance of the existing native
owner, not acceptance of the entire PD root/discovery/TSO packages. P03 and
P06 retain their service-mode and complete lifetime obligations.

Starting integration is 130502f91290243a675232165f46dda09798f59d; native master
is c97dafb89883312deb526dc8d8f36cc7f7001f47. Both branches were clean and equal
to their freshly fetched remote heads. Go master remains
93a01d31f6da205ae4bf376825293903a6899fdb, selecting client-go
v2.0.8-0.20260928031501-8edb23f6c7ee and PD client
v0.0.0-20260805103528-afa43111d149. The PD root has no doc.go. Its client.go
GetRegion/GetAllStores/ScanRegions and inner_client.go getServiceClient show
the selected connection retained outside discovery synchronization.

## Progress


- [x] Fetch both repositories; verify the native lock spans, source connection acquisition and TSO cancellation owner.
- [x] Reproduce request serialization and blocked leader replacement using real local tonic transport (all four tests fail with bounded timeouts).
- [x] Release cluster synchronization before every request wait; retain source response/error/deadline behavior and single reconnect ownership.
- [x] Run native owner/caller regression suites, static checks and source comparison; publish native master 952013279bc64e590f17c18b9c9222fdaf5a3604 (remote SHA verified).
- [x] Synchronize TiDB through the maintained patch/generation workflow; validate consumers and reconcile P07.
- [x] Run lint and the actual locked server pre-commit hook; enforce final publication through the fresh-build/push/remote-verification chain below.

## Design and milestones


First extend the existing native timestamp transport fixture with controlled PD
metadata replies and stalled requests. Before production changes, verify that
one stalled region request blocks unrelated metadata/timestamps, that a stalled
timestamp blocks metadata/replacement, and that reconnect discovery can block
requests. Tests must release/abort their stalled work before asserting failure.
Keep the original TSO stream-reuse, replacement, cancellation and deadline cases.

Cluster remains the sole owner of its TSO connection manager. Do not clone that
lifecycle owner or attach cancellation to temporary request handles. Instead,
each Cluster request constructor returns a Send + 'static future retaining the
needed tonic client, cluster ID and owned arguments (or selected TSO context).
The explicit static lifetime makes accidental borrowing of Cluster impossible.
Callers still await the same public operation; decoding and logical metrics
remain inside the shared retry lifecycle. Remove retry_mut and use one retry
macro which constructs the owned future under a short read lock, drops the
guard, then awaits it. Retry attempts acquire the current connection again.

Keep reconnect serialization separate from request publication. Discover a
replacement outside the cluster lock, publish its connection/membership under
a short write lock, and join any retired TSO outside that lock. Failed discovery
keeps the old connection and stream alive. A canceled discovery releases its
reconnect guard. Preserve same-URL healthy-stream reuse, canceled-context
replacement and joined retirement. No retry-budget or provider policy changes
are part of P07.

After native tests and self-review, commit and push to ngaut/client-rust master.
Run bash rust/scripts/sync-tikv-client-rs.sh from TiDB root, retaining all four
maintained patches and regenerated protocol outputs. Check the complete diff;
generated outputs must not be hand-edited. Only close P07 if its whole recorded
request/replacement boundary is proven. Preserve earlier audit snapshots.

## Validation


From /Users/qiliu/projects/client-rust run the new focused transport regressions
before and after the change, then:

    cargo test --locked --lib pd::
    cargo test --locked --lib region_cache::test
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

The previous all-target Clippy run reports two unrelated redundant closures in
pd/circuitbreaker_tests.rs; no suppression or unrelated cleanup is intended.
Compare the controlled transport contract against the pinned Go client, with
race detection and enabled/cleaned-up failpoints when required by its package.
Record exact source-oracle commands and outcomes as work proceeds.

From TiDB rust/ run:

    cargo test --locked -p tidb-pd-client --lib --test all -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test all pd_region_loader_source
    cargo test --locked -p tidb-txnkv --test all region_topology_source
    cargo check --locked -p tidb-pd-client -p tidb-txnkv -p tidb-server --all-targets

Confirm available targets before invoking them. At root run make lint,
git diff --check and verify JSON/Markdown finding consistency. This Rust-only
change does not require bazel_prepare. Use TERM=xterm git -c core.hooksPath=hooks
commit so the actual hook runs cd rust && cargo build --locked -p tidb-server.
After the final commit/amend, rerun that exact locked server build immediately
before pushing HEAD:hparser-integration. Verify remote SHA equality and clean
working trees. Keep logs under /private/tmp/native-pd-concurrency-* and
/private/tmp/tidb-pd-concurrency-*.

## Surprises & Discoveries


Cluster::Drop cancels the TSO manager. Deriving Clone on Cluster would give a
temporary request ownership of shutdown, or delay shutdown if moved behind a
new shared owner. Retain only request resources instead. Reconnect currently
holds the same write lock over discovery and retirement, so changing metadata
RPC locking alone would leave a long request-admission barrier.

## Decision Log


On 2026-10-02 choose owned request futures plus separate reconnect serialization.
This keeps all existing public request call shapes and response handling while
making the lock lifetime checkable by Rust. A read lock across network awaits
would allow some concurrency but still block replacement. A cloned lifecycle
owner would weaken cancellation ownership. Neither satisfies the full boundary.

Disk has about 7.5 GiB available. Native package build outputs can be cleaned
after their previous validation/push; dependencies and source must remain intact.

Native cargo clean -p tikv-client removed 376,321 generated artifacts (Cargo's
logical accounting: 686.8 GiB). Available filesystem space rose from 7.5 GiB to
407 GiB. Source/dependency inputs, Cargo.lock and prior validation logs remain.
The first extended tonic fixture compile needed explicit response/request
ProstCodec types because the same service now implements multiple unary RPCs.
After that test-only correction, all four tests fail before production repair
and pass afterward. Original assertions were retained.

## Native validation evidence


The focused command cargo test --locked --lib source_pd_concurrency_ fails all
four cases before repair: stalled metadata excludes other metadata and TSO,
stalled TSO excludes metadata, retained metadata prevents replacement, and
discovery prevents metadata. After repair all four pass. Headers, cluster ID,
region/store/range results, old-connection completion, new-leader selection and
TSO retirement are asserted through local tonic transport. These are not live
PD failover or benchmark claims.

All 151 native PD cases, 89 cache cases and 1,528 library cases pass (two existing
library cases ignored). Strict library Clippy, formatting and diff checks pass.
The final cargo check --locked --all-targets also passes, covering public
Cluster call sites in external API tests and examples after the future-lifetime
change; /private/tmp/native-pd-concurrency-check-all-targets.log retains output.
Logs are /private/tmp/native-pd-concurrency-{red,green,pd,cache,all,clippy}.log.
The remaining cluster guards cover only future construction, identity lookup,
leader publication and reconnect timestamp publication; no guard spans RPC,
discovery or TSO retirement awaits. Cluster::Drop retains sole manager
cancellation ownership, and no retry_mut implementation remains.

The Go root has failpoint references. Copy the pinned PD module to
/private/tmp/pd-request-ownership.VqBmsj, make that disposable copy writable,
then copy rust/docs/parity/current-audit/pd-request-concurrency-oracle_test.go.txt
to its request_ownership_oracle_test.go. From the disposable copy run:

    bash -c 'set -e; trap '\''/Users/qiliu/projects/tidb/tools/bin/failpoint-ctl disable . '\'' EXIT; /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl enable .; GOTOOLCHAIN=go1.25.14 go test -race . -run "^(TestRequestOwnershipOracle|TestLoadKeyspaceByID.*)$" -count=1 -v'

The four original LoadKeyspaceByID cases and the retained request-ownership
oracle pass with race detection and the source TestMain's leak checks.
/private/tmp/go-pd-concurrency-oracle.log retains the output. The oracle uses
the actual Go metadata methods over gRPC and substitutes atomic service
publication in discovery; it does not certify the entire discovery/TSO owner.
It verifies overlapping requests, source ranges/results and old/new connection
identity after publication. Its release and response waits are bounded.

The maintained dependency sync applied all four patches and regenerated
protobuf artifacts without a generated-code diff. The three changed native
source files compare byte-for-byte with the synchronized TiDB copy. Cargo.lock
does not change. The initial sandboxed PD test run could not bind its localhost
etcd fixture (EPERM) and exposed a shared-metric assertion when run in parallel.
The network-enabled serial command above, matching earlier PD receipts, passes
the library and all-target integration suites without changing their assertions.

## Outcomes & Retrospective


Native 952013279bc64e590f17c18b9c9222fdaf5a3604 is published on master, with
the remote SHA verified. Its request futures and reconnect publication remove
the recorded P07 lock-ownership mismatch throughout the existing API, without
adding a second implementation or transferring Cluster shutdown ownership.
The TiDB dependency is synchronized and P07 is repaired. P03/P06 and complete
Go package acceptance remain open. The register is now 85 tracked, 73 unresolved
(67 open, six partial), twelve repaired; all 85 JSON/Markdown entries agree.

TiDB validation passes 28 PD library cases (one existing ignored), 46 PD
integration cases, 15 region-loader cases and seven topology cases. The locked
all-target check and make lint pass. Existing unrelated compiler warnings are
retained. The PD integration cases include their existing local TLS checks;
live PD failover, mixed-node clusters, service-mode changes, other platforms and
sysbench/TPC-C/TPC-H/YCSB remain unverified. No throughput claim is made.
The actual pre-commit hook passed cd rust && cargo build --locked -p tidb-server;
output is /private/tmp/tidb-pd-concurrency-commit.log. This receipt amendment
runs through the same hook, logged in /private/tmp/tidb-pd-concurrency-amend.log.
After the final amendment, publication must use this chain from repository root:

    (cd rust && cargo build --locked -p tidb-server > /private/tmp/tidb-pd-concurrency-prepush.log 2>&1) && git push origin HEAD:hparser-integration && git rev-parse HEAD && git ls-remote origin refs/heads/hparser-integration && git status --short

Verify equal local/remote hashes and clean trees for TiDB and native client-rust.
The final response retains the published TiDB hash; this receipt cannot embed
its own commit hash. No force push, generated-code hand edit or unrelated
source cleanup is used.
