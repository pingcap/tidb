# Preserve Go PD stream connection ownership

This living ExecPlan follows TiDB PLANS.md. The minimum acceptance unit is the
complete pinned PD pkg/connectionctx package, a dependency of the still-open
W01 PD root, discovery and TSO packages. Keep all sections current.

## Purpose and context


Native metadata reconnect currently replaces a healthy TSO stream even when the
leader URL is unchanged. Go clients/tso.tryConnectToTSO retains the registered
connection and collects only stale URLs. Implement its complete connectionctx
owner, and integrate the existing native single-leader stream through it.
This avoids revoking healthy in-flight work during unrelated metadata refresh.

The source is github.com/tikv/pd/client at
v0.0.0-20260805103528-afa43111d149, selected by freshly fetched TiDB Go master
93a01d31f6da205ae4bf376825293903a6899fdb. Native baseline is
5928b6e480b441496f9a3cd9bed6a7e8d56215a1; TiDB integration starts at
6b4f1a9d861d46930cdcdae554a80164ba58f3ce. The package contains manager.go and
manager_test.go only: no doc.go, generated inputs, fixtures, package build
files, platform variants or build tags. Inventory both artifacts, all five
testutil support artifacts and shared module/build/license inputs by hash.

## Progress


- [x] Refresh both repositories and master; PD pin unchanged. Read complete source, original tests and production TSO call sites.
- [x] Reproduce healthy-stream replacement before production edits: the stream-local sequence restarted at 1 instead of continuing to 2.
- [x] Implement every manager operation and original TestManager/TestCancelFunc case, including explicit ownership rejection and cancellation.
- [x] Migrate native single-leader TSO selection/replacement/drop; preserve worker joins and completed results.
- [x] Pass original Go race/goleak suite, 1,435 native tests (two pre-existing ignored), strict Clippy, all-target compilation, formatting and source inventory checks.
- [ ] Publish native master; synchronize TiDB; run adapter tests, root lint, actual commit-hook locked server build and fresh pre-push locked build; push integration.

## Plan of work and milestones


First extend src/pd/timestamp_tests.rs with a loopback transport regression:
metadata reconnect to the same leader must reuse its active stream and preserve
its next response. Run it on unchanged production code and record the failure.

Implement src/pd/connectionctx.rs: a short synchronous RwLock protects URL to
Arc<ConnectionCtx<T>> entries. Store returns whether ownership was accepted;
rejection leaves cancellation with the caller. Replacement/GC/release cancel
before deletion; exclusive store removes other URLs even when its own URL is
already present. Selection uses uniform reservoir sampling. Retrieved handles
retain the same stream/context identity through removal. Cancellation callbacks
execute under the same write lock as Go and must not re-enter the manager.
The manager itself starts no task. Rust assertions and resource-drop checks
map the original Go tests and goleak responsibilities.

Migrate src/pd/cluster.rs from its unconditional single-oracle replacement to
this shared owner. Carry the actual successfully dialed URL out of leader
connection establishment. Reuse the healthy matching entry; release canceled
entries; use exclusive store for new leaders. Retain retired oracle handles
until their workers join. Cluster drop releases all managed contexts. The
native single-leader mode is retained; this does not add proxy/service modes.
Expose the existing oracle cancellation handle internally for the context
adapter. Do not change request batching/retry limits or public close semantics.

## Validation and acceptance


From the native root, run the fail-before regression, focused package and
transport cases, then the full library suite and static checks:

    cargo test --locked --lib source_connectionctx_ -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

Run unchanged pinned source in a separate temporary module copy:

    go test -mod=readonly -race ./pkg/connectionctx -count=1

No failpoint call occurs in this package or selected support path. Do not
instrument unrelated packages. No Go/Bazel/module changes are planned in TiDB,
so bazel_prepare is not triggered. Use the maintained TiDB sync script after
native publication, targeted PD/transaction bridge tests, root make lint and
both mandatory locked tidb-server build gates. Record exact results and gaps
in the TiDB receipt. Never edit generated artifacts or force-push.

## Surprises & Discoveries


The fail-before transport reproduction is in /private/tmp/pd-connectionctx-red.log.
The unchanged source Go suite passes with race detection and goleak. Additional
transport cases prove leader replacement, canceled same-URL replacement, failed
refresh preservation, exact dialed URL ownership and retained pending-stream
cancellation/join. Full native validation is /private/tmp/pd-connectionctx-native.log;
no new test is ignored.

Source pkg/connectionctx explicitly rejects duplicate ownership without
canceling the candidate. CleanAllAndStore still cancels other URLs before
returning false for a duplicate. Both distinctions require direct regression
coverage; unconditional insertion or automatic rejected-candidate cancellation
would change the source contract.

## Decision Log


Decision (2026-10-01): complete the connectionctx prerequisite with real caller
integration before parent PD acceptance. A same-URL special case alone would
retain the missing shared ownership contract. Do not claim the complete root,
discovery or dispatcher from this leaf. Use Rust Arc/locks and the existing
Cancellation adapter; do not reproduce Go garbage collection or goroutines.

## Outcomes & Retrospective


The complete package and caller integration pass the original Go race/goleak
suite and 1,435 native library tests (two pre-existing ignored). Strict Clippy,
all-target compilation, formatting, diff and inventory checks pass. The first
focused 59-case PD run preceded the exact shared-context test refinement and
one added overwrite control; both are covered in the final full suite. Native
publication and TiDB synchronization/build gates are recorded by the TiDB
receipt, avoiding a self-referential native commit hash. P03 and P07 remain open; P06
remains partial, including public PD close and full parent lifecycle. No live
PD/Linux/workload performance claim follows from local package tests.

## Recovery and artifacts


Keep source and test edits reviewable; do not reset shared work. Failed tests
are evidence to repair, not permission to suppress cases. Publish client-rust
master before synchronizing TiDB. If integration advances, preserve incoming
commits and repeat the final build gates. Evidence and source hashes belong in
doc/pd-connectionctx-package.json and TiDB's current-audit repair receipt.
