# Continue the complete coprocessor package parity audit


This living ExecPlan follows PLANS.md. The acceptance unit remains the whole
TiDB master `pkg/store/copr` package, with its separately built dependencies and
tests. No individual function or milestone is a transcreated-package claim.

## Purpose and current source


Readers must preserve master's retry behavior when TiKV reports a lock despite
the exact request having marked its transaction resolved or committed. Such a
reply must consume the existing TxnLockFast retry budget before resolution;
repeated ignored hints must terminate with the registered storage error instead
of spinning. Clean reads must incur no additional hint allocation or wait.

The working branch is hparser-integration, pulled before work. Master remains
64e8c4c05ecbe7dfe3eca211c4fb44f97bd75c59. The complete root and dependency
artifact inventory is rust/docs/parity/copr-package-inventory.md (20 root
artifacts plus five artifacts in separate copr_test/metrics packages).
Master pins client-go/v2 v2.0.8-0.20260921040125-5f38569c8cc0. Its complete
txnkv/txnlock package is an additional open dependency unit, not a claimed port
of lock_resolver.go alone. Its source is cached under the matching Go module
directory; inventory every artifact and module build input before editing.

## Progress


- [x] Refresh branch/master and inspect current source, inventories and live Rust paths.
- [x] Confirm exact-RPC lock hints are retained but ignored-hint backoff is missing.
- [x] Confirm store-batched tasks are rejected by the live coordinator; serial timeout scaling alone cannot close that gap.
- [x] Inventory all six pinned txnlock artifacts (2,197 lines) and three module inputs; preserve whole-package boundaries.
- [x] Add the 64-case master-backed resolved/committed/shared hint, ordering, cancellation and retry-exhaustion regression.
- [x] Implement the coprocessor retry and typed error propagation through the existing response/SQL owners; verify red/green behavior.
- [x] Audit the four snapshot read callers and remaining store-batch requirements; record the unresolved integration below.
- [ ] Reconcile the complete snapshot package and store-batch admission/reconciliation/retry/deadline behavior, with their original tests.
- [x] Run scoped Rust and original master tests, dependent compilation, lint and self-review.
- [ ] Satisfy the remaining whole-package build/platform/generated/live-store and workload gates.

## Context and implementation milestones


Go coprocessor.go snapshots ResolvedLocks/CommittedLocks from the sent request,
passes them into client-go ResolveLocksWithOpts, and suppresses duplicate hint
backoffs across the parent and children of one store-batch response. The pinned
client-go helper checks read operations only, backs off once if any returned
lock matches either hint set, then resolves every lock normally. Ordinary TTL
backoff can follow separately. Repeated responses receive fresh hint checks.

Rust DirectUnaryQueryResponse retains client_request.context per dispatch and
has a per-region RegionBackoffBudget plus an injectable RegionRetryWaiter.
Its lock delegate handles resolution, and snapshot_locks supplies hints to later
RPCs. Use the sent context, never the potentially newer shared snapshot sets.
The raw/decoded response owners currently erase storage error identities into
strings. Preserve the relevant registered storage error through those owners
and the existing SQL conversion. Add a regression before each behavior fix;
the original code must fail on observable retry/wake/error behavior.

First reconcile the already-live coprocessor response path and its error
transport. Then inspect all read callers of the shared lock resolver, including
Get, BatchGet and Scan, for the same source contract. Store batching requires
coordinator admission, task/result reconciliation, retry preservation and
serial timeout scaling together. Keep that full milestone open until all of
those behaviors and original tests are owned, rather than adding an unused
timeout helper. The complete MPP, metrics, region-cache, build and live-store
package gates in the existing inventory also remain open.

## Decision Log


Use the existing retry budget and cancellation authority. Do not create a new
attempt cap, busy loop, hidden retry policy or a success fallback. Hint checking
belongs only on a lock response; successful request dispatch must retain its
current cost. Preserve Rust ownership across asynchronous completion and
callbacks while matching Go's externally observable contract.

## Validation and recovery


Run from rust/ with offline locked Cargo and 12 jobs: affected direct_unary
integration/unit tests, response_channel/query_runtime tests, relevant txnkv
lock/snapshot source tests, and dependent compilation through tidb-exec and
tidb-executor. Run root make lint, scoped rustfmt and git diff --check. Original
master coprocessor ignored-hint tests live under TestHandleBatchCopResponse;
use the failpoint-aware wrapper because the package contains failpoint calls.
Report the already-unsatisfied fresh-worktree Bazel gate separately (bazel is
not installed), along with real TiKV and matched sysbench/TPC-C/TPC-H/YCSB
benchmarks. Do not infer workload performance from synthetic retry counts.

Keep the preexisting untracked vs_helper.rs and fragment.rs untouched. Preserve
all source/test state before isolated red-proof mutations and restore it even
if a command fails. Only commit reviewed task files; fetch/pull before push.

## Surprises & Discoveries


The Rust coordinator explicitly rejects envelopes with batch_task_list in both
initial and retry preparation. Existing request/protobuf support does not prove
that a real store batch can execute. The new master timeout and per-response
child hint rules therefore require integration beyond their metadata fields.
Existing generic Source(String) conversions also prevent a lock timeout from
reaching SQL with Go's registered code 9004.

## Outcomes & Retrospective


The live coprocessor path now charges one TxnLockFast delay before resolving a
lock that matches either hint set in the exact sent request. TxnLockFast is the
client-go retry category for a short lock-resolution wait. An ordinary shared
lock response charges at most once, even when multiple children match. Repeated
ignored hints consume the existing region budget and return the registered
9004 / HY000 / [tikv:9004]Resolve lock timeout error through raw responses,
decoded rows/chunks and the executor's CopRowStream. Cancellation stops before
resolution. Successful reads gain no new hint allocation or wait.

This is tested seed evidence within the open whole-package audit. It does not
complete pkg/store/copr, the pinned txnlock package or any snapshot package.
There is no real-store or sysbench/TPC-C/TPC-H/YCSB performance claim.


## Implementation and remaining caller audit, 2026-09-22


In rust/crates/tidb-distsql/src/cop_paging/direct_unary_query_transport.rs,
settle_dispatch checks client_request.context before invoking the lock delegate.
Shared-lock children are checked instead of the outer lock when present, as in
master. Both ignored-hint and ordinary TTL exhaustion retain the existing
backoffer's selected category. QueryResponseError::Sql and SelectResponseSource
preserve the registered code rather than turning it into Source(String).
CopRowStream already converts the decoded error to StorageError::Sql; a new
row/chunk regression proves that boundary and its SQLSTATE.

StorageDriverError::from_backoff in tidb-txnkv/src/driver_error.rs owns the
existing category-to-error mapping moved unchanged from tidb-exec's
pessimistic_lock_error.rs. Both callers now use that one mapping. Its existing
14-category test still passes. No write-lock retry policy changed.

Decision (2026-09-22): keep the snapshot changes as a subsequent integration
milestone after this coprocessor fix. The four read sites in
rust/crates/tidb-txnkv/src/transaction/coordinator/snapshot_read.rs are
snapshot_get_with, snapshot_scan_with, RealOptimisticTransaction::snapshot_get_at
and RealOptimisticTransaction::snapshot_batch_get_at. All call resolve_blocking_locks with reader permission but
omit ignored-hint backoff. The point and scan sites retain sent contexts.
BatchGet currently gathers locks from all published region replies and retains
only the last lock_context, whereas client-go snapshot.go resolves within each
batch using that batch's request hints. It also has separate lock and region
budgets and string-only SnapshotGet errors. Inventory the complete pinned
client-go txnkv/txnsnapshot package before implementing this milestone; reconcile
per-response ownership, shared budgets, cancellation and SQL identity together.
Do not add the read-only backoff to prewrite or pessimistic writer callers.

The live Rust CopReadTaskRuntime rejects batch_task_list during initial and
retry preparation. The next store-batch milestone must admit the envelope,
reconcile parent/child responses, preserve retry/hint state, and multiply the
per-RPC timeout by child count plus one only for serial execution. Master
suppresses duplicate ignored-hint backoffs across one parent/child response.
Passing Go's batched tests below does not mean Rust supports those envelopes.

## Validation receipt


All commands below ran successfully unless explicitly identified as a red
proof. In rust/:

    cargo test --offline --locked -j12 -p tidb-distsql --test all direct_unary --message-format=short
    cargo test --offline --locked -j12 -p tidb-distsql --test all query_runtime_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-distsql --test all response_channel --message-format=short
    cargo test --offline --locked -j12 -p tidb-distsql --lib direct_unary --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test all driver_error_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-exec --lib pessimistic_lock_error --message-format=short
    cargo test --offline --locked -j12 -p tidb-distsql --test all registered_storage_error_survives --message-format=short
    cargo test --offline --locked -j12 -p tidb-exec --lib registered_storage_errors_reach --message-format=short
    cargo check --offline --locked -j12 -p tidb-txnkv -p tidb-distsql -p tidb-exec -p tidb-executor -p tidb-session --message-format=short

Test counts in order are 67, 13, 6, 2, 9, 11, 1 and 1; filtered suites overlap.
The 67 direct-unary tests include the new 64-case nested matrix. Existing
compiler warnings remain. Logs use /private/tmp/tidb-copr-hints- with suffixes
direct.log, query.log, channel.log, lib.log, driver.log, pessimistic.log,
decoder.log, sql.log and check.log.

The original transport failed ignored_request_lock_hints with zero sleeps
instead of two; red.log records that assertion. Restoring source-string error
flattening in an isolated try/finally probe then made the decoder test fail
with Source instead of code 9004, and CopRowStream fail with Backend instead
of Sql. decoder-red.log and sql-red.log capture those failures. Restoring the
implementation made both tests pass; the final check ran after restoration.
The temporary probe is /private/tmp/tidb-copr-hints-error-red.py.

From the repository root:

    make lint
    python3 /private/tmp/tidb-copr-hints-format.py --check
    git diff --check

The formatter checks only changed Rust functions/lines, preserving unrelated
formatting. A partial-rustfmt enum newline artifact was detected and corrected
before final validation. No Go, module or Bazel artifacts changed, so this diff
does not trigger change-based bazel_prepare. The earlier fresh Go worktree's
bazel_prepare attempt remains unsatisfied because bazel is unavailable.

From /private/tmp/tidb-master-cc83514 at master
64e8c4c05ecbe7dfe3eca211c4fb44f97bd75c59:

    GOTOOLCHAIN=go1.26.0 GOCACHE=/private/tmp/tidb-gocache ./tools/check/failpoint-go-test.sh pkg/store/copr -run '^TestHandleBatchCopResponse$/(backs_off_when_TiKV_ignores_lock_hints|stops_after_repeated_ignored_lock_hints|backs_off_once_per_response_for_ignored_lock_hints)$' -count=1 -v

All selected original subtests passed (1.192s package runtime), including the
resolved/committed, unhinted parent/child, different hinted transaction and
shared-child cases. go.log records wrapper cleanup to failpoint refcount zero;
the worktree remained clean. This is supplementary source-oracle evidence,
not satisfaction of the missing Bazel or Rust store-batch gates.

The complete six-file txnlock inventory and three module-input SHA-256 values
were rechecked against the pinned module cache. Self-review found no unrelated
source churn or invented Go behavior. The working branch and master were
fetched and the working branch pulled again before publication; both were
unchanged. Commit the reviewed files with `distsql: back off ignored request
lock hints`, push hparser-integration and verify HEAD equals its remote ref.
Preserve the two preexisting untracked user files. Disk availability after
validation was approximately 231 GiB; the earlier cleanup remains effective.

Main remaining correctness risk is untested real TiKV/concurrent response
behavior beyond the scripted matrix. The fix prevents unbounded immediate
retries under ignored hints; matched workload throughput and latency are still
unmeasured. Full original package, build, generated/platform, MPP, metrics and
live-store gates remain open. This receipt changes no package acceptance status.

Revision note: recorded exact source ownership, the tested coprocessor outcome,
red/green evidence, required gates and concrete snapshot/store-batch gaps so the
whole-package audit can continue without treating this milestone as completion.
