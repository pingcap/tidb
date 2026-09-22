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

The working branch is hparser-integration, pulled before work. Current master is
0b505ecc58b659655345b7bb85a619db02f94300. The complete root and dependency
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
- [x] Inventory the complete pinned txnsnapshot and config/retry dependency packages.
- [x] Reconcile the live snapshot lock-hint, scan lock, per-batch retry and registered exhaustion boundaries; preserve the whole-package claim as open.
- [x] Reconcile MaxTS first-lock behavior in both point-read entry paths; verify red/green Rust regressions and the original Go autocommit/hint tests.
- [x] Integrate completion-order BatchGet recovery, independently progressing split workers, cancellation/join and resolving-record ownership; verify scoped regressions and the original Go cancellation test.
- [ ] Reconcile the remaining complete snapshot package and store-batch admission/reconciliation/retry/deadline behavior, with their original tests.
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


## Snapshot integration milestone, 2026-09-22


The previous turn published a2685738f1 and remote equality was verified. The
next turn refreshed master and pulled the branch before editing; neither moved.
Inventory all seven pinned txnkv/txnsnapshot artifacts before changing its Rust
owners (client-go-txnsnapshot-package-inventory.md).

Correction to the earlier four-site audit: raw Scan does not use read lock hints
in master. scan.go retries response-level errors using ResolveLocks (ForRead
false), but Scanner.Next resolves each pair error by snapshot.get and keeps the
other pairs. Rust currently retries the whole scan under read permission for
both. Fix this along with exact-RPC hint backoff in Get and BatchGet. Use one
read-call budget for region errors and lock waits; preserve typed exhausted
categories through OptimisticCoordinatorError and the executor storage boundary.
BatchGet must resolve per physical response, preserving its context and clean
values. Cover shared locks, cached hints, cancellation/exhaustion, bounded page
reads, missing values and clean-prefix retention. No write-lock policy changes.

Add regressions first, run the affected snapshot/lock/scan tests and compiler
checks, then make lint and scoped formatting. Re-run source-oracle tests where
available; live-store, complete package and workload acceptance remain open.
Do not treat the previous statement that all four sites need identical hint
checks as authoritative: scanner source above disproves that assumption.


## Snapshot integration receipt, 2026-09-22


The seven txnsnapshot artifacts and four config/retry artifacts now have complete
SHA-256/line/owner inventories in rust/docs/parity. Each remains an open atomic
package claim; the Rust changes below are integrated seed evidence.

In tidb-txnkv/src/transaction/coordinator/snapshot_read.rs, the transaction-free
point reader and ordinary Get now check the exact RPC's hint sets before lock
resolution. A single call's region errors, ignored hints and live-lock waits
share one budget. BatchGet retains each physical response's context, pending
keys and budget; successful values survive retries. It no longer merges the
remaining keys of independently running batches into one retry. A region split
forks charged time/category history but resets delay schedules, as client-go
Backoffer.Clone/Fork does; an unsplit retry retains its existing schedule.
The native implementation is RegionBackoffBudget::fork in tidb-txnkv/src/retry.rs.

Scanner pair errors now use snapshot_get_with for the locked key and retain the
other page rows; missing values are skipped. The final physical key still moves
the scan cursor even if that key no longer exists. Response-level lock errors
retry the original page through resolution with ForRead false and no read hints
on Scan requests. Visibility is checked before consuming response-level errors,
as in scan.go. Each returned row ends Scanner.Next's budget scope; clean rows
reuse an untouched budget instead of generating a random seed per row, and only
the page's final key is copied for its continuation. These choices avoid adding
per-row allocation or atomic seed updates on clean scans.

Resolved/committed classifications are recorded before the ordinary TTL wait,
including before a cancelled/exhausted wait, matching ClientHelper's ordering.
The TTL cap is the actual returned duration. The previous helper imposed an
unsupported 10ms minimum and could exhaust the budget prematurely. Shared-lock
hint checks still charge once per physical response, not once per child.

OptimisticCoordinatorError::SnapshotBackoff preserves the existing category and
diagnostic. The executor's cluster_table_storage.rs converts it to the existing
SQL error carrier before the legacy text-based retry heuristic. Its storage
conversion also preserves an existing StorageError::Sql. Configured writes and
multi-statement transactions retain that code through their existing error
carriers. Tests prove both 9004 Resolve lock timeout and 9005 Region unavailable;
unregistered categories keep the existing driver mapping. These changes do not
change the write-side lock resolver or add a new retry cap.

### Regression evidence


The original implementation failed scan_pair_locks_use_point_get_without_replaying_clean_rows
because it attempted another whole Scan instead of the two required point Gets.
The ignored-hints test failed because seven locked replies returned without the
six source-shaped waits (minimum 1+2+4+8+16+32ms). Both resolved and committed
hints now reach subsequent Get/BatchGet RPCs. The BatchGet boundary regression
failed with physical request sizes [5120, 1, 2] instead of [5120, 1, 1, 1],
showing that two independent response retries had been merged.

The response-level Scan regression failed because the old path stamped a read
hint after MinCommitTSPushed rather than using non-read lock cleanup. The SQL
regression observed Retryable("[tikv:9004]Resolve lock timeout") instead of the
registered SQL error. The fork regression observed a 4ms child delay where Go
starts a new 2ms delay schedule while retaining charged time. An isolated
restoration of the old 10ms TTL floor made the new 1ms-TTL regression exhaust its
budget prematurely. Each failing behavior passed after its fix. Temporary
try/finally probes restored the working source before final green runs.

### Exact validation


From rust/, all of these passed:

    cargo test --offline --locked -j12 -p tidb-txnkv --test all snapshot_ --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test all region_error_recovery_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test lock_resolver_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --lib transaction::coordinator --message-format=short
    cargo test --offline --locked -j12 -p tidb-exec --lib snapshot_backoff --message-format=short
    cargo test --offline --locked -j12 -p tidb-exec --lib cluster_table_storage --message-format=short
    cargo test --offline --locked -j12 -p tidb-exec --lib commit_error_tests --message-format=short
    cargo test --offline --locked -j12 -p tidb-server --lib configured_write_backoff_error_is_coded_on_the_wire --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --lib snapshot_lock_wait_charges --message-format=short
    cargo check --offline --locked -j12 -p tidb-txnkv -p tidb-distsql -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server --message-format=short

Test counts in order are 17, 26, 30, 24, 3, 6, 2, 1 and 1; suites overlap.
The wire test exercises the existing configured-error carrier, not a live TiKV
snapshot timeout. An earlier aggregate filter for lock_resolver_source selected
zero tests; it was rejected as evidence and replaced by the standalone command
above. Existing warnings remain. Logs use /private/tmp/tidb-snapshot- with
suffixes all.log, region.log, lock.log, coordinator.log, sql-green.log,
storage.log, configured.log, wire.log, ttl-green.log and check.log. Red proofs
are scan-red.log, hints-red.log, batch-red.log, scan-response-red.log,
sql-red.log, fork-red.log and ttl-red.log.

From the root, make lint, scoped formatting and whitespace checks passed:

    make lint
    python3 /private/tmp/tidb-snapshot-format.py --check
    git diff --check

From /private/tmp/tidb-master-cc83514 at master
64e8c4c05ecbe7dfe3eca211c4fb44f97bd75c59:

    GOTOOLCHAIN=go1.26.0 GOCACHE=/private/tmp/tidb-gocache go test github.com/tikv/client-go/v2/txnkv/txnsnapshot -run '^(TestSnapshotRuntimeStats.*|TestCollectBatchGetResponseDataPointResponseStats|TestAsyncBatchGetCancellationWaitsForRetryWorker)$' -count=1 -v
    GOTOOLCHAIN=go1.26.0 GOCACHE=/private/tmp/tidb-gocache go test github.com/tikv/client-go/v2/config/retry -run '^(TestBackoffDeepCopy|TestBackoffUpdateUsingFork|TestBackoffErrorType)$' -count=1 -v

All seven snapshot originals and three retry originals passed (0.122s and
3.943s package runtimes). The retry TestMain ran its goleak check. Neither
package imports/injects failpoints; no source transformation was needed. Go
results are supplementary oracle evidence and do not establish Rust ownership
of the complete original async worker/statistics matrix. Logs are go.log and
go-retry.log with the same prefix. The Go worktree remains unchanged.

### Remaining scope and publication


Self-review corrected Scan's source boundary, lock-set publication ordering and
Backoffer fork semantics rather than carrying the earlier incorrect assumptions
forward. No Go/Bazel/generated/module files changed, so this diff does not trigger
change-based bazel_prepare. The fresh-master-worktree gate is still unsatisfied
because bazel is missing. Full txnsnapshot options, MaxTS first-lock rules,
reverse scans, worker cancellation/join behavior, metrics and original-test
reconciliation remain open, as do txnlock/config/retry package gates. The live
post-publication BatchGet recovery is still sequential; original Go async tests
passing is not evidence of complete Rust worker parity. Store-batched coprocessor
envelopes remain unsupported in the live coordinator.

The main compatibility risk is concurrent read/lock behavior outside the
scripted tests. Real TiKV and matched sysbench/TPC-C/TPC-H/YCSB workloads were
not run, so no throughput or latency gain is claimed. The regression does prove
that clean scan rows survive one locked row without another whole-page RPC.
The goal and all incomplete package claims remain active.

Publish the reviewed files as `txnkv: align snapshot lock retries with master`,
then push hparser-integration and verify remote equality. Preserve the existing
untracked vs_helper.rs and fragment.rs. Approximately 235 GiB remained available
after validation; no additional user data was removed.

Revision note: added complete dependency inventories, integrated snapshot read
behavior, typed error propagation, source-backed fork/TTL fixes, red/green
receipts and explicit remaining whole-package boundaries.


Pre-publication refresh: the branch fast-forwarded from a2685738f1 to
3802c14928, adding only an upstream TPC-H performance receipt in
rust/docs/tpch50-perf-parity-2026-09-22.md. TiDB master stayed at 64e8c4c05e.
That historical benchmark receipt is preserved and is not a measurement of
this snapshot change. No implementation or validation inputs changed in the
fast-forward. Final scoped formatting and whitespace checks passed afterward.


## Current-master MaxTS and worker milestone


The previous snapshot milestone was pushed as bd7f36502c and remote equality
was verified. At the next turn the working branch was pulled unchanged, while
TiDB master advanced to 0b505ecc58b659655345b7bb85a619db02f94300. The client-go
module pin, pkg/store/copr, pkg/kv and module inputs are unchanged from 64e8c4c05e;
the existing dependency inventory hashes still apply. The clean Go oracle
worktree was advanced to the new master. Its new executor adaptive LIMIT
admission controller, related executor/join changes and session-variable changes
are an additional required current-master audit boundary, not covered by the
old executor receipts.

Complete the MaxTS first-lock rule in both point-read entry paths before
continuing the snapshot worker audit. MaxTS is Go's latest-committed point-read
marker (math.MaxUint64), not a transaction timestamp. KVSnapshot.get records
the first lock's transaction; at MaxTS only, a later different transaction not
included in either sent request hint set is added to resolvedLocks without a
status RPC. If TiKV repeats that lock despite the hint, the normal resolver and
backoff must run. Ordinary snapshot timestamps and the original first lock do
not take the shortcut. Track this per Get, including point reads resolving Scan
pairs; do not let one Get's first-lock choice leak into the next.

Prove sent contexts and status-request order with scripted Get/Scan responses
before editing implementation. Audit batch worker publication, completion,
retry and cancellation ownership; preserve the complete txnsnapshot package
acceptance boundary. No worker-parity claim follows merely from concurrent
initial RPC publication or source-only Go tests.

### MaxTS outcome and validation


Both snapshot_get_with (direct autocommit and Scan pair Get) and
RealOptimisticTransaction::snapshot_get_at now retain the first transaction
locally to one Get. A later different transaction at MaxTS is inserted into
SnapshotLockSet's ignored set only when neither exact sent hint set names it.
The next RPC carries that hint. A repeated hinted response takes the existing
backoff and resolver path. Clean successful reads allocate no new collections;
the new state is one stack-local optional transaction ID.

The regression max_ts_get_only_skips_new_unhinted_transactions_after_its_first_lock
failed before implementation: the third sent request had no resolved hint for
transaction 91. It now passes across ordinary/MaxTS timestamps and transactional
Get/Scan-pair entry paths. It verifies transactions 90,91,91,90,92 followed by
success, the status-RPC order, and a later independent Get choosing 93 as its
own first transaction. This completes that rule's seed evidence; the entire
txnsnapshot package remains the acceptance unit.

From rust/, these exact commands passed:

    cargo test --offline --locked -j12 -p tidb-txnkv --test all max_ts_get_only --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test all snapshot_ --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test lock_resolver_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --lib transaction::coordinator --message-format=short
    cargo check --offline --locked -j12 -p tidb-txnkv -p tidb-distsql -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server --message-format=short

The regression passed one test; the surrounding runs passed 18 snapshot, 30
lock-resolver and 24 coordinator tests. Existing compilation warnings remain.
From the repository root:

    make lint
    python3 /private/tmp/tidb-snapshot-format.py --check
    git diff --check

The first lint attempt could not resolve proxy.golang.org while installing
the missing pinned revive tool. Rerunning with network permission restored the
tool and passed the complete lint target. Scoped formatting and whitespace
checks passed. No Go, module, generated or Bazel source changed, so the diff
does not trigger change-based bazel_prepare. The previously unsatisfied fresh
Go worktree gate still requires bazel.

From /private/tmp/tidb-master-cc83514 at
0b505ecc58b659655345b7bb85a619db02f94300:

    GOTOOLCHAIN=go1.26.0 GOCACHE=/private/tmp/tidb-gocache go test github.com/tikv/client-go/v2/tikv -run '^TestKV$/^(TestAutocommitPointGetResolvesIgnoredLockHint|TestSnapshotReadsBackOffWhenServerReturnsHintedLock|TestBatchGetIgnoredCommittedLockExhaustsBackoff|TestResolveLocksWithOptsBacksOffOnlyForRead)$' -count=1 -v

All four selected original methods and their subcases passed, including the
package TestMain goleak gate (0.119s package runtime). Original TestKV calls
util.EnableFailpoints before creating clients. This pinned library uses runtime
EvalFailpoint, not source Inject rewriting; no persistent transformation was
made, and the runtime flag ends with the test process. The worktree is clean.
These cross-package originals are oracle evidence, not a claim that Rust owns
the entire tikv package test suite.

Logs use /private/tmp/tidb-snapshot-maxts- with suffixes red.log, green.log,
all.log, lock.log, coordinator.log, check.log, lint.log and go.log. All 17
source/test/support artifacts in the txnlock, txnsnapshot and retry inventories
were rechecked for exact SHA-256 and line counts, together with module go.mod,
go.sum and LICENSE. TiDB go.mod, go.sum, DEPS.bzl, pkg/store/copr and pkg/kv have
no diff between 64e8c4c05e and 0b505ecc58. Current inventory source pins were
advanced while preserving historical receipts.

### Worker audit and next whole-package boundaries


TransactionCommandClient::publish_transaction_batch_gets admits all Tonic
requests before completion, but complete_published_batch waits in input order.
snapshot_batch_get_at then handles region recovery, lock status and TTL waits
serially before another publication round. Go snapshot_async.go instead starts
independent retry workers as responses arrive, cancels outstanding work and
joins workers before its collector or statistics can escape. Concurrent initial
publication alone therefore cannot satisfy this contract. The Rust generic
client/timestamp interfaces also admit non-Send fixtures; preserve those native
ownership boundaries when selecting the worker design, and test cancellation
while a retry worker owns the collector before claiming this milestone.

Current master's new adaptive LIMIT controller belongs to the complete
pkg/executor/internal/exec package: BUILD.bazel, executor.go, indexusage.go,
indexusage_test.go, adaptive_limit_controller.go and
adaptive_limit_controller_test.go (six artifacts, 2,737 lines). Its executor,
join, session-variable and upgrade callers are additional required integration
work. No adaptive-controller implementation or package receipt is claimed here.

Self-review found no unrelated source edits. The remaining correctness risk is
concurrent real-store lock behavior beyond scripted response sequences; reverse
scans, options, replica routing, metrics and the original full test matrices
remain open. Real TiKV and matched sysbench/TPC-C/TPC-H/YCSB runs were not run;
no workload throughput claim follows from fewer status RPCs. Disk availability
is about 235 GiB after the earlier cleanup. Preserve the two preexisting user
files vs_helper.rs and fragment.rs. Commit the reviewed milestone as
`txnkv: match master MaxTS first-lock reads`, push hparser-integration and verify
remote equality. The overall goal remains active.

Revision note: refreshed the master boundary, added MaxTS red/green and original
Go validation, and recorded the distinct unresolved BatchGet worker lifecycle.

## BatchGet worker implementation milestone


MaxTS was published as 8acd92ec20 with remote equality verified. The next refresh
left branch and master unchanged. Complete the previously identified worker
lifecycle using SharedReadRuntime::fork_client, which already retains the same
transport, region cache and resolving-lock registry with independent client
borrows. Keep successful multi-batch reads on the caller's completion loop;
start a scoped worker only for a region/lock recovery response. Process initial
responses as they complete, retain one backoffer per physical batch, and join
all recovery workers before returning on cancellation or error. A split retry
must fork its existing budget to independently progressing children.

Decision: add Send/Clone bounds only to concurrent BatchGet entrypoints, leaving
single Get and Scan fixture capabilities unchanged. Production StoreWriteClient,
StoreWriteLoader and StorePdCapability already satisfy the needed bounds. Use
native completion futures and the existing cancellation/deadline-aware
rpc::wait_with_call bridge; do not poll or create a transport/timer service.
Share only result collection and resolved/committed hint publication under short
locks. Do not hold client or collector locks during another worker's RPC/wait.

First prove two physical locked batches can enter their status RPCs together;
the original round-based loop must fail the bounded synchronization regression.
Then verify completion order, cancellation while a worker owns the collector,
join-before-return, independent failure/success, split budgets and clean-batch
preservation. Full snapshot options/metrics/original-test and store gates remain
part of the open package acceptance boundary.

### Worker outcome, discoveries and validation


The old round-based implementation is replaced by the integrated
transaction/coordinator/snapshot_batch_get.rs owner. Tonic's
begin_transaction_batch_gets publishes every initial request before the caller
waits on the existing cancellation-aware completion bridge. FuturesUnordered
lets a later physical response start recovery while an earlier RPC remains
pending. Ordinary successful responses stay on the caller; only region/lock
recovery starts scoped native workers. One-batch reads use the synchronous
entrypoint without a future, client clone or worker. Each worker has its own
client borrow over the existing process transport/cache; short shared locks
protect result collection and hint sets. No client/collector lock spans another
worker's wait. A split retry forks its charged backoff into concurrent children.
RPC counts and cache publication are collected only after workers have joined.

Decision: retain resolving-lock records at the read/worker owner rather than
inside one resolver invocation. The cancellation test exposed a zero-record
interval during the retry RPC. Go retains its token until the Get/BatchGet
worker returns, so ResolvingLocksGuard now supports UpdateResolvingLocks and
the shared snapshot resolver updates a caller-owned guard before hinted-lock
backoff. Both point-read paths also retain that guard through their complete
Get. Non-snapshot lock callers preserve their existing one-attempt guard.
The source-importing standalone resolver test reexports the guard type so the
same production source remains exercised there.

Red proofs were behavioral, not compilation failures. The first concurrency
test failed when two physical lock responses could not enter status recovery
together. Temporarily replacing completion order with request order stalled
the later batch's retry until the call deadline. The record-lifetime assertion
then observed zero resolving records while a retry RPC was paused, where Go
requires one. These probes are recorded in /private/tmp/tidb-batch-workers-
red.log, order-red.log and registry-red.log. The temporary order probe restored
production source in a finally block before final validation.

The final snapshot suite covers independently progressing initial retries,
recovery before an earlier RPC completes, split-child concurrency, successful
sibling retention, cache exclusion after errors, and worker join/record release.
Cancellation, deadline and sibling-error cases each pause both a status RPC
and the retry BatchGet RPC. They assert that the call cannot return early,
that one resolving record remains visible during the pause, and that the record
has disappeared when the caller receives the result. The standalone admission
test also exercises the production Tonic future entrypoint's error identity.

From rust/, these exact commands passed:

    cargo test --offline --locked -j12 -p tidb-txnkv --test all snapshot_ --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test all region_error_recovery_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test lock_resolver_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --lib transaction::coordinator --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --lib transaction_admission_errors --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --lib rpc::batch::completion --message-format=short
    cargo check --offline --locked -j12 -p tidb-txnkv -p tidb-distsql -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server --message-format=short

The test commands passed 22, 26, 30, 24, one and two tests respectively; existing
compiler warnings remain. Logs use /private/tmp/tidb-batch-workers- with suffixes
snapshot-final.log, region.log, lock.log, coordinator.log, admission.log,
completion.log and check.log. The focused initial green proof is green.log.

From /private/tmp/tidb-master-cc83514 at master
0b505ecc58b659655345b7bb85a619db02f94300:

    GOTOOLCHAIN=go1.26.0 GOCACHE=/private/tmp/tidb-gocache go test github.com/tikv/client-go/v2/txnkv/txnsnapshot -run '^TestAsyncBatchGetCancellationWaitsForRetryWorker$' -count=1 -v

The original cancellation/join regression passed in 0.122s package runtime;
go.log records it. That package has no failpoint injection, so no transformation
was required. The reference worktree is unchanged.

From the repository root:

    make lint
    python3 /private/tmp/tidb-snapshot-format.py --check
    rustfmt --edition 2021 --check rust/crates/tidb-txnkv/src/transaction/coordinator/snapshot_batch_get.rs
    git diff --check

Lint passed with network permission after the sandboxed invocation could not
resolve the pinned revive tool's module source. Scoped formatting preserves
unrelated existing Rust formatting; the new module is checked in full. No Go,
module, generated or Bazel files changed, so change-based bazel_prepare is not
triggered. The fresh-worktree Bazel gate remains unsatisfied because bazel is
unavailable.

Whole-package acceptance remains open. In particular, the live coordinator's
EnableAsyncBatchGet configuration path, original async statistics/options,
replica/tier behavior, reverse scanning and full test reconciliation remain
required. The new worker execution matches the async path selected by TiDB's
default; it is not acceptance of every configurable snapshot path. Scoped
native workers preserve Rust borrowing and join guarantees, but their resource
cost under widespread simultaneous lock/region failures needs live workload
measurement. No real TiKV or matched sysbench/TPC-C/TPC-H/YCSB workload was run,
so no throughput/latency gain is claimed. The new master adaptive LIMIT package
boundary is still outstanding. Preserve the user-owned vs_helper.rs and
fragment.rs, refresh before publishing, and keep the goal active.

Revision note: replaced the serial BatchGet recovery loop, reconciled the
resolving-record lifetime exposed by the new lifecycle tests, and retained
explicit remaining complete-package validation boundaries.
