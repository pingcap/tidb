# `pkg/kv` parity audit ExecPlan

## Objective

Inventory the complete Go `pkg/kv` package before edits, keep its Rust owner
boundary explicit, and close concrete Rust-only behavior or missing Go
semantics only when the dependency-closed tests prove the change.

## Completed this batch

1. Read and inventoried all 30 Go production/test/Bazel artifacts (5,145
   lines), including the complete test harness and interface mocks. Confirmed
   there are no fixtures, generated files, benchmarks, or platform variants.
2. Read the `tidb-txnkv` KV contracts, transport owner, source-derived KV
   suite, batch wire/scheduler suites, and their build wiring. Kept the broad
   package claim at an explicit SEED/boundary because the SQL/session seam,
   TLS, several transaction options, and some MPP/storage implementations are
   not dependency-closed here.
3. Added `BatchCommandTag::ALL` in protobuf field-number order and concrete
   scheduler test types so the owner’s source suite compiles.
4. Fixed asynchronous and synchronous Coprocessor pending results to resolve
   publication receipts only for successful responses. Added a focused
   blocking-pull regression alongside the existing nonblocking elapsed-
   deadline regression; both preserve the original typed `Timeout`.
5. Recorded the full package parity receipt in `rust/testport/receipts/kv.md`.
6. Restored the missing Go `MaxCount`/`MinCount` pushdown cases and the two
   batch-task request flags, with focused Go regressions and fail-before/pass-
   after evidence.
7. Published the follow-up Go parity commit to `origin/hparser-integration`,
   fetched and fast-forward pulled, and verified the remote SHA.

## Validation gate

- [x] Focused batch source suite passes (76 tests), including the 11-test
  Coprocessor dispatch regression surface.
- [x] Full aggregated `tidb-txnkv` source suite passes with the documented
  stack setting (407 passed, 11 ignored).
- [x] Go `pkg/kv` focused unit suite passes.
- [x] `cargo fmt --all -- --check` passes.
- [x] Workspace Rust check passes offline and locked.
- [x] Ready profile `make lint` passes.
- [x] `make bazel_prepare` attempted for the Go/Bazel changes; blocked because
  the local `bazel` executable is unavailable.
- [x] Meaningful follow-up batch committed, pushed to
  `origin/hparser-integration`, and remote SHA verified after a fast-forward
  fetch.

## Remaining boundaries

The package remains an explicit SEED/boundary, not a complete transcreation
claim. The repository-wide package loop must continue with the next uncovered
package after this batch is committed and pushed. External etcd/live TiKV,
Bazel analysis, and omitted SQL/session integrations remain unverified.

## Continue request-limiter waiter lifecycle (2026-09-22)


### Purpose and source boundary


The complete `pkg/kv` and `pkg/store/copr` packages remain the atomic audit
units. Their inventories are `rust/testport/receipts/kv.md` and
`rust/docs/parity/copr-package-inventory.md`; this milestone is bounded seed
evidence within them. Master is freshly fetched at
64e8c4c05ecbe7dfe3eca211c4fb44f97bd75c59. Go's channel-backed limiter removes
canceled acquirers and lets another live worker use available capacity. Rust's
native async driver needs equivalent progress without blocking the runtime
threads that settle in-flight requests.

The SF50 profile identifies admission/wakeup contention as a major q10/q17
cost. Commit 1efd14bfee introduced FIFO wake-one and deregister_waker, but no
production caller deregisters. A canceled or already admitted driver can retain
a stale queue entry. Merely removing queued entries is insufficient: if release
already selected a driver which then cancels, it must pass available capacity
to another waiter. No throughput improvement is claimed before measurement.

### Progress


- [x] Inspect current master admission/cancellation contracts and existing Rust ownership.
- [x] Reproduce canceled-selected-waiter starvation with a deterministic regression.
- [x] Own dispatcher registrations through acquire, route/waker changes, close, error and Drop.
- [x] Preserve FIFO wake-one; transfer a free-slot notification when a selected waiter leaves.
- [x] Refresh complete inventories; run scoped source/lifecycle suites, dependent compilation and required lint.
- [x] Publish the reviewed limiter checkpoint (`1f9c5d4eb9`); remote divergence is `0 0`.

### Plan of work and decisions


Extend the existing `tidb-txnkv/src/kv_contract.rs` limiter tests first. Hold one
token, register two drivers, release to the first and cancel it before acquire;
the second must wake and acquire without another release. Also retain the
existing dedup/one-wake test and cover cancellation while all slots are held.
The regression must fail before the fix. Keep the public admission/capacity
contract and explicit redundant-release guard unchanged.

Use a small Rust Drop-owned registration in
`tidb-distsql/src/cop_paging/direct_unary_query_transport.rs`, separate from the
existing acquired-token permit. Remove registrations after successful acquire
and all response termination/replacement paths. Drop releases only the waiter,
never a token it did not acquire. In deregister_waker, remove the departing
driver under the queue lock and select one successor when a slot is free; wake
outside the lock. This retains bounded wake traffic and avoids deadlocks from
waker callbacks. Region retry still releases the acquired token before choosing
a new store. Do not expand this into a scheduler or storage protocol change.

### Validation and recovery


From `rust/`, run the affected `tidb-txnkv` library limiter tests, its aggregated
`kv_package_source` suite, `tidb-distsql` direct-unary library and integration
suites, and dependent compilation, using offline locked Cargo with 12 jobs.
Run original master KV limiter tests through the repository's failpoint-aware
wrapper if the package requires transformation. Run root `make lint`, scoped
Rust formatting and `git diff --check`. Existing live-cluster, full package,
Bazel and four-workload gates remain open; the temporary master worktree's
missing-bazel gate is not erased by direct Go runtime tests. Keep user-owned
untracked files untouched and publish only reviewed task changes.

### Surprises and discoveries


The prior commit message says deregistration occurs on acquire, but `rg`
finds no call outside its definition. Its performance numbers cannot establish
the cancellation lifecycle. The comparison inventory also predates a master
KV materialized-view transaction-source constant and coprocessor lock-hint /
serial-batch-timeout changes; refresh and keep these separate open boundaries
explicit instead of treating the old inventory pin as current parity.

### Outcomes and retrospective


The deterministic selected-waiter cancellation regression failed with zero
wakeups for the next live driver, where one was required. Its final version
covers cancellation before and after release, FIFO ordering and exactly one
successor wake. The real asynchronous ordered/unordered reader-close regression
also failed with zero successor wakeups when only the dispatcher was restored
to 14620bca2f, while retaining the corrected limiter. The fixed dispatcher was
restored after that isolated mutation. Both tests pass with the complete fix.
Logs are `/private/tmp/tidb-limiter-waiter-red.log` and
`/private/tmp/tidb-limiter-waiter-close-red.log`; the initial test-import compile
error was corrected before collecting the behavioral red evidence.

RequestAttemptWaiter owns the queue entry until successful admission, route or
waker replacement, Close, terminal pull result or Drop. A permit still owns the
actual RPC token separately. Queue callbacks run outside the mutex, and a
departing selected driver passes a notification only when capacity is free.
The ownership unit test also checks deduplication, wake replacement, successful
acquire before a queued wake and store replacement. The runtime close test
starts an actual asynchronous worker, verifies no RPC is admitted while the
limiter is full, closes it and checks the next registered driver's wake.

The original Go TestCoprRequestLimiterConcurrentAcquireRelease had no Rust
source counterpart. Its added Rust translation runs 32 workers on four runtime
threads, each acquiring/releasing 20 times, with capacity three. All 640
acquisitions complete, no worker exceeds capacity, and all three tokens can be
reacquired afterward. The other four original Go limiter cases already have
Rust equivalents. This closes that test omission without claiming the complete
KV package is finished.

Fresh source inventories record 30 KV artifacts / 5,437 lines and the complete
25-artifact copr subtree / 12,604 lines, with Git blobs and SHA-256 hashes. The
KV constant added since the earlier pin is already implemented in Rust. Copr's
new per-RPC lock-hint and serial store-batch-timeout contracts still require
their own complete-package audit evidence. The current master pin remains
64e8c4c05ecbe7dfe3eca211c4fb44f97bd75c59 after the final refresh.

### Exact validation receipt


From `rust/`, these passed, with test counts 3, 38, 2, 2 and 66 respectively:

    cargo test --offline --locked -j12 -p tidb-txnkv --lib kv_contract::tests --message-format=short
    cargo test --offline --locked -j12 -p tidb-txnkv --test all kv_package_source --message-format=short
    cargo test --offline --locked -j12 -p tidb-distsql --lib direct_unary_query_transport::tests --message-format=short
    cargo test --offline --locked -j12 -p tidb-distsql --lib cop_iterator --message-format=short
    cargo test --offline --locked -j12 -p tidb-distsql --test all direct_unary --message-format=short
    cargo check --offline --locked -j12 -p tidb-txnkv -p tidb-distsql -p tidb-exec -p tidb-executor --message-format=short

From the repository root:

    make lint
    python3 /private/tmp/tidb-limiter-waiter-format.py --check
    git diff --check

Logs have prefix `/private/tmp/tidb-limiter-waiter-` and suffixes `kv-lib.log`,
`kv-source.log`, `owner.log`, `driver.log`, `transport.log`, `compile.log` and
`lint.log`. The temporary formatter scopes rustfmt to modified functions/lines;
the existing unrelated formatting and user-owned untracked files are preserved.
The final added source concurrency test was included in the 38-test KV rerun.
Existing compiler warnings remain.

From `/private/tmp/tidb-master-cc83514` at the current master pin:

    GOTOOLCHAIN=go1.26.0 GOCACHE=/private/tmp/tidb-gocache ./tools/check/failpoint-go-test.sh pkg/kv -run '^(TestCoprRequestLimiter.*|TestQueryCopStoreLimiter)$' -count=1 -v

All five original tests passed (0.023s package runtime), including cancellation
and concurrent acquire/release. pkg/kv/txn.go imports and injects failpoints, so
the wrapper performed real enable/disable transformations; cleanup returned the
refcount to zero and the master worktree was clean. Output is `go.log` under the
same prefix. The earlier missing-bazel fresh-workspace gate remains unsatisfied.
No Go/Bazel/module changes were made in the working branch, so this Rust-only
diff does not trigger its change-based bazel_prepare gate.

### Remaining risks and acceptance limits


The fix removes stale wake traffic and a free-capacity stall while retaining the
existing one-driver wake policy and token limit. Concurrent scheduling remains
the main correctness risk; the targeted runtime and source tests cover the
changed ownership boundaries but are not a model check of all interleavings.
No new protocol or SQL feature is introduced. Full original package/build/
platform/generated gates, real TiKV, async limiter-wait metric parity and matched
sysbench/TPC-C/TPC-H/YCSB measurements remain open. No throughput improvement is
claimed from the wake-count regressions. Both package claims remain open.

Publication: `1f9c5d4eb9368b4f57a1761bd9d810a56de723fe`
(`distsql: retire coprocessor limiter waiters safely`) is pushed to
origin/hparser-integration. The branch was pulled before publication and was
already up to date; afterward HEAD and origin/hparser-integration matched, with
`git rev-list --left-right --count HEAD...origin/hparser-integration` returning
`0 0`. Only the user's preexisting untracked `tidb-expr/src/vs_helper.rs` and
`tidb-planner/src/fragment.rs` remained. Approximately 242 GiB is available after
the earlier disk cleanup and subsequent validation builds. This docs-only
publication receipt does not change the validated code or close package gates.
