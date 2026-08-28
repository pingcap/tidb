# Go/Rust Sysbench parity review

Date: 2026-08-28
Rust target: `hparser-integration` at `2027377a4ace76d06a6cc81ac82c51b68505a3b6`
Go semantic baseline: `origin/master` at `25050b53f84fd14c4cfa97a7bb3826876c333c29`

This receipt is the implementation and review boundary for the next parity
cycle. It separates measured facts from hypotheses, keeps the Go unit tests as
the source of truth, and does not treat a plan-cache hit counter as proof that
an executable physical plan is being reused.

## Executive conclusion

The latest hparser branch has already closed the easy, concentrated gaps:

* TiKV `Get` and coprocessor work are at parity in the existing measurements;
  both sides issue one cop request for the range shape and process the same
  MVCC bytes.
* Rust is ahead on autocommit `oltp_point_select` (1.103x) and
  `oltp_write_only` (1.041x).
* The remaining deficit is transactional read work: Rust is 0.872x on
  `oltp_read_write` and 0.775x on `oltp_read_only` in the alternating
  2026-08-26 receipt. In the read-only mix, the measured +640us transaction
  gap is attributed to point reads (+156us), simple range (+66us), sum
  (+76us), order (+105us), and distinct (+173us).
* The largest isolated boundary is not a single algorithm. The explicit
  transaction read request/reply hop costs 5.3us per read; the coproducer
  handoff is about 11us per statement; and the transport worker is about
  10us per RPC. Together these are only about 17% of the read-only deficit.

Therefore the next code change must be an instrumented, reversible campaign,
not a speculative rewrite of the transaction coordinator. The first code
milestone should remove the explicit-transaction read handoff while proving
snapshot, lock, retry, and commit semantics. The full physical-plan cache is
the second major track and must be landed as a complete Go-package claim; it
cannot be replaced by the existing Rust key/admission cache.

## Source-of-truth map

The plan-cache source files are identical between hparser and `origin/master`
at the refs above. The authoritative Go tests and the Rust surfaces they gate
are:

| Go package / tests | Rust surface | Required parity evidence |
| --- | --- | --- |
| `pkg/planner/core/plan_cache*.go`, `pkg/planner/core/casetest/plancache/**`, `pkg/planner/core/plan_cache_lru_test.go` | `tidb-session::{prepared_plan_cache,non_prepared_plan_cache,prepared_path_pins}` plus the physical-plan owner in `tidb-exec` | cache key, admission, schema/blacklist invalidation, range rebuild, LRU/memory behavior, and all corresponding Go cases |
| `pkg/executor/test/plancache/**`, `pkg/executor/prepared_test.go` | `tidb-exec` statement planning and executor construction | rows and output metadata remain correct when parameter values change; no stale executor state is reused |
| `pkg/session/test/txn/**`, `pkg/session/txn.go` | `tidb-exec::cluster_table_storage`, `tidb-server::cluster_session_node`, `tidb-txnkv::transaction::coordinator` | one snapshot timestamp per explicit transaction, lock/rollback/commit ordering, retry/error classification |
| `pkg/executor` read/write tests and sysbench statement shapes | `tidb-exec` protocol/drain path and `tidb-txnkv` client/transport | per-statement phase timers and alternating Go/Rust measurements; no storage-work regression |

The complete transcreation unit is the Go package, including its production
files, generated/build variants, original tests, fixtures, and support code.
If the Rust ownership boundary crosses crates, the commit must name the whole
set (`tidb-session` + `tidb-exec`, or `tidb-exec` + `tidb-txnkv` +
`tidb-server`) rather than claiming a partial function port.

## What is already landed

The branch history contains the following relevant, independently reviewable
changes:

* `c8f67f1a03`: coprocessor scans use a producer pool instead of a fresh
  thread.
* `7b4e2c91aa`: RPC handoff and receipt cross the boundary once.
* `372c955e7a`: transaction answers use crossbeam instead of a fresh standard
  channel per statement RPC.
* `7c267b40ee`: autocommit statement snapshots open inline on the connection
  worker. The living plan was corrected to mark M3s DONE.
* `0f7953702f`, `4ee77f0109`, and `da8da8a0c4`: aggregation-family pinning,
  single-pass delivery, and global aggregate costing alignment.

These changes explain why point-select and write-only sysbench are already at
or above Go. They do not implement Go's general physical-plan cache.

## Prioritized implementation plan

### P0 — Reproduce before changing code

Use a fresh TiUP playground and the same dataset for both servers. Alternate
the engines inside every sample, run at least three samples per shape, and
record p50/p95 plus phase timers. Keep the following counters in the receipt:

* PD timestamp waits, explicit-transaction read handoffs, cop RPC count,
  transport-worker waits, and executor drain/open/materialize time;
* TiKV request type/latency, MVCC bytes, and wire bytes;
* sysbench error count and transaction retry/rollback count.

Do not compare a Rust-only ladder followed by a Go-only ladder: the existing
plan documents show that ordering can reverse the conclusion.

### P1 — Inline explicit-transaction reads (highest measured leverage)

The current `SessionSnapshot` sends every `Get`, `BatchGet`, and `Scan` through
`TransactionRequest` and waits on a zero-capacity reply. Replace that boundary
with a shared transaction state only after the following design is proven:

1. Keep the transaction object in an `Arc<Mutex<...>>` (or an equivalent
   connection-owned state machine) so `ClusterSnapshot: Send` remains true.
2. Dispatch optimistic and pessimistic transactions explicitly; do not erase
   their lock/commit protocols behind an unchecked enum shortcut.
3. Preserve one `read_ts` for every statement in an explicit transaction,
   lock-cache behavior for locking reads, region/lock retry classification, and
   commit/rollback ownership.
4. Add Rust regressions corresponding to the Go transaction tests: repeated
   reads at one timestamp, concurrent `SELECT ... FOR UPDATE` blocking,
   rollback after a failed read, retry after a region/lock error, and commit
   after a mixed read/write transaction.
5. Benchmark before/after with the P0 receipt. The expected gain is bounded by
   the measured 5.3us/read handoff; a larger gain requires a new profile, not a
   claim based on this estimate.

This phase is intentionally separate from `StatementSnapshot`: M3s already
removed the autocommit handshake, while the explicit transaction is shared by
multiple statements and has materially stronger lifecycle constraints.

### P2 — Remove only the transport boundary that profiling justifies

The prior attempt to share a runtime and call the transport directly hit
Tokio's `Cannot start a runtime from within a runtime` panic and was reverted.
Any retry must first provide one long-lived runtime ownership model, then test
channel-pool serialization, cancellation, deadlines, and shutdown. The target
is the measured ~10us/RPC transport-worker term; do not trade it for nested
runtime panics or connection leaks.

### P3 — Implement Go's physical-plan cache as one package claim

The current Rust cache has Go-compatible admission/key/invalidation contracts
and a narrow prepared point-get/access-path pin. It does not retain a physical
plan. The implementation order should be:

1. single-table parameterized range plan (`CachedSelectPlan`),
2. safe range rebuild equivalent to `RebuildPlan4CachedPlan`/`isSafeRange`,
3. prepared-plan LRU with schema, SQL mode, timezone, blacklist, parameter
   types, memory accounting, and correct `@@last_plan_from_cache`,
4. index/point/batch access, then joins and aggregation,
5. non-prepared cache only after prepared correctness is green.

The first milestone must build the same query twice with different parameters
and compare rows, output columns, and ordinary-path plans. A hit flag alone is
not acceptance. The Go corpus and Rust differential corpus must both be green.

### P4 — Share aggregate children only if P0/P1 still attribute the cost

`DISTINCT` still performs speculative StreamAgg/HashAgg planning over the same
children. Sharing child access-path enumeration is a valid Go-aligned
optimization, but the existing measurement prices it as a shape-specific
planning cost, not as the dominant read-only gap. Implement it only with a
regression showing a meaningful reduction in the `distinct_range` phase.

### P5 — Row/protocol work is last

The row materialization phase is about 3.2us for 50 rows (0.064us/row) in the
current receipt, and wire/MVCC work matches Go. Do not rewrite `SelectMeta` or
streaming ownership until a new profile moves this term above the transaction
and transport boundaries.

## Acceptance gates for each pushed package batch

* The entire corresponding Go package test/support/fixture inventory is named
  in the commit and remains the semantic oracle.
* Rust focused tests pass, including new regressions; the differential corpus
  has no new divergence.
* A fresh alternating sysbench receipt reports errors, retries, p50/p95, and
  per-phase timings. No result is accepted from a stale or one-sided ladder.
* `oltp_point_select` and `oltp_write_only` must not regress from their current
  Go-relative wins. The read-heavy workload must improve on the targeted phase;
  overall parity (`rust/go >= 1.0`) is the final goal, not an assumption about
  an intermediate patch.
* Each batch must be independently revertible. If a batch crosses Rust crate
  boundaries, its commit message must list all crates and the complete Go
  package claim.

## Validation performed for this receipt

The latest remote ref was fetched before review. On a clean worktree at that
ref:

* `cargo test --offline --locked -j8 -p tidb-datatype --lib`: 365 passed.
* `cargo test --offline --locked -j8 -p tidb-codec --lib`: 44 passed.
* `cargo test --offline --locked -j8 -p tidb-lexer --lib`: 85 passed, 286
  ignored (the ignored cases are explicitly marked Go-parity gaps in the
  source tests).
* `cargo check --offline --locked -j8 -p tidb-server` and the analogous
  `tidb-executor` test command could not reach compilation because this macOS
  environment has no OpenSSL headers/pkg-config (`openssl-sys`); this is an
  environment blocker, not a Rust test failure.

No fresh live sysbench run was claimed in this receipt. The workload numbers
above are the existing alternating 2026-08-26 measurements recorded in
`rust/docs/plan-cache-parity-execplan.md`; P0 must regenerate them before a
performance patch is declared effective.
