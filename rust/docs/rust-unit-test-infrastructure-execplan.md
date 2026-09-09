# Make the Rust unit-test port executable and measurable

This ExecPlan is a living document. It follows `PLANS.md` at the repository
root and must be updated whenever a milestone changes, a blocker is removed,
or a coverage number is re-baselined. It is intentionally separate from
`rust/testport/TESTPORT_EXECPLAN.md`: that document describes how Go tests are
ported and landed, while this document describes how the Rust runtime and
test infrastructure make the ported tests executable.

## Purpose / Big Picture

After this plan is implemented, an agent can take a Go test anchor from
`hparser-integration`, run the corresponding Rust test in the owning crate,
and obtain a deterministic behavior result with a traceable receipt. The
branch will report both test-port coverage (which Go behaviors are executable)
and Rust line/branch coverage (which Rust code is exercised). An ignored test
will represent a deliberate product or environment boundary, not an
unclassified missing harness.

The observable outcome is a set of fast, package-scoped Rust tests that cover
parser, session, planner, executor, DDL, transaction, and statistics behavior,
plus an explicitly separated RealTiKV/nightly tier. No test is made green by
deleting assertions or blindly removing `#[ignore]`.

Baseline for this plan: the remote `hparser-integration` tip
`81533646bfd7e6a7f9b56494900fc53eb7340825`, whose latest port is
`pkg/session.part4` ([commit](https://github.com/pingcap/tidb/commit/81533646bfd7e6a7f9b56494900fc53eb7340825)).
If the branch advances before implementation starts, rerun the inventory in
Milestone 0 and record the new commit instead of silently mixing baselines.

## Progress

- [x] (2026-08-29) Baseline the branch tip, manifest, source-carrier tests,
      ignored reasons, and representative receipts.
- [x] (2026-08-29) Identify the enabling infrastructure in dependency order:
      parser, session/catalog, KV/transaction, DDL/schema, planner/executor,
      statistics, and cluster-only infrastructure.
- [ ] M0 — normalize the anchor inventory and publish the coverage report.
- [ ] M1 — make parser and AST tests executable through a parser test kit.
- [ ] M2 — make session and Domain tests executable through a session harness.
- [ ] M3 — make storage, transaction, and streaming tests deterministic.
- [ ] M4 — make DDL, schema lease, MDL, and reorg tests executable.
- [ ] M5 — make planner/executor integration and plan-cache tests executable.
- [ ] M6 — close statistics, authentication, resource-group, and observability
      gaps.
- [ ] M7 — move RealTiKV and other heavy tests to a separately verified tier.
- [ ] At every stopping point, update this section, `Surprises & Discoveries`,
      `Decision Log`, and the affected receipt files.

## Surprises & Discoveries

- Observation: the manifest contains 7,025 Go test functions in 151 batches,
  while a raw scan of Rust source-carrier files at the baseline finds 7,693
  `#[test]` declarations, 3,471 `#[ignore]` declarations, and 4,222
  declarations without `#[ignore]`.
  Evidence: `rust/testport/MANIFEST.json` and the `rust/crates/**/tests_*source.rs`
  files at commit `81533646bfd7e6a7f9b56494900fc53eb7340825`.
  Consequence: these raw counts are not a coverage denominator; source
  carriers include split/duplicate entries, benchmarks, `TestMain`, and
  documentary or partial ports. Milestone 0 must normalize unique Go anchors.

- Observation: parser part1 has 60 ignored entries because the tests need
  `tidb-ast`/`tidb-parser`, while making `tidb-lexer` depend back on those
  crates would create a dependency cycle.
  Evidence: `rust/testport/receipts/b051.md`.
  Consequence: parser ownership and dependency layering must be fixed before
  parser ignores are burned down.

- Observation: planner part8 has 56 ignored entries, two `TestMain` entries,
  and no running tests; its reasons mention plan cache, session/executor,
  statistics, and MPP/TiFlash.
  Evidence: `rust/testport/receipts/b085.md`.
  Consequence: planner coverage requires an execution harness, not more
  isolated plan-construction stubs.

- Observation: all 39 tests in the session part4 source carrier are ignored;
  the corresponding b148 receipt currently identifies only
  `PrepareZero`, `PrimaryKeyAutoIncrement`, and `ParseWithParams` as runnable.
  Evidence: `rust/crates/tidb-session/src/tests_session_part4_source.rs` and
  `rust/testport/receipts/b148.md`.
  Consequence: session bootstrap, schema, process-info, task-ID, and DDL seams
  are high-leverage blockers.

- Observation: ignored reasons overlap. For example, a single test may need
  both a session and an executor, and reason text mentioning `session`,
  `executor`, `stats`, or `plan cache` cannot be summed into independent
  queues.
  Consequence: each anchor needs one primary blocker and optional secondary
  dependencies in the normalized inventory.

## Decision Log

- Decision: measure coverage by unique Go anchor `(go_file, test_name)`, not by
  the number of Rust source-carrier declarations.
  Rationale: one Go test can be split into several Rust modules, and source
  carriers intentionally retain duplicates and non-tests. Date/Author:
  2026-08-29 / Codex.

- Decision: retain a documentary source carrier until its replacement test has
  a receipt; do not delete it merely to reduce the ignored count.
  Rationale: the carrier is the audit trail from Rust back to the Go contract.
  Date/Author: 2026-08-29 / Codex.

- Decision: use dependency-injected fakes for fast tests and keep RealTiKV,
  TiFlash, cross-node, and long-running concurrency tests in a separate tier.
  Rationale: deterministic unit tests should not depend on a playground, wall
  clock, network timing, or a live cluster. Date/Author: 2026-08-29 / Codex.

- Decision: parser ownership is split by layer: lexer tests stay in
  `tidb-lexer`, AST tests in `tidb-ast`, grammar tests in `tidb-parser`, and
  parse/restore integration tests are owned by `tidb-parser` (or a dedicated
  integration crate that depends on both). Rationale: this removes the
  lexer/parser cycle instead of hiding it behind ignored tests. Date/Author:
  2026-08-29 / Codex.

- Decision: an agent may claim a complete Go package only when all production
  sources, generated/build variants, original tests/support files, fixtures,
  and validation gates are inventoried. Rationale: this follows the
  repository's Go-to-Rust transcreation rule; a partial file port remains
  explicit seed evidence, not a completed package claim. Date/Author:
  2026-08-29 / Codex.

## Context and Orientation

`rust/testport/MANIFEST.json` is the batch inventory. Receipts under
`rust/testport/receipts/` record what was ported, what runs, and why a test is
ignored. A **source carrier** is a Rust file that preserves the source-level
test mapping, often with an ignored body while the owning runtime is missing.
An **anchor** is one unique `(Go file, Go test name)` pair. A **harness** is a
test-only object that constructs the runtime dependencies needed by a test. A
**fixture** is checked-in input or expected output, usually produced by Go or a
deterministic fake. **RealTiKV** means a test requiring an actual TiUP/TiKV
cluster; it is not a normal in-process unit test.

The largest raw gaps at the baseline are:

| Crate | Test declarations | Ignored | Approximate ignored ratio | Primary blockers | Priority |
| --- | ---: | ---: | ---: | --- | --- |
| `tidb-session` | 280 | 239 | 85% | bootstrap, Domain, InfoSchema, DDL, process info | P0 |
| `tidb-planner` | 1,397 | 1,131 | 81% | plan cache, session/executor, stats, MPP | P0 |
| `tidb-lexer` | 360 | 286 | 79% | parser/AST integration and grammar fixtures | P0 |
| `tidb-executor` | 1,865 | 1,454 | 78% | DDL, MDL, reorg, transactions, chunks, FK | P0 |
| `tidb-stats` | 671 | 197 | 29% | StatsHandle, Analyze, stats persistence | P1 |
| `tidb-expr` | 417 | 136 | 33% | evaluator, session context, vectorized/PB paths | P1 |
| `tidb-txnkv` | 458 | 12 | 3% | cluster boundaries and deterministic failures | P2 |
| `tidb-ast` | 138 | 11 | 8% | parser integration and restore cases | P1 |
| `tidb-distsql` | 204 | 2 | 1% | region/streaming integration | P2 |

The following batches are useful starting queues:

| Batch / area | Representative missing behavior | Required foundation |
| --- | --- | --- |
| b051–b059 parser and AST | parse, restore, DDL/DML grammar, privilege syntax, charset and error positions | `ParserTestKit`, fixtures, corrected crate ownership |
| b117–b118 Domain | bootstrap, external workload TTL, sysvar cache, replica read, InfoSync/PD writer | `SessionHarness`, fake clock, Domain services |
| b144–b149 session | schema checker, metadata tables, non-transactional DML, auth, resource groups, chunk results, process info, task IDs | bootstrap catalog, InfoSchema, privilege/process/metrics providers |
| b078–b099 planner | plan cache, mock stats, partition pruning, physical plan and MPP | planner/executor runtime adapter and statistics injection |
| b100–b116 DDL | job lifecycle, partition DDL, MDL, reorg/backfill, cancellation | deterministic DDL runner, schema lease, MDL, fake PD/GC |
| b119–b143 executor | FK, index merge, infoschema, locks, async workers, result chunks | multi-session KV fixture, lock manager, retrievers, worker control |

## Plan of Work

### Milestone 0 — Normalize the inventory and establish gates

Add a small, reproducible inventory tool under `rust/testport/tools/` and a
generated report under `rust/testport/coverage/`. The tool must read
`MANIFEST.json`, scan Rust source carriers, and emit one row per unique Go
anchor. It must classify `runnable`, `ignored`, `benchmark`, `harness`,
`duplicate`, `partial`, and `realtikv` instead of treating all `#[ignore]`
attributes as product gaps.

Each ignored anchor must have a primary blocker, optional secondary blockers,
an owner, and an expiry/review date. Existing receipt files remain the human
review surface; the machine-readable report prevents counts from drifting.

Acceptance:

- The report reconciles to the manifest without double-counting split carriers.
- A new ignored test without a structured reason fails the inventory check.
- The report shows both `unique_anchor_total` and
  `unique_non_harness_anchor_total`.
- CI can compare the report with the previous commit and reject an unexplained
  increase in ignored anchors.

### Milestone 1 — Parser and AST test kit

Implement the parser test kit in the owning parser/AST crates rather than in
`tidb-lexer`. The exact module names may follow local crate conventions, but
the stable contract must be:

    parse(sql, options) -> AST or parse error
    restore(ast) -> canonical SQL
    parse_restore(sql) -> canonical SQL
    parse_error(sql) -> error class, position, and message

The kit must load SQL fixtures, compare golden restore output, select SQL mode,
charset, and collation, and test both success and error paths. Keep lexical
tests in `tidb-lexer`; move grammar and AST integration carriers to
`tidb-parser`/`tidb-ast` with a receipt preserving the original Go anchors.

Do not make `tidb-lexer` depend on `tidb-parser` or `tidb-ast` in a way that
creates a cycle. If a shared helper is needed, put only neutral token/fixture
utilities in a lower-level crate.

Acceptance:

- b051–b059 have a parser-layer owner for every non-harness anchor.
- Representative DDL, DML, privilege, charset, SQL-mode, and error-position
  cases execute with assertions.
- Parse/restore golden output is checked against Go-produced fixtures where
  formatting is part of the contract.
- Lexer-only tests remain runnable without pulling in the parser.

### Milestone 2 — Session and Domain harness

Add a reusable `SessionHarness` in `tidb-session` test support. It should
construct a session, statement context, session variables, an in-memory
catalog, InfoSchema, transaction client, fake clock, failpoint registry,
process-info registry, privilege provider, resource-group provider, and
cleanup guards. It must support multiple sessions sharing one catalog and KV
fixture.

The harness should expose operations equivalent to the Go testkit lifecycle:

    open_session()
    execute(sql)
    begin(), commit(), rollback()
    result_rows() / result_error()
    close_session()

Bootstrap tests need a deterministic catalog containing the system tables used
by TiDB. Schema reloads must be explicit and observable through a version or
lease object; tests must not sleep for a background reload. Process info and
statement/task IDs must be allocated by an injected registry so concurrent
tests do not share global state.

Acceptance:

- b144–b149 and b117–b118 can construct the same fixture without duplicating
  bootstrap code.
- Metadata, schema-change, non-transactional DML, auth, resource-group,
  process-info, and task-ID tests have real assertions.
- Session cleanup leaves no transaction, worker, failpoint, process entry, or
  metrics registration behind.
- A test can run repeatedly and in parallel without depending on wall-clock
  timing or test order.

### Milestone 3 — Deterministic KV, transaction, and streaming fixtures

Add a test-only KV fixture, preferably alongside existing `tidb-txnkv` and
`tidb-unistore` mocks. It must model the behavior that unit tests assert rather
than emulate a production cluster: MVCC visibility, locks, 2PC, async commit,
pessimistic locking, rollback, retry, region errors, and deterministic RPC
failure injection.

The same fixture must support two or more sessions so tests can exercise lock
waits, write conflicts, foreign keys, stale reads, and transaction retries.
Expose explicit barriers or a fake scheduler for concurrency. For DistSQL and
executor tests, provide deterministic region splits and a `ResultSet`/chunk
source whose `next_batch` boundaries can be controlled.

Acceptance:

- Transaction tests assert commit, rollback, conflict, lock timeout, and
  retry behavior without a live TiKV process.
- Chunk and streaming tests assert row order, batch boundaries, and errors at
  the same points as the Go test.
- Failure injection is deterministic and can be reset in `Drop`/cleanup.
- Existing low-gap `tidb-txnkv` and `tidb-distsql` tests remain green.

### Milestone 4 — DDL, schema lease, MDL, and reorg runtime

Add a deterministic DDL job runner in `tidb-executor`/`tidb-exec` test support.
It must model enqueue/dequeue, owner and scheduler decisions, cancellation,
rollback, schema-version publication, schema lease, metadata locks, table
cache reload, and reorg/backfill progress. Replace wall-clock waits with a
fake clock and explicit barriers. Add failpoints at the same state transitions
that the Go tests exercise.

System-table reads used by bootstrap, DDL, and statistics tests must come from
the same catalog/KV fixture as the session harness. A test that needs PD or GC
should receive a deterministic fake service; it must not silently skip the
behavior.

Acceptance:

- b100–b116 DDL anchors can run through a controlled job lifecycle.
- MDL wait, timeout, cancellation, schema reload, partition DDL, reorg, and
  backfill tests assert both state and error semantics.
- A failed DDL job leaves the catalog, locks, and job queue in a testable
  terminal state.
- The receipts identify any remaining RealTiKV-only cases instead of marking
  them as ordinary unit-test gaps.

### Milestone 5 — Planner/executor integration and plan cache

Build a planner/executor integration adapter on top of `SessionHarness`. It
must parse a statement once where possible, build a plan with injected
statistics, execute it against the deterministic KV/catalog fixture, and
return rows, warnings, or errors. Add prepared and non-prepared plan-cache
operations with explicit hit, miss, invalidation, and schema-change cases.

The adapter must support the runtime dependencies used by the Go tests:
parameter binding, partition pruning, index merge, joins, aggregation,
window functions, chunk reads, and statement cancellation. MPP/TiFlash cases
should use a documented mock until a RealTiFlash environment is available;
the mock must assert the routing and fallback contract rather than return an
empty success.

Acceptance:

- b078–b099 and the executor plan-cache/index-merge/window groups have
  executable anchors with behavior assertions.
- A plan-cache test proves both reuse and invalidation after schema or session
  changes.
- Planner estimates can be supplied by a stats fixture and compared with the
  Go oracle.
- Unsupported MPP behavior is classified as a real environment boundary, not
  hidden behind an unqualified `#[ignore]`.

### Milestone 6 — Statistics, security, resource groups, and observability

Add a `StatsHandle` test fixture that can load and persist histogram, TopN,
CMSketch, feedback, and analyze state through deterministic mysql.stats_* rows.
Provide fake analyze jobs and schema-version invalidation. The same fixture
must be consumable by planner tests so estimate differences are visible.

Add injected providers for privilege/authentication, resource-group tagging,
metrics, process info, statement/task IDs, and warning collection. Avoid
process-wide mutable singletons; each harness owns its providers and exposes
them for assertions.

Acceptance:

- Remaining stats-handle, analyze, infoschema-retriever, auth,
  resource-group, process-info, and metrics anchors have a named owner and
  either run or carry a narrowly justified boundary.
- Statistics persisted by a test can be reloaded by a new session and produce
  the expected plan decision.
- Auth failures, resource-group hints, task IDs, process info, and metrics are
  asserted through public behavior, not internal implementation details only.

### Milestone 7 — Heavy and RealTiKV tier

Keep tests that require TiUP, TiKV, TiFlash, cross-node scheduling, long
concurrency windows, or production failpoints in an explicit integration or
nightly tier. Use the repository's RealTiKV lifecycle instructions: start the
playground in the background, wait for readiness, run the scoped tests, and
always clean up the playground and data.

The nightly tier must still use the same anchor inventory and receipts. A
test's environment requirement is metadata, not an excuse to lose its Go
oracle or assertions.

Acceptance:

- Every remaining ignored anchor is classified as unsupported, heavy,
  benchmark, harness, duplicate, partial, or a current product gap.
- Heavy tests have a reproducible command and cleanup procedure.
- PR CI remains bounded while nightly coverage continues to grow.

## Concrete Steps

All commands below run from the repository root unless stated otherwise.

1. Record the baseline and protect unrelated worktree changes.

       git rev-parse HEAD
       git ls-remote origin refs/heads/hparser-integration
       git status --short --branch

   The implementation agent must not reset or clean a dirty worktree. Work in
   a dedicated branch/worktree and cherry-pick only the intended commits.

2. Reconcile the manifest and source carriers.

       jq '.branch, (.scope_rows | length)' rust/testport/MANIFEST.json
       rg -n '#\[(ignore|test)' rust/crates rust/testport

   The inventory tool added in M0 should replace ad-hoc counting and emit a
   deterministic report suitable for CI review.

3. For each milestone, select one complete Go package or explicitly bounded
   package slice from `MANIFEST.json`. Read the package's Go production files,
   tests, support artifacts, fixtures, and existing receipt before changing
   Rust. A partial source-carrier conversion may be useful seed evidence, but
   it is not a completed transcreation claim.

4. Run the smallest scoped Rust gate after each change.

       cd rust
       PROTOC=/opt/protoc/bin/protoc \
       cargo nextest run --locked -p <crate> -E 'test(<module>)' --no-fail-fast
       cargo fmt --check

   If a crate uses failpoints, enable them before the test and disable them in
   cleanup. Do not run a broad workspace suite until the scoped gate is stable.

5. Record a receipt for every landed batch. The receipt must state the source
   anchors, runnable/ignored/other classifications, exact command, baseline
   failure set, result, and remaining blocker. Update the generated inventory
   in the same change.

6. At a milestone boundary, run coverage for the affected crate.

       cd rust
       cargo llvm-cov nextest --locked -p <crate> --all-features

   Store the summary in the milestone receipt; do not compare numbers from
   different feature sets or different anchor denominators.

7. Before claiming readiness for review, use the repository Ready verification
   profile. If Go tests, Go imports, Bazel targets, or Go dependencies changed,
   run `make bazel_prepare` and include generated metadata changes. This plan
   itself does not require `make bazel_prepare` because it changes only Rust
   documentation.

## Validation and Acceptance

The plan is complete only when all of the following are true:

- Every in-scope Go anchor has exactly one normalized inventory row.
- Every Rust test that is counted as runnable has a non-empty body and at
  least one meaningful assertion.
- Every ignored row has a primary blocker, owner, expiry/review date, and a
  receipt link.
- Parser, session, transaction, DDL, planner/executor, and statistics tests
  can run in their fast tier without sleep-based synchronization or external
  services.
- Go-produced fixtures or Go test outputs are used wherever byte format,
  error text, SQL rendering, protocol flags, or plan shape is the contract.
- Failpoint tests enable and disable failpoints around each test, and all
  harness-owned global state is cleaned after the test.
- `cargo nextest` passes for each changed crate with the documented feature
  set, and `cargo llvm-cov` produces a report for the same scope.
- The ignored-gap count does not increase without a reviewed reason.
- RealTiKV and other heavy tests are visible in a separate gate with a
  reproducible lifecycle and cleanup.

Track these metrics in `rust/testport/coverage/`:

    unique_non_harness_anchor_total
    executed_and_passing_anchor_total
    runnable_anchor_coverage =
      executed_and_passing_anchor_total /
      unique_non_harness_anchor_total
    ignored_gap_total_by_primary_blocker
    ignored_gap_burndown_since_baseline
    Rust line/function/branch coverage per crate

Do not set a single 100% target for all 7,025 manifest functions. Benchmarks,
`TestMain`, RealTiKV, and intentionally unsupported features belong to other
classes. For implemented feature surfaces, the milestone target is at least
80% of non-harness anchors executable and passing, with no unowned ignored
rows; the exact target must be recorded per package in the receipt.

## Idempotence and Recovery

Inventory generation is read-only and safe to rerun. Generated reports must be
stable for the same commit; if ordering changes, sort by Go package, file, and
test name before committing. Test harnesses must reset fake time, failpoints,
catalog versions, locks, process entries, metrics, and temporary fixtures in
cleanup guards so a failed test does not poison the next test.

If a milestone fails midway, keep the source carriers and receipt, mark the
affected anchors `partial` or `blocked`, and record the exact failing command.
Do not delete tests or force-push over another agent's work. Rebase or cherry-pick
the milestone commit in a dedicated worktree, rerun the scoped gate, and only
then update the shared branch.

## Artifacts and Notes

The baseline evidence used to write this plan is available in:

- `rust/testport/MANIFEST.json` — package and batch inventory.
- `rust/testport/TESTPORT_EXECPLAN.md` — existing porting and receipt workflow.
- `rust/testport/receipts/b051.md` — parser dependency-cycle gap.
- `rust/testport/receipts/b085.md` — planner runtime/plan-cache gap.
- `rust/testport/receipts/b117.md` — Domain and external-service gaps.
- `rust/testport/receipts/b134.md` — executor FK/index-merge/infoschema gaps.
- `rust/testport/receipts/b148.md` — session receipt with only three runnable
  examples at the baseline.
- `rust/crates/tidb-session/src/tests_session_part4_source.rs` — representative
  source carrier with all 39 entries ignored at the baseline.

The raw source-carrier count is an upper-bound inventory signal, not a product
coverage claim. Any implementation PR must report normalized anchor numbers
and the exact affected package, rather than saying that all ignored attributes
are missing behavior.

## Interfaces and Dependencies

The following are test-support contracts, not requests to expose production
internals:

- `ParserTestKit`: parse, restore, parse/restore golden, and parse-error APIs;
  depends on `tidb-lexer`, `tidb-ast`, and `tidb-parser` in acyclic direction.
- `SessionHarness`: session lifecycle, SQL execution, shared catalog,
  InfoSchema, transaction client, fake clock, failpoints, privilege,
  resource-group, process-info, metrics, and cleanup providers.
- `MockKvCluster`: deterministic MVCC, locks, transaction outcomes, region
  errors, and multi-session visibility.
- `DdlJobRunner`: job lifecycle, schema version/lease, MDL, reorg/backfill,
  cancellation, and fake PD/GC.
- `PlannerExecutorHarness`: parse/plan/execute, prepared statements, plan
  cache, injected statistics, chunk/result batches, and MPP/TiFlash mock
  routing.
- `StatsHandleFixture`: mysql.stats_* load/save, Analyze jobs, histogram/TopN/
  CMSketch/feedback, and schema invalidation.
- `AnchorInventory`: unique Go anchor identity, classification, blocker,
  owner, expiry, receipt link, and coverage aggregation.

Each interface should be implemented in the narrowest owning crate or a
test-support module consumed by that crate. Avoid a new global utility crate
unless at least two crates need the same dependency and the dependency graph
remains acyclic. Production APIs should only be changed when the Go contract
requires them; test seams should prefer traits, constructors, and explicit
dependency injection.

## Agent Handoff Contract

Every implementation agent receives one complete Go package, or one explicitly
bounded package slice already recorded in `MANIFEST.json`. The agent must not
mix unrelated production fixes into the test-infrastructure change. A handoff
is ready for landing only when it contains all of the following:

1. The exact Go source files, production files, test/support files, fixtures,
   and generated variants in scope.
2. The Rust owning crate and source-carrier files being replaced or extended.
3. The primary infrastructure blocker removed and any secondary blocker left
   in the inventory.
4. Tests with behavior assertions, not merely compilation or no-panic checks.
5. The scoped `cargo nextest`, formatting, and (when appropriate) coverage
   commands, including the observed result and baseline failure set.
6. An updated receipt and inventory row for every affected anchor.
7. A short note in the milestone's `Decision Log` when the implementation
   chooses a fake, a fixture format, a dependency boundary, or a deliberate
   product limitation.

Use this handoff template in the PR description or receipt:

    Package / batch:
    Go anchors:
    Rust crate and files:
    Blocker removed:
    Remaining blockers:
    Fixtures/oracles:
    Commands and results:
    Coverage delta:
    Risks and follow-ups:

Parallel work is allowed only when the dependency graph is respected. M0 is a
shared prerequisite. Parser work (M1) and the first session-harness prototype
(M2) may proceed in parallel after the inventory is stable. M3 depends on the
session fixture contract; M4 depends on the catalog/KV and fake-clock pieces;
M5 depends on M1, M2, and injected statistics; M6 can start its provider
interfaces while M5 is in progress. A landing supervisor should keep the
shared branch linear, cherry-pick complete agent commits, run the scoped gate,
and reject receipts that claim a partial Go package as complete.

## Outcomes & Retrospective

This section is intentionally empty until milestones land. At completion,
record the final normalized anchor coverage, per-crate Rust coverage, the
number of ignored rows by category, the commands and CI jobs that passed, and
which gaps remain intentionally RealTiKV/nightly or unsupported. Explain any
change to the milestone order and the evidence that caused it.
