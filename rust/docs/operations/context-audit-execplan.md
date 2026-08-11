# Complete and certify `pkg/util/context` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's context package defines the value-store context shape, monotonic context IDs, SQL warning wire/storage semantics, plan-cache decision tracking, and one-shot range-fallback warnings. These primitives feed statement, DistSQL, session, executor, and error-context code. This plan audits every Go production, test, and build artifact; maps every source assertion and public branch to Rust; closes source-observed JSON and panic-recovery gaps; validates live warning and plan-cache consumers; and publishes the result as one Go package commit.

## Progress

- [x] (2026-08-11 18:14Z) Fixed the complete five-file Go inventory at `59dfa4d3b214ded26f957249efbda21f95149bb5`; current package bytes match that pin.
- [x] (2026-08-11 18:15Z) Confirmed there is no `doc.go`, generated input, fixture, testdata, build/platform variant, benchmark, fuzz target, example, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-11 18:19Z) Read all Go owner/test/build artifacts, all three Rust owner modules, the public context contract, and the warning/plan-cache consumer surfaces.
- [x] (2026-08-11 18:21Z) Passed the complete four-test Go source suite normally and under `-race`; passed the seven-test Rust owner and six-test integration baselines.
- [x] (2026-08-11 18:22Z) Used a public Go probe to fix the exact SQLWarn JSON wire, tracker recovery after callback panic, and sync.Once completion after callback panic.
- [x] (2026-08-11 18:28Z) Added three exact Rust regressions, retained all failures against the old implementation, and made the smallest production/test corrections.
- [x] (2026-08-11 18:36Z) Passed the nine-test owner, six-test integration, and all selected WIP consumer gates; passed formatting and owner library Clippy; added the atomic semantic receipt.
- [x] (2026-08-11 18:55Z) Completed pre-sync Ready validation and self-reviewed the final five-file one-package diff.
- [x] (2026-08-11 19:02Z) Rebased the single package commit onto fresh remote `119c5f137` and repeated the complete Ready profile successfully.
- [ ] Push without force and verify the freshly fetched remote SHA.

## Surprises & Discoveries

- Observation: all four top-level Go tests have direct Rust assertion coverage, but the typed terror fixture is not source-equivalent.
  Evidence: SQLWarn round-trip, IgnoreWarn, StaticWarnHandler copy/truncate behavior, and handler copying are mapped. The Rust SQLWarn test constructs `execution result undetermined unknown`; Go `GenWithStackByArgs("unknown")` against a template with no verbs produces `execution result undetermined%!(EXTRA string=unknown)`. Round-tripping the wrong Rust fixture masks the wire mismatch.

- Observation: callback panic adds permanent mutex poisoning to the Rust plan-cache tracker.
  Evidence: Go locks with `defer Unlock`; after recovering a warning callback panic, the tracker remains usable with `planCacheUnqualified == "risky callback"`. Rust invokes the callback while holding `Mutex<TrackerState>` and every later method calls `lock().unwrap()`, so the first later access panics on poison.

- Observation: callback panic leaves Rust Once poisoned, unlike Go sync.Once.
  Evidence: Go `sync.Once.Do` marks the call done even if the callback panics. The public probe recovers the first range-fallback warning panic, then a second call returns normally and the callback count remains one. Rust `Once::call_once` poisons and makes the second call panic before invoking the callback.

- Observation: warning retention deliberately permits a batch to cross the single-append cap.
  Evidence: both Go and Rust check `len < MaxUint16` once before appending the whole batch. A 65,534-entry buffer plus two errors retains 65,536 entries, and the error count wraps to zero as Go `uint16`; the public integration contract already pins this non-obvious behavior.

- Observation: Rust uses native ownership/type boundaries for several Go dynamic surfaces.
  Evidence: `WarnErr` closes Go's open error interface over typed terror versus message, warning getters return owned copies instead of shared slices, truncate accepts only nonnegative `usize`, ValueStoreContext has an implementation-owned typed key domain, and optional trait objects replace nil handlers. These choices preserve repository behavior while making aliasing and invalid dynamic values unrepresentable.

- Observation: the owner has several live or parallel Rust consumers.
  Evidence: `tidb-distsql` consumes the warning cap, `tidb-executor` reexports and applies it, `tidb-session` owns the SQL-visible warning buffer and non-prepared plan cache, `tidb-exec` maintains the detailed warning publication contract, and `tidb-error::errctx` mirrors the appender capability to avoid dependency cycles.

- Observation: the tidb-exec warning contract moved into an aggregate test target.
  Evidence: the historical receipt names `--test warning_publication_source`, which no longer exists. `tests/all.rs` registers that source file, and `cargo test -p tidb-exec --test all 'warning_publication_source::'` runs its current eight tests.

- Observation: Rust 1.97.1 incremental metadata and unrelated dependency lint require scoped Ready invocations on this machine.
  Evidence: the first repeated `tidb-exec` build ICEd in rustc's metadata encoder while reusing a cross-worktree incremental target; the same command passed with `CARGO_INCREMENTAL=0`. Consumer Clippy then reached an existing `tidb-protocol` `double_must_use` warning outside this package; `--no-deps` checked the three direct consumer crates themselves with `-D warnings` and passed.

## Decision Log

- Decision: Recover poisoned `TrackerState` locks with `PoisonError::into_inner`.
  Rationale: callback panic is possible while the state guard is held; Go unlocks during unwind and adds no persistent failure mode. Preserving the already-committed state after recovery matches the source probe.
  Date/Author: 2026-08-11 / Codex

- Decision: Catch the range-warning callback panic inside `Once`, let Once complete normally, then resume the panic outside it.
  Rationale: this preserves all three Go properties: the initiating call still panics, concurrent calls wait for the callback to finish, and every later call observes the operation as done rather than poisoned or rerun.
  Date/Author: 2026-08-11 / Codex

- Decision: Generate the typed terror fixture with the existing Go-format authority and assert the complete observed JSON string.
  Rationale: the production serializer already carries compatible class/code/message/RFC fields; a literal source wire prevents another self-consistent but source-wrong round trip.
  Date/Author: 2026-08-11 / Codex

- Decision: Retain the closed WarnErr, typed ValueStore key, owned warning snapshots, unsigned indexes, synchronized warning store, and optional handler boundaries.
  Rationale: these are native Rust ownership/type integrations. Source assertions and live consumers require messages, levels, order, caps, state, and JSON identity, not Go interface nilness or backing-array aliasing.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat `59dfa4d3b214ded26f957249efbda21f95149bb5` as the accepted Go package pin.
  Rationale: it is the latest and only direct package artifact commit in the accepted history, contains all five current files, and current bytes match exactly.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The complete inventory, source pin, owner and consumer reads, assertion mapping, Go normal/race baseline, Rust owner/integration baseline, public source probe, failing regressions, corrections, receipt, WIP consumer validation, pre-sync Ready validation, final self-review, synchronization, and post-sync Ready validation are complete. Only the external non-force push and fresh-remote verification remain.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/context/BUILD.bazel`, `context.go`, `plancache.go`, `warn.go`, and `warn_test.go`. `context.go` owns ValueStoreContext and the atomic ID generator. `plancache.go` owns PlanCacheType, PlanCacheTracker, five-field save/restore, and RangeFallbackHandler. `warn.go` owns levels, custom SQLWarn JSON, warning traits/handlers, copy/truncate/reset/batch/cap behavior, and the function-backed test appender. It depends on `pkg/parser/terror` and `github.com/pingcap/errors`; the Bazel test is marked flaky and short.

The four Go tests cover five plain/traced/typed/EOF SQL warnings, IgnoreWarn, four CopyWarnings destination-capacity shapes, out-of-range/in-range/zero truncation, and independent handler copying including nil input. Plan-cache, context ID, extension methods, batch cap, and consumers are production branches without direct Go owner tests.

Rust owns the mapping in `rust/crates/tidb-util/src/context/{mod,plancache,warn}.rs` and the public integration contract in `rust/crates/tidb-util/tests/context_contract.rs`. Relevant consumer evidence lives in `tidb-distsql/src/warning.rs`, `tidb-executor/src/stmt_context.rs`, `tidb-session/src/warnings.rs` plus plan-cache tests, and `tidb-exec/src/warning_publication.rs` plus its source contract.

## Milestones

The source-oracle milestone inventories and pins all five Go artifacts, lists exactly four tests, passes normal and race runs, and records exact JSON and panic-recovery behavior. Acceptance is the literal two-entry JSON string, usable tracker with retained reason after recovery, and a non-panicking second Once call with one callback total.

The parity milestone adds failing Rust regressions before production changes. Acceptance is a source-exact JSON fixture, recoverable tracker mutex, and Once-done-after-panic behavior, with the seven original owner and six integration tests still passing.

The integration milestone validates the warning cap in DistSQL/session, source warning publication in tidb-exec, and a session plan-cache flow. Acceptance is no regression in any selected live/parallel consumer.

The publication milestone adds the current receipt and plan, runs the complete Ready profile, synchronizes one commit to current `hparser-integration`, pushes without force, and verifies matching local and fresh remote SHAs.

## Plan of Work

First update the SQLWarn test with the source-shaped typed terror and literal JSON. Add one tracker callback-panic regression and one RangeFallback Once-panic regression. Run each exact test against the old production implementation and retain the failure.

Then centralize tracker locking in a poison-recovering helper. Wrap only the Once callback with `catch_unwind`, return normally from the Once closure, and resume the captured panic outside. Keep warning storage, public interfaces, message text, and consumer code unchanged.

Add a semantic receipt containing the accepted Go pin, owner/integration evidence, and the four focused consumer commands. Complete Ready validation with Go normal/race tests and source probe, owner and integration tests, full `tidb-util`, focused consumer tests, formatting, owning all-target Clippy, relevant consumer Clippy, repository lint, and the Bazel gate decision.

## Concrete Steps

From repository root, run the Go authority and public probe:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/context
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestSQLWarn|TestIgnoreWarn|TestStaticWarnHandler|TestCopyWarnHandler)$' -tags=intest,deadlock -count=1 ./pkg/util/context
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^(TestSQLWarn|TestIgnoreWarn|TestStaticWarnHandler|TestCopyWarnHandler)$' -tags=intest,deadlock -count=1 ./pkg/util/context
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-context-panic-probe.go

From `rust`, run owner and consumer gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'context::'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --test context_contract
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-distsql --lib 'warning::tests::collector_stops_at_the_statement_warning_limit' -- --exact
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-session --lib 'tests_core::session_state::the_session_warning_buffer_stops_at_the_source_retention_limit' -- --exact
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-session --lib 'tests_non_prepared_plan_cache::two_statements_differing_only_in_a_literal_share_an_entry_and_keep_their_own_rows' -- --exact
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-exec --test all 'warning_publication_source::'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 --no-deps -p tidb-distsql -p tidb-session -p tidb-exec --lib -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/context.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

Go must list exactly the four named tests, and normal plus race-enabled runs must pass. The public probe must retain the literal JSON, `tracker_panic=warning callback panic calls=1 usable=true reason="risky callback"`, and `once_first=warning callback panic once_second=<nil> calls=1`.

Focused Rust must pass the seven existing owner tests plus the new panic regressions, the six integration tests, and every selected consumer. The complete owning crate, integration tests, doctest, formatting, all-target Clippy, semantic receipt, and repository lint must pass. The final commit may contain only context owner/test, receipt, and this plan. Publication must be one linear non-force update with matching fresh remote SHA.

## Idempotence and Recovery

All checks are safe to rerun. The Go probe lives under `/tmp` and never enters the repository; move it to Trash after evidence is recorded. Panic tests catch their expected unwind and leave no global state. If remote advances, rebase the one package commit and repeat Ready validation.

## Artifacts and Notes

Initial Go evidence on Go 1.25.10 `darwin/arm64`:

    go test -list: exactly 4 tests
    all 4 source tests: pass normally and under -race
    SQLWarn JSON: [{"level":"Error","msg":"any error"},{"level":"Warning","err":{"class":21,"code":2,"message":"execution result undetermined%!(EXTRA string=unknown)","rfccode":"global:2"}}]
    tracker recovery: panic observed once; tracker remains usable and retains "risky callback"
    Once recovery: first call panics, second does not, callback count remains one

Initial Rust evidence:

    context owner tests: 7 passed, 0 failed, 0 ignored
    context_contract integration: 6 passed, 0 failed, 0 ignored

Regression and WIP evidence:

    old typed-terror fixture: exact JSON assertion failed with "execution result undetermined unknown" instead of the source `%!(EXTRA string=unknown)` text
    old tracker recovery: exact regression failed on the first later lock with PoisonError
    old range Once recovery: exact regression failed because the second call panicked on a poisoned Once
    fixed exact regressions: all three pass
    context owner tests after fix: 9 passed, 0 failed, 0 ignored
    context_contract integration: 6 passed, 0 failed, 0 ignored
    distsql warning-retention consumer: pass
    session warning-retention consumer: pass
    session non-prepared plan-cache consumer: pass
    tidb-exec aggregate warning-publication contract: 8 passed, 0 failed, 0 ignored
    cargo fmt --all --check: initially requested one mechanical fixture-call adjustment; pass after cargo fmt
    cargo clippy -p tidb-util --lib -- -D warnings: pass

Pre-sync Ready evidence:

    Go test list: exactly 4 named owner tests
    Go targeted owner tests: pass normally and under -race
    Go public probe: exact JSON, usable tracker after callback panic, and Once done after callback panic
    Rust context owner: 9 passed
    Rust context_contract: 6 passed
    Rust focused consumers: DistSQL 1 passed; session warning 1 passed; session plan cache 1 passed; tidb-exec warning publication 8 passed
    Full tidb-util: 343 passed, 1 ignored subprocess helper; every integration target and doctest passed
    cargo fmt --all --check: pass
    tidb-util all-target Clippy with -D warnings: pass
    direct DistSQL/session/tidb-exec --no-deps library Clippy with -D warnings: pass
    semantic package gate: pass, 1 package and 6 unique commands
    repository make lint with revive 1.2.1: pass
    git diff --check and five-file atomic-boundary self-review: pass

Post-sync Ready evidence on remote base `119c5f137`:

    Go test list: exactly 4 named owner tests
    Go targeted owner tests: pass normally and under -race
    Go public probe: exact JSON, usable tracker after callback panic, and Once done after callback panic
    semantic package gate: pass, 1 package and 6 unique commands
    Full tidb-util: 344 passed, 1 ignored subprocess helper; every integration target and doctest passed
    cargo fmt --all --check: pass
    tidb-util all-target Clippy with -D warnings: pass
    direct DistSQL/session/tidb-exec --no-deps library Clippy with -D warnings: pass
    repository make lint with revive 1.2.1: pass

Build-artifact recovery evidence:

    one test-list command accidentally omitted CARGO_TARGET_DIR and created a worktree-local rust/target
    the later shared-target build stopped with no space left on device; no test failed
    cargo clean --target-dir /tmp/tidb-package-audit-context/rust/target removed only that ignored build artifact and freed about 2.6 GiB
    the interrupted tidb-exec consumer then passed with the shared target
    the pre-sync tidb-exec rerun first hit a Rust 1.97.1 incremental metadata ICE; CARGO_INCREMENTAL=0 passed all eight tests
    direct consumer Clippy first exhausted the remaining filesystem space, without a lint diagnostic
    cargo clean removed about 11.4 GiB of only rebuildable target artifacts from nine completed historical audit worktrees; the current shared target was retained
    consumer Clippy without dependency lint then exposed only the existing tidb-protocol double_must_use warning; the scoped --no-deps consumer command passed

Failpoint decision:

    no failpoint, testfailpoint, or Bazel failpoint dependency match in the package

Build metadata decision:

    make bazel_prepare is not required: no Go/Bazel/module/manifest edit, Go import change, or new Go test is planned

## Interfaces and Dependencies

The public Rust ValueStoreContext, context ID function, plan-cache types/handlers, warning levels/errors/traits/handlers, cap, and save/restore tuple remain unchanged. The implementation retains `serde`, `tidb-error`, standard synchronization primitives, and all existing consumers; no manifest or dependency changes are planned.

Plan revision note: created after complete Go/Rust owner reads, exact inventory/history and byte-pin checks, failpoint/build decisions, Go list/normal/race tests, Rust owner/integration baselines, assertion mapping, live consumer/receipt review, and public Go JSON/panic probes.
