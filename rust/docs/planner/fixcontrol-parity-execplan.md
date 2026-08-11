# Complete `pkg/planner/util/fixcontrol` and wire Fix52592 end to end

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

The Rust SQL node accepts the `tidb_opt_fix_control` system variable, but before this work it stored only the raw string. Its parser was an isolated seed: it did not expose Go-compatible typed getters, did not keep a parsed session map, and no planner decision consumed the map. In particular, TiDB fix control 52592 is meant to force point lookups through the ordinary coprocessor range path for `SELECT`, `UPDATE`, and `DELETE`; Rust continued to choose `Point_Get` or `Batch_Point_Get`.

After this plan is complete, setting `tidb_opt_fix_control` through session, global, default, and direct statement-AST `SET_VAR` paths will update one parsed session authority. The package's get/set fixtures retain Go's exact errors and warnings; direct SET_VAR retains first-wins/conflict order within the explicitly documented optimizer-lifecycle boundary below. Fix52592 will disable both direct and ordinary point-get conversions for reads and writes. The server's pre-execution read-shape classifier will see the same effective persistent or statement-scoped value, so a statement that needs the ordinary path is not incorrectly announced as a MaxTS point read. SQL `EXPLAIN`, physical storage requests, raw variable reads, warnings, typed getters, and cluster snapshot counters make the result observable.

## Progress

- [x] (2026-08-11 12:58Z) Pinned the seven-artifact Go package at its last-change commit `811a10e115d416aadcc9407ac4df0fdd4deb1181`, tree `c7f04c91c529664398fda49f92bd7c5bbc0b1404`; `git diff --quiet <pin>..HEAD -- pkg/planner/util/fixcontrol` succeeds.
- [x] (2026-08-11 12:58Z) Ran the initial Rust regression and recorded unresolved imports for the typed API and 17 issue constants; the aggregate test target also has unrelated existing compile drift in `cost_factors.rs` and `row_size.rs`.
- [x] (2026-08-11 12:58Z) Implemented the issue catalog, source-shaped parser errors, typed string/bool/int/float getters, and Go-compatible hexadecimal/special float parsing; focused `tidb-planner` unit tests pass.
- [x] (2026-08-11 13:05Z) Synchronized the two fixture artifacts (one input/output suite) through `Session::run`, including raw/parsed atomicity, exact 1105 errors, duplicate warnings, GLOBAL inheritance, DEFAULT, and rejected cluster-row seeding.
- [x] (2026-08-11 13:12Z) Closed direct-AST SET_VAR writer/restore behavior for SELECT, UPDATE, DELETE, and EXPLAIN: two-pass first-wins emits 3126 before validation 1105; unknown names remain in their explicitly deferred hint-parser disposition.
- [x] (2026-08-11 13:20Z) Carried the parsed controls in `StmtContext` and gated every Fix52592 SELECT/batch/singleton-range and UPDATE/DELETE point decision. Plan and physical get/scan tests are green, including unique-key and actual write results.
- [x] (2026-08-11 13:30Z) Made `statement_read_shape` derive the effective persistent/direct-AST value before execution. A real cluster snapshot-counter test is green for persistent ON, direct ON, ON plus hint OFF, invalid-first first-wins, and prepared execution.
- [x] (2026-08-11 13:43Z) Ran the unchanged Go oracle, focused parser/session/access/server tests, both server-factory regressions, owning-crate checks, repository lint, formatting, and final diff inspection. All required Ready gates pass; only the separately documented pre-existing planner aggregate compile drift remains.

## Surprises & Discoveries

- Observation: Rust's `f64::from_str` does not accept Go `strconv.ParseFloat`'s hexadecimal grammar, so `0x1p2` silently took a typed getter's default.
  Evidence: the new source-shaped unit first failed before the hexadecimal parser and now passes for `0x1p2`, `+Inf`, `-Inf`, `NaN`, and malformed values.

- Observation: Rust `ParseIntError`'s display text is not Go's public error contract.
  Evidence: Rust reports `invalid digit found in string` for `55.5`, while Go reports `strconv.ParseInt: parsing "55.5": invalid syntax`; the API now uses `IntParseError` with syntax/range spelling.

- Observation: Rust's integer parser also gives syntax/range a different precedence from Go's incremental digit loop.
  Evidence: `9223372036854775808x` was incorrectly saturated as range instead of returning value 0 plus syntax, while `18446744073709551616x` as a key was incorrectly syntax instead of range. A shared source-shaped uint64 loop now returns range immediately on overflow and only inspects later digits while still in range.

- Observation: a planner-only Fix52592 gate would still let the cluster server request MaxTS for a statement whose `SET_VAR` overlay disables the point path.
  Evidence: `Session::statement_read_shape` runs before `dispatch::apply_set_var_hints`, so classification must inspect the AST's first applicable hint in addition to persistent session state.

- Observation: EXPLAIN is dispatched through an early administrative door before the ordinary query/DML hint application point.
  Evidence: the first hinted EXPLAIN GREEN attempt still showed `Point_Get`; applying the direct target hint before `apply_schema_stmt` changed the same trace to `TableRangeScan` and kept statement-end restoration.

- Observation: global rows loaded from a cluster are not necessarily valid under this binary's registry, so an infallible seed with `expect` can panic while opening a connection.
  Evidence: an invalid `tidb_opt_fix_control` row now returns 1105 and leaves the previous raw systems, shared GLOBAL handle, and parsed map unchanged; both server factories propagate that error.

- Observation: Go parses all SET_VAR hints before applying the first value. Unknown names are rejected before conflict detection, while a duplicate known name emits 3126 before validation of an invalid first value.
  Evidence: the mixed invalid-first/valid-second regression reports 3126 then 1105; two unknown entries report no fabricated 3126 under this package's deferred unknown-warning disposition.

- Observation: the canonical planner aggregate currently cannot compile for reasons outside this package.
  Evidence: `cost_factors.rs` compares an `f64` constant with integer `10_000`, and `row_size.rs` calls a removed `estimate_width` method. Focused owning-crate tests remain available and the unrelated files will not be edited in this package commit.

## Decision Log

- Decision: Treat the seven direct artifacts in `pkg/planner/util/fixcontrol` as the atomic Go package, but claim only its set/get API plus the Fix52592 production integration in this commit.
  Rationale: the other 16 issue constants have consumers owned by other Go packages such as planner core, ranger, bindinfo, and executor. Exporting their identifiers and getters is required by `get.go`; claiming that every external behavior is active would overstate this package. Each remaining consumer is explicitly deferred to its owning package.
  Date/Author: 2026-08-11 / Codex

- Decision: Store an `OptimizerFixControl` alongside raw system-variable text in `SessionVars` and update it at set/reset/seed/restore primitives.
  Rationale: parsing only at planner read sites hides writer drift and lets session, global seed, or `SET_VAR` restore paths disagree. One derived map owned by the same session state closes all current writers without frontend-specific copies.
  Date/Author: 2026-08-11 / Codex

- Decision: Carry a cloned parsed map in `tidb_executor::StmtContext`.
  Rationale: access planning already receives `StmtContext`, while making executor code reach back into a mutable `Session` would invert ownership and complicate statement overlays.
  Date/Author: 2026-08-11 / Codex

- Decision: Extend Fix52592 to the read-shape classifier rather than gate only executor access construction.
  Rationale: MaxTS versus ordinary timestamp choice is made before execution and is an externally meaningful consistency/performance contract. It must use the same effective persistent and statement-scoped value as the planner.
  Date/Author: 2026-08-11 / Codex

- Decision: Scope this commit's SET_VAR claim to hints physically present in the statement AST.
  Rationale: SQL bindings inject hints later in `pkg/bindinfo`, after this package's pre-execution classifier and direct overlay point. Binding-produced SET_VAR and its Fix44389 consumer are deferred to that owning Go package; this plan does not claim all SET_VAR producers.
  Date/Author: 2026-08-11 / Codex

- Decision: Preserve source-observable duplicate conflict ordering, but do not claim Go's two identical validation warnings from its two optimizer lifecycle passes.
  Rationale: Rust installs one statement overlay once. It emits the production 1105 and continues, while the second Go 1105 is an optimizer/cache lifecycle artifact owned by planner core. Exact 3126 conflict rows and first-wins state are retained here.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

Implementation and Ready validation are complete with no known P0/P1 blocker. The direct Go package API and fixture suite are synchronized; Fix52592 is live across session writers, SELECT/batch/unique access, UPDATE/DELETE reads, direct-AST hints, EXPLAIN, and the cluster MaxTS classifier. The unchanged Go oracle, five parser/getter tests, four session fixture/access tests, the real cluster snapshot-counter test, both connection-factory rejection tests, four owning-crate checks, repository lint, formatting, and diff checks all pass. The canonical planner aggregate remains blocked before selecting this package by the documented pre-existing `cost_factors.rs` and `row_size.rs` compile drift. Explicitly deferred surfaces are the external consumers of the other 16 issue IDs, binding-injected SET_VAR, Go's second optimizer-lifecycle validation warning, generic unknown/not-updatable hint warnings, and configured real-TiKV session planners that do not use `tidb_session::SessionVars`.

## Context and Orientation

The pinned Go package has exactly seven direct artifacts: `BUILD.bazel`, `get.go`, `set.go`, `fixcontrol_test.go`, `main_test.go`, and the two fixture artifacts `testdata/fix_control_suite_in.json` and `testdata/fix_control_suite_out.json` (one input/output suite). There are no nested Go sources, build-tag variants, generated files, failpoints, benchmarks, fuzz targets, or examples. `set.go::ParseToMap` parses comma-separated unsigned issue numbers and string values and returns duplicate-assignment warnings. `get.go` defines 17 issue constants and typed getters. `fixcontrol_test.go::TestFixControl` executes every fixture SQL through a real session, then checks the raw variable, parsed map, typed conversions, exact error, and `SHOW WARNINGS`; `TestParseToMapEmptyValue` owns the empty-value edge.

`main_test.go::TestMain` runs common test setup, parses flags, loads/optionally records the fixture suite, wraps `testing.M`, and verifies goroutine leaks with an explicit ignore list. Rust tests use deterministic in-process sessions, deserialize both artifacts directly, do not record outputs, and own no process goroutines, so setup/flag/record/goleak are build-harness dispositions rather than missing production semantics. `BUILD.bazel` lists `get.go`/`set.go` in the library and both test Go files plus `data = glob(["testdata/**"])`; its `timeout = "short"`, `flaky = True`, and `shard_count = 2` are scheduling metadata. The Rust focused commands consume the full fixture suite without a Bazel scheduling analogue.

The Rust parser lives in `rust/crates/tidb-planner/src/fix_control.rs`. Session system variables live in `rust/crates/tidb-session/src/vars.rs`; SQL assignments and statement hints live in `variables.rs`; statement context construction lives in `stmt_ctx.rs`. Read access choices are in `rust/crates/tidb-executor/src/driver/access.rs`; update/delete call the shared `write_read_path` from `driver/dml.rs`. The server asks `Session::statement_read_shape` whether it can use MaxTS before `Session::execute_statement` applies `SET_VAR` hints.

A point get is a direct primary/unique-key lookup. A batch point get is the multi-key form. The ordinary path represents the same keys as one or more ranges and reads them through the coprocessor. Fix52592 deliberately forces that ordinary path because direct point paths cannot provide every projection optimization supported by coprocessor execution.

## Plan of Work

First add a Rust test module that deserializes both pinned JSON fixture artifacts and runs each SQL string through `Session::run`. Before changing writers, retain the failure showing that the raw variable changes while no parsed map exists. Add explicit cases for global seeding, `DEFAULT`, duplicate warnings, statement-scoped restore, wrong-scope validation, and invalid input atomicity.

Next extend `SessionVars` with an `OptimizerFixControl` value and accessor. Parse before committing a raw `tidb_opt_fix_control` write so invalid input leaves both representations unchanged. Seed raw systems, parsed control, and shared GLOBAL handle atomically and fallibly; refresh the map after scope-aware `DEFAULT` and after a SET_VAR snapshot is restored. The SQL assignment layer appends duplicate-key warnings at level Warning/code 1105 only after its scope-aware writer succeeds; an invalid parse travels as `ValidationRefused`, maps to 1105/HY000, and the ordinary statement error path appends the Error warning. `SET GLOBAL` validates and warns but does not mutate the current session's map; a new session parses the shared raw global value when seeded.

Then add the parsed value to `StmtContext` and attach it in both query and DML builders in `stmt_ctx.rs`. In `driver/access.rs`, when Fix52592 is true, skip the early batch point source, the direct point source, and the singleton handle-range trace conversion. In `write_read_path`, skip the DML point source so update and delete use the range reader. Keep impossible unsigned-negative predicates as `TableDual`, and keep already-ranged predicates unchanged.

Finally make hint extraction common to query, update, delete, and EXPLAIN targets that physically own direct hints. Reuse that extraction in `statement_read_shape` to calculate the effective value before the overlay is applied. Preserve Go's two-pass first-hint-wins ordering and let an explicit OFF override persistent ON for the one statement. Use cluster snapshot counters, not only an enum unit test, to prove ON pays for a timestamp and OFF retains MaxTS. Binding-injected hints and configured real-TiKV sessions that bypass `SessionVars` remain explicit unsupported/deferred surfaces; this commit does not claim every server configuration or every SET_VAR producer.

## Concrete Steps

Run all commands from repository root unless noted. The unchanged Go oracle is:

    go test -tags=intest ./pkg/planner/util/fixcontrol -run '^(TestFixControl|TestParseToMapEmptyValue)$' -count=1

Run focused Rust parser/getter tests:

    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-planner fix_control::tests -- --nocapture

Run the synchronized session fixture and Fix52592 runtime tests after their module is added:

    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session tests_fix_control -- --nocapture

Run the physical access-path session test and real cluster snapshot-counter test, then owning-crate checks:

    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session tests_fix_control::fix_52592_disables_point_and_batch_paths_for_select_update_and_delete -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server fix_52592_and_its_statement_overlay_gate_max_ts_before_execution -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server factory_rejects_invalid_persisted_fix_control_without_process_leak -- --nocapture
    cargo +1.97.0 check --manifest-path rust/Cargo.toml -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server
    cargo +1.97.0 fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check

The historical canonical planner aggregate command is retained as evidence, but currently fails before selecting the fix-control test because of unrelated source drift:

    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p difftest-planner-tests --test all fix_control

Before a completion claim, follow `.agents/skills/tidb-verify-profile` Ready guidance for all changed owning crates and run repository lint as required by `AGENTS.md`.

## Validation and Acceptance

Every input and output row in both Go JSON fixtures must be consumed by a Rust test. Successful assignment preserves the raw text exactly while producing the same map and typed values. Duplicate unequal values append exactly `repeated assignment for fix control: ...` as Warning 1105. Invalid keys, missing quote, and missing colon fail with Go's exact messages, append Error 1105, and leave both raw and parsed state at the preceding valid value.

`SET GLOBAL` must affect a new session but not rewrite the current session copy. `SET SESSION ... = DEFAULT` must restore the empty default map. Direct-AST `SET_VAR` must apply only during one query/update/delete/EXPLAIN and restore on success or error, with the first duplicate hint winning. A duplicate known name reports exact 3126; when the first fix-control value is invalid, 3126 precedes the production 1105 validation warning. Binding-injected SET_VAR, unknown/not-updatable hint warnings, and Go's second optimizer-lifecycle copy of the validation warning are not claimed by this package.

With Fix52592 OFF, singleton primary/unique-key reads and writes may show `Point_Get`, and multi-key reads may show `Batch_Point_Get`. With it ON, the corresponding select, update, and delete plans must show ordinary table/index ranges; an already-ranged query remains a range scan and an impossible unsigned-negative predicate remains `TableDual`. Physical storage probes require exact get/no-scan shapes while OFF and scans while ON, and actual updates/deletes must still affect the right rows. The early read-shape classifier must return Unknown rather than Point/MaxTS for persistent or hinted ON, including prepared values; a statement hint OFF over persistent ON must restore point classification for that statement. UPDATE/DELETE are already non-MaxTS statement shapes, while their physical read paths still obey the same Fix52592 gate.

## Idempotence and Recovery

All Go and Cargo test/check/fmt commands are safe to rerun. Tests use in-memory sessions and do not require a TiKV playground. No generated Go or Bazel file is edited, so `make bazel_prepare` is not required under the repository decision matrix. If a test fails midway, leave the working tree unstaged, update `Progress` and `Surprises & Discoveries`, fix only the owning slice, and rerun the same focused command. Do not edit the unrelated aggregate failures in `cost_factors.rs` or `row_size.rs` as part of this package.

## Artifacts and Notes

Initial parser/getter RED:

    error[E0432]: unresolved imports `tidb_planner::fix_control::OptimizerFixControl`, `FIX_52592`, ...
    error: could not compile `difftest-planner-tests`

Focused parser/getter GREEN after source-shaped numeric parsing:

    running 5 tests
    test result: ok. 5 passed; 0 failed

Session fixture, writer, plan, and physical-storage GREEN:

    running 4 tests
    test result: ok. 4 passed; 0 failed

The real cluster MaxTS regression passes 1/1. The two production connection factories reject an invalid persisted fix-control row with exact 1105/HY000, publish no process entry, and retain the valid open/live/drop lifecycle; the focused factory command passes 2/2. The four-crate `cargo check`, unchanged tagged Go oracle, repository `make lint`, `cargo fmt --check`, and `git diff --check` all pass.

The source-shaped integer precedence regression was RED before the incremental decimal parser: two of five parser tests failed because `9223372036854775808x` and `18446744073709551616x:ON` were classified with Rust's syntax/range precedence. The same focused command is now GREEN 5/5, including positive and negative signed saturation and unsigned-key overflow.

The canonical planner aggregate is retained as negative baseline evidence, not represented as a fix-control failure: it stops at unrelated compile errors in `cost_factors.rs` and `row_size.rs` before test selection. No source in those packages is changed here.

The 17 exported constants are 52592, 33031, 43817, 44262, 44389, 44830, 44823, 44855, 45132, 45822, 45798, 46177, 47400, 49736, 52869, 54337, and 56318. Only Fix52592 becomes a newly live control here. The other constants and typed getters are complete API surface, while their external consumers remain explicit work for their owning Go packages.

## Interfaces and Dependencies

`tidb_planner::fix_control::OptimizerFixControl` owns a `BTreeMap<u64, String>` and exposes `parse`, `as_map`, and typed `get_*`/`get_*_with_default` methods. `IntParseError`, `FloatParseError`, and `ParseError` use Go-compatible messages.

`tidb_session::SessionVars` owns an `OptimizerFixControl`, exposes it immutably, and keeps it synchronized with `tidb_opt_fix_control` through `set_system`, scope-aware reset, fallible/atomic `seed_from_globals`, and `restore_system`. `Session::apply_assignment` appends parser warnings only after the scope-aware writer succeeds.

`tidb_executor::StmtContext` carries a cloned `OptimizerFixControl` and exposes a read-only accessor. `driver::access` and `driver::dml` consult `FIX_52592`. `Session::statement_read_shape` and `apply_set_var_hints` share direct-AST hint extraction so pre-execution classification and execution agree for the claimed producer surface.

Plan revision note: created after the source pin, direct artifact audit, parser/getter RED and focused GREEN, numeric grammar review, external-consumer boundary correction, and discovery of the pre-execution MaxTS/SET_VAR ordering risk.
