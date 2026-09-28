# Close structural parity gaps against Go master

This living ExecPlan follows repository `PLANS.md`. Work is on
`hparser-integration`; Go master is the reference, not Go files modified by the
Rust integration branch. Whole Go packages remain the minimum completion unit;
these milestones are integration checkpoints, not completed package claims.

## Purpose / Big Picture

Make DML, CTE optimization, expression pushdown and foreign-key DDL share the
same ownership and lifecycle contracts as Go. Queries must keep their statement
context through every execution layer, and planner improvements must reach the
actual write/read path. Continue auditing structural differences after closing
these four reported boundaries.

## Progress

- [x] Fetched master and fast-forwarded the clean checkout from a4827714cc to
  dbedb7ba96 on 2026-09-27. Incoming work addresses all four original findings.
- [x] Verify incoming multi-table DML, lazy CTE, shared protobuf expression and
  catalog-aware foreign-key DDL implementation and regressions.
- [x] Close remaining coprocessor shared-expression context ownership gaps.
- [x] Audit residual structural differences, recording confirmed behavior and
  unverified risks separately.
- [x] Run relevant Rust and Go gates, lint and review; record failed gates.
- [x] Prepare the reviewed integration checkpoint for commit and push;
  publication is recorded in branch history.

## Context and Orientation

`tidb-executor/driver/multi_dml.rs` now consumes physical DML sources for KV
rows; matrix-backed tables still have a separate interpreter. `tidb-planner`
now exposes a CteOptimizer callback in RuleContext, consumed during statistics
derivation. `tidb-exec/foreign_key_build.rs` now separates catalog-free metadata
construction from submitter/owner catalog validation. `tidb-expr/distsql_builtin.rs`
decodes protobuf expressions to shared Expression trees, and unistore's
`cophandler.rs` initially used this only for signatures absent from its older
SimpleSig engine. The incoming shared evaluator called NoColumns, dropping
the DAG request's session timezone and division precision. This checkpoint
uses a request-owned context for every catalog-admitted scalar and implicit
cast, including selection, aggregate arguments and TopN keys. A DAG is the serialized coprocessor
plan sent by TiDB; its context owns these settings and warning policy.

## Plan of Work

First run the original failing cluster tests and the affected session suites
on the fetched code. Inspect each incoming fix rather than reimplement it.
For coprocessor expressions, reproduce a conditional wrapping a context-sensitive
builtin, then ensure selection, aggregation and TopN pass the same request
context to shared evaluation. Do not add only a CaseWhenInt special case.
Retain source signature semantics, short-circuiting and warning ownership.
Afterward inspect residual multi-table row identity and cache contracts, CTE
stats/physical-plan ownership, and FK submission/owner validation. Fix confirmed
structural gaps with fail-before/pass-after coverage; record remaining package
inventory and workload gates explicitly.

## Concrete Steps and Validation

Run from the repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_multi_table_dml -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_recursive_cte -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib unistore_cop -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib schema_changes -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

Server tests need host sysctl access. Go reference tests use the pinned archive
at /private/tmp/tidb-go-structural-20260927 (origin/master
8936d7bdcb13a4fc767de42489aace2711c2c6fd). From that directory:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/structural-go-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustCopRequestContext$' -count=1

The overlay installs /private/tmp/structural-go-reference_test.go as a temporary
cardinality test. It creates a table with DATETIME `2024-01-02 03:04:05` and
checks `CASE WHEN id > 0 THEN UNIX_TIMESTAMP(d) ELSE 0 END = UNIX_TIMESTAMP(d)`
in UTC, +08:00 and America/New_York. Go passed. The wrapper disabled failpoints
and reached refcount zero; its cleanup also printed git errors because this is
an archive, not a checkout. The initial go1.25.12 attempt was refused because
master requires go1.25.14; this was a toolchain issue, not a parity result.
No Go/Bazel input changes are planned, so bazel_prepare is not triggered.

## Decision Log

- Reuse incoming fixes and validate their remaining boundaries. The fresh branch
  changes invalidate the previous audit as a description of current code.
  Date: 2026-09-27.
- Keep request settings at the coprocessor context boundary. A builtin-specific
  patch cannot prevent the same bug in conditional/aggregate/TopN expressions.
  Date: 2026-09-27.

## Surprises & Discoveries

Incoming commits already add physical multi-DML reads, lazy CTE optimization,
shared expression decoding and FK metadata/validation. Shared expression
execution still uses NoColumns rather than DAG evaluation settings.

## Idempotence and Recovery

Tests are local and repeatable. Preserve uncommitted work; temporary baseline
mutations must restore files in finally blocks and must not overlap other builds.
Fetch before publishing; rebase concurrent changes without force pushing, then
repeat affected checks. Never claim whole-package or workload parity from a
subset of integration tests.

## Outcomes & Retrospective

The new timezone regression failed before the context fix (UTC filtered out
the matching row) and passed afterward in all three zones. Unit coverage also
proved the decoder rejected encoder-admitted implicit casts, and that the
initial shared routing rejected a real-valued condition. Both now pass.
The cast decoder reuses the encoder's cast signature table and native cast
builder rather than constructing an unsupported generic `cast` function.
Request warnings are drained into the protobuf response.

Validation evidence (local logs are temporary):

- `tidb-unistore --lib cophandler`: 63 passed after the final truth-conversion
  fix. Covers selection, aggregate and TopN context, precision, cast warning
  policies, real/string condition coercion and NULL.
- `tidb-server --lib pushed_`: 3 passed on the final code.
  `schema_changes`: 21 passed.
- `tidb-session --lib tests_recursive_cte`: 14 passed.
- `tidb-session --lib tests_multi_table_dml`: 30 passed, 1 failed on the fetched
  baseline. `a_syntax_error_carries_gos_sentence_and_position` expects multiple
  statements to fail, while the incoming multi-statement implementation accepts
  them. Not changed without further Go contract verification.
- `tidb-server --lib unistore_cop`: 112 passed, 3 failed on both broad runs.
  `cluster_info_reports_this_node` expects one node and receives two; also
  reproduced with the production files restored to fetched HEAD.
  `stats_notifier_uses_a_real_internal_transaction_like_go` and
  `unchanged_updates_lock_only_matched_rows` fail around transaction cleanup /
  lock recovery deadlines. They remain unresolved, not counted as parity wins.
- `cargo check ... -p tidb-server --all-targets` and `make lint` passed on
  the final code. `git diff --check` passed.
- The expression-only `distsql_builtin` test command could not build its test
  binary: seven existing calls to substring/sec_to_time/maketime omit the
  newly required Columns parameter in `builtin_ext/string2.rs` and
  `time_fn/tests.rs`. Those functions and tests are unchanged in this checkpoint.
  Decoder regression coverage ran successfully through tidb-unistore instead.
- No workload benchmarks or full Go package inventory / completion gates ran.
  Shared evaluation currently materializes a MutRow per expression; throughput
  and allocation cost are unmeasured.

The next structural audit should trace multi-table target identity end to end:
KV sources now use physical plans, but matrix-backed sources retain a separate
interpreter; source row widths and handles are reconstructed from KV metadata,
and the physical DML root is built with `FkPlanSpec::default()`. These are
verified architecture differences, not yet demonstrated SQL failures. Per-row
FK enforcement exists, so empty plan metadata alone does not prove missing
constraint enforcement. Complete source/test/variant inventories and workload
gates remain required before any whole-package parity claim.
