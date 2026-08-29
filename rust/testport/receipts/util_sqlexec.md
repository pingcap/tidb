# `pkg/util/sqlexec` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The main `pkg/util/sqlexec` package has exactly four artifacts, all read in
full:

- `restricted_sql_executor.go` — restricted and ordinary SQL executor,
  parser, statement, record-set, detach, no-delay-result, option, drain, and
  `ExecSQL` contracts;
- `simple_record_set.go` — the materialized in-memory record set;
- `main_test.go` — the package test harness, containing only `TestMain` with
  goleak verification and no functional package test;
- `BUILD.bazel` — the production and test target inventory.

There is no `doc.go`, README, fixture, benchmark, generated/platform variant,
or additional package-local artifact. The checkout is byte-identical to the
pin. `pkg/util/sqlexec/mock` is a distinct Go package with its own Bazel target
and generated artifact; it is not included in this main-package claim.

## Rust ownership and audit result

`rust/crates/tidb-sqlexec` is the package owner. It implements the complete
production method sets and option state from `restricted_sql_executor.go`,
including all three restricted-executor calls, all three ordinary-executor
calls, parser and concurrent-statement contracts, record-set detachment's
three outcomes, and the full no-delay result. `DrainRecordSet`,
`DrainRecordSetAndClose`, and `ExecSQL` retain Go's caller context, chunk-size
rules, close ordering, and error precedence.

`SimpleRecordSet` preserves Go's exported data, private cursor, field-count
iteration, allocator capacities, and close-to-restart behavior. Rust owns
materialized `Datum` rows after draining because a Rust chunk row borrows its
chunk; this retains the same values and order without extending an invalid
borrow across `Renew`. The shared result-field type is owned by
`rust/crates/tidb-resolve`, matching Go's package boundary. The three parser
parameters are owned by `tidb-parser`, matching Go's `parser.ParseParam`.

The ordinary consumers now use the shared owner instead of narrower Rust-only
execution paths:

- expression optional properties expose the real restricted SQL method set
  instead of an empty marker trait;
- metrics summary retrieval uses that same restricted executor rather than a
  local SQL-to-rows trait;
- timer table storage calls ordinary `ExecuteInternal`, receives a `RecordSet`,
  and drains it through `sqlexec::ExecSQL` rather than requiring executors to
  return pre-drained rows.

The concrete session implementation of these interfaces belongs to Go
`pkg/session` and remains a later package-level integration unit. This receipt
does not claim `pkg/session`, the generated mock subpackage, or repository-wide
parity. The subsequently completed resolve package is recorded independently
in `planner_core_resolve.md`.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 --
  pkg/util/sqlexec` — passed; the complete Go package matches the pin.
- `GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.10 go test
  -tags=intest,deadlock -count=1 ./pkg/util/sqlexec
  ./pkg/planner/core/resolve` — passed (`sqlexec` has no functional tests;
  `resolve` has no test files).
- `cargo check --offline -q -p tidb-sqlexec -p tidb-expr -p tidb-exec
  -p tidb-timer` — passed.
- `cargo test --offline -q -p tidb-sqlexec` — passed; no Rust-only functional
  package tests were added where Go has none.
- `cargo test --offline -q -p tidb-resolve` — passed; no package-local tests.
- `cargo test --offline -q -p tidb-exec --lib
  metrics_reader::tests::summary` — passed, 7 tests.
- `cargo test --offline -q -p tidb-expr --lib expropt::tests` — passed, 1
  test.
- `cargo test --offline -q -p tidb-timer --test all table_store_sql_test` —
  passed, 8 tests.
- scoped `cargo fmt` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the shared interfaces eliminate three narrower call contracts;
  all migrated consumers now preserve Go's context, options, arguments,
  record-set lifecycle, and returned field metadata.
- Compatibility: concrete session wiring is intentionally not inferred or
  claimed here; existing consumers compile against the common owner.
- Performance: draining retains Go's initial and renewed chunk capacities and
  the 1024-row `ExecSQL` batch size; owned drained rows are required by Rust's
  safe lifetime model.
