# `pkg/util/dbutil` parity audit ExecPlan

## Objective

Keep the complete Go-master `pkg/util/dbutil` boundary current while deciding
whether its SQL, schema, retry, and tooling contracts can be owned by Rust as
one dependency-closed package.

## Progress

- [x] Read all seventeen Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 2,518 lines across the public
  utility and nested `dbutiltest` helper, including every production function,
  SQL-mock/table/index/retry/variable test, README, and Bazel target.
- [x] Confirm there are no package docs, fixtures/testdata, generated inputs or
  outputs, platform/build-tag variants, benchmarks, fuzz targets, or additional
  nested packages.
- [x] Compare the full boundary with Rust SQL parsing, privilege/SHOW GRANTS,
  statistics, table-mode, retry, and transport fragments. They do not form a
  dependency-closed owner for the Go `database/sql` interfaces, MySQL connector,
  exact SQL/error contracts, schema-comparison helper, and BR/Lightning/tooling
  call graph.
- [x] Keep the package explicitly unclaimed: no Rust-only behavior was found,
  no missing Go behavior is safe to implement in isolation, and no speculative
  facade or duplicate regression carrier was added.

## Validation gate

This is a docs-only Ready authority refresh. No Go, Rust, Bazel, or module file
changed, so `make bazel_prepare` is not required.

- [x] Current and exact detached Go-master package tests pass (16 tests;
  nested helper compiles with no test files).
- [x] Rust ownership search and boundary review complete.
- [x] Ready formatting and scoped diff checks pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Porting this package requires the complete BR/Lightning/TiDB-tooling SQL helper
surface, connector/session setup, schema and statistics consumers, retry/error
compatibility, and all live-database matrix regressions as one dependency-closed
change. Do not introduce a partial Rust adapter or Rust-only behavior here.
