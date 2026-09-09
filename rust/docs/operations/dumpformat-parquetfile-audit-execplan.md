# `pkg/dumpformat/parquetfile` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master production, test, generated, fixture, and build
artifact in `pkg/dumpformat/parquetfile`, compare the complete Parquet reader/
writer contract with Rust owners, and avoid a fabricated native implementation
without Arrow, object-store, and Spark dependency closure.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all 20 tracked artifacts: 16 production/generated
  files and 9,567 Go text lines, seven test/benchmark files containing 29
  tests and three benchmarks, plus the Aurora and Hive binary fixtures. Read
  the generated Spark table header/provenance and verified no generator input
  is checked in.
- [x] (2026-09-01) Trace all production functions, nested test cases, fixture
  consumers, BUILD dependencies, failpoint hook, preload/streaming modes,
  decimal/time conversion, and Spark rebase behavior. No dependency-closed
  Rust Parquet owner exists.
- [x] (2026-09-01) Run the complete failpoint-enabled Go-master package suite;
  it passed. Run format, lint, and diff hygiene gates for the receipt batch.
- [ ] Fetch immediately before staging, create one meaningful receipt batch,
  push it to `origin/hparser-integration`, and verify remote SHAs.
- [ ] Continue the rolling audit with the next unrecorded package outside the
  dump-format family.

## Scope and decision

This is a package-atomic Parquet import/export unit. Its contract includes
Arrow schema construction, SQL decimal/timestamp/INT96 conversion, nullable
definition levels, cloud object-store range reads and preload thresholds,
Spark's generated timezone tables, allocator accounting, writer memory limits,
and binary Aurora/Hive fixtures. Rust has none of the required dependencies or
call sites. Keep the boundary explicit; do not create a detached crate or
copy generated data without a dependency-closed consumer and source generator.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dumpformat/parquetfile -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The Go suite passes and the repository gates are clean. No Rust or Bazel source
was changed, so no Rust test target or `make bazel_prepare` gate applies.

## Outcome

The complete inventory and explicit owner decision are recorded in
`rust/testport/receipts/dumpformat_parquetfile.md`; the rolling audit continues
with the next unrecorded package.
