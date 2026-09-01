# `pkg/dumpformat/parquetfile` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master Parquet production/test/generated/fixture artifact,
validate the full package, compare all behavior with Rust owners, and keep
the package-atomic boundary explicit when no Rust Arrow/Parquet importer
exists.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all 20 artifacts (9,586 lines plus two binary fixture
  payloads), including the 29-shard BUILD target, 126 production functions,
  32 test/benchmark declarations, 13 test helpers, generated Spark rebase
  data, and both Parquet fixtures.
- [x] (2026-09-01) Compare Go-master logical/schema/reader/writer/rebase and
  memory behavior with Rust's parser and datatype leaves; no dependency-closed
  owner exists and no safe Rust fix is identified.
- [x] (2026-09-01) Run the exact detached Go-master failpoint suite; it passes
  in 1.261s and failpoints are disabled during teardown.
- [ ] Fetch immediately before staging, create one meaningful receipt batch,
  push it to `origin/hparser-integration`, and verify remote SHAs.
- [ ] Continue the rolling audit with the next unrecorded top-level package.

## Scope and decision

This package is a complete Arrow/Parquet SQL import/export implementation with
cross-package object-store, Lightning, parser, type, and memory dependencies.
The checked-in Spark table is generated input and must not be hand-edited; the
two binary files are real parser fixtures. Rust's isolated Parquet syntax and
decimal helper do not provide a dependency-closed implementation. Do not add a
detached Rust reader/writer or claim parity from a fixture-only port.

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

The detached Go suite passes. No Rust or Go/Bazel source changed, so no Rust
test or `make bazel_prepare` gate is applicable to this documentation-only
boundary. Generated-table regeneration and full workspace tests remain
unverified.

## Outcome

The complete inventory, hashes, fixture metadata, source validation, and Rust
ownership decision are recorded in
`rust/testport/receipts/dumpformat_parquetfile.md`. The rolling audit now
continues with the next unrecorded top-level package.
