# `pkg/util/importer` — Go-master package boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has no
source delta from extraction pin `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All eight Go-master artifacts were read in full before deciding ownership:

| Artifact | Lines | Git blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 28 | `d8332383e10cc34bfd530ec119cb5d91f904017f` | `b83cc23f2bf6f7222f300b271015ca53af1d3fd28622280a7cf25a6e85b5feb1` | public library target and parser/database dependencies |
| `config.go` | 39 | `1bbaf5a9c35fb44d9c98fef3dfad5fbab0e6b661` | `246b6a8cd8aeda577cd5d78aadbb796d159aee99a2b213fdc5503a99b8e98cfc` | importer configuration struct and diagnostic formatter |
| `data.go` | 165 | `3cb5b304290e7a0b91de75a3573d62f49ae2bd92` | `e3d2a6c84c7b77541ca702b5940369c43d2b3dedb14670f65da79fd109b66496` | synchronized unique integer, string, time, date, timestamp, and year generators |
| `db.go` | 265 | `e5c9e6f4de44ab6bb7ef16d8a27aa37450feccc2` | `3f65d83c90e6c81088167d1231f4a442e22a31af920fd48805c645a9c6b4176e` | typed random/unique SQL value generation, row INSERT assembly, and MySQL DB lifecycle |
| `importer.go` | 52 | `5975327975a310bc69226a7976cf0fe8d6555e5c` | `54aed8dfc747d18eb77f608788c3608c98cad9e7dbf9b9261587a30bbd7742ea` | `DoProcess` orchestration: parse DDL, create workers, create table/index, generate jobs |
| `job.go` | 105 | `ae8c92da54d8d48044c7fb62613465015b78363b` | `0403d56726e3b254cca2d780645dd2fd1f2dd963366e9a278828d1b96acebefa` | buffered job production, transactional batch inserts, worker completion and TPS reporting |
| `parser.go` | 275 | `48e58985e98b0e24017b6d61392efea1f2bce715` | `7551a311ed52ac0ff95f5f1b662e89a2048653108b17ce11d70845b906391478` | CREATE TABLE/INDEX parser, column rules, unique/index maps, and SQL column-list construction |
| `rand.go` | 152 | `07eed3f184fbdaaf165d421f5656c6ad67fdf48d` | `2d88e6a0847a2fc111073d628a5cbeb7b3f6535abc06e5322e41d906fc6c936b` | seeded-global PRNG helpers for numeric, string, date/time, timestamp, year, duration, and bool values |

The package contains 1,081 Go lines (28 build + 1,053 source), 34
production functions/methods, and no `doc.go`, Go test file, fixture or
testdata tree, generated output, platform/build-tag variant, benchmark,
fuzz target, or nested package.

## Go behavior and consumers

This is a standalone utility used by the `cmd/importer` data-generation
program. It parses a caller-supplied CREATE TABLE and optional CREATE INDEX,
interprets column comments such as `[[range=1,10;step=1]]`, tracks unique
columns, emits type-specific SQL literals, and inserts generated rows through
worker-owned MySQL transactions. The package intentionally uses `log.Fatal`
for malformed generator rules and setup failures, and its random/date
helpers preserve the source's inclusive/exclusive range and formatting
contracts.

## Rust ownership and decision

Rust has SQL `IMPORT INTO` AST/parser support and BR restore/SST machinery,
but those are separate server/import protocols. No Rust crate exposes the
dependency-closed `Config`/table parser/typed SQL literal generator/MySQL
worker orchestration owned by this package, and no Rust command consumes this
library's generated INSERT text. Reusing BR's importer or the SQL import
executor would silently change the standalone command's behavior. No
Rust-only behavior was found and no safe missing Go behavior can be added
without first porting the command and its parser/SQL-driver composition root.
This complete package is therefore explicitly unclaimed; no production Rust
change or focused regression test was added in this boundary batch.

## Validation

Profile: WIP for the continuing repository audit; no source or build artifact
changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/importer -count=1` — passed (`[no test files]`).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/importer` — empty; source is unchanged at Go master.
- Rust search across parser, executor, BR, and command crates found only independent SQL-import/restore paths and no dependency-closed owner.

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
separate `cmd/importer` package, its integration-oriented `db_test.go`, and
runtime MySQL import exercise are outside this package inventory and remain
to be audited as their own command boundary.

## Risks and unverified scope

- Correctness: generator behavior depends on parser AST field types, MySQL
  driver semantics, and the exact malformed-range/PRNG behavior in the source.
- Compatibility: preserve SQL literal quoting, unique-value progression,
  worker/batch accounting, and fatal-error policy if this command is ported.
- Performance: no runtime path changed.
- Not verified locally: `cmd/importer` end-to-end execution against MySQL,
  malformed DDL/rule matrices, and concurrent worker failure handling.
