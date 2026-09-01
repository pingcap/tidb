# `pkg/util/ddl-checker` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
This utility runs parsed DDL against a mock TiDB session and can synchronize a
table definition from an upstream SQL connection.

## Complete inventory

All four Go-master artifacts were read in full before the ownership decision.
There is no package documentation, generated output, platform variant,
fixture, benchmark, fuzz target, or nested package.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 35 | `489608bcb776cc0e9f43b42e542f3447e8913ebc` | `8482f816fb4244145b994917c2c9890ce85a51b4d907abb2f68a02286544aa19` | library plus flaky short test target and mock/session dependencies inventoried |
| `ddl_syncer.go` | 68 | `f94a1451cdcdbd05685b540e333d16f5112feb29` | `9d7b29ba67f076dc55a86ff077d134cd8326ca5dc205498207d7f614fc4909c6` | upstream DB open, CREATE TABLE fetch, drop/recreate, and close lifecycle inventoried |
| `executable_checker.go` | 164 | `7eda964c4e5ec0ef9760602beae41da6c0912dae` | `5823996165384bb8227755daaadbac1ab0842a7645064b98f88f2192f4e5f3b3` | mockstore/session executor, parser, DDL table-existence classification, and close guard inventoried |
| `executable_checker_test.go` | 84 | `430f9117ef5700d79d8c2dde95e8ce1416c1593d` | `416445607398dcf152167981fd7596e67fca428660b0b318c1a39dd834a55648` | source parse/execute matrix and mock-store setup inventoried |

The package has 12 named production functions/methods, one `ExecutableChecker`
and one `DDLSyncer` carrier, and the `parseTestData` test matrix with 12
DDL/DML cases. The
checker initializes a full Go mockstore session, executes SQL, parses one
statement with session charset/collation, classifies table names required to
exist or not exist for each supported DDL AST type, and rejects non-DDL input.
It uses an atomic close guard and reports a repeated close as an error. The
syncer obtains upstream `SHOW CREATE TABLE`, drops the local table, recreates
it, and closes both resources while preserving the first close error.

## Rust ownership and integration decision

Rust has DDL planning, schema-state, mock-session, and table-lifecycle tests,
but no dependency-closed equivalent of this utility's mock TiDB SQL executor,
AST table-existence classifier, upstream `database/sql` syncer, or flaky
source test harness. The existing native DDL owners are ordinary execution
paths; adding a checker-only session or a second upstream syncer would be
Rust-only behavior and would not satisfy the Go package's parser/session
contract. The package remains explicitly unclaimed; no source change is
justified.

## Validation

Profile: **WIP**. This is a complete four-artifact inventory and explicit
boundary audit with no code change, so `make bazel_prepare` and the Ready lint
gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/ddl-checker -count=1
# expected guard failure: TestExecute requires --tags=intest

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest ./pkg/util/ddl-checker -count=1
# ok
```

## Risks and unverified behavior

- Correctness: DDL AST classification, session charset/collation, SQL error
  propagation, close ordering, and upstream CREATE TABLE synchronization stay
  Go-owned contracts.
- Compatibility: this helper is test/tooling infrastructure rather than the
  Rust DDL runtime; a future port must include parser, session, mockstore, and
  upstream DB dependencies together.
- Performance: no runtime code changed; the Go checker creates a mockstore and
  session per checker as its intended test cost.
- Not verified locally: Bazel's flaky target, all 12 source cases under every
  storage backend, live upstream MySQL synchronization, and a Rust equivalent
  of the source test harness. The untagged Go run intentionally fails its
  repository test guard; the tagged command is the canonical passing result.
