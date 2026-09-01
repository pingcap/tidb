# `pkg/util/ddl-checker` — Go-master parity boundary receipt

Go authority: `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package contains exactly four artifacts, all read in full (351 lines
total):

- `ddl_syncer.go` (68 lines): upstream `SHOW CREATE TABLE`, local drop/recreate,
  and close-order lifecycle;
- `executable_checker.go` (164 lines): mockstore/session executor, parser,
  table-existence checks, DDL classification, and idempotent close guard;
- `executable_checker_test.go` (84 lines): two source tests (`TestParse` and
  `TestExecute`) with the 12-case DDL/DML matrix;
- `BUILD.bazel` (35 lines): library and flaky test target with mock/session
  dependencies.

There is no `doc.go`, `main_test.go`, fixture, testdata, benchmark, fuzz
target, example, generated/platform variant, nested package, or other build
input. All four files are byte-identical to Go master. The source tests require
the repository's `intest` build tag.

## Rust ownership decision

Rust has DDL planning, parser, schema, mock-session, and table-lifecycle
pieces, but no dependency-closed equivalent of this package's mock TiDB SQL
executor, session charset/collation parser, AST table-existence classifier,
upstream `database/sql` synchronizer, or close-error lifecycle. Adding a
checker-only session or a second upstream syncer would be Rust-only behavior
and would not satisfy the Go package contract. The package remains explicitly
unclaimed; no production Rust edit or test-only replacement was justified.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/util/ddl-checker -count=1 -v` — passed (`TestParse`, `TestExecute`).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/util/ddl-checker -count=1 -v)` — passed (`TestParse`, `TestExecute`).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed in the clean detached Go-master checkout; the active checkout may be temporarily instrumented by a concurrent failpoint test worker.
- `git diff --check` — passed for the documentation diff.

This batch changes documentation only; no Go source, import section, test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The package has no failpoint use; the `intest` tag is the
canonical test prerequisite.

## Risks and unverified scope

- Correctness: parser/session and 12-case source matrix pass in both Go
  checkouts, but no Rust checker exists to validate.
- Compatibility: this is Go test/tooling infrastructure; future porting must
  include parser, session, mockstore, AST, and upstream DB dependencies as one
  package unit.
- Performance: no runtime code changed; Go's intended mockstore-per-checker
  test cost is unchanged.
- Not verified locally: Bazel's flaky target, every backend, live upstream
  MySQL synchronization, and a Rust equivalent of the checker harness.
