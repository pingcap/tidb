# `pkg/util/tableutil` — Go-master parity boundary receipt

Status: complete inventory; no dependency-closed Rust owner for the Go
interface/global-factory package is claimed.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package has exactly two tracked artifacts, both read in full:

- `tableutil.go` (46 lines): the `TempTable` interface with eight methods
  covering auto-ID allocation, modified state, stats, size, and metadata, plus
  the process-global `TempTableFromMeta` factory variable.
- `BUILD.bazel` (12 lines): one public library target depending on
  `pkg/meta/autoid` and `pkg/meta/model`.

There is no `doc.go`, source test, `main_test.go`, benchmark, fixture,
generated/platform variant, nested package, or other build artifact. The
current production and Bazel files are unchanged from the pinned audit.

## Rust ownership and boundary

Rust has real temporary-table behavior split across `tidb-model::TempTableType`,
`tidb-executor::KvTable`, `tidb-session`'s local/global temporary-table state,
transaction commit/rollback handling, and DDL validation. Those owners do not
implement one dependency-closed `TempTable` trait with Go's auto-ID, mutable
stats, size, and metadata object contract, nor do they expose a process-global
`TempTableFromMeta` factory.

The Go consumers are cross-package and ordinary: `pkg/table/tables` installs
the factory during package initialization, `pkg/sessionctx/variable` creates
per-session overlays, and `pkg/sessiontxn/isolation` filters temporary-table
keys. Adding a detached Rust trait or factory would duplicate the existing
session/transaction path and would not satisfy those consumers. The source
package therefore remains an explicit integration boundary; no Rust
production or supplemental test file changed.

## Validation

Profile: WIP boundary audit in the continuing package loop; no Ready fix claim
applies.

- `git ls-tree -r --name-only origin/master -- pkg/util/tableutil` and full
  reads — passed; confirmed both artifacts and the absence of source tests,
  fixtures, and variants.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/tableutil -count=1` — passed (`[no test files]`).
- `git diff --check` — passed for this audit.

No Go, Bazel, or Rust runtime file changed, so `make bazel_prepare` and Rust
tests are not required. Temporary-table SQL behavior is covered by the
existing session/executor owners, but the package-level factory contract is
not executable in Rust.

## Risks and unverified scope

Correctness and compatibility risk is concentrated in the missing shared
object seam: auto-ID allocator cloning, per-session stats/size state, and
factory initialization must be made one atomic package unit before claiming
parity. Performance is unchanged because no new trait dispatch or global
factory was introduced. Cross-package temporary-table integration remains
unverified as a Go-to-Rust package transcreation.
