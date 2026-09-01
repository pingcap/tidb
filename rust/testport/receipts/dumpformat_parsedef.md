# `pkg/dumpformat/parsedef` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly two tracked artifacts and 50 lines. Both files
were read in full before editing, including the public BUILD dependency list
and the single production type/method. There is no `doc.go`, test file,
fixture/testdata directory, generated or platform-specific variant, fuzz or
benchmark input, or generator source. The parent `pkg/dumpformat/OWNERS` file
is repository governance metadata, not a Go package artifact.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `b2f2ba9546040d0cff1e157b1dc96a93309e947f` | `2a1a09a0ee01c81a554e82b2650660437a1ace5641025146451e73a4bc9f7a85` | public `parsedef` library target, depending on `types` and zapcore |
| `def.go` | 38 | `7d127c14d66e1a085bc01cef2ef2412a93206a0c` | `5dcbcfa0f64f6ad22c8dc35fbd0fe651f6ee0229a8876711baf6aba8b4162d1e` | `Row` data carrier and `MarshalLogArray` |

`def.go` declares one exported struct (`RowID`, `Row []types.Datum`, and
`Length`) and one method, `Row.MarshalLogArray`, which appends each Datum's
string representation to a zap `ArrayEncoder` and returns its error contract
(currently always nil). No other functions or branches exist.

## Rust ownership and parity decision

Rust's `tidb-exec::result::Row` is a `Vec<Datum>` execution result type, not a
drop-in for Go's importer row metadata (`RowID`/`Length`) or zapcore
`ArrayMarshaler`. The Go carrier is consumed by Lightning/dump-format parser
and logging code; no dependency-closed Rust dump-format or zap logging owner
exists. A new standalone Rust struct or logging facade would be uncalled
Rust-only behavior and would not satisfy the Go consumers, so no production
fix or ignored test-carrier change is made in this package.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
package compiles under the pinned Go toolchain:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/parsedef -count=1
# ? github.com/pingcap/tidb/pkg/dumpformat/parsedef [no test files]

git diff --check
```

No Go source, import section, test, Bazel target, or module dependency
changed; `make bazel_prepare` is not required. Rust tests and a full workspace
build were not run because this package has no Rust owner or changed Rust
source. Runtime correctness, compatibility, and performance risk are
unchanged; the receipt is an explicit ownership boundary, not a completed
Rust transcreation claim.
