# `pkg/dumpformat/testutils` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly two tracked artifacts and 296 lines. Both files
were read in full before editing, including the public BUILD target and the
complete Parquet test-file writer. There is no `doc.go`, package-local test,
fixture/testdata directory, generated or platform-specific variant, fuzz or
benchmark input, or generator source.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 17 | `473468527c08015b76e165ea002c7d6284b74534` | `aabaa86209d06803deaca014b89c52c4f92fda062296c76e5dfe41bc03c8a242` | public `testutils` library and Arrow/objstore dependencies |
| `parquet_writer.go` | 279 | `aa756f5411a2bd8f86eb6467b0c0057a8e647507` | `0c1871bdd97ca6b9789e2296359f1a2959e28540cf2c135985f6af3677413d68` | test-only Parquet schema, column slicing, object-store wrapper, and writer |

`parquet_writer.go` was audited function by function: definition-level range
calculation, typed column-batch dispatch, value slicing, `ParquetColumn`, the
seek/read/write/close object-store wrapper, backend setup, and the complete
`WriteParquetFile` row-group loop. The helper supports optional columns,
dictionary/snappy properties, row-group splitting, and eight Arrow primitive
writer types (including the Float32 case added on Go master). It is consumed by
Go Parquet/importer tests outside this package; those callers and their binary
fixtures remain separate package claims.

## Rust ownership and parity decision

The Rust workspace has no Arrow/Parquet test writer, dump-format testutils
crate, or dependency-closed object-store writer owner. Adding a detached Rust
fixture generator would be uncalled Rust-only behavior and would not exercise
the Go consumers. No Rust-only behavior was found to remove, and no
speculative implementation or ignored carrier was added.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
package compiles under the pinned Go toolchain:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/testutils -count=1
# ? github.com/pingcap/tidb/pkg/dumpformat/testutils [no test files]

git diff --check
```

No Go source, import section, test, Bazel target, or module dependency
changed; `make bazel_prepare` is not required. Rust tests and a full workspace
build were not run because this package has no Rust owner or changed Rust
source. Runtime correctness, compatibility, and performance risk are
unchanged; Parquet fixture compatibility is explicitly deferred to the
dependency-closed `pkg/dumpformat/parquetfile` claim.
