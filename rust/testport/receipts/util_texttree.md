# `pkg/util/texttree` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete inventory

The package has exactly four Go-master artifacts and 174 lines, all read in
full:

- `texttree.go`: five exported tree runes plus `Indent4Child` and
  `PrettyIdentifier`.
- `texttree_test.go`: `TestPrettyIdentifier` and `TestIndent4Child`.
- `main_test.go`: the common Go test setup and Go-runtime goroutine-leak
  harness.
- `BUILD.bazel`: one library and one short, race-enabled test target.

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 24 | `72b9a3ec23d8884c942ce451237698ce78a7df93` | `141c6579b9690212387b2bc00f566206196a8d8872bcf6040f1d790a27305531` |
| `main_test.go` | 33 | `d30e6de9248613b9fe874896072199d80fd6ec54` | `a93a0ab98f6e98ffbafa13fa0601c7728cd1db9ef2155d0c63fdb91ce090117c` |
| `texttree.go` | 81 | `13aa7e04084671a35bd3e64f7c420579652ae5ff` | `4a4710c5308f82d91d0d3e064d0570dbbf15de208a88f8719cefa9e708d5c251` |
| `texttree_test.go` | 36 | `019123142513f417fc08c4e3faddd2b107fb9968` | `957f94023050ffc28f9a8d599e2c64efbdac1f5a9ca36200ae9e0d35cd268197` |

There is no package doc, benchmark, fixture, generated source, platform
variant, README, or ownership file. `main_test.go` controls only the Go test
process; Cargo owns the Rust test process, so it has no production or
test-behavior port.

## Rust ownership and audit result

`rust/crates/tidb-util/src/texttree.rs` owns the complete package. It uses
`tidb_datatype::GoString` because Go strings may contain arbitrary bytes. As
in Go, indentation is iterated as Unicode code points, consuming each invalid
UTF-8 byte as one replacement character; `PrettyIdentifier` then appends the
identifier's original bytes without decoding them.

The audit removed the previous valid-UTF-8-only `&str`/`String` narrowing,
duplicate supplemental test groups, Rust-only `must_use` diagnostics, and the
remaining arbitrary-byte supplemental regression. Exactly the two Go test
identities remain. The ordinary consumers in `tidb-util::plancodec` and
`tidb-planner::explain` convert the result to `String` only at their
source-guaranteed valid-UTF-8 plan metadata boundary.

## Validation

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No source behavior changed and no new
regression test is added; both exact source-derived tests remain the focused
regressions.

```text
git diff --exit-code 0bc44483e3e41a8ea917d4382dc202369468d200..origin/master \
  -- pkg/util/texttree
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/texttree -count=1
# passed (current worktree and exact detached Go-master worktree; two tests)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib texttree::tests --offline --locked -- --test-threads=1
# passed: two source-derived tests

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-planner --lib --offline --locked
# passed: planner/explain consumer (workspace warnings only)

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

No Go or Bazel file changed, so `make bazel_prepare` is not required. Full
workspace tests and Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: the full source surface and both consumers are covered by the
  pinned implementation and source tests.
- Compatibility: the public Rust return type is now byte-preserving
  `GoString`; both existing valid-UTF-8 production consumers are adapted.
- Performance: valid input still performs one rune pass and one output
  allocation, matching the source algorithm's shape.
