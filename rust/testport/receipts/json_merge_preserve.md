# JSON_MERGE_PRESERVE grouping parity receipt

Status: bounded Rust parity fix implemented in the isolated worktree. Go
`origin/master` is the source oracle; freshly built Go binaries cannot run on
this host, so the expected shape is derived from the source implementation and
the existing Go-captured JSON operation corpus.

Comparison source: Go `origin/master` at
`6331b8787b4203a91aafe49ee1dc801ee497bf98`.

Final batch commit: `71ffce262e` (pushed to `hparser-integration`).

## Inventory completed before editing

The complete JSON owners were enumerated before editing, including production,
unit/benchmark/fuzz tests, fixtures, generated/platform data, and build
artifacts:

| Tree | Files | Go/Rust lines |
| --- | ---: | ---: |
| `pkg/types` | 60 | 28,545 Go lines |
| `rust/crates/tidb-datatype` | 104 | 51,114 Rust lines |

The behavior-bearing Go artifacts read before editing were
`json_binary.go`, `json_binary_functions.go`, `json_constants.go`,
`json_binary_functions_test.go`, and `json_binary_test.go`, with the complete
package inventory above. Rust owners read were `binary_json.rs`,
`binary_json_ops.rs`, the JSON operation fixture/test, and all dependent JSON
conversion/stringification tests.

## Go behavior restored

Go's `MergeBinaryJSON` first copies non-object arguments into a result list and
groups each *adjacent run* of object arguments. `mergeBinaryObject` recursively
merges duplicate keys within a run, then `mergeBinaryArray` flattens one array
layer across the result list. A non-object between two objects therefore keeps
the later objects in their own merge run.

Rust previously left-folded `merge_preserve_node`, so after `[1]` changed the
accumulator into an array, later objects were appended separately. The new
`merge_binary_nodes`/`merge_binary_objects` helpers mirror Go's run grouping,
recursive duplicate-key merge, bytewise key ordering, and one-level array
flattening without changing binary storage.

## Focused regression

`binary_json_ops::tests::test_binary_json_merge` now includes
`JSON_MERGE_PRESERVE('[1]', '{"a":1}', '{"a":2}')` and asserts
`[1,{"a":[1,2]}]`, the Go result. The pre-fix left fold produced
`[1,{"a":1},{"a":2}]`; the existing object/array merge rows remain covered.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-datatype --lib binary_json_ops::tests::test_binary_json_merge -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype --all-targets -- -D warnings
```

The focused merge regression and full serialized datatype owner profile pass
(384 unit tests plus 63 generated/integration tests). Owner compilation,
formatting, and diff checks pass. Strict clippy remains blocked by the
pre-existing `tidb-mysql/src/consts.rs:117-120`
`clippy::map-or-identity` diagnostics; no diagnostic points at this batch's
files.

## Risks and remaining boundaries

- The implementation clones intermediate `JSONNode` values to keep the public
  fallible API simple; it changes no persisted encoding and remains bounded by
  the input document size.
- JSON invalid-byte/surrogate rendering remains an explicit follow-up in
  `docs/json-binary-divergence-audit.md`.
- The Go executable oracle was unavailable; the expected grouping and output
  are anchored to `MergeBinaryJSON`'s source branches and captured fixtures.
