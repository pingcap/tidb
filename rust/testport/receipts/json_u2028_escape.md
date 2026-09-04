# JSON U+2028/U+2029 text-escaping parity receipt

Status: bounded Rust parity fix implemented and pushed from the isolated
worktree. Go remains the source oracle; this machine cannot launch a freshly
built Go server, so the expected bytes are taken directly from
`pkg/types/json_binary.go`'s `jsonMarshalStringTo` implementation.

Final batch commit: `929dc85d5c` (pushed to `hparser-integration`).

Comparison source: Go `origin/master` at
`6331b8787b4203a91aafe49ee1dc801ee497bf98`.

## Inventory completed before editing

The package owners were enumerated before editing, including production files,
tests, fixtures, generated/platform data, benchmarks, fuzz targets, and build
artifacts:

| Tree | Files | Go/Rust lines |
| --- | ---: | ---: |
| `pkg/types` (Go source/tests/build artifacts) | 60 | 28,545 Go lines |
| `rust/crates/tidb-datatype` (Cargo/source/tests/fixtures/benches/fuzz/data) | 104 | 51,114 Rust lines |

Reproducible inventory commands:

```text
find pkg/types -type f | sort
find pkg/types -type f -name '*.go' -print0 | xargs -0 wc -l
find rust/crates/tidb-datatype -type f | sort
find rust/crates/tidb-datatype -type f \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'build.rs' \) -print0 | xargs -0 wc -l
```

The behavior-bearing Go artifacts were read before editing:
`json_constants.go`, `json_binary.go` (including `jsonMarshalStringTo`, scalar
and object marshal paths), `json_binary_functions.go`, and their complete JSON
tests. Rust owners read before editing include `binary_json.rs`,
`binary_json_ops.rs`, JSON conversion/stringification modules, and the JSON
source/fixture tests.

## Go behavior restored

Go escapes LINE SEPARATOR (U+2028) and PARAGRAPH SEPARATOR (U+2029) as the
ASCII sequences `\\u2028` and `\\u2029` in every JSON text rendering. This is
independent of storage: the binary JSON payload still contains the original
UTF-8 string. Rust previously delegated all quoting to `serde_json`, which
leaves both separators raw.

`marshal_json_string` now keeps `serde_json`'s existing escaping for every
other character and replaces only these two separators. `BinaryJSON` scalar
values, object keys in both value/node formatters, and `quote_json_string`
share the helper, so nested documents and path keys have the same safety rule.

## Focused regression

`tidb_datatype::binary_json::tests::json_text_escapes_line_and_paragraph_separators_like_go`
parses a scalar and an object containing U+2028/U+2029 and asserts the exact Go
escaped text. The existing `test_quote_string` corpus also pins the helper's
ordinary quoting behavior and the two separator escapes.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-datatype --lib binary_json::tests::json_text_escapes_line_and_paragraph_separators_like_go -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --lib binary_json::tests::test_quote_string -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype --all-targets -- -D warnings
```

The two focused tests and the full serialized owner profile pass (383 unit
tests plus 63 generated/integration tests). Compilation, formatting, and diff
checks pass. Strict clippy remains blocked by the pre-existing
`tidb-mysql/src/consts.rs:117-120` `clippy::map-or-identity` diagnostics; no
diagnostic points at this batch's files.

## Risks and remaining boundaries

- This changes text rendering only; binary JSON storage and comparisons are
  untouched.
- The separate JSON merge-preserve grouping and invalid-UTF-8/surrogate
  boundaries remain explicit follow-ups in
  `docs/json-binary-divergence-audit.md`.
- The Go executable oracle was unavailable in this environment; the expected
  byte spelling is anchored to the source implementation and existing source
  fixtures.
