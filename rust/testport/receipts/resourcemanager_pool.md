# `pkg/resourcemanager/pool` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `basepool.go`

## Rust ownership and integration

- `tidb-resourcemanager::pool` owns the three pool errors with their exact
  source strings and the shared `BasePool` state.
- A new base pool records the current time, has an empty name, and starts its
  task generator at zero. Name and last-tune setters replace their values;
  task IDs use the source atomic one-based wrapping increment.
- Rust locks provide the native equivalent of Go's atomic time and ordinary
  name field without adding policy or lifecycle behavior.
- The pinned package has no Go tests or support artifacts, so no Rust-only test
  carrier was added. Its concrete `spool` and `workerpool` consumers remain
  separate package units.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo check --quiet --offline -p tidb-resourcemanager
cargo test --quiet --offline -p tidb-resourcemanager --lib
```

Results: formatting, the package check, and the existing two dependent
resource-manager source tests passed. Cargo emitted only the existing
`tikv-client` private-bound warning.

Not run: concrete pool package tests, which belong to their later package
units; workspace-wide tests; or the Ready-profile `make lint` gate.
