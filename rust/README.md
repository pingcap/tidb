# tidb-rs

Source-first Rust rewrite of TiDB's SQL layer. The strategy is defined by
[`docs/design/2026-07-11-tidb-rust-rewrite.md`](../docs/design/2026-07-11-tidb-rust-rewrite.md).

This workspace builds a standalone SQL node (`crates/tidb-server`). It never
links Go and Rust in one process: no cgo, no FFI migration seam.
Cross-language compatibility is proved through TiDB's serialized protocols and
the differential tests.

## Rules

The Go implementation under `pkg/` is authoritative. A port starts from one
complete Go package plus its original tests and support files; differential
output verifies the port but does not replace reading the source.
Unsupported behavior fails before mutation; it is never approximated with a
locally convenient rule to make a golden pass.

`Cargo.lock` is checked in. Validation uses `--locked`.

## Layout

| Path | Contents |
| --- | --- |
| `crates/tidb-*` | One crate per Go package, named after its path (`pkg/planner/core/operator/physicalop` is `tidb-planner-core-operator-physicalop`); a Go package may span several crates when that gives a cleaner dependency graph |
| `third_party/tikv-client-rs` | The client-go counterpart the store and transaction crates depend on, as Go TiDB depends on `github.com/tikv/client-go/v2` |
| `difftests` | Differential infrastructure: lexer/parser rings, Go helpers (`gorun`, `goeval`, `godump`), checked corpora, result and transaction fixtures |
| `scripts` | Real-TiKV runners, benchmarks and generators (`scripts/README.md`) |
| `docs/parity/current-audit/structural-findings.{md,json}` | The unresolved structural mismatches with Go; a fixed finding is removed |
| `docs/system-table-sql-execplan.md` | The active ExecPlan |

## Validation

```sh
cargo build --locked -p tidb-server
cargo nextest run -p <crate>
cargo test -p tidb-server --lib integration_corpus_matches_recordings -- --ignored
```

The last command replays the integration corpus against its recordings; it is
a milestone check, not a per-commit one.
