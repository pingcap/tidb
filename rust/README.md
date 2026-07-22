# tidb-rs

Source-first Rust rewrite of TiDB's SQL layer. The complete strategy and
phasing are defined by
[`docs/design/2026-07-11-tidb-rust-rewrite.md`](../docs/design/2026-07-11-tidb-rust-rewrite.md).

This workspace builds a standalone SQL node. It never links Go and Rust in one
process: no cgo, no FFI migration seam. Cross-language compatibility is proved
through TiDB's serialized protocols and the differential rings.

## Start here

- [`docs/architecture/workspace.md`](docs/architecture/workspace.md): crate
  boundaries and target concurrency boundaries.
- [`docs/operations/validation.md`](docs/operations/validation.md): exact WIP
  validation commands.

The Go implementation is authoritative. Work starts from one complete Go
package plus its original tests/support; differential output verifies the port
but does not replace reading or translating the source.

`Cargo.lock` is checked as part of this application workspace. Validation uses
`--locked` for reproducibility.

## Workspace

| Path | Ownership | Go source boundary |
| --- | --- | --- |
| `crates/tidb-lexer` | scanning and token classes | `pkg/parser/lexer.go`, parser token tables |
| `crates/tidb-ast` | typed AST and canonical restore | `pkg/parser/ast/**` |
| `crates/tidb-parser` | recursive-descent grammar | `pkg/parser/**_parser.go` |
| `crates/tidb-datatype` | SQL scalar representation | `pkg/types/**`, later extracted TiKV query datatypes |
| `crates/tidb-codec` | byte-compatible comparable scalar and datum-key encoding | dependency-closed paths in `pkg/util/codec/**` |
| `crates/tidb-txnkv` | source-backed KV key/range/version foundation; future TiKV transaction client | `pkg/kv/**`, then client-go transaction protocols |
| `crates/tidb-expr` | expression construction/evaluation | `pkg/expression/**` |
| `crates/tidb-exec` | seed session/catalog executor | `pkg/session/**`, `pkg/executor/**` |
| `difftests` | differential infrastructure | Go helpers and checked corpora |
| `difftests/parser-tests` | parser-ring tests and selector shards | lexer/parser/static Go oracle only |
| `difftests/planner-tests` | plan-ring source translations | source-backed planner primitive tests |
| `difftests/result-tests` | result-ring tests | expression, query, and table result parity |
| `difftests/transaction-tests` | transaction-ring source translations | `pkg/kv/**`, later real-TiKV/failure-injection evidence |

The seed executor and evaluator are migration scaffolding, not a parity claim.
`tidb-txnkv` currently delivers key/range/version plus source-backed
`Int`/`Common`/`Partition` handles and a portable handle map. `tidb-codec`
provides their real comparable key encoding dependency. Neither crate yet
contains RPC, MVCC, locks, retries, or commit protocols.
Unsupported behavior must fail before mutation. It must not be approximated
with a locally convenient rule just to make a golden pass.

## Unit of work

One complete Go package or module is the minimum unit. A package may map to
several Rust crates; all implementation and original tests still move together.

## Whole-package development loop

Transcreate one complete Go package and all original tests. During translation,
run only the focused owning test needed for immediate feedback and keep coding.
Commit and push meaningful green changes when useful; neither is an acceptance
operation. At the package boundary, run the original Go tests and every target
in the owning Rust crates.

Keep one primary Rust implementation/test module per original Go owner file.
Do not create files for individual syntax alternatives, bugs, waves, or test
rows. A Rust-native split is justified only by a stable dependency boundary,
not by partial completion.

At whole-package closure, run formatting, the Go package tests, the owning Rust
crate tests, and package-specific generators or differential checks. Compile
direct reverse dependencies when a public Rust API changed. For example:

```sh
cargo fmt --all -- --check
cargo test --offline --locked -j12 -p <owning-crate> --all-targets
```

Workspace-wide Clippy/tests, repository lint, and live-cluster checks run at
dependency-layer integration points and deployable milestones, not after every
leaf package. There are no status files, queues, campaigns, receipts, freeze
gates, or separate integration ceremonies; the source tree, tests, and Git are
the state.
