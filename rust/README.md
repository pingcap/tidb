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
| `crates/tidb-datatype` | SQL scalar representation, charset, and collation | `pkg/parser/charset`, `pkg/util/collate`, `pkg/types/**`, later extracted TiKV query datatypes |
| `crates/tidb-codec` | byte-compatible comparable scalar and datum-key encoding | dependency-closed paths in `pkg/util/codec/**` |
| `crates/tidb-txnkv` | source-backed KV key/range/version foundation; future TiKV transaction client | `pkg/kv/**`, then client-go transaction protocols |
| `crates/tidb-expr` | expression construction/evaluation | `pkg/expression/**` |
| `crates/tidb-exec` | seed session/catalog executor | `pkg/session/**`, `pkg/executor/**` |
| `difftests` | differential infrastructure | Go helpers and checked corpora |
| `difftests/parser-tests` | parser differential tests | lexer/parser/static Go oracle only |
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

A dependency-closed batch is the unit of work. It should contain as many related
Go packages as can move coherently. Every included package is still indivisible:
all implementation, original tests, and support artifacts move together, even
when the package maps to several Rust crates.

## Development loop

Use the single three-step loop in the design document. There is no separate
Rust workflow or tracking mechanism.
