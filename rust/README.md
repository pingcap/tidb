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
| `crates/tidb-hack` | zero-copy byte views and allocation-aware hash maps | `pkg/util/hack` |
| `crates/tidb-util` | complete dependency-leaf utility packages | `pkg/util/{disjointset,zeropool,encrypt,checksum,intest,sqlescape,mathutil}`, followed by whole utility packages only |
| `crates/tidb-datatype` | SQL scalar representation, charset, and collation | `pkg/parser/charset`, `pkg/util/collate`, `pkg/types/**`, later extracted TiKV query datatypes |
| `crates/tidb-codec` | byte-compatible comparable scalar and datum-key encoding | dependency-closed paths in `pkg/util/codec/**` |
| `crates/tidb-txnkv` | complete KV contracts plus real PD/TiKV transaction runtime | `pkg/kv/**` and the required client-go transaction protocols |
| `crates/tidb-tablecodec` | table row/index formats above codecs and canonical KV handles | `pkg/tablecodec` |
| `crates/tidb-expr` | expression construction/evaluation | `pkg/expression/**` |
| `crates/tidb-exec` | seed session/catalog executor | `pkg/session/**`, `pkg/executor/**` |
| `difftests` | differential infrastructure | Go helpers and checked corpora |
| `difftests/parser-tests` | parser differential tests | lexer/parser/static Go oracle only |
| `difftests/planner-tests` | plan-ring source translations | source-backed planner primitive tests |
| `difftests/result-tests` | result-ring tests | expression, query, and table result parity |
| `difftests/transaction-tests` | transaction differential and live-cluster proofs | `pkg/kv/**`, RealTiKV, retry, lock, and fault-injection behavior |

The seed executor and evaluator are migration scaffolding, not a parity claim.
`tidb-txnkv` owns the Rust `pkg/kv` contracts and the existing PD/TiKV region,
RPC, MVCC, lock, retry, and optimistic-commit runtime. `tidb-codec` provides
comparable key encoding, and `tidb-tablecodec` combines those foundations
without redefining handle ownership. The dependency-leaf table-key framing
remains in `tidb-codec` because transaction diagnostics also decode those keys.
The package is accepted only after its complete source, tests, benchmarks,
support files, direct consumers, and live TiKV checks pass together.
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
