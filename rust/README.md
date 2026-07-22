# tidb-rs

Source-first Rust rewrite of TiDB's SQL layer. The complete strategy and
phasing are defined by
[`docs/design/2026-07-11-tidb-rust-rewrite.md`](../docs/design/2026-07-11-tidb-rust-rewrite.md).

This workspace builds a standalone SQL node. It never links Go and Rust in one
process: no cgo, no FFI migration seam. Cross-language compatibility is proved
through TiDB's serialized protocols and the differential rings.

## Start here

- [`HANDOFF.md`](HANDOFF.md): current architecture, completed packages, and
  active ExecPlan.
- [`docs/architecture/workspace.md`](docs/architecture/workspace.md): crate
  boundaries and target concurrency boundaries.
- [`docs/operations/validation.md`](docs/operations/validation.md): exact WIP
  validation commands.
- [`ported-packages.json`](ported-packages.json): compact current package set.

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

## Evidence model

The rewrite has four independent rings:

- parser: parse/restore/error parity against the checked Go oracle;
- plan: `EXPLAIN` and plan-digest parity;
- result: SQL result and error parity against Go TiDB and, where useful,
  MySQL;
- transaction: client-go-compatible failure injection plus real-TiKV and
  Jepsen evidence.

The result ring's expression corpus includes source-owned scalar vectors for
`CONCAT` and `CONCAT_WS`; their paired golden output and coverage fragment
preserve NULL, separator, numeric, empty-field, and raw-binary behavior while
keeping unsupported temporal/error/session boundaries visible.

Inventory is not coverage. Only a current whole-package record backed by the
source audit and owning-crate tests counts; partial Rust code remains seed
material.

Executable corpus namespaces use paired `<topic>.txt` and
`<topic>.golden.txt` files. Explanatory evidence belongs under
`difftests/corpus/coverage/`; the validation gate rejects prose or orphan
files in executable namespaces.

## Whole-package development loop

Transcreate one complete Go package, translate all original tests, then record
it with one command from `rust/`:

```sh
scripts/port.py record pkg/example -p tidb-example
```

This derives the Go digest and dependencies and runs every target in the owning
crate. The manifest stores only the digest and crate names. There is no start
command, claim, queue, campaign, gate, transfer ledger, or receipt.

Before push, run the ordinary repository validation commands:

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
cargo test --offline --locked -j12 --workspace --all-targets
```

## Current structural priorities

- select dependency-ready Go packages with downstream runtime value;
- transcreate each selected package structurally from Go, including its full
  input domain and error behavior;
- translate every original test, benchmark, fuzz target, example, fixture,
  build variant, and support file before recording the package;
- split a Go package across Rust crates when that improves cohesion or compile
  time, while keeping one whole-package acceptance boundary;
- run real PD/TiKV and MySQL-client checks when the package reaches those
  boundaries.

Progress and exact reviewed counts live only in [`HANDOFF.md`](HANDOFF.md), so
this entrypoint does not become another per-wave changelog.
