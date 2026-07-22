# tidb-rs

Source-first Rust rewrite of TiDB's SQL layer. The complete strategy and
phasing are defined by
[`docs/design/2026-07-11-tidb-rust-rewrite.md`](../docs/design/2026-07-11-tidb-rust-rewrite.md).

This workspace builds a standalone SQL node. It never links Go and Rust in one
process: no cgo, no FFI migration seam. Cross-language compatibility is proved
through TiDB's serialized protocols and the differential rings.

## Start here

- [`HANDOFF.md`](HANDOFF.md): current architecture, completed packages, and
  largest gaps.
- [`PARALLEL.md`](PARALLEL.md): the retained link for the single-worker
  whole-package contract.
- [`docs/architecture/workspace.md`](docs/architecture/workspace.md): crate
  boundaries and target concurrency boundaries.
- [`docs/operations/validation.md`](docs/operations/validation.md): exact WIP
  validation commands.
- [`ports/`](ports/): the complete set of accepted whole-package proofs.
- [`workstreams/plans/2026-07-whole-package-transcreation.md`](workstreams/plans/2026-07-whole-package-transcreation.md): active execution plan.

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
several Rust crates; all implementation and original tests still finish under
one proof.

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

Inventory is not coverage. Only a valid whole-package proof under `ports/`
counts as completed; partial Rust code remains seed material.

Executable corpus namespaces use paired `<topic>.txt` and
`<topic>.golden.txt` files. Explanatory evidence belongs under
`difftests/corpus/coverage/`; the validation gate rejects prose or orphan
files in executable namespaces.

## Whole-package development loop

Transcreate one complete Go package, translate all of its original tests, then
finish it with one command from `rust/`:

```sh
scripts/package-port.py finish pkg/example \
  --crate tidb-example \
  --rust-path rust/crates/tidb-example/src/lib.rs \
  --rust-path rust/crates/tidb-example/tests/package_source.rs \
  --test-target tidb-example:package_source
```

This derives the complete Go source/test/support inventory, checks dependency
closure, and runs 12-job formatting, all-target Clippy, library tests, and the
declared integration tests before writing one proof. There is no start command,
claim, queue, campaign, transfer ledger, or separate receipt.

Before push or after shared-foundation changes, run the grouped workspace check:

```sh
scripts/package-port.py checkpoint
```

## Current structural priorities

- select dependency-ready Go packages with downstream runtime value;
- transcreate each selected package structurally from Go, including its full
  input domain and error behavior;
- translate every original test, benchmark, fuzz target, example, fixture,
  build variant, and support file before writing its proof;
- split a Go package across Rust crates when that improves cohesion or compile
  time, while keeping one package proof;
- run real PD/TiKV and MySQL-client checks when the package reaches those
  boundaries.

Progress and exact reviewed counts live only in [`HANDOFF.md`](HANDOFF.md), so
this entrypoint does not become another per-wave changelog.
