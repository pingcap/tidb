# tidb-rs

Source-first Rust rewrite of TiDB's SQL layer. The complete strategy and
phasing are defined by
[`docs/design/2026-07-11-tidb-rust-rewrite.md`](../docs/design/2026-07-11-tidb-rust-rewrite.md).

This workspace builds a standalone SQL node. It never links Go and Rust in one
process: no cgo, no FFI migration seam. Cross-language compatibility is proved
through TiDB's serialized protocols and the differential rings.

## Start here

- [`HANDOFF.md`](HANDOFF.md): reviewed current counters, active gaps, and
  environment constraints.
- [`PARALLEL.md`](PARALLEL.md): agent ownership, protected integration seams,
  and landing protocol.
- [`docs/architecture/workspace.md`](docs/architecture/workspace.md): crate
  boundaries and target concurrency boundaries.
- [`docs/operations/validation.md`](docs/operations/validation.md): exact WIP
  validation commands.
- [`difftests/PORTING_LEDGER.md`](difftests/PORTING_LEDGER.md): every upstream
  Go test and fixture remains an explicit obligation.
- [`difftests/SOURCE_LEDGER.md`](difftests/SOURCE_LEDGER.md): every production
  Go source file remains an explicit crate-routing and porting obligation.
- [`execplans/`](execplans): living plans for multi-crate and structural work.

The Go implementation is authoritative. A Rust feature starts from a bounded
Go source domain plus its original tests; differential output verifies the
port but does not replace reading or translating the source.

`Cargo.lock` is checked as part of this application workspace. Validation uses
`--locked` so parallel agents cannot silently resolve different dependencies.

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
| `difftests` | shared evidence infrastructure | Go helpers, checked corpora, upstream inventory |
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

## Parallel unit of work

Agents own source domains, not horizontal file types. A normal feature slice
contains:

1. the owning Go implementation and original tests;
2. one typed Rust domain envelope;
3. focused unit/regression ports;
4. one source-derived selector or differential corpus;
5. exact focused validation and a narrow integration request.

Shared dispatchers, `Datum`/`EvalContext`, cluster/session state, Cargo
metadata, and checked inventory snapshots are stewarded seams. Feature workers
do not edit them concurrently. Once a seam is split into domain envelopes,
ownership moves down to those envelopes so unrelated features compile and land
in parallel.

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

Inventory is not coverage. The production-source and upstream-test ledgers
are independent: `UNTRIAGED`, `PARTIAL`, parser acceptance, and `Unsupported`
are all visible obligations. A source or test row moves to covered only when
its complete cited contract has executable Rust evidence.

The module-qualified `external_go_*_inventory.tsv` ledgers independently pin
the direct client-go and pd-client source/test universes from the offline Go
module cache. Their ownership states do not inflate TiDB product-parity totals.

Executable corpus namespaces use paired `<topic>.txt` and
`<topic>.golden.txt` files. Explanatory evidence belongs under
`difftests/corpus/coverage/`; the validation gate rejects prose or orphan
files in executable namespaces.

## Development loop

Run Cargo commands from `rust/` and always use 12 jobs:

```sh
cargo fmt --all
cargo test --locked -j 12 --workspace -q
cargo clippy --locked -j 12 --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

Then run the checked inventories and the focused ring required by the change;
the exact commands are maintained in
[`docs/operations/validation.md`](docs/operations/validation.md). Direct Go
wrappers run from the repository root. On local arm64 they may fail in the Go
toolchain's `gensymlate` process; record that as unverified instead of treating
it as a Rust failure or silently replacing the oracle.

## Current structural priorities

- keep shrinking AST/parser/executor roots into Go-source-owned vertical
  domains; roots retain exhaustive routing contracts only;
- continue `tidb-codec` and `tidb-txnkv` from complete dependency-closed Go
  source/test units, then add timestamp/protocol/MVCC/lock/commit services only
  with real consumers and transaction-ring evidence;
- finish routing datatype-owned `Datum` through typed `BuildContext` and one
  statement-scoped `EvalContext` for charset/collation, SQL mode, warnings,
  and time;
- introduce planner/catalog boundaries only when a complete Go-owned API,
  an immediate consumer, and source-derived evidence move together;
- dispatch exact Go test files, SQL fixture/result families, shell programs,
  testdata, and suite support artifacts from the generated ledger; never use a
  giant Go package as one agent task;
- close the four differential rings without weakening checked snapshots.

Progress and exact reviewed counts live only in [`HANDOFF.md`](HANDOFF.md), so
this entrypoint does not become another per-wave changelog.
