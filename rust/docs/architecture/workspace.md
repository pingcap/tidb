# Rust rewrite workspace architecture

`rust/` is a Cargo workspace for a standalone, source-faithful TiDB SQL node.
It never links Go through cgo or FFI. The Go tree is the behavioral authority.

## Layout

```text
rust/
  crates/              production Rust crates
  difftests/           differential harnesses and checked corpora
  ports/               one generated proof per completed Go package
  scripts/
    package-port.py    whole-package inventory and acceptance
    run-*.sh           bounded live/real-cluster checks
  docs/                current architecture and validation guidance
  workstreams/         subsystem notes and the active ExecPlan
```

Legacy feature-slice scheduling, claims, campaigns, transfer ledgers, mutable
status, and receipts have been removed. They are not architecture.

## Crate responsibilities

| Crate | Responsibility | Primary Go authority |
| --- | --- | --- |
| `tidb-lexer` | scanning and token classes | `pkg/parser` lexer/token sources |
| `tidb-ast` | typed AST and canonical restore | `pkg/parser/ast` |
| `tidb-parser` | grammar and parser-level helpers | `pkg/parser` |
| `tidb-error` | error identity and MySQL error mapping | parser terror/MySQL errors and shared error packages |
| `tidb-mysql` | MySQL constants, charset, privileges, types | `pkg/parser/mysql` |
| `tidb-datatype` | SQL scalar values and type metadata | `pkg/types` |
| `tidb-codec` | row, key, and comparable encodings | `pkg/util/codec` and row codecs |
| `tidb-proto` | checked protocol definitions | kvproto/tipb inputs |
| `tidb-protocol` | MySQL packet and command framing | server packet/protocol packages |
| `tidb-txnkv` | PD/region/TiKV transport and transaction primitives | `pkg/kv`, `pkg/store`, client-go, pd-client |
| `tidb-distsql` | coprocessor request/response lifecycle | `pkg/distsql` |
| `tidb-expr` | expression construction and evaluation | `pkg/expression` |
| `tidb-planner` | resolution and logical/physical planning | `pkg/planner` |
| `tidb-exec` | session-facing execution | `pkg/executor`, selected session contracts |
| `tidb-server` | process startup, MySQL connection lifecycle, dispatch | `cmd/tidb-server`, `pkg/server` |

The exact package-to-crate mapping is many-to-many. Rust boundaries follow
cohesion, ownership, dependency direction, and compile-time cost. They never
make a partial Go package acceptable.

## Dependency direction

Protocol-definition and datatype foundations remain below SQL behavior.
Parser/AST code does not depend on execution. Expression code consumes AST and
datatypes. Planner consumes parser/AST, datatypes, statistics contracts, and
catalog interfaces without depending on the server. Executor consumes plans,
expressions, datatypes, DistSQL, and transaction interfaces. Server owns
network lifecycle and delegates SQL behavior rather than reimplementing it.

Storage has one production authority for PD access, region routing, TiKV
transport, retries, lock resolution, and transaction state. Session state has
one authority for variables, statement context, prepared state, transaction
lifecycle, warnings, and status publication. Duplicate authorities are removed
when a complete package takes ownership.

## Runtime boundaries

- MySQL protocol is the client boundary.
- PD, kvproto/TiKV, tipb, TiFlash MPP, and etcd-compatible APIs are the cluster
  boundaries.
- Tokio owns network I/O, timers, cancellation, and supervised background
  tasks.
- CPU parsing/planning/evaluation stays synchronous until profiling proves a
  bounded offload is needed.
- Unsupported behavior fails before mutation or external publication.
- Real PD/TiKV checks are mandatory for storage semantics; mocks are focused
  tests, not release evidence.

## Whole-package acceptance

`scripts/package-port.py finish` derives every top-level Go package file and
`testdata` file, all test/benchmark/fuzz/example entry points and literal
subtests, direct internal dependencies, and a content digest. It verifies the
declared Rust crates, paths, and test targets, then writes one proof under
`ports/<go-package>.toml` only after focused checks pass.

Git is the only history, review, atomic commit, rollback, and repair mechanism.
The pre-push workspace sweep is `scripts/package-port.py checkpoint`.

## Test process layout

Crates with many independent files under `tests/` use
`scripts/aggregate-tests.rs` as a shared Cargo build script. It includes each
source file as a private module in one integration harness, so test ownership
and source paths stay intact without compiling and launching hundreds of tiny
binaries. A source that requires integration-crate-root topology carries the
`aggregate-test: standalone` marker and remains an explicit Cargo target.

The checkpoint gives aggregate harnesses 12 internal test threads and runs
standalone binaries 12-way with one test thread each. This keeps the total
thread budget bounded while matching the two different workload shapes.
