# TiDB to Rust rewrite handoff

## Goal

Replace TiDB's Go SQL layer with a standalone Rust SQL node without losing Go
behavior or any original test/support obligation. The minimum implementation
and acceptance unit is one complete upstream Go package or module. A Go package
may map to several Rust crates.

## Current architecture

- `rust/` is one Cargo workspace; it does not link Go through cgo or FFI.
- The upper boundary is MySQL protocol. Lower boundaries are PD, TiKV/kvproto,
  tipb coprocessor DAGs, TiFlash MPP, and etcd-compatible coordination.
- Existing parser, planner, executor, session, protocol, and storage code is a
  connected seed. Partial feature code is not package completion.
- Completed whole packages are exactly the valid proofs under [`ports/`](ports/).
- [`scripts/package-port.py`](scripts/package-port.py) is the only package
  acceptance tool.
- Integration test source files are grouped into crate-level harnesses unless
  they require crate-root topology. The workspace currently exposes 70 Cargo
  integration targets instead of the previous 429 while retaining every test
  source.

## Verified live boundary

The repository has bounded live tests showing a Rust MySQL listener, text and
prepared query execution, real PD/RegionCache/TiKV reads, and optimistic
prewrite/commit/rollback. Those are useful runtime proofs, not overall parity or
whole-package completion.

## Completed whole packages

`scripts/package-port.py check` is authoritative. At this handoff the proofs
cover:

- `pkg/parser/auth`
- `pkg/parser/format`
- `pkg/parser/mysql`
- `pkg/parser/opcode`
- `pkg/parser/terror`
- `pkg/parser/util`
- `pkg/server/internal/handshake`

## Largest gaps

| Area | Remaining whole-package work |
| --- | --- |
| Parser and AST | Root parser, generated grammar/support, complete AST families, restore/error behavior, and all original parser tests |
| Types and codec | Complete datums, conversions, temporal/JSON/vector/enum/set, charset/collation, row/key/value codecs, generators, and tests |
| Session and server | Authentication/TLS/plugin lifecycle, commands, prepared/cursor/long-data behavior, variables, transactions, retries, privileges, bootstrap, and tests |
| Planner and executor | Resolve/preprocess, logical/physical optimization, costs, hints, expressions, joins, windows, CTEs, spill, admin execution, testdata, and plan/result parity |
| Storage and distributed query | Complete PD/client-go parity, region cache, snapshots, scans, locks, retries, transaction variants, coprocessor/MPP, faults, and real-cluster tests |
| Domain, metadata, statistics, DDL | Schema loading/leases/MDL, system tables, statistics lifecycle, ownership, DDL/reorg/backfill/recovery, and all original tests |

## Working loop

1. Select a dependency-ready whole package with meaningful downstream value.
2. Run `scripts/package-port.py inventory <go-package>` and audit every file.
3. Transcreate all production behavior directly from Go and translate every
   original test/support obligation.
4. Run focused tests while editing.
5. Run `scripts/package-port.py finish ...`; commit its generated proof with the
   code.
6. Before push or after shared-foundation changes, run
   `scripts/package-port.py checkpoint`, repository Ready validation, and any
   required differential/live suite.

There is no start command, queue, claim, campaign, transfer ledger, mutable
status, frozen workspace, or separate receipt. Git is the history and rollback
mechanism.
