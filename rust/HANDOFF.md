# TiDB to Rust rewrite handoff

This is the current operating handoff. Generated ownership state belongs in
[`STATUS.md`](STATUS.md); implementation history belongs in Git. Do not append
campaign diaries, dated completion narratives, or hand-maintained parity
percentages here.

## Goal and non-negotiable unit

Rewrite TiDB's SQL layer as a standalone Rust SQL node without losing Go
behavior or any original test/support obligation. The minimum transcreation
unit is one complete upstream Go package or module, not a function, file,
branch, SQL shape, or feature slice.

A package claim includes:

- every production Go file in the package, including platform variants and
  generated sources together with their generator inputs;
- every package test, subtest, benchmark, fuzz target, example, test-only
  package, fixture, golden result, embedded file, testdata directory, failpoint,
  helper program, and runner script;
- package build metadata and directly owned support artifacts; and
- the dependency contracts and Rust output paths needed to integrate the whole
  package without a second implementation authority.

No package is transcreated until that entire inventory is accounted for and
its required differential and live gates pass. A useful partial port remains
explicitly incomplete; it is not a template, a parity claim, or a reason to
split the rest of the package by branch.

## Current architecture

- `rust/` is one Cargo workspace producing a standalone SQL-server process. It
  does not link Go through cgo, FFI, or an in-process compatibility backend.
- The upper boundary is the MySQL wire protocol. The lower boundaries are PD,
  kvproto/TiKV BatchCommands, tipb coprocessor DAGs, MPP, and etcd-compatible
  cluster coordination.
- One process authority owns PD access, RegionCache, TiKV transport, retry and
  lock-resolution policy, background supervision, and shutdown ordering.
  Reads and writes must converge on this authority instead of adding another
  client or mock runtime.
- Rust crate boundaries are implementation boundaries, not permission to split
  an upstream package's acceptance. One Go package may map to several crates,
  but one implementation owner remains responsible for the complete inventory,
  integration, and package receipt; no crate sub-result is independently
  promoted.
- Shared crate roots, manifests, generated inventories, and cross-package
  routing are serialized integration seams. The active package claim declares
  every implementation, test, evidence, and integration path it may change.

## Verified live facts

The repository contains bounded live proofs, not package or product parity:

- A Rust MySQL listener has authenticated clients and executed text and binary
  prepared traffic through the Rust parser/planner/executor path.
- The connected read path has reached real PD and TiKV through RegionCache and
  BatchCommands and returned rows over MySQL.
- The connected write path has performed real optimistic prewrite/commit and
  rollback operations, and a prepared write has been read back independently
  from real TiKV.
- The live harnesses exercise stock MySQL/sysbench-shaped prepared traffic and
  record protocol and storage events. They prove only the bounded statements
  and topology scenarios encoded by those harnesses.

The executable evidence is under [`scripts/`](scripts/), including the
Campaign-28 real-TiKV and prepared-write proofs. Exact ownership counts and
states are generated in [`STATUS.md`](STATUS.md). Neither source is evidence
that any upstream Go package is completely transcreated unless a package
receipt says so.

Four leaf packages currently have schema-2 receipts under
[`workstreams/package-receipts/`](workstreams/package-receipts/):

- `pkg/server/internal/handshake`, implemented in `tidb-server`. It closes only
  that exact package inventory, not the server handshake
  parser/authentication lifecycle or the `pkg/server` subsystem.
- `pkg/parser/format`, implemented across `tidb-ast` and `tidb-datatype`. Its
  current receipt covers Go simple-rune Unicode casing, arbitrary format bytes,
  one-call writer semantics, the original package tests/support, and the
  declared production consumer seam. It does not credit downstream AST-node
  packages.
- `pkg/parser/mysql`, implemented across `tidb-error` and `tidb-mysql`. Its
  receipt covers all ten production files, all eleven original Go tests, build
  metadata, exact error-code/name/SQLSTATE authorities, Unicode tables, and the
  declared parser/protocol consumer seams.
- `pkg/parser/util`, implemented across `tidb-hash`, `tidb-lexer`,
  `tidb-parser`, and `tidb-planner`. Its receipt covers both production files,
  every original test/build obligation, arbitrary-byte Go string hashing with
  malformed UTF-8 rune iteration, MySQL escape behavior, and the declared
  parser/planner consumer seams.

## Whole-package gaps

The current Rust workspace is a connected seed assembled from legacy partial
feature evidence. The following upstream package families remain open as
whole-package obligations:

| Package family | Connected seed that exists | Package-level gap |
| --- | --- | --- |
| `pkg/server` | MySQL handshake, command loop, result framing, bounded prepared execution | Full authentication/plugin/TLS, command, cursor, long-data, parameter, result-type, status, error, connection-lifecycle, and original server test/support inventory |
| `pkg/session`, `pkg/sessionctx` | Bounded session and prepared-statement state | Full transaction lifecycle, autocommit/status semantics, variables, statement context, retry/replay, privileges, bootstrap interaction, and all original tests |
| `pkg/parser` | Broad parser/AST seed and differential corpus | Complete package/module source, generated/support inputs, every grammar/restore/error obligation, and all parser tests |
| `pkg/planner` | Bounded statement lowering, ranges, projections, DML, and selected relational operators | Full preprocess/resolve, logical and physical optimization, costs, hints, bindings, privileges, all statement families, testdata, and plan-diff closure |
| `pkg/executor`, `pkg/expression` | Bounded row execution, selected expressions/aggregates, and prepared DML | Complete executor and builtin/vectorized semantics, joins, windows, CTEs, spill, admin paths, error/coercion behavior, and all original tests |
| `pkg/kv`, `pkg/store`, `pkg/distsql` plus pinned client-go/PD modules | Real PD/RegionCache/TiKV read transport and normal optimistic-write seed | Full snapshots/scans/batching, mem-buffer and staging, locks, retries, pessimistic/async-commit/1PC, resource control, coprocessor/MPP behavior, faults, and all pinned tests |
| `pkg/types`, codec/util dependencies | Selected Datum, decimal, row/key, collation, and binary encoders | Every type, conversion, collation/charset, temporal/JSON/enum/set/vector behavior, codec variant, generator/support file, and original test |
| `pkg/infoschema`, `pkg/meta`, `pkg/domain` | Static configured catalog sufficient for bounded live proofs | Real schema/meta loading, leases, MDL, system tables, domain lifecycle, schema synchronization, and all original tests |
| `pkg/statistics` | Isolated source-grounded statistics leaves | Full statistics data model, estimation, loading, feedback, analyze lifecycle, persistence, ownership, and testdata |
| `pkg/ddl` and ownership/background packages | No package-complete implementation | Full DDL state machine, reorg/backfill, ownership, coordination, compatibility, recovery, and destructive-failure testing |

This table describes the shape of the remaining work. It does not replace the
generated ledgers and must not be converted into percentage progress.

## Package execution workflow

1. Generate a package manifest from the authoritative Go tree. Freeze every
   source, test, support, fixture, generated, and build-metadata obligation.
2. Resolve the package dependency DAG. Claim one package or a dependency-closed
   package group; never claim a partial file/function subset as a transcreation
   unit.
3. Declare the complete Rust write set, including shared integration seams,
   before creating a worktree or changing behavior.
4. Translate directly from the Go implementation and port the complete
   package test/support inventory. Rust-specific redesign is limited to
   runtime mechanisms that cannot be carried faithfully, such as GC pools,
   goroutine ownership, or untyped registries.
5. Run focused package checks during implementation. Close an ordinary package
   directly with `campaign_close.py --package <owner> --gate`; it derives the
   transaction from the exact claim, refreshes only that package's ledger rows,
   builds evidence tools once, runs independent read-only static checks
   concurrently, then runs full workspace tests, Clippy, and applicable live
   gates using 12 jobs. The final content digest replaces a duplicate post-test
   static pass. Create a tracked campaign only when two or more packages are
   genuinely dependency-inseparable.
6. Issue a package receipt only after inventory closure and required
   differential/live evidence. Until then every carried ledger row stays
   honestly untriaged, partial, or blocked.

New package implementation is integrated only with its complete receipt. A
package that cannot close remains on its package branch or is explicitly
blocked; it is not split into smaller mergeable ports.

The executable coordination contract is [`PARALLEL.md`](PARALLEL.md).
The living migration plan is
[`workstreams/plans/2026-07-whole-package-transcreation.md`](workstreams/plans/2026-07-whole-package-transcreation.md).
Existing schema-1 records under [`workstreams/slices/`](workstreams/slices/)
are frozen legacy evidence. They must not be copied, extended, or used as the
dispatch template for new package work.

## Immediate migration steps

1. Select the next larger dependency-closed frontier from the checked package
   DAG. Prioritize packages that unblock the connected SQL transaction path;
   do not optimize for easy package counts.
2. Freeze every package claim before its first implementation commit. The
   common `base_commit` and committed-diff guard must cover the full work, not
   only a takeover snapshot.
3. Declare exclusive stable `rust_paths` and serialized shared
   `integration_paths` before creating package worktrees. A Go package may
   span several Rust crates under one umbrella claim and receipt.
4. Transcreate the package through one owner. Treat any crate-local intermediate
   result as staging only; never promote it independently.
5. Add exact source/test/support evidence only after the complete package
   contract and all original tests are represented.
6. Close one package with `campaign_close.py --package <owner> --gate`. For a
   genuinely inseparable group only, close the tracked frontier with
   `campaign_close.py --campaign <campaign> --gate`. Both paths issue receipts
   atomically, regenerate status, and require the Ready profile before the next
   package starts.

The highest-value first frontier is the connected SQL transaction path:
`pkg/kv`/`pkg/store` dependencies, `pkg/session`/`pkg/sessionctx`, and the
required `pkg/server` prepared transaction lifecycle. The package DAG decides
the exact grouping; the existing bounded `BEGIN`/`COMMIT`, prepared DML, and
normal-2PC leaves are inputs, not completion evidence.

## Operating rules

- Read [`STATUS.md`](STATUS.md), the relevant package manifest, the owning Go
  package docs/source/tests, [`PARALLEL.md`](PARALLEL.md), root `AGENTS.md`, and
  `PLANS.md` before editing.
- Use 12 jobs for builds. Run focused checks during implementation and the
  shared Ready gate before a completion claim.
- Keep unsupported behavior fail-closed before state mutation or PD/TiKV
  publication, but do not treat fail-closed partial behavior as transcreation.
- Do not add compatibility aliases, duplicate codecs, a second transaction
  client, in-memory acceptance paths, or handwritten goldens when a Go oracle
  exists.
- Preserve unrelated files and local claims. Do not hand-edit generated
  ledgers or status.
- Read live claim count from `work-unit-queue.py check`; `STATUS.md` records
  stable manifest/receipt state and deliberately excludes ignored lease files.
- Record current architecture and blockers here; record changing counters in
  generated status and let Git carry history.
