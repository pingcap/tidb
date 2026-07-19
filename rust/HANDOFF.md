# TiDB to Rust rewrite handoff

_Current operating handoff. Updated 2026-07-19. This file intentionally does
not preserve completed-campaign or wave history._

## Standing goal

Rewrite TiDB's SQL layer in Rust without missing any behavior or test
obligation owned by the original TiDB implementation. Organize the work as
source-owned, dependency-closed slices that multiple agents can implement in
parallel, then prove each integrated vertical against Go and, where required,
real PD/TiKV.

The target is a standalone Rust process tree. There is no cgo. Go and Rust
communicate only through existing serialized network protocols during the
strangler transition.

## Read in this order

1. [`STATUS.md`](STATUS.md) is the generated authority for current queue,
   campaign, source-ledger, and original-test-ledger state. Regenerate it; do
   not copy its counters here.
2. [`workstreams/plans/2026-07-read-path-25.md`](workstreams/plans/2026-07-read-path-25.md)
   is the living ExecPlan for the active Campaign 25 vertical and its frozen
   acceptance boundary.
3. [`../docs/design/2026-07-11-tidb-rust-rewrite.md`](../docs/design/2026-07-11-tidb-rust-rewrite.md)
   defines the long-term architecture, migration order, and differential
   verification model.
4. [`PARALLEL.md`](PARALLEL.md) and
   [`workstreams/slices/README.md`](workstreams/slices/README.md) define the
   checked ownership, claim, worktree, promotion, and shared-gate protocol.

Also obey the repository root `AGENTS.md`, `PLANS.md`, and the closest package
documentation before editing an owning subsystem.

## Go is the source contract

Every Rust behavior is a port of an identified Go implementation and its
complete relevant tests. Differential tools confirm a port; sampled outputs
do not define semantics.

For every slice:

1. Locate and read the owning Go implementation, callers, tests, fixtures,
   lifecycle hooks, and generated/support artifacts.
2. Port the normal Go control flow, types, errors, and state transitions. Do
   not infer a general rule from a few `godump` or `gorun` results.
3. Record every source and exact original obligation in the checked slice and
   evidence ledgers. An unported edge remains explicit `PARTIAL`, `BLOCKED`, or
   `UNTRIAGED`; it is never hidden behind a broad parity claim.
4. Use the parser, plan, result, or transaction differential ring to verify the
   corresponding boundary. If Rust and a probe disagree, inspect the Go source
   and environment; Go wins.
5. Reject unsupported behavior before runtime side effects. Do not create an
   approximate fallback, a second policy authority, or an in-memory substitute
   for a promised real-cluster path.

Primary Go entry points:

| Domain | Go source of truth |
| --- | --- |
| Lexer, grammar, AST, restore | `pkg/parser/lexer.go`, `pkg/parser/parser.y`, `pkg/parser/ast/**` |
| Expressions and SQL types | `pkg/expression/**`, `pkg/types/**`, `pkg/util/collate/**` |
| Planner | `pkg/planner/**` |
| Executor and session | `pkg/executor/**`, `pkg/session/**`, `pkg/sessionctx/**` |
| Catalog and DDL | `pkg/infoschema/**`, `pkg/meta/**`, `pkg/ddl/**` |
| Storage and distributed reads | `pkg/kv/**`, `pkg/store/**`, `pkg/distsql/**`, plus the pinned PD/client-go sources in the generated ledgers |
| MySQL server | `cmd/tidb-server/**`, `pkg/server/**` |

The untracked repo-root helpers are confirmation tools:

- `./godump restore` provides Go parser restore output.
- `./gorun` provides mock-backed Go session result output.

Run them from the repository root. Cargo commands run from `rust/`; per-slice
worktrees have their own `rust/` directory.

## Current workspace map

The workspace is split by owning behavior rather than horizontal utility
layers:

| Area | Rust packages | Responsibility |
| --- | --- | --- |
| Foundation | `tidb-error` | shared typed error boundary |
| SQL syntax | `tidb-lexer`, `tidb-ast`, `tidb-parser` | tokens, typed AST, parsing, restore |
| SQL semantics | `tidb-datatype`, `tidb-expr`, `tidb-planner`, `tidb-stats` | scalar authority, evaluation, binding/planning, statistics |
| Storage path | `tidb-proto`, `tidb-codec`, `tidb-pd-client`, `tidb-txnkv`, `tidb-distsql` | wire types, keys, PD/TiKV transport, routing, distributed reads |
| Runtime | `tidb-exec`, `tidb-protocol`, `tidb-server` | execution, MySQL protocol, deployable process lifecycle |
| Evidence | `difftests` and its parser/planner/result/transaction packages | inventories, ledgers, oracles, selectors, and gates |
| Coordination | `workstreams/**`, `scripts/**` | checked slices, campaigns, claims, worktrees, promotion, status, integration |

Protected routing files and shared manifests have one steward. Feature agents
edit only the `rust_paths` declared by their claimed slice. A public seam must
move with its first real consumer; do not add disconnected helper crates or
duplicate catalog, scalar, planner, topology, transport, or session authority.

## Active Campaign 25

Campaign 25 builds the first configured two-relation read-only SQL-node
vertical. One stock MySQL connection must execute bounded two-table signed
`BIGINT` joins against real TiKV. Both scans use one PD timestamp, retain the
existing range and residual-Selection path, and join decoded rows in the Rust
TiDB executor.

The implementation DAG is:

```text
A configured catalog -> B relation binding -> C FullSchema join plan --+
                                                                    +-> E join runtime -> F live SQL-node proof
Campaign 24 range path -> D same-snapshot real-TiKV multi-read -------+
Campaign 24 live harness -----------------------------------------------> F
```

Current state:

- A, configured catalog: implemented and promoted with exact `PARTIAL`
  source/test evidence; claim retained until the final campaign receipt.
- B, relation binding: implemented and promoted `PARTIAL`, including visible
  `USING` projection binding without a rewrite/reparse workaround.
- C, FullSchema join planning: implemented and promoted `PARTIAL`; relation
  scans lower through a structured planner seam, not reconstructed SQL.
- D, real-TiKV multi-read: implemented and promoted `PARTIAL`; one read
  session, one nonzero snapshot per statement, two transports, shared
  cancellation, and contradiction-before-TSO behavior are covered by focused
  evidence. Commit `55485d1b3b` integrates the supplied-plan APIs E requires
  without adding another planner or reader authority.
- E, configured inner-join runtime: claimed and active in
  `rust/.worktrees/executor-configured-inner-join-runtime`. It is the immediate
  implementation critical path and the sole Campaign 25 `tidb-exec` routing
  steward.
- F, server/live multi-relation proof: it cannot start until E's exact
  join-runtime evidence is promoted. Its server write-set split is still being
  prepared, so the current single checked slice is not yet the final dispatch
  shape. The resulting members own server configuration/routing and the
  stock-MySQL-to-real-TiKV live acceptance path.

The bounded contract remains exactly two configured, nonpartitioned tables;
direct signed `BIGINT` projections; local flattened-`AND` comparisons; one
non-null cross-side equality for `INNER JOIN ... ON` or `USING`; plus
`CROSS JOIN` and comma syntax. Dynamic InfoSchema, NULL/coercion/collation join
semantics, outer/semi/anti joins, arbitrary join predicates, aggregates,
ordering, limits, DML, and general write/transaction parity remain explicit
gaps.

## Agent workflow

The root steward prepares dependency-ready, consumer-complete slices before an
agent starts. Parallelism is constrained first by declared mutable Rust paths,
then by semantic Go ownership.

From the primary repository root:

```sh
python3 rust/scripts/work-unit-queue.py check
python3 rust/scripts/work-unit-queue.py ready --target <crate> --ring <ring>
python3 rust/scripts/work-unit-queue.py claim-slice \
  --owner <slice> --slice <slice>
python3 rust/scripts/slice-worktree.py --slice <slice>
```

`slice-worktree.py` creates or reuses `codex/<slice>` under the ignored,
writable `rust/.worktrees/<slice>` root and probes a real write. Do not use the
old sibling `../tidb-rust-worktrees` layout: desktop subagents can read it but
cannot safely edit it.

Each feature agent must:

- work only inside its slice worktree and declared `rust_paths`;
- read all claimed Go source and original obligations before implementation;
- preserve the frozen interface to downstream slices or report the exact seam
  change before editing a shared owner;
- add source-shaped focused tests and exact owner-named evidence;
- prove a regression fails on the predecessor for the intended reason before
  claiming a bug fix passes;
- run focused validation only, then commit a narrow leaf change and report the
  commit, files, tests, risks, and unverified surfaces;
- leave the claim active and never run or release against the shared campaign
  gate.

The root steward reviews and integrates leaf commits. When a completed member
must unlock a downstream exact-evidence dependency, use the atomic incremental
promotion flow without running or consuming the final campaign gate:

```sh
python3 rust/scripts/campaign_close.py \
  --campaign 2026-07-read-path-25 --promote-member <slice>
python3 rust/scripts/campaign_close.py \
  --campaign 2026-07-read-path-25 --promote-member <slice> --apply
```

Promotion retains the claim and honest `PARTIAL` gaps. Existing test-domain
labels are stable partition identities and must not be rewritten during
evidence transfer. The tool may add a missing exact row for an already-split
test file atomically; it must roll back on failure.

Keep the pipeline full: while the current frontier executes, prepare the next
six mutually writable slices and freeze their public seams. Prefer completing
or consolidating existing `PARTIAL` families that unlock real consumers over
creating isolated helpers. Do not make feature agents wait for root discovery,
evidence repair, or shared-file design.

## Verification contract

Use the repository `WIP` profile while Campaign 25 is open.

Feature-agent fast lane:

- the narrow focused test or already-registered shard named by the slice;
- focused package Clippy when the leaf needs Cargo integration;
- `cargo fmt --all -- --check` or the narrow rustfmt equivalent;
- exact evidence/anchor checks required by the slice;
- `git diff --check`.

Set `CARGO_BUILD_JOBS=12` for every Cargo build. Reuse one stable target per
checkout; never share a target directory between worktrees because generated
evidence embeds `CARGO_MANIFEST_DIR`. Do not run a workspace build after every
leaf and do not let each agent create a throwaway full target.

After the final campaign member set and live proof freeze, the root steward
runs exactly one shared gate from `rust/`:

```sh
python3 scripts/work-unit-queue.py check
CARGO_BUILD_JOBS=12 scripts/rewrite-gate.sh integrate
```

Then run from the repository root:

```sh
make -j12 lint
```

The campaign is not Ready until the stock-client live test passes against real
PD/TiKV, the shared receipt covers every frozen claim, all members consume that
receipt, claims are released with `--integrated`, campaign membership is
archived, and generated `STATUS.md` is refreshed. A failed gate releases
nothing; fix the owning leaf and rerun the frozen shared gate.

`make bazel_prepare` is required only if the change triggers the root
`AGENTS.md` Go/Bazel/module conditions. Do not run `make bazel_lint_changed`.
Use the repository RealTiKV lifecycle rules for any real-cluster test: start in
the background, prove readiness, retain failure diagnostics, and clean every
owned process and data artifact.

## Immediate next work

1. Finish E in its current worktree: consume C's typed plan and D's two
   same-snapshot real-TiKV results, implement deterministic bounded inner-join
   materialization/projection/cancellation, and preserve required-row limits
   without over-reading or client-side scan filtering.
2. Review E against its complete claimed Go sources/tests, integrate its
   focused evidence, and promote it atomically to exact `PARTIAL` so F unlocks.
3. Freeze the final F server/live slice split in the campaign manifest, claim
   the dependency-ready members, and create their writable worktrees. Connect
   the existing server configuration/session/lifecycle authority to the
   two-table runtime, extending the shared live harness instead of copying its
   lifecycle code.
4. Run the Campaign 25 stock-client proof against real PD/TiKV: aliases, ON,
   USING, CROSS/comma, local range and residual predicates, projections from
   both sides, no-match output, one snapshot, topology churn, blocked shutdown,
   zero exit, and tag-owned cleanup.
5. Freeze every final campaign member, run the one shared integration gate and
   repository lint, consume/release every receipt-backed claim, and regenerate
   status.
6. In parallel when a slot opens, scope the next six disjoint slices so the
   dispatcher does not return to an empty ready frontier after Campaign 25.

## Durable environment facts

- Primary checkout: `/Users/qiliu/projects/tidb`.
- Primary integration branch: `hparser-integration`; feature branches use
  `codex/<slice>` and are not pushed unless requested.
- Rust toolchain is pinned by `rust-toolchain.toml`; the current workspace
  requires Rust 1.97 and forbids unsafe code.
- The workspace baseline is tracked. `rust/.worktrees/` is intentionally
  ignored; claim leases under `rust/workstreams/claims/` are local coordination
  state, not committed evidence.
- `godump`, `gorun`, and `goeval` at the repository root are untracked helper
  binaries. Do not commit them.
- Existing unrelated user files and dirty changes must be preserved. Commit a
  slice with explicit paths; never sweep unrelated files into a commit.
- This handoff, generated status, checked manifests/ledgers, and living
  ExecPlans are the complete shared context. Agents must not depend on private
  chat history or machine-local notes.
