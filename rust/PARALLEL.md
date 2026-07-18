# Parallel Rewrite Protocol

This is the entrypoint for concurrent Rust rewrite work. It is deliberately
short: live ownership and validation rules live with each workstream rather
than accumulating stale wave tables here.

## Start Here

1. Read generated `STATUS.md` for the current queue and ledger counters. Read
   `HANDOFF.md` only for durable decisions and historical context.
2. Read `docs/architecture/workspace.md` for crate boundaries and the five
   protected routing seams.
3. Read the relevant `workstreams/<ring>/README.md` before taking a task.
4. Select an exact production owner from `difftests/SOURCE_LEDGER.md` and its
   original obligations from `difftests/PORTING_LEDGER.md`. For an active
   vertical domain, read its checked `workstreams/domains/<domain>.toml` record
   and run `cargo run --locked -j 12 -p difftest --bin domain_queue -- --check`.
5. Read the owning Go implementation and tests before editing Rust. Go is the
   source of truth; a golden only confirms a port.

For ordinary source-first work, use the checked-ledger family queue instead of
pairing the two ledgers by hand:

```sh
scripts/work-unit-queue.py queue --target <crate> --ring <ring> --limit 3
scripts/work-unit-queue.py claim --owner <owner> --source <go-path> \
  --test <go-test-path>:<line>:<name>
```

Raw families are discovery input, not the preferred dispatch unit. Before
dispatch, a steward records a dependency-ready, consumer-complete unit under
`workstreams/slices/`. A slice can atomically own multiple Go files, exact
tests, one focused target, and prerequisite slices:

```sh
scripts/work-unit-queue.py ready --target <crate> --ring <ring>
scripts/work-unit-queue.py claim-slice --owner <owner> --slice <slice>
```

Do not dispatch a source file merely because it scores highly in `queue`.
Dispatch it only when its slice has a real immediate consumer and every named
dependency is satisfied. Whole-slice dependencies in `depends_on` require the
named slice to be `covered`. A consumer that needs only one proven capability
from a larger partial slice names each exact source/test ledger anchor and its
expected evidence owner plus `PARTIAL` or `COVERED` minimum in
`evidence_prerequisites`; an owner is checked only at that anchor, never across
all of its rows, and artifact or neighboring-anchor coverage is never inferred.
This prevents dependency-blocked files from producing parallel piles of
unrelated `PARTIAL` implementations without forcing every reusable capability
into an artificial leaf slice.
Direct `claim` remains available for evidence repairs and already
dependency-closed single-file families; repeat `--source` when one atomic
vertical slice crosses files.

Same-directory/stem test matches are explicitly candidates, not ownership
proof. The claiming agent must read the Go source and tests before accepting
them and must add every missing exact obligation it discovers:

```sh
scripts/work-unit-queue.py amend --owner <owner> \
  --source <other-go-source-path> \
  --test <other-go-test-path>:<line>:<name>
```

`amend` updates an active lease atomically and rejects unknown or
already-claimed source/test anchors. Use it when reading the implementation
reveals another authoritative Go file; do not abandon and recreate the claim
just to enlarge a truthful vertical.

## Speed protocol

Use three validation speeds. A feature agent stays in the fast lane until its
source family, focused tests, and evidence fragments are complete. The static
lane merges claims/evidence/ledgers without linking the workspace, and the
evidence/workspace steward runs the expensive lane once a substantial
multi-domain batch is frozen.

| Lane | Owner | Required work | Workspace build |
| --- | --- | --- | --- |
| Fast | feature agent | exact Go source/test read, dependency-closed leaf, focused test or standalone wrapper, `rustfmt --check`, TSV/anchor checks, `git diff --check` | no workspace build |
| Static | evidence/workspace steward | claims, shared manifest rows, generated ledgers, docs, duplicate/queue checks | evidence tools only |
| Integrate | evidence/workspace steward | frozen multi-domain source-family batch | one batched `cargo test` + Clippy gate |

Do not give every agent a private full Cargo target. That repeats linking and
can exhaust disk. Keep one persistent target directory per checkout for the
integrate lane (for example `CARGO_TARGET_DIR=$HOME/.cache/tidb-rust-target`)
and reuse it across waves; clean obsolete targets only after checking disk
usage. Set `CARGO_BUILD_JOBS=12` for every Cargo build. Agents may run focused
package tests when a leaf needs Cargo integration, but they should not launch
the workspace gate independently.

The root steward should batch exactly one integration gate after all active
lanes report frozen with a substantial source-family delta, then update the
ledgers and generated snapshot together. A failed gate is diagnosed once for
the whole batch; agents fix their own leaf
without reopening unrelated lanes. This keeps the shared filesystem critical
section limited to routing, manifest, generated ledgers, and durable docs.

An agent must never expose a half-completed evidence consolidation in the
shared checkout. Create the replacement artifact, replacement evidence, and
checked transfer, and remove the retired artifact/evidence in one atomic patch;
then immediately run the static queue check. Deleting the old artifact first
leaves the global inventory invalid and prevents root from claiming otherwise
disjoint ready slices. If the consolidation cannot be made atomic, keep the old
owner intact until freeze and let the evidence steward perform the transfer.

Partition the next wave by mutable `rust_paths` before assigning semantic Go
domains. If two useful verticals must change the same routing file, either give
that seam to one steward or consolidate those verticals into one integration
claim while keeping their evidence separate. Never hide the collision behind
an owner name that has no matching checked slice: that bypasses write-set
serialization and only moves the conflict to the gate.

Prefer high-value queue units over a fixed quota of tiny methods. Rank an
assignment by (untriaged source/test obligations reduced, immediate consumer
enabled, and number of downstream leaves unblocked), then use the exact queue
commands below. A source-backed partial leaf is useful evidence, but three
isolated partial leaves should not outrank completing one source family and its
original test ring.

### Campaign mode: optimize obligations per gate, not wave count

The current long tail cannot be completed by repeating three-helper waves.
The dispatcher must keep at least two three-agent batches (six disjoint ready
slices) prepared ahead of implementation so agents never wait for root to
discover the next unit. A zero-length ready queue is dispatch starvation, not
proof that useful work is unavailable.

Group dependency-compatible slices into a subsystem campaign. Prefer an agent
unit that closes a coherent Go source family plus its complete original test
file over one that introduces another partial helper. Pre-create campaign leaf
modules and their narrow routing seam before fan-out; feature agents then edit
only `src/<campaign>/<leaf>.rs`, the mirrored test leaf, and owner-named
evidence. Shared `lib.rs`, `Cargo.toml`, generated inventories, and dispatcher
matches stay steward-owned.

Measure throughput as source/test obligations promoted per expensive gate.
For ordinary campaigns, freeze a gate only after at least 9 production source
files or 50 original test/support obligations have moved, or after one real
end-to-end consumer has become runnable. Smaller gates require a concrete
correctness or dependency reason. Keep one persistent target directory and run
the full 12-job gate once for the whole campaign.

### Per-slice Git worktrees

The tracked Rust baseline makes filesystem isolation real. The primary
dispatcher must claim the checked slice first, then create one branch and one
worktree for that exact owner. Local claim files remain ignored leases in the
primary checkout; they are not copied into feature branches or treated as
evidence.

From the primary repository root:

```sh
slice=<checked-slice-name>
worktree_root="$(dirname "$(git rev-parse --show-toplevel)")/tidb-rust-worktrees"
mkdir -p "$worktree_root"
python3 rust/scripts/work-unit-queue.py claim-slice --owner "$slice" --slice "$slice"
git worktree add -b "codex/$slice" "$worktree_root/$slice" HEAD
```

Feature agents edit only the slice's declared `rust_paths` inside that
worktree. They do not run the shared integration gate. The integrator reviews
and merges the feature commit into the campaign branch, verifies the frozen
claim/write-set union, and runs the single persistent 12-job gate. After a
slice is receipted and released, remove its clean worktree and branch through
normal non-forced Git operations.

The backlog policy is completion-biased: existing `PARTIAL` owners that can be
closed or consolidated outrank new partial leaves. A new partial is justified
only when it unlocks a named connected consumer or removes a prerequisite for
multiple ready slices. Mechanical translation starts from the Go control flow,
types, and complete test table; Rust-specific redesign follows only after the
source-shaped differential boundary passes. This keeps direct transition fast
without copying unverified semantics or accumulating duplicate authorities.

## Ownership Model

Feature agents own one domain module, its mirrored test module, and one
source-derived differential corpus or selector. They do not edit a routing
seam, `Cargo.toml`, shared queue snapshots, the ledger, or this protocol.

Four stewards serialize the small set of necessary shared edits:

- **AST/parser-routing steward**: domain envelopes, parser dispatch, AST
  re-exports, and statement routing tests.
- **executor-routing steward**: database state, transaction primitives, and
  top-level execution dispatch.
- **server-routing steward**: `tidb-server` command dispatch, framed
  response/sequence lifecycle, and its narrow protocol/session integration.
- **datatype/context steward**: `Datum`, charset/collation metadata,
  `EvalContext`, and their public re-exports. Result workers consume this seam
  but do not grow competing scalar or session-context types.
- **evidence/workspace steward**: `Cargo.toml`, physical moves, static oracle
  snapshots, inventory/ledger generation, and full-workspace validation.

A feature that needs a steward seam reports the exact Go source rule, affected
Rust type/function, source-derived test, and proposed narrow interface. The
steward lands that integration after the leaf work passes its focused checks.

Checked domain records make a **complete source-family partition**
mechanically visible: one `file:` owner conflicts with every other owner for
that Go file, while a symbol-split file must assign every top-level function
and method exactly once. Do not add a checked domain record for a first narrow
method inside an otherwise unpartitioned Go file; it correctly fails the queue
until every sibling symbol has an owner. Such an early direct port instead
records its exact source/evidence artifact and selector, then joins a checked
record when the whole source file can be partitioned. Every checked record
names existing Rust leaves, exact selector evidence, status, and 12-job
required commands. They are intentionally a sparse current-work queue, not a
second copy of either generated ledger. Active work uses ignored local
`workstreams/claims/<owner>.claim.json` leases. Claim creation is an atomic
transaction across every active source/test anchor; overlapping work is
rejected before editing begins. Run `scripts/work-unit-queue.py check` before
dispatch and release integrated or abandoned work explicitly:

```sh
# After the shared integration gate; rejects a slice still marked ready/active,
# a missing gate receipt, or implementation/test edits made after that receipt.
scripts/work-unit-queue.py release --owner <slice> --integrated

# Recovery/abandonment only; explicit and makes no completion assertion.
scripts/work-unit-queue.py release --owner <owner> --abandon
```

Claims coordinate active agents but never override the checked ledgers or
convert an obligation to `PARTIAL`/`COVERED`. A stale inventory anchor makes
the claim check fail visibly instead of silently hiding work. A schema-2 claim
named for a checked slice must exactly equal that slice's source/test sets at
every slice status; use `release` to remove a stale lease before reconciling
the manifest and taking a fresh claim. Release always requires exactly one of
`--integrated` or `--abandon`; omission cannot silently bypass the gate.
`--integrated` is the guard that prevents completed work
from silently re-entering the ready queue. The `integrate` gate records every
active claim and hashes the complete immutable Rust workspace both before and
after the full checks. It issues receipts only when the claim set and workspace
remain byte-identical for the entire gate. Root must rerun that batched gate if
any implementation, test, script, manifest input, or undeclared Rust path
changes; evidence, generated-ledger, slice-status, and handoff promotion may
happen afterward without invalidating the receipt.

Once a root dispatcher is split, ownership moves downward: one feature agent
owns one typed AST/parser/executor domain envelope plus its selector, or one
expression family plus its result selector. Agents should not be organized by
file type (one AST agent, one parser agent, one test agent), because that makes
every feature cross three queues and destroys parallel throughput.
The first `ddl/alter/` leaves demonstrate this rule: index visibility and
terminal re-partitioning each own their typed payload/grammar/evidence, while
the shared ALTER statement remains a routing seam. Extend that directory only
with another exact Go symbol family; never put generic ALTER behavior back in
the root.

An extraction deletes the old implementation, mixed test rows, private root
re-exports, and obsolete type names in the same increment. Do not leave a
forwarding wrapper or compatibility alias: it creates two apparent owners and
forces later agents to inspect both paths. Public API stability is preserved
only where a real external consumer requires it; internal consumers import the
new physical owner directly.

Because all agents share one filesystem, cross-crate changes must preserve a
compilable workspace at tool/message boundaries. Add new leaf types/functions
first while unused, then switch every constructor/match/re-export in one
`apply_patch` integration step. Do not leave an AST enum changed while its
parser constructors still target the old shape: that blocks every unrelated
Cargo lane and defeats the parallel layout.

## Non-Negotiable Evidence

- Every upstream Go test, test-suite program/support artifact, SQL input
  fixture, and checked-in SQL expected result stays visible in
  `difftests/corpus/coverage/go_test_inventory.tsv`. The companion checked
  `go_test_fixture_access_inventory.tsv` is AST-derived and records every
  `//go:embed` and supported `os` file-access expression; direct local string
  literals become exact fixture obligations while joins/helpers/patterns stay
  explicit unresolved obligations.
- Every non-test Go production file stays visible in
  `difftests/corpus/coverage/go_source_inventory.tsv`, including explicitly
  deferred and not-yet-assigned owners.
- Every parser change has a Go source anchor and an exact restore/error
  comparison. Do not hand-write goldens.
- Evidence fragments are append-only ownership units: one file owns one
  source-domain owner, and a new wave must create a new owner-named fragment
  instead of reusing or overwriting an existing `*.tsv`. This prevents a
  later lane from silently deleting an earlier lane's anchors while the
  generated ledger still looks superficially healthy.
- A consolidation may retire a duplicate implementation/evidence owner only
  through a checked `evidence/transfers/*.tsv` record. The transfer must prove
  that the new owner holds the exact source/test anchors, every replacement
  artifact exists, and every retired artifact is gone. Silent fragment
  deletion is forbidden. For a source-only move, all three test-anchor columns
  are `-`; for a test-only move, `source_path` is `-`. Never attach an unrelated
  source or test just to fill a transfer row.
- Ownership may move more than once. Preserve every historical transfer: the
  validator requires one connected, acyclic, non-branching chain whose terminal
  owner matches the current ledger. Intermediate replacement artifacts may be
  retired by the next checked transfer; only terminal artifacts must still
  exist. Never rewrite an older row to pretend the intermediate owner did not
  exist.
- Active schema-2 slice claims also serialize every declared `rust_paths`
  write surface. Ready work with a colliding Rust path is hidden until the
  earlier claim releases; source/test disjointness alone is not safe parallelism.
- `Unsupported` is correct only when the seed executor cannot model the real
  state semantics. It must happen before state mutation.
- A wave is not accepted without its focused regression plus the workspace WIP
  ring in `docs/operations/validation.md`.

## Workstreams

- [Parser](workstreams/parser/README.md)
- [Datatype and evaluation context](workstreams/datatype/README.md)
- [Codec and row identity](workstreams/codec/README.md)
- [Evidence and workspace](workstreams/evidence/README.md)
- [Result](workstreams/result/README.md)
- [Plan](workstreams/plan/README.md)
- [Statistics](workstreams/stats/README.md)
- [Transaction](workstreams/transaction/README.md)
- [Protocol](workstreams/protocol/README.md)
- [DistSQL context](workstreams/distsql/README.md)
- [Server](workstreams/server/README.md)

The directory layout and planned root-seam refactor are recorded in
`execplans/2026-07-14-parallel-workspace-layout.md`.
