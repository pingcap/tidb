# Parallel package transcreation protocol

This is the execution contract for concurrent Go-to-Rust work. The minimum
unit of ownership, implementation, validation, and completion is one complete
upstream Go package or module.

## Non-negotiable package boundary

A package transcreation owns the complete upstream package inventory:

- all production Go files, build-tag and platform variants, generated files,
  and generator inputs;
- all tests and nested cases, external test packages, benchmarks, fuzz targets,
  examples, testdata, fixtures, golden results, embedded data, failpoints,
  helper binaries, and runner scripts;
- package build metadata and directly owned support artifacts; and
- every Rust implementation/test/evidence destination required by the package.

No partial file, function, method, switch branch, SQL shape, or feature is a
valid transcreation unit. Partial code may be preserved as seed evidence, but
it remains incomplete until absorbed into a complete package claim and cannot
be marked package-complete.

Rust crate boundaries do not weaken this rule. One Go package may map to
multiple crates, and a dependency-closed package group may share one campaign,
but each package keeps one complete umbrella claim, manifest, integration
decision, and receipt. Write-disjoint Rust-crate subteams may work in parallel
inside that umbrella; their outputs are not independently promotable package
claims.

## Start here

1. Read generated [`STATUS.md`](STATUS.md) for ledger state; do not infer
   product progress from its counts.
2. Read [`docs/architecture/workspace.md`](docs/architecture/workspace.md) for
   crate boundaries and protected seams.
3. Read the owning Go package documentation, every production file, and its
   complete test/support inventory.
4. Read the checked package manifest and dependency-DAG record. If either is
   missing or stale, the package is not dispatchable.
5. Read the relevant workstream README and
   [`docs/operations/validation.md`](docs/operations/validation.md).

The source and test ledgers remain authoritative inventories. Package
manifests group their rows atomically; they do not replace, waive, or promote
them.

## Package manifest contract

Before dispatch, the evidence steward generates and checks one manifest per Go
package or pinned external module package. The manifest must record:

- upstream module/version and package import path;
- every production, test, fixture, support, generated, and build artifact;
- target Rust crates, stable package-owned paths, and steward-owned seams;
- package dependencies and the exact contracts consumed from them;
- focused Rust targets, Go-oracle selectors, differential rings, and live gates;
- the shared seams that require a named steward; and
- state: `inventory`, `ready`, `active`, `blocked`, or `covered`.

`covered` is legal only after the generated inventory is closed and a receipt
records every required gate. Compilation, one live query, or coverage of a
subset of ledger rows is not package coverage.

## Dependency-first dispatch

Build a DAG whose nodes are whole Go packages. Dispatch only:

- a single package whose package dependencies expose the required checked
  contracts; or
- a dependency-closed package group owned by one integration campaign.

Prepare more than one source/test/write-disjoint package at a time so agents
can work in parallel. Do not manufacture parallelism by splitting one package
into independently promotable implementation claims. A package lead may
delegate inventory/review work and implementation of frozen, write-disjoint
Rust crates or crate-local leaves. Those branches merge into one package
staging branch; the package still has one claim, one frozen inventory, one
integration owner, and one receipt. No file, function, branch, Rust crate, or
test subset is independently integrated into the shared branch.

Order the ready frontier by:

1. package inventory obligations closed;
2. deployed consumer unblocked;
3. downstream packages unblocked; and
4. expensive-gate reuse.

## Ownership and worktrees

The dispatcher acquires the complete package claim before creating a worktree.
Claims must reject overlap in upstream source, original test/support artifacts,
and stable package-owned Rust paths. `rust_paths` are exclusive package-agent
leaves. `integration_paths` are existing shared crate seams edited only by the
integration steward; they may overlap across package manifests but must not
overlap any schema-2 stable path by ancestry. The claim freezes both exact
lists.

Use one `codex/<package-owner>` branch and one writable in-repo worktree per
package claim. Local lease files remain ignored coordination state. Feature
teams may edit only their declared Rust package leaves, mirrored tests, and
owner-named evidence. They do not edit shared manifests, crate roots,
`Cargo.toml`, generated inventories, status, or this protocol.

For a Go package mapped to several Rust crates, the package owner may create
temporary `codex/<package-owner>/<rust-subtree>` branches and worktrees. Each
subteam receives a write-disjoint subset of the already-declared umbrella Rust
write set and a frozen interface. The package owner integrates those branches
into `codex/<package-owner>`; only that staging branch can enter the shared
frontier gate. A subteam cannot release, promote, or receipt part of the Go
package.

Shared edits are serialized through named stewards:

- AST/parser routing;
- datatype and evaluation-context authority;
- planner/executor/session dispatch;
- server protocol and connection lifecycle;
- transaction/storage runtime authority; and
- workspace, manifests, ledgers, receipts, and validation.

When two packages require the same mutable seam, freeze the leaf APIs first
and give the seam to one steward. Do not hide the collision behind different
owner names.

Every schema-2 claim freezes the current Git `base_commit`. Claims entering a
shared gate must have the same base. From that base through `HEAD`, every
committed path under `rust/crates/**`, plus Rust `Cargo.toml` and `Cargo.lock`
paths, must be inside the active claims' combined stable or integration write
sets. Gate begin requires that same Rust code/manifests scope to be clean of
staged, unstaged, and untracked changes.

## Translation rule

Translate the package from Go before redesigning it:

1. Preserve control-flow ordering, constants, encodings, error identity/text,
   SQL and wire output, hashes/equality, arithmetic, cancellation, and state
   transitions.
2. Port the complete original package test/support inventory, including
   negative and fault cases. A new Rust test does not discharge an unported Go
   obligation unless the manifest maps it explicitly.
3. Preserve one implementation authority. Delete superseded partial helpers,
   duplicate codecs, wrappers, and internal aliases when the package owner
   switches consumers.
4. Redesign only mechanisms coupled to the Go runtime: GC pools, goroutine and
   channel ownership, pointer identity, `any` registries, or init-time globals.
   The observable TiDB contract does not change.
5. Keep unsupported behavior fail-closed before mutation or publication while
   the package is incomplete. Fail-closed is safety, not completion.

## Validation lanes

| Lane | Owner | Required proof | Shared workspace build |
| --- | --- | --- | --- |
| Package | package team | manifest inventory read, complete source/test translation, focused tests, formatting, anchors, `git diff --check` | Focused only |
| Static | evidence steward | package manifests, DAG, claims, duplicate ownership, generated ledgers, paths, transfer records | Evidence tools only |
| Integrate | integration steward | frozen package frontier, workspace tests, all-target Clippy, differential rings, required live proofs | One reused 12-job target |

Set `CARGO_BUILD_JOBS=12` for every Cargo build. Reuse one checkout-specific
target directory and never share it across Git worktrees because evidence
binaries embed their checkout through `CARGO_MANIFEST_DIR`. Only the integration
steward runs the full gate; package teams stay in focused loops.

An integration receipt hashes the frozen package claims and immutable Rust
workspace before and after the gate. Durable leaf receipts content-address
stable `rust_paths`; they record integration seam path names and the shared gate
attestation without hashing seam bytes. This preserves proof of the package
leaf while allowing later packages to reuse a steward-owned seam. Any change
during the gate or committed Rust code outside the active write-set union
invalidates the gate.

## Evidence rules

- Every upstream production source remains in
  `difftests/corpus/coverage/go_source_inventory.tsv`.
- Every original test/support artifact remains in
  `difftests/corpus/coverage/go_test_inventory.tsv`; fixture access remains in
  the generated fixture-access inventory.
- Pinned client-go and PD-client packages use module-qualified source and test
  anchors and retain separate external totals.
- Evidence fragments remain owner-named and append-only. Ownership transfer
  uses checked transfer records; never silently delete or rewrite history.
- Claims coordinate writes but cannot change ledger state. Generated status and
  ledgers are integration outputs, not feature-team edit surfaces.
- A package receipt enumerates its complete inventory and exact focused,
  differential, fault, and live gates. Missing inventory makes the receipt
  invalid.
- Progress is reported as exact package inventory/receipt closure and explicit
  remaining package gaps, never a percentage inferred from Rust LOC or a
  successful bounded feature.

## Legacy schema-1 feature slices

All existing records under [`workstreams/slices/`](workstreams/slices/) using
`schema = "1"` are frozen legacy evidence from the earlier feature-slice
workflow. They may be read to locate prior code, tests, claims, and receipts,
but they are not valid templates or dispatch units for new work.

Do not:

- create another schema-1 feature slice;
- extend or reclassify an existing schema-1 record to represent package work;
- copy its partial source/test set into a package manifest; or
- infer package coverage from its `covered` state.

Migration preserves those records unchanged, associates their evidence with
the owning package manifest, and leaves ledger/status state untouched until
the whole package satisfies its receipt.

## Integration sequence

For each parallel frontier:

1. Generate and audit complete package inventories.
2. Freeze dependency contracts, stable Rust paths, integration seams, and the
   shared Git base.
3. Atomically claim every package and create worktrees.
4. Translate and test packages independently; keep shared seams unchanged.
5. Have stewards land seam changes and generated artifacts in dependency order.
6. Run static ownership/DAG checks.
7. Run one 12-job workspace/differential/live gate for the frozen frontier.
8. Issue receipts, release claims, and regenerate status only after the gate.

A receipted package that needs repair must first be reopened as one complete
package transaction:

```bash
python3 scripts/work-unit-queue.py reopen-package --owner <package-slice>
```

The command accepts only a schema-2 `covered` manifest with its exact current
receipt. It requires a globally quiescent claim/gate state, refuses packages
with covered transitive dependents, removes the receipt, and returns the
manifest to a dependency-ready, legally claimable `ready` state. Reopen covered
dependents first, then their dependencies. Historical campaign manifests and
`workstreams/campaigns/integrated-members.tsv` remain immutable evidence; the
new repair closes through a new campaign and receipt.

If a package cannot close, keep it `blocked`, record the exact missing package
obligations, and continue with another source/test/write-disjoint ready package.
Keep unfinished implementation on its package branch; do not integrate or
split the missing branches into a smaller completion unit.

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

The physical workspace boundaries are described in
[`docs/architecture/workspace.md`](docs/architecture/workspace.md). The next
implementation step is to add the checked package-manifest and package-DAG
format without modifying legacy schema-1 records or generated ledger status.
