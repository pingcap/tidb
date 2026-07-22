# Enforce and execute whole-package Go-to-Rust transcreation

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` at repository root. This plan follows the repository
requirement for an ExecPlan on a significant refactor.

## Purpose / Big Picture

Make one complete upstream Go package or pinned external-module package the
smallest unit that can be claimed, implemented, integrated, and called
transcreated. After this change, the dispatcher cannot create a new partial
file/function/branch claim, and reviewers can see the exact production,
test/support, dependency, Rust-write, and validation inventory for every
package.

The first observable outcome is governance: a schema-2 package manifest
expands automatically from the checked source and test ledgers, freezes that
expanded inventory in its active claim, and fails when an artifact is missing,
duplicated, or added after the claim. The second outcome is delivery: the first
dependency-ready frontier closes complete packages and produces immutable
package receipts before its Rust code reaches the shared branch.

## Progress

- [x] (2026-07-22) Froze all existing schema-1 feature slices as legacy
  evidence and abandoned active partial-slice claims.
- [x] (2026-07-22) Rewrote the design, handoff, and parallel protocol around
  complete package ownership and removed dated campaign history.
- [ ] Add and test the fail-closed schema-2 package manifest, package inventory
  view, frozen claims, and legacy registry.
- [ ] Generate and review the first dependency-ready package frontier.
- [ ] Claim source/test/Rust-write-disjoint complete packages and create their
  in-repository worktrees.
- [ ] Transcreate the first complete package frontier from Go source and all
  original test/support artifacts.
- [ ] Run one reused 12-job integration gate and issue immutable package
  receipts.
- [ ] Regenerate status and report exact package closure and remaining gaps.

## Surprises & Discoveries

- Observation: the connected Rust SQL node is composed from many useful but
  partial feature/file slices; none of those slices proves a complete upstream
  package.
  Evidence: existing schema-1 manifests enumerate selected source and test
  anchors, while `rust/HANDOFF.md` lists open branches and test families in
  every connected SQL-node subsystem.
- Observation: a Go package can map to more than one Rust crate.
  Evidence: the generated source ledger assigns packages such as `pkg/parser`
  to multiple target crates. Schema-2 therefore records the complete target
  set; a singular Rust target must not make a complete package undispatchable.
- Observation: test-only packages and recursively owned `testdata/**` support
  are real package obligations.
  Evidence: the generated test ledger contains Go test package directories and
  fixture/build/support rows that do not have a production source with the
  same filename stem.

## Decision Log

- Decision: one complete upstream package or pinned module package is the
  minimum implementation and completion unit.
  Rationale: selected files and branches hide unported behavior and original
  tests behind a successful bounded feature proof.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: keep schema-1 manifests immutable as a checked legacy registry.
  Rationale: they remain useful evidence anchors, but accepting new schema-1
  records would preserve the unsafe partial-slice dispatch path.
  Date/Author: 2026-07-22 / Codex.
- Decision: derive schema-2 inventories from authoritative ledgers instead of
  hand-listing flattened anchors.
  Rationale: automatic expansion makes new or omitted package artifacts fail
  closed and prevents a manifest author from selecting only convenient tests.
  Date/Author: 2026-07-22 / Codex.
- Decision: allow multiple Rust target crates but only one package claim and
  receipt.
  Rationale: Rust crate boundaries are compile/ownership mechanisms and do not
  weaken upstream package acceptance. Write-disjoint crate subteams may work
  in parallel and merge into the package staging branch, but no sub-result is
  independently integrated or counted complete.
  Date/Author: 2026-07-22 / Codex.

## Outcomes & Retrospective

The policy and current-state documentation are complete. Queue enforcement and
the first package receipt remain open. Do not report package progress or resume
Rust implementation until the schema-2 checker passes and the frontier's
complete inventories have been independently reviewed.

## Context and Orientation

The authoritative production inventory is
`rust/difftests/corpus/coverage/go_source_inventory.tsv`. The authoritative
original test/support inventory is
`rust/difftests/corpus/coverage/go_test_inventory.tsv`; pinned external module
inventories are adjacent `external_go_*` files. These generated ledgers remain
atomic evidence. `rust/scripts/work-unit-queue.py` groups their rows into
package-sized work, validates active claims and campaigns, and exposes queue
state. `rust/workstreams/slices/` contains frozen schema-1 feature records and
new schema-2 package manifests. `rust/PARALLEL.md` is the executable ownership
protocol, and `rust/HANDOFF.md` records only current architecture and gaps.

A package inventory contains direct production files in the package, direct
test Go files, and support artifacts assigned to the nearest ancestor package.
`testdata/**` belongs recursively to that nearest package. An ordinary nested
directory that contains its own Go package is a separate package. A package
with tests but no production file is still a valid test-only package. Pinned
external packages use a module-qualified identity such as
`client-go::internal/client` so equal paths in different modules cannot
collide.

## Plan of Work

First, extend `rust/scripts/work-unit-queue.py` with schema 2. A schema-2
manifest declares package identities, the complete set of Rust target crates,
dependencies, mutable Rust paths, consumers, and validation gates. It must not
declare flattened source or test anchors. Loading the manifest expands every
checked source/test/support row and rejects invalid selectors, mixed or missing
package identity, unknown fields, target-set drift, and an absent legacy
schema-1 registry.

Second, make claims immutable inventory snapshots. `claim-slice` freezes the
expanded source, test, external-source, and external-test arrays. Validation
requires the claim owner and manifest identity to match and re-expansion to be
byte-for-byte equivalent after sorting. Raw partial schema-2 claims and schema-2
claim amendment are invalid. Readiness, overlap, campaign, receipt, and Rust
path checks consume all four inventory dimensions.

Third, expose a read-only package inventory view. It reports package identity,
all Rust targets, production source count and lines, original obligation count,
aggregate status, and active owner. It includes test-only packages and packages
whose production rows are covered but tests remain open.

Fourth, generate the first frontier from the checked package view. Prefer
packages that are dependency-ready, advance the connected SQL-node path, and
have disjoint Rust writes. Small packages may prove the workflow, but the same
frontier must include a high-value dependency package so completion is not
gamed through trivial package counts. Every candidate receives an independent
inventory/dependency audit before dispatch.

Finally, transcreate each selected package directly from all Go production and
test/support artifacts. A package that cannot close stays unintegrated and
reports its exact dependency blocker. The integration steward lands shared
seams in dependency order, runs one reused 12-job Ready gate, issues receipts,
releases claims, and regenerates status.

## Concrete Steps

Run governance checks from the repository root:

    python3 rust/scripts/test_work_unit_queue.py
    python3 rust/scripts/work-unit-queue.py check
    python3 rust/scripts/work-unit-queue.py packages
    git diff --check

Inspect a package manifest and atomically claim it:

    python3 rust/scripts/work-unit-queue.py claim-slice \
      --owner <package-owner> --slice <package-owner>

Create one branch/worktree per accepted package under `rust/.worktrees/` and
use the `codex/` branch prefix. Package teams run only their focused checks.
After a meaningful frontier freezes, the integration steward runs from
`rust/` with one checkout-specific target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/tidb-rust-package-gate \
      scripts/rewrite-gate.sh integrate

Expected governance output contains zero active legacy partial claims and a
package table whose expanded counts match the ledgers. Expected integration
output includes an immutable receipt for every claimed package. Only then may
a manifest become `covered` and its claim be released as integrated.

## Validation and Acceptance

Queue tests must prove direct package expansion; recursive `testdata`
ownership; nested and prefix-sibling exclusion; test-only packages;
module-qualified separation; invalid selector rejection; missing legacy
registry rejection; new schema-1 rejection; source, test/support, external, and
Rust-path overlap rejection; owner/manifest identity; immutable claim drift;
and readiness suppression on every overlap dimension.

The first transcreated package is accepted only when every generated inventory
row is mapped to Rust implementation or explicit non-applicability evidence,
every original package test/support obligation has a Rust or differential
proof, its focused tests pass, and every applicable static, differential,
fault, and live gate is recorded in the package receipt. A bounded successful
query, compile, source count, or feature test is not acceptance.

Ready validation for code changes also requires repository `make -j12 lint`.
Do not run `make bazel_lint_changed`. Rust-only governance changes do not
require `make bazel_prepare`.

## Idempotence and Recovery

Package inventory generation and checks are read-only and safe to rerun. Claim
creation is atomic under the queue lock and fails on an existing owner or any
overlap. If implementation is abandoned, release only with `--abandon`; never
promote ledger state. If the inventory changes after claim, the checker must
fail. Abandon and recreate the claim from the new checked manifest rather than
amending a schema-2 snapshot.

Package worktrees are isolated by complete upstream and Rust write sets.
Preserve unrelated files in the primary checkout. Never delete or rewrite
schema-1 evidence to resolve an overlap; fix the package manifest, dependency
contract, or steward ownership instead.

## Artifacts and Notes

The policy is defined in
`docs/design/2026-07-11-tidb-rust-rewrite.md`, `rust/HANDOFF.md`, and
`rust/PARALLEL.md`. The schema-2 implementation and its tests are the next
required artifact. The first frontier manifests and receipts will be appended
here as they are accepted.

## Interfaces and Dependencies

`rust/scripts/work-unit-queue.py` remains the sole claim authority. Its
schema-2 loader returns expanded `go_sources`, `go_tests`, `module_sources`,
and `module_tests` internally even though the manifest declares only packages.
Claims persist those arrays as frozen evidence. Campaign and receipt tooling
must consume the same expansion instead of maintaining another package parser.

The generated Go ledgers are inputs and are never hand-edited. Package
dependency records refer only to complete package manifests or explicit
checked external capabilities. Rust crates remain implementation targets;
package manifests and receipts remain the acceptance authority.

Revision note (2026-07-22): replaced the historical feature-slice campaign
plan after the whole-package transcreation directive made its work units
invalid. Git preserves the retired plan history.
