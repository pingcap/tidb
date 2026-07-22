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
- [x] (2026-07-22) Rewrote the design, handoff, and execution protocol around
  complete package ownership and removed dated campaign history.
- [x] (2026-07-22) Added and tested the fail-closed schema-2 package manifest,
  package inventory view, frozen claims, legacy registry, direct-import
  closure, campaign close transaction, and immutable package receipts.
- [x] (2026-07-22) Generated and reviewed the first dependency-ready package
  frontier: `pkg/parser/format` and `pkg/server/internal/handshake`.
- [x] (2026-07-22) Claimed source/test/support-complete, Rust-write-disjoint
  `pkg/parser/format` and `pkg/server/internal/handshake` package manifests.
- [x] (2026-07-22) Built and initially gated the first package frontier from Go
  source and the declared original test/support inventory. The parser-format
  package spans `tidb-ast` and `tidb-datatype`; its later audit and receipt
  reopening are recorded below.
- [x] (2026-07-22) Ran the reused 12-job integration gate and issued receipts
  for both packages.
- [x] (2026-07-22) Added an atomic `reopen-package` transition and used it when
  a later audit invalidated the `pkg/parser/format` receipt.
- [x] (2026-07-22) Regenerated status: one schema-2 package remains covered,
  `pkg/parser/format` is ready for complete repair, no package claim remains
  active, and all remaining source/test rows stay partial or untriaged.

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
- Observation: source inventory closure alone does not prove dependency
  closure.
  Evidence: the first draft selected `pkg/server/internal/dump`, but its Go
  source imports uncovered `pkg/parser/mysql` and `pkg/types`. The package was
  changed to `blocked`, and schema-2 validation is being extended to derive
  and enforce direct internal imports rather than trusting `depends_on = []`.
- Observation: `pkg/parser/format` is an immediate real example of one Go
  package mapping to multiple Rust crates.
  Evidence: its stateful formatter and output escaping belong to
  `tidb-datatype`, while its restore flags, context, special-comment writer,
  and CTE scope belong to `tidb-ast`; both must close under one package receipt.
- Observation: package-owned Rust leaves and shared crate integration seams
  cannot use the same durability rule.
  Evidence: crate roots and existing consumer tests are edited by later
  packages, while package receipts must continue to hash the stable
  implementation and mirrored tests. Schema 2 now freezes `rust_paths` as
  exclusive content-bound ownership and records separately checked
  `integration_paths` whose bytes are gate-attested but not permanently owned.
- Observation: focused package Clippy is necessary but not sufficient for a
  Ready frontier.
  Evidence: the first full workspace gate found a `needless_borrow` in the Go
  package-support ledger binary outside the three implementation crates. The
  gate rolled back, the warning was fixed, and the complete gate then passed.
- Observation: a takeover claim created after implementation commits can bind
  current content and evidence but cannot retrospectively prove commit-scope
  isolation.
  Evidence: the first frontier existed before schema-2 `base_commit` claims.
  Future package claims must be created before their implementation commits so
  the committed-diff guard can reject every out-of-claim Rust change.
- Observation: a passing package campaign can still contain a false-positive
  receipt when its semantic oracle or consumer write set is incomplete.
  Evidence: the `pkg/parser/format` audit found Rust full-Unicode casing where
  Go applies simple-rune casing, different short-write behavior, and an omitted
  production consumer integration path. The receipt was reopened rather than
  preserving an incorrect covered count.

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
  weaken upstream package acceptance. Crate-local intermediate results remain
  staging under one implementation owner; no sub-result is independently
  integrated or counted complete.
  Date/Author: 2026-07-22 / Codex.
- Decision: reject a ready package when any direct internal TiDB import is
  neither inside the same umbrella manifest nor represented by a covered
  schema-2 dependency.
  Rationale: existing Rust helpers for an imported type are partial evidence,
  not proof that the dependency package contract is complete.
  Date/Author: 2026-07-22 / Codex.
- Decision: distinguish stable package-owned Rust paths from shared
  integration paths in every schema-2 manifest and receipt.
  Rationale: stable files need exclusive ownership and durable content hashes;
  crate roots, dispatch, and shared consumer seams need exact gate-time scope
  without making later legitimate integration invalidate an earlier package.
  Date/Author: 2026-07-22 / Codex.
- Decision: close every dependency frontier with `campaign_close.py --gate`
  as one rollback transaction.
  Rationale: per-member promotion could expose half-integrated packages. The
  campaign close now requires exact COVERED source/test evidence, current
  support dispositions, a shared gate receipt, and all package receipts before
  it releases any claim.
  Date/Author: 2026-07-22 / Codex.
- Decision: execute package/frontier implementation through one owner and
  serialize all mutable integration seams.
  Rationale: whole-package responsibility, evidence, and rollback stay
  unambiguous; concurrency is not allowed to split acceptance or create a
  second authority.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: treat a package receipt as durable evidence for an exact accepted
  revision, not as irrevocable truth.
  Rationale: later semantic or inventory discoveries must lower reported
  coverage immediately. `reopen-package` performs the fail-closed transition
  and refuses dependency-inconsistent reopening.
  Date/Author: 2026-07-22 / Qiliu and Codex.

## Outcomes & Retrospective

The whole-package workflow and atomic receipt lifecycle are implemented.
`pkg/server/internal/handshake` retains a content-bound receipt.
`pkg/parser/format` does not: its receipt was reopened after a semantic and
consumer-path audit found missing parity. The earlier integration campaign
remains historical evidence that its then-declared gates passed; it is no
longer current package-completion evidence.

The immediate outcome is to repair and re-gate the complete
`pkg/parser/format` package, then select the next dependency-closed frontier
from the checked DAG. Every claim must precede implementation commits, and the
single owner must declare stable leaves and all mutable integration seams.

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

Fourth, generate each frontier from the checked package view. Prefer
packages that are dependency-ready, advance the connected SQL-node path, and
have disjoint Rust writes. Small packages may prove the workflow, but the same
frontier must include a high-value dependency package so completion is not
gamed through trivial package counts. Every candidate receives an independent
inventory/dependency audit before selection.

Finally, transcreate each selected package directly from all Go production and
test/support artifacts. A package that cannot close stays unintegrated and
reports its exact dependency blocker. The implementation owner lands shared
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

Create one branch/worktree for the active accepted package under
`rust/.worktrees/` and use the `codex/` branch prefix. Run focused checks during
implementation. After a meaningful frontier freezes, close it from `rust/`
with one checkout-specific target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/tidb-rust-package-gate \
      python3 scripts/campaign_close.py --campaign <campaign> --gate

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
contract, or integration-path ownership instead.

## Artifacts and Notes

The policy is defined in
`docs/design/2026-07-11-tidb-rust-rewrite.md`, `rust/HANDOFF.md`, and
`rust/PARALLEL.md`. The schema-2 implementation, governance regressions, first
frontier manifests, exact evidence, and immutable package receipts are checked
artifacts. The next plan revision must name the next dependency-closed package
frontier before any new Rust implementation commit is made.

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
