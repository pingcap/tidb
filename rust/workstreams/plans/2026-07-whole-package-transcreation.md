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
- [x] (2026-07-22) Repaired and re-gated the complete `pkg/parser/format`
  package: generated all 2,879 non-identity Go Unicode-15 simple-case mappings,
  preserved arbitrary bytes and one-call writer results, declared the missing
  production consumer seam, and issued a fresh immutable receipt through a
  one-member atomic campaign. Stable status now reports two covered schema-2
  packages, the live queue reports zero active claims, and all remaining rows
  stay partial or untriaged.
- [x] (2026-07-22) Refactor the close loop so guarded generated-state
  preparation and static validation happen before workspace Clippy/tests, and
  transient ignored claims cannot stale the tracked dashboard. Added exact
  owner/anchor rollback regressions and passed static inventories, 100 Python
  governance tests, workspace Clippy/tests, parser isolation, and repository
  `make -j12 lint`.
- [x] (2026-07-22) Audited, repaired, fully gated, and receipted the complete
  `pkg/parser/mysql` package across `tidb-error` and `tidb-mysql`. The proof now
  compares all 954 error constants, 952 error-name entries, and 244 SQLSTATE
  entries against a source-derived Go oracle and executes the checked Unicode
  generator from the package test target.
- [x] (2026-07-22) Removed tracked campaign choreography from the ordinary
  single-package close path. `campaign_close.py --package <owner> --gate` now
  derives the atomic transaction directly from the exact active claim; tracked
  campaigns remain only for dependency-inseparable multi-package frontiers.
  Reopening and re-closing `pkg/parser/mysql` through the new path passed the
  real full gate and replaced only its receipt with a direct-close schema-2
  receipt; campaign state and membership history were unchanged.
- [x] (2026-07-22) Audited, repaired, fully gated, and directly receipted the
  complete `pkg/parser/util` package across `tidb-hash`, `tidb-lexer`,
  `tidb-parser`, and `tidb-planner`. The source-owned hashing interface now
  preserves arbitrary Go string bytes and matches Go's malformed UTF-8 range
  semantics; all original package test/build obligations and consumer seams
  are covered. Stable status now reports four covered schema-2 packages.
- [x] (2026-07-22) Shortened the shared close gate without weakening its proof:
  build evidence tools once, fan out all independent read-only static checks,
  and use the existing begin/finish workspace digest to replace the duplicate
  post-test static pass. On the same warm target, one static stage fell from
  20.58 seconds to 9.70 seconds; a close now avoids about 31 seconds of
  previously serialized or repeated static work.
- [x] (2026-07-22) Removed excluded build-volume traversal from gate hashing.
  The previous `Path.rglob` walked 541,184 files in the 33 GiB local
  `rust/target` tree on every digest even though every target artifact was then
  discarded. Directory pruning preserved byte-for-byte digest results while
  reducing the gate digest from 12.363 to 0.272 seconds and the release digest
  from 12.130 to 0.208 seconds.
- [x] (2026-07-22) Audited, repaired, fully gated, and directly receipted the
  complete `pkg/parser/opcode` package. The Rust authority now preserves Go's
  machine-width `int` domain and exact one-call byte-writer behavior rather
  than narrowing those contracts to `i32` and `fmt::Write`. Stable status now
  reports five covered schema-2 packages.
- [x] (2026-07-22) Replaced manual next-package setup with a single-worker fast
  path. `frontier` derives dependency-ready package candidates from exact Go
  imports, and `start-package` derives inventory, rings, targets, dependencies,
  manifest, and immutable claim in one rollback-on-failure command. One-pass
  package grouping reduced the existing parser queue query from about 40
  seconds to under 4 seconds; the dependency-checked frontier completes in
  under 3 seconds.
- [x] (2026-07-22) Audited, transcreated, and directly receipted the complete
  `pkg/parser/terror` package, including every original test/build obligation
  and required `tidb-datatype`/`tidb-txnkv` consumer changes.
- [x] (2026-07-22) Removed the full-workspace build from the ordinary package
  hot loop. `close-package` now regenerates and validates the exact package,
  derives all touched Cargo crates, runs only their all-target Clippy, library
  tests, and explicitly touched integration-test targets under the frozen
  receipt, and reserves the complete workspace/governance sweep for an
  explicit grouped checkpoint and push/release readiness. On the warm cache,
  the former three-crate package test sweep took 138.31 seconds; the narrowed
  equivalent took 3.96 seconds.

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
- Observation: campaign cardinality is not a correctness invariant.
  Evidence: the package loader required two members even though one owner is
  executing one whole package at a time. The correct close invariant is one or
  more unique package members that exactly equal the active schema-2 claim set;
  both single-member close and stray-claim rejection now have regressions.
- Observation: the package gate discovered cheap generated-state drift only
  after running the expensive workspace suite.
  Evidence: parser-format close paid three rolled-back full attempts for stale
  test-ledger rendering, transient claim count in `STATUS.md`, and stale
  source-ledger rendering before the fourth attempt passed. None of the three
  late failures was a semantic Rust failure.
- Observation: a one-member campaign duplicated information already frozen by
  the package manifest and exact claim, yet required a campaign file, status
  mutation, archive row, and extra commits around every package.
  Evidence: closing `pkg/parser/mysql` changed three campaign bookkeeping
  surfaces in addition to the manifest and receipt even though the campaign had
  exactly one member and the claim already fixed every accepted input.
- Observation: passing existing Rust tests did not prove the input domain of a
  source-owned Go interface.
  Evidence: `pkg/parser/util.IHasher.HashString` accepts arbitrary Go string
  bytes, but the Rust trait accepted only `&str`. Exact malformed UTF-8 oracles
  exposed the narrowing and the complete package close repaired it before
  issuing a receipt.
- Observation: the integration gate serialized independent readers and then
  reran all of them after testing, although `gate-finish` already rejects any
  checked-input mutation by digest.
  Evidence: the warm static stage took 20.58 seconds, including a 6.666-second
  Go-test-ledger scan. Concurrent execution reduced it to 9.70 seconds, and
  removing the redundant second pass preserves the same frozen-input
  attestation while avoiding another full static stage.
- Observation: excluding `target/` after recursive enumeration does not exclude
  its cost.
  Evidence: the local 33 GiB target contained 541,184 files. The old digest
  spent over 12 seconds walking them per call and a one-package close invoked
  workspace digests repeatedly; top-down pruning produces the identical digest
  in under 0.3 seconds.
- Observation: the old queue ranked work before checking dependency closure and
  recomputed nearest-package ownership for every package-row pair.
  Evidence: after `pkg/parser/opcode` closed, it reported zero ready manifests;
  a parser queue query made 15,842,190 `_nearest_package` calls and took about
  40 seconds while still putting the dependency-blocked root parser package
  first. One-pass grouping plus direct-import frontier filtering now returns
  only the two actually startable parser packages in under 3 seconds.
- Observation: crate-only integration paths made dependency-bearing package
  claims impossible to complete honestly.
  Evidence: the first `pkg/parser/terror` start needed JSON/logging dependencies
  in `tidb-error`, but the generated Cargo lock sits at `rust/Cargo.lock`, above
  every target crate. Schema 2 now permits only the workspace Cargo manifest
  and lock as shared root integration seams and keeps both claim- and
  gate-attested.

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
- Decision: allow a one-package atomic campaign and require campaign membership
  to equal the complete active package-claim set.
  Rationale: batching unrelated packages does not increase parity confidence;
  exact claim closure prevents a gate from attesting or releasing work outside
  the receipt transaction.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: prepare only exact active-claim ledger anchors and run static gates
  before expensive workspace validation.
  Rationale: the final generated owner and anchor must belong to the frozen
  claim, so legacy-to-package promotion remains legal while unrelated or
  unowned churn rolls back. Static-first ordering shortens failed loops without
  removing any acceptance proof.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: tracked `STATUS.md` records stable manifest/receipt state, not
  ignored claim leases.
  Rationale: a tracked generated file cannot remain current against untracked
  transient inputs without forcing meaningless implementation and release
  commits. Live claim state remains authoritative through the queue command.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: derive the normal single-package close from its exact schema-2
  claim; reserve tracked campaigns for two or more packages that cannot be
  accepted independently.
  Rationale: the claim already supplies the complete membership, base revision,
  inventory, and write set. Removing the duplicate campaign record shortens the
  loop without weakening the shared gate, rollback, or immutable receipt. This
  supersedes using a one-member campaign as the ordinary close form.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: run independent read-only static gates concurrently once per close
  and let the existing gate-begin/gate-finish digest prove their inputs remained
  unchanged through workspace tests.
  Rationale: rerunning deterministic readers on byte-identical inputs adds
  latency but no evidence; the digest fails closed on any mutation.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: prune excluded workspace directories during digest enumeration,
  while preserving the prior path ordering and exact digest algorithm.
  Rationale: ignored Cargo artifacts are not attestation inputs, so traversing
  them is pure scale-dependent overhead and can make the safety mechanism cost
  more as the cache becomes more useful.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: replace the multi-worker ready queue as the normal entrypoint with
  a dependency-filtered `frontier` and a single-worker `start-package`
  command.
  Rationale: with one owner, pre-authored ready manifests and lease scheduling
  create idle gaps without adding isolation. Candidate derivation, manifest
  validation, and exact claiming should happen just in time, while receipts and
  the close gate continue to protect correctness.
  Date/Author: 2026-07-22 / Qiliu and Codex.
- Decision: ordinary package close validates only the exact package inventory
  and Cargo crates containing its declared Rust paths; reserve the full
  workspace/governance sweep for explicit grouped checkpoints and
  push/release readiness.
  Rationale: package preflight already proves exact ownership and inventory.
  Rebuilding every unrelated crate after every leaf adds release-level latency
  without package-level evidence. The frozen receipt still fails on mutation,
  and checkpoints preserve cross-workspace integration proof.
  Date/Author: 2026-07-22 / Qiliu and Codex.

## Outcomes & Retrospective

The whole-package workflow and atomic receipt lifecycle are implemented.
`pkg/server/internal/handshake`, repaired `pkg/parser/format`,
`pkg/parser/mysql`, `pkg/parser/util`, `pkg/parser/opcode`, and
`pkg/parser/terror` now have current content-bound receipts. The parser-format
repair executes the complete
generated Go simple-case oracle through the public Rust restore context and
adds byte and writer-boundary regressions; its receipt also records the real
field-type consumer seam that the earlier manifest omitted.

The immediate outcome is to start the next dependency-ready parser package
through the new single-worker command. `frontier` currently identifies
`pkg/parser/terror` and `pkg/parser/duration`; `pkg/parser/terror` is the larger
and more connected next unit. The owner must still audit its complete Go
contract and declare every stable Rust leaf and mutable integration seam before
`start-package` creates the manifest and claim.

Before that claim, the close loop was shortened at the mechanism layer:
active-package ledger rows are regenerated under exact claim scope, independent
static checks fan out before workspace work, the content digest replaces a
duplicate post-test pass, and the stable dashboard no longer depends on ignored
leases. This changes failure latency, not package acceptance.

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

Fourth, select each frontier with `work-unit-queue.py frontier`, which derives
direct internal imports and reports only packages whose schema-2 prerequisites
are covered. Start the selected package with `start-package`, which writes and
validates its manifest and creates the exact claim in one single-worker
operation. Use a dependency-closed package group only when one package cannot
close independently; never batch unrelated packages to satisfy a count.

Finally, transcreate each selected package directly from all Go production and
test/support artifacts. A package that cannot close stays unintegrated and
reports its exact dependency blocker. The implementation owner lands shared
seams in dependency order and closes an ordinary package directly from its
claim. A tracked campaign is created only when several packages are
inseparable. Ordinary close validates its touched crates and issues the frozen
receipt; grouped checkpoints run the reused 12-job full workspace gate before
push/release readiness.

## Concrete Steps

Run governance checks from the repository root:

    python3 rust/scripts/test_work_unit_queue.py
    python3 rust/scripts/work-unit-queue.py check
    python3 rust/scripts/work-unit-queue.py frontier --target <crate> --limit 20
    git diff --check

Declare the complete Rust write set, then scaffold and claim the package:

    python3 rust/scripts/work-unit-queue.py start-package \
      --package <go-package> \
      --rust-path rust/crates/<crate>/src/<stable-leaf>.rs \
      --integration-path rust/crates/<crate>/src/lib.rs

Work in the current checkout when there is one worker; a branch/worktree is an
optional isolation mechanism, not part of package acceptance. Run focused
checks during implementation. After the package freezes, close it from `rust/`
with one checkout-specific target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/tidb-rust-package-gate \
      python3 scripts/work-unit-queue.py close-package --owner <package-owner>

At a grouped integration/push boundary, run the full checkpoint once:

    CARGO_BUILD_JOBS=12 scripts/rewrite-gate.sh checkpoint

Expected governance output contains zero active legacy partial claims and a
package table whose expanded counts match the ledgers. Expected close
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

`work-unit-queue.py close-package --owner <owner>` prepares derived source/test
rows before its close preflight. Preparation restores its snapshot on generator,
scope, or preflight
failure. Once preparation succeeds, a later touched-crate gate failure retains the
now-current derived files but still rolls back every receipt, status transition,
and claim release; the next attempt therefore does not repeat bookkeeping.

If worktrees are used, isolate them by complete upstream and Rust write sets.
With one worker, keep the current checkout and preserve unrelated files. Never
delete or rewrite schema-1 evidence to resolve an overlap; fix the package
manifest, dependency contract, or integration-path ownership instead.

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
Claims persist those arrays as frozen evidence. Direct package close, campaign,
and receipt tooling must consume the same expansion instead of maintaining
another package parser.

The generated Go ledgers are inputs and are never hand-edited. Package
dependency records refer only to complete package manifests or explicit
checked external capabilities. Rust crates remain implementation targets;
package manifests and receipts remain the acceptance authority.

Revision note (2026-07-22): replaced the historical feature-slice campaign
plan after the whole-package transcreation directive made its work units
invalid. Git preserves the retired plan history.
