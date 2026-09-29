# Audit structural parity across the Rust workspace

This living ExecPlan follows `PLANS.md`. The user expanded the scope from region
aggregation to ALL structural mismatches against TiDB master. This task discovers
and records evidence; it does not authorize reporting incomplete packages as
transcreated. Existing authorization covers committing and pushing the audit to
`hparser-integration`, with both mandatory locked Rust server builds.

## Purpose / Big Picture


Produce a reproducible package inventory and a current mismatch register spanning
every in-repository Rust crate, so subsequent repairs address shared ownership,
execution paths and lifecycle contracts rather than isolated symptoms. A
structural mismatch means Go and Rust select, compose or share behavior
differently in a way that can change results, diagnostics, persistence,
concurrency or resource use. Different Rust syntax or crate boundaries alone
are not mismatches.

## Progress


- [x] Refresh master and integration branch; working tree starts clean at 2c120bc7fa.
- [x] Read repository policy, architecture index, existing audit index and DDL map.
- [x] Inventory all Rust workspace packages and all upstream Go package artifacts.
- [x] Trace selected live server, session, planner, executor, storage and control-plane paths; full package semantics remain open.
- [x] Revalidate selected existing findings and inspect shared representations and contexts.
- [x] Record 23 source-confirmed families (two also confirmed by paired runtime probes), unresolved candidates and coverage limits.
- [x] Validate inventory, evidence links, negative verifier checks, deterministic regeneration and final `make lint`.
- [ ] Publish through the mandatory locked-build commit hook and immediate pre-push locked build; publication outcome is recorded in the task terminal evidence.

## Surprises & Discoveries


The workspace has 88 Cargo packages. Existing audit documents contain historical
findings, some since fixed. Source comments also describe older entry points.
Neither is sufficient evidence that a mismatch remains live. The preceding
region-group repair found that Go's ordinary mock coprocessor now uses its MPP
executor tree, while Rust still composes a restricted flat scan pipeline.

## Decision Log


- Decision: Cover the whole source/package inventory before selecting repairs.
  Rationale: The user explicitly broadened the audit; another isolated patch
  would not satisfy that request. Date/Author: 2026-09-29, Codex.
- Decision: Separate source-confirmed mismatches from runtime reproductions,
  candidate text matches and unexamined behavior. Never label an unexamined
  package parity-complete. Date/Author: 2026-09-29, Codex.
- Decision: Use Go master source, not the integration branch's Go modifications,
  for authority. Preserve complete package inventories even when a finding
  crosses several Rust crates. Date/Author: 2026-09-29, Codex.

## Context and Orientation


`rust/Cargo.toml` defines the workspace. `tidb-server` hosts the live network
process; `tidb-session`, `tidb-executor`, `tidb-exec`, and `tidb-planner` share SQL
behavior. Many other crates own source-shaped library components. Cargo
dependency reachability only proves that a crate can be linked, not that its
implementation is called. Follow constructors, dispatchers and consumers.

Go master is initially `12b639a1161cd5a60126a47277f5ad14c320fd4a`. The temporary
reference snapshot `/private/tmp/tidb-go-structural-20260927` was refreshed to
that commit in the prior task; all 7,726 tracked blobs matched it. Use Git blobs
or this verified snapshot for comparisons. Current Rust base is
`2c120bc7face45b069dc197ab98a8f001724c140`.

## Plan of Work


Create `rust/docs/audits/structural-20260929/` with a deterministic inventory
generator, package/crate coverage data, source evidence and a human-readable
register. Include complete Go package source, variants, tests, fixtures and build
inputs. Treat source annotations as mapping hints, not acceptance receipts.

Inspect each major subsystem's live dispatch and follow its shared state through
construction, updates, errors, cancellation and cleanup. Recheck known reports
against current code. Record one finding per shared root cause with exact Go and
Rust references, caller evidence, impact, related package inventory and required
repair validation. Keep missing wiring and missing implementation distinct from
confirmed wrong answers. Validate evidence hashes and references mechanically.

## Milestones


The inventory milestone accounts for every workspace crate and upstream Go
package without silently dropping generated/platform/test/support files. The
architecture milestone records verified cross-cutting mismatches in each major
subsystem or explicitly records incomplete inspection. The publication milestone
provides a checked-in, reproducible register with honest coverage and the required
commit/push build evidence.

## Concrete Steps


From the repository root, refresh references and record commit IDs:

    git fetch origin master hparser-integration
    git merge --ff-only origin/hparser-integration
    cargo metadata --offline --locked --no-deps --format-version 1 --manifest-path rust/Cargo.toml

Use `rg` for callers and `git show origin/master:<path>` for Go evidence. Resolve
the enabled Cargo graph with `--filter-platform aarch64-apple-darwin`. The audit
generator and verifier will accept pinned refs and report complete inventory
counts and evidence status. Exact final commands belong in the receipt.

Before committing, self-review all findings and run the verifier. Any tooling
code change requires `make lint`. Commit using `TERM=xterm git -c
core.hooksPath=hooks commit ...`, whose hook runs the locked server build. Rerun
`cd rust && cargo build --locked -p tidb-server` immediately before pushing.

## Validation and Acceptance


Every finding must have real source evidence on both sides and a causal contract
difference. A candidate search hit, ignored test, dependency edge or stale comment
alone is insufficient. The inventory must count every package/crate and preserve
unreviewed status. The final report must not claim that finite static inspection
proves the absence of every other mismatch. Runtime reproductions, when performed,
are reported separately from source analysis. No benchmark improvement or complete
package parity is implied by this audit.

## Idempotence and Recovery


Audit generation is read-only against pinned Git trees and writes only its own
artifact directory. Preserve unrelated changes and the reusable Go snapshot.
Use deterministic ordering and compressed source inventories to limit disk use.
Do not run broad runtime suites merely to inflate evidence; choose a reproduction
only when it distinguishes the structural contract under investigation.

## Artifacts and Notes


The prior group lifecycle receipt is
`rust/docs/planner/region-group-lifecycle-20260929/`. Older indexes such as
`rust/docs/go-divergence-sweep.md` are leads requiring current verification.

## Interfaces and Dependencies


Use Python's standard library, Git blob/tree inspection and Cargo metadata for
inventory tooling. No production dependency or SQL behavior changes are planned.
The finding register must retain status, severity, owning Go packages, Rust
locations, evidence references, impact and next validation requirements.

## Outcomes & Retrospective


Inventory covers 88 Rust workspace crates, 855 Go packages and every tracked
artifact at the pinned baselines. The register contains 23 confirmed structural
families. Paired Go/Rust probes confirm PREPARE metadata/error suppression and
missing sequence metadata. Other families retain source evidence and explicit
validation requirements. Stale leads were rejected rather than counted.

Inventory is complete for the pinned trees; exhaustive semantic discovery is
not. Every Go/Rust package remains not_fully_verified, with per-crate coverage
and all candidate/ignore annotations retained. No implementation fix or benchmark
improvement is claimed. Tool tests, deterministic generation, source verification
and make lint passed; publication gates remain the final step.
