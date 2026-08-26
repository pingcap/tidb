# Port all Go unit tests from master to the Rust workspace

This ExecPlan is a living document per `PLANS.md`. Keep `Progress`,
`Surprises & Discoveries`, `Decision Log`, and `Outcomes` up to date as work
proceeds.

## Purpose / Big Picture

TiDB is being transcreated from Go into Rust under `rust/`. Repository policy
(`AGENTS.md`, non-negotiable 6) makes one complete upstream Go package —
including its **original test/support artifacts** — the minimum unit of a
transcreation claim. This plan ensures that every Go package's unit tests from
`master` are ported onto `hparser-integration` as Rust tests in the owning
crate, split by Go package and module across parallel agents, with a
supervision loop that lands verified work until the manifest is complete.

Scope order: packages mapped by the `rust/README.md` ownership table first;
other modules (`br`, `dumpling`, `lightning`) are out of scope for wave one.

## Method

- Agents translate each Go `func TestXxx` into Rust tests following each
  crate's existing `tests_*.rs` conventions, re-deriving intent from the Go
  source on `origin/master` (never trusting Rust comments — see
  `PARTITION_GO_PARITY_EXECPLAN.md`).
- Every batch runs a scoped gate: `cargo nextest run -p <crates> --no-fail-fast`;
  pass requires failure-set ⊆ baseline recorded before edits.
- Unsupported behavior becomes an explicit gap: `#[ignore]` + receipt entry.
  No approximations.

## Topology

- 3 idle PD pods (testbeds 8227447 / 8180329 / 8203166), one evot agent each.
- Each agent works in its own git worktree on branch `testport/<pod>-<batch>`
  and pushes that branch; a landing supervisor cherry-picks onto
  `hparser-integration`, re-runs the gate against the receipt's baseline,
  fast-forwards and pushes. Batch state machine:
  pending → pushed → landed | failed-final | land-failed-final.
- Canonical batch inventory: `rust/testport/MANIFEST.json`; receipts under
  `rust/testport/receipts/<batch>.md`.

## Progress

- [ ] M0 — bootstrap 3 pods (toolchain, repo clone, evot config, worker loop).
- [ ] M1 — inventory master test surface for mapped packages; cut batches;
      commit MANIFEST.json (this file's sibling) to the branch.
- [ ] M2 — wave execution: agents process queues; supervision loop lands
      batches; progress tracked in MANIFEST.json status fields.
- [ ] M3 — completion: full-manifest terminal states + coverage report
      (translated Go test functions / total in scope).

## Decision Log

- Decision: translation target is the Rust workspace (not syncing *_test.go),
  per AGENTS.md non-negotiable 6. Date/Author: 2026-08-26, dbsid/agent.
- Decision: pod branches + supervisor landing instead of direct pushes, to keep
  `hparser-integration` linear and gated. Date/Author: 2026-08-26.
- Decision: one evot agent per pod (4 CPU / 8Gi limit each); batches capped at
  ~60 Go test functions. Date/Author: 2026-08-26.

## Surprises & Discoveries

- (append-only)

## Outcomes & Retrospective

- (at completion)
