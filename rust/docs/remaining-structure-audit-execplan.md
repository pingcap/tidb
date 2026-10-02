# Reconcile the complete remaining structural mismatch register

This living ExecPlan follows `PLANS.md`. This is a review, not a package
transcreation or a production repair.

## Purpose / Big Picture


Give the user one current list of known Go/Rust ownership and lifecycle
differences, including client-rust, without counting completed repairs or
search hits as defects. A structural mismatch is a different owner, lifetime,
state machine, shared contract or production integration, rather than an
isolated expression result. Preserve the distinction between observed failures,
source-confirmed differences, and unreviewed package obligations.

## Context and Orientation


The TiDB checkout is on `hparser-integration`, initially
`771e62b2871890eeae2296cbeed04a19b1316201`. Freshly fetched Go master is
`93a01d31f6da205ae4bf376825293903a6899fdb`. It pins client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee`. Native client-rust at
`/Users/qiliu/projects/client-rust` is current with published master
`6f663b396552eec6d1bfad76b65f813e317884a4`.

The existing register is `rust/docs/parity/current-audit/structural-findings.md`.
Its introductory counts are stale: T03 is repaired, while the header still
counts it as open. The README also retains an obsolete blocked lifetime repair.
The inventories enumerate package artifacts; their review flags do not grant
acceptance and must not be silently upgraded by an inventory refresh.

## Progress


- [x] Refresh both implementation branches and Go master; verify source pins.
- [x] Read the existing register, coverage generator, inventory generator and DDL review instructions.
- [x] Refresh artifact inventories and reconcile all existing findings with current source/history.
- [x] Extend source review to native client ownership and overlooked production boundaries.
- [x] Publish a complete known-open list, corrected counts, evidence and explicit review limits.
- [x] Validate documentation/evidence, run the SQL diagnostic, formatter, diff check and root lint.
- [x] Pass the actual locked-build pre-commit hook and a separate locked server build; final publication repeats the build after the final commit and records the remote result in the completion message.

### Follow-up at integration 313b3cfea3


- [x] Fetch both implementation branches and Go master; integration is 313b3cfea3500e3026a862d2be11a7fbb3d65481, native is 6163ecfc587b248dcbf0e30c1c9d905b4bc5a665, and Go master is unchanged.
- [x] Compare all recorded source references and intervening production changes; identify stale PD descriptions and partition changes requiring fresh review.
- [x] Rerun retained SQL/wire diagnostics and check partition data preservation.
- [x] Publish a per-ID disposition for all 77 unresolved findings, update stale current descriptions, and retain historical evidence separately.
- [x] Validate all IDs, assignments, source blobs, links and output cleanup; check diagnostic formatting, run git diff --check and make lint.
- [x] Confirm the actual commit hook and post-commit locked build pass. The final receipt amendment repeats both gates before the normal branch push; its remote revision is recorded in the completion message.

## Plan of Work and Milestones


First regenerate inventory using the existing script and compare its digests
with the old snapshot. Read the changed upstream source and the Rust production
owners for findings affected by recent work. For unchanged findings, preserve
the original reproduction receipts and record source continuity rather than
pretending to have rerun those experiments.

Then trace native client construction, request context, routing, health and
shutdown, and survey each subsystem queue. Add a finding only after identifying
the corresponding upstream owner and a concrete differing Rust boundary.
Unimplemented package receipts and markers are review candidates until checked.
Do not duplicate a single root cause across multiple finding IDs.

Finally update the register and README and write a review receipt with source
anchors and explicit scope. Preserve historical receipts. No behavior changes
are part of this audit; package-level acceptance requires separate complete
implementation and validation work.

## Concrete Steps and Validation


From the repository root:

    python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master
    python3 rust/scripts/build-structural-coverage.py
    git diff --check
    make lint

Check that finding IDs are unique, open/repaired totals sum correctly, source
anchors exist, and every Rust crate and inventoried Go package remains assigned
exactly once. Run the actual pre-commit hook via `git -c core.hooksPath=hooks
commit`; it must run `cd rust && cargo build --locked -p tidb-server`. Rerun that
same locked build after the final commit and before pushing. No SQL reproduction,
distributed failure or benchmark is claimed unless executed and recorded here.

## Surprises & Discoveries


The recent health repair shares native state but TiDB's latency, feedback and
periodic tick producers still require integration. This belongs to open T02;
deleting duplicate types alone does not close the routing-owner migration.
The read-side health-feedback admission still differs under mutex contention
(T04), despite repaired nonblocking writers. At the original review baseline,
native TSO lacked a deadline/join owner (P06); the follow-up retains the now
published prerequisite repairs. Native PD metadata RPCs retain a global write lock over network
awaits (P07). Eight further missing production boundaries were consolidated.

Statement-summary settings/readers did not establish a live writer. After both
summary switches were enabled, the SQL diagnostic executed CREATE/INSERT/SELECT
but read zero cumulative statement rows. The first diagnostic's incorrect
variable name/scope were corrected; only the valid run is retained as evidence.

At the 313b3cfea3 follow-up, all six SQL/wire diagnostics completed. Accepted
repartition switches routing without migrating rows: both RANGE-to-HASH and
nonpartitioned-to-HASH make old rows invisible. This is stronger evidence for
existing D01, not another independently counted owner. Source review of the
cluster route also finds ADD/DROP metadata assumptions and a thread-local
metadata handoff in place of durable reorganization. No live TiKV repartition
was run. Earlier privilege and FK runtime repairs remain intact in the probes.

## Decision Log


- Decision: retain all existing IDs and distinguish partial from repaired.
  Rationale: source continuity and regression receipts must remain traceable.
  Date: 2026-10-01.
- Decision: use source continuity for unchanged previously reviewed findings.
  Rationale: it is stronger than stale line numbers but is not a new runtime test.
  Date: 2026-10-01.
- Decision: retain 77 unresolved IDs and attach the new partition failure to D01.
  Rationale: one absent durable DDL owner has multiple concrete consequences;
  counting each symptom separately would distort the repair scope. Correct P06
  and T02 for accepted prerequisites without accepting their parent packages.
  Date: 2026-10-01, follow-up at 313b3cfea3.

## Idempotence and Recovery


Inventory regeneration is deterministic for a pinned Go reference and installed
module contents. It does not mutate production code or accepted receipts. Keep
unrelated user changes untouched. Retry failed read-only checks independently;
do not reinterpret a missing tool or test failure as parity evidence.

## Outcomes & Retrospective


The review now records 85 findings: 77 unresolved and eight repaired. All 77
open entries appear once in the concise review and are linked to the detailed
register. The refreshed inventory accounts for 969 Go package directories and
83 Rust crates, without semantic acceptance. Eleven entries were newly added
to the register; several consolidate older explicit package-boundary receipts.
The new Go skyline range-count variable is separately recorded as functional
upstream drift rather than inflating the structural root-cause count.

The diagnostic, source/ID/link checker, scope generator, formatter, diff check
and root lint pass. The real `core.hooksPath=hooks` commit ran the locked server
build successfully; a separate post-commit locked build also passed. Logs are
`/private/tmp/tidb-remaining-structure-commit.log` and
`/private/tmp/tidb-remaining-structure-prepush.log`. This validation note requires
a final commit amendment; its hook and the mandatory fresh build before push
are recorded in the corresponding `-commit-final.log` and `-prepush-final.log`
files when published. Git remote verification and the completion message record
the final revision without embedding a self-referential commit hash here.

No production fix, whole-package acceptance, distributed failure or workload
improvement is claimed. The review distinguishes existing repaired owners from
missing integration, instead of turning stale receipt wording into deletions.

### Follow-up outcome at 313b3cfea3


The current review retains 72 open, five partial and eight repaired statuses.
Six diagnostics completed; their outputs preserve both unresolved symptoms and
earlier successful repairs. The new partition result strengthens D01. Published
PD prerequisites remain accepted only at their recorded scope, and the isolated
grpcutil candidate remains rejected. Current source continuity covers all 85 IDs
and 138 literal path references, with 13 changed preexisting references reviewed.

The per-ID/row/link/blob checker, diagnostic rustfmt check, git diff --check and
make lint pass. Root lint log: /private/tmp/tidb-structural-recheck-lint.log.
Only freshly built review example executables/dependency files are removed to
reclaim space; sources, recorded outputs, compiler logs and shared build objects
remain. Cleanup details: /private/tmp/tidb-structural-recheck-cleanup.json.
Cleanup reclaimed 933,588,992 allocated bytes (about 890 MiB). The actual hook
passed its locked server build (0.36 seconds), and the separate post-commit
locked build passed (13.29 seconds). Logs are
/private/tmp/tidb-structural-recheck-commit.log and
/private/tmp/tidb-structural-recheck-prepush.log. This validation note's final
amendment must repeat the hook and then the post-commit build before push;
those final gate logs use the same names with a -final suffix. The completion
message records the final remote revision without embedding a self-referential
commit hash in the receipt. Native client-rust is unchanged by the review.
