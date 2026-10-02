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

### Review after removals at integration 68d6de685a


- [x] Fetch integration, Go master and native master; all are current. Native
  remains 6163ecfc and Go remains 93a01d31; no dependency update is required.
- [x] Read all 77 unresolved entries and eight repaired entries. Compare all
  138 prior literal source references and every intervening production change;
  21 references changed, with no native or upstream source change.
- [x] Rerun six retained SQL/wire diagnostics into separate outputs; confirm
  removed shortcuts now refuse while current correctness defects still occur.
- [x] Publish per-ID source continuity and a complete owner/prerequisite review;
  reconcile obsolete live-path wording without closing missing-owner findings.
- [x] Validate counts, assignment, source evidence, links and temporary cleanup
  with python3 /private/tmp/check-post-removal-review.py; git diff --check passes.
- [x] Pass the actual locked-build hook and a fresh post-commit build. Repeat
  both gates for the final receipt amendment; publication and remote verification
  are recorded in the final task result.

This follow-up is a review only. No production source, original fixture or
package acceptance is changed. Preserve the older reproduction files as history;
current observations live under parity/current-audit/post-removal-recheck.
Use the prior review's diagnostic runner with post_removal_ example names, and
keep known passing controls and source-only limits. The review should make clear
which code is already retired, which needs replacement before retirement, and
which useful owner merely lacks production construction/callers.

### Review after shared worker repairs at cfc6a174bb


- [x] Fetch integration and Go master; both are unchanged at cfc6a174bb and 93a01d31. Native dependency remains 6163ecfc.
- [x] Read every current unresolved description and inventory all 18 changed crate paths since the previous full review at 68d6de685a.
- [x] Reconcile every unresolved ID against current owners, production consumers and source continuity; distinguish inactive seeds, missing runtime and reproduced failures.
- [x] Rerun the six retained SQL/wire diagnostics and the controlled C04 admission probe; review DDL repair regressions and remaining source boundaries.
- [x] Publish the supported count and per-ID evidence; correct stale descriptions without inflating closure into package acceptance.
- [x] Validate evidence/counts/links and pass the actual locked-build hook plus a fresh post-commit build. The final receipt amendment repeats both gates before push; remote verification is recorded in the completion message.

This review asks whether the existing 75 unresolved findings remain true, rather
than assuming an unaccepted package proves a defect. Carry prior evidence only
where source, inputs and callers remain unchanged. Inspect all intervening runtime
changes, including source sites outside each finding's original reference list.
Record current observations under parity/current-audit/worker-followup-recheck;
do not overwrite historical diagnostic outputs. Retain repaired D04/D06 and the
repaired portions of D05 explicitly. Inactive MV seeds must not be counted as live
runtime defects. No production repair is included in this review.


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


- Decision: keep 75 unresolved IDs while separating 73 live/missing-runtime
  contracts from two disabled MV seeds; remove repaired symptoms from current
  allegations without treating a partial repair as whole-ID closure.
  Rationale: the current source and diagnostics support each remaining contract,
  but only 14 IDs have fresh symptom/refusal observations. Old D05 blanket error
  conversion and E02 orphan acceptance are no longer valid allegations.
  Date: 2026-10-02, follow-up at cfc6a174bb.
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


At the 68d6de685a post-removal review, all six retained diagnostics completed.
Repartition refusal preserves existing rows; IMPORT refuses without writing;
CLUSTER_CONFIG refuses instead of returning its capture. D11, A04, K03 and N01
still reproduce their active correctness failures. Source-only GC/cache/account
risks remain explicitly untested in a distributed deployment. No production
code changed; all 77 unresolved and eight repaired statuses are retained.
The new report, source continuity and six outputs are under current-audit;
README, register headings and repair sequence now distinguish live from retired
behavior. This is a review milestone, not completion of the parity goal.


The post-removal evidence checker passes all 85 status comparisons, all 77
workstream assignments, 138 current/prior source references (21 changed), six
fresh outputs, report links and temporary cleanup. Only rust/docs files changed.
The shared persisted-job function remains byte-identical for D04/D05/D07.
No new production/test code requires a fresh make lint run; the prior runtime
change's lint and regression receipt is retained, not represented as rerun here.


The post-removal review's actual hook passed cd rust && cargo build --locked
-p tidb-server in 0.29 seconds. The separate post-commit locked build passed in
12.93 seconds. Logs: /private/tmp/tidb-post-removal-review-commit.log and
/private/tmp/tidb-post-removal-review-prepush.log. The final receipt amendment
reruns both gates, recorded with -commit-final.log and -prepush-final.log, before
git push origin HEAD:hparser-integration. No production or native-client change
is included in this review commit.


### Review outcome at cfc6a174bb


All 75 unresolved IDs remain valid unmet contracts: 69 open, six partial; 73
concern live paths or missing runtime integration and D09/D10 are disabled seeds.
Ten repaired IDs remain repaired. Six diagnostics completed and the controlled
LFU admission regression remains red, providing fresh symptom/refusal evidence
for 14 IDs. The other 59 live/missing-runtime gaps retain source/caller evidence.
The old system-table DDL test still fails at the p1 catalog lookup; its cause
remains unclassified rather than becoming another structural ID.

Current evidence includes all 85 dispositions, 124 prior source references for
unresolved entries (22 changed across 16 IDs), all 18 intervening crate paths,
and a specific assessment for each unresolved ID. The review corrects stale D05
blanket-1105 and E02 orphan-acceptance wording and distinguishes retired unsafe
shortcuts from complete missing owners. No production code or acceptance claim
changes. The receipt is parity/current-audit/worker-followup-structural-review.md.
The checker command python3 /private/tmp/check-worker-followup-review.py passes
all ID/status/row/source/link/output/cleanup assertions; git diff --check also
passes. The actual commit hook passed its locked server build in 0.68 seconds;
the separate post-commit locked build passed in 20.43 seconds. Logs are
/private/tmp/tidb-worker-followup-review-commit.log and
/private/tmp/tidb-worker-followup-review-prepush.log. The final receipt amendment
repeats the actual hook and fresh build with -commit-final.log and
-prepush-final.log before the normal push. No production code changed, so no
additional lint or broad suite is required. The two reviewed failing tests remain
explicit unresolved evidence, not a successful test-suite claim.

Only the six fresh diagnostic executables and their six dependency files were
removed, reclaiming 932,683,776 allocated bytes (about 890 MiB). Sources, outputs,
compiler logs and shared build objects remain; the exact local cleanup manifest
is /private/tmp/worker-followup-cleanup.json.
