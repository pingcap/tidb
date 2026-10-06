# Retire completed utility audit plans

This living ExecPlan follows root PLANS.md. The preceding model carrier cleanup
was published as ed663646e2841f22efc9e609006007752802caab; its final evidence is in
/workspace/.cloud-setup/model-carrier-cleanup/final-handoff.json.

## Purpose and Context


Remove duplicated historical audit instructions so future work starts from
current findings and maintained test owners. Work in /workspace/tidb on
hparser-integration. Refreshed Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Thirty completed utility plans under
rust/docs/operations repeat inventories and validation boundaries retained in
rust/testport/receipts. They describe September pins as current and carry
completed publication instructions. They are historical records, not current
Cloud startup or package-acceptance gates.

## Progress


- [x] Read candidate plans and compare retained receipt inventories and limits.
- [x] Remove thirty completed plans and repair current/historical references.
- [x] Verify Cargo metadata and retained receipt/source/test continuity.
- [x] Complete root lint, final diff and reference review.
- [ ] Commit through the actual hook; fresh locked build and normal push.
- [ ] Verify the remote SHA and save the cloud checkpoint.

## Milestones and Plan of Work


First inventory the thirty reviewed plans listed in
rust/docs/parity/current-audit/utility-plan-cleanup-validation.json. Preserve
all thirty package receipts. Plans with unchecked work remain. Also keep codec,
serialization, SQLKiller and stringutil plans outside this reviewed removal
inventory; their separate consumer corrections and historical build limits are
not silently discarded.

Next delete the selected plans. Replace 31 references in
rust/testport/TESTPORT_EXECPLAN.md with immutable Git archive links. In israce,
ppcpuusage and prefetch receipts, change only the obsolete current-plan pointer
to an archive link. Keep dated command transcripts unchanged, including old
paths within historical commands. Add a short operations README directing new
work to the current parity register, maintained workflow and retained receipts.

Finally record exact removed-file and retained-receipt hashes. Update both
finding registers' cleanup pointers without changing findings or counts.
Validate the complete documentation batch once, then publish through the normal
repository gates. Do not run unrelated behavioral suites for unchanged code.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust compare before
and after cargo metadata --locked --no-deps --format-version 1 byte-for-byte.
Require every production source, test, script, manifest and lockfile to remain
unchanged. Twenty-seven receipts must be byte-identical; three may differ only
in their archive-pointer text. Every removed plan must have an archive link
anchored at ed663646e2841f22efc9e609006007752802caab and an existing retained receipt.
Check historical references resolve to those archives. From repository root run
make lint and git diff --check. No Go/Bazel changes require bazel_prepare.

Commit normally with executable hooks/pre-commit selected by core.hooksPath=hooks.
It must pass cd rust && cargo build --locked -p tidb-server. Repeat the same
locked build immediately before normal push to origin hparser-integration and
verify the remote SHA. Never bypass hooks or force-push.

## Surprises & Discoveries


Completed audit plans repeat old authority-refresh and publication steps even
when receipts preserve the inventories independently. Three receipts still call
their completed plans current. Some historical receipts disagree about earlier
owner completeness; retain their dated evidence and use the current structural
register for present dispositions rather than treating deletion as acceptance.

## Decision Log


On 2026-10-06 retire only the reviewed completed utility-plan set. Preserve
inventories, missing consumers, platform limits and original validation evidence
in receipts. Archive old plan links rather than rewriting dated evidence into
claims about current Go master. Keep executable tests and source untouched.
This reduces stale instructions, not measured compilation/runtime cost.

## Outcomes & Retrospective


Thirty plans and 1177 lines removed; 31 historical links and three current-plan
pointers repaired. Metadata is byte-identical and code/tests/scripts are
unchanged. Root lint and diff/reference review passed. Publication is pending;
external final-handoff.json records its completed gates and remote verification. All 86 findings retain
30 repaired and 56 unresolved (27 open, 29 partial); no package acceptance claim.

## Recovery, Artifacts and Dependencies


Restore a reviewed before-image with git show
ed663646e2841f22efc9e609006007752802caab:<path>, preserving concurrent changes.
The retirement receipt contains immutable archive links and hashes; external
logs and final publication/checkpoint results live in
/workspace/.cloud-setup/utility-plan-cleanup. No dependency, API, test target or
build script changes. Cloud draft persistence is distinct from Publish and
validation in a fresh task.

Revision 2026-10-06: replace the completed model cleanup plan with retirement of
superseded utility audit instructions and explicit preservation of their evidence.
