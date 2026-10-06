# Remove disconnected compiler and result models

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Retire the unused tidb-exec error/result vocabulary and synthetic compiler
model. The live query engine is tidb-executor, orchestrated by tidb-session.
Result differential tests already use difftests/result-tests/src/result_label.rs;
the exec copy has no consumer. Work in /workspace/tidb on hparser-integration
from 24f0e73699c7c0152604ebef5116324990b08804. Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed after refresh.

## Progress


- [x] Trace qualified, multiline and wildcard imports across tracked Rust sources.
- [x] Remove three unused modules and exports; migrate two useful formatter tests.
- [x] Run shared formatter/helper tests, lint and continuity/diff checks.
- [ ] Pass real hook and fresh pre-push locked builds, then verify remote SHA.
- [ ] Verify recovery bundle and save/read back Cloud checkpoint.

## Milestones and Plan of Work


Delete tidb-exec/src/error.rs, result.rs and compiler.rs. Remove their module
registrations and root exports. The compiler's PriorityPlanNode and ResultSetNode
are private surrogate trees used only by its nineteen unit tests; no live
session or planner constructs them. Deleting this disconnected seed does not
implement Go's automatic expensive-query priority or complete DB-label policy.
Keep those source obligations open and do not change finding dispositions.

Move result.rs's eight byte-encoding vectors and ordered/unordered row checks
into the actual shared result_label owner, adapting ResultSet::label to
rows_label. Do not alter either retained rendering function. Fix lib.rs's stale
claim that tidb-session does not depend on tidb-exec and remove the deleted enum
name from an explain comment. Preserve live executor, session, server and native
client sources. No dependency or generated-code changes are needed.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh; from /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p difftest-result-tests --lib -- --test-threads=1

All existing helper tests and both migrated formatter tests must pass. Confirm
moved byte vectors are unchanged; only the ResultSet wrapper is replaced with
rows_label calls. Verify no remaining import or qualified reference to retired
exports, and retained formatter production text is byte-identical. Run root
make lint and git diff --check. Removal of unused models changes no behavior;
no invented fail-before regression is required. The actual pre-commit hook
must pass cd rust && cargo build --locked -p tidb-server. Rerun that build
immediately before normal authorized push and verify the remote SHA.

## Surprises & Discoveries


The error enum claimed to serve every execution domain but had no callers.
The result container still owned two Go-oracle byte tests after actual
comparison suites moved to the shared formatter. The compiler model's comments
claimed live Rust planner/session seams did not exist, although those seams
now execute in other crates. Its tests never exercised those paths.

## Decision Log


Remove disconnected representations together; retain useful byte/order
coverage in the actual formatter. Do not delete schema-validator, recordset
lifecycle or transaction owners: caller tracing confirms they remain live.
Preserve complete Go package obligations rather than counting seed removal
as parity repair. Native client-rust remains unchanged.

## Outcomes & Retrospective


Implementation and validation complete: 28 shared helper tests, including
both migrated formatter cases, pass. Lint and continuity/diff checks pass.
Three files, nineteen model tests and 749 net Rust lines removed.
Publication gates remain pending here; external final-handoff.json records
their subsequent results.

## Recovery, Artifacts and Dependencies


Use git show 24f0e73699:<path> for individual before-images without overwriting
concurrent changes. External logs: /workspace/.cloud-setup/compiler-result-cleanup.
Durable receipt: rust/docs/parity/current-audit/compiler-result-cleanup-validation.json.
No dependencies change. Cloud draft save, Publish and fresh-task restore are
separate. Revision: replace completed statement/error boundary cleanup with
unused compiler/result retirement and migration to shared formatter tests.
