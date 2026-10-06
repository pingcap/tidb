# Retire macro-generated and panic-only test placeholders

This living ExecPlan follows root PLANS.md. Earlier receipts remain indexed in
parity/current-audit/README.md.

## Purpose / Big Picture


Remove 26 documentary registrations from four compiled test owners. Empty bodies
and unconditional panic placeholders cannot validate their Go contracts; their
contract notes belong in the audit receipt. Useful behavioral tests remain.
This reduces test compilation/registration inputs; no measured speedup is claimed.

## Context and Orientation


Base f511259253227d7007745d45f63d30decb83d0e0 in /workspace/tidb on
hparser-integration. Refreshed Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed
is exported at /workspace/.cloud-setup/go-master. No deeper Rust AGENTS.md applies.

Remove gap_evaluator and its eleven invocations from tidb-expr's
src/tests/aggregation_arithmetic_cast_source.rs; twelve panic-only cases from
tidb-unistore/src/tests_mockstore_part1_go_parity.rs; two empty cases from
 tidb-util/src/memory/tracker.rs; and server_id_constant from
 tidb-session/src/tests_domain_domain_utils_source.rs. Preserve all other code.
These cases are ignored registrations, never useful passing validation. Preserve
exact original names and contract comments in placeholder-macro-cleanup-validation.json.
Original Go packages and behaviors remain obligations; old absence claims are
historical and are not accepted as newly reproduced findings.

## Progress


- [x] Trace Go anchors, module registrations and placeholder bodies.
- [x] Remove 26 placeholders and update stale module/receipt descriptions.
- [x] Verify retained code, check affected test targets and run lint.
- [ ] Complete actual hook, fresh pre-push build, remote verification and cloud save.

## Milestones and Plan of Work


Retire macro-generated empty tests and panic-only bodies as one batch. Preserve
before-image hashes and every original contract. Update the historical b025,
b061, b066 and b117 receipts to distinguish old skipped counts from current
registrations. Keep both finding registers' dispositions unchanged. Verify all
retained executable bodies and production code before committing.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-expr -p tidb-util -p tidb-unistore -p tidb-session --tests

From repository root run make lint, git diff --check and the external
/workspace/.cloud-setup/placeholder-macro-cleanup/verify.py. Verify removed bodies
are only empty or unconditional panic stubs; all retained executable inputs
must match their before-images. Runtime suites are not rerun for this deletion.
Normal commit must execute hooks/pre-commit and its locked tidb-server build.
Immediately before pushing rerun cd rust && cargo build --locked -p tidb-server
with CARGO_BUILD_JOBS=1, then normal push and remote SHA verification. Never
bypass hooks or force-push. Final publication evidence is external final-handoff.json.

## Surprises & Discoveries


The aggregate macro escaped earlier empty-function cleanup and still cited the
retired tidb-exec aggregate runtime. Twelve mock-store cases do no work before
panicking. The skipped memory/Domain cases contain comments alone. Other ignored
tests exercise behavior or isolated helpers and remain untouched.

## Decision Log


On 2026-10-06, remove documentary registrations while preserving their exact
Go obligations outside the test runner. Do not remove real regressions based on
language-specific names, and do not claim skipped placeholders passed.

## Recovery / Interfaces and Dependencies


Use git show f511259253227d7007745d45f63d30decb83d0e0:<path> for before-images.
External inventories/logs live in /workspace/.cloud-setup/placeholder-macro-cleanup.
No production interface, dependency, manifest, lockfile or native-client change.

## Outcomes & Retrospective


Twenty-six placeholder registrations are removed. Affected test-target compilation,
root lint, retained-body verification and diff checking passed; publication remains.
The 56 unresolved findings remain unchanged. This revision replaces the completed
scratch-log cleanup plan; baseline-log-cleanup-validation.json retains its evidence.
