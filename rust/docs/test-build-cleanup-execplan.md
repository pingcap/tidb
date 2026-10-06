# Retire historical scratch baseline logs

This living ExecPlan follows root PLANS.md. Earlier cleanup receipts remain
indexed in parity/current-audit/README.md.

## Purpose / Big Picture


Remove obsolete test-run output from normal searches and make historical
receipts point to immutable evidence. Old failure lists are not current gates.
The batch removes 15 scratch files, 7964 lines and 591246 bytes; it does not
change or speed up test execution.

## Context and Orientation


Base 98371ab85edd89f7ff5c03000b6fb8083b38ce9c on hparser-integration in
/workspace/tidb. Fresh Go master remains b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
The retired rust/testport/scratch directory contains historical counts, failure
lists and one 7273-line raw nextest log. No build or runtime consumer references
these files. Ten receipts now point to exact Git before-images.

The b099 scope inventory remains in baseline-log-cleanup-validation.json because
it has no standalone receipt. b087's scratch divergences predate repairs recorded
in receipts/b087.md. b106's integration-only enumeration and b118's differing
baseline runs remain historical limitations. No old failure is declared fixed
and no complete Go package is accepted by deletion.

## Progress


- [x] Trace references and preserve original inventories and limitations.
- [x] Remove 15 files and migrate ten receipts to immutable archive links.
- [x] Validate documentation scope, before-image hashes, links and diff hygiene.
- [ ] Complete hook, fresh pre-push build, remote verification and cloud save.

## Milestones and Plan of Work


Identify all tracked scratch files and references. Preserve hashes and otherwise
orphaned Go obligations, then remove the full set and repair receipt references.
Verify only historical documentation and audit metadata change. Tests, source,
scripts, manifests and lockfiles must remain byte-identical.

## Validation and Acceptance


From /workspace/tidb, run python3
/workspace/.cloud-setup/baseline-log-cleanup/verify.py, git diff --check and make lint.
The verifier must validate archive links against pinned Git blobs, match all
before-image hashes and reject executable input or finding changes. Runtime
suites and all-target compilation are unnecessary for historical docs/logs.
Source /workspace/.cloud-setup/env.sh and use CARGO_BUILD_JOBS=1. Normal git commit
must execute hooks/pre-commit with cd rust && cargo build --locked -p tidb-server.
Repeat that locked build immediately before the authorized normal push. Verify
remote SHA; never force-push or bypass hooks. Postcommit results are recorded in
/workspace/.cloud-setup/baseline-log-cleanup/final-handoff.json.

## Surprises & Discoveries


The largest log contains obsolete compiler warnings as well as old failures.
b087's receipt records subsequent repairs to both scratch divergences. The
scratch files therefore cannot serve as current failure or acceptance records.

## Decision Log


On 2026-10-06, retire historical logs as one batch while preserving exact recovery
and original obligations. Keep useful Go-contract and Rust-correctness tests;
different names alone do not establish that a test is obsolete. No speedup claim.

## Recovery / Interfaces and Dependencies


Recover a file with git show
98371ab85edd89f7ff5c03000b6fb8083b38ce9c:rust/testport/scratch/<name>
into a temporary file. All archive links pin that revision. No dependency,
supported interface, Go source, executable test or script changes.

## Outcomes & Retrospective


Scratch artifacts are removed and historical evidence remains recoverable.
Archive/hash/scope verification, diff checking and root lint passed. Publication remains pending. The 56 unresolved findings are unchanged.

This revision replaces the completed orphan-source cleanup plan; its evidence
remains in parity/current-audit/orphan-storage-cleanup-validation.json.
