# Retire superseded server and session audit plans

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove duplicated historical workflows that recommend stale test targets,
laptop tool paths and completed publication steps. Work in /workspace/tidb
on hparser-integration from 8a1d95106e9eb4c66402cd7f7a92842fccd3633f.
Fresh Go master is 5b7e1eb8f5f8252391b6e68330d1648a26a80c17. Its seven-file
statistics update preserves FM sketches through JSON and storage; it is a
separate behavioral delta, not implemented or validated by this cleanup.
The selected external dependencies are unchanged. The comparison export was
updated only after all seven old file images matched the preceding Go pin.

## Progress


- [x] Review server/session plans, retained receipts and current references.
- [x] Refresh Go comparison and identify its separate statistics delta.
- [x] Remove 32 superseded plans and redirect six TESTPORT references.
- [x] Verify receipt preservation, publication ancestry, links and scope.
- [ ] Complete actual hook, fresh pre-push build, remote and Cloud checks.

## Milestones and Plan of Work


Remove server-* and session-* audit plans under rust/docs/operations except
server-handler-tests-audit-execplan.md, whose unfinished validation remains.
Each removed plan must map to a retained receipt under rust/testport/receipts;
session-syssession maps to session_syssession.md. The handshake/parse plan
maps to both original receipts. Preserve receipt bytes and historical test
results, failures, Go obligations and package-acceptance limits.

Redirect six references in rust/testport/TESTPORT_EXECPLAN.md to their retained
receipts. The dated git-diff command in session_syssession.md remains exact
historical evidence, not an active command recommendation. Verify every
removed plan's last commit is an ancestor of the already published base;
this establishes publication of the document, not passing its old runtime
gates. Update both finding registers and current-audit README without changing
findings. Collapse the repeated 62-link README chronology into a link to the
versioned historical index, preserving every old receipt reference. Record inventory and validation in server-session-plan-cleanup-validation.json.

## Validation and Acceptance


Run the external continuity/inventory check under
/workspace/.cloud-setup/server-session-plan-cleanup and git diff --check from
/workspace/tidb. Require all 32 receipt mappings to resolve, all before-image
hashes and publication ancestors to match, all retained receipts to stay
byte-identical, and no current Markdown link to a deleted plan. Changes must
be Markdown/JSON evidence only: no production, test, harness, script,
dependency, fixture, Go or Bazel changes. No Rust/Go test sweep or make lint
is required for this documentation-only scope.

Repository policy still requires the actual precommit locked server build
for rust/ changes. Source /workspace/.cloud-setup/env.sh, commit normally and
run CARGO_BUILD_JOBS=1 cargo build --locked -p tidb-server from rust/ immediately
before authorized push. Verify remote SHA and the saved Cloud checkpoint.

## Surprises & Discoveries


Most plans kept themselves open solely to continue an unrelated next package
or publish an already published September audit. One handler-test plan still
has unfinished shared gates and is intentionally retained. Runtime cleanup,
authentication and protobuf-sync checks inspected in this batch remain useful.

## Decision Log


Retire redundant plans as a group, preserving original evidence and remaining
Go obligations in their receipts. Keep meaningful Go and Rust correctness
tests. Do not conflate deleted planning documents with repaired findings or
package acceptance. Do not rerun expensive suites for unchanged executable code.

## Outcomes & Retrospective


Implementation and documentation validation complete: 32 plans /1267 lines
removed; 33 evidence receipts and the unfinished handler-test plan preserved.
Six references now point to retained receipts. The 62-link README cleanup
chronology is retained through its versioned historical index. Publication
gates remain pending here; external final-handoff.json records completion. No new behavior, package closure,
live-cluster validation or measured performance improvement is claimed.

## Recovery, Artifacts and Dependencies


Restore individual files using git show 8a1d95106e:<path>, preserving concurrent
work. Before-image hashes, receipt mappings and Go refresh evidence live under
/workspace/.cloud-setup/server-session-plan-cleanup. The durable receipt is
rust/docs/parity/current-audit/server-session-plan-cleanup-validation.json.
Publication evidence belongs in external final-handoff.json after commit.
Cloud draft saving, Publish and fresh-task restoration are separate. Revision:
replace completed Domain facade cleanup with historical workflow retirement.
