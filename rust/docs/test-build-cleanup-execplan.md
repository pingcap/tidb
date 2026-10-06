# Consolidate chunk tests into their maintained owners

This living ExecPlan follows root PLANS.md. Keep progress, discoveries, decisions
and outcomes current. Previous static-test-root cleanup completed and was pushed
at 49bdae21fee12e020e8d4b995ea7a5ea6b1ae2b7; its external final-handoff.json
records publication and checkpoint validation.

## Purpose and Context


Maintain one set of chunk owner tests for Go allocation, iteration and row
mutation semantics. Remove stale duplicate wrappers after migrating distinct
inputs. Work in /workspace/tidb on hparser-integration, base 49bdae21fee12e020e8d4b995ea7a5ea6b1ae2b7.
Freshly fetched Go master remains b36c940a4332c866d8b0e2afde88f5e7c2fd7fed;
compare pkg/util/chunk. This cleanup leaves the 56 unresolved findings unchanged.
A carrier is a separate Rust test file registered by src/lib.rs; the maintained
owners here are the cfg(test) modules beside each implementation.

## Progress


- [x] Compare seven carrier surfaces with original Go and maintained Rust owners.
- [x] Migrate distinct allocator/list/mutable-row inputs into existing cases.
- [x] Remove five carriers, five module declarations and two duplicate cases.
- [x] Verify production prefixes, 223 prior owner assertions and retained carrier bodies.
- [x] Complete grouped chunk tests (71 passed), root lint, format and final self-review.
- [ ] Commit through actual hook, fresh prepush build, push and verify remote SHA.
- [ ] Refresh verified bundle and cloud startup checkpoint.

## Milestones and Plan of Work


First compare tests_alloc/pool/iterator/list/mutrow.rs with their same-name
implementation modules under rust/crates/tidb-chunk/src. Preserve allocator
constructor equality, concurrent column counts, integer pool roundtrip, clear
and refill iteration, list reuse counts, maximum duration and zero timestamps.
Delete carriers only after these inputs reach the maintained tests. Remove only
the duplicate width case from tests_codec.rs and projection mapping case from
tests_chunk_util.rs; retain their other regression bodies. No production edits.

Then run one grouped library test invocation and lint. Review source continuity
against the base and record results in chunk-test-owner-cleanup-validation.json.
Publish only after the normal hook and fresh build required by root AGENTS.md.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh; from /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-chunk --lib -- alloc::tests pool::tests iterator::tests list::tests mutrow::tests codec::tests tests_codec tests_chunk_util chunk_util::tests --test-threads=1

The selected owner and retained carrier tests must pass. Confirm no owner
assertion is lost, production prefixes are unchanged and five removed modules
have no registrations. From repository root run make lint and git diff --check.
Use rustfmt --edition 2021 --config skip_children=true --check on changed files.
Commit normally: executable hooks/pre-commit selected by core.hooksPath=hooks
must pass cd rust && cargo build --locked -p tidb-server. Repeat that command
immediately before authorized push to origin hparser-integration and verify SHA.

## Surprises & Discoveries


The ignored duplicate-ownership case has no assertions and stale comments about
inaccessible pool internals, while the current owner directly checks pointers.
Two old allocator cases change global settings without the maintained test lock.
The duplicate codec roundtrip has distinct JSON-string and join-key response
storage coverage, so it stays. Original Go tests and historical receipts stay.

## Decision Log


On 2026-10-06 consolidate assertions into existing owners rather than dropping
Rust ownership regressions. Preserve meaningful vectors before deletion. Retain
the existing 32-by-64 threaded allocator case and add column-count assertions;
remove the redundant 100000-allocation loop. This is not validation of Go's
original 1000-goroutine stress scale, nor a measured runtime improvement.

## Outcomes & Retrospective


Five files and two duplicate cases removed: 21 registrations (20 active, one
ignored), 988 net Rust lines. Distinct inputs survive under the maintained owners.
Grouped execution passed: 71 passed, zero failed/ignored, 179 filtered; root lint
and source continuity passed. Publication and cloud checkpoint remain pending;
external final-handoff.json will record their final results. This is test maintenance, not a
claim that a complete Go package or any unresolved structural finding is repaired.

## Recovery, Interfaces and Dependencies


Recover original files with git show 49bdae21fee12e020e8d4b995ea7a5ea6b1ae2b7:<path>.
Do not overwrite concurrent work. Manifests, dependencies, lockfiles and native
client remain unchanged. Durable evidence is rust/docs/parity/current-audit/
chunk-test-owner-cleanup-validation.json; external verification script, hashes,
logs and publication results live at /workspace/.cloud-setup/chunk-test-owner-cleanup.
Preserve all finding dispositions in both registers. Refresh the recovery bundle
only after verifying its replacement. Saved configuration is separate from
Publish and fresh-task restoration, which remain unverified.

Revision 2026-10-06: replace the completed static-root plan with the chunk owner
cleanup, preserving the previous receipt and publication pointer.
