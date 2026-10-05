# Remove obsolete test scaffolding and audit plans

This living ExecPlan follows root PLANS.md. Current base is 2087a78910ddbfece658b0b52a75ffcd39068f35; comparison authority remains Go master 93a01d31f6da205ae4bf376825293903a6899fdb. Earlier cleanup evidence is in [the discard-check receipt](parity/current-audit/discard-check-cleanup-validation.json).

## Purpose / Big Picture


Reduce repeated test compilation/execution and obsolete instructions. Remove metadata type-presence markers, a duplicate integration-module registration, print-only probes and retired annotation-audit plans. Keep Go behavioral obligations, native ownership/error checks, actual failing tests, source fixtures and production code.

## Progress


- [x] Inspect candidates and preserve exact before-images in /workspace/.cloud-setup/audit-plan-cleanup/before.
- [x] Remove ten redundant/nonbehavioral test functions, sixteen nonzero-size assertions, one duplicate metadata module registration and nineteen obsolete plans; redirect references to retained receipts.
- [x] Verify retained source bodies and harness registration; run metadata integration tests and affected expression/unistore test compilation together, then root lint.
- [x] Self-review, commit through the actual locked-server-build hook and refresh recovery/startup state.

## Milestones and validation


The metadata carrier milestone leaves pkg_meta_model_package_anchors.rs as its existing standalone Cargo test target, with twenty-five meaningful cases registered once. pkg_meta_model_semantics.rs retains five distinct behavioral cases. Six removed cases only discard size_of, one repeats the retained index assertion, and another only asserts positive native sizes. Sixteen additional positive-size markers convey no Go layout contract. Preserve the separate u64 flag-width and shared-ownership tests.

The diagnostic milestone removes probe2 from tidb-expr/src/pushdown_catalog.rs and probe_pow_expr from tidb-unistore/src/cophandler.rs. Both print results without checking them; neighboring actual pushdown and expression assertions remain. The document milestone retires nineteen annotation-audit plans for deleted test names, preserving package receipts and their unverified boundaries. Actual multi-node harnesses remain because they exercise live behavior.

From rust/ after sourcing /workspace/.cloud-setup/env.sh, run CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-model --test pkg_meta_model_semantics --test pkg_meta_model_package_anchors -- --test-threads=1 and cargo check --locked -p tidb-expr -p tidb-unistore --tests. From root run make lint and git diff --check. Expected metadata outcome is 30 passing distinct cases; verify Cargo metadata still lists each standalone target once. Commit normally with core.hooksPath=hooks, TERM=xterm and CARGO_BUILD_JOBS=1; the actual hook must pass cd rust && cargo build --locked -p tidb-server. No Go/Bazel files changed. No broad behavioral sweep is warranted for these deletions.

## Surprises & Discoveries


Cargo auto-discovers the metadata anchor file while the semantics target also includes it as a module, compiling and running all twenty-six original cases twice. Most no-assertion search hits are meaningful helper-based tests or Go benchmark translations and remain. The dbterror audit contains an outstanding generated-catalog concern and is retained; stringutil contains substantive byte semantics and is retained.

Two initial hooks exhausted disk and correctly aborted. Remove failed temporaries, three inactive test executables and check-only metadata without paired compiled libraries; record hashes and actual free-space deltas. Free space recovered to 1.1 GB. Retry the same normal hook; preserve source, compiled dependencies and recovery data.

## Decision Log


Retire the complete selected class of obsolete documentation and metadata markers together, preserving semantic receipts rather than another permanent duplicate inventory. The exact deletion inventory is outside the tree; Git at the base commit preserves every removed byte. No package or structural finding is accepted by cleanup. Date: 2026-10-05 UTC.

## Outcomes & Retrospective


Removed 19 obsolete plans, ten unique redundant/nonbehavioral cases, 26 duplicate registrations and sixteen size markers. All 30 metadata cases pass, affected expression/unistore test targets compile, root lint and diff checks pass. Source comparison confirms 113 retained bodies differ only by the sixteen removed markers. The normal commit is gated by the actual locked server build; recovery/startup refresh follows it. See [exact validation](parity/current-audit/audit-plan-cleanup-validation.json). No speedup measurement or fresh-task restoration is claimed. The user corrected the repository installation grant; verify managed Git write access through authorized publication after the fresh locked build.

## Recovery


Recover selected removed files from the base commit into a separate path and review before restoring. Preserve concurrent work and never reset or force-push. After the normal commit, refresh and verify the unpublished bundle and saved Cloud draft without changing network or credential settings.

## Comment-only harness continuation


Current base: 17a3095068e7f14c363f90e41a6d65437ea6072c; refreshed Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb. Remove the remaining comment-only test bodies across codec, executor, expression, metadata and planner in one batch, including empty benchmark documentary modules and the obsolete DDL compile marker. Keep every executable behavioral test body unchanged. The Go contracts remain outstanding; record exact removed names, original lines and source hashes in comment-test-cleanup-validation.json, with Git recovery at the base. Historical comments claiming whole owners are absent are not current findings.

Remove stale receipt mapping rows and current candidate rows referring to the retired shells, retaining their Go identities in the cleanup receipt. Delete modules that contain no code after removal. Do not add a permanent cleanup script or test-count gate. No production algorithm, Go fixture, native client or dependency changes belong in this batch.

- [x] Inspect the complete selected class and refresh comparison refs.
- [x] Remove 260 entries, ten empty modules and stale mappings; verify all 178 retained executable test bodies byte-for-byte.
- [x] Grouped affected all-target checks, 26 codec cases, one metadata case, root lint and diff review pass.
- [ ] Commit through the actual locked-server hook, repeat the locked build immediately before authorized push and verify the remote SHA; refresh reusable startup state.

Validation from rust/ after activating /workspace/.cloud-setup/env.sh: cargo check --locked -p tidb-codec -p tidb-executor -p tidb-expr -p tidb-meta -p tidb-planner --all-targets; cargo test --locked -p tidb-codec --lib -- tests::go_codec_port; cargo test --locked -p tidb-meta --test all -- meta_test_go_parity --test-threads=1. Use CARGO_BUILD_JOBS=1. Root make lint and git diff --check complete the batch checks. These are harness deletions, not SQL fixes: exact retained-body comparison and test-target compilation establish preservation without linking every large suite. Full Go/Rust suites, live clusters and performance are not claimed.

Discovery: the earlier empty-shell cleanup matched truly empty braces, leaving 259 comment-only bodies (including two false-passing tests and 120 ignored benchmark shells). The unrelated compile-only DDL marker also survives beside real helper tests. Go's codec bytes test and DDL worker-pool test contain real assertions; empty Rust names do not implement them. Removing registration therefore does not accept their original package obligations.

Continuation outcome: 260 nonbehavioral entries, ten empty modules, 261 stale mapping rows and 296 candidate rows removed. Two obsolete benchmark reports are reduced to recovery references. All 178 retained test bodies are byte-identical; grouped compilation, 27 behavioral tests and root lint pass. Publication must complete the actual hook/fresh locked-build gates; the Cloud comment-test-cleanup/final-handoff.json records the exact result. No speedup measurement or new parity acceptance is claimed.
