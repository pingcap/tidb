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

## Retire unused JSON and percentile aggregate models


Base 1432306a5b7c64e46d8cd49dc1b32ffedb6a50d4 and freshly fetched Go master 93a01d31f6da205ae4bf376825293903a6899fdb. The three public tidb-exec models json_arrayagg, json_objectagg and percentile have no production callers; their thirteen tests exercise private string fragments, alternate sorting and native size formulas. Actual SQL uses tidb-executor HashAgg partials and the shared BinaryJSON/selection owners. Remove all three models, exports and dedicated test files together. Keep useful Go merge/value/reset/error vectors in the existing parallel HashAgg test module and existing session/percentile suites. This retires duplicate implementations without claiming a complete aggfuncs package.

- [x] Trace every caller and inspect current Go aggregate owners and retained runtime tests.
- [x] Remove three unused models, their exports and thirteen tests; migrate useful vectors to the existing live merge/spill/reset test module.
- [x] Twenty distinct executor/session cases, the final maximum-vector rerun, affected all-target checks and root lint pass.
- [x] Prepare publication through the actual hook and fresh prepush locked builds; exact outcome belongs in Cloud aggregate-leaf-cleanup/final-handoff.json and must not be assumed without that receipt.

From rust/ with /workspace/.cloud-setup/env.sh active, run CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-executor -p tidb-session --lib -- json_and_percentile_merge_spill_reset_follow_go approx_percentile json_agg tests_json --test-threads=1, then cargo check --locked -p tidb-exec -p tidb-executor -p tidb-server --all-targets. Run make lint and git diff --check from root. Keep existing Rust safety/Go lifecycle tests. The deleted private nonfinite-before-mutation assertion and native-size identities do not prove Go error timing or memory accounting; those original obligations remain unaccepted. Recover removed source from the base commit, never reset concurrent work.

Aggregate cleanup discovery: an older percentile source test file is not registered, so it is not executable coverage. The exact 1..=28 maximum-selection vector was therefore moved into the live partial test and rerun successfully. Go memory accounting, full error timing, original package variants and performance acceptance remain separate obligations. Three dead models (508 source lines) and thirteen private tests are retired; production behavior is unchanged.

## Unregistered executor/session source-test continuation

Base: `da5f2ac9a7874429a7d7d36e85606ea54b285508`; fresh Go master remains `93a01d31f6da205ae4bf376825293903a6899fdb`.
Remove five unreachable source files together with stale current-coverage claims.
Every remaining Rust source, manifest, build script and registered test remains
unchanged. This reduces misleading material, not compiler time. No complete Go
package or finding is accepted by this cleanup.

### Progress

- [x] Trace registration across tracked source, manifests and shared test generator.
- [x] Remove five files (1,107 lines), 21 explicit tests and 49 empty macro placeholders.
- [x] Preserve original Go anchors, before hashes and Git recovery commands; condense two stale receipts.
- [x] Grouped affected all-target checks, root lint and source-isolation verification pass (3,802 retained files identical).
- [x] Prepare publication through the actual locked-build hook and fresh prepush gate; final outcome must be read from Cloud orphan-test-cleanup/final-handoff.json.

### Discoveries and decisions

The initial textual scan counted 22 test attributes; one belongs to a macro
that emits 49 empty ignored tests. Actual unreachable declarations are 21
explicit tests plus those 49 placeholders. Cargo's shared generator only scans
`tests/`; these files are in `src/` and have no module registration. Do not
restore stale fixture APIs merely to run previously unreachable tests. Keep
Go obligations explicit, including transaction concurrency and percentile
variant/error behavior; the prior live HashAgg test already carries the exact
1..=28 percentile maximum vector.

### Validation and recovery

From `rust/` with the Cloud environment active, run `CARGO_BUILD_JOBS=1 cargo check
--locked -p tidb-executor -p tidb-session --all-targets`; run `make lint` and
`git diff --check` at root. Verify all retained `.rs`, manifests and build
scripts against base. No new SQL fix or regression is introduced. Follow the
actual precommit and fresh prepush locked server build requirements. Recover
individual files using the receipt's `git show` commands without resetting
concurrent changes. Exact outcomes are in
[the receipt](parity/current-audit/orphan-test-cleanup-validation.json); final
publication outcome belongs in Cloud `orphan-test-cleanup/final-handoff.json`.

Outcome: five unreachable files and two obsolete receipt narratives are retired.
All 68 named Go declarations were found in current master after resolving
`TestGetDBNames` to `pkg/util/metricsutil/db_labels_test.go`; original Go
obligations and all 86 finding dispositions remain unchanged. No behavioral
test execution or compile-time improvement is claimed for this deletion.
