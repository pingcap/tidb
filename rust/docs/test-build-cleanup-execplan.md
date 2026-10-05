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

## Unused execution and statement models


Base `a59185577928e2e4a1e7a559cc3b090882211386`; refreshed Go master remains
`93a01d31f6da205ae4bf376825293903a6899fdb`. Remove nine exported tidb-exec models
and their dedicated tests together: chunk flags, CTE error selection, ordered
Apply buffering, input accounting, statement reference counts, batch flushing,
row-ID reservation and INSERT/DELETE row-column sizing. All exported symbols
have only private test callers. These models neither own nor integrate the
live Go lifecycle. Their removal reduces the compiled surface and eliminates
misleading standalone coverage; no timing improvement is claimed.

### Progress


- [x] Trace all module/export callers and check original Go source paths against refreshed master; record the stale executor_test.go citation.
- [x] Remove nine models, nine test files and nine module declarations in one batch.
- [x] Affected all-target checks, nine retained live Apply cases, lint and complete diff review pass.
- [ ] Commit through the actual locked-server hook; rebuild immediately before authorized push and verify the remote SHA.

### Discoveries and decisions


The private Apply BTreeMap simulator duplicates ordering examples already in
live executor SQL tests. Keep those tests and actual worker error/cancellation,
queue shutdown and reopen tests unchanged. The batch-flusher seed starts a
sleeping thread yet has no production consumer. Removing these models does
not implement chunk/session reuse, CTE error publication,
statement-cache reference counting, row-ID reservation or full runaway flushing.
Those Go obligations remain outstanding. The old RUV2 models also cite an
absent executor_test.go; current Go executor.go, insert_common.go and delete.go
have neither nextIOAcc nor rowsColMultiply. Do not promote those historical
claims into current-master requirements. Keep used min/max deque code, Rust
safety tests and operational scripts with real path/service safeguards.

### Validation and recovery


Activate `/workspace/.cloud-setup/env.sh`. From `rust/`, run with
`CARGO_BUILD_JOBS=1`: `cargo check --locked -p tidb-exec -p tidb-server --all-targets`
and `cargo test --locked -p tidb-executor --lib -- ordered_parallel_apply
apply::parallel::tests --test-threads=1`. Run root `make lint` and
`git diff --check`. Verify every retained Rust source, manifest and build script
except the nine removed exports is identical to base. The existing shared test
generator removes registration automatically; do not edit generated output.
Exact inventory, source hashes, Go anchors and outcomes are in
[the receipt](parity/current-audit/dead-leaf-cleanup-validation.json).
Recover individual files with its `git show` command; never reset concurrent
work. Full suites, live clusters and performance are outside this cleanup.

### Outcome


Nine unused models and 25 private tests are removed (1,546 file lines plus
nine exports). All 3,701 retained source/manifest files are byte-identical.
Nine live Apply tests, affected all-target checks, lint and diff review pass.
The actual hook and fresh prepush gate remain required; publication evidence
belongs in Cloud `dead-leaf-cleanup/final-handoff.json`. No finding status or
package acceptance is changed by removing unused seed models.

## Unused session and statement models


Base `075db57c2cfbde5a6d1b01ceac980bbfcd6e2b62`; refreshed Go master `b36c940a4332c866d8b0e2afde88f5e7c2fd7fed`.
Remove ten unconsumed models and their dedicated private test files from
`rust/crates/tidb-exec`, plus their `src/lib.rs` exports. The modules are
setvar_hint_restore, sysvar_error, read_consistency, txn_running_state, lazy_txn_state, nextgen_readonly_vars, session_token_timing, charset_variable_groups, session_context_key, alternative_plan_signals. This removes compiled surface that cannot affect SQL.
The original Go session, statement and variable package obligations remain;
no unresolved finding is closed by deleting isolated models.

### Progress


- [x] Trace module names and all exported top-level symbols across tracked Rust source, manifests and scripts; only the dedicated tests consume these exports.
- [x] Inspect all selected bodies and original Go source/test anchors at refreshed master; remove ten modules, ten test files and ten exports in one batch.
- [x] Affected all-target checks and lint pass; all 3,760 retained source/manifest/script files are identical, and generated test registration drops all ten removed modules.
- [ ] Commit through the actual server-build hook, rebuild immediately before push, verify remote SHA and refresh Cloud recovery/configuration.

### Surprises & Discoveries


The SET_VAR map seed never participates in hint application. Actual application
and restoration live in tidb-session `variables.rs` and `warnings.rs`, with
SQL tests in `tests_binding.rs`, `tests_fix_control.rs` and
`tests_session_var_hooks.rs`. Keep these and the live charset tests unchanged.
The remaining deleted models only format integers, duplicate constants, or
simulate state with booleans; none owns a live transaction or authentication
lifecycle. Go master advanced while this batch began; client-go selection is
unchanged. Earlier audit evidence is not retroactively marked revalidated.

### Decision Log


Remove unused source and its private tests together, rather than keeping
exported but unconsumed compatibility examples. Retain original Go anchors in
[the receipt](parity/current-audit/session-leaf-cleanup-validation.json) so
future package work can recover the requirements without counting these models
as implemented lifecycle coverage. Retain Rust-specific correctness tests and
active operational safety checks. No new harness or deletion-only tests.

### Validation and recovery


Source `/workspace/.cloud-setup/env.sh`. In `rust/`, run
`CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-server --all-targets`.
At repository root run `make lint` and `git diff --check`. Hash every retained
Rust source, manifest and script against the clean base (except ten removed
exports in lib.rs); unchanged bodies establish that live paths and retained
test assertions were not weakened. The shared aggregate-tests build script
must stop registering deleted tests automatically. No behavioral test rerun is
needed for disconnected model deletion. Use the receipt's git-show recovery
command for individual removed files, never reset the checkout. Normal commit
and fresh prepush `cd rust && cargo build --locked -p tidb-server` are mandatory.

### Outcomes & Retrospective


Ten models, ten test files and 18 private tests are removed (1,146 file lines
plus ten exports). Grouped checks and lint pass; no live runtime or retained
test body changes. Publication evidence belongs in Cloud
`session-leaf-cleanup/final-handoff.json`. Retire the redundant sessionstates audit plan and mark its retained source
inventory receipt historical; the deleted timing seed was never a runtime
owner. No runtime speedup or measured compile-time improvement is claimed.

## Unused configured join and result adapters


Base `4d28b313c35f8c6bc7850e2dc3595182cf35243f`, Go `b36c940a4332c866d8b0e2afde88f5e7c2fd7fed`.
Retire the isolated configured INNER/CROSS join runtime and its ordered wrapper,
plus tableless result-response, status-to-packet, summary-row reader, result-row
count and missing-handle adapters. Delete their seven modules, seven dedicated
test files and exports in `rust/crates/tidb-exec/src/lib.rs` together. This
removes 35 private tests and 3239 file lines plus 13 export/documentation lines.

### Progress


- [x] Trace the whole deletion set, including methods implemented on the retained multi-read session; no callers remain outside the selected set and exports.
- [x] Compare original anchors against refreshed Go master, preserve actual physical builder and wire owners, and retire the seven modules/harnesses together.
- [x] All 3,746 retained source/manifest/script files are identical, generated registrations exclude all seven harnesses, affected all-target checks and lint pass.
- [ ] Normal hook commit, fresh locked prepush build, exact remote verification and Cloud draft/recovery refresh.

### Surprises & Discoveries


The configured join retains all right rows and owns a private nested-loop
runtime, yet has no server/session consumer. Live joins, limits and TopN are
built by `tidb-executor/src/driver/physical_builder.rs`, matching the shared
Go executor builder ownership. The configured TopN state still has live server
consumers and stays intact. The tableless metadata wrapper's comment predates
the real catalog-backed planner. The status wrapper similarly has no wire
caller; retain the actual protocol/connection writers and their assertions.
Historical PD receipts mention these private harnesses, including old failures;
mark those sections historical without erasing their results or changing the
used TopN tests. Original Go obligations and finding dispositions stay open.

### Decision Log


Remove complete unreachable runtime paths, not merely their tests. Keep live
and Rust-specific correctness tests, source metadata/protocol owners and active
service safeguards. No new deletion-only tests or permanent check scripts.

### Validation and recovery


Source `/workspace/.cloud-setup/env.sh`; in `rust/`, run
`CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-server --all-targets`.
Run root `make lint` and `git diff --check`. Compare retained source, manifest
and script hashes to base, except removed lib.rs exports; check the regenerated
aggregate test list excludes all seven modules. No live algorithm changes, so
no behavioral suite rerun is needed. Commit through the actual hook and run
`cd rust && cargo build --locked -p tidb-server` immediately before pushing.
Recover individual files with the git-show command in
[the receipt](parity/current-audit/result-path-cleanup-validation.json).

### Outcomes & Retrospective


Deletion and grouped validation complete. No runtime or build timing
improvement is claimed. Cloud `result-path-cleanup/final-handoff.json` records
publication and environment persistence after the tracked validation completes.
