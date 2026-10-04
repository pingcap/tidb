# Remove empty planner tests while retaining Go obligations


This living ExecPlan follows root PLANS.md. Codex Cloud is the sole implementation workspace; no push is authorized. Integration starts at `65c099a9715bf77b92484a1ca9934d008ff57d90` on hparser-integration. Freshly fetched Go master remains `93a01d31f6da205ae4bf376825293903a6899fdb`; native client master remains `19a56ccda1e128218cd33c69709038219aced9bc` and is unchanged.

## Purpose / Big Picture


Remove compiled tests that perform no assertions and only advertise historical missing owners. Users and future contributors should see executable regression coverage separately from outstanding upstream obligations. This is test-harness maintenance, not complete Go package transcreation or repair of 55 behavioral findings. Production cache and Apply behavior remains unchanged.

## Progress


- [x] Read root instructions, confirm clean branches and refresh Go comparison refs.
- [x] Inventory five pure-placeholder files: 55 ignored empty functions; all 55 named Go test declarations still exist.
- [x] Capture the pre-removal aggregate test list (1306 entries).
- [x] Move complete historical contracts and candidate evidence into the audit ledger; remove files and useless LRU diagnostic probes.
- [x] Verify the aggregate list loses exactly the 55 named empty tests and nothing else; run real LRU/instance/Apply planner tests and lint.
- [ ] Run locked server build through the actual hook, commit locally and refresh recovery/startup handoff without pushing.

## Context and Orientation


The five files are `rust/crates/tidb-planner/tests/plan_cache_lru_instance_suites_source.rs`, `prepare_plan_cache_session_suite_source.rs`, `task_stringer_physical_unit_source.rs`, `casetest_rule_suites_source.rs` and `core_integration_apply_lateral_cardinality_source.rs`. Every test in these files is an ignored empty function. Their comments contain source contracts, but execute no behavior. `rust/scripts/aggregate-tests.rs` discovers remaining test modules automatically; never hand-edit its generated output. Existing LRU tests live in `rust/crates/tidb-planner/src/plan_cache_lru.rs`; two loops only print probes and have no assertions.

## Milestones and Plan of Work


First, preserve each removed function's historical contract, ignore reason and module context, plus the current Go declaration path/line/source hash, in `rust/docs/parity/current-audit/placeholder-test-obligations.json`. Archive rows for removed paths from the current candidate-gap index in that ledger; remove only those dangling index rows. Historical testport receipts remain unchanged. Delete only the verified pure-placeholder modules and two assertion-free LRU diagnostic loops.

Second, rebuild the aggregate harness and compare listed names against the saved baseline. Exactly 55 named entries disappear; every other entry survives. Run actual cache owners and retained parallel Apply planner tests. Because this changes test wiring and assertions only, prior SQL/wire behavior evidence is retained without repeating expensive unrelated execution.

Third, record commands/results and unchanged finding counts in both current registers and the audit README. Run make lint and the mandatory locked server build, commit through the actual hook and refresh the verified unpublished bundle and Cloud draft. Preserve all unrelated environment configuration and concurrent repository work.

## Concrete Steps / Validation and Acceptance


Activate shells with `source /workspace/.cloud-setup/env.sh`. From `/workspace/tidb/rust`, run `cargo test --locked -p tidb-planner --test all -- --list` before and after removal. Expect 1306 before, 1251 after, with exactly the ledger's 55 removed names absent. Run `cargo test --locked -p tidb-planner --lib plan_cache_ -- --test-threads=1` and `cargo test --locked -p tidb-planner --test all casetest_parallel_apply_suite_source -- --test-threads=1`; expect all selected executable regressions to pass. From `/workspace/tidb`, run `make lint` and `git diff --check`. The actual normal precommit hook must run `cd rust && cargo build --locked -p tidb-server` successfully; do not bypass it.

## Surprises & Discoveries


The two cache-suite placeholders still claim shared cache owners are unported, although C01/C02 now have working owners. A rule placeholder still claims actual parallel Apply reporting is absent. These comments are historical evidence, not current truth. All 55 upstream declaration identities were found in freshly fetched Go; removal therefore preserves their obligations rather than treating them as deleted upstream tests.

## Decision Log


Decision: retire compiled empty shells into an explicit unverified ledger. Rationale: empty ignored functions cannot validate a Go contract; retaining full documentary contracts avoids erasing real missing fixture coverage. Date: 2026-10-04.

Decision: keep serial Apply and current CTE clone guards. Rationale: Go retains serial fallback on clone/build failure; the replacement ownership is not yet complete. Removing guards without migration would introduce unsupported concurrent execution. Date: 2026-10-04.

## Idempotence and Recovery


Inventory before deletion, hash each file and verify all bodies are empty. After removal, use the committed ledger and parent commit to inspect the historical files. Do not reset or overwrite concurrent work. Generated test discovery follows actual files on subsequent builds. Preserve the prior verified bundle until its replacement verifies successfully.

## Interfaces and Dependencies


No production API, Cargo dependency, lockfile, Go source or native client changes. The ledger is audit data; it is not imported into the test harness and does not mark upstream obligations passed. Recorded structural counts remain 29 repaired and 57 unresolved (40 open, 17 partial).

## Outcomes & Retrospective


Removal and scoped verification passed: exactly 55 ignored empty entries disappeared, all 1251 other entries remain, all 55 Go identities and historical Rust hashes verify, 22 real Rust cases pass, planner all-target checking and make lint pass. The locked server build also passed. Normal hook commits and recovery/draft delivery are in progress. Cloud evidence belongs under `/workspace/.cloud-setup/placeholder-removal`. Complete upstream fixtures, failpoints, live TiKV, performance and package acceptance remain outside this cleanup.
