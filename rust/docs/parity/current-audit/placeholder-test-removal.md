# Retire empty planner test modules

The user asked to continue removing stale/useless tests while following Go. This cleanup removes **55 ignored empty functions in five modules**, plus two assertion-free LRU diagnostic loops. No useful assertion, production cache/Apply implementation, Go source, dependency or native client is removed.

The parent is `65c099a9715bf77b92484a1ca9934d008ff57d90`. Go master was freshly fetched and remains `93a01d31f6da205ae4bf376825293903a6899fdb`; integration's fetched remote remains `7b991676da79f044774caf6da4dfffe247160feb`. Native remains `19a56ccda1e128218cd33c69709038219aced9bc`. All execution is in Codex Cloud. No push is authorized or attempted.

## Removed modules and retained evidence

`plan_cache_lru_instance_suites_source.rs` contained two empty suite placeholders; `prepare_plan_cache_session_suite_source.rs` contained 25; `task_stringer_physical_unit_source.rs` contained three; `casetest_rule_suites_source.rs` contained 21; `core_integration_apply_lateral_cardinality_source.rs` contained four. All are under `rust/crates/tidb-planner/tests`. None contained executable helpers or nonempty test bodies.

All 55 named Go declarations still exist in the freshly fetched comparison tree. Their full historical contracts, ignore reasons, module context and source identities are retained in [the obligation ledger](placeholder-test-obligations.json). Historical "unported" comments are identified as historical seed evidence, not current assertions that owners remain absent. Full Go fixtures/matrices remain unverified; moving an empty function does not discharge its obligation.

The current `candidate-gaps.tsv` had 81 rows pointing into these deleted modules. Those rows moved into the ledger with original line/evidence, and were removed from the current index to avoid dangling source references. Historical b086/b088/b091/b092 testport receipts are preserved. The automatic aggregate-test build script discovers the remaining modules; no generated output or manifest was edited.

Two loops in `tidb-planner/src/plan_cache_lru.rs` performed `Get` probes only to print diagnostics, including mismatched parameter probes and pre-shrink recency touches. They had no assertions. Existing useful hit/eviction/capacity/memory assertions remain and pass. The corresponding current Go LRU suite retains behavioral assertions; it does not require these Rust-only diagnostic loops.

## Validation and limits

The aggregate test list changes from **1306 to 1251 entries**. The exact set difference is the ledger's 55 empty functions, with no additions or other removals. Historical Rust file hashes and current Go declaration paths/lines/hashes verify. **19 real cache tests and three Apply planner tests pass**, together with planner all-target checking and make lint. Commands/log hashes and the locked-build gate are recorded in [validation](placeholder-test-removal-validation.json).

No SQL behavior changed, so the preceding batch's wire evidence is retained without rerunning unrelated SQL suites. Full Go package/test/fixture acceptance, failpoints, live multi-node TiKV and performance were not verified by this cleanup. Serial Apply and the current CTE/shuffle safety guards remain: Go retains serial fallback, and removing broader guards requires complete replacement ownership first.

Structural finding counts are unchanged: **86 tracked, 29 repaired, 57 unresolved (40 open, 17 partial)**. This is harness cleanup, not 55 repaired findings. Both current registers record that distinction. The [living ExecPlan](../../placeholder-test-removal-execplan.md) supplies reproducible steps and recovery instructions. Local normal commits must pass the actual locked server-build hook; recovery/draft publication receipts live in the Cloud handoff. Nothing is pushed.
