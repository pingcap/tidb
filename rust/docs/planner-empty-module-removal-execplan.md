# Retire the remaining pure-placeholder planner test modules


This living ExecPlan follows root PLANS.md. Codex Cloud is the sole workspace and no push is authorized. Integration starts at `6d625b5223f5d7a94865f55ea7298e91982a54f9` on hparser-integration. Freshly fetched Go master remains `93a01d31f6da205ae4bf376825293903a6899fdb`; native master remains `19a56ccda1e128218cd33c69709038219aced9bc` and is unchanged.

## Purpose / Big Picture


Retire every remaining planner test module proven to contain only comments and ignored empty functions. The Rust harness should list executable coverage separately from upstream obligations. This continues the preceding 55-entry cleanup in one batch: 64 modules and 612 empty entries. No useful assertion, production behavior, Go test/benchmark or original obligation is removed. This is maintenance, not complete Go package acceptance.

## Progress


- [x] Read instructions, confirm clean integration/native checkouts and refresh comparison refs.
- [x] Strictly inventory 64 comment/empty-function-only modules; match all 612 Go declarations without unresolved identity ambiguity.
- [x] Capture the aggregate baseline of 1251 entries.
- [x] Archive full historical source/contracts and current Go identities, relocate dangling audit rows and delete the verified modules.
- [x] Prove exact harness set difference and unchanged retained sources; run retained regressions, all-target checking, lint and locked server build.
- [x] Commit through the actual build hook and refresh verified recovery/startup handoff without pushing.

## Context and Orientation


`rust/crates/tidb-planner/tests` is discovered automatically by `rust/scripts/aggregate-tests.rs`; the generated aggregate lives in OUT_DIR and must not be hand-edited. The complete removal manifest will be in `rust/docs/parity/current-audit/planner-empty-module-obligations.json`. Each selected file must reduce to whitespace after stripping comments and ignored empty test declarations. Mixed modules with useful tests/helpers are outside this removal. A placeholder is an ignored test function with an empty body; it validates nothing even if its comment describes a valid Go contract.

## Milestones and Plan of Work


First, archive every byte of each selected historical source and each empty function's contract/reason, plus pinned Go declaration path/line/hash. Some benchmark identities are in module-level mapping tables; bootstrap names such as TestMain require their exact package path. Preserve all original obligations as unverified, including Go code generators, fixtures and benchmarks. Move current candidate-gap index rows pointing into deleted files into the ledger. Leave historical testport receipts frozen. Delete only files passing the strict proof; no manual test registration edits are needed.

Second, rebuild and compare harness names against the 1251-entry baseline. Expect exactly the 612 ledger names removed, leaving 639 entries, with no other additions/removals. Verify every retained planner test file is byte-identical to its parent version. Run retained cache and physical/window/correlation tests plus the complete remaining aggregate harness; check all planner targets and lint. Production SQL is unchanged, so retain prior wire evidence without unrelated reruns.

Third, record completed commands/results and unchanged structural counts in the audit README, both registers and full structural plan. Run the locked server build and normal commits through the actual hook. Replace the existing recovery bundle only after verification. Survey actual Cloud checkouts, save the exact final HEAD and startup instructions in the draft, and verify readback without changing installer/network/secrets/runtime binding.

## Concrete Steps / Validation and Acceptance


Activate shells with `source /workspace/.cloud-setup/env.sh`. From `/workspace/tidb/rust`, run `cargo test --locked -p tidb-planner --test all -- --list` before and after removal. Expect 1251 then 639 entries and the exact manifest difference. Run `cargo test --locked -p tidb-planner --lib plan_cache_ -- --test-threads=1`, `cargo test --locked -p tidb-planner --lib physical::tests -- --test-threads=1` and the complete retained aggregate `cargo test --locked -p tidb-planner --test all -- --test-threads=1`. Run `cargo check --locked -p tidb-planner --all-targets` and `cargo build --locked -p tidb-server`. From `/workspace/tidb`, run `make lint` and `git diff --check`. The actual executable precommit hook with core.hooksPath=hooks must run the same locked server build; never bypass it.

## Surprises & Discoveries


An initial name-only lookup missed benchmark owners recorded in header tables and found ambiguous TestMain/TestBenchDaily names. Matching the documented table rows and full package paths resolves all 612 identities. Source declaration presence is not validation of complete fixtures, generated artifacts, platform obligations or performance.

## Decision Log


Decision: remove all strictly proven pure-placeholder modules together. Rationale: they execute no assertions; exhaustive mechanical proof avoids arbitrary small batches and protects mixed useful modules. Date: 2026-10-04.

Decision: archive the complete source, not just extracted names. Rationale: shared benchmark setup, module mappings and generator assumptions are real source obligations that per-function summaries might omit. Date: 2026-10-04.

Decision: keep production Apply/CTE safety guards and serial fallback. Rationale: this is test-harness cleanup; Go retains serial fallback, and replacing current broader clone guards requires complete runtime ownership first. Date: 2026-10-04.

## Idempotence and Recovery


Inventory and hash before deletion. Assert source equality to the parent before writing/removing anything; stop if concurrent edits appear. Recovery uses the parent commit and archived source; never reset/overwrite concurrent work. Rebuild aggregate discovery from actual files. Preserve the last verified bundle until its replacement verifies.

## Interfaces and Dependencies


No production API, dependency, lockfile, native client or Go source changes. The ledger is documentary data outside the compiled harness. Structural counts remain 86 tracked, 29 repaired and 57 unresolved (40 open, 17 partial). Removing 612 empty entries does not repair 612 behavioral findings.

## Outcomes & Retrospective


All 64 pure modules and 612 empty entries are removed; 674 candidate rows moved into the ledger. Exact harness set difference and complete archived Go/source identities verify. All 72 retained planner test files are byte-identical to the parent; no strictly pure placeholder module remains. The retained aggregate passes 277 tests with 362 ignored. All 355 distinct retained regressions, all-target checking, lint and locked build pass. Source committed through the actual locked build hook; final receipt, verified recovery bundle and exact draft readback follow as delivery checks. Cloud logs/scripts belong under `/workspace/.cloud-setup/planner-empty-module-removal`. Full Go suites/fixtures, failpoints, multi-node TiKV, performance and complete package acceptance remain unverified.
