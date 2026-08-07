# Lock down the complete `pkg/types` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan continues the Go-to-Rust transcreation goal on branch `codex/lockdown-343-wave7`. Go under `pkg/types` in this worktree is authoritative. The divergent main checkout must not be used as source evidence.

## Purpose / Big Picture

After this package unit is complete, reviewers can verify that the Rust datatype implementation accounts for every production source, original test/support artifact, generated input, fixture, build artifact, function, and behavior branch in the upstream Go `pkg/types` package. Each Go item will be classified exactly once as `PORTED`, `DECLINED`, or `UNREACHABLE`, with a checked gate that detects Go-source drift and disappearance of a named Rust symbol. Only that complete package claim may be integrated into the coordinator and pushed.

## Progress

- [x] (2026-08-07) Confirmed package atomicity from root `AGENTS.md` non-negotiable 6 and preserved both local seed commits outside the coordinator.
- [x] (2026-08-07) Re-ran the Go-test coverage census after the seed commits: 202 mapped tests comprise 34 `NAME-EXACT`, 77 `NAME-FUZZY`, 20 `NAME-TOKENS`, 27 `REFERENCED`, and 44 `NONE`.
- [x] (2026-08-07) Preserved mutation and crate validation evidence for commit `38a4122dc2cea40b6326d12f4f773feb77c68c92`, which ports the exact `TestTimeOverflow` source rows.
- [x] (2026-08-07) Preserved mutation and targeted validation evidence for commit `9648e257c09f8bcda4732bf6b460dccf47f15788`, which completes the audited `convert_test.go` source tables.
- [x] (2026-08-07) Completed, committed, and mutation-probed the first `datum_test.go`/`core_time_test.go` source-row batch: 12 independently named Rust tests pass and reduce the census from 44 to 32 `NONE` without production-code changes.
- [x] (2026-08-07) Audited the six uncovered `etc_test.go` type-predicate tests against the existing all-type-code table; six independently named tests pass and reduce the census from 32 to 26 `NONE` without production-code changes.
- [x] (2026-08-07) Audited all eleven mechanically uncovered `json_binary_test.go` tests row by row; eleven exact-name Rust tests pass and reduce the census from 26 to 15 `NONE` without production-code changes.
- [ ] Audit and account for every `pkg/types` production, test, support, benchmark, generated, fixture, and build file.
- [ ] Port or prove every remaining Go test table, starting with the 44 mechanically uncovered tests and then auditing all weak name matches row by row.
- [ ] Check in the complete function/branch inventory and its Go-source and Rust-symbol gates.
- [ ] Mutation-probe every landed rule with boundary cases and restore from explicit saved copies.
- [ ] Run the package WIP gates, then integrate the complete package into a clean coordinator worktree.
- [ ] Run the Ready/full-workspace gate, verify direct ratchets, non-force push `origin:hparser-integration`, and verify the remote SHA.

## Surprises & Discoveries

- Observation: the ranked coverage brief was stale for `TestCompareBinary` and `TestCheckTimestamp`; their Rust tests already contain all 31 and 17 Go rows respectively.
  Evidence: `binary_json.rs::test_binary_compare_and_opaque` and the timestamp source-row test were audited against their owning Go tables before this continuation.
- Observation: the test-name census is useful only as a negative queue. A fuzzy or exact name does not prove table-row or assertion parity.
  Evidence: the generator itself states that a 200-row Go table counts once even if Rust carries one row.
- Observation: after the two seed commits, the current census has 44 `NONE`, down from 52; the remaining queue starts in `core_time_test.go` and `datum_test.go`.
  Evidence: `python3 rust/scripts/test-coverage-inventory.py --json --cache /tmp/tidb-types-go-test-declarations.tsv` on `9648e257c`.
- Observation: `pkg/types` has no `doc.go` and no nested `AGENTS.md`; root repository policy is the applicable instruction set.
  Evidence: `rg --files pkg/types` and `find pkg/types rust/crates/tidb-datatype -name AGENTS.md`.
- Observation: `TestEstimatedMemUsage` is representation-specific rather than a portable equality contract.
  Evidence: the Go probe prints `go-layout: Datum=72 MyDecimal=40 Time=8` and `go-estimated: rows=10 bytes=5530`; the Rust probe prints `rust-layout: Datum=64 Decimal=64 Time=16`. The Rust test pins its own measured layout/formula and explicitly rejects a false 5,530-byte parity claim.
- Observation: the expected ceiling for the Go reverse-bound `DOUBLE(23,-1)` row is `GetMaxValue(target)`, not `math.MaxFloat64`.
  Evidence: the first WIP run failed with Rust `Real(1e24)` versus the mistaken `Real(1.7976931348623157e308)` expectation; changing only the expected value to the Rust mapping of `GetMaxValue` made all 12 tests pass.
- Observation: the eleven JSON gaps were traceability gaps in already ported behavior, plus four missing `GetKeys` assertions and one missing typed BinaryJSON-copy assertion; no production behavior change was required.
  Evidence: exact tests for unquote, remove, contains, copy, keys, depth, parse errors, typed creation, callback extraction, opaque values, and hashes pass 11/11. `TestCreateBinary`'s Go-only `int8` panic is unreachable because Rust accepts the closed `BinaryJSONValue` enum instead of `any`.
- Observation: the JSON key-length rejection exists in both the small-object encoder and its large-object fallback; mutating only the first guard does not remove the behavior.
  Evidence: `test_get_keys` survived the first-guard mutation, then failed when both guards were disabled because parsing the 65,536-byte key unexpectedly succeeded.

## Decision Log

- Decision: treat the complete root Go package `pkg/types` as one atomic integration unit even though its Rust implementation spans many modules in `tidb-datatype`.
  Rationale: root `AGENTS.md` explicitly forbids integrating or reporting partial package transcreation.
  Date/Author: 2026-08-07 / Codex
- Decision: keep `38a4122dc` and `9648e257c` as local seed evidence until the package inventory, drift gates, mutation probes, and Ready evidence are complete.
  Rationale: early cherry-picking would turn partial evidence into an invalid package claim.
  Date/Author: 2026-08-07 / Codex
- Decision: use existing Rust modules and table-driven tests, adding independently nameable source-row tests when the census cannot trace a complete existing table.
  Rationale: this keeps changes minimal while making each Go test artifact auditable and mutation-addressable.
  Date/Author: 2026-08-07 / Codex
- Decision: after the complete clean-workspace Ready gate, push non-force directly to `origin` branch `hparser-integration`; do not push unit branches or the incomplete package.
  Rationale: the user confirmed that `dbsid` now has write permission to `pingcap/tidb` and explicitly superseded the handoff's old no-push/two-remote delivery rule.
  Date/Author: 2026-08-07 / Codex

## Outcomes & Retrospective

The package is not complete. Two local seed commits improve exact source-row coverage, but no integration or push is permitted until the full census, classification inventory, drift gates, mutation evidence, and Ready gate are complete.

## Context and Orientation

The authoritative Go package is the root directory `pkg/types`; `pkg/types/parser_driver` is a separate Go package and therefore a separate future claim. The primary Rust mapping is `rust/crates/tidb-datatype`, whose modules cover datums and conversion, decimal arithmetic, MySQL time and duration, binary JSON, field types, enum/set, literals, overflow, formatting, and helpers. The same Rust crate also contains independently owned parser charset and collation work, which must not be silently claimed as `pkg/types` coverage.

The generated negative coverage queue lives in `rust/docs/operations/test-coverage-inventory.md` and is produced by `rust/scripts/test-coverage-inventory.py`. It is not a completeness receipt. The stronger deliverable will inventory every Go source item and name its Rust evidence or a measured reason why it is declined or unreachable.

All Cargo commands use the exclusive target directory `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-343-wave7/tgt` and run serially with 12 jobs. The coordinator worktree is `/Users/chenhuansheng/Documents/GitHub/tidb-hparser-coordinator-341` and must remain untouched until this package unit is complete.

## Plan of Work

First, enumerate every root-package file and extract Go production declarations, tests, support helpers, generated inputs, build metadata, and meaningful control-flow branches. Establish a stable source digest or declaration manifest rather than relying on prose line numbers.

Second, use the mechanical `NONE` queue as the first audit order. For each Go test, inspect the entire table and assertions, locate existing Rust behavior, and either rename/split a complete existing Rust table or add the missing rows and behavior. Then audit the `NAME-EXACT`, `NAME-FUZZY`, `NAME-TOKENS`, and `REFERENCED` sets because their names alone are weak evidence.

Third, check in a package inventory with exactly one `PORTED`, `DECLINED`, or `UNREACHABLE` row for every production function and classified branch. Add a gate that regenerates or checks the Go side and another gate that resolves every `PORTED` Rust symbol. A deleted Rust test or function must fail a named gate.

Fourth, commit coherent filename-sized batches by exact path. After each commit, copy the affected production and test files to explicit temporary saved paths, mutate one boundary rule at a time, require the intended named test to fail, restore the saved bytes, and rerun the named test. Never restore with Git checkout or stash.

Finally, run the crate and differential WIP gates. Once and only once the package claim is complete, cherry-pick the parent-ordered package commits into the clean coordinator, run the repository Ready profile and the full Rust workspace gate, verify direct ratchet constants, and push the coordinator tip to `origin:hparser-integration` without force. Confirm the remote SHA with `git ls-remote`.

## Concrete Steps

Run source and test census commands from the package worktree root. Use the cached Go declaration TSV to avoid repeating the slow Go scan:

    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 python3 rust/scripts/test-coverage-inventory.py --json --cache /tmp/tidb-types-go-test-declarations.tsv

Run focused Rust tests from `rust/` with the exclusive target directory:

    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-343-wave7/tgt cargo nextest run -p tidb-datatype -j12 -E 'test(<named-test>)'

At each package checkpoint run:

    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-343-wave7/tgt cargo nextest run -p tidb-datatype -j12 --no-fail-fast
    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-343-wave7/tgt cargo nextest run -p difftest-result-tests -j12 --no-fail-fast
    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-343-wave7/tgt cargo fmt -p tidb-datatype -- --check
    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-343-wave7/tgt cargo clippy -p tidb-datatype --all-targets -j12
    git diff --check

The final coordinator gate additionally runs `cargo nextest run --workspace --no-fail-fast`, the applicable source/inventory checks, `cargo fmt --all -- --check`, `cargo clippy --all-targets -j12`, and repository `make lint`. Do not run `make bazel_lint_changed`; the user did not request that expensive optional sweep.

## Validation and Acceptance

The package is accepted only when every root `pkg/types` file and every production function/branch has one classification, all original Go tests and support artifacts have reviewed evidence, both drift gates fail under a deliberate source/symbol mutation, every behavior mutation is killed by a named boundary test, and all restored WIP and Ready commands pass from their required clean worktrees.

A lower `NONE` count is progress but is not acceptance. A package crate test pass without the inventory and mutation evidence is also not acceptance. The observable delivery is a verified remote `origin:hparser-integration` SHA whose history contains the complete atomic package receipt.

## Idempotence and Recovery

Census and inventory checks must be read-only in check mode and deterministic in write mode. Cargo commands are safe to rerun with the exclusive target directory. Mutation probes always begin from a committed tree, use saved copies outside the repository, and verify restoration with `git diff --check` plus `git status --short`.

The coordinator is not modified until the unit is complete. If a final gate fails, fix the package branch and rerun before integrating; do not force-push or rewrite the official branch. The protected worktree `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-337-wave2` is unrelated and must not be removed or overwritten.

## Artifacts and Notes

Current local commit chain:

    9648e257c09f8bcda4732bf6b460dccf47f15788 38a4122dc2cea40b6326d12f4f773feb77c68c92 rust: complete Go convert source tables
    38a4122dc2cea40b6326d12f4f773feb77c68c92 5dae9fdc30c031e9d86568c053223a144a847cfa rust: cover Go time overflow source rows

The `TestTimeOverflow` mutations changed an overflow expectation and collapsed all parser inputs to one valid datetime; both failed the named source-row test. The `convert_test.go` mutations removed ASCII and UTF8MB4 skip behavior, changed binary type rendering, accepted a negative tiny integer, and changed JSON-true decimal conversion; each failed its intended named test after the missing source rows were added. Saved copies were restored and the committed versions passed.

The first datum batch was saved under `/tmp/tidb-types-datum-probes.P0fYAW`. Its boundary mutations produced these named failures:

    Enum numeric value forced to zero -> test_to_uint32_source_rows FAILED at UInt(0) versus UInt(1)
    Binary literal accepted as JSON null -> test_to_json_source_rows FAILED on BinaryLiteral([129])
    Decimal fit overflow marker forced false -> test_produce_dec_with_specified_tp_source_rows FAILED at 123.99 DECIMAL(3,1)
    Reverse ceiling increment disabled -> test_change_reverse_result_by_upper_lower_bound_source_rows FAILED at UInt(1) versus UInt(2)
    clone_row reversed its output -> test_clone_datum_source_rows FAILED with reversed datum order
    NULL removed from the sentinel lattice -> test_null_not_equal_with_others_source_rows FAILED at the sentinel unreachable guard
    Invalid UTF-8 accepted through lossy conversion -> test_is_printable_source_rows FAILED for [97, 98, 99, 195]

After restoring all saved copies, `git status --short` showed only `?? tgt/` and the same 12-test filter reported `12 passed, 299 skipped`.

The JSON source-row batch split previously combined tests into exact Go-name evidence. Its WIP filter reported `11 passed, 317 skipped`, and the post-batch census reported `46 NAME-EXACT`, `95 NAME-FUZZY`, `19 NAME-TOKENS`, `27 REFERENCED`, and `15 NONE`. `GetKeys` now includes non-object empty results, one and two sorted keys, element count, and the 65,536-byte key error; the parse test pins both Go error texts; and the typed creation test copies an existing BinaryJSON value while recording the dynamic `int8` input as unreachable in Rust.

The JSON batch was saved under `/tmp/tidb-types-json-probes.eqw1rx`. Reversing key sort order, changing array containment from `all` to `any`, deleting array index zero instead of the selected index, adding callback scalar autowrap, removing array hash structure bytes, collapsing the trailing-value error, changing the opaque type-code rendering, replacing typed BinaryJSON copies with JSON null, emptying non-string unquote results, and incrementing array depth each failed its intended exact test. Disabling both object key-length guards also failed `test_get_keys`; disabling only the small-object guard survived because the large-object fallback retained the contract. After restoring both saved files byte-for-byte, the exact eleven-test filter again reported `11 passed, 317 skipped` and `git status --short` showed only `?? tgt/`.

The `etc_test.go` predicate batch reused the same save directory for `field_type_mod.rs`. Removing `NewDate` from `is_type_temporal` failed `test_is_type_temporal_source_rows` on the `NewDate` row. Adding `Unspecified` to `is_type_numeric` failed `test_is_type_numeric_source_rows` on the explicit false row. The first restore attempt was issued from `rust/` with an erroneous extra `rust/` path prefix and therefore changed nothing; rerunning the copy from repository root restored the saved bytes, after which the six-test filter reported `6 passed, 311 skipped`.

## Interfaces and Dependencies

No new third-party dependency is expected. Inventory tooling should use repository Python or Rust standard-library facilities and stable source identifiers. Runtime ports should continue using existing `tidb-datatype` public types such as `Datum`, `Decimal`, `BinaryJSON`, `Time`, `Duration`, `FieldType`, `Enum`, `Set`, and `ConversionContext`; introducing a parallel datatype layer would increase semantic risk and violate the minimal-diff requirement.

Revision note: created on 2026-08-07 to preserve the complete-package LOCKDOWN contract and the post-seed census before continuing `datum_test.go` coverage; updated the same day with datum, type-predicate, and JSON source-row evidence.
