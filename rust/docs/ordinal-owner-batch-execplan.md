# Share YEAR, ENUM, SET and BIT conversion diagnostics

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Table conversion must retain the value and each warning/error that Go produces. Four remaining targets bypass the typed conversion owner: YEAR, ENUM, SET and BIT. Moving them together removes table event reconstruction and fixes source-warning ordering, YEAR early returns, ENUM/SET error identity and BIT width precedence. Observe the result through datatype conversions and SQL writes under strict, warning and ignore policies.

## Progress


- [x] Confirmed clean integration at 88086505a3805d90205561b08b9cc6e7e45a2153; fetched Go master 7a3dacb52efe58d28db360ae8639d8838c376544. Native 8b752f9638ad157931725b66ffdc57e0465432a9 is unchanged.
- [x] Five grouped Rust regressions failed before; six of seventeen baseline SQL checks failed. Expanded Go oracle: 120 cases, all compared directly with Rust value, SQL error and ordered warnings.
- [x] Migrated four targets and every table caller; removed event reconstruction, ODKU warning rewrite and its statement API. Retained the small event helper still used by three DDL callers.
- [x] 70 final Rust tests, 120 direct Go comparisons, all-target checks, lint, formatting, locked server build and all29 SQL checks pass.
- [x] Maintained both registers, repair sequence and durable validation receipt; prepared publication. Actual hook, pre-push build, remote verification and Cloud checkpoint outcomes follow in external final-handoff.json.

## Context and Orientation


rust/crates/tidb-datatype/src/datum_convert.rs owns Datum.ConvertTo. Its diagnostics.rs carries a typed error beside the value and emits warnings through ConversionContext. Four targets still emit a single ScalarConversionEvent, losing error identity and earlier warnings. rust/crates/tidb-executor/src/driver/write_cast.rs reconstructs errors using destination type. Go references are pkg/types/datum.go, time.go, enum.go, set.go, binary_literal.go and pkg/table/column.go at the pinned master above. No selected package doc.go exists. This maintains existing K03 owners; it is not a complete package claim. X01 remains separate unless an expression caller changes.

## Plan of Work


Reuse reported integer and binary conversion helpers. YEAR stops on fatal source errors and applies AdjustYear only after success. ENUM/SET retain typed truncation 1265 and consume handled source warnings before parsing the numeric result. BIT processes source conversion before width errors. Migrate table callers, preserve ENUM/SET from generic 1265 retitling, then remove unreachable event reconstruction. Reuse existing test suites; add no repository harness.

## Milestones


First capture a Go oracle and grouped failing regressions. Second implement all four targets and caller migrations before rebuilding. Third validate the connected boundary and publish with evidence. Complete packages and unrelated runtime owners remain unresolved.

## Concrete Steps


In /workspace/tidb/rust source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Run cargo test --locked --no-fail-fast -p tidb-datatype -p tidb-executor --lib -- ordinal_owner_batch_ --test-threads=1 before and after production changes. The grouped command actually ran from /workspace/tidb with --manifest-path rust/Cargo.toml and filters datum_convert:: enum_set_tests:: binary_literal_tests:: driver::write_cast::. It selected 51 datatype tests and 16 executor tests; the Rust crate-local parallel-frontend option was not active for this invocation. The required server build and publication builds run from rust/ as prescribed. Run cargo check --locked --all-targets -p tidb-datatype -p tidb-executor -p tidb-server, and cargo build --locked -p tidb-server. Run make lint from /workspace/tidb with the same activation. Live SQL uses the built unistore server and joins its owned process afterward.

## Validation and Acceptance


Strict YEAR from malformed numeric text returns zero beside its source error; warning mode retains and adjusts the prefix. ENUM/SET retain 1265 while earlier handled warnings remain first. BIT preserves source warnings before 1406 width failure. Regressions must fail before and pass after; neighboring conversions must remain valid. Full Go suites, live TiKV and performance remain unverified.

## Surprises & Discoveries


Plain invalidConv failures use the existing unregistered compatibility error form to preserve SQL fallback 1105 beside Go's zero/NULL value. Go BIT conversion checks lengths below 128, but NewBinaryLiteralFromUint panics for byte sizes above eight. Retain Rust's safe unsupported-domain rejection. Expression CAST AS YEAR has distinct signatures and must not blindly use table YEAR adjustment.

The intermediate live run exposed ENUM debug text in ODKU warnings. Shared Datum.sql_bytes already owns Go ToString, so remove the duplicate formatter and add exact ENUM/SET ODKU assertions. Final validation includes the existing stringify suite. Unrelated cargo fmt output was reverted before validation; final formatting was scoped to edited modules.

## Decision Log


- Decision: group four targets and their table callers as one owner migration.
  Rationale: they share a diagnostic bypass and obsolete reconstruction, allowing one validation boundary.
  Date/Author: 2026-10-07 / Codex.

## Idempotence and Recovery


Preserve concurrent work, caches and exact push destinations. No dependency or Go files change; Bazel regeneration does not apply. Never bypass hooks or force-push. Actual pre-commit must run cargo build --locked -p tidb-server; rerun immediately before each push and verify remote SHA. Retire only verified inactive completed test executables with recorded hashes if link space requires it. Cloud saving is not Publish or fresh restoration.

## Interfaces and Dependencies


Keep DatumConversion, ConversionContext, enum/set parsers and StmtContext as owners. No new dependencies or transports. Legacy value/event callers retain their API while sharing conversion stages.

## Artifacts and Notes


External evidence lives in /workspace/.cloud-setup/ordinal-owner-batch. Commit a validation receipt and update both finding registers. Publication and configuration readback evidence belongs in external final-handoff.json to avoid a self-referential commit.

## Outcomes & Retrospective


Four targets now share conversion stages and every table caller consumes their typed outcomes. Five initial Rust regressions now pass within 70 final tests; direct comparison matches all120 Go cases. Six baseline SQL failures and a later ENUM ODKU diagnostic-format failure are repaired; all29 final SQL checks pass. Publication remains pending.
