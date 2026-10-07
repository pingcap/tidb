# Share numeric conversion sources and decimal production

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Fix connected K03 table conversion and X01 expression casting defects against Go master 7a3dacb52efe58d28db360ae8639d8838c376544. DECIMAL writes must retain original source errors and warning order; ENUM/SET must use Go's float-to-decimal source path; invalid vector conversions retain NULL and a plain error; legacy callers retain their error-return API. Binary-literal casts return their numeric value without destination fitting. This maintains existing owners and does not claim complete package transcreation.

## Progress


- [x] Verified clean hparser-integration at c64fc5eb6f92f289fa00b5cbf8fb5a701b8cac70 and refreshed Go master; native remains unchanged.
- [x] Read root instructions and Go types/expression source. Captured baseline wire evidence and Go numeric oracle; corrected unsigned-DECIMAL hypothesis before editing.
- [x] Four regressions failed before; 60 of120 direct comparisons differed. Migrated shared sources/production and AST/protobuf literal callers; removed duplicate fitting and event reconstruction.
- [x] 101 distinct Rust tests,120 direct Go comparisons and22 live SQL checks pass; affected all-target check, lint, scoped format and locked build pass.
- [x] Updated both registers and batch map; self-review and scoped formatting pass. Prepared publication; actual hook, fresh build/push, remote SHA and saved Cloud checkpoint outcomes follow in external final-handoff.json.

## Context and Orientation


rust/crates/tidb-datatype/src/datum_convert.rs owns table conversion and standalone ProduceDecWithSpecifiedTp. Its duplicated table decimal fitting path reconstructs coarse scalar events instead of retaining MyDecimal errors. diagnostics.rs owns the returned typed error and warning sink. rust/crates/tidb-expr/src/cast.rs and scalar_function/pb_builtin.rs select expression source signatures. Go references are pkg/types/datum.go, mydecimal.go and pkg/expression/builtin_cast.go; no package doc.go is present for these selected sources.

## Plan of Work and Milestones


First add behavioral cases in the nearest datatype/expression suites and capture a combined failing run. Then use MyDecimal source conversion directly, share decimal production while retaining table versus expression completion, and remove redundant conversion branches only after callers migrate. Finally validate the whole connected segment and publish receipts. Do not change native dependencies or accept whole packages from these cases.

## Concrete Steps and Validation


From /workspace/tidb/rust, source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Run cargo test --locked --no-fail-fast -p tidb-datatype -p tidb-expr --lib -- numeric_production_batch --test-threads=1 before edits. After edits group neighboring datum_convert and cast tests, then cargo check --locked --all-targets -p tidb-datatype -p tidb-expr -p tidb-server. Run make lint from /workspace/tidb. Build with cargo build --locked -p tidb-server from rust/; exercise a real unistore server and join its owned process. Actual precommit and fresh prepush builds remain mandatory. Evidence belongs in /workspace/.cloud-setup/numeric-production-batch and one committed validation JSON.

## Surprises & Discoveries


Go MyDecimal.FromUint preserves its negative bit. Unsigned table DECIMAL conversion therefore still returns overflow even after production zeroes the magnitude; the oracle disproved an initial source-reading hypothesis. Do not change that behavior. The initial live warning expectation confused datatype diagnostics with INSERT completion: Go retitles/completes both warnings as1366. After correcting only that expectation, all22 live checks pass. Resource limits require retiring only completed inactive executable artifacts with hashes and hardlink accounting; retain compiler caches.

## Decision Log


- Decision: group source diagnostics, common decimal fitting, invalid numeric source results and literal cast admission.
  Rationale: table and expression paths share value production but have different source/error policies; testing their boundaries together prevents accidental policy unification.
  Date/Author: 2026-10-07 / Codex.

## Idempotence and Recovery


Preserve concurrent changes and exact destinations. Do not force-push or bypass hooks. Scope formatting to edited files. No Go/manifests change, so Bazel regeneration does not apply. External scripts are receipts, not new repository harnesses. Cloud draft saving does not establish Publish or fresh restoration.

## Interfaces and Dependencies


Reuse MyDecimal, Decimal, ConversionContext, Diagnostics and DatumConversion. Preserve legacy value/event interfaces and source warnings. No new dependency or transport is required.

## Outcomes & Retrospective


101 selected Rust tests pass (50 datatype,51 expression), including five new regression methods. Direct comparison matches all120 Go datatype cases. Self-review retained legacy unsupported-conversion errors; the50 datatype tests passed again. All-target/server checks, lint and all22 wire checks pass. Publication results follow in external final-handoff.json. Both parent findings remain partial; the other54 unresolved roots retain carried evidence.
