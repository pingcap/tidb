# Shared JSON numeric conversion batch

## Purpose and context


Repair the remaining K03 write-conversion and X01 typed-expression gaps together: JSON strings, booleans and nonnumeric documents must produce Go's numeric values, original errors and ordered warnings across table casts and expression arithmetic. Work starts at integration 8025bb156829edb1b247da9c4dcccfd10fbe340d in /workspace/tidb; native /workspace/client-rust remains cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1. Refreshed Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed is exported at /workspace/.cloud-setup/go-master. Compare pkg/types/{convert,datum}.go and pkg/expression/builtin_cast.go. This is maintenance of existing owners, not complete package acceptance.

## Milestones


First reproduce the related failures in existing datatype, expression and executor suites under the json_numeric_batch filter. Then move numeric JSON diagnostics into the existing datatype conversion owner and migrate table/expression consumers, deleting their duplicated branches. Finally validate the grouped regressions, neighboring source suites, real MySQL/unistore behavior, affected all-target compilation and make lint. Update the finding registers without closing broader obligations.

## Progress


- [x] Recheck live source and select related K03/X01 gaps.
- [x] Capture four failing Rust regressions and six MySQL assertions before production edits.
- [x] Implement shared source diagnostics and migrate selected datatype, table, numeric-cast, arithmetic and general integer-wrapper consumers.
- [x] Validate behavior, self-review, update receipts and registers: 129 Rust cases and seven real MySQL assertions pass; eleven neighboring tests remain ignored. All-target checks, lint and locked server build pass.
- [ ] Commit with actual hook, fresh locked build immediately before authorized push, verify remote and save startup checkpoint.

## Validation and recovery


Source /workspace/.cloud-setup/env.sh; run Cargo from /workspace/tidb/rust with CARGO_BUILD_JOBS=1. Run cargo test --locked -p tidb-datatype -p tidb-expr -p tidb-executor --lib --no-fail-fast json_numeric_batch -- --test-threads=1. Logs and wire receipts belong in /workspace/.cloud-setup/json-numeric-batch. Run affected all-target cargo check and make lint at root. The actual precommit hook and immediate pre-push gate must each pass cd rust && cargo build --locked -p tidb-server. Never bypass hooks or force push. Preserve concurrent edits; undo only this batch's own edits if interrupted. No native dependency changes are planned.

## Surprises & Discoveries


JSON integer arithmetic currently parses the serialized document, turning a JSON string such as "7" into zero. The table converter excludes all JSON sources from contextual diagnostics. Go ConvertTo(DECIMAL) returns an unset datum on fatal JSON source conversion, before target fitting.

## Decision Log


Keep diagnostic ownership at the source conversion stage, and keep destination production and caller-specific INSERT/UPDATE error completion at their current owners. Extend existing behavioral suites without creating test harnesses. Temporal targets remain a separate obligation. The failing expression test exposed two further live owners: ops.rs serialized JSON bit operands, and numeric_batch_supported excluded JSON numeric casts. Both migrate with this batch; the fifth regression explicitly validates scalar/vector selected rows and NULLs.

## Outcomes & Retrospective


The batch repairs original diagnostic identity and warning order, JSON bitwise values and typed numeric JSON batch admission through shared owners. Four Rust and six MySQL baseline failures are repaired; the final suite passes 129 Rust cases and seven MySQL assertions. Eleven existing tests remain ignored. The register now has 86 findings, 30 repaired and 56 unresolved (27 open, 29 partial): X01 advances to partial; neither broad parent is closed. No full Go package, multi-node or performance acceptance. Publication results follow in /workspace/.cloud-setup/json-numeric-batch/final-handoff.json and saved startup instructions after the actual commit/push gates; this committed pre-publication receipt does not claim those future operations already succeeded.
