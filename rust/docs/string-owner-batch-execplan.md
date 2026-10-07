# Share string conversion and production policy

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


SQL string casts and table conversion must preserve Go's source-dependent bytes, error identity and statement policy. JSON/vector casts to BINARY must not gain NUL padding or its packet-limit refusal. Strict writes must not silently downgrade truncation inside CAST. Charset errors must preserve their converted prefix/replacements and skip width production. The user can observe these rules through HEX/LENGTH, warnings and INSERT rollback.

## Progress


- [x] Refreshed Go master 7a3dacb52efe58d28db360ae8639d8838c376544; integration starts 466116e6a96c795eaa4c61600626f65024e1544a. Native 8b752f9638ad157931725b66ffdc57e0465432a9 is unchanged.
- [x] Inspected source owners and added seven regressions to existing datatype/expression suites; Go types oracle covers charset and width/error policies.
- [x] Seven Rust regressions failed before; seven of thirteen live SQL checks failed (six controls already passed). The Go types oracle completed eighteen cases.
- [x] Shared encoding dispatch and typed parser1300 retention; migrated AST CHAR/BINARY production; removed three obsolete helpers and the duplicated table dispatch. All seven regressions now pass.
- [x] 90 distinct affected Rust tests, all-target checks, lint, formatting and locked server build pass. All 17 live SQL checks pass, including seven fail-before checks, six retained controls and four DST controls.
- [x] Prepared validated source, receipts and checkpoint instructions for normal publication. Actual hook, fresh pre-push build, remote verification and Cloud save results are recorded in the external final-handoff.json to avoid a self-referential commit.

## Context and Orientation


rust/crates/tidb-datatype/src/datum_convert.rs owns Datum.ConvertTo and ProduceStrWithSpecifiedTp; multibyte_encoding.rs owns bytes plus EncodingError. Its datum string conversion currently discards bytes and flattens encoding errors. tidb-executor/src/driver/write_cast.rs compensates with a separate encoding decision tree before table completion. tidb-expr/src/cast.rs has distinct AST CHAR/BINARY truncation/padding loops while protobuf signatures already use the shared production helper. Both expression paths pad JSON/vector sources although Go builtinCastJSONAsStringSig and builtinCastVectorFloat32AsStringSig return immediately after production.

Go references are pkg/types/datum.go, pkg/parser/charset/encoding_base.go, pkg/table/column.go and pkg/expression/builtin_cast.go at the master above. No package doc.go exists in the selected types/table/expression directories. The changes maintain existing owners in K03/X01; complete package acceptance, N01 raw SQL ingress and unsupported source signatures remain separate obligations.

## Plan of Work


Expose the existing charset conversion decision at the datatype owner, returning its original bytes/error pair for string-like sources. Both datum conversion and the table preflight consume it; retain the table's existing error naming and skip-width policy. Give EncodingError its original parser1300 identity for contextful Datum conversion. Preserve the legacy value/event interface's error contract.

Route AST CHAR/BINARY results through eval_string_cast_with_type and ProduceStrWithSpecifiedTp. Keep binary-argument decoding at the AST wrapper boundary, because protobuf expressions serialize that wrapper separately. Shared padding applies only to Go signatures that call padZeroForBinaryType; JSON/vector skip it. Remove obsolete AST truncation, padding and stringifier helpers after checking every caller.

## Milestones


First, record seven fail-before cases and the Go source oracle. Second, migrate the connected production callers together and verify the retained source-specific behavior. Third, run one grouped validation boundary and publish through the required gates with durable receipts.

## Validation and Acceptance


From /workspace/tidb/rust, source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Run cargo test --locked --no-fail-fast -p tidb-datatype -p tidb-expr --lib -- string_owner_batch_ --test-threads=1 before edits, then group affected datum/encoding/cast tests. Run cargo check --locked --all-targets -p tidb-datatype -p tidb-expr -p tidb-executor -p tidb-server and cargo build --locked -p tidb-server. Run make lint from /workspace/tidb with the same environment. The live MySQL probe uses PYTHONPATH=/workspace/.cloud-setup/python and env.sh's RUST_MIN_STACK=33554432, verifies CAST bytes, strict rollback and charset controls, and joins its owned server on completion.

## Surprises & Discoveries


Table writes already compensate for the datum charset defect with a private preflight. This batch must preserve that valid behavior rather than count it as a newly reproduced write defect. Go datatype conversion returns parser1300 regardless of truncation policy; the table layer translates it before applying statement policy. JSON/vector expression signatures intentionally do not pad BINARY, unlike ordinary string/numeric/temporal signatures.

The grouped retained suites exposed a stale lenient DST expectation (1292). Go table.castColumnValue handles the error as warning 8179 before INSERT.handleErr; strict INSERT alone retitles a returned 8179 as 1292. Correct the lenient expectation, preserve the strict assertion, and cover both through live SQL. This is retained-test maintenance, not a new production DST repair.

## Decision Log


- Decision: reuse typed string production while retaining the AST-only binary decoder.
  Rationale: Go inserts from_binary before the cast; protobuf callers carry explicit wrappers. Implicitly decoding both paths would double-convert remote expressions.
  Date/Author: 2026-10-07 / Codex.
- Decision: keep K03/X01 partial and N01 untouched.
  Rationale: complete conversion/signature/package and raw ingress obligations exceed these existing-owner repairs.
  Date/Author: 2026-10-07 / Codex.

## Idempotence and Recovery


Preserve concurrent work and dependency caches. Only retire identified inactive test executables if necessary for link space, retaining hashes, sources and logs. Do not replay one-use edit scripts. No dependencies/generated Go sources change; Bazel regeneration is not applicable. Never bypass hooks or force push. Saving a Cloud draft does not establish Publish or fresh restoration.

## Interfaces and Dependencies


Retain DatumConversion and ConversionContext as the value/error and warning-policy authorities. EncodingResult carries transformed bytes and EncodingError before production. Existing StmtContext finishes table diagnostics. No new transport, dependency or protocol is required.

## Artifacts and Notes


Working evidence is under /workspace/.cloud-setup/string-owner-batch. Commit the final validation receipt and update both finding registers; actual publication and Cloud readback follow in final-handoff.json outside the checkout.

## Outcomes & Retrospective


Implemented and validated the connected charset, truncation and padding repairs. Removed three AST helpers and the duplicate table dispatch. Seven fail-before Rust regressions now pass within 90 distinct affected tests; 17 live SQL checks pass. The grouped run exposed and corrected one stale lenient DST test without changing production DST behavior. K03/X01 remain partial; no complete package is accepted. Commit/push and Cloud checkpoint evidence follows in the external final handoff.
