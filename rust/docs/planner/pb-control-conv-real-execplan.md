# Preserve protobuf control, CONV, and real-cast contracts

This living ExecPlan follows PLANS.md.

## Purpose / Big Picture


Repair the three confirmed discrepancies in existing expression support: Go accepts string IF/CASE signatures, evaluates CONV bases before its ordinary string argument, and applies the complete result field type to string-to-real casts. This is a correctness checkpoint, not a whole-package transcreation claim.

## Progress


- [x] Audit matching protobuf inputs against Go master 4b2221cddf; pull integration branch to 473f89543b.
- [x] Preserve failing regressions: three request tests and the builder metadata test fail on the baseline.
- [x] Repair native typed dispatch and string control construction.
- [x] Run targeted validation, make lint and review.
- [x] Prepare the reviewed changes for commit and publication to hparser-integration.

## Context and Orientation


Shared wire dispatch lives in rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs. Its Kernel enum retains the selected implementation. The pushdown_catalog.rs constructs signatures and casts. Datatype datum_convert.rs already implements Go's float production (precision and range checking). Request tests are in tidb-unistore/src/cophandler/eval_context.rs.

Go reference sources are pkg/expression/builtin_control.go, builtin_math.go, builtin_cast.go and distsql_builtin.go. /private/tmp/tidb-go-structural-20260927 is a reference archive; relevant audited files match fetched master. /private/tmp/pb-more-audit.md records the original observations and exact commands.

## Milestones and Plan of Work


First add permanent assertions for the three observed cases to the request-context tests and run them before production edits. Then retain String domains in IF/CASE kernels, give CONV its own typed evaluator with Go's argument ordering, and reuse float production with original errors after the source-specific cast. String control construction must derive branch metadata using existing native inference, rather than selecting a collation from one arbitrary operand. Retain Go's existing refusal for unsupported inputs.

Finally run the focused decoder/request/catalog tests and server compilation and integration tests, plus make lint. Preserve unrelated changes and publish only reviewed changes to hparser-integration.

## Decision Log


2026-09-28: Follow signature-specific contracts, not generic eager evaluation. Go's CONV evaluates bases before its ordinary string input; real casts have distinct source-domain behavior. No new dependencies or generated output edits are needed.

## Surprises & Discoveries


The audit returned unsupported IfString/CaseWhenString, NULL instead of a strict conversion error for CONV(NULL, invalid integer cast, 10), and 1.26 instead of 1.3 for a string-to-DOUBLE(3,1) cast. Go assertions passed for all cases.

## Validation and Acceptance


From repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib pb_contract -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib pushdown_catalog -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

Use the existing failpoint wrapper for Go probes in the reference archive. Rust-only changes do not require bazel_prepare. Require formerly failing assertions to pass; check strict and warning modes, lazy branches and metadata. Benchmark gains and full package parity are not validation claims of this change.

## Idempotence and Recovery


Run Cargo only while sources are stable. Keep Go probes outside the checkout using an overlay and let the failpoint wrapper restore state. Preserve any concurrent changes when pulling before publication.

## Outcomes & Retrospective


The original three request regressions now pass. The constructor regression also failed before the fix and passes after native branch aggregation, including reversed branch order, nested string controls, mixed numeric/string domains and aligned decimal precision.

String IF/CASE now use the shared typed control kernels. CONV owns its typed base-first evaluator, preserving NULL short circuit and statement warning/error policy. String-to-real casts reuse datatype float production with complete metadata and typed errors; real-to-real casts retain their distinct Go behavior.

Native CASE collation derivation was still deferred; it now aggregates only value branches like Go. Control descriptions store inferred metadata once, so encoding and enclosing inference reuse it. Descriptions built from native expressions retain the original resolved type and collation. Small executor match/test updates accommodate the added metadata field.

The first extended Go constructor probe used NewFunction, which folded IFNULL with a non-NULL first literal to that literal (width 8). Switching the probe to NewFunctionBase, Go's no-fold constructor, compares the intended construction boundary and confirms merged width 30 in either branch order. This does not claim a new constant-folding implementation.

Validation passed: catalog 26 tests, decoder 17, collation 4, datatype 32, coprocessor 82, scan/selection 16, pushed server queries 3; server all-target compilation, Go probes, make lint and git diff --check also passed. No benchmarks, full Rust/Go package suite, or RealTiKV tests have run. Existing unrelated full-suite failures remain outside this checkpoint.

## Validation Commands and Evidence


All Cargo commands use --offline --locked --manifest-path rust/Cargo.toml and -- --test-threads=1 for tests. Logs are /private/tmp/pb-contract-*.log. The exact final commands are:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib pushdown_catalog -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib collation_derive -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-datatype --lib datum_convert -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all wide_scan_selection_source -- --test-threads=1
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

From /private/tmp/tidb-go-structural-20260927:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-more-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRust(MorePBContracts|PBControlMetadata|PBRealProductionErrors)$' -count=1 -v

The Go command passes and the failpoint refcount returns to zero. Reference probes are temporary files; permanent regressions are in the Rust suites. No Go/Bazel or generated files changed, so bazel_prepare is not required.

## Changed Files and Compatibility


Production changes are in rust/crates/tidb-expr/src/{pushdown_catalog.rs,collation_derive.rs,scalar_function/pb_builtin.rs} and rust/crates/tidb-datatype/src/{datum_convert.rs,lib.rs}. rust/crates/tidb-executor/src/predicate_pushdown.rs ignores the new immutable metadata when remapping column offsets; its kv_table/table_scan.rs test initializer includes the new field. Permanent request tests are in rust/crates/tidb-unistore/src/cophandler/eval_context.rs and SQL integration assertions in rust/crates/tidb-server/src/cluster_session_node/tests/unistore_cop.rs. This plan is the only documentation change.

Strict CONV errors previously hidden by NULL now surface; warnings are retained exactly once. Numeric precision and overflow follow the original datatype producer. String control pushdown becomes available with native branch collation aggregation, so execution placement can change. Inferred control metadata is retained once and reused during encoding. Performance improvements are not claimed without measurements.
