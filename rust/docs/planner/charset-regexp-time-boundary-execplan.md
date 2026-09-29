# Follow Go at shared expression boundaries

This living ExecPlan follows `PLANS.md` and records implementation against the complete owning Go package inventory, without claiming that selected repairs constitute a transcreated package.

## Purpose / Big Picture

Resolve the five root causes captured by `pb-boundary-audit-20260929`: bypassed charset case mappings, duplicate wildcard semantics, differing regexp contracts, lost negative fractional timestamp signs, and repeated/missing timestamp precision application. Expressions should reuse Go-shaped shared services and retain their typed metadata. The 23,568-row Go capture is the primary differential regression surface.

## Progress

- [x] Read instructions, fetch target/master; worktree initially clean at 0ee4452bd7.
- [x] Identify shared encoding/collator primitives and existing tidb-util Go regexp adapter.
- [x] Permanent regression failed with all 4,032 audited differing rows before production edits.
- [x] Charset/wildcard primitives integrated; typed timestamp precision and negative-fraction handling corrected, retaining negative zero.
- [x] Shared regexp adapter integrated; captured syntax/classes/byte-text cases pass. Added source-derived grammar tests for quoting, ranges, repeats and flags.
- [x] Scoped suites, 23,568-row regression, earlier 8,298-row replay, and lint completed. Locked server builds remain mandatory publishing gates.
- [x] Diff reviewed; changes prepared for the hook-enforced commit and immediate pre-push build. Git history records publication.

## Context and Orientation

Go reference is master 3425d7860550d38b43c6faa9f3078647b2796abb. The audit's package inventories cover pkg/expression, pkg/parser/charset and pkg/util/collate; these remain atomic completion units, with unresolved coverage outside these repairs. Rust expression dispatch is in tidb-expr/src/scalar_function.rs and scalar_function/pb_builtin.rs. Encoding and collator implementations already live in tidb-datatype. tidb-util/src/go_regexp.rs already translates Go ASCII Perl classes but is private and does not fully constrain Rust regexp syntax. Time conversion is in tidb-expr/src/time_fn/session_tz.rs.

## Plan of Work

Add a regression in tidb-unistore's existing cophandler evaluation tests using the compressed captured Go matrix and the real request context. Observe 4,032 differing rows before fixing production code. Preserve warnings and row modes in the check.

Route typed case conversion through Encoding selected from the argument FieldType. Replace expression wildcard compilation/matching with the collator-owned WildcardPattern, preserving statement cache lifetime. Add an explicit result-precision parameter to shared FROM_UNIXTIME production, preserve the decimal sign, and use that operation in both protobuf and native typed dispatch.

Extend the existing Go regexp adapter using syntax structure, retaining the Rust engine as execution backend. Use one compiler in expression and utility consumers. Validate Go syntax restrictions, translate ASCII Perl classes and word boundaries, and preserve Go's invalid text rune interpretation in predicate evaluation. Do not silently claim complete Go regexp package parity from the finite matrix; document uncovered diagnostics and positional byte-offset risks.

## Milestones

First establish a failing permanent regression. Next make encoding/wildcard/time rows pass through shared implementations. Finally make regexp boundary rows pass, checking existing utility and expression clients before removing duplicate behavior. Each milestone is verified with the captured Go inputs, not self-derived expectations.

## Concrete Steps and Validation

From repository root run targeted cargo tests with --offline --locked --manifest-path rust/Cargo.toml. The regression is `typed_pb_boundary_matches_go` in tidb-unistore. Run existing tidb-expr string, LIKE, regexp, temporal tests and tidb-util regexp consumers, plus relevant tidb-session SQL tests. No Go/Bazel files or dependencies are planned, so bazel_prepare is not required. Run make lint for production changes. Both commit-hook and immediately-pre-push commands must run `cd rust && cargo build --locked -p tidb-server` successfully.

## Decision Log

- Decision: reuse existing primitives and adapter rather than patch individual inputs. Rationale: direct primitives passed 5,064 Go checks while expression routes failed. Date: 2026-09-29.
- Decision: keep package completion claims separate from these repairs. Rationale: package inventories and earlier audits still contain unimplemented behavior; passing a boundary matrix cannot close those receipts. Date: 2026-09-29.

## Surprises & Discoveries

The existing private utility regexp adapter provides reusable ASCII-class translation; expression compilation bypassed it. Case tables and wildcard byte/rune selection already exist and were proven against the captured Go data.

## Outcomes & Retrospective

All 23,568 captured cases now agree in results, error states and warning counts, resolving all 4,032 differences in this audit. The older 8,298-row audit still has exactly the same 392 documented differences (the full difference JSON is identical), so those earlier gaps remain open. Native SQL checks confirm GBK/GB18030 case mapping, regexp classes/set syntax and negative fractional timestamps. No package-completion or performance claim is made.

The expression library reports 1,240 passes, 93 ignored, and the same three previously recorded failures: builtin_info_json_math_source::exp, builtin_math_misc_op_source::vectorized_builtin_op_func, and time_fn::tests::str_to_date_partial_formats_follow_no_zero_date. The utility library passes 573 tests with 2 ignored; cophandler passes 84; the SQL regexp family passes 2. The permanent differential test passed after the final grammar changes. make lint passed.

These gates do not establish complete Go regexp package parity. Full syntax/error-diagnostic equivalence, positional regexp invalid-byte offsets, all native/row/vector routes, and full package receipts remain unverified. The captured matrix compares error states rather than all SQL error codes/messages. No sysbench/TPCC/TPCH/YCSB benchmark was run. Existing statement cache lifetimes are retained; performance implications of shared matcher selection and compile-time normalization are unmeasured.

## Idempotence and Recovery

Tests and builds are repeatable. Temporary replay runners restore source files and must not run concurrently with edits. Preserve unrelated user changes. Review only the intended diff before staging. No history rewriting or force push is required.

## Artifacts and Interfaces

Audit fixtures and SHA256 records remain immutable. Regression tests read their captured rows. New shared interfaces should accept typed charset/result precision explicitly and should not infer them again from per-row values. The regexp adapter remains a native Rust implementation; no Go runtime dependency is introduced.


## Validation receipt

Commands executed from repository root (logs retained in /private/tmp):

    cargo test --offline --manifest-path rust/Cargo.toml -p tidb-unistore --lib typed_pb_boundary_matches_go
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib typed_pb_boundary_matches_go
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib go_syntax_and_character_contracts
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib from_unixtime_goeval_vectors
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --test all regexp_family_source
    python3 /private/tmp/run-pb-expanded-rust.py
    python3 /private/tmp/compare-pb-expanded.py
    make lint
    git diff --check

The first run intentionally failed and updated Cargo.lock for the already present flate2 dev dependency; subsequent runs were locked. Earlier replay scripts are preserved in pb-expanded-audit-20260928. Both Rust collectors succeeded and the resulting differences.json was asserted equal to the committed prior artifact. Production sources were restored after temporary replay. No Go/Bazel source or dependency changed; bazel_prepare was not required.

Publishing uses `git -c core.hooksPath=hooks commit`, whose required locked server build must pass, followed by `(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration`. The commit/push must stop if either build fails.

Revision 2026-09-29: replaced duplicate expression implementations with shared primitives, integrated Go regexp grammar/class adaptation, recorded before/after and SQL validation, and explicitly retained package/error/benchmark coverage limits.
