# Complete and certify `pkg/util/column-mapping` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's column-mapping package selects one schema- or table-level rule, rewrites one row value with a prefix, suffix, or packed partition ID, and deliberately rejects matched DDL because DDL rewriting is not implemented. This plan audits every Go production, test, documentation, and build artifact; maps every source assertion and public branch to Rust; closes source-observed case-folding and configuration gaps; validates the owning crate; and publishes the result as one Go package commit.

## Progress

- [x] (2026-08-11 17:35Z) Fixed the complete four-file Go inventory at `51e1e13494f5e547be10601a302d6cb9cf88ae64`; current package bytes match that pin.
- [x] (2026-08-11 17:36Z) Confirmed there is no `doc.go`, generated input, fixture, testdata, build/platform variant, benchmark, fuzz target, example, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-11 17:38Z) Read all Go production/test/documentation/build artifacts and the complete Rust owner, then mapped all seven Go tests to Rust assertions.
- [x] (2026-08-11 17:39Z) Passed the complete Go source suite normally and under `-race`; passed the 10-test focused Rust baseline.
- [x] (2026-08-11 17:40Z) Used a public Go probe to fix simple Unicode lowercase and missing/null JSON-field behavior.
- [x] (2026-08-11 17:46Z) Added two source-derived regressions, retained both failures against the old Rust implementation, and made the smallest production corrections.
- [x] (2026-08-11 17:49Z) Passed both exact regressions, the 11-test focused suite, formatting after one mechanical adjustment, and WIP library Clippy; added the atomic semantic receipt.
- [x] (2026-08-11 17:56Z) Completed pre-sync Ready validation and self-reviewed the final one-package diff on base `8520ab126cc2de2eb1f558a3bc4d8c305cfa59a2`.
- [x] (2026-08-11 18:05Z) Fresh fetch found the independent versioninfo/consumer commit `db23aa671cb8bae673d58f537fe80018ef40abd9`; rebased cleanly and repeated the complete Ready profile. Publication follows this immutable evidence snapshot by ordinary push and fresh remote-SHA verification.

## Surprises & Discoveries

- Observation: all seven top-level Go tests have direct Rust assertion coverage.
  Evidence: rule validation, row/DDL handling and cache behavior, partition-info lookup, partition-rule configuration, partition-bit vectors and errors, origin-ID rewriting, and case-sensitive matching are present. Rust adds rule lifecycle/table priority, public config names, and concurrent caller coverage.

- Observation: case-insensitive normalization differs for Unicode characters with full lowercase expansion.
  Evidence: Go `strings.ToLower` maps `İDB` to the three-rune string `idb`; current Rust `str::to_lowercase` maps it to `i\u{307}db`. A public Go mapping with exact schema rule `idb` matches input `İDB` and rewrites `7` to `tenant:7`; current Rust cannot match that exact rule.

- Observation: Rust exposes the source config field names but rejects omitted fields that Go fills with zero values.
  Evidence: `encoding/json` accepts `{}` for Go `Rule`, and the package permits empty `SourceColumn` and `CreateTableQuery` in otherwise valid rules. Current Rust derives `Default` but lacks `#[serde(default)]`, so partial documents fail before `Rule::valid` can apply source validation. The same owning crate already uses struct-level Serde defaults for analogous rule configs.

- Observation: Go's dynamic and pointer domains do not map literally to native Rust types.
  Evidence: nil `*Mapping` and nil `*Rule` are no-ops, while Rust uses non-null owned references; Go `any` distinguishes several integer runtime types, while Rust normalizes the accepted numeric result domain to `Value::Int(i64)`; Go mutates caller-owned rules during case normalization/defaulting, while Rust consumes or clones rules. These ownership/type differences have no Go or Rust repository consumer and remain explicit native API decisions.

- Observation: malformed partition bit configurations differ intentionally outside the documented domain.
  Evidence: Go performs unchecked package-global integer/shift arithmetic; Rust accepts unsigned sizes, checks their sum, and leaves one sign bit. The README documents a signed packed ID layout and every source test uses a total at most 19 bits. Reproducing invalid wrap/zero behavior would weaken the Rust invariant without preserving a tested or consumed contract.

- Observation: the package has no live Go or Rust consumer in this repository.
  Evidence: the Go import path appears only in its Bazel target and the Rust module is only exported from `tidb-util::lib`; validation therefore terminates at the complete owning crate rather than inventing a downstream gate.

- Observation: Go exposes broader dynamic surfaces than the native Rust owner.
  Evidence: Go embeds the public Selector in Mapping, exposes the mutable `Exprs` dispatch map, and can return the original DDL statement together with an error. Rust keeps its typed selector private, uses closed expression dispatch, and returns a single `Result`; no repository consumer uses the Go-only mutation/insertion or count-plus-error-style surface.

- Observation: explicit JSON nulls and nil slices have a representation difference beyond missing-field defaults.
  Evidence: Go accepts null into scalar Rule fields and distinguishes a nil `Arguments` slice from an allocated empty slice when marshaling. Rust's ordinary Serde strings reject explicit null and `Vec<String>` has one empty representation. Rule validation and every package operation treat nil and empty arguments identically; repository configs do not consume explicit null here.

## Decision Log

- Decision: Use `tidb_mysql::to_lowercase` for rule and lookup normalization.
  Rationale: this existing dependency implements Go's pinned simple-rune `strings.ToLower` semantics and is already used by sibling `tidb-util` routing/filter packages.
  Date/Author: 2026-08-11 / Codex

- Decision: Add struct-level `#[serde(default)]` to `Rule` and test a useful partial config.
  Rationale: missing Go config fields receive their zero values; `Rule` already derives `Default`, and this is the established mapping in sibling public config structs. Nil-versus-empty slice serialization remains a native representation difference because production validation treats both identically.
  Date/Author: 2026-08-11 / Codex

- Decision: Retain non-null ownership, normalized integer values, synchronized process-global configuration, and valid-domain bit-size checks.
  Rationale: these are native Rust integration boundaries or stronger concurrency/invariant guarantees. No repository consumer requires Go's nil receiver, dynamic type provenance, caller-visible rule mutation, data race, or malformed configuration behavior.
  Date/Author: 2026-08-11 / Codex

- Decision: Retain typed private selector/expression dispatch and Rust's single-result DDL error surface.
  Rationale: they prevent invalid selector payloads and runtime dispatch mutation, while the DDL error still preserves the source rejection and full statement in its message. No Go or Rust repository consumer requires the broader dynamic surface.
  Date/Author: 2026-08-11 / Codex

- Decision: Align omitted config fields, but retain native explicit-null and empty-vector representation.
  Rationale: omitted optional fields are a normal valid Rule configuration and map directly through `Default`; adding field-specific null adapters and a nil/empty wrapper would add public complexity without changing validation or any repository workflow.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat `51e1e13494f5e547be10601a302d6cb9cf88ae64` as the accepted Go package pin.
  Rationale: it is the latest commit changing a direct package artifact, contains all four current files, and current bytes match exactly.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The complete inventory, source pin, Go normal/race baseline, Rust focused baseline, assertion mapping, consumer search, public source probes, failing regressions, implementation corrections, receipt, pre- and post-sync Ready validation, and final self-review are complete. Ordinary push and fresh remote-SHA verification are the terminal publication operations performed after this evidence snapshot.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/column-mapping/BUILD.bazel`, `README.md`, `column.go`, and `column_test.go`. `column.go` owns process-global partition sizing, expression dispatch, tagged Rule configuration, selector and cache lifecycle, row and DDL handling, prefix/suffix mutation, and partition-ID parsing/packing. It depends on `pkg/util/table-rule-selector` and `github.com/pingcap/errors`. The Bazel target is a flaky short unit test with testify only.

The seven Go tests cover validation, case-insensitive row handling and cache reuse/reset, matched/unmatched DDL, partition metadata lookup, process-global bit configuration, successful/error partition vectors, integer/string origin IDs, and case-sensitive nonmatching behavior.

Rust owns the mapping in `rust/crates/tidb-util/src/column_mapping.rs`, exports it through `rust/crates/tidb-util/src/lib.rs`, and uses `rust/crates/tidb-util/src/table_rule_selector.rs`. `tidb-mysql` already supplies the Go-compatible simple case table. There is no separate live consumer.

## Milestones

The source-oracle milestone inventories and pins all four Go artifacts, lists exactly seven tests, passes normal and race runs, and records public Unicode/config behavior. Acceptance is the exact `idb` normalization, `tenant:7` rewrite, and successful missing/null field decoding.

The parity milestone adds failing Rust regressions before production changes. Acceptance is exact simple-lowercase matching plus successful useful partial config decoding, with all original and extra Rust assertions still passing.

The publication milestone adds the current receipt and plan, runs the complete Ready profile, synchronizes one commit to current `hparser-integration`, pushes without force, and verifies matching local and fresh remote SHAs.

## Plan of Work

First add focused Rust regressions for an exact ASCII rule matched by source-simple Unicode lowercase input and for a partial tagged Rule config. Run each exact test against the old implementation and retain its failure. Then import the existing Go-compatible lowercase helper, replace only the two normalization sites, and enable defaulted struct deserialization.

Review the complete diff against every Go assertion and all production branches. Add a semantic receipt containing the accepted Go pin and owning-crate evidence. Run focused WIP checks, then Ready validation with Go normal/race tests and source probe, the full `tidb-util` suite, formatting, all-target Clippy, the semantic gate, and repository lint. The Bazel gate remains negative unless later edits trigger it.

## Concrete Steps

From repository root, run the Go authority and public probe:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/column-mapping
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestRule|TestHandle|TestQueryColumnInfo|TestSetPartitionRule|TestComputePartitionID|TestPartitionID|TestCaseSensitive)$' -tags=intest,deadlock -count=1 ./pkg/util/column-mapping
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^(TestRule|TestHandle|TestQueryColumnInfo|TestSetPartitionRule|TestComputePartitionID|TestPartitionID|TestCaseSensitive)$' -tags=intest,deadlock -count=1 ./pkg/util/column-mapping
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-column-mapping-unicode-probe.go

From `rust`, run focused and Ready Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'column_mapping::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/column_mapping.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

Go must list exactly the seven named tests, and normal plus race-enabled runs must pass. The public probe must retain `lowered_schema="idb"`, the `tenant:7` rewrite, positions `[-1 0]`, and successful missing/null JSON decoding.

Focused Rust must pass the 10 existing tests plus the source-derived regressions. The complete owning crate, integration tests, doctest, formatting, all-target Clippy, semantic receipt, and repository lint must pass. The final commit may contain only the column-mapping owner/test, receipt, and this plan. Publication must be one linear non-force update with matching fresh remote SHA.

## Idempotence and Recovery

All checks are safe to rerun. The Go probe lives under `/tmp` and never enters the repository; move it to Trash after evidence is recorded. If remote advances, rebase the one package commit and repeat Ready validation. If a regression exposes selector behavior rather than normalization, isolate the dependency before changing this package.

## Artifacts and Notes

Initial Go evidence on Go 1.25.10 `darwin/arm64`:

    go test -list: exactly 7 tests
    all 7 source tests: pass normally and under -race
    Unicode probe: lowered_schema="idb", values=[tenant:7], positions=[-1 0], err=<nil>
    config probe: {}, arguments:null, and schema-pattern:null all decode successfully to zero values

Initial Rust evidence:

    column_mapping::tests: 10 passed, 0 failed, 0 ignored

Regression and WIP evidence:

    old Unicode normalization: exact regression failed with left "i\\u{307}db", right "idb"
    old partial config decode: exact regression failed with missing field `source-column`
    fixed exact regressions: both pass
    column_mapping::tests after fix: 11 passed, 0 failed, 0 ignored
    cargo fmt --all --check: initially requested only mechanical formatting of two new calls
    cargo fmt --all: applied only those two formatting changes
    cargo clippy -p tidb-util --lib -- -D warnings: pass

Pre-sync Ready evidence on base `8520ab126cc2de2eb1f558a3bc4d8c305cfa59a2`:

    all 7 Go source tests, normal and -race: pass
    Go probe: exact simple-lowercase rewrite and missing/null config results unchanged
    column_mapping::tests: 11 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 1 unique command
    complete tidb-util suite: 341 passed, 0 failed, 1 ignored; integration tests and doctest pass
    Go and Rust simple-case tables: Unicode 15.0.0
    cargo fmt --all --check: pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    repository lint: pass with revive v1.2.1 and all dashboard linters
    make bazel_prepare: not run; no Go, Bazel, module, target-list, or Rust manifest trigger
    not verified: explicit JSON null parity, invalid partition-size wrap behavior, and Go-only dynamic surfaces retained by the decisions above

Lint tool recovery evidence:

    make -o tools/bin/revive lint: first attempt did not enter lint because the new worktree lacked tools/bin/revive
    make lint: tool installation was blocked by a proxy.golang.org timeout
    reused binary version: revive v1.2.1 from the already validated checksum worktree
    make -o tools/bin/revive lint with the verified binary: pass
    temporary tool symlink and empty directory: removed after the passing run

Post-sync Ready evidence on remote base `db23aa671cb8bae673d58f537fe80018ef40abd9`:

    remote advance: one independent versioninfo/consumer commit; no column-mapping path overlap
    rebase: clean; column-mapping commit still contains exactly its three owner/receipt/plan files
    all 7 Go source tests, normal and -race: pass
    Go probe: exact simple-lowercase rewrite and missing/null config results unchanged
    column_mapping::tests: 11 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 1 unique command
    complete tidb-util suite: 341 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo fmt --all --check: pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    repository lint: pass with revive v1.2.1 and all dashboard linters
    temporary lint tool symlink and empty directory: removed by cleanup trap
    make bazel_prepare: not run; no Go, Bazel, module, target-list, or Rust manifest trigger
    not verified: explicit JSON null parity, invalid partition-size wrap behavior, and Go-only dynamic surfaces retained by the decisions above

Failpoint decision:

    no failpoint, testfailpoint, or Bazel failpoint dependency match in the package

Build metadata decision:

    make bazel_prepare is not required: no Go/Bazel/module/manifest edit, Go import change, or new Go test is planned

## Interfaces and Dependencies

The public Rust constants, `Rule`, `Value`, `ColumnMappingError`, `Mapping`, row/DDL methods, and partition-rule setter remain. The implementation continues using `serde`, the existing `tidb-mysql` dependency, and the translated table-rule selector; no manifest or dependency changes are planned.

Plan revision note: created after complete source and Rust owner reads, exact inventory/history and byte-pin checks, failpoint/build decisions, Go list/normal/race tests, Rust focused baseline, assertion mapping, consumer search, sibling config/case-mapping review, and public Go probes.
