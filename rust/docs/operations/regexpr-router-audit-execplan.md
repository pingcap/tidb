# Transcreate `pkg/util/regexpr-router` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's Rust SQL node needs the regexp-aware routing behavior currently owned by Go `pkg/util/regexpr-router`. After this work, Rust callers can construct schema- and table-level route rules, route names with the same wildcard and regular-expression semantics as Go, retrieve the original rules, and derive extended-column values from extractor capture groups. The result is observable through Rust tests corresponding to every Go test in `regexpr_router_test.go`, while the original Go tests remain the behavioral oracle.

The acceptance and publication unit is the entire direct Go package inventory, not an individual Rust file or feature. One commit will contain this package's production module, all source-test counterparts, its semantic receipt, this plan, and the crate export. That commit is pushed linearly to `hparser-integration`.

## Progress

- [x] (2026-08-11 09:55Z) Confirmed that the direct package inventory is `BUILD.bazel`, `regexpr_router.go`, and `regexpr_router_test.go`, with no `doc.go` or failpoint use.
- [x] (2026-08-11 09:57Z) Pinned all three files byte-for-byte to commit `61c09e601e17d8c284f3d7f8a3acb64506da5cc4` and ran all eight authoritative Go tests successfully.
- [x] (2026-08-11 10:16Z) Implemented the complete production surface in `rust/crates/tidb-util/src/regexpr_router/mod.rs`.
- [x] (2026-08-11 10:18Z) Ported all eight Go test surfaces into `rust/crates/tidb-util/src/regexpr_router/tests.rs` and added the semantic receipt.
- [x] (2026-08-11 10:32Z) Completed WIP and Ready validation: Go oracle, 8 Rust package tests, semantic receipt, full owning crate, format, all-target owning-crate clippy, and `make lint` pass.
- [x] (2026-08-11 10:45Z) Rebased the single package commit onto remote `d445f1e3b`, reran every Ready gate, and finalized the exact five-file publication unit. The remote ref is verified externally after the linear push.

## Surprises & Discoveries

- Observation: `RouteTable.FetchExtendColumn` does not use the old table router's selector. It calls the regexp filter again, so case-insensitive routers lowercase the probe before selecting extractors.
  Evidence: `pkg/util/regexpr-router/regexpr_router.go` calls `f.filter.Match(curTable)` in both `Route` and `FetchExtendColumn`; Go `pkg/util/filter.Filter.Match` normalizes case-insensitive probes.

- Observation: `AddRule` mutates caller-owned source patterns before the regexp filter is built. A filter-construction error can therefore leave a lowercase rule even though it was not appended.
  Evidence: `rule.Valid()` and `rule.ToLower()` precede `filter.New(...)` in `AddRule`.

- Observation: the shell did not initially expose `go`, but the workspace cache contains a pinned Go 1.25.10 toolchain.
  Evidence: `/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin/go` ran the eight Go tests in 0.667 seconds.

- Observation: the first `make lint` attempt could not download `revive@v1.2.1` because `proxy.golang.org` timed out; this was an environment prerequisite failure before linting began.
  Evidence: rerunning the original target with `GOPROXY=file:///Users/chenhuansheng/.cache/codex-gopath-1.25.10/pkg/mod/cache/download` installed the same pinned tool from the local module proxy and completed every lint recipe successfully.

- Observation: the optional workspace source-size ratchet still fails only on three pre-existing files; neither new regexpr-router file approaches the 2,200-line threshold.
  Evidence: the baseline reports `job_args.rs` at 2,249 lines, `kv_table.rs` at 2,785, and `pipeline_mysql_client_source.rs` at 2,202; the new module and test file are 248 and 210 lines.

## Decision Log

- Decision: Reuse `crate::filter::Filter` and `crate::table_filter::MySQLReplicationRules` for matching, rather than translating regexp/glob matching a second time.
  Rationale: Go `regexpr-router` delegates those semantics to `pkg/util/filter`; the Rust filter package is already a complete transcreation. This preserves the dependency boundary and reduces duplicate matching authority.
  Date/Author: 2026-08-11 / Codex

- Decision: Reuse `crate::table_router::TableRule` and extractor types, but implement `RouteTable` independently of `crate::table_router::Table`.
  Rationale: rule validation and configuration fields belong to table-router, while regexpr-router deliberately selects through `filter.Filter` and has different extractor probe normalization. Delegating routing to the old router would change regexp and case-insensitive behavior.
  Date/Author: 2026-08-11 / Codex

- Decision: Accept rules as mutable references in `new` and `add_rule`.
  Rationale: Go mutates caller-owned patterns in case-insensitive mode. A by-value-only Rust API would hide that observable contract.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The complete three-file Go inventory now has one Rust production module, eight source-aligned Rust tests, and one source-pinned semantic receipt. The implementation composes the already accepted filter and table-router packages, so no duplicate regexp/glob engine or new dependency was introduced. Both before and after rebasing, the Go oracle, package receipt, full owning crate, formatting, clippy, and repository lint gates passed.

Correctness risk is bounded by the unchanged Go package tests and the existing dependency suites. Compatibility risk is limited to the established Rust ownership choice: `TableRule` values are cloned into the router rather than retaining mutable Go-style pointers, consistent with the accepted Rust table-router API. Performance risk is low for the source contract: routing creates only short match vectors, while extractor regexps are deliberately recompiled per call because the Go package does the same. The optional source-size sweep remains red only on the documented unrelated baseline files.

The publication commit contains exactly five paths: the module export, production module, eight-test module, semantic receipt, and this package plan. It is based on remote `d445f1e3b`; the push and `ls-remote` SHA check are external Git receipts and do not require a second documentation-only commit.

## Context and Orientation

Go `pkg/util/regexpr-router/regexpr_router.go` owns `RouteTable`. Each added `table-router.TableRule` is classified as a schema rule when `TablePattern` is empty and a table rule otherwise. It becomes one `filter.Filter`: schema rules populate only `DoDBs`; table rules populate both `DoDBs` and `DoTables`. Routing gathers every match, gives table rules priority for a non-empty table, rejects more than one selected rule, and preserves the original name when a target is empty.

`rust/crates/tidb-util/src/filter/mod.rs` already implements the Go replication filter. `rust/crates/tidb-util/src/table_router/mod.rs` already owns `TableRule`, `TableExtractor`, `SchemaExtractor`, and `SourceExtractor`, including validation and Go-compatible simple lowercasing. The new `rust/crates/tidb-util/src/regexpr_router/` module composes those accepted packages without creating another pattern engine.

A semantic receipt is a `*.semantic.toml` file consumed by `rust/scripts/semantic-package-gate.py`. It pins the complete direct Go package inventory to one accepted commit, lists Rust evidence files, and names the Cargo commands that prove the claim.

## Plan of Work

Add `rust/crates/tidb-util/src/regexpr_router/mod.rs`. Define the public filter-type constants, a package-local error, a private filter wrapper, and public `RouteTable` methods corresponding to Go `NewRegExprRouter`, `AddRule`, `Route`, `AllRules`, and `FetchExtendColumn`. Compile extractor expressions from their public pattern strings at extraction time, concatenate capture groups other than group zero, and treat absent optional groups as empty strings.

Add `rust/crates/tidb-util/src/regexpr_router/tests.rs`. Preserve one explicit Rust test surface for each of `TestCreateRouter`, `TestAddRule`, `TestSchemaRoute`, `TestTableRoute`, `TestRegExprRoute`, `TestFetchExtendColumn`, `TestAllRule`, and `TestDupMatch`. Schema and table wildcard cases also compare against `table_router::Table`, matching the Go tests' old-router differential.

Export the module from `rust/crates/tidb-util/src/lib.rs` and add `rust/crates/tidb-util/tests/regexpr_router.semantic.toml`, pinning source commit `61c09e601e17d8c284f3d7f8a3acb64506da5cc4`.

## Concrete Steps

Run commands from repository root unless a subshell changes directory.

First prove the Go oracle:

    pushd pkg/util/regexpr-router
    go test -run 'Test(CreateRouter|AddRule|SchemaRoute|TableRoute|RegExprRoute|FetchExtendColumn|AllRule|DupMatch)$' -tags=intest,deadlock
    popd

Expect `PASS` and the package name `github.com/pingcap/tidb/pkg/util/regexpr-router`.

During implementation, run the narrow Rust surface:

    cd rust
    cargo test -p tidb-util --lib regexpr_router

Before publication, run the package receipt, full owning crate, formatting, linting, and repository Ready gate:

    python3 rust/scripts/semantic-package-gate.py rust/crates/tidb-util/tests/regexpr_router.semantic.toml
    cd rust && cargo test -p tidb-util
    cd rust && cargo clippy -p tidb-util --all-targets -- -D warnings
    cd rust && cargo fmt --all --check
    make lint

The final source gate must report one package and deduplicated successful Cargo commands. All tests and linters must exit zero.

## Validation and Acceptance

Acceptance requires all eight current Go tests to pass unchanged and their Rust counterparts to pass. The semantic package gate must confirm that every direct Go artifact still matches the pinned source commit and that every listed Rust evidence file is tracked. The full `tidb-util` suite protects already accepted utility packages. `cargo clippy`, `cargo fmt`, and `make lint` are the mandatory code-quality and Ready checks.

Before committing, inspect `git diff --check`, the complete diff, and the exact changed-file list. The list may contain only the new regexpr-router module/tests/receipt, the `tidb-util` export, and this package plan. Fetch the remote branch, rebase without force, rerun the complete validation set, then push `HEAD:refs/heads/hparser-integration`. Acceptance is complete only when `git ls-remote` reports the pushed commit for that branch.

## Idempotence and Recovery

All test and lint commands are read-only and safe to rerun. The semantic gate writes nothing. If the remote branch advances, fetch and rebase the unpublished local commit; resolve only conflicts inside this package's owned files, then rerun every Ready command. Never force-push. The existing dirty primary worktree is unrelated and must remain untouched.

## Artifacts and Notes

Initial Go oracle transcript:

    PASS
    ok github.com/pingcap/tidb/pkg/util/regexpr-router 0.667s

Initial source audit found zero output from the failpoint probes and zero diff between the current package and commit `61c09e601e17d8c284f3d7f8a3acb64506da5cc4`.

Initial Rust WIP transcript:

    running 8 tests
    test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 316 filtered out

Targeted `cargo clippy -p tidb-util --lib -- -D warnings` also exited zero.

Ready evidence before remote synchronization:

    semantic package gate: 1 packages, 1 unique commands
    test result: ok. 323 passed; 0 failed; 1 ignored
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-util --all-targets -- -D warnings: exit 0
    make lint: exit 0

The full crate command also passed 22 integration-contract tests and one doctest. Bazel preflight showed no Go, Bazel, or module-file changes, so `make bazel_prepare` is not required.

The same evidence passed again after rebasing onto remote `d445f1e3b`:

    Go oracle: PASS, 8 authoritative tests
    semantic package gate: 1 packages, 1 unique commands
    tidb-util library: 323 passed; 0 failed; 1 ignored
    integration contracts and doctest: all passed
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-util --all-targets -- -D warnings: exit 0
    make lint: exit 0

## Interfaces and Dependencies

In `rust/crates/tidb-util/src/regexpr_router/mod.rs`, provide:

    pub type FilterType = i32;
    pub const TBL_FILTER: FilterType = 1;
    pub const SCHM_FILTER: FilterType = 2;

    pub struct RouteTable { ... }

    impl RouteTable {
        pub fn new(case_sensitive: bool, rules: &mut [TableRule]) -> Result<Self, RegExprRouterError>;
        pub fn add_rule(&mut self, rule: &mut TableRule) -> Result<(), RegExprRouterError>;
        pub fn route(&self, schema: &str, table: &str) -> Result<(String, String), RegExprRouterError>;
        pub fn all_rules(&self) -> (Vec<TableRule>, Vec<TableRule>);
        pub fn fetch_extend_column(&self, schema: &str, table: &str, source: &str) -> (Vec<String>, Vec<String>);
    }

The implementation depends on `crate::filter::Filter`, `crate::table_filter::{MySQLReplicationRules, Table}`, `crate::table_router::TableRule`, and the workspace `regex` dependency. No new crate or Go module dependency is required.

Plan revision note: created on 2026-08-11 after source, failpoint, and Go-oracle preflight; updated after production implementation, the eight-test port, both Ready gates, and final rebase onto remote `d445f1e3b`. The Git remote ref is the external publication receipt.
