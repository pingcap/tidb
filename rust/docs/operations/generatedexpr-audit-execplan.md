# Certify `pkg/util/generatedexpr` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB stores generated-column and partial-index expressions as SQL text inside table metadata. Before those expressions can be validated or used by execution code, `pkg/util/generatedexpr` reparses the text into an abstract syntax tree and checks every referenced column against its table. The Rust SQL node already contains a seed implementation, but it has not been accepted as a complete Go package: the exact Go test, all four direct Go artifacts, and the real model/executor consumers are not tied together by one semantic receipt.

After this plan is complete, one package-level commit will demonstrate that the Rust implementation preserves both exported Go functions, passes the unchanged Go test, and remains integrated with partial-index and generated-column consumers. The commit and push unit is the complete direct Go package inventory: `BUILD.bazel`, `gen_expr_test.go`, `generated_expr.go`, and `main_test.go`.

## Progress

- [x] (2026-08-11 11:12Z) Fixed the four-file direct Go inventory at source commit `6dc58175478a10cc0a3d159644cbf115570c7525`; confirmed no `doc.go` and no failpoint use.
- [x] (2026-08-11 11:13Z) Ran unchanged Go `TestParseExpression`; it passed in 0.426 seconds.
- [x] (2026-08-11 11:13Z) Audited the seed production implementation and its model/executor integration; all three existing focused Rust tests passed.
- [x] (2026-08-11 11:15Z) Added the exact Go-test mapping, syntax-error coverage, complete semantic receipt, and this package ExecPlan.
- [x] (2026-08-11 11:15Z) Completed WIP validation; the semantic gate accepted one package and ran all four unique integration commands.
- [x] (2026-08-11 11:17Z) Completed the pre-rebase Ready profile: full owning-crate tests, format, all-target clippy, and repository lint passed.
- [x] (2026-08-11 11:17Z) Confirmed the Bazel gate does not require `make bazel_prepare`; the diff contains no Go, Bazel, Go module, import, or top-level Go test changes.
- [x] (2026-08-11 11:20Z) Committed the package evidence, rebased without conflict onto remote `9e065257f13f9425ebccf4ff8a535a566b64ac1a`, and repeated every Ready gate successfully.
- [ ] Publish the validated one-package commit linearly to `hparser-integration` and verify the remote branch SHA.

## Surprises & Discoveries

- Observation: the Rust implementation arrived as a cross-module seed rather than a complete package claim.
  Evidence: commit `d5252436d98faaacefe1aedee7c5707f4d4c0a96` added `tidb-model/src/generated_expr.rs` and routed three existing consumers through it, but added no `pkg/util/generatedexpr` semantic receipt.

- Observation: Go deliberately returns the first projection when expression text contains a comma, rather than requiring exactly one projection.
  Evidence: `ParseExpression` prefixes `select ` and reads `Fields.Fields[0]`; the Rust seed uses `parse_multi` and likewise clones the first `SelectField::Expr`.

- Observation: `SimpleResolveName` validates names but does not replace column nodes with offsets or IDs.
  Evidence: the Go visitor compares `ColumnName.Name.L` with each `ColumnInfo.Name.L` and returns the original node; the Rust visitor uses the last qualified path segment and returns the unchanged owned AST.

- Observation: the first all-target clippy attempt exhausted the shared Cargo incremental cache rather than finding a code issue.
  Evidence: it failed while writing `query-cache.bin` with `No space left on device`; deleting only `/tmp/tidb-package-audit.DnxFlT/rust/target/debug/incremental` and rerunning the identical command succeeded.

## Decision Log

- Decision: Keep the implementation in `tidb-model::generated_expr`.
  Rationale: the package consumes `TableInfo`, `ColumnInfo`, TiDB AST nodes, and the parser, while model `IndexInfo` already delegates to it. A new crate would duplicate model ownership and introduce dependency edges without improving isolation.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat the three existing model/executor call sites as integration evidence, not as ownership claims for their broader modules.
  Rationale: a complete package claim must state how the package is integrated, but the user excluded optimizer and transaction modules. Focused tests prove only expression parsing/name resolution at those call sites and do not claim the surrounding executor rules as transcreated packages.
  Date/Author: 2026-08-11 / Codex

- Decision: Add tests and package evidence without changing production behavior.
  Rationale: line-by-line review found the seed's parser prefix, first-field selection, Go-compatible case folding, qualifier handling, traversal order, error text, and unchanged-AST result consistent with the source. Rewriting correct code would add risk without closing a semantic gap.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The package implementation and pre-rebase Ready validation are complete. The exact Go test is now visible as a named Rust test, malformed SQL and both name-resolution outcomes are covered, and focused integration commands exercise model partial-index parsing plus both executor consumers.

Correctness risk is low after the unchanged Go oracle, package gate, 283-test owning-crate run, and integration checks. Compatibility risk is contained to the native Rust error type: Go wraps parse failures with `util.SyntaxError`, while Rust returns `tidb_parser::ParseError`; both preserve failure rather than producing an expression. Performance risk is low because the certification adds tests and evidence only and does not alter production parsing, AST cloning, or visitor work.

The same evidence passed after rebasing onto remote `9e065257f13f9425ebccf4ff8a535a566b64ac1a`; that remote increment changed the workspace lockfile but none of the five generated-expression evidence files.

## Context and Orientation

The Go implementation is in `pkg/util/generatedexpr/generated_expr.go`. `ParseExpression` builds a synthetic `SELECT`, uses the repository parser pool with the default connection charset and collation, and returns the first projected expression. `SimpleResolveName` walks the expression and stops at the first column whose case-insensitive name is absent from `model.TableInfo.Columns`. `gen_expr_test.go` contains the package's sole ordinary Go test; `main_test.go` supplies common test setup and leak checking; `BUILD.bazel` is the build artifact.

The Rust implementation is `rust/crates/tidb-model/src/generated_expr.rs`, exported by `rust/crates/tidb-model/src/lib.rs`. `rust/crates/tidb-model/src/index.rs` uses it for partial-index condition parsing. `rust/crates/tidb-executor/src/generated_column_substitute.rs` uses it to canonicalize generated expressions, and `rust/crates/tidb-executor/src/kv_table.rs` uses both parsing and name resolution when extracting partial-index columns. These consumers are integration evidence only; they remain owned by their own Go package claims.

A semantic receipt is a TOML file consumed by `rust/scripts/semantic-package-gate.py`. It pins the exact direct Go package inventory and bytes, lists every Rust evidence file, and runs focused Cargo commands. The receipt for this package is `rust/crates/tidb-model/tests/generatedexpr.semantic.toml`.

## Plan of Work

First preserve the existing production algorithm and extend its unit-test module. Add one test using the exact input and function-name assertion from Go `TestParseExpression`. Retain the stronger first-projection test separately, and add malformed SQL coverage for the public error result. Existing name-resolution tests continue to prove case-insensitive qualified names, unchanged AST output, first-error order, original spelling, and table spelling.

Next add the semantic receipt. Pin all four direct Go artifacts at `6dc58175478a10cc0a3d159644cbf115570c7525`. List the Rust implementation, module export, `IndexInfo` delegation, and the two executor integrations as evidence. Run focused commands for the generated-expression module and one real assertion at each integration boundary.

Finally run the WIP checks, then the repository Ready profile. No Go, Bazel, or Go module file changes are planned, so `make bazel_prepare` should remain unnecessary; confirm from the actual diff. Commit only this package's test/evidence/plan changes, fetch and rebase onto the current remote `hparser-integration`, repeat every Ready gate, and push without force.

## Concrete Steps

Run the authoritative Go test from `pkg/util/generatedexpr`:

    /Users/chenhuansheng/.cache/codex-go1.25.10/go/bin/go test -run '^TestParseExpression$' -tags=intest,deadlock

Expect `PASS`.

Run focused Rust validation from `rust`:

    cargo test -p tidb-model --lib generated_expr --locked
    cargo test -p tidb-model --lib index::tests::foreign_key_partial_index_condition_boundaries --locked -- --exact
    cargo test -p tidb-executor --lib generated_column_substitute::tests::a_declared_columns_ddl_parentheses_leave_the_key --locked -- --exact
    cargo test -p tidb-executor --lib kv_table::tests::test_extract_columns_from_condition --locked -- --exact

Run the package gate from repository root:

    python3 rust/scripts/semantic-package-gate.py rust/crates/tidb-model/tests/generatedexpr.semantic.toml

Expect `semantic package gate: 1 packages, 4 unique commands`.

Before publication, run format, full owning-crate tests, all-target clippy for the changed owning crate, and repository lint:

    cd rust && cargo fmt --all --check
    cd rust && cargo test -p tidb-model --locked
    cd rust && cargo clippy -p tidb-model --all-targets --locked -- -D warnings
    make lint

## Validation and Acceptance

Acceptance requires the unchanged Go test to parse `json_extract(a, '$.a')` and observe function name `json_extract`. Rust must make the same observation. Rust must also preserve the source-only contracts not covered by that Go test: return the first projection, propagate malformed SQL as an error, resolve qualified and Unicode column names through Go-compatible case folding, return the original expression unchanged, and report the first missing column with the source error text.

The semantic gate must report exactly one Go package and four unique commands. The full `tidb-model` crate, its doctests and integration tests, format, all-target clippy, and `make lint` must pass. The final commit diff must contain no Go, Bazel, or Go module changes and must remain one package unit after rebasing.

## Idempotence and Recovery

Every test and lint command is read-only and safe to rerun. The semantic gate fails closed if source bytes or direct package inventory drift from the pin. If the remote branch advances, fetch its exact head, rebase the single local package commit, resolve only genuine overlaps in the five declared evidence files, and repeat all post-rebase gates before a non-force push. Do not modify the primary dirty worktree.

## Artifacts and Notes

Initial Go oracle:

    PASS
    ok github.com/pingcap/tidb/pkg/util/generatedexpr 0.426s

Initial Rust baseline:

    running 3 tests
    test result: ok. 3 passed; 0 failed; 0 ignored; 208 filtered out

Pre-rebase Ready evidence:

    exact generated-expression tests: 5 passed; 0 failed
    semantic package gate: 1 packages, 4 unique commands
    tidb-model library: 213 passed; 0 failed
    tidb-model integration tests: 70 passed; 0 failed
    tidb-model doctests: 0 tests; exit 0
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-model --all-targets --locked -- -D warnings: exit 0
    make lint: exit 0

Post-rebase Ready evidence on remote base `9e065257f13f9425ebccf4ff8a535a566b64ac1a`:

    Go TestParseExpression: PASS (0.437s)
    semantic package gate: 1 packages, 4 unique commands
    tidb-model library: 213 passed; 0 failed
    tidb-model integration tests: 70 passed; 0 failed
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-model --all-targets --locked -- -D warnings: exit 0
    make lint: exit 0

The accepted direct Go inventory is:

    pkg/util/generatedexpr/BUILD.bazel
    pkg/util/generatedexpr/gen_expr_test.go
    pkg/util/generatedexpr/generated_expr.go
    pkg/util/generatedexpr/main_test.go

## Interfaces and Dependencies

`rust/crates/tidb-model/src/generated_expr.rs` owns these public interfaces:

    pub fn parse_expression(expression: &str) -> Result<tidb_ast::Expr, tidb_parser::ParseError>;

    pub fn simple_resolve_name(
        expression: tidb_ast::Expr,
        table: &tidb_model::TableInfo,
    ) -> Result<tidb_ast::Expr, ResolveNameError>;

`tidb-parser` supplies native Rust SQL parsing; `tidb-ast` supplies expressions and the Go-shaped visitor contract; `tidb-model` supplies `TableInfo` and `ColumnInfo`. No new third-party dependency or lockfile change is required.

Plan revision note: created after inventory, source pin, Go-oracle execution, seed history review, production line audit, and baseline Rust validation; updated after WIP, semantic, Bazel, both Ready gates, remote synchronization, and final diff review. Only linear publication and remote-SHA verification remain pending.
