# `pkg/planner/core` — coalesced `USING`/`NATURAL JOIN` qualified-name receipt

## Scope and complete owner inventory

This batch follows Go-master (`a0cdff369bd4c7060a840e3943049a79470e8af4`,
2026-09-06) for the name-resolution and column-pruning contract of coalesced
join columns. Before editing, the complete tracked Go package tree was
inventoried with:

```text
git ls-tree -r --name-only master -- pkg/planner/core
git grep -n -E '^func ' master -- 'pkg/planner/core/**/*.go'
```

| Go package tree | Tracked files | Go production | Go tests | non-Go/build/fixture inputs |
| --- | ---: | ---: | ---: | ---: |
| `pkg/planner/core` (including operator subtrees, tests, and testdata) | 544 | 192 | 152 | 59 Bazel/build files, 141 fixture/testdata/golden matches, 3 generated/bootstrap-marked files |

The review covered every listed production and test file, all generated and
platform variants, build inputs, fixtures, and metadata; 2,625 Go function
declarations were inventoried. No Go file was edited. The direct Go owners
are:

* `pkg/planner/core/logical_plan_builder.go:736-1060` (`buildJoin`),
  `:1104-1360` (`buildUsingClause`, `buildNaturalJoin`, and
  `coalesceCommonColumns`), and `:1657-1685`
  (`findColFromNaturalUsingJoin`) — construct the visible schema while
  retaining redundant qualified columns in `FullSchema`/`FullNames`;
* `pkg/planner/core/expression_rewriter.go:2717-2880` — resolves qualified
  references against the join's full name set and remaps redundant columns
  where the Go plan shape permits it; and
* `pkg/planner/core/operator/logicalop/logical_join.go:83-120`, `:794-850`,
  and `:1200+` — defines `FullSchema`/`FullNames`, redundant-column mapping,
  and used-column extraction. Its `planCanResolveUsedCol` recursion treats
  selections, limits, sorts, and max-one-row wrappers as transparent for
  pruning, but treats projections as derived-table boundaries.

## Failure and implementation

The Rust planner resolved every expression against the executable visible
schema. After `JOIN ... USING (a)` or `NATURAL JOIN`, that schema contains one
canonical `a`; a qualified `n2.a` therefore failed with
`UnknownColumn("n2.a")`. Column pruning also passed only visible child schemas
to `LogicalJoin::extract_used_cols`, so the hidden side could be removed before
the qualified reference was evaluated. Qualified wildcard expansion similarly
omitted the redundant side when the current FROM node was the join itself.

The Rust parity change is deliberately Rust-only:

* `rust/crates/tidb-planner/src/plan_builder.rs:699-982,1332-1380` adds an
  optional full schema/name scope to `PlanScopeResolver`; visible names remain
  authoritative for unqualified lookup, with `FullNames` as a qualified-only
  fallback. Plan-aware scalar, projection, sort, selection, and GROUP BY
  rewriting now carry that scope through transparent wrappers.
* `rust/crates/tidb-planner/src/plan_builder/from.rs:1037-1060,1173-1190`
  rewrites plain-join and lateral-apply `ON` expressions with the plan-aware
  resolver.
* `rust/crates/tidb-planner/src/plan_builder.rs:2515-2565` mirrors Go's
  wildcard rule: qualified `join_alias.*` reads `FullSchema` only for a
  direct join/apply node; an inner join wrapped by an `ON` selection remains a
  visible-schema boundary.
* `rust/crates/tidb-planner/src/logical/rewrite.rs:81-110,1076-1095` uses a
  full-capable child schema for join pruning through Go-transparent wrappers,
  while retaining visible schemas at projection/derived-table boundaries.

## Regression coverage

`rust/crates/tidb-session/src/tests_coalesced_joins.rs` adds
`qualified_using_column_survives_projection_pruning`, which asserts that
`SELECT n2.a FROM n1 JOIN n2 USING (a)` returns `1` after pruning. The focused
matrix also covers either qualifier, nested natural joins, row-preserving
outer joins, qualified wildcard rules, join-column order, and the plain-join
boundary.

Focused commands:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_coalesced_joins::qualified_using_column_survives_projection_pruning \
  -- --nocapture --test-threads=1
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_coalesced_joins -- --nocapture --test-threads=1
```

The new regression and seven adjacent parity tests pass individually. The
complete module is **14 passed, 5 failed**; the five failures are existing
GROUP BY alias/error-code and parallel HashAgg-worker baseline failures, not
coalesced qualified-name regressions. Before this batch, the same module
failed on qualified `n2.a`, qualified wildcard expansion, and pruning of a
coalesced child; those cases now pass.

## Ready validation profile

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
```

Formatting, whitespace, and the executor all-target check pass; the existing
workspace warnings remain non-fatal. `make lint` is run again immediately
before the batch commit and passes (including the Go dashboard linter).
