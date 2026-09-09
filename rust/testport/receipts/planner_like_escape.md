# `pkg/planner/core` LIKE default-escape parity receipt

Go comparison commit: `078b070563f855a293fc09f93afe4b9911bc67fd`,
“fix bug of `like` expression with `NO_BACKSLASH_ESCAPES` sql_mode”.

## Complete inventory

The complete `pkg/planner/core` tree was inventoried before editing: 559
files in 86 directories and 241,065 total lines (356 Go sources, 59 Bazel
build files, 143 JSON casetest fixtures, and one archive fixture). The direct
package contains 107 artifacts: 106 Go production/test files and its root
`BUILD.bazel` (72,951 and 340 lines respectively). Nested package boundaries
were retained rather than folded into the root claim; the relevant physical
plan casetest subtree has 9 artifacts/11,180 lines, the index subtree has
9/2,217, and the plan-cache subtree has 11/5,457. Every production file,
test, fixture, generated input/output, platform/build variant, and support
artifact in that tree was included in the inventory walk; no additional
LIKE-specific generated or platform owner exists.

The Rust owner walk covered `tidb-planner/src/plan_builder.rs` and its
`plan_builder/tests.rs` seam, `tidb-executor/src/driver/access.rs`, and
`tidb-session/src/prepared_ast.rs` plus `tests_sql_mode_scanner.rs`.

## Restored behavior

Go's `patternLikeOrIlikeToExpression` selects escape byte `0` when
`NO_BACKSLASH_ESCAPES` and `EnableNoBackslashEscapesInLike` are active. Rust
already selected that byte in the live statement context and evaluator, but
the plan builder's `PlanScopeResolver` silently fell back to `\\`; consequently
an index path parsed `a\\b` as `ab` and returned no row. The resolver now carries
the statement's `like_default_escape` into every scalar rewrite, so the
rewritten expression and planner/ranger third argument are identical.

Go also adds `EnableNoBackslashEscapesInLike` to `NewPlanCacheKey`. Rust's
`PreparedPlanCacheEnvironment` now records the same switch, and the session
environment builder publishes it, preventing a prepared plan built with one
implicit escape policy from being reused after the variable changes.

## Regression coverage

- `tidb-planner::plan_builder::tests::test_like_rewrite_uses_the_statement_default_escape`
  parses under `NO_BACKSLASH_ESCAPES` and asserts the lowered LIKE escape
  constant is zero. Before the resolver fix it was the historical backslash.
- `tidb-session::tests_sql_mode_scanner::no_backslash_escapes_like_default_reaches_a_table_filter`
  is the fail-before integration regression: a forced secondary-index query
  returned `0` instead of `1`; it now passes and still verifies the OFF switch
  returns `0`.
- `no_backslash_escapes_like_default_changes_plan_cache_environment` asserts
  that toggling the session variable produces a distinct prepared-plan
  environment, matching Go's cache-key bit.

## Validation

Ready profile passed:

```text
cargo test --offline --locked -p tidb-session 'no_backslash_escapes_like_default' -- --nocapture
cargo test --offline --locked -p tidb-planner test_like_rewrite_uses_the_statement_default_escape -- --nocapture
git diff --check
cargo fmt --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
  TMPDIR=/tmp/tidb-codex make lint
```

The Go reference test suite remains an external comparison source; no Go
files were changed. The Rust changes are limited to the planner/session/cache
seams that own the missing behavior.
