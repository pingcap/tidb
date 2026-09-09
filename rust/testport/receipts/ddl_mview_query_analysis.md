# `pkg/ddl` MV query analysis slice receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
`validateCreateMaterializedViewQuery` analysis body of
`pkg/ddl/materialized_view.go` (master `94a9cbedab`), landed on top of
batch 11's lowering arm.

## Drift content and Rust alignment

- `resolveMViewColumnName` → `resolve_mview_column_name`: a schema
  qualifier must match the base schema, a table qualifier must match the
  base table name or the FROM alias, and the column must exist in the base
  table — each mismatch is `ErrColumnNotExists` (1054, `Unknown column
  '<col>' in '<base table>'`).
- GROUP BY item loop: every item must be a plain column reference
  (8200 `GROUP BY expression is not supported in CREATE MATERIALIZED
  VIEW`); duplicates refuse (8200); every referenced column is `used`.
- WHERE analysis: the clause builds through `build_simple_expr` over a
  resolver backed by the base table's columns (`BaseTableResolver`, whose
  qualifier rules match Go's `buildMViewSingleTableExpr` scope); a build
  failure is 8200 `CREATE MATERIALIZED VIEW WHERE clause is not supported`,
  and Go's `expression.CheckNonDeterministic` (unfoldable functions —
  rand/sleep/uuid/sysdate/... — recursively over the built tree, via
  `tidb_expr::constant_fold::is_unfoldable`) refuses with 8200 `…must be
  deterministic`. Every WHERE column is `used`.
- SELECT field loop: wildcard fields refuse (8200); bare columns must
  appear in GROUP BY (8200) without duplicates (8200); aggregates are
  whitelisted to count/sum/min/max (`unsupported aggregate function …:
  agg <name>` 8200); DISTINCT aggregates refuse (8200); count arity-1
  (`count(*)/count(1) must have exactly one argument …`); non-column or
  non-1 constants route to `only supports count(*)/count(1)` / `only
  supports column argument …`; SUM over DATE/DATETIME/TIMESTAMP/TIME
  refuses; SUM over a nullable column records the nullable-sum pairing.
- `count(*)/count(1)` is required (8200 `CREATE MATERIALIZED VIEW must
  contain count(*)/count(1)`); a nullable-column SUM without a matching
  COUNT refuses (8200, Go's exact pairing message); every GROUP BY column
  must appear in the SELECT list (plain 1105, Go's `errors.Errorf`);
  MIN/MAX requires a visible public index whose leading columns cover the
  GROUP BY columns (batch 4's `find_visible_index_with_prefix_covering_columns`,
  8200); every used column must be covered by the mlog column list (8200
  `materialized view log does not contain column <name>`).

In Go, count(*) parses with a nil argument and count(1) as the constant 1;
the Rust parser normalizes both to `Expr::Int("1")`, so one
`is_count_star_or_one` arm covers both shapes.

## Known gaps recorded (not fixed in this batch)

- The returned analysis (`mviewQueryAnalysis`: per-GROUP-BY select indices,
  NOT-NULL flags, `HasMinOrMax`) is consumed by the job submission, which
  this tier does not wire yet; the checks themselves are complete and their
  refusals are the observable surface.

## Regression tests

`cluster_ddl_source.rs::materialized_view_lowering_follows_go_admission_order`
extends to the full analysis: the mlog column list is injected with real
base-column coverage, and the valid statement
`SELECT id, COUNT(1) … GROUP BY id` reaches the documented job seam —
proving the analysis passes a well-formed query end to end while every
malformed variant (GROUP BY missing/ROLLUP/HAVING/ORDER BY/LIMIT/DISTINCT,
mlog missing, cross-schema base, unknown schema, over-long comment,
disabled flag) carries Go's exact refusal.

Fail-before evidence: the analysis, the resolver, and the coverage checks
do not exist in the pre-batch tree; the valid-statement seam assertion and
the clause checks bind to symbols and behavior absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# full-suite failure set identical to the pre-batch base (8 pre-existing
# failures). Zero new failures.
```

No Go source changed in this batch.
