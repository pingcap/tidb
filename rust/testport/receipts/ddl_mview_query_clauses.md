# `pkg/ddl` MV query-clause refusals slice receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
`validateCreateMaterializedViewQuery` clause-refusal block of
`pkg/ddl/materialized_view.go` (HAVING / ORDER BY / LIMIT / DISTINCT),
landed after batch 11's GROUP BY required / WITH-ROLLUP refusals.

## Drift content and Rust alignment

Go refuses, in source order after the GROUP BY requirement:
`CREATE MATERIALIZED VIEW does not support HAVING clause`,
`… does not support ORDER BY clause`,
`… does not support LIMIT clause`,
`… does not support SELECT DISTINCT`.

→ `plan_create_materialized_view` (`tidb-exec/src/cluster_ddl.rs`) checks
`sel.having.is_some()`, `!sel.order_by.is_empty()`, `sel.limit.is_some()`
and `sel.distinct` in that order, each refusing with
`DdlAdmissionError::unsupported` (8200 `Unsupported %s`).

## Known gaps recorded (not fixed in this batch)

The remainder of `validateCreateMaterializedViewQuery` — the GROUP BY item
analysis, WHERE determinism (`CheckNonDeterministic` over
`buildMViewSingleTableExpr`), the per-field aggregation checks (count /
sum / min / max only, DISTINCT-aggregate refusal, count(*) arity) and the
mlog column-coverage computation — needs the expression-analysis owner and
is the next slice of this file.

## Regression tests

`cluster_ddl_source.rs::materialized_view_query_clause_refusals_follow_go`
— four statements (HAVING, ORDER BY, LIMIT, DISTINCT variants of the
GROUP BY-ed single-table MV) each refuse with Go's exact message and code
8200, against a real base table plus an existing `$mlog$` table.

Fail-before evidence: the checks do not exist in the pre-batch tree; the
refusal test binds to the new match arms (pre-batch, the clauses fell
through to the job seam).

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# full-suite failure set identical to the batch-9-era baseline (8
# pre-existing failures). Zero new failures.
```

No Go source changed in this batch.
