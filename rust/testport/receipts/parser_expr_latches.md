# Rust `tidb-parser` predicate/`IS` chaining latch receipt

Status: bounded Rust-only alignment batch. Closes divergence item 3 of
`rust/docs/parser-lexer-divergence.md` (predicate and `IS` chaining).

Comparison source: Go `origin/master` at `a85e0fd5df`, owning file
`pkg/parser/expr_parser.go` byte-identical. Go's `parseInfixExpr` carries two
loop latches the Rust loop lacked:

- `noMorePredicate` (`expr_parser.go:45`): once IN/LIKE/ILIKE/BETWEEN/REGEXP/
  RLIKE/MEMBER OF (or a NOT-prefixed predicate) built a node, its result can
  never be the left operand of another predicate — the yacc production
  `predicate: bit_expr ...` has a `bit_expr` left side. Tested at the head of
  every predicate arm (`:59`/`:86`/`:97`/`:109`/`:120`/`:169`/`:186`), set
  after each build.
- `noMoreIS` (`expr_parser.go:39`): `IS [NOT] TRUE/FALSE/UNKNOWN` sits at
  expression level and never chains; `IS [NOT] NULL` chains at
  boolean_primary level (`parseIsExpr:716` returns `chainable=true` only for
  NULL; `:728`/`:738` return false for TRUE/FALSE/UNKNOWN — UNKNOWN included,
  despite building the Null node).

## Implementation

`tidb-parser/src/expr.rs` gains both latches on the infix loop: the
NOT-prefixed arm, the bare predicate arm, the MEMBER OF arm, and the JSON
extract arm test them (`break` = Go's `return left`, leaving the operator
unconsumed for the statement-level syntax error); the predicate/MEMBER OF
arms set `noMorePredicate`, and the NOT arm sets it after its build.
`parse_is` (`expr/predicate.rs`) now returns `(Expr, chainable)` with
chainable computed exactly as `parseIsExpr` (NULL true; TRUE/FALSE/UNKNOWN
false), and the IS arm sets `noMoreIS = !chainable`.

## Doc correction

The divergence doc's BETWEEN half was inaccurate: Go's `parseBetweenExpr:637`
parses the HIGH side at `precPredicate` with a FRESH `noMorePredicate`, so
`1 BETWEEN 0 AND 2 BETWEEN 0 AND 2` chains through the HIGH side and is
Go-legal. Both engines accept it; the doc is corrected in the same commit.

## Regressions

`tests/parser_root_source.rs`: chained `LIKE`/`IN`/`NOT LIKE` rejections
(error at the unconsumed operator), `IS TRUE IS TRUE` / `IS TRUE IS FALSE`
rejections, `IS NULL IS NOT NULL` chaining pin, and the BETWEEN-high chaining
pin. All four rejection cases were proven to PARSE against the unfixed loop
(captured by stashing the production edit), and the chaining pins pass both
ways.

## Validation

Profile: Ready for this bounded Rust package batch.

- Full `tidb-parser` suite: lib 730 passed + aggregate 97 passed, 0 failed
  (one documented gap test ignored).
- `cargo fmt --all -- --check`, workspace `make lint`, `git diff --check`:
  clean (recorded in `TESTPORT_EXECPLAN.md`).

## Risks

- Compatibility: valid SQL never trips the latches — single predicates,
  `IS NULL` chains, JSON extract after a column, and MEMBER OF all keep
  their paths; only MySQL-rejected chains now error at parse time
  (client-visible, matching MySQL/TiDB).
- Performance: two boolean stores per infix loop; no measurable change.
