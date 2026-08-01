# `tidb-parser` / `tidb-lexer` vs the hand-written Go parser

## What this compares against

This branch (`hparser-integration`) **replaces** TiDB's goyacc parser with a
hand-written recursive-descent parser in Go. `pkg/parser/parser.y` and
`pkg/parser/yy_parser.go` **do not exist on this branch** — verified by
`ls`. Every Go citation below is therefore to the live hand-written
implementation:

| Go file | What it owns |
| --- | --- |
| `pkg/parser/prec.go` | the precedence table (`tokenPrecedence`, `tokenToOp`) |
| `pkg/parser/expr_parser.go` | the Pratt loop (`parseExpression`, `parseInfixExpr`) and prefix operators |
| `pkg/parser/expr_prefix_parser.go` | prefix atoms, `isReservedClauseKeyword` |
| `pkg/parser/reserved_words.go` | `IsReserved` (token-constant based) |
| `pkg/parser/keywords.go` | the 684-entry public keyword catalog |
| `pkg/parser/misc.go` | `tokenMap` (keyword string -> token constant) |
| `pkg/parser/lexer.go` | the scanner: strings, numbers, comments, sql_mode hooks |
| `pkg/parser/parser_helpers.go` | `isIdentLike` |
| `pkg/parser/select_parser.go` | field alias and CTE-name admission |

Rust side: `rust/crates/tidb-lexer`, `rust/crates/tidb-parser`.

## Machine-diffed equal lists (strong negative evidence)

1. **Full keyword catalog — IDENTICAL, 684/684 entries, word *and*
   reserved flag.** `pkg/parser/keywords.go`'s `Keywords` table extracted
   by regex vs `tidb-lexer/src/keyword_catalog/{reserved,unreserved,tidb_specific}.rs`
   extracted by regex, both sorted: `diff` returned empty. This is the
   catalog that backs `information_schema.keywords`.

2. **Reserved subset of that catalog — IDENTICAL, 233/233.**
   `keyword_catalog/reserved.rs` vs the `true` rows of `keywords.go`:
   `diff` empty.

3. **Clause-introducing keyword set — IDENTICAL, 13/13.**
   `pkg/parser/expr_prefix_parser.go:262` `isReservedClauseKeyword`
   = `{from, where, group, order, limit, having, union, into, forKwd,
   lock, selectKwd, set, on}`; `tidb-parser/src/expr.rs:1033`
   `is_clause_keyword` = the same 13 strings, same order.

4. **Infix operator -> (opcode, precedence) map — IDENTICAL.**
   `pkg/parser/prec.go`'s `tokenPrecedence`+`tokenToOp` vs
   `tidb-parser/src/expr.rs:290-321` `infix_op`, pairwise over
   `| & ^ << >> + - * / % = <=> >= > <= < != <> && || OR XOR AND DIV MOD`:
   every operator lands on the same level, and both sides recurse with
   `prec + 1` (`expr_parser.go:255` region, `expr.rs:290`), so **every
   binary operator is left-associative on both sides**. The one entry that
   differs is `||`, covered in finding #1.

5. **Precedence *level ordering* — IDENTICAL.** Rust
   (`tidb-parser/src/prec.rs`) omits Go's `precConcat = 14` and numbers
   `COLLATE` 14 instead of 15; since only relative order is ever compared
   (`min_prec >` / `p < min_prec`), the two tables induce the same
   ordering on every level both define:
   `NONE < OR < XOR < AND < NOT < COMPARISON < PREDICATE < BIT_OR <
   BIT_AND < SHIFT < ADD_SUB < MUL_DIV < BIT_XOR < UNARY < COLLATE`.
   Notably both put `PREDICATE` (LIKE/IN/BETWEEN/REGEXP) **above**
   `COMPARISON`, and `BIT_XOR` **above** `MUL_DIV`. So `a = b LIKE c`
   parses `a = (b LIKE c)` and `2 * 3 ^ 1` parses `2 * (3 ^ 1)` on both
   sides. No divergence here.

So sections 1 (the precedence table proper) and 3 (the keyword catalog)
are, mechanically, in agreement — the divergences below are all in how
those tables are *consulted*.

## Ranked inventory

### 1. `||` under `PIPES_AS_CONCAT` is string concatenation in Go and boolean OR in Rust — same SQL, different meaning

- Go: `pkg/parser/lexer.go:248` emits token `pipes` only when
  `sqlMode.HasPipesAsConcatMode()` (otherwise it rewrites to `pipesAsOr`);
  `pkg/parser/prec.go:80-85` gives `pipes` `precConcat = 14`, and
  `pkg/parser/expr_parser.go:216-230` builds a `FuncCallExpr{FnName:
  "concat"}` for it.
- Rust: `tidb-parser/src/expr.rs:312` — `"||" => Some((BinaryOp::LogicOr,
  prec::OR))`, unconditionally. `tidb-parser/src/prec.rs:45-51` documents
  the omission of the `precConcat` level as deliberate.

SQL: with `sql_mode = 'PIPES_AS_CONCAT'`,

```sql
SELECT 'a' || 'b';
```

- Go parse: `CONCAT('a', 'b')` -> `'ab'`.
- Rust parse: `'a' OR 'b'` -> `0`.

And the precedence flips with it, so the damage is not limited to a bare
`||`. `SELECT 1 = 'a' || 'b'` is `1 = CONCAT('a','b')` in Go
(`precConcat` 14 > `precComparison` 5) but `(1 = 'a') OR 'b'` in Rust
(`prec::OR` 1 < `COMPARISON` 5) — a different comparison and a different
result shape.

This is the only found case where a *default-mode* query is safe but a
supported `sql_mode` silently re-reads the statement.

### 2. Rust chains predicates and `IS TRUE`; Go refuses both

Go's infix loop carries two latches that Rust has no equivalent for:

- `noMorePredicate` (`pkg/parser/expr_parser.go:45`, set at :80, :92,
  :103, :114, :125, :204, tested at :59, :86, :97, :109, :120, :169,
  :182): once a predicate (`IN`/`LIKE`/`ILIKE`/`BETWEEN`/`REGEXP`/`RLIKE`/
  `MEMBER OF`) has been built, the *result* of that predicate can never be
  the left operand of another. This reproduces the yacc shape
  `predicate: bit_expr LIKE ...` where the left side is a `bit_expr`, not
  a `predicate`.
- `noMoreIS` (`pkg/parser/expr_parser.go:39`, set at :149, tested at
  :136): `IS [NOT] NULL` chains (it is at `boolPri` level), but
  `IS [NOT] TRUE/FALSE/UNKNOWN` does not (it is at `Expression` level).

Rust's loop (`tidb-parser/src/expr.rs:52-215`) has neither latch — every
predicate arm ends in a bare `continue`.

SQL:

```sql
SELECT 'a' LIKE 'b' LIKE 'c';
```

- Go: `parseLikeExpr` sets `noMorePredicate`; the second `LIKE` hits
  `if noMorePredicate { return left }`, the statement parser then finds
  `LIKE` unconsumed -> syntax error (this matches MySQL, which also
  rejects it).
- Rust: parses to `PatternLike(PatternLike('a','b'), 'c')` — accepted, and
  evaluated.

Same shape for `SELECT 1 IN (1) IN (0)`, `SELECT 1 BETWEEN 0 AND 2
BETWEEN 0 AND 2`, and:

```sql
SELECT 1 IS TRUE IS TRUE;
```

- Go: rejected (`noMoreIS`).
- Rust: `IsTruth(IsTruth(1))`.

### 3. Rust's expression-prefix identifier gate is `is_reserved` (232 keywords) where Go's is `isReservedClauseKeyword` (13)

- Go: `pkg/parser/expr_prefix_parser.go:222-235` — the final fallback of
  `parsePrefixKeywordExpr` admits **any** token with
  `tok.Tp >= identifier && !isReservedClauseKeyword(tok.Tp)` as either a
  function call (if `(` follows) or a bare column reference.
- Rust: `tidb-parser/src/expr.rs:894` — `_ if !is_reserved(&t.text) =>
  self.parse_ident_or_func()`, with a *separate*, narrower arm at
  `expr.rs:918-922` that only rescues a reserved keyword when `(`
  immediately follows.

So a reserved keyword used as a **bare** column reference parses in Go and
is a `ParseError` in Rust:

```sql
SELECT rows FROM t;
```

- Go: `rows` is `>= identifier` and not clause-introducing -> column
  reference `t.rows`.
- Rust: `is_reserved("ROWS")` is true, next token is `FROM` not `(` ->
  `Err("unsupported keyword in expression")`.

Roughly 220 keywords sit in this gap (`is_reserved`'s 232 minus the 13
clause keywords, minus those handled by an earlier arm). Go is more
permissive than MySQL here; Rust is closer to MySQL. Either way the two
disagree on what a client can run.

This gate is also **why the reserved-keyword list divergence in #4 cannot
be fixed in isolation** — see below.

### 4. `RESERVED_KEYWORDS` is missing `DATABASE`, `DATABASES`, `DISTINCT`

Mechanical diff, Go `keywords.go` reserved rows (233) vs
`tidb-lexer/src/reserved.rs`'s `RESERVED_KEYWORDS` (232):

```
Go-only:   DATABASE, DATABASES, DISTINCT
Rust-only: SCHEMA, SCHEMAS
```

The Rust-only two are **not** errors: `pkg/parser/misc.go:722-723` maps
`"SCHEMA"`/`"SCHEMAS"` to the `database`/`databases` token constants,
which `IsReserved` returns true for. So the Go *parser* does reject
`SCHEMA` as an identifier even though `keywords.go` advertises it as
unreserved — Rust's list correctly models the parser.

The three Go-only entries are a derivation bug, and `reserved.rs`'s own
module doc names its cause without following it through: `tokenMap` is
many-to-one, so inverting it keeps only one spelling per token constant.
Three pairs collide — `DISTINCT`/`DISTINCTROW` -> `distinct`,
`DATABASE`/`SCHEMA` -> `database`, `DATABASES`/`SCHEMAS` -> `databases` —
and in each pair the *other* spelling won. The doc caught the
`DISTINCTROW` case and hand-added it; it did not generalize.

Consequence, at the two Go call sites that use `IsReserved`:

```sql
SELECT 1 AS database;
```

- Go: `pkg/parser/select_parser.go:455` — `IsReserved(database)` is true
  -> syntax error.
- Rust: accepted, field alias `database`.

Same for a CTE name (`pkg/parser/select_parser.go:185`): `WITH database AS
(SELECT 1) SELECT * FROM database` is a Go syntax error and a Rust
success.

**Not fixed here, deliberately.** Adding the three strings to
`RESERVED_KEYWORDS` is a one-line change that is *correct for the alias
path and wrong for the expression path*, because of finding #3: Rust
reuses the one list for both gates. `tidb-parser/src/tests/format.rs:84`
asserts `database.table.column` parses to
`` `database`.`table`.`column` `` — which Go also accepts (the
expression-prefix fallback, not `IsReserved`) — and that test would start
failing. The coherent fix is #3 and #4 together: move the
expression-prefix bare-identifier arm onto `is_clause_keyword`, *then*
add the three keywords. That is a wide-blast-radius change and is written
up rather than attempted, especially since this machine cannot run
`cargo test` (see "Unverified").

### 5. `HIGH_NOT_PRECEDENCE` disables `NOT IN` / `NOT LIKE` / `NOT BETWEEN` in Go but not in Rust

Go rewrites the token in the **lexer**: `pkg/parser/lexer.go:252-253`,
`if tok == not && s.sqlMode.HasHighNotPrecedenceMode() { return not2 }`.
`not2` is handled as a *prefix* operator only
(`pkg/parser/expr_parser.go:389-395`); the infix loop's `case not:`
(`expr_parser.go:57`) never fires, because no `not` token is ever
produced in that mode.

Rust puts the switch in the **parser** (`tidb-parser/src/expr.rs:576-582`,
`if self.high_not_precedence { prec::UNARY } else { prec::NOT }`) and
leaves the token spelling alone, so `expr.rs:57-70`'s `NOT IN`/`NOT
LIKE`/`NOT BETWEEN`/`NOT REGEXP` lookahead still matches.

SQL, with `sql_mode = 'HIGH_NOT_PRECEDENCE'`:

```sql
SELECT 1 NOT IN (1, 2);
```

- Go: `NOT` lexes as `not2`; the infix loop sees no operator it handles,
  returns `1`, and the leftover `not2 IN ...` is a syntax error.
- Rust: `NOT IN` predicate, result `0`.

The *prefix* behaviour agrees: both bind `NOT` at the unary level in that
mode and both emit the `!`-restoring node (Go `opcode.Not2`, Rust
`UnaryOp::Not` vs `UnaryOp::NotKeyword`). Real MySQL accepts `NOT IN`
under `HIGH_NOT_PRECEDENCE`, so this one is a Go-side defect that Rust
happens not to share — recorded because it is a behaviour difference a
client can hit, not because Rust needs changing.

## Unverified

- **Nothing in this document was executed.** `syspolicyd` on this machine
  wedges every freshly built binary at `_dyld_start`, so `cargo test`,
  `gorun` and `goeval` could not be run. Every parse claim above is read
  from source on both sides; the SQL strings are worked examples of the
  cited code paths, not captured output.
- `cargo check`/`cargo clippy` were the only gates run, and only on
  crates this document touches.
