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
| `pkg/parser/expr_parser.go` | the Pratt loop (`parseExpression`, `parseInfixExpr`), prefix operators, `LIKE`/`REGEXP`/`BETWEEN` bodies |
| `pkg/parser/expr_prefix_parser.go` | prefix atoms, `isReservedClauseKeyword` |
| `pkg/parser/expr_subquery_parser.go` | `parseInExpr` |
| `pkg/parser/reserved_words.go` | `IsReserved` (token-constant based) |
| `pkg/parser/keywords.go` | the 684-entry public keyword catalog |
| `pkg/parser/misc.go` | `tokenMap` (keyword string -> token constant) |
| `pkg/parser/lexer.go` | the scanner: strings, numbers, comments, sql_mode hooks |
| `pkg/parser/parser_helpers.go` | `isIdentLike` |
| `pkg/parser/select_parser.go` | field alias and CTE-name admission |
| `pkg/parser/select_clauses_parser.go` | `parseSetOprRest` (UNION/INTERSECT/EXCEPT) |
| `pkg/parser/util/escape.go` | `UnescapeChar` |
| `pkg/parser/tidb/feature.go` | `CanParseFeature` |

Rust side: `rust/crates/tidb-lexer`, `rust/crates/tidb-parser`.

## Counts

- 11 divergences found; 3 fixed in this branch, 8 written up.
- 8 areas machine-diffed and found equal (below).

## Machine-diffed equal lists (strong negative evidence)

1. **Full keyword catalog — IDENTICAL, 684/684 entries, word *and*
   reserved flag.** `pkg/parser/keywords.go`'s `Keywords` table extracted
   by regex vs `tidb-lexer/src/keyword_catalog/{reserved,unreserved,tidb_specific}.rs`
   extracted by regex, both sorted: `diff` returned empty. This is the
   catalog that backs `information_schema.keywords`.

2. **Reserved subset of that catalog — IDENTICAL, 233/233.**
   `keyword_catalog/reserved.rs` vs the `true` rows of `keywords.go`:
   `diff` empty. (The *parser-facing* list is a different, wrong one —
   finding #5.)

3. **Clause-introducing keyword set — IDENTICAL, 13/13.**
   `pkg/parser/expr_prefix_parser.go:262` `isReservedClauseKeyword`
   = `{from, where, group, order, limit, having, union, into, forKwd,
   lock, selectKwd, set, on}`; `tidb-parser/src/expr.rs:1033`
   `is_clause_keyword` = the same 13 strings.

4. **Infix operator -> (opcode, precedence) map — IDENTICAL.**
   `pkg/parser/prec.go`'s `tokenPrecedence`+`tokenToOp` vs
   `tidb-parser/src/expr.rs:290-321` `infix_op`, pairwise over
   `| & ^ << >> + - * / % = <=> >= > <= < != <> && || OR XOR AND DIV MOD`:
   every operator lands on the same level, and both sides recurse with
   `prec + 1` (`expr_parser.go`'s generic tail, `expr.rs:232`), so **every
   binary operator is left-associative on both sides**. The one entry that
   differs is `||`, covered in finding #2.

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
   sides.

6. **Sub-expression precedence levels inside predicates — IDENTICAL
   except for the LIKE/REGEXP pattern (finding #1).**
   `BETWEEN` low = `precPredicate + 1`, high = `precPredicate`
   (`expr_parser.go:630,637` vs `expr/predicate.rs:99,101`); `IN` list
   items = `precNone` (`expr_subquery_parser.go:290` vs
   `predicate.rs:56,59`); `MEMBER OF` array = `precUnary`
   (`expr_parser.go:192` vs `expr.rs:198`); `->`/`->>` at `precCollate`
   with the same "left operand must be a bare column reference"
   restriction (`expr_parser.go:169-175` vs `expr.rs:150-155`).

7. **Set-operation AST shape — EQUIVALENT, despite very different code.**
   Go runs a real precedence climb (`select_clauses_parser.go:418-465`,
   `INTERSECT` = 2, `UNION`/`EXCEPT` = 1, recursing with `prec+1`) but then
   **flattens every nested `SetOprStmt` back into one linear
   `SelectList.Selects`** (`:477-486`, `:562-566`), marking each element
   with `AfterSetOperator`. Rust builds that linear list directly
   (`select.rs:854-856`, a flat `while self.peek_set_op()` loop over
   `SetOprTerm { op, in_braces, body }`). Traced by hand for
   `SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 UNION SELECT 4`, both sides
   yield `[1, 2(Union), 3(Intersect), 4(Union)]`. **Caveat, not a parser
   finding:** the parser therefore does *not* encode `INTERSECT`'s higher
   precedence on either side — whoever consumes the flat list has to
   re-group adjacent `Intersect` runs, and that consumer was not audited
   here.

8. **String-escape byte table — IDENTICAL.** `pkg/parser/util/escape.go`'s
   `UnescapeChar` vs `tidb-lexer/src/escape.rs`'s `unescape_char`, all
   nine arms (`n 0 b Z r t`, the `% _` backslash-preserving pair, and the
   backslash-dropping default). `decode_quoted_string`
   (`tidb-lexer/src/lib.rs:1062`) reproduces `scanString`'s
   doubled-delimiter collapse and its `NO_BACKSLASH_ESCAPES` gate; hand-
   traced against `'a\''`, `'\\'`, and `'a\'` under both modes.

So the precedence table proper and the keyword catalog are, mechanically,
in agreement — most divergences below are in how those tables are
*consulted*, and one is a level that neither table covers.

---

## Ranked inventory

### Rank 1 — the same SQL parses to a different meaning

#### 1. A `LIKE`/`REGEXP` pattern absorbed binary operators it must not — FIXED

- Go: `pkg/parser/expr_parser.go:590` (`parseLikeExpr`) and `:656`
  (`parseRegexpExpr`) both parse the pattern with
  `p.parseExpression(precUnary)`, under the comments *"yacc: BitExpr
  LikeOrNotOp SimpleExpr — pattern is SimpleExpr"* and *"precUnary
  excludes all binary arithmetic/bitwise operators"*.
- Rust, before this change:
  `tidb-parser/src/expr/predicate.rs:71` and `:85` used
  `self.parse_expr(prec::BIT_OR)` — level 7, not 13 — with a comment
  claiming the opposite ("the pattern is a bit_expr", "confirmed via
  `godump restore`").

Every binary operator from `|` (7) up through `^` (12) therefore bound
into the pattern operand in Rust and onto the whole predicate in Go:

```sql
SELECT 'a' LIKE 'a' + 0;
```

- Go: `+` is level 10 < `precUnary` 13, so the pattern is just `'a'`; the
  Pratt loop then applies `+` to the completed predicate:
  `('a' LIKE 'a') + 0` -> `1`.
- Rust (before): `+` is 10 >= `BIT_OR` 7, so the pattern is `'a' + 0`
  (= `0`): `'a' LIKE 0` -> `0`.

Same for `SELECT 'a' REGEXP 'a' | 0` (Go `1`, Rust `0`) and every
`* / % DIV MOD - << >> & | ^` in that position.

**Fixed** — `predicate.rs` now uses `prec::UNARY` for both patterns, with
the Go citation in place of the mistaken one. `cargo check`/`clippy`/`fmt`
clean. No existing Rust test binds an operator into a pattern, so nothing
in-tree pinned the old behaviour.

#### 2. `||` under `PIPES_AS_CONCAT` is string concatenation in Go and boolean OR in Rust

- Go: `pkg/parser/lexer.go:248` emits token `pipes` only when
  `sqlMode.HasPipesAsConcatMode()` (otherwise it rewrites to `pipesAsOr`);
  `pkg/parser/prec.go:80-85` gives `pipes` `precConcat = 14`, and
  `pkg/parser/expr_parser.go:216-230` builds a `FuncCallExpr{FnName:
  "concat"}` for it.
- Rust: `tidb-parser/src/expr.rs` used to hard-code `"||" =>
  Some((BinaryOp::LogicOr, prec::OR))` unconditionally, with
  `tidb-parser/src/prec.rs` documenting the omission of the `precConcat`
  level as deliberate.

```sql
-- sql_mode = 'PIPES_AS_CONCAT'
SELECT 'a' || 'b';
```

- Go: `CONCAT('a', 'b')` -> `'ab'`.
- Rust: `'a' OR 'b'` -> `0`.

The precedence flips with it, so the damage is not limited to a bare
`||`: `SELECT 1 = 'a' || 'b'` is `1 = CONCAT('a','b')` in Go (14 > 5) but
`(1 = 'a') OR 'b'` in Rust (1 < 5).

**Fixed** — `SqlMode::pipes_as_concat` now reaches the parser, `prec::CONCAT
= 14` is modelled (pushing `prec::COLLATE` to 15, as in Go's own table), and
the Pratt loop builds `CONCAT(left, right)` for `||` under the flag. Pinned
by `tidb-parser`'s `pipes_as_concat_sql_mode_matches_go` (restore text
captured from `pkg/parser` with `SetSQLMode(mysql.ModePipesAsConcat)`) and
by the `pipes_as_concat_sql_mode` result-ring corpus topic.

### Rank 2 — one side accepts SQL the other rejects

#### 3. Rust chains predicates and `IS TRUE`; Go refuses both

Go's infix loop carries two latches that Rust has no equivalent for:

- `noMorePredicate` (`pkg/parser/expr_parser.go:45`, set at :80, :92,
  :103, :114, :125, :204, tested at :59, :86, :97, :109, :120, :169,
  :182): once a predicate (`IN`/`LIKE`/`ILIKE`/`BETWEEN`/`REGEXP`/`RLIKE`/
  `MEMBER OF`) has been built, its *result* can never be the left operand
  of another. This reproduces the source production
  `predicate: bit_expr LIKE ...`, whose left side is a `bit_expr`.
- `noMoreIS` (`pkg/parser/expr_parser.go:39`, set at :149, tested at
  :136): `IS [NOT] NULL` chains (it is at `boolPri` level), but
  `IS [NOT] TRUE/FALSE/UNKNOWN` does not (it is at `Expression` level).

Rust's loop (`tidb-parser/src/expr.rs:52-215`) has neither latch — every
predicate arm ends in a bare `continue`.

```sql
SELECT 'a' LIKE 'b' LIKE 'c';
```

- Go: `parseLikeExpr` sets `noMorePredicate`; the second `LIKE` hits
  `if noMorePredicate { return left }`, and the statement parser then
  finds `LIKE` unconsumed -> syntax error (MySQL also rejects it).
- Rust: parses to `Like(Like('a','b'), 'c')` — accepted, and evaluated.

Same shape for `SELECT 1 IN (1) IN (0)` and `SELECT 1 BETWEEN 0 AND 2
BETWEEN 0 AND 2`, and:

```sql
SELECT 1 IS TRUE IS TRUE;
```

- Go: rejected (`noMoreIS`).
- Rust: `IsTruth(IsTruth(1))`.

**Not fixed** — two new pieces of loop state plus seven guarded arms.

#### 4. Rust's expression-prefix identifier gate is `is_reserved` (232 keywords) where Go's is `isReservedClauseKeyword` (13)

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
  reference.
- Rust: `is_reserved("ROWS")` is true, the next token is `FROM` not `(` ->
  `Err("unsupported keyword in expression")`.

Around 220 keywords sit in this gap. Go is more permissive than MySQL
here; Rust is closer to MySQL. Either way the two disagree on what a
client can run — and this gate is why #5 cannot be fixed on its own.

#### 5. `RESERVED_KEYWORDS` is missing `DATABASE`, `DATABASES`, `DISTINCT`

Mechanical diff, Go `keywords.go` reserved rows (233) vs
`tidb-lexer/src/reserved.rs`'s `RESERVED_KEYWORDS` (232) — the list the
**parser** consults, distinct from the equal catalog in negative-evidence
item 2:

```
Go-only:   DATABASE, DATABASES, DISTINCT
Rust-only: SCHEMA, SCHEMAS
```

The Rust-only two are **not** errors: `pkg/parser/misc.go:722-723` maps
`"SCHEMA"`/`"SCHEMAS"` to the `database`/`databases` token constants,
which `IsReserved` returns true for. So the Go *parser* does reject
`SCHEMA` as an identifier even though `keywords.go` advertises it as
unreserved — Rust's list correctly models the parser, and the catalog and
the parser list are allowed to disagree here.

The three Go-only entries are a derivation bug whose cause `reserved.rs`'s
own module doc names without following through: `tokenMap` is many-to-one,
so inverting it keeps only one spelling per token constant. Three pairs
collide — `DISTINCT`/`DISTINCTROW` -> `distinct`, `DATABASE`/`SCHEMA` ->
`database`, `DATABASES`/`SCHEMAS` -> `databases` — and in each pair the
*other* spelling won. The doc caught the `DISTINCTROW` case and hand-added
it; it did not generalize.

Consequence at the two Go call sites that use `IsReserved`:

```sql
SELECT 1 AS database;
```

- Go: `pkg/parser/select_parser.go:455` — `IsReserved(database)` is true
  -> syntax error.
- Rust: accepted, field alias `database`.

Same for a CTE name (`pkg/parser/select_parser.go:185`): `WITH database AS
(SELECT 1) SELECT * FROM database` is a Go syntax error and a Rust
success.

**Not fixed, deliberately.** Adding the three strings is a one-line change
that is *correct for the alias path and wrong for the expression path*,
because of #4: Rust reuses one list for both gates.
`tidb-parser/src/tests/format.rs:84` asserts `database.table.column`
parses to `` `database`.`table`.`column` `` — which Go also accepts, via
the expression-prefix fallback, not `IsReserved` — and that test would
start failing. The coherent fix is #4 and #5 together: move the
expression-prefix bare-identifier arm onto `is_clause_keyword`, *then* add
the three keywords. That is a wide-blast-radius change, and this machine
cannot run `cargo test` to bound it (see "Unverified"). The edit was made,
`cargo check`ed, and reverted rather than shipped blind.

#### 6. `0X41` was a lex error in Rust and a hex literal in Go — FIXED

- Go: `pkg/parser/lexer.go:769` — `case ch1 == 'x' || ch1 == 'X':`, one
  arm, both spellings hex. Note the deliberate asymmetry two lines later:
  `case ch1 == 'b'` is a bit literal but `case ch1 == 'B'` is an
  identifier.
- Rust, before this change: `tidb-lexer/src/lib.rs`'s `scan_number` handled `b'x'` as
  hex and gave `b'X'` its own arm returning `TokenKind::Invalid` — the
  `0b`/`0B` asymmetry copied onto the wrong pair.

```sql
SELECT 0X41;
```

- Go: `hexLit` -> `'A'`.
- Rust (before): `Invalid` token -> lex error.

**Fixed** — `b'x' | b'X'` share the hex arm. `x'41'`/`X'41'` were already
correct on both sides (`startWithXx` vs `scan_x`), as were `0x`
(identifier), `0x7fz3` (identifier), `0b`/`0B`.

#### 7. `HIGH_NOT_PRECEDENCE` disables `NOT IN` / `NOT LIKE` / `NOT BETWEEN` in Go but not in Rust

Go rewrites the token in the **lexer**: `pkg/parser/lexer.go:252-253`,
`if tok == not && s.sqlMode.HasHighNotPrecedenceMode() { return not2 }`.
`not2` is handled as a *prefix* operator only
(`pkg/parser/expr_parser.go:389-395`); the infix loop's `case not:`
(`expr_parser.go:57`) never fires, because no `not` token is ever produced
in that mode.

Rust puts the switch in the **parser** (`tidb-parser/src/expr.rs:576-582`)
and leaves the token spelling alone, so `expr.rs:57-70`'s `NOT IN`/`NOT
LIKE`/`NOT BETWEEN`/`NOT REGEXP` lookahead still matches.

```sql
-- sql_mode = 'HIGH_NOT_PRECEDENCE'
SELECT 1 NOT IN (1, 2);
```

- Go: `NOT` lexes as `not2`; the infix loop sees no operator it handles,
  returns `1`, and the leftover `not2 IN ...` is a syntax error.
- Rust: `NOT IN` predicate, result `0`.

The *prefix* behaviour agrees: both bind `NOT` at the unary level in that
mode and both emit the `!`-restoring node (Go `opcode.Not2`; Rust
`UnaryOp::Not` vs `UnaryOp::NotKeyword`). Real MySQL accepts `NOT IN` here,
so this is a Go-side defect Rust happens not to share — recorded because a
client can hit the difference, not because Rust needs changing.

#### 8. A malformed `/*T![` feature list was an ordinary comment in Rust and executable SQL in Go — FIXED

`pkg/parser/lexer.go:908-951` (`scanFeatureIDs`) rewinds and returns a
**nil** slice for *both* a missing `[` and a malformed list, and
`pkg/parser/tidb/feature.go`'s `CanParseFeature()` over zero features is
vacuously `true` — so `pkg/parser/lexer.go:540-544` makes both cases
executable. Rust special-cased only the missing `[` and let a malformed
list fall through to the ordinary-comment path.

```sql
SELECT /*T![ x] 1 */ 2;
```

(the space after `[` is what makes the list malformed)

- Go: executable comment; the body `[ x] 1` becomes live SQL -> syntax
  error.
- Rust (before): comment dropped -> `SELECT 2`, one row.

**Fixed** — `scan_slash`'s `b'T'` arm now treats `scan_feature_ids() ==
None` as executable, collapsing to Go's exact shape. A *well-formed* list
naming an unsupported feature still demotes the comment, matching
`CanParseFeature`; the existing `/*T![unsupported] ... */` tests
(`tidb-lexer/src/tests/lexer_source.rs:296,410,487`) cover that path and
are unaffected.

### Rank 3 — AST shape / token text differences

#### 9. Under `ANSI_QUOTES`, Rust leaves backslash escapes unprocessed in the resulting identifier

Go converts an already-scanned string token to an identifier *after*
`scanString` has decoded it: `pkg/parser/lexer.go:244-248` flips only
`tok`, leaving `v.Lit` as the decoded buffer (doubled delimiters collapsed
**and** backslash escapes resolved via `handleEscape`).

Rust re-derives the text from the raw span:
`tidb-lexer/src/lib.rs:352` flips the kind, and `:369` computes the text
with `unquote(raw, '"')` — which (`:1053`) only collapses `""`.

```sql
-- sql_mode = 'ANSI_QUOTES'
SELECT 1 AS "a\tb";
```

- Go: alias identifier is `a<TAB>b` (3 characters).
- Rust: alias identifier is `a\tb` (4 characters, literal backslash).

Column labels on the wire differ. `""` doubling agrees on both sides, as
does backtick quoting (`scanQuotedIdent` at `lexer.go:683` does *not*
process backslashes, and neither does `unquote(raw, '`')`).

#### 10. Whitespace class: Go's is byte-widened Unicode, Rust's is ASCII

`pkg/parser/lexer.go:393` (`skipWhitespace`), `:399` (`scan`'s dispatch)
and `:489` (the `-- ` comment introducer) all test
`unicode.IsSpace(rune(b))` — a *byte* widened to a rune, so the raw bytes
`0x85` (NEL) and `0xA0` (NBSP) count as whitespace.
`tidb-lexer/src/lib.rs:979`'s `is_space` is the ASCII set only, and
`is_ident_extend` claims every byte `>= 0x80`. So `SELECT 1 --\xa0 x` is a
comment in Go and a `- -` operator pair in Rust.

**Reachability caveat:** `Lexer::new` takes `&str`, so a bare `0x85`/`0xA0`
byte cannot occur in Rust's input except as a UTF-8 continuation byte,
where its leading byte (`0xC2` for U+00A0) is *not* Go-whitespace either
and both sides agree. This is only observable on input Go accepts and the
Rust API cannot represent — see #11, which is the same boundary.

#### 11. Rust's string and backtick scanners are not client-charset aware

`scanString`/`scanQuotedIdent` (`pkg/parser/lexer.go:717,687`) call
`s.r.skipRune(s.client)` first, copying a whole multi-byte character
verbatim before any delimiter/backslash test. Rust's `scan_string`
(`lib.rs:498`) and `scan_quoted_ident` (`:481`) are byte-oriented and, as
above, cannot even receive the input: the whole `tidb-lexer` API is
`&str`-typed. For a non-UTF-8 client charset whose trailing bytes can be
`0x5C` or `0x27` (gbk, big5, sjis), Go copies such a character through
while any byte-oriented reader would see a backslash or a closing quote.
The divergence is therefore structural — a missing client-charset seam,
not a wrong branch — and is recorded here so it is not mistaken for
parity.

#### 12. `@@instance.x` is split differently

`pkg/parser/lexer.go:624` recognizes exactly `{"global.", "session.",
"local."}`; `tidb-lexer/src/lib.rs:525` adds `"instance."`. Go falls
through to `scanIdentifierOrString`, whose `isUserVarChar` includes `.`,
so it still yields one `doubleAtIdentifier` spanning the whole text —
which is why this is filed at rank 3 rather than rank 2. Any Rust code
that keys on the split prefix rather than the whole span will disagree.

## Fixes made in this branch

| Finding | File |
| --- | --- |
| #1 LIKE/REGEXP pattern precedence | `rust/crates/tidb-parser/src/expr/predicate.rs` |
| #6 `0X` hex prefix | `rust/crates/tidb-lexer/src/lib.rs` |
| #8 malformed `/*T![` list | `rust/crates/tidb-lexer/src/lib.rs` |

## Unverified

- **Nothing in this document was executed.** `syspolicyd` on this machine
  wedges every freshly built binary at `_dyld_start`, so `cargo test`,
  `nextest`, `gorun` and `goeval` could not be run. Every parse claim is
  read from source on both sides; the SQL strings are worked examples of
  the cited code paths, not captured output. In particular the three fixes
  are compile-verified only — no test run confirms them, and no test run
  confirms they broke nothing.
- Gates that WERE run, on the crates touched: `cargo check -p tidb-lexer
  -p tidb-parser` (exit 0), `cargo clippy -p tidb-lexer -p tidb-parser
  --all-targets` (exit 0), `cargo fmt --all --check` (exit 0).
- Not audited: JOIN nesting and the `NATURAL`/`USING` forms, CTE and
  recursive-CTE structure, window-function clauses, `GROUP BY ... WITH
  ROLLUP`, and the statement-level accept/reject surface (which statements
  one side parses and the other refuses). The expression, lexer, keyword
  and set-operation surfaces consumed the available time.
- Negative-evidence item 7 establishes that neither parser encodes
  `INTERSECT`'s higher precedence in the AST — it is deferred to the
  consumer of the flat `Selects`/`terms` list. That consumer was **not**
  checked on either side, and a mismatch there would be a rank-1 defect
  invisible to this audit.
