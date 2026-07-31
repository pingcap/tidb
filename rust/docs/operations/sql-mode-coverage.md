# `sql_mode` coverage inventory

Every `sql_mode` flag Go defines, where Go consults it, whether this tier
consults it, and — for the ones we do — whether the behavior matches.

The Go source of truth is `pkg/parser/mysql/const.go` (the constant block at
`const.go:516`, the `HasXxxMode` predicates at `const.go:425-502`, and the
`Str2SQLMode` table at `const.go:594`). This file was built by grepping `pkg/`
for each constant and each predicate, then capturing the observable behavior
from real TiDB with `gorun` (unistore-backed mock cluster) and comparing.

## How to read this

- **Go consults it** — the deciding call site in `pkg/`, with file:line. A flag
  with *no* consult site outside `const.go` is one Go parses and stores but
  never acts on; parity there means accepting the value, not implementing it.
- **This tier** — MATCHES / DIVERGES / NOT CONSULTED, from grepping `rust/crates`
  for the flag name and reading the deciding code.
- A verdict of NOT CONSULTED is a statement about *silent* difference: the
  session accepts `SET sql_mode = 'X'`, reports it back from `@@sql_mode`, and
  then behaves as if it were unset.

## The structural finding

The lexer already models five of the flags Go's scanner consults
(`crates/tidb-lexer/src/lib.rs:106` — `real_as_float`, `no_backslash_escapes`,
`ansi_quotes`, `high_not_precedence`, `ignore_space`), and
`tidb_parser::parse_with_sql_mode` (`crates/tidb-parser/src/lib.rs:409`) is the
door for passing them.

**The door is now walked through, on the session tier.** The mode reaches the
lexer by riding the object every executor entry already takes:

* `Session::scanner_sql_mode` (`crates/tidb-session/src/stmt_ctx.rs`) reads the
  live `@@sql_mode` text and builds the lexer's `SqlMode`;
* `Session::parse` is the single session-tier parse door — Go's
  `session.ParseSQL` — and all 8 session parse sites go through it;
* `StmtContext::sql_mode` carries it into the executor, where
  `StmtContext::parse` is the single re-parse door for SELECT/INSERT/UPDATE/
  DELETE/ALTER TABLE/CREATE INDEX;
* the five DDL entries that take no `StmtContext` (`run_create_table_in`,
  `run_drop_table_in`, `run_rename_table_in`, `run_truncate_table_in`,
  `run_drop_index_in`) take the mode as an explicit parameter, so the compiler
  asks for it rather than a call site defaulting it silently.

`tidb-exec` (the real-TiKV tier) is NOT covered: it has no session object to
read a mode from, and its statements arrive through a different front. Its 7
parse sites remain mode-less, and every scanner flag is uniformly absent there
— a gap, not a split.

The shape being worked around is still real: this tier re-parses the statement
text below the session, where Go parses once in `session.ParseSQL` and passes
the AST. Threading the mode through one context object keeps the number of
doors at three (`Session::parse`, `StmtContext::parse`, the DDL parameter)
rather than thirty, but parsing once remains the root fix. See "What would fix
this" below.

## Per-flag verdict table

Captures are `gorun` against real TiDB; each probe was run twice, once under the
default `sql_mode` as the control and once with the flag set alone.

The "Capture" column is what real TiDB answers. The scanner rows below now
MATCH on the session tier; each is guarded end to end by
`crates/tidb-session/src/tests_sql_mode_scanner.rs`, which asserts every flag
through a SELECT *and* through a re-parsing statement (INSERT/UPDATE/DELETE or
DDL), because those are two different parse doors.

### Scanner / grammar flags (the lexer seam)

| Flag | Go consults it | This tier | Capture |
| --- | --- | --- | --- |
| `NO_BACKSLASH_ESCAPES` | `pkg/parser/lexer.go:730` (string scan), `pkg/planner/core/expression_rewriter.go:2384` (LIKE default escape), `pkg/sessionctx/variable/sysvar.go:1947` (handshake status flag) | **MATCHES** on the session tier — the lexer's support (`tidb-lexer/src/lib.rs:510`, `:1074`) now has callers; the LIKE-default-escape consult site is still unported | `SELECT LENGTH('a\nb')` → **3** default, **4** under the flag. This tier answers 3 both times. |
| `ANSI_QUOTES` | `pkg/parser/lexer.go:242` (double-quoted token becomes an identifier), `pkg/parser/hintparser.go:1125`, `pkg/util/stringutil/string_util.go:400` (identifier quoting) | **MATCHES** on the READ side (`tidb-lexer/src/lib.rs:352`, now reached). The RESTORE side is still open: TiDB also QUOTES identifiers with `"` under the flag (captured: `SHOW CREATE TABLE rf` prints `CREATE TABLE "rf"`), and this tier still prints backticks | `SELECT "id" AS a` → **row `id`** default, **ERR** (unknown column) under the flag. This tier answers `id` both times. |
| `PIPES_AS_CONCAT` | `pkg/parser/lexer.go:248` (`pipes` → `pipesAsOr` when unset), `pkg/parser/prec.go:84` (`precConcat` vs `precOr`) | **NOT CONSULTED** — not even a lexer field; `crates/tidb-parser/src/expr.rs:312` hard-codes `"\|\|" => LogicOr`, and `crates/tidb-parser/src/prec.rs:44` documents the missing `precConcat` level | `SELECT 1 \|\| 2` → **1** default, **12** under the flag. This tier answers 1 both times. |
| `HIGH_NOT_PRECEDENCE` | `pkg/parser/lexer.go:252` (`not` → `not2`) | **MATCHES** on the session tier (`tidb-parser/src/expr.rs:577`, `:594`, now reached) | `SELECT NOT 1 BETWEEN 0 AND 3` → **0** default, **1** under the flag. This tier answers 0 both times. |
| `REAL_AS_FLOAT` | `pkg/parser/ddl_fieldtype_parser.go:67`, `pkg/parser/expr_cast_parser.go:348` | **MATCHES** on the session tier — reached through the DDL entries' explicit `sql_mode` parameter (`tidb-parser/src/ddl/field_type.rs:55`, `cast.rs:209`) | `CREATE TABLE rf (a REAL)` → `SHOW CREATE TABLE` says **`double`** default, **`float`** under the flag. This tier says `double` both times. |
| `IGNORE_SPACE` | `pkg/parser/misc.go:1148` (a space before `(` stops builtin-name recognition) | **CONSULTED** — passed with the rest (`tidb-lexer/src/lib.rs:402`), which matters because `ANSI` turns it on | No divergence at the obvious probe: `SELECT count (1)` → **1** in both modes. The flag's reachable effect is narrower than the name suggests; not a ranked gap. |

### Statement-context flags (the `stmt_ctx` seam — already wired)

| Flag | Go consults it | This tier | Capture |
| --- | --- | --- | --- |
| `ONLY_FULL_GROUP_BY` | `pkg/planner/core/logical_plan_builder.go:1861`, `checkOnlyFullGroupBy*` at `:3731`-`:3781` | **MATCHES** (partial) — `stmt_ctx.rs:215`, `:233`; the DISTINCT half landed recently | covered by `tests_harvested_relation_engine.rs:872` (`having_cannot_see_an_ungrouped_column_in_any_sql_mode`) |
| `STRICT_TRANS_TABLES` / `STRICT_ALL_TABLES` | `HasStrictMode()` — `pkg/util/misc.go:636`, `pkg/table/column.go:277`, `pkg/planner/optimize.go:254`, `pkg/ddl/index.go:141` | **MATCHES** (partial) — `stmt_ctx.rs:231` folds both into one strict bit, as Go's `HasStrictMode` does; per-statement-kind NOT NULL rule landed | `tests_bad_null.rs`, `tests_write_conversion.rs` |
| `ERROR_FOR_DIVISION_BY_ZERO` | `pkg/executor/select.go:1131`, `:1286`, `:1308`; `pkg/ddl/reorg.go:266` | **MATCHES** (partial) — `stmt_ctx.rs:230`; threaded for generated columns and DML | `tests_generated_columns.rs:479` |
| `NO_AUTO_VALUE_ON_ZERO` | `pkg/executor/insert_common.go:831`, `:908`, `:1004`, `:1089`, `:1169` | **REFUSED, not diverging** — `stmt_ctx.rs:247` reads it; `tidb-executor/src/driver/dml.rs:283` raises "not supported yet" rather than silently doing the wrong thing | `tests_auto_increment.rs:345` |

### Date/time flags (owned by a parallel worker — inventoried, not touched)

| Flag | Go consults it | This tier |
| --- | --- | --- |
| `NO_ZERO_IN_DATE` | `pkg/util/misc.go:641`, `pkg/table/column.go:301`, `pkg/executor/select.go:1141`, `pkg/expression/builtin_time.go:314` | in flight — `tidb-executor/src/stmt_context.rs:464` names it as unmodelled |
| `NO_ZERO_DATE` | `pkg/util/misc.go:642`, `pkg/table/column.go:277-297`, `pkg/ddl/add_column.go:1239` | in flight — same |
| `ALLOW_INVALID_DATES` | `pkg/util/misc.go:640`, `pkg/ddl/reorg.go:255`, `pkg/table/tables/partition.go:291` | in flight — same |

### Flags Go accepts but does not act on

Parity here means accepting and echoing the value, which this tier does via
`tidb_mysql::get_sql_mode` (`crates/tidb-mysql/src/consts.rs:631`).

| Flag | Go consult sites outside `const.go` | Note |
| --- | --- | --- |
| `PAD_CHAR_TO_FULL_LENGTH` | none — only the unrelated `FlagPadCharToFullLength` column-flag bit at `pkg/meta/model/flags.go:27` | Captured: `LENGTH(CHAR(10) 'ab')` → **2** in both modes. Go does not implement the pad. This tier mirrors the bit at `crates/tidb-model/src/flags.rs:25`. |
| `NO_ENGINE_SUBSTITUTION` | none | in TiDB's default `sql_mode`, inert |
| `NO_DIR_IN_CREATE`, `NOT_USED`, `NO_KEY_OPTIONS`, `NO_TABLE_OPTIONS`, `NO_FIELD_OPTIONS`, `MYSQL323`, `MYSQL40`, `POSTGRESQL`, `MSSQL`, `DB2`, `MAXDB`, `ANSI` | none | accepted, stored, inert |
| `ORACLE` | `pkg/planner/core/preprocess.go:408`, `:1144` (a derived table may go unaliased) | **NOT CONSULTED** here. Narrow: affects only whether a derived table must be named. |
| `NO_AUTO_CREATE_USER` | `pkg/executor/grant.go:186` | **NOT CONSULTED** as a flag, but this tier already refuses implicit GRANT-creates unconditionally (`crates/tidb-session/src/account.rs:909`), which is the default-mode behavior. Diverges only for a session that clears the flag. |

### Expression flags

| Flag | Go consults it | This tier | Capture |
| --- | --- | --- | --- |
| `NO_UNSIGNED_SUBTRACTION` | `pkg/expression/builtin_arithmetic.go:378`, `:473`, `builtin_arithmetic_vec.go:387` | **NOT CONSULTED** — `crates/tidb-expr/src/builtin_arithmetic.rs:291` and `ops.rs:538` both document assuming it unset (the default), which is the right default but not the flag | `BIGINT UNSIGNED a=1; SELECT a - 2` → **ERR** (out of range) default, **-1** under the flag. This tier gives the default answer in both modes. |

## The composite that makes this reachable by accident

`SET sql_mode = 'ANSI'` expands to five flags, four of them scanner flags
(Go `CombinationSQLMode`, mirrored exactly at
`crates/tidb-mysql/src/consts.rs:545`). Captured from TiDB:

```
SET sql_mode='ANSI';
SELECT @@sql_mode;  -- REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI
SELECT "id" AS a;   -- ERR (identifier, not string)
SELECT 1 || 2;      -- 12
```

Because the expansion is what gets STORED in `@@sql_mode`, reading flag names
out of that text sees everything a combination brought in — which is why
`scanner_sql_mode_of` matches names rather than re-deriving the bitset. This
tier now answers the column for `SELECT "id"`; `1 || 2` is still `1`, the one
remaining flag of the four (`PIPES_AS_CONCAT`, gap 1 below).

## Ranked gaps

1. **`PIPES_AS_CONCAT`** — the last scanner flag of the `ANSI` expansion still
   unmodelled, and the only one whose fix is not plumbing: the lexer has no
   field for it and `crates/tidb-parser/src/prec.rs:44` documents the missing
   `precConcat` level.
2. **`ANSI_QUOTES` on the RESTORE side** — reading is done; `SHOW CREATE TABLE`
   and every other restore still print backticks where TiDB prints `"` under
   the flag (Go `pkg/util/stringutil/string_util.go:400`).
3. **The `tidb-exec` tier** — its 7 parse sites have no session mode to read.
   Uniformly absent there, so not a split; wiring it needs a session-state
   channel that tier does not have yet.
4. **`NO_UNSIGNED_SUBTRACTION`** — a self-contained expression-tier flag with
   no parser blast radius. Cheapest honest win on this list.
5. **`NO_BACKSLASH_ESCAPES` in LIKE's default escape** — Go
   `pkg/planner/core/expression_rewriter.go:2384`, gated by
   `@@tidb_enable_no_backslash_escapes_in_like`; the variable exists here
   (`crates/tidb-vardef/src/tidb_vars.rs:712`), the consult does not.
6. **`ORACLE`'s unaliased derived table** — narrow, low value.

## Why the one-site version would have been worse than the gap

`Session::execute_statement` (`crates/tidb-session/src/dispatch.rs`) does
produce an AST, and a SELECT is executed from *that* AST. Switching only that
call to `parse_with_sql_mode` would have made all four scanner probes pass.

It would also have been the wrong change. DML and DDL do not use that AST: the
raw `sql` string is handed down and re-parsed (`run_insert_reporting`,
`run_update_in`, `run_delete_in`, `tidb-executor/src/ddl.rs`, and the six
`ddl/*.rs` sites). The two recorded `NO_BACKSLASH_ESCAPES` cases in
`tests/integrationtest/t/generated_columns.test:189-214` are precisely a
`CREATE TABLE ... AS (concat(s, '\c'))` and an `INSERT ... VALUES ('a\b')` —
both on the re-parsing side. A one-site change would have left
`SELECT 'a\nb'` correct and `INSERT ... VALUES ('a\nb')` wrong in the same
session under the same flag: a flag uniformly absent is a gap someone can find,
a flag honored in SELECT and ignored in INSERT writes different bytes than it
reads.

That is why the fix went through `StmtContext` — the object every executor
entry already takes — and why the guard tests assert every flag through a
re-parsing statement as well as a SELECT.

## Measured

`generated_columns` is the topic where the two recorded
`NO_BACKSLASH_ESCAPES` cases live. Replayed against the recorded TiDB
output, before and after (the "before" produced by forcing the mode back to
`SqlMode::default()` at both parse doors, on top of this change):

```
before   93 matched   17 diverged of 120   Rows matched: 3
after   103 matched    7 diverged of 120   Rows matched: 13
```

Ten statements stop diverging, and all seven that remain are EXPLAIN
plan-shape rows (missing index-access properties), none of them a `sql_mode`
case. The suite-wide `KNOWN_DIVERGENCES` ratchet is unchanged at 55: that gate
covers the onboarded topic list, which `generated_columns` is not on.

Controls run with the change: 839 of 839 `tidb-parser` + `tidb-lexer` +
`tidb-ast` tests (the Go-derived restore rows among them) and 5759 of 5759
workspace tests.

## What a DDL expression does with the mode

Captured from TiDB, and mirrored here: a generated column or a DEFAULT created
under `NO_BACKSLASH_ESCAPES` is stored in the parser's CANONICAL form, so a
later reader's mode cannot change it.

```
set sql_mode='NO_BACKSLASH_ESCAPES';
create table g (s varchar(20), c varchar(40) as (concat(s,'\c')) stored,
                d varchar(20) default 'x\y');
insert into g (s) values ('a');    -- a | a\c | 3 | x\y | 3
show create table g;               -- ... AS (concat(`s`, _utf8mb4'\\c')) ...
set sql_mode=default;
insert into g (s) values ('b');    -- b | b\c | 3   (same expression)
```

The mode therefore matters exactly once, at the statement that WRITES the
expression. `tidb-ast`'s `Literal` carries the "`NO_BACKSLASH_ESCAPES` was
active" bit (`crates/tidb-ast/src/base.rs:82`) so its restore is canonical the
same way.

## What would still fix this properly

1. **Parse once.** `Session::run_with_columns` parses under the session's mode
   and passes the `Stmt` down; the tiers below take an AST, not a string. That
   removes the flag-threading problem instead of solving it, and removes the
   redundant parses too. Large, and it touches every tier.
2. **`tidb-exec`.** That tier has no session object at all; giving it one is
   the same seam as `Session` storage convergence, not a parser task.

Do not add a thread-local "current sql_mode" read by `parse`. It would make the
captures pass while leaving the parser's behavior dependent on ambient state
that no signature admits to.
