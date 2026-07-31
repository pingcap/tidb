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

**Nothing walks through that door.** Every parse call site in the session,
executor, and exec crates calls the mode-less `tidb_parser::parse` /
`parse_multi`, which hard-code `SqlMode::default()` — all-false. Grepped:
8 sites in `tidb-session`, 7 in `tidb-exec`, 15 in `tidb-executor`. So the
session's `@@sql_mode` reaches the *statement context*
(`crates/tidb-session/src/stmt_ctx.rs:164-247`, where `ONLY_FULL_GROUP_BY`,
`ERROR_FOR_DIVISION_BY_ZERO`, `STRICT_*` and `NO_AUTO_VALUE_ON_ZERO` are read)
but never reaches the *lexer*. Every scanner-facing flag is therefore
NOT CONSULTED regardless of the lexer's own readiness.

The reason it is not a one-line fix: the SQL **text** flows down through the
tiers and is re-parsed at each of them, so the mode has ~30 doors to walk
through rather than one. Go parses once, in `session.ExecuteStmt`, and passes
the AST. Threading the mode is a workaround for that shape; parsing once and
passing the AST is the root fix. See "What would fix this" below.

## Per-flag verdict table

Captures are `gorun` against real TiDB; each probe was run twice, once under the
default `sql_mode` as the control and once with the flag set alone.

The "This tier answers X both times" claims for scanner flags are a *proven
consequence*, not a sampled observation: `tidb_parser::parse` (`lib.rs:385`)
resolves to `parse_with_configuration(sql, false, SqlMode::default())`,
`SqlMode::default()` is all-false, and no session-tier call site uses any other
entry point. There is no path by which a scanner flag can reach the lexer today.

### Scanner / grammar flags (the lexer seam)

| Flag | Go consults it | This tier | Capture |
| --- | --- | --- | --- |
| `NO_BACKSLASH_ESCAPES` | `pkg/parser/lexer.go:730` (string scan), `pkg/planner/core/expression_rewriter.go:2384` (LIKE default escape), `pkg/sessionctx/variable/sysvar.go:1947` (handshake status flag) | **DIVERGES** — lexer supports it (`tidb-lexer/src/lib.rs:510`, `:1074`) but no caller passes it | `SELECT LENGTH('a\nb')` → **3** default, **4** under the flag. This tier answers 3 both times. |
| `ANSI_QUOTES` | `pkg/parser/lexer.go:242` (double-quoted token becomes an identifier), `pkg/parser/hintparser.go:1125`, `pkg/util/stringutil/string_util.go:400` (identifier quoting) | **DIVERGES** — lexer supports it (`tidb-lexer/src/lib.rs:352`) but no caller passes it | `SELECT "id" AS a` → **row `id`** default, **ERR** (unknown column) under the flag. This tier answers `id` both times. |
| `PIPES_AS_CONCAT` | `pkg/parser/lexer.go:248` (`pipes` → `pipesAsOr` when unset), `pkg/parser/prec.go:84` (`precConcat` vs `precOr`) | **NOT CONSULTED** — not even a lexer field; `crates/tidb-parser/src/expr.rs:312` hard-codes `"\|\|" => LogicOr`, and `crates/tidb-parser/src/prec.rs:44` documents the missing `precConcat` level | `SELECT 1 \|\| 2` → **1** default, **12** under the flag. This tier answers 1 both times. |
| `HIGH_NOT_PRECEDENCE` | `pkg/parser/lexer.go:252` (`not` → `not2`) | **DIVERGES** — parser supports it (`tidb-parser/src/expr.rs:577`, `:594`) but no caller passes it | `SELECT NOT 1 BETWEEN 0 AND 3` → **0** default, **1** under the flag. This tier answers 0 both times. |
| `REAL_AS_FLOAT` | `pkg/parser/ddl_fieldtype_parser.go:67`, `pkg/parser/expr_cast_parser.go:348` | **DIVERGES** — parser supports it (`tidb-parser/src/ddl/field_type.rs:55`, `cast.rs:209`) but no caller passes it | `CREATE TABLE rf (a REAL)` → `SHOW CREATE TABLE` says **`double`** default, **`float`** under the flag. This tier says `double` both times. |
| `IGNORE_SPACE` | `pkg/parser/misc.go:1148` (a space before `(` stops builtin-name recognition) | **NOT CONSULTED at the session** — lexer supports it (`tidb-lexer/src/lib.rs:402`, `tidb-parser/src/ddl/create.rs:162`), no caller passes it | No divergence at the obvious probe: `SELECT count (1)` → **1** in both modes. The flag's reachable effect is narrower than the name suggests; not a ranked gap. |

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

## Ranked gaps

1. **`NO_BACKSLASH_ESCAPES` / `ANSI_QUOTES` / `HIGH_NOT_PRECEDENCE` /
   `REAL_AS_FLOAT` — supported by the lexer, unreachable from the session.**
   Four flags, one cause, one fix. Highest value per unit of change *if* the
   re-parse shape is addressed; a per-call-site thread is ~30 edits with the
   widest blast radius in the tree (the parser is shared by everything).
2. **`PIPES_AS_CONCAT`** — same seam, plus the lexer field and the `precConcat`
   level do not exist yet. Do it in the same change as (1) or not at all.
3. **`NO_UNSIGNED_SUBTRACTION`** — a self-contained expression-tier flag with
   no parser blast radius. Cheapest honest win on this list.
4. **`ORACLE`'s unaliased derived table** — narrow, low value.

## Why the cheap version of the scanner fix is worse than the gap

`Session::execute_statement` (`crates/tidb-session/src/dispatch.rs:277`) does
produce an AST, and a SELECT is executed from *that* AST. So switching that one
call to `parse_with_sql_mode` would make all four scanner probes above pass.

It would also be the wrong change. DML and DDL do not use that AST: the raw
`sql` string is handed down and re-parsed
(`tidb_executor::run_insert_reporting(sql, ...)` at `dispatch.rs:366`,
`run_update_in(sql, ...)` at `:395`, `tidb-executor/src/ddl.rs:228`, and the six
`ddl/*.rs` sites). The two recorded `NO_BACKSLASH_ESCAPES` divergences in
`tests/integrationtest/t/generated_columns.test:189-214` are precisely a
`CREATE TABLE ... AS (concat(s, '\\c'))` and an `INSERT ... VALUES ('a\\b')` —
both on the re-parsing side. The one-site change would leave
`SELECT 'a\nb'` correct and `INSERT ... VALUES ('a\nb')` wrong in the same
session, under the same flag.

A flag that is uniformly absent is a gap someone can find. A flag that is
honored in SELECT and ignored in INSERT is a data-corruption shape. This
inventory therefore names the seam rather than half-wiring it.

## What would fix this

The re-parse-per-tier shape is the reason a scanner flag cannot be wired
cheaply. Two options, in preference order:

1. **Parse once.** `Session::run_with_columns` parses under the session's mode
   and passes the `Stmt` down; the tiers below take an AST, not a string. This
   removes the flag-threading problem instead of solving it, and removes the
   redundant parses too. Large, and it touches every tier.
2. **Thread `SqlMode` to every parse entry.** Mechanical, ~30 call sites, and
   every future call site is a fresh chance to forget — the edge case does not
   disappear, it multiplies.

Do not add a thread-local "current sql_mode" read by `parse`. It would make the
captures pass while leaving the parser's behavior dependent on ambient state
that no signature admits to.
