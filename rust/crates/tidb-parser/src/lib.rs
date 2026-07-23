// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! A recursive-descent SQL parser (Phase 0 subset), transliterated from the
//! hand-written parser in `pkg/parser`.
//!
//! Statements covered: `SELECT` (`DISTINCT`/`ALL`, select list — including a
//! bare `*` and a qualified wildcard `t.*` / `db.t.*`, distinguished from a
//! plain `t.a` column reference by a two-phase lookahead that only commits
//! once it has confirmed the `.` `*` shape —, a `FROM` join
//! tree with comma/`INNER`/`LEFT`/`RIGHT`/`CROSS`/`STRAIGHT_JOIN`/`NATURAL
//! [LEFT|RIGHT]` + `ON`/`USING` (`NATURAL` may only precede a plain/`LEFT`/
//! `RIGHT` join, never `INNER`/`CROSS`/`STRAIGHT_JOIN`, and never combines
//! with an explicit `ON`/`USING` — both genuine `ParseError`s, confirmed
//! via `godump restore` — see [`tidb_ast::Join::natural`]'s own doc)
//! and the `FROM DUAL` placeholder, each table optionally preceded by a
//! `PARTITION (name, ...)` clause and followed by one or more `USE`/
//! `FORCE`/`IGNORE INDEX [FOR JOIN|ORDER BY|GROUP BY] (name, ...)` hints
//! — both also accepted by single-table `UPDATE`/`DELETE`, and
//! `PARTITION` additionally by `INSERT`'s own target table, not just
//! `SELECT`'s own `FROM` (see [`tidb_ast::TableRef::partitions`] and
//! [`tidb_ast::TableRef::hints`]'s own docs for the exact grammar —
//! `PARTITION` parses BEFORE the alias, hints AFTER it — and each
//! construct's own deliberate execution-time scope boundary: a
//! `PARTITION` clause is ALWAYS `Unsupported` since this crate never
//! implements table partitioning at all, while an index hint's NAME is
//! simply never validated against the table's own real indexes), and
//! `WHERE`/`GROUP BY expr [ASC|DESC],
//! ...`/`HAVING`/`ORDER BY`/`LIMIT`/a `FOR UPDATE`/`FOR SHARE`/`LOCK IN
//! SHARE MODE` locking clause — see `crate::select`'s own doc for the
//! flexible ordering `ORDER BY`/`LIMIT`/the locking clause share, and how
//! that same grammar attaches across a set operation's own multiple terms;
//! `GROUP BY`'s own per-item `ASC`/`DESC` is a separate, simpler grammar
//! (each item independent, no shared flexible ordering with the other
//! three) — see [`tidb_ast::GroupByItem`]'s own doc for why an explicit
//! `ASC` still needs to be distinguished from no direction at all despite
//! restoring identically), set operations (`UNION`/`UNION
//! ALL`/`EXCEPT`/`INTERSECT`, with parenthesized terms and statement-level
//! `ORDER BY`/`LIMIT`/locking clause),
//! `INSERT [IGNORE] ... VALUES [ON DUPLICATE KEY UPDATE ...]` (an assignment's
//! value may reference `VALUES(col)`, the row that would have been inserted),
//! single-table `UPDATE ... SET`, single-table `DELETE FROM`, and
//! `CREATE TABLE [IF NOT EXISTS]` (column definitions —
//! `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT`/`FLOAT`/`DOUBLE`/
//! `VARCHAR(n)`/`CHAR(n)`/`DECIMAL(p,s)`/`TEXT`/`DATE`/`DATETIME[(p)]`/
//! `TIME[(p)]`/`TIMESTAMP[(p)]`/`YEAR[(p)]` (dates are plain string values
//! to this parser/executor — no special date-literal syntax or arithmetic;
//! only the column TYPE is recognized, so `DEFAULT '...'` reuses the
//! existing string-literal parsing unchanged; `DEFAULT CURRENT_TIMESTAMP` is
//! not modelled, while the narrow Go `ON UPDATE` NOW/CURDATE-family form is
//! retained as a typed column option and executor-rejected until catalog
//! write-time maintenance exists), each optionally
//! `UNSIGNED`/`ZEROFILL` (a bare `ZEROFILL` implies, and restores,
//! `UNSIGNED`, matching the Go AST), then an optional `CHARACTER SET name`/
//! `CHARSET name` (an alias; both restore as `CHARACTER SET`, canonically
//! uppercased) — which, per real MySQL grammar, may ONLY appear here,
//! immediately after the type, not among the other column options below (a
//! real `ParseError` elsewhere, confirmed via `godump restore`, not
//! assumed; `CHARACTER SET BINARY`'s implicit type-rename,
//! `VARCHAR`→`VARBINARY` and similar, is not modelled) — with
//! `PRIMARY KEY`/`UNIQUE [KEY]`/`NOT NULL`/`NULL`/`AUTO_INCREMENT`/
//! `DEFAULT <expr>`/`COMMENT '...'`/`COLLATE name` options (a column's
//! `COMMENT` text restores as a plain quoted string, unlike `DEFAULT`'s
//! `_UTF8MB4`-prefixed string literals — a real asymmetry, confirmed via
//! `godump restore` rather than assumed; `COLLATE`'s name canonically
//! lowercases, the OPPOSITE convention from `CHARACTER SET`'s uppercase,
//! and — unlike `CHARACTER SET` — is positionally free among the other
//! options, confirmed by probing it both before and after `NOT NULL`),
//! plus table-level constraints ([`tidb_ast::TableConstraint`]) —
//! `[CONSTRAINT [name]] PRIMARY KEY [name] (col1, col2, ...)`,
//! `[CONSTRAINT [name]] UNIQUE [KEY|INDEX] [name] (col1, col2, ...)`, and
//! `[CONSTRAINT [name]] CHECK (expr) [[NOT] ENFORCED]` (`ENFORCED` is the
//! default when neither keyword is written; the constraint is parsed and
//! restored but never enforced by `tidb-exec`, matching real TiDB's own
//! out-of-the-box default — see [`tidb_ast::TableConstraint::Check`]'s own
//! doc) — a `CONSTRAINT` name wins over an inline index name when both are
//! given, matching the Go AST (confirmed via `godump restore`, not
//! assumed), and **all restore in WRITTEN order relative to each other**
//! (e.g. `UNIQUE(b), PRIMARY KEY(a)` restores in that exact order, not
//! `PRIMARY KEY` first — confirmed via `godump restore`, not assumed; this
//! superseded an earlier, incorrect fixed-order assumption) — then
//! trailing table options ([`tidb_ast::TableOption`]): `ENGINE [=] name`
//! (case preserved verbatim — MySQL/TiDB never canonicalize it),
//! `AUTO_INCREMENT [=] n`, `[DEFAULT] {CHARACTER SET | CHARSET} [=] name`
//! and `[DEFAULT] COLLATE [=] name` (both uppercase and gain a `DEFAULT`
//! prefix on restore even when not written — table-level `COLLATE`
//! uppercases, the OPPOSITE convention from a column's own `COLLATE`,
//! which lowercases), and `COMMENT [=] '...'` (restores WITH `=`
//! regardless of whether it was written) — an `ENGINE`/`CHARACTER SET`/
//! `COLLATE` value may be a bare word or an equivalent quoted string, both
//! accepted; a `,` between options is accepted and dropped; **these too
//! all restore in WRITTEN order**, unlike most other lists in this AST,
//! which restore in a fixed canonical order (confirmed via `godump
//! restore` on several reorderings, not assumed), all restore-verified; a
//! bare `KEY`/`INDEX` (a plain secondary index, not a uniqueness
//! constraint), other constraint kinds (`CONSTRAINT ... FOREIGN KEY`), and
//! every other table option (`ROW_FORMAT=...`, `KEY_BLOCK_SIZE=...`, ...)
//! are parsed but discarded, not retained in the AST), and
//! `ALTER TABLE` — `ADD [COLUMN] col ... [FIRST | AFTER col]`,
//! `DROP [COLUMN] name`, `MODIFY [COLUMN] col ... [FIRST | AFTER col]`
//! (changes an existing column's type/options and/or position, never its
//! name), and `CHANGE [COLUMN] old_name col ... [FIRST | AFTER col]` (like
//! `MODIFY`, but also renames `old_name` to `col`'s own name) — each bare
//! keyword (`ADD`/`DROP`/`MODIFY`/`CHANGE`, no `COLUMN`) restores
//! identically to its `COLUMN`-qualified form, matching the Go AST — and
//! `RENAME [TO | AS] name` (renames the table; `TO`, `AS`, or neither all
//! restore identically as `RENAME AS`), `ADD [CONSTRAINT [name]] {INDEX |
//! KEY} [name] (cols)` (a plain secondary index — `KEY` normalizes to
//! `INDEX` on restore, matching the Go AST), and `ADD [CONSTRAINT [name]]
//! UNIQUE [INDEX | KEY] [name] (cols)` (a `CONSTRAINT` name wins over an
//! inline index name when both are given, same as `CREATE TABLE`'s
//! table-level constraints); every other action (`ADD [CONSTRAINT]
//! FOREIGN KEY`/`CHECK`, ...) is an honest [`ParseError`]), and a
//! standalone `RENAME TABLE old1 TO new1 [, old2 TO new2 ...]` statement —
//! a different top-level statement kind than `ALTER TABLE ... RENAME`,
//! though both rename a table; pairs apply in written order. `DROP TABLE
//! [IF EXISTS] name [, name2, ...] [RESTRICT | CASCADE]`
//! ([`tidb_ast::DropTableStmt`] — the trailing `RESTRICT`/`CASCADE`
//! modifier is accepted but restores to nothing, confirmed via `godump
//! restore`; `DROP TEMPORARY TABLE` is out of scope, matching `CREATE
//! TEMPORARY TABLE` not being modelled either). Ordinary system-variable
//! `SET` lists, charset/session-state/resource-group commands, and transaction
//! setting sugar are source-owned by the `set` module; explicit scopes and
//! `@@` names remain visible in [`tidb_ast::SetStmt`]. `SET @name (= | :=)
//! value` is a genuinely separate user-variable production represented by
//! [`tidb_ast::SetUserVarStmt`], so the top router selects it before ordinary
//! system-variable SET. `BEGIN`/`START TRANSACTION` are synonyms when unqualified,
//! both restoring as `START TRANSACTION`; explicit `BEGIN` modes and Go's
//! read-only/AS OF and causal-consistency START options remain AST payload.
//! `COMMIT` and `ROLLBACK` carry no payload. `SAVEPOINT name`,
//! `ROLLBACK TO [SAVEPOINT] name` (the `SAVEPOINT`
//! keyword here is optional and dropped on restore), and `RELEASE
//! SAVEPOINT name` (unlike `ROLLBACK TO`, `SAVEPOINT` here is NOT
//! optional — `RELEASE name` alone is a genuine `ParseError`, confirmed
//! via `godump restore`) each carry just the savepoint name, restored
//! VERBATIM with case preserved — no backtick-quoting, unlike a plain
//! table/column identifier (see `tidb_exec::Database`'s own doc for the
//! execution-time savepoint-list semantics).
//!
//! The expression parser is precedence-climbing with the exact precedence table
//! from `pkg/parser/prec.go`. Expressions cover columns, user/system variables,
//! int/decimal/float/hex/bit/string/NULL/boolean literals, unary and binary
//! operators, parentheses, function calls, aggregates (`COUNT`/`SUM`/`AVG`/
//! `MAX`/`MIN`/`STD*`/`VAR*`/`BIT_*`) and `GROUP_CONCAT([DISTINCT] arg [, arg
//! ...] [SEPARATOR '...'])` (its own shape — multiple arguments and a
//! separator, distinct from the single-argument aggregates above; `SEPARATOR`
//! defaults to `,` and always restores explicitly, matching the Go AST; an
//! `ORDER BY` inside the call is not modelled), date-part extraction
//! (`YEAR`/`MONTH`/`DAY`/`QUARTER` — lexer keywords, so they need the same
//! keyword-before-`(` dispatch as `IF`/`COALESCE`; `DAYOFMONTH` is already a
//! plain identifier, needing no such handling), `DATE_ADD`/`DATE_SUB`
//! (also lexer keywords, added to the same dispatch set) whose second
//! argument is `INTERVAL value unit` — a general prefix expression
//! ([`tidb_ast::Expr::Interval`], matching real MySQL grammar rather than
//! being special-cased to these two function names; the unit is any
//! keyword token, captured as text, so it parses broadly even though only
//! `DAY` is evaluated — see `tidb_expr::date_fn`). `date_expr + INTERVAL
//! amount unit` / `date_expr - INTERVAL amount unit` DESUGAR to
//! `DATE_ADD`/`DATE_SUB` calls right here, at parse time — a genuine real
//! MySQL/TiDB grammar rule (confirmed via `godump restore`, discovered by
//! measuring this crate's own coverage against real TiDB's integration
//! test suite, not guessed), not a `tidb-exec`-side rewrite:
//! `Parser::fold_interval_arith` runs inside the SAME precedence-
//! climbing loop every other binary operator does, so a chain like `a +
//! INTERVAL 5 DAY + INTERVAL 3 DAY` builds on the ALREADY-desugared
//! result at each step, left-associatively, exactly matching real TiDB's
//! own nesting. `+` is commutative here (`INTERVAL ... + date_expr` also
//! desugars, with the non-`Interval` operand always becoming `DATE_ADD`'s
//! FIRST argument regardless of which side it was written on), but `-`
//! is not (`INTERVAL ... - date_expr`, `INTERVAL ... + INTERVAL ...`,
//! and a parenthesized standalone `INTERVAL` are parse errors). `EXTRACT(unit FROM
//! expr)` ([`tidb_ast::Expr::Extract`] — its OWN grammar and AST shape,
//! `unit FROM value`, the opposite argument order from `INTERVAL`'s own
//! `value unit`; same broad-parse/narrower-eval split, any unit keyword
//! parses, only a subset evaluates), the `IN` / `BETWEEN` / `LIKE`
//! / `IS` predicates, `CASE [value] (WHEN cond THEN result)+ [ELSE
//! result] END` (`value` present is the simple form, absent the searched
//! form — one AST node, [`tidb_ast::Expr::Case`], covers both; at least
//! one `WHEN` clause is required, confirmed via `godump restore`: `CASE
//! END`/`CASE 1 END` are genuine parse errors), `CAST(expr AS type)` /
//! `CONVERT(expr, type)` / `CONVERT(expr USING charset)` (`crate::cast`
//! — a narrower, MySQL-specific target-type grammar than `crate::ddl`'s
//! own column types; see [`tidb_ast::CastType`]'s own doc for the exact
//! accepted subset and its several real, non-obvious restore
//! normalizations), subqueries (derived
//! tables, scalar/`IN`/`EXISTS`/`ANY`/`ALL`), and a `WITH [RECURSIVE]
//! name [(col, ...)] AS (query) [, ...]` clause before either a plain
//! `SELECT` ([`tidb_ast::SelectStmt::with`]) or an outer set operation
//! ([`tidb_ast::SetOprStmt::with`]). The same query representation keeps
//! that grammar compositional in derived/LATERAL tables and `IN`
//! subqueries; scalar/`EXISTS`/`ANY`/`ALL` slots still intentionally hold
//! only a plain `SelectStmt`. `RECURSIVE` parses so it doesn't silently
//! misparse as non-recursive; execution of a CTE-prefixed set operation
//! remains an explicit `Unsupported` boundary in `tidb-exec`, and window
//! functions ([`tidb_ast::Expr::Window`]) — scope: the zero-argument
//! ranking functions `ROW_NUMBER`/`RANK`/`DENSE_RANK`; the frame-based
//! window AGGREGATES `COUNT`/`SUM`/`AVG`/`MAX`/`MIN` (sharing
//! [`Expr::Aggregate`]'s own single-argument shape — `parse_aggregate`
//! itself detects a trailing `OVER` and dispatches to `Expr::Window`
//! instead); the "value function" family `FIRST_VALUE`/`LAST_VALUE`
//! (one argument), `NTH_VALUE` (two: value, then a 1-based position), and
//! `LAG`/`LEAD` (one to three: value, an optional offset, an optional
//! out-of-range default); and the "distribution function" family
//! `NTILE(n)` (one argument) and `PERCENT_RANK`/`CUME_DIST` (zero
//! arguments) — all with an INLINE window spec (`OVER (PARTITION BY
//! expr, ... ORDER BY expr [ASC|DESC], ... [ROWS ...])`, including an
//! explicit `ROWS BETWEEN <bound> AND <bound>` frame clause, or its
//! single-bound `ROWS <bound>` shorthand — normalized at parse time to
//! the full `BETWEEN` form, matching real TiDB's own restore, confirmed
//! via `godump`; see [`tidb_ast::WindowFrame`]'s own doc); a named window
//! reference (`OVER w`, `OVER (w ...)`) and the `WINDOW name AS (...),
//! ...` clause that defines it (see [`tidb_ast::WindowOver`]/
//! [`tidb_ast::WindowDef`] — resolving a name and validating what an
//! extension may add both happen in `tidb_exec`, not here, matching how
//! this parser accepts any unit keyword after `INTERVAL` syntactically
//! too); `DISTINCT` in the aggregate position (`MAX(DISTINCT x) OVER
//! (...)`), `IGNORE NULLS`/`FROM LAST` (real ANSI SQL grammar `LAG`/
//! `LEAD`/etc. can carry, but confirmed via `gorun` that real TiDB itself
//! rejects both unconditionally, so not parsing them matches real scope
//! exactly), and a `RANGE` frame clause (needs numeric-/interval-distance
//! comparison against the `ORDER BY` key's own value, a genuinely
//! different and larger problem than `ROWS`'s physical offsets) are all
//! genuine `ParseError`s, not silently misparsed or dropped. Unsupported
//! constructs return [`ParseError`] rather than guessing, so coverage
//! can be measured honestly.
//!
//! A `SELECT /*+ ... */` optimizer-hint comment (only recognized
//! directly after `SELECT`, matching `tidb_lexer`'s own
//! `HINTED_KEYWORDS` gate) is re-lexed and parsed by a NESTED sub-
//! `Parser` over the comment's own inner text (`crate::select::
//! parse_hint_comment`), reusing this crate's own token-cursor
//! primitives rather than a bespoke hint-only lexer — real TiDB's own
//! hint grammar has its own dedicated ~1200-line mini-parser
//! (`pkg/parser/hintparser.go`) covering roughly 30 distinct hint
//! shapes; this models only the four shapes (join/aggregate-pushdown
//! table-list hints, index hints, `SET_VAR`, argument-less hints)
//! confirmed — via a stratified sample of real TiDB's own
//! integration-test corpus — to cover the overwhelming majority of
//! real-world hint usage; see [`tidb_ast::Hint`]'s own doc for the exact
//! scope boundary.
//!
//! ## Module layout
//!
//! Split by concern so unrelated features can be extended without touching
//! the same file: `binding` (SQL binding commands), `ddl`
//! (`CREATE`/`ALTER`/`RENAME`/`DROP TABLE`), `dml` (`INSERT`/`UPDATE`/`DELETE`),
//! `resource_group` (the complete CREATE/ALTER/DROP resource-group source
//! domain), `privilege` (privilege `GRANT`/`REVOKE`), `show` (ordinary
//! metadata inspection), `user` (account-owned grammar),
//! `select` (`SELECT`/set operations/`FROM` join tree), `expr` (the
//! precedence-climbing expression parser and predicates), and `prec` (the
//! precedence-level constant table, already its own file before this split).
//! Each adds one or more
//! `impl Parser { ... }` blocks in its own file — same pattern as
//! `tidb-exec`'s `Database` split: Rust allows a type's methods to span as
//! many files as its crate likes, as long as the type stays visible. This
//! file keeps the shared vocabulary (`ParseError`, `Parser`'s struct
//! definition, the token-cursor primitives every other module's methods
//! call — `peek`/`bump`/`is_kw`/`expect_kw`/..., name-path parsing, and
//! `decode_string`, used by multiple domains), and the public [`parse`] /
//! [`parse_multi`] entry points. `statement` owns the source-ordered
//! top-level dispatch.
//! Tests mirror these source domains under `src/tests/`, so a vertical owner
//! does not contend on `tests/stmt.rs`.

mod admin;
mod analyze;
pub mod arena;
pub mod auth;
mod binding;
mod brie;
mod cast;
mod ddl;
mod digest;
mod dml;
mod expr;
mod flush;
mod load_data;
mod masking;
mod misc;
mod placement;
mod prec;
mod privilege;
mod procedure;
mod query_watch;
mod resource_group;
mod select;
mod sequence;
mod set;
mod show;
mod statement;
mod traffic;
mod user;

use tidb_ast::{
    AdminStmt, DdlStmt, DescribeTableStmt, ExplainForStmt, ExplainStmt, ExplainTarget, Expr,
    PlanReplayerStmt, PlanReplayerTarget, StatsLockStmt, StatsLockTable, Stmt,
};

#[allow(deprecated)]
pub use digest::{
    digest_hash, digest_normalized, normalize, normalize_digest, normalize_digest_for_binding,
    normalize_for_binding, normalize_keep_hint, Digest, RedactMode,
};
pub use select::{parse_hint, HintDiagnostic, HintParseResult};

/// One parsed statement plus recoverable diagnostics emitted while parsing it.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseOutput {
    /// Parsed AST node.
    pub statement: Stmt,
    /// Recoverable parser diagnostics, in source order.
    pub warnings: Vec<HintDiagnostic>,
}
pub use tidb_lexer::SqlMode;
use tidb_lexer::{
    is_builtin_function_keyword, is_reserved, unescape_char, Lexer, Token, TokenKind,
};

/// A parse failure with a human-readable reason and the byte offset at which it
/// occurred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// A description of what went wrong.
    pub message: String,
    /// The byte offset immediately after the unexpected token, matching the
    /// Go parser's yacc-compatible column boundary.
    pub offset: usize,
    /// Byte offset where Go's `near` excerpt begins.
    pub near_offset: usize,
}

impl ParseError {
    /// Renders the parser diagnostic using Go TiDB's observable error format.
    /// Semantic errors already carry their `[class:code]` prefix; ordinary
    /// grammar failures use the source location and remaining SQL excerpt.
    pub fn compatibility_message(&self, sql: &str) -> String {
        if self.message.starts_with('[') {
            return self.message.clone();
        }
        let near_offset = self.near_offset.min(sql.len());
        let offset = self.offset.min(sql.len());
        let line_start = sql.as_bytes()[..offset]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1);
        let line = sql.as_bytes()[..offset]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count()
            + 1;
        if sql[near_offset..].starts_with("/*") {
            let comment_line = sql.as_bytes()[..near_offset]
                .iter()
                .filter(|byte| **byte == b'\n')
                .count()
                + 1;
            return format!("near '{}' at line {comment_line}", &sql[near_offset..]);
        }
        let column = offset.saturating_sub(line_start);
        let mut near_end = (near_offset + 80).min(sql.len());
        while !sql.is_char_boundary(near_end) {
            near_end -= 1;
        }
        format!(
            "line {line} column {column} near \"{}\" ",
            &sql[near_offset..near_end]
        )
    }
}

type PResult<T> = Result<T, ParseError>;

/// Parses a single SQL statement (with an optional trailing `;`).
pub fn parse(sql: &str) -> PResult<Stmt> {
    parse_with_mariadb(sql, false)
}

/// Parses one statement without discarding recoverable parser warnings.
pub fn parse_with_warnings(sql: &str) -> PResult<ParseOutput> {
    let mut parser = Parser::new_with_configuration(sql, false, SqlMode::default());
    let statement = parse_one_with_parser(sql, &mut parser)?;
    Ok(ParseOutput {
        statement,
        warnings: parser.warnings,
    })
}

/// Parses one statement with the same MariaDB compatibility switch as Go's
/// parser configuration. The default [`parse`] API remains MySQL/TiDB-strict;
/// callers must opt in before `AS ROW START|END` becomes grammar.
pub fn parse_with_mariadb(sql: &str, enable_mariadb: bool) -> PResult<Stmt> {
    parse_with_configuration(sql, enable_mariadb, SqlMode::default())
}

/// Parses one statement under the scanner SQL-mode flags owned by the Rust
/// lexer. This is the direct counterpart of Go `Parser.SetSQLMode` for the
/// currently transcreated mode surface.
pub fn parse_with_sql_mode(sql: &str, sql_mode: SqlMode) -> PResult<Stmt> {
    parse_with_configuration(sql, false, sql_mode)
}

/// Parses one statement with Go's connection charset/collation metadata.
pub fn parse_with_connection(sql: &str, charset: &str, collation: &str) -> PResult<Stmt> {
    parse_with_full_configuration(sql, false, SqlMode::default(), charset, collation)
}

/// Parses one statement with Go's `Parser.EnableWindowFunc` scanner switch.
pub fn parse_with_window_functions(sql: &str, enabled: bool) -> PResult<Stmt> {
    let mut parser = Parser::new_with_all_configuration(
        sql,
        false,
        SqlMode::default(),
        tidb_mysql::DefaultCharset,
        tidb_mysql::DefaultCollationName,
        enabled,
    );
    parse_one_with_parser(sql, &mut parser)
}

fn parse_with_configuration(sql: &str, enable_mariadb: bool, sql_mode: SqlMode) -> PResult<Stmt> {
    parse_with_full_configuration(
        sql,
        enable_mariadb,
        sql_mode,
        tidb_mysql::DefaultCharset,
        tidb_mysql::DefaultCollationName,
    )
}

fn parse_with_full_configuration(
    sql: &str,
    enable_mariadb: bool,
    sql_mode: SqlMode,
    charset: &str,
    collation: &str,
) -> PResult<Stmt> {
    let mut p =
        Parser::new_with_full_configuration(sql, enable_mariadb, sql_mode, charset, collation);
    parse_one_with_parser(sql, &mut p)
}

fn parse_one_with_parser(sql: &str, p: &mut Parser) -> PResult<Stmt> {
    let start = p.peek().offset;
    let mut stmt = p.parse_statement()?;
    let end = if p.is_op(";") {
        p.bump().end_offset
    } else {
        p.peek().offset
    };
    if end > start {
        stmt.set_text(None, sql.as_bytes()[start..end].to_vec());
    }
    p.skip_semicolons();
    if !p.at_eof() {
        return Err(p.err_here("unexpected trailing tokens"));
    }
    Ok(stmt)
}

/// Parses every SQL statement in `sql`, preserving the source parser's
/// semicolon boundary.
///
/// TiDB's Go `Parser.Parse` API returns a slice because mysqltest inputs may
/// contain several statements on one physical line. The ordinary [`parse`]
/// entrypoint intentionally remains the strict one-statement API used by
/// callers that expect one AST node; this companion returns the complete
/// sequence instead of silently dropping trailing statements. Empty input and
/// runs containing only semicolons produce an empty vector, matching Go's
/// parser result for those inputs.
pub fn parse_multi(sql: &str) -> PResult<Vec<Stmt>> {
    parse_multi_with_mariadb(sql, false)
}

/// Multi-statement parser with the same MariaDB compatibility switch as
/// [`parse_with_mariadb`].
pub fn parse_multi_with_mariadb(sql: &str, enable_mariadb: bool) -> PResult<Vec<Stmt>> {
    parse_multi_with_configuration(sql, enable_mariadb, SqlMode::default())
}

/// Parses multiple statements under the scanner SQL-mode flags owned by the
/// Rust lexer.
pub fn parse_multi_with_sql_mode(sql: &str, sql_mode: SqlMode) -> PResult<Vec<Stmt>> {
    parse_multi_with_configuration(sql, false, sql_mode)
}

/// Parses SQL bytes using TiDB's `CharsetClient` decoding boundary.
///
/// This covers every client encoding with a dedicated Go parser
/// implementation: UTF-8/UTF8MB4, ASCII, Latin-1, binary, GBK, and GB18030.
pub fn parse_multi_bytes(sql: &[u8], client_charset: &str) -> PResult<Vec<Stmt>> {
    let decoded = decode_client_sql(sql, client_charset)?;
    parse_multi(&decoded)
}

/// Single-statement counterpart of [`parse_multi_bytes`].
pub fn parse_bytes(sql: &[u8], client_charset: &str) -> PResult<Stmt> {
    let decoded = decode_client_sql(sql, client_charset)?;
    parse(&decoded)
}

fn decode_client_sql<'a>(
    sql: &'a [u8],
    client_charset: &str,
) -> PResult<std::borrow::Cow<'a, str>> {
    match client_charset {
        "" | "binary" => Ok(String::from_utf8_lossy(sql)),
        "utf8" | "utf8mb3" | "utf8mb4" => std::str::from_utf8(sql)
            .map(std::borrow::Cow::Borrowed)
            .map_err(|_| ParseError {
                message: "invalid UTF-8 in client SQL".to_owned(),
                offset: 0,
                near_offset: 0,
            }),
        "ascii" if sql.is_ascii() => Ok(std::borrow::Cow::Borrowed(
            std::str::from_utf8(sql).expect("ASCII is UTF-8"),
        )),
        "ascii" => Err(ParseError {
            message: "invalid ASCII in client SQL".to_owned(),
            offset: 0,
            near_offset: 0,
        }),
        "latin1" => Ok(std::borrow::Cow::Owned(
            sql.iter().map(|byte| char::from(*byte)).collect(),
        )),
        "gbk" | "gb18030" => {
            let encoding = if client_charset == "gbk" {
                encoding_rs::GBK
            } else {
                encoding_rs::GB18030
            };
            // Go's scanner uses the client encoding to find character
            // boundaries, but it does not reject every malformed byte inside
            // a quoted token. Replacement decoding preserves that permissive
            // lexical contract while still preventing a GBK/GB18030 trail
            // byte `0x5c` from becoming a SQL backslash escape.
            Ok(encoding.decode_without_bom_handling(sql).0)
        }
        // `charset.FindEncoding` deliberately falls back to the binary
        // implementation for an unknown or differently-cased name.
        _ => Ok(String::from_utf8_lossy(sql)),
    }
}

fn parse_multi_with_configuration(
    sql: &str,
    enable_mariadb: bool,
    sql_mode: SqlMode,
) -> PResult<Vec<Stmt>> {
    let mut p = Parser::new_with_configuration(sql, enable_mariadb, sql_mode);
    let mut statements = Vec::new();
    let mut source_start = statement_source_start(sql, 0);
    while p.is_op(";") {
        source_start = p.bump().end_offset;
    }
    if !p.at_eof() {
        source_start = statement_source_start(sql, source_start);
    }
    while !p.at_eof() {
        p.reset_param_marker_positions();
        let mut statement = p.parse_statement()?;
        let had_delimiter = p.is_op(";");
        let end = if had_delimiter {
            p.bump().end_offset
        } else {
            p.peek().offset
        };
        if end > source_start {
            statement.set_text(None, sql.as_bytes()[source_start..end].to_vec());
        }
        statements.push(statement);
        if !had_delimiter && !p.at_eof() {
            return Err(p.err_here("expected ';' between statements"));
        }
        source_start = end;
        while p.is_op(";") {
            source_start = p.bump().end_offset;
        }
        if !p.at_eof() {
            source_start = statement_source_start(sql, source_start);
        }
    }
    Ok(statements)
}

fn statement_source_start(sql: &str, base: usize) -> usize {
    let prefix = &sql.as_bytes()[..base];
    let last_open = prefix.windows(3).rposition(|window| window == b"/*!");
    let last_close = prefix.windows(2).rposition(|window| window == b"*/");
    if last_open.is_some_and(|open| last_close.is_none_or(|close| open > close)) {
        return base;
    }
    let mut start = base;
    while sql
        .as_bytes()
        .get(start)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        start += 1;
    }
    start
}

struct Parser {
    source: String,
    toks: Vec<Token>,
    pos: usize,
    enable_mariadb: bool,
    param_marker_position: usize,
    connection_charset: String,
    connection_collation: String,
    real_as_float: bool,
    high_not_precedence: bool,
    ignore_space: bool,
    warnings: Vec<HintDiagnostic>,
}

impl Parser {
    #[cfg(test)]
    fn new(sql: &str) -> Self {
        Self::new_with_configuration(sql, false, SqlMode::default())
    }

    fn new_with_configuration(sql: &str, enable_mariadb: bool, sql_mode: SqlMode) -> Self {
        Self::new_with_full_configuration(
            sql,
            enable_mariadb,
            sql_mode,
            tidb_mysql::DefaultCharset,
            tidb_mysql::DefaultCollationName,
        )
    }

    fn new_with_full_configuration(
        sql: &str,
        enable_mariadb: bool,
        sql_mode: SqlMode,
        charset: &str,
        collation: &str,
    ) -> Self {
        Self::new_with_all_configuration(sql, enable_mariadb, sql_mode, charset, collation, true)
    }

    fn new_with_all_configuration(
        sql: &str,
        enable_mariadb: bool,
        sql_mode: SqlMode,
        charset: &str,
        collation: &str,
        support_window_functions: bool,
    ) -> Self {
        let mut lexer = Lexer::new(sql).with_sql_mode(sql_mode);
        lexer.set_support_window_func(support_window_functions);
        let (toks, lexer_warnings) = lexer.tokenize_with_warnings();
        Parser {
            source: sql.to_owned(),
            toks,
            pos: 0,
            enable_mariadb,
            param_marker_position: 0,
            connection_charset: charset.to_owned(),
            connection_collation: collation.to_owned(),
            real_as_float: sql_mode.real_as_float,
            high_not_precedence: sql_mode.high_not_precedence,
            ignore_space: sql_mode.ignore_space,
            warnings: lexer_warnings
                .into_iter()
                .map(|message| HintDiagnostic { message })
                .collect(),
        }
    }

    fn reset_param_marker_positions(&mut self) {
        self.param_marker_position = 0;
    }

    fn warn(&mut self, message: &'static str) {
        self.warnings.push(HintDiagnostic {
            message: message.to_owned(),
        });
    }

    fn next_param_marker_position(&mut self) -> usize {
        let position = self.param_marker_position;
        self.param_marker_position = self
            .param_marker_position
            .checked_add(1)
            .expect("parameter marker position overflowed usize");
        position
    }

    /// Constructs the nested parser for an optimizer-hint comment. Real
    /// TiDB uses a dedicated hint lexer, including a narrower query-block
    /// token boundary around dots.
    fn new_hint_with_ansi_quotes(sql: &str, ansi_quotes: bool) -> Self {
        Parser {
            source: sql.to_owned(),
            toks: Lexer::new(sql)
                .with_sql_mode(SqlMode {
                    ansi_quotes,
                    ..SqlMode::default()
                })
                .with_hint_mode()
                .tokenize(),
            pos: 0,
            enable_mariadb: false,
            param_marker_position: 0,
            connection_charset: tidb_mysql::DefaultCharset.to_owned(),
            connection_collation: tidb_mysql::DefaultCollationName.to_owned(),
            real_as_float: false,
            high_not_precedence: false,
            ignore_space: false,
            warnings: Vec::new(),
        }
    }

    fn source_line(&self, offset: usize) -> usize {
        self.source.as_bytes()[..offset.min(self.source.len())]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count()
            + 1
    }

    // ---- token cursor ----

    fn peek(&self) -> &Token {
        &self.toks[self.pos]
    }

    fn peek_n(&self, n: usize) -> &Token {
        let i = (self.pos + n).min(self.toks.len() - 1);
        &self.toks[i]
    }

    fn bump(&mut self) -> Token {
        let t = self.toks[self.pos].clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn at_eof(&self) -> bool {
        self.peek().kind == TokenKind::Eof
    }

    fn err_here(&self, msg: &str) -> ParseError {
        ParseError {
            message: msg.to_string(),
            offset: self.peek().end_offset,
            near_offset: self.peek().offset,
        }
    }

    fn skip_semicolons(&mut self) {
        while self.is_op(";") {
            self.bump();
        }
    }

    /// True if the current token is the keyword `kw` (case-insensitive).
    fn is_kw(&self, kw: &str) -> bool {
        let t = self.peek();
        t.kind == TokenKind::Keyword && t.text.eq_ignore_ascii_case(kw)
    }

    /// True if the token `n` ahead is the keyword `kw`.
    fn is_kw_at(&self, n: usize, kw: &str) -> bool {
        let t = self.peek_n(n);
        t.kind == TokenKind::Keyword && t.text.eq_ignore_ascii_case(kw)
    }

    /// True if the current token is the operator/punctuation `op`.
    fn is_op(&self, op: &str) -> bool {
        let t = self.peek();
        t.kind == TokenKind::Op && t.text == op
    }

    /// Consumes an `IF EXISTS` clause if present, returning whether it was.
    /// `EXISTS` is required after `IF` (matching real MySQL/TiDB grammar).
    fn parse_if_exists(&mut self) -> PResult<bool> {
        if self.is_kw("IF") {
            self.bump();
            self.expect_kw("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Consumes an `IF NOT EXISTS` clause if present. This is intentionally
    /// separate from `IF EXISTS`: the two clauses occur on different DDL
    /// statements and accepting one in place of the other would alter the
    /// statement's error-suppression contract.
    fn parse_if_not_exists(&mut self) -> PResult<bool> {
        if self.is_kw("IF") {
            self.bump();
            self.expect_kw("NOT")?;
            self.expect_kw("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Parses Go's `DO expr [, expr ...]` list. Each expression uses the
    /// regular lowest-precedence entrypoint: `DO` does not define a narrower
    /// expression grammar, and commas delimit only after a complete expression
    /// has been parsed.
    fn parse_do_stmt(&mut self) -> PResult<Vec<Expr>> {
        self.expect_kw("DO")?;
        let mut exprs = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            exprs.push(self.parse_expr(prec::NONE)?);
        }
        Ok(exprs)
    }

    /// Shared string-literal primitive. Source-owned callers choose the
    /// diagnostic; this helper owns no authentication or DDL policy.
    fn parse_string_literal(&mut self, message: &str) -> PResult<String> {
        if self.peek().kind != TokenKind::Str {
            return Err(self.err_here(message));
        }
        Ok(decode_string(&self.bump().text))
    }

    /// True if the token `n` positions ahead is the operator `op`.
    fn is_op_at(&self, n: usize, op: &str) -> bool {
        let t = self.peek_n(n);
        t.kind == TokenKind::Op && t.text == op
    }

    fn expect_op(&mut self, op: &str) -> PResult<()> {
        if self.is_op(op) {
            self.bump();
            Ok(())
        } else {
            let unexpected = self.bump();
            Err(ParseError {
                message: format!("expected '{op}'"),
                offset: unexpected.end_offset,
                near_offset: unexpected.offset,
            })
        }
    }

    fn expect_kw(&mut self, kw: &str) -> PResult<()> {
        if self.is_kw(kw) {
            self.bump();
            Ok(())
        } else {
            let unexpected = self.bump();
            Err(ParseError {
                message: format!("expected keyword {kw}"),
                offset: unexpected.end_offset,
                near_offset: unexpected.offset,
            })
        }
    }

    /// Parses the common target grammar for Go's distinct `LOCK STATS` and
    /// `UNLOCK STATS` statements. The optional partition list is attached to
    /// the final table and Go accepts both `PARTITION p0, p1` and
    /// `PARTITION (p0, p1)` spellings.
    fn parse_stats_lock(&mut self, locked: bool) -> PResult<StatsLockStmt> {
        self.expect_kw(if locked { "LOCK" } else { "UNLOCK" })?;
        self.expect_kw("STATS")?;
        let mut tables = vec![StatsLockTable {
            name: self.parse_table_name()?,
            partitions: Vec::new(),
        }];
        while self.is_op(",") {
            self.bump();
            tables.push(StatsLockTable {
                name: self.parse_table_name()?,
                partitions: Vec::new(),
            });
        }
        if self.is_kw("PARTITION") {
            self.bump();
            let parenthesized = if self.is_op("(") {
                self.bump();
                true
            } else {
                false
            };
            let target = tables
                .last_mut()
                .expect("LOCK STATS requires its first table before partitions");
            target.partitions.push(table_name_token_text(self.bump()));
            while self.is_op(",") {
                self.bump();
                target.partitions.push(table_name_token_text(self.bump()));
            }
            if parenthesized {
                self.expect_op(")")?;
            }
        }
        Ok(StatsLockStmt { tables })
    }

    /// Parses the source-backed `EXPLAIN` wrapper subset from
    /// `pkg/parser/set_explain_parser.go`: option grammar remains here while
    /// the inner statement reuses the ordinary statement parser. A bare
    /// table target is Go's shared `ShowColumns` fallback, not an explain
    /// wrapper, and therefore produces [`AdminStmt::DescribeTable`]. Go's other
    /// `ExplainStmt` branches have dedicated AST payloads and deliberately
    /// remain parse errors until they can be represented faithfully.
    fn parse_explain(&mut self) -> PResult<Stmt> {
        self.expect_kw("EXPLAIN")?;
        self.parse_explain_tail()
    }

    /// Parses the portion of Go's `parseExplainStmt` after its leading
    /// `EXPLAIN`/`DESC`/`DESCRIBE` token. Keeping that common tail literal is
    /// important: `DESC SELECT ...` is not a describe-table statement in
    /// TiDB, and Go restores it as an ordinary `EXPLAIN` wrapper.
    fn parse_explain_tail(&mut self) -> PResult<Stmt> {
        let explore = if self.is_kw("EXPLORE") {
            self.bump();
            if self.peek().kind == TokenKind::Str {
                let digest = decode_string(&self.bump().text);
                return Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Explain(
                    Box::new(ExplainStmt {
                        analyze: false,
                        format: String::new(),
                        target: ExplainTarget::ExploreDigest(digest),
                    }),
                ))));
            }
            if self.is_kw("REPLAYER") {
                self.bump();
                let token = self.bump();
                if token.kind != TokenKind::Str {
                    return Err(self.err_here("expected EXPLAIN EXPLORE replayer file"));
                }
                return Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Explain(
                    Box::new(ExplainStmt {
                        analyze: false,
                        format: String::new(),
                        target: ExplainTarget::ExploreReplayer(decode_string(&token.text)),
                    }),
                ))));
            }
            true
        } else {
            false
        };
        let analyze = if self.is_kw("ANALYZE") {
            self.bump();
            true
        } else {
            false
        };
        let mut format = "row".to_string();
        if self.is_kw("FORMAT") {
            self.bump();
            self.expect_op("=")?;
            let token = self.peek().clone();
            format = match token.kind {
                TokenKind::Str => {
                    self.bump();
                    decode_string(&token.text)
                }
                TokenKind::Ident | TokenKind::Keyword => {
                    self.bump();
                    token.text
                }
                _ => return Err(self.err_here("expected EXPLAIN format identifier or string")),
            };
        }

        if !explore && !analyze && self.is_kw("FOR") {
            self.bump();
            self.expect_kw("CONNECTION")?;
            let token = self.bump();
            let connection_id = token
                .text
                .parse::<u64>()
                .map_err(|_| self.err_here("expected connection ID"))?;
            return Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::ExplainFor(
                Box::new(ExplainForStmt {
                    format,
                    connection_id,
                }),
            ))));
        }

        if self.peek().kind == TokenKind::Str {
            let digest = decode_string(&self.bump().text);
            return Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Explain(
                Box::new(ExplainStmt {
                    analyze,
                    format,
                    target: ExplainTarget::PlanDigest(digest),
                }),
            ))));
        }

        // Go's default arm maps `EXPLAIN <table> [column]` to `SHOW COLUMNS`
        // and its `ExplainStmt.Restore` then emits `DESC ...`. It is not the
        // `TABLE <query>` result-set branch (whose leading TABLE keyword is
        // reserved and deliberately does not enter this path).
        if !explore && is_name_or_keyword(self.peek()) {
            return self.parse_describe_table();
        }
        let statement_start = self.peek().offset;
        let mut statement =
            if self.is_op("(") && self.is_op_at(1, "(") && self.is_kw_at(2, "VALUES") {
                Stmt::Query(tidb_ast::NodeBox::new(
                    self.parse_explain_parenthesized_values()?,
                ))
            } else {
                self.parse_statement()?
            };
        if explore {
            let statement_end = if self.at_eof() {
                self.source.len()
            } else {
                self.peek().offset
            };
            if statement_end > statement_start {
                statement.set_text(
                    None,
                    self.source[statement_start..statement_end]
                        .trim()
                        .as_bytes()
                        .to_vec(),
                );
            }
        }
        if !(matches!(&statement, Stmt::Query(_) | Stmt::Dml(_))
            || matches!(
                &statement,
                Stmt::Ddl(ddl) if matches!(ddl.as_ref(), DdlStmt::AlterTable(_))
            ))
        {
            return Err(self.err_here("unsupported EXPLAIN inner statement"));
        }
        Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Explain(
            Box::new(ExplainStmt {
                analyze,
                format,
                target: if explore {
                    ExplainTarget::ExploreStatement(Box::new(statement))
                } else {
                    ExplainTarget::Statement(Box::new(statement))
                },
            }),
        ))))
    }

    /// Parses the complete Plan Replayer operation family.
    fn parse_plan_replayer_dump_explain(&mut self) -> PResult<Stmt> {
        self.expect_kw("PLAN")?;
        self.expect_kw("REPLAYER")?;
        if self.is_kw("LOAD") {
            self.bump();
            let token = self.bump();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected PLAN REPLAYER file"));
            }
            return Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::PlanReplayer(Box::new(PlanReplayerStmt::Load(decode_string(
                    &token.text,
                )))),
            )));
        }
        if self.is_kw("CAPTURE") {
            self.bump();
            let remove = if self.is_kw("REMOVE") {
                self.bump();
                true
            } else {
                false
            };
            let sql_digest = self.bump();
            let plan_digest = self.bump();
            if sql_digest.kind != TokenKind::Str || plan_digest.kind != TokenKind::Str {
                return Err(self.err_here("expected SQL and plan digest strings"));
            }
            return Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::PlanReplayer(Box::new(PlanReplayerStmt::Capture {
                    remove,
                    sql_digest: decode_string(&sql_digest.text),
                    plan_digest: decode_string(&plan_digest.text),
                })),
            )));
        }
        if self.is_kw("DUMP") {
            self.bump();
        }
        let historical_stats = if self.is_kw("WITH") {
            self.bump();
            self.expect_kw("STATS")?;
            self.expect_kw("AS OF")?;
            self.expect_kw("TIMESTAMP")?;
            Some(Box::new(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        self.expect_kw("EXPLAIN")?;
        let analyze = if self.is_kw("ANALYZE") {
            self.bump();
            true
        } else {
            false
        };
        let target = if self.peek().kind == TokenKind::Str {
            PlanReplayerTarget::File(decode_string(&self.bump().text))
        } else if self.is_op("(") && self.peek_n(1).kind == TokenKind::Str {
            self.bump();
            let mut statements = Vec::new();
            loop {
                let token = self.bump();
                if token.kind != TokenKind::Str {
                    return Err(self.err_here("expected SQL string"));
                }
                statements.push(decode_string(&token.text));
                if !self.is_op(",") {
                    break;
                }
                self.bump();
            }
            self.expect_op(")")?;
            PlanReplayerTarget::Statements(statements)
        } else if self.is_kw("SLOW") {
            self.parse_slow_query_target()?
        } else if self.is_kw("WITH") {
            let start = self.peek().offset;
            let mut statement = Stmt::Query(tidb_ast::NodeBox::new(self.parse_with_select()?));
            let end = if self.at_eof() {
                self.source.len()
            } else {
                self.peek().offset
            };
            if end > start {
                statement.set_text(None, self.source.as_bytes()[start..end].to_vec());
            }
            PlanReplayerTarget::Statement(Box::new(statement))
        } else {
            let start = self.peek().offset;
            let mut statement = self.parse_statement()?;
            let end = if self.at_eof() {
                self.source.len()
            } else {
                self.peek().offset
            };
            if end > start {
                statement.set_text(None, self.source.as_bytes()[start..end].to_vec());
            }
            PlanReplayerTarget::Statement(Box::new(statement))
        };
        Ok(Stmt::Admin(tidb_ast::NodeBox::new(
            AdminStmt::PlanReplayer(Box::new(PlanReplayerStmt::Dump {
                historical_stats,
                analyze,
                target: Box::new(target),
            })),
        )))
    }

    /// Direct translation of `admin_query_parser.go::parseSlowQueryStmt`.
    /// The same target is accepted both after PLAN REPLAYER DUMP EXPLAIN and
    /// as a top-level SLOW QUERY statement, whose Go AST is the zero-value
    /// PlanReplayer container and therefore restores with the full prefix.
    pub(crate) fn parse_slow_query_statement(&mut self) -> PResult<Stmt> {
        let target = self.parse_slow_query_target()?;
        Ok(Stmt::Admin(tidb_ast::NodeBox::new(
            AdminStmt::PlanReplayer(Box::new(PlanReplayerStmt::Dump {
                historical_stats: None,
                analyze: false,
                target: Box::new(target),
            })),
        )))
    }

    fn parse_slow_query_target(&mut self) -> PResult<PlanReplayerTarget> {
        self.expect_kw("SLOW")?;
        self.expect_kw("QUERY")?;
        let where_clause = if self.is_kw("WHERE") {
            self.bump();
            Some(Box::new(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        let order_by = if self.is_kw("ORDER") {
            self.bump();
            self.expect_kw("BY")?;
            self.parse_order_list()?
        } else {
            Vec::new()
        };
        let limit = if self.is_kw("LIMIT") {
            self.bump();
            Some(Box::new(self.parse_limit()?))
        } else {
            None
        };
        Ok(PlanReplayerTarget::SlowQuery {
            where_clause,
            order_by,
            limit,
        })
    }

    /// Parses the common `DESC`/`DESCRIBE` and `EXPLAIN <table>` fallback
    /// shape from Go's `parseExplainStmt`. `TABLE` is deliberately not
    /// consumed here: Go routes that reserved keyword through the distinct
    /// `EXPLAIN TABLE <query>` production, which this seed does not model.
    fn parse_describe_table(&mut self) -> PResult<Stmt> {
        if !is_name_or_keyword(self.peek()) {
            return Err(self.err_here("expected table name after DESC"));
        }
        let table = self.parse_name_path()?;
        let column = if is_name_or_keyword(self.peek()) {
            Some(self.parse_name_path()?)
        } else {
            None
        };
        Ok(Stmt::Admin(tidb_ast::NodeBox::new(
            AdminStmt::DescribeTable(Box::new(DescribeTableStmt { table, column })),
        )))
    }

    /// Parses a dotted name path from Go's wider `isIdentLike` slots. This
    /// neutral primitive is shared by standalone FLUSH tables and ADMIN SHOW
    /// NEXT_ROW_ID; neither domain owns the other's grammar.
    fn parse_ident_like_name_path(&mut self) -> PResult<Vec<String>> {
        let mut path = vec![self.parse_ident_like_name()?];
        while self.is_op(".") {
            self.bump();
            path.push(self.parse_ident_like_name()?);
        }
        Ok(path)
    }

    fn parse_ident_like_name(&mut self) -> PResult<String> {
        if is_ident_like_name(self.peek()) {
            Ok(table_name_token_text(self.bump()))
        } else {
            Err(self.err_here("expected an identifier-like name"))
        }
    }

    fn is_ident_like_name(&self) -> bool {
        is_ident_like_name(self.peek())
    }

    /// Consumes the next token's Go `yySymType.ident` payload without an
    /// identifier-class check. A few legacy rename tails deliberately accept
    /// literals and even EOF, whose empty payload means no restored rename.
    fn parse_any_token_name(&mut self) -> String {
        table_name_token_text(self.bump())
    }

    /// Go's `Token.IsKeyword` compares the decoded token literal regardless
    /// of lexical token kind. Legacy grammar slots therefore accept quoted
    /// strings, back-quoted identifiers, and single-@ names when their payload
    /// spells the requested word.
    fn token_literal_is_at(&self, offset: usize, expected: &str) -> bool {
        token_literal_text(self.peek_n(offset)).eq_ignore_ascii_case(expected)
    }

    fn expect_token_literal(&mut self, expected: &str) -> PResult<()> {
        if !self.token_literal_is_at(0, expected) {
            return Err(self.err_here(&format!("expected {expected}")));
        }
        self.bump();
        Ok(())
    }

    /// Go `peekKeyword` accepts the dedicated keyword token or an ordinary
    /// identifier with the same decoded literal, but not strings/user vars.
    fn keyword_or_ident_is_at(&self, offset: usize, expected: &str) -> bool {
        let token = self.peek_n(offset);
        matches!(token.kind, TokenKind::Keyword | TokenKind::Ident)
            && token_literal_text(token).eq_ignore_ascii_case(expected)
    }

    fn expect_keyword_or_ident(&mut self, expected: &str) -> PResult<()> {
        if !self.keyword_or_ident_is_at(0, expected) {
            return Err(self.err_here(&format!("expected {expected}")));
        }
        self.bump();
        Ok(())
    }

    /// The default identifier-driven grammar arms first require Go's
    /// `isIdentLike`, then dispatch on the decoded literal.
    fn ident_like_literal_is_at(&self, offset: usize, expected: &str) -> bool {
        let token = self.peek_n(offset);
        is_ident_like_name(token) && token_literal_text(token).eq_ignore_ascii_case(expected)
    }

    fn parse_ident_like_name_list(&mut self) -> PResult<Vec<String>> {
        let mut names = vec![self.parse_ident_like_name()?];
        while self.is_op(",") {
            self.bump();
            names.push(self.parse_ident_like_name()?);
        }
        Ok(names)
    }

    /// Go `isIdentLike` slot with the explicit `stringLit` exclusion used by
    /// partition-definition and DROP STATS partition names. User variables
    /// keep their decoded identifier payload; ordinary/non-reserved keyword
    /// names reuse the scanner-context-aware name parser.
    fn parse_non_string_ident_like_name(&mut self) -> PResult<String> {
        if self.peek().kind == TokenKind::UserVar {
            self.parse_ident_like_name()
        } else {
            self.parse_name_or_keyword()
        }
    }

    // ---- names ----

    /// Parses a single identifier name (its decoded text).
    fn parse_name(&mut self) -> PResult<String> {
        if self.peek().kind == TokenKind::Ident {
            Ok(normalize_identifier(self.bump().text))
        } else {
            Err(self.err_here("expected identifier"))
        }
    }

    /// Parses a single identifier name, ALSO accepting a non-reserved
    /// keyword (real MySQL/TiDB grammar: a non-reserved keyword — e.g.
    /// `UUID`, `STATUS` — is usable as a bare table/column name anywhere
    /// the grammar isn't otherwise ambiguous there, confirmed via `godump
    /// restore`: `SELECT uuid FROM t` parses). Used for table/column name
    /// PATHS ([`Parser::parse_name_path`]) specifically — NOT a universal
    /// replacement for [`Parser::parse_name`]: at least one confirmed
    /// exception exists (`PARTITION (name, ...)`'s own names are PLAIN
    /// identifiers ONLY, see [`Parser::parse_partition_opt`]'s own doc),
    /// so other identifier positions keep using the stricter
    /// `parse_name` until individually confirmed to share this same
    /// broader acceptance.
    fn parse_name_or_keyword(&mut self) -> PResult<String> {
        let t = self.peek();
        if t.kind == TokenKind::Ident || (t.kind == TokenKind::Keyword && !is_reserved(&t.text)) {
            Ok(normalize_identifier(self.bump().text))
        } else {
            Err(self.err_here("expected identifier"))
        }
    }

    /// Consumes and returns an identifier if one is next, else consumes
    /// nothing and returns `None` — for an optional name ahead of a
    /// non-identifier token (`(`, a keyword, ...).
    fn try_parse_name(&mut self) -> PResult<Option<String>> {
        if self.peek().kind == TokenKind::Ident {
            Ok(Some(self.parse_name()?))
        } else {
            Ok(None)
        }
    }

    /// Parses a charset or collation name, which — unlike most names — may
    /// lex as a keyword rather than a plain identifier (`BINARY`, `ASCII`
    /// are both charset names and lexer keywords).
    fn parse_charset_name(&mut self) -> PResult<String> {
        match self.peek().kind {
            TokenKind::Ident | TokenKind::Keyword => Ok(self.bump().text),
            _ => Err(self.err_here("expected a charset/collation name")),
        }
    }

    /// Parses the charset name after `USING` in `CONVERT(expr USING
    /// charset)` / `CHAR(... USING charset)`, and (as of the `COLLATE`
    /// fix below) a `COLLATE` clause's own collation name too — a
    /// genuinely MORE permissive grammar than
    /// [`Parser::parse_charset_name`]'s own broader-but-still-restricted
    /// Ident-or-Keyword acceptance: real TiDB's own
    /// `parseConvertFunc`/`parseCharFuncCall`/`parseCollateExpr` ALL do
    /// `tok := p.next()` — literally ANY next token, using its raw text
    /// — so a QUOTED STRING LITERAL is ALSO valid in every one of these
    /// three positions, confirmed via `godump restore`
    /// (`CONVERT(expr USING "binary")` restores identically to the bare-
    /// identifier form; `expr COLLATE 'binary'` restores as `COLLATE
    /// binary`, identically to the bare-identifier form too — same
    /// lowercased, unquoted restore regardless of which form was
    /// written). Every OTHER call site of `parse_charset_name` (hint
    /// names, DDL charset clauses) is NOT confirmed to accept a string
    /// literal and routes through dedicated, narrower grammar in real
    /// TiDB, so this widening stays scoped to these three call sites,
    /// not applied to `parse_charset_name` itself.
    fn parse_using_charset_name(&mut self) -> PResult<String> {
        if self.peek().kind == TokenKind::Str {
            return Ok(decode_string(&self.bump().text));
        }
        self.parse_charset_name()
    }

    /// Parses an optional `PARTITION (name, ...)` clause (an EMPTY name
    /// list is a genuine `ParseError`, at least one is required), or
    /// returns an empty `Vec` if the `PARTITION` keyword isn't next —
    /// shared by `crate::select`'s own `parse_table_ref` (a `SELECT`/
    /// `UPDATE`/`DELETE` table reference) and `crate::dml`'s own
    /// `INSERT` target-table parsing (confirmed via `godump restore`
    /// that both accept it). Names are PLAIN identifiers only, unlike
    /// [`Parser::parse_charset_name`]'s own broader Ident-or-Keyword
    /// acceptance (see [`tidb_ast::TableRef::partitions`]'s own doc for
    /// the confirming probe).
    fn parse_partition_opt(&mut self) -> PResult<Vec<String>> {
        if !self.is_kw("PARTITION") {
            return Ok(Vec::new());
        }
        self.bump();
        self.expect_op("(")?;
        let mut names = vec![self.parse_name()?];
        while self.is_op(",") {
            self.bump();
            names.push(self.parse_name()?);
        }
        self.expect_op(")")?;
        Ok(names)
    }

    /// Parses a dotted name path `a`, `a.b`, `a.b.c`. Each segment may be a
    /// non-reserved keyword (see [`Parser::parse_name_or_keyword`]'s own
    /// doc) — e.g. `t.uuid` is real, valid MySQL grammar.
    fn parse_name_path(&mut self) -> PResult<Vec<String>> {
        let mut path = vec![self.parse_name_or_keyword()?];
        while self.is_op(".") && is_name_or_keyword(self.peek_n(1)) {
            // A ColumnName has at most schema.table.column components in
            // TiDB's Go AST. Rejecting a fourth component here keeps the
            // parser's unsupported boundary explicit instead of silently
            // constructing an AST shape that Go cannot restore.
            if path.len() == 3 {
                return Err(self.err_here("name path has too many components"));
            }
            self.bump(); // '.'
            path.push(self.parse_name_or_keyword()?);
        }
        Ok(path)
    }

    /// Direct translation of `join_parser.go::parseTableName`, the shared
    /// `[schema.]table` production used by statement-level table lists. It is
    /// intentionally different from a generic column-name path: it has at
    /// most two components, accepts quoted/user-variable/charset tokens in
    /// the first slot, and uses the Go parser's broader second-slot boundary.
    pub(crate) fn parse_table_name(&mut self) -> PResult<Vec<String>> {
        if self.is_op("*") && self.is_op_at(1, ".") {
            self.bump();
            self.bump();
            let token = self.bump();
            return Ok(vec!["*".to_owned(), table_name_token_text(token)]);
        }

        let token = self.bump();
        if !is_table_name_first_token(&token) {
            return Err(self.err_here("expected table name"));
        }
        let first = table_name_token_text(token);
        if self.is_op(".") && !self.is_op_at(1, "*") {
            self.bump();
            let second = self.bump();
            if matches!(
                second.kind,
                TokenKind::Eof | TokenKind::Invalid | TokenKind::Op | TokenKind::HintComment
            ) {
                return Err(self.err_here("expected table name after schema"));
            }
            if first.trim().is_empty() {
                return Err(self.err_here("incorrect database name"));
            }
            Ok(vec![first, table_name_token_text(second)])
        } else {
            Ok(vec![first])
        }
    }

    /// Direct translation of `parser_helpers.go::parseColumnName` for AST
    /// slots that require a column name rather than a generic dotted path.
    pub(crate) fn parse_column_name_path(&mut self) -> PResult<Vec<String>> {
        let first = self.bump();
        if first.kind == TokenKind::Str || !is_ident_like_name(&first) {
            return Err(self.err_here("expected column name"));
        }
        let mut path = vec![table_name_token_text(first)];
        while self.is_op(".") {
            if path.len() == 3 {
                return Err(self.err_here("column name has too many components"));
            }
            self.bump();
            if self.is_op("*") {
                self.bump();
                path.push("*".to_owned());
                return Ok(path);
            }
            let component = self.bump();
            if !is_ident_like_name(&component) {
                return Err(self.err_here("expected column name component"));
            }
            path.push(table_name_token_text(component));
        }
        Ok(path)
    }
}

/// Whether `t` is acceptable to [`Parser::parse_name_or_keyword`]: a plain
/// identifier, or a non-reserved keyword.
fn is_name_or_keyword(t: &Token) -> bool {
    t.kind == TokenKind::Ident || (t.kind == TokenKind::Keyword && !is_reserved(&t.text))
}

fn normalize_identifier(name: String) -> String {
    name.replace('\u{fffd}', "?")
}

fn table_name_token_text(token: Token) -> String {
    match token.kind {
        TokenKind::Str => decode_string(&token.text),
        TokenKind::UserVar if token.text.starts_with("@@") => token.text,
        TokenKind::UserVar => decode_at_name(&token.text),
        _ => normalize_identifier(token.text),
    }
}

fn token_literal_text(token: &Token) -> String {
    match token.kind {
        TokenKind::Str => decode_string(&token.text),
        TokenKind::UserVar if token.text.starts_with("@@") => token.text.clone(),
        TokenKind::UserVar => decode_at_name(&token.text),
        _ => normalize_identifier(token.text.clone()),
    }
}

fn is_table_name_first_token(token: &Token) -> bool {
    match token.kind {
        TokenKind::Ident | TokenKind::Str | TokenKind::UserVar | TokenKind::CharsetIntroducer => {
            true
        }
        TokenKind::Keyword => !is_reserved(&token.text),
        _ => false,
    }
}

/// A Go `isIdentLike` slot accepts both identifier and keyword tokens.
fn is_ident_like_name(t: &Token) -> bool {
    match t.kind {
        TokenKind::Ident | TokenKind::Str | TokenKind::UserVar => true,
        TokenKind::Keyword => !is_reserved(&t.text) && !is_builtin_function_keyword(&t.text),
        _ => false,
    }
}

/// Decodes the payload of a raw `UserVar` token (the lexer scans `@name`,
/// `@'name'`, and `` @`name` `` as one token). Strips the leading `@`, then
/// decodes a quoted (`'`/`"`) string or a back-quoted identifier, or returns a
/// bare payload verbatim. Account hosts, prepared-statement parameters, and
/// binding values all share this lexical primitive.
fn decode_at_name(raw: &str) -> String {
    let rest = raw.strip_prefix('@').unwrap_or(raw);
    match rest.as_bytes().first() {
        Some(b'\'') | Some(b'"') => decode_string(rest),
        Some(b'`') => rest.trim_matches('`').replace("``", "`"),
        _ => rest.to_string(),
    }
}

/// Decodes a string-literal token's raw source (including delimiters) into its
/// logical value: outer quotes removed, doubled delimiters collapsed, and the
/// common backslash escapes resolved (matching the scanner's `handleEscape`).
/// Shared by `crate::expr` (string literals) and `crate::ddl` (`COMMENT`
/// text).
fn decode_string(raw: &str) -> String {
    let bytes = raw.as_bytes();
    if bytes.len() < 2 {
        return raw.to_string();
    }
    let quote = bytes[0];
    let inner = &bytes[1..bytes.len() - 1];
    let mut out = Vec::with_capacity(inner.len());
    let mut offset = 0;
    while offset < inner.len() {
        let byte = inner[offset];
        if byte == quote {
            // A doubled delimiter collapses to one.
            if inner.get(offset + 1) == Some(&quote) {
                offset += 1;
            }
            out.push(byte);
        } else if byte == b'\\' {
            if let Some(&escaped) = inner.get(offset + 1) {
                out.extend(unescape_char(escaped));
                offset += 1;
            }
        } else {
            out.push(byte);
        }
        offset += 1;
    }
    String::from_utf8(out).expect("unescaping valid UTF-8 SQL preserves valid UTF-8")
}

#[cfg(test)]
mod tests;
