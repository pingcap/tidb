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

//! `SELECT` (select list, `FROM` join tree, `WHERE`/`GROUP BY`/`HAVING`/
//! `ORDER BY`/`LIMIT`/a locking clause) and set operations (`UNION`/
//! `EXCEPT`/`INTERSECT`, with parenthesized terms and statement-level
//! `ORDER BY`/`LIMIT`/locking clause). Called from
//! `crate::Parser::parse_statement`, and from `crate::expr` for
//! subqueries (`parse_query_subquery`).
//!
//! `ORDER BY`/`LIMIT`/the locking clause (`Parser::parse_order_limit_lock`)
//! parse in ANY relative order — a real MySQL/TiDB grammar flexibility,
//! confirmed via `godump restore`: `LIMIT 1 ORDER BY a` and `FOR UPDATE
//! ORDER BY a` both parse, each restoring in a FIXED canonical order
//! regardless of how they were written (see [`tidb_ast::SelectStmt::lock`]'s
//! own doc for the two different canonical orders a plain `SELECT` vs. a
//! set operation's own statement-level tail use). For a set operation,
//! this tail attaches differently depending on WHICH term it trails and
//! whether it's `ORDER BY`/`LIMIT` or the locking clause — a genuinely
//! surprising asymmetry, confirmed via `godump restore` rather than
//! assumed uniform, not discovered until the differential corpus caught a
//! real regression while this was being implemented:
//! - The FIRST term's own trailing `ORDER BY`/`LIMIT`/lock (disambiguated
//!   by a following set operator) all stay together as that term's own —
//!   `Parser::parse_select_or_setopr`'s own eager tail-parse handles this.
//! - The LAST term's trailing tail (no following operator left to
//!   disambiguate it) is the WHOLE statement's own, REGARDLESS of whether
//!   that last term was parenthesized (confirmed: `(SELECT a FROM t1)
//!   UNION ALL (SELECT a FROM t2) ORDER BY 1 LIMIT 10`, both terms
//!   parenthesized, still has a real statement-level tail after the last
//!   one) — `Parser::parse_setopr_rest`'s own loop handles this.
//! - Any MIDDLE term (non-first, non-last) can have its own locking
//!   clause (confirmed: `t1 UNION t2 FOR UPDATE UNION t3` keeps `FOR
//!   UPDATE` attached to `t2`) — but its `ORDER BY`/`LIMIT` NEVER attach
//!   per-term; they ALWAYS become the whole statement's own instead
//!   (confirmed: `t1 UNION t2 ORDER BY x UNION t3` restores with `ORDER
//!   BY x` moved all the way to the very end, after `t3`) — a real
//!   asymmetry between the locking clause and `ORDER BY`/`LIMIT` for a
//!   non-first term specifically, also handled in
//!   `Parser::parse_setopr_rest`'s own loop.
//!
//! A sole parenthesized statement folds outer `ORDER BY`/`LIMIT` into the
//! inner query while retaining `SelectStmt::is_in_braces`. TiDB rejects a
//! locking clause or `INTO OUTFILE` at that outer position.
//!
//! A SOLE parenthesized set operation (`(SELECT ... UNION SELECT ...)`) is
//! represented separately by [`tidb_ast::SetOprStmt::is_in_braces`]. Its
//! statement-level tail is restored before the closing parenthesis, matching
//! TiDB's `SetOprStmt.IsInBraces` behavior.
//!
//! Implementing the locking clause surfaced a genuine, PRE-EXISTING
//! parser bug, unrelated to locking itself and fixed in the same pass: a
//! bare (unparenthesized) NON-FINAL term's own trailing `ORDER BY`/
//! `LIMIT` was never parsed at all (`SELECT a FROM t ORDER BY a UNION
//! SELECT b FROM t2` failed with "unexpected trailing tokens" even
//! though real TiDB accepts it) — the old code only ever attempted tail
//! parsing on the WHOLE first term once it was already known there was
//! no following set operator, too late to matter for this shape.

use tidb_ast::{
    Cte, Expr, Hint, HintKind, HintTable, Join, JoinNode, JoinType, LeadingElement, Limit,
    OrderItem, QueryStmt, SampleMethod, SampleUnit, SelectField, SelectStatementKind, SelectStmt,
    SetOp, SetOprStmt, SetOprTerm, SetOprTermBody, TableRef, TableSample, UnaryOp, WindowDef,
    WithClause,
};
use tidb_lexer::TokenKind;

use crate::{is_name_or_keyword, prec, PResult, Parser};

mod hint;
mod join;

pub(crate) use hint::parse_hint_comment;
pub use hint::*;

impl Parser {
    /// Parses `WITH [RECURSIVE] name [(col, ...)] AS (query) [, ...]
    /// <select>`. Each CTE's own `query` may itself be a nested `WITH` or a
    /// `UNION`/`UNION ALL`-joined set operation (`parse_select_or_setopr`,
    /// needed for `WITH RECURSIVE`'s `base UNION [ALL] recursive` shape —
    /// see [`tidb_ast::Cte`]'s own doc). The outer query may be either a
    /// plain `SELECT` or a set operation; TiDB owns that prefix on the
    /// outer query node, not on its first term.
    pub(crate) fn parse_with_select(&mut self) -> PResult<QueryStmt> {
        let with = self.parse_with_clause()?;
        self.attach_with_to_query(with)
    }

    /// Parses the CTE prefix shared by top-level query and DML statements.
    /// Go attaches identical `WithClause` data to either owner; parsing it
    /// once avoids a second, subtly different grammar for mutation prefixes.
    pub(crate) fn parse_with_clause(&mut self) -> PResult<WithClause> {
        self.expect_kw("WITH")?;
        let recursive = if self.is_kw("RECURSIVE") {
            self.bump();
            true
        } else {
            false
        };
        let mut ctes = Vec::new();
        loop {
            // Go's `parseWithStmt` accepts `isIdentLike` names here (then
            // rejects only reserved words), so non-reserved lexer keywords
            // such as the source-backed `level` CTE column are valid. This
            // is the same rule used by ordinary table/column name paths;
            // keeping the CTE prefix on that shared primitive avoids a
            // narrower grammar at this one source boundary.
            let name = self.parse_name_or_keyword()?;
            let mut columns = Vec::new();
            if self.is_op("(") {
                self.bump();
                columns.push(self.parse_name_or_keyword()?);
                while self.is_op(",") {
                    self.bump();
                    columns.push(self.parse_name_or_keyword()?);
                }
                self.expect_op(")")?;
            }
            self.expect_kw("AS")?;
            self.expect_op("(")?;
            let query_start = self.peek().offset;
            // Go's CTE production delegates the body to the general
            // subquery parser, so another WITH may start this body. The
            // existing QueryStmt/WithClause representation already owns
            // that nested query losslessly; only this parser seam was
            // narrower than the source grammar.
            let query = if self.is_kw("WITH") {
                self.parse_with_select()?
            } else {
                self.parse_select_or_setopr()?
            };
            let query_end = self.peek().offset;
            let mut query = tidb_ast::NodeBox::new(query);
            if query_end > query_start {
                query.set_text(
                    None,
                    self.source.as_bytes()[query_start..query_end].to_vec(),
                );
            }
            self.expect_op(")")?;
            ctes.push(Cte {
                name,
                columns,
                query,
            });
            if self.is_op(",") {
                self.bump();
                continue;
            }
            break;
        }
        Ok(WithClause { recursive, ctes })
    }

    pub(crate) fn attach_with_to_query(&mut self, with: WithClause) -> PResult<QueryStmt> {
        // A parenthesized outer query after WITH keeps its own braces on the
        // query node. Derived-table parsing owns its wrapper separately, so
        // this must not be folded into the generic parenthesized-term path.
        if self.starts_parenthesized_query() {
            self.bump();
            let query = if self.is_kw("WITH") {
                self.parse_with_select()?
            } else {
                self.parse_select_or_setopr()?
            };
            self.expect_op(")")?;
            return Ok(match query {
                QueryStmt::Select(mut select) => {
                    select.with = Some(with);
                    select.is_in_braces = true;
                    QueryStmt::Select(select)
                }
                QueryStmt::SetOpr(mut setopr) => {
                    setopr.with = Some(with);
                    setopr.is_in_braces = true;
                    QueryStmt::SetOpr(setopr)
                }
            });
        }
        match self.parse_select_or_setopr()? {
            QueryStmt::Select(mut sel) => {
                sel.with = Some(with);
                Ok(QueryStmt::Select(sel))
            }
            QueryStmt::SetOpr(mut setopr) => {
                setopr.with = Some(with);
                Ok(QueryStmt::SetOpr(setopr))
            }
        }
    }

    /// Parses a `SELECT` and any trailing set operations (`UNION`/`EXCEPT`/
    /// `INTERSECT`), returning either a `Select` or a `SetOpr` statement.
    pub(crate) fn parse_select_or_setopr(&mut self) -> PResult<QueryStmt> {
        let (first_braces, mut first) = self.parse_one_term()?;
        if !first_braces {
            // A bare (non-parenthesized) first term is always `Select` —
            // `parse_one_term` only ever produces `Nested` from its own
            // parenthesized branch.
            let SetOprTermBody::Select(sel) = &mut first else {
                unreachable!("a bare first term is always Select")
            };
            // A BARE term's own tail must be parsed EAGERLY, before
            // checking for a following set operator below, so that
            // operator becomes visible afterward (confirmed via `godump
            // restore`: `t1 ORDER BY a FOR UPDATE UNION t2` restores
            // with `ORDER BY a FOR UPDATE` immediately after `t1`, so
            // that tail must be consumed as `t1`'s own before `UNION`
            // can be seen). A PARENTHESIZED term does NOT need this same
            // eager parse — its own closing `)` already unambiguously
            // ends the term.
            let (order_by, limit, lock) = self.parse_order_limit_lock()?;
            sel.order_by = order_by;
            sel.limit = limit;
            sel.lock = lock;
        }
        if self.peek_set_op().is_some() {
            return Ok(QueryStmt::SetOpr(Box::new(
                self.parse_setopr_rest(first_braces, first)?,
            )));
        }
        match first {
            SetOprTermBody::Select(mut sel) => {
                if first_braces {
                    let (order_by, limit, lock) = self.parse_order_limit_lock()?;
                    if lock.is_some() {
                        return Err(self.err_here(
                            "a parenthesized SELECT cannot carry an outer locking clause",
                        ));
                    }
                    sel.order_by = order_by;
                    sel.limit = limit;
                    sel.is_in_braces = true;
                } else {
                    if self.is_kw("INTO") && {
                        let after = self.peek_n(1);
                        after.kind == TokenKind::UserVar && !after.text.starts_with("@@")
                    } {
                        self.bump();
                        sel.into_vars = self.parse_into_user_vars()?;
                    } else {
                        sel.into_outfile = self.parse_opt_into_outfile()?;
                    }
                }
                Ok(QueryStmt::Select(sel))
            }
            // A sole parenthesized set operation is a statement-level shape,
            // not a nested term. TiDB stores the source parentheses on the
            // `SetOprStmt` itself; trailing ORDER BY/LIMIT is folded back
            // into that statement before restore, so `(SELECT 1 UNION
            // SELECT 2) ORDER BY 1` becomes `(SELECT 1 UNION SELECT 2
            // ORDER BY 1)`.
            SetOprTermBody::Nested(mut setopr) => {
                let (order_by, limit, lock) = self.parse_order_limit_lock()?;
                setopr.outer_order_by = order_by;
                setopr.outer_limit = limit;
                setopr.outer_lock = lock;
                setopr.is_in_braces = true;
                Ok(QueryStmt::SetOpr(setopr))
            }
        }
    }

    /// Parses one term of a (possible) set operation: a parenthesized
    /// `(SELECT ... [UNION/EXCEPT/INTERSECT ...])` (fully self-contained,
    /// including its own tail — a NESTED set operation is possible here,
    /// see [`tidb_ast::SetOprTermBody::Nested`]'s own doc) or a bare
    /// `SELECT` core with its own tail left unparsed (the caller's job —
    /// see [`Parser::parse_select_or_setopr`]'s own doc for why bare and
    /// parenthesized terms need different tail-parsing timing).
    fn parse_one_term(&mut self) -> PResult<(bool, SetOprTermBody)> {
        if self.starts_parenthesized_query() {
            self.bump();
            // A leading WITH may itself own a parenthesized outer query:
            // `(WITH cte AS (...) (SELECT ...)) UNION ...`. In that shape
            // the parentheses after WITH belong to the SelectStmt, while the
            // parentheses consumed here belong to the set-operation term.
            // Keep both owners. Ordinary nested parentheses around SELECT
            // are collapsed by Go, so their inner SelectStmt bit is cleared.
            let inner_starts_with = self.is_kw("WITH");
            let inner = if inner_starts_with {
                self.parse_with_select()?
            } else {
                self.parse_select_or_setopr()?
            };
            self.expect_op(")")?;
            let body = match inner {
                QueryStmt::Select(mut sel) => {
                    if !inner_starts_with {
                        sel.is_in_braces = false;
                    }
                    SetOprTermBody::Select(sel)
                }
                QueryStmt::SetOpr(so) => SetOprTermBody::Nested(so),
            };
            Ok((true, body))
        } else if self.is_kw("SELECT") {
            Ok((
                false,
                SetOprTermBody::Select(Box::new(self.parse_select_no_tail()?)),
            ))
        } else if self.is_kw("TABLE") {
            // Go's `parseSubquery` admits the TABLE shorthand anywhere a
            // result-set query is accepted, including both CTE bodies and an
            // INSERT ... WITH ... TABLE source. Keep it as the existing
            // typed SelectStmt/Table variant instead of desugaring to SELECT.
            Ok((
                false,
                SetOprTermBody::Select(Box::new(self.parse_table_no_tail()?)),
            ))
        } else if self.is_kw("VALUES") {
            Ok((
                false,
                SetOprTermBody::Select(Box::new(self.parse_values_no_tail()?)),
            ))
        } else {
            Err(self.err_here("expected SELECT, TABLE, or VALUES query term"))
        }
    }

    /// Parses the standalone `TABLE table_name` result-set statement.  Its
    /// table shorthand owns the same statement tail as a standalone SELECT,
    /// but nested query terms (`WITH`, set operations, and DML sources) have
    /// distinct source productions and are deliberately kept out of this
    /// focused grammar slice.
    pub(crate) fn parse_table_statement(&mut self) -> PResult<SelectStmt> {
        let mut sel = self.parse_table_no_tail()?;
        let (order_by, limit, lock) = self.parse_order_limit_lock()?;
        sel.order_by = order_by;
        sel.limit = limit;
        sel.lock = lock;
        if self.is_kw("INTO") && {
            let after = self.peek_n(1);
            after.kind == TokenKind::UserVar && !after.text.starts_with("@@")
        } {
            self.bump();
            sel.into_vars = self.parse_into_user_vars()?;
        } else {
            sel.into_outfile = self.parse_opt_into_outfile()?;
        }
        Ok(sel)
    }

    /// Parses a `SELECT` up to and including `HAVING`, leaving `ORDER BY`/
    /// `LIMIT`/the locking clause unparsed (consumed by
    /// [`Parser::parse_order_limit_lock`]).
    fn parse_select_no_tail(&mut self) -> PResult<SelectStmt> {
        self.expect_kw("SELECT")?;
        // A `/*+ ... */` hint comment is only ever recognized DIRECTLY
        // after `SELECT` (matching `tidb_lexer`'s own `HINTED_KEYWORDS`
        // gate) — before `DISTINCT`/`ALL`, confirmed via `godump restore`:
        // a hint comment written after `DISTINCT` instead is silently
        // dropped as an ordinary comment, not an error, since the lexer
        // itself never recognizes it as a hint in that position.
        let hints = if self.peek().kind == TokenKind::HintComment {
            let token = self.bump();
            let result = parse_hint_comment(&token.text, self.source_line(token.offset));
            self.warnings.extend(result.diagnostics);
            result.hints
        } else {
            Vec::new()
        };
        // Go accepts all SELECT modifiers in one freely ordered loop and
        // restores them in the fixed field order owned by `SelectStmt`.
        let mut distinct = false;
        let mut all = false;
        let mut calc_found_rows = false;
        let mut priority = tidb_ast::StatementPriority::None;
        let mut sql_small_result = false;
        let mut sql_big_result = false;
        let mut sql_buffer_result = false;
        let mut sql_no_cache = false;
        let mut straight_join = false;
        loop {
            if self.is_kw("DISTINCT") || self.is_kw("DISTINCTROW") {
                self.bump();
                if all {
                    return Err(self.err_here("wrong usage of ALL and DISTINCT"));
                }
                distinct = true;
            } else if self.is_kw("ALL") {
                self.bump();
                if distinct {
                    return Err(self.err_here("wrong usage of ALL and DISTINCT"));
                }
                all = true;
            } else if self.is_kw("HIGH_PRIORITY") {
                self.bump();
                priority = tidb_ast::StatementPriority::High;
            } else if self.is_kw("LOW_PRIORITY") {
                self.bump();
                priority = tidb_ast::StatementPriority::Low;
            } else if self.is_kw("DELAYED") {
                self.bump();
                priority = tidb_ast::StatementPriority::Delayed;
            } else if self.is_kw("STRAIGHT_JOIN") {
                self.bump();
                straight_join = true;
            } else if self.is_kw("SQL_CALC_FOUND_ROWS") {
                self.bump();
                calc_found_rows = true;
            } else if self.is_kw("SQL_CACHE") {
                self.bump();
                sql_no_cache = false;
            } else if self.is_kw("SQL_NO_CACHE") {
                self.bump();
                sql_no_cache = true;
            } else if self.is_kw("SQL_SMALL_RESULT") {
                self.bump();
                sql_small_result = true;
            } else if self.is_kw("SQL_BIG_RESULT") {
                self.bump();
                sql_big_result = true;
            } else if self.is_kw("SQL_BUFFER_RESULT") {
                self.bump();
                sql_buffer_result = true;
            } else {
                break;
            }
        }
        let fields = self.parse_select_list()?;
        // MySQL's grammar admits `INTO @var [, ...]` BETWEEN the select list
        // and FROM — the common spelling — as well as trailing; both land in
        // the same field.
        let mid_into_vars = if self.is_kw("INTO") && {
            let after = self.peek_n(1);
            after.kind == TokenKind::UserVar && !after.text.starts_with("@@")
        } {
            self.bump();
            self.parse_into_user_vars()?
        } else {
            Vec::new()
        };
        let from = if self.is_kw("FROM") {
            self.bump();
            // `FROM DUAL` alone is a no-op placeholder table and is dropped,
            // matching the Go AST.
            if self.is_kw("DUAL") {
                self.bump();
                None
            } else {
                Some(self.parse_from()?)
            }
        } else {
            None
        };
        let where_clause = if self.is_kw("WHERE") {
            self.bump();
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        let group_by = if self.is_kw("GROUP") {
            self.bump();
            self.expect_kw("BY")?;
            self.parse_group_by_list()?
        } else {
            Vec::new()
        };
        // `WITH ROLLUP` — only recognized directly after a non-empty
        // `GROUP BY` list (mirroring real TiDB's own hand-written parser,
        // `pkg/parser/select_clauses_parser.go`'s `parseGroupByClause`,
        // which only checks for it there; a bare `GROUP BY WITH ROLLUP`
        // with no items is itself already a `ParseError` from
        // `parse_group_by_list` above).
        let rollup = !group_by.is_empty() && self.is_kw("WITH") && self.is_kw_at(1, "ROLLUP");
        if rollup {
            self.bump(); // WITH
            self.bump(); // ROLLUP
        }
        let having = if self.is_kw("HAVING") {
            self.bump();
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        let windows = if self.is_kw("WINDOW") {
            self.parse_window_clause()?
        } else {
            Vec::new()
        };
        Ok(SelectStmt {
            kind: SelectStatementKind::Select,
            is_in_braces: false,
            with: None,
            hints,
            priority,
            sql_small_result,
            sql_big_result,
            sql_buffer_result,
            sql_no_cache,
            straight_join,
            calc_found_rows,
            distinct,
            all,
            fields,
            values: Vec::new(),
            from,
            where_clause,
            group_by,
            rollup,
            having,
            windows,
            order_by: Vec::new(),
            limit: None,
            lock: None,
            into_outfile: None,
            into_vars: mid_into_vars,
        })
    }

    /// Parses Go's `TABLE table_name` shorthand into the same wildcard/table
    /// query shape as a select while preserving its distinct AST kind for
    /// restore. Unlike a `FROM` clause, this grammar accepts exactly one bare
    /// table name: aliases, joins, and comma lists are not silently absorbed.
    fn parse_table_no_tail(&mut self) -> PResult<SelectStmt> {
        self.expect_kw("TABLE")?;
        let table = TableRef {
            name: self.parse_table_name_path()?,
            partitions: Vec::new(),
            alias: None,
            as_of: None,
            hints: Vec::new(),
            sample: None,
        };
        Ok(SelectStmt {
            kind: SelectStatementKind::Table,
            is_in_braces: false,
            with: None,
            hints: Vec::new(),
            priority: tidb_ast::StatementPriority::None,
            sql_small_result: false,
            sql_big_result: false,
            sql_buffer_result: false,
            sql_no_cache: false,
            straight_join: false,
            calc_found_rows: false,
            distinct: false,
            all: false,
            fields: vec![SelectField::Wildcard(Vec::new())].into(),
            values: Vec::new(),
            from: Some(Join {
                left: JoinNode::Table(table),
                right: None,
                tp: JoinType::Cross,
                straight: false,
                on: None,
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            }),
            where_clause: None,
            group_by: Vec::new(),
            rollup: false,
            having: None,
            windows: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            lock: None,
            into_outfile: None,
            into_vars: Vec::new(),
        })
    }

    /// Parses Go's standalone `VALUES ROW(...), ...` statement. This is not
    /// an `INSERT` value-list and does not desugar into a synthetic `SELECT`:
    /// the required `ROW` leader, zero-length rows, and its own restore shape
    /// are all observable in Go's `SelectStmtKindValues` AST.
    pub(crate) fn parse_values_statement(&mut self) -> PResult<SelectStmt> {
        let mut statement = self.parse_values_no_tail()?;
        let (order_by, limit, lock) = self.parse_order_limit_lock()?;
        statement.order_by = order_by;
        statement.limit = limit;
        statement.lock = lock;
        if self.is_kw("INTO") && {
            let after = self.peek_n(1);
            after.kind == TokenKind::UserVar && !after.text.starts_with("@@")
        } {
            self.bump();
            statement.into_vars = self.parse_into_user_vars()?;
        } else {
            statement.into_outfile = self.parse_opt_into_outfile()?;
        }
        Ok(statement)
    }

    fn parse_values_no_tail(&mut self) -> PResult<SelectStmt> {
        self.expect_kw("VALUES")?;
        let mut values = Vec::new();
        loop {
            self.expect_kw("ROW")?;
            self.expect_op("(")?;
            let mut row = Vec::new();
            if !self.is_op(")") {
                row.push(self.parse_expr_or_default()?);
                while self.is_op(",") {
                    self.bump();
                    row.push(self.parse_expr_or_default()?);
                }
            }
            self.expect_op(")")?;
            values.push(row);
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(SelectStmt {
            kind: SelectStatementKind::Values,
            is_in_braces: false,
            with: None,
            hints: Vec::new(),
            priority: tidb_ast::StatementPriority::None,
            sql_small_result: false,
            sql_big_result: false,
            sql_buffer_result: false,
            sql_no_cache: false,
            straight_join: false,
            calc_found_rows: false,
            distinct: false,
            all: false,
            fields: vec![SelectField::Wildcard(Vec::new())].into(),
            values,
            from: None,
            where_clause: None,
            group_by: Vec::new(),
            rollup: false,
            having: None,
            windows: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            lock: None,
            into_outfile: None,
            into_vars: Vec::new(),
        })
    }

    /// Parses the exact parenthesized `VALUES` target used by
    /// `EXPLAIN FORMAT = TRADITIONAL ((VALUES ROW ()) ORDER BY 1)` in
    /// `planner/core/plan.test:172`.
    ///
    /// Go's `parseSubquery` marks the inner `ast.SelectStmt.IsInBraces` and
    /// then folds the outer statement-level `ORDER BY` onto that same node;
    /// the two source parentheses consequently restore as one pair. Keep
    /// this as a narrow EXPLAIN seam instead of widening ordinary top-level
    /// statement dispatch until the broader parenthesized `VALUES` family
    /// has its own source/test ownership.
    pub(crate) fn parse_explain_parenthesized_values(&mut self) -> PResult<QueryStmt> {
        self.expect_op("(")?;
        self.expect_op("(")?;
        let mut values = self.parse_values_statement()?;
        self.expect_op(")")?;
        let (order_by, limit, lock) = self.parse_order_limit_lock()?;
        self.expect_op(")")?;
        values.order_by = order_by;
        values.limit = limit;
        values.lock = lock;
        values.is_in_braces = true;
        Ok(QueryStmt::Select(Box::new(values)))
    }

    /// Parses `WINDOW name AS (def), name2 AS (def2), ...` (the `WINDOW`
    /// keyword itself is consumed here) — each entry's own definition
    /// body is [`Parser::parse_window_def_body`], shared with a
    /// parenthesized `OVER (...)` reference.
    fn parse_window_clause(&mut self) -> PResult<Vec<(String, WindowDef)>> {
        self.expect_kw("WINDOW")?;
        let mut windows = Vec::new();
        loop {
            let name = self.parse_name()?;
            self.expect_kw("AS")?;
            let def = self.parse_window_def_body()?;
            windows.push((name, def));
            if self.is_op(",") {
                self.bump();
            } else {
                break;
            }
        }
        Ok(windows)
    }

    /// Parses `[ORDER BY ...] [LIMIT ...] [locking clause]` in ANY
    /// relative order — a loop over the three, rather than three
    /// sequential `if`s, so any permutation is accepted (confirmed via
    /// `godump restore`: `LIMIT 1 ORDER BY a` and `FOR UPDATE ORDER BY
    /// a` both parse, each restoring in a FIXED canonical order
    /// regardless of how they were written — see
    /// [`tidb_ast::SelectStmt::lock`]'s own doc). `ORDER BY`/`LIMIT`
    /// silently OVERWRITE on repetition (confirmed via `godump restore`:
    /// `LIMIT 1 LIMIT 2` restores as `LIMIT 2`, the last one winning),
    /// but a SECOND locking clause is a genuine `ParseError` (confirmed
    /// via `godump restore`: `FOR UPDATE FOR UPDATE` errors) — this loop
    /// replicates that exact asymmetry rather than uniformly erroring or
    /// uniformly overwriting. The caller decides where the result
    /// attaches (see [`Parser::parse_term_and_tail`]'s own doc).
    fn parse_order_limit_lock(
        &mut self,
    ) -> PResult<(Vec<OrderItem>, Option<Limit>, Option<tidb_ast::SelectLock>)> {
        let mut order_by = Vec::new();
        let mut limit = None;
        let mut lock = None;
        loop {
            if self.is_kw("ORDER") {
                self.bump();
                self.expect_kw("BY")?;
                order_by = self.parse_order_list()?;
            } else if self.is_kw("LIMIT") {
                self.bump();
                limit = Some(self.parse_limit()?);
            } else if self.is_kw("FETCH") {
                self.bump();
                if self.is_kw("FIRST") || self.is_kw("NEXT") {
                    self.bump();
                } else {
                    return Err(self.err_here("expected FIRST or NEXT after FETCH"));
                }
                let count = if self.is_kw("ROW") || self.is_kw("ROWS") {
                    Expr::Int("1".to_owned())
                } else {
                    self.parse_expr(prec::NONE)?
                };
                if self.is_kw("ROW") || self.is_kw("ROWS") {
                    self.bump();
                } else {
                    return Err(self.err_here("expected ROW or ROWS in FETCH clause"));
                }
                self.expect_kw("ONLY")?;
                limit = Some(Limit {
                    offset: None,
                    count,
                });
            } else if self.is_kw("FOR") || self.is_kw("LOCK") {
                if lock.is_some() {
                    return Err(self.err_here("duplicate locking clause"));
                }
                lock = Some(self.parse_select_lock()?);
            } else {
                break;
            }
        }
        Ok((order_by, limit, lock))
    }

    /// Parses the complete trailing `INTO OUTFILE` payload. Always called AFTER
    /// [`Parser::parse_order_limit_lock`], never folded into that same
    /// loop — real TiDB's own grammar checks for `INTO` only once that
    /// entire loop has already finished, not interleaved with it.
    /// Parses `INTO @var [, @var ...]` — Go `SelectIntoVars`. Only entered
    /// when the token after `INTO` is a user variable; every other spelling
    /// stays on the OUTFILE path and its errors.
    fn parse_into_user_vars(&mut self) -> PResult<Vec<String>> {
        let mut vars = vec![self.bumped_at_name()];
        while self.is_op(",") {
            self.bump();
            let token = self.peek();
            if token.kind != TokenKind::UserVar || token.text.starts_with("@@") {
                return Err(self.err_here("expected a user variable after INTO @..., "));
            }
            vars.push(self.bumped_at_name());
        }
        Ok(vars)
    }

    fn parse_opt_into_outfile(&mut self) -> PResult<Option<tidb_ast::SelectIntoOption>> {
        if !self.is_kw("INTO") {
            return Ok(None);
        }
        self.bump();
        self.expect_kw("OUTFILE")?;
        if self.peek().kind != TokenKind::Str {
            return Err(self.err_here("expected a string literal after INTO OUTFILE"));
        }
        let file_name = self.bumped_string();
        let fields = if self.is_kw("FIELDS") || self.is_kw("COLUMNS") {
            self.bump();
            self.parse_fields_clause(false)?
        } else {
            tidb_ast::LoadDataFields::default()
        };
        let lines = if self.is_kw("LINES") {
            self.bump();
            self.parse_lines_clause()?
        } else {
            tidb_ast::LoadDataLines::default()
        };
        Ok(Some(tidb_ast::SelectIntoOption {
            file_name,
            fields,
            lines,
        }))
    }

    /// Parses `FOR UPDATE|SHARE [OF table[, table...]] [NOWAIT|SKIP
    /// LOCKED]` or `LOCK IN SHARE MODE` (the latter normalizes to
    /// `LockKind::Share` and never accepts `OF`/`NOWAIT`/`SKIP LOCKED` —
    /// confirmed via `godump restore`, both are genuine `ParseError`s on
    /// that spelling; see [`tidb_ast::SelectLock`]'s own doc). Starts
    /// from the `FOR`/`LOCK` keyword itself (the caller only peeks it).
    fn parse_select_lock(&mut self) -> PResult<tidb_ast::SelectLock> {
        if self.is_kw("LOCK") {
            self.bump();
            self.expect_kw("IN")?;
            self.expect_kw("SHARE")?;
            self.expect_kw("MODE")?;
            return Ok(tidb_ast::SelectLock {
                kind: tidb_ast::LockKind::Share,
                of: Vec::new(),
                wait: tidb_ast::LockWait::Default,
            });
        }
        self.expect_kw("FOR")?;
        let kind = if self.is_kw("UPDATE") {
            self.bump();
            tidb_ast::LockKind::Update
        } else if self.is_kw("SHARE") {
            self.bump();
            tidb_ast::LockKind::Share
        } else {
            return Err(self.err_here("expected UPDATE or SHARE after FOR"));
        };
        let mut of = Vec::new();
        if self.is_kw("OF") {
            self.bump();
            of.push(self.parse_name_path()?);
            while self.is_op(",") {
                self.bump();
                of.push(self.parse_name_path()?);
            }
        }
        let wait = if self.is_kw("NOWAIT") {
            self.bump();
            tidb_ast::LockWait::NoWait
        } else if self.is_kw("SKIP") {
            self.bump();
            self.expect_kw("LOCKED")?;
            tidb_ast::LockWait::SkipLocked
        } else if kind == tidb_ast::LockKind::Update && self.is_kw("WAIT") {
            self.bump();
            if !matches!(self.peek().kind, TokenKind::IntLit | TokenKind::DecLit) {
                return Err(self.err_here("expected an unsigned integer after WAIT"));
            }
            let seconds = self
                .bump()
                .text
                .parse::<u64>()
                .map_err(|_| self.err_here("expected an unsigned integer after WAIT"))?;
            tidb_ast::LockWait::Wait(seconds)
        } else {
            tidb_ast::LockWait::Default
        };
        Ok(tidb_ast::SelectLock { kind, of, wait })
    }

    /// Returns the set operator at the current position, without consuming it.
    pub(crate) fn peek_set_op(&self) -> Option<()> {
        (self.is_kw("UNION") || self.is_kw("EXCEPT") || self.is_kw("INTERSECT")).then_some(())
    }

    /// Consumes a set operator (`UNION`/`EXCEPT`/`INTERSECT` with optional `ALL`).
    fn parse_set_op(&mut self) -> PResult<SetOp> {
        let kind = if self.is_kw("UNION") {
            0
        } else if self.is_kw("EXCEPT") {
            1
        } else if self.is_kw("INTERSECT") {
            2
        } else {
            return Err(self.err_here("expected a set operator"));
        };
        self.bump();
        let all = if self.is_kw("ALL") {
            self.bump();
            true
        } else {
            if self.is_kw("DISTINCT") || self.is_kw("DISTINCTROW") {
                self.bump(); // DISTINCT is the default
            }
            false
        };
        Ok(match kind {
            0 => SetOp::Union { all },
            1 => SetOp::Except { all },
            _ => SetOp::Intersect { all },
        })
    }

    /// Parses the remaining terms and statement-level tail of a set operation,
    /// given an already-parsed first term.
    pub(crate) fn parse_setopr_rest(
        &mut self,
        first_braces: bool,
        first: SetOprTermBody,
    ) -> PResult<SetOprStmt> {
        let mut terms = vec![SetOprTerm {
            op: None,
            in_braces: first_braces,
            body: first,
        }];
        // Statement-level ORDER BY / LIMIT / locking clause apply to the
        // whole set operation — populated below from whichever term
        // turns out to be the LAST one (see
        // `Parser::parse_select_or_setopr`'s own doc for why real
        // MySQL/TiDB attaches a trailing tail there with no term-level
        // owner left to disambiguate it).
        let mut order_by = Vec::new();
        let mut limit = None;
        let mut lock = None;
        while self.peek_set_op().is_some() {
            let op = self.parse_set_op()?;
            let (in_braces, mut body) = self.parse_one_term()?;
            // Unlike the very FIRST term (see `parse_select_or_setopr`'s
            // own doc), a term HERE always attempts its own trailing
            // tail regardless of whether it was parenthesized — a
            // multi-term set operation has no "sole parenthesized
            // statement" special case to worry about (confirmed via
            // `godump restore`: `(SELECT a FROM t1) UNION ALL (SELECT a
            // FROM t2) ORDER BY 1 LIMIT 10`, both terms parenthesized,
            // still has a real trailing statement-level tail after the
            // last one, not folded into it).
            let (term_order_by, term_limit, term_lock) = self.parse_order_limit_lock()?;
            if in_braces
                && (!term_order_by.is_empty() || term_limit.is_some())
                && self.peek_set_op().is_some()
            {
                return Err(self.err_here("set operation cannot follow ORDER BY or LIMIT"));
            }
            // A real, confirmed asymmetry (via `godump restore`, not
            // assumed uniform): `ORDER BY`/`LIMIT` after a NON-FIRST
            // term NEVER attach to that specific term, even when a
            // following set operator would otherwise disambiguate it the
            // same way it does for the FIRST term (see
            // `parse_select_or_setopr`'s own eager tail-parse) — they
            // ALWAYS become the whole statement's own (confirmed:
            // `t1 UNION t2 ORDER BY x UNION t3` restores with `ORDER BY
            // x` moved all the way to the very end, after `t3`, not kept
            // next to `t2`). A later occurrence overwrites an earlier
            // one, same "last one wins" rule as a single repeated
            // `LIMIT`/`ORDER BY`. The LOCKING clause does NOT share this
            // restriction: it sticks to a specific non-first, non-last
            // term exactly like it does for the first term (confirmed:
            // `t1 UNION t2 FOR UPDATE UNION t3` keeps `FOR UPDATE`
            // attached to `t2`) — but only when that term is a plain
            // `SELECT`; a NESTED term has no term-level lock slot of its
            // own to redirect onto (an obscure, unconfirmed-in-the-wild
            // combination — `t1 UNION (t2 UNION t3) FOR UPDATE UNION
            // t4` — so it deliberately falls back to the statement-level
            // `lock` below instead of inventing one).
            if !term_order_by.is_empty() {
                order_by = term_order_by;
            }
            if term_limit.is_some() {
                limit = term_limit;
            }
            match &mut body {
                SetOprTermBody::Select(sel) if self.peek_set_op().is_some() => {
                    sel.lock = term_lock;
                }
                _ => {
                    lock = term_lock;
                }
            }
            terms.push(SetOprTerm {
                op: Some(op),
                in_braces,
                body,
            });
        }
        Ok(SetOprStmt {
            with: None,
            is_in_braces: false,
            terms,
            order_by,
            limit,
            lock,
            outer_order_by: Vec::new(),
            outer_limit: None,
            outer_lock: None,
        })
    }

    pub(crate) fn parse_expr_list(&mut self) -> PResult<Vec<Expr>> {
        let mut list = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            list.push(self.parse_expr(prec::NONE)?);
        }
        Ok(list)
    }

    /// Parses `GROUP BY`'s own `expr [ASC|DESC], ...` list — each item
    /// carries its OWN independent direction (confirmed via `godump
    /// restore`), unlike `ORDER BY`'s [`OrderItem`] (a plain `bool`),
    /// `desc` here distinguishes "no direction written" (`None`, the
    /// only case real MySQL/TiDB executes normally by default) from an
    /// EXPLICIT `ASC` (`Some(false)`) — see [`tidb_ast::GroupByItem`]'s
    /// own doc for why that distinction matters even though both restore
    /// identically.
    fn parse_group_by_list(&mut self) -> PResult<Vec<tidb_ast::GroupByItem>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr(prec::NONE)?;
            let desc = if self.is_kw("ASC") {
                self.bump();
                Some(false)
            } else if self.is_kw("DESC") {
                self.bump();
                Some(true)
            } else {
                None
            };
            items.push(tidb_ast::GroupByItem { expr, desc });
            if self.is_op(",") {
                self.bump();
            } else {
                break;
            }
        }
        Ok(items)
    }

    pub(crate) fn parse_order_list(&mut self) -> PResult<Vec<OrderItem>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr(prec::NONE)?;
            let desc = if self.is_kw("ASC") {
                self.bump();
                false
            } else if self.is_kw("DESC") {
                self.bump();
                true
            } else {
                false
            };
            items.push(OrderItem { expr, desc });
            if self.is_op(",") {
                self.bump();
            } else {
                break;
            }
        }
        Ok(items)
    }

    pub(crate) fn parse_limit(&mut self) -> PResult<Limit> {
        // `LIMIT count`, `LIMIT offset, count`, or `LIMIT count OFFSET offset`.
        let first = self.parse_limit_option()?;
        if self.is_op(",") {
            self.bump();
            let count = self.parse_limit_option()?;
            Ok(Limit {
                offset: Some(first),
                count,
            })
        } else if self.is_kw("OFFSET") {
            self.bump();
            let offset = self.parse_limit_option()?;
            Ok(Limit {
                offset: Some(offset),
                count: first,
            })
        } else {
            Ok(Limit {
                offset: None,
                count: first,
            })
        }
    }

    /// Parses one LIMIT/OFFSET operand and enforces TiDB's unsigned 64-bit
    /// literal boundary. Go's `parseLimitOption` routes integer literals
    /// through `toUint64Value`, so `2^64 - 1` is the largest accepted literal
    /// and `2^64` (or larger) is a parse error. Keep the existing expression
    /// grammar for non-literal operands; this check only closes the source
    /// owned integer-overflow boundary without inventing a narrower AST.
    fn parse_limit_option(&mut self) -> PResult<Expr> {
        let expr = self.parse_expr(prec::NONE)?;
        match &expr {
            Expr::Int(digits) => {
                digits
                    .parse::<u64>()
                    .map_err(|_| self.err_here("LIMIT value out of range"))?;
            }
            // The lexer classifies an integer that overflows u64 as a
            // decimal literal, mirroring Go's scanner's `toDecimal` fallback.
            // LIMIT accepts only an unsigned integer literal at this grammar
            // boundary, so reject that overflow token before it can restore.
            Expr::Decimal(_) => return Err(self.err_here("LIMIT value out of range")),
            _ => {}
        }
        Ok(expr)
    }

    pub(crate) fn parse_select_list(&mut self) -> PResult<tidb_ast::SelectFieldList> {
        let mut fields = tidb_ast::SelectFieldList::default();
        let (field, text, projection_offset) = self.parse_select_field()?;
        fields.push_with_text_and_projection_offset(field, text, projection_offset);
        while self.is_op(",") {
            self.bump();
            let (field, text, projection_offset) = self.parse_select_field()?;
            fields.push_with_text_and_projection_offset(field, text, projection_offset);
        }
        Ok(fields)
    }

    fn parse_select_field(&mut self) -> PResult<(SelectField, Vec<u8>, Option<usize>)> {
        let start = self.peek().offset;
        let mut projection_offset = None;
        let field = if self.is_op("*") {
            self.bump();
            SelectField::Wildcard(Vec::new())
        // A qualified wildcard (`t.*`, `db.t.*`) looks like a name path up
        // until its final segment, which is `*` instead of another name —
        // try that shape before falling through to general expression
        // parsing (whose name-path parsing stops at a trailing `.` that
        // isn't followed by an identifier, so it would otherwise choke on
        // the unconsumed `.` `*`).
        } else if let Some(path) = self.try_take_wildcard() {
            SelectField::Wildcard(path)
        } else {
            self.string_projection_offset = None;
            let expr = self.parse_expr(prec::NONE)?;
            if is_string_value_literal(&expr) {
                projection_offset = self.string_projection_offset;
            }
            let alias = self.parse_opt_alias()?;
            SelectField::Expr { expr, alias }
        };
        let end = self.peek().offset;
        let text = if end > start {
            self.source[start..end].trim().as_bytes().to_vec()
        } else {
            Vec::new()
        };
        Ok((field, text, projection_offset))
    }

    /// If the upcoming tokens form a qualified wildcard — `IDENT ('.' IDENT)*
    /// '.' '*'` — consumes them and returns the qualifier path. Otherwise
    /// consumes nothing and returns `None` (a pure lookahead scan confirms
    /// the whole shape before any token is consumed, so a plain column
    /// reference like `t.a` is left untouched for the normal expression
    /// parser to handle).
    fn try_take_wildcard(&mut self) -> Option<Vec<String>> {
        let mut n = 0;
        loop {
            if self.peek_n(n).kind != TokenKind::Ident {
                return None;
            }
            n += 1;
            if !(self.peek_n(n).kind == TokenKind::Op && self.peek_n(n).text == ".") {
                return None;
            }
            n += 1;
            if self.peek_n(n).kind == TokenKind::Op && self.peek_n(n).text == "*" {
                break;
            }
            // A name follows the dot instead: keep scanning the path.
        }
        let mut path = Vec::new();
        loop {
            path.push(self.bump().text);
            self.bump(); // '.'
            if self.is_op("*") {
                self.bump();
                break;
            }
        }
        Some(path)
    }

    /// Parses an optional `AS name` or bare `name` alias. Go uses TWO
    /// DIFFERENT acceptance rules here, not one (`pkg/parser/
    /// select_parser.go:452-464`, `parseSelectField`'s own alias
    /// handling): the explicit `AS name` form checks `IsReserved` (`!=
    /// identifier && != stringLit && (< identifier || IsReserved(...))`
    /// is a syntax error) — the SAME 236-keyword gate `tidb_lexer::
    /// is_reserved` mirrors, used at every other identifier position;
    /// the BARE (no-`AS`) form checks the separate, much narrower
    /// `CanBeImplicitAlias` (curated per-keyword exclusion list,
    /// `select_clauses_parser.go:269`) instead, since an unmarked
    /// trailing word is ambiguous with the start of the next clause in a
    /// way `AS` disambiguates. Conflating the two into one shared
    /// acceptance check (as this function previously did, following its
    /// own now-corrected but WRONG claim that Go uses "the same rule
    /// either way") wrongly accepted `SELECT 1 AS database`/`AS dec` —
    /// both genuine Go syntax errors, confirmed via a direct `pkg/parser`
    /// probe on this branch — because `is_alias_excluded_keyword` below
    /// (Go's `CanBeImplicitAlias` mirror) has never listed every reserved
    /// keyword, only the ones ambiguous as a BARE trailing word.
    fn parse_opt_alias(&mut self) -> PResult<Option<String>> {
        // The lexer combines adjacent `AS OF` into one token for stale-read
        // grammar. In a select-field alias position there is no stale-read
        // production: this is still `AS` followed by the reserved word `OF`,
        // and Go rejects it through FieldAsName's Identifier gate.
        if self.is_kw("AS OF") {
            return Err(self.err_here("expected identifier after AS"));
        }
        if self.is_kw("AS") {
            self.bump();
            return Ok(Some(self.parse_explicit_alias_name()?));
        }
        if self.can_be_alias_name() {
            return Ok(Some(self.parse_alias_name()?));
        }
        Ok(None)
    }

    fn parse_opt_table_alias(&mut self) -> PResult<Option<String>> {
        if self.is_kw("AS") {
            self.bump();
            if self.peek().kind == TokenKind::Str {
                return Err(self.err_here("table alias must be an identifier"));
            }
            return Ok(Some(self.parse_explicit_alias_name()?));
        }
        if self.peek().kind != TokenKind::Str && self.can_be_alias_name() {
            return Ok(Some(self.parse_alias_name()?));
        }
        Ok(None)
    }

    /// Reports whether the CURRENT token is eligible as a BARE (no `AS`)
    /// alias name — mirrors Go's `CanBeImplicitAlias` (see
    /// [`Parser::parse_opt_alias`]'s own doc for why this is deliberately
    /// NOT the same gate the explicit `AS name` form uses).
    fn can_be_alias_name(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Ident | TokenKind::Str)
            || (self.peek().kind == TokenKind::Keyword
                && !is_alias_excluded_keyword(&self.peek().text))
    }

    /// Reports whether the CURRENT token is eligible as an EXPLICIT `AS
    /// name` alias — mirrors Go's `IsReserved` check at
    /// `select_parser.go:455`/`join_parser.go:558` exactly (see
    /// [`Parser::parse_opt_alias`]'s own doc).
    fn can_be_explicit_alias_name(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Ident | TokenKind::Str)
            || (self.peek().kind == TokenKind::Keyword && !crate::is_reserved(&self.peek().text))
    }

    /// Parses a BARE alias name that MUST be present — used only where a
    /// bare name was already confirmed eligible via
    /// [`Parser::can_be_alias_name`].
    fn parse_alias_name(&mut self) -> PResult<String> {
        if self.can_be_alias_name() {
            Ok(self.bump_alias_token())
        } else {
            Err(self.err_here("expected identifier"))
        }
    }

    /// Parses an EXPLICIT `AS name` alias name that MUST be present —
    /// unlike the bare form, `AS` alone with nothing eligible following
    /// it is a genuine `ParseError`, not a silently-absent alias. See
    /// [`Parser::parse_opt_alias`]'s own doc for why this uses a
    /// different (broader) acceptance gate than [`Parser::parse_alias_name`].
    fn parse_explicit_alias_name(&mut self) -> PResult<String> {
        if self.can_be_explicit_alias_name() {
            Ok(self.bump_alias_token())
        } else {
            Err(self.err_here("expected identifier"))
        }
    }

    /// Consumes the current (already-validated-eligible) token as an
    /// alias name, decoding a string literal or normalizing an
    /// identifier/keyword as appropriate.
    fn bump_alias_token(&mut self) -> String {
        let name = self.bump();
        if name.kind == TokenKind::Str {
            self.decode_string(&name.text)
        } else {
            crate::normalize_identifier(name.text)
        }
    }
}

fn is_string_value_literal(expr: &Expr) -> bool {
    let mut inner = expr;
    while let Expr::Paren(next) | Expr::Unary(UnaryOp::Plus, next) = inner {
        inner = next;
    }
    matches!(inner, Expr::String(_))
}

/// Whether `name` is one of the keywords real MySQL/TiDB never accepts
/// as a bare (no-`AS`) implicit alias — mirrors real TiDB's own
/// `pkg/parser/select_clauses_parser.go`'s `CanBeImplicitAlias` exactly
/// (read directly, then spot-checked against `godump restore` for
/// several less-obvious entries, e.g. confirming `WINDOW` is excluded
/// EVEN with an explicit `AS` — `SELECT 1 AS window` is also a genuine
/// `ParseError` there, so no extra context-dependent lookahead is
/// needed for it here beyond this flat exclusion list). Every OTHER
/// keyword — including many that look "reserved" at a glance, like
/// `SUM`/`JSON`/`BINARY`/`CHAR`/`TIMESTAMP`/`MATCH`/`COLLATION` — is a
/// perfectly valid bare alias (confirmed via `godump restore`:
/// `SELECT 1 sum`/`SELECT 1 json`/`SELECT 1 binary` all parse fine).
fn is_alias_excluded_keyword(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "SELECT"
            | "FROM"
            | "WHERE"
            | "GROUP"
            | "ORDER"
            | "LIMIT"
            | "HAVING"
            | "SET"
            | "UPDATE"
            | "DELETE"
            | "INSERT"
            | "INTO"
            | "VALUES"
            | "RETURNING"
            | "ON"
            | "USING"
            | "AS"
            | "IF"
            | "EXISTS"
            | "JOIN"
            | "INNER"
            | "CROSS"
            | "LEFT"
            | "RIGHT"
            | "NATURAL"
            | "STRAIGHT_JOIN"
            | "UNION"
            | "EXCEPT"
            | "INTERSECT"
            | "USE"
            | "IGNORE"
            | "FORCE"
            | "FETCH"
            | "OFFSET"
            | "FOR"
            | "LOCK"
            | "IN"
            | "NOT"
            | "AND"
            | "OR"
            | "IS"
            | "NULL"
            | "TRUE"
            | "FALSE"
            | "LIKE"
            | "BETWEEN"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "CREATE"
            | "ALTER"
            | "DROP"
            | "TABLE"
            | "INDEX"
            | "COLUMN"
            | "PRIMARY"
            | "KEY"
            | "UNIQUE"
            | "FOREIGN"
            | "CHECK"
            | "CONSTRAINT"
            | "DEFAULT"
            | "ALL"
            | "DISTINCT"
            | "PARTITION"
            | "WITH"
            | "WINDOW"
            | "OVER"
            | "GROUPS"
            | "ROW"
            | "FUNCTION"
            | "OF"
            | "TABLESAMPLE"
            // Window function names are reserved and cannot be aliases.
            | "CUME_DIST"
            | "DENSE_RANK"
            | "FIRST_VALUE"
            | "LAG"
            | "LAST_VALUE"
            | "LEAD"
            | "NTH_VALUE"
            | "NTILE"
            | "PERCENT_RANK"
            | "RANK"
            | "ROW_NUMBER"
    )
}
