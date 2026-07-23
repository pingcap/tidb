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
    SetOp, SetOprStmt, SetOprTerm, SetOprTermBody, TableRef, TableSample, WindowDef, WithClause,
};
use tidb_lexer::TokenKind;

use crate::{decode_at_name, decode_string, is_name_or_keyword, prec, PResult, Parser};

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
                    sel.into_outfile = self.parse_opt_into_outfile()?;
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
            self.bump(); // (
            let inner = if self.is_kw("WITH") {
                self.parse_with_select()?
            } else {
                self.parse_select_or_setopr()?
            };
            self.expect_op(")")?;
            let body = match inner {
                QueryStmt::Select(mut sel) => {
                    sel.is_in_braces = false;
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
        sel.into_outfile = self.parse_opt_into_outfile()?;
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
        statement.into_outfile = self.parse_opt_into_outfile()?;
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

    /// Parses one hint inside a `/*+ ... */` comment, dispatching on the
    /// hint's own name — see [`tidb_ast::Hint`]'s own doc for exactly
    /// which names/shapes are modelled. Called on the NESTED sub-`Parser`
    /// [`parse_hint_comment`] constructs over the comment's own inner
    /// text, reusing this same token-cursor infrastructure rather than a
    /// bespoke hint-only lexer/parser.
    fn parse_one_hint(&mut self) -> PResult<Hint> {
        if !matches!(self.peek().kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(self.err_here("expected an optimizer hint name"));
        }
        let name = self.bump().text.to_ascii_uppercase();
        match name.as_str() {
            "JOIN_FIXED_ORDER" if !self.is_op("(") => Ok(Hint {
                name,
                kind: HintKind::Nullary { qb_name: None },
            }),
            "INL_JOIN"
            | "INL_HASH_JOIN"
            | "INL_MERGE_JOIN"
            | "HASH_JOIN"
            | "HASH_JOIN_BUILD"
            | "HASH_JOIN_PROBE"
            | "BROADCAST_JOIN"
            | "SHUFFLE_JOIN"
            | "NO_HASH_JOIN"
            | "MERGE_JOIN"
            | "NO_MERGE_JOIN"
            | "TIDB_SMJ"
            | "TIDB_INLJ"
            | "TIDB_HJ"
            | "NO_INDEX_JOIN"
            | "NO_INDEX_HASH_JOIN"
            | "NO_INDEX_MERGE_JOIN" => {
                self.expect_op("(")?;
                // An OPTIONAL leading `@qb_name`, read directly from
                // `pkg/parser/hintparser.go`'s `parseTableLevelHint`
                // (calls the SAME shared `parseQBName()` the
                // `MAX_EXECUTION_TIME`/`NTH_PLAN`/`QB_NAME` arms already
                // use) — see `tidb_ast::HintKind::Tables`'s own doc.
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let mut tables = Vec::new();
                if !self.is_op(")") {
                    tables.push(self.parse_hint_table()?);
                    while self.is_op(",") {
                        self.bump();
                        tables.push(self.parse_hint_table()?);
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Tables { qb_name, tables },
                })
            }
            // `MERGE` is a genuine PARSE/RESTORE asymmetry in real TiDB,
            // confirmed via `godump restore` after this project's own
            // coverage measurement caught it: it PARSES a table list
            // exactly like `MERGE_JOIN`/etc. above (`MERGE(t1, t2)` is
            // valid grammar), but ALWAYS restores as bare `MERGE()`,
            // discarding the parsed tables entirely — real TiDB's own
            // restore code puts `"merge"` in its argument-less bucket
            // even though `parseOneHint` dispatches it through the
            // SAME table-list parser as `MERGE_JOIN`. `NO_MERGE`
            // (distinct from `NO_MERGE_JOIN`, which IS a normal
            // table-list hint) is a genuinely different, real MySQL
            // compatibility hint that real TiDB doesn't support AT ALL
            // — parsed only far enough to skip its own args, producing
            // NO hint node, a real, narrower divergence from this
            // project's own "unrecognized name" `ParseError`
            // (deliberately not replicated — see `parse_one_hint`'s own
            // final `_ =>` arm).
            "MERGE" => {
                self.expect_op("(")?;
                if !self.is_op(")") {
                    self.parse_hint_table()?;
                    while self.is_op(",") {
                        self.bump();
                        self.parse_hint_table()?;
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Nullary { qb_name: None },
                })
            }
            "USE_INDEX"
            | "FORCE_INDEX"
            | "USE_INDEX_MERGE"
            | "IGNORE_INDEX"
            | "INDEX_LOOKUP_PUSHDOWN"
            | "NO_INDEX_LOOKUP_PUSHDOWN"
            | "ORDER_INDEX"
            | "NO_ORDER_INDEX" => {
                self.expect_op("(")?;
                // Direct translation of Go's `parseIndexLevelHint`: every
                // index-level spelling has the SAME optional query-block
                // prefix, one required hint table, optional comma, and
                // optional index-name list.
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let table = self.parse_hint_table()?;
                // Optional comma between the table and the index list
                // (confirmed via `godump restore`: both `USE_INDEX(t idx)`
                // and `USE_INDEX(t, idx)` parse and restore identically).
                if self.is_op(",") {
                    self.bump();
                }
                let mut indexes = Vec::new();
                if !self.is_op(")") {
                    indexes.push(self.parse_charset_name()?);
                    while self.is_op(",") {
                        self.bump();
                        indexes.push(self.parse_charset_name()?);
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Index {
                        qb_name,
                        table,
                        indexes,
                    },
                })
            }
            // `LEADING` gets its own recursive arm: real TiDB's own
            // `parseLeadingTableList` calls `parseLeadingElement()`
            // unconditionally once before ever checking for a comma, so
            // `LEADING()` (empty) is a genuine parse failure there (real
            // TiDB drops the hint silently with a warning; confirmed via
            // `godump restore` — `LEADING()` restores with NO hint at
            // all, unlike `INL_JOIN()`, which restores fine). This
            // project's own narrower, `ParseError`-over-silent-drop
            // convention (see `tidb_ast::Hint`'s own doc) applies the
            // same way here: requiring at least one table below makes
            // `LEADING()` a `ParseError` instead of silently vanishing.
            // The recursive tree and optional hint-level `@qb` prefix are
            // preserved in `HintKind::Leading` so restore matches Go.
            "LEADING" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let mut elements = vec![self.parse_leading_element()?];
                while self.is_op(",") {
                    self.bump();
                    elements.push(self.parse_leading_element()?);
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    // Go's `parseLeadingHint` accepts an optional hint-level
                    // QB prefix before the recursive table list. Preserve it
                    // in the same tree instead of flattening nested groups.
                    kind: HintKind::Leading { qb_name, elements },
                })
            }
            "SET_VAR" => {
                self.expect_op("(")?;
                let var_name = self.parse_charset_name()?;
                self.expect_op("=")?;
                let value = self.parse_hint_value()?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::SetVar { var_name, value },
                })
            }
            "USE_TOJA" | "USE_CASCADES" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let value = if self.is_kw("TRUE") {
                    self.bump();
                    true
                } else if self.is_kw("FALSE") {
                    self.bump();
                    false
                } else {
                    return Err(self.err_here("expected TRUE or FALSE"));
                };
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Bool { qb_name, value },
                })
            }
            "WRITE_SLOW_LOG" => {
                if !self.is_op("(") {
                    return Ok(Hint {
                        name,
                        kind: HintKind::Nullary { qb_name: None },
                    });
                }
                self.bump();
                let value = if self.is_kw("TRUE") {
                    self.bump();
                    true
                } else if self.is_kw("FALSE") {
                    self.bump();
                    false
                } else {
                    return Err(self.err_here("expected TRUE or FALSE"));
                };
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Bool {
                        qb_name: None,
                        value,
                    },
                })
            }
            // `RESOURCE_GROUP(name)` — a single BARE identifier argument
            // (confirmed via `godump restore`: `RESOURCE_GROUP(default)`
            // parses, so `parse_charset_name` — which accepts any
            // identifier-OR-keyword token, the SAME lenient acceptance
            // `SET_VAR`'s own `var_name` above already relies on — is the
            // right fit, not the narrower `parse_name`). No `@qb_name`
            // suffix is accepted here — real TiDB's own
            // `parseResourceGroupHint` only ever calls `parseIdentifier`,
            // never `parseHintTable`, confirmed via `godump restore`:
            // `RESOURCE_GROUP(rg1@sel_1)` is real TiDB's own silent-drop-
            // with-warning case (the whole hint vanishes from restore),
            // so it stays a genuine `ParseError` here — the SAME
            // narrower, `ParseError`-over-silent-drop convention already
            // applied to `LEADING()`/`USE_TOJA(1)`.
            "RESOURCE_GROUP" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let group_name = self.parse_charset_name()?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Name {
                        qb_name,
                        name: group_name,
                    },
                })
            }
            "QUERY_TYPE" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                if !self.peek().text.eq_ignore_ascii_case("OLAP")
                    && !self.peek().text.eq_ignore_ascii_case("OLTP")
                {
                    return Err(self.err_here("expected OLAP or OLTP"));
                }
                let value = self.bump().text.to_ascii_uppercase();
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Keyword { qb_name, value },
                })
            }
            "MEMORY_QUOTA" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                if self.peek().kind != TokenKind::IntLit {
                    return Err(self.err_here("expected memory quota integer"));
                }
                let value = self
                    .bump()
                    .text
                    .parse::<u64>()
                    .map_err(|_| self.err_here("invalid memory quota"))?;
                let multiplier = if self.peek().text.eq_ignore_ascii_case("MB") {
                    self.bump();
                    1_048_576_u64
                } else if self.peek().text.eq_ignore_ascii_case("GB") {
                    self.bump();
                    1_073_741_824_u64
                } else {
                    return Err(self.err_here("expected MB or GB"));
                };
                let bytes = value
                    .checked_mul(multiplier)
                    .and_then(|bytes| i64::try_from(bytes).ok())
                    .ok_or_else(|| self.err_here("memory quota overflow"))?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::MemoryQuota { qb_name, bytes },
                })
            }
            "TIME_RANGE" => {
                self.expect_op("(")?;
                if self.peek().kind != TokenKind::Str {
                    return Err(self.err_here("expected TIME_RANGE start string"));
                }
                let from = decode_string(&self.bump().text);
                self.expect_op(",")?;
                if self.peek().kind != TokenKind::Str {
                    return Err(self.err_here("expected TIME_RANGE end string"));
                }
                let to = decode_string(&self.bump().text);
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::TimeRange { from, to },
                })
            }
            // `NAME([@qb_name] N)` — an OPTIONAL leading query-block name
            // before a mandatory integer, read directly from real TiDB's
            // own `pkg/parser/hintparser.go`: `parseMaxExecTimeHint`/
            // `parseNthPlanHint` both call the SAME shared `parseQBName()`
            // immediately after `(`, matching `parse_hint_table`'s own
            // `@qb_name` detection (`TokenKind::UserVar`), just in the
            // PREFIX position instead of the suffix — see
            // `tidb_ast::HintKind::Number`'s own doc.
            "MAX_EXECUTION_TIME" | "NTH_PLAN" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                if self.peek().kind != TokenKind::IntLit {
                    return Err(self.err_here("expected an integer hint argument"));
                }
                let value: i64 = self
                    .bump()
                    .text
                    .parse()
                    .map_err(|_| self.err_here("invalid integer hint argument"))?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Number { qb_name, value },
                })
            }
            // `QB_NAME(name [, ViewNameList])` — the optional path is
            // dot-separated `name[@sel_N]` or bare `@sel_N` entries.
            // This must not reuse `parse_hint_table`: its `db.table`
            // handling would consume the ViewNameList separator. See
            // `pkg/parser/hintparser.go`'s `parseQBNameHint` and
            // `tidb_ast::HintKind::QbName` for the typed/restoration
            // contract.
            "QB_NAME" => {
                self.expect_op("(")?;
                let qb_name = match self.peek().kind {
                    TokenKind::Ident | TokenKind::Keyword => self.bump().text,
                    TokenKind::CharsetIntroducer => {
                        let token = self.bump();
                        self.source[token.offset..token.end_offset].to_owned()
                    }
                    TokenKind::BitLit
                        if self.peek().text.to_ascii_lowercase().starts_with("0b") =>
                    {
                        self.bump().text
                    }
                    TokenKind::BitLit => {
                        return Err(self.err_here("Cannot use bit-value literal"));
                    }
                    TokenKind::HexLit
                        if self.peek().text.to_ascii_lowercase().starts_with("0x") =>
                    {
                        self.bump().text
                    }
                    TokenKind::HexLit => {
                        return Err(self.err_here("Cannot use hexadecimal literal"));
                    }
                    TokenKind::DecLit | TokenKind::FloatLit => {
                        return Err(self.err_here("Cannot use decimal number"));
                    }
                    _ => return Err(self.err_here("expected a query-block name")),
                };
                let mut views = Vec::new();
                if self.is_op(",") {
                    self.bump();
                    loop {
                        views.push(self.parse_qb_name_view()?);
                        if !self.is_op(".") {
                            break;
                        }
                        self.bump();
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::QbName { qb_name, views },
                })
            }
            // `READ_FROM_STORAGE([@qb] STORE[t, ...], STORE2[t2, ...],
            // ...)` — see `tidb_ast::HintKind::ReadFromStorage`'s own
            // doc for the exact restore shape. Real TiDB's own
            // `parseStorageHint` treats an unrecognized store name (not
            // `TIKV`/`TIFLASH`) as a silent-drop-the-rest case (its own
            // `default:` arm skips to the close paren and returns
            // whatever groups were already built) — NOT replicated
            // here, since it's a genuinely obscure malformed-input edge
            // case with zero corpus coverage to verify against; this
            // project's own general `ParseError`-over-silent-drop
            // convention applies instead (see [`tidb_ast::Hint`]'s own
            // doc), the SAME choice already made for `LEADING()`'s own
            // empty-table-list case.
            "READ_FROM_STORAGE" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let mut groups = Vec::new();
                loop {
                    if !matches!(self.peek().kind, TokenKind::Ident | TokenKind::Keyword) {
                        return Err(self.err_here("expected TIKV or TIFLASH"));
                    }
                    let store = self.bump().text.to_ascii_uppercase();
                    if store != "TIKV" && store != "TIFLASH" {
                        return Err(self.err_here("expected TIKV or TIFLASH"));
                    }
                    // The bracketed table list is OPTIONAL — real TiDB's
                    // own `parseStorageHint` only enters it via `if
                    // hp.match('[')`, so a bare `TIKV` with no list at
                    // all is also valid grammar (not exercised by the
                    // corpus, but cheap to mirror exactly since it falls
                    // straight out of the same `if`).
                    let mut tables = Vec::new();
                    if self.is_op("[") {
                        self.bump();
                        tables.push(self.parse_hint_table()?);
                        while self.is_op(",") {
                            self.bump();
                            tables.push(self.parse_hint_table()?);
                        }
                        self.expect_op("]")?;
                    }
                    groups.push((store, tables));
                    if !self.is_op(",") {
                        break;
                    }
                    self.bump();
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::ReadFromStorage { qb_name, groups },
                })
            }
            "STREAM_AGG"
            | "HASH_AGG"
            | "MPP_1PHASE_AGG"
            | "MPP_2PHASE_AGG"
            | "AGG_TO_COP"
            | "NO_DECORRELATE"
            | "NO_INDEX_MERGE"
            | "IGNORE_PLAN_CACHE"
            | "LIMIT_TO_COP"
            | "USE_PLAN_CACHE"
            | "SEMI_JOIN_REWRITE"
            | "STRAIGHT_JOIN"
            | "READ_CONSISTENT_REPLICA" => {
                // The parens are optional; when present they may contain
                // one query-block name. Restore always shows the parens;
                // see `tidb_ast::HintKind::Nullary`.
                let qb_name = if self.is_op("(") {
                    self.bump();
                    let qb_name = if self.peek().kind == TokenKind::UserVar {
                        Some(decode_at_name(&self.bump().text))
                    } else {
                        None
                    };
                    self.expect_op(")")?;
                    qb_name
                } else {
                    None
                };
                Ok(Hint {
                    name,
                    kind: HintKind::Nullary { qb_name },
                })
            }
            _ => Err(self.err_here("unsupported optimizer hint")),
        }
    }

    /// Parses one hint's table argument: `name [@qb_name]` (see
    /// [`tidb_ast::HintTable`]'s own doc — no partition list, no alias,
    /// unlike a `FROM`-clause [`TableRef`]). The query-block suffix lexes
    /// as a `UserVar` token (`@name`, indistinguishable at the token-kind
    /// level from `@@name`). Decode its payload with the shared `@`-name
    /// helper so bare, quoted, and escaped query-block names all reach the
    /// AST as logical names before restore, matching Go's hint lexer.
    fn parse_hint_table(&mut self) -> PResult<HintTable> {
        let mut name = self.parse_charset_name()?;
        // An optional `db.table` schema qualifier — read directly from
        // `pkg/parser/hintparser.go`'s own `parseHintTable`, which
        // checks for a `.` immediately after the first identifier
        // before ever considering the `@qb_name` suffix below. Every
        // OTHER hint table list in the real-TiDB integration-test
        // corpus this project measures coverage against only ever uses
        // unqualified names, so `db_name` stays `None` there — this is
        // exercised only via `HintKind::ReadFromStorage`'s own corpus
        // target (`` READ_FROM_STORAGE(TIKV[`s`.`t`]) ``).
        let db_name = if self.is_op(".") {
            self.bump();
            let table = self.parse_charset_name()?;
            Some(std::mem::replace(&mut name, table))
        } else {
            None
        };
        let qb_name = if self.peek().kind == TokenKind::UserVar {
            Some(decode_at_name(&self.bump().text))
        } else {
            None
        };
        let mut partitions = Vec::new();
        if self.is_kw("PARTITION") {
            self.bump();
            self.expect_op("(")?;
            partitions.push(self.parse_charset_name()?);
            while self.is_op(",") {
                self.bump();
                partitions.push(self.parse_charset_name()?);
            }
            self.expect_op(")")?;
        }
        Ok(HintTable {
            db_name,
            name,
            qb_name,
            partitions,
        })
    }

    /// Parses one recursive Go `LeadingList` element: either a plain hint
    /// table or a parenthesized nested list. The nested shape is required for
    /// `LEADING((t1, t2), sub)` and restores with its parentheses intact.
    fn parse_leading_element(&mut self) -> PResult<LeadingElement> {
        if self.is_op("(") {
            self.bump();
            let mut elements = vec![self.parse_leading_element()?];
            while self.is_op(",") {
                self.bump();
                elements.push(self.parse_leading_element()?);
            }
            self.expect_op(")")?;
            Ok(LeadingElement::Group(elements))
        } else {
            Ok(LeadingElement::Table(self.parse_hint_table()?))
        }
    }

    /// Parses one `QB_NAME` ViewNameList entry: `name [@sel_N]` or bare
    /// `@sel_N`. Unlike a general hint-table argument, a dot after this
    /// entry belongs to the ViewNameList itself rather than a schema
    /// qualifier.
    fn parse_qb_name_view(&mut self) -> PResult<HintTable> {
        if self.peek().kind == TokenKind::UserVar {
            return Ok(HintTable {
                db_name: None,
                name: String::new(),
                qb_name: Some(decode_at_name(&self.bump().text)),
                partitions: Vec::new(),
            });
        }
        let name = self.parse_charset_name()?;
        let qb_name = if self.peek().kind == TokenKind::UserVar {
            Some(decode_at_name(&self.bump().text))
        } else {
            None
        };
        Ok(HintTable {
            db_name: None,
            name,
            qb_name,
            partitions: Vec::new(),
        })
    }

    /// Skips an optional `(...)` argument group — used by
    /// `parse_hint_comment` when dropping a hint occurrence whose name
    /// real TiDB either doesn't recognize at all, or recognizes but
    /// always treats as unsupported (see that function's own doc).
    /// Depth-tracks nested parens so a paren-heavy argument list (were
    /// one ever present) is skipped past its own true matching close,
    /// not just the first `)` seen.
    fn skip_hint_args(&mut self) {
        if !self.is_op("(") {
            return;
        }
        self.bump(); // (
        let mut depth = 1i32;
        while depth > 0 && !self.at_eof() {
            if self.is_op("(") {
                depth += 1;
            } else if self.is_op(")") {
                depth -= 1;
            }
            self.bump();
        }
    }

    /// Parses a `SET_VAR` hint's own value: a string literal (decoded),
    /// an integer/decimal literal (raw text), an optionally-signed
    /// integer/decimal, or a bare identifier/keyword (`SET_VAR(x=on)`,
    /// `SET_VAR(x=legacy)`) — covering every shape found in real TiDB's
    /// own integration-test corpus. Restore always re-quotes the result
    /// as a string regardless of which of these it came from (see
    /// [`tidb_ast::HintKind::SetVar`]'s own doc), so the exact original
    /// shape doesn't need to be preserved past this point.
    fn parse_hint_value(&mut self) -> PResult<String> {
        match self.peek().kind {
            TokenKind::Str => Ok(decode_string(&self.bump().text)),
            TokenKind::IntLit => {
                let value = self.bump().text;
                value
                    .parse::<u64>()
                    .map(|_| value)
                    .map_err(|_| self.err_here("integer value is out of range"))
            }
            TokenKind::DecLit => {
                let value = self.bump().text;
                if !value.contains(['.', 'e', 'E']) && value.parse::<u64>().is_err() {
                    Err(self.err_here("integer value is out of range"))
                } else {
                    Ok(value)
                }
            }
            TokenKind::FloatLit => Ok(self.bump().text),
            TokenKind::Ident | TokenKind::Keyword => Ok(self.bump().text),
            TokenKind::Op if self.is_op("-") || self.is_op("+") => {
                let sign = self.bump().text;
                match self.peek().kind {
                    TokenKind::IntLit | TokenKind::DecLit | TokenKind::FloatLit => {
                        let digits = self.bump().text;
                        Ok(if sign == "-" {
                            format!("-{digits}")
                        } else {
                            digits
                        })
                    }
                    _ => Err(self.err_here("expected a number after +/- in hint value")),
                }
            }
            _ => Err(self.err_here("expected a SET_VAR hint value")),
        }
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
    fn parse_opt_into_outfile(&mut self) -> PResult<Option<tidb_ast::SelectIntoOption>> {
        if !self.is_kw("INTO") {
            return Ok(None);
        }
        self.bump();
        self.expect_kw("OUTFILE")?;
        if self.peek().kind != TokenKind::Str {
            return Err(self.err_here("expected a string literal after INTO OUTFILE"));
        }
        let file_name = decode_string(&self.bump().text);
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
        let (field, text) = self.parse_select_field()?;
        fields.push_with_text(field, text);
        while self.is_op(",") {
            self.bump();
            let (field, text) = self.parse_select_field()?;
            fields.push_with_text(field, text);
        }
        Ok(fields)
    }

    fn parse_select_field(&mut self) -> PResult<(SelectField, Vec<u8>)> {
        let start = self.peek().offset;
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
            let expr = self.parse_expr(prec::NONE)?;
            let alias = self.parse_opt_alias()?;
            SelectField::Expr { expr, alias }
        };
        let end = self.peek().offset;
        let text = if end > start {
            self.source[start..end].trim().as_bytes().to_vec()
        } else {
            Vec::new()
        };
        Ok((field, text))
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

    /// Parses an optional `AS name` or bare `name` alias. An alias name
    /// — whether introduced by `AS` or bare — accepts a string literal,
    /// a plain identifier, or MOST keywords, the SAME acceptance rule
    /// either way (read directly from Go's `CanBeImplicitAlias`; confirmed
    /// by the EXPLAIN corpus's `AS 'PROFIT'` scalar fields). See
    /// [`Parser::can_be_alias_name`]'s own doc for the exact, curated
    /// keyword exclusion list this mirrors.
    fn parse_opt_alias(&mut self) -> PResult<Option<String>> {
        if self.is_kw("AS") {
            self.bump();
            return Ok(Some(self.parse_alias_name()?));
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
            return Ok(Some(self.parse_alias_name()?));
        }
        if self.peek().kind != TokenKind::Str && self.can_be_alias_name() {
            return Ok(Some(self.parse_alias_name()?));
        }
        Ok(None)
    }

    /// Reports whether the CURRENT token is eligible as an alias name —
    /// shared by [`Parser::parse_opt_alias`]'s own `AS name` and bare
    /// `name` branches, see that function's own doc.
    fn can_be_alias_name(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Ident | TokenKind::Str)
            || (self.peek().kind == TokenKind::Keyword
                && !is_alias_excluded_keyword(&self.peek().text))
    }

    /// Parses an alias name that MUST be present (the `AS name` form —
    /// unlike the bare form, `AS` alone with nothing eligible following
    /// it is a genuine `ParseError`, not a silently-absent alias).
    fn parse_alias_name(&mut self) -> PResult<String> {
        if self.can_be_alias_name() {
            let name = self.bump();
            Ok(if name.kind == TokenKind::Str {
                decode_string(&name.text)
            } else {
                crate::normalize_identifier(name.text)
            })
        } else {
            Err(self.err_here("expected identifier"))
        }
    }

    /// Parses a plain table reference with an optional `PARTITION
    /// (...)` clause, alias, and any trailing `USE`/`FORCE`/`IGNORE
    /// INDEX` hints — in that exact order (confirmed via `godump
    /// restore`: `PARTITION` comes BEFORE the alias, hints come AFTER
    /// it). Also used by `crate::dml`'s single-table `UPDATE`/`DELETE`
    /// (confirmed via `godump restore` that both accept `PARTITION`/
    /// index hints too, not just `SELECT`'s own `FROM` clause).
    pub(crate) fn parse_table_ref(&mut self) -> PResult<TableRef> {
        let name = self.parse_table_name_path()?;
        let partitions = self.parse_partition_opt()?;
        // `AS OF TIMESTAMP expr` and a plain alias are mutually exclusive
        // at this SAME grammar position (confirmed via `godump restore`,
        // in either order) — see `tidb_ast::TableRef::as_of`'s own doc.
        // `tidb_lexer` already merges `AS OF` into ONE keyword token
        // (`"AS OF"`, matching real TiDB's own lexer-level `AS OF`/
        // `MEMBER OF` two-word merges), so this checks for that single
        // token, not two.
        let (alias, as_of) = if self.is_kw("AS OF") {
            self.bump(); // AS OF
            self.expect_kw("TIMESTAMP")?;
            (None, Some(Box::new(self.parse_expr(prec::NONE)?)))
        } else {
            (self.parse_opt_table_alias()?, None)
        };
        let mut hints = Vec::new();
        while self.is_kw("USE") || self.is_kw("FORCE") || self.is_kw("IGNORE") {
            hints.push(self.parse_index_hint()?);
        }
        let sample = self.parse_table_sample_opt()?;
        Ok(TableRef {
            name,
            partitions,
            alias,
            as_of,
            hints,
            sample,
        })
    }

    /// Parses the table-name grammar used by a `FROM` table source. This is
    /// intentionally narrower than a generic name path: TiDB's
    /// `parseTableName` has one table-only exception, `*.table`, used by
    /// wildcard bindings. Keeping it at this boundary prevents `*` from
    /// becoming a general identifier in DDL or expression paths.
    fn parse_table_name_path(&mut self) -> PResult<Vec<String>> {
        if self.is_op("*") && self.is_op_at(1, ".") && is_name_or_keyword(self.peek_n(2)) {
            self.bump(); // wildcard schema
            self.bump(); // '.'
            return Ok(vec!["*".to_owned(), self.parse_name_or_keyword()?]);
        }
        self.parse_name_path()
    }

    /// Parses an optional `TABLESAMPLE [SYSTEM|BERNOULLI|REGION|REGIONS]
    /// (expr [PERCENT|ROWS]) [REPEATABLE(seed)]` clause — see
    /// `tidb_ast::TableSample`'s own doc. Mirrors real TiDB's own
    /// hand-written parser (`pkg/parser/join_parser.go`)'s inline handling
    /// exactly, including its ordering: after any index hints.
    fn parse_table_sample_opt(&mut self) -> PResult<Option<TableSample>> {
        if !self.is_kw("TABLESAMPLE") {
            return Ok(None);
        }
        self.bump();
        let method = if self.is_kw("SYSTEM") {
            self.bump();
            Some(SampleMethod::System)
        } else if self.is_kw("BERNOULLI") {
            self.bump();
            Some(SampleMethod::Bernoulli)
        } else if self.is_kw("REGION") || self.is_kw("REGIONS") {
            self.bump();
            Some(SampleMethod::Region)
        } else {
            None
        };
        self.expect_op("(")?;
        let (expr, unit) = if self.is_op(")") {
            (None, None)
        } else {
            let expr = self.parse_expr(prec::NONE)?;
            let unit = if self.is_kw("PERCENT") {
                self.bump();
                Some(SampleUnit::Percent)
            } else if self.is_kw("ROWS") {
                self.bump();
                Some(SampleUnit::Rows)
            } else {
                None
            };
            (Some(Box::new(expr)), unit)
        };
        self.expect_op(")")?;
        let repeatable = if self.is_kw("REPEATABLE") {
            self.bump();
            self.expect_op("(")?;
            let seed = self.parse_expr(prec::NONE)?;
            self.expect_op(")")?;
            Some(Box::new(seed))
        } else {
            None
        };
        Ok(Some(TableSample {
            method,
            expr,
            unit,
            repeatable,
        }))
    }

    /// Parses ONE `USE`/`FORCE`/`IGNORE INDEX [FOR JOIN|ORDER BY|GROUP
    /// BY] (name, ...)` hint — a complete, independent unit (the caller's
    /// own loop handles multiple stacked hints; a scope qualifier cannot
    /// be chained onto a PRIOR hint without repeating its own
    /// `USE`/`FORCE`/`IGNORE INDEX` keyword, confirmed via `godump
    /// restore` to be a genuine `ParseError` otherwise). `INDEX`/`KEY`
    /// are true synonyms (confirmed via `godump restore`: both
    /// normalize to `INDEX`); the name list may be EMPTY (`USE INDEX
    /// ()`, real MySQL grammar) and each name may be a keyword-shaped
    /// identifier (`primary`, `key`, `asc`, ... all confirmed valid),
    /// or a quoted string literal (`USE INDEX ('idx')`, which the Go
    /// hand-parser accepts because it consumes the next token without a
    /// token-kind check).  Unquoting that literal here preserves Go AST
    /// restore's canonical backquoted index name.  Other identifier-like
    /// names reuse [`Parser::parse_charset_name`]'s own broader
    /// Ident-or-Keyword acceptance.
    fn parse_index_hint(&mut self) -> PResult<tidb_ast::IndexHint> {
        let kind = if self.is_kw("USE") {
            self.bump();
            tidb_ast::IndexHintKind::Use
        } else if self.is_kw("FORCE") {
            self.bump();
            tidb_ast::IndexHintKind::Force
        } else {
            self.bump(); // IGNORE
            tidb_ast::IndexHintKind::Ignore
        };
        if self.is_kw("INDEX") || self.is_kw("KEY") {
            self.bump();
        } else {
            return Err(self.err_here("expected INDEX or KEY"));
        }
        let scope = if self.is_kw("FOR") {
            self.bump();
            if self.is_kw("JOIN") {
                self.bump();
                tidb_ast::IndexHintScope::Join
            } else if self.is_kw("ORDER") {
                self.bump();
                self.expect_kw("BY")?;
                tidb_ast::IndexHintScope::OrderBy
            } else if self.is_kw("GROUP") {
                self.bump();
                self.expect_kw("BY")?;
                tidb_ast::IndexHintScope::GroupBy
            } else {
                return Err(self.err_here("expected JOIN, ORDER BY, or GROUP BY"));
            }
        } else {
            tidb_ast::IndexHintScope::All
        };
        self.expect_op("(")?;
        let mut indexes = Vec::new();
        if !self.is_op(")") {
            indexes.push(self.parse_index_hint_name()?);
            while self.is_op(",") {
                self.bump();
                indexes.push(self.parse_index_hint_name()?);
            }
        }
        self.expect_op(")")?;
        Ok(tidb_ast::IndexHint {
            kind,
            scope,
            indexes,
        })
    }

    /// Parses one table-level index-hint name.  Go's `parseIndexHint`
    /// consumes the next token directly rather than requiring an identifier,
    /// so a quoted string is accepted and restored as a backquoted index name
    /// (`USE INDEX ('idx')` -> `USE INDEX (`idx`)`).  Keep this widening local
    /// to table hints: other charset/name positions intentionally retain
    /// their narrower token contracts.
    fn parse_index_hint_name(&mut self) -> PResult<String> {
        if self.peek().kind == TokenKind::Str {
            return Ok(decode_string(&self.bump().text));
        }
        self.parse_charset_name()
    }

    /// Parses the `FROM` clause into a join tree matching the Go AST's
    /// `TableRefs` grammar: a bare single table is wrapped as
    /// `Join { right: None }`, an explicit join is used directly, and each comma
    /// nests the accumulated `TableRefs` on the left.
    pub(crate) fn parse_from(&mut self) -> PResult<Join> {
        self.parse_from_with_comma().map(|(join, _)| join)
    }

    /// Parses a `TableRefs` join tree and records whether its outer grammar
    /// used a comma. UPDATE needs that distinction because TiDB accepts its
    /// `ORDER BY`/`LIMIT` tail for explicit JOINs but rejects it for the
    /// comma-separated multi-table form.
    pub(crate) fn parse_from_with_comma(&mut self) -> PResult<(Join, bool)> {
        // The first `EscapedTableRef`: use a join directly, otherwise wrap a
        // bare table.
        let mut refs = match self.parse_join_table()? {
            JoinNode::Join(j) => *j,
            // A bare table or derived-table leaf is wrapped as a single-table
            // `TableRefs` node.
            leaf => Join {
                left: leaf,
                right: None,
                tp: JoinType::Cross,
                straight: false,
                on: None,
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            },
        };
        let mut has_comma = false;
        while self.is_op(",") {
            has_comma = true;
            self.bump();
            let right = self.parse_join_table()?;
            refs = Join {
                left: JoinNode::Join(Box::new(refs)),
                right: Some(right),
                tp: JoinType::Cross,
                straight: false,
                on: None,
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            };
        }
        Ok((refs, has_comma))
    }

    /// Parses one `EscapedTableRef`: a table factor followed by zero or more
    /// explicit joins (`t`, `t1 JOIN t2 ON ...`, `t1 NATURAL JOIN t2`),
    /// left-associatively. Leaves are bare tables (no single-table wrapper).
    fn parse_join_table(&mut self) -> PResult<JoinNode> {
        let first = self.parse_table_factor()?;
        self.parse_join_tail(first)
    }

    /// Continues a join chain from an already parsed left factor. The right
    /// side is parsed recursively so stacked `ON` clauses bind inside-out,
    /// exactly like `pkg/parser/join_parser.go`.
    fn parse_join_tail(&mut self, mut node: JoinNode) -> PResult<JoinNode> {
        loop {
            // `NATURAL` may only precede a plain/`LEFT`/`RIGHT` join — an
            // explicit `INNER`/`CROSS`/`STRAIGHT_JOIN` right after it is a
            // genuine `ParseError` (confirmed via `godump restore`), even
            // though bare `NATURAL JOIN` (no `INNER` prefix) shares the
            // SAME `JoinType::Cross` a plain `INNER`/`CROSS`/bare `JOIN`
            // uses.
            let natural = self.is_kw("NATURAL");
            if natural {
                self.bump();
                if self.is_kw("INNER") || self.is_kw("CROSS") || self.is_kw("STRAIGHT_JOIN") {
                    return Err(
                        self.err_here("NATURAL cannot be combined with INNER/CROSS/STRAIGHT_JOIN")
                    );
                }
            }
            let Some((tp, straight)) = self.peek_join_kind() else {
                if natural {
                    return Err(self.err_here("expected JOIN after NATURAL"));
                }
                break;
            };
            self.consume_join_kind();
            let right = if natural || straight {
                self.parse_table_factor()?
            } else {
                self.parse_join_rhs()?
            };
            node = self.apply_join_condition(node, right, tp, natural, straight)?;
        }
        Ok(node)
    }

    /// Parses the recursive right side of a keyword join. Each recursion owns
    /// one following `ON`/`USING`, yielding the same right-leaning tree as Go.
    fn parse_join_rhs(&mut self) -> PResult<JoinNode> {
        let first = self.parse_table_factor()?;
        self.parse_join_tail(first)
    }

    fn apply_join_condition(
        &mut self,
        left: JoinNode,
        right: JoinNode,
        tp: JoinType,
        natural: bool,
        straight: bool,
    ) -> PResult<JoinNode> {
        if natural {
            return Ok(JoinNode::Join(Box::new(Join {
                left,
                right: Some(right),
                tp,
                straight: false,
                on: None,
                using: Vec::new(),
                natural: true,
                explicit_parens: false,
            })));
        }
        let (on, using) = self.parse_join_cond()?;
        if on.is_none() && using.is_empty() {
            if matches!(tp, JoinType::Left | JoinType::Right) {
                return Err(self.err_here("LEFT/RIGHT JOIN requires ON or USING"));
            }
            if !straight {
                return Ok(Self::make_cross_join(left, right));
            }
        }
        Ok(JoinNode::Join(Box::new(Join {
            left,
            right: Some(right),
            tp,
            straight,
            on,
            using,
            natural: false,
            explicit_parens: false,
        })))
    }

    /// Rotates an unqualified cross join into the left edge of an already
    /// right-leaning subtree, transcreated from Go's `makeCrossJoin`.
    fn make_cross_join(left: JoinNode, right: JoinNode) -> JoinNode {
        fn insert_leftmost(mut join: Box<Join>, left: JoinNode) -> Box<Join> {
            join.left = match join.left {
                JoinNode::Join(child) if child.right.is_some() => {
                    JoinNode::Join(insert_leftmost(child, left))
                }
                old_left => JoinNode::Join(Box::new(Join {
                    left,
                    right: Some(old_left),
                    tp: JoinType::Cross,
                    straight: false,
                    on: None,
                    using: Vec::new(),
                    natural: false,
                    explicit_parens: false,
                })),
            };
            join
        }

        match right {
            JoinNode::Join(join) if join.right.is_some() && !join.explicit_parens => {
                JoinNode::Join(insert_leftmost(join, left))
            }
            right => JoinNode::Join(Box::new(Join {
                left,
                right: Some(right),
                tp: JoinType::Cross,
                straight: false,
                on: None,
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            })),
        }
    }

    /// Parses one table factor: a derived table `(SELECT ...) [AS] alias`
    /// — `SELECT ...` may itself be a `UNION`/`EXCEPT`/`INTERSECT`-joined
    /// set operation (`parse_select_or_setopr`, the SAME function a CTE's
    /// own definition uses — see [`tidb_ast::JoinNode::Derived`]'s own
    /// doc), possibly with its own parenthesized terms
    /// (`((SELECT ...) UNION (SELECT ...)) alias`, needing
    /// [`Parser::looks_like_derived_table`]'s own multi-`(` lookahead to
    /// even recognize as a derived table at all, since a set operation's
    /// first TERM may independently be parenthesized before this table
    /// factor's own wrapping paren is seen) — or a plain table reference.
    fn parse_table_factor(&mut self) -> PResult<JoinNode> {
        // `LATERAL (subquery) [AS] alias [(col, ...)]` — checked first
        // since the keyword unambiguously marks this shape (unlike a plain
        // derived table, which needs `looks_like_derived_table`'s own
        // lookahead to disambiguate from a parenthesized join group). Real
        // TiDB's own `parseLateralTableSource` (`pkg/parser/
        // join_parser.go`) allows the SAME `SELECT`/`WITH`/`VALUES`/`(`
        // start tokens `parse_select_or_setopr` already accepts — no
        // extra multi-paren lookahead needed here since the `(` right
        // after `LATERAL` is unambiguous.
        if self.is_kw("LATERAL") {
            self.bump();
            self.expect_op("(")?;
            let query_start = self.peek().offset;
            let subquery = self.parse_derived_query_payload()?;
            let query_end = self.peek().offset;
            let mut subquery = tidb_ast::NodeBox::new(subquery);
            if query_end > query_start {
                subquery.set_text(
                    None,
                    self.source.as_bytes()[query_start..query_end].to_vec(),
                );
            }
            self.expect_op(")")?;
            if self.is_kw("AS") {
                self.bump();
            }
            let alias = self.parse_name()?;
            let mut column_names = Vec::new();
            if self.is_op("(") {
                self.bump();
                column_names.push(self.parse_name()?);
                while self.is_op(",") {
                    self.bump();
                    column_names.push(self.parse_name()?);
                }
                self.expect_op(")")?;
            }
            return Ok(JoinNode::Derived {
                subquery,
                alias: Some(alias),
                lateral: true,
                column_names,
            });
        }
        // Go's `parseTableSource` has a distinct structural-parenthesis
        // branch for `((SELECT ...) alias JOIN ...)`. The outer parens group
        // a join whose FIRST factor is a derived table; they are not another
        // layer of the derived query itself. Parse that first factor through
        // the existing typed `JoinNode::Derived` path, then continue its
        // normal join chain before consuming the outer close paren. Without
        // this distinction the generic derived-table branch below consumes
        // the outer `(` and incorrectly expects `)` before the alias/join.
        if self.starts_parenthesized_derived_join() {
            self.bump(); // structural outer `(`
            let grouped = self.parse_join_table()?;
            self.expect_op(")")?;
            let mut grouped = match grouped {
                JoinNode::Join(join) => join,
                leaf => Box::new(Join {
                    left: leaf,
                    right: None,
                    tp: JoinType::Cross,
                    straight: false,
                    on: None,
                    using: Vec::new(),
                    natural: false,
                    explicit_parens: false,
                }),
            };
            grouped.explicit_parens = true;
            return Ok(JoinNode::Join(grouped));
        }
        if self.looks_like_derived_table() {
            self.bump(); // (
            let query_start = self.peek().offset;
            let subquery = self.parse_derived_query_payload()?;
            let query_end = self.peek().offset;
            let mut subquery = tidb_ast::NodeBox::new(subquery);
            if query_end > query_start {
                subquery.set_text(
                    None,
                    self.source.as_bytes()[query_start..query_end].to_vec(),
                );
            }
            self.expect_op(")")?;
            // Unlike `LATERAL`, a plain derived table's alias is
            // OPTIONAL (confirmed via `godump restore`: `SELECT * FROM
            // (SELECT 1)` alone, with no alias, parses and restores
            // unchanged) — the SAME `AS name`-or-bare-`name` grammar a
            // plain table reference already gets via `parse_table_ref`.
            let alias = self.parse_opt_table_alias()?;
            return Ok(JoinNode::Derived {
                subquery,
                alias,
                lateral: false,
                column_names: Vec::new(),
            });
        }
        // A parenthesized join: `(table_refs)` — a PURELY structural
        // grouping paren around a single table, a comma-joined list, or
        // an explicit `JOIN` chain (confirmed via `godump restore`: no
        // information is lost by unwrapping — `SELECT * FROM (t)`
        // restores as `SELECT * FROM \`t\``, the parens simply dropped,
        // and a nested case like `(t1 JOIN t2 USING (a)) JOIN (t3 JOIN
        // t4 USING (a)) ON (...)` restores with parens re-derived purely
        // from structure, the SAME rule `Join::restore_into`'s own
        // `use_comma_join` check already applies elsewhere). Read
        // directly from real TiDB's own hand-written parser
        // (`pkg/parser/join_parser.go`'s `parseTableSource`, the final
        // `else` branch: `join, _ := p.parseCommaJoin(); ... return
        // join` — no alias parsing at all after the closing paren,
        // unlike a derived table). Reuses `parse_from` (the SAME
        // top-level `FROM`-clause entry point), since real MySQL grammar
        // allows a comma-joined list inside these parens too, not just a
        // single explicit-`JOIN` chain.
        if self.is_op("(") {
            self.bump();
            let mut inner = self.parse_from()?;
            self.expect_op(")")?;
            inner.explicit_parens = true;
            return Ok(JoinNode::Join(Box::new(inner)));
        }
        Ok(JoinNode::Table(self.parse_table_ref()?))
    }

    /// Parses the query after a derived table's structural opening `(`.
    ///
    /// Consecutive whole-query wrappers collapse into the one pair owned by
    /// the table source. Only leading `((` pairs are peeled here: a single
    /// `(SELECT ...)` may instead be the first parenthesized term of a set
    /// operation, whose braces Go preserves.
    fn parse_derived_query_payload(&mut self) -> PResult<QueryStmt> {
        let mut query = if self.is_kw("WITH") {
            self.parse_with_select()?
        } else {
            self.parse_select_or_setopr()?
        };
        clear_derived_query_outer_braces(&mut query);
        Ok(query)
    }

    /// Reports whether the current position starts a derived table: `(`,
    /// then zero or more FURTHER `(` (each one a parenthesized set-
    /// operation TERM, not this table factor's own wrapper), then
    /// `SELECT` or `WITH`. A plain single-`(` lookahead (`is_kw_at(1, "SELECT")`,
    /// what every OTHER subquery position in this crate still uses) can't
    /// see past a first term that's independently parenthesized
    /// (`((SELECT ...) UNION (SELECT ...)) alias`) — this table-factor
    /// position specifically needs the deeper check since
    /// `parse_select_or_setopr`'s own `parse_one_term` already handles an
    /// individual term's own parens correctly once inside; only the
    /// OUTER gate here was too narrow. Whole-input tokenization (no
    /// bounded lookahead ring, unlike real TiDB's own streaming lexer
    /// bridge) makes this safe to do unconditionally.
    fn looks_like_derived_table(&self) -> bool {
        let mut n = 0;
        while self.peek_n(n).kind == TokenKind::Op && self.peek_n(n).text == "(" {
            n += 1;
        }
        n > 0 && (self.is_kw_at(n, "SELECT") || self.is_kw_at(n, "WITH"))
    }

    /// Whether the current tokens start Go's structural parenthesized-join
    /// form `((SELECT ...) alias JOIN ...)`. The immediate inner pair must
    /// contain a derived query, then be followed by an alias and a join before
    /// the outer `)`. Restricting this disambiguation to that source grammar
    /// keeps ordinary nested derived-table parentheses on their existing path.
    fn starts_parenthesized_derived_join(&self) -> bool {
        if !self.is_op("(") || !self.is_op_at(1, "(") {
            return false;
        }
        let mut depth = 0usize;
        let mut offset = 1usize;
        loop {
            let token = self.peek_n(offset);
            if token.kind == TokenKind::Eof {
                return false;
            }
            match token.text.as_str() {
                "(" => depth += 1,
                ")" => {
                    depth = match depth.checked_sub(1) {
                        Some(depth) => depth,
                        None => return false,
                    };
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            offset += 1;
        }
        // The factor after the immediate inner close is either `AS alias`
        // or a bare alias. This selected Go grammar then requires an explicit
        // join inside the still-open outer grouping paren.
        let alias_offset = if self.is_kw_at(offset + 1, "AS") {
            offset + 2
        } else {
            offset + 1
        };
        let alias = self.peek_n(alias_offset);
        if !(alias.kind == TokenKind::Ident
            || (alias.kind == TokenKind::Keyword && !is_alias_excluded_keyword(&alias.text)))
        {
            return false;
        }
        self.is_op_at(alias_offset + 1, ")")
            || self.is_kw_at(alias_offset + 1, "JOIN")
            || self.is_kw_at(alias_offset + 1, "INNER")
            || self.is_kw_at(alias_offset + 1, "CROSS")
            || self.is_kw_at(alias_offset + 1, "LEFT")
            || self.is_kw_at(alias_offset + 1, "RIGHT")
            || self.is_kw_at(alias_offset + 1, "NATURAL")
            || self.is_kw_at(alias_offset + 1, "STRAIGHT_JOIN")
    }

    /// Returns the join type and straight flag if the current position starts a
    /// join clause, without consuming tokens.
    fn peek_join_kind(&self) -> Option<(JoinType, bool)> {
        if self.is_kw("JOIN") || self.is_kw("INNER") || self.is_kw("CROSS") {
            Some((JoinType::Cross, false))
        } else if self.is_kw("STRAIGHT_JOIN") {
            Some((JoinType::Cross, true))
        } else if self.is_kw("LEFT") {
            Some((JoinType::Left, false))
        } else if self.is_kw("RIGHT") {
            Some((JoinType::Right, false))
        } else {
            None
        }
    }

    /// Consumes a join-kind prefix: `JOIN`, `INNER JOIN`, `CROSS JOIN`,
    /// `STRAIGHT_JOIN`, or `{LEFT|RIGHT} [OUTER] JOIN`.
    fn consume_join_kind(&mut self) {
        if self.is_kw("STRAIGHT_JOIN") {
            self.bump();
            return;
        }
        if self.is_kw("INNER") || self.is_kw("CROSS") {
            self.bump();
        } else if self.is_kw("LEFT") || self.is_kw("RIGHT") {
            self.bump();
            if self.is_kw("OUTER") {
                self.bump();
            }
        }
        // The trailing JOIN keyword (present for every form except STRAIGHT_JOIN).
        if self.is_kw("JOIN") {
            self.bump();
        }
    }

    /// Parses an optional `ON expr` or `USING (col, ...)` join condition.
    fn parse_join_cond(&mut self) -> PResult<(Option<Expr>, Vec<String>)> {
        if self.is_kw("ON") {
            self.bump();
            Ok((Some(self.parse_expr(prec::NONE)?), Vec::new()))
        } else if self.is_kw("USING") {
            self.bump();
            self.expect_op("(")?;
            let mut names = vec![self.parse_name()?];
            while self.is_op(",") {
                self.bump();
                names.push(self.parse_name()?);
            }
            self.expect_op(")")?;
            Ok((None, names))
        } else {
            Ok((None, Vec::new()))
        }
    }

    /// Parses `(SELECT ...)`/`(WITH ... SELECT ...)` in a subquery position
    /// that preserves a top-level `UNION`/`EXCEPT`/`INTERSECT`. TiDB's Go
    /// `parseExistsSubquery` uses the general `parseSubquery` path, so an
    /// every scalar, `EXISTS`, `IN`, and `ANY`/`ALL` body can retain a set
    /// operation without narrowing to one select term.
    pub(crate) fn parse_query_subquery(&mut self) -> PResult<tidb_ast::NodeBox<QueryStmt>> {
        self.expect_op("(")?;
        let start = self.peek().offset;
        let mut query = if self.is_kw("WITH") {
            self.parse_with_select()?
        } else {
            self.parse_select_or_setopr()?
        };
        clear_derived_query_outer_braces(&mut query);
        let end = self.peek().offset;
        let mut query = tidb_ast::NodeBox::new(query);
        if end > start {
            query.set_text(None, self.source.as_bytes()[start..end].to_vec());
        }
        self.expect_op(")")?;
        Ok(query)
    }
}

fn clear_derived_query_outer_braces(query: &mut QueryStmt) {
    match query {
        QueryStmt::Select(select) => select.is_in_braces = false,
        QueryStmt::SetOpr(setopr) => setopr.is_in_braces = false,
    }
}

/// One warning or syntax error produced while parsing an optimizer-hint list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HintDiagnostic {
    /// Source-compatible diagnostic text.
    pub message: String,
}

/// Complete result of the standalone optimizer-hint parser.
#[derive(Debug, Clone, PartialEq)]
pub struct HintParseResult {
    /// Successfully parsed hints, in source order.
    pub hints: Vec<Hint>,
    /// Recoverable hint diagnostics, in source order.
    pub diagnostics: Vec<HintDiagnostic>,
}

/// Parses one complete `/*+ ... */` optimizer-hint comment.
///
/// This is the Rust-native equivalent of `pkg/parser.ParseHint`: malformed or
/// unsupported hint occurrences produce diagnostics and are skipped while
/// later occurrences remain parseable. `ansi_quotes` mirrors the only SQL-mode
/// bit consulted by the source hint scanner, and `initial_line` is retained in
/// syntax diagnostics for callers embedding a comment in a larger statement.
pub fn parse_hint(input: &str, ansi_quotes: bool, initial_line: usize) -> HintParseResult {
    let Some(inner) = input
        .strip_prefix("/*+")
        .and_then(|value| value.strip_suffix("*/"))
    else {
        return HintParseResult {
            hints: Vec::new(),
            diagnostics: vec![hint_syntax_diagnostic(initial_line)],
        };
    };

    let mut parser = Parser::new_hint_with_ansi_quotes(inner, ansi_quotes);
    let mut hints = Vec::new();
    let mut diagnostics = Vec::new();
    while !parser.at_eof() {
        let name = matches!(parser.peek().kind, TokenKind::Ident | TokenKind::Keyword)
            .then(|| parser.peek().text.to_ascii_uppercase());

        if let Some(name) = name.as_deref() {
            let source_name = parser.peek().text.clone();
            let unsupported = is_always_unsupported_hint_name(name)
                || (name == "JOIN_FIXED_ORDER" && parser.peek_n(1).text == "(");
            if unsupported {
                parser.bump();
                parser.skip_hint_args();
                diagnostics.push(HintDiagnostic {
                    message: format!(
                        "[parser:8061]Optimizer hint {source_name} is not supported by TiDB and is ignored"
                    ),
                });
            } else if !is_recognized_hint_token_name(name) {
                parser.bump();
                if parser.is_op("(") && parser.peek_n(1).text == ")" {
                    parser.bump();
                    parser.bump();
                    diagnostics.push(hint_syntax_diagnostic(initial_line));
                } else {
                    parser.skip_hint_args();
                    diagnostics.push(HintDiagnostic {
                        message: format!(
                            "[parser:8061]Optimizer hint {source_name} is not supported by TiDB and is ignored"
                        ),
                    });
                }
            } else {
                parse_standalone_hint_occurrence(
                    &mut parser,
                    &mut hints,
                    &mut diagnostics,
                    initial_line,
                );
            }
        } else {
            parse_standalone_hint_occurrence(
                &mut parser,
                &mut hints,
                &mut diagnostics,
                initial_line,
            );
        }

        if parser.is_op(",") {
            parser.bump();
        }
    }

    if hints.is_empty() && diagnostics.is_empty() {
        diagnostics.push(hint_syntax_diagnostic(initial_line));
    }
    HintParseResult { hints, diagnostics }
}

fn parse_standalone_hint_occurrence(
    parser: &mut Parser,
    hints: &mut Vec<Hint>,
    diagnostics: &mut Vec<HintDiagnostic>,
    initial_line: usize,
) {
    let start = parser.pos;
    match parser.parse_one_hint() {
        Ok(Hint {
            name,
            kind: HintKind::ReadFromStorage { qb_name, groups },
        }) => {
            hints.extend(groups.into_iter().map(|group| Hint {
                name: name.clone(),
                kind: HintKind::ReadFromStorage {
                    qb_name: qb_name.clone(),
                    groups: vec![group],
                },
            }));
        }
        Ok(hint) => hints.push(hint),
        Err(error) => {
            if matches!(
                error.message.as_str(),
                "Cannot use decimal number"
                    | "Cannot use bit-value literal"
                    | "Cannot use hexadecimal literal"
                    | "integer value is out of range"
            ) {
                diagnostics.push(HintDiagnostic {
                    message: error.message,
                });
            }
            diagnostics.push(hint_syntax_diagnostic(initial_line));
            parser.pos = start;
            parser.bump();
            parser.skip_hint_args();
        }
    }
}

fn hint_syntax_diagnostic(initial_line: usize) -> HintDiagnostic {
    HintDiagnostic {
        message: format!("Optimizer hint syntax error at line {initial_line} "),
    }
}

/// Parses a `/*+ ... */` hint comment token's own raw text (INCLUDING the
/// `/*+`/`*/` delimiters, exactly as `tidb_lexer` emits it for a
/// `TokenKind::HintComment` token) into its own hints. Re-lexes the inner
/// text with a fresh, fully self-contained nested [`Parser`] (see its own
/// `new`) rather than a bespoke hint-only lexer, reusing the SAME
/// token-cursor primitives (`peek`/`bump`/`is_kw`/`expect_op`/...) every
/// other parsing function in this crate already uses — real TiDB's own
/// hint grammar has its OWN dedicated ~1200-line lexer/parser
/// (`pkg/parser/hintparser.go`) covering roughly 30 distinct hint shapes;
/// this covers only the four shapes confirmed (via a stratified sample of
/// real TiDB's own integration-test corpus) to account for the
/// overwhelming majority of real-world hint usage — see
/// [`tidb_ast::Hint`]'s own doc for the exact scope boundary.
pub(crate) fn parse_hint_comment(text: &str, initial_line: usize) -> HintParseResult {
    parse_hint(text, false, initial_line)
}

/// Whether `name` (already uppercased) is one of the ~85 hint names real
/// TiDB's own lexer recognizes as a SPECIAL hint token (`hintTokenMap`,
/// `pkg/parser/misc.go`) — read directly from that map, not guessed.
/// Anything NOT in this list tokenizes as a generic `hintIdentifier`
/// there, which `parseOneHint`'s own `default:` case ALWAYS treats as
/// "warn and drop" — see `Parser::parse_hint_comment`'s own doc for how
/// this is used (a name real TiDB doesn't recognize at all can never
/// carry real content, by construction, so it's always safe to drop —
/// UNLIKE a name that simply isn't yet in THIS crate's own smaller
/// `parse_one_hint` dispatch, e.g. `READ_FROM_STORAGE`, which IS in
/// this list and so is deliberately left alone here, kept a
/// `ParseError` by `parse_one_hint`'s own `_ =>` arm instead).
fn is_recognized_hint_token_name(name: &str) -> bool {
    matches!(
        name,
        "JOIN_FIXED_ORDER"
            | "JOIN_ORDER"
            | "JOIN_PREFIX"
            | "JOIN_SUFFIX"
            | "BKA"
            | "NO_BKA"
            | "BNL"
            | "NO_BNL"
            | "HASH_JOIN"
            | "HASH_JOIN_BUILD"
            | "HASH_JOIN_PROBE"
            | "NO_HASH_JOIN"
            | "MERGE"
            | "NO_MERGE"
            | "INDEX_MERGE"
            | "NO_INDEX_MERGE"
            | "MRR"
            | "NO_MRR"
            | "NO_ICP"
            | "NO_RANGE_OPTIMIZATION"
            | "SKIP_SCAN"
            | "NO_SKIP_SCAN"
            | "SEMIJOIN"
            | "NO_SEMIJOIN"
            | "MAX_EXECUTION_TIME"
            | "SET_VAR"
            | "RESOURCE_GROUP"
            | "QB_NAME"
            | "HYPO_INDEX"
            | "AGG_TO_COP"
            | "LIMIT_TO_COP"
            | "IGNORE_PLAN_CACHE"
            | "WRITE_SLOW_LOG"
            | "HASH_AGG"
            | "MPP_1PHASE_AGG"
            | "MPP_2PHASE_AGG"
            | "IGNORE_INDEX"
            | "INL_HASH_JOIN"
            | "INDEX_HASH_JOIN"
            | "NO_INDEX_HASH_JOIN"
            | "INL_JOIN"
            | "INDEX_JOIN"
            | "NO_INDEX_JOIN"
            | "INL_MERGE_JOIN"
            | "INDEX_MERGE_JOIN"
            | "NO_INDEX_MERGE_JOIN"
            | "MEMORY_QUOTA"
            | "NO_SWAP_JOIN_INPUTS"
            | "QUERY_TYPE"
            | "READ_CONSISTENT_REPLICA"
            | "READ_FROM_STORAGE"
            | "BROADCAST_JOIN"
            | "SHUFFLE_JOIN"
            | "MERGE_JOIN"
            | "NO_MERGE_JOIN"
            | "STREAM_AGG"
            | "SWAP_JOIN_INPUTS"
            | "USE_INDEX_MERGE"
            | "USE_INDEX"
            | "ORDER_INDEX"
            | "NO_ORDER_INDEX"
            | "INDEX_LOOKUP_PUSHDOWN"
            | "NO_INDEX_LOOKUP_PUSHDOWN"
            | "USE_PLAN_CACHE"
            | "USE_TOJA"
            | "TIME_RANGE"
            | "USE_CASCADES"
            | "NTH_PLAN"
            | "FORCE_INDEX"
            | "STRAIGHT_JOIN"
            | "LEADING"
            | "SEMI_JOIN_REWRITE"
            | "NO_DECORRELATE"
            | "TIDB_HJ"
            | "TIDB_INLJ"
            | "TIDB_SMJ"
            | "OLAP"
            | "OLTP"
            | "TIKV"
            | "TIFLASH"
            | "PARTITION"
            | "FALSE"
            | "TRUE"
            | "MB"
            | "GB"
            | "DUPSWEEDOUT"
            | "FIRSTMATCH"
            | "LOOSESCAN"
            | "MATERIALIZATION"
    )
}

/// Whether `name` (already uppercased) is one of the "unsupported MySQL
/// hint" names real TiDB's own `parseOneHint` recognizes by name but
/// ALWAYS routes to `parseUnsupportedHint` (`pkg/parser/hintparser.go`,
/// the `case hintBKA, hintNoBKA, ...:` bucket) — genuinely recognized
/// syntax, but real TiDB itself never attaches any semantic content to
/// it regardless of args, always warn-and-drop. `NO_MERGE` (distinct
/// from the real, content-bearing `NO_MERGE_JOIN`) is the one confirmed
/// via the real corpus (`godump restore`: `merge(q) no_merge(q1)` keeps
/// `MERGE()` but drops `no_merge(q1)` entirely) — the rest of this
/// bucket is included too since it's the SAME verified Go-source case,
/// even though none of those individually appear in the real-TiDB
/// integration-test corpus this project measures coverage against.
fn is_always_unsupported_hint_name(name: &str) -> bool {
    matches!(
        name,
        "BKA"
            | "NO_BKA"
            | "BNL"
            | "NO_BNL"
            | "NO_MERGE"
            | "INDEX_MERGE"
            | "MRR"
            | "NO_MRR"
            | "NO_ICP"
            | "NO_RANGE_OPTIMIZATION"
            | "SKIP_SCAN"
            | "NO_SKIP_SCAN"
            | "SEMIJOIN"
            | "NO_SEMIJOIN"
    )
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
