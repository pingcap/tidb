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

//! The `FROM` clause: table references, `TABLESAMPLE`, index hints, derived
//! tables, and the join tree, mirroring Go's table-reference productions
//! (`TableRefs` / `TableFactor` / `JoinTable`) in `pkg/parser/parser.y`.

use super::*;

impl Parser {
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
    pub(crate) fn parse_table_name_path(&mut self) -> PResult<Vec<String>> {
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
            return Ok(self.bumped_string());
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
