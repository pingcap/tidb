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

//! `INSERT [IGNORE] ... VALUES [ON DUPLICATE KEY UPDATE ...]`, single-table
//! `UPDATE ... SET`, and single-table `DELETE FROM`. Called from
//! `crate::Parser::parse_statement`.

use tidb_ast::{
    Assignment, BatchDml, BatchDmlDryRun, BatchDmlStmt, ColumnOrUserVar, DeleteKind, DeleteStmt,
    Expr, Hint, ImportIntoStmt, ImportSource, InsertStmt, LoadDataOption, UnaryOp, UpdateKind,
    UpdateStmt,
};

use crate::{decode_string, prec, select::parse_hint_comment, PResult, Parser};
use tidb_lexer::TokenKind;

impl Parser {
    /// Direct Rust translation of Go's `parseImportIntoStmt` in
    /// `pkg/parser/import_brie_parser.go`:
    ///
    /// ```text
    /// IMPORT INTO table [(cols)] [SET ...] FROM 'path' [FORMAT 'fmt'] [WITH opts]
    /// IMPORT INTO table [(cols)] FROM SELECT ...
    /// IMPORT INTO table [(cols)] FROM (SELECT ...)
    /// ```
    ///
    /// The parser owns syntax/restore only. Option semantics and every file
    /// or query import action stay at the executor's explicit Unsupported
    /// boundary; parsing a valid Go statement must not depend on an
    /// incomplete external-storage implementation.
    pub(crate) fn parse_import_into(&mut self) -> PResult<ImportIntoStmt> {
        self.expect_kw("IMPORT")?;
        self.expect_kw("INTO")?;
        let table = self.parse_name_path()?;

        let columns_and_user_vars = if self.is_op("(") {
            self.bump();
            let mut columns = Vec::new();
            // Go's hand parser accepts the same empty/trailing-comma shape
            // as its `for { item; if !comma { break } }` loop: neither
            // should be tightened incidentally while translating IMPORT.
            while !self.is_op(")") {
                if self.peek().kind == TokenKind::UserVar {
                    let token = self.bump();
                    let Some(name) = token.text.strip_prefix('@') else {
                        return Err(self.err_here("expected an IMPORT user variable"));
                    };
                    // `@@x` is a system variable in expression grammar, but
                    // Go's IMPORT column list accepts only single-@ vars.
                    if name.starts_with('@') {
                        return Err(self.err_here("expected a single-@ IMPORT user variable"));
                    }
                    columns.push(ColumnOrUserVar::UserVar(name.to_owned()));
                } else {
                    columns.push(ColumnOrUserVar::Column(self.parse_name_or_keyword()?));
                }
                if self.is_op(",") {
                    self.bump();
                } else {
                    break;
                }
            }
            self.expect_op(")")?;
            columns
        } else {
            Vec::new()
        };

        let column_assignments = if self.is_kw("SET") {
            self.bump();
            let mut assignments = vec![self.parse_assignment(false)?];
            while self.is_op(",") {
                self.bump();
                assignments.push(self.parse_assignment(false)?);
            }
            assignments
        } else {
            Vec::new()
        };

        self.expect_kw("FROM")?;
        let source = if self.peek().kind == TokenKind::Str {
            let path = decode_string(&self.bump().text);
            let format = if self.is_kw("FORMAT") {
                self.bump();
                if self.peek().kind != TokenKind::Str {
                    return Err(self.err_here("expected IMPORT FORMAT string"));
                }
                Some(decode_string(&self.bump().text))
            } else {
                None
            };
            ImportSource::File { path, format }
        } else {
            let parenthesized = self.is_op("(");
            let query = if parenthesized {
                self.bump();
                // Go's `FROM (SELECT ...)` branch invokes its select parser
                // directly; unlike the unparenthesized source branch it
                // does not admit a leading WITH clause inside these parens.
                if !self.is_kw("SELECT") {
                    return Err(self.err_here("expected SELECT after IMPORT FROM ("));
                }
                let query = self.parse_select_or_setopr()?;
                self.expect_op(")")?;
                query
            } else if self.is_kw("SELECT") || self.is_kw("WITH") {
                self.parse_select_or_setopr()?
            } else {
                return Err(self.err_here("expected IMPORT file path or SELECT source"));
            };

            // Go rejects these mappings only for a SELECT source. Keep the
            // test here, after the source is known, rather than making the
            // AST represent a semantically impossible combination.
            if columns_and_user_vars
                .iter()
                .any(|column| matches!(column, ColumnOrUserVar::UserVar(_)))
            {
                return Err(
                    self.err_here("cannot use an IMPORT user variable with a SELECT source")
                );
            }
            if !column_assignments.is_empty() {
                return Err(self.err_here("cannot use IMPORT SET with a SELECT source"));
            }
            ImportSource::Select {
                query: Box::new(query),
                parenthesized,
            }
        };

        let options = if self.is_kw("WITH") {
            self.bump();
            let mut options = vec![self.parse_import_option()?];
            while self.is_op(",") {
                self.bump();
                options.push(self.parse_import_option()?);
            }
            options
        } else {
            Vec::new()
        };

        Ok(ImportIntoStmt {
            table,
            columns_and_user_vars,
            column_assignments,
            source,
            options,
        })
    }

    /// Go's `LoadDataOpt`: a raw lowercased option name plus an optional
    /// `SignedLiteral` payload. It intentionally does not validate known
    /// option names; that is import-job semantic validation, not grammar.
    fn parse_import_option(&mut self) -> PResult<LoadDataOption> {
        let token = self.peek().clone();
        if !matches!(token.kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(self.err_here("expected IMPORT option name"));
        }
        self.bump();
        let value = if self.is_op("=") || self.is_op(":=") {
            self.bump();
            Some(self.parse_import_signed_literal()?)
        } else {
            None
        };
        Ok(LoadDataOption {
            name: token.text.to_ascii_lowercase(),
            value,
        })
    }

    /// Go's `SignedLiteral` production used only by IMPORT options:
    /// literals, or `+`/`-` directly followed by a numeric literal. Parse at
    /// unary precedence after checking the first token, so a general binary
    /// expression cannot accidentally enter this literal-only grammar.
    pub(crate) fn parse_import_signed_literal(&mut self) -> PResult<Expr> {
        if self.is_op("+") || self.is_op("-") {
            let operator = if self.is_op("+") {
                UnaryOp::Plus
            } else {
                UnaryOp::Minus
            };
            self.bump();
            if !matches!(
                self.peek().kind,
                TokenKind::IntLit | TokenKind::FloatLit | TokenKind::DecLit
            ) {
                return Err(self.err_here("expected numeric IMPORT option literal"));
            }
            return Ok(Expr::Unary(
                operator,
                Box::new(self.parse_expr(prec::UNARY)?),
            ));
        }
        if !matches!(
            self.peek().kind,
            TokenKind::IntLit
                | TokenKind::FloatLit
                | TokenKind::DecLit
                | TokenKind::Str
                | TokenKind::CharsetIntroducer
                | TokenKind::HexLit
                | TokenKind::BitLit
        ) && !(self.is_kw("NULL") || self.is_kw("TRUE") || self.is_kw("FALSE"))
        {
            return Err(self.err_here("expected IMPORT option literal"));
        }
        self.parse_expr(prec::UNARY)
    }

    /// Parses TiDB's non-transactional DML wrapper directly from
    /// `pkg/parser/dml_parser.go`'s `parseNonTransactionalDMLStmt`:
    /// `BATCH [ON column] LIMIT N [DRY RUN [QUERY]] <DML>`.
    ///
    /// The inner grammar is deliberately restricted to the three
    /// `ShardableDMLStmt` families Go accepts, so nested BATCH and arbitrary
    /// statements never become accepted merely because the outer syntax did.
    pub(crate) fn parse_batch_dml(&mut self) -> PResult<BatchDmlStmt> {
        self.expect_kw("BATCH")?;
        let shard_column = if self.is_kw("ON") {
            self.bump();
            Some(self.parse_name_path()?)
        } else {
            None
        };
        self.expect_kw("LIMIT")?;
        let limit_token = self.peek().clone();
        if limit_token.kind != tidb_lexer::TokenKind::IntLit {
            return Err(self.err_here("expected BATCH LIMIT integer"));
        }
        self.bump();
        let limit = limit_token
            .text
            .parse::<u64>()
            .map_err(|_| self.err_here("BATCH LIMIT integer is out of range"))?;

        let dry_run = if self.is_kw("DRY") {
            self.bump();
            self.expect_kw("RUN")?;
            if self.is_kw("QUERY") {
                self.bump();
                BatchDmlDryRun::Query
            } else {
                BatchDmlDryRun::SplitDml
            }
        } else {
            BatchDmlDryRun::None
        };

        let dml = if self.is_kw("INSERT") || self.is_kw("REPLACE") {
            BatchDml::Insert(Box::new(self.parse_insert()?))
        } else if self.is_kw("UPDATE") {
            BatchDml::Update(Box::new(self.parse_update()?))
        } else if self.is_kw("DELETE") {
            BatchDml::Delete(Box::new(self.parse_delete()?))
        } else {
            return Err(self.err_here("expected INSERT, REPLACE, UPDATE, or DELETE after BATCH"));
        };

        Ok(BatchDmlStmt {
            shard_column,
            limit,
            dry_run,
            dml,
        })
    }

    pub(crate) fn parse_insert(&mut self) -> PResult<InsertStmt> {
        // `REPLACE` shares the entire `INSERT ... VALUES` grammar (real
        // TiDB uses the same `InsertStmt` node with `IsReplace`), so it's
        // parsed here rather than as a separate statement — it just never
        // carries `IGNORE` or `ON DUPLICATE KEY UPDATE`.
        let replace = self.is_kw("REPLACE");
        if replace {
            self.bump();
        } else {
            self.expect_kw("INSERT")?;
        }
        let hints = self.parse_dml_hints()?;
        // Priority modifiers (LOW_PRIORITY/DELAYED/HIGH_PRIORITY) are not
        // modelled yet.
        let ignore = if !replace && self.is_kw("IGNORE") {
            self.bump();
            true
        } else {
            false
        };
        if self.is_kw("INTO") {
            self.bump();
        }
        let table = self.parse_name_path()?;
        // `PARTITION (...)` comes right after the table name, before the
        // column list (confirmed via `godump restore`; the reverse order
        // is a genuine `ParseError`).
        let partitions = self.parse_partition_opt()?;
        // Optional explicit column list. The one ambiguity at this boundary
        // is real TiDB's parenthesized query source: `INSERT INTO t (SELECT
        // ...)` is NOT a column list. Keep that source in InsertStmt rather
        // than flattening its SQL so execution still uses the normal typed
        // INSERT ... SELECT path.
        let mut source = None;
        let mut source_parenthesized = false;
        let columns = if self.is_parenthesized_insert_source() {
            source = Some(Box::new(self.parse_parenthesized_insert_source()?));
            source_parenthesized = true;
            Vec::new()
        } else if self.is_op("(") {
            self.bump();
            let mut cols = vec![self.parse_name_or_keyword()?];
            while self.is_op(",") {
                self.bump();
                cols.push(self.parse_name_or_keyword()?);
            }
            self.expect_op(")")?;
            cols
        } else {
            Vec::new()
        };
        // The values come in one of three mutually-exclusive forms:
        //   - `SET col=val, ...` — assignment form, stored as typed
        //     `set_columns` paths + a single `rows` entry (the RHS values),
        //     with `set_syntax` marking it for restore. Cannot co-occur with
        //     an explicit `(col, ...)` list.
        //   - `[VALUE[S]] (...)`, ... — literal row list.
        //   - `SELECT`/`UNION`/`WITH` — a query source (task #140).
        let mut rows = Vec::new();
        let mut set_syntax = false;
        let mut set_columns = Vec::new();
        if source.is_some() {
            // The parenthesized source was consumed at the table/column-list
            // boundary above. Only the trailing ON DUPLICATE clause remains.
        } else if self.is_kw("SET") {
            self.bump();
            set_syntax = true;
            let (set_cols, set_vals) = self.parse_set_assignment_list()?;
            set_columns = set_cols;
            rows.push(set_vals);
        } else if self.is_kw("WITH") {
            // Go's InsertStmt has no separate CTE field: `WITH` belongs to
            // its typed query source (`InsertStmt.Select`). Keep that same
            // ownership boundary by reusing the shared query parser rather
            // than making INSERT carry a text-only prefix.
            source = Some(Box::new(self.parse_with_select()?));
        } else if self.is_kw("SELECT") {
            source = Some(Box::new(self.parse_select_or_setopr()?));
        } else if self.is_parenthesized_insert_source() {
            source = Some(Box::new(self.parse_parenthesized_insert_source()?));
            source_parenthesized = true;
        } else {
            // `VALUE` is an accepted synonym for `VALUES` (real MySQL/TiDB
            // grammar; both restore as `VALUES` — confirmed via `godump
            // restore`).
            if self.is_kw("VALUE") {
                self.bump();
            } else {
                self.expect_kw("VALUES")?;
            }
            rows.push(self.parse_value_row()?);
            while self.is_op(",") {
                self.bump();
                rows.push(self.parse_value_row()?);
            }
        }
        // `REPLACE` never carries `ON DUPLICATE KEY UPDATE`.
        let on_duplicate = if !replace && self.is_kw("ON") {
            self.bump();
            self.expect_kw("DUPLICATE")?;
            self.expect_kw("KEY")?;
            self.expect_kw("UPDATE")?;
            // Go routes this through `parseAssignment`, whose RHS is
            // `parseExprOrDefault`: `col=DEFAULT` is legal here just as it
            // is in INSERT VALUES/SET and a single-table UPDATE.
            let mut assignments = vec![self.parse_assignment(true)?];
            while self.is_op(",") {
                self.bump();
                assignments.push(self.parse_assignment(true)?);
            }
            assignments
        } else {
            Vec::new()
        };
        Ok(InsertStmt {
            hints,
            ignore,
            table,
            partitions,
            columns,
            set_columns,
            rows,
            source,
            source_parenthesized,
            set_syntax,
            on_duplicate,
            replace,
        })
    }

    /// Go's `parseInsertStmt` accepts a result-set source inside one pair of
    /// INSERT-owned parentheses both immediately after the target table and
    /// after an explicit target-column list. Restrict this leaf to the
    /// already typed SELECT/WITH result-set family; `TABLE` and `VALUES`
    /// result-set statements need their own typed QueryStmt variants first.
    fn is_parenthesized_insert_source(&self) -> bool {
        self.is_op("(") && (self.is_kw_at(1, "SELECT") || self.is_kw_at(1, "WITH"))
    }

    fn parse_parenthesized_insert_source(&mut self) -> PResult<tidb_ast::QueryStmt> {
        self.expect_op("(")?;
        let source = if self.is_kw("WITH") {
            self.parse_with_select()?
        } else {
            self.parse_select_or_setopr()?
        };
        self.expect_op(")")?;
        Ok(source)
    }

    /// Parses `col=val, ...` for `INSERT ... SET`, retaining each LHS as a
    /// typed name path just like Go's `ast.ColumnName`. RHS values remain a
    /// parallel single row; each may be a bare `DEFAULT`.
    fn parse_set_assignment_list(&mut self) -> PResult<(Vec<Vec<String>>, Vec<Expr>)> {
        let mut cols = Vec::new();
        let mut vals = Vec::new();
        loop {
            let assignment = self.parse_assignment(true)?;
            cols.push(assignment.col);
            vals.push(assignment.value);
            if self.is_op(",") {
                self.bump();
            } else {
                break;
            }
        }
        Ok((cols, vals))
    }

    fn parse_value_row(&mut self) -> PResult<Vec<Expr>> {
        self.expect_op("(")?;
        let mut row = Vec::new();
        if !self.is_op(")") {
            row.push(self.parse_insert_value()?);
            while self.is_op(",") {
                self.bump();
                row.push(self.parse_insert_value()?);
            }
        }
        self.expect_op(")")?;
        Ok(row)
    }

    /// Parses one `INSERT` value: either a bare `DEFAULT` keyword (meaning
    /// "this column's declared default", modelled as a zero-arg `DEFAULT`
    /// func and restored as the bare keyword) or an arbitrary expression. A
    /// `DEFAULT(col)` with
    /// parens is a normal expression handled by `parse_expr`, NOT
    /// intercepted here.
    fn parse_insert_value(&mut self) -> PResult<Expr> {
        self.parse_expr_or_default()
    }

    /// Parses Go `parseExprOrDefault` positions that this parser models:
    /// INSERT value/SET items, ON DUPLICATE KEY UPDATE, and single-table
    /// UPDATE assignments.
    /// `DEFAULT(column)` remains an ordinary expression and is delegated to
    /// the general expression parser.
    pub(crate) fn parse_expr_or_default(&mut self) -> PResult<Expr> {
        if self.is_kw("DEFAULT") && !self.is_op_at(1, "(") {
            self.bump();
            return Ok(Expr::Func {
                name: "DEFAULT".to_string(),
                args: Vec::new(),
            });
        }
        self.parse_expr(prec::NONE)
    }

    pub(crate) fn parse_update(&mut self) -> PResult<UpdateStmt> {
        self.expect_kw("UPDATE")?;
        let hints = self.parse_dml_hints()?;
        // Priority modifiers (LOW_PRIORITY) are not modelled; `IGNORE` is.
        let ignore = if self.is_kw("IGNORE") {
            self.bump();
            true
        } else {
            false
        };
        // Single-table `UPDATE tbl SET` vs multi-table `UPDATE join SET`.
        // Parse one table ref and peek: if `SET` follows it's single-table;
        // otherwise a `,`/`JOIN` continues a join source, so rewind and
        // parse the full `FROM`-style join.
        let kind = if self.is_op("(") {
            // A derived table begins with `(`, which cannot enter the
            // single-table `parse_table_ref` probe below. Go parses the full
            // table-reference grammar directly, so route this source-backed
            // shape through the existing typed join tree instead of rejecting
            // it before its `SET` clause can be reached.
            let (from, comma_join) = self.parse_from_with_comma()?;
            UpdateKind::Multi {
                from: Box::new(from),
                comma_join,
            }
        } else {
            let save = self.pos;
            let _ = self.parse_table_ref()?;
            if self.is_kw("SET") {
                self.pos = save;
                UpdateKind::Single(self.parse_table_ref()?)
            } else {
                self.pos = save;
                let (from, comma_join) = self.parse_from_with_comma()?;
                UpdateKind::Multi {
                    from: Box::new(from),
                    comma_join,
                }
            }
        };
        self.expect_kw("SET")?;
        // Go's `parseAssignment` always routes the RHS through
        // `parseExprOrDefault`, regardless of whether the table reference is
        // single-table, joined, or contains a derived input. Keep the grammar
        // uniform here too; executor support remains its own boundary (for
        // example, derived-table UPDATE is parsed/restored then rejected
        // before mutation).
        let mut assignments = vec![self.parse_assignment(true)?];
        while self.is_op(",") {
            self.bump();
            assignments.push(self.parse_assignment(true)?);
        }
        let where_clause = if self.is_kw("WHERE") {
            self.bump();
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        let (order_by, limit) = self.parse_dml_order_limit()?;
        if matches!(
            &kind,
            UpdateKind::Multi {
                comma_join: true,
                ..
            }
        ) && (!order_by.is_empty() || limit.is_some())
        {
            return Err(self.err_here("UPDATE comma-join does not allow ORDER BY or LIMIT"));
        }
        Ok(UpdateStmt {
            hints,
            ignore,
            kind,
            assignments,
            where_clause,
            order_by,
            limit,
        })
    }

    pub(crate) fn parse_assignment(&mut self, allow_bare_default: bool) -> PResult<Assignment> {
        let col = self.parse_name_path()?;
        self.expect_op("=")?;
        let value = if allow_bare_default {
            self.parse_expr_or_default()?
        } else {
            self.parse_expr(prec::NONE)?
        };
        Ok(Assignment { col, value })
    }

    pub(crate) fn parse_delete(&mut self) -> PResult<DeleteStmt> {
        self.expect_kw("DELETE")?;
        let hints = self.parse_dml_hints()?;
        // Priority/QUICK modifiers are not modelled; `IGNORE` is.
        let ignore = if self.is_kw("IGNORE") {
            self.bump();
            true
        } else {
            false
        };
        let kind = if self.is_kw("FROM") {
            self.bump();
            // Either single-table `FROM tbl` or multi-table `FROM targets
            // USING join`. Parse a name path and peek: a following `,` or
            // `USING` means the multi-table `USING` spelling; otherwise it's
            // an ordinary single-table delete (re-parse the full `TableRef`
            // from the same position, since it may carry an alias/hints).
            let save = self.pos;
            let _ = self.parse_name_path()?;
            if self.is_op(",") || self.is_kw("USING") {
                self.pos = save;
                let targets = self.parse_delete_targets()?;
                self.expect_kw("USING")?;
                let from = Box::new(self.parse_from()?);
                DeleteKind::Multi {
                    targets,
                    using: true,
                    from,
                }
            } else {
                self.pos = save;
                DeleteKind::Single(self.parse_table_ref()?)
            }
        } else {
            // Multi-table `DELETE targets FROM join`.
            let targets = self.parse_delete_targets()?;
            self.expect_kw("FROM")?;
            let from = Box::new(self.parse_from()?);
            DeleteKind::Multi {
                targets,
                using: false,
                from,
            }
        };
        let where_clause = if self.is_kw("WHERE") {
            self.bump();
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        let (order_by, limit) = self.parse_dml_order_limit()?;
        if matches!(&kind, DeleteKind::Multi { .. }) && (!order_by.is_empty() || limit.is_some()) {
            return Err(self.err_here("multi-table DELETE does not allow ORDER BY or LIMIT"));
        }
        Ok(DeleteStmt {
            hints,
            ignore,
            kind,
            where_clause,
            order_by,
            limit,
        })
    }

    /// Parses the fixed-order `ORDER BY ... LIMIT ...` tail shared by TiDB's
    /// UPDATE and DELETE grammars (`pkg/parser/dml_parser.go`).
    fn parse_dml_order_limit(
        &mut self,
    ) -> PResult<(Vec<tidb_ast::OrderItem>, Option<tidb_ast::Limit>)> {
        let order_by = if self.is_kw("ORDER") {
            self.bump();
            self.expect_kw("BY")?;
            self.parse_order_list()?
        } else {
            Vec::new()
        };
        let limit = if self.is_kw("LIMIT") {
            self.bump();
            Some(self.parse_dml_limit()?)
        } else {
            None
        };
        Ok((order_by, limit))
    }

    /// Parses UPDATE/DELETE's `LIMIT row_count`. Go's DML grammar uses
    /// `LimitClauseSimple`, unlike SELECT: offsets and `OFFSET` are not part
    /// of this production.
    fn parse_dml_limit(&mut self) -> PResult<tidb_ast::Limit> {
        let count = self.parse_expr(prec::NONE)?;
        if self.is_op(",") || self.is_kw("OFFSET") {
            return Err(self.err_here("UPDATE/DELETE LIMIT accepts row_count only"));
        }
        Ok(tidb_ast::Limit {
            offset: None,
            count,
        })
    }

    /// Parses the optional optimizer-hint block immediately after a bindable
    /// DML verb. It reuses SELECT's typed hint parser rather than preserving
    /// raw comment text, matching Go's shared `parseOptHints` call.
    fn parse_dml_hints(&mut self) -> PResult<Vec<Hint>> {
        if self.peek().kind == TokenKind::HintComment {
            let text = self.bump().text;
            parse_hint_comment(&text)
        } else {
            Ok(Vec::new())
        }
    }

    /// Parses a multi-table `DELETE`'s comma-separated target table list
    /// (bare name paths, no aliases). A trailing `.*` (`t1.*`) is accepted
    /// and dropped, matching real TiDB — the target names the table itself.
    fn parse_delete_targets(&mut self) -> PResult<Vec<Vec<String>>> {
        let mut targets = vec![self.parse_delete_target()?];
        while self.is_op(",") {
            self.bump();
            targets.push(self.parse_delete_target()?);
        }
        Ok(targets)
    }

    fn parse_delete_target(&mut self) -> PResult<Vec<String>> {
        let name = self.parse_name_path()?;
        // `t1.*` — the `.*` is a redundant wildcard on a delete target.
        if self.is_op(".") && self.is_op_at(1, "*") {
            self.bump();
            self.bump();
        }
        Ok(name)
    }
}
