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

//! `INSERT` (including `ON DUPLICATE KEY UPDATE` and `IGNORE` conflict
//! handling), single-table `UPDATE ... SET ... [WHERE ...]`, and
//! single-table `DELETE FROM ... [WHERE ...]`. Called from
//! `crate::database`'s top-level `run`; its `run_dml` boundary restores the
//! catalog on any error, making each ordinary DML statement atomic.

use tidb_ast::{
    Assignment, ColumnType, DeleteKind, DeleteStmt, Expr, InsertStmt, ReferentialAction,
    UpdateKind, UpdateStmt,
};
use tidb_datatype::Datum;
use tidb_expr::{eval, eval_in};

use crate::catalog::{table_key, AutoIncrementColumn, Column, ForeignKey};
use crate::literal::value_to_literal;
use crate::select::is_truthy;
use crate::session::RelResolver;
use crate::table_reference::{check_no_as_of, check_no_partition, check_no_table_sample};
use crate::{Database, ExecError, Row};

impl Database {
    /// Inserts each row, or — if it conflicts with an existing row on any
    /// `PRIMARY KEY` or `UNIQUE` group — applies `ON DUPLICATE KEY UPDATE`
    /// if present. A conflict with no such clause raises the executor's
    /// source-shaped [`ExecError::DuplicateKey`] error; the enclosing DML
    /// dispatch restores any earlier rows from the same statement.
    pub(crate) fn insert(&mut self, ins: &InsertStmt) -> Result<i64, ExecError> {
        check_no_partition(&ins.partitions)?;
        let key = table_key(&ins.table);
        let (ncols, cols, col_types, col_defaults, key_groups, auto_increment) = {
            let table = self
                .tables
                .get(&key)
                .ok_or_else(|| ExecError::UnknownTable(key.clone()))?;
            (
                table.cols.len(),
                table.cols.clone(),
                table.col_types.clone(),
                table.col_defaults.clone(),
                table.key_groups.clone(),
                table.auto_increment,
            )
        };
        // Go retains `INSERT ... SET` targets as qualified `ColumnName`s.
        // This single-table executor resolves the same target identity as
        // UPDATE/ON DUPLICATE KEY UPDATE: its final path component names the
        // physical table column, while the AST retains the full path for
        // faithful restore.
        let insert_cols = if ins.set_syntax {
            ins.set_columns
                .iter()
                .map(|path| {
                    path.last()
                        .cloned()
                        .ok_or(ExecError::Unsupported("empty INSERT SET column path"))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            ins.columns.clone()
        };
        // The rows to insert come EITHER from a `VALUES`/`SET` list (each
        // expr evaluated as a constant, with omitted columns and bare
        // `DEFAULT` values resolved against `col_defaults` by
        // `assemble_row`) OR from an `INSERT ... SELECT` query source (each
        // result row already a full `Vec<Datum>`). Both feed the SAME
        // per-row `insert_one_row` (width coercion, conflict/`REPLACE`/`ON
        // DUPLICATE`/`IGNORE`, foreign keys).
        let mut affected = 0;
        let mut generated_auto_id = false;
        if let Some(source) = &ins.source {
            let produced = match source.as_ref() {
                tidb_ast::QueryStmt::Select(select) => self.select(select, None)?,
                tidb_ast::QueryStmt::SetOpr(setopr) => self.setopr(setopr, None)?,
            };
            for values in produced.rows {
                affected += self.insert_one_row(
                    &key,
                    values,
                    &cols,
                    &col_types,
                    &key_groups,
                    auto_increment,
                    &mut generated_auto_id,
                    ins,
                    ncols,
                )?;
            }
        } else {
            for row in &ins.rows {
                let values = assemble_row(&cols, &col_defaults, &insert_cols, row)?;
                affected += self.insert_one_row(
                    &key,
                    values,
                    &cols,
                    &col_types,
                    &key_groups,
                    auto_increment,
                    &mut generated_auto_id,
                    ins,
                    ncols,
                )?;
            }
        }
        Ok(affected)
    }

    /// Materializes one row's auto-ID before any duplicate, foreign-key, or
    /// `REPLACE` handling. That order is observable: TiDB deliberately
    /// leaves gaps for failed/ignored conflicts and transaction rollback.
    fn apply_auto_increment(
        &mut self,
        key: &str,
        values: &mut Row,
        auto_increment: AutoIncrementColumn,
        col_types: &[ColumnType],
        cols: &[String],
    ) -> Result<Option<u64>, ExecError> {
        let column = auto_increment.column;
        let value = values.get(column).ok_or(ExecError::ColumnCountMismatch)?;
        match auto_increment_positive(value) {
            Some(value) => self.rebase_auto_increment(key, value),
            None if matches!(value, Datum::Null | Datum::Int(0) | Datum::UInt(0)) => {
                let next = self
                    .auto_increment_next
                    .borrow()
                    .get(key)
                    .copied()
                    .flatten()
                    .ok_or_else(|| ExecError::OutOfRange(cols[column].clone()))?;
                // Coerce before advancing the cursor. A signed INT/BIGINT
                // whose next unsigned candidate is out of range must report
                // exhaustion, not consume an unrepresentable value.
                values[column] =
                    coerce_column(Datum::UInt(next), &col_types[column], &cols[column])?;
                self.auto_increment_next
                    .borrow_mut()
                    .insert(key.to_string(), next.checked_add(1));
                return Ok(Some(next));
            }
            None => {}
        }
        Ok(None)
    }

    /// Publishes a generated value only for a row TiDB treats as inserted
    /// (or a hard-erroring plain insert). `IGNORE` and ON-DUPLICATE rows
    /// still consume an allocator value but deliberately retain the prior
    /// `LAST_INSERT_ID`, so allocation itself cannot write session status.
    fn publish_generated_auto_id(&mut self, generated: Option<u64>, published: &mut bool) {
        if let Some(generated) = generated.filter(|_| !*published) {
            *self.statement_last_insert_id.borrow_mut() = Some(generated);
            *published = true;
        }
    }

    /// Raises (never lowers) a table allocator after a successfully supplied
    /// positive auto-ID. `None` is the one representable exhausted state:
    /// inserting `u64::MAX` makes a `BIGINT UNSIGNED` allocator unable to
    /// issue another value without inventing an overflow sentinel.
    fn rebase_auto_increment(&mut self, key: &str, value: u64) {
        let candidate = value.checked_add(1);
        let mut allocators = self.auto_increment_next.borrow_mut();
        let next = allocators.entry(key.to_string()).or_insert(Some(1));
        if matches!((*next, candidate), (Some(current), Some(candidate)) if candidate > current)
            || candidate.is_none()
        {
            *next = candidate;
        }
    }

    /// Applies explicit positive-ID rebasing after an UPDATE/ON-DUPLICATE
    /// row write has actually succeeded. `NULL` and zero are neither
    /// allocation requests nor rebases in UPDATE; that differs intentionally
    /// from INSERT's NULL/zero allocation rule.
    fn rebase_auto_increment_from_row(&mut self, key: &str, row: &Row) {
        let auto_increment = self.tables.get(key).and_then(|table| table.auto_increment);
        if let Some(value) = auto_increment
            .and_then(|auto_increment| row.get(auto_increment.column))
            .and_then(auto_increment_positive)
        {
            self.rebase_auto_increment(key, value);
        }
    }

    /// Inserts one full-width row (from an already-assembled `VALUES`/`SET`
    /// row or a `SELECT` source): checks the column count, coerces each
    /// value to its declared type width (`VARCHAR`/`CHAR`/`BIT`/`DECIMAL`),
    /// then applies the conflict rules — `REPLACE` delete-then-insert, `ON
    /// DUPLICATE KEY UPDATE`, `IGNORE`, or a plain insert with
    /// foreign-key checks. `VALUES`/`SET` rows are already `ncols` wide
    /// (see `assemble_row`); a `SELECT` source must produce exactly that
    /// width.
    #[allow(clippy::too_many_arguments)]
    fn insert_one_row(
        &mut self,
        key: &str,
        row: Row,
        cols: &[String],
        col_types: &[tidb_ast::ColumnType],
        key_groups: &[Vec<usize>],
        auto_increment: Option<AutoIncrementColumn>,
        generated_auto_id: &mut bool,
        ins: &InsertStmt,
        ncols: usize,
    ) -> Result<i64, ExecError> {
        if row.len() != ncols {
            return Err(ExecError::ColumnCountMismatch);
        }
        // Enforce the declared column type's storage width. `col_types.get(i)`
        // keeps this total rather than panicking if `col_types` is ever out
        // of sync with the row width (it never is in practice).
        let mut values: Row = row
            .into_iter()
            .enumerate()
            .map(|(i, v)| match col_types.get(i) {
                Some(ty) => coerce_column(v, ty, &cols[i]),
                None => Ok(v),
            })
            .collect::<Result<_, _>>()?;
        let generated = if let Some(auto_increment) = auto_increment {
            self.apply_auto_increment(key, &mut values, auto_increment, col_types, cols)?
        } else {
            None
        };

        // `REPLACE`: delete EVERY existing row that conflicts on any key
        // group (a new row can collide with several rows via different
        // unique keys), then insert the new row — MySQL's
        // delete-then-insert semantics, confirmed via `gorun`.
        if ins.replace {
            let removed = {
                let t = self.tables.get_mut(key).unwrap();
                let before = t.rows.len();
                t.rows.retain(|r| {
                    !key_groups
                        .iter()
                        .any(|group| group.iter().all(|&pki| r[pki] == values[pki]))
                });
                before - t.rows.len()
            };
            self.check_foreign_keys(key, &values)?;
            self.tables.get_mut(key).unwrap().rows.push(values);
            self.publish_generated_auto_id(generated, generated_auto_id);
            return Ok(i64::try_from(removed + 1).expect("row count fits i64"));
        }

        // A conflict on any group requires every one of ITS columns to
        // match (a composite key), not just one; the first group with a
        // match (checked in declaration order — `PRIMARY KEY` first)
        // determines the conflicting row.
        let conflict = key_groups.iter().find_map(|group| {
            self.tables
                .get(key)
                .unwrap()
                .rows
                .iter()
                .position(|r| group.iter().all(|&pki| r[pki] == values[pki]))
        });
        match conflict {
            Some(ridx) if !ins.on_duplicate.is_empty() => {
                let changed =
                    self.apply_on_duplicate(key, ridx, &ins.on_duplicate, cols, &values)?;
                let updated = self.tables.get(key).unwrap().rows[ridx].clone();
                self.rebase_auto_increment_from_row(key, &updated);
                Ok(if changed { 2 } else { 0 })
            }
            // `IGNORE` silently keeps the existing row, skipping the
            // conflicting insert rather than erroring or updating.
            Some(_) if ins.ignore => Ok(0),
            Some(_) => {
                self.publish_generated_auto_id(generated, generated_auto_id);
                Err(ExecError::DuplicateKey)
            }
            None => {
                match self.check_foreign_keys(key, &values) {
                    // `INSERT IGNORE` turns a child-side referential
                    // violation into a warning and skips just this row;
                    // later VALUES rows continue. Keep every other checker
                    // error visible instead of treating IGNORE as a blanket
                    // error suppressor.
                    Err(ExecError::ForeignKeyViolation) if ins.ignore => Ok(0),
                    Err(err) => {
                        self.publish_generated_auto_id(generated, generated_auto_id);
                        Err(err)
                    }
                    Ok(()) => {
                        self.tables.get_mut(key).unwrap().rows.push(values);
                        self.publish_generated_auto_id(generated, generated_auto_id);
                        Ok(1)
                    }
                }
            }
        }
    }

    /// Updates every row matching `WHERE` (all rows if absent): each
    /// assignment's value is evaluated against the row's ORIGINAL values —
    /// not progressively, so `SET a = a + 1, b = a` leaves `b` at the OLD
    /// `a`, not the new one (confirmed via `gorun`, not assumed: MySQL
    /// evaluates a `SET` clause's expressions as if simultaneously, then
    /// applies them together) — then all assignments for that row are
    /// applied at once. A `PRIMARY KEY`/`UNIQUE` conflict newly created by
    /// an update is not modelled (out of scope, like a duplicate `INSERT`
    /// with no `ON DUPLICATE KEY UPDATE` clause). Rows are matched against
    /// `WHERE` and evaluated using a snapshot taken before any of them are
    /// applied (so one row's own update never affects another row's
    /// `WHERE`/`SET` evaluation), but committed to the table one row at a
    /// time, immediately after that row's own child-side/parent-side
    /// `FOREIGN KEY` checks succeed. The enclosing DML boundary restores all
    /// table state if any later row/check fails, making the statement atomic.
    pub(crate) fn update(&mut self, upd: &UpdateStmt) -> Result<i64, ExecError> {
        // Multi-table update execution is not yet modelled (the SET
        // assignments must be routed back to each joined base table's rows,
        // updating a given base row at most once). Parsed and restored, but
        // honestly rejected at execution for now.
        let table = match &upd.kind {
            UpdateKind::Single(t) => t,
            UpdateKind::Multi { from, .. } => {
                return self.update_multi(upd.ignore, from, &upd.assignments, &upd.where_clause);
            }
        };
        check_no_partition(&table.partitions)?;
        check_no_table_sample(&table.sample)?;
        check_no_as_of(&table.as_of)?;
        let key = table_key(&table.name);
        let qual = table.alias.clone().unwrap_or_else(|| key.clone());
        let (cols, col_types, col_defaults, rows) = {
            let table = self
                .tables
                .get(&key)
                .ok_or_else(|| ExecError::UnknownTable(key.clone()))?;
            (
                table.cols.clone(),
                table.col_types.clone(),
                table.col_defaults.clone(),
                table.rows.clone(),
            )
        };
        let rel_cols: Vec<Column> = cols
            .iter()
            .map(|n| Column {
                tables: vec![qual.clone()],
                name: n.clone(),
            })
            .collect();
        let session = self.session_state();
        let mut affected = 0;
        for row in &rows {
            let resolver = RelResolver::new(&rel_cols, row, session.clone());
            if let Some(pred) = &upd.where_clause {
                let folded = self.resolve_subqueries(pred, &resolver)?;
                if !is_truthy(eval_in(&folded, &resolver)?)? {
                    continue;
                }
            }
            let mut updated = row.clone();
            for assign in &upd.assignments {
                let colname = assign
                    .col
                    .last()
                    .ok_or(ExecError::Unsupported("empty assignment column"))?;
                let idx = cols
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(colname))
                    .ok_or_else(|| ExecError::UnknownColumn(colname.clone()))?;
                let v = if is_default_placeholder(&assign.value) {
                    eval_default(
                        col_defaults
                            .get(idx)
                            .ok_or(ExecError::Unsupported("missing column default metadata"))?,
                    )?
                } else {
                    eval_in(&assign.value, &resolver)?
                };
                updated[idx] = match col_types.get(idx) {
                    Some(ty) => coerce_column(v, ty, colname)?,
                    None => v,
                };
            }
            self.check_foreign_keys(&key, &updated)?;
            self.propagate_parent_update(&key, row, &updated)?;
            affected += i64::from(updated != *row);
            let t = self.tables.get_mut(&key).unwrap();
            if let Some(pos) = t.rows.iter().position(|r| r == row) {
                t.rows[pos] = updated.clone();
            }
            self.rebase_auto_increment_from_row(&key, &updated);
        }
        Ok(affected)
    }

    /// Deletes every row matching `WHERE` (all rows if absent), cascading
    /// to any table with a `FOREIGN KEY` pointing at this one (see
    /// `delete_row_cascading`). Rows are processed one at a time, in
    /// order. The enclosing DML boundary restores earlier removals and
    /// cascades if a later `RESTRICT` check fails, so the complete statement
    /// is atomic.
    pub(crate) fn delete(&mut self, del: &DeleteStmt) -> Result<i64, ExecError> {
        let table = match &del.kind {
            DeleteKind::Single(t) => t,
            DeleteKind::Multi { targets, from, .. } => {
                return self.delete_multi(del.ignore, targets, from, &del.where_clause);
            }
        };
        check_no_partition(&table.partitions)?;
        check_no_table_sample(&table.sample)?;
        check_no_as_of(&table.as_of)?;
        let key = table_key(&table.name);
        let qual = table.alias.clone().unwrap_or_else(|| key.clone());
        let (cols, rows) = {
            let table = self
                .tables
                .get(&key)
                .ok_or_else(|| ExecError::UnknownTable(key.clone()))?;
            (table.cols.clone(), table.rows.clone())
        };
        let rel_cols: Vec<Column> = cols
            .iter()
            .map(|n| Column {
                tables: vec![qual.clone()],
                name: n.clone(),
            })
            .collect();
        let mut to_delete = Vec::new();
        let session = self.session_state();
        for row in &rows {
            let matches = match &del.where_clause {
                Some(pred) => {
                    let resolver = RelResolver::new(&rel_cols, row, session.clone());
                    let folded = self.resolve_subqueries(pred, &resolver)?;
                    is_truthy(eval_in(&folded, &resolver)?)?
                }
                None => true,
            };
            if matches {
                to_delete.push(row.clone());
            }
        }
        let mut affected = 0;
        for row in &to_delete {
            match self.delete_row_cascading(&key, row) {
                Ok(deleted) => affected += i64::from(deleted),
                // Like multi-table DELETE IGNORE, TiDB turns a parent-side
                // FK restriction into a skipped target row. Keeping this in
                // the single-table path too makes the affected count follow
                // the actual mutation rather than the WHERE match count.
                Err(ExecError::ForeignKeyViolation) if del.ignore => {}
                Err(err) => return Err(err),
            }
        }
        Ok(affected)
    }

    /// Executes a multi-table `UPDATE <join> SET ...`: builds the join,
    /// keeps the rows matching `WHERE`, then applies each `SET` assignment
    /// to the base table its target column is qualified with.
    ///
    /// Two MySQL semantics are honored (both gorun-verified): a base row
    /// that appears in several joined rows is updated AT MOST ONCE (the
    /// first joined row that reaches it wins), and every assignment's
    /// right-hand side is evaluated against the ORIGINAL joined row — so
    /// `SET t1.b = t2.c, t2.c = 99` writes `t2`'s old `c` into `t1.b`, not
    /// the just-assigned `99`. Assignment targets are routed by qualifier
    /// (the column's table qualifier, or — for a bare column — the single
    /// joined table that has it), the same qualifier a `SELECT` column
    /// reference would resolve against.
    fn update_multi(
        &mut self,
        _ignore: bool,
        from: &tidb_ast::Join,
        assignments: &[Assignment],
        where_clause: &Option<Expr>,
    ) -> Result<i64, ExecError> {
        // A derived table can be a readable join input, but making it an
        // UPDATE target requires TiDB's updatable-view/derived-table rules.
        // The in-memory executor only has base-table row identity, so reject
        // before relation construction or any target-row mutation.
        if join_has_derived_table(from) {
            return Err(ExecError::Unsupported("UPDATE derived table target"));
        }
        let rel = self.build_join(from, &[])?;
        let session = self.session_state();
        // Keep the joined rows matching WHERE, in order (order decides which
        // joined row "wins" a base row under the update-once rule).
        let mut kept: Vec<Row> = Vec::new();
        for row in &rel.rows {
            let keep = match where_clause {
                Some(pred) => {
                    let resolver = RelResolver::new(&rel.cols, row, session.clone());
                    let folded = self.resolve_subqueries(pred, &resolver)?;
                    is_truthy(eval_in(&folded, &resolver)?)?
                }
                None => true,
            };
            if keep {
                kept.push(row.clone());
            }
        }
        let leaves = collect_table_leaves(from);
        // Plan one update target per distinct qualifier that an assignment
        // writes to.
        let mut plan: Vec<UpdateTarget> = Vec::new();
        for assign in assignments {
            let colname = assign
                .col
                .last()
                .ok_or(ExecError::Unsupported("empty assignment column"))?
                .clone();
            // The target table's qualifier: the column's own qualifier if
            // written (`t1.a` → `t1`), else the single joined table holding
            // a column of that name.
            let qual = if assign.col.len() >= 2 {
                assign.col[assign.col.len() - 2].to_ascii_lowercase()
            } else {
                self.resolve_bare_update_column(&colname, &leaves)?
            };
            let key = leaves
                .iter()
                .find(|(q, _)| *q == qual)
                .map(|(_, k)| k.clone())
                .ok_or_else(|| ExecError::UnknownTable(qual.clone()))?;
            let base_cols = self.tables.get(&key).unwrap().cols.clone();
            let col_idx = base_cols
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&colname))
                .ok_or_else(|| ExecError::UnknownColumn(colname.clone()))?;
            let (col_ty, col_default) = {
                let table = self.tables.get(&key).unwrap();
                (
                    table.col_types.get(col_idx).cloned(),
                    table.col_defaults.get(col_idx).cloned().flatten(),
                )
            };
            // Reuse (or create) this qualifier's target plan.
            let tp = match plan.iter_mut().find(|t| t.qual == qual) {
                Some(t) => t,
                None => {
                    let reassembly = base_cols
                        .iter()
                        .map(|name| {
                            rel.cols
                                .iter()
                                .position(|c| {
                                    c.name.eq_ignore_ascii_case(name)
                                        && c.tables.iter().any(|t| t.eq_ignore_ascii_case(&qual))
                                })
                                .ok_or_else(|| ExecError::UnknownColumn(name.clone()))
                        })
                        .collect::<Result<_, _>>()?;
                    plan.push(UpdateTarget {
                        qual: qual.clone(),
                        key,
                        reassembly,
                        assigns: Vec::new(),
                        seen: Vec::new(),
                    });
                    plan.last_mut().unwrap()
                }
            };
            tp.assigns.push(UpdateAssignment {
                index: col_idx,
                rhs: assign.value.clone(),
                ty: col_ty,
                default: col_default,
                colname,
            });
        }
        // Apply: each surviving joined row updates each target's base row
        // once.
        let mut affected = 0;
        for jrow in &kept {
            for tp in &mut plan {
                let orig: Row = tp.reassembly.iter().map(|&i| jrow[i].clone()).collect();
                if tp.seen.contains(&orig) {
                    continue; // update-once
                }
                tp.seen.push(orig.clone());
                let resolver = RelResolver::new(&rel.cols, jrow, session.clone());
                let mut updated = orig.clone();
                for assignment in &tp.assigns {
                    let v = if is_default_placeholder(&assignment.rhs) {
                        eval_default(&assignment.default)?
                    } else {
                        eval_in(&assignment.rhs, &resolver)?
                    };
                    updated[assignment.index] = match &assignment.ty {
                        Some(ty) => coerce_column(v, ty, &assignment.colname)?,
                        None => v,
                    };
                }
                self.check_foreign_keys(&tp.key, &updated)?;
                self.propagate_parent_update(&tp.key, &orig, &updated)?;
                affected += i64::from(updated != orig);
                let t = self.tables.get_mut(&tp.key).unwrap();
                if let Some(pos) = t.rows.iter().position(|r| *r == orig) {
                    t.rows[pos] = updated.clone();
                }
                self.rebase_auto_increment_from_row(&tp.key, &updated);
            }
        }
        Ok(affected)
    }

    /// Resolves an unqualified `SET` column in a multi-table `UPDATE` to the
    /// qualifier of the single joined table that declares it — an error if
    /// no table or more than one has a column of that name (ambiguous),
    /// matching MySQL.
    fn resolve_bare_update_column(
        &self,
        colname: &str,
        leaves: &[(String, String)],
    ) -> Result<String, ExecError> {
        let mut found: Option<String> = None;
        for (qual, key) in leaves {
            let has = self
                .tables
                .get(key)
                .is_some_and(|t| t.cols.iter().any(|c| c.eq_ignore_ascii_case(colname)));
            if has {
                if found.is_some() {
                    return Err(ExecError::Unsupported("ambiguous UPDATE column"));
                }
                found = Some(qual.clone());
            }
        }
        found.ok_or_else(|| ExecError::UnknownColumn(colname.to_string()))
    }

    /// Executes a multi-table `DELETE ... FROM/USING <join>`: builds the
    /// join, keeps the rows matching `WHERE`, then for each target table
    /// removes every base row that contributed to a surviving joined row.
    ///
    /// A joined row is a concatenation of its source tables' columns, each
    /// column tagged in the join relation with its table qualifier (the
    /// alias if one was written, else the table name — so a target must
    /// name that qualifier, matching MySQL: `DELETE x FROM t AS x` works,
    /// `DELETE t FROM t AS x` is an "unknown table" error). A target's base
    /// row is reassembled from the joined row by looking up each of the
    /// base table's columns under that qualifier, then deleted by value
    /// (rows carry no identity here, so duplicate identical base rows are
    /// indistinguishable — the same observable outcome as MySQL for the
    /// join it produced).
    fn delete_multi(
        &mut self,
        ignore: bool,
        targets: &[Vec<String>],
        from: &tidb_ast::Join,
        where_clause: &Option<Expr>,
    ) -> Result<i64, ExecError> {
        let rel = self.build_join(from, &[])?;
        // Keep only the joined rows matching WHERE.
        let session = self.session_state();
        let mut kept: Vec<Row> = Vec::new();
        for row in &rel.rows {
            let matches = match where_clause {
                Some(pred) => {
                    let resolver = RelResolver::new(&rel.cols, row, session.clone());
                    let folded = self.resolve_subqueries(pred, &resolver)?;
                    is_truthy(eval_in(&folded, &resolver)?)?
                }
                None => true,
            };
            if matches {
                kept.push(row.clone());
            }
        }
        // Map each target's qualifier to the physical base table it names.
        let leaves = collect_table_leaves(from);
        let mut affected = 0;
        for target in targets {
            let tq = target
                .last()
                .expect("a delete target has at least one segment")
                .to_ascii_lowercase();
            let key = leaves
                .iter()
                .find(|(qual, _)| *qual == tq)
                .map(|(_, key)| key.clone())
                .ok_or_else(|| ExecError::UnknownTable(tq.clone()))?;
            // The join-relation column index for each of the base table's
            // columns, reached under this target's qualifier.
            let base_cols = self.tables.get(&key).unwrap().cols.clone();
            let idxs: Vec<usize> = base_cols
                .iter()
                .map(|name| {
                    rel.cols
                        .iter()
                        .position(|c| {
                            c.name.eq_ignore_ascii_case(name)
                                && c.tables.iter().any(|t| t.eq_ignore_ascii_case(&tq))
                        })
                        .ok_or_else(|| ExecError::UnknownColumn(name.clone()))
                })
                .collect::<Result<_, _>>()?;
            // Reassemble each contributing base row (dedup — a base row can
            // join several times).
            let mut to_delete: Vec<Row> = Vec::new();
            for row in &kept {
                let base_row: Row = idxs.iter().map(|&i| row[i].clone()).collect();
                if !to_delete.contains(&base_row) {
                    to_delete.push(base_row);
                }
            }
            for row in &to_delete {
                match self.delete_row_cascading(&key, row) {
                    Ok(deleted) => affected += i64::from(deleted),
                    // `DELETE IGNORE` downgrades a foreign-key restriction to
                    // a skipped row rather than aborting the statement.
                    Err(ExecError::ForeignKeyViolation) if ignore => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(affected)
    }

    /// Removes `row` from `table`, first propagating to every OTHER
    /// table's `FOREIGN KEY` that references `table` (this table is the
    /// "parent" from their perspective) — a dependent group is skipped
    /// entirely if any of its local columns is `NULL` (MATCH SIMPLE, same
    /// rule `check_foreign_keys` uses); otherwise, per that FK's own `ON
    /// DELETE` action: `CASCADE` recursively removes the matching
    /// dependent rows too (verified via `gorun` to cascade transitively
    /// through multiple FK hops, not assumed one-level-only — so this
    /// recurses), `SET NULL` nulls out their referencing columns in
    /// place, and `RESTRICT`/`NO ACTION`/`SET DEFAULT`/no `ON DELETE`
    /// clause at all — real MySQL doesn't actually implement `SET
    /// DEFAULT` for InnoDB, treating it identically to `RESTRICT`,
    /// confirmed via `gorun` not assumed — rejects `row`'s own removal
    /// (checked BEFORE `row` is actually removed from `table`, so a
    /// rejected `DELETE` never partially removes the row it was rejecting).
    fn delete_row_cascading(&mut self, table: &str, row: &Row) -> Result<bool, ExecError> {
        if !self.foreign_key_checks.is_enabled() {
            if let Some(pos) = self
                .tables
                .get(table)
                .unwrap()
                .rows
                .iter()
                .position(|r| r == row)
            {
                self.tables.get_mut(table).unwrap().rows.remove(pos);
                return Ok(true);
            }
            return Ok(false);
        }
        let dependents: Vec<(String, ForeignKey)> = self
            .tables
            .iter()
            .flat_map(|(tname, t)| {
                t.foreign_keys
                    .iter()
                    .filter(|fk| fk.ref_table == table)
                    .map(|fk| (tname.clone(), fk.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        for (child_table, fk) in dependents {
            let parent_cols = &self.tables.get(table).unwrap().cols;
            let ref_idxs: Vec<usize> = fk
                .ref_cols
                .iter()
                .map(|n| {
                    parent_cols
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(n))
                        .ok_or_else(|| ExecError::UnknownColumn(n.clone()))
                })
                .collect::<Result<_, _>>()?;
            let ref_values: Vec<Datum> = ref_idxs.iter().map(|&i| row[i].clone()).collect();
            let matching: Vec<Row> = self
                .tables
                .get(&child_table)
                .unwrap()
                .rows
                .iter()
                .filter(|crow| {
                    fk.local_cols.iter().all(|&li| crow[li] != Datum::Null)
                        && fk
                            .local_cols
                            .iter()
                            .zip(&ref_values)
                            .all(|(&li, rv)| crow[li] == *rv)
                })
                .cloned()
                .collect();
            if matching.is_empty() {
                continue;
            }
            match fk.on_delete {
                Some(ReferentialAction::Cascade) => {
                    for crow in &matching {
                        self.delete_row_cascading(&child_table, crow)?;
                    }
                }
                Some(ReferentialAction::SetNull) => {
                    let t = self.tables.get_mut(&child_table).unwrap();
                    for crow in &matching {
                        if let Some(pos) = t.rows.iter().position(|r| r == crow) {
                            for &li in &fk.local_cols {
                                t.rows[pos][li] = Datum::Null;
                            }
                        }
                    }
                }
                _ => return Err(ExecError::ForeignKeyViolation),
            }
        }
        let t = self.tables.get_mut(table).unwrap();
        if let Some(pos) = t.rows.iter().position(|r| r == row) {
            t.rows.remove(pos);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Propagates `table`'s own row change (`old_row` → `new_row`, both
    /// full rows) to every OTHER table's `FOREIGN KEY` that references
    /// `table`, mirroring `delete_row_cascading`'s structure but for a
    /// VALUE change rather than a removal. A dependent FK is skipped
    /// entirely unless its REFERENCED columns' values actually differ
    /// between `old_row`/`new_row` — updating a column `table` declares
    /// but no FK references (or updating a referenced column to the SAME
    /// value) never triggers this, confirmed via `gorun`, not assumed
    /// (real TiDB allows both freely even when dependents exist). When a
    /// referenced value DOES change: `CASCADE` propagates the new value
    /// into matching dependents' referencing columns (recursing — a
    /// cascaded value could itself be a column further tables reference,
    /// though this is naturally a no-op recursion when it isn't, since
    /// the recursive call simply finds zero further dependents),
    /// `SET NULL` nulls those columns instead, and
    /// `RESTRICT`/`NO ACTION`/`SET DEFAULT`/no `ON UPDATE` clause at all
    /// rejects the update outright — checked BEFORE `table`'s own row is
    /// committed, same as `delete_row_cascading`.
    fn propagate_parent_update(
        &mut self,
        table: &str,
        old_row: &Row,
        new_row: &Row,
    ) -> Result<(), ExecError> {
        if !self.foreign_key_checks.is_enabled() {
            return Ok(());
        }
        let dependents: Vec<(String, ForeignKey)> = self
            .tables
            .iter()
            .flat_map(|(tname, t)| {
                t.foreign_keys
                    .iter()
                    .filter(|fk| fk.ref_table == table)
                    .map(|fk| (tname.clone(), fk.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        for (child_table, fk) in dependents {
            let parent_cols = &self.tables.get(table).unwrap().cols;
            let ref_idxs: Vec<usize> = fk
                .ref_cols
                .iter()
                .map(|n| {
                    parent_cols
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(n))
                        .ok_or_else(|| ExecError::UnknownColumn(n.clone()))
                })
                .collect::<Result<_, _>>()?;
            let old_values: Vec<Datum> = ref_idxs.iter().map(|&i| old_row[i].clone()).collect();
            let new_values: Vec<Datum> = ref_idxs.iter().map(|&i| new_row[i].clone()).collect();
            if old_values == new_values {
                continue;
            }
            let matching: Vec<Row> = self
                .tables
                .get(&child_table)
                .unwrap()
                .rows
                .iter()
                .filter(|crow| {
                    fk.local_cols.iter().all(|&li| crow[li] != Datum::Null)
                        && fk
                            .local_cols
                            .iter()
                            .zip(&old_values)
                            .all(|(&li, ov)| crow[li] == *ov)
                })
                .cloned()
                .collect();
            if matching.is_empty() {
                continue;
            }
            match fk.on_update {
                Some(ReferentialAction::Cascade) => {
                    for crow in &matching {
                        let mut new_crow = crow.clone();
                        for (&li, nv) in fk.local_cols.iter().zip(&new_values) {
                            new_crow[li] = nv.clone();
                        }
                        self.propagate_parent_update(&child_table, crow, &new_crow)?;
                        let t = self.tables.get_mut(&child_table).unwrap();
                        if let Some(pos) = t.rows.iter().position(|r| r == crow) {
                            t.rows[pos] = new_crow;
                        }
                    }
                }
                Some(ReferentialAction::SetNull) => {
                    let t = self.tables.get_mut(&child_table).unwrap();
                    for crow in &matching {
                        if let Some(pos) = t.rows.iter().position(|r| r == crow) {
                            for &li in &fk.local_cols {
                                t.rows[pos][li] = Datum::Null;
                            }
                        }
                    }
                }
                _ => return Err(ExecError::ForeignKeyViolation),
            }
        }
        Ok(())
    }

    /// Applies an `ON DUPLICATE KEY UPDATE` clause to the conflicting row at
    /// `ridx`: each assignment's value is resolved against the *existing*
    /// row (like an ordinary `UPDATE ... SET`), with `VALUES(col)`
    /// substituted for the row that would have been inserted.
    fn apply_on_duplicate(
        &mut self,
        key: &str,
        ridx: usize,
        assignments: &[Assignment],
        cols: &[String],
        proposed: &Row,
    ) -> Result<bool, ExecError> {
        let (existing, col_defaults) = {
            let table = self.tables.get(key).unwrap();
            (table.rows[ridx].clone(), table.col_defaults.clone())
        };
        let rel_cols: Vec<Column> = cols
            .iter()
            .map(|n| Column {
                tables: Vec::new(),
                name: n.clone(),
            })
            .collect();
        let mut updated = existing.clone();
        for assign in assignments {
            let colname = assign
                .col
                .last()
                .ok_or(ExecError::Unsupported("empty assignment column"))?;
            let idx = cols
                .iter()
                .position(|c| c.eq_ignore_ascii_case(colname))
                .ok_or_else(|| ExecError::UnknownColumn(colname.clone()))?;
            let v = if is_default_placeholder(&assign.value) {
                eval_default(
                    col_defaults
                        .get(idx)
                        .ok_or(ExecError::Unsupported("missing column default metadata"))?,
                )?
            } else {
                let resolved = resolve_values_fn(&assign.value, cols, proposed)?;
                let resolver = RelResolver::new(&rel_cols, &existing, self.session_state());
                eval_in(&resolved, &resolver)?
            };
            updated[idx] = v;
        }
        self.check_foreign_keys(key, &updated)?;
        let changed = updated != existing;
        self.tables.get_mut(key).unwrap().rows[ridx] = updated;
        Ok(changed)
    }

    /// Checks `row`'s `FOREIGN KEY` columns (this table is the "child")
    /// against each constraint's referenced ("parent") table: a group is
    /// skipped entirely if ANY of its local values is `NULL` (MATCH
    /// SIMPLE — MySQL/TiDB's default, confirmed via `gorun` for composite
    /// keys too: `(1, NULL)` and `(NULL, 1)` both bypass the check even
    /// with no matching parent row); otherwise at least one parent row
    /// must match on every referenced column. This is the "child side"
    /// (an `INSERT`/`UPDATE` into the CONSTRAINT-DECLARING table); the
    /// "parent side" (a `DELETE` on the REFERENCED table cascading,
    /// restricting, or nulling out dependents) is `delete_row_cascading`'s
    /// own responsibility, not this one's — a table's own foreign keys
    /// are cloned out first so this can run while the caller still holds
    /// other `self.tables` borrows.
    fn check_foreign_keys(&self, table: &str, row: &Row) -> Result<(), ExecError> {
        if !self.foreign_key_checks.is_enabled() {
            return Ok(());
        }
        let fks = self.tables.get(table).unwrap().foreign_keys.clone();
        for fk in &fks {
            if fk.local_cols.iter().any(|&i| row[i] == Datum::Null) {
                continue;
            }
            let parent = self
                .tables
                .get(&fk.ref_table)
                .ok_or_else(|| ExecError::UnknownTable(fk.ref_table.clone()))?;
            let ref_idxs: Vec<usize> = fk
                .ref_cols
                .iter()
                .map(|n| {
                    parent
                        .cols
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(n))
                        .ok_or_else(|| ExecError::UnknownColumn(n.clone()))
                })
                .collect::<Result<_, _>>()?;
            let matches = parent.rows.iter().any(|prow| {
                fk.local_cols
                    .iter()
                    .zip(&ref_idxs)
                    .all(|(&li, &ri)| row[li] == prow[ri])
            });
            if !matches {
                return Err(ExecError::ForeignKeyViolation);
            }
        }
        Ok(())
    }
}

/// Rewrites `VALUES(col)` references in an `ON DUPLICATE KEY UPDATE`
/// assignment to the literal value the proposed row would have inserted for
/// that column. Non-`VALUES` nodes are rebuilt structurally (mirroring
/// `crate::subquery`'s approach) so a nested reference — e.g.
/// `count = count + VALUES(count)` — is also resolved.
fn resolve_values_fn(e: &Expr, cols: &[String], proposed: &Row) -> Result<Expr, ExecError> {
    let rec = |x: &Expr| resolve_values_fn(x, cols, proposed);
    Ok(match e {
        Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("VALUES") => {
            let [Expr::Column(path)] = args.as_slice() else {
                return Err(ExecError::Unsupported("VALUES() argument"));
            };
            let colname = path
                .last()
                .ok_or(ExecError::Unsupported("VALUES() argument"))?;
            let idx = cols
                .iter()
                .position(|c| c.eq_ignore_ascii_case(colname))
                .ok_or_else(|| ExecError::UnknownColumn(colname.clone()))?;
            value_to_literal(proposed[idx].clone())
        }
        Expr::Unary(op, x) => Expr::Unary(*op, Box::new(rec(x)?)),
        Expr::Binary(op, l, r) => Expr::Binary(*op, Box::new(rec(l)?), Box::new(rec(r)?)),
        Expr::Paren(x) => Expr::Paren(Box::new(rec(x)?)),
        Expr::Assign { name, value } => Expr::Assign {
            name: name.clone(),
            value: Box::new(rec(value)?),
        },
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => Expr::Trim {
            expr: Box::new(rec(expr)?),
            remstr: remstr.as_deref().map(rec).transpose()?.map(Box::new),
            direction: *direction,
        },
        Expr::Position { substr, str } => Expr::Position {
            substr: Box::new(rec(substr)?),
            str: Box::new(rec(str)?),
        },
        Expr::WeightString { expr, as_type } => Expr::WeightString {
            expr: Box::new(rec(expr)?),
            as_type: *as_type,
        },
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            origin_position: *origin_position,
            args: args.iter().map(rec).collect::<Result<_, _>>()?,
        },
        Expr::GenericFuncCall {
            schema,
            name,
            args,
            origin_position,
        } => Expr::GenericFuncCall {
            schema: schema.clone(),
            name: name.clone(),
            origin_position: *origin_position,
            args: args.iter().map(rec).collect::<Result<_, _>>()?,
        },
        other => other.clone(),
    })
}

/// Enforces a declared column type's storage width on a value about to be
/// stored, returning the (possibly truncated/trimmed) value or an error.
///
/// `VARCHAR(n)` and `CHAR(n)` length are modelled. The shared length rule,
/// confirmed via `gorun` (not assumed): a string of at most `n` characters
/// passes; a longer string is truncated to `n` characters IF every excess
/// character is a space (`'abc '` in width 3 → `'abc'`, silently),
/// otherwise it is a `DataTooLong` error (`'abcd'` → error). Length is
/// counted in Unicode characters, not bytes (`'中文字八'`, 4 chars,
/// overflows width 3).
///
/// The two types then diverge on STORAGE: `VARCHAR` keeps trailing spaces
/// that are within the length (`'ab '` in `VARCHAR(3)` stays `'ab '`),
/// while `CHAR` strips ALL trailing spaces on storage (`'ab '` in
/// `CHAR(3)` becomes `'ab'`, and `'a  '` becomes `'a'`) — MySQL's CHAR
/// right-trim, applied AFTER the length check, so `'abc  '` in `CHAR(3)`
/// first truncates its two excess spaces to `'abc'` then trims to `'abc'`
/// (both confirmed via `gorun`).
///
/// `BIT(n)` width is also enforced: the value's numeric magnitude must fit
/// in `n` bits (`b'1000'` = 8 overflows `BIT(3)`, whose max is 7 — a
/// `DataTooLong` error, matching real TiDB's own `gorun` `ERR`). Both a
/// bit-literal input (stored as minimal big-endian bytes — see
/// `tidb_expr::binary_literal`, task #117) and an integer input are
/// measured by that same magnitude.
///
/// `DECIMAL(p,s)`/`NUMERIC(p,s)` integer-digit overflow is enforced by
/// rounding to scale `s` FIRST, then range-checking the integer part
/// against `p - s` digits — `99.995` rounds to `100.00` and overflows
/// `DECIMAL(4,2)` (an `OutOfRange` error, distinct from the string/bit
/// `DataTooLong`), while `99.994` rounds to `99.99` and fits. See
/// [`tidb_expr::fit_decimal_column`]. `INT`/`BIGINT` use the declared
/// signedness and width too: unsigned storage is a real [`Datum::UInt`],
/// never a decimal-string workaround, so keys, comparison, and ordering all
/// retain the source domain after DML.
fn coerce_column(value: Datum, ty: &ColumnType, col_name: &str) -> Result<Datum, ExecError> {
    if let Some(kind) = IntegerColumnKind::from_column_type(ty) {
        return coerce_integer_column(value, kind, col_name);
    }
    if ty.name.eq_ignore_ascii_case("BIT") {
        return coerce_bit(value, ty, col_name);
    }
    if ty.name.eq_ignore_ascii_case("DECIMAL") || ty.name.eq_ignore_ascii_case("NUMERIC") {
        // `DECIMAL(p, s)` defaults to `(10, 0)` when args are omitted, and
        // to scale 0 when only `p` is given — matching MySQL.
        let precision = ty
            .args
            .first()
            .and_then(|a| a.as_text_lossy().parse::<u32>().ok())
            .unwrap_or(10);
        let scale = ty
            .args
            .get(1)
            .and_then(|a| a.as_text_lossy().parse::<u32>().ok())
            .unwrap_or(0);
        return match tidb_expr::fit_decimal_column(value, precision, scale) {
            Some(v) => Ok(v),
            None => Err(ExecError::OutOfRange(col_name.to_string())),
        };
    }
    let is_char = ty.name.eq_ignore_ascii_case("CHAR");
    let is_varchar = ty.name.eq_ignore_ascii_case("VARCHAR");
    if !is_char && !is_varchar {
        return Ok(value);
    }
    let Datum::String(s) = &value else {
        return Ok(value);
    };
    let s = s
        .as_utf8()
        .map_err(|_| ExecError::Unsupported("invalid UTF-8 character column value"))?;
    let Some(len) = ty
        .args
        .first()
        .and_then(|a| a.as_text_lossy().parse::<usize>().ok())
    else {
        return Ok(value);
    };
    // Shared length rule: pass within `len`, else truncate iff the excess
    // is all spaces, else error.
    let truncated: String = if s.chars().count() <= len {
        // Avoid a clone in the common (in-length, non-`CHAR`) case.
        if !is_char {
            return Ok(value);
        }
        s.to_string()
    } else if s.chars().skip(len).all(|c| c == ' ') {
        s.chars().take(len).collect()
    } else {
        return Err(ExecError::DataTooLong(col_name.to_string()));
    };
    // `CHAR` additionally right-trims trailing spaces on storage.
    Ok(Datum::new_string(if is_char {
        truncated.trim_end_matches(' ').to_string()
    } else {
        truncated
    }))
}

/// The two integer storage shapes that the current seed executes. Keeping
/// this DML-local rather than adding a second schema representation means
/// `ColumnType { name, unsigned }` remains the sole DDL truth: every INSERT
/// and UPDATE call site already reaches [`coerce_column`].
#[derive(Clone, Copy)]
enum IntegerColumnKind {
    Signed { lower: i64, upper: i64 },
    Unsigned { upper: u64 },
}

/// Extracts the only explicit ID values that can rebase an allocator after
/// the normal column-coercion funnel has established its signed/unsigned
/// storage shape. Zero is intentionally absent: it allocates only in INSERT.
fn auto_increment_positive(value: &Datum) -> Option<u64> {
    match value {
        Datum::Int(value) if *value > 0 => Some(*value as u64),
        Datum::UInt(value) if *value > 0 => Some(*value),
        _ => None,
    }
}

impl IntegerColumnKind {
    fn from_column_type(ty: &ColumnType) -> Option<Self> {
        let (lower, signed_upper, unsigned_upper) = match ty.name.as_str() {
            "INT" | "INTEGER" => (
                i64::from(i32::MIN),
                i64::from(i32::MAX),
                u64::from(u32::MAX),
            ),
            "BIGINT" => (i64::MIN, i64::MAX, u64::MAX),
            _ => return None,
        };
        Some(if ty.unsigned {
            Self::Unsigned {
                upper: unsigned_upper,
            }
        } else {
            Self::Signed {
                lower,
                upper: signed_upper,
            }
        })
    }
}

/// An integer after source-shaped rounding but before a destination column's
/// range check. Negative magnitudes deliberately remain unsigned so
/// `BIGINT`'s `-2^63` has a representation and is not accidentally narrowed
/// before the signed target can accept it.
enum RoundedInteger {
    Negative(u64),
    Nonnegative(u64),
}

/// Coerces a value into the exact storage domain of an `INT`/`BIGINT`
/// column. This ports the strict assignment path through Go's
/// `Datum.ConvertTo`/`convertToInt`/`convertToUint`: negatives and values
/// above the declared bound are errors, not non-strict-mode clamp-and-warning
/// substitutes. Decimal/string values round half away from zero; native
/// floating values use Go's `RoundFloat` ties-to-even rule.
fn coerce_integer_column(
    value: Datum,
    kind: IntegerColumnKind,
    col_name: &str,
) -> Result<Datum, ExecError> {
    let out_of_range = || ExecError::OutOfRange(col_name.to_string());
    let rounded = match value {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int(value) if value < 0 => RoundedInteger::Negative(value.unsigned_abs()),
        Datum::Int(value) => RoundedInteger::Nonnegative(value as u64),
        Datum::UInt(value) => RoundedInteger::Nonnegative(value),
        Datum::Decimal(value) => {
            rounded_decimal_integer(&value.to_string()).ok_or_else(out_of_range)?
        }
        Datum::Real(value) => rounded_float_integer(value).ok_or_else(out_of_range)?,
        Datum::String(value) => rounded_string_integer(
            value
                .as_utf8()
                .map_err(|_| ExecError::OutOfRange(col_name.to_string()))?,
        )
        .ok_or_else(out_of_range)?,
        Datum::Bytes(_) => return Err(out_of_range()),
        Datum::MinNotNull | Datum::MaxValue => return Err(out_of_range()),
    };
    match (kind, rounded) {
        (IntegerColumnKind::Unsigned { upper }, RoundedInteger::Nonnegative(value))
            if value <= upper =>
        {
            Ok(Datum::UInt(value))
        }
        (IntegerColumnKind::Unsigned { .. }, _) => Err(out_of_range()),
        (IntegerColumnKind::Signed { upper, .. }, RoundedInteger::Nonnegative(value))
            if value <= upper as u64 =>
        {
            Ok(Datum::Int(value as i64))
        }
        (IntegerColumnKind::Signed { lower, .. }, RoundedInteger::Negative(magnitude))
            if magnitude <= lower.unsigned_abs() =>
        {
            Ok(Datum::Int(if magnitude == i64::MIN.unsigned_abs() {
                i64::MIN
            } else {
                -(magnitude as i64)
            }))
        }
        (IntegerColumnKind::Signed { .. }, _) => Err(out_of_range()),
    }
}

/// Rounds an exact decimal spelling to an integer, ties away from zero — the
/// rule Go uses for DECIMAL and numeric-string assignment. Decimal's display
/// spelling is canonical and exponent-free; string exponents take the float
/// fallback in [`rounded_string_integer`] below.
fn rounded_decimal_integer(text: &str) -> Option<RoundedInteger> {
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
    if (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let mut magnitude = if integer.is_empty() {
        0
    } else {
        integer.parse::<u64>().ok()?
    };
    if fraction
        .as_bytes()
        .first()
        .is_some_and(|byte| *byte >= b'5')
    {
        magnitude = magnitude.checked_add(1)?;
    }
    Some(if negative && magnitude != 0 {
        RoundedInteger::Negative(magnitude)
    } else {
        RoundedInteger::Nonnegative(magnitude)
    })
}

/// Parses a strict SQL string assignment. In strict mode any malformed tail
/// is an error rather than the non-strict truncation warning path. Both plain
/// and scientific decimal text remain exact — source `StrToUint` expands the
/// exponent as decimal text before its UInt64 range/rounding check, so an f64
/// fallback would lose the `u64::MAX` boundary.
fn rounded_string_integer(text: &str) -> Option<RoundedInteger> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if !text.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
        return rounded_decimal_integer(text);
    }
    rounded_decimal_integer(&expand_scientific_decimal(text)?)
}

/// Exact, bounded counterpart of TiDB's `convertScientificNotation` for an
/// integer-column assignment. It only materializes the digits that can affect
/// a UInt64 result: an integer part beyond 20 significant digits is already
/// out of range, and after the decimal point only the first fractional digit
/// can change half-away rounding. This makes enormous exponents a clean range
/// error instead of an allocation hazard or a precision-losing f64 parse.
fn expand_scientific_decimal(text: &str) -> Option<String> {
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let exponent_at = text.bytes().position(|byte| matches!(byte, b'e' | b'E'))?;
    let (mantissa, exponent) = text.split_at(exponent_at);
    let exponent = exponent.strip_prefix(['e', 'E'])?.parse::<i64>().ok()?;
    let mut decimal_seen = false;
    let mut digits_before_decimal = 0_i64;
    let mut digits = String::new();
    for byte in mantissa.bytes() {
        match byte {
            b'.' if !decimal_seen => decimal_seen = true,
            byte if byte.is_ascii_digit() => {
                if !decimal_seen {
                    digits_before_decimal += 1;
                }
                digits.push(byte as char);
            }
            _ => return None,
        }
    }
    if digits.is_empty() {
        return None;
    }
    let leading_zeros = digits.bytes().take_while(|byte| *byte == b'0').count();
    let digits = &digits[leading_zeros..];
    if digits.is_empty() {
        return Some("0".to_string());
    }
    let decimal_at = digits_before_decimal.checked_sub(leading_zeros as i64)?;
    let decimal_at = decimal_at.checked_add(exponent)?;
    // A nonzero magnitude with more than 20 integer digits is above UInt64.
    // With the point at or left of the second fractional place, rounding can
    // never reach one, so normalizing directly to zero is exact.
    if decimal_at > 20 {
        return None;
    }
    if decimal_at < 0 {
        return Some("0".to_string());
    }
    let needed = if decimal_at == 0 {
        1
    } else {
        (decimal_at as usize).saturating_add(1)
    };
    let digits = &digits[..digits.len().min(needed)];
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    match decimal_at {
        0 => {
            out.push_str("0.");
            out.push_str(digits);
        }
        decimal_at if decimal_at as usize >= digits.len() => {
            out.push_str(digits);
            out.extend(std::iter::repeat_n('0', decimal_at as usize - digits.len()));
        }
        decimal_at => {
            let decimal_at = decimal_at as usize;
            out.push_str(&digits[..decimal_at]);
            out.push('.');
            out.push_str(&digits[decimal_at..]);
        }
    }
    Some(out)
}

/// Ports `types.RoundFloat` (`math.RoundToEven`) before an integer-column
/// range check. The f64 path is necessarily limited to values representable
/// as a float; exact UInt64 literals enter as [`Datum::UInt`] and never pass
/// through this lossy conversion.
fn rounded_float_integer(value: f64) -> Option<RoundedInteger> {
    if !value.is_finite() {
        return None;
    }
    let rounded = value.round_ties_even();
    if rounded >= 0.0 {
        // `u64::MAX as f64` is already rounded to 2^64, so equality is an
        // overflow too. Exact max values use Datum::UInt/Decimal instead.
        if rounded >= u64::MAX as f64 {
            return None;
        }
        Some(RoundedInteger::Nonnegative(rounded as u64))
    } else {
        let magnitude = -rounded;
        if magnitude >= u64::MAX as f64 {
            return None;
        }
        let magnitude = magnitude as u64;
        Some(if magnitude == 0 {
            RoundedInteger::Nonnegative(0)
        } else {
            RoundedInteger::Negative(magnitude)
        })
    }
}

/// One target of a multi-table `UPDATE`, grouped by table qualifier: the
/// physical base table, how to reassemble its row from a joined row
/// (`reassembly[i]` is the join-relation index of the base table's i-th
/// column), the assignments writing to it, and the set of base rows already
/// updated (for the update-once rule).
struct UpdateAssignment {
    index: usize,
    rhs: Expr,
    ty: Option<ColumnType>,
    default: Option<Expr>,
    colname: String,
}

struct UpdateTarget {
    qual: String,
    key: String,
    reassembly: Vec<usize>,
    assigns: Vec<UpdateAssignment>,
    seen: Vec<Row>,
}

/// Collects every base-table leaf of a `FROM`/`USING` join as
/// `(qualifier, table_key)` pairs — the qualifier is the table's alias if
/// written, else its name; both lowercased for case-insensitive target
/// matching. Derived-table leaves are skipped: they can't be `DELETE`
/// targets.
fn collect_table_leaves(join: &tidb_ast::Join) -> Vec<(String, String)> {
    let mut out = Vec::new();
    collect_node_leaves(&join.left, &mut out);
    if let Some(right) = &join.right {
        collect_node_leaves(right, &mut out);
    }
    out
}

fn join_has_derived_table(join: &tidb_ast::Join) -> bool {
    node_has_derived_table(&join.left) || join.right.as_ref().is_some_and(node_has_derived_table)
}

fn node_has_derived_table(node: &tidb_ast::JoinNode) -> bool {
    match node {
        tidb_ast::JoinNode::Derived { .. } => true,
        tidb_ast::JoinNode::Table(_) => false,
        tidb_ast::JoinNode::Join(join) => join_has_derived_table(join),
    }
}

fn collect_node_leaves(node: &tidb_ast::JoinNode, out: &mut Vec<(String, String)>) {
    match node {
        tidb_ast::JoinNode::Table(tref) => {
            let key = table_key(&tref.name);
            let qual = tref.alias.clone().unwrap_or_else(|| key.clone());
            out.push((qual.to_ascii_lowercase(), key));
        }
        tidb_ast::JoinNode::Join(j) => {
            collect_node_leaves(&j.left, out);
            if let Some(right) = &j.right {
                collect_node_leaves(right, out);
            }
        }
        // A derived table cannot be a delete target.
        tidb_ast::JoinNode::Derived { .. } => {}
    }
}

/// True if `expr` is a bare `DEFAULT` value placeholder, meaning "use
/// this column's declared default". This is shared by INSERT values, ON
/// DUPLICATE KEY UPDATE, and single-table UPDATE. `DEFAULT(col)` is not this.
fn is_default_placeholder(expr: &Expr) -> bool {
    matches!(expr, Expr::Default(None))
}

/// Evaluates a column's declared `DEFAULT`, or `NULL` if it has none.
fn eval_default(default: &Option<Expr>) -> Result<Datum, ExecError> {
    match default {
        Some(e) => Ok(eval(e)?),
        None => Ok(Datum::Null),
    }
}

/// Builds one full-width (`cols.len()`) row from an `INSERT`'s written
/// values, resolving column defaults. `provided` are the value expressions
/// as written; `insert_cols` is the statement's column list — empty means
/// positional (every column, in order), otherwise it names the target
/// columns (an explicit `(a, b)` list or a desugared `SET` form). Every
/// column not written gets its declared default (or `NULL`); a written
/// bare `DEFAULT` value resolves to that column's default too.
fn assemble_row(
    cols: &[String],
    col_defaults: &[Option<Expr>],
    insert_cols: &[String],
    provided: &[Expr],
) -> Result<Row, ExecError> {
    // Which table-column index each provided value targets.
    let targets: Vec<usize> = if insert_cols.is_empty() && provided.is_empty() {
        // `INSERT INTO t VALUES ()` is the all-default row form, not a
        // zero-column positional row. It reaches the same default assembly
        // as an explicit sparse column list, allowing an AUTO_INCREMENT
        // column to see its default NULL allocation request afterward.
        Vec::new()
    } else if insert_cols.is_empty() {
        (0..cols.len()).collect()
    } else {
        insert_cols
            .iter()
            .map(|name| {
                cols.iter()
                    .position(|c| c.eq_ignore_ascii_case(name))
                    .ok_or_else(|| ExecError::UnknownColumn(name.clone()))
            })
            .collect::<Result<_, _>>()?
    };
    if targets.len() != provided.len() {
        return Err(ExecError::ColumnCountMismatch);
    }
    // Start every column at its default, then overwrite the written ones.
    let mut row = col_defaults
        .iter()
        .map(eval_default)
        .collect::<Result<Row, _>>()?;
    for (&t, expr) in targets.iter().zip(provided) {
        row[t] = if is_default_placeholder(expr) {
            eval_default(&col_defaults[t])?
        } else {
            eval(expr)?
        };
    }
    Ok(row)
}

/// `BIT(n)` width enforcement — the value passes through unchanged if its
/// numeric magnitude fits in `n` bits, else it is a `DataTooLong` error.
/// A `Datum::String` (a bit-literal's minimal big-endian bytes, or any byte
/// string) is read as a big-endian unsigned integer; a `Datum::Int` is
/// its own magnitude. `NULL`/negative/over-64-bit/other values pass
/// through here (validating them is out of this slice's scope). `n` comes
/// from the type arg — always present, since `BIT` with no explicit width
/// materializes `BIT(1)` (task #123).
fn coerce_bit(value: Datum, ty: &ColumnType, col_name: &str) -> Result<Datum, ExecError> {
    let Some(n) = ty
        .args
        .first()
        .and_then(|a| a.as_text_lossy().parse::<u32>().ok())
    else {
        return Ok(value);
    };
    let magnitude = match &value {
        Datum::Int(i) if *i >= 0 => *i as u64,
        Datum::String(s) if s.bytes().len() <= 8 => s
            .bytes()
            .iter()
            .fold(0u64, |acc, b| (acc << 8) | u64::from(*b)),
        Datum::Bytes(s) if s.len() <= 8 => s.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b)),
        _ => return Ok(value),
    };
    let max = if n >= 64 { u64::MAX } else { (1u64 << n) - 1 };
    if magnitude > max {
        return Err(ExecError::DataTooLong(col_name.to_string()));
    }
    Ok(value)
}
