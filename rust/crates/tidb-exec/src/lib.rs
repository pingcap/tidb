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

//! A minimal query executor — the Phase-1 seed of the design's `tidb-exec`.
//!
//! Two entry points share the same operators:
//! - [`execute`] runs a *table-less* `SELECT` / set operation over a single
//!   synthetic row (constant queries).
//! - [`Database`] holds an in-memory catalog and runs a script. `CREATE TABLE`
//!   builds a table, tracking every `PRIMARY KEY` / `UNIQUE` constraint it
//!   declares — a single column's own option or a table-level composite
//!   `(col1, col2, ...)` form are unified into one representation, and a row
//!   matching an existing row on ANY of these groups (every column in that
//!   group, for a composite key) is a duplicate-key **conflict**, exactly
//!   like real MySQL does not distinguish which constraint was violated.
//!   `INSERT` builds rows — or, on a conflict,
//!   applies **`ON DUPLICATE KEY UPDATE`** if present (its assignments are
//!   resolved against the *existing* row, with `VALUES(col)` substituted for
//!   the row that would have been inserted), or — under **`IGNORE`** —
//!   silently keeps the existing row instead. Standalone **`UPDATE ... SET
//!   ... [WHERE ...]`** and **`DELETE FROM ... [WHERE ...]`** (single
//!   table only, matching the parser's own subset) reuse the exact same
//!   `WHERE` filtering `SELECT` does; `UPDATE`'s `SET` expressions are all
//!   evaluated against the row's ORIGINAL values before any assignment is
//!   applied — `SET a = a + 1, b = a` leaves `b` at the OLD `a`, not the
//!   new one (confirmed via `gorun`, not assumed: MySQL treats a `SET`
//!   clause as simultaneous, not chained) — and a `PRIMARY KEY`/`UNIQUE`
//!   conflict newly created by an `UPDATE` is not modelled (out of scope,
//!   like a duplicate `INSERT` with no `ON DUPLICATE KEY UPDATE` clause).
//!   A table-level **`FOREIGN KEY`** is fully enforced on both the child
//!   and parent sides, for both `INSERT`/`UPDATE` (child) and `DELETE`/
//!   `UPDATE` (parent): on the "child side," every `INSERT`/`UPDATE` into
//!   the table THAT DECLARES the constraint re-checks its referencing
//!   columns against the referenced ("parent") table — a group is
//!   skipped entirely if ANY of its local values is `NULL` (MATCH SIMPLE,
//!   MySQL/TiDB's default, confirmed for composite keys too via
//!   `gorun`), otherwise at least one parent row must match on every
//!   referenced column, else [`ExecError::ForeignKeyViolation`]. On the
//!   "parent side," a `DELETE` from a referenced row, OR an `UPDATE` that
//!   actually changes a referenced column's VALUE (touching an
//!   unreferenced column, or "changing" a referenced one to the SAME
//!   value, never triggers this — confirmed via `gorun`, not assumed),
//!   propagates per each dependent FK's own `ON DELETE`/`ON UPDATE`
//!   action: `CASCADE` recursively removes/repoints matching dependent
//!   rows too — verified via `gorun` to cascade TRANSITIVELY through
//!   multiple `FOREIGN KEY` hops, not just one level — `SET NULL` nulls
//!   out their referencing columns instead, and `RESTRICT`/`NO ACTION`/
//!   `SET DEFAULT`/no `ON DELETE`/`ON UPDATE` clause at all — real MySQL
//!   doesn't actually implement `SET DEFAULT` for InnoDB, treating it
//!   identically to `RESTRICT`, confirmed via `gorun` not assumed —
//!   rejects the statement outright.
//!   **`ALTER TABLE`** supports
//!   `ADD COLUMN` (backfilling every existing row with the new column's
//!   `DEFAULT`, or `NULL` if it has none — matching real TiDB's instant-add
//!   behavior — at the position `FIRST`/`AFTER col` names, or the end if
//!   neither was written, shifting every existing `PRIMARY KEY`/`UNIQUE`
//!   tracked index that comes at or after the insertion point) and
//!   `DROP COLUMN` (removing it from every row and from any `key_groups`
//!   entry, dropping a group left empty), `MODIFY COLUMN` (repositioning an
//!   existing column via `FIRST`/`AFTER`, leaving its stored values and name
//!   untouched), `CHANGE COLUMN` (like `MODIFY`, but also renames it) —
//!   both reposition via a shared `reposition_column` helper that reindexes
//!   `key_groups` by simulating the same move on a plain index vector,
//!   rather than hand-deriving shift arithmetic for an arbitrary move — and
//!   `RENAME` (moves the catalog entry itself, rows/columns/`key_groups`
//!   untouched; the old name no longer resolves) — via a shared
//!   `rename_table` helper also used by the standalone **`RENAME TABLE`**
//!   statement, whose several `old TO new` pairs apply sequentially in
//!   written order (correctly performing a multi-table swap through a
//!   temporary name, verified against real TiDB), `ADD INDEX` (a plain
//!   secondary index — a structural no-op, since this executor always does
//!   full table scans), and `ADD UNIQUE` (appends a new `key_groups`
//!   entry, extending conflict detection to an already-populated table).
//!   `SELECT` builds a **relation** —
//!   a table scan, a **derived table** `(SELECT …) AS alias`, or a nested-loop **join**
//!   of any of those (inner/cross, `LEFT`, `RIGHT`, with `ON` or `USING`
//!   predicates and table aliases, chaining to any number of tables) — then
//!   applies **selection** (a `NULL`/zero `WHERE` drops the row, with
//!   `IN`/`BETWEEN`/`IS NULL` predicates and **subqueries** — scalar
//!   `= (SELECT …)`, `IN (SELECT …)`, `[NOT] EXISTS`, `<cmp> ANY|ALL
//!   (SELECT …)`, correlated or not — resolved per row against the enclosing
//!   scope), **projection** (including `*`, a qualified wildcard `t.*` /
//!   `db.t.*` that scopes the expansion to just that table/alias's columns
//!   instead of every column in the join — the same scoping applies inside a
//!   derived table's own select list, requalified under its alias —,
//!   qualified `t.col` references, and subqueries — resolved the same way as
//!   in `WHERE`), **grouping and
//!   aggregation** (`GROUP BY` with `COUNT`/`SUM`/`AVG`/`MAX`/`MIN`,
//!   variance/standard deviation and `BIT_*`, including
//!   `COUNT(DISTINCT)`; `SUM` folds `Int`/`Decimal`/`Float` via the same
//!   exact `+` a constant expression uses, so a decimal column's `SUM`
//!   needs no rounding either; `AVG` is `SUM` divided by the non-`NULL`
//!   count via [`tidb_expr::avg_of`], which for `Int`/`Decimal` grows the
//!   result scale by the session's MySQL `div_precision_increment` and ROUNDS to
//!   it — the one aggregate that needs true division, not exact digit
//!   arithmetic — but for `Float` is plain native `f64` division instead
//!   (that scale-growth rule is `Decimal`-specific, confirmed via `gorun`,
//!   not assumed to also apply to a real `DOUBLE` column's `AVG`);
//!   `MAX`/`MIN`/`ORDER BY` cover `Float` too (see `aggregate::value_cmp`/
//!   `order::sort_value_cmp`); **`GROUP_CONCAT`**
//!   concatenates each row's arguments exactly like `CONCAT` (NULL
//!   propagation and int/string coercion reused as-is, not reimplemented) —
//!   a row whose concatenation is `NULL` contributes nothing — then joins
//!   the non-`NULL` per-row results with its separator, deduping first
//!   under `DISTINCT`; a non-aggregate
//!   subquery in the select list or **`HAVING`** resolves against the
//!   group's first row, correlated or not, and so does a subquery inside an
//!   aggregate's own argument — `SUM((SELECT …))` — resolved per row before
//!   folding),
//!   **set operations** (`UNION`/`UNION ALL`/`EXCEPT`/`INTERSECT`), and a
//!   final **`ORDER BY`** / **`LIMIT`** stage (ordering by a column, an
//!   aggregate, a non-selected column, or a position). A `USING` join
//!   **coalesces** its named columns into one physical column reachable under
//!   either side's qualifier (or unqualified) — matching MySQL: `SELECT *`
//!   shows it once, not twice.
//!
//! Anything outside [`tidb_expr`]'s value domain or beyond this subset
//! returns [`ExecError`], so results-ring coverage against real TiDB
//! execution is measured, not assumed.
//!
//! **`SET timestamp = <epoch>`** / **`SET time_zone = <zone>`** (a single
//! session-variable assignment — [`tidb_ast::SetStmt`]'s own doc has the
//! scope note; only these two variable names are recognized, any other is
//! `Unsupported`) control Database's one session clock setting: a stored
//! non-default `timestamp` text fixes the epoch and is preserved for
//! `@@timestamp`; exactly `timestamp = 0` and `DEFAULT` use the live statement
//! clock. `Database::run` captures that live clock once per statement,
//! matching TiDB's StatementContext cache.
//! `Database::now_value` returns `(utc_secs, nanos,
//! tz_offset_seconds)` — the RAW clock, never pre-adjusted), backing
//! `NOW()`/`CURRENT_TIMESTAMP()`/`CURDATE()`/`CURTIME()`/`UTC_TIMESTAMP()`/
//! `UTC_DATE()`/`UTC_TIME()` (`tidb_expr::Columns::now`; the LOCAL-vs-UTC
//! adjustment itself happens per function in `tidb_expr::func`, not here).
//! Fixed UTC offsets plus source-observable `SYSTEM`/`UTC` labels are
//! modelled for `time_zone`; named IANA zones remain out of scope because
//! this seed carries no timezone database. `SYSTEM` therefore keeps its
//! correct readback label but has a deterministic zero clock offset rather
//! than inheriting a host-dependent zone. Every `RelResolver` constructed
//! while executing it carries the SAME value, matching real MySQL's "the
//! clock is fixed once per statement, not once per call" semantics. A
//! clock-reading function inside an `INSERT ... VALUES`
//! row is out of scope (rows are const-evaluated via `tidb_expr::eval`, a
//! pre-existing boundary, not new here) — every other expression-
//! evaluation site (`SELECT` projection/`WHERE`, `UPDATE ... SET`/`WHERE`,
//! `DELETE ... WHERE`, join `ON`) reads it correctly.
//!
//! Transaction control is owned by `crate::transaction::TransactionState`.
//! Its typed phase is either idle or active with one rollback catalog and its
//! savepoints, so checkpoints cannot outlive their transaction. `BEGIN`, a
//! lazy non-autocommit table access, commit/rollback, DDL implicit commit,
//! savepoint ordering, and the false-to-true autocommit commit transition all
//! route through that one owner. The active catalog image is deliberately an
//! in-memory seed model, not an MVCC snapshot or `tidb-txnkv` client.
//!
//! **`SET @name = value` user variables** (`tidb_ast::SessionStmt::SetUserVar`,
//! `Database::exec_set_uservar`) are a genuinely SEPARATE feature from
//! `@@sysvar` above — EVERY name is inherently valid here (an unset one
//! reads `NULL`, never an error, confirmed via `gorun`) and the value is
//! stored in a NEW, always-mutable `Database.user_vars` map, not a fixed
//! pair of tracked fields. `value` gets the SAME table-less resolver +
//! subquery-resolution treatment `crate::select::execute_select`'s no-`FROM`
//! `SELECT` path already uses (confirmed via `gorun`: it may reference
//! OTHER already-set user variables, including the name being assigned
//! itself, `SET @x := @x + 1`, and subqueries). User variables are
//! session-scoped, NOT transactional (survive a LATER `ROLLBACK`/DDL,
//! unlike table data — confirmed via `gorun`) — `Database::run` never
//! touches `user_vars` when the transaction phase ends.
//! `SessionState` gained a `user_vars: BTreeMap<String, Datum>`
//! snapshot field alongside `now`/`autocommit`/`tz_*` — since a
//! `BTreeMap` can't be `Copy`, `SessionState` itself is now `Clone`-only,
//! so the handful of construction sites that reuse ONE `SessionState`
//! across a loop/closure call `.clone()` explicitly where an implicit
//! `Copy` used to suffice. Deliberately EXCLUDES the inline `@x := expr`
//! ASSIGNMENT EXPRESSION (usable mid-`SELECT` for the classic MySQL
//! row-numbering idiom, `SELECT @rn := @rn + 1 FROM t`) — that needs live
//! mutation DURING row iteration (later rows must see earlier rows' own
//! assignments), which this snapshot-once-per-statement design can't
//! support; a genuinely separate, larger follow-up (interior mutability,
//! e.g. `RefCell`, would be the natural next step).
//!
//! `SET [SESSION] TRANSACTION ISOLATION LEVEL ...` is parser sugar for
//! `tx_isolation` or `tx_isolation_one_shot`; both validated readback values
//! live with the transaction session settings. No storage isolation is
//! claimed. `READ ONLY`/`READ WRITE` remain TiDB's gated compatibility-only
//! `tx_read_only` alias and do not fabricate write enforcement.
//!
//! **`@@[scope.]name` system-variable readback** (`Expr::SysVar`,
//! [`tidb_expr::Columns::sysvar`]) is modelled ONLY for `autocommit` and
//! `time_zone` — the two session-state variables this executor already
//! tracks for its own execution — a PERMANENT scope boundary, not
//! deferred work: real MySQL/TiDB has roughly 600 system variables, and
//! this crate has no ambition to become a general-purpose variable
//! store; any other name is a genuine `Unsupported` (confirmed via
//! `gorun`: real TiDB itself errors on an unrecognized `@@name` too).
//! `SessionState` bundles `Database::now_value` with
//! `autocommit`/`time_zone` into one value,
//! read once per statement and threaded to every `RelResolver`
//! construction site (a mechanical rename of the EXISTING `now`
//! threading, not a new parameter — every call site just swapped which
//! expression it evaluates for that argument). `@@GLOBAL.*` always
//! answers with the FIXED real-MySQL default (`1`/`"SYSTEM"`) regardless
//! of this session's own state — confirmed via `gorun`: `SET
//! autocommit=0` leaves `@@global.autocommit` at `1` — since this
//! executor has no separate global variable store and `SET GLOBAL`
//! already fails elsewhere, so "global" here just means "as if this
//! session never touched it". `@@time_zone`'s default readback is the
//! literal string `"SYSTEM"`, confirmed via `gorun`, NOT `"+00:00"` —
//! `TimeZoneSetting` makes the `SYSTEM`/`UTC`/fixed-offset distinction
//! structural. The fully-stateless [`execute`] and its set-operation fold
//! entry points (no `Database` at all) use `SessionState::default`,
//! which — UNLIKE `now`'s own `None` default — answers with real
//! MySQL's actual out-of-the-box session values, since `autocommit`/
//! `time_zone` have a well-defined, deterministic default with no
//! session at all, unlike the wall clock. `Expr::UserVar` (`@name`,
//! user-defined session variable ASSIGNMENT via `SET @x = ...`) is a
//! genuinely separate, unrelated feature and stays entirely unmodelled.
//!
//! **`CASE`** (see [`tidb_expr`]'s own doc for its lazy short-circuit
//! evaluation) needed a new arm in TWO structural tree-walks here, both
//! of which previously fell through a silent `_`/`other => other.clone()`
//! wildcard for any AST node they didn't explicitly recognize — the SAME
//! shape of gap the `Datum`-tuple-match audit hunted for earlier in this
//! project, just one level up (an `Expr` variant instead of a `Datum`
//! one): `resolve_subqueries` (a subquery nested inside ANY `CASE`
//! branch — the compare value, a `WHEN` condition or `THEN` result, or
//! `ELSE` — would otherwise never be resolved to a value at all) and
//! `expr_has_aggregate`/`eval_group` (an aggregate nested inside a `CASE`
//! branch would otherwise not even be recognized as making the query
//! aggregating, let alone folded correctly per group).
//!
//! **Non-recursive, `QueryStmt::Select`-bodied `WITH`** (`crate::cte`)
//! desugars every `FROM`-clause reference to a CTE name into an ordinary
//! derived table BEFORE anything else runs — a pure, catalog-free AST
//! rewrite (unlike `resolve_subqueries`, it needs no `&self`), so EVERY
//! existing derived-table capability (self-joins, chained CTEs
//! referencing earlier ones, column renaming via an alias) works for free
//! with no new execution-time machinery. A CTE reference inside a
//! `WHERE`/`HAVING`/select-list subquery, as opposed to the `FROM`
//! clause, is not resolved (a documented scope boundary on `crate::cte`
//! itself, not an oversight).
//!
//! **`WITH RECURSIVE`, and any CTE (recursive clause or not) whose own
//! body is `UNION`/`UNION ALL`-joined** (`crate::recursive_cte`) needs
//! real query EXECUTION rather than an AST rewrite (`JoinNode::Derived`'s
//! own `subquery` field is a plain, re-executable `SelectStmt`, with no
//! way to hold a `SetOprStmt` or freeze in a fixed set of rows), so both
//! are resolved together as `Database` methods. Each CTE materializes
//! into an already-computed `Relation` (an individual CTE within a `WITH
//! RECURSIVE` clause need not itself be self-referencing — `RECURSIVE`
//! is a clause-level flag, confirmed via `gorun`), threaded through
//! `crate::select`'s own `ctes` scope parameter and checked BEFORE the
//! real catalog wherever a bare table name is resolved — the SAME
//! "CTE name shadows a real table" behavior the non-recursive path
//! already has, just backed by frozen rows instead of a re-resolvable
//! subquery. A `UNION`-bodied CTE that never references its own name
//! (legal MySQL syntax even without `RECURSIVE`, confirmed via `gorun`)
//! is simply evaluated once and folded via `crate::setopr`'s own
//! `ctes`-scoped variant — the EXACT machinery an ordinary top-level
//! `UNION` statement already uses, so the CTE's own `ORDER BY`/`LIMIT`
//! and mixed `UNION`/`UNION ALL` terms fall out for free; self-referencing
//! one WITHOUT `RECURSIVE` on the clause is a real [`ExecError::UnknownTable`]
//! (confirmed via `gorun`: the self-reference resolves to no table at
//! all), not a silent non-recursive misevaluation. A genuinely
//! self-referencing body (which DOES require `RECURSIVE`) instead
//! iterates to a fixpoint, each round seeing ONLY the PREVIOUS round's
//! newly-added rows (not the whole accumulated table so far) —
//! confirmed via `gorun` this delta-only visibility is REQUIRED, not an
//! optimization, and folds via the SAME `combine` helper
//! `crate::setopr`'s own top-level `UNION` already uses. See
//! `crate::recursive_cte`'s own doc for the exact scope boundaries on
//! that fixpoint case (self-join within a recursive TERM, an aggregate,
//! `DISTINCT`, or `ORDER BY` inside a term, and mixed `UNION`/`UNION
//! ALL` kinds, are all rejected). A `LIMIT` on the CTE DEFINITION's own
//! trailing clause IS modelled — a real early-termination optimization
//! that caps the TOTAL accumulated row count, stopping the fixpoint
//! before a bounding `WHERE` clause alone would; the definition's own
//! trailing `ORDER BY`, unlike `LIMIT`, remains rejected (a genuine
//! `ERR` in real TiDB too). See also the maximum-recursion-depth cap
//! (`1000`, matching real MySQL/TiDB's own `cte_max_recursion_depth`
//! default, confirmed via `gorun`, not exposed as a settable session
//! variable).
//!
//! **`DROP TABLE`** (`Database::drop_table`, `crate::ddl`) combines
//! two independently confirmed (via `gorun`) rules: a referential-integrity
//! check runs FIRST over the WHOLE statement, all-or-nothing (a table
//! referenced by a FOREIGN KEY of some OTHER table not ALSO being dropped
//! in the same statement blocks the entire drop, not just that one table —
//! dropping a parent and child together in one statement succeeds
//! regardless of listed order); existence is THEN checked per name, and
//! that part is NOT all-or-nothing (`DROP TABLE a, missing, c` without
//! `IF EXISTS` still drops both `a` and `c`, only `missing` is reported).
//! Like every other executable DDL statement, it ends the active transaction
//! phase before running (the implicit-commit rule).
//!
//! **`ONLY_FULL_GROUP_BY`** (`crate::aggregate::check_group_by_scope`,
//! real MySQL/TiDB's default `sql_mode`, confirmed via `gorun`): a
//! non-aggregated column in the select list, `HAVING`, or `ORDER BY` of a
//! `GROUP BY`/aggregate query is [`ExecError::UngroupedColumn`] unless it
//! is a bare `GROUP BY` column, or the WHOLE checked expression exactly
//! matches a `GROUP BY` expression (a purely syntactic, top-level-only
//! check — `SELECT id ... GROUP BY id+1` still errors even though `id` is
//! mathematically recoverable from `id+1`, and `SELECT id+1+1 ... GROUP BY
//! id+1` still errors despite `id+1` appearing nested inside the checked
//! expression, matching real TiDB exactly rather than a true
//! functional-dependency proof). A subquery's own body is a separate scope
//! and exempt, but the outer-scope operand of `x IN (SELECT ...)`/`x <op>
//! ANY|ALL (SELECT ...)` is still checked. Implementing this surfaced a
//! genuinely separate, PRE-EXISTING bug in `crate::aggregate::expr_has_aggregate`
//! (confirmed via `gorun` to be a real, not theoretical, divergence): it
//! only recursed into a narrow set of `Expr` variants, so an aggregate
//! hidden inside an ordinary function call's own argument (`IF(1=1,
//! COUNT(*), 0)`), or common patterns like `HAVING COUNT(*) BETWEEN 1 AND
//! 5`/`HAVING COUNT(*) IN (...)`, was never recognized as making the query
//! aggregating at all, taking the wrong row-wise path and failing outright
//! — fixed by extending its recursion, and giving `eval_group` a single
//! general `Database::fold_group_aggregates` helper
//! (rather than one special-cased match arm per shape) that folds every
//! `Aggregate`/`GroupConcat` leaf anywhere in `Func`/`In`/`Between`/`Like`/
//! `Is`/`Interval`/the outer-scope operand of `InSubquery`/
//! `CompareSubquery` down to its own group-scoped value (via
//! `crate::literal::value_to_literal`, the same splice-back technique
//! `resolve_subqueries` already uses) before the now aggregate-free
//! expression falls through to the ordinary per-row path — deliberately
//! NOT touching `Expr::Case`'s own separate, already-verified lazy
//! short-circuit arm, which keeps claiming `Case` first. `LIKE` against a
//! non-string operand (`COUNT(*) LIKE '2'`) surfaced as a SEPARATE,
//! general, pre-existing gap while testing this fix — confirmed unrelated
//! to aggregation (`WHERE v LIKE '10'` on a plain `int` column failed
//! identically) — closed the following turn in [`tidb_expr`] itself (see
//! its own crate doc), not scoped to this fix.
//!
//! **`GROUP BY expr [ASC|DESC]`** (`Database::aggregate`'s own
//! leading check): real MySQL/TiDB rejects ANY explicit direction on a
//! `GROUP BY` item at EXECUTION time by default — confirmed via `gorun`:
//! `[expression:1235] function GROUP BY expr ASC|DESC has only noop
//! implementation in tidb now, use tidb_enable_noop_functions to enable
//! these functions`. That session variable is not modelled at all here,
//! so the rejection is unconditional: an explicit `ASC` errors exactly
//! like `DESC` does, even though `ASC` restores identically to no
//! direction at all (`tidb_ast::GroupByItem::desc` is `Option<bool>`,
//! not a plain `bool`, specifically so this crate can still tell the two
//! apart — see that type's own doc).
//!
//! **`PARTITION (...)`** (`crate::table_reference::check_no_partition`, called by every
//! `SELECT`/`UPDATE`/`DELETE`/`INSERT` table-resolution site before it
//! does anything else): ALWAYS `Unsupported`, unconditionally, whenever
//! the clause is non-empty — this crate never implements `CREATE TABLE
//! ... PARTITION BY` at all, so every table here is permanently
//! "non-partitioned," and real MySQL/TiDB's own error for a `PARTITION`
//! clause in exactly that situation (`PARTITION () clause on non
//! partitioned table`, confirmed via `gorun`) therefore applies
//! universally — no per-table validation needed the way `USE`/`FORCE`/
//! `IGNORE INDEX`'s own hinted name would (that construct is a narrower,
//! silent divergence instead — parsed but never checked against the
//! table's real indexes, since this crate doesn't track index names at
//! all either — see `tidb_ast::TableRef::hints`'s own doc for why the
//! two constructs, despite looking similar, get different treatment
//! here).
//!
//! **`NATURAL [LEFT|RIGHT] JOIN`** (`crate::select::natural_join_columns`,
//! feeding into the SAME `crate::select::build_using_join` an explicit
//! `USING (...)` join already uses — confirmed via `gorun` that
//! `NATURAL JOIN` really is exactly `JOIN ... USING (<every column name
//! common to both sides>)`: coalesced columns, `LEFT`/`RIGHT` outer-join
//! `NULL`-padding, and the SAME `LEFT`/`RIGHT` column-order swap an
//! explicit `USING` join already has, all reused with no separate
//! implementation needed). Zero common columns degenerates to a plain
//! cross join with no special-casing either — `build_using_join`'s own
//! row-matching loop over an empty column list is vacuously `true` for
//! every pair, confirmed via `gorun` (`t3 NATURAL JOIN t4` with no
//! shared column names returns the full cartesian product). Multiple
//! common columns are ordered as they appear in the LEFT side's own
//! columns, not the right side's or alphabetically (confirmed via
//! `gorun`, a 3-common-column mixed-order probe, not assumed).
//!
//! **Window functions** (`crate::window` — the ranking functions
//! `ROW_NUMBER`/`RANK`/`DENSE_RANK`; the frame-based window AGGREGATES
//! `COUNT`/`SUM`/`AVG`/`MAX`/`MIN`; the "value function" family
//! `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`/`LAG`/`LEAD`; and the
//! "distribution function" family `NTILE`/`PERCENT_RANK`/`CUME_DIST` —
//! all with an inline `OVER (PARTITION BY ... ORDER BY ...)` spec — see
//! [`tidb_ast::Expr::Window`]'s own doc for the exact syntactic scope)
//! reuse the SAME "precompute once, then splice a literal back into the
//! tree" technique already established for subqueries and aggregate
//! folding: `crate::window::collect_windows_in` finds every DISTINCT
//! window call (deduplicated by FULL structural equality — name,
//! arguments, AND spec — so `SUM(a) OVER (...)` and `SUM(b) OVER (...)`
//! stay separate entries, not merged) in the select list/`ORDER BY`,
//! `Database::compute_window` computes each one's value for every row
//! ONCE via stable partitioning + a stable per-partition sort (matching
//! `crate::order::cmp_keys`'s own total order, including
//! `NULL`-sorts-first-ascending), and `crate::window::resolve_windows`
//! splices each row's own value back as a literal before the rewritten,
//! now window-free row defers entirely to the ordinary
//! `Database::project_row` path — no new per-row evaluation logic
//! needed at all.
//!
//! Two genuinely different rules govern where a value comes from, both
//! confirmed via `gorun`, not assumed: a window AGGREGATE's value is
//! resolved per group via `Database::eval_group` and folded via
//! `crate::aggregate::fold_aggregate_values` (split out of
//! `Database::compute_aggregate`'s own row-resolution step specifically so
//! a window aggregate's argument may itself be an aggregate expression,
//! e.g. `SUM(SUM(salary)) OVER (...)` — `eval_in`, which
//! `compute_aggregate` uses to resolve raw rows, has no notion of
//! `Expr::Aggregate` at all), and `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`
//! pick one group's own value, BOTH over the SAME default FRAME — no `ORDER BY`
//! means the frame is the WHOLE partition (every row sees the same
//! value); an `ORDER BY` means the default `RANGE BETWEEN UNBOUNDED
//! PRECEDING AND CURRENT ROW` frame, where `RANGE`'s "CURRENT ROW" is
//! PEER-GROUP-inclusive (tied rows share one cumulative/peer value, not
//! each their own individual result). `LAG`/`LEAD`, by contrast, use
//! PHYSICAL (`ROWS`-style) adjacency within the sorted partition instead
//! — confirmed two rows TIED on `ORDER BY` still get their own DISTINCT
//! physical predecessor/successor value, unlike `LAST_VALUE`'s
//! peer-group sharing; a negative offset is a real MySQL error, not
//! silently clamped or wrapped.
//!
//! The distribution family splits the SAME two ways again:
//! `PERCENT_RANK` reuses `RANK`'s own peer-aware rank number
//! (`(rank-1)/(partition_len-1)`, a documented `0` rather than a `0/0`
//! division when the partition has only one row — confirmed via
//! `gorun`), and `CUME_DIST` reuses the SAME default FRAME
//! `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` already use
//! (`frame_len/partition_len`) via the shared `for_each_default_frame`
//! helper — both PEER-GROUP-aware. `NTILE`, by contrast, is PHYSICAL
//! (position-based) like `ROW_NUMBER`/`LAG`/`LEAD` — confirmed via
//! `gorun` that two rows TIED on `ORDER BY` can land in DIFFERENT
//! buckets — using MySQL's own `q`/`r` bucket-size split (`partition_len
//! = q*n + r`; the first `r` buckets get `q+1` rows, the rest get `q`); a
//! non-positive bucket count is a real MySQL error.
//!
//! `GROUP BY` combination (confirmed via `gorun`, not assumed): a window
//! function computes over the POST-aggregation "virtual rows," one per
//! group — `HAVING` filters groups BEFORE window computation runs, so an
//! excluded group is invisible to the window, not just the final output
//! (see `crate::aggregate::Database::aggregate`'s own doc for the exact
//! sequencing). A row-wise (non-`GROUP BY`) query is the degenerate case
//! of one row per group, so `crate::select::select_rows` and
//! `crate::aggregate::Database::aggregate` share every line of
//! `Database::compute_window`'s own logic — `select_rows` just wraps each
//! passing row as its own single-element group first. A window's own
//! `PARTITION BY`/`ORDER BY`/arguments may reference an aggregate
//! expression directly (`RANK() OVER (ORDER BY SUM(salary))`) but NOT a
//! select-list alias (a genuine `ERR`, the same as any other ungrouped
//! bare column); a window function inside `HAVING` itself is likewise
//! rejected.
//!
//! Implementing this surfaced a genuinely separate, PRE-EXISTING gap: an
//! `ORDER BY`/`HAVING` referencing a select-list alias (`GROUP BY dept
//! ORDER BY c` / `HAVING c > 1` where `c` aliases `COUNT(*)`) was not
//! resolved at all — confirmed via `gorun` to be a real, general
//! divergence, not specific to window functions (`SELECT id AS x FROM t
//! ORDER BY x` failed identically with no aggregation involved) — fixed in
//! `crate::order` (`resolve_alias` for `ORDER BY`'s own whole-item
//! resolution; `resolve_having_aliases` for `HAVING`, which may reference
//! an alias ANYWHERE in its expression tree, e.g. `HAVING c + 1 > 3`).
//! `GROUP BY` ITSELF may also reference a select-list alias (`SELECT dept
//! AS x, COUNT(*) FROM t GROUP BY x`, confirmed via `gorun`, closed in a
//! later turn) — resolved via the SAME `resolve_alias`, reused by
//! `Database::aggregate` before EITHER computing
//! group keys or deriving `check_group_by_scope`'s "pinned" bare-column
//! list, so `HAVING`/`ORDER BY`/the select list may reference EITHER the
//! alias or the underlying real column once `GROUP BY` establishes it
//! (`HAVING dept = 'a'` and `ORDER BY dept` both work even when `GROUP
//! BY` itself was written as `x`). A `GROUP BY` item resolving to an
//! AGGREGATE alias (`SELECT dept, COUNT(*) AS c ... GROUP BY c`) is a
//! genuine `ERR` — no special rejection needed, since grouping's own
//! per-row `eval_in` call naturally has no notion of `Expr::Aggregate`
//! either. Only EXPLICIT `AS` aliases are matched everywhere — a
//! genuinely ambiguous collision between an alias and a same-named real
//! column (`SELECT id, dept AS id FROM t ORDER BY id`, a real `ERR` in
//! real TiDB) is a deliberately narrower, undetected scope boundary,
//! since reproducing it exactly would need tracking every field's
//! IMPLICIT display name too, not just explicit aliases.
//!
//! `crate::setopr`'s `output_index` (a `UNION`/`UNION ALL` statement's
//! OWN internal `ORDER BY`, resolved against already-projected rows with
//! no input columns to fall back on) had the SAME gap independently —
//! discovered while probing an unrelated `WITH`-CTE increment, confirmed
//! via `gorun` (`SELECT 1 AS n UNION SELECT 2 AS n ORDER BY n`), fixed by
//! reusing `crate::order::resolve_alias` there too rather than adding a
//! parallel ad hoc alias search.
//!
//! An explicit `ROWS BETWEEN <bound> AND <bound>` frame clause
//! ([`tidb_ast::WindowFrame`]) restricts the window-AGGREGATE and
//! `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` shapes to a PHYSICAL row-offset
//! range per row — NOT peer-group-aware, unlike the implicit default
//! frame those same shapes fall back to without one (confirmed via
//! `gorun`: two rows TIED on `ORDER BY` still get their own DISTINCT
//! `ROWS`-frame value). A frame bound is one of `UNBOUNDED PRECEDING`/
//! `N PRECEDING`/`CURRENT ROW`/`N FOLLOWING`/`UNBOUNDED FOLLOWING`,
//! ranked in that order; a frame whose `start` bound ranks AFTER its
//! `end` is a genuine execution error REGARDLESS of the individual
//! bounds' own offsets (confirmed via `gorun`: `ROWS BETWEEN CURRENT ROW
//! AND 1 PRECEDING` errors even though both bounds are individually
//! valid), whereas two bounds of the SAME kind whose offsets happen to
//! produce an empty range at runtime (`ROWS BETWEEN 2 FOLLOWING AND 1
//! FOLLOWING`) is NOT a static error — it silently yields an empty frame
//! (`NULL` for every aggregate but `COUNT`, which is legitimately `0`)
//! for every row where it applies. A frame clause parses on EVERY window
//! function but has NO EFFECT on the ranking/`NTILE`/`PERCENT_RANK`/
//! `CUME_DIST`/`LAG`/`LEAD` shapes (also confirmed via `gorun`) — those
//! match arms simply never consult `spec.frame` at all. Only `ROWS`
//! framing is modelled — `RANGE BETWEEN ...` (needs numeric- or
//! interval-distance comparison against the `ORDER BY` key's own value, a
//! genuinely different and larger problem than physical offsets) is a
//! deliberate `ParseError`, confirmed via `godump`/`gorun` to be real,
//! valid MySQL grammar this executor does not yet support.
//!
//! ## Module layout
//!
//! Split by concern so unrelated features can be extended without touching
//! the same file: `database` (top-level statement coordination), `ddl`
//! (DDL dispatch and ALTER, with source-owned create/index/table leaves),
//! `admin_runtime` (administrative-statement
//! boundaries), `session_runtime` (`SET`, user variables, transaction-control
//! statements, and statement clocks), `dml` (`INSERT`/`UPDATE`/
//! `DELETE`), `select`
//! (scan/join + selection + row projection), `aggregate` (`GROUP BY`/
//! aggregation/`HAVING`), `subquery` (subquery resolution, shared by
//! `select` and `aggregate`), `order` (`ORDER BY`/`LIMIT`), `setopr`
//! (table-aware set operations), `cte` (non-recursive `WITH`
//! desugaring — plain functions, not `Database` methods, since it needs
//! no catalog access), `recursive_cte` (`WITH RECURSIVE`, plus any
//! `UNION`-bodied CTE regardless of the clause's own `RECURSIVE` flag —
//! genuinely needs `&self`, unlike `cte`, since it executes the
//! base/recursive terms rather than just rewriting the AST), and
//! `window` (window
//! function evaluation — a mix of plain functions for the tree-walk
//! helpers and one `Database` method for the actual partition/sort/rank
//! computation, which needs `now_value` for `NOW()`/
//! `CURRENT_TIMESTAMP()` inside a `PARTITION BY`/`ORDER BY` key).
//! Each of the `Database`-method modules adds one or more
//! `impl Database { ... }` blocks in its own file — Rust allows a type's
//! methods to be split across as many files as its defining crate likes, as
//! long as the type itself is visible, so `Database` methods are spread by
//! concern instead of accumulating in one shared `impl` block.
//! The catalog's data shapes (`Table`/`ForeignKey`/`Column`/`Relation`/
//! `table_key`) live in `catalog`, and the per-statement session
//! machinery (`SessionState`/`RelResolver`) in `session`. Internal
//! consumers import those owners directly; this root does not preserve
//! pre-split compatibility paths. This file keeps the public crate-level
//! vocabulary the rest builds on
//! (`Row`/`ResultSet`/`ExecError`/`Database`/`Outcome`). The public table-less
//! [`execute`] contract is physically owned by `setopr`; SELECT execution is
//! physically owned by `select`. The unit tests are their own directory module
//! (`tests/`), split by feature area — see `tests/mod.rs`'s own doc.

mod admin_runtime;
pub mod advisory_lock_state;
pub mod aggregate;
pub mod alternative_plan_signals;
pub mod analyze_panic_error;
pub mod apply_cache;
pub mod bit_agg;
pub mod broadcast_query_error;
mod catalog;
pub mod charset_variable_groups;
pub mod chunk_alloc_status;
mod cluster;
pub mod cluster_index_id;
pub mod concurrent_entry_map;
pub mod config_int_json;
pub mod configured_inner_join;
pub mod context_id;
mod cte;
pub mod cte_first_error;
pub mod cume_dist;
pub mod cursor_tracker;
pub mod dag_request;
mod database;
mod ddl;
pub mod ddl_job_comments;
pub mod delete_rows_col_multiply;
pub mod distsql_recordset;
mod dml;
pub mod effective_auth_plugin;
mod error;
pub mod error_context;
mod error_conversion;
pub mod explain;
pub mod first_row;
pub mod global_sysvar_initial;
pub mod group_concat;
pub mod hash_join_version;
pub mod hint_updatable_vars;
pub mod insert_rows_col_multiply;
pub mod isolation_state;
pub mod join_table_meta;
pub mod json_arrayagg;
pub mod json_objectagg;
pub mod lack_handles;
pub mod lazy_txn_state;
pub mod lead_lag;
mod literal;
pub mod minmax_deque;
pub mod mock_global_accessor;
pub mod next_io_acc;
pub mod nextgen_readonly_vars;
pub mod nontransactional;
pub mod noop_read_only;
pub mod ntile;
pub mod option_values;
mod order;
pub mod ordered_apply_buffer;
mod partition;
pub mod pd_approximate_count;
pub mod percentile;
pub mod placement_labels;
pub mod plan_cache_params;
pub mod privilege_set;
pub mod process_info;
pub mod read_consistency;
pub mod readable_size;
pub mod real_tikv_multi_read;
pub mod real_tikv_read;
pub mod recordset_lifecycle;
mod recursive_cte;
pub mod removed_sysvar;
pub mod reserved_row_id;
mod result;
mod result_field_resolver;
mod result_metadata;
mod result_response;
mod result_schema;
mod result_schema_join_output;
mod result_schema_multi;
mod result_schema_projection;
pub mod retry_info;
mod select;
mod sequence;
pub mod sequence_state;
mod session;
pub mod session_context_key;
pub mod session_metrics;
pub mod session_pool_capacity;
pub mod session_reuse_state;
mod session_runtime;
mod session_settings;
pub mod session_status;
pub mod session_token_timing;
mod setopr;
pub mod setvar_hint_restore;
pub mod slow_log_match;
pub mod slow_log_rules;
pub mod slow_log_split;
pub mod slow_log_threshold;
pub mod statement_pushdown;
pub mod statement_refcount;
pub mod statement_rows_reader;
mod statement_status;
pub mod stats_load_result;
pub mod status_registry;
mod status_result;
pub mod stddevpop;
pub mod stddevsamp;
pub mod storage_reader;
mod subquery;
pub mod system_db_filter;
pub mod sysvar_error;
pub mod sysvar_scope;
pub mod sysvar_type;
mod table_reference;
pub mod tagged_ptr;
pub mod traffic_form;
mod transaction;
pub mod txn_read_ts;
pub mod txn_running_state;
pub mod txn_summary;
pub mod typed_condition_eval;
pub mod upgrade_versions;
pub mod used_stats;
pub mod varpop;
pub mod varsamp;
pub mod vec_group_checker_int;
pub mod warning_publication;
mod window;
pub mod window_value_int;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use tidb_datatype::Datum;

pub use catalog::Table;
pub use cluster::{Cluster, Session};
pub use error::ExecError;
pub use error_context::{
    resolve_err_level, ErrGroup, ErrorContext, ErrorContextFlags, ErrorDisposition, Level, LevelMap,
};
pub use error_conversion::{exec_error_descriptor, exec_error_kind, RenderedExecError};
pub use result::{Outcome, ResultSet, Row};
pub use result_field_resolver::{
    resolve_result_fields, resolve_select_fields, ResolvedResultField, ResultFieldResolveError,
    ResultFieldSpec,
};
pub use result_metadata::{
    col_names_to_result_fields, columns_from_adapted_fields, convert_result_field,
    AdaptedResultField, FieldNameMetadata, IdentifierMetadata, ResultFieldMetadata,
    ResultFieldTypeMetadata, MAX_ALIAS_IDENTIFIER_LEN, NOT_FIXED_DEC, NOT_NULL_FLAG, UNSIGNED_FLAG,
};
pub use result_response::{
    derive_tableless_select_columns, derive_tableless_select_result, resolve_query_result_columns,
    AutomaticResultResponse, AutomaticResultResponseError,
};
pub use result_schema::{
    resolve_catalog_select_fields, CatalogColumn, CatalogSchemaError, CatalogTableSchema,
};
pub use result_schema_join_output::{
    derive_join_output_metadata, JoinOutputChild, JoinOutputField, JoinOutputMetadata,
    JoinOutputOrigin, JoinOutputSchemaError, JoinOutputUnsupported,
};
pub use result_schema_multi::{resolve_catalog_relation_select_fields, CatalogRelationSchemaError};
pub use result_schema_projection::{project_join_output_fields, JoinProjectionError};
pub use setopr::execute;
pub use statement_status::{
    PublishedStatementStatus, StatementKind, StatementStatus, StatementWarning, WarningLevel,
};
pub use status_result::{finish_and_snapshot, StatusResultSnapshot};
pub use warning_publication::{
    warnings_from_json, warnings_to_json, IgnoreWarnings, StaticWarningHandler, WarningAppender,
    WarningHandler, WarningPublication, WarningSummary, MAX_WARNING_COUNT,
};

/// An in-memory database: a catalog of tables plus a statement runner. This is
/// the seed of real (table-backed) execution — `CREATE TABLE` / `INSERT` build
/// state, and `SELECT` scans it.
#[derive(Debug, Clone, Default)]
pub struct Database {
    pub(crate) tables: BTreeMap<String, Table>,
    /// The next value for each table's `AUTO_INCREMENT` allocator. This is
    /// deliberately separate from `tables` and from transaction/savepoint
    /// snapshots: TiDB consumes auto IDs before duplicate/FK checks and does
    /// not put them back on a later statement error or `ROLLBACK`.
    /// `None` marks a `BIGINT UNSIGNED` allocator exhausted after issuing
    /// `u64::MAX`; every other stored value is the next candidate.
    pub(crate) auto_increment_next: Rc<RefCell<BTreeMap<String, Option<u64>>>>,
    /// The sole source of both @@timestamp and the session clock. See
    /// TimestampSetting and Database::statement_clock.
    pub(crate) timestamp: session_settings::TimestampSetting,
    /// Lazily captures TiDB's dynamic wall clock once per top-level
    /// statement. Fixed timestamps also flow through this cache so every
    /// resolver observes one clock value regardless of how it is reached.
    pub(crate) statement_clock: RefCell<Option<(i64, u32)>>,
    /// `ROW_COUNT()`'s signed affected-row result for the preceding
    /// top-level statement. It is deliberately outside transaction snapshots:
    /// statement status is session metadata, not catalog data.
    pub(crate) previous_affected_rows: i64,
    /// The unsigned `LAST_INSERT_ID()` value promoted from the preceding
    /// top-level statement. It is session statement-status, never catalog
    /// data, so transaction rollback must not restore it.
    pub(crate) previous_last_insert_id: u64,
    /// `LAST_INSERT_ID(expr)` writes this current-statement cell during
    /// expression evaluation. `Database::run` promotes it only when the NEXT
    /// top-level statement starts, matching TiDB's StmtCtx handoff.
    pub(crate) statement_last_insert_id: Rc<RefCell<Option<u64>>>,
    /// TiDB's `sql_select_limit` session value. It is an unsigned row cap
    /// applied only to top-level SELECT/set-operation statements that did not
    /// write their own LIMIT; `u64::MAX` is the source default/no-limit
    /// sentinel. Like every session variable, it is outside catalog
    /// transaction snapshots, so rollback never restores an earlier cap.
    pub(crate) sql_select_limit: session_settings::SqlSelectLimit,
    /// TiDB's bounded session `default_week_format` setting. It is not
    /// transactional: only expression evaluation reads it.
    pub(crate) default_week_format: u8,
    /// TiDB's nontransactional decimal-division scale increment. The same
    /// value reaches scalar `/`, `AVG`, and windowed/nested aggregate paths.
    pub(crate) div_precision_increment: session_settings::DivPrecisionIncrement,
    /// Persistent two-seed session RNG for `RAND()`; it deliberately remains
    /// outside transaction snapshots, like TiDB's own session variables.
    pub(crate) rng: Rc<RefCell<tidb_expr::MysqlRng>>,
    /// Constant `RAND(N)` generators keyed by the stable argument-list
    /// storage address of each AST function occurrence. Cleared at every
    /// top-level statement boundary.
    pub(crate) statement_rngs: Rc<RefCell<BTreeMap<usize, tidb_expr::MysqlRng>>>,
    /// The source-observable session `time_zone` form. Fixed offsets drive
    /// clock rendering; `SYSTEM` and `UTC` retain their distinct labels.
    pub(crate) time_zone: session_settings::TimeZoneSetting,
    /// The session-scoped foreign-key enforcement switch. It applies to every
    /// already-modelled child/parent DML and parent-drop boundary, but does
    /// not retroactively validate rows written while it was disabled.
    pub(crate) foreign_key_checks: session_settings::ForeignKeyChecks,
    /// TiDB's `sql_safe_updates` compatibility variable. TiDB registers it
    /// as a no-op (unlike MySQL's client-side safety convention), so this is
    /// observable session state only and must not alter DML execution.
    pub(crate) sql_safe_updates: bool,
    /// Source-owned transaction settings and the seed's typed idle/active
    /// rollback-image lifecycle. See [`crate::transaction::TransactionState`]
    /// for the Go anchors, invariants, and the explicit real-KV boundary.
    pub(crate) transaction: transaction::TransactionState,
    /// TiDB's session-scoped `tidb_enable_noop_functions` compatibility
    /// mode. It defaults to `OFF`; `ON`/`WARN` permit `tx_read_only`/
    /// `transaction_read_only` to be set to true even though they remain
    /// deliberately behaviorless no-op variables in TiDB itself.
    pub(crate) noop_functions_mode: session_settings::NoopFunctionsMode,
    /// TiDB's multi-statement session enum. It has no effect on this
    /// executor's one-statement parse entrypoint; see MultiStatementMode for
    /// the explicit protocol boundary.
    pub(crate) multi_statement_mode: session_settings::MultiStatementMode,
    /// TiDB's per-session `tidb_retry_limit`, the maximum number of commit
    /// retries. Go accepts the signed `int64` range `-1..=MaxInt64` and
    /// defaults to 10. This seed retains the typed session value; scheduling
    /// those retries belongs to the future transactional KV runtime.
    pub(crate) tidb_retry_limit: i64,
    /// One shared session value for TiDB's `tx_read_only` and
    /// `transaction_read_only` aliases. The variables are intentionally
    /// compatibility no-ops (see `pkg/sessionctx/variable/noop.go`), so this
    /// is readback state only: it must not make the in-memory executor reject
    /// writes that real TiDB still executes.
    pub(crate) tx_read_only: bool,
    /// User-defined session variables (`SET @name = value`, read back via
    /// `Expr::UserVar`), keyed by LOWERCASED name (case-insensitive,
    /// confirmed via `gorun`: `@X` and `@x` are the SAME variable) — the
    /// display case in a restored `@name` reference comes from the AST's
    /// own `name` field, never from this map's key. NOT transactional
    /// (confirmed via `gorun`: survives a LATER `ROLLBACK` and a DDL
    /// statement's own implicit commit, unlike table data), so — unlike the
    /// active transaction phase — nothing in `Database::run` ever
    /// resets this. Reading an unset name is `NULL`, never an error
    /// (confirmed via `gorun`) — the opposite convention from `@@sysvar`,
    /// where an unrecognized name is a genuine error; see
    /// `tidb_expr::Columns::get_uservar`'s own doc for why the two
    /// differ.
    ///
    /// `Rc<RefCell<...>>`, not a plain `BTreeMap` — the inline `@x :=
    /// expr` ASSIGNMENT EXPRESSION (`tidb_ast::Expr::Assign`, evaluated
    /// via `tidb_expr::Columns::set_uservar`) needs to MUTATE this
    /// map as a side effect DURING row-by-row evaluation, deep inside
    /// `tidb_expr::eval_in`'s own `&dyn Columns` immutable-borrow call
    /// tree — interior mutability, not a signature change, is what makes
    /// that possible without threading `&mut Database` through every
    /// read-only query path (`select`/`aggregate`/subquery resolution
    /// all take `&self`). `Database::session_state()` hands out
    /// `Rc::clone(&self.user_vars)` (cheap — the SAME cell, not a deep
    /// snapshot) to every `RelResolver` built while executing ONE
    /// statement, including every nested subquery/derived-table
    /// `SELECT`'s own resolvers — so a `:=` anywhere in that tree is
    /// immediately visible everywhere else in it, and — since it is the
    /// SAME `Rc` `Database` itself owns, not a copy — automatically
    /// persists into `Database.user_vars` with no separate "write back
    /// after the statement" step, exactly like `exec_set_uservar`'s own
    /// direct mutation already does for the top-level `SET @x = value`
    /// form.
    pub(crate) user_vars: Rc<RefCell<BTreeMap<String, Datum>>>,
    /// The sequence catalog (`CREATE SEQUENCE`), keyed like `tables` (by
    /// [`table_key`] — sequences share the table NAMESPACE, confirmed via
    /// `gorun`: creating either kind under the other kind's existing name
    /// is a real error, though each `DROP` statement kind only drops its
    /// own kind). Kept OUTSIDE `tables` and NEVER included in the transaction
    /// rollback catalog: sequence allocation is genuinely non-transactional
    /// (confirmed via `gorun`, `ROLLBACK` does not undo a `NEXTVAL`) —
    /// keeping it out of the snapshot makes that fall out for free rather
    /// than needing an exclusion rule. The same `Rc`-shared interior-
    /// mutability architecture as `user_vars` (see that field's doc):
    /// `NEXTVAL`/`SETVAL` are side effects DURING evaluation, reached
    /// through `&dyn Columns`.
    pub(crate) sequences: Rc<RefCell<BTreeMap<String, sequence::Sequence>>>,
    /// `LASTVAL`'s session state: sequence key → the last value `NEXTVAL`
    /// produced in this session (absent until the first `NEXTVAL`; a
    /// `SETVAL` alone never seeds it — confirmed via `gorun`). An entry is
    /// removed when its sequence is dropped, so a recreated same-name
    /// sequence starts with `LASTVAL` = `NULL` again, matching real
    /// TiDB's sequence-ID keying.
    pub(crate) seq_lastval: Rc<RefCell<BTreeMap<String, i64>>>,
}

#[cfg(test)]
mod tests;
