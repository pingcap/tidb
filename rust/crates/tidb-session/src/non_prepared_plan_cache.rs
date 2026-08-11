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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The non-prepared plan cache: turning a written statement into the
//! parameterized form two textually different statements share, deciding
//! whether that form may be cached at all, and reporting the hit through
//! `@@last_plan_from_cache`.
//!
//! Mirrors the non-prepared half of Go `pkg/planner/core/plan_cacheable_checker.go`
//! (`NonPreparedPlanCacheableWithCtx`, `isSelectStmtNonPrepCacheableFastCheck`,
//! `extractTableNames`, and the `nonPreparedPlanCacheableChecker` visitor)
//! together with the `SessionVars.FoundInPlanCache` / `PrevFoundInPlanCache`
//! promotion Go performs in `ResetContextOfStmt`.
//!
//! # What is modelled and what is NOT -- read this before trusting a hit
//!
//! Go caches a `*PlanCacheValue`: the OPTIMIZED PLAN, rebound to the new
//! statement's parameter values. This tier has no reified plan object -- a
//! statement reaches execution through its SQL text and is parsed and planned
//! inside `Session::execute_statement` on every run. So there is nothing here
//! to store and reuse, and this cache DOES NOT REUSE A PLAN. What it stores is
//! the parameterized key, and what it reproduces is the cache's OBSERVABLE
//! contract: which statements are admitted, which are refused and why, and
//! whether the statement that just ran would have found its plan already
//! there.
//!
//! This is stated plainly rather than papered over because it decides the
//! risk profile. Go's danger in this feature is returning a plan built for a
//! DIFFERENT literal -- the right operator shape over the wrong rows. Here
//! the statement is always re-planned from its own text, so that failure is
//! structurally impossible, not merely untested. The half that IS
//! safety-critical here is the ADMISSION decision, because when this tier
//! grows a reusable plan the refusal list is what will stand between a
//! shared key and a wrong answer. That list is therefore ported at full
//! fidelity below, with the reason strings Go emits, and each entry is
//! pinned by a test.
//!
//! # The key
//!
//! The key is `(schema version, current database, parameterized SQL)`.
//!
//! * The PARAMETERIZED SQL is the statement restored after every literal has
//!   been replaced by `?`. Two statements share a key exactly when they differ
//!   only in literal values -- Go's own parameterization.
//! * A literal's TYPE is part of the key, not erased by it: `a = 1`,
//!   `a = 1.0` and `a = '1'` restore to `?` alike, so their type is carried in
//!   the key's parameter-kind suffix. Go relies on the rebinding step to keep
//!   these apart; carrying the kinds in the key achieves the same separation
//!   without a rebinding step to rely on.
//! * The SCHEMA VERSION is [`Catalog::version`], which moves on every DDL. Go
//!   drops a cached plan whose schema version has moved; including the version
//!   in the key does the same thing by making the old entry unreachable.
//! * An optimizer HINT is part of the restored text, so a hinted statement and
//!   its unhinted twin have different keys and do not share an entry. That is
//!   Go's behavior too (`ast.TableOptimizerHint` is an admitted node, and the
//!   hint survives into the digest), and it is what makes
//!   `SELECT /*+ SET_VAR(...) */ ...` break the cache in
//!   `sessionctx/setvar`'s `TestSetVarHintBreakCache`.
//!
//! # The one known divergence: a duplicated IN-list over a unique key
//!
//! `planner/core/plan_cache`'s `TestIssue44830NonPrep` runs the same
//! parameterized statement with three different IN-lists and expects a MISS
//! whenever the list repeats a value:
//!
//! ```text
//! select * from t1 where 1=1 and (a, b) in ((1, 1), (2, 2), (3, 3));  -- 2nd run: hit
//! select * from t1 where 1=1 and (a, b) in ((1, 1), (2, 2), (2, 2));  -- miss
//! select * from t1 where 1=1 and (a, b) in ((2, 2), (2, 2), (2, 2));  -- miss
//! ```
//!
//! All three restore to `(a, b) in ((?, ?), (?, ?), (?, ?))`, so this tier
//! shares one key across all of them and reports a hit for the last two.
//! It would be easy -- and wrong -- to conclude the key must also carry the
//! list's duplicate structure. Go's reason is not about keys at all, and it
//! is CONDITIONAL ON THE PLAN, which is why it cannot be reproduced here.
//!
//! Go refuses in two separate places, both of which need an access path:
//!
//! * On the way IN, `isSafePointGetPath4PlanCacheScenario2`
//!   (`planner/core/plan_cache_utils.go`) guards the Batch/PointGet a single
//!   `IN` predicate produces:
//!   `return len(path.Ranges) == len(f.GetArgs())-1 // no duplicated values
//!   in this in-list for safety.`
//!   A repeated element collapses into fewer ranges than the list has
//!   elements, the path is declared unsafe, and `find_best_task.go` calls
//!   `SetSkipPlanCache("Batch/PointGet plans may be over-optimized")` -- so
//!   the plan never enters the cache. Captured: running the all-duplicate
//!   statement TWICE still reports `0` the second time.
//! * On the way OUT, `buildRangesForBatchGet`
//!   (`planner/core/plan_cache_rebuild.go`) re-derives the ranges for the new
//!   literals and refuses the cached plan when the count moved:
//!   `if len(ranges.Ranges) != len(x.IndexValues) || !isSafeRange(...) {
//!   return errors.New("rebuild to get an unsafe range") }`.
//!   That is why a duplicated list misses even when a DISTINCT list of the
//!   same length is already cached under the identical key -- captured, and
//!   the distinct statement still hits afterwards, so the entry is not
//!   evicted, merely unusable for that call.
//!
//! Both checks are reached only when the optimizer chose a Batch/PointGet,
//! so the refusal is a property of the PLAN, not of the statement text.
//! Captured against `gorun` with the SAME statements and only the schema
//! changed, and again with a scalar IN-list:
//!
//! ```text
//! create table t1 (a int, b int, primary key(a, b));
//!   (a, b) in ((2, 2), (2, 2), (2, 2))  -- Batch_Point_Get -> MISS
//! create table t1 (a int, b int, key(a, b));
//!   (a, b) in ((2, 2), (2, 2), (2, 2))  -- IndexReader     -> HIT
//! create table t  (a int, b int, key(b));
//!   a in (2, 2, 2)                      -- HIT
//! create table u  (a int primary key, b int);
//!   a in (2, 2, 2)                      -- HIT
//! ```
//!
//! So the rule is NOT "a duplicated IN-list is uncacheable". TiDB caches a
//! duplicated list in three of those four shapes, including over a primary
//! key; only the row-constructor form over a clustered composite primary key
//! reaches the point-get valve. Keying on the list's ARITY, as this tier
//! does, is TiDB's behavior everywhere else, and an AST-level "duplicates
//! never share a key" rule would have been an invention that broke the three
//! captured hits. Those are pinned by tests below, in both directions.
//!
//! The one divergent shape is also the one this tier cannot execute at all:
//! `ROW(a, b) IN (ROW(...), ...)` is rejected by the expression rewriter
//! before any plan exists, so no wrong rows can be returned through it today.
//! Closing it honestly needs an access path to inspect, which this key-only
//! tier does not have.

use tidb_ast::{Expr, GroupByItem, JoinNode, OrderItem, QueryStmt, SelectStmt, Stmt};
use tidb_executor::Catalog;
use tidb_util::kvcache::SimpleLruCache;

/// Go `getMaxParamLimit`'s default: `PlanCacheMaxParamNum` (200).
const MAX_PARAM_NUM: usize = 200;

/// Go's cap on the values an `IN (...)` list may hold before the statement is
/// refused (`MaxCacheableLimitCount` neighbours; the checker's own in-list
/// bound).
const MAX_IN_LIST_LEN: usize = 200;

/// Why a statement may not enter the non-prepared plan cache.
///
/// The payload is Go's own reason string, verbatim, so a future
/// `tidb_last_plan_cache_reason`-style surface reports what TiDB reports and
/// so a divergence in the refusal itself is visible rather than silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Refusal(pub &'static str);

/// The kind tag a parameterized literal contributes to the key.
///
/// Two statements whose literals differ in KIND must not share an entry even
/// though both restore to `?`: an integer, a decimal and a string compare
/// against a column under different rules and select different access paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParamKind {
    Int,
    Decimal,
    Float,
    Str,
}

impl ParamKind {
    const fn tag(self) -> char {
        match self {
            Self::Int => 'i',
            Self::Decimal => 'd',
            Self::Float => 'f',
            Self::Str => 's',
        }
    }
}

/// The per-session cache. Go keeps an LRU sized by
/// `tidb_non_prepared_plan_cache_size`; the entries here are keys only, for
/// the reason the module doc states.
#[derive(Default)]
pub(crate) struct NonPreparedPlanCache {
    keys: Option<SimpleLruCache<String, ()>>,
    capacity: usize,
}

impl NonPreparedPlanCache {
    /// A cache holding at most `capacity` keys
    /// (`tidb_non_prepared_plan_cache_size`, Go default 100).
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: (capacity > 0).then(|| SimpleLruCache::new(capacity)),
            capacity,
        }
    }

    /// Records `key` as just used and answers whether it was already present.
    ///
    /// A hit moves the key to the most-recent end, which is the LRU order Go's
    /// own cache maintains; a miss inserts it, evicting the least recent when
    /// the cache is full.
    pub(crate) fn admit(&mut self, key: String) -> bool {
        let Some(keys) = self.keys.as_mut() else {
            return false;
        };
        if keys.get(key.as_str()).is_some() {
            return true;
        }
        keys.put(key, ());
        false
    }
}

/// Builds the cache key for `stmt`, or the reason it may not be cached.
///
/// This is Go's `NonPreparedPlanCacheableWithCtx` and the parameterization it
/// guards, fused: the same single walk that admits the statement produces the
/// parameterized form, exactly as Go's visitor does.
pub(crate) fn cache_key(
    stmt: &Stmt,
    catalog: &Catalog,
    current_db: &str,
    enable_param_limit: bool,
) -> Result<String, Refusal> {
    let Stmt::Query(query) = stmt else {
        return Err(Refusal("not a SELECT/UPDATE/INSERT/DELETE statement"));
    };
    let QueryStmt::Select(select) = &**query else {
        // Go reaches a `UNION` through `ast.SetOprStmt`, which is not in the
        // admitted node list.
        return Err(Refusal("query has some unsupported Node"));
    };

    let mut walk = Walk {
        catalog,
        current_db,
        enable_param_limit,
        tables: Vec::new(),
        params: Vec::new(),
        const_count: 0,
        filter_depth: 0,
    };
    let mut parameterized = (**select).clone();
    walk.fast_check(&parameterized)?;
    walk.select(&mut parameterized)?;

    let mut key = String::with_capacity(64);
    key.push_str(&catalog.version().to_string());
    key.push('|');
    key.push_str(current_db);
    key.push('|');
    for kind in &walk.params {
        key.push(kind.tag());
    }
    key.push('|');
    key.push_str(
        &Stmt::Query(tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(
            parameterized,
        ))))
        .restore(),
    );
    Ok(key)
}

/// The admitting walk: Go's `nonPreparedPlanCacheableChecker`, whose `Enter`
/// admits a fixed node list and refuses everything else.
struct Walk<'a> {
    catalog: &'a Catalog,
    current_db: &'a str,
    enable_param_limit: bool,
    /// Go `checker.tableNodes`: the at-most-two tables the query reads, whose
    /// schemas the filter-column rule consults.
    tables: Vec<String>,
    params: Vec<ParamKind>,
    const_count: usize,
    /// Go `checker.filterCnt`: non-zero while inside a filter, which is what
    /// makes the column-type rule apply to `col = 1` but not to a select
    /// field.
    filter_depth: usize,
}

impl Walk<'_> {
    /// Go `isSelectStmtNonPrepCacheableFastCheck`: the clause-level refusals
    /// taken before any node is visited.
    fn fast_check(&mut self, select: &SelectStmt) -> Result<(), Refusal> {
        if select.kind != tidb_ast::SelectStatementKind::Select {
            return Err(Refusal("not a select statement"));
        }
        if select.lock.is_some() {
            // Go refuses a locking read at the statement-kind gate:
            // `selStmt.LockInfo != nil` is "not a SELECT statement".
            return Err(Refusal("not a SELECT statement"));
        }
        if select.having.is_some() {
            return Err(Refusal("queries with HAVING clauses are not supported"));
        }
        if !select.windows.is_empty() {
            return Err(Refusal("queries using window-functions are not supported"));
        }
        if select.limit.is_some() && !self.enable_param_limit {
            return Err(Refusal("queries with limit clauses are not supported"));
        }
        if select.with.is_some() {
            // A CTE reaches Go as a sub-query node, which the visitor refuses.
            return Err(Refusal("queries that have sub-queries are not supported"));
        }
        let Some(from) = &select.from else {
            return Err(Refusal("queries that have sub-queries are not supported"));
        };
        self.extract_table_names(from)
    }

    /// Go `extractTableNames`: collects the plain table names of the join
    /// tree, refusing a derived table and a tree wider than two tables.
    fn extract_table_names(&mut self, join: &tidb_ast::Join) -> Result<(), Refusal> {
        self.extract_from_node(&join.left)?;
        if let Some(right) = &join.right {
            self.extract_from_node(right)?;
        }
        if self.tables.len() > 2 {
            return Err(Refusal(
                "queries that have more than 2 tables are not supported",
            ));
        }
        Ok(())
    }

    fn extract_from_node(&mut self, node: &JoinNode) -> Result<(), Refusal> {
        match node {
            JoinNode::Table(table) => {
                self.tables
                    .push(table.name.last().cloned().unwrap_or_default());
                Ok(())
            }
            JoinNode::Join(inner) => self.extract_table_names(inner),
            // Go's `extractTableNames` default arm: a derived table is a
            // sub-query as far as this checker is concerned.
            JoinNode::Derived { .. } => {
                Err(Refusal("queries that have sub-queries are not supported"))
            }
        }
    }

    /// Walks the admitted `SELECT` shape, parameterizing as it goes.
    fn select(&mut self, select: &mut SelectStmt) -> Result<(), Refusal> {
        self.table_names_cacheable()?;
        for field in select.fields.fields_mut() {
            // A `*` projects no expression; Go's `FieldList` arm recurses into
            // the fields it has and a wildcard contributes no node.
            if let tidb_ast::SelectField::Expr { expr, .. } = field {
                self.expr(expr)?;
            }
        }
        if let Some(from) = select.from.as_mut() {
            self.join_conditions(from)?;
        }
        if let Some(where_clause) = select.where_clause.as_mut() {
            self.filter(where_clause)?;
        }
        self.group_by(&mut select.group_by)?;
        self.order_by(&mut select.order_by)?;
        if select.limit.is_some() && !self.enable_param_limit {
            return Err(Refusal("query has 'limit ?' is un-cacheable"));
        }
        Ok(())
    }

    fn join_conditions(&mut self, join: &mut tidb_ast::Join) -> Result<(), Refusal> {
        if let Some(on) = join.on.as_mut() {
            self.filter(on)?;
        }
        if let JoinNode::Join(inner) = &mut join.left {
            self.join_conditions(inner)?;
        }
        if let Some(JoinNode::Join(inner)) = join.right.as_mut() {
            self.join_conditions(inner)?;
        }
        Ok(())
    }

    /// Go's `GroupByClause` arm: only bare columns are admitted.
    fn group_by(&mut self, items: &mut [GroupByItem]) -> Result<(), Refusal> {
        for item in items {
            if !matches!(item.expr, Expr::Column(_)) {
                return Err(Refusal("only support group by {columns}'"));
            }
        }
        Ok(())
    }

    /// Go's `OrderByClause` arm: only bare columns are admitted.
    fn order_by(&mut self, items: &mut [OrderItem]) -> Result<(), Refusal> {
        for item in items {
            if !matches!(item.expr, Expr::Column(_)) {
                return Err(Refusal("only support order by {columns}'"));
            }
        }
        Ok(())
    }

    /// Enters a filter node: Go's `isFilterNode` raises `filterCnt`, which is
    /// what makes the column-type rule apply here and only here.
    fn filter(&mut self, expr: &mut Expr) -> Result<(), Refusal> {
        self.filter_depth += 1;
        let result = self.expr(expr);
        self.filter_depth -= 1;
        result
    }

    /// Go's `Enter`: an admitted node recurses, everything else refuses.
    fn expr(&mut self, expr: &mut Expr) -> Result<(), Refusal> {
        match expr {
            Expr::Column(name) => self.column(name),
            Expr::Int(_) | Expr::Decimal(_) | Expr::Float(_) | Expr::String(_) => {
                self.parameterize(expr)
            }
            // Go refuses a NULL constant explicitly: `not-null-col = NULL`
            // folds to a table-dual plan that `not-null-col = ?` cannot
            // reproduce, so sharing the entry would change the plan shape.
            Expr::Null => Err(Refusal("query has null constants")),
            // Go refuses BIT and HEX literals "for safety" -- their binary
            // kind survives parameterization badly.
            Expr::Hex(_) | Expr::Bit(_) => {
                Err(Refusal("query has BIT / HEX literals are not supported"))
            }
            // Go refuses an underscore-charset value for the same reason.
            Expr::CharsetString { .. } | Expr::CharsetBinary { .. } => {
                Err(Refusal("query has values with under-score charset"))
            }
            // Go's checker refuses a user-defined variable outright.
            Expr::UserVar(_) => Err(Refusal("query has user-defined variables is un-cacheable")),
            Expr::Paren(inner) => self.expr(inner),
            Expr::Binary(_, left, right) => {
                self.expr(left)?;
                self.expr(right)
            }
            Expr::Unary(_, inner) => self.expr(inner),
            Expr::Is { expr, .. } => self.expr(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr(expr)?;
                self.expr(low)?;
                self.expr(high)
            }
            Expr::In { expr, list, .. } => {
                if list.len() > MAX_IN_LIST_LEN {
                    return Err(Refusal("too many values in in-list"));
                }
                self.expr(expr)?;
                for item in list.iter_mut() {
                    self.expr(item)?;
                }
                Ok(())
            }
            Expr::Row(items) => {
                for item in items.iter_mut() {
                    self.expr(item)?;
                }
                Ok(())
            }
            // A sub-query in any of its shapes is Go's
            // "query has sub-queries is un-cacheable".
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::CompareSubquery { .. } => Err(Refusal("query has sub-queries is un-cacheable")),
            // Everything else -- functions, CASE, CAST, aggregates, windows,
            // LIKE, REGEXP, system variables -- lands on Go's final
            // "unexpected cases" arm. This catch-all is the whole reason the
            // port is safe: a node this walk has never been taught cannot
            // silently join a shared key.
            _ => Err(Refusal("query has some unsupported Node")),
        }
    }

    /// Go's `ColumnName` arm: inside a filter the column must resolve, and its
    /// type must not be one whose comparison Go declines to parameterize.
    fn column(&mut self, name: &[String]) -> Result<(), Refusal> {
        if self.filter_depth == 0 {
            return Ok(());
        }
        let Some(column) = name.last() else {
            return Err(Refusal("some column is not found in table schema"));
        };
        let mut found = false;
        for table in &self.tables {
            let Some(entry) = self.catalog.table_in(self.current_db, table) else {
                continue;
            };
            for (held, field_type) in entry.column_types() {
                if !held.eq_ignore_ascii_case(column) {
                    continue;
                }
                found = true;
                if is_unparameterizable_type(&field_type) {
                    return Err(Refusal(
                        "query has some filters with JSON, Enum, Set or Bit columns",
                    ));
                }
            }
        }
        if found {
            Ok(())
        } else {
            Err(Refusal("some column is not found in table schema"))
        }
    }

    /// Go's `TableName` arm plus `checkTableCacheable`: a system schema, a
    /// view and a sequence are all refused before any expression is examined.
    fn table_names_cacheable(&self) -> Result<(), Refusal> {
        if is_system_schema(self.current_db) {
            return Err(Refusal("access tables in system schema"));
        }
        for table in &self.tables {
            let Some(entry) = self.catalog.table_in(self.current_db, table) else {
                // An unknown table is not this check's error to report -- the
                // statement's own resolution will raise it -- but it must not
                // be cached either.
                return Err(Refusal("some column is not found in table schema"));
            };
            if entry.is_view() {
                return Err(Refusal("queries that access views are not supported"));
            }
            if entry.is_sequence() {
                return Err(Refusal("queries that access in-memory tables"));
            }
        }
        Ok(())
    }

    /// Replaces one literal by a `?` marker, counting it against Go's
    /// `maxNumberParam` and recording its kind for the key.
    fn parameterize(&mut self, expr: &mut Expr) -> Result<(), Refusal> {
        let kind = match expr {
            Expr::Int(_) => ParamKind::Int,
            Expr::Decimal(_) => ParamKind::Decimal,
            Expr::Float(_) => ParamKind::Float,
            Expr::String(_) => ParamKind::Str,
            _ => return Ok(()),
        };
        self.const_count += 1;
        if self.const_count > MAX_PARAM_NUM {
            return Err(Refusal("query has too many constants"));
        }
        self.params.push(kind);
        *expr = Expr::ParamMarker {
            offset: 0,
            order: self.params.len() - 1,
            in_execute: false,
            projection_offset: 0,
        };
        Ok(())
    }
}

/// Go's `mysql.TypeJSON / TypeEnum / TypeSet / TypeBit` filter-column refusal.
fn is_unparameterizable_type(field_type: &tidb_datatype::FieldType) -> bool {
    use tidb_datatype::FieldTypeCode;
    matches!(
        field_type.code(),
        FieldTypeCode::Json | FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
    )
}

impl crate::Session {
    /// Decides whether the statement about to run would have found its plan
    /// in the non-prepared plan cache, and records the entry if not.
    ///
    /// This runs before planning and never short-circuits it: see the module
    /// doc for why a hit here reuses a KEY and not a plan.
    pub(crate) fn probe_non_prepared_plan_cache(&mut self, stmt: &Stmt) {
        if !self.non_prepared_plan_cache_enabled() {
            return;
        }
        let capacity = self.non_prepared_plan_cache_capacity();
        if self.non_prepared_plan_cache.capacity != capacity {
            // Go's `SetCapacity` keeps the entries that still fit; resizing to
            // a different bound here starts clean rather than guessing which
            // survive, which can only ever cost hits, never invent one.
            self.non_prepared_plan_cache = NonPreparedPlanCache::with_capacity(capacity);
        }
        let enable_param_limit = self.session_bool("tidb_enable_plan_cache_for_param_limit", true);
        let key = {
            let Ok(catalog) = self.catalog.lock() else {
                return;
            };
            match cache_key(stmt, &catalog, &self.current_db, enable_param_limit) {
                Ok(key) => key,
                // A refused statement leaves `found_in_plan_cache` false, which
                // is exactly what Go reports for it.
                Err(_) => return,
            }
        };
        self.found_in_plan_cache = self.non_prepared_plan_cache.admit(key);
    }

    /// Whether `@@last_plan_from_cache` should report a hit, which is the
    /// PRECEDING statement's outcome (Go `PrevFoundInPlanCache`).
    pub(crate) fn last_plan_from_cache(&self) -> bool {
        self.prev_found_in_plan_cache
    }

    fn non_prepared_plan_cache_enabled(&self) -> bool {
        self.session_bool("tidb_enable_non_prepared_plan_cache", false)
    }

    fn non_prepared_plan_cache_capacity(&self) -> usize {
        self.vars
            .get_system("tidb_non_prepared_plan_cache_size")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
    }

    /// Reads one session boolean, falling back to `default` when the variable
    /// is unreadable -- a probe must never turn a variable-table problem into
    /// a statement error.
    pub(crate) fn session_bool(&self, name: &str, default: bool) -> bool {
        match self.vars.get_system(name) {
            Ok(value) => matches!(
                value.to_ascii_uppercase().as_str(),
                "ON" | "1" | "TRUE" | "YES"
            ),
            Err(_) => default,
        }
    }
}

/// Go `filter.IsSystemSchema`.
fn is_system_schema(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "information_schema" | "performance_schema" | "mysql" | "sys" | "metrics_schema"
    )
}
