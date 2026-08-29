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

//! How physical reads are lowered and what work the driver hands down to
//! them.
//!
//! The shared planner owns ordinary access-path enumeration and selection.
//! This module retains two executor concerns:
//!
//! 1. Go's statement-level `TryFastPlan`, represented by
//!    [`try_batch_point_get`] and [`try_point_get`].
//! 2. Mechanical offers to the selected reader: [`prune_scan_columns`],
//!    [`negotiate_scan_filter`], and [`offer_scan_limit`].
//!
//! Steps 2-4 are offers, not commands: [`crate::table_access`] holds the
//! contract, every method of it is fail-closed, and the source alone decides.
//! The order matters and is fixed here -- pruning runs before the predicate
//! split so a pushed conjunct's `column_offset` is already in narrow space,
//! and the cap is offered last, after the residual `WHERE` is known, because
//! a residual filter above the source forbids one.
//!
use super::point_get_key::{names_no_rows, point_get_value};
use super::*;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct FastPointOutput {
    offsets: Vec<usize>,
    columns: Vec<(String, FieldType)>,
}

/// One residual `column = ?`/`column = const` conjunct of a cached point
/// read: the equality the key lookup cannot answer, so the decoded row must.
#[derive(Clone, Debug)]
pub(crate) enum PointResidualBound {
    /// A constant whose datum is already in the column's domain.
    Literal(Datum),
    /// A `?` marker resolved against each EXECUTE's parameters.
    Param(usize),
    /// `column IS NULL`: the decoded row must carry a NULL in its slot.
    IsNull,
}

/// The immutable part of Go's cached `PointGetPlan` for one prepared handle
/// lookup. Runtime cursor state and the execute-time handle are deliberately
/// absent: both are rebuilt for every cache hit.
#[derive(Debug)]
pub struct PreparedPointGetPlan {
    /// The catalog's KEY-DECODE METADATA version at resolve time — the
    /// counter DDL moves and DML never touches (`Catalog::metadata_version`).
    /// A transaction's own writes bump the mutation counter every statement,
    /// so keying a cache hit on it would invalidate every read after the
    /// transaction's first write; Go's plan cache invalidates on SCHEMA
    /// changes, and this counter is that boundary.
    schema_version: u64,
    current_database: String,
    database: String,
    table: String,
    table_id: i64,
    /// One marker order per PINNED key column, aligned with
    /// [`Self::pin_types`] — the full row handle for [`PreparedPointTarget::
    /// RowHandle`], or every column of a unique secondary index.
    /// `None` pins that column to the matching [`Self::handle_literals`]
    /// constant instead of an EXECUTE parameter.
    parameter_orders: Vec<Option<usize>>,
    /// The pinned columns' field types, in key order; `bind` moves each
    /// execute's value into this domain before encoding the key.
    pin_types: Vec<FieldType>,
    /// Which key the pins name, and so which single read answers them all.
    pub(crate) target: PreparedPointTarget,
    /// The literal constants pinning handle columns, aligned with
    /// [`Self::parameter_orders`]; `None` where a marker pins instead. A NULL
    /// literal never matches, so it binds to an empty execution.
    handle_literals: Vec<Option<Datum>>,
    /// Empty for an integer PK handle; one offset for the supported prepared
    /// common-handle shape. Keeping this explicit lets `bind` rebuild the
    /// encoded handle without re-running the AST path matcher.
    common_handle_offsets: Vec<usize>,
    /// Equalities the key lookup leaves open (`stsrcd = ?` next to the full
    /// handle pin). Each entry positions INTO THE OUTPUT ROW (the index inside
    /// [`FastPointOutput::offsets`] the residual column decodes to), paired
    /// with the bound to compare against.
    residuals: Vec<(usize, PointResidualBound)>,
    /// A WHERE conjunct that SCHEMA contradicts (`NOT NULL` column `IS NULL`)
    /// makes every row unmatched before any key is read. The plan then binds
    /// to an always-empty execution, exactly like a NULL pin, and no storage
    /// read ever runs -- the cached shape of Go's `TableDual`.
    contradiction: bool,
    output: FastPointOutput,
    row_decoder: crate::kv_table::PreparedPointGetRowDecoder,
    cached_keys: std::sync::Mutex<Vec<PreparedPointCacheKey>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPointCacheKey {
    schema_version: u64,
    environment: PreparedPlanCacheEnvironment,
    parameter_types: Vec<PreparedParameterType>,
}

impl PreparedPointCacheKey {
    fn matches(&self, actual: &Self) -> bool {
        self.schema_version == actual.schema_version
            && self.environment == actual.environment
            && prepared_parameter_types_compatible(&self.parameter_types, &actual.parameter_types)
    }
}

/// Builds the always-empty plan for a schema-contradicted WHERE: nothing is
/// pinned and nothing is residual, because no row can survive the predicate.
fn contradiction_plan(
    schema_version: u64,
    current_database: &str,
    database: &str,
    table_name: &str,
    table_id: i64,
    common_handle_offsets: Vec<usize>,
    handle_offset: Option<usize>,
    table: &crate::KvTable,
    output: FastPointOutput,
) -> Option<PreparedPointGetPlan> {
    Some(PreparedPointGetPlan {
        schema_version,
        current_database: current_database.to_owned(),
        database: database.to_owned(),
        table: table_name.to_owned(),
        table_id,
        parameter_orders: Vec::new(),
        pin_types: Vec::new(),
        target: PreparedPointTarget::RowHandle,
        handle_literals: Vec::new(),
        common_handle_offsets,
        residuals: Vec::new(),
        contradiction: true,
        row_decoder: crate::kv_table::PreparedPointGetRowDecoder::new_with_handles(
            table.visible_columns(),
            handle_offset,
            &[],
            &output.offsets,
        )
        .ok()?,
        output,
        cached_keys: std::sync::Mutex::new(Vec::new()),
    })
}

impl PreparedPointGetPlan {
    /// The catalog version against which the point shape was resolved.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// The default database in force when an unqualified table was resolved.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    /// The qualified table name the plan reads, `(database, table)`.
    #[must_use]
    pub fn names(&self) -> (&str, &str) {
        (&self.database, &self.table)
    }

    /// The transaction read policy this retained point plan can safely use.
    /// A row-handle lookup performs one read and may use Go's autocommit
    /// MaxTS optimization. A secondary-unique lookup reads the index entry
    /// and then the row, so Go's `noSecondRead` guard keeps it on an ordinary
    /// timestamped snapshot even though the point executor itself is reusable.
    #[must_use]
    pub const fn statement_read_shape(&self) -> crate::access_path::StatementReadShape {
        match self.target {
            PreparedPointTarget::RowHandle => {
                crate::access_path::StatementReadShape::AutocommitPointGet
            }
            PreparedPointTarget::UniqueIndex { .. } => {
                crate::access_path::StatementReadShape::Unknown
            }
        }
    }

    /// Rebuilds the parameter-dependent handle and resolves every residual
    /// bound against this EXECUTE's parameters. A value that cannot be moved
    /// exactly into its column's domain declines the cache and must be
    /// replanned; any NULL comparison makes the statement match no rows, so it
    /// binds to an execution whose handle is `None` (or whose residuals can
    /// never pass).
    #[must_use]
    pub fn bind(
        self: &Arc<Self>,
        values: &[Datum],
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Option<PreparedPointGetExecution> {
        self.bind_with_environment(values, zone, &PreparedPlanCacheEnvironment::default())
    }

    /// Rebuilds this point plan under Go's complete prepared-plan cache key.
    #[must_use]
    pub fn bind_with_environment(
        self: &Arc<Self>,
        values: &[Datum],
        zone: &tidb_datatype::SessionTimeZone,
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedPointGetExecution> {
        let key = PreparedPointCacheKey {
            schema_version: self.schema_version,
            environment: environment.clone(),
            parameter_types: values.iter().map(PreparedParameterType::of).collect(),
        };
        let cache_hit = self
            .cached_keys
            .lock()
            .ok()
            .is_some_and(|cached| cached.iter().any(|entry| entry.matches(&key)));
        let execution = |handle, range_values, residuals| {
            // Go inserts a generated plan before building or opening its
            // executor. A runtime error therefore does not erase the plan or
            // turn the next identical EXECUTE into another miss.
            if !cache_hit {
                let mut cached = self.cached_keys.lock().ok()?;
                if !cached.iter().any(|entry| entry.matches(&key)) {
                    cached.push(key.clone());
                }
            }
            Some(PreparedPointGetExecution {
                plan: Arc::clone(self),
                handle,
                range_values,
                residuals,
                cache_hit,
            })
        };
        if self.contradiction {
            // `NOT NULL col IS NULL` matched no rows at PLAN time; parameters
            // cannot change a schema fact.
            return execution(None, None, Vec::new());
        }
        let mut key_values = Vec::with_capacity(self.parameter_orders.len());
        for (index, handle_type) in self.pin_types.iter().enumerate() {
            let value = match (&self.parameter_orders[index], &self.handle_literals[index]) {
                (Some(order), _) => values.get(*order)?,
                (None, Some(literal)) => literal,
                (None, None) => return None,
            };
            if value.is_null() {
                // `handle = NULL` matches nothing; an empty result IS the
                // answer, and no storage read may run for it.
                return execution(None, None, Vec::new());
            }
            key_values.push(match point_get_value(handle_type, value) {
                Some(value) => value,
                None if names_no_rows(handle_type, value) => {
                    // A parameter longer than the column's capacity compares
                    // equal to no stored value: the empty set IS the answer,
                    // the same observable result Go's re-optimized plan
                    // produces, served without re-planning.
                    return execution(None, None, Vec::new());
                }
                None => return None,
            });
        }
        let mut residuals = Vec::with_capacity(self.residuals.len());
        for (position, bound) in &self.residuals {
            let check = match bound {
                PointResidualBound::IsNull => ResidualCheck::IsNull,
                PointResidualBound::Literal(value) => ResidualCheck::Equal(value.clone()),
                PointResidualBound::Param(order) => {
                    let value = values.get(*order)?;
                    if value.is_null() {
                        // `residual = NULL` never passes either.
                        return execution(None, None, Vec::new());
                    }
                    ResidualCheck::Equal(
                        match point_get_value(&self.output.columns[*position].1, value) {
                            Some(value) => value,
                            None if names_no_rows(&self.output.columns[*position].1, value) => {
                                return execution(None, None, Vec::new());
                            }
                            None => return None,
                        },
                    )
                }
            };
            residuals.push((*position, check));
        }
        // A full-handle pin rebuilds the one record key; a unique-index pin
        // carries every index key value into the one closed point range.
        if matches!(self.target, PreparedPointTarget::RowHandle) {
            let first = key_values.first()?;
            let handle = if self.common_handle_offsets.is_empty() {
                match first {
                    Datum::Int(value) => Some(TableHandle::Int(*value)),
                    Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                    _ => None,
                }
            } else {
                let encoded = tidb_codec::encode_key_in_timezone(zone, &key_values).ok()?;
                let handle = tidb_txnkv::CommonHandle::new(encoded).ok()?;
                Some(TableHandle::Common(handle.encoded().to_vec()))
            };
            return execution(handle, None, residuals);
        }
        execution(None, Some(key_values), residuals)
    }

    /// Whether the catalog still names the same unpartitioned physical table.
    #[must_use]
    pub fn matches_catalog(&self, catalog: &Catalog, current_database: &str) -> bool {
        if self.schema_version != catalog.metadata_version()
            || !self.current_database.eq_ignore_ascii_case(current_database)
        {
            return false;
        }
        matches!(
            catalog.get_in(&self.database, &self.table),
            Some(TableEntry::Kv(table))
                if table.table_id == self.table_id && table.partition().is_none()
        )
    }
}

/// One cache hit after its execute-time parameter has been rebuilt into a
/// handle. The executor itself is still created fresh by
/// [`run_prepared_point_get`].
/// Which key a prepared point plan's pins name, and so which single read
/// answers them: the full row handle or every column of one non-prefix unique
/// secondary index. These are exactly Go `tryPointGetPlan`'s two point-read
/// families.
#[derive(Clone, Debug)]
pub(crate) enum PreparedPointTarget {
    /// The pins cover every handle column: ONE record-key read.
    RowHandle,
    /// The pins cover every column of this non-prefix unique secondary index.
    UniqueIndex { index_id: i64 },
}

#[derive(Clone, Debug)]
pub struct PreparedPointGetExecution {
    plan: Arc<PreparedPointGetPlan>,
    /// [`PreparedPointTarget::RowHandle`] only: the execute's rebuilt handle.
    /// `None` here — or in [`Self::range_values`] on the other arms — binds a
    /// NULL pin, which matches no row and reads nothing.
    handle: Option<TableHandle>,
    /// The unique-index arm's full key values, already moved into their
    /// columns' domains.
    range_values: Option<Vec<Datum>>,
    /// This execute's residual predicates, parameters already resolved:
    /// `(offset into the output row, the check the decoded slot must pass)`.
    residuals: Vec<(usize, ResidualCheck)>,
    cache_hit: bool,
}

/// The immutable half of a reusable SELECT plan.  This is the complete shared
/// planner tree, not a hand-built executor shortcut: access paths, readers,
/// joins, aggregation, sort, and every nested range-bearing physical node are
/// retained together and rebuilt recursively on a cache hit.
#[derive(Debug)]
pub struct PreparedSelectPlan {
    current_database: String,
    table_names: Vec<(String, String)>,
    parameter_count: usize,
    limit_parameter_orders: Vec<usize>,
    statement: tidb_ast::Stmt,
    cached_plans: std::sync::Mutex<Vec<CachedSelectPlanEntry>>,
}

#[derive(Debug)]
struct CachedSelectPlanEntry {
    schema_version: u64,
    stats_version_hash: u64,
    environment: PreparedPlanCacheEnvironment,
    parameter_types: Vec<PreparedParameterType>,
    limit_values: Vec<u64>,
    plan: Arc<std::sync::Mutex<super::planner_bridge::CachedSelectPlan>>,
}

/// Session facts in Go's prepared-plan cache key that can change physical
/// planning without changing the statement, schema, or parameter types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPlanCacheEnvironment {
    sql_mode: tidb_mysql::SqlMode,
    time_zone: String,
    pushdown_blacklist_generation: u64,
    connection_charset: String,
    connection_collation: String,
    partition_prune_mode: String,
    isolation_read_engines: String,
    sql_select_limit: String,
    in_transaction: bool,
    autocommit: bool,
    invalidate_on_fresh_stats: bool,
    binding_sql: String,
    skip_stats_on_binding: bool,
    plan_cache_max_plan_size: u64,
    enable_generated_columns: bool,
}

impl Default for PreparedPlanCacheEnvironment {
    fn default() -> Self {
        Self::new(tidb_mysql::SqlMode::default(), String::new(), 0)
    }
}

impl PreparedPlanCacheEnvironment {
    /// Builds the non-schema portion of Go's plan-cache environment key.
    #[must_use]
    pub fn new(
        sql_mode: tidb_mysql::SqlMode,
        time_zone: String,
        pushdown_blacklist_generation: u64,
    ) -> Self {
        Self {
            sql_mode,
            time_zone,
            pushdown_blacklist_generation,
            connection_charset: String::new(),
            connection_collation: String::new(),
            partition_prune_mode: String::new(),
            isolation_read_engines: String::new(),
            sql_select_limit: String::new(),
            in_transaction: false,
            autocommit: true,
            invalidate_on_fresh_stats: true,
            binding_sql: String::new(),
            skip_stats_on_binding: false,
            plan_cache_max_plan_size: tidb_vardef::defaults::DEF_TIDB_PLAN_CACHE_MAX_PLAN_SIZE
                as u64,
            enable_generated_columns: true,
        }
    }

    /// Adds the session facts that Go's `NewPlanCacheKey` hashes because they
    /// can change access paths, partition expansion, expression semantics, or
    /// whether a UnionScan/transactional reader is required.
    #[must_use]
    pub fn with_session_state(
        mut self,
        connection_charset: String,
        connection_collation: String,
        partition_prune_mode: String,
        isolation_read_engines: String,
        sql_select_limit: String,
        in_transaction: bool,
        autocommit: bool,
        invalidate_on_fresh_stats: bool,
    ) -> Self {
        self.connection_charset = connection_charset;
        self.connection_collation = connection_collation;
        self.partition_prune_mode = partition_prune_mode;
        self.isolation_read_engines = isolation_read_engines;
        self.sql_select_limit = sql_select_limit;
        self.in_transaction = in_transaction;
        self.autocommit = autocommit;
        self.invalidate_on_fresh_stats = invalidate_on_fresh_stats;
        self
    }

    /// Adds the two post-optimization admission inputs read by Go's
    /// `isPlanCacheable` / `isPhysicalPlanCacheable`.
    #[must_use]
    pub const fn with_cache_admission(
        mut self,
        plan_cache_max_plan_size: u64,
        enable_generated_columns: bool,
    ) -> Self {
        self.plan_cache_max_plan_size = plan_cache_max_plan_size;
        self.enable_generated_columns = enable_generated_columns;
        self
    }

    /// Adds Go's matched `Binding.BindSQL` to the cache key. The stats-version
    /// component is omitted only when a binding matched and
    /// `tidb_plan_cache_skip_stats_on_binding` is enabled.
    #[must_use]
    pub fn with_binding_sql(mut self, binding_sql: Option<&str>, skip_stats: bool) -> Self {
        self.binding_sql = binding_sql.unwrap_or_default().to_owned();
        self.skip_stats_on_binding = !self.binding_sql.is_empty() && skip_stats;
        self
    }

    pub(crate) const fn plan_cacheability(
        &self,
        parameter_count: usize,
    ) -> tidb_planner::physical_plan_cache::PlanCacheabilityContext {
        tidb_planner::physical_plan_cache::PlanCacheabilityContext {
            parameter_count,
            enable_generated_columns: self.enable_generated_columns,
            max_plan_size: self.plan_cache_max_plan_size,
        }
    }

    pub(crate) const fn hashes_fresh_statistics(&self) -> bool {
        self.invalidate_on_fresh_stats && !self.skip_stats_on_binding
    }
}

/// Go's prepared-plan cache keys physical plans by the current parameter
/// types. Values with the same type rebuild one retained tree; a different
/// type gets its own physical enumeration instead of inheriting a path chosen
/// for an incompatible comparison domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedParameterType {
    Null,
    Int,
    UInt,
    Decimal {
        precision: i32,
        scale: i32,
    },
    Real,
    Float32,
    String {
        charset: tidb_datatype::Charset,
        collation: tidb_datatype::Collation,
    },
    Bytes,
    Raw,
    BinaryLiteral,
    Bit,
    Duration,
    Enum,
    Set,
    Date,
    DateTime,
    Timestamp,
    Json,
    VectorFloat32,
    MinNotNull,
    MaxValue,
}

/// One execution lease on a cache-owned, recursively rebuilt physical tree.
/// The generation prevents another bind from silently changing the retained
/// parameter values between admission and executor construction.
#[derive(Debug)]
pub struct PreparedSelectExecution {
    plan: Arc<PreparedSelectPlan>,
    schema_version: u64,
    cache_hit: bool,
    cached_plan: Arc<std::sync::Mutex<super::planner_bridge::CachedSelectPlan>>,
    generation: u64,
}

impl PreparedSelectPlan {
    /// The immutable statement retained at PREPARE time.
    #[must_use]
    pub const fn statement(&self) -> &tidb_ast::Stmt {
        &self.statement
    }

    /// Qualified table names read by this plan, for transaction MDL tracking.
    #[must_use]
    pub fn table_names(&self) -> &[(String, String)] {
        &self.table_names
    }

    /// On the first execution for a schema and parameter-type key, runs the
    /// shared optimizer with the real parameter values and retains its entire
    /// physical tree. A hit recursively rebuilds every parameter-dependent
    /// range in place before extracting its lowering receipt.
    #[must_use]
    pub fn bind(
        self: &Arc<Self>,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: &crate::StmtContext,
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedSelectExecution> {
        self.bind_for_statement(
            values,
            catalog,
            current_database,
            ctx,
            environment,
            &self.statement,
        )
    }

    /// Generates and retains a plan from the statement after a matched SQL
    /// binding has replaced its hints. The binding SQL itself belongs to
    /// `environment`, mirroring Go's prepared-plan cache key.
    #[must_use]
    pub fn bind_for_statement(
        self: &Arc<Self>,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: &crate::StmtContext,
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Option<PreparedSelectExecution> {
        self.bind_inner(
            values,
            catalog,
            current_database,
            Some(ctx),
            environment,
            statement,
        )
    }

    /// Rebuilds an existing cache entry without constructing a planner
    /// statement context. A miss returns `None`; the caller can then build
    /// the context and call [`Self::bind`] to enumerate and retain a plan.
    #[must_use]
    pub fn bind_cached(
        self: &Arc<Self>,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedSelectExecution> {
        self.bind_cached_for_statement(
            values,
            catalog,
            current_database,
            environment,
            &self.statement,
        )
    }

    /// Rebuilds an entry keyed by the currently matched binding and effective
    /// statement, without allocating a planner context on a hit.
    #[must_use]
    pub fn bind_cached_for_statement(
        self: &Arc<Self>,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Option<PreparedSelectExecution> {
        self.bind_inner(
            values,
            catalog,
            current_database,
            None,
            environment,
            statement,
        )
    }

    fn bind_inner(
        self: &Arc<Self>,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: Option<&crate::StmtContext>,
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Option<PreparedSelectExecution> {
        if !self.current_database.eq_ignore_ascii_case(current_database) {
            return None;
        }
        if !matches!(statement, tidb_ast::Stmt::Query(_)) {
            return None;
        }
        let parameter_types = values
            .iter()
            .map(PreparedParameterType::of)
            .collect::<Vec<_>>();
        let limit_values = self
            .limit_parameter_orders
            .iter()
            .map(|order| match values.get(*order) {
                Some(Datum::Int(value)) if (0..=10_000).contains(value) => Some(*value as u64),
                Some(Datum::UInt(value)) if *value <= 10_000 => Some(*value),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let schema_version = catalog.metadata_version();
        let stats_version_hash = self.stats_version_hash(catalog, environment);
        let mut cached_plans = self.cached_plans.lock().ok()?;
        let cached = cached_plans.iter().position(|entry| {
            entry.schema_version == schema_version
                && entry.stats_version_hash == stats_version_hash
                && entry.environment == *environment
                && prepared_parameter_types_compatible(&entry.parameter_types, &parameter_types)
                && entry.limit_values == limit_values
        });
        let (cached_plan, generation, cache_hit) = match cached {
            Some(index) => {
                let plan = Arc::clone(&cached_plans[index].plan);
                let generation = plan.lock().ok()?.bind(values);
                match generation {
                    Some(generation) => (plan, generation, true),
                    None => {
                        // Go rejects a cache entry whose in-place range rebuild
                        // fails and generates a fresh plan. Do not leave a
                        // partially rebuilt tree available to the next execute.
                        cached_plans.remove(index);
                        return None;
                    }
                }
            }
            None => {
                let ctx = ctx?;
                let statement = crate::bind_prepared_statement(statement, values).ok()?;
                let tidb_ast::Stmt::Query(query) = statement else {
                    return None;
                };
                let query = query.into_inner();
                cached_plans.retain(|entry| {
                    entry.schema_version == schema_version
                        && entry.stats_version_hash == stats_version_hash
                        && entry.environment == *environment
                });
                let mut plan = super::planner_bridge::cached_query_plan(
                    &query,
                    catalog,
                    current_database,
                    ctx,
                    environment.plan_cacheability(self.parameter_count),
                )?;
                let generation = plan.bind(values)?;
                let plan = Arc::new(std::sync::Mutex::new(plan));
                cached_plans.push(CachedSelectPlanEntry {
                    schema_version,
                    stats_version_hash,
                    environment: environment.clone(),
                    parameter_types,
                    limit_values,
                    plan: Arc::clone(&plan),
                });
                (plan, generation, false)
            }
        };
        Some(PreparedSelectExecution {
            plan: Arc::clone(self),
            schema_version,
            cache_hit,
            cached_plan,
            generation,
        })
    }

    fn stats_version_hash(
        &self,
        catalog: &Catalog,
        environment: &PreparedPlanCacheEnvironment,
    ) -> u64 {
        if !environment.invalidate_on_fresh_stats || environment.skip_stats_on_binding {
            return 0;
        }
        self.table_names.iter().fold(0, |hash, (database, table)| {
            let version = match catalog.get_in(database, table) {
                Some(TableEntry::Kv(table)) => catalog
                    .table_statistics(table.stats_physical_id())
                    .map_or(0, |statistics| statistics.version),
                _ => 0,
            };
            hash.wrapping_add(version)
        })
    }
}

impl PreparedParameterType {
    pub(crate) fn of(value: &Datum) -> Self {
        match value {
            Datum::Null => Self::Null,
            Datum::Int(_) => Self::Int,
            Datum::UInt(_) => Self::UInt,
            Datum::Decimal(value) => {
                let (precision, scale) = value.precision_and_frac();
                Self::Decimal { precision, scale }
            }
            Datum::Real(_) => Self::Real,
            Datum::Float32(_) => Self::Float32,
            Datum::String(value) => Self::String {
                charset: value.charset(),
                collation: value.collation(),
            },
            Datum::Bytes(_) => Self::Bytes,
            Datum::Raw(_) => Self::Raw,
            Datum::BinaryLiteral(_) => Self::BinaryLiteral,
            Datum::Bit(_) => Self::Bit,
            Datum::Duration(_) => Self::Duration,
            Datum::Enum(_, _) => Self::Enum,
            Datum::Set(_, _) => Self::Set,
            Datum::Time(value) => match value.kind() {
                tidb_datatype::TimeType::Date => Self::Date,
                tidb_datatype::TimeType::DateTime => Self::DateTime,
                tidb_datatype::TimeType::Timestamp => Self::Timestamp,
            },
            Datum::Json(_) => Self::Json,
            Datum::VectorFloat32(_) => Self::VectorFloat32,
            Datum::MinNotNull => Self::MinNotNull,
            Datum::MaxValue => Self::MaxValue,
        }
    }

    /// Go `checkTypesCompatibility4PC`: decimal precision and scale are
    /// asymmetric. A plan generated for a wider decimal can serve a narrower
    /// value, but the reverse must enumerate a new physical plan.
    fn compatible_with(self, actual: Self) -> bool {
        match (self, actual) {
            (
                Self::Decimal {
                    precision: expected_precision,
                    scale: expected_scale,
                },
                Self::Decimal {
                    precision: actual_precision,
                    scale: actual_scale,
                },
            ) => expected_precision >= actual_precision && expected_scale >= actual_scale,
            _ => self == actual,
        }
    }
}

pub(crate) fn prepared_parameter_types_compatible(
    expected: &[PreparedParameterType],
    actual: &[PreparedParameterType],
) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| expected.compatible_with(*actual))
}

impl PreparedSelectExecution {
    /// The immutable plan whose schema identity gates this execution.
    #[must_use]
    pub fn plan(&self) -> &PreparedSelectPlan {
        &self.plan
    }

    /// Whether this execute rebuilt an existing physical plan rather than
    /// generating the first plan for its schema and parameter-type key.
    #[must_use]
    pub const fn cache_hit(&self) -> bool {
        self.cache_hit
    }

    /// The schema identity used when the retained physical tree was built.
    /// The ordinary statement executor compares it with the catalog while
    /// holding the catalog guard and plans normally if a DDL moved it.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Runs a callback while the cache-owned SELECT root is pinned to the
    /// generation rebuilt for this execution. The callback is the ordinary
    /// session statement funnel; this type does not own another executor.
    pub fn with_plan<R>(
        &self,
        callback: impl FnOnce(&tidb_ast::Stmt, &mut tidb_planner::physical::PhysicalPlan) -> R,
    ) -> Option<R> {
        let mut cached = self.cached_plan.lock().ok()?;
        let (statement, physical) = cached.execution_mut(self.generation)?;
        Some(callback(statement, physical))
    }
}

#[cfg(test)]
pub(crate) fn run_prepared_select_for_test(
    execution: &PreparedSelectExecution,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    if execution.schema_version() != catalog.metadata_version()
        || !execution
            .plan()
            .current_database
            .eq_ignore_ascii_case(current_database)
    {
        return Err(DriverError::unsupported(
            "prepared SELECT test execution saw a moved schema",
        ));
    }
    execution
        .with_plan(|statement, physical| {
            let tidb_ast::Stmt::Query(query) = statement else {
                unreachable!("a prepared query owns a query statement")
            };
            super::run_query_meta_stmt_with_physical(
                query,
                Some(physical),
                catalog,
                current_database,
                ctx,
            )
        })
        .ok_or_else(|| {
            DriverError::unsupported(
                "prepared SELECT test generation changed before executor construction",
            )
        })?
}

/// One residual predicate bound to an EXECUTE's parameters.
#[derive(Clone, Debug)]
enum ResidualCheck {
    Equal(Datum),
    IsNull,
}

impl PreparedPointGetExecution {
    /// The immutable plan whose safety gates this execution must satisfy.
    #[must_use]
    pub fn plan(&self) -> &PreparedPointGetPlan {
        &self.plan
    }

    /// Whether this execution rebuilt a point plan under an existing complete
    /// prepared-plan cache key.
    #[must_use]
    pub const fn cache_hit(&self) -> bool {
        self.cache_hit
    }
}

/// Recognizes the fail-closed subset of Go prepared plans that can reuse a
/// `PointGetExecutor`: one marker pinning an unpartitioned integer handle and
/// a source-column projection. Hints and every root operator decline.
#[must_use]
pub fn build_prepared_point_get_plan(
    stmt: &tidb_ast::Stmt,
    parameter_count: usize,
    catalog: &Catalog,
    current_database: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<PreparedPointGetPlan> {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return None;
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        return None;
    };
    if !crate::access_path::select_is_bare_point_read(select)
        || !select.hints.is_empty()
        || select.priority != tidb_ast::StatementPriority::None
        || select.sql_small_result
        || select.sql_big_result
        || select.sql_buffer_result
        || select.sql_no_cache
        || select.straight_join
    {
        return None;
    }
    let table_ref = single_table_ref(&select.from)?;
    if !table_ref.partitions.is_empty()
        || table_ref.as_of.is_some()
        || !prepared_primary_index_hint(table_ref)
        || table_ref.sample.is_some()
    {
        return None;
    }
    let Ok((database, table_name)) = split_table_path(&table_ref.name, current_database) else {
        return None;
    };
    let entry @ TableEntry::Kv(table) = catalog.get_in(database, table_name)? else {
        return None;
    };
    if table.partition().is_some() {
        return None;
    }
    let (handle_offset, common_handle_offsets) = if let Some(offset) = table.pk_handle_offset() {
        (Some(offset), Vec::new())
    } else {
        let offsets = table.common_handle_offsets();
        // A composite common handle encodes its prefix columns in order; the
        // walker below pins each of them exactly once, so any width works.
        if offsets.is_empty() {
            return None;
        }
        (None, offsets.to_vec())
    };
    let columns = entry.column_list();
    let visible = table_ref.alias.as_deref().unwrap_or(table_name);
    let scope = single_table_scope(
        visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.clone(),
    );
    let output = match fast_point_output(select, &scope) {
        Some(output) => output,
        None => return None,
    };
    // A generated output can evaluate expressions while its stored row is
    // decoded. Keep those plans on the full statement context; the cached
    // point path below needs only SELECT's temporal/default conversion state.
    if output.offsets.iter().any(|offset| {
        table
            .visible_columns()
            .get(*offset)
            .is_none_or(|column| column.generated.is_some())
    }) {
        return None;
    }
    // One walk flattens the WHERE conjunction into resolved equalities; the
    // handle pins exactly one per handle column and everything else filters
    // the decoded row.
    let conjuncts = match prepared_point_eq_conjuncts(select.where_clause.as_ref()?, &scope, zone) {
        Some(conjuncts) => conjuncts,
        None => return None,
    };
    let resolver = ScopeResolver { scope: &scope };
    let mut resolved = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        match conjunct {
            PreparedPointConjunct::Eq {
                path,
                order,
                literal,
            } => {
                let Some((offset, _, _)) = resolver.resolve(&path) else {
                    return None;
                };
                resolved.push((offset, PreparedPointPredicate::Eq(order, literal)));
            }
            PreparedPointConjunct::IsNull { path } => {
                let Some((offset, _, _)) = resolver.resolve(&path) else {
                    return None;
                };
                resolved.push((offset, PreparedPointPredicate::IsNull));
            }
        }
    }
    drop(resolver);
    // A schema contradiction ends the search before any key is named: a NOT
    // NULL column can never satisfy `IS NULL`, so the answer is always empty
    // (Go constant-folds this into a `TableDual` and its plan cache keeps it).
    if resolved.iter().any(|(offset, kind)| {
        matches!(kind, PreparedPointPredicate::IsNull)
            && columns[*offset]
                .1
                .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
    }) {
        return contradiction_plan(
            catalog.metadata_version(),
            current_database,
            database,
            table_name,
            table.table_id,
            common_handle_offsets.clone(),
            handle_offset,
            table,
            output,
        );
    }
    let handle_offsets: Vec<usize> = match handle_offset {
        Some(offset) => vec![offset],
        None => common_handle_offsets.to_vec(),
    };
    if handle_offsets.is_empty() {
        return None;
    }
    // Go tries the full primary/common handle first, then the first public
    // non-prefix UNIQUE index whose complete key is pinned. A partial key is
    // an ordinary range plan and belongs to the general cached physical tree.
    let (target, pin_offsets) = if handle_offsets
        .iter()
        .all(|offset| column_pinned_once(*offset, &resolved))
    {
        (PreparedPointTarget::RowHandle, handle_offsets.clone())
    } else {
        let index = table.plan_indexes().find(|index| {
            index.unique
                && !index.has_prefix()
                && !index.column_offsets.is_empty()
                && index
                    .column_offsets
                    .iter()
                    .all(|offset| column_pinned_once(*offset, &resolved))
        })?;
        (
            PreparedPointTarget::UniqueIndex { index_id: index.id },
            index.column_offsets.clone(),
        )
    };
    let mut parameter_orders = Vec::with_capacity(pin_offsets.len());
    let mut handle_literals = Vec::with_capacity(pin_offsets.len());
    for offset in &pin_offsets {
        let mut hits = resolved
            .iter()
            .filter(|(pinned, kind)| *pinned == *offset && kind.eq_parts().is_some());
        let Some((_, predicate)) = hits.next() else {
            return None;
        };
        let (marker_order, literal) = match predicate.eq_parts() {
            Some(parts) => parts,
            None => return None,
        };
        if hits.next().is_some() {
            return None;
        }
        // The pin value is encoded into a KEY, so its domain has to order
        // like its bytes; otherwise the cache declines to the planner.
        if !point_byte_safe(&columns[*offset].1) {
            return None;
        }
        parameter_orders.push(marker_order);
        handle_literals.push(literal);
    }
    // Whatever equality the key lookup does not answer filters the decoded
    // row. Each residual column must survive into the output row and compare
    // byte-wise (or natively for non-strings), so the cached check is exactly
    // the scan's own `=`; anything else declines to the ordinary planner.
    let mut residuals = Vec::new();
    for (offset, kind) in &resolved {
        if pin_offsets.contains(offset) {
            continue;
        }
        let position = match output.offsets.iter().position(|o| o == offset) {
            Some(position) => position,
            None => return None,
        };
        match kind {
            PreparedPointPredicate::IsNull => {
                residuals.push((position, PointResidualBound::IsNull));
            }
            PreparedPointPredicate::Eq(marker_order, literal) => {
                let column_type = &output.columns[position].1;
                if !point_byte_safe(column_type) {
                    return None;
                }
                let bound = match marker_order {
                    Some(order) => PointResidualBound::Param(*order),
                    None => {
                        let value = literal.as_ref()?;
                        if value.is_null() {
                            // `residual = NULL` matches no row under SQL
                            // semantics; leave such statements to the ordinary
                            // planner rather than caching an always-empty
                            // answer.
                            return None;
                        }
                        PointResidualBound::Literal(value.clone())
                    }
                };
                residuals.push((position, bound));
            }
        }
    }
    // Every referenced marker must exist when the parameters arrive.
    let max_order = parameter_orders
        .iter()
        .flatten()
        .copied()
        .chain(residuals.iter().filter_map(|(_, bound)| match bound {
            PointResidualBound::Param(order) => Some(*order),
            _ => None,
        }))
        .max();
    if max_order.is_some_and(|order| order >= parameter_count) {
        return None;
    }
    Some(PreparedPointGetPlan {
        schema_version: catalog.metadata_version(),
        current_database: current_database.to_owned(),
        database: database.to_owned(),
        table: table_name.to_owned(),
        table_id: table.table_id,
        parameter_orders,
        pin_types: pin_offsets
            .iter()
            .map(|offset| columns.get(*offset).map(|column| column.1.clone()))
            .collect::<Option<Vec<_>>>()?,
        handle_literals,
        target,
        row_decoder: crate::kv_table::PreparedPointGetRowDecoder::new_with_handles(
            table.visible_columns(),
            handle_offset,
            &common_handle_offsets,
            &output.offsets,
        )
        .ok()?,
        common_handle_offsets,
        residuals,
        contradiction: false,
        output,
        cached_keys: std::sync::Mutex::new(Vec::new()),
    })
}

/// Builds the complete shared-planner tree retained by a prepared SELECT.
#[must_use]
pub fn build_prepared_select_plan(
    stmt: &tidb_ast::Stmt,
    parameter_count: usize,
    _catalog: &Catalog,
    current_database: &str,
    _ctx: &crate::StmtContext,
) -> Option<PreparedSelectPlan> {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return None;
    };
    if parsed_parameter_count(stmt) != parameter_count {
        return None;
    }
    let mut table_names = Vec::new();
    collect_prepared_table_names(query, current_database, &mut table_names);
    let limit_parameter_orders = prepared_limit_parameter_orders(stmt);

    Some(PreparedSelectPlan {
        current_database: current_database.to_owned(),
        table_names,
        parameter_count,
        limit_parameter_orders,
        statement: stmt.clone(),
        cached_plans: std::sync::Mutex::new(Vec::new()),
    })
}

pub(super) fn prepared_limit_parameter_orders(stmt: &tidb_ast::Stmt) -> Vec<usize> {
    struct LimitMarkerCollector {
        orders: Vec<usize>,
    }

    impl tidb_ast::Visitor for LimitMarkerCollector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(limit) = node.downcast_ref::<tidb_ast::Limit>() else {
                return false;
            };
            for expression in std::iter::once(&limit.count).chain(limit.offset.as_ref()) {
                if let tidb_ast::Expr::ParamMarker { order, .. } = expression {
                    self.orders.push(*order);
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut statement = stmt.clone();
    let mut collector = LimitMarkerCollector { orders: Vec::new() };
    tidb_ast::Visitable::accept(&mut statement, &mut collector);
    collector.orders.sort_unstable();
    collector.orders.dedup();
    collector.orders
}

fn collect_prepared_table_names(
    query: &tidb_ast::QueryStmt,
    current_database: &str,
    names: &mut Vec<(String, String)>,
) {
    fn collect_query(
        query: &tidb_ast::QueryStmt,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        match query {
            tidb_ast::QueryStmt::Select(select) => {
                collect_join(&select.from, current_database, names);
            }
            tidb_ast::QueryStmt::SetOpr(set) => collect_set(set, current_database, names),
        }
    }

    fn collect_set(
        set: &tidb_ast::SetOprStmt,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        for term in &set.terms {
            match &term.body {
                tidb_ast::SetOprTermBody::Select(select) => {
                    collect_join(&select.from, current_database, names);
                }
                tidb_ast::SetOprTermBody::Nested(set) => {
                    collect_set(set, current_database, names);
                }
            }
        }
    }

    fn collect(
        node: &tidb_ast::JoinNode,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        match node {
            tidb_ast::JoinNode::Table(table) => {
                if let Ok((database, name)) = split_table_path(&table.name, current_database) {
                    let item = (database.to_owned(), name.to_owned());
                    if !names.iter().any(|present| present == &item) {
                        names.push(item);
                    }
                }
            }
            tidb_ast::JoinNode::Join(join) => {
                collect(&join.left, current_database, names);
                if let Some(right) = &join.right {
                    collect(right, current_database, names);
                }
            }
            tidb_ast::JoinNode::Derived { subquery, .. } => {
                collect_query(subquery, current_database, names);
            }
        }
    }

    fn collect_join(
        from: &Option<tidb_ast::Join>,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        if let Some(from) = from {
            collect(&from.left, current_database, names);
            if let Some(right) = &from.right {
                collect(right, current_database, names);
            }
        }
    }

    collect_query(query, current_database, names);
}

/// One `column = ?`, `column = const`, or `column IS NULL` conjunct of a
/// prepared point read's WHERE, its column path kept UNRESOLVED until the
/// builder maps it through the statement's scope.
enum PreparedPointConjunct {
    Eq {
        path: Vec<String>,
        order: Option<usize>,
        literal: Option<Datum>,
    },
    IsNull {
        path: Vec<String>,
    },
}

/// One WHERE conjunct RESOLVED to its column offset. `Eq` may pin a key;
/// `IsNull` is inherently a row-level check (NULL never equals a key value).
enum PreparedPointPredicate {
    Eq(Option<usize>, Option<Datum>),
    IsNull,
}

type ResolvedConjunct = (usize, PreparedPointPredicate);

impl PreparedPointPredicate {
    fn eq_parts(&self) -> Option<(Option<usize>, Option<Datum>)> {
        match self {
            PreparedPointPredicate::Eq(order, literal) => Some((*order, literal.clone())),
            PreparedPointPredicate::IsNull => None,
        }
    }
}

/// Flattens a WHERE conjunction into `(column path, ? order, literal datum)`
/// triples — exactly [`point_equal_pairs`]' shape rule with markers kept
/// SYMBOLIC so a PREPARE-time plan can bind them per EXECUTE. Constants
/// evaluate once here (a NULL literal survives as `Some(Datum::Null)`);
/// anything outside `col = constant|?`, including ORs, comparisons of two
/// columns, or functions of row columns, refuses the whole conjunction.
fn prepared_point_eq_conjuncts(
    where_clause: &tidb_ast::Expr,
    scope: &FromScope,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Vec<PreparedPointConjunct>> {
    use tidb_ast::{BinaryOp, Expr};
    fn unparenthesized(expr: &Expr) -> &Expr {
        match expr {
            Expr::Paren(inner) => unparenthesized(inner),
            other => other,
        }
    }
    fn walk(
        expr: &Expr,
        zone: &tidb_datatype::SessionTimeZone,
        out: &mut Vec<PreparedPointConjunct>,
    ) -> bool {
        match expr {
            Expr::Paren(inner) => walk(inner, zone, out),
            Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
                walk(lhs, zone, out) && walk(rhs, zone, out)
            }
            Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
                let (column, value_expr) = match (unparenthesized(lhs), unparenthesized(rhs)) {
                    (Expr::Column(path), other) => (path, other),
                    (other, Expr::Column(path)) => (path, other),
                    _ => return false,
                };
                if column.is_empty() {
                    return false;
                }
                match unparenthesized(value_expr) {
                    Expr::ParamMarker { order, .. } => {
                        out.push(PreparedPointConjunct::Eq {
                            path: column.clone(),
                            order: Some(*order),
                            literal: None,
                        });
                        true
                    }
                    _ => {
                        let Ok(rewritten) = rewrite_expr_resolved(
                            value_expr,
                            &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                        ) else {
                            return false;
                        };
                        let Expression::Constant(constant) = rewritten else {
                            return false;
                        };
                        let Ok(value) = constant.eval() else {
                            return false;
                        };
                        out.push(PreparedPointConjunct::Eq {
                            path: column.clone(),
                            order: None,
                            literal: Some(value),
                        });
                        true
                    }
                }
            }
            Expr::Is { expr, target, not }
                if matches!(target, tidb_ast::IsTarget::Null) && !*not =>
            {
                // `col IS NULL`: a row-level check (NULL never equals, so it
                // can never pin a key), admitted beside the equalities.
                let Expr::Column(path) = unparenthesized(expr) else {
                    return false;
                };
                if path.is_empty() {
                    return false;
                }
                out.push(PreparedPointConjunct::IsNull { path: path.clone() });
                true
            }
            _ => false,
        }
    }
    let _ = scope;
    let mut out = Vec::new();
    if walk(where_clause, zone, &mut out) && !out.is_empty() {
        Some(out)
    } else {
        None
    }
}

/// YCSB's MySQL adapter pins every single-row lookup with
/// `FORCE INDEX(PRIMARY)`. That hint does not alter a clustered-handle point
/// read, so it is safe to retain in the prepared cache; all other hint shapes
/// stay on the ordinary planner to preserve their access-path semantics.
fn prepared_primary_index_hint(table_ref: &tidb_ast::TableRef) -> bool {
    table_ref.hints.iter().all(|hint| {
        matches!(
            (hint.kind, hint.scope, hint.indexes.as_slice()),
            (
                tidb_ast::IndexHintKind::Force | tidb_ast::IndexHintKind::Use,
                tidb_ast::IndexHintScope::All,
                [name]
            ) if name.eq_ignore_ascii_case("PRIMARY")
        )
    })
}

/// Executes one rebound prepared point plan with fresh mutable runtime state.
/// `None` means the schema identity moved after the cache decision.
pub fn run_prepared_point_get(
    execution: &PreparedPointGetExecution,
    catalog: &mut Catalog,
    current_database: &str,
    ctx: &crate::kv_table::PreparedPointGetDecodeContext,
) -> Result<Option<SelectMeta>, DriverError> {
    let plan = execution.plan();
    if !plan.matches_catalog(catalog, current_database) {
        return Ok(None);
    }
    let Some(TableEntry::Kv(table)) = catalog.get_mut_in_for_read(&plan.database, &plan.table)
    else {
        return Ok(None);
    };
    // A NULL key bound to an always-empty execution; no read may run.
    if execution.handle.is_none() && execution.range_values.is_none() {
        return Ok(Some((plan.output.columns.clone(), Vec::new())));
    }
    // The residual equalities the key lookup could not answer decide per row,
    // comparing in each column's own domain — the same `=` the ordinary scan
    // would have evaluated.
    let matches_residuals =
        |row: &[Datum]| residuals_pass(row, &plan.output.columns, &execution.residuals);
    let _ = matches_residuals;
    let decode_error = |error: crate::kv_table::KvTableError| {
        ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
    };
    let rows = match plan.target {
        PreparedPointTarget::RowHandle => {
            let handle = execution.handle.as_ref().expect("row-handle arm binds one");
            match table
                .get_prepared_point_row(handle, &plan.row_decoder, ctx)
                .map_err(decode_error)?
            {
                None => Vec::new(),
                Some(row) if matches_residuals(&row) => vec![row],
                Some(_) => Vec::new(),
            }
        }
        PreparedPointTarget::UniqueIndex { index_id } => {
            let values = execution
                .range_values
                .as_deref()
                .expect("index-prefix arm binds key values");
            let handle = table
                .lookup_unique(index_id, values, ctx.zone())
                .map_err(decode_error)?;
            match handle {
                Some(handle) => match table
                    .get_prepared_point_row(&handle, &plan.row_decoder, ctx)
                    .map_err(decode_error)?
                {
                    Some(row) if matches_residuals(&row) => vec![row],
                    _ => Vec::new(),
                },
                None => Vec::new(),
            }
        }
    };
    Ok(Some((plan.output.columns.clone(), rows)))
}

/// The residual gate shared by every prepared point-read arm: each unconsumed
/// predicate must hold on the decoded row -- an `=` compared in its own
/// domain, or a NULL present where `IS NULL` demanded one.
fn residuals_pass(
    row: &[Datum],
    columns: &[(String, FieldType)],
    residuals: &[(usize, ResidualCheck)],
) -> bool {
    residuals.iter().all(|(position, check)| match check {
        ResidualCheck::IsNull => row.get(*position).is_some_and(Datum::is_null),
        ResidualCheck::Equal(expected) => row.get(*position).is_some_and(|actual| {
            actual
                .compare(expected, (&columns[*position].1).collation())
                .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
        }),
    })
}

/// Whether ONE equality conjunct names this column (an `IS NULL` beside it
/// does not compete for a key). The column pins a key only if exactly one
/// such equality exists and nothing else touches it.
fn column_pinned_once(offset: usize, resolved: &[ResolvedConjunct]) -> bool {
    let mut hits = resolved
        .iter()
        .filter(|(pinned, kind)| *pinned == offset && kind.eq_parts().is_some());
    hits.next().is_some() && hits.next().is_none()
}

/// Whether equality over this column's domain can be answered from KEY BYTES
/// alone: a binary collation (or a non-string domain). A key PIN needs this
/// because its value is encoded into the lookup key, and a residual
/// comparison needs it because it compares decoded bytes against the bound.
fn point_byte_safe(field_type: &FieldType) -> bool {
    field_type.eval_type() != tidb_datatype::EvalType::String
        || matches!(
            field_type.collation(),
            tidb_datatype::Collation::Binary
                | tidb_datatype::Collation::AsciiBin
                | tidb_datatype::Collation::Latin1Bin
                | tidb_datatype::Collation::Utf8Bin
                | tidb_datatype::Collation::Utf8Mb4Bin
                | tidb_datatype::Collation::Utf8Mb40900Bin
        )
}

/// Go `planner.optimize` calls `TryFastPlan` before constructing the ordinary
/// logical plan. Keep that ordering for a complete point-read SELECT: only a
/// source-column projection whose whole predicate is owned by the point key is
/// returned here. Every residual operator or unsupported table shape declines
/// to the ordinary planner below.
fn fast_point_schema(table: &KvTable, output: &FastPointOutput) -> Schema {
    let columns = output
        .offsets
        .iter()
        .zip(&output.columns)
        .enumerate()
        .map(|(position, (offset, (name, field_type)))| {
            let id = table
                .columns
                .get(*offset)
                .map_or(tidb_model::column::EXTRA_HANDLE_ID, |column| column.id);
            let mut column = Column::new(id, field_type.clone());
            column.id = id;
            column.index = position as i64;
            column.orig_name.clone_from(name);
            column
        })
        .collect();
    Schema::new(columns)
}

fn closed_point_ranges(keys: &[Vec<Datum>]) -> tidb_planner::ranger::types::Ranges {
    keys.iter()
        .map(|values| tidb_planner::ranger::types::Range {
            low_val: values.clone(),
            high_val: values.clone(),
            collators: vec![tidb_datatype::Collation::Binary; values.len()],
            low_exclude: false,
            high_exclude: false,
        })
        .collect()
}

/// Go `TryFastPlan` for a query statement, producing the same complete
/// physical root later consumed by `executorBuilder.build`. This is the
/// optimizer stage; it never drains a source or constructs a separate
/// executor pipeline.
pub(crate) fn try_fast_point_physical_plan(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<tidb_planner::physical::PhysicalPlan>, DriverError> {
    try_fast_point_physical_plan_with_allocator(
        select,
        catalog,
        current_db,
        ctx,
        &tidb_planner::plan_base::PlanIdAllocator::new(),
    )
}

/// Go's fast-plan builder using the enclosing statement's plan-id counter.
/// DML tries its point child before allocating the write root, while a plain
/// query starts from a fresh counter; both call sites therefore share the
/// same implementation without assigning fixed IDs by hand.
pub(crate) fn try_fast_point_physical_plan_with_allocator(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    plan_ids: &tidb_planner::plan_base::PlanIdAllocator,
) -> Result<Option<tidb_planner::physical::PhysicalPlan>, DriverError> {
    try_fast_point_physical_plan_with_allocator_mode(
        select, catalog, current_db, ctx, plan_ids, false,
    )
}

/// Go's `tryUpdatePointPlan` / `tryDeletePointPlan` retain the complete table
/// row and its handle on the PointGet child instead of applying a SELECT-list
/// projection.
pub(crate) fn try_fast_dml_point_physical_plan_with_allocator(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    plan_ids: &tidb_planner::plan_base::PlanIdAllocator,
) -> Result<Option<tidb_planner::physical::PhysicalPlan>, DriverError> {
    try_fast_point_physical_plan_with_allocator_mode(
        select, catalog, current_db, ctx, plan_ids, true,
    )
}

fn try_fast_point_physical_plan_with_allocator_mode(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    plan_ids: &tidb_planner::plan_base::PlanIdAllocator,
    dml_source: bool,
) -> Result<Option<tidb_planner::physical::PhysicalPlan>, DriverError> {
    if ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
        || select.into_outfile.is_some()
        || select.lock.is_some()
    {
        return Ok(None);
    }
    let Some(table_ref) = single_table_ref(&select.from) else {
        return Ok(None);
    };
    if table_ref.as_of.is_some() || table_ref.sample.is_some() || !table_ref.partitions.is_empty() {
        return Ok(None);
    }
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    let Some(entry @ TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return Ok(None);
    };
    if table.partition().is_some() {
        return Ok(None);
    }
    let columns = entry.column_list();
    let visible = table_ref.alias.as_deref().unwrap_or(name);
    let mut scope = single_table_scope(
        visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.clone(),
    );
    scope.zone = ctx.session_zone();
    let output = if dml_source {
        fast_dml_point_output(table)
    } else {
        let Some(output) = fast_point_output(select, &scope) else {
            return Ok(None);
        };
        output
    };
    let schema = fast_point_schema(table, &output);

    let mut batch = fast_batch_partition_supported(table)
        .then(|| try_batch_point_get(select, table, &columns, &scope.zone))
        .transpose()?
        .flatten();
    if batch.as_ref().is_some_and(|batch| !batch.ignores_hints()) {
        let hints = crate::index_hints::single_table_scan_hints(
            select,
            Some(table_ref),
            table,
            current_db,
            ctx,
        )?;
        batch = batch.filter(|batch| batch.allowed_by(&hints));
    }
    if let Some(batch) = batch {
        let mut base =
            tidb_planner::physical::BasePhysicalPlan::new(plan_ids, "Batch_Point_Get", 0);
        base.base
            .set_stats(Some(tidb_planner::stats_info::StatsInfo::new(
                batch.plan_rows as f64,
                [],
            )));
        base.base.set_schema(Some(schema));
        let ranges = closed_point_ranges(&batch.key_values);
        crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
        return Ok(Some(tidb_planner::physical::PhysicalPlan::BatchPointGet(
            tidb_planner::physical::PhysicalBatchPointGet {
                base,
                table_id: table.table_id,
                index_id: batch.index.as_ref().map(|(id, _)| *id),
                ranges,
                range_rebuild: None,
                keep_order: false,
                desc: false,
            },
        )));
    }

    let hints = crate::index_hints::single_table_scan_hints(
        select,
        Some(table_ref),
        table,
        current_db,
        ctx,
    )?;
    if !hints.allows_table() {
        return Ok(None);
    }
    let Some(point) = try_point_get(
        &PointPlanStmt::of_select(select),
        table,
        &columns,
        &scope.zone,
    )?
    else {
        return Ok(None);
    };
    if !point_get_consumes_where(select, table, &columns, &scope.zone) {
        return Ok(None);
    }
    let mut base = tidb_planner::physical::BasePhysicalPlan::new(plan_ids, "Point_Get", 0);
    base.base
        .set_stats(Some(tidb_planner::stats_info::StatsInfo::new(1.0, [])));
    base.base.set_schema(Some(schema));
    crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
    Ok(Some(tidb_planner::physical::PhysicalPlan::PointGet(
        tidb_planner::physical::PhysicalPointGet {
            base,
            table_id: table.table_id,
            index_id: point.index_id,
            ranges: closed_point_ranges(&[point.key_values]),
            range_rebuild: None,
        },
    )))
}

fn fast_dml_point_output(table: &KvTable) -> FastPointOutput {
    let mut offsets = (0..table.columns.len()).collect::<Vec<_>>();
    let mut columns = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect::<Vec<_>>();
    if table.pk_handle_offset().is_none() && table.common_handle_offsets().is_empty() {
        offsets.push(table.columns.len());
        columns.push((
            tidb_model::column::EXTRA_HANDLE_NAME.to_owned(),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
        ));
    }
    FastPointOutput { offsets, columns }
}

/// Whether Rust can route Go's partitioned fast batch point plan from the
/// handles retained after key lookup. Secondary-index values are no longer
/// available at that point, so every partition dependency must be part of a
/// clustered handle; other valid plans fall back to the ordinary index path.
fn fast_batch_partition_supported(table: &KvTable) -> bool {
    table.partition().is_none_or(|partition| {
        let handle_offsets = table
            .pk_handle_offset()
            .into_iter()
            .chain(table.common_handle_offsets().iter().copied())
            .collect::<Vec<_>>();
        !handle_offsets.is_empty()
            && !partition.dependencies.is_empty()
            && matches!(partition.expr, tidb_expr::expression::Expression::Column(_))
            && partition.dependencies.iter().all(|dependency| {
                handle_offsets
                    .iter()
                    .any(|offset| table.columns[*offset].name.eq_ignore_ascii_case(dependency))
            })
    })
}

#[cfg(test)]
thread_local! {
    static ORDINARY_ACCESS_PATH_ENTRIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_ordinary_access_path_entries() {
    ORDINARY_ACCESS_PATH_ENTRIES.with(|entries| entries.set(0));
}

#[cfg(test)]
pub(crate) fn ordinary_access_path_entries() -> usize {
    ORDINARY_ACCESS_PATH_ENTRIES.with(std::cell::Cell::get)
}

fn fast_point_output(select: &tidb_ast::SelectStmt, scope: &FromScope) -> Option<FastPointOutput> {
    let resolver = ScopeResolver { scope };
    let mut offsets = Vec::new();
    let mut columns = Vec::new();
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        match field {
            SelectField::Expr {
                expr: expr @ tidb_ast::Expr::Column(path),
                alias,
            } => {
                let (offset, field_type, _) = resolver.resolve(path)?;
                let name = alias.clone().unwrap_or_else(|| {
                    default_field_display_name(&select.fields, field_index, expr)
                });
                offsets.push(offset);
                columns.push((name, field_type));
            }
            SelectField::Expr { .. } => return None,
            SelectField::Wildcard(qualifier) => {
                if qualifier.last().is_none() {
                    for (offset, name, field_type) in scope.star_columns() {
                        offsets.push(offset);
                        columns.push((name, field_type));
                    }
                    continue;
                }
                let table_name = qualifier.last()?;
                let mut matched = false;
                for table in scope
                    .tables
                    .iter()
                    .filter(|table| table.name.eq_ignore_ascii_case(table_name))
                {
                    matched = true;
                    for (local_offset, (name, field_type)) in table.columns.iter().enumerate() {
                        offsets.push(table.offset + local_offset);
                        columns.push((name.clone(), field_type.clone()));
                    }
                }
                if !matched {
                    return None;
                }
            }
        }
    }
    Some(FastPointOutput { offsets, columns })
}

fn point_get_consumes_where(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    point_get_predicate_is_consumed(
        select.where_clause.as_ref(),
        table,
        columns,
        zone,
        sole_table_ref(&select.from).map_or(&[][..], |table_ref| table_ref.partitions.as_slice()),
    )
}

pub(crate) fn point_get_predicate_is_consumed(
    where_clause: Option<&tidb_ast::Expr>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    named_partitions: &[String],
) -> bool {
    let Some(where_clause) = where_clause else {
        return false;
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs, zone) || pairs.is_empty() {
        return false;
    }
    let key_matches = |offsets: &[usize]| {
        offsets.len() == pairs.len()
            && offsets.iter().all(|offset| {
                columns.get(*offset).is_some_and(|(name, _)| {
                    pairs
                        .iter()
                        .any(|pair| pair.column.eq_ignore_ascii_case(name))
                })
            })
    };
    if table
        .pk_handle_offset()
        .is_some_and(|offset| key_matches(std::slice::from_ref(&offset)))
        || key_matches(table.common_handle_offsets())
    {
        return true;
    }
    // The extra handle pins a row as completely as an integer primary key
    // does, so Go's point plan over `_tidb_rowid = c` carries no `Selection`
    // either -- its recorded plan for `select * from t where _tidb_rowid = 0`
    // is a bare `Point_Get table:t handle:0`.
    if table.pk_handle_offset().is_none()
        && table.common_handle_offsets().is_empty()
        && (table.partition().is_none() || named_partitions.len() == 1)
        && pairs.len() == 1
        && pairs[0]
            .column
            .eq_ignore_ascii_case(tidb_model::column::EXTRA_HANDLE_NAME)
    {
        return true;
    }
    let matches_unique_index = table
        .plan_indexes()
        .any(|index| index.unique && !index.has_prefix() && key_matches(&index.column_offsets));
    matches_unique_index
}

/// The partitions a single-table `SELECT` still reads, named as declared and
/// in definition order -- Go's `PartitionProcessor` output, which is the list
/// `EXPLAIN` fans a static-mode plan out over.
///
/// Two narrowings compose, in either order and both cumulative, exactly as
/// [`crate::KvTable::restrict_read_to_partitions`] composes them for the read
/// itself: the statement's own `PARTITION (p, ...)` list, and whatever the
/// `WHERE` pruned. Empty for an unpartitioned table.
///
/// An unresolvable `PARTITION (p)` name answers the FULL list rather than
/// failing here: the read has already raised 1735 for it, and this is only
/// ever asked for a plan that got built.
pub(crate) fn single_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let table_ref = sole_table_ref(from)?;
    // A `PARTITION (...)` restriction is refused by `build_from`; declining
    // the fast path here too keeps a point get from answering a statement the
    // scan would have rejected.
    if !table_ref.partitions.is_empty() {
        return None;
    }
    Some(table_ref)
}

/// [`single_table_ref`] WITHOUT its fast-path refusal: the one table a `FROM`
/// names, whether or not the statement narrowed it with `PARTITION (...)`.
///
/// The refusal above is about which ACCESS PATHS may be chosen. Callers that
/// only want to know which table -- and which partitions of it -- the
/// statement reads want this one, so that a `PARTITION (p)` narrowing is
/// reported rather than silently read as "no single table".
pub(crate) fn sole_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let join = from.as_ref()?;
    if join.right.is_some() {
        return None;
    }
    let JoinNode::Table(table_ref) = &join.left else {
        return None;
    };
    Some(table_ref)
}

/// Go `tryWhereIn2BatchPointGet`: a single-table `SELECT` whose whole `WHERE`
/// is `column IN (constants)` over the handle or a single-column unique index
/// reads those rows directly instead of scanning.
///
/// Go rejects the fast plan when `ORDER BY`, `GROUP BY`, `LIMIT`, `HAVING`,
/// `DISTINCT` or a window spec is present, when the `IN` is negated, and when
/// its list is empty. The handle path applies when the table's primary key IS
/// the handle and the column names it; otherwise a unique index whose only
/// column it is.
///
/// The row form, `(a, b) IN ((1, 2), (3, 4))`, is a composite-key
/// `Batch_Point_Get` when the tuples pin every column of a unique index or a
/// clustered common handle.
pub(crate) struct BatchPointLookup {
    index: Option<(i64, String)>,
    common_handle: bool,
    plan_rows: usize,
    key_values: Vec<Vec<Datum>>,
}

impl BatchPointLookup {
    fn handle(handles: Vec<TableHandle>, plan_rows: usize) -> Self {
        let key_values = handles
            .iter()
            .filter_map(|handle| match handle {
                TableHandle::Int(value) => Some(vec![Datum::Int(*value)]),
                TableHandle::Common(_) => None,
            })
            .collect();
        Self {
            index: None,
            common_handle: false,
            plan_rows,
            key_values,
        }
    }

    fn common_handle(key_values: Vec<Vec<Datum>>, plan_rows: usize) -> Self {
        Self {
            index: None,
            common_handle: true,
            plan_rows,
            key_values,
        }
    }

    fn index(
        plan_rows: usize,
        table: &KvTable,
        columns: &[(String, FieldType)],
        index: &crate::kv_table::KvIndex,
        key_values: Vec<Vec<Datum>>,
    ) -> Self {
        let index_columns = index
            .column_offsets
            .iter()
            .map(|offset| {
                columns.get(*offset).map_or_else(
                    || index_key_part_name(table, *offset),
                    |column| column.0.clone(),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            index: Some((index.id, format!("index:{}({index_columns})", index.name))),
            common_handle: false,
            plan_rows,
            key_values,
        }
    }

    fn ignores_hints(&self) -> bool {
        self.index.is_none() && !self.common_handle
    }

    fn allowed_by(&self, hints: &crate::index_hints::AvailablePaths) -> bool {
        match &self.index {
            Some((index_id, _)) => hints.allows_index(*index_id),
            None if self.common_handle => hints.allows_common_primary(),
            None => true,
        }
    }
}

pub(crate) fn try_batch_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<BatchPointLookup>, DriverError> {
    let stmt = PointPlanStmt::of_select(select);
    try_batch_point_get_stmt(&stmt, table, columns, zone)
}

pub(crate) fn try_batch_point_get_stmt(
    select: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<BatchPointLookup>, DriverError> {
    if select.having.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.limit.is_some()
        || select.distinct
    {
        return Ok(None);
    }
    let Some(where_clause) = select.where_clause else {
        return Ok(None);
    };
    // The WHERE must be exactly the IN, as Go requires a PatternInExpr.
    let tidb_ast::Expr::In { expr, list, not } = where_clause else {
        return Ok(None);
    };
    if *not || list.is_empty() {
        return Ok(None);
    }
    // The row form is Go's composite-key Batch_Point_Get. Each tuple value
    // is converted into the indexed column's domain before the key lookup;
    // any value that cannot round-trip exactly declines the fast path and
    // leaves the ordinary scan to preserve the written predicate's answer.
    if let tidb_ast::Expr::Row(left) = &**expr {
        let mut names = Vec::with_capacity(left.len());
        for column in left {
            let tidb_ast::Expr::Column(path) = column else {
                return Ok(None);
            };
            let Some(name) = path.last() else {
                return Ok(None);
            };
            names.push(name);
        }
        let mut table = table.clone();
        // A clustered composite primary key is represented by the common
        // handle offsets, not by a KvIndex. Its encoded datum key is the
        // record handle itself, so it can use the same direct lookup source as
        // a unique index without manufacturing a redundant index entry.
        let common_offsets = table.common_handle_offsets().to_vec();
        if common_offsets.len() == names.len() {
            let mut positions = Vec::with_capacity(common_offsets.len());
            for offset in &common_offsets {
                let Some((column_name, _)) = columns.get(*offset) else {
                    positions.clear();
                    break;
                };
                let Some(position) = names
                    .iter()
                    .position(|name| column_name.eq_ignore_ascii_case(name))
                else {
                    positions.clear();
                    break;
                };
                positions.push(position);
            }
            if positions.len() == common_offsets.len() {
                let mut handles = Vec::with_capacity(list.len());
                let mut all_key_values = Vec::with_capacity(list.len());
                for candidate in list {
                    let tidb_ast::Expr::Row(values) = candidate else {
                        return Ok(None);
                    };
                    if values.len() != left.len() {
                        return Ok(None);
                    }
                    let mut key_values = Vec::with_capacity(common_offsets.len());
                    for (offset, position) in common_offsets.iter().zip(&positions) {
                        let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
                            &values[*position],
                            &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                        ) else {
                            return Ok(None);
                        };
                        let Ok(value) = constant.eval() else {
                            return Ok(None);
                        };
                        let Some(value) = point_get_value(&columns[*offset].1, &value) else {
                            return Ok(None);
                        };
                        key_values.push(value);
                    }
                    let encoded =
                        tidb_codec::encode_key_in_timezone(zone, &key_values).map_err(|e| {
                            DriverError::Parse(format!("common handle encode failed: {e:?}"))
                        })?;
                    let handle = tidb_txnkv::CommonHandle::new(encoded).map_err(|e| {
                        DriverError::Parse(format!("common handle build failed: {e:?}"))
                    })?;
                    let handle = TableHandle::Common(handle.encoded().to_vec());
                    if !handles.contains(&handle) {
                        handles.push(handle);
                        all_key_values.push(key_values);
                    }
                }
                return Ok(Some(BatchPointLookup::common_handle(
                    all_key_values,
                    list.len(),
                )));
            }
        }
        for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
            if !index.unique || index.has_prefix() || index.column_offsets.len() != names.len() {
                continue;
            }
            let mut positions = Vec::with_capacity(index.column_offsets.len());
            for offset in &index.column_offsets {
                let Some((column_name, _)) = columns.get(*offset) else {
                    positions.clear();
                    break;
                };
                let Some(position) = names
                    .iter()
                    .position(|name| column_name.eq_ignore_ascii_case(name))
                else {
                    positions.clear();
                    break;
                };
                positions.push(position);
            }
            if positions.len() != index.column_offsets.len() {
                continue;
            }
            let mut index_values = Vec::with_capacity(list.len());
            for candidate in list {
                let tidb_ast::Expr::Row(values) = candidate else {
                    return Ok(None);
                };
                if values.len() != left.len() {
                    return Ok(None);
                }
                let mut key_values = Vec::with_capacity(index.column_offsets.len());
                for (offset, position) in index.column_offsets.iter().zip(&positions) {
                    let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
                        &values[*position],
                        &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                    ) else {
                        return Ok(None);
                    };
                    let Ok(value) = constant.eval() else {
                        return Ok(None);
                    };
                    let Some(value) = point_get_value(&columns[*offset].1, &value) else {
                        return Ok(None);
                    };
                    key_values.push(value);
                }
                index_values.push(key_values);
            }
            let mut handles = Vec::with_capacity(index_values.len());
            for handle in table
                .lookup_unique_batched(index.id, &index_values, zone)
                .map_err(|e| DriverError::Parse(format!("index batch lookup failed: {e:?}")))?
                .into_iter()
                .flatten()
            {
                if !handles.contains(&handle) {
                    handles.push(handle);
                }
            }
            return Ok(Some(BatchPointLookup::index(
                list.len(),
                &table,
                columns,
                &index,
                index_values,
            )));
        }
        return Ok(None);
    }
    let tidb_ast::Expr::Column(path) = &**expr else {
        return Ok(None);
    };
    let Some(name) = path.last() else {
        return Ok(None);
    };

    // Every list element must be a constant, or this is not a point plan.
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
            item,
            &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
        ) else {
            return Ok(None);
        };
        let Ok(value) = constant.eval() else {
            return Ok(None);
        };
        values.push(value);
    }

    // The handle path.
    if let Some(offset) = table.pk_handle_offset() {
        if columns[offset].0.eq_ignore_ascii_case(name) {
            // Go `newBatchPointGetPlan` runs every list element through
            // `getPointGetValue` and returns `nil` -- no batch plan at all --
            // as soon as one of them is not exactly representable, so a list
            // mixing `1.0` with `1.5` still answers from a scan rather than
            // silently dropping the element it cannot key.
            let mut handles = Vec::with_capacity(values.len());
            for value in &values {
                match point_get_value(&columns[offset].1, value) {
                    Some(Datum::Int(v)) => {
                        let handle = TableHandle::Int(v);
                        if !handles.contains(&handle) {
                            handles.push(handle);
                        }
                    }
                    Some(Datum::UInt(v)) => {
                        let handle = TableHandle::Int(v as i64);
                        if !handles.contains(&handle) {
                            handles.push(handle);
                        }
                    }
                    _ => return Ok(None),
                }
            }
            return Ok(Some(BatchPointLookup::handle(handles, list.len())));
        }
    }

    // The unique-index path.
    let mut table = table.clone();
    for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
        if !index.unique || index.column_offsets.len() != 1 {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        // Resolved through `get` for the same reason the single point get
        // does: an EXPRESSION key part's hidden generated column sits past
        // the end of the scope's visible columns, and no `IN` list names it.
        let Some((index_column, field_type)) = columns.get(index.column_offsets[0]) else {
            continue;
        };
        if !index_column.eq_ignore_ascii_case(name) {
            continue;
        }
        let mut converted = Vec::with_capacity(values.len());
        for value in &values {
            let Some(value) = point_get_value(field_type, value) else {
                return Ok(None);
            };
            converted.push(value);
        }
        let index_values = converted
            .into_iter()
            .map(|value| vec![value])
            .collect::<Vec<_>>();
        let mut handles = Vec::with_capacity(index_values.len());
        for handle in table
            .lookup_unique_batched(index.id, &index_values, zone)
            .map_err(|e| DriverError::Parse(format!("index batch lookup failed: {e:?}")))?
            .into_iter()
            .flatten()
        {
            if !handles.contains(&handle) {
                handles.push(handle);
            }
        }
        return Ok(Some(BatchPointLookup::index(
            list.len(),
            &table,
            columns,
            &index,
            index_values,
        )));
    }
    Ok(None)
}

/// Moves every pair's constant into its column's domain, in place.
///
/// Returns false when any pair names an unknown column or holds a constant
/// the column cannot represent exactly, which is Go's "no point plan; let the
/// scan decide" answer.
pub(crate) fn convert_pairs_to_column_domain(
    pairs: &mut [NameValuePair],
    columns: &[(String, FieldType)],
) -> bool {
    for pair in pairs {
        let Some((_, field_type)) = columns
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&pair.column))
        else {
            return false;
        };
        let Some(value) = point_get_value(field_type, &pair.value) else {
            return false;
        };
        pair.value = value;
    }
    true
}

/// One `column = constant` equality from a `WHERE`, Go's `nameValuePair`.
pub(crate) struct NameValuePair {
    column: String,
    value: Datum,
}

impl NameValuePair {
    /// The column this equality pinned.
    pub(crate) fn column(&self) -> &str {
        &self.column
    }

    /// The value it pinned the column to, already moved into that column's
    /// domain by [`convert_pairs_to_column_domain`].
    pub(crate) const fn value(&self) -> &Datum {
        &self.value
    }
}

fn index_key_part_name(table: &KvTable, offset: usize) -> String {
    let Some(column) = table.columns.get(offset) else {
        return String::new();
    };
    match &column.generated {
        Some(generated) if table.is_hidden(offset) => generated.expr_text.clone(),
        _ => column.name.clone(),
    }
}

/// Go `getNameValuePairs`: flattens a `WHERE` that is a conjunction of
/// `column = constant` equalities into pairs, returning `None` for any other
/// shape.
///
/// Go accepts the constant on either side of the `=`, and recurses only
/// through `AND`; anything else (an `OR`, a comparison, a function call)
/// makes the statement ineligible for a point get, which is what returning
/// `None` means here.
pub(crate) fn name_value_pairs(
    expr: &tidb_ast::Expr,
    pairs: &mut Vec<NameValuePair>,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    point_equal_pairs(expr, pairs, zone, &[])
}

/// [`Self::name_value_pairs`]-shaped walker that ALSO resolves `?` markers
/// against EXECUTE parameters, so one implementation decides the point-pin
/// rule for a bound tree and for a PREPARE template alike. A template whose
/// marker position is unfilled refuses, exactly like a non-literal.
pub(crate) fn point_equal_pairs(
    expr: &tidb_ast::Expr,
    pairs: &mut Vec<NameValuePair>,
    zone: &tidb_datatype::SessionTimeZone,
    params: &[Datum],
) -> bool {
    use tidb_ast::{BinaryOp, Expr};
    fn unparenthesized(expr: &Expr) -> &Expr {
        match expr {
            Expr::Paren(inner) => unparenthesized(inner),
            other => other,
        }
    }
    match expr {
        Expr::Paren(inner) => point_equal_pairs(inner, pairs, zone, params),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            point_equal_pairs(lhs, pairs, zone, params)
                && point_equal_pairs(rhs, pairs, zone, params)
        }
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
            // Stripped on both sides for the reason the arm above recurses
            // through `Expr::Paren`: parentheses are syntax, and Go has
            // unwrapped them before a point-get key is ever looked for, so
            // `(a)=1` names the same key that `a=1` does.
            let (column, value_expr) = match (unparenthesized(lhs), unparenthesized(rhs)) {
                (Expr::Column(path), other) => (path, other),
                (other, Expr::Column(path)) => (path, other),
                _ => return false,
            };
            let Some(name) = column.last() else {
                return false;
            };
            // A `?` marker IS a literal once its parameter arrived; every
            // other shape must resolve to a constant exactly as
            // [`name_value_pairs`] demands — anything needing evaluation
            // against a row is not a point-get key.
            let value = if let Expr::ParamMarker { order, .. } = unparenthesized(value_expr) {
                let Some(datum) = params.get(*order) else {
                    return false;
                };
                datum.clone()
            } else {
                let Ok(value) = rewrite_expr_resolved(
                    value_expr,
                    &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                ) else {
                    return false;
                };
                let Expression::Constant(constant) = value else {
                    return false;
                };
                let Ok(value) = constant.eval() else {
                    return false;
                };
                value
            };
            pairs.push(NameValuePair {
                column: name.clone(),
                value,
            });
            true
        }
        _ => false,
    }
}

/// The clauses [`try_point_get`] decides a point plan from.
///
/// This exists because Go decides a WRITE's point plan from the SAME
/// function as a read's: `tryUpdatePointPlan`/`tryDeletePointPlan`
/// (`pkg/planner/core/point_get_plan.go`) build an `ast.SelectStmt` out of
/// the write's own `TableRefs`/`Where`/`Order`/`Limit` and hand it to
/// `tryPointGetPlan`. This struct IS that synthesis, expressed as the field
/// copy Go performs rather than as a second point-plan builder -- there is
/// one rule here and one implementation of it, and a write cannot drift from
/// a read about which statements are point plans.
pub(crate) struct PointPlanStmt<'a> {
    where_clause: Option<&'a tidb_ast::Expr>,
    order_by: &'a [tidb_ast::OrderItem],
    limit: Option<&'a tidb_ast::Limit>,
    /// Go's synthesized statement carries no select list, so it has neither
    /// of these; only a real `SELECT` can.
    having: Option<&'a tidb_ast::Expr>,
    group_by: &'a [tidb_ast::GroupByItem],
    /// `DISTINCT` is present only on a real `SELECT`; writes set this false.
    distinct: bool,
    /// Go's `DataSource.PartitionNames`: the statement's own
    /// `PARTITION (p, ...)` list, EXACTLY as written and before any pruning.
    ///
    /// It is a point-plan input rather than a read restriction because
    /// `find_best_task.go`'s point-get conversion tests its LENGTH: "Partition
    /// table can't use `_tidb_rowid` to generate PointGet Plan unless one
    /// partition is explicitly specified" -- `len(ds.PartitionNames) != 1`
    /// disables the conversion. What the `WHERE` happened to prune is NOT
    /// that list, which is why the restricted `KvTable` cannot answer this.
    named_partitions: &'a [String],
    /// The single write target Go copies into the SELECT-shaped fast-plan
    /// input. The ordinary write path uses the same table reference when it
    /// enters the shared logical planner, preserving alias, partition, and
    /// index-hint identity.
    table_ref: Option<&'a tidb_ast::TableRef>,
}

impl<'a> PointPlanStmt<'a> {
    /// A `SELECT`'s own clauses.
    pub(crate) fn of_select(select: &'a tidb_ast::SelectStmt) -> Self {
        PointPlanStmt {
            where_clause: select.where_clause.as_ref(),
            order_by: &select.order_by,
            limit: select.limit.as_ref(),
            having: select.having.as_ref(),
            group_by: &select.group_by,
            distinct: select.distinct,
            named_partitions: sole_table_ref(&select.from)
                .map_or(&[][..], |table_ref| table_ref.partitions.as_slice()),
            table_ref: sole_table_ref(&select.from),
        }
    }

    /// Go's synthesized `ast.SelectStmt` for a single-table write: the three
    /// clauses `tryUpdatePointPlan`/`tryDeletePointPlan` copy across, and
    /// nothing else.
    pub(crate) fn of_write(
        where_clause: Option<&'a tidb_ast::Expr>,
        order_by: &'a [tidb_ast::OrderItem],
        limit: Option<&'a tidb_ast::Limit>,
        table_ref: &'a tidb_ast::TableRef,
    ) -> Self {
        PointPlanStmt {
            where_clause,
            order_by,
            limit,
            having: None,
            group_by: &[],
            distinct: false,
            named_partitions: &table_ref.partitions,
            table_ref: Some(table_ref),
        }
    }

    /// The SELECT-shaped logical input used for an ordinary single-table
    /// write. Go builds the write input directly as a DataSource rather than
    /// maintaining a separate access chooser; the Rust shared planner exposes
    /// SELECT as that same entry point, so copy the write clauses and request
    /// the complete row through `*`.
    pub(crate) fn write_select(&self) -> Option<tidb_ast::SelectStmt> {
        let table_ref = self.table_ref?.clone();
        let mut fields = tidb_ast::SelectFieldList::default();
        fields.push(tidb_ast::SelectField::Wildcard(Vec::new()));
        Some(tidb_ast::SelectStmt {
            kind: Default::default(),
            is_in_braces: false,
            with: None,
            hints: Vec::new(),
            priority: Default::default(),
            sql_small_result: false,
            sql_big_result: false,
            sql_buffer_result: false,
            sql_no_cache: false,
            straight_join: false,
            calc_found_rows: false,
            distinct: false,
            all: false,
            fields,
            values: Vec::new(),
            from: Some(tidb_ast::Join {
                left: tidb_ast::JoinNode::Table(table_ref),
                right: None,
                tp: tidb_ast::JoinType::Cross,
                straight: false,
                on: None,
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            }),
            where_clause: self.where_clause.cloned(),
            group_by: Vec::new(),
            rollup: false,
            having: None,
            windows: Vec::new(),
            order_by: self.order_by.to_vec(),
            limit: self.limit.cloned(),
            lock: None,
            into_outfile: None,
            into_vars: Vec::new(),
        })
    }
}

/// The row a point get reads, when the statement qualifies for one.
///
/// Go `TryFastPlan`/`tryPointGetPlan`: a single-table statement with no
/// `HAVING` and no `ORDER BY`, whose `WHERE` is a conjunction of equalities
/// that pins either the handle or every column of a unique index, reads one
/// row directly instead of scanning. `LIMIT` is allowed only when it cannot
/// remove the row (`count > 0` and `offset == 0`), matching Go's check.
///
/// Returns `Ok(None)` when the statement does not qualify, so the caller
/// falls back to the ordinary scan.
/// What pinned a point get: Go `PointGetPlan`'s split between a HANDLE plan
/// and an INDEX plan, which is exactly what its `AccessObject` prints --
/// `table:t handle:N` for the first, `table:t, index:idx(cols)` for the
/// second, never both.
#[derive(Clone, Debug)]
pub(crate) struct PointGetPin {
    /// The exact handle or unique-index values retained by Go's point plan.
    pub(crate) key_values: Vec<Datum>,
    /// The selected unique-index id, or `None` for a record-handle lookup.
    pub(crate) index_id: Option<i64>,
}

pub(crate) fn try_point_get(
    select: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<PointGetPin>, DriverError> {
    if select.having.is_some() || !select.order_by.is_empty() || !select.group_by.is_empty() {
        return Ok(None);
    }
    if let Some(limit) = select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        if count == 0 || offset > 0 {
            return Ok(None);
        }
    }
    let Some(where_clause) = select.where_clause else {
        return Ok(None);
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs, zone) || pairs.is_empty() {
        return Ok(None);
    }
    // Go `findPKHandle`'s `!tblInfo.PKIsHandle` branch: when the table has no
    // primary-key handle, the pair naming `_tidb_rowid` IS the handle pair,
    // and its type is `TypeLonglong` rather than any stored column's -- which
    // is why this runs BEFORE the column-domain conversion below, where
    // `_tidb_rowid` names nothing to convert against.
    //
    // Go refuses it for a PARTITIONED table (`point_get_plan.go`: "Partition
    // table can't use `_tidb_rowid` to generate PointGet Plan"), because a row
    // id alone does not say which partition holds the row -- UNLESS the
    // statement said which one. `find_best_task.go`'s point-get conversion
    // carries the exception verbatim:
    //
    // ```go
    // // Partition table can't use `_tidb_rowid` to generate PointGet Plan
    // // unless one partition is explicitly specified.
    // if canConvertPointGet && path.IsIntHandlePath &&
    //     !ds.Table.Meta().PKIsHandle && len(ds.PartitionNames) != 1 {
    //     canConvertPointGet = false
    // }
    // ```
    //
    // The test is on the WRITTEN list's length, not on how many partitions
    // survived: TiDB's own recording gives `Point_Get table:t, partition:p0`
    // for `select *,_tidb_rowid from t partition(p0) where _tidb_rowid=1`,
    // and a `TableRangeScan` for both the bare form and `partition(p0,p1)`.
    let single_named_partition = select.named_partitions.len() == 1;
    if table.pk_handle_offset().is_none()
        && table.common_handle_offsets().is_empty()
        && (table.partition().is_none() || single_named_partition)
        && pairs.len() == 1
        && pairs[0]
            .column
            .eq_ignore_ascii_case(tidb_model::column::EXTRA_HANDLE_NAME)
    {
        let handle_type = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let Some(value) = point_get_value(&handle_type, &pairs[0].value) else {
            return Ok(None);
        };
        return Ok(Some(PointGetPin {
            key_values: vec![value],
            index_id: None,
        }));
    }

    // Go `getNameValuePairs` moves every constant into its column's domain
    // before the pair is usable as a key, and abandons the whole point plan
    // when one of them will not survive the round trip. Doing it here, once
    // for every pair, is what keeps the handle arm below dealing only in
    // integers and the unique-index arm dealing only in column-typed values.
    if !convert_pairs_to_column_domain(&mut pairs, columns) {
        return Ok(None);
    }

    // The handle path: the primary key pinned by exactly one equality, which
    // is Go's `len(pairs) == 1` condition on the handle pair.
    if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if pairs.len() == 1 && pairs[0].column.eq_ignore_ascii_case(handle_column) {
            return Ok(Some(PointGetPin {
                key_values: vec![pairs[0].value.clone()],
                index_id: None,
            }));
        }
        // Go's `else if handlePair.value.Kind() != KindNull { return nil }`:
        // once a HANDLE pair exists among the conjuncts, the unique-index arm
        // is never tried -- the fast point plan is refused outright and the
        // ordinary planner takes over, whose `convertToPointGet` prints the
        // bare handle plan (`Point_Get table:t`) with the extra conjunct as a
        // filter. Falling through to the unique index here instead printed
        // `index:i(i, j)` where TiDB's recorded plan names no index.
        if pairs
            .iter()
            .any(|pair| pair.column.eq_ignore_ascii_case(handle_column))
        {
            return Ok(None);
        }
    }

    // A clustered composite primary key is encoded directly as a common
    // handle rather than materialized as a secondary `KvIndex`. When every
    // handle column is pinned, it is the same one-row lookup as an integer
    // point get; extra equalities remain in the filter above the source.
    let common_offsets = table.common_handle_offsets().to_vec();
    if !common_offsets.is_empty() {
        let mut values = Vec::with_capacity(common_offsets.len());
        for offset in common_offsets {
            let Some((name, _)) = columns.get(offset) else {
                values.clear();
                break;
            };
            let Some(pair) = pairs
                .iter()
                .find(|pair| pair.column.eq_ignore_ascii_case(name))
            else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() == table.common_handle_offsets().len() {
            // A clustered common handle IS the record key, so it prints as
            // a handle plan, not an index one.
            return Ok(Some(PointGetPin {
                key_values: values,
                index_id: None,
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    for index in table.plan_indexes() {
        if !index.unique {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        let mut values = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            // Go `getIndexValues` resolves each key part by NAME against the
            // `WHERE`'s pairs, so a key part the statement cannot name
            // declines the whole index. The hidden generated column an
            // EXPRESSION key part was rewritten into is exactly such a part:
            // it lives past the end of the scope's visible column list, and
            // `tidb_shard(a)` is not a name any `WHERE` writes. Resolving the
            // name through `get` makes "no visible column at that offset" and
            // "not pinned by the WHERE" the same answer -- without it the
            // offset indexes past the end and panics, which is what
            // `explain_shard_index`'s `where a=100` reached.
            let Some(pair) = columns.get(*offset).and_then(|(name, _)| {
                pairs
                    .iter()
                    .find(|pair| pair.column.eq_ignore_ascii_case(name))
            }) else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() != index.column_offsets.len() {
            continue;
        }
        return Ok(Some(PointGetPin {
            key_values: values,
            index_id: Some(index.id),
        }));
    }
    Ok(None)
}

/// The record keys one single-table point `UPDATE`/`DELETE` locks BEFORE it
/// runs, so its row read folds into the lock that demands it.
///
/// Go's pessimistic point write takes this fold for granted:
/// `tryUpdatePointPlan`/`tryDeletePointPlan` (`pkg/planner/core/point_get_plan.go`)
/// pin ONE row by handle equality, and `PointGetExecutor.getAndLock`
/// (`pkg/executor/point_get.go:549`) asks that lock to carry the row back
/// (`InitReturnValues(1)`, line 614) instead of issuing a separate get. This
/// function is the plan-shape half of that fold: it accepts exactly the writes
/// whose whole read is one handle-pinned row — the conjunction rule
/// [`name_value_pairs`] enforces for reads, with `?` markers resolved against
/// EXECUTE parameters — and returns the encoded record keys for the pinned
/// rows. Anything else (an `OR`, a range, an index-pinned row, a multi-table
/// write, an unfilled marker) returns an empty vector and keeps today's
/// read-then-lock order.
///
/// Accepting is safe in exactly one direction, and it is the right one: a
/// statement accepted here reads at most the rows its pinned keys name, so
/// pre-locking them can never miss a row the statement would have read
/// unlocked. Refusing merely costs the extra round trip the fold removes.
pub(crate) fn point_write_prelock_keys(
    table: &KvTable,
    where_clause: &tidb_ast::Expr,
    params: &[Datum],
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Vec<u8>> {
    let mut pairs = Vec::new();
    if !point_equal_pairs(where_clause, &mut pairs, zone, params) || pairs.is_empty() {
        return Vec::new();
    }
    // The column list is the table's own (all columns, in order), because the
    // domain conversion and the handle offsets below both index into it --
    // the same list [`super::access_path`] builds for a write's own read-path
    // decision (`write_read_path`).
    let columns: Vec<(String, tidb_datatype::FieldType)> = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    if !convert_pairs_to_column_domain(&mut pairs, &columns) {
        return Vec::new();
    }
    let handle = if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if pairs.len() == 1 && pairs[0].column.eq_ignore_ascii_case(handle_column) {
            match pairs[0].value {
                Datum::Int(value) => Some(TableHandle::Int(value)),
                Datum::UInt(value) => Some(TableHandle::Int(value as i64)),
                _ => return Vec::new(),
            }
        } else {
            // A handle equality among other conjuncts refuses outright, as
            // [`try_point_get`] does: the fast point plan is off and the
            // ordinary planner filters, which this pre-lock must not guess
            // about.
            return Vec::new();
        }
    } else {
        // A clustered composite primary key IS the record key: every handle
        // column pinned is one known row, same as [`try_point_get`]'s common
        // arm. A partial pin refuses.
        let common_offsets = table.common_handle_offsets();
        if common_offsets.is_empty() || common_offsets.len() != pairs.len() {
            return Vec::new();
        }
        let mut values = Vec::with_capacity(common_offsets.len());
        for offset in common_offsets {
            let Some((name, _)) = columns.get(*offset) else {
                return Vec::new();
            };
            let Some(pair) = pairs
                .iter()
                .find(|pair| pair.column.eq_ignore_ascii_case(name))
            else {
                return Vec::new();
            };
            values.push(pair.value.clone());
        }
        let Ok(encoded) = tidb_codec::encode_key_in_timezone(zone, &values) else {
            return Vec::new();
        };
        Some(TableHandle::Common(encoded))
    };
    let Some(handle) = handle else {
        return Vec::new();
    };
    table
        .record_physical_ids()
        .into_iter()
        .map(|id| tidb_codec::table_key::encode_row_key_with_handle(id, &handle.record_handle()))
        .collect()
}
