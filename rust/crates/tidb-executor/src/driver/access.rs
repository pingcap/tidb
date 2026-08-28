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
use crate::predicate_pushdown::ScanColumnComparison;
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
#[derive(Clone, Debug)]
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
        if self.contradiction {
            // `NOT NULL col IS NULL` matched no rows at PLAN time; parameters
            // cannot change a schema fact.
            return Some(PreparedPointGetExecution {
                plan: Arc::clone(self),
                handle: None,
                range_values: None,
                residuals: Vec::new(),
            });
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
                return Some(PreparedPointGetExecution {
                    plan: Arc::clone(self),
                    handle: None,
                    range_values: None,
                    residuals: Vec::new(),
                });
            }
            key_values.push(match point_get_value(handle_type, value) {
                Some(value) => value,
                None if names_no_rows(handle_type, value) => {
                    // A parameter longer than the column's capacity compares
                    // equal to no stored value: the empty set IS the answer,
                    // the same observable result Go's re-optimized plan
                    // produces, served without re-planning.
                    return Some(PreparedPointGetExecution {
                        plan: Arc::clone(self),
                        handle: None,
                        range_values: None,
                        residuals: Vec::new(),
                    });
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
                        return Some(PreparedPointGetExecution {
                            plan: Arc::clone(self),
                            handle: None,
                            range_values: None,
                            residuals: Vec::new(),
                        });
                    }
                    ResidualCheck::Equal(
                        match point_get_value(&self.output.columns[*position].1, value) {
                            Some(value) => value,
                            None if names_no_rows(&self.output.columns[*position].1, value) => {
                                return Some(PreparedPointGetExecution {
                                    plan: Arc::clone(self),
                                    handle: None,
                                    range_values: None,
                                    residuals: Vec::new(),
                                });
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
            return Some(PreparedPointGetExecution {
                plan: Arc::clone(self),
                handle,
                range_values: None,
                residuals,
            });
        }
        Some(PreparedPointGetExecution {
            plan: Arc::clone(self),
            handle: None,
            range_values: Some(key_values),
            residuals,
        })
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
}

/// The immutable half of a reusable SELECT plan.  This is the complete shared
/// planner tree, not a hand-built executor shortcut: access paths, readers,
/// joins, aggregation, sort, and every nested range-bearing physical node are
/// retained together and rebuilt recursively on a cache hit.
#[derive(Debug)]
pub struct PreparedSelectPlan {
    current_database: String,
    table_names: Vec<(String, String)>,
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
}

/// Go's prepared-plan cache keys physical plans by the current parameter
/// types. Values with the same type rebuild one retained tree; a different
/// type gets its own physical enumeration instead of inheriting a path chosen
/// for an incompatible comparison domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedParameterType {
    Null,
    Int,
    UInt,
    Decimal,
    Real,
    Float32,
    String,
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
    /// Qualified table names read by this plan, for transaction MDL tracking.
    #[must_use]
    pub fn table_names(&self) -> &[(String, String)] {
        &self.table_names
    }

    /// The immutable PREPARE-time SELECT. Statement hints do not depend on
    /// execute-time parameter values, so resource-group selection can read
    /// this tree without cloning the cache-owned bound AST.
    #[must_use]
    pub fn select_template(&self) -> &tidb_ast::SelectStmt {
        let tidb_ast::Stmt::Query(query) = &self.statement else {
            unreachable!("a prepared SELECT plan owns a query statement")
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            unreachable!("a prepared SELECT plan owns a SELECT query")
        };
        select
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
        self.bind_inner(values, catalog, current_database, Some(ctx), environment)
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
        self.bind_inner(values, catalog, current_database, None, environment)
    }

    fn bind_inner(
        self: &Arc<Self>,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: Option<&crate::StmtContext>,
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedSelectExecution> {
        if !self.current_database.eq_ignore_ascii_case(current_database) {
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
                && entry.parameter_types == parameter_types
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
                let statement = crate::bind_prepared_statement(&self.statement, values).ok()?;
                let tidb_ast::Stmt::Query(query) = statement else {
                    return None;
                };
                let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
                    return None;
                };
                cached_plans.retain(|entry| {
                    entry.schema_version == schema_version
                        && entry.stats_version_hash == stats_version_hash
                        && entry.environment == *environment
                });
                let mut plan = super::planner_bridge::cached_select_plan(
                    &select,
                    catalog,
                    current_database,
                    ctx,
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
        if !environment.invalidate_on_fresh_stats {
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
    fn of(value: &Datum) -> Self {
        match value {
            Datum::Null => Self::Null,
            Datum::Int(_) => Self::Int,
            Datum::UInt(_) => Self::UInt,
            Datum::Decimal(_) => Self::Decimal,
            Datum::Real(_) => Self::Real,
            Datum::Float32(_) => Self::Float32,
            Datum::String(_) => Self::String,
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

    #[cfg(test)]
    pub(crate) fn aggregation_families(
        &self,
    ) -> (
        Option<super::planner_bridge::AggregationFamily>,
        Option<super::planner_bridge::AggregationFamily>,
    ) {
        let cached = self
            .cached_plan
            .lock()
            .expect("cached SELECT tree is available");
        let (_, physical) = cached
            .execution(self.generation)
            .expect("cached SELECT generation is current");
        super::planner_bridge::physical_aggregation_families(physical)
    }
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
    let scope = PlanTrace::single_table_scope(
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
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        return None;
    };
    if parsed_parameter_count(stmt) != parameter_count
        || !matches!(select.kind, tidb_ast::SelectStatementKind::Select)
        || select.sql_no_cache
    {
        return None;
    }
    let mut table_names = Vec::new();
    collect_prepared_table_names(&select.from, current_database, &mut table_names);
    let limit_parameter_orders = prepared_limit_parameter_orders(stmt);

    Some(PreparedSelectPlan {
        current_database: current_database.to_owned(),
        table_names,
        limit_parameter_orders,
        statement: stmt.clone(),
        cached_plans: std::sync::Mutex::new(Vec::new()),
    })
}

fn prepared_limit_parameter_orders(stmt: &tidb_ast::Stmt) -> Vec<usize> {
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
    from: &Option<tidb_ast::Join>,
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

    collect_join(from, current_database, names);
}

/// Executes one rebound cached physical plan. `None` means DDL invalidated
/// the template after admission.
pub fn run_prepared_select(
    execution: &PreparedSelectExecution,
    catalog: &mut Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<SelectMeta>, DriverError> {
    if execution.schema_version != catalog.metadata_version()
        || !execution
            .plan
            .current_database
            .eq_ignore_ascii_case(current_database)
    {
        return Ok(None);
    }
    let cached = match execution.cached_plan.lock() {
        Ok(cached) => cached,
        Err(_) => return Ok(None),
    };
    let Some((select, physical)) = cached.execution(execution.generation) else {
        return Ok(None);
    };
    super::physical_builder::run_cached_select(select, physical, catalog, ctx).map(Some)
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
pub(crate) fn try_fast_point_select(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<Option<SelectMeta>, DriverError> {
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
    // The ordinary source builder owns these refusal diagnostics. A fast plan
    // must not turn either unsupported clause into an ordinary current read.
    if table_ref.as_of.is_some() || table_ref.sample.is_some() || !table_ref.partitions.is_empty() {
        return Ok(None);
    }
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    let Some(entry @ TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return Ok(None);
    };
    let table = table.clone();
    let columns = entry.column_list();
    let visible = table_ref.alias.as_deref().unwrap_or(name);
    let mut scope = PlanTrace::single_table_scope(
        visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.clone(),
    );
    scope.zone = ctx.session_zone();
    let Some(output) = fast_point_output(select, &scope) else {
        return Ok(None);
    };

    // Go attempts BatchPointGet before PointGet. Its integer-handle arm
    // returns before consulting index hints, while secondary and clustered
    // indexes still pass through `indexIsAvailableByHints`.
    let mut batch = fast_batch_partition_supported(&table)
        .then(|| try_batch_point_get(select, &table, &columns, &scope.zone))
        .transpose()?
        .flatten();
    if batch.as_ref().is_some_and(|batch| !batch.ignores_hints()) {
        let hints = crate::index_hints::single_table_scan_hints(
            select,
            Some(table_ref),
            &table,
            current_db,
            ctx,
        )?;
        batch = batch.filter(|batch| batch.allowed_by(&hints));
    }
    if let Some(batch) = batch {
        let BatchPointLookup {
            handles,
            index,
            plan_rows,
            ..
        } = batch;
        let exec = HandleSourceExec::new_projected_with_context(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&output.columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handles.clone(),
            output.offsets,
            crate::kv_table::RowDecodeContext::for_query(ctx),
        );
        if let Some(trace) = trace.as_deref_mut() {
            let partitions = table.handle_partition_names(&handles, &scope.zone, ctx);
            match index {
                Some((_, index)) => trace.push_fast_index_batch_point_get(
                    source_table_name(&scope, &table.name),
                    plan_rows,
                    &partitions,
                    &index,
                    ctx.static_partition_prune(),
                    &batch_point_branch_estimates(catalog, &table, &partitions, plan_rows),
                ),
                None => trace.push_fast_batch_point_get(
                    source_table_name(&scope, &table.name),
                    &table,
                    &handles,
                    plan_rows,
                    &partitions,
                ),
            }
            trace.set_scan_act_rows(exec.produced_rows());
        }
        crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
        if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
            return Ok(Some((output.columns, Vec::new())));
        }
        let types = output
            .columns
            .iter()
            .map(|(_, field_type)| field_type.clone())
            .collect::<Vec<_>>();
        let rows = drain_executor_rows(Box::new(exec), &types, &ctx.statement_memory())?;
        return Ok(Some((output.columns, rows)));
    }

    let hints = crate::index_hints::single_table_scan_hints(
        select,
        Some(table_ref),
        &table,
        current_db,
        ctx,
    )?;
    if !hints.allows_table() {
        return Ok(None);
    }
    let Some(handle) = try_point_get(
        &PointPlanStmt::of_select(select),
        &table,
        &columns,
        &scope.zone,
    )?
    else {
        return Ok(None);
    };
    if !point_get_consumes_where(select, &table, &columns, &scope.zone) {
        return Ok(None);
    }

    let exec = HandleSourceExec::new_projected_with_context(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(&output.columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        handle.handle.clone().into_iter().collect(),
        output.offsets,
        crate::kv_table::RowDecodeContext::for_query(ctx),
    );
    if let Some(trace) = trace.as_deref_mut() {
        trace.push_fast_point_get(
            source_table_name(&scope, &table.name),
            &table,
            handle.handle.as_ref(),
        );
        trace.set_scan_act_rows(exec.produced_rows());
    }
    crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
    if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
        return Ok(Some((output.columns, Vec::new())));
    }
    let types = output
        .columns
        .iter()
        .map(|(_, field_type)| field_type.clone())
        .collect::<Vec<_>>();
    let rows = drain_executor_rows(Box::new(exec), &types, &ctx.statement_memory())?;
    Ok(Some((output.columns, rows)))
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

/// A plain column projection over an exact range can be returned directly by
/// the coprocessor read. Every clause that needs another root operator keeps
/// the ordinary pipeline for that operator to be built in the right place.
fn handle_predicate_is_consumed(
    where_clause: Option<&tidb_ast::Expr>,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    where_clause.is_some_and(|where_clause| {
        predicate_is_exact_range(where_clause)
            && crate::handle_range::build_handle_ranges(table, where_clause, zone)
                .is_some_and(|built| built.access_count > 0 && built.residual.is_empty())
    })
}

fn predicate_is_exact_range(predicate: &tidb_ast::Expr) -> bool {
    match predicate {
        tidb_ast::Expr::Paren(inner) => predicate_is_exact_range(inner),
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd | tidb_ast::BinaryOp::LogicOr,
            left,
            right,
        ) => predicate_is_exact_range(left) && predicate_is_exact_range(right),
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::Eq
            | tidb_ast::BinaryOp::NullEq
            | tidb_ast::BinaryOp::Ge
            | tidb_ast::BinaryOp::Gt
            | tidb_ast::BinaryOp::Le
            | tidb_ast::BinaryOp::Lt,
            ..,
        )
        | tidb_ast::Expr::In { .. }
        | tidb_ast::Expr::Between { .. }
        | tidb_ast::Expr::Is {
            target: tidb_ast::IsTarget::Null,
            ..
        } => true,
        _ => false,
    }
}

/// Whether every equality in a single-point `WHERE` is one of the key parts
/// that produced the handle. `try_point_get` may also be used as a narrowed
/// source when a common/unique key is pinned alongside an extra predicate;
/// that shape must retain its Selection above the source.
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

/// Whether a write's narrowed read path consumed its complete predicate.
/// Go's update/delete fast plan has no Selection in this case; a range path
/// or a point lookup with any extra equality must still evaluate the WHERE.
pub(crate) fn write_read_path_consumes_predicate(
    read_path: Option<&WriteReadPath>,
    stmt: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    match read_path {
        Some(WriteReadPath::Batch(_)) => true,
        Some(WriteReadPath::Point(_)) => point_get_predicate_is_consumed(
            stmt.where_clause,
            table,
            columns,
            zone,
            stmt.named_partitions,
        ),
        Some(WriteReadPath::Ranges(..)) => {
            handle_predicate_is_consumed(stmt.where_clause, table, zone)
        }
        Some(WriteReadPath::IndexRanges(..)) | None => false,
    }
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
            .eq_ignore_ascii_case(crate::driver::leaf_demand::EXTRA_HANDLE_NAME)
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
pub(crate) fn surviving_partitions(
    select: &tidb_ast::SelectStmt,
    table_ref: Option<&tidb_ast::TableRef>,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<(String, i64)> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let selected = table_ref
        .map(|table_ref| table_ref.partitions.as_slice())
        .filter(|names| !names.is_empty())
        .and_then(|names| {
            crate::partition_pruning::ids_for_selected_partitions(partition, names).ok()
        });
    let pruned = pruned_partition_ids(select, table, zone);
    partition
        .definitions
        .iter()
        .filter(|def| selected.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .filter(|def| pruned.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .map(|def| (def.name.clone(), def.id))
        .collect()
}

/// The partitions ONE LEAF of a multi-table `FROM` reads, named as declared
/// and in definition order.
///
/// Go's `PartitionProcessor.rewriteDataSource` walks the WHOLE logical plan
/// and divides every `DataSource` it finds, so a partitioned table inside a
/// join is fanned out exactly as a single-table `SELECT`'s is -- captured
/// over `PARTITION BY LIST (ltype)` with a predicate on a non-partitioning
/// column, where TiDB prints
/// `TableFullScan table:tx2, partition:p1` and `... partition:p2` under a
/// `PartitionUnion(Probe)`. Recognising only the single-table shape is what
/// printed one partition-less `TableFullScan table:tx2` there.
///
/// This is [`surviving_partitions`] MINUS the `WHERE` narrowing: a join
/// leaf's read is restricted by its `PARTITION (p, ...)` list alone
/// (`restricted_to_partitions` at the leaf build site), so the list named
/// here is exactly the set the leaf's executor walks. Go additionally prunes
/// the leaf by its own pushed-down conditions; doing that here would have to
/// narrow the leaf's READ in the same breath, which the index and lookup
/// arms do not yet route through one restriction point. Naming more
/// partitions than Go is a plan that over-describes the read, never one that
/// reads too few rows.
pub(crate) fn leaf_read_partitions(
    table: &KvTable,
    named_partitions: &[String],
) -> Vec<(String, i64)> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let selected = Some(named_partitions)
        .filter(|names| !names.is_empty())
        .and_then(|names| {
            crate::partition_pruning::ids_for_selected_partitions(partition, names).ok()
        });
    partition
        .definitions
        .iter()
        .filter(|def| selected.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .map(|def| (def.name.clone(), def.id))
        .collect()
}

/// The estimate each surviving partition's own `DataSource` carries, in the
/// order [`surviving_partitions`] lists them.
///
/// Go reads it from that partition's `PhysicalTableID`
/// (`stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)`),
/// which under static pruning is the only id `ANALYZE` ever stored a
/// histogram under.
pub(crate) fn surviving_partition_estimates(
    catalog: &Catalog,
    partitions: &[(String, i64)],
) -> Vec<crate::access_cost::ScanEstimate> {
    partitions
        .iter()
        .map(|(_, id)| {
            let stats = catalog.table_statistics(*id);
            crate::access_cost::ScanEstimate {
                rows: crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref)),
                pseudo: stats.is_none_or(|stats| stats.pseudo),
            }
        })
        .collect()
}

/// Whether `expr` names a scalar-subquery plan column whose eager
/// evaluation recorded a NULL (Go's `EvaluateExprWithNull` replacement).
pub(crate) fn single_point_handle(ranges: &[IndexRange]) -> Option<TableHandle> {
    let [range] = ranges else {
        return None;
    };
    if range.low_exclusive || range.high_exclusive {
        return None;
    }
    match (range.low.as_slice(), range.high.as_slice()) {
        ([Datum::Int(low)], [Datum::Int(high)]) if low == high => Some(TableHandle::Int(*low)),
        ([Datum::UInt(low)], [Datum::UInt(high)]) if low == high => {
            Some(TableHandle::Int(*low as i64))
        }
        _ => None,
    }
}

/// Installs the streaming index-range source for a committed index path, and
/// records the node `EXPLAIN` prints for it.
#[allow(clippy::too_many_arguments)]
fn batch_point_branch_estimates(
    catalog: &Catalog,
    table: &KvTable,
    partitions: &[String],
    point_count: usize,
) -> Vec<f64> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    partitions
        .iter()
        .map(|name| {
            partition
                .definitions
                .iter()
                .find(|definition| definition.name == *name)
                .and_then(|definition| catalog.table_statistics(definition.id))
                .filter(|stats| !stats.pseudo)
                .map_or(point_count as f64, |stats| {
                    let rows = crate::access_cost::realtime_row_count(Some(stats.as_ref()));
                    (point_count as f64).min(rows).max(1.0)
                })
        })
        .collect()
}

/// The schema a fast-path source emits: the scope's columns in scope order,
/// each carrying the unique id the driver's resolver hands expressions.
pub(crate) fn source_schema_columns(columns: &[(String, FieldType)]) -> Vec<Column> {
    columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect()
}

/// Offers the source only the columns the statement reads, narrowing `scope`
/// with it when the source takes the offer (Go's `rule_column_pruning.go`).
///
/// This runs BEFORE any expression is built, which is the whole point: every
/// offset below is resolved against the narrowed scope from the start, so no
/// already-built index has to be renumbered. It also runs before the predicate
/// push-down, so a pushed conjunct's `column_offset` is already in narrow
/// space -- and the kept set contains the `WHERE`'s columns because the gate
/// collected them.
///
/// No "was the source replaced?" flag is needed: `accept_column_prune`
/// defaults to refusing, so a fast-path source that cannot project simply says
/// no and the full-width path stands. Each source answers for itself,
/// fail-closed -- the same rule the pushed filter and row cap follow.
pub(crate) fn prune_scan_columns(
    select: &tidb_ast::SelectStmt,
    scope: &mut FromScope,
    from_source: &mut Option<Box<dyn Executor>>,
) {
    let Some(source) = from_source.as_mut() else {
        return;
    };
    let Some(keep) = crate::column_prune::prunable_columns(select, scope) else {
        return;
    };
    if keep.len() < scope.width()
        && source
            .table_access()
            .is_some_and(|access| access.accept_column_prune(&keep))
    {
        *scope = crate::column_prune::pruned_scope(scope, &keep);
    }
}

/// Offers the source the conjuncts it can apply itself, and reports both the
/// `WHERE` that must still run above it (`None`: the source took all of it)
/// and the physical Selection conditions the source accepted. Execution keeps
/// the original built filters inside [`PushedScanFilter`]; the returned view
/// uses the paired scan descriptions to expose Go's folded comparison
/// constants and top-level CNF without repeating conversions or warnings.
///
/// Over a single base table every source below is a real streaming scan, so
/// each answers for itself whether it can keep the promise
/// [`crate::table_access`] describes -- an index range can (it tests every row
/// it emits), a point get's handle source refuses. Only the residual then
/// needs a `Selection`; when the scan takes the whole `WHERE` there is no
/// `Selection` executor left, but the recorded plan is unchanged either way --
/// Go prints one `Selection` over the scan for both halves (captured,
/// `pkg/executor/zz_dump_pushdown_test.go`), and this tier prints no
/// `TableReader`/`cop[tikv]` task to distinguish them.
pub(crate) fn negotiate_scan_filter(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    source: &mut Box<dyn Executor>,
    ctx: &crate::StmtContext,
    access_consumed_where: bool,
    trace: Option<&mut PlanTrace>,
) -> (Option<tidb_ast::Expr>, Vec<Expression>) {
    if access_consumed_where {
        return (None, Vec::new());
    }
    match (&select.where_clause, scope.tables.len()) {
        (Some(predicate), 1) => {
            let (pushed, residual) = split_scan_predicates(predicate, &scope_resolver(scope), ctx);
            let accepted = !pushed.is_empty()
                && source
                    .table_access()
                    .is_some_and(|access| access.accept_scan_filter(&pushed, ctx));
            if accepted {
                // `TableFullScan`'s `actRows` counts rows read, not rows kept,
                // so it is taken from the scan itself rather than from the
                // (now filtered) chunks leaving it.
                if let (Some(trace), Some(scanned)) = (
                    trace,
                    source
                        .table_access()
                        .and_then(|access| access.scanned_rows_counter()),
                ) {
                    trace.set_scan_act_rows(scanned);
                }
                (residual, pushed.selection_conditions())
            } else {
                (Some(predicate.clone()), Vec::new())
            }
        }
        (where_clause, _) => (where_clause.clone(), Vec::new()),
    }
}

/// The conjuncts of `where_clause` the scan took, as written, when
/// `residual` is what it left behind.
///
/// Go never has to recover this: `expression.PushDownExprs` hands
/// `addPushedDownSelection4PhysicalTableScan` the two halves as two slices
/// (`pkg/planner/core/find_best_task.go:3205`), one becoming the cop
/// `Selection` and the other `CopTask.RootTaskConds`. This tier's driver
/// holds the residual as an AST and the pushed half only as built
/// expressions, so the AST of the pushed half -- which EXPLAIN needs to
/// PRICE the cop `Selection` (`cardinality.Selectivity` takes the
/// conditions, not the whole `WHERE`) -- is recovered by subtraction. It is
/// exact: [`split_scan_predicates`] builds `residual` by cloning conjuncts
/// out of this very list, in this order.
///
/// `None` means nothing was pushed, so there is no cop `Selection` to print.
pub(crate) fn scan_pushed_conjuncts(
    where_clause: &tidb_ast::Expr,
    residual: &tidb_ast::Expr,
) -> Option<tidb_ast::Expr> {
    let mut whole = Vec::new();
    collect_conjuncts(where_clause, &mut whole);
    let mut left_behind = Vec::new();
    collect_conjuncts(residual, &mut left_behind);
    let mut left_behind = left_behind.into_iter().peekable();
    let mut pushed: Vec<&tidb_ast::Expr> = Vec::new();
    for conjunct in whole {
        if left_behind.peek().is_some_and(|next| *next == conjunct) {
            left_behind.next();
            continue;
        }
        pushed.push(conjunct);
    }
    // A residual conjunct this walk never matched means the two lists are not
    // the ones this function documents, so it claims nothing.
    if left_behind.next().is_some() {
        return None;
    }
    pushed.into_iter().cloned().reduce(|left, right| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    })
}

/// The partitions a single-table `SELECT`'s `WHERE` proves it has to read,
/// or `None` when nothing narrows them.
///
/// The ranges come from the crate's ONE range builder
/// ([`crate::index_range::detach_cond_and_build_range_for_index`]), asked for
/// the partition expression's column exactly as it would be asked for a
/// single-column index on it. That reuse is the point: Go prunes with the
/// same `ranger` machinery it builds index ranges with, and a second range
/// implementation here would be a second answer to disagree with.
///
/// Pruning is declined -- reading everything -- in two cases, each a
/// SUPERSET and so never a wrong answer:
///
/// * a table with no partitioning;
/// * a partition expression that is not a bare COLUMN. Go prunes `year(a)`
///   through `MakePartitionByFnCol`'s monotonicity analysis, which this tier
///   does not port; a monotonicity claim that is wrong drops a partition
///   holding matching rows;
/// * a `SELECT` with no `WHERE`, which constrains nothing.
fn pruned_partition_ids(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Vec<i64>> {
    let partition = table.partition()?;
    let where_clause = select.where_clause.as_ref()?;
    let tuple_partitioning = matches!(
        partition.kind,
        crate::PartitionKind::Key
            | crate::PartitionKind::ListColumns { .. }
            | crate::PartitionKind::RangeColumns { .. }
    );
    // A bare column is the one scalar partition expression whose own value a
    // range over a column is. Tuple partitioning owns its named tuple.
    let mut range_columns = Vec::with_capacity(partition.dependencies.len());
    for dependency in &partition.dependencies {
        let column = table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(dependency))?;
        if !tuple_partitioning && partition.expr_text != format!("`{}`", column.name) {
            return None;
        }
        range_columns.push(crate::index_range::RangeColumn::whole(
            column.name.clone(),
            column.field_type.clone(),
        ));
    }
    if range_columns.is_empty() {
        return None;
    }
    // Go `PartitionProcessor.prune` runs its conditions through
    // `applyPredicateSimplification` -- whose first act is
    // `expression.PushDownNot` -- BEFORE handing them to the pruner, and its
    // own comment gives the reason: a `not (a < 5)` the ranger cannot read
    // yields no range at all, which reads here as "prune nothing" and leaves
    // the `values less than (0)` partition in a plan TiDB prunes it out of.
    let normalized = crate::partition_pruning::push_down_not(where_clause);
    // Go `DetachCondAndBuildRangeForPartition`, which is the one ranger entry
    // that does NOT convert its points to sort keys: a partition bound is a
    // written value compared under the partition column's own collation, not
    // an index's stored form.
    let built = crate::index_range::detach_cond_and_build_range_for_partition(
        &range_columns,
        &normalized,
        zone,
    )?;
    crate::partition_pruning::pruned_ids(partition, &built.ranges)
}

/// How `EXPLAIN` names one key part of an index.
///
/// An ordinary key part is the column's name. An expression index's key part
/// is the EXPRESSION, not the hidden column the DDL rewrote it into: Go
/// prints `` index:k1(`a` + 1, b) ``, and the hidden column's generated name
/// appears in no user-visible output at all. The text is the one the column
/// already stores, so the plan and `SHOW CREATE TABLE` cannot disagree.
pub(crate) fn index_key_part_name(table: &KvTable, offset: usize) -> String {
    let Some(column) = table.columns.get(offset) else {
        return String::new();
    };
    match &column.generated {
        Some(generated) if table.is_hidden(offset) => generated.expr_text.clone(),
        _ => column.name.clone(),
    }
}

/// The estimate `EXPLAIN` prints for a table read that stayed a full scan.
///
/// This is the same [`crate::access_cost`] answer the path choice used, so
/// the printed plan and the costed plan cannot disagree. A table with no
/// loaded statistics is Go's `PseudoTable`, and the estimate says so.
pub(crate) fn full_scan_estimate(
    catalog: &Catalog,
    entry: &TableEntry,
) -> crate::access_cost::ScanEstimate {
    let stats = match entry {
        TableEntry::Kv(table) => catalog.table_statistics(table.stats_physical_id()),
        // A memory table's rows are computed at query time and an
        // INFORMATION_SCHEMA view has no `mysql.stats_*` row, so there is
        // nothing to have analyzed; Go prints the pseudo constant for these
        // too.
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    };
    // The row count is real whenever a `mysql.stats_meta` row carries one,
    // even when no histogram was ever analyzed -- and in that state Go prints
    // the real count AND `stats:pseudo`. `realtime_row_count` owns the rule,
    // so this row and the cost that chose it agree by construction.
    crate::access_cost::ScanEstimate {
        rows: crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref)),
        pseudo: stats.is_none_or(|stats| stats.pseudo),
    }
}

/// `cardinality.Selectivity` for a single base table's `WHERE`.
///
/// This is what makes a `Selection` over a full scan print the estRows Go
/// prints. `None` means there is no `WHERE` to estimate, and nothing else:
/// a table with no analyzed histograms is Go's `PseudoTable`, which
/// `Selectivity` estimates through the SAME body using pseudo histograms
/// (`pkg/statistics/table.go:1034-1061` fills one per column), so routing it
/// anywhere else is what made `a = 1 and b = 2` print 10.00 against TiDB's
/// 1.00. [`crate::access_cost::selectivity`] owns both arms, and the
/// `stats:pseudo` flag stays where it was decided
/// ([`full_scan_estimate`]) -- which statistics exist is unchanged here, only
/// what is computed from them.

pub(crate) fn stats_selectivity(
    catalog: &Catalog,
    table: &KvTable,
    scope: &FromScope,
    where_clause: Option<&tidb_ast::Expr>,
) -> Option<f64> {
    stats_selectivity_with_default_string_match_selectivity(
        catalog,
        table,
        scope,
        where_clause,
        0.0,
    )
}

pub(crate) fn stats_selectivity_with_default_string_match_selectivity(
    catalog: &Catalog,
    table: &KvTable,
    scope: &FromScope,
    where_clause: Option<&tidb_ast::Expr>,
    default_string_match_selectivity: f64,
) -> Option<f64> {
    let predicate = where_clause?;
    let stats = catalog.table_statistics(table.stats_physical_id());
    Some(
        crate::access_cost::selectivity_with_default_string_match_selectivity(
            predicate,
            table,
            &scope_resolver(scope),
            stats.as_ref().map(AsRef::as_ref),
            default_string_match_selectivity,
        ),
    )
}

/// `cardinality.Selectivity` for a `SELECT`'s `WHERE` over a single base
/// table, when that table has loaded statistics.
pub(crate) fn select_stats_selectivity(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Option<f64> {
    select_predicate_stats_selectivity(
        select,
        select.where_clause.as_ref()?,
        catalog,
        current_db,
        scope,
    )
    // A predicate spanning a join has no single DataSource statistics node.
    // Go's `Selectivity` leaves it uncovered and charges the global
    // `selectionFactor` once.
    .or_else(|| (scope.tables.len() > 1).then_some(tidb_planner::cost_factors::SELECTION_FACTOR))
}

/// `cardinality.Selectivity` for one residual predicate of a single-table
/// `SELECT`. Unlike [`select_stats_selectivity`], this deliberately does not
/// re-price access conditions already represented by a range scan.
/// The `KvTable` a scan of this `SELECT` will actually read: the catalog
/// handle narrowed by an explicit `PARTITION (...)` clause and then by
/// pruning.
///
/// Go runs `PartitionProcessor` during LOGICAL optimization, so by the time
/// anything asks `Selectivity` or
/// `stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)` the
/// `DataSource` IS the surviving partition and its id names that partition.
/// Reading the catalog handle straight, as [`single_kv_table`] does, skips
/// that step -- and static pruning stores a histogram per PHYSICAL partition
/// and no merged one, so the lookup missed and a pruned scan printed
/// `stats:pseudo` over 10000 rows after `ANALYZE` had just measured two.
/// The shared planner's access lowering already narrows before its own lookup;
/// this is the same narrowing for estimate callers that build their own handle.
fn pruned_single_kv_table(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<KvTable> {
    let mut table = single_kv_table(&select.from, catalog, current_db)?;
    if table.partition().is_none() {
        return Some(table);
    }
    if let Some(table_ref) = single_table_ref(&select.from) {
        if !table_ref.partitions.is_empty() {
            let name = table_ref
                .name
                .last()
                .map(String::as_str)
                .unwrap_or(table.name.as_str());
            table =
                super::from::restricted_to_partitions(&table, &table_ref.partitions, name).ok()?;
        }
    }
    if let Some(ids) = pruned_partition_ids(select, &table, zone) {
        table.restrict_read_to_partitions(&ids);
    }
    Some(table)
}

pub(crate) fn select_predicate_stats_selectivity(
    select: &tidb_ast::SelectStmt,
    predicate: &tidb_ast::Expr,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Option<f64> {
    select_predicate_stats_selectivity_in_session(
        select, predicate, catalog, current_db, scope, 0.0,
    )
}

/// [`select_predicate_stats_selectivity`] with the session's raw
/// `tidb_default_string_match_selectivity`, which Go's `Selectivity` reads
/// for every string-match conjunct it cannot cover with statistics
/// (`pkg/planner/cardinality/selectivity.go`: `GetStrMatchDefaultSelectivity`).
pub(crate) fn select_predicate_stats_selectivity_in_session(
    select: &tidb_ast::SelectStmt,
    predicate: &tidb_ast::Expr,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
    default_string_match_selectivity: f64,
) -> Option<f64> {
    let table = pruned_single_kv_table(select, catalog, current_db, &scope.zone)?;
    stats_selectivity_with_default_string_match_selectivity(
        catalog,
        &table,
        scope,
        Some(predicate),
        default_string_match_selectivity,
    )
}

/// The loaded-statistics row count for a single-table predicate.
///
/// A decorrelated `EXISTS`/`NOT EXISTS` is a separate logical semi join in Go;
/// its preserved `DataSource` therefore owns only the ordinary local
/// predicates.  Callers that still hold the original SELECT (which also
/// contains the subquery) use this helper with the local conjuncts so the
/// semi join does not charge its `0.8` factor twice.  `None` means the source
/// is not one base table or statistics are unavailable.
pub(crate) fn select_predicate_stats_rows(
    select: &tidb_ast::SelectStmt,
    predicate: Option<&tidb_ast::Expr>,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Option<f64> {
    let table = pruned_single_kv_table(select, catalog, current_db, &scope.zone)?;
    let stats = catalog.table_statistics(table.stats_physical_id());
    let realtime = crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref));
    let selectivity = predicate
        .map(|predicate| stats_selectivity(catalog, &table, scope, Some(predicate)).unwrap_or(1.0))
        .unwrap_or(1.0);
    Some(realtime * selectivity)
}

/// The full-scan estimate and stats-backed selectivity a single-table write's
/// recorded read plan prints, resolved from the catalog by name.
pub(crate) fn single_table_trace_estimate(
    catalog: &Catalog,
    database: &str,
    name: &str,
    visible: &str,
    columns: &[(String, FieldType)],
    where_clause: Option<&tidb_ast::Expr>,
) -> (crate::access_cost::ScanEstimate, Option<f64>) {
    let Some(entry) = catalog.get_in(database, name) else {
        return (
            crate::access_cost::ScanEstimate::pseudo(crate::plan_trace::PSEUDO_ROW_COUNT),
            None,
        );
    };
    let estimate = full_scan_estimate(catalog, entry);
    let TableEntry::Kv(table) = entry else {
        return (estimate, None);
    };
    let scope = PlanTrace::single_table_scope(visible, None, columns.to_vec());
    (
        estimate,
        stats_selectivity(catalog, table, &scope, where_clause),
    )
}

/// How a single-table `UPDATE`/`DELETE` FETCHES the records it then filters.
///
/// Both arms narrow only which records are fetched. The write's own per-row
/// `WHERE` evaluation is unchanged and still decides which rows the statement
/// acts on, so the affected row set is the full scan's either way -- see
/// [`write_read_path`].
pub(crate) enum WriteReadPath {
    /// Go's `Point_Get`: one record, read by key -- carrying HOW it was
    /// pinned, because the plan prints the pin (`AccessObject`). A `None`
    /// handle is a key no row can carry, which Go also plans as a
    /// `Point_Get` that reads nothing.
    Point(PointGetPin),
    /// Go's `Batch_Point_Get`: several records read directly by their
    /// clustered or unique handles.
    Batch(Vec<TableHandle>),
    /// Go's `TableRangeScan`: the handle intervals the `WHERE` implies, and
    /// the estimate `EXPLAIN` prints for them.
    Ranges(Vec<IndexRange>, crate::access_cost::ScanEstimate),
    /// Go's `IndexRangeScan`: the id of the index the chooser preferred, the
    /// ranges of it the `WHERE` implies, and the estimate `EXPLAIN` prints. A
    /// write fetches the candidate records through the index and still filters
    /// per row above, so the ranges are a superset of the affected rows.
    IndexRanges(i64, Vec<IndexRange>, crate::access_cost::ScanEstimate),
}

/// The read a single-table `UPDATE`/`DELETE` performs to find its target
/// rows; `None` when nothing narrows it and the write reads the whole table.
///
/// Go plans a write's read from the same predicate, with the same functions,
/// as a read's. `tryUpdatePointPlan`/`tryDeletePointPlan`
/// (`pkg/planner/core/point_get_plan.go`) synthesize an `ast.SelectStmt` out
/// of the write's `TableRefs`/`Where`/`Order`/`Limit` and hand it to
/// `tryPointGetPlan` -- the SAME function a `SELECT` reaches through
/// `TryFastPlan` -- and only when that declines does the ordinary path plan a
/// `DataSource` whose table path gets its ranges from `deriveTablePathStats`
/// exactly as a `SELECT`'s does. This function is that order, and it calls
/// the same two builders the read side calls: [`try_point_get`] and
/// [`crate::handle_range`], the crate's single range algebra.
///
/// The point arm is what makes `WHERE id = 500` one key lookup instead of a
/// scan over the degenerate range `[500,500]`. A single-key range still costs
/// a range scan against storage; a key lookup does not, and that difference
/// is the whole reason Go replaces the read rather than narrowing it.
///
/// Neither arm may change the answer. A point plan is decided ONLY from
/// equalities that pin a whole key ([`try_point_get`] is Go's
/// `getNameValuePairs` rule: `AND` of `column = constant`, nothing else), the
/// key's constant is moved into the column's domain first or the plan is
/// abandoned ([`super::point_get_key`]), and the `WHERE` is still evaluated
/// per row above the fetch -- so an extra conjunct the key did not pin still
/// filters, and a key naming a row that does not exist simply reads nothing.
pub(crate) fn write_read_path(
    catalog: &Catalog,
    database: &str,
    name: &str,
    stmt: &PointPlanStmt<'_>,
    ctx: &crate::StmtContext,
) -> Result<Option<WriteReadPath>, DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return Ok(None);
    };
    // Go's order: the fast plan first, the table path only when it declines.
    // The column list is the table's own, because `try_point_get` reads it at
    // the offsets `pk_handle_offset`/`KvIndex::column_offsets` name, and those
    // are offsets into `KvTable::columns`.
    let columns: Vec<(String, FieldType)> = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    let zone = &ctx.session_zone();
    let disable_point_get = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false);
    if let Some(batch) = (!disable_point_get)
        .then(|| try_batch_point_get_stmt(stmt, table, &columns, zone))
        .transpose()?
        .flatten()
    {
        return Ok(Some(WriteReadPath::Batch(batch.into_handles())));
    }
    if let Some(handle) = (!disable_point_get)
        .then(|| try_point_get(stmt, table, &columns, zone))
        .transpose()?
        .flatten()
    {
        return Ok(Some(WriteReadPath::Point(handle)));
    }
    // Go's ordinary UPDATE/DELETE path builds one DataSource and sends it
    // through the same logical/physical search as a SELECT. The shared
    // planner is therefore the only ordinary path chooser here too; this
    // lowering merely consumes its exact scan receipt.
    let select = stmt.write_select().ok_or_else(|| {
        DriverError::unsupported("single-table write has no planner table reference")
    })?;
    let decision = super::planner_bridge::select_decision(&select, catalog, database, ctx)
        .ok_or_else(|| {
            DriverError::unsupported(format!(
                "shared planner could not plan the read for write target {name}"
            ))
        })?;
    let visible = stmt
        .table_ref
        .and_then(|table_ref| table_ref.alias.as_deref())
        .unwrap_or(name);
    let selected =
        super::planner_bridge::AccessDecision::for_leaf(&decision.access, table.table_id, visible)
            .ok_or_else(|| {
                DriverError::unsupported(format!(
                    "shared planner returned no access receipt for write target {visible}"
                ))
            })?;
    match super::leaf_access::lower_planner_access(table, visible, &columns, catalog, selected)? {
        super::leaf_access::LeafAccessPath::Table {
            ranges: Some(ranges),
            estimate,
            ..
        } => Ok(Some(WriteReadPath::Ranges(ranges, estimate))),
        super::leaf_access::LeafAccessPath::Table { ranges: None, .. } => Ok(None),
        super::leaf_access::LeafAccessPath::Index(path) => {
            let (index_id, ranges, estimate) = path.into_scan_parts();
            Ok(Some(WriteReadPath::IndexRanges(index_id, ranges, estimate)))
        }
    }
}

/// Splits a `WHERE` over one base table into the conjuncts the scan can apply
/// itself and the predicate that must stay above it.
///
/// This is Go's `rule_predicate_push_down` split narrowed to the shape the
/// bounded TiKV Selection lowering already speaks -- see
/// [`crate::predicate_pushdown`] for the rule and for why the pushed half may be
/// removed from the `Selection` only when the source promises to apply it to
/// every row, staged writes included.
///
/// The residual is the remaining conjuncts re-joined with `AND` in their
/// original order, so what runs above the scan is the `WHERE` minus exactly
/// what moved into it. `None` means every conjunct was pushed.
/// Go `constructBinaryOpFunction` (`expression_rewriter.go:413`) for the
/// one shape this tier's filter description still needs: a ROW comparison
/// `(c1, .., cn) op (v1, .., vn)` with `op` one of `>`, `>=`, `<`, `<=`
/// becomes the DNF whose `i`-th branch ANDs prefix equalities
/// `c1 = v1 .. c(i-1) = v(i-1)` with ONE comparison on the i-th elements --
/// degenerated to `>` / `<` for every branch but the last when `op` is
/// `>=` / `<=`. The three-valued logic of the expansion reproduces the row
/// comparison's own NULL semantics element for element.
///
/// `None` leaves the conjunct alone: either it is not a row comparison
/// (single-element tuples and equalities included) or its two sides are not
/// same-length element lists, which is the caller's residual path anyway.
fn expand_row_comparison(conjunct: &tidb_ast::Expr) -> Option<tidb_ast::Expr> {
    let tidb_ast::Expr::Binary(
        op @ (tidb_ast::BinaryOp::Gt
        | tidb_ast::BinaryOp::Ge
        | tidb_ast::BinaryOp::Lt
        | tidb_ast::BinaryOp::Le),
        lhs,
        rhs,
    ) = conjunct
    else {
        return None;
    };
    let left = row_elements(lhs)?;
    let right = row_elements(rhs)?;
    if left.is_empty() || left.len() != right.len() || left.len() == 1 {
        // A one-element "row" is spelled away by the parser already; anything
        // else here is the ordinary comparison paths' conjunct.
        return None;
    }
    let mut branches = Vec::with_capacity(left.len());
    for index in 0..left.len() {
        let mut conjuncts = Vec::with_capacity(index + 1);
        // Step 1.1: every PREFIX element compares equal.
        for earlier in 0..index {
            conjuncts.push(tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::Eq,
                Box::new(left[earlier].clone()),
                Box::new(right[earlier].clone()),
            ));
        }
        // Step 1.2: especially for GE/LE, every branch but the last carries
        // the strict form.
        let effective = if index < left.len() - 1 {
            match op {
                tidb_ast::BinaryOp::Ge => tidb_ast::BinaryOp::Gt,
                tidb_ast::BinaryOp::Le => tidb_ast::BinaryOp::Lt,
                other => *other,
            }
        } else {
            *op
        };
        conjuncts.push(tidb_ast::Expr::Binary(
            effective,
            Box::new(left[index].clone()),
            Box::new(right[index].clone()),
        ));
        // Step 1.3: AND the branch's sides.
        branches.push(conjuncts.into_iter().reduce(|accumulated, next| {
            tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::LogicAnd,
                Box::new(accumulated),
                Box::new(next),
            )
        }));
    }
    // Step 2: OR the branches.
    branches
        .into_iter()
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .reduce(|accumulated, next| {
            tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::LogicOr,
                Box::new(accumulated),
                Box::new(next),
            )
        })
}

/// The element list of a row value, through any parentheses. A bare
/// parenthesised expression (`(a) > (1)`) is NOT a row: the parser models it
/// as `Paren`, and only `(a, b)` builds [`tidb_ast::Expr::Row`].
fn row_elements(expr: &tidb_ast::Expr) -> Option<&[tidb_ast::Expr]> {
    match expr {
        tidb_ast::Expr::Row(items) => Some(items),
        _ => None,
    }
}
pub(crate) fn split_scan_predicates(
    where_clause: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
    ctx: &crate::StmtContext,
) -> (PushedScanFilter, Option<tidb_ast::Expr>) {
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    let mut predicates = Vec::new();
    let mut filters = Vec::new();
    let mut residual: Vec<&tidb_ast::Expr> = Vec::new();
    for conjunct in conjuncts {
        // Go's expression rewriter lowers a row comparison
        // `(c1, .., cn) op (v1, .., vn)` into the DNF of prefix equalities
        // plus one degenerate comparison BEFORE pushdown decides what the
        // coprocessor may run (`constructBinaryOpFunction`,
        // `expression_rewriter.go:413`); this tier's ranger consumes the raw
        // shape instead, but the filter description still needs the rewritten
        // form, so expand it here. A conjunct that is not a row comparison
        // keeps its own text.
        let expanded = expand_row_comparison(conjunct);
        let described = expanded.as_ref().unwrap_or(conjunct);
        // Go `find_best_task.go`'s two `expression.PushDownExprs(pctx,
        // ..., kv.TiKV)` calls, which split the index and table filters into
        // what the coprocessor may run and what stays above it.
        if !crate::pushdown_blacklist::blacklist_admits(
            described,
            resolver,
            ctx,
            tidb_expr::infer_pushdown::PushDownStore::TiKv,
        ) {
            residual.push(conjunct);
            continue;
        }
        match scan_predicate(described, resolver).and_then(|mut predicate| {
            let mut filter = rewrite_expr_resolved(described, resolver).ok()?;
            // Go `refineArgs`: `int column <cmp> non-int constant` folds the
            // constant into the column's type ONCE here, so the filter this
            // scan runs on every row compares int to int. Without it the
            // string is re-coerced per row -- the same work, and the same
            // 1292 truncation, once for each row scanned.
            let unrefined = filter.clone();
            tidb_expr::builtin_compare::refine_comparisons(&mut filter, ctx).ok()?;
            // ... and the DESCRIPTION beside it has to say the same thing:
            // Go refines before it builds the comparison at all, so the
            // constant it sends TiKV -- and prints -- is the refined one.
            crate::predicate_pushdown::adopt_refined_literals(&mut predicate, &unrefined, &filter);
            Some((predicate, filter))
        }) {
            Some((predicate, filter)) => {
                predicates.push(predicate);
                filters.push(filter);
            }
            None => residual.push(conjunct),
        }
    }
    let residual = residual.into_iter().cloned().reduce(|left, right| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    });
    (PushedScanFilter::new(predicates, filters), residual)
}

/// One conjunct as a coprocessor-describable predicate, when it is one.
///
/// The describable shapes are a column-versus-constant comparison,
/// `IS [NOT] NULL`, `[NOT] IN` over constants, and the `OR`/`NOT` composition
/// of those -- exactly the set TiKV's whitelist admits unconditionally
/// (`infer_pushdown.go`'s `scalarExprSupportedByTiKV`). `AND` is absent
/// because the caller already flattened the top-level `AND` into separate
/// conjuncts, and a nested one inside an `OR` is described by recursing into
/// the branch as its own conjunct list would not be.
fn scan_predicate(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanPredicate> {
    match conjunct {
        tidb_ast::Expr::Paren(inner) => scan_predicate(inner, resolver),
        // `NOT x` and `!x`; the arithmetic unary operators are not predicates.
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Not | tidb_ast::UnaryOp::NotKeyword, inner) => {
            Some(ScanPredicate::Not(Box::new(scan_predicate(
                inner, resolver,
            )?)))
        }
        // `x OR y`, flattened: the chain is left-associative, so flattening
        // and re-folding preserves the same disjunction.
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, ..) => {
            let mut branches = Vec::new();
            collect_disjuncts(conjunct, &mut branches);
            Some(ScanPredicate::Or(
                branches
                    .into_iter()
                    .map(|branch| scan_predicate(branch, resolver))
                    .collect::<Option<Vec<_>>>()?,
            ))
        }
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, ..) => {
            let mut branches = Vec::new();
            collect_conjuncts(conjunct, &mut branches);
            Some(ScanPredicate::And(
                branches
                    .into_iter()
                    .map(|branch| scan_predicate(branch, resolver))
                    .collect::<Option<Vec<_>>>()?,
            ))
        }
        // Only `IS [NOT] NULL`. `IS TRUE`/`IS FALSE`/`IS UNKNOWN` are separate
        // Go functions with their own signatures and their own NULL handling.
        tidb_ast::Expr::Is {
            expr,
            target: tidb_ast::IsTarget::Null,
            not,
        } => {
            let (offset, column_type) = resolve_column(expr, resolver)?;
            Some(ScanPredicate::IsNull {
                column_offset: offset,
                column_type,
                negated: *not,
            })
        }
        tidb_ast::Expr::In { expr, list, not } => {
            if list.is_empty() {
                return None;
            }
            let mut literals = Vec::with_capacity(list.len());
            for element in list {
                let (literal, literal_type) =
                    constant_value_and_type(element, &resolver.time_zone())?;
                // A NULL member makes `IN` UNKNOWN rather than false for a
                // non-matching row, and `NOT IN` UNKNOWN for every row; that
                // is not the membership test this description promises.
                if literal == Datum::Null {
                    return None;
                }
                literals.push((literal, literal_type));
            }
            // Keep the existing integer-column description unchanged. Other
            // column families continue to fail closed in the TiPB lowering.
            if let Some((offset, column_type)) = resolve_column(expr, resolver) {
                if column_type.eval_type() != tidb_datatype::EvalType::String {
                    return Some(ScanPredicate::In {
                        column_offset: offset,
                        // A non-string column compares under `binary`, and
                        // no `COLLATE` can change that; the adoption step
                        // leaves it alone.
                        collation: column_type.collation(),
                        column_type,
                        literals: literals.into_iter().map(|(value, _)| value).collect(),
                        negated: *not,
                    });
                }
            }

            // Go `inFunctionClass.getFunction` selects `InString` from the
            // tested expression, not from whether that expression is a bare
            // column. Every list item is coerced to that same evaluation type.
            let tested = scan_operand(expr, resolver)?;
            if tested.eval_type() != tidb_datatype::EvalType::String
                || literals.iter().any(|(value, field_type)| {
                    field_type.eval_type() != tidb_datatype::EvalType::String
                        || !matches!(value, Datum::String(_) | Datum::Bytes(_))
                })
            {
                return None;
            }
            Some(ScanPredicate::ScalarIn {
                // The tested expression's own collation, which is the derived
                // one whenever no argument is explicit;
                // `adopt_refined_literals` replaces it with the built
                // expression's.
                collation: match &tested {
                    tidb_expr::pushdown_catalog::PbScalar::Column { field_type, .. } => {
                        field_type.collation()
                    }
                    _ => tidb_datatype::Collation::Utf8Mb4Bin,
                },
                tested,
                literals: literals.into_iter().map(|(value, _)| value).collect(),
                negated: *not,
            })
        }
        tidb_ast::Expr::Like {
            expr,
            pattern,
            not,
            ilike: false,
            escape,
        } => {
            let (column_offset, column_type) = resolve_column(expr, resolver)?;
            if column_type.eval_type() != tidb_datatype::EvalType::String {
                return None;
            }
            let mut pattern_expr = &**pattern;
            while let tidb_ast::Expr::Paren(inner) = pattern_expr {
                pattern_expr = inner;
            }
            let pattern = match pattern_expr {
                tidb_ast::Expr::String(pattern) | tidb_ast::Expr::RawString(pattern) => {
                    pattern.as_bytes().to_vec()
                }
                _ => return None,
            };
            let predicate = ScanPredicate::Like {
                column_offset,
                // The column's, which is the derived collation whenever no
                // argument is explicit; `adopt_refined_literals` replaces it
                // with the built expression's.
                collation: column_type.collation(),
                column_type,
                pattern,
                escape: escape.unwrap_or_else(|| resolver.like_default_escape()),
            };
            Some(if *not {
                ScanPredicate::Not(Box::new(predicate))
            } else {
                predicate
            })
        }
        tidb_ast::Expr::Between {
            expr,
            low,
            high,
            not: false,
        } => {
            let (column_offset, column_type) = resolve_column(expr, resolver)?;
            let zone = resolver.time_zone();
            let (low, low_type) = comparison_constant(low, &column_type, &zone)?;
            let (high, high_type) = comparison_constant(high, &column_type, &zone)?;
            Some(ScanPredicate::And(vec![
                ScanPredicate::Compare(ScanComparison {
                    column_offset,
                    collation: column_type.collation(),
                    column_type: column_type.clone(),
                    literal_type: low_type,
                    op: ScanComparisonOp::Ge,
                    literal: low,
                    column_on_left: true,
                }),
                ScanPredicate::Compare(ScanComparison {
                    column_offset,
                    collation: column_type.collation(),
                    column_type,
                    literal_type: high_type,
                    op: ScanComparisonOp::Le,
                    literal: high,
                    column_on_left: true,
                }),
            ]))
        }
        // A builtin call, when the push-down catalog resolves a signature TiKV
        // evaluates for it. The whole `WHERE sin(a)` conjunct is then the
        // Selection condition, evaluated for truth exactly as a `Selection`
        // above the scan would evaluate it.
        _ => scan_column_comparison(conjunct, resolver)
            .map(ScanPredicate::ColumnCompare)
            .or_else(|| scan_comparison(conjunct, resolver).map(ScanPredicate::Compare))
            .or_else(|| scan_operand_call(conjunct, resolver).map(ScanPredicate::Builtin)),
    }
}

/// One argument of a described builtin call: a column of the scanned table, an
/// already-folded concrete constant (numeric, string, temporal, or JSON), or a
/// nested call the catalog also resolves.
///
/// Anything else -- a subquery, a call whose signature TiKV does not evaluate,
/// or a constant with no faithful TiPB leaf -- makes the whole conjunct
/// residual, which is Go's own rule: `scalarFuncToPBExpr` returns nil as soon
/// as one child does.
fn scan_operand(
    argument: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<tidb_expr::pushdown_catalog::PbScalar> {
    use tidb_expr::pushdown_catalog::PbScalar;
    if let tidb_ast::Expr::Paren(inner) = argument {
        return scan_operand(inner, resolver);
    }
    if let tidb_ast::Expr::Column(_) = argument {
        let (offset, field_type) = resolve_column(argument, resolver)?;
        return Some(PbScalar::Column { offset, field_type });
    }
    // A constant subtree first, so a folded literal (`DATE_FORMAT(d,
    // CONCAT('%Y','-%m'))`) is the constant Go would have folded rather than a
    // nested call. `from_expression` carries the concrete TiPB leaf encoding
    // for strings, temporal values, durations and JSON as well as integers.
    if let Some((value, field_type)) = constant_value_and_type(argument, &resolver.time_zone()) {
        return tidb_expr::pushdown_catalog::from_expression(
            &tidb_expr::expression::Expression::Constant(tidb_expr::constant::Constant::new(
                value, field_type,
            )),
        );
    }
    scan_operand_call(argument, resolver)
}

/// A builtin call as an operand, in either of the two spellings the parser
/// produces for one: an explicit `Expr::Func`, and the operator form real TiDB
/// also desugars to a named scalar function -- `MOD(a, b)` parses as the `%`
/// binary operator, and Go's `ScalarFunction` for it is named `mod` either way.
fn scan_operand_call(
    argument: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<tidb_expr::pushdown_catalog::PbScalar> {
    let (name, args): (String, Vec<&tidb_ast::Expr>) = match argument {
        tidb_ast::Expr::Func { name, args, .. } => {
            (name.to_ascii_lowercase(), args.iter().collect())
        }
        tidb_ast::Expr::Binary(op, lhs, rhs) => (
            tidb_expr::scalar_function::binary_op_name(*op).to_owned(),
            vec![lhs, rhs],
        ),
        // Go's parser lowers `candidate MEMBER OF (document)` into the plain
        // builtin call `FuncCallExpr{FnName: json_memberof}`
        // (`pkg/parser/expr_parser.go:200`), so `scan_predicate` sees it
        // through the same fallthrough every other function takes -- which is
        // why Go's cop Selection carries `json_memberof`. This tree keeps a
        // distinct node; naming the call here restores that equivalence.
        tidb_ast::Expr::MemberOf { expr, array } => ("json_memberof".to_owned(), vec![expr, array]),
        tidb_ast::Expr::TimestampDiff { .. } => {
            let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(argument, resolver).ok()?;
            return tidb_expr::pushdown_catalog::from_expression(&rewritten);
        }
        _ => return None,
    };
    // DATE_ADD/SUB carries its INTERVAL value and unit in one AST child. The
    // expression rewriter turns that pair into the generated
    // `date_add_<unit>`/`date_sub_<unit>` call before Go chooses a protobuf
    // signature; recursively scanning the raw `Expr::Interval` would lose
    // the unit and can never form a matching catalog row.
    if matches!(
        name.as_str(),
        "date_add" | "date_sub" | "adddate" | "subdate"
    ) && args.len() == 2
        && matches!(args[1], tidb_ast::Expr::Interval { .. })
    {
        let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(argument, resolver).ok()?;
        return tidb_expr::pushdown_catalog::from_expression(&rewritten);
    }
    let operands = args
        .into_iter()
        .map(|nested| scan_operand(nested, resolver))
        .collect::<Option<Vec<_>>>()?;
    tidb_expr::pushdown_catalog::build_call(&name, operands)
}

/// Flattens an `OR` chain into its branches, in source order.
fn collect_disjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_disjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, lhs, rhs) => {
            collect_disjuncts(lhs, out);
            collect_disjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The scan-input offset and declared type of `expr`, when it is a plain
/// reference to a column of the scanned table.
fn resolve_column(
    expr: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<(u32, FieldType)> {
    match expr {
        tidb_ast::Expr::Paren(inner) => resolve_column(inner, resolver),
        tidb_ast::Expr::Column(path) => {
            let (offset, column_type, _) = resolver.resolve(path)?;
            Some((u32::try_from(offset).ok()?, column_type))
        }
        _ => None,
    }
}

/// A constant expression's value and the exact type Go's expression builder
/// assigns it. Evaluating the rewritten tree also admits folded arithmetic and
/// `DATE_ADD`, rather than restricting this boundary to bare literal nodes.
fn constant_value_and_type(
    expr: &tidb_ast::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Datum, FieldType)> {
    let resolver = tidb_expr::rewriter::ZonedNoResolver::new(zone.clone());
    let rewritten = rewrite_expr_resolved(expr, &resolver).ok()?;
    let field_type = rewritten.static_type()?.clone();
    let value =
        tidb_expr::eval_expression_once(&rewritten, &tidb_expr::ZonedNoColumns(zone.clone()))
            .ok()?;
    Some((value, field_type))
}

/// One conjunct as a column-versus-constant comparison, when it is one.
fn scan_comparison(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanComparison> {
    let tidb_ast::Expr::Binary(op, lhs, rhs) = conjunct else {
        return None;
    };
    let op = ScanComparisonOp::from_ast(*op)?;
    // Go accepts the constant on either side and the protobuf preserves the
    // operand order it was written in, so the side is recorded rather than
    // normalized away.
    let (column, value, column_on_left) = match (&**lhs, &**rhs) {
        (tidb_ast::Expr::Column(path), other) => (path, other, true),
        (other, tidb_ast::Expr::Column(path)) => (path, other, false),
        _ => return None,
    };
    // A second column reference on the "constant" side leaves the shape.
    let (offset, column_type, _) = resolver.resolve(column)?;
    let zone = resolver.time_zone();
    let (literal, literal_type) = comparison_constant(value, &column_type, &zone)?;
    // A NULL constant makes the comparison unknown for every row; that is a
    // whole-predicate property Go handles in the ranger, not a filter shape.
    if literal == Datum::Null {
        return None;
    }
    Some(ScanComparison {
        column_offset: u32::try_from(offset).ok()?,
        collation: column_type.collation(),
        column_type,
        literal_type,
        op,
        literal,
        column_on_left,
    })
}

/// One conjunct as a source-ordered comparison between two scan columns.
///
/// Go's `columnToPBExpr` sends both `ColumnRef` children when the comparison
/// is supported by TiKV. The TiPB lowering applies the type-family gate;
/// refusing there keeps a comparison local when the two declared types need a
/// coercion this tier does not yet model.
fn scan_column_comparison(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanColumnComparison> {
    let tidb_ast::Expr::Binary(op, lhs, rhs) = conjunct else {
        return None;
    };
    let op = ScanComparisonOp::from_ast(*op)?;
    let (tidb_ast::Expr::Column(left), tidb_ast::Expr::Column(right)) = (&**lhs, &**rhs) else {
        return None;
    };
    let (left_offset, left_type, _) = resolver.resolve(left)?;
    let (right_offset, right_type, _) = resolver.resolve(right)?;
    Some(ScanColumnComparison {
        left_offset: u32::try_from(left_offset).ok()?,
        left_type,
        right_offset: u32::try_from(right_offset).ok()?,
        right_type,
        op,
    })
}

fn comparison_constant(
    value: &tidb_ast::Expr,
    column_type: &FieldType,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Datum, FieldType)> {
    let (mut literal, mut literal_type) = constant_value_and_type(value, zone)?;
    if column_type.code() == FieldTypeCode::NewDecimal
        && literal_type.eval_type() == tidb_datatype::EvalType::Int
    {
        // `GetAccurateCmpType` selects ETDecimal for DECIMAL versus INT, and
        // `WrapWithCastAsDecimal` folds a constant cast. Its final type is
        // refined from the resulting MyDecimal's own precision and scale.
        let decimal = match literal {
            Datum::Int(value) => tidb_datatype::Decimal::from_int(value),
            Datum::UInt(value) => tidb_datatype::Decimal::from_uint(value),
            _ => return None,
        };
        let (precision, scale) = decimal.precision_and_frac();
        literal_type = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flags(literal_type.flags())
            .with_flen(i64::from(precision))
            .with_decimal(i64::from(scale));
        literal = Datum::Decimal(decimal);
    }
    if matches!(
        column_type.code(),
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
    ) && literal_type.eval_type() == tidb_datatype::EvalType::String
    {
        // `GetAccurateCmpType` selects ETDatetime for a temporal column
        // compared with a string constant. `WrapWithCastAsTime` then builds
        // DATETIME(26,6), and constant folding leaves a MysqlTime literal.
        // When the constant does NOT fold -- `created <= 'garbage'` -- Go's
        // `RefineComparedConstant` returns the ORIGINAL constant unchanged
        // (`builtin_compare.go:1588-1597`: a non-overflow conversion error is
        // `return con, false`), and the pushed comparison carries the cast as
        // an evaluation-time `CastStringAsTime` instead. Describe exactly
        // that: the raw string literal beside the temporal column, with the
        // cast deferred to the filter expression the source evaluates.
        let target = FieldType::new(FieldTypeCode::Datetime)
            .with_flen(26)
            .with_decimal(tidb_datatype::MAX_FSP)
            .with_added_flags(tidb_datatype::FieldTypeFlags::BINARY);
        match literal.convert_to_in(&target, tidb_datatype::DEFAULT_STATEMENT_FLAGS, &zone) {
            Ok(converted) if converted.event.is_none() => {
                literal = converted.value;
                literal_type = target;
            }
            _ => {}
        }
    }
    Some((literal, literal_type))
}

/// Flattens an `AND` chain into its conjuncts.
fn collect_conjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_conjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) => {
            collect_conjuncts(lhs, out);
            collect_conjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The single TiKV-backed table a `FROM` names, when it names exactly one.
/// A point get applies only to that shape (Go `getSingleTableNameAndAlias`).
/// The one plain table a `FROM` names, when it names exactly one.
///
/// Split out of [`single_kv_table`] because the access-path decision needs the
/// REFERENCE, not just the table it resolves to: the `USE`/`FORCE`/`IGNORE
/// INDEX` hints that decide which paths exist live on the reference.
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

/// [`single_kv_table`] over [`sole_table_ref`]: the stored table a `FROM`
/// names even when a `PARTITION (...)` list narrowed it.
pub(crate) fn sole_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let table_ref = sole_table_ref(from)?;
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    }
}

pub(crate) fn single_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let table_ref = single_table_ref(from)?;
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        // A view stores no rows, so there is no point get to try.
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    }
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
    handles: Vec<TableHandle>,
    index: Option<(i64, String)>,
    common_handle: bool,
    plan_rows: usize,
}

impl BatchPointLookup {
    fn handle(handles: Vec<TableHandle>, plan_rows: usize) -> Self {
        Self {
            handles,
            index: None,
            common_handle: false,
            plan_rows,
        }
    }

    fn common_handle(handles: Vec<TableHandle>, plan_rows: usize) -> Self {
        Self {
            handles,
            index: None,
            common_handle: true,
            plan_rows,
        }
    }

    fn index(
        handles: Vec<TableHandle>,
        plan_rows: usize,
        table: &KvTable,
        columns: &[(String, FieldType)],
        index: &crate::kv_table::KvIndex,
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
            handles,
            index: Some((index.id, format!("index:{}({index_columns})", index.name))),
            common_handle: false,
            plan_rows,
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

    pub(crate) fn into_handles(self) -> Vec<TableHandle> {
        self.handles
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
                    }
                }
                return Ok(Some(BatchPointLookup::common_handle(handles, list.len())));
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
            let mut handles = Vec::with_capacity(list.len());
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
                if let Some(handle) = table
                    .lookup_unique(index.id, &key_values, zone)
                    .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
                {
                    if !handles.contains(&handle) {
                        handles.push(handle);
                    }
                }
            }
            return Ok(Some(BatchPointLookup::index(
                handles,
                list.len(),
                &table,
                columns,
                &index,
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
        let values = converted;
        let mut handles = Vec::new();
        for value in &values {
            if let Some(handle) = table
                .lookup_unique(index.id, std::slice::from_ref(value), zone)
                .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
            {
                if !handles.contains(&handle) {
                    handles.push(handle);
                }
            }
        }
        return Ok(Some(BatchPointLookup::index(
            handles,
            list.len(),
            &table,
            columns,
            &index,
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
    fn write_select(&self) -> Option<tidb_ast::SelectStmt> {
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
    /// The resolved record handle (`None` = a key no row can carry; Go still
    /// plans the `Point_Get` and reads nothing).
    pub(crate) handle: Option<TableHandle>,
    /// The UNIQUE INDEX that pinned the row, when one did: its name and its
    /// column names, in index order. `None` is the handle pin.
    pub(crate) index: Option<(String, Vec<String>)>,
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
            .eq_ignore_ascii_case(crate::driver::leaf_demand::EXTRA_HANDLE_NAME)
    {
        let handle_type = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let Some(value) = point_get_value(&handle_type, &pairs[0].value) else {
            return Ok(None);
        };
        return Ok(Some(PointGetPin {
            handle: match value {
                Datum::Int(value) => Some(TableHandle::Int(value)),
                Datum::UInt(value) => Some(TableHandle::Int(value as i64)),
                _ => return Ok(None),
            },
            index: None,
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
                handle: match &pairs[0].value {
                    Datum::Int(value) => Some(TableHandle::Int(*value)),
                    Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                    // Unreachable: the conversion above has already put the
                    // value in the handle column's integer domain or refused
                    // the plan.
                    _ => return Ok(None),
                },
                index: None,
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
            let encoded = tidb_codec::encode_key_in_timezone(zone, &values)
                .map_err(|e| DriverError::Parse(format!("common handle encode failed: {e:?}")))?;
            let handle = tidb_txnkv::CommonHandle::new(encoded)
                .map_err(|e| DriverError::Parse(format!("common handle build failed: {e:?}")))?;
            // A clustered common handle IS the record key, so it prints as
            // a handle plan, not an index one.
            return Ok(Some(PointGetPin {
                handle: Some(TableHandle::Common(handle.encoded().to_vec())),
                index: None,
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    let mut table = table.clone();
    for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
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
        let handle = table
            .lookup_unique(index.id, &values, zone)
            .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?;
        // Go `PointGetPlan.AccessObject` prints the pinning index --
        // `table:t, index:idx(cols)` -- and no handle, even though execution
        // resolved one through the index entry.
        let index_columns = index
            .column_offsets
            .iter()
            .filter_map(|offset| table.columns.get(*offset))
            .map(|column| column.name.clone())
            .collect();
        return Ok(Some(PointGetPin {
            handle,
            index: Some((index.name.clone(), index_columns)),
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

#[cfg(test)]
mod access_tests {
    use super::*;

    #[test]
    fn point_get_rejects_either_open_endpoint() {
        let closed = IndexRange {
            low: vec![Datum::Int(7)],
            high: vec![Datum::Int(7)],
            low_exclusive: false,
            high_exclusive: false,
        };
        assert_eq!(
            single_point_handle(std::slice::from_ref(&closed)),
            Some(TableHandle::Int(7))
        );

        let mut low_open = closed.clone();
        low_open.low_exclusive = true;
        assert_eq!(single_point_handle(&[low_open]), None);

        let mut high_open = closed;
        high_open.high_exclusive = true;
        assert_eq!(single_point_handle(&[high_open]), None);
    }

    fn expand(sql: &str) -> Option<tidb_ast::Expr> {
        let statement = tidb_parser::parse(sql).expect("query parses");
        let Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let QueryStmt::Select(select) = &*query else {
            panic!("expected select")
        };
        let where_clause = select.where_clause.as_ref()?;
        let mut conjuncts = Vec::new();
        collect_conjuncts(where_clause, &mut conjuncts);
        conjuncts
            .iter()
            .find_map(|conjunct| expand_row_comparison(conjunct))
    }

    fn where_clause_of(sql: &str) -> Option<tidb_ast::Expr> {
        let statement = tidb_parser::parse(sql).expect("query parses");
        let Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let QueryStmt::Select(select) = &*query else {
            panic!("expected select")
        };
        select.where_clause.clone()
    }

    /// The parser keeps explicit parentheses; the expansion builds its DNF
    /// without them. Parentheses are transparent, so strip them (recursively)
    /// before two shapes are compared.
    fn strip_parens(expr: &tidb_ast::Expr) -> tidb_ast::Expr {
        match expr {
            tidb_ast::Expr::Paren(inner) => strip_parens(inner),
            tidb_ast::Expr::Binary(op, lhs, rhs) => tidb_ast::Expr::Binary(
                *op,
                Box::new(strip_parens(lhs)),
                Box::new(strip_parens(rhs)),
            ),
            other => other.clone(),
        }
    }

    #[test]
    fn a_row_comparison_expands_to_go_prefix_equality_dnf() {
        // Go `constructBinaryOpFunction`'s default arm, spelled out for the
        // taobench batch read's leading shape.
        let expanded = expand("SELECT * FROM t WHERE (a, b, c) > (1, 2, 3)")
            .expect("a row comparison expands");
        let expected = strip_parens(
            &where_clause_of(
                "SELECT * FROM t WHERE a > 1 OR (a = 1 AND b > 2) OR \
                 (a = 1 AND b = 2 AND c > 3)",
            )
            .expect("the expected shape parses"),
        );
        assert_eq!(strip_parens(&expanded), expected);
    }

    #[test]
    fn a_ge_row_comparison_degenerates_every_branch_but_the_last() {
        let expanded =
            expand("SELECT * FROM t WHERE (a, b) >= (1, 2)").expect("a row comparison expands");
        let expected = strip_parens(
            &where_clause_of("SELECT * FROM t WHERE a > 1 OR (a = 1 AND b >= 2)")
                .expect("the expected shape parses"),
        );
        assert_eq!(strip_parens(&expanded), expected);

        // A one-element "row" is not a row comparison; equality and other
        // operators keep their own paths.
        assert!(expand("SELECT * FROM t WHERE (a, b) = (1, 2)").is_none());
        assert!(expand("SELECT * FROM t WHERE a > 1").is_none());
    }
}
