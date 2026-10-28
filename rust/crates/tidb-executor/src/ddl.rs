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

//! `CREATE TABLE` execution: builds a `tidb_model::TableInfo` from the parsed
//! statement and registers a TiKV-byte-backed table for it -- tying the
//! transcreated metadata structs into the runnable path.
//!
//! This is the metadata slice of Go's `pkg/ddl` `buildTableInfo` /
//! `buildColumnAndConstraint`: column types map through
//! [`column_field_type::build_field_type`], and charset/collation through
//! [`column_types`]' transcreation of Go `ResolveCharsetCollation`.
//! DEFERRED (documented): the
//! schema-version/DDL-job machinery (the driver applies metadata directly; the
//! DDL job queue is a separate tier). Constraints/indexes and the column
//! options are no longer deferred; see [`crate::column_default`] for what a
//! `DEFAULT` may be.
//!
//! # `CREATE TABLE ... LIKE` copies the source's `TableInfo`
//!
//! Go `BuildTableInfoWithLike` copies the source's `TableInfo` (`Meta()`)
//! and resets the fields that identify rather than describe it;
//! [`create_table_in_catalog`] runs the DDL package's port of it
//! (`tidb_ddl_build::table_info_build::build_table_info_with_like`) and
//! loads the copy with `TableFromMeta` like any other new table.
//!
//! # One builder, one `buildIndexColumns`
//!
//! [`create_table_in_catalog`] builds the table with the DDL package's own
//! `BuildTableInfoWithStmt` (`tidb_ddl_build::table_info_build`) and loads it
//! back with `TableFromMeta`, so this tier and the cluster tier build the same
//! `TableInfo` from the same statement. Every key part -- CREATE TABLE, ADD
//! INDEX here and in the cluster, the hypothetical index -- is judged by the
//! one `buildIndexColumns` port ([`index::build_index_columns`]).
//!
//! # `PARTITION BY HASH`, `BY RANGE`, and `BY RANGE COLUMNS` are REAL here
//!
//! This builder once skipped `CreateTableStmt::partitioning` entirely, so a
//! partitioned `CREATE TABLE` SUCCEEDED and produced an UNPARTITIONED table --
//! a wrong table rather than a missing feature. It then refused every method,
//! which was honest but empty. It now BUILDS real HASH and RANGE
//! partitionings: [`table_partition::build_table_partitioning`] allocates one
//! physical table id per partition and validates the clause the way Go does,
//! the rows are stored under those ids (see [`crate::partition_routing`]),
//! a `WHERE` on the partition expression PRUNES which of them a scan reads
//! (see [`crate::partition_pruning`]), and `SHOW CREATE TABLE` prints the
//! clause back verbatim.
//!
//! KEY is still refused, because
//! this tier can neither route a row into one of their partitions nor prune
//! them, and each carries validation of its own that would start silently
//! passing the moment the clause was accepted. See [`table_partition`]'s
//! module doc.
//!
//! Refusing was itself a cascade decision, taken deliberately: it moved
//! `table not found in catalog` up sharply, because those tables honestly did
//! not exist. Accepting routed methods gives that back for those tables.
//!
//! # What folds at DDL time, and what therefore needs the session's context
//!
//! [`run_create_table_in`] RE-PARSES the statement text the session already
//! parsed, and it settles two things by EVALUATING an expression: a column
//! `DEFAULT` and a RANGE partition bound. Both take the session's
//! [`crate::StmtContext`], because a folded value can read `@@time_zone`.
//!
//! The rest of this statement's `CREATE`-time features were AUDITED against
//! real TiDB for the same hole, by issuing one `CREATE TABLE` from two
//! sessions differing only in `@@time_zone` and reading the stored definition
//! back. The audit is recorded here so the next reader does not redo it:
//!
//! * **Partition bounds -- AFFECTED.** `VALUES LESS THAN
//!   (UNIX_TIMESTAMP('2020-01-03 15:10:00'))` stores `1578064200` under
//!   `+00:00` and `1578035400` under `+08:00`. This is the case the threading
//!   exists for.
//! * **Expression `DEFAULT`s -- NOT affected.** A temporal literal default is
//!   stored VERBATIM in both zones (`ts timestamp default '2020-01-01
//!   00:00:00'` reads back identically under `+00:00` and `+08:00`), and a
//!   FUNCTION-CALL default takes Go's whitelist route (3770) rather than the
//!   folder, so no zone-reading function can reach the fold at all. The
//!   context is passed to that fold anyway, because it is the same
//!   `EvalSimpleAst` Go passes its own `BuildContext` to.
//! * **Generated columns -- AFFECTED. This entry used to say the opposite and
//!   the opposite was wrong.** Go's `checkIllegalFn4Generated` does reject
//!   every name in `expression.IllegalFunctions4GeneratedColumns`, and
//!   `UNIX_TIMESTAMP`/`NOW`/`CURDATE`/`CURTIME` are on it -- captured, `create
//!   table g (a timestamp, b bigint as (unix_timestamp(a)))` is an ERROR in
//!   both zones. But that list is a list of FUNCTION NAMES, and a temporal
//!   LITERAL is not on it: `b datetime as (timestamp '2020-01-01
//!   10:00:00+00:00')` is ACCEPTED, and its value moves with the reading
//!   session. So a zone-dependent generated expression not only can exist,
//!   one is trivially written.
//!
//!   It does not fold wrongly at DDL time, though, which is why this entry
//!   sat under the DDL-zone audit and read as closed: Go keeps the AST and
//!   REWRITES it per statement, so the DDL zone never reaches the value at
//!   all. The bug was on the READ path, and is fixed in
//!   [`crate::generated_column`] (`GeneratedColumn::source`).
//! * **Expression indexes -- NOT affected.** Go refuses a zone-reading
//!   expression there outright: `key idx((timestamp '...'))` is
//!   `[ddl:8200] Unsupported creating expression index containing unsafe
//!   functions without allow-expression-index in config` (captured), because
//!   `timestampliteral` is not on `variable.GAFunction4ExpressionIndex`.
//!   [`crate::expression_index`] ports that half of the gate.
//! * **`CHECK` constraints -- NOT affected.** They are DISCARDED at DDL time
//!   under the stock `tidb_enable_check_constraint = OFF`; see
//!   [`run_create_table_in`].
//! * **`tidb_exec::table_info_build`, the OTHER builder -- NOT affected.** It
//!   refuses `PARTITION BY` outright and its `set_default_value` accepts only
//!   a LITERAL or a temporal marker (`CURRENT_TIMESTAMP` / `CURRENT_DATE`),
//!   never folding an expression, so it has no DDL-time evaluation to give a
//!   zone to.
//!
//! One zone-dependent DDL-time behaviour the audit found -- TIMESTAMP
//! `DEFAULT` RANGE validation -- is CLOSED. `ts timestamp default
//! '1970-01-01 00:30:00'` is accepted under `+00:00` and `-08:00` and is
//! 1067 under `+08:00` (it falls below the epoch once converted);
//! `'2038-01-19 03:14:07'` is the mirror image, accepted under
//! `+00:00`/`+08:00` and 1067 under `-08:00`. The stored VALUE is the
//! literal either way; it is the ACCEPTANCE that moves, so the bug it was
//! was a wrong-ACCEPT rather than a wrong-value.
//!
//! It was never the storage seam -- storing a TIMESTAMP in UTC and reading
//! it back in the reader's zone (`tests_timezone_storage` in `tidb-session`)
//! left it untouched, because the range check lives one layer up. Go's
//! `checkTimestampType` converts the literal out of `types.Context.Location()`
//! into UTC before comparing against `MinTimestamp`/`MaxTimestamp`, and
//! `Datum::convert_to_time_target` validated against a hardcoded `Utc`. It
//! now takes the zone: [`normalize_column_default`] passes the statement's
//! own, as does the write path's `cast_value_for_column`, which had the
//! identical gap (`SET time_zone='-08:00'; INSERT ... VALUES
//! ('2038-01-19 03:14:07')` is 1292). `tests_timestamp_range` in
//! `tidb-session` holds the captures, the DST cases, and the read-side
//! asymmetry that keeps the bound OFF the read path.
//!
//! # Where a resolved collation is, and is NOT, consulted
//!
//! DDL-time resolution is complete: every string column carries its real
//! charset and collation, and every metadata surface (`SHOW FULL COLUMNS`,
//! `SHOW CREATE TABLE`, `SHOW TABLE STATUS`, `information_schema.columns`)
//! reports them, so a `VARBINARY` is distinguishable from a `VARCHAR`. The
//! write path validates a column's bytes against its charset, so a non-UTF-8
//! string is rejected by a `utf8mb4` column and accepted by a binary one.
//!
//! Expression-level derivation is complete too: `tidb_expr::collation_derive`
//! transcreates Go's `CheckAndDeriveCollationFromExprs`/`deriveCollation`, so a
//! comparison's collation is aggregated from its operands by coercibility
//! (EXPLICIT `COLLATE` > IMPLICIT column > SYSCONST > COERCIBLE literal >
//! NUMERIC > IGNORABLE `NULL`) and stamped on the function's result type, which
//! is where the comparer, `LIKE`, `IN`, `INSTR`/`LOCATE`/`STRCMP`, and the
//! sort/group key comparers all read it from. The previously documented
//! byte-wise divergences are GRADUATED and covered by
//! `tidb-session`'s `tests_collation`: `_ci` `=`/`<>`/`<`/`IN`/`BETWEEN`/
//! `LIKE`/`ORDER BY`/`GROUP BY`, `binary`'s NO PAD against `utf8mb4_bin`'s PAD
//! SPACE, the collation-aware string builtins, and the exact 1267/1271
//! "Illegal mix of collations" and 1253 charset-mismatch texts.
//!
//! The non-Unicode charsets are GRADUATED too, and covered by
//! `tidb-session`'s `tests_charset`. A `gbk`/`gb18030` column validates its
//! writes against the charset (1366 for an unrepresentable character),
//! defaults to `gbk_chinese_ci`/`gb18030_chinese_ci` and orders by those
//! weights, and transcodes at exactly the boundary Go transcodes at -- the
//! implicit `to_binary` wrap on a binary-aware function's argument, which is
//! why `HEX`/`LENGTH`/`ASCII`/`CAST(... AS BINARY)` report the GBK form while
//! the stored bytes stay UTF-8. `CONVERT(x USING cs)` retags and
//! `?`-replaces. `latin1` and `ascii` need no transcode at all (Go's
//! `isLegacyCharset`); see `tidb_expr::convert_charset` for the whole seam
//! and its captured evidence.
//!
//! DIVERGENCE (documented, captured, and NOT yet fixed) in what remains:
//!
//! * A `_charset'literal'` introducer (`_gbk'...'`) is a 1064 PARSE error
//!   here; TiDB answers 1115 "Unsupported character introducer: 'gbk'" from
//!   its own parser, so both refuse the form but under different codes. An
//!   unknown collation spelled in `COLLATE` is likewise 1064 here against
//!   TiDB's `ddl:1273 Unknown collation: '...'`.
//! * Go's `from_binary` half of `HandleBinaryLiteral` (a binary argument
//!   flowing into a non-binary string result) is not wired: it needs the
//!   DERIVED result charset of the whole function, and no captured case
//!   reaches it.
//! * `deriveCollation`'s `DATE_FORMAT`/`TIME_FORMAT`, `CASE` (Go's own comment
//!   marks its aggregation incorrect), `FIELD`, and `CAST`-to-string arms fall
//!   into the default arm here, which gives them the connection collation
//!   rather than an aggregated one.
//! * A comparison between a string and a NUMBER promotes to REAL and consults
//!   no collation (matching Go), so no collation-mismatch error can arise
//!   there even when TiDB's own planner would have rewritten the expression.

#[allow(unused_imports)]
pub use tidb_ddl_build::{ttl_info_from_options};

pub use tidb_ddl_build::modify_column_type::check_modify_types;
/// AUTO_RANDOM declaration validation shared by local and cluster DDL.
pub use tidb_ddl_build::{check_too_long_identifier, create_table_ttl_info, table_affinity, validate_comment_length, validate_table_affinity, validate_temporary_table_create, CommentOwner, MAX_IDENTIFIER_LENGTH};
pub use tidb_ddl_build::auto_random;
pub use tidb_ddl_build::check_constraint;
pub use tidb_ddl_build::column_field_type;
pub use tidb_ddl_build::column_types;
pub use tidb_ddl_build::constraint_names;
pub use tidb_ddl_build::index;
mod hypo_index;
mod indexes;
pub use tidb_ddl_build::modify_column_type;
#[cfg(test)]
mod tests_modify_column_type;
pub use tidb_ddl_build::index::{check_and_build_index_condition_string, check_create_global_index, check_expression_index_auto_increment, check_index_condition, get_name_for_anonymous_index, IndexConditionColumn, IndexedTableColumn};
pub use tidb_ddl_build::partition_interval;
pub use tidb_ddl_build::split_region::{normalize_split_policy, SplitPolicyTarget};
pub use tidb_ddl_build::storage_class;
pub use tidb_ddl_build::table_partition;
#[cfg(test)]
mod tests_table_partition;
pub use tidb_ddl_build::table_partition_list;
pub use tidb_ddl_build::table_partition_range;

pub use tidb_ddl_build::column_defaults::{check_column_default_value, normalize_column_default, settle_column_default, validate_column_default, SettledColumnDefault};

pub use table_partition::{append_partition_defs, build_partition_metadata, escape_identifier, linear_partitioning_warning, partition_placement_text, partition_spec_from_metadata, StoredPartitionDefinition, StoredPartitionMetadata};

pub use hypo_index::{set_advisor_hypo_index, HypoIndexMap, HypoIndexes, HypoTiFlashReplicas};
pub use indexes::{index_backfill_error};
pub use tidb_ddl_build::column_types::is_int_column_type;

pub use tidb_ddl_build::table_lifecycle::split_drop_table_targets;

use crate::driver::{Catalog, DriverError};
use tidb_ast::{DdlStmt, Stmt};

/// Go `CREATE TABLE` through its job, in one process: the table built from
/// the statement (`BuildTableInfoWithStmt`), the ids the job draws
/// (`assignGIDsForJobs`, or a bootstrap table's reserved metadef id), and the
/// table loaded back from that metadata (`TableFromMeta`), registered in
/// `catalog`. Returns whether a table was created (`false` only for `IF NOT
/// EXISTS` over an existing name).
pub fn create_table_in_catalog(
    create: &tidb_ast::CreateTableStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    reserved_table_id: Option<i64>,
) -> Result<bool, DriverError> {
    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    let Some(charset) = catalog.database_charset(&database) else {
        return Err(DriverError::from(
            tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
                .generate_with_stack_by_args(&[(database).into()]),
        ));
    };
    if catalog.contains_in(&database, &name) {
        let exists = DriverError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_EXISTS
                .generate_with_stack_by_args(&[(format!("{database}.{name}")).into()]),
        );
        if create.if_not_exists {
            ctx.append_suppressed(&exists);
            return Ok(false);
        }
        return Err(exists);
    }
    let refused = |error: tidb_ddl_build::table_info_build::DdlAdmissionError| {
        DriverError::Mysql(error.to_mysql_error())
    };
    let mb_ctx = new_metabuild_context_with_sctx(ctx);
    let mut info = match &create.like_table {
        // Go `CreateTable` with `ReferTable`: the preprocessor's checks on
        // the source, then `BuildTableInfoWithLike` over `referTbl.Meta()`.
        Some(source) => {
            let (source_database, source_name) =
                crate::driver::split_table_path_pub(source, current_db)?;
            let not_exists = || {
                DriverError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[
                            (&source_database).into(),
                            (&source_name).into(),
                        ]),
                )
            };
            let refer = match catalog.table_in(source_database, source_name) {
                Some(crate::TableEntry::Kv(table)) => table.meta(),
                // A view or sequence reaches `BuildTableInfoWithLike`'s
                // `ErrWrongObject`, which names the TARGET schema.
                Some(crate::TableEntry::View(_) | crate::TableEntry::Sequence(_)) => {
                    return Err(DriverError::from(
                        tidb_error::catalog::infoschema::ERR_WRONG_OBJECT
                            .generate_with_stack_by_args(&[
                                (&database).into(),
                                (&source_name).into(),
                                ("BASE TABLE").into(),
                            ]),
                    ));
                }
                Some(crate::TableEntry::Mem(_)) => {
                    return Err(DriverError::unsupported(
                        "CREATE TABLE ... LIKE a fixture table with no TableInfo",
                    ));
                }
                None => return Err(not_exists()),
            };
            let refer = refer.read();
            tidb_ddl_build::check_create_table_like_source(&refer, create.temporary)
                .map_err(DriverError::Mysql)?;
            tidb_ddl_build::table_info_build::build_table_info_with_like(
                &database,
                &name,
                &refer,
                create.temporary,
                create.on_commit_delete,
            )
            .map_err(refused)?
        }
        None => tidb_ddl_build::table_info_build::build_table_info_in_schema(
            create,
            &database,
            charset.charset.name(),
            charset.collation.name(),
            &mb_ctx,
            ctx.enable_check_constraint(),
        )
        .map_err(refused)?,
    };
    // Go `CreateTable`: `checkTableInfoValidWithStmt` and the region split
    // policies, then `createTableWithInfoJob`'s `checkTableInfoValidExtra`.
    tidb_ddl_build::table_info_build::check_table_info_valid_with_stmt(&mb_ctx, &info)
        .map_err(refused)?;
    tidb_ddl_build::table_info_build::set_create_table_split_policies(create, &mut info)
        .map_err(refused)?;
    tidb_ddl_build::table_info_build::check_table_info_valid_extra(
        &mut |warning| mb_ctx.append_warning(warning),
        &info,
    )
    .map_err(refused)?;
    info.id = reserved_table_id.unwrap_or_else(fixture_global_id);
    if let Some(partition) = info.partition.as_ref() {
        let count = partition.read().definitions.len();
        for index in 0..count {
            let id = fixture_global_id();
            partition
                .read()
                .definitions
                .update(index, |definition| definition.id = id);
        }
    }
    info.state = tidb_model::SchemaState::PUBLIC;
    let db_id = catalog.database_id(&database).unwrap_or_default();
    let auto_ids = tidb_ddl_build::table_from_meta::LocalTableAutoIds::default();
    let table = tidb_ddl_build::table_from_meta::cluster_table(
        &info,
        &crate::storage::MemTableStorage::new(),
        &tidb_ddl_build::table_from_meta::AutoIdSource::In { db_id, ids: &auto_ids },
    )
    .map_err(DriverError::unsupported)?;
    catalog.register_kv_in(&database, &name, table)?;
    Ok(true)
}

/// The ids a table built with no store behind it takes: Go's tests over
/// `MockInfoSchema` number their tables themselves, a stored table's come
/// from meta's `GenGlobalID`. One process-wide sequence, so two fixtures'
/// tables never share an id.
fn fixture_global_id() -> i64 {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
}

/// Parses and executes a `CREATE TABLE`, building a [`TableInfo`] and
/// registering a TiKV-byte-backed table in `catalog`. Returns whether a table
/// was created (`false` only for `IF NOT EXISTS` over an existing name).
pub fn run_create_table_on(sql: &str, catalog: &mut Catalog) -> Result<bool, DriverError> {
    // A stock session's context: UTC, the SHIPPED strict `sql_mode` and
    // `tidb_enable_clustered_index` ON (`StmtContext`'s derived `Default`
    // is non-strict and leaves clustering at Go's zero value, OFF).
    run_create_table_on_with(
        sql,
        catalog,
        tidb_executor_default_database(),
        &crate::StmtContext::for_query(),
    )
}

/// [`run_create_table_on`] in `current_db` under `ctx`: the session settings
/// (SQL mode, time zone, clock, `tidb_enable_clustered_index`) the statement
/// is built with.
pub fn run_create_table_on_with(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let stmt = tidb_parser::parse(sql)
        .map_err(|e| DriverError::from(tidb_util::misc::syntax_error(format!("{e:?}"))))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    let DdlStmt::CreateTable(create) = &**ddl else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    // Go's statement path: `Compile`'s preprocessor (`checkCreateTableGrammar`)
    // before the DDL executor.
    tidb_planner::preprocess::check_ddl_grammar(&stmt, ctx.strict()).map_err(DriverError::Mysql)?;
    create_table_in_catalog(create, catalog, current_db, ctx, None)
}

/// Every session setting `CREATE TABLE` reads, as one named value.
///
/// These arrive together because they are one thing -- the state of the
/// session issuing the statement -- and passing them as loose positional
/// arguments made two unrelated `bool`s adjacent, where a swapped call site
/// would compile and quietly build a different table. Naming them also puts
/// the whole list in one place, so the next setting Go reads here is an
/// added FIELD, which every construction site must supply, rather than an
/// added parameter that a caller can be forgiven for defaulting.
#[derive(Debug, Clone, Copy)]
pub struct CreateTableSettings {
    /// The session's scanner `sql_mode`: this entry RE-PARSES text the
    /// session already parsed, so without it a double-quoted name would mean
    /// one thing to the session and another here.
    pub sql_mode: tidb_parser::SqlMode,
    /// The session's parser switches (Go `SessionVars.BuildParserConfig`),
    /// for the same re-parse: `DOUBLE(10)` is a syntax error only under
    /// `tidb_enable_strict_double_type_check`.
    pub parser_config: tidb_parser::ParserConfig,
    /// `@@foreign_key_checks`: with it off, Go stores a foreign key as
    /// written instead of resolving it against the referenced table.
    pub foreign_key_checks: bool,
    /// `@@tidb_enable_check_constraint`, GLOBAL-scope and OFF by default;
    /// see this function's doc for what OFF makes of a `CHECK`.
    pub enable_check_constraint: bool,
    /// The session's `@@tidb_enable_clustered_index`. Go reads it off
    /// `SessionVars` in `metabuild.go` and hands it to `BuildTableInfo`; it
    /// decides the table's ROW ENCODING and HANDLE SEMANTICS, not a plan, so
    /// it has to be the session's own value and never a default assumed here.
    pub clustered_index_mode: tidb_vardef::modes::ClusteredIndexDefMode,
}

impl Default for CreateTableSettings {
    /// A stock session: `@@foreign_key_checks` ON,
    /// `@@tidb_enable_check_constraint` OFF, and
    /// `@@tidb_enable_clustered_index` at Go's
    /// `DefTiDBEnableClusteredIndex` = `ClusteredIndexDefModeOn`
    /// (`pkg/sessionctx/vardef/tidb_vars.go`), which is also the value
    /// `sysvar.go` registers. A stock session clusters every primary key it
    /// can, not only integer ones.
    fn default() -> Self {
        Self {
            sql_mode: tidb_parser::SqlMode::default(),
            parser_config: tidb_parser::ParserConfig::default(),
            foreign_key_checks: true,
            enable_check_constraint: false,
            clustered_index_mode: tidb_vardef::modes::ClusteredIndexDefMode(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_CLUSTERED_INDEX,
            ),
        }
    }
}

/// The default schema an unqualified `CREATE TABLE` lands in.
fn tidb_executor_default_database() -> &'static str {
    crate::driver::DEFAULT_DATABASE
}

/// A memory schema's table built from its `CREATE TABLE` text, as Go's
/// `perfschema.Init` builds it: `BuildTableInfoFromAST` under
/// `metabuild.NewNonStrictContext()` (so `DEFAULT '0000-00-00 ...'` is
/// admitted), the reserved `table_id`, column ids numbered from 1, then
/// loaded like any table (`TableFromMeta`) and registered in `database`.
pub fn run_create_system_table_in(
    sql: &str,
    catalog: &mut Catalog,
    database: &str,
    table_id: i64,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql)
        .map_err(|e| DriverError::from(tidb_util::misc::syntax_error(format!("{e:?}"))))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    let DdlStmt::CreateTable(create) = &**ddl else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    let mut info = tidb_ddl_build::table_info_build::build_table_info(
        create,
        tidb_mysql::charset::DefaultCharset,
        "",
    )
    .map_err(|error| DriverError::Mysql(error.to_mysql_error()))?;
    info.id = table_id;
    for (offset, column) in info.columns.iter_deref().enumerate() {
        column.write().id = i64::try_from(offset).unwrap_or(0) + 1;
    }
    info.db_id = catalog.database_id(database).unwrap_or_default();
    info.state = tidb_model::SchemaState::PUBLIC;
    let auto_ids = tidb_ddl_build::table_from_meta::LocalTableAutoIds::default();
    let table = tidb_ddl_build::table_from_meta::cluster_table(
        &info,
        &crate::storage::MemTableStorage::new(),
        &tidb_ddl_build::table_from_meta::AutoIdSource::In { db_id: info.db_id, ids: &auto_ids },
    )
    .map_err(DriverError::unsupported)?;
    let name = info.name.original().to_owned();
    catalog.register_kv_in(database, &name, table)
}

/// Go `NewMetaBuildContextWithSctx`: the metadata-build context a DDL
/// statement's builder runs under, from the statement's context.
#[must_use]
pub fn new_metabuild_context_with_sctx(ctx: &crate::StmtContext) -> tidb_expr::metabuild::Context {
    use tidb_expr::metabuild::{
        with_clustered_index_def_mode, with_enable_auto_increment_in_generated, with_expr_ctx,
        with_info_schema, with_pre_split_regions, with_primary_key_required,
        with_shard_row_id_bits, Context, MetaOnlyInfoSchema,
    };
    Context::new([
        with_expr_ctx(std::sync::Arc::new(ctx.clone())),
        with_enable_auto_increment_in_generated(ctx.auto_increment_in_generated()),
        with_primary_key_required(ctx.primary_key_required()),
        with_clustered_index_def_mode(ctx.clustered_index_def_mode()),
        with_shard_row_id_bits(ctx.shard_row_id_bits()),
        with_pre_split_regions(ctx.pre_split_regions()),
        with_info_schema(
            ctx.latest_index_schema()
                .map(|schema| schema as std::sync::Arc<dyn MetaOnlyInfoSchema>),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{run_insert_on, run_select_on};
    use tidb_datatype::Datum;
    use tidb_datatype::{Charset, Collation};
    use crate::kv_table::TableCharset;

    /// CREATE TABLE -> INSERT -> SELECT, all from SQL strings, with rows as
    /// real TiKV-format bytes and metadata as tidb-model structs.
    #[test]
    fn create_insert_select_from_sql() {
        let mut catalog = Catalog::mock();
        assert!(run_create_table_on(
            "CREATE TABLE t (a BIGINT, b BIGINT UNSIGNED, s VARCHAR(10))",
            &mut catalog
        )
        .unwrap());

        assert_eq!(
            run_insert_on(
                "INSERT INTO t (a, s) VALUES (7, 'x')",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        let rows = run_select_on(
            "SELECT a, s FROM t",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Datum::Int(7));
        match &rows[0][1] {
            Datum::Bytes(b) => assert_eq!(b.as_slice(), b"x"),
            Datum::String(s) => assert_eq!(s.bytes(), b"x"),
            other => panic!("unexpected string datum {other:?}"),
        }
    }

    /// An index's visibility is stated by the DDL and read by the planner off
    /// `KvIndex::visible` -- one fact, two places. Every statement that
    /// creates an index resolves it at the single point that builds the
    /// `KvIndex`, so an `INVISIBLE` key is maintained (it is in `indexes()`)
    /// and hidden from every access path (it is not in `plan_indexes()`),
    /// which is Go's rule. Hardcoding `visible: true` -- as all three sites
    /// did -- made the planner choose an index Go never chooses. The
    /// `CREATE INDEX` and `ADD INDEX` sites are the session's
    /// (`tidb-session/tests/index_ddl_prefix_and_visibility_source.rs`).
    #[test]
    fn an_index_declared_invisible_is_maintained_but_never_planned() {
        let mut catalog = Catalog::mock();
        run_create_table_on(
            "CREATE TABLE t (a INT, b INT, KEY idx_a (a) INVISIBLE, KEY idx_b (b))",
            &mut catalog,
        )
        .unwrap();
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t") else {
            panic!("expected a kv table");
        };
        let maintained: Vec<&str> = kv.indexes().iter().map(|i| i.name.as_str()).collect();
        let planned: Vec<&str> = kv.plan_indexes().map(|i| i.name.as_str()).collect();
        assert_eq!(
            maintained,
            vec!["idx_a", "idx_b"],
            "every declared index is maintained by writes"
        );
        assert_eq!(
            planned,
            vec!["idx_b"],
            "only the visible one is an access path"
        );
    }

    #[test]
    fn if_not_exists_and_duplicates() {
        let mut catalog = Catalog::mock();
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).unwrap());
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).is_err());
        assert!(
            !run_create_table_on("CREATE TABLE IF NOT EXISTS t (a INT)", &mut catalog).unwrap()
        );
    }

    /// A single-column integer primary key sets Go's `PKIsHandle`; a
    /// non-integer one does not (`isIntCol`).
    #[test]
    fn primary_key_sets_the_handle_flag_only_for_an_integer_column() {
        let mut catalog = Catalog::mock();
        run_create_table_on("CREATE TABLE t (a INT, PRIMARY KEY (a))", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE s (a VARCHAR(4) PRIMARY KEY)", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE h (a INT)", &mut catalog).unwrap();

        let handle_offset = |name: &str| match catalog.get_table_for_test(name) {
            Some(crate::TableEntry::Kv(kv)) => kv.pk_handle_offset(),
            _ => panic!("expected a kv table"),
        };
        assert_eq!(handle_offset("t"), Some(0));
        assert_eq!(handle_offset("s"), None, "a string PK is not a handle");
        assert_eq!(handle_offset("h"), None, "no PK, no handle column");
    }

    /// The `(charset, collation, flen)` a column resolves to, for the
    /// charset/collation captures below.
    fn resolved(catalog: &Catalog, table: &str, column: &str) -> (String, String, i64) {
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test(table) else {
            panic!("expected a kv table");
        };
        let column = kv
            .columns
            .iter()
            .find(|c| c.name == column)
            .expect("column exists");
        (
            column.field_type.charset_name().to_owned(),
            column.field_type.collation_name().to_owned(),
            column.field_type.flen(),
        )
    }

    /// Captured from TiDB (`SHOW FULL COLUMNS` + `information_schema.columns`
    /// over one table carrying every string form): the BINARY/VARBINARY/BLOB
    /// family is charset `binary`, the CHAR/VARCHAR/TEXT family takes the
    /// table's default (`utf8mb4`/`utf8mb4_bin` -- NOT `utf8mb4_0900_ai_ci`),
    /// an explicit COLLATE wins, and the `BINARY` column attribute picks the
    /// charset's `_bin` collation.
    #[test]
    fn ddl_resolves_charset_and_collation_per_column() {
        let mut catalog = Catalog::mock();
        run_create_table_on(
            "CREATE TABLE t1 (\
                 c_varchar VARCHAR(10), c_char CHAR(10), \
                 c_varbinary VARBINARY(10), c_binary BINARY(3), \
                 c_blob BLOB, c_text TEXT, c_tinytext TINYTEXT, c_longblob LONGBLOB, \
                 c_vc_cs VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, \
                 c_vc_bin VARCHAR(10) BINARY, \
                 c_enum ENUM('a','B'), c_set SET('a','B'), c_int INT)",
            &mut catalog,
        )
        .unwrap();

        let case = |column: &str| resolved(&catalog, "t1", column);
        assert_eq!(
            case("c_varchar"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 10)
        );
        assert_eq!(case("c_char"), ("utf8mb4".into(), "utf8mb4_bin".into(), 10));
        assert_eq!(case("c_varbinary"), ("binary".into(), "binary".into(), 10));
        assert_eq!(case("c_binary"), ("binary".into(), "binary".into(), 3));
        // The BLOB/TEXT family carries its type's fixed capacity as flen.
        assert_eq!(case("c_blob"), ("binary".into(), "binary".into(), 65535));
        assert_eq!(
            case("c_text"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 65535)
        );
        assert_eq!(
            case("c_tinytext"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 255)
        );
        assert_eq!(
            case("c_longblob"),
            ("binary".into(), "binary".into(), 4_294_967_295)
        );
        assert_eq!(
            case("c_vc_cs"),
            ("utf8mb4".into(), "utf8mb4_general_ci".into(), 10)
        );
        // `VARCHAR(10) BINARY` is the charset's `_bin` collation, NOT charset
        // `binary`: it still reports utf8mb4 (captured).
        assert_eq!(
            case("c_vc_bin"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 10)
        );
        // ENUM/SET take the table charset, and their flen is the display
        // length Go derives from the members.
        assert_eq!(case("c_enum"), ("utf8mb4".into(), "utf8mb4_bin".into(), 1));
        assert_eq!(case("c_set"), ("utf8mb4".into(), "utf8mb4_bin".into(), 3));

        // A binary-charset string column has no charset for `HasCharset`,
        // which is what makes SHOW/information_schema report NULL for it.
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t1") else {
            panic!("expected a kv table");
        };
        let has_charset = |column: &str| {
            kv.columns
                .iter()
                .find(|c| c.name == column)
                .unwrap()
                .field_type
                .has_charset()
        };
        assert!(has_charset("c_varchar") && has_charset("c_enum"));
        assert!(!has_charset("c_varbinary") && !has_charset("c_blob"));
        assert!(!has_charset("c_int"));
    }

    /// Captured from TiDB over `... DEFAULT CHARSET=latin1`: a column with no
    /// clause takes the table's charset AND collation, an explicit
    /// `CHARACTER SET utf8mb4` takes that charset's default collation, and
    /// `CHARACTER SET binary` turns a VARCHAR into a `varbinary`.
    #[test]
    fn ddl_column_charset_falls_back_to_the_table_default() {
        let mut catalog = Catalog::mock();
        run_create_table_on(
            "CREATE TABLE t2 (a VARCHAR(10), b VARCHAR(10) CHARACTER SET utf8mb4, \
                 c VARCHAR(10) CHARACTER SET latin1, d VARCHAR(10) CHARACTER SET binary) \
                 DEFAULT CHARSET=latin1",
            &mut catalog,
        )
        .unwrap();
        let case = |column: &str| resolved(&catalog, "t2", column);
        assert_eq!(case("a"), ("latin1".into(), "latin1_bin".into(), 10));
        assert_eq!(case("b"), ("utf8mb4".into(), "utf8mb4_bin".into(), 10));
        assert_eq!(case("c"), ("latin1".into(), "latin1_bin".into(), 10));
        assert_eq!(case("d"), ("binary".into(), "binary".into(), 10));

        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t2") else {
            panic!("expected a kv table");
        };
        assert_eq!(
            kv.charset(),
            TableCharset {
                charset: Charset::Latin1,
                collation: Collation::Latin1Bin,
            }
        );
    }

    /// A `COLLATE` alone determines the charset, and a `COLLATE` that does not
    /// belong to the written `CHARACTER SET` is rejected rather than silently
    /// producing a contradictory field type.
    #[test]
    fn ddl_collate_alone_picks_the_charset_and_a_mismatched_pair_is_rejected() {
        let mut catalog = Catalog::mock();
        run_create_table_on(
            "CREATE TABLE t (a VARCHAR(10) COLLATE latin1_bin)",
            &mut catalog,
        )
        .unwrap();
        assert_eq!(
            resolved(&catalog, "t", "a"),
            ("latin1".into(), "latin1_bin".into(), 10)
        );
        assert!(run_create_table_on(
            "CREATE TABLE bad (a VARCHAR(10) CHARACTER SET latin1 COLLATE utf8mb4_bin)",
            &mut catalog,
        )
        .is_err());
    }

    /// Captured from TiDB: `INSERT INTO tb(b BINARY(3)) VALUES ('ab')` reads
    /// back as `0x616200` with `LENGTH` 3 -- a fixed-width binary column is
    /// zero-padded to its flen -- while `VARBINARY(3)` keeps the two bytes.
    #[test]
    fn binary_column_zero_pads_to_its_length() {
        let mut catalog = Catalog::mock();
        run_create_table_on(
            "CREATE TABLE tb (b BINARY(3), vb VARBINARY(3))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO tb VALUES ('ab','ab')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let rows = run_select_on(
            "SELECT b, vb FROM tb",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let bytes = |value: &Datum| match value {
            Datum::Bytes(b) => b.clone(),
            Datum::String(s) => s.bytes().to_vec(),
            other => panic!("unexpected string datum {other:?}"),
        };
        assert_eq!(bytes(&rows[0][0]), vec![b'a', b'b', 0]);
        assert_eq!(bytes(&rows[0][1]), vec![b'a', b'b']);
    }
}

/// Go's `sessionctx.Context` as `pkg/ddl` reads it: a statement's context.
impl tidb_ddl_build::session_context::DdlSessionContext for crate::StmtContext {
    fn metabuild_context(&self) -> tidb_expr::metabuild::Context {
        new_metabuild_context_with_sctx(self)
    }

    fn enable_check_constraint(&self) -> bool {
        crate::StmtContext::enable_check_constraint(self)
    }

    fn foreign_key_checks(&self) -> bool {
        crate::StmtContext::foreign_key_checks(self)
    }

    fn allow_remove_auto_inc(&self) -> bool {
        crate::StmtContext::allow_remove_auto_inc(self)
    }

    fn auto_increment_in_generated(&self) -> bool {
        crate::StmtContext::auto_increment_in_generated(self)
    }

    fn ddl_query(&self) -> &str {
        crate::StmtContext::ddl_query(self)
    }

    fn ddl_sql_mode(&self) -> i64 {
        crate::StmtContext::ddl_sql_mode(self)
    }

    fn sql_mode(&self) -> tidb_parser::SqlMode {
        crate::StmtContext::sql_mode(self)
    }

    fn ddl_query_rewrite(&self) -> Option<(String, String)> {
        crate::StmtContext::ddl_query_rewrite(self)
    }

    fn rewrite_ddl_job_query(&self, original: String, replacement: String) {
        crate::StmtContext::rewrite_ddl_job_query(self, original, replacement);
    }

    fn ddl_cdc_write_source(&self) -> u64 {
        crate::StmtContext::ddl_cdc_write_source(self)
    }

    fn ddl_reorg_vars(&self) -> &tidb_ddl_build::session_context::DdlReorgVars {
        crate::StmtContext::ddl_reorg_vars(self)
    }

    fn ddl_trace_id(&self) -> &[u8] {
        crate::StmtContext::ddl_trace_id(self)
    }

    fn ddl_session_alias(&self) -> &str {
        crate::StmtContext::ddl_session_alias(self)
    }

    fn ddl_reorg_priority(&self) -> i64 {
        crate::StmtContext::ddl_reorg_priority(self)
    }

    fn ddl_connection_id(&self) -> u64 {
        crate::StmtContext::ddl_connection_id(self)
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        crate::StmtContext::connection_charset_info(self)
    }

    fn session_zone(&self) -> tidb_datatype::SessionTimeZone {
        crate::StmtContext::session_zone(self)
    }

    fn like_default_escape(&self) -> u8 {
        crate::StmtContext::like_default_escape(self)
    }

    fn default_collation_for_utf8mb4(&self) -> &str {
        crate::StmtContext::default_collation_for_utf8mb4(self)
    }

    fn append_warning_parts(&self, code: u16, message: &str) {
        crate::StmtContext::append_warning_parts(self, code, message);
    }

    fn append_error_parts(&self, code: u16, message: &str) {
        crate::StmtContext::append_error_parts(self, code, message);
    }

    fn append_note_parts(&self, code: u16, message: &str) {
        crate::StmtContext::append_note_parts(self, code, message);
    }

    fn set_hypo_tiflash_replica(&self, schema: &str, table: &str, add: bool) {
        crate::StmtContext::set_hypo_tiflash_replica(self, schema, table, add);
    }

    fn add_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: tidb_model::index::IndexInfo,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        crate::StmtContext::add_hypo_index(self, schema, table, index)
    }

    fn drop_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        crate::StmtContext::drop_hypo_index(self, schema, table, index, if_exists)
    }

    fn append_sql_warn(&self, warning: tidb_util::context::SqlWarn) {
        crate::StmtContext::append_sql_warn(self, warning);
    }

    fn take_warnings(&self) -> Vec<tidb_util::context::SqlWarn> {
        crate::StmtContext::take_warnings(self)
    }

    fn with_isolated_warnings(&self) -> Box<dyn tidb_ddl_build::session_context::DdlSessionContext> {
        Box::new(crate::StmtContext::with_isolated_warnings(self))
    }
}
