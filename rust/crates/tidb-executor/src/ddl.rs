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
//! [`column_types::field_type_of`]'s transcreation of Go
//! `ResolveCharsetCollation` and `OverwriteCollationWithBinaryFlag` (see its
//! doc for the exact precedence). DEFERRED (documented): the
//! schema-version/DDL-job machinery (the driver applies metadata directly; the
//! DDL job queue is a separate tier). Constraints/indexes and the column
//! options are no longer deferred; see [`crate::column_default`] for what a
//! `DEFAULT` may be.
//!
//! # `CREATE TABLE ... LIKE` copies a built table, not a statement
//!
//! Go `BuildTableInfoWithLike` copies the source's whole `TableInfo` and
//! resets the fields that identify rather than describe it, so this path
//! leaves before any column-definition work and calls
//! [`crate::KvTable::create_like`] instead. That method builds the copy from
//! an EMPTY table rather than resetting a clone, so the source's rows, row
//! handles, auto-increment counter and foreign keys cannot be inherited by
//! omission -- see its doc for the full "not inherited" list and why each
//! entry is a correctness bug rather than a missing feature.
//!
//! `tidb_exec::table_info_build` still REFUSES the form. That is a narrower
//! surface, not a disagreement: it declines to build a table rather than
//! building a different one from the same statement.
//!
//! # The type rule set is shared with the `TableInfo` builder
//!
//! `tidb_exec::table_info_build` is the OTHER `CREATE TABLE` metadata builder
//! in this workspace, and the two disagreeing about the same statement is the
//! measured generator of this campaign's worst bug class. Their declared-type
//! halves are now ONE implementation, [`column_field_type`], so `BLOB(n)`
//! promotion, fractional-seconds precision, the unsigned integer display width
//! and `ZEROFILL` cannot drift apart again. What each builder still owns is
//! its genuinely different end: this one writes a `KvTable` into the catalog
//! and reaches the store, the other writes a `TableInfo` a real TiDB would
//! persist. The constraint/index lowering is still written twice -- named, not
//! unified, because the two lower onto different shapes (`KvIndex` column
//! OFFSETS here, `IndexInfo` there).
//!
//! One rule set has since been lifted OUT of that half. A key part's declared
//! LENGTH (`KEY idx (a(10))`) is validated by [`index_prefix`], because
//! whether a length is legal, and what length TiDB then stores, is a pure
//! function of the column's `FieldType` and the declared number and mentions
//! neither an offset nor an `IndexColumn`. It was written twice, and the two
//! copies disagreed: the `TableInfo` builder validated nothing at all. See
//! that module for the evidence and for what it deliberately does not decide.
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

mod alter_metadata;
mod alter_table;
/// AUTO_RANDOM declaration validation shared by local and cluster DDL.
pub mod auto_random;
pub mod column_field_type;
mod column_types;
pub mod index_prefix;
mod indexes;
mod table_cache;
mod table_constraints;
mod table_lifecycle;
pub mod table_partition;
pub mod table_partition_list;
pub mod table_partition_range;

pub use alter_table::{
    check_column_default_value, normalize_column_default, prepare_column_default,
    run_alter_table_in, settle_column_default, validate_column_default, PreparedColumnDefault,
    SettledColumnDefault,
};
pub use table_partition::linear_partitioning_warning;

use column_types::{database_charset_of, field_type_of, table_charset_of, NOT_NULL_FLAG};
pub use indexes::{run_create_index_in, run_drop_index_in};
use table_constraints::{
    is_int_column, primary_key_column, table_foreign_keys, table_indexes, AUTO_INCREMENT_FLAG,
    PRI_KEY_FLAG,
};

use indexes::{index_part_names, is_visible};
pub use table_lifecycle::{run_drop_table_in, run_rename_table_in, run_truncate_table_in};

use crate::driver::{Catalog, DriverError};
use crate::kv_table::{FkAction, KvColumn, KvForeignKey, KvIndex, KvTable, TableCharset};
use crate::SchemaErrorKind;
use tidb_ast::{ColumnDef, DdlStmt, Stmt};
use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
use tidb_model::column::ColumnInfo;

/// The row-handle layout a table is built with, decided ONCE per
/// `CREATE TABLE` and then read by everything downstream.
///
/// This is deliberately a single value rather than a pair of booleans. Go
/// carries `TableInfo.PKIsHandle` and `TableInfo.IsCommonHandle` separately
/// and every reader has to know that at most one of them is ever true; here
/// that invariant is the type. It also makes "clustered, but nobody recorded
/// which columns" and "handle columns recorded on a non-clustered table"
/// unrepresentable, which is exactly the shape the `NONCLUSTERED` bug had.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HandleKind {
    /// No clustered primary key: the table gets an implicit `_tidb_rowid`
    /// handle, and a primary key, if declared, is an ordinary unique index.
    RowId,
    /// Go `TableInfo.PKIsHandle`: a single integer primary key column whose
    /// value IS the row handle. The table stores no separate index for it.
    IntHandle(usize),
    /// Go `TableInfo.IsCommonHandle` (with `CommonHandleVersion = 1`): the
    /// primary key's columns, in key order, datum-encode into the row key.
    CommonHandle(Vec<usize>),
}

impl HandleKind {
    /// Go `ShouldBuildClusteredIndex` plus the `isSingleIntPK` split that
    /// follows it in `BuildTableInfo` (`pkg/ddl/create_table.go`).
    ///
    /// The three inputs are exactly Go's: the session's
    /// `@@tidb_enable_clustered_index`, the statement's explicit
    /// `CLUSTERED`/`NONCLUSTERED` clause, and whether the key is a single
    /// integer column. An explicit clause WINS over the variable in both
    /// directions -- that is the whole point of writing it -- and Go performs
    /// no type-eligibility check on it at all: `NONCLUSTERED` on an integer
    /// key is honoured, and `CLUSTERED` on a string or composite key is
    /// honoured as a COMMON handle rather than being refused or downgraded.
    ///
    /// With no clause, Go's `ClusteredIndexDefModeIntOnly` additionally
    /// consults the `alter-primary-key` config, which defaults to false and
    /// is not modelled here; with it false the mode reduces to
    /// "clustered only for a single integer key", which is what this returns.
    fn decide(
        mode: tidb_vardef::modes::ClusteredIndexDefMode,
        storage: Option<tidb_ast::PrimaryKeyStorage>,
        pk_offsets: &[usize],
        columns: &[ColumnInfo],
    ) -> Self {
        if pk_offsets.is_empty() {
            return Self::RowId;
        }
        let single_int = pk_offsets.len() == 1 && is_int_column(&columns[pk_offsets[0]]);
        let clustered = match storage {
            Some(tidb_ast::PrimaryKeyStorage::Clustered) => true,
            Some(tidb_ast::PrimaryKeyStorage::NonClustered) => false,
            None => match mode {
                tidb_vardef::modes::ClusteredIndexDefMode::ON => true,
                tidb_vardef::modes::ClusteredIndexDefMode::INT_ONLY => single_int,
                // Go's `default:` arm, which `ClusteredIndexDefModeOff` and
                // any unknown integer both take.
                _ => false,
            },
        };
        if !clustered {
            Self::RowId
        } else if single_int {
            Self::IntHandle(pk_offsets[0])
        } else {
            Self::CommonHandle(pk_offsets.to_vec())
        }
    }

    /// Go `TableInfo.HasClusteredIndex`.
    fn is_clustered(&self) -> bool {
        !matches!(self, Self::RowId)
    }

    /// The columns, in key order, whose values ARE the row key. Empty when
    /// the handle is the implicit `_tidb_rowid`.
    fn offsets(&self) -> &[usize] {
        match self {
            Self::RowId => &[],
            Self::IntHandle(offset) => std::slice::from_ref(offset),
            Self::CommonHandle(offsets) => offsets.as_slice(),
        }
    }
}

/// The `AUTO_INCREMENT [=] n` table option's value, if the list carries one.
///
/// Go reads the same option at CREATE (seeding the allocator) and at ALTER
/// (rebasing it). `FORCE AUTO_INCREMENT`, which lets Go move the counter
/// DOWN, is refused rather than silently treated as the plain form.
fn auto_increment_option(options: &[tidb_ast::TableOption]) -> Result<Option<i64>, DriverError> {
    let mut seed = None;
    for option in options {
        match option {
            tidb_ast::TableOption::AutoIncrement(value) => {
                // Go's parser holds this option in `opt.UintValue` (a `uint64`)
                // and every reader converts with `int64(opt.UintValue)`, so a
                // value above `i64::MAX` becomes negative rather than being
                // rejected -- see `rebase_auto_increment` for what that means.
                seed = Some(value.parse::<u64>().map_err(|_| {
                    DriverError::unsupported("AUTO_INCREMENT= needs an integer value")
                })? as i64);
            }
            tidb_ast::TableOption::ForceAutoIncrement(_) => {
                return Err(DriverError::unsupported(
                    "FORCE AUTO_INCREMENT is not supported yet",
                ));
            }
            _ => {}
        }
    }
    Ok(seed)
}

fn auto_random_base_option(options: &[tidb_ast::TableOption]) -> Result<Option<i64>, DriverError> {
    let mut seed = None;
    for option in options {
        match option {
            tidb_ast::TableOption::AutoRandomBase(value) => {
                seed = Some(value.parse::<u64>().map_err(|_| {
                    DriverError::unsupported("AUTO_RANDOM_BASE needs an integer value")
                })? as i64);
            }
            tidb_ast::TableOption::ForceAutoRandomBase(_) => {
                return Err(DriverError::unsupported(
                    "FORCE AUTO_RANDOM_BASE is not valid on CREATE TABLE",
                ));
            }
            _ => {}
        }
    }
    Ok(seed)
}

const MAX_TABLE_COMMENT_BYTES: usize = 2048;

/// The final `COMMENT=` option a CREATE or ALTER TABLE applies.
/// The `COMMENT` a column definition names, if it names one.
///
/// `None` and `Some("")` are different answers: Go's MODIFY overlays only the
/// options that are present, so an absent COMMENT keeps the column's existing
/// one while `COMMENT ''` clears it.
pub(crate) fn column_comment_option(options: &[tidb_ast::ColumnOption]) -> Option<String> {
    options.iter().find_map(|option| match option {
        tidb_ast::ColumnOption::Comment(text) => Some(text.clone()),
        _ => None,
    })
}

pub(crate) fn table_comment_option(
    options: &[tidb_ast::TableOption],
    table: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<String>, DriverError> {
    let Some(comment) = options.iter().rev().find_map(|option| match option {
        tidb_ast::TableOption::Comment(comment) => Some(comment),
        _ => None,
    }) else {
        return Ok(None);
    };
    normalize_table_comment(comment, table, ctx).map(Some)
}

/// Validates the comment text a single ALTER TABLE option writes.
pub fn normalize_table_comment(
    comment: &str,
    table: &str,
    ctx: &crate::StmtContext,
) -> Result<String, DriverError> {
    if comment.len() <= MAX_TABLE_COMMENT_BYTES {
        return Ok(comment.to_owned());
    }
    let error = DriverError::TableCommentTooLong(table.to_owned());
    if ctx.strict() {
        return Err(error);
    }
    let reported = error.to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    let mut end = MAX_TABLE_COMMENT_BYTES;
    while !comment.is_char_boundary(end) {
        end -= 1;
    }
    Ok(comment[..end].to_owned())
}

/// Parses and executes a `CREATE TABLE`, building a [`TableInfo`] and
/// registering a TiKV-byte-backed table in `catalog`. Returns whether a table
/// was created (`false` only for `IF NOT EXISTS` over an existing name).
pub fn run_create_table_on(sql: &str, catalog: &mut Catalog) -> Result<bool, DriverError> {
    // A stock session, including `tidb_enable_check_constraint` OFF, which is
    // the only mode this tier models; see [`CreateTableSettings::default`].
    run_create_table_in(
        sql,
        catalog,
        tidb_executor_default_database(),
        CreateTableSettings::default(),
        // A stock session's context, which is what this entry stands in for:
        // UTC, and the SHIPPED `sql_mode`, which is strict. `StmtContext`'s
        // DERIVED `Default` is non-strict, and DDL now reads that bit -- so
        // taking the derived default here would have made this entry point
        // build a catalog no stock session builds. The tests that call it
        // never set a zone; the session's own path passes its real context.
        &crate::StmtContext::default().with_strict(true),
    )
}

/// Resolves the charset/collation persisted by `CREATE DATABASE`.
pub fn resolve_database_charset(
    options: &[tidb_ast::DatabaseOption],
) -> Result<TableCharset, DriverError> {
    database_charset_of(options)
}

/// Go `checkUnsupportedTableOptions`: reject the two unsupported compatibility
/// clauses and storage engines outside TiDB's MySQL/MariaDB allowlist.
pub fn validate_table_options(options: &[tidb_ast::TableOption]) -> Result<(), DriverError> {
    const VALID_ENGINES: &[&str] = &[
        "archive",
        "blackhole",
        "csv",
        "example",
        "federated",
        "innodb",
        "memory",
        "merge",
        "mgr_myisam",
        "myisam",
        "ndb",
        "heap",
        "aria",
        "myrocks",
        "tokudb",
    ];

    for option in options {
        match option {
            tidb_ast::TableOption::Union(_) => {
                return Err(DriverError::TableOptionUnionUnsupported)
            }
            tidb_ast::TableOption::InsertMethod(_) => {
                return Err(DriverError::TableOptionInsertMethodUnsupported)
            }
            tidb_ast::TableOption::Engine(engine)
                if !VALID_ENGINES
                    .iter()
                    .any(|valid| engine.eq_ignore_ascii_case(valid)) =>
            {
                return Err(DriverError::UnknownStorageEngine(engine.clone()))
            }
            _ => {}
        }
    }
    Ok(())
}

/// How many of an `ALTER TABLE`'s actions are `CHECK`-constraint actions the
/// `tidb_enable_check_constraint = OFF` model DISCARDS, which is one warning
/// each. `ADD [CONSTRAINT n] CHECK` and `ALTER {CHECK|CONSTRAINT} n
/// {ENFORCED|NOT ENFORCED}` are both in this class; `DROP {CHECK|CONSTRAINT}
/// n` is NOT -- it raises 3940 instead, and Go raises it with the variable
/// off just as with it on (captured).
#[must_use]
pub fn discarded_check_constraint_actions(alter: &tidb_ast::AlterTableStmt) -> usize {
    alter
        .actions
        .iter()
        .map(|action| match action {
            tidb_ast::AlterTableAction::AddCheck(_) | tidb_ast::AlterTableAction::AlterCheck(_) => {
                1
            }
            tidb_ast::AlterTableAction::AddColumns { constraints, .. } => constraints
                .iter()
                .filter(|constraint| matches!(constraint, tidb_ast::TableConstraint::Check(_)))
                .count(),
            _ => 0,
        })
        .sum()
}

/// How many of an `ALTER TABLE`'s actions ADD a `CHECK` constraint, which is
/// what the `tidb_enable_check_constraint = ON` refusal is gated on: with the
/// variable ON, Go would STORE and enforce these.
#[must_use]
pub fn added_check_constraint_actions(alter: &tidb_ast::AlterTableStmt) -> usize {
    alter
        .actions
        .iter()
        .map(|action| match action {
            tidb_ast::AlterTableAction::AddCheck(_) => 1,
            tidb_ast::AlterTableAction::AddColumns { constraints, .. } => constraints
                .iter()
                .filter(|constraint| matches!(constraint, tidb_ast::TableConstraint::Check(_)))
                .count(),
            _ => 0,
        })
        .sum()
}

/// How many `CHECK` constraints a `CREATE TABLE` writes, counting both the
/// table-level `[CONSTRAINT name] CHECK (expr)` form and the form written
/// inline on a column.
///
/// Go emits ONE `tidb_enable_check_constraint is off` warning per constraint
/// it discards (captured: a table with two of them produces two warnings), so
/// the session needs the count, not a boolean. It lives here so "what counts
/// as a CHECK constraint" has a single definition shared with the executor's
/// own discard path.
#[must_use]
pub fn check_constraint_count(create: &tidb_ast::CreateTableStmt) -> usize {
    let table_level = create
        .table_constraints
        .iter()
        .filter(|constraint| matches!(constraint, tidb_ast::TableConstraint::Check(_)))
        .count();
    let column_level = create
        .columns
        .iter()
        .flat_map(|column| &column.options)
        .filter(|option| matches!(option, tidb_ast::ColumnOption::Check(_)))
        .count();
    table_level + column_level
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

/// [`run_create_table_on`] creating the table in `current_db`.
///
/// `enable_check_constraint` is `@@global.tidb_enable_check_constraint`, and it
/// decides what a `CHECK` constraint MEANS rather than merely whether it is
/// enforced. Captured from real TiDB (`gorun`, plus `SHOW WARNINGS` through
/// testkit) with the variable at its OFF default:
///
/// * `create table ck (a int, check (a > 0))` succeeds, warning 1105
///   `tidb_enable_check_constraint is off` once per constraint;
/// * `SHOW CREATE TABLE ck` restores `CREATE TABLE \`ck\` (\n  \`a\` int(11)
///   DEFAULT NULL\n) ...` -- with NO `CONSTRAINT ... CHECK` clause, and
///   `information_schema.check_constraints` is empty. The constraint is
///   DISCARDED at DDL time, not stored-but-unenforced;
/// * `insert into ck values (-1)` therefore succeeds.
///
/// So discarding is the faithful behaviour, and storing the constraint would
/// be the divergence: `SHOW CREATE TABLE` would grow a clause TiDB does not
/// print. With the variable ON, TiDB stores the constraint (auto-named
/// `<table>_chk_<N>`), prints it, and enforces it with error 3819; none of
/// that is modelled here, so this refuses rather than silently discarding.
pub fn run_create_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    settings: CreateTableSettings,
    // The SESSION's evaluation context. This entry re-parses text the session
    // already parsed, and it also FOLDS constants -- a column `DEFAULT` and a
    // RANGE partition bound -- so without it those folds run under a context
    // that is not the session's and a `time_zone`-dependent bound is settled
    // to the wrong integer. Go passes its `expression.BuildContext` from the
    // statement into `BuildTableInfo` for the same reason.
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let CreateTableSettings {
        sql_mode,
        foreign_key_checks,
        enable_check_constraint,
        clustered_index_mode,
    } = settings;
    let stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let create = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::CreateTable(create) => create,
            _ => {
                return Err(DriverError::unsupported(
                    "only CREATE TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::unsupported(
                "only CREATE TABLE is supported here",
            ))
        }
    };

    if enable_check_constraint && check_constraint_count(create) > 0 {
        return Err(DriverError::unsupported(
            "CHECK constraints are only modelled with tidb_enable_check_constraint off",
        ));
    }

    // `DROP TEMPORARY TABLE` is already refused here; creating one and
    // storing it as an ORDINARY table is the same gap on the other side, and
    // the more dangerous half: the table then outlives its session, is
    // visible to every other one, and answers statements TiDB refuses on a
    // temporary table outright -- `ADMIN CHECK TABLE` among them (Go's 8006,
    // `preprocessor.checkAdminCheckTableGrammar`). Refusing at CREATE keeps
    // the TEMPORARY keyword from being silently dropped.
    if create.temporary != tidb_ast::CreateTableTemporary::None {
        return Err(DriverError::unsupported(
            "temporary tables are not supported yet",
        ));
    }
    validate_table_options(&create.table_options)?;
    // Go refuses CTAS outright and has never implemented it:
    // `preprocess.go` -> `checkCreateTableGrammar` does
    //
    //   if stmt.Select != nil {
    //       p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
    //       return
    //   } else if len(stmt.Cols) == 0 && stmt.ReferTable == nil {
    //       p.err = dbterror.ErrTableMustHaveColumns
    //
    // A bare `errors.New` carries no terror code, so the client sees 1105
    // with exactly this text. The `else if` ordering matters: with a column
    // list present, `CREATE TABLE t (a INT) AS SELECT ...` still has to fail
    // on the CTAS arm rather than pass the column check and create an empty
    // table.
    if create.ctas.is_some() {
        return Err(DriverError::unsupported(
            "'CREATE TABLE ... SELECT' is not implemented yet",
        ));
    }
    if create.like_table.is_none() && create.columns.is_empty() {
        return Err(DriverError::unsupported("a table needs columns"));
    }

    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name);
    if catalog.contains_in(&database, name) {
        if create.if_not_exists {
            return Ok(false);
        }
        // Go `infoschema.ErrTableExists` (1050) prints the db-qualified name:
        // "Table 'test.t1' already exists".
        return Err(DriverError::Schema(crate::SchemaErrorKind::TableExists(
            format!("{database}.{name}"),
        )));
    }

    // `CREATE TABLE ... LIKE` copies a built table rather than building one
    // from column definitions, so it leaves before any of that work. The
    // target's own existence was settled just above, which is the order Go
    // reports the two in: an existing target is 1050 even when the source is
    // a view.
    if let Some(source) = &create.like_table {
        let (source_db, source_name) = crate::driver::split_table_path_pub(source, current_db)?;
        let (source_db, source_name) = (source_db.to_owned(), source_name.to_owned());
        // The ids are allocated between the two borrows of the source table,
        // because allocating needs the catalog mutably.
        let partitions = create_like_source(&source_db, &source_name, catalog)?
            .partition()
            .map_or(0, |partition| partition.definitions.len());
        let id = catalog.allocate_table_id();
        let mut ids = (0..partitions)
            .map(|_| catalog.allocate_table_id())
            .collect::<Vec<_>>()
            .into_iter();
        let mut copy = create_like_source(&source_db, &source_name, catalog)?
            .create_like(id, &mut || ids.next().expect("one id per copied partition"));
        copy.name = name.to_owned();
        catalog.register_kv_in(&database, name, copy)?;
        return Ok(true);
    }

    // Go reports a missing schema (1049) before it looks at the column
    // definitions: captured, `create table nosuchdb.t (a bigint, primary
    // key(zzz))` answers `[schema:1049]Unknown database 'nosuchdb'` rather
    // than complaining about the key. Registration would refuse this anyway
    // -- that is what makes the table impossible to lose -- but only after
    // the columns had been built, which is the wrong error.
    //
    // The `LIKE` branch above deliberately leaves before this: Go resolves a
    // LIKE source while preprocessing the statement, so a missing SOURCE wins
    // over a missing target schema (captured: `create table nosuchdb.c2 like
    // nosuchsrc.q` is `[schema:1146]Table 'nosuchsrc.q' doesn't exist`).
    if !catalog.has_database(&database) {
        return Err(DriverError::Schema(
            crate::SchemaErrorKind::UnknownDatabase(database),
        ));
    }

    // Go `checkDuplicateColumn` (`create_table.go:697`), which the LIVE
    // builder never had -- the cluster builder did. Captured:
    // `create table a1(a int, a int)` answers
    // `Error|1060|Duplicate column name 'a'`.
    let mut seen_columns: Vec<&str> = Vec::with_capacity(create.columns.len());
    for def in &create.columns {
        if let Some(previous) = seen_columns
            .iter()
            .find(|name| name.eq_ignore_ascii_case(&def.name))
        {
            return Err(DriverError::DuplicateColumnName((*previous).to_owned()));
        }
        seen_columns.push(&def.name);
    }
    // Go `checkDuplicateConstraint` (`create_table.go:1174`). `run_create_index_in`
    // already raises 1061, so without this the two entry points disagreed.
    // Captured: `create table a2(a int, b int, key i(a), key i(b))` answers
    // `Error|1061|Duplicate key name 'i'`.
    let mut seen_indexes: Vec<&str> = Vec::new();
    for constraint in &create.table_constraints {
        let tidb_ast::TableConstraint::Index(index) = constraint else {
            continue;
        };
        let Some(name) = index.name.as_deref().filter(|name| !name.is_empty()) else {
            continue;
        };
        if let Some(previous) = seen_indexes
            .iter()
            .find(|seen| seen.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::DuplicateKeyName((*previous).to_owned()));
        }
        seen_indexes.push(name);
    }

    // Build the ColumnInfos (ids 1..n, offsets in definition order).
    let database_charset = catalog
        .database_charset(&database)
        .expect("database existence was checked above");
    let table_charset = table_charset_of(&create.table_options, database_charset)?;
    let mut columns = Vec::with_capacity(create.columns.len());
    for (i, def) in create.columns.iter().enumerate() {
        let field_type = field_type_of(def, table_charset)?;
        let column_id = i64::try_from(
            i.checked_add(1)
                .expect("a parsed table cannot contain usize::MAX columns"),
        )
        .expect("a parsed table cannot exceed the model column-id domain");
        let mut col = ColumnInfo::new(column_id, &def.name, field_type);
        col.version = tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION;
        col.offset =
            i64::try_from(i).expect("a parsed table cannot exceed the model column-offset domain");
        // Go `columnDefToCol` copies the definition's COMMENT onto the
        // ColumnInfo, which is what every metadata reader then reports.
        col.comment = column_comment_option(&def.options).unwrap_or_default();
        columns.push(col);
    }

    // Column flags are applied in source order. `NULL` really can clear a
    // preceding NOT NULL/AUTO_INCREMENT/inline-primary flag, while its
    // occurrence remains recorded for `checkPriKeyConstraint` even if a
    // later option sets NOT NULL again.
    let mut has_null_flag = vec![false; create.columns.len()];
    for (i, def) in create.columns.iter().enumerate() {
        for option in &def.options {
            match option {
                tidb_ast::ColumnOption::NotNull => {
                    columns[i].add_flag(u64::from(NOT_NULL_FLAG));
                }
                tidb_ast::ColumnOption::Null => {
                    columns[i].del_flag(u64::from(NOT_NULL_FLAG));
                    if columns[i].field_type.has_flag(FieldTypeFlags::TIMESTAMP) {
                        columns[i].del_flag(u64::from(FieldTypeFlags::ON_UPDATE_NOW));
                    }
                    has_null_flag[i] = true;
                }
                tidb_ast::ColumnOption::AutoIncrement => {
                    columns[i].add_flag(u64::from(NOT_NULL_FLAG | AUTO_INCREMENT_FLAG));
                }
                tidb_ast::ColumnOption::InlineKey(inline)
                    if matches!(inline.kind, tidb_ast::InlineKeyKind::Primary { .. }) =>
                {
                    columns[i].add_flag(u64::from(NOT_NULL_FLAG | PRI_KEY_FLAG));
                }
                tidb_ast::ColumnOption::Default(_) => {
                    // Go `removeOnUpdateNowFlag`: an explicit default clears a
                    // preceding ON UPDATE on TIMESTAMP (but not DATETIME).
                    if columns[i].field_type.has_flag(FieldTypeFlags::TIMESTAMP) {
                        columns[i].del_flag(u64::from(FieldTypeFlags::ON_UPDATE_NOW));
                    }
                }
                tidb_ast::ColumnOption::OnUpdate(expr) => {
                    crate::column_default::validate_on_update_current_timestamp(
                        expr,
                        &columns[i].field_type,
                    )
                    .map_err(|_| DriverError::InvalidOnUpdate(def.name.clone()))?;
                    columns[i].add_flag(u64::from(FieldTypeFlags::ON_UPDATE_NOW));
                }
                _ => {}
            }
        }
    }

    // Go rejects a second auto column with ErrWrongAutoKey (1075) and a
    // non-integer one with "Incorrect column specifier"; captured from real
    // TiDB, which -- unlike MySQL -- does NOT require the column to be a key.
    let mut auto_increment_offset = None;
    for (i, def) in create.columns.iter().enumerate() {
        if !def
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement))
        {
            continue;
        }
        if auto_increment_offset.is_some() {
            return Err(DriverError::WrongAutoKey);
        }
        if !is_int_column(&columns[i]) {
            return Err(DriverError::WrongColumnSpecifier(def.name.clone()));
        }
        auto_increment_offset = Some(i);
    }

    let primary_key = primary_key_column(create, &columns)?;
    let pk_offsets: Vec<usize> = match &primary_key {
        Some(declared) => {
            let mut offsets = Vec::with_capacity(declared.columns.len());
            for name in &declared.columns {
                offsets.push(
                    columns
                        .iter()
                        .position(|col| col.name.original().eq_ignore_ascii_case(name))
                        .ok_or(DriverError::unsupported(
                            "the primary key names a column the table does not define",
                        ))?,
                );
            }
            offsets
        }
        None => Vec::new(),
    };
    // The ONE place this table's storage layout is decided. Every reader
    // below -- the stored `TableInfo`, the row encoder's handle columns, the
    // index builder, the foreign-key cover test, the partition builder --
    // reads this value rather than re-deriving the rule.
    let handle = HandleKind::decide(
        clustered_index_mode,
        primary_key.as_ref().and_then(|declared| declared.storage),
        &pk_offsets,
        &columns,
    );
    let auto_random = auto_random::validate(
        create,
        &columns
            .iter()
            .map(|column| column.field_type.clone())
            .collect::<Vec<_>>(),
        handle.offsets(),
    )?;
    for offset in &pk_offsets {
        // Go `checkIndexColumn` reaches a primary key too, and a clustered
        // primary key never becomes an entry in `table_indexes` (its
        // encoding IS the row key), so the JSON refusal has to be repeated
        // on this path rather than being left to the index builder.
        if columns[*offset].field_type.code() == FieldTypeCode::Json {
            return Err(DriverError::JsonUsedInKey(
                columns[*offset].name.original().to_owned(),
            ));
        }
    }

    // Go evaluates a constant DEFAULT at DDL time and stores the value on the
    // ColumnInfo while it walks the options, but defers every flag-sensitive
    // check until `processAndCheckDefaultValueAndColumn`. Keep that split so
    // the primary-key check below can preserve Go's observable error order.
    struct StagedCreateDefault {
        has_default: bool,
        value: crate::column_default::ColumnDefault,
    }
    let mut staged_defaults: Vec<Option<StagedCreateDefault>> =
        Vec::with_capacity(create.columns.len());
    for def in &create.columns {
        let mut default_value = None;
        for option in &def.options {
            match option {
                tidb_ast::ColumnOption::Default(expr) => {
                    let field_type = columns[staged_defaults.len()].field_type.clone();
                    // Go `SetDefaultValue`: a FUNCTION-CALL default takes the
                    // whitelist route and never the constant folder, which is
                    // why `DEFAULT (abs(1))` is 3770 in TiDB despite folding.
                    // Go `EvalSimpleAst`: non-function expressions are
                    // evaluated rather than required to be literal already,
                    // which is what settles `DEFAULT (1 + 1)` to 2.
                    let built =
                        crate::column_default::build_in_context(expr, &field_type, &def.name, ctx)?;
                    default_value = Some(match built {
                        crate::column_default::ColumnDefault::Value(value) => {
                            let settled = alter_table::settle_column_default(
                                value,
                                &field_type,
                                &def.name,
                                ctx,
                                &ctx.session_zone(),
                            )?;
                            StagedCreateDefault {
                                has_default: settled.has_default,
                                value: crate::column_default::ColumnDefault::Value(settled.stored),
                            }
                        }
                        computed => StagedCreateDefault {
                            has_default: true,
                            value: computed,
                        },
                    });
                }
                // AUTO_INCREMENT is its own value source, handled below.
                tidb_ast::ColumnOption::AutoIncrement => {}
                // A generated column's value source is its expression, built
                // below once every column's name and type is known.
                tidb_ast::ColumnOption::Generated { .. } => {}
                _ => {}
            }
        }
        staged_defaults.push(default_value);
    }

    // Go `checkPriKeyConstraint`, in source order:
    // 1. an inline PRI flag plus DEFAULT NULL is 1067;
    // 2. only then is a table-level primary key's PRI flag installed;
    // 3. any explicit NULL option on the resulting primary key is 1171.
    let mut primary_offsets = vec![false; columns.len()];
    for offset in &pk_offsets {
        primary_offsets[*offset] = true;
    }
    for (offset, column) in columns.iter_mut().enumerate() {
        let staged = staged_defaults[offset].as_ref();
        let has_null_default = staged.is_some_and(|default| {
            default.has_default
                && matches!(
                    &default.value,
                    crate::column_default::ColumnDefault::Value(value) if value.is_null()
                )
        });
        if column.field_type.has_flag(PRI_KEY_FLAG) && has_null_default {
            return Err(DriverError::InvalidDefault(
                column.name.original().to_owned(),
            ));
        }
        if primary_offsets[offset] && !column.field_type.has_flag(PRI_KEY_FLAG) {
            column.add_flag(u64::from(PRI_KEY_FLAG));
        }
        if column.field_type.has_flag(PRI_KEY_FLAG) && has_null_flag[offset] {
            return Err(DriverError::PrimaryCantHaveNull);
        }
    }

    // Go `checkDefaultValue` runs only after the primary-key transition above.
    // It validates the persisted spelling but does not replace it with the
    // typed runtime value. `setNoDefaultValueFlag` is likewise based on the
    // now-final option flags, not the flags present when DEFAULT was visited.
    let mut defaults = Vec::with_capacity(staged_defaults.len());
    for (offset, staged) in staged_defaults.into_iter().enumerate() {
        let Some(staged) = staged else {
            defaults.push(None);
            continue;
        };
        if staged.has_default {
            if let crate::column_default::ColumnDefault::Value(stored) = &staged.value {
                alter_table::validate_column_default(
                    stored,
                    &columns[offset].field_type,
                    &create.columns[offset].name,
                    columns[offset].version,
                    ctx.ddl_default_conversion_flags(),
                    &ctx.session_zone(),
                )?;
            }
        }
        if !staged.has_default && columns[offset].field_type.has_flag(NOT_NULL_FLAG) {
            defaults.push(None);
        } else {
            defaults.push(Some(staged.value));
        }
    }

    // Primary-key columns become NOT NULL in the finished metadata only after
    // the source checks above have observed the intermediate flag state.
    for offset in &pk_offsets {
        columns[*offset].add_flag(u64::from(NOT_NULL_FLAG));
    }

    let table_id = catalog.allocate_table_id();

    // The generated columns, built against the table's own final column list
    // so their expressions index the stored row directly.
    let column_names: Vec<String> = columns
        .iter()
        .map(|c| c.name.original().to_owned())
        .collect();
    let column_types: Vec<tidb_datatype::FieldType> =
        columns.iter().map(|c| c.field_type.clone()).collect();
    let generated = crate::generated_column::build_generated_columns_with_like_default_escape(
        &create.columns,
        &column_names,
        &column_types,
        &ctx.session_zone(),
        ctx.like_default_escape(),
    )
    .map_err(generated_column_error)?;
    // Go `ErrUnsupportedOnGeneratedColumn`: a VIRTUAL generated column cannot
    // be the primary key, because the key would have no stored value to be.
    // A STORED one can. Captured: `create table t (a int, b int as (a+1),
    // primary key(b))` is 3106.
    for offset in &pk_offsets {
        if generated[*offset]
            .as_ref()
            .is_some_and(|generated| !generated.stored)
        {
            return Err(DriverError::UnsupportedOnGeneratedColumn(
                "Defining a virtual generated column as primary key".to_owned(),
            ));
        }
    }

    let kv_columns: Vec<KvColumn> = columns
        .iter()
        .enumerate()
        .map(|(position, c)| KvColumn {
            name: c.name.original().to_owned(),
            id: c.id,
            field_type: c.field_type.clone(),
            // These vectors were built in the same definition-order pass.
            // Keeping that position directly avoids narrowing the model's
            // signed i64 offset back into a host index.
            column_info_version: c.version,
            comment: c.comment.clone(),
            generated: generated[position].clone(),
            default_value: defaults[position].clone(),
            // A column present at CREATE TABLE has no pre-existing rows.
            origin_default: None,
        })
        .collect();
    let table = KvTable::new(table_id, kv_columns);
    let mut table = table;
    table.set_name(name);
    table.set_charset(table_charset);
    if let Some(comment) = table_comment_option(&create.table_options, name, ctx)? {
        table.set_comment(comment);
    }
    match &handle {
        HandleKind::RowId => {}
        HandleKind::IntHandle(offset) => table.set_pk_handle_offset(*offset),
        HandleKind::CommonHandle(offsets) => table.set_common_handle_offsets(offsets.clone()),
    }
    if let Some(spec) = auto_random {
        table.set_auto_random(spec);
        if let Some(seed) = auto_random_base_option(&create.table_options)? {
            if seed > 1 {
                table
                    .rebase_auto_random(seed)
                    .map_err(auto_random::rebase_error)?;
            }
        }
    }
    let clustered = handle.is_clustered();
    if let Some(offset) = auto_increment_offset {
        table.set_auto_increment_offset(offset);
        // Go `handleAutoIncID`: CREATE seeds the allocator only when the
        // option is `> 1` -- a SIGNED comparison on `int64(opt.UintValue)`,
        // so `AUTO_INCREMENT = 18446744073709551615` (and any value above
        // `i64::MAX`) seeds nothing and the first row lands on 1 even for a
        // `BIGINT UNSIGNED` column. Only ALTER rebases in the column's own
        // domain; captured from Go, the two really do disagree here.
        if let Some(seed) = auto_increment_option(&create.table_options)? {
            if seed > 1 {
                table
                    .rebase_auto_increment(seed)
                    .map_err(|error| DriverError::AutoIdUnavailable(error.0))?;
            }
        }
    }
    let (indexes, hidden_columns) = table_indexes(create, &columns, clustered, ctx)?;
    for hidden in hidden_columns {
        // Go `checkExpressionIndexAutoIncrement`: an expression index may not
        // read an AUTO_INCREMENT column. Captured as 3754 naming the index,
        // which is the index whose part built this column.
        if let Some(auto) = auto_increment_offset {
            let auto_name = columns[auto].name.original().to_owned();
            if hidden
                .generated
                .dependencies
                .iter()
                .any(|dependency| dependency.eq_ignore_ascii_case(&auto_name))
            {
                return Err(DriverError::ExpressionIndexCanNotRefer(
                    indexes
                        .iter()
                        .find(|index| {
                            index
                                .column_offsets
                                .iter()
                                .any(|offset| *offset >= columns.len())
                        })
                        .map_or_else(String::new, |index| index.name.clone()),
                ));
            }
        }
        table.add_hidden_column(KvColumn {
            name: hidden.name,
            id: table.next_column_id(),
            field_type: hidden.field_type,
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            comment: String::new(),
            generated: Some(hidden.generated),
            default_value: None,
            origin_default: None,
        });
    }
    for index in indexes {
        table.add_index(index);
    }
    for foreign_key in table_foreign_keys(create, &columns, catalog, &database, foreign_key_checks)?
    {
        // Go `addForeignKeyIndex`: a foreign key needs an index on its
        // referencing columns, and TiDB adds one named after the constraint
        // UNLESS an existing key -- the clustered primary key included --
        // already has those columns as a PREFIX. Captured: a child whose FK
        // column is its primary key, and one with `KEY kk (pid, k)`, get no
        // extra index, while one with `KEY kk (k, pid)` does.
        // Go `IsIndexPrefixCovered` (`pkg/meta/model/index.go`) additionally
        // requires each covering key part to store the WHOLE column: an
        // entry holding `'abc'` for `'abcdef'` cannot answer "does a parent
        // row with this value exist", so a prefix index earns the child no
        // exemption and TiDB adds the constraint's own index beside it.
        // The constraint stores NAMES; the index it may need is built over
        // offsets, so resolve once here against the table as it stands.
        let fk_offsets: Vec<usize> = foreign_key
            .cols
            .iter()
            .filter_map(|name| {
                columns
                    .iter()
                    .position(|column| column.name.original().eq_ignore_ascii_case(name))
            })
            .collect();
        let covered = |offsets: &[usize]| offsets.starts_with(&fk_offsets[..]);
        let covered_index = |index: &KvIndex| {
            covered(&index.column_offsets)
                && fk_offsets.iter().enumerate().all(|(position, at)| {
                    let length = index.prefix_length(position);
                    length == crate::ddl::index_prefix::UNSPECIFIED_LENGTH
                        || columns
                            .get(*at)
                            .is_some_and(|column| length >= column.field_type.flen())
                })
        };
        if !covered(handle.offsets()) && !table.indexes().iter().any(covered_index) {
            let id = table.next_index_id();
            table.add_index(KvIndex {
                id,
                name: foreign_key.name.clone(),
                comment: String::new(),
                unique: false,
                column_offsets: fk_offsets.clone(),
                // Go's auto-created foreign-key index names whole columns:
                // an `FKInfo` has no per-column length to carry.
                prefix_lengths: vec![
                    crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
                    fk_offsets.len()
                ],
                // Local to the table it constrains: an `FKInfo` carries no
                // `GLOBAL` to record.
                global: false,
                visible: true,
            });
        }
        table.allocate_foreign_key_id();
        table.add_foreign_key(foreign_key);
    }
    // Last, because Go's unique-key rule (8264/1503) reads the table's
    // finished index list, and the partitions' physical ids are allocated
    // from the same counter the table's own id came from -- as ONE ascending
    // block right after it, which is what lets a scan cover the whole
    // relation with a single key range.
    if let Some(partition) = table_partition::build_table_partitioning(
        create,
        &column_names,
        &column_types,
        table.indexes(),
        handle.offsets(),
        &mut || catalog.allocate_table_id(),
        ctx,
    )? {
        table.set_partition(partition);
    }
    catalog.register_kv_in(&database, name, table)?;
    Ok(true)
}

/// Resolves the table `CREATE TABLE ... LIKE` copies.
///
/// Go `ddl.BuildTableInfoWithLike` raises `ErrWrongObject` (1347) with the
/// third argument `BASE TABLE` for a view or a sequence, which is a different
/// error from naming something that does not exist at all (1146).
fn create_like_source<'a>(
    database: &str,
    name: &str,
    catalog: &'a Catalog,
) -> Result<&'a crate::KvTable, DriverError> {
    let wrong_object = || {
        Err(DriverError::Schema(crate::SchemaErrorKind::WrongObject {
            name: format!("{database}.{name}"),
            expected: "BASE TABLE",
        }))
    };
    match catalog.table_in(database, name) {
        Some(crate::TableEntry::Kv(table)) => Ok(table),
        Some(crate::TableEntry::View(_) | crate::TableEntry::Sequence(_)) => wrong_object(),
        // A matrix-backed fixture table has no stored structure to copy. It
        // only exists in this crate's own tests, so this is unreachable from
        // SQL, but it must not be mistaken for "does not exist".
        Some(crate::TableEntry::Mem(_) | crate::TableEntry::Cte(_)) => Err(
            DriverError::unsupported("CREATE TABLE LIKE needs a stored table"),
        ),
        None => Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        ))),
    }
}

/// The refusal a piece of name-keyed metadata makes when a DDL would drop or
/// rename the column it names, in Go's own error for that metadata.
///
/// The two spellings of the column name are Go's and are observable: the
/// generated-column arms report `oldColName.O`, the name AS WRITTEN, while
/// `checkDropColumnWithPartitionConstraint` passes `colName.L` and so reports
/// it LOWERCASED (`pkg/ddl/executor.go` `ErrDependentByPartitionFunctional
/// .GenWithStackByArgs(colName.L)`).
fn column_dependent_error(
    dependent: crate::kv_table::ColumnDependent,
    column: &str,
) -> DriverError {
    use crate::kv_table::ColumnDependent;
    match dependent {
        ColumnDependent::ExpressionIndex => {
            DriverError::DependentByFunctionalIndex(column.to_owned())
        }
        ColumnDependent::GeneratedColumn => {
            DriverError::DependentByGeneratedColumn(column.to_owned())
        }
        ColumnDependent::Partition => {
            DriverError::DependentByPartitionFunctional(column.to_lowercase())
        }
    }
}

/// The same refusal as Go's `err.Error()` renders it: message with the
/// `[class:code]` prefix in front.
///
/// This exists for one caller. Go's MODIFY COLUMN path passes `errG.Error()`
/// as the ARGUMENT of `ErrUnsupportedOnGeneratedColumn`, so the prefix ends up
/// inside the 3106 message that reaches the client -- recorded verbatim in
/// `tests/integrationtest/r/ddl/column_change.result`. Everywhere else the
/// prefix is a server-log detail the wire never sees, which is why nothing
/// else in this tier renders one.
fn column_dependent_error_text(
    dependent: crate::kv_table::ColumnDependent,
    column: &str,
) -> String {
    let rendered = column_dependent_error(dependent, column).to_mysql_error();
    format!("[ddl:{}]{}", rendered.code, rendered.message)
}

/// Names a generated-column DDL refusal the way Go's own error does.
pub(crate) fn generated_column_error(
    error: crate::generated_column::GeneratedDdlError,
) -> DriverError {
    use crate::generated_column::GeneratedDdlError;
    match error {
        GeneratedDdlError::UnknownDependency(name) => DriverError::UnknownColumnInClause {
            column: name,
            clause: "generated column function".to_owned(),
        },
        GeneratedDdlError::NonPrior => DriverError::GeneratedColumnNonPrior,
        GeneratedDdlError::DisallowedFunction(column) => {
            DriverError::GeneratedColumnFunctionNotAllowed(column)
        }
        GeneratedDdlError::Unsupported(reason) => {
            DriverError::UnsupportedOnGeneratedColumn(reason.to_owned())
        }
        GeneratedDdlError::Unbuildable(reason) => DriverError::unsupported(reason),
    }
}

/// Go `checkColumnDefaultValue`'s type test: the codes whose literal default
/// is 1101.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{run_insert_on, run_select_on};
    use tidb_datatype::Datum;
    use tidb_datatype::{Charset, Collation};

    #[test]
    fn table_engine_validation_matches_the_go_allowlist_and_error() {
        for engine in [
            "ARCHIVE",
            "blackhole",
            "csv",
            "example",
            "federated",
            "InnoDB",
            "memory",
            "merge",
            "mgr_myisam",
            "myisam",
            "ndb",
            "heap",
            "aria",
            "myrocks",
            "tokudb",
        ] {
            validate_table_options(&[tidb_ast::TableOption::Engine(engine.to_owned())])
                .unwrap_or_else(|error| panic!("Go admits ENGINE={engine}: {error}"));
        }

        let error = run_create_table_on(
            "CREATE TABLE invalid_engine (id BIGINT) ENGINE=imaginary",
            &mut Catalog::default(),
        )
        .expect_err("Go rejects an engine outside its compatibility allowlist")
        .to_mysql_error();
        assert_eq!(error.code, 1286);
        assert_eq!(error.state, *b"42000");
        assert_eq!(error.message, "Unknown storage engine 'imaginary'");
    }

    /// CREATE TABLE -> INSERT -> SELECT, all from SQL strings, with rows as
    /// real TiKV-format bytes and metadata as tidb-model structs.
    #[test]
    fn create_insert_select_from_sql() {
        let mut catalog = Catalog::default();
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
    /// did -- made the planner choose an index Go never chooses.
    #[test]
    fn an_index_declared_invisible_is_maintained_but_never_planned() {
        let mut catalog = Catalog::default();
        run_create_table_on(
            "CREATE TABLE t (a INT, b INT, KEY idx_a (a) INVISIBLE, KEY idx_b (b))",
            &mut catalog,
        )
        .unwrap();
        run_create_index_in(
            "CREATE INDEX idx_c ON t (a) INVISIBLE",
            &mut catalog,
            crate::driver::DEFAULT_DATABASE,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_alter_table_in(
            "ALTER TABLE t ADD INDEX idx_d (b) INVISIBLE",
            &mut catalog,
            crate::driver::DEFAULT_DATABASE,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t") else {
            panic!("expected a kv table");
        };
        let maintained: Vec<&str> = kv.indexes().iter().map(|i| i.name.as_str()).collect();
        let planned: Vec<&str> = kv.plan_indexes().map(|i| i.name.as_str()).collect();
        assert_eq!(
            maintained,
            vec!["idx_a", "idx_b", "idx_c", "idx_d"],
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
        let mut catalog = Catalog::default();
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
        let mut catalog = Catalog::default();
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
        let mut catalog = Catalog::default();
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
        let mut catalog = Catalog::default();
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
        let mut catalog = Catalog::default();
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
        let mut catalog = Catalog::default();
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
