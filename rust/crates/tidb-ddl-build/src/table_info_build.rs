// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/ddl`'s `CREATE TABLE` **metadata** builder: one parsed
//! `CREATE TABLE` in, the `TableInfo` a real TiDB would persist out.
//!
//! This is the metadata half of `BuildTableInfoFromAST` only — everything that
//! decides what bytes land under `DB:<id>` / `Table:<id>`. It is deliberately
//! NOT the DDL job framework, and it writes no index data: for a brand-new
//! empty table there is none to write.
//!
//! The Go path this transcreates, in order:
//!
//! * `BuildTableInfoWithStmt` — resolve the table charset/collation, then
//!   `buildColumnsAndConstraints`, then `BuildTableInfo`.
//! * `buildColumnAndConstraint` — resolve each column's charset/collation
//!   (`getCharsetAndCollateInColumnDef` -> `ResolveCharsetCollation` ->
//!   `OverwriteCollationWithBinaryFlag`), then `setCharsetCollationFlenDecimal`
//!   fills in the declared type's default flen/decimal.
//! * `columnDefToCol` — walk the column options in written order, then
//!   `processDefaultValue` (`setTimestampDefaultValue`, `setYearDefaultValue`,
//!   `setNoDefaultValueFlag`) and `processColumnFlags`.
//! * `setColumnFlagWithConstraint` — a table-level constraint stamps
//!   `PriKeyFlag`/`UniqueKeyFlag`/`MultipleKeyFlag` on its first column.
//! * `BuildTableInfo` — allocate column IDs, decide the handle
//!   (`ShouldBuildClusteredIndex`), then build one `IndexInfo` per surviving
//!   constraint with `AllocateIndexID`.
//!
//! **The declared type -> `FieldType` rule set is NOT owned here.** It lives in
//! [`crate::column_field_type`] and is SHARED with the
//! runnable-path `CREATE TABLE` builder (the in-process appliers), because the two
//! had drifted apart on five rules at once and four of this campaign's
//! accept-then-discard bugs were the two disagreeing about the same statement.
//! What stays here is this tier's own end: the charset/collation resolution
//! over Go's string-named registry, the column OPTIONS walk, and the
//! constraint/index lowering onto a `TableInfo`.
//!
//! **Ground truth.** Every rule here is checked field-for-field against the
//! `TableInfo`s a real TiDB v8.5 builds for its own `mysql.*` bootstrap DDL,
//! captured by running Go's own `ddl.BuildTableInfoFromAST` over
//! `pkg/session`'s `systemTablesOfBaseNextGenVersion` under the exact context
//! classic bootstrap uses (`mysql.ModeNone`, `ClusteredIndexDefModeIntOnly`).
//! The capture is `tests/data/mysql_bootstrap_tableinfos.json` and the
//! comparison is `tests/mysql_bootstrap_tableinfo_source.rs`.
//! That capture defines the metadata target; it does not authorize a live
//! session path to reuse `ModeNone`. [`build_table_info`] is the explicit
//! bootstrap/fixture entrypoint, while live callers pass their statement
//! context through [`build_table_info_with_context`].
//!
//! **What is refused rather than guessed.** Validity checks Go performs on top
//! of the metadata (index length limits, too-long identifiers, generated-column
//! dependency resolution, foreign keys, partitioning, `AUTO_INCREMENT`'s
//! allocator wiring) are not reproduced; a declaration needing one is refused
//! by name in [`build_table_info`], before any caller spends a timestamp.

use std::fmt;
use std::sync::atomic::Ordering;

use tidb_error::mysql::MysqlError;

use tidb_ast::IndexType;
use tidb_ast::{CiString, IndexOptions};
use tidb_ast::{
    ColumnDef, ColumnOption, ColumnType, CreateTableStmt, Expr, IndexConstraintKind, IndexPart,
    InlineKeyKind, PrimaryKeyStorage, TableConstraint, TableOption,
};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
// The declared-type -> `FieldType` rule set is SHARED with the runnable-path
// `CREATE TABLE` builder; see `column_field_type`'s module doc for why.
use crate::column_field_type::{
    build_field_type as build_shared_field_type,
    process_column_flags, try_auto_convert_too_big_varchar, ColumnTypeError,
};
use tidb_hack::GoCaseCompare;
use tidb_model::column::{
    ColumnDefaultValue, ColumnInfo, GoAny, GoStringSet, CURR_LATEST_COLUMN_INFO_VERSION,
};
use tidb_model::index::IndexInfo;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::{TableInfo, TABLE_INFO_VERSION5};
use tidb_model::GoShared;

/// Go `types.UnspecifiedLength`.
pub const UNSPECIFIED_LENGTH: i64 = -1;

use tidb_util::context::WarnErr;
use tidb_vardef::modes::ClusteredIndexDefMode;

/// Go `errCheckConstraintIsOff` (`pkg/ddl/executor.go`): a plain
/// `errors.NewNoStackError`, so it carries no error class and reaches
/// `SHOW WARNINGS` as 1105.
fn err_check_constraint_is_off() -> WarnErr {
    WarnErr::Message(format!(
        "{} is off",
        tidb_vardef::tidb_vars::TIDB_ENABLE_CHECK_CONSTRAINT
    ))
}

/// Why a statement cannot be built as written.
///
/// Every value is produced before any caller reads a snapshot or spends a PD
/// timestamp, so a refusal never leaves a partial change behind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlAdmissionError {
    /// Exact, self-contained explanation naming the offending clause.
    pub reason: String,
    /// The MySQL error number the client is told.
    pub code: u16,
    /// The Go `*terror.Error` identity (class and code) the refusal is raised
    /// as, `None` for a plain `errors.Errorf` (1105) with no Go counterpart.
    /// Go's `Error()` prints it as `[class:code]`, and the persisted job
    /// error carries it.
    pub identity: Option<tidb_error::terror::TerrorIdentity>,
}

impl DdlAdmissionError {
    /// Go `dbterror.ErrUnsupportedDDLOperation` (8200, `Unsupported %s`): a
    /// table shape this node will not create because it could not then serve
    /// it. Refusing under Go's own errno is what lets a client tell "this
    /// server does not do that" from an internal failure.
    pub fn unsupported(what: impl Into<String>) -> Self {
        Self::from_terror(
            &tidb_util::dbterror::ERR_GENERAL_UNSUPPORTED_DDL
                .generate_with_stack_by_args(&[what.into().into()]),
        )
    }

    /// The refusal for a Go error raised as Go raises it
    /// (`ErrX.GenWithStackByArgs(...)`).
    #[must_use]
    pub fn from_terror(error: &tidb_error::terror::TerrorError) -> Self {
        Self {
            reason: error.message().to_owned(),
            code: error.to_sql_error().code,
            identity: Some(error.identity()),
        }
    }

    /// A refusal Go raises as a plain `errors.Errorf` (1105, no class).
    pub fn plain(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            code: GENERIC_ERROR_CODE,
            identity: None,
        }
    }

    /// A refusal carrying an already classified driver-tier failure.
    #[must_use]
    pub fn from_mysql_error(error: MysqlError) -> Self {
        Self {
            reason: error.message,
            code: error.code,
            identity: error.identity,
        }
    }

    /// The refusal as the client's MySQL error, the inverse of
    /// [`Self::from_mysql_error`].
    #[must_use]
    pub fn to_mysql_error(&self) -> MysqlError {
        MysqlError::from_parts(self.code, self.sql_state(), self.reason.clone())
            .with_identity_opt(self.identity)
    }

    /// The SQLSTATE paired with [`Self::code`] by Go's MySQL error catalog.
    ///
    /// Keeping this derivation on the admission error prevents transport
    /// callers from preserving the errno while silently flattening every DDL
    /// refusal back to `HY000`.
    #[must_use]
    pub fn sql_state(&self) -> [u8; 5] {
        tidb_error::mysql::mysql_state(self.code)
            .as_bytes()
            .try_into()
            .expect("the MySQL error catalog stores five-byte SQLSTATEs")
    }
}

/// The MySQL error number for a refusal that has no Go code of its own.
pub const GENERIC_ERROR_CODE: u16 = 1105;
/// Go `mysql.MaxConstraintIdentifierLen`.
impl fmt::Display for DdlAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for DdlAdmissionError {}

type Refusal<T> = Result<T, DdlAdmissionError>;

/// Go `BuildTableInfoFromAST` under `metabuild.NewNonStrictContext()`
/// (`ModeNone`, `tidb_enable_clustered_index` at its `ON` default).
///
/// `db_charset`/`db_collate` are the owning database's, which a table with no
/// charset option of its own inherits. The returned table carries no ID and no
/// `update_ts`: those belong to the transaction that publishes it. Live DDL
/// must call [`build_table_info_in_schema`] with the issuing session's
/// context instead.
pub fn build_table_info(
    create: &CreateTableStmt,
    db_charset: &str,
    db_collate: &str,
) -> Refusal<TableInfo> {
    build_table_info_with_context(
        create,
        db_charset,
        db_collate,
        &tidb_expr::metabuild::Context::new_non_strict(),
        tidb_vardef::ENABLE_CHECK_CONSTRAINT.load(Ordering::SeqCst),
    )
}

/// Go `BuildTableInfoFromAST` -> `buildTableInfoWithCheck` under `context`:
/// [`build_table_info_in_schema`] (`BuildTableInfoWithStmt`), then
/// [`check_table_info_valid_with_stmt`] and [`check_table_info_valid_extra`]
/// -- the statement-free build bootstrap uses for its system tables.
///
/// `enable_check_constraint` is Go's `vardef.EnableCheckConstraint.Load()`
/// as the caller's node holds it (see [`build_table_info_in_schema`]).
pub fn build_table_info_with_context(
    create: &CreateTableStmt,
    db_charset: &str,
    db_collate: &str,
    context: &tidb_expr::metabuild::Context,
    enable_check_constraint: bool,
) -> Refusal<TableInfo> {
    let schema = match create.name.as_slice() {
        [schema, _] => schema.clone(),
        _ => String::new(),
    };
    let tb_info = build_table_info_in_schema(
        create,
        &schema,
        db_charset,
        db_collate,
        context,
        enable_check_constraint,
    )?;
    // checkTableInfoValidWithStmt will do the constant fold the partition
    // expression first, then checkTableInfoValidExtra will pass the
    // tableInfo check successfully.
    check_table_info_valid_with_stmt(context, &tb_info)?;
    check_table_info_valid_extra(&mut |warning| context.append_warning(warning), &tb_info)?;
    Ok(tb_info)
}

/// Go `BuildTableInfoWithStmt` (`create_table.go:793`): the statement's
/// `TableInfo`, without the validity checks Go runs after it
/// ([`check_table_info_valid_with_stmt`], [`check_table_info_valid_extra`]).
/// `schema` is the child's resolved schema, which an unqualified `REFERENCES`
/// inherits (Go `preprocess.go:2097`).
///
/// A live session must build under its own context
/// (`NewMetaBuildContextWithSctx`): literal temporal defaults are admitted
/// under its mode bits, a literal `TIMESTAMP` is persisted as the UTC
/// projection of its session wall clock, and a utf8mb4 table without a
/// collation takes `@@default_collation_for_utf8mb4`.
///
/// `enable_check_constraint` is Go's `vardef.EnableCheckConstraint.Load()`,
/// which `buildTableInfo` and `columnDefToCol` read. This port keeps that
/// process-wide value per node (`GlobalSysvars::enable_check_constraint`,
/// which a statement context carries), so the caller supplies it.
pub fn build_table_info_in_schema(
    create: &CreateTableStmt,
    schema: &str,
    db_charset: &str,
    db_collate: &str,
    context: &tidb_expr::metabuild::Context,
    enable_check_constraint: bool,
) -> Refusal<TableInfo> {
    let refuse = |what: &str| {
        Err(DdlAdmissionError::plain(format!(
            "CREATE TABLE {what} is not supported by this node"
        )))
    };
    let temporary = crate::validate_temporary_table_create(create)
        .map_err(default_admission_error)?;
    if create.like_table.is_some() {
        return refuse("... LIKE");
    }
    if create.ctas.is_some() {
        return refuse("... AS <query>");
    }
    if create.columns.is_empty() {
        return Err(DdlAdmissionError::plain("CREATE TABLE declares no columns"));
    }

    // Go `BuildTableInfoWithStmt`: the table options' charset and collation,
    // then the database's, then the server default.
    let default_utf8mb4_coll = context.get_default_collation_for_utf8mb4().to_owned();
    let (table_charset, table_collate) =
        crate::column_types::get_charset_and_collate_in_table_option(
            0,
            &create.table_options,
            &default_utf8mb4_coll,
        )
        .map_err(default_admission_error)?;
    let (table_charset, table_collate) = crate::column_types::resolve_charset_collation(
        &[(&table_charset, &table_collate), (db_charset, db_collate)],
        &default_utf8mb4_coll,
    )
    .map_err(default_admission_error)?;

    // Go `handleTableOptions` validates ENGINE_ATTRIBUTE and its
    // STORAGE_CLASS sugar before it looks at any other option.
    let engine_attribute = crate::storage_class::engine_attribute_from_table_options(
        &create.table_options,
    )
    .map_err(default_admission_error)?;
    let mut comment = String::new();
    let mut auto_inc_id = 0i64;
    let mut auto_rand_id = 0i64;
    let mut auto_id_cache = 0i64;
    let mut compression = String::new();
    let mut affinity = None;
    let mut shard_row_id_bits = None;
    let mut pre_split_regions = None;
    for option in &create.table_options {
        match option {
            // Settled above (`GetCharsetAndCollateInTableOption`).
            TableOption::CharacterSet(_) | TableOption::Collate(_) => {}
            TableOption::Comment(text) => comment = text.clone(),
            // Go `handleTableOptions`: the parsed value is unsigned, and
            // anything past int64 is refused with its own message.
            TableOption::AutoIdCache(value) => {
                let parsed = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain("AUTO_ID_CACHE needs an integer value")
                })?;
                if parsed > i64::MAX as u64 {
                    return Err(DdlAdmissionError::plain(
                        "table option auto_id_cache overflows int64",
                    ));
                }
                auto_id_cache = parsed as i64;
            }
            // Go `handleTableOptions`: `tbInfo.Compression = op.StrValue`.
            TableOption::Compression(value) => compression = value.clone(),
            // Go parses `FORCE AUTO_INCREMENT` into the same
            // `TableOptionAutoIncrement`, and CREATE TABLE reads only its value.
            // `tbInfo.AutoIncID = int64(op.UintValue)`: a value past i64::MAX
            // wraps, as Go's conversion does.
            TableOption::AutoIncrement(value) | TableOption::ForceAutoIncrement(value) => {
                auto_inc_id = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain(format!(
                        "CREATE TABLE AUTO_INCREMENT = {value} is not an integer this node can store"
                    ))
                })? as i64;
            }
            TableOption::AutoRandomBase(value) | TableOption::ForceAutoRandomBase(value) => {
                auto_rand_id = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain(format!(
                        "CREATE TABLE AUTO_RANDOM_BASE = {value} is not an integer this node can store"
                    ))
                })? as i64;
            }
            // Go's own `handleTableOptions` records nothing for these: the
            // storage engine is always InnoDB in name only, and the statistics
            // options are MySQL compatibility no-ops.
            TableOption::Engine(_) | TableOption::StatsPersistent | TableOption::PackKeys => {}
            // The policy REFERENCE is stamped by the planner, which holds the
            // snapshot the name resolves against (`plan_ddl`'s CreateTable
            // arm). This builder only has the statement, so it records
            // nothing here rather than refusing an option the statement can
            // in fact honour.
            TableOption::PlacementPolicy(_) => {}
            // Validated above and applied once the table is built
            // (`handleEngineAttributeForCreateTable`).
            TableOption::EngineAttribute(_) | TableOption::StorageClass(_) => {}
            // Go `handleTableOptions`' TTL arm, applied below.
            TableOption::Ttl { .. }
            | TableOption::TtlEnable(_)
            | TableOption::TtlJobInterval(_) => {}
            // Go `handleTableOptions`, applied once the handle is known.
            TableOption::ShardRowIdBits(value) => {
                shard_row_id_bits = Some(value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain("SHARD_ROW_ID_BITS needs an integer value")
                })?);
            }
            TableOption::PreSplitRegions(value) => {
                pre_split_regions = Some(value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain("PRE_SPLIT_REGIONS needs an integer value")
                })?);
            }
            // Go `handleTableOptions`: `NewTableAffinityInfoWithLevel`, the
            // last one winning.
            TableOption::Affinity(level) => {
                affinity =
                    crate::table_affinity(level).map_err(default_admission_error)?;
            }
            // Every other option is outside Go `handleTableOptions`' switch:
            // MySQL compatibility it accepts and records nowhere (the parser
            // already warned about the ones it ignores).
            _ => {}
        }
    }
    // Go `buildColumnsAndConstraints`: the table-level PRIMARY KEY is located
    // first because every column needs to know whether it is one of its keys.
    let mut constraints = Vec::new();
    let check_constraints = if enable_check_constraint {
        crate::check_constraint::create_inputs(create)
    } else {
        Vec::new()
    };
    // go `BuildHiddenColumnInfo` runs per table-level index constraint: an
    // expression key part becomes one hidden VIRTUAL generated column, so
    // the AST of each part is captured here and synthesized once every real
    // column exists (the hidden column's dependencies and the anonymous
    // index's name resolve against the completed column list).
    let mut constraint_exprs: Vec<Vec<Option<tidb_ast::Expr>>> = Vec::new();
    for constraint in &create.table_constraints {
        // Go `ast.ConstraintCheck` with the flag off — the DEFAULT — warns
        // and skips (`ddl/create_table.go:1470`), exactly like the
        // column-level spelling.
        if let TableConstraint::Check(_) = constraint {
            if !enable_check_constraint {
                context.append_warning(err_check_constraint_is_off());
            }
            continue;
        }
        // Go `buildTableInfo` builds FOREIGN KEY constraints as FKInfo, not
        // as an index constraint: see `foreign_key_build::build_fk_infos`.
        if let TableConstraint::ForeignKey(_) = constraint {
            continue;
        }
        constraint_exprs.push(table_constraint_expr_parts(constraint));
        constraints.push(lower_table_constraint(constraint)?);
    }
    let out_primary_key: Option<Constraint> = constraints
        .iter()
        .find(|constraint| constraint.kind == ConstraintKind::PrimaryKey)
        .cloned();

    let mut columns = Vec::with_capacity(create.columns.len());
    let mut inline_slots = Vec::new();
    for (offset, column) in create.columns.iter().enumerate() {
        let (info, inline) = build_column(
            offset,
            column,
            out_primary_key.as_ref(),
            &table_charset,
            &table_collate,
            context,
            enable_check_constraint,
            GeneratedColumns::Deferred {
                preceding: &columns,
                schema,
                table: create.name.last().map_or("", String::as_str),
            },
        )?;
        // go `buildColumnsAndConstraints` warns `ErrWarnDeprecatedZerofill`
        // (1681) for every ZEROFILL column it builds -- and go's YEAR type
        // carries `mysql.ZerofillFlag` intrinsically (`defaultTypeForYear`),
        // so a bare `c_yr YEAR` column warns it too (oracle: the 25-column
        // CREATE with exactly one YEAR column warns exactly once). ADD COLUMN
        // and MODIFY COLUMN build their column elsewhere and never warn it.
        if column.ty.zerofill || info.field_type.code() == FieldTypeCode::Year {
            context.append_warning(WarnErr::Terror(
                tidb_util::dbterror::ERR_WARN_DEPRECATED_ZEROFILL.clone(),
            ));
        }
        // An inline PRIMARY KEY / UNIQUE becomes a constraint of its own, in
        // the position Go appends it: after every table-level one. Inline
        // constraints carry no key parts, so they contribute no expression
        // parts to the hidden-column synthesis.
        for (option, constraint) in inline {
            inline_slots.push((offset, option, constraints.len()));
            constraints.push(constraint);
            constraint_exprs.push(Vec::new());
        }
        columns.push(info);
    }

    // Go `checkConstraintNames`, run right after `buildColumnsAndConstraints`:
    // duplicate explicit names fail (1061, 1826, 3822) and every anonymous key
    // is named -- before the hidden columns of an expression index, which are
    // named after their index.
    let settled = crate::constraint_names::check_constraint_names(create, enable_check_constraint)
        .map_err(|error| {
            DdlAdmissionError::from_terror(&error.proto.generate_with_stack(error.message))
        })?;
    let table_index_names = create
        .table_constraints
        .iter()
        .enumerate()
        .filter(|(_, constraint)| matches!(constraint, TableConstraint::Index(_)))
        .map(|(index, _)| settled.table_constraint(index));
    for (constraint, name) in constraints.iter_mut().zip(table_index_names) {
        if let Some(name) = name {
            name.clone_into(&mut constraint.name);
        }
    }
    for (column, option, position) in inline_slots {
        if let Some(name) = settled.inline_key(column, option) {
            name.clone_into(&mut constraints[position].name);
        }
    }

    // Go `setColumnFlagWithConstraint` runs in `buildColumnsAndConstraints`,
    // before `buildTableInfo` builds any hidden column, and skips every
    // expression key part (`key.Expr != nil`): an expression index's hidden
    // column carries no key flag. The expression parts are still unnamed
    // here, so they match no column.
    for constraint in &constraints {
        stamp_constraint_flags(&mut columns, constraint)?;
    }

    // go `BuildHiddenColumnInfo` (pkg/ddl/create_table.go:1590): every
    // expression index part is lowered to a hidden VIRTUAL generated column
    // `_V$_<index>_<part-position>` and the key part points at it. Runs now
    // — after every declared column exists — because the hidden column's
    // dependency check and the anonymous index's name both resolve against
    // the completed column list, exactly as go interleaves them.
    synthesize_expression_index_columns(
        &mut columns,
        &mut constraints,
        &constraint_exprs,
        context,
    )?;

    let mut seen: Vec<String> = Vec::with_capacity(columns.len());
    for column in &columns {
        let lowercase = column.name.lowercase().to_owned();
        if seen.contains(&lowercase) {
            // Go `ErrDupFieldName` (1060): `Duplicate column name '%-.192s'`
            // -- the lowercased collision is reported by its original name.
            return Err(DdlAdmissionError::from_terror(
                &tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                    .generate_with_stack_by_args(&[column.name.original().into()]),
            ));
        }
        seen.push(lowercase);
    }

    let mut table = build_table(
        create,
        columns,
        constraints,
        table_charset,
        table_collate,
        context,
    )?;
    // Go `buildTableInfo`: FOREIGN KEY -> FKInfo (plus its auto index) runs
    // inside the constraint loop, before `BuildTableInfo` appends the check
    // constraints that read `tbInfo.ForeignKeys`.
    crate::foreign_key::build_fk_infos(create, schema, &mut table)?;
    append_check_constraints(
        &mut table,
        &check_constraints,
        crate::check_constraint::CheckConstraintBuildMode::Create,
        context,
    )?;
    table.temp_table_type = temporary;
    let handle_offsets = if table.pk_is_handle {
        table
            .columns
            .iter_deref()
            .position(|column| column.read().field_type.has_flag(FieldTypeFlags::PRI_KEY))
            .into_iter()
            .collect::<Vec<_>>()
    } else if table.is_common_handle {
        table
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                index.primary.then(|| {
                    index
                        .columns
                        .iter_deref()
                        .map(|column| column.read().offset as usize)
                        .collect::<Vec<_>>()
                })
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let fields = table
        .columns
        .iter_deref()
        .map(|column| column.read().field_type.clone())
        .collect::<Vec<_>>();
    if let Some(spec) = crate::auto_random::validate(create, &fields, &handle_offsets)
        .map_err(default_admission_error)?
    {
        table.auto_random_bits = spec.shard_bits;
        table.auto_random_range_bits = spec.range_bits;
    }
    // Go `buildTableInfoWithCheck` validates the table comment right after
    // `handleTableOptions`, under the statement's SQL mode.
    table.comment = crate::validate_comment_length(
        &comment,
        &tidb_hack::go_to_lower(table.name.original()),
        crate::CommentOwner::Table,
        context,
    )
    .map_err(default_admission_error)?;
    // Go `buildTableInfoWithCheck`: a heap table that is not temporary takes
    // the session's `tidb_shard_row_id_bits` / `tidb_pre_split_regions`,
    // which `handleTableOptions` then overrides.
    let clustered = table.pk_is_handle || table.is_common_handle;
    if !clustered && temporary == tidb_model::TempTableType::NONE {
        table.shard_row_id_bits = context.get_shard_row_id_bits();
        table.max_shard_row_id_bits = table.shard_row_id_bits;
        table.pre_split_regions = context.get_pre_split_regions();
    }
    if let Some(bits) = shard_row_id_bits {
        // A positive count on a table whose rows are keyed by the primary
        // key is refused; otherwise it is clamped, never refused.
        if bits > 0 && clustered {
            return Err(default_admission_error(
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_UNSUPPORTED_SHARD_ROW_ID_BITS
                        .generate_with_stack_by_args(&[]),
                ),
            ));
        }
        table.shard_row_id_bits = bits.min(tidb_vardef::tidb_vars::MAX_SHARD_ROW_ID_BITS);
        table.max_shard_row_id_bits = table.shard_row_id_bits;
    }
    if let Some(regions) = pre_split_regions {
        table.pre_split_regions = regions;
    }
    if let Some(engine_attribute) = engine_attribute {
        if let Some(settings) =
            crate::storage_class::storage_class_settings_of(&engine_attribute)
                .map_err(default_admission_error)?
        {
            let (tier, transitions) =
                crate::storage_class::build_storage_class_for_table(&settings);
            table.storage_class_tier = tier;
            table.storage_class_transitions = transitions.into();
        }
        table.engine_attribute = engine_attribute;
    }
    table.auto_inc_id = auto_inc_id;
    table.auto_id_cache = auto_id_cache;
    table.compression = compression;
    table.auto_rand_id = auto_rand_id;
    // Go `handleTableOptions`' last step (`shardingBits`): PRE_SPLIT_REGIONS
    // never exceeds the table's sharding bits, SHARD_ROW_ID_BITS else
    // AUTO_RANDOM's.
    let sharding_bits = if table.shard_row_id_bits > 0 {
        table.shard_row_id_bits
    } else {
        table.auto_random_bits
    };
    if table.pre_split_regions > sharding_bits {
        table.pre_split_regions = sharding_bits;
    }
    // Go `BuildIndexInfo` calls `setGlobalIndexVersion` after every index has
    // been resolved against the completed table metadata.
    for index in table.indices.iter_deref() {
        crate::index::set_global_index_version(&table, &mut index.write());
    }
    // Go `buildTablePartitionInfo`, called LAST because its unique-key rule
    // (8264/1503) reads the table's finished index list.
    //
    // The physical ids stay zero here. Go allocates them when the DDL job is
    // submitted -- one per definition, right after the table's own
    // (`ddl/jobsubmit/submit.go` `assignIDsForTable`) -- so a builder that
    // invented them would hand out ids the cluster's allocator never issued.
    if create.partitioning.is_some() {
        if temporary != tidb_model::TempTableType::NONE {
            return Err(default_admission_error(
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_PARTITION_NO_TEMPORARY
                        .generate_with_stack_by_args(&[]),
                ),
            ));
        }
        // Go `checkPartitionType` ignores LINEAR and builds the plain
        // HASH/KEY table, warning that it did so (8200).
        if let Some(message) = crate::table_partition::linear_partitioning_warning(create) {
            context.append_warning(WarnErr::Terror(
                tidb_util::dbterror::ERR_UNSUPPORTED_CREATE_PARTITION.generate(message),
            ));
        }
        let names = table
            .columns
            .iter_deref()
            .map(|column| column.read().name.original().to_owned())
            .collect::<Vec<_>>();
        let indexes = table
            .indices
            .iter_deref()
            .map(|index| index.read().clone())
            .collect::<Vec<_>>();
        // `handle_offsets` is the CLUSTERED primary key's, already resolved
        // above: it is no entry in `indices` -- its encoding IS the row key
        // -- which is why Go reports the unique-key rule against
        // `CLUSTERED INDEX` rather than against an index name.
        if let Some((mut metadata, spec)) = crate::table_partition::build_partition_metadata(
            create,
            &names,
            &fields,
            &indexes,
            &handle_offsets,
            &mut || 0,
            context,
        )
        .map_err(partition_refusal)?
        {
            // Go `buildPartitionDefinitionsInfo` ->
            // `rebuildStorageClassForPartitions`.
            if let Some(classes) = crate::storage_class::partition_storage_classes(
                &table.engine_attribute,
                &spec,
                context,
            )
            .map_err(default_admission_error)?
            {
                for (definition, class) in metadata.definitions.iter_mut().zip(classes) {
                    definition.storage_class = class;
                }
            }
            table.partition = Some(GoShared::new(partition_info(&metadata)));
            // Go `buildTablePartitionInfo`.
            if table
                .indices
                .iter_deref()
                .any(|index| index.read().has_condition())
            {
                return Err(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
                        .generate_with_stack_by_args(&[
                            "partial index is not supported on partitioned table".into(),
                        ]),
                ));
            }
        }
    }
    // Go `validateTableAffinity`, right after `buildTablePartitionInfo`.
    crate::validate_table_affinity(
        temporary != tidb_model::TempTableType::NONE,
        table.get_partition_info().is_some(),
        affinity.as_ref(),
    )
    .map_err(default_admission_error)?;
    table.affinity = affinity.map(GoShared::new);
    // Go `columnDefToCol` records each generated column's expression and
    // dependences; `checkGeneratedColumn` (`checkTableInfoValidWithStmt`'s
    // first step) is enforced by the same build
    // (`generated_column::build_generated_columns*`), so it runs here.
    check_generated_columns(create, &mut table, context)?;
    // Go `handleTableOptions` records the TTL definition;
    // `checkTableInfoValidWithStmt` validates it.
    table.ttl_info = crate::create_table_ttl_info(&create.table_options)
        .map_err(default_admission_error)?
        .map(GoShared::new);
    Ok(table)
}

/// Go `BuildTableInfoWithLike` (`create_table.go`): `refer`'s public
/// definition named `schema`.`table`, with the statement's temporary kind
/// (`setTemporaryType`) and Go's reset list -- the auto-increment counter,
/// the foreign keys, the cache status and the TiFlash replica's
/// availability -- and the CHECK constraints renamed for the new table.
pub fn build_table_info_with_like(
    schema: &str,
    table: &str,
    refer: &TableInfo,
    temporary: tidb_ast::CreateTableTemporary,
    on_commit_delete: bool,
) -> Refusal<TableInfo> {
    // Go names the TARGET schema with the source's name here
    // (`ErrWrongObject.GenWithStackByArgs(ident.Schema, referTblInfo.Name,
    // "BASE TABLE")`).
    if refer.is_sequence() || refer.is_view() {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_WRONG_OBJECT.generate_with_stack(format!(
                "'{schema}.{}' is not BASE TABLE",
                refer.name.original()
            )),
        ));
    }
    let temp_table_type = crate::set_temporary_type(temporary, on_commit_delete)
        .map_err(DdlAdmissionError::from_mysql_error)?;
    let mut info = refer.clone_like_go();
    info.temp_table_type = temp_table_type;
    // Go `referTblInfo.Cols()` and the PUBLIC indices: a column or index
    // still being added is not part of the definition being copied.
    info.columns = tidb_model::GoSharedPointerSlice::from_handles(
        info.columns
            .iter_deref()
            .filter(|column| column.read().state == SchemaState::PUBLIC)
            .map(Some)
            .collect(),
    );
    info.indices = tidb_model::GoSharedPointerSlice::from_handles(
        info.indices
            .iter_deref()
            .filter(|index| index.read().state == SchemaState::PUBLIC)
            .map(Some)
            .collect(),
    );
    info.name = CiString::new(table.to_owned());
    info.auto_inc_id = 0;
    info.foreign_keys = tidb_model::GoSharedPointerSlice::from_handles(Vec::new());
    info.table_cache_status_type = tidb_model::TableCacheStatusType::DISABLE;
    if temporary == tidb_ast::CreateTableTemporary::None {
        if let Some(replica) = &info.tiflash_replica {
            // Go copies the replica before clearing availability, so the
            // source's metadata stays as it is.
            let mut replica = replica.read().clone();
            replica.available = false;
            replica.available_partition_ids = Default::default();
            info.tiflash_replica = Some(GoShared::new(replica));
        }
    } else {
        // A temporary table has no TiFlash replica, TTL or affinity.
        info.tiflash_replica = None;
        info.ttl_info = None;
        info.affinity = None;
    }
    // Go `renameCheckConstraint`: target-local names from `<table>_chk_1` in
    // declaration order, pointed at the target table.
    for (offset, constraint) in info.constraints.iter_deref().enumerate() {
        let mut constraint = constraint.write();
        constraint.name =
            CiString::new(format!("{}_chk_{}", tidb_hack::go_to_lower(table), offset + 1));
        constraint.table = CiString::new(table.to_owned());
    }
    Ok(info)
}

/// Go `CreateTable`'s region split loop (`executor.go:1053`), after
/// `checkTableInfoValidWithStmt`: each `SPLIT ...` clause normalized
/// (`normalizeSplitPolicy`) and recorded where it says -- the table's own
/// keyspace or the named index.
pub fn set_create_table_split_policies(
    create: &CreateTableStmt,
    tb_info: &mut TableInfo,
) -> Refusal<()> {
    for split in &create.splits {
        let target = match &split.target {
            tidb_ast::CreateTableSplitTarget::Table => crate::split_region::SplitPolicyTarget::Table,
            tidb_ast::CreateTableSplitTarget::PrimaryKey => {
                crate::split_region::SplitPolicyTarget::PrimaryKey
            }
            tidb_ast::CreateTableSplitTarget::Index(name) => {
                crate::split_region::SplitPolicyTarget::Index(name)
            }
        };
        let (index_name, policy) =
            crate::split_region::normalize_split_policy(target, &split.option, tb_info)
                .map_err(default_admission_error)?;
        set_region_split_policy(tb_info, &index_name, policy);
    }
    Ok(())
}

/// Go `checkTableInfoValidWithStmt` (`create_table.go:511`), run on the
/// built `TableInfo` of a CREATE TABLE -- written out or copied by `LIKE`.
///
/// Two of its steps are enforced where this port builds what they read:
/// `checkGeneratedColumn` by [`build_table_info_in_schema`]'s generated
/// column build, and the partition checks (`checkPartitionDefinitionConstraints`,
/// `rebuildStorageClassForPartitions`, `checkPartitionFuncType`,
/// `checkPartitioningKeysConstraints`) by its partition build. A `LIKE` copy
/// carries none of either: its statement declares no columns or partitions.
/// Go's statement argument feeds only those two, so it is not taken here.
///
/// # Errors
///
/// `ErrTableWithoutPrimaryKey` (3750) under `sql_require_primary_key`, or
/// `checkTTLInfoValid`'s.
pub fn check_table_info_valid_with_stmt(
    ctx: &tidb_expr::metabuild::Context,
    tb_info: &TableInfo,
) -> Refusal<()> {
    // Check if table has a primary key if required.
    if ctx.primary_key_required() && tb_info.get_pk_name().original().is_empty() {
        return Err(DdlAdmissionError::from_terror(
            &tidb_error::catalog::infoschema::ERR_TABLE_WITHOUT_PRIMARY_KEY
                .generate_with_stack_by_args(&[]),
        ));
    }
    if tb_info.ttl_info.is_some() {
        // Whether another table's foreign key refers to this one is the
        // infoschema's to say; a table being created has no such referrer.
        check_ttl_info_valid(tb_info, false).map_err(default_admission_error)?;
    }
    Ok(())
}

/// Go `checkTableInfoValidExtra` (`create_table.go:649`): a table about to be
/// created, from an untrusted source, checked for name length, duplicate and
/// too-long columns, column and index counts, column attributes, global
/// indexes, its charset and collation, and then `checkTableInfoValid`.
/// `append_warning` is Go's `ec errctx.Context`.
///
/// Go's `checkColumnarIndexIfNeedTiFlashReplica` returns at once for a table
/// with no columnar index, which is every table this port builds.
///
/// # Errors
///
/// The first of Go's errors the table trips.
pub fn check_table_info_valid_extra(
    append_warning: &mut dyn FnMut(WarnErr),
    tb_info: &TableInfo,
) -> Refusal<()> {
    crate::check_too_long_identifier(tb_info.name.original()).map_err(default_admission_error)?;
    let columns = tb_info.columns.iter_deref().collect::<Vec<_>>();
    check_duplicate_column(&columns)?;
    for column in &columns {
        crate::check_too_long_identifier(column.read().name.original())
            .map_err(default_admission_error)?;
    }
    check_too_many_columns(columns.len())?;
    check_too_many_indexes(tb_info.indices.len()).map_err(default_admission_error)?;
    for column in &columns {
        let column = column.read();
        crate::column_field_type::check_column_attributes(&column.field_type).map_err(|error| {
            DdlAdmissionError::from_mysql_error(error.refusal(column.name.original()))
        })?;
    }
    check_global_indexes(append_warning, tb_info)?;
    check_charset_and_collation(&tb_info.charset, &tb_info.collate)?;
    check_table_info_valid(tb_info)
}

/// Go `checkDuplicateColumn`: `ErrColumnExists` (1060) naming the second
/// column whose lowercase name was already seen.
fn check_duplicate_column(cols: &[GoShared<ColumnInfo>]) -> Refusal<()> {
    let mut col_names = std::collections::HashSet::with_capacity(cols.len());
    for col in cols {
        let col = col.read();
        if !col_names.insert(col.name.lowercase().to_owned()) {
            return Err(DdlAdmissionError::from_terror(
                &tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                    .generate_with_stack_by_args(&[col.name.original().into()]),
            ));
        }
    }
    Ok(())
}

/// Go `checkTooManyColumns`: `ErrTooManyFields` (1117) above
/// `config.TableColumnCountLimit`.
pub(crate) fn check_too_many_columns(column_count: usize) -> Refusal<()> {
    let limit = tidb_config::config_tree::config::get_global_config().table_column_count_limit;
    if u64::try_from(column_count).unwrap_or(u64::MAX) > u64::from(limit) {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_TOO_MANY_FIELDS.generate_with_stack_by_args(&[]),
        ));
    }
    Ok(())
}

/// Go `checkTooManyIndexes`: `ErrTooManyKeys` (1069) above the configured
/// `index-limit`.
///
/// # Errors
///
/// 1069 naming the limit.
pub fn check_too_many_indexes(index_count: usize) -> Result<(), MysqlError> {
    let limit = tidb_config::config_tree::config::get_global_config().index_limit;
    if i64::try_from(index_count).unwrap_or(i64::MAX) > limit {
        return Err(MysqlError::from_terror(
            &tidb_util::dbterror::ERR_TOO_MANY_KEYS.generate_with_stack_by_args(&[limit.into()]),
        ));
    }
    Ok(())
}

/// Go `checkGlobalIndexes` -> `checkGlobalIndex`: a GLOBAL index needs a
/// partitioned table, and one with a prefix or VIRTUAL generated part warns
/// that auto analyze cannot sample it
/// (`validateGlobalIndexWithGeneratedColumns`).
fn check_global_indexes(
    append_warning: &mut dyn FnMut(WarnErr),
    tb_info: &TableInfo,
) -> Refusal<()> {
    let is_partitioned = tb_info
        .get_partition_info()
        .is_some_and(|partition| partition.read().partition_type != tidb_ast::PartitionType::NONE);
    for index_info in tb_info.indices.iter_deref() {
        let index_info = index_info.read();
        if !index_info.global {
            continue;
        }
        if !is_partitioned {
            return Err(DdlAdmissionError::from_terror(
                &tidb_util::dbterror::ERR_GENERAL_UNSUPPORTED_DDL
                    .generate_with_stack_by_args(&["Global Index on non-partitioned table".into()]),
            ));
        }
        validate_global_index_with_generated_columns(append_warning, tb_info, &index_info);
    }
    Ok(())
}

/// Go `validateGlobalIndexWithGeneratedColumns`: auto analyze is not
/// effective when a global index contains prefix columns or virtual generated
/// columns, so Go warns once.
fn validate_global_index_with_generated_columns(
    append_warning: &mut dyn FnMut(WarnErr),
    tb_info: &TableInfo,
    index_info: &IndexInfo,
) {
    for col in index_info.columns.iter_deref() {
        let col = col.read();
        let is_virtual_generated = usize::try_from(col.offset)
            .ok()
            .and_then(|offset| tb_info.columns.get(offset))
            .is_some_and(|column| column.read().is_virtual_generated());
        let is_prefix_col = col.length != tidb_datatype::UNSPECIFIED_LENGTH;
        if is_virtual_generated || is_prefix_col {
            append_warning(WarnErr::Terror(
                tidb_util::dbterror::ERR_WARN_GLOBAL_INDEX_NEED_MANUALLY_ANALYZE.generate(format!(
                    "Auto analyze is not effective for index '{}', need analyze manually",
                    index_info.name.original()
                )),
            ));
            return;
        }
    }
}

/// Go `checkCharsetAndCollation` (`executor.go:1546`).
fn check_charset_and_collation(cs: &str, co: &str) -> Refusal<()> {
    if !tidb_datatype::valid_charset_and_collation(cs, co) {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNKNOWN_CHARACTER_SET
                .generate_with_stack_by_args(&[cs.into()]),
        ));
    }
    if !co.is_empty() {
        crate::column_types::get_collation_by_name(co).map_err(default_admission_error)?;
    }
    Ok(())
}

/// Go `checkTableInfoValid`: Go also loads the table (`TableFromMeta`), which
/// this port's loader does when the table is opened; the observable half is
/// `checkInvisibleIndexOnPK`, since a change can turn an invisible unique
/// index into the implicit primary key.
///
/// # Errors
///
/// 3522 for an invisible primary key.
pub fn check_table_info_valid(tbl_info: &TableInfo) -> Refusal<()> {
    check_invisible_index_on_pk(tbl_info)
}

/// Go `checkInvisibleIndexOnPK` (`executor.go`): a table whose primary key
/// is an index (not the row handle) may not make it invisible.
///
/// # Errors
///
/// 3522 for an invisible primary key.
pub fn check_invisible_index_on_pk(tbl_info: &TableInfo) -> Refusal<()> {
    if tbl_info.pk_is_handle {
        return Ok(());
    }
    if tbl_info
        .get_primary_key()
        .is_some_and(|primary| primary.read().invisible)
    {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_PK_INDEX_CANT_BE_INVISIBLE.generate_with_stack_by_args(&[]),
        ));
    }
    Ok(())
}

/// Records a normalized split policy where Go's DDL puts it:
/// `TableInfo.TableSplitPolicy` for the table's keyspace, else the named
/// index's `RegionSplitPolicy`.
pub fn set_region_split_policy(
    table: &mut TableInfo,
    index_name: &str,
    policy: tidb_model::RegionSplitPolicy,
) {
    if index_name.is_empty() {
        table.table_split_policy = Some(GoShared::new(policy));
        return;
    }
    if let Some(index) = table
        .indices
        .iter_deref()
        .find(|index| index.read().name.lowercase() == index_name)
    {
        index.write().region_split_policy = Some(GoShared::new(policy));
    }
}

/// Go `checkTTLInfoValid`, in its order: a temporary table has no TiKV rows to
/// expire (8151); the interval must evaluate (`checkTTLIntervalExpr`); a
/// clustered composite primary key may not hold a FLOAT / DOUBLE column
/// (`checkPrimaryKeyForTTLTable`, 8153); a table another table's foreign key
/// refers to cannot take one (8152); and the column must exist and be DATE,
/// DATETIME or TIMESTAMP (`checkTTLInfoColumnType`, 1054 / 8148).
pub fn check_ttl_info_valid(
    table: &TableInfo,
    referred_by_foreign_key: bool,
) -> Result<(), MysqlError> {
    let Some(ttl) = table.ttl_info.as_ref().map(|ttl| ttl.read().clone()) else {
        return Ok(());
    };
    if table.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_TEMP_TABLE_NOT_ALLOWED_WITH_TTL
                .generate_with_stack_by_args(&[]),
        ));
    }
    tidb_ttl::eval_expire_time(
        &chrono::Utc::now(),
        &ttl.interval_expr_str,
        tidb_ttl::TimeUnitType::from_i64(ttl.interval_time_unit),
    )
    .map_err(|error| tidb_error::mysql::MysqlError::unknown(error.to_string()))?;
    if table.is_common_handle {
        if let Some(primary) = table
            .indices
            .iter_deref()
            .find(|index| index.read().primary)
        {
            let float_key = primary.read().columns.iter_deref().any(|part| {
                tidb_model::column::find_column_info(&table.columns, part.read().name.original())
                    .is_some_and(|column| {
                        matches!(
                            column.read().field_type.code(),
                            FieldTypeCode::Float | FieldTypeCode::Double
                        )
                    })
            });
            if float_key {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_UNSUPPORTED_PRIMARY_KEY_TYPE_WITH_TTL
                        .generate_with_stack_by_args(&[]),
                ));
            }
        }
    }
    if referred_by_foreign_key {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK
                .generate_with_stack_by_args(&[]),
        ));
    }
    let named = ttl.column_name.original();
    match tidb_model::column::find_column_info(&table.columns, named) {
        None => Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_BAD_FIELD
                .generate_with_stack_by_args(&[named.to_owned().into(), "TTL config".into()]),
        )),
        Some(column) if !column.read().field_type.code().is_type_time() => {
            Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_COLUMN_IN_TTL_CONFIG
                    .generate_with_stack_by_args(&[named.to_owned().into()]),
            ))
        }
        Some(_) => Ok(()),
    }
}

/// Go `checkTableInfoValidWithStmt` -> `checkGeneratedColumn`: once the table
/// is built, every generated expression is resolved against ALL its declared
/// columns -- a generated column may read a LATER ordinary one (`b int as
/// (c+1), c int` is accepted), only an earlier generated one (3107), never an
/// unknown name (1054), nor the AUTO_INCREMENT column unless
/// `tidb_enable_auto_increment_in_generated` allows it (3109). The resolved
/// text and dependences replace what `columnDefToCol` recorded.
fn check_generated_columns(
    create: &CreateTableStmt,
    table: &mut TableInfo,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    if !create.columns.iter().any(|column| {
        column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Generated { .. }))
    }) {
        return Ok(());
    }
    // The declared columns lead `Columns` in definition order; hidden
    // expression-index columns follow them.
    let declared = table
        .columns
        .iter_deref()
        .take(create.columns.len())
        .collect::<Vec<_>>();
    let names = declared
        .iter()
        .map(|column| column.read().name.original().to_owned())
        .collect::<Vec<_>>();
    let types = declared
        .iter()
        .map(|column| column.read().field_type.clone())
        .collect::<Vec<_>>();
    let built = crate::generated_column::build_generated_columns_with_like_default_escape(
        &create.columns,
        &names,
        &types,
        &context.get_expr_ctx().get_eval_ctx().time_zone(),
        context.get_expr_ctx().get_eval_ctx().like_default_escape(),
        context.enable_auto_increment_in_generated(),
        context.get_expr_ctx().get_charset_info().0,
    )
    .map_err(generated_column_admission_error)?;
    for (column, generated) in declared.iter().zip(built) {
        let Some(generated) = generated else {
            continue;
        };
        let mut column = column.write();
        column.generated_expr_string = generated.expr_text;
        column.generated_stored = generated.stored;
        // Go `findDependedColumnNames` keys the set by `Name.L`.
        for dependency in generated.dependencies {
            column
                .dependences
                .insert(tidb_hack::go_to_lower(&dependency));
        }
    }
    Ok(())
}

/// Go `BuildIndexInfo`'s partial-index half for `index` on `table`:
/// `CheckAndBuildIndexConditionString`, then `buildAffectColumn` over the
/// stored condition. An index with no condition is left as it is.
pub fn build_index_condition(
    table: &TableInfo,
    index: &mut IndexInfo,
    condition: Option<&Expr>,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    let column = |name: &str| {
        tidb_model::column::find_column_info(&table.columns, name).map(|column| {
            let column = column.read();
            crate::index::IndexConditionColumn {
                field_type: column.field_type.clone(),
                generated: column.is_generated(),
            }
        })
    };
    index.condition_expr_string = crate::index::check_and_build_index_condition_string(
        table.partition.is_some(),
        &column,
        condition,
        context.get_expr_ctx().get_charset_info(),
    )
    .map_err(default_admission_error)?;
    if index.has_condition() {
        let columns =
            tidb_table::kv_table::extract_columns_from_index_condition(index, table, true)
                .map_err(|error| DdlAdmissionError::plain(error))?;
        index.affect_column = tidb_model::GoSharedPointerSlice::from_handles(
            tidb_table::kv_table::dedup_index_columns(columns)
                .into_iter()
                .map(Some)
                .collect(),
        );
    }
    Ok(())
}

pub fn append_check_constraints(
    table: &mut TableInfo,
    checks: &[crate::check_constraint::CheckConstraintInput],
    mode: crate::check_constraint::CheckConstraintBuildMode,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    let names = table
        .indices
        .iter_deref()
        .filter(|_| mode == crate::check_constraint::CheckConstraintBuildMode::Create)
        .map(|index| index.read().name.lowercase().to_owned())
        .chain(
            table
                .constraints
                .iter_deref()
                .map(|constraint| constraint.read().name.lowercase().to_owned()),
        )
        .collect::<Vec<_>>();
    let columns = table
        .columns
        .iter_deref()
        .map(|column| column.read().clone())
        .collect::<Vec<_>>();
    let foreign_keys = table
        .foreign_keys
        .iter_deref()
        .map(|foreign_key| {
            let foreign_key = foreign_key.read();
            crate::check_constraint::CheckConstraintForeignKey {
                columns: foreign_key
                    .cols
                    .snapshot()
                    .into_iter()
                    .map(|column| column.original().to_owned())
                    .collect(),
                has_referential_action: foreign_key.on_delete != 0 || foreign_key.on_update != 0,
            }
        })
        .collect::<Vec<_>>();
    let infos = crate::check_constraint::build_constraint_infos(
        &table.name,
        &columns,
        names,
        &foreign_keys,
        checks,
        &mut table.max_constraint_id,
        SchemaState::PUBLIC,
        mode,
        context,
    )
    .map_err(|error| {
        DdlAdmissionError::from_terror(&error.proto.generate_with_stack(error.message))
    })?;
    for info in infos {
        table.constraints.push_go(info);
    }
    Ok(())
}

/// Renders a partition refusal in the words the driver tier raised it with,
/// preserving the MySQL error code a client would have seen.
fn partition_refusal(error: MysqlError) -> DdlAdmissionError {
    DdlAdmissionError::from_mysql_error(error)
}

/// The stored `model.PartitionInfo` for a built clause.
pub fn partition_info(
    metadata: &crate::table_partition::StoredPartitionMetadata,
) -> tidb_model::partition::PartitionInfo {
    let mut partition = tidb_model::partition::PartitionInfo {
        partition_type: metadata.kind,
        expr: metadata.expr.clone(),
        columns: metadata
            .columns
            .iter()
            .map(|name| CiString::new(name.clone()))
            .collect::<Vec<_>>()
            .into(),
        enable: metadata.enable,
        is_empty_columns: metadata.is_empty_columns,
        num: metadata.num,
        ..tidb_model::partition::PartitionInfo::default()
    };
    partition.definitions = metadata
        .definitions
        .iter()
        .map(|definition| {
            let mut stored = tidb_model::partition::PartitionDefinition {
                id: definition.id,
                name: CiString::new(definition.name.clone()),
                ..tidb_model::partition::PartitionDefinition::default()
            };
            stored.less_than = definition.less_than.clone().into();
            stored.in_values = definition
                .in_values
                .iter()
                .map(|tuple| tuple.clone().into())
                .collect::<Vec<_>>()
                .into();
            // Go `buildPartitionDefinitionsInfo` puts the validated comment on
            // the definition it stores (`ddl/partition.go:1576` and `:1670`),
            // and `AppendPartitionDefs` prints it back from there. Leaving it
            // out here meant the comment was length-checked at CREATE and then
            // dropped, so a cluster round trip lost it while the in-process
            // path -- which reads the spec, not this -- still showed it.
            stored.comment = definition.comment.clone();
            // Go `setPartitionPlacementFromOptions` stores the partition's
            // OWN policy reference; an inherited table policy is deliberately
            // not copied down, because the table's rules already cover the
            // partition's range and copying would freeze the cascade.
            stored.placement_policy_ref = definition
                .placement_policy
                .as_ref()
                .map(|reference| tidb_model::GoShared::new(reference.clone()));
            stored.storage_class_tier = definition.storage_class.0.clone();
            stored.storage_class_transitions = definition.storage_class.1.clone().into();
            stored
        })
        .collect::<Vec<_>>()
        .into();
    partition
}

/// One constraint, in the single normalized shape Go's `ast.Constraint` has by
/// the time `BuildTableInfo` sees it — inline column options included.
#[derive(Clone, Debug, PartialEq)]
struct Constraint {
    kind: ConstraintKind,
    name: String,
    parts: Vec<KeyPart>,
    clustered: Option<PrimaryKeyStorage>,
    comment: String,
    invisible: bool,
    global: bool,
    index_type: Option<IndexType>,
    /// A partial index's `WHERE`.
    condition: Option<Expr>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConstraintKind {
    PrimaryKey,
    Unique,
    Key,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KeyPart {
    name: String,
    prefix_len: i64,
    desc: bool,
}

/// Go `CheckPKOnGeneratedColumn`: every key part names a public column, and
/// none is a VIRTUAL generated column -- a hidden one is an expression index
/// (3756), any other is 3106.
fn check_pk_on_generated_column(table: &TableInfo, constraint: &Constraint) -> Refusal<()> {
    for part in &constraint.parts {
        let Some(column) = table.find_public_column_by_name(&tidb_hack::go_to_lower(&part.name))
        else {
            return Err(DdlAdmissionError::from_terror(
                &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS
                    .generate_with_stack_by_args(&[part.name.clone().into()]),
            ));
        };
        let column = column.read();
        if column.is_virtual_generated() {
            if column.hidden {
                return Err(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_PRIMARY_KEY
                        .generate_with_stack_by_args(&[]),
                ));
            }
            return Err(DdlAdmissionError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN
                    .generate_with_stack_by_args(&[
                        "Defining a virtual generated column as primary key".into(),
                    ]),
            ));
        }
    }
    Ok(())
}

fn lower_table_constraint(constraint: &TableConstraint) -> Refusal<Constraint> {
    let TableConstraint::Index(index) = constraint else {
        return Err(DdlAdmissionError::plain(
            "CREATE TABLE CHECK constraints are lowered separately",
        ));
    };
    let kind = match index.kind {
        IndexConstraintKind::PrimaryKey => ConstraintKind::PrimaryKey,
        IndexConstraintKind::Unique
        | IndexConstraintKind::UniqueKey
        | IndexConstraintKind::UniqueIndex => ConstraintKind::Unique,
        IndexConstraintKind::Key | IndexConstraintKind::Index => ConstraintKind::Key,
        // Go `checkFullTextSupportedInStarter`: a classic deployment refuses
        // a FULLTEXT key outright, with its own wording and the 8200 code.
        IndexConstraintKind::Fulltext => {
            return Err(DdlAdmissionError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX.generate_with_stack(
                    "FULLTEXT index is only supported in starter deployment mode",
                ),
            ))
        }
        other => {
            return Err(DdlAdmissionError::plain(format!(
                "CREATE TABLE {other:?} indexes are not supported by this node"
            )))
        }
    };
    // Go `BuildTableInfo`: `CLUSTERED` / `NONCLUSTERED` written on anything
    // but the primary key is `ErrUnsupportedClusteredSecondaryKey`.
    if index.options.primary_key_storage.is_some() && kind != ConstraintKind::PrimaryKey {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_CLUSTERED_SECONDARY_KEY
                .generate_with_stack_by_args(&[]),
        ));
    }
    let mut parts = Vec::with_capacity(index.parts.len());
    for part in &index.parts {
        match part {
            IndexPart::Column {
                name,
                prefix_len,
                desc,
            } => {
                parts.push(KeyPart {
                    name: name.clone(),
                    prefix_len: prefix_len.unwrap_or(UNSPECIFIED_LENGTH),
                    desc: *desc,
                });
            }
            IndexPart::Expr { desc, .. } => {
                // go `BuildHiddenColumnInfo`: the expression part becomes a
                // hidden generated column; the placeholder name is filled
                // by `synthesize_expression_index_columns` once every
                // declared column exists. Position is what matters here.
                parts.push(KeyPart {
                    name: String::new(),
                    prefix_len: UNSPECIFIED_LENGTH,
                    desc: *desc,
                });
            }
        }
    }
    Ok(Constraint {
        kind,
        // An anonymous key is named by `checkConstraintNames` once every
        // column exists.
        name: index.name.clone().unwrap_or_default(),
        parts,
        clustered: index.options.primary_key_storage,
        comment: index.options.comment.clone().unwrap_or_default(),
        invisible: index.options.visibility == Some(tidb_ast::IndexVisibility::Invisible),
        global: index.options.global,
        index_type: index_type_of(&index.options),
        condition: index.options.condition.clone(),
    })
}

fn index_type_of(options: &IndexOptions) -> Option<IndexType> {
    // Go `buildIndexInfo` (pkg/ddl/index.go): `if indexOption.Tp ==
    // ast.IndexTypeInvalid { idxInfo.Tp = ast.IndexTypeBtree } else {
    // idxInfo.Tp = indexOption.Tp }`. Invalid is the only value Go
    // special-cases; every other declared type (HASH, RTREE, HYPO, HNSW,
    // VECTOR, INVERTED, FULLTEXT, or any future/unknown ordinal) is copied
    // through unchanged. `IndexType` is a value-preserving newtype over the
    // same ordinals Go persists, so this passthrough is total by
    // construction -- there is no case here that needs a name.
    options.index_type.map(|declared| match declared {
        IndexType::INVALID => IndexType::BTREE,
        other => other,
    })
}

/// The expression payload of each index part, position-aligned with the
/// lowered constraint's parts: `Some` for an `IndexPart::Expr`, `None` for a
/// named column. This is the AST go hands to `BuildHiddenColumnInfo`.
fn table_constraint_expr_parts(constraint: &TableConstraint) -> Vec<Option<Expr>> {
    let TableConstraint::Index(index) = constraint else {
        return Vec::new();
    };
    index
        .parts
        .iter()
        .map(|part| match part {
            IndexPart::Column { .. } => None,
            IndexPart::Expr { expr, .. } => Some(expr.clone()),
        })
        .collect()
}

/// go `BuildHiddenColumnInfo` (pkg/ddl/create_table.go:1590) plus
/// `getAnonymousIndexPrefix`/`GetName4AnonymousIndex` (executor.go:4709):
/// every expression key part of every table-level index becomes one hidden
/// VIRTUAL generated column `_V$_<index>_<part-position>`, and the key part
/// is rewritten to point at it. `SHOW CREATE TABLE` and `SHOW INDEX` render
/// the stored expression back from that column; the column itself never
/// appears in the column list.
fn synthesize_expression_index_columns(
    columns: &mut Vec<ColumnInfo>,
    constraints: &mut [Constraint],
    constraint_exprs: &[Vec<Option<Expr>>],
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    let names: Vec<String> = columns
        .iter()
        .map(|column| column.name.original().to_owned())
        .collect();
    let types: Vec<FieldType> = columns.iter().map(|c| c.field_type.clone()).collect();
    // `checkConstraintNames` has named every index, an expression index
    // included (`expression_index`, `expression_index_2`, ...).
    for (constraint, exprs) in constraints.iter_mut().zip(constraint_exprs) {
        if exprs.is_empty() {
            // Inline constraints carry no key parts.
            continue;
        }
        if constraint.kind == ConstraintKind::PrimaryKey && exprs.iter().any(|expr| expr.is_some())
        {
            // Go builds each hidden column before it checks the primary key,
            // so an expression Go refuses outright (1235) wins over 3756.
            for expr in exprs.iter().flatten() {
                crate::expression_index::check_illegal_fn_4_generated(
                    "primary",
                    crate::expression_index::GenType::Index,
                    expr,
                )
                .map_err(default_admission_error)?;
            }
            // go `CheckPKOnGeneratedColumn` -> `ErrUnsupportedOnGeneratedColumn`
            // (3756): the primary key cannot be an expression index.
            return Err(DdlAdmissionError::from_terror(
                &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_PRIMARY_KEY
                    .generate_with_stack_by_args(&[]),
            ));
        }
        let parts: Vec<_> = exprs
            .iter()
            .zip(&constraint.parts)
            .map(|(expr, part)| match expr {
                Some(expr) => IndexPart::Expr {
                    expr: expr.clone(),
                    desc: part.desc,
                },
                None => IndexPart::Column {
                    name: part.name.clone(),
                    prefix_len: None,
                    desc: part.desc,
                },
            })
            .collect();
        let hidden =
            crate::expression_index::build_hidden_columns_with_like_default_escape(
                &constraint.name,
                &parts,
                &names,
                &types,
                &context.get_expr_ctx().get_eval_ctx().time_zone(),
                context.get_expr_ctx().get_eval_ctx().like_default_escape(),
                context.get_expr_ctx().get_charset_info().0,
            )
            .map_err(default_admission_error)?;
        // Go `buildHiddenColumnInfoWithCheck` -> `checkExpressionIndexAutoIncrement`
        // (3754), unless `tidb_enable_auto_increment_in_generated` allows it.
        let auto_increment = columns.iter().find_map(|column| {
            column
                .field_type
                .has_flag(FieldTypeFlags::AUTO_INCREMENT)
                .then(|| column.name.original().to_owned())
        });
        crate::index::check_expression_index_auto_increment(
            &constraint.name,
            &hidden,
            auto_increment.as_deref(),
            context,
        )
        .map_err(default_admission_error)?;
        for (position, hidden) in hidden {
            let column = hidden_index_column_info(hidden, columns.len());
            let part = &mut constraint.parts[position];
            part.name = column.name.original().to_owned();
            // go resets the length of an expression part to Unspecified.
            part.prefix_len = UNSPECIFIED_LENGTH;
            columns.push(column);
        }
    }
    Ok(())
}

/// Go `BuildHiddenColumnInfo`'s `ColumnInfo` for one expression key part: a
/// hidden, public, VIRTUAL generated column at `offset`. The id is the
/// caller's to allocate (`AllocateColumnID`).
pub fn hidden_index_column_info(
    hidden: crate::expression_index::HiddenIndexColumn,
    offset: usize,
) -> ColumnInfo {
    let generated = hidden.generated;
    // Go keys `Dependences` by `Name.L`.
    let mut dependences = GoStringSet::default();
    for dependency in generated.dependencies {
        dependences.insert(tidb_hack::go_to_lower(&dependency));
    }
    ColumnInfo {
        id: 0,
        name: CiString::new(hidden.name),
        offset: i64::try_from(offset).expect("a column offset fits in i64"),
        origin_default_value: GoAny::nil(),
        origin_default_value_bit: Default::default(),
        default_value: GoAny::nil(),
        default_value_bit: Default::default(),
        default_is_expr: false,
        generated_expr_string: generated.expr_text,
        generated_stored: false,
        dependences,
        field_type: hidden.field_type,
        changing_field_type: None,
        state: SchemaState::PUBLIC,
        comment: String::new(),
        hidden: true,
        change_state_info: None,
        version: CURR_LATEST_COLUMN_INFO_VERSION,
    }
}

/// Builds the one column an `ALTER TABLE ... ADD COLUMN` appends, against the
/// stored table's charset pair — Go `buildColumnAndConstraint` run by the
/// add-column DDL job. `generated_preceding` is present only for the ADD path
/// that can validate a virtual generated expression against the columns
/// before it; CREATE/MODIFY keep their existing admission boundary.
/// Names a column option the way the statement spelled it.
///
/// A refusal reaches the client verbatim, so it must not carry a Rust `Debug`
/// dump: `ADD COLUMN ... AS (a+1) VIRTUAL` used to be refused with
/// `Generated { expression: Binary(Plus, Column(["a"]), Int("1")),
/// expression_text: [97, 43, 49], stored: false }`, which names this port's
/// AST rather than the user's SQL and leaks the byte spelling of their own
/// expression back at them.
fn column_option_sql_name(option: &ColumnOption) -> &'static str {
    match option {
        ColumnOption::InlineKey(_) => "an inline key option",
        ColumnOption::NotNull => "NOT NULL",
        ColumnOption::Null => "NULL",
        ColumnOption::AutoIncrement => "AUTO_INCREMENT",
        ColumnOption::Default(_) => "DEFAULT",
        ColumnOption::Generated { stored: true, .. } => "a STORED generated expression",
        ColumnOption::Generated { .. } => "a VIRTUAL generated expression",
        ColumnOption::OnUpdate(_) => "ON UPDATE",
        ColumnOption::Comment(_) => "COMMENT",
        ColumnOption::Collate(_) => "COLLATE",
        ColumnOption::Check(_) => "CHECK",
        ColumnOption::Reference(_) => "REFERENCES",
        ColumnOption::ColumnFormat(_) => "COLUMN_FORMAT",
        ColumnOption::Storage(_) => "STORAGE",
        ColumnOption::AutoRandom(_) => "AUTO_RANDOM",
        ColumnOption::SecondaryEngineAttribute(_) => "SECONDARY_ENGINE_ATTRIBUTE",
        ColumnOption::MariaDbRowStart => "ROW START",
        ColumnOption::MariaDbRowEnd => "ROW END",
    }
}

pub fn build_added_column(
    column: &ColumnDef,
    table_charset: &str,
    table_collate: &str,
    context: &tidb_expr::metabuild::Context,
    enable_check_constraint: bool,
    generated_preceding: Option<&[ColumnInfo]>,
) -> Refusal<ColumnInfo> {
    for option in &column.options {
        match option {
            ColumnOption::Null
            | ColumnOption::Comment(_)
            | ColumnOption::NotNull
            | ColumnOption::Default(_)
            // Go `getCharsetAndCollateInColumnDef` resolves it with the type.
            | ColumnOption::Collate(_) => {}
            // `build_column` below owns Go's single OFF warning and returns
            // the inline constraint that `CreateNewColumn` discards.
            ColumnOption::Check(_) => {}
            // A STORED expression is refused only after it has been built:
            // Go validates the expression (arity, columns) first.
            ColumnOption::Generated { .. } if generated_preceding.is_some() => {}
            // Go builds the ON UPDATE of an added column like CREATE TABLE's.
            ColumnOption::OnUpdate(_) => {}
            other => {
                return Err(DdlAdmissionError::unsupported(format!(
                    "ADD COLUMN {} waits on its DDL course",
                    column_option_sql_name(other)
                )))
            }
        }
    }
    // Go `CreateNewColumn`: a default that calls one of these would give the
    // existing rows a value no replica can reproduce, so it is refused before
    // the column is built.
    for option in &column.options {
        let ColumnOption::Default(expr) = option else {
            continue;
        };
        let mut expr = expr;
        while let Expr::Paren(inner) = expr {
            expr = inner.as_ref();
        }
        let Expr::Func { name, .. } = expr else {
            continue;
        };
        match name.to_ascii_lowercase().as_str() {
            "nextval" => {
                return Err(default_admission_error(
                    tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_ADD_COLUMN_WITH_SEQUENCE_AS_DEFAULT
                            .generate_with_stack_by_args(&[(column.name.clone())
                                .to_string()
                                .into()]),
                    ),
                ));
            }
            "rand" | "uuid" | "uuid_to_bin" | "replace" | "upper" => {
                return Err(default_admission_error(
                    tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION
                            .generate_with_stack_by_args(&[]),
                    ),
                ));
            }
            _ => {}
        }
    }
    let (mut info, _constraints) = build_column(
        0,
        column,
        None,
        table_charset,
        table_collate,
        context,
        enable_check_constraint,
        generated_preceding.map_or(GeneratedColumns::Refused, GeneratedColumns::Existing),
    )?;
    if generated_preceding.is_some()
        && column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Generated { stored: true, .. }))
    {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN.generate_with_stack_by_args(
                &["Adding generated stored column through ALTER TABLE".into()],
            ),
        ));
    }
    // Go `checkAutoIncrementRef` (`AddColumn`): unless
    // `tidb_enable_auto_increment_in_generated` allows it, a generated column
    // may not read the table's AUTO_INCREMENT column (3109).
    if info.is_generated() && !context.enable_auto_increment_in_generated() {
        let auto_increment = generated_preceding
            .unwrap_or(&[])
            .iter()
            .find(|existing| existing.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT));
        if let Some(auto_increment) = auto_increment {
            if info.dependences.contains(auto_increment.name.lowercase()) {
                return Err(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_GENERATED_COLUMN_REF_AUTO_INC
                        .generate_with_stack_by_args(
                            &[tidb_hack::go_to_lower(&column.name).into()],
                        ),
                ));
            }
        }
    }
    // Go ignores every constraint returned here. Unsupported key/FK options
    // were refused above; the only possible result is the inline CHECK that
    // pinned `ALTER TABLE ... ADD COLUMN` does not persist.
    stamp_origin_default(&mut info, context, true)?;
    Ok(info)
}

/// Go `GetModifiableColumnJob`'s new column before any of its options:
/// `checkColumnAttributes` over the declared type, `table.ToColumn`, then
/// `ProcessColumnCharsetAndCollation` against the table's pair. The caller
/// gives it the old column's id, offset, state, origin default, version and
/// index flags, then runs [`process_modify_column_options`].
pub fn new_modified_column(
    column: &ColumnDef,
    table_charset: &str,
    table_collate: &str,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<ColumnInfo> {
    let field_type = build_column_field_type(column, table_charset, table_collate, context)?;
    Ok(ColumnInfo {
        id: 0,
        name: CiString::new(column.name.clone()),
        offset: 0,
        origin_default_value: GoAny::nil(),
        origin_default_value_bit: Default::default(),
        default_value: GoAny::nil(),
        default_value_bit: Default::default(),
        default_is_expr: false,
        generated_expr_string: String::new(),
        generated_stored: false,
        dependences: GoStringSet::default(),
        field_type,
        changing_field_type: None,
        state: SchemaState::PUBLIC,
        comment: String::new(),
        hidden: false,
        change_state_info: None,
        version: CURR_LATEST_COLUMN_INFO_VERSION,
    })
}

/// Go `ProcessModifyColumnOptions` (`pkg/ddl/modify_column.go:2297`): the
/// options of a MODIFY / CHANGE COLUMN definition applied to `info`, which
/// already carries the old column's index flags (and NOT NULL for a primary
/// key), in source order, then `processAndCheckDefaultValueAndColumn` with no
/// outer primary key: `processDefaultValue`, `processColumnFlags`,
/// `checkPriKeyConstraint`, `checkColumnValueConstraint` and
/// `checkDefaultValue`.
///
/// Unlike CREATE's `columnDefToCol`, a key, a reference or a check is refused
/// here, AUTO_INCREMENT only stamps its flag, and neither DEFAULT nor NULL
/// drops `ON UPDATE`.
pub fn process_modify_column_options(
    info: &mut ColumnInfo,
    column: &ColumnDef,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    let name = info.name.original().to_owned();
    let lower = tidb_hack::go_to_lower(&name);
    // Go `ErrUnsupportedModifyColumn.GenWithStackByArgs(reason)`.
    let unsupported = |reason: &str| {
        DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN
                .generate_with_stack_by_args(&[reason.into()]),
        )
    };
    let mut field_type = info.field_type.clone();
    let mut staged_default = None;
    let mut set_on_update_now = false;
    let mut has_null_flag = false;
    for option in &column.options {
        match option {
            ColumnOption::Default(expr) => {
                staged_default = Some(stage_column_default(
                    &name,
                    &field_type,
                    expr,
                    context,
                    &[],
                    &[],
                )?);
            }
            // Go `setColumnComment`, once per option.
            ColumnOption::Comment(comment) => {
                info.comment = crate::validate_comment_length(
                    comment,
                    &lower,
                    crate::CommentOwner::Field,
                    context,
                )
                .map_err(default_admission_error)?;
            }
            ColumnOption::NotNull => field_type.add_flags(FieldTypeFlags::NOT_NULL),
            ColumnOption::Null => {
                has_null_flag = true;
                field_type.del_flags(FieldTypeFlags::NOT_NULL);
            }
            ColumnOption::AutoIncrement => field_type.add_flags(FieldTypeFlags::AUTO_INCREMENT),
            // `ErrUnsupportedModifyColumn.GenWithStack(...)`: the reason is
            // the whole message.
            ColumnOption::InlineKey(key) => {
                let constraint = match key.kind {
                    InlineKeyKind::Primary { .. } => "PRIMARY KEY",
                    InlineKeyKind::Unique => "UNIQUE KEY",
                };
                return Err(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.generate_with_stack(
                        format!("can't change column constraint ({constraint})"),
                    ),
                ));
            }
            ColumnOption::OnUpdate(expr) => {
                // Go `ErrInvalidOnUpdate` (1294), for the type and then for
                // the expression.
                if !matches!(
                    field_type.code(),
                    FieldTypeCode::Timestamp | FieldTypeCode::Datetime
                ) || !tidb_expr::is_valid_current_timestamp_expr(expr, Some(&field_type))
                {
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_INVALID_ON_UPDATE
                            .generate_with_stack_by_args(&[name.into()]),
                    ));
                }
                field_type.add_flags(FieldTypeFlags::ON_UPDATE_NOW);
                set_on_update_now = true;
            }
            // Go records the expression text and the names it reads only;
            // `checkModifyGeneratedColumn` judges them against the table
            // afterwards.
            ColumnOption::Generated {
                expression, stored, ..
            } => {
                info.generated_expr_string = expression.restore_with_flags_and_string_charset(
                    modify_generated_restore_flags(),
                    context.get_expr_ctx().get_charset_info().0,
                );
                info.generated_stored = *stored;
                // Go `FindColumnNamesInExpr`, keyed by `Name.L`: no qualifier
                // check here (`checkModifyGeneratedColumn`'s rule 4 makes it).
                let (_, dependences) =
                    crate::generated_column::find_depended_column_names("", "", column)
                        .map_err(default_admission_error)?;
                info.dependences = GoStringSet::default();
                for dependency in dependences {
                    info.dependences.insert(dependency);
                }
            }
            ColumnOption::Collate(collate) => field_type.set_collation_name(collate.clone()),
            ColumnOption::Reference(_) => return Err(unsupported("can't modify with references")),
            ColumnOption::Check(_) => return Err(unsupported("can't modify with check")),
            // Handled by `checkAutoRandom` later.
            ColumnOption::AutoRandom(_) => {}
            // Go's default arm names the `ast.ColumnOptionType` number.
            ColumnOption::ColumnFormat(_) => {
                return Err(unsupported("unknown column option type: 14"))
            }
            ColumnOption::Storage(_) => return Err(unsupported("unknown column option type: 15")),
            ColumnOption::SecondaryEngineAttribute(_) => {
                return Err(unsupported("unknown column option type: 17"))
            }
            ColumnOption::MariaDbRowStart => {
                return Err(unsupported("unknown column option type: 18"))
            }
            ColumnOption::MariaDbRowEnd => {
                return Err(unsupported("unknown column option type: 19"))
            }
        }
    }

    // Go `processAndCheckDefaultValueAndColumn(ctx, col, nil, ...)`.
    let has_default_value = staged_default
        .as_ref()
        .is_some_and(StagedColumnDefault::has_default);
    process_default_value(info, &mut field_type, has_default_value, set_on_update_now);
    process_column_flags(&mut field_type);
    // Go `checkPriKeyConstraint`: the primary-key flag copied from the old
    // column refuses a NULL default (1067), then any explicit NULL (1171).
    if field_type.has_flag(FieldTypeFlags::PRI_KEY)
        && staged_default
            .as_ref()
            .is_some_and(StagedColumnDefault::has_null_default)
    {
        return Err(default_admission_error(
            tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE
                    .generate_with_stack_by_args(&[name.to_string().into()]),
            ),
        ));
    }
    if field_type.has_flag(FieldTypeFlags::PRI_KEY) && has_null_flag {
        return Err(default_admission_error(
            tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_PRIMARY_CANT_HAVE_NULL.generate_with_stack_by_args(&[]),
            ),
        ));
    }
    crate::column_field_type::check_column_value_constraint(&field_type)
        .map_err(|error| DdlAdmissionError::from_mysql_error(error.refusal(&name)))?;
    // Go `checkDefaultValue`, with the final flags in place.
    info.field_type = field_type;
    if let Some(staged_default) = staged_default {
        persist_column_default(&name, info, staged_default, context)?;
    }
    Ok(())
}

/// Go `ProcessModifyColumnOptions`' restore flags for a generated
/// expression: CREATE / ADD COLUMN's, keeping a table qualifier.
fn modify_generated_restore_flags() -> tidb_ast::RestoreFlags {
    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_ast::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES
        | tidb_ast::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
}

/// Go `GetModifiableColumnJob`: once a MODIFY / CHANGE may change the
/// column's data, `generateOriginDefaultValue(newCol, sctx, false)` gives it
/// an origin default from its new default (an expression is evaluated, with
/// no unsafe-function check); otherwise the old origin stands.
pub fn stamp_modified_origin_default(
    info: &mut ColumnInfo,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    stamp_origin_default(info, context, false)
}

/// Go `checkDropColumnForStatePublic`: a NOT NULL column without an origin
/// default gets one -- `generateOriginDefaultValue(col, nil, true)` -- so a
/// row written while the column is write-only still reads a value.
pub fn stamp_dropped_column_origin_default(info: &mut ColumnInfo) -> Refusal<()> {
    if !info.get_origin_default_value().is_nil()
        || !info.field_type.has_flag(FieldTypeFlags::NOT_NULL)
    {
        return Ok(());
    }
    let declared = info.get_default_value();
    let is_now = matches!(
        declared.view(),
        Some(tidb_model::GoAnyView::String(bytes)) if bytes.as_bytes() == b"CURRENT_TIMESTAMP"
    );
    if info.default_is_expr && !is_now {
        // With no session Go takes the zero value instead of evaluating the
        // expression.
        let mut probe = info.clone_like_go();
        probe
            .set_default_value(tidb_model::GoAny::nil())
            .map_err(|error| DdlAdmissionError::plain(error.to_string()))?;
        probe.default_is_expr = false;
        stamp_origin_default(&mut probe, &tidb_expr::metabuild::Context::new_non_strict(), true)?;
        info.origin_default_value = probe.origin_default_value.clone();
        info.origin_default_value_bit = probe.origin_default_value_bit.clone();
        return Ok(());
    }
    stamp_origin_default(info, &tidb_expr::metabuild::Context::new_non_strict(), true)
}

/// Go `generateOriginDefaultValue`'s `DefaultIsExpr` arm: with
/// `check_unsafe_func` only `DATE_FORMAT(NOW(), ...)` and `STR_TO_DATE` pass
/// (`ErrBinlogUnsafeSystemFunction`, 1674); the expression is then evaluated
/// once (`table.GetColDefaultValue`), cast to the column's type, and stored as
/// its `ToString` text.
fn expression_default_origin(
    info: &ColumnInfo,
    text: &str,
    context: &tidb_expr::metabuild::Context,
    check_unsafe_func: bool,
) -> Refusal<String> {
    let lower = text.to_lowercase();
    if check_unsafe_func && !lower.contains("date_format(now(),") && !lower.contains("str_to_date")
    {
        return Err(default_admission_error(
            tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION
                    .generate_with_stack_by_args(&[]),
            ),
        ));
    }
    let name = info.name.original();
    let failed = |reason: String| {
        DdlAdmissionError::plain(format!(
            "the default of column {name} cannot be evaluated: {reason}"
        ))
    };
    let expression = tidb_model::generated_expr::parse_expression(text)
        .map_err(|error| failed(error.message))?;
    let rewritten = tidb_expr::rewriter::rewrite_expr(&expression)
        .map_err(|error| failed(format!("{error:?}")))?;
    let value = tidb_expr::eval_expression_once(&rewritten, context.get_expr_ctx().get_eval_ctx())
        .map_err(|error| failed(format!("{error:?}")))?;
    if value.is_null() {
        return Ok(String::new());
    }
    let converted = value
        .convert_to_in(
            &info.field_type,
            context.get_expr_ctx().get_eval_ctx().type_ctx_flags(),
            &context.get_expr_ctx().get_eval_ctx().time_zone(),
        )
        .map_err(|_| {
            default_admission_error(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE
                    .generate_with_stack_by_args(&[name.to_owned().into()]),
            ))
        })?;
    // Go `castColumnValue`: a value an ENUM / SET cannot hold is
    // `types.ErrTruncated` with no arguments, which strict DDL raises.
    if converted.event.is_some()
        && matches!(
            info.field_type.code(),
            FieldTypeCode::Enum | FieldTypeCode::Set
        )
    {
        return Err(DdlAdmissionError::from_terror(
            &tidb_error::catalog::types::ERR_TRUNCATED,
        ));
    }
    let value = converted.value;
    let bytes = value
        .to_bytes()
        .map_err(|error| failed(error.to_string()))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// The statement's timestamp as Go's `generateOriginDefaultValue` renders
/// it for a `CURRENT_TIMESTAMP` origin default: UTC for TIMESTAMP, the
/// session zone's wall clock for DATETIME. A context without a statement
/// clock reads the wall clock, as Go's `ctx == nil` arm does.
fn statement_instant_text(
    context: &tidb_expr::metabuild::Context,
    time_type: tidb_datatype::TimeType,
    decimal: i64,
) -> String {
    use chrono::{Datelike, Timelike};
    let (seconds, nanos, offset) = tidb_expr::Columns::now(context.get_expr_ctx().get_eval_ctx()).unwrap_or_else(|| {
        let now = chrono::Local::now();
        (
            now.timestamp(),
            now.timestamp_subsec_nanos(),
            now.offset().local_minus_utc(),
        )
    });
    let utc = chrono::DateTime::from_timestamp(seconds, nanos).unwrap_or_default();
    let wall = if time_type == tidb_datatype::TimeType::Timestamp {
        utc.naive_utc()
    } else {
        utc.naive_utc() + chrono::Duration::seconds(i64::from(offset))
    };
    let fsp = decimal.clamp(0, 6);
    tidb_datatype::Time::from_date_checked(
        wall.year(),
        i32::try_from(wall.month()).expect("a month fits in i32"),
        i32::try_from(wall.day()).expect("a day fits in i32"),
        i32::try_from(wall.hour()).expect("an hour fits in i32"),
        i32::try_from(wall.minute()).expect("a minute fits in i32"),
        i32::try_from(wall.second()).expect("a second fits in i32"),
        if fsp == 0 {
            0
        } else {
            i32::try_from(wall.nanosecond() / 1_000).expect("microseconds fit in i32")
        },
        time_type,
        fsp,
    )
    .expect("the statement's calendar date is a valid time")
    .to_string()
}

/// Go `generateOriginDefaultValue` over a column just built, under the
/// statement's context; `check_unsafe_func` is ADD COLUMN's, which refuses an
/// expression default whose value would differ on a replica.
fn stamp_origin_default(
    info: &mut ColumnInfo,
    context: &tidb_expr::metabuild::Context,
    check_unsafe_func: bool,
) -> Refusal<()> {
    // Go `generateOriginDefaultValue`: the value a row written BEFORE this
    // column reports. The declared default when there is one; the type's
    // zero value for NOT NULL without one; nothing for a nullable
    // defaultless column, whose absent value IS NULL.
    let declared = info.get_default_value();
    let origin: Option<String> = if !declared.is_nil() {
        let text = match declared.view() {
            Some(tidb_model::GoAnyView::String(bytes)) => {
                String::from_utf8_lossy(bytes.as_bytes()).into_owned()
            }
            Some(tidb_model::GoAnyView::Int(value)) => value.to_string(),
            Some(tidb_model::GoAnyView::Uint(value)) => value.to_string(),
            Some(tidb_model::GoAnyView::Float(value)) => value.to_string(),
            _ => {
                return Err(DdlAdmissionError::unsupported(
                    "ADD COLUMN with a non-literal default waits on its DDL course",
                ))
            }
        };
        if text.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
            // Go `generateOriginDefaultValue` stamps the statement's
            // timestamp once, at DDL time (`GetStmtTimestamp`, so a session
            // `@@timestamp` applies): existing rows report the moment the
            // column was added, while the DECLARED default stays the word
            // CURRENT_TIMESTAMP for every later INSERT to evaluate. TIMESTAMP
            // stores the UTC wall clock, DATETIME the session zone's, at the
            // column's fsp (`types.NewTime(FromGoTime(t), tp, decimal)`).
            let time_type = match info.field_type.code() {
                FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
                FieldTypeCode::Datetime => tidb_datatype::TimeType::DateTime,
                other => {
                    return Err(DdlAdmissionError::unsupported(format!(
                        "a CURRENT_TIMESTAMP default on a {other:?} column is not a shape \
                         Go's DDL stamps"
                    )))
                }
            };
            Some(statement_instant_text(
                context,
                time_type,
                info.field_type.decimal(),
            ))
        } else if info.default_is_expr {
            Some(expression_default_origin(
                info,
                &text,
                context,
                check_unsafe_func,
            )?)
        } else {
            // Go stores the declared value itself: a BIT literal's bytes are
            // not UTF-8, and decoding them lossily corrupted the backfill.
            if let Some(tidb_model::GoAnyView::String(bytes)) = declared.view() {
                info.set_origin_default_value(ColumnDefaultValue::string_bytes(
                    bytes.as_bytes().to_vec(),
                ))
                .map_err(|error| DdlAdmissionError::plain(error.to_string()))?;
                return Ok(());
            }
            Some(text)
        }
    } else if info.field_type.has_flag(FieldTypeFlags::NOT_NULL) {
        // Go `generateOriginDefaultValue`: an ENUM takes its first element,
        // every other type `table.GetZeroValue(col).ToString()`.
        let field_type = &info.field_type;
        Some(match field_type.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
            | FieldTypeCode::Float
            | FieldTypeCode::Double
            | FieldTypeCode::NewDecimal => "0".to_owned(),
            // A fixed-width BINARY(n) zero value is n zero bytes.
            FieldTypeCode::String
                if field_type.flen() > 0 && field_type.charset_name() == "binary" =>
            {
                "\0".repeat(field_type.flen() as usize)
            }
            FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob => String::new(),
            FieldTypeCode::Duration => "00:00:00".to_owned(),
            FieldTypeCode::Date => "0000-00-00".to_owned(),
            FieldTypeCode::Datetime | FieldTypeCode::Timestamp => "0000-00-00 00:00:00".to_owned(),
            // `ZeroBinaryLiteral` and `Set{}` print as nothing.
            FieldTypeCode::Bit | FieldTypeCode::Set => String::new(),
            // `types.ParseEnumValue(col.GetElems(), 1)`.
            FieldTypeCode::Enum => field_type
                .elems()
                .snapshot()
                .first()
                .map(|element| element.to_utf8_lossy_go())
                .ok_or_else(|| {
                    DdlAdmissionError::plain("an ENUM column with no element has no first value")
                })?,
            FieldTypeCode::Json => "null".to_owned(),
            FieldTypeCode::VectorFloat32 => "[]".to_owned(),
            // An empty `Datum` prints as nothing.
            _ => String::new(),
        })
    } else {
        None
    };
    if let Some(origin) = origin {
        info.set_origin_default_value(ColumnDefaultValue::str(&origin))
            .map_err(|error| DdlAdmissionError::plain(error.to_string()))?;
    }
    Ok(())
}

/// Where a column's generated expression is resolved.
#[derive(Clone, Copy)]
enum GeneratedColumns<'a> {
    /// ADD COLUMN without the table's columns: this node does not build one.
    Refused,
    /// ADD COLUMN: against the table's existing columns, now.
    Existing(&'a [ColumnInfo]),
    /// CREATE TABLE: Go `columnDefToCol` records the expression text and
    /// its dependences only, and `checkGeneratedColumn` resolves every
    /// expression against the whole table once it is built. `preceding` is
    /// the columns before this one, which a DEFAULT expression reads;
    /// `schema` and `table` judge a qualified column name.
    Deferred {
        preceding: &'a [ColumnInfo],
        schema: &'a str,
        table: &'a str,
    },
}

impl<'a> GeneratedColumns<'a> {
    fn preceding(self) -> &'a [ColumnInfo] {
        match self {
            Self::Refused => &[],
            Self::Existing(columns)
            | Self::Deferred {
                preceding: columns, ..
            } => columns,
        }
    }
}

/// A column definition's type as Go settles it before any option is applied:
/// `getCharsetAndCollateInColumnDef`, `ResolveCharsetCollation` and
/// `OverwriteCollationWithBinaryFlag` over the table's pair, the type with its
/// flen and decimal (`setCharsetCollationFlenDecimal`, which also turns a
/// too-long VARCHAR into a TEXT outside strict mode), the deprecated display
/// width warning, and `checkColumnAttributes`.
fn build_column_field_type(
    column: &ColumnDef,
    table_charset: &str,
    table_collate: &str,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<FieldType> {
    let name = &column.name;
    // Go `buildColumnAndConstraint`: `getCharsetAndCollateInColumnDef`, then
    // `ResolveCharsetCollation` over the column's pair and the table's, then
    // `OverwriteCollationWithBinaryFlag`.
    let default_utf8mb4_coll = context.get_default_collation_for_utf8mb4();
    let (chs, coll) =
        crate::column_types::get_charset_and_collate_in_column_def(column, default_utf8mb4_coll)
            .map_err(default_admission_error)?;
    let (chs, coll) = crate::column_types::resolve_charset_collation(
        &[(&chs, &coll), (table_charset, table_collate)],
        default_utf8mb4_coll,
    )
    .map_err(default_admission_error)?;
    let (charset, collate) = crate::column_types::overwrite_collation_with_binary_flag(
        column,
        chs,
        coll,
        default_utf8mb4_coll,
    );

    // `build_field_type` is what actually stamps `binary`/`binary` on a type
    // that carries no charset, so the resolved pair above is only an input.
    let mut field_type = build_field_type(name, &column.ty, &charset, &collate)?;
    if let Some(warning) =
        try_auto_convert_too_big_varchar(&mut field_type, name, context.get_sql_mode().has_strict_mode())?
    {
        context.append_warning(WarnErr::Terror(
            tidb_util::dbterror::ERR_AUTO_CONVERT.generate(warning),
        ));
    }
    // go deprecates an EXPLICIT integer display width
    // (`ErrIntegerDisplayWidthDeprecated`, 1681), warned even when the width
    // equals the type's default (`int(11)`); the `tinyint(1)` boolean
    // convention is the one exemption.
    if matches!(
        field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
    ) {
        let declared_width = match column.ty.args.as_slice() {
            [tidb_ast::ColumnTypeArg::Text(text)] => text.parse::<i64>().ok(),
            _ => None,
        };
        let boolean_convention =
            field_type.code() == FieldTypeCode::Tiny && declared_width == Some(1);
        if declared_width.is_some() && !boolean_convention {
            context.append_warning(WarnErr::Terror(
                tidb_util::dbterror::ERR_WARN_DEPRECATED_INTEGER_DISPLAY_WIDTH.clone(),
            ));
        }
    }
    // Go `checkColumnAttributes` -- each refusal carries go's own errno and
    // text (1426/1427/3505); flattening them to a generic 1105 turned them
    // into a Rust Debug leak on the wire.
    crate::column_field_type::check_column_attributes(&field_type)
        .map_err(|error| DdlAdmissionError::from_mysql_error(error.refusal(name)))?;
    Ok(field_type)
}

/// Go `buildColumnAndConstraint` + `columnDefToCol` for one column.
fn build_column(
    offset: usize,
    column: &ColumnDef,
    out_primary_key: Option<&Constraint>,
    table_charset: &str,
    table_collate: &str,
    context: &tidb_expr::metabuild::Context,
    enable_check_constraint: bool,
    generated_columns: GeneratedColumns<'_>,
) -> Refusal<(ColumnInfo, Vec<(usize, Constraint)>)> {
    let name = &column.name;
    // Go `buildColumnAndConstraint`: the hidden handle's name is reserved.
    let lower = tidb_hack::go_to_lower(name);
    if lower == "_tidb_rowid" {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_WRONG_COLUMN_NAME
                .generate_with_stack_by_args(&[lower.into()]),
        ));
    }
    // A table-qualified column name (`CREATE TABLE t (t.a INT)`) was judged
    // by Go's preprocess (`ErrWrongDBName` / `ErrWrongTableName`); the
    // builder then reads the bare name, as `buildColumnAndConstraint` does.

    let mut field_type = build_column_field_type(column, table_charset, table_collate, context)?;
    // Go `checkColumnValueConstraint` (1291 for a duplicate ENUM/SET member),
    // asked of the declared type, as this builder always has.
    crate::column_field_type::check_column_value_constraint(&field_type)
        .map_err(|error| DdlAdmissionError::from_mysql_error(error.refusal(name)))?;

    let mut info = ColumnInfo {
        id: 0,
        name: CiString::new(name.clone()),
        offset: i64::try_from(offset).expect("a column offset fits in i64"),
        origin_default_value: GoAny::nil(),
        origin_default_value_bit: Default::default(),
        default_value: GoAny::nil(),
        default_value_bit: Default::default(),
        default_is_expr: false,
        generated_expr_string: String::new(),
        generated_stored: false,
        // Go `columnDefToCol` leaves this map nil for every supported normal
        // column. It allocates only for generated columns, which this tier
        // refuses before metadata construction.
        dependences: GoStringSet::default(),
        // Keep the declared type until option and constraint processing has
        // produced the final one. Staged defaults are installed through the
        // model setter only after that final type is in place below.
        field_type: field_type.clone(),
        changing_field_type: None,
        state: SchemaState::PUBLIC,
        comment: String::new(),
        hidden: false,
        change_state_info: None,
        version: CURR_LATEST_COLUMN_INFO_VERSION,
    };

    let mut constraints = Vec::new();
    let mut staged_default = None;
    let mut has_null_flag = false;
    let mut set_on_update_now = false;
    for (option_index, option) in column.options.iter().enumerate() {
        match option {
            ColumnOption::NotNull => field_type.add_flags(FieldTypeFlags::NOT_NULL),
            ColumnOption::Null => {
                field_type.del_flags(FieldTypeFlags::NOT_NULL);
                remove_on_update_now(&mut field_type);
                has_null_flag = true;
            }
            ColumnOption::InlineKey(inline) => match inline.kind {
                InlineKeyKind::Primary { storage } => {
                    if field_type.has_flag(FieldTypeFlags::PRI_KEY) {
                        continue;
                    }
                    constraints.push((
                        option_index,
                        Constraint {
                            kind: ConstraintKind::PrimaryKey,
                            // Named by `checkConstraintNames` with the rest.
                            name: String::new(),
                            parts: vec![KeyPart {
                                name: name.clone(),
                                prefix_len: UNSPECIFIED_LENGTH,
                                desc: false,
                            }],
                            clustered: storage,
                            comment: String::new(),
                            invisible: false,
                            global: inline.global,
                            index_type: None,
                            condition: None,
                        },
                    ));
                    field_type.add_flags(FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL);
                }
                // Go's arm tests `UniqueFlag` but sets `UniqueKeyFlag`, so a
                // repeated inline UNIQUE is a second constraint (`a`, `a_2`).
                InlineKeyKind::Unique => {
                    constraints.push((
                        option_index,
                        Constraint {
                            kind: ConstraintKind::Unique,
                            name: String::new(),
                            parts: vec![KeyPart {
                                name: name.clone(),
                                prefix_len: UNSPECIFIED_LENGTH,
                                desc: false,
                            }],
                            clustered: None,
                            comment: String::new(),
                            invisible: false,
                            global: inline.global,
                            index_type: None,
                            condition: None,
                        },
                    ));
                    field_type.add_flags(FieldTypeFlags::UNIQUE_KEY);
                }
            },
            ColumnOption::Default(expr) => {
                let preceding_names: Vec<String> = generated_columns
                    .preceding()
                    .iter()
                    .map(|column| column.name.original().to_owned())
                    .collect();
                let preceding_types: Vec<FieldType> = generated_columns
                    .preceding()
                    .iter()
                    .map(|column| column.field_type.clone())
                    .collect();
                staged_default = Some(stage_column_default(
                    name,
                    &field_type,
                    expr,
                    context,
                    &preceding_names,
                    &preceding_types,
                )?);
                remove_on_update_now(&mut field_type);
            }
            ColumnOption::OnUpdate(expr) => {
                if !matches!(
                    field_type.code(),
                    FieldTypeCode::Timestamp | FieldTypeCode::Datetime
                ) {
                    // Go `ErrInvalidOnUpdate` (1294).
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_INVALID_ON_UPDATE
                            .generate_with_stack_by_args(&[name.into()]),
                    ));
                }
                if !tidb_expr::is_valid_current_timestamp_expr(expr, Some(&field_type)) {
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_INVALID_ON_UPDATE
                            .generate_with_stack_by_args(&[name.into()]),
                    ));
                }
                field_type.add_flags(FieldTypeFlags::ON_UPDATE_NOW);
                set_on_update_now = true;
            }
            // Go stamps the flags here; the allocator itself is a separate
            // meta key this builder does not own.
            ColumnOption::AutoIncrement => {
                // Go `preprocessor.checkAutoIncrementOp`: the allocator hands
                // out integers, so only a numeric column can hold one. The
                // list is Go's exactly, and it is WIDER than "integer" --
                // FLOAT and DOUBLE are in it, and captured from TiDB
                // `id DOUBLE NOT NULL AUTO_INCREMENT` really is accepted.
                //
                // Refused here rather than left to the writer: the cluster
                // tier used to refuse every AUTO_INCREMENT table for its own
                // reason, which hid this. Without the check a
                // `VARCHAR AUTO_INCREMENT` table is created and then answers
                // every INSERT with a decode failure -- a table that cannot be
                // used, reported as a success.
                if !matches!(
                    field_type.code(),
                    FieldTypeCode::Tiny
                        | FieldTypeCode::Short
                        | FieldTypeCode::Long
                        | FieldTypeCode::Float
                        | FieldTypeCode::Double
                        | FieldTypeCode::LongLong
                        | FieldTypeCode::Int24
                ) {
                    // Go raises a plain `errors.Errorf` here, not a coded
                    // error, which is why TiDB answers 1105 and not 1063.
                    return Err(DdlAdmissionError::plain(format!(
                        "Incorrect column specifier for column '{name}'"
                    )));
                }
                field_type.add_flags(FieldTypeFlags::AUTO_INCREMENT | FieldTypeFlags::NOT_NULL);
            }
            // The persisted bit layout is validated and installed after the
            // clustered handle has been selected, because that decision is
            // part of the AUTO_RANDOM contract.
            ColumnOption::AutoRandom(_) => {}
            // Go `setColumnComment`, once per option.
            ColumnOption::Comment(comment) => {
                info.comment = crate::validate_comment_length(
                    comment,
                    &lower,
                    crate::CommentOwner::Field,
                    context,
                )
                .map_err(default_admission_error)?;
            }
            ColumnOption::Generated {
                expression, stored, ..
            } => {
                let preceding = match generated_columns {
                    GeneratedColumns::Refused => {
                        return Err(DdlAdmissionError::unsupported(
                            "a generated expression waits on its DDL course",
                        ));
                    }
                    GeneratedColumns::Deferred { schema, table, .. } => {
                        info.generated_expr_string = expression
                            .restore_with_flags_and_string_charset(
                                crate::generated_column::generated_restore_flags(),
                                context.get_expr_ctx().get_charset_info().0,
                            );
                        info.generated_stored = *stored;
                        let (_, dependences) =
                            crate::generated_column::find_depended_column_names(
                                schema, table, column,
                            )
                            .map_err(default_admission_error)?;
                        for dependency in dependences {
                            info.dependences.insert(dependency);
                        }
                        continue;
                    }
                    GeneratedColumns::Existing(preceding) => preceding,
                };
                let names = preceding
                    .iter()
                    .map(|column| column.name.original().to_owned())
                    .collect::<Vec<_>>();
                let types = preceding
                    .iter()
                    .map(|column| column.field_type.clone())
                    .collect::<Vec<_>>();
                let generated = crate::generated_column::build_added_generated_column_with_like_default_escape(
                    name,
                    expression,
                    *stored,
                    &names,
                    &types,
                    &context.get_expr_ctx().get_eval_ctx().time_zone(),
                    context.get_expr_ctx().get_eval_ctx().like_default_escape(),
                    context.get_expr_ctx().get_charset_info().0,
                )
                .map_err(generated_column_admission_error)?;
                info.generated_expr_string = generated.expr_text;
                info.generated_stored = generated.stored;
                // Go `findDependedColumnNames` keys the set by `Name.L`.
                for dependency in generated.dependencies {
                    info.dependences.insert(tidb_hack::go_to_lower(&dependency));
                }
            }
            // Already folded into the charset/collation resolution above.
            // Go `field_types.HasCharset(colDef.Tp)` is judged on the PARSED
            // type, which still carries `BinaryFlag` for a `BINARY` attribute
            // (`processColumnFlags` strips it only afterwards): a string
            // column with `BINARY COLLATE utf8_general_ci` keeps the `utf8_bin`
            // that `OverwriteCollationWithBinaryFlag` settled.
            ColumnOption::Collate(collate) => {
                let binary_modified = column.ty.binary
                    && !matches!(field_type.code(), FieldTypeCode::Enum | FieldTypeCode::Set);
                if field_type.has_charset() && !binary_modified {
                    field_type.set_collation_name(collate.clone());
                }
            }
            // Go discards this declaration with one warning while the global
            // switch is OFF. With it ON the outer table builder owns the
            // declaration after every column exists, so it can resolve the
            // expression against the complete table just as Go does.
            ColumnOption::Check(_) => {
                if !enable_check_constraint {
                    context.append_warning(err_check_constraint_is_off());
                }
            }
            // go `columnDefToCol` parses an inline `REFERENCES` clause and
            // never reads it (MySQL compatibility): the referenced table is
            // not opened and nothing is stored, whether or not the table
            // exists. Only the table-level `CONSTRAINT ... FOREIGN KEY`
            // spelling is honored.
            ColumnOption::Reference(_) => {}
            other => {
                return Err(DdlAdmissionError::plain(format!(
                    "column `{name}` carries {}, which this node does not support",
                    describe_column_option(other)
                )))
            }
        }
    }

    // Go `processDefaultValue` then `processColumnFlags`, before its ordered
    // `checkPriKeyConstraint` gates below.
    let has_default_value = staged_default
        .as_ref()
        .is_some_and(StagedColumnDefault::has_default);
    process_default_value(
        &mut info,
        &mut field_type,
        has_default_value,
        set_on_update_now,
    );
    process_column_flags(&mut field_type);

    // Go `checkPriKeyConstraint` first asks about an INLINE primary key,
    // because only that flag exists before the outer constraint is visited.
    // Its DEFAULT NULL is 1067, and this must win over an explicit NULL that
    // would otherwise reach the later 1171 arm.
    if field_type.has_flag(FieldTypeFlags::PRI_KEY)
        && staged_default
            .as_ref()
            .is_some_and(StagedColumnDefault::has_null_default)
    {
        return Err(default_admission_error(
            tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE
                    .generate_with_stack_by_args(&[name.to_owned().into()]),
            ),
        ));
    }

    // Only after the inline precheck does Go stamp a table-level PRIMARY KEY
    // naming this column.
    if !field_type.has_flag(FieldTypeFlags::PRI_KEY) {
        if let Some(primary_key) = out_primary_key {
            if primary_key
                .parts
                .iter()
                .any(|part| part.name.go_lower_eq(name))
            {
                field_type.add_flags(FieldTypeFlags::PRI_KEY);
            }
        }
    }
    if field_type.has_flag(FieldTypeFlags::PRI_KEY) && has_null_flag {
        return Err(default_admission_error(
            tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_PRIMARY_CANT_HAVE_NULL.generate_with_stack_by_args(&[]),
            ),
        ));
    }

    // Go's final `checkDefaultValue` now sees every final flag. The staged
    // spelling is persisted only after that check succeeds.
    info.field_type = field_type;
    if let Some(staged_default) = staged_default {
        persist_column_default(name, &mut info, staged_default, context)?;
    }
    Ok((info, constraints))
}

fn generated_column_admission_error(
    error: crate::generated_column::GeneratedDdlError,
) -> DdlAdmissionError {
    use crate::generated_column::GeneratedDdlError;
    match error {
        // Go `checkDependedColExist` reports a key of `findDependedColumnNames`'s
        // map, which is the name's lowercase form.
        GeneratedDdlError::UnknownDependency(name) => DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_BAD_FIELD.generate_with_stack(format!(
                "Unknown column '{}' in 'generated column function'",
                tidb_hack::go_to_lower(&name)
            )),
        ),
        GeneratedDdlError::NonPrior => DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_GENERATED_COLUMN_NON_PRIOR.generate_with_stack_by_args(&[]),
        ),
        GeneratedDdlError::IllegalFunction(error) => DdlAdmissionError::from_mysql_error(error),
        GeneratedDdlError::RefAutoInc(column) => DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_GENERATED_COLUMN_REF_AUTO_INC
                .generate_with_stack_by_args(&[column.into()]),
        ),
        GeneratedDdlError::Unsupported(reason) => DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN
                .generate_with_stack_by_args(&[reason.into()]),
        ),
        GeneratedDdlError::Unbuildable(reason) => DdlAdmissionError::unsupported(reason),
        GeneratedDdlError::Eval(error) => {
            let error = error.to_mysql_error();
            DdlAdmissionError::from_mysql_error(error)
        }
    }
}

/// Go `AlterColumn` followed by `updateColumnDefaultValue`: replaces one
/// existing column's DEFAULT in place, or drops it.
///
/// `default_value` is `None` for `DROP DEFAULT`, which Go answers by storing
/// no value and setting `NoDefaultValueFlag`; anything else goes through the
/// same staging and validation the column's own definition went through, so
/// an `ALTER` and a `CREATE` cannot disagree about what a spelling means.
pub fn set_column_default(
    name: &str,
    info: &mut ColumnInfo,
    default_value: Option<&Expr>,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    // Go clears both marks first, then either re-sets them for the DROP form
    // or lets the staging path decide.
    info.field_type.del_flags(FieldTypeFlags::NO_DEFAULT_VALUE);
    info.default_is_expr = false;
    let Some(expr) = default_value else {
        info.set_default_value(GoAny::nil())
            .map_err(|error| DdlAdmissionError::plain(error.to_string()))?;
        info.field_type.add_flags(FieldTypeFlags::NO_DEFAULT_VALUE);
        return Ok(());
    };
    let field_type = info.field_type.clone();
    let staged = stage_column_default(name, &field_type, expr, context, &[], &[])?;
    // Go `updateColumnDefaultValue` turns a staged value that carries no
    // default at all into `ErrInvalidDefaultValue`, which is why
    // `sql_mode=''` plus `SET DEFAULT ''` on TEXT is 1067 here even though
    // the same spelling is accepted when the column is first defined.
    if !staged.has_default() {
        return Err(default_admission_error(
            tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE
                    .generate_with_stack_by_args(&[name.to_owned().into()]),
            ),
        ));
    }
    persist_column_default(name, info, staged, context)
}

/// Go `updateColumnDefaultValue`'s owner-side recheck: the installed default
/// runs `checkColumnDefaultValue` and then `checkDefaultValue` again under
/// `newReorgExprCtx()`, which is ModeNone plus the default statement flags,
/// not the session's SQL mode. That is why `SET DEFAULT '2999-00-00'` is 1067
/// under STRICT_TRANS_TABLES even though MODIFY COLUMN accepts the same
/// spelling.
pub fn recheck_column_default_in_reorg_context(info: &ColumnInfo) -> Refusal<()> {
    // An expression default or a clock marker has no literal to convert;
    // Go's `getColDefaultValue` evaluates it (`GetTimeValue` takes
    // CURRENT_TIMESTAMP and CURRENT_DATE on a time column), which cannot fail
    // here once the session pass admitted it.
    if info.default_is_expr || crate::table_from_meta::stored_clock_marker(info).is_some() {
        return Ok(());
    }
    let Some(text) = info.get_default_value().builtin_string().cloned() else {
        return Ok(());
    };
    if matches!(
        info.field_type.code(),
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
    ) && text.as_bytes().eq_ignore_ascii_case(b"CURRENT_DATE")
    {
        return Ok(());
    }
    let name = info.name.original();
    let collation = tidb_datatype::Collation::from_name(info.field_type.collation_name())
        .unwrap_or(tidb_datatype::Collation::Binary);
    let value = tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
        text.as_bytes().to_vec(),
        collation,
    ));
    let zone = tidb_datatype::SessionTimeZone::utc();
    let (has_default, _) = crate::column_defaults::check_column_default_value(
        value.clone(),
        &info.field_type,
        name,
        &tidb_expr::metabuild::Context::new_non_strict(),
        &zone,
    )
    .map_err(default_admission_error)?;
    if !has_default {
        return Err(default_admission_error(
            tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE
                    .generate_with_stack_by_args(&[name.to_owned().into()]),
            ),
        ));
    }
    // `checkDefaultValue` runs only when the column keeps a default.
    if info.field_type.has_flag(FieldTypeFlags::NO_DEFAULT_VALUE) {
        return Ok(());
    }
    crate::column_defaults::validate_column_default(
        &value,
        &info.field_type,
        name,
        info.version,
        tidb_datatype::DEFAULT_STATEMENT_FLAGS,
        &zone,
    )
    .map_err(default_admission_error)?;
    Ok(())
}

/// Go `removeOnUpdateNowFlag`.
fn remove_on_update_now(field_type: &mut FieldType) {
    if field_type.has_flag(FieldTypeFlags::TIMESTAMP) {
        field_type.del_flags(FieldTypeFlags::ON_UPDATE_NOW);
    }
}

/// Go `processDefaultValue`: a `TIMESTAMP`/`YEAR` column with no declared
/// default gets one, then a `NOT NULL` column with no default at all is marked.
fn process_default_value(
    info: &mut ColumnInfo,
    field_type: &mut FieldType,
    has_default_value: bool,
    set_on_update_now: bool,
) {
    if !has_default_value {
        if field_type.has_flag(FieldTypeFlags::TIMESTAMP)
            && field_type.has_flag(FieldTypeFlags::NOT_NULL)
        {
            info.default_value = ColumnDefaultValue::str(if set_on_update_now {
                "0000-00-00 00:00:00"
            } else {
                "CURRENT_TIMESTAMP"
            })
            .into();
        }
        if field_type.code() == FieldTypeCode::Year && field_type.has_flag(FieldTypeFlags::NOT_NULL)
        {
            info.default_value = ColumnDefaultValue::str("0000").into();
        }
        // Go `setNoDefaultValueFlag`.
        if field_type.has_flag(FieldTypeFlags::NOT_NULL)
            && !field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT)
            && !field_type.has_flag(FieldTypeFlags::TIMESTAMP)
        {
            field_type.add_flags(FieldTypeFlags::NO_DEFAULT_VALUE);
        }
    }
}

/// Go `setColumnFlagWithConstraint`.
fn stamp_constraint_flags(columns: &mut [ColumnInfo], constraint: &Constraint) -> Refusal<()> {
    let find = |columns: &mut [ColumnInfo], name: &str| -> Option<usize> {
        columns
            .iter()
            .position(|column| column.name.lowercase() == tidb_hack::go_to_lower(name))
    };
    match constraint.kind {
        ConstraintKind::PrimaryKey => {
            for part in &constraint.parts {
                let Some(index) = find(columns, &part.name) else {
                    continue;
                };
                let column = &mut columns[index];
                column
                    .field_type
                    .add_flags(FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL);
                // Go re-runs `setNoDefaultValueFlag` here now that NOT NULL is
                // certain, using the default the column already carries.
                if column.default_value.is_nil()
                    && !column.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT)
                    && !column.field_type.has_flag(FieldTypeFlags::TIMESTAMP)
                {
                    column
                        .field_type
                        .add_flags(FieldTypeFlags::NO_DEFAULT_VALUE);
                }
            }
        }
        // Only the FIRST key column is stamped, and a multi-column unique key
        // stamps `MultipleKeyFlag` rather than `UniqueKeyFlag` — MySQL's own
        // `SHOW COLUMNS` contract.
        ConstraintKind::Unique => {
            if let Some(part) = constraint.parts.first() {
                if let Some(index) = find(columns, &part.name) {
                    let flag = if constraint.parts.len() > 1 {
                        FieldTypeFlags::MULTIPLE_KEY
                    } else {
                        FieldTypeFlags::UNIQUE_KEY
                    };
                    columns[index].field_type.add_flags(flag);
                }
            }
        }
        ConstraintKind::Key => {
            if let Some(part) = constraint.parts.first() {
                if let Some(index) = find(columns, &part.name) {
                    columns[index]
                        .field_type
                        .add_flags(FieldTypeFlags::MULTIPLE_KEY);
                }
            }
        }
    }
    Ok(())
}

/// Go `BuildTableInfo`: column IDs, the handle decision, then the indexes.
#[allow(clippy::too_many_arguments)]
fn build_table(
    create: &CreateTableStmt,
    mut columns: Vec<ColumnInfo>,
    constraints: Vec<Constraint>,
    charset: String,
    collate: String,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<TableInfo> {
    let table_name = create
        .name
        .last()
        .cloned()
        .ok_or_else(|| DdlAdmissionError::plain("CREATE TABLE names no table"))?;
    let mut table = TableInfo {
        name: CiString::new(table_name),
        charset,
        collate,
        state: SchemaState::PUBLIC,
        version: TABLE_INFO_VERSION5,
        ..TableInfo::default()
    };
    for column in &mut columns {
        table.max_column_id += 1;
        column.id = table.max_column_id;
    }
    // Go `BuildIndexInfo`'s `allTableColumns := tblInfo.Columns`: the
    // columns every index below is built over.
    let all_table_columns = columns.clone();
    table.columns = columns.into();

    if constraints
        .iter()
        .filter(|constraint| constraint.kind == ConstraintKind::PrimaryKey)
        .count()
        > 1
    {
        // Go `ErrMultiplePriKey` (1068).
        return Err(DdlAdmissionError::from_terror(
            &tidb_error::catalog::infoschema::ERR_MULTIPLE_PRI_KEY.generate_with_stack_by_args(&[]),
        ));
    }

    for constraint in &constraints {
        if constraint.kind == ConstraintKind::PrimaryKey {
            check_pk_on_generated_column(&table, constraint)?;
            // Go `buildTableInfo` blocks every partial primary key.
            if constraint.condition.is_some() {
                return Err(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
                        .generate_with_stack_by_args(&[
                            "create an primary key with partial index is not supported".into(),
                        ]),
                ));
            }
            let single_int = is_single_int_primary_key(constraint, &table);
            if should_build_clustered_index(
                context.get_clustered_index_def_mode(),
                constraint.clustered,
                single_int,
            ) {
                if single_int {
                    table.pk_is_handle = true;
                } else {
                    table.is_common_handle = true;
                    table.common_handle_version = 1;
                }
                // Go `buildTableInfo`: a clustered primary key can be neither
                // invisible nor global.
                if constraint.invisible {
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_PK_INDEX_CANT_BE_INVISIBLE
                            .generate_with_stack_by_args(&[]),
                    ));
                }
                if constraint.global {
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_GENERAL_UNSUPPORTED_DDL
                            .generate_with_stack_by_args(&[
                                "create an index that is both a global index and a clustered index"
                                    .into(),
                            ]),
                    ));
                }
            }
            break;
        }
    }

    for constraint in &constraints {
        // A clustered single-integer primary key IS the row key, so Go records
        // it in `pk_is_handle` and the column's own flag, and builds no index.
        if constraint.kind == ConstraintKind::PrimaryKey && table.pk_is_handle {
            continue;
        }
        let primary = constraint.kind == ConstraintKind::PrimaryKey;
        let name = if primary {
            "PRIMARY".to_owned()
        } else {
            constraint.name.clone()
        };
        // Go `BuildIndexInfo` -> `checkTooLongIndex` (1059).
        crate::check_too_long_identifier(&name).map_err(default_admission_error)?;
        // Go `BuildIndexInfo` -> `buildIndexColumns` over `tbInfo.Columns`,
        // whose expression parts already name their hidden columns.
        let parts: Vec<crate::index::IndexPartSpecification<'_>> = constraint
            .parts
            .iter()
            .map(|part| crate::index::IndexPartSpecification {
                column: &part.name,
                length: part.prefix_len,
            })
            .collect();
        let (index_columns, mv_index) = crate::index::build_index_columns(
            Some(context),
            &all_table_columns,
            &parts,
            tidb_model::index::ColumnarIndexType::NA,
        )
        .map_err(default_admission_error)?;
        table.max_index_id += 1;
        let mut index = IndexInfo {
            id: table.max_index_id,
            name: CiString::new(name),
            table: CiString::default(),
            columns: index_columns.into(),
            state: SchemaState::PUBLIC,
            comment: constraint.comment.clone(),
            tp: constraint.index_type.unwrap_or(IndexType::BTREE),
            unique: primary || constraint.kind == ConstraintKind::Unique,
            primary,
            invisible: constraint.invisible,
            global: constraint.global,
            mv_index,
            ..IndexInfo::default()
        };
        build_index_condition(&table, &mut index, constraint.condition.as_ref(), context)?;
        index.comment = crate::validate_comment_length(
            &index.comment,
            index.name.original(),
            crate::CommentOwner::Index,
            context,
        )
        .map_err(default_admission_error)?;
        table.indices.push_go(index);
    }
    Ok(table)
}

/// Go `isSingleIntPKFromCol`.
fn is_single_int_primary_key(constraint: &Constraint, table: &TableInfo) -> bool {
    let [part] = constraint.parts.as_slice() else {
        return false;
    };
    table
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase().go_lower_eq(&part.name))
        // Go `isIntCol` looks at the type alone: an UNSIGNED BIGINT primary
        // key is just as much a clustered handle as a signed one, and a YEAR
        // one is never the int handle.
        .is_some_and(|column| {
            crate::column_types::is_int_column_type(column.read().field_type.code())
        })
}

/// Go `ShouldBuildClusteredIndex`.
fn should_build_clustered_index(
    mode: ClusteredIndexDefMode,
    declared: Option<PrimaryKeyStorage>,
    single_int_primary_key: bool,
) -> bool {
    match declared {
        Some(PrimaryKeyStorage::Clustered) => true,
        Some(PrimaryKeyStorage::NonClustered) => false,
        Some(PrimaryKeyStorage::Unknown(_)) => false,
        None => match mode {
            ClusteredIndexDefMode::ON => true,
            ClusteredIndexDefMode::INT_ONLY => single_int_primary_key,
            _ => false,
        },
    }
}

impl From<ColumnTypeError> for DdlAdmissionError {
    fn from(error: ColumnTypeError) -> Self {
        Self::plain(error.reason)
    }
}

/// The shared [`build_shared_field_type`] reported as this builder's refusal.
fn build_field_type(
    name: &str,
    declared: &ColumnType,
    charset: &str,
    collate: &str,
) -> Refusal<FieldType> {
    Ok(build_shared_field_type(name, declared, charset, collate)?)
}

/// A DEFAULT retained between Go's option-order storage stages and its final
/// `checkDefaultValue` pass.
enum StagedColumnDefault {
    /// A temporal marker is computed at row-write time and therefore has no
    /// settled spelling to cast through the final column type.
    TemporalMarker(String),
    /// Go's computed default whose stored text names a function call or
    /// other expression: `DEFAULT (json_array())` keeps the spelling and
    /// re-runs per omitted row (metadata-only — go stores the same text
    /// with `default_is_expr`).
    Computed(String),
    /// A literal's exact metadata spelling, including fixed-binary padding.
    Settled(crate::column_defaults::SettledColumnDefault),
}

impl StagedColumnDefault {
    fn has_default(&self) -> bool {
        match self {
            Self::TemporalMarker(_) | Self::Computed(_) => true,
            Self::Settled(default) => default.has_default,
        }
    }

    fn has_null_default(&self) -> bool {
        matches!(
            self,
            Self::Settled(default) if default.has_default && default.stored.is_null()
        )
    }
}

fn default_admission_error(error: MysqlError) -> DdlAdmissionError {
    DdlAdmissionError::from_mysql_error(error)
}

/// Go `SetDefaultValue` -> `getDefaultValue` -> `checkColumnDefaultValue`,
/// bounded to the literal and `CURRENT_TIMESTAMP` defaults a `CREATE TABLE`
/// actually writes. Literal defaults are deliberately not persisted here:
/// later column options and table constraints can still change their validity.
///
/// The stored form is Go's own string payload. Most values finish through
/// `Datum.ToString`; binary/bit literals take Go's earlier raw-byte branches,
/// and `CURRENT_TIMESTAMP` stores its marker word.
fn stage_column_default(
    name: &str,
    field_type: &FieldType,
    expr: &Expr,
    context: &tidb_expr::metabuild::Context,
    preceding_names: &[String],
    preceding_types: &[FieldType],
) -> Refusal<StagedColumnDefault> {
    let built = tidb_table::column_default::build_with_current_database(
        expr,
        name,
        field_type,
        context
            .get_expr_ctx()
            .get_eval_ctx()
            .current_database()
            .as_deref(),
        context.get_expr_ctx().get_charset_info().0,
        |expr| {
            // go resolves a DEFAULT expression against the table's own columns
            // (`getDefaultValue` runs the expression resolver over them), so an
            // unknown name fails with ErrBadField (1054) 'expression'.
            let resolver = crate::generated_column::TableColumnResolver::with_like_default_escape(
                preceding_names,
                preceding_types,
                context.get_expr_ctx().get_eval_ctx().time_zone(),
                context.get_expr_ctx().get_eval_ctx().like_default_escape(),
            );
            let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver).map_err(
                |error| match (resolver.missing_name(), error) {
                    (Some(name), _) => MysqlError::from_terror(
                        &tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(
                            &[tidb_hack::go_to_lower(name).into(), "expression".into()],
                        ),
                    ),
                    (None, tidb_expr::EvalError::Unsupported(message)) => {
                        MysqlError::unknown(message)
                    }
                    (None, _) => MysqlError::unknown("a DEFAULT this node cannot evaluate"),
                },
            )?;
            tidb_expr::eval_expression_once(&rewritten, context.get_expr_ctx().get_eval_ctx())
                .map_err(|_| MysqlError::unknown("a DEFAULT this node cannot evaluate"))
        },
    )
    .map_err(default_admission_error)?;

    match built {
        tidb_table::column_default::ColumnDefault::Computed(computed) => {
            if computed.is_expr() {
                // Go stores the expression spelling with `default_is_expr`
                // and re-runs it per omitted row; the catalog writer carries
                // the same metadata instead of refusing (oracle m3-catalog:
                // CREATE TABLE c13(a json DEFAULT (json_array()))).
                // Go's `DefaultValue` is a Go string: meta JSON writes each
                // byte that is not UTF-8 as U+FFFD.
                return Ok(StagedColumnDefault::Computed(go_json_text(&computed.text)));
            }
            Ok(StagedColumnDefault::TemporalMarker(go_json_text(
                &computed.text,
            )))
        }
        tidb_table::column_default::ColumnDefault::Value(value) => {
            let zone = context.get_expr_ctx().get_eval_ctx().time_zone();
            let settled =
                crate::column_defaults::settle_column_default(value, field_type, name, context, &zone)
                    .map_err(default_admission_error)?;
            Ok(StagedColumnDefault::Settled(settled))
        }
    }
}

/// A Go string as meta JSON writes it: each byte that is not UTF-8 (carried
/// as a `tidb_hack::raw_bytes` escape) becomes U+FFFD.
fn go_json_text(text: &str) -> String {
    if tidb_hack::raw_bytes::contains_escapes(text) {
        tidb_datatype::decode_go_utf8_lossy(tidb_hack::raw_bytes::str_to_bytes(text).as_ref())
    } else {
        text.to_owned()
    }
}

/// Go `checkDefaultValue` followed by `ColumnInfo.SetDefaultValue`, after the
/// final FieldType has been installed. The setter is load-bearing for BIT:
/// it retains the raw-byte shadow alongside the JSON-facing default value.
fn persist_column_default(
    name: &str,
    info: &mut ColumnInfo,
    staged: StagedColumnDefault,
    context: &tidb_expr::metabuild::Context,
) -> Refusal<()> {
    match staged {
        StagedColumnDefault::TemporalMarker(text) => {
            info.default_is_expr = false;
            info.set_default_value(ColumnDefaultValue::str(&text))
                .map_err(|error| DdlAdmissionError::plain(error.to_string()))?;
        }
        StagedColumnDefault::Computed(text) => {
            // Go `checkDefaultValue`: an AUTO_INCREMENT column takes no
            // expression default.
            if info.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT) {
                return Err(default_admission_error(
                    tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE
                            .generate_with_stack_by_args(&[name.to_owned().into()]),
                    ),
                ));
            }
            // Go stores the expression spelling with `default_is_expr`; the
            // per-row evaluation re-runs it for every omitted column.
            info.default_is_expr = true;
            info.set_default_value(ColumnDefaultValue::str(&text))
                .map_err(|error| DdlAdmissionError::plain(error.to_string()))?;
        }
        StagedColumnDefault::Settled(settled) => {
            // Any inline-key NULL default returned at the earlier precheck,
            // so a PRI flag reaching this final Go `checkDefaultValue` arm
            // came from the table-level constraint and is 1171, even when a
            // separate NOT NULL option is also present.
            if settled.has_default
                && settled.stored.is_null()
                && info.field_type.has_flag(FieldTypeFlags::PRI_KEY)
            {
                return Err(default_admission_error(
                    tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_PRIMARY_CANT_HAVE_NULL
                            .generate_with_stack_by_args(&[]),
                    ),
                ));
            }
            let zone = context.get_expr_ctx().get_eval_ctx().time_zone();
            crate::column_defaults::validate_column_default(
                &settled.stored,
                &info.field_type,
                name,
                info.version,
                context.get_expr_ctx().get_eval_ctx().type_ctx_flags(),
                &zone,
            )
            .map_err(default_admission_error)?;
            let stored = if settled.stored.is_null() {
                GoAny::nil()
            } else {
                ColumnDefaultValue::string_bytes(settled.stored.sql_bytes().map_err(|_| {
                    DdlAdmissionError::plain(format!(
                        "column `{name}` has an invalid default value"
                    ))
                })?)
                .into()
            };
            info.set_default_value(stored)
                .map_err(|error| DdlAdmissionError::plain(error.to_string()))?;
        }
    }
    Ok(())
}

fn describe_column_option(option: &ColumnOption) -> &'static str {
    match option {
        ColumnOption::Generated { .. } => "a generated expression",
        ColumnOption::Check(_) => "a CHECK constraint",
        ColumnOption::Reference(_) => "a REFERENCES clause",
        ColumnOption::ColumnFormat(_) => "COLUMN_FORMAT",
        ColumnOption::Storage(_) => "STORAGE",
        ColumnOption::AutoRandom(_) => "AUTO_RANDOM",
        ColumnOption::SecondaryEngineAttribute(_) => "SECONDARY_ENGINE_ATTRIBUTE",
        ColumnOption::MariaDbRowStart => "ROW START",
        ColumnOption::MariaDbRowEnd => "ROW END",
        // Handled by the caller before it reaches this description.
        ColumnOption::NotNull
        | ColumnOption::Null
        | ColumnOption::InlineKey(_)
        | ColumnOption::AutoIncrement
        | ColumnOption::Default(_)
        | ColumnOption::OnUpdate(_)
        | ColumnOption::Comment(_)
        | ColumnOption::Collate(_) => "a key, nullability, default, or comment option",
    }
}
