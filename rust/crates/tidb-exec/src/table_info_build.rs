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
//! [`tidb_executor::ddl::column_field_type`] and is SHARED with the
//! runnable-path `CREATE TABLE` builder (`tidb_executor::ddl`), because the two
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

use tidb_ast::IndexType;
use tidb_ast::{CiString, IndexOptions};
use tidb_ast::{
    ColumnDef, ColumnOption, ColumnType, CreateTableStmt, Expr, IndexConstraintKind, IndexPart,
    InlineKeyKind, PrimaryKeyStorage, TableConstraint, TableOption,
};
use tidb_datatype::{
    get_collation_by_name, get_default_charset_and_collate, get_default_collation, FieldType,
    FieldTypeCode, FieldTypeFlags, SessionTimeZone,
};
// The declared-type -> `FieldType` rule set is SHARED with the runnable-path
// `CREATE TABLE` builder; see `column_field_type`'s module doc for why.
use tidb_executor::ddl::column_field_type::{
    build_field_type as build_shared_field_type, column_type_code as shared_column_type_code,
    process_column_flags, ColumnTypeError,
};
use tidb_model::column::{
    ColumnDefaultValue, ColumnInfo, GoAny, GoStringSet, CURR_LATEST_COLUMN_INFO_VERSION,
};
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::{TableInfo, TABLE_INFO_VERSION5};

/// Go `types.UnspecifiedLength`.
pub const UNSPECIFIED_LENGTH: i64 = -1;

/// Go `vardef.ClusteredIndexDefMode`: how a `PRIMARY KEY` with no explicit
/// `CLUSTERED`/`NONCLUSTERED` becomes (or does not become) the row handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusteredIndexDefMode {
    /// Go `ClusteredIndexDefModeIntOnly`: only a single-column integer primary
    /// key is clustered. This is what classic bootstrap runs under
    /// (`session.go` sets it explicitly "for the bootstrap SQLs"), and so it is
    /// what a live cluster's own `mysql.*` carries.
    IntOnly,
    /// Go `ClusteredIndexDefModeOn`: every primary key is clustered.
    On,
    /// Go `ClusteredIndexDefModeOff`.
    Off,
}

/// Why a statement cannot be built as written.
///
/// Every value is produced before any caller reads a snapshot or spends a PD
/// timestamp, so a refusal never leaves a partial change behind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlAdmissionError {
    /// Exact, self-contained explanation naming the offending clause.
    pub reason: String,
    /// The MySQL error number the client is told, which defaults to the
    /// generic 1105 and is Go's own code where this node refuses exactly what
    /// Go names.
    pub code: u16,
}

impl DdlAdmissionError {
    /// Builds a refusal from its explanation, reported as the generic 1105.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            code: GENERIC_ERROR_CODE,
        }
    }

    /// Go `dbterror.ErrUnsupportedDDLOperation` (8200, `Unsupported %s`): a
    /// table shape this node will not create because it could not then serve
    /// it. Refusing under Go's own errno is what lets a client tell "this
    /// server does not do that" from an internal failure.
    pub fn unsupported(what: impl Into<String>) -> Self {
        Self {
            reason: format!("Unsupported {}", what.into()),
            code: tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
        }
    }

    /// Builds a refusal that carries one of Go's own error numbers, for the
    /// statements TiDB itself rejects rather than the ones this node merely
    /// declines to serve.
    pub fn with_code(code: u16, reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            code,
        }
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

/// The length this builder stores for one index key part, validated against
/// the column by the rule set shared with the executor tier.
///
/// Before this existed, the builder stored whatever length was written, so
/// `key idx(a(3))` on an INTEGER produced an `IndexInfo` carrying a 3-byte
/// prefix on a type that has none -- metadata real TiDB refuses with 1089,
/// and which the executor tier refused too. See
/// `tidb_executor::ddl::index_prefix` for why the rules are shared while the
/// lowering is not.
fn prefix_length(
    field_type: &tidb_datatype::FieldType,
    column: &str,
    part: &KeyPart,
) -> Refusal<i64> {
    use tidb_executor::ddl::index_prefix::{stored_index_length, IndexedColumn, PrefixError};

    let declared = (part.prefix_len != UNSPECIFIED_LENGTH).then_some(part.prefix_len);
    // This builder lowers an ALREADY ADMITTED catalog, whose expression key
    // parts have become ordinary hidden columns; the hidden-column arms of
    // `checkIndexColumn` were decided when the catalog was built.
    stored_index_length(field_type, IndexedColumn::Named(column), declared, true).map_err(|error| {
        match error {
        PrefixError::IncorrectPrefixKey => DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrWrongSubKey,
            "Incorrect prefix key; the used key part isn't a string, the used length is longer \
             than the key part, or the storage engine doesn't support unique prefix keys",
        ),
        PrefixError::BlobKeyWithoutLength(column) => DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrBlobKeyWithoutLength,
            format!("BLOB/TEXT column '{column}' used in key specification without a key length"),
        ),
        PrefixError::KeyPart0(column) => DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrKeyPart0,
            format!("Key part '{column}' length cannot be 0"),
        ),
        PrefixError::WrongKeyColumn(column) => DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrWrongKeyColumn,
            format!("The used storage engine can't index column '{column}'"),
        ),
        PrefixError::JsonUsedAsKey(column) => DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrJSONUsedAsKey,
            format!("JSON column '{column}' cannot be used in key specification."),
        ),
        PrefixError::TooLongKey { length, max } => DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrTooLongKey,
            format!("Specified key was too long ({length} bytes); max key length is {max} bytes"),
        ),
        // The hidden-column arms, which `IndexedColumn::Named` above cannot
        // reach.
        other => unreachable!("a named key part cannot report {other:?}"),
        }
    })
}

/// Go `buildIndexColumns`' running sum over one index's key parts, mapped onto
/// this builder's refusal. The per-part rules are [`prefix_length`]; this is
/// the separate limit on their TOTAL.
fn key_length_sum<'a>(
    parts: impl IntoIterator<Item = (&'a tidb_datatype::FieldType, i64)>,
    part_count: usize,
    unique: bool,
) -> Refusal<()> {
    use tidb_executor::ddl::index_prefix::{check_index_key_length, PrefixError};

    // Strict: this tier builds a table info from an ALREADY ADMITTED catalog,
    // so the truncating arm cannot be reached here.
    check_index_key_length(parts, part_count, unique, true)
        .map(|_| ())
        .map_err(|error| match error {
            PrefixError::TooLongKey { length, max } => DdlAdmissionError::with_code(
                tidb_error::tidb::errcode::ErrTooLongKey,
                format!(
                    "Specified key was too long ({length} bytes); max key length is {max} bytes"
                ),
            ),
            other => unreachable!("the key-length sum reports only ErrTooLongKey, got {other:?}"),
        })
}

/// The MySQL error number for a refusal that has no Go code of its own.
const GENERIC_ERROR_CODE: u16 = 1105;

impl fmt::Display for DdlAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for DdlAdmissionError {}

type Refusal<T> = Result<T, DdlAdmissionError>;

/// Builds a bootstrap `TableInfo` under classic bootstrap's exact `ModeNone`
/// evaluation context.
///
/// `db_charset`/`db_collate` are the owning database's, which a table with no
/// charset option of its own inherits. The returned table carries no ID and no
/// `update_ts`: those belong to the transaction that publishes it. Live DDL
/// must call [`build_table_info_with_context`] with the issuing session's
/// context instead.
pub fn build_table_info(
    create: &CreateTableStmt,
    db_charset: &str,
    db_collate: &str,
    clustered_mode: ClusteredIndexDefMode,
) -> Refusal<TableInfo> {
    let context = bootstrap_ddl_statement_context();
    build_table_info_with_context(create, db_charset, db_collate, clustered_mode, &context)
}

/// [`build_table_info`] under the statement's actual SQL mode and time zone.
///
/// A live session must use this entrypoint: literal temporal defaults are
/// admitted under its mode bits, and a literal `TIMESTAMP` is persisted as the
/// UTC projection of its session wall clock.
pub fn build_table_info_with_context(
    create: &CreateTableStmt,
    db_charset: &str,
    db_collate: &str,
    clustered_mode: ClusteredIndexDefMode,
    context: &tidb_executor::StmtContext,
) -> Refusal<TableInfo> {
    let refuse = |what: &str| {
        Err(DdlAdmissionError::new(format!(
            "CREATE TABLE {what} is not supported by this node"
        )))
    };
    if create.temporary != tidb_ast::CreateTableTemporary::None {
        return refuse("TEMPORARY");
    }
    if create.like_table.is_some() {
        return refuse("... LIKE");
    }
    if create.ctas.is_some() {
        return refuse("... AS <query>");
    }
    if create.partitioning.is_some() {
        return refuse("PARTITION BY");
    }
    if !create.splits.is_empty() {
        return refuse("SPLIT REGION");
    }
    if create.columns.is_empty() {
        return Err(DdlAdmissionError::new("CREATE TABLE declares no columns"));
    }

    if let Err(error) = tidb_executor::ddl::validate_table_options(&create.table_options) {
        let error = error.to_mysql_error();
        return Err(DdlAdmissionError::with_code(error.code, error.message));
    }

    // Go `GetCharsetAndCollateInTableOption`: the LAST declared pair wins, and
    // whatever it leaves unset the database supplies.
    let mut declared_charset = None;
    let mut declared_collate = None;
    let mut comment = String::new();
    let mut auto_inc_id = 0i64;
    let mut auto_rand_id = 0i64;
    let mut auto_id_cache = 0i64;
    for option in &create.table_options {
        match option {
            TableOption::CharacterSet(charset) => declared_charset = Some(charset.clone()),
            TableOption::Collate(collate) => declared_collate = Some(collate.clone()),
            TableOption::Comment(text) => comment = text.clone(),
            // Go `handleTableOptions`: the parsed value is unsigned, and
            // anything past int64 is refused with its own message.
            TableOption::AutoIdCache(value) => {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|_| DdlAdmissionError::new("AUTO_ID_CACHE needs an integer value"))?;
                if parsed > i64::MAX as u64 {
                    return Err(DdlAdmissionError::new(
                        "table option auto_id_cache overflows int64",
                    ));
                }
                auto_id_cache = parsed as i64;
            }
            TableOption::AutoIncrement(value) => {
                auto_inc_id = value.parse().map_err(|_| {
                    DdlAdmissionError::new(format!(
                        "CREATE TABLE AUTO_INCREMENT = {value} is not an integer this node can store"
                    ))
                })?;
            }
            TableOption::AutoRandomBase(value) => {
                auto_rand_id = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::new(format!(
                        "CREATE TABLE AUTO_RANDOM_BASE = {value} is not an integer this node can store"
                    ))
                })? as i64;
            }
            // Go's own `handleTableOptions` records nothing for these: the
            // storage engine is always InnoDB in name only, and the statistics
            // options are MySQL compatibility no-ops.
            TableOption::Engine(_) | TableOption::StatsPersistent | TableOption::PackKeys => {}
            other => {
                return Err(DdlAdmissionError::new(format!(
                    "CREATE TABLE option {other:?} is not supported by this node"
                )))
            }
        }
    }
    let (table_charset, table_collate) = resolve_charset_collation(
        declared_charset.as_deref(),
        declared_collate.as_deref(),
        db_charset,
        db_collate,
    )?;

    // Go `buildColumnsAndConstraints`: the table-level PRIMARY KEY is located
    // first because every column needs to know whether it is one of its keys.
    let mut constraints = Vec::new();
    for constraint in &create.table_constraints {
        // Go `ast.ConstraintCheck` with the flag off — the DEFAULT — warns
        // and skips (`ddl/create_table.go:1470`), exactly like the
        // column-level spelling.
        if matches!(constraint, TableConstraint::Check(_)) {
            context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
            continue;
        }
        constraints.push(lower_table_constraint(constraint)?);
    }
    let out_primary_key: Option<Constraint> = constraints
        .iter()
        .find(|constraint| constraint.kind == ConstraintKind::PrimaryKey)
        .cloned();

    let mut columns = Vec::with_capacity(create.columns.len());
    for (offset, column) in create.columns.iter().enumerate() {
        let (info, inline) = build_column(
            offset,
            column,
            out_primary_key.as_ref(),
            &table_charset,
            &table_collate,
            context,
        )?;
        // An inline PRIMARY KEY / UNIQUE becomes a constraint of its own, in
        // the position Go appends it: after every table-level one.
        constraints.extend(inline);
        columns.push(info);
    }

    let mut seen: Vec<String> = Vec::with_capacity(columns.len());
    for column in &columns {
        let lowercase = column.name.lowercase().to_owned();
        if seen.contains(&lowercase) {
            return Err(DdlAdmissionError::new(format!(
                "CREATE TABLE declares column `{}` twice",
                column.name.original()
            )));
        }
        seen.push(lowercase);
    }

    // Go `setColumnFlagWithConstraint`, run over every constraint once every
    // column exists.
    for constraint in &constraints {
        stamp_constraint_flags(&mut columns, constraint)?;
    }

    let mut table = build_table(
        create,
        columns,
        constraints,
        table_charset,
        table_collate,
        clustered_mode,
    )?;
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
    if let Some(spec) = tidb_executor::ddl::auto_random::validate(create, &fields, &handle_offsets)
        .map_err(default_admission_error)?
    {
        table.auto_random_bits = spec.shard_bits;
        table.auto_random_range_bits = spec.range_bits;
    }
    table.comment = comment;
    table.auto_inc_id = auto_inc_id;
    table.auto_id_cache = auto_id_cache;
    table.auto_rand_id = auto_rand_id;
    Ok(table)
}

/// One constraint, in the single normalized shape Go's `ast.Constraint` has by
/// the time `BuildTableInfo` sees it — inline column options included.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Constraint {
    kind: ConstraintKind,
    name: String,
    parts: Vec<KeyPart>,
    clustered: Option<PrimaryKeyStorage>,
    comment: String,
    invisible: bool,
    global: bool,
    index_type: Option<IndexType>,
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
}

fn lower_table_constraint(constraint: &TableConstraint) -> Refusal<Constraint> {
    let TableConstraint::Index(index) = constraint else {
        return Err(DdlAdmissionError::new(
            "CREATE TABLE FOREIGN KEY constraints are not supported by this node",
        ));
    };
    let kind = match index.kind {
        IndexConstraintKind::PrimaryKey => ConstraintKind::PrimaryKey,
        IndexConstraintKind::Unique
        | IndexConstraintKind::UniqueKey
        | IndexConstraintKind::UniqueIndex => ConstraintKind::Unique,
        IndexConstraintKind::Key | IndexConstraintKind::Index => ConstraintKind::Key,
        other => {
            return Err(DdlAdmissionError::new(format!(
                "CREATE TABLE {other:?} indexes are not supported by this node"
            )))
        }
    };
    let mut parts = Vec::with_capacity(index.parts.len());
    for part in &index.parts {
        match part {
            IndexPart::Column {
                name,
                prefix_len,
                desc,
            } => {
                if *desc {
                    return Err(DdlAdmissionError::new(
                        "CREATE TABLE descending index parts are not supported by this node",
                    ));
                }
                parts.push(KeyPart {
                    name: name.clone(),
                    prefix_len: prefix_len.unwrap_or(UNSPECIFIED_LENGTH),
                });
            }
            IndexPart::Expr { .. } => {
                return Err(DdlAdmissionError::new(
                    "CREATE TABLE expression index parts are not supported by this node",
                ))
            }
        }
    }
    Ok(Constraint {
        kind,
        // Go `setEmptyConstraintName`: an unnamed key takes its first column's
        // name. A PRIMARY KEY is always renamed to `PRIMARY` later.
        name: index.name.clone().unwrap_or_else(|| {
            parts
                .first()
                .map(|part| part.name.clone())
                .unwrap_or_default()
        }),
        parts,
        clustered: index.options.primary_key_storage,
        comment: index.options.comment.clone().unwrap_or_default(),
        invisible: index.options.visibility == Some(tidb_ast::IndexVisibility::Invisible),
        global: index.options.global,
        index_type: index_type_of(&index.options),
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

/// Builds the one column an `ALTER TABLE ... ADD COLUMN` appends, against the
/// stored table's charset pair — Go `buildColumnAndConstraint` run by the
/// add-column DDL job. The nullable-no-default shape is the one this tier
/// serves: existing rows then read the implicit NULL default with no rewrite,
/// which is also MySQL's answer. Every option that would need a row rewrite
/// or a second allocator is refused BY NAME.
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
    context: &tidb_executor::StmtContext,
) -> Refusal<ColumnInfo> {
    for option in &column.options {
        match option {
            ColumnOption::Null
            | ColumnOption::Comment(_)
            | ColumnOption::NotNull
            | ColumnOption::Default(_) => {}
            other => {
                return Err(DdlAdmissionError::unsupported(format!(
                    "ADD COLUMN {} waits on its DDL course",
                    column_option_sql_name(other)
                )))
            }
        }
    }
    let (mut info, constraints) =
        build_column(0, column, None, table_charset, table_collate, context)?;
    if !constraints.is_empty() {
        return Err(DdlAdmissionError::unsupported(
            "ADD COLUMN must not introduce constraints on this node",
        ));
    }
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
            // Go `generateOriginDefaultValue` stamps time.Now() once, at DDL
            // time: existing rows report the moment the column was added,
            // while the DECLARED default stays the word CURRENT_TIMESTAMP for
            // every later INSERT to evaluate. TIMESTAMP stamps the UTC wall
            // clock, DATETIME the local one — Go's exact split.
            Some(match info.field_type.code() {
                FieldTypeCode::Timestamp => crate::mysql_bootstrap::utc_now_timestamp().to_string(),
                FieldTypeCode::Datetime => {
                    use chrono::{Datelike, Timelike};
                    let now = chrono::Local::now();
                    tidb_datatype::Time::from_date_checked(
                        now.year(),
                        i32::try_from(now.month()).expect("a month fits in i32"),
                        i32::try_from(now.day()).expect("a day fits in i32"),
                        i32::try_from(now.hour()).expect("an hour fits in i32"),
                        i32::try_from(now.minute()).expect("a minute fits in i32"),
                        i32::try_from(now.second()).expect("a second fits in i32"),
                        0,
                        tidb_datatype::TimeType::DateTime,
                        0,
                    )
                    .expect("the current local calendar date is a valid datetime")
                    .to_string()
                }
                other => {
                    return Err(DdlAdmissionError::unsupported(format!(
                        "a CURRENT_TIMESTAMP default on a {other:?} column is not a shape \
                         Go's DDL stamps"
                    )))
                }
            })
        } else if info.default_is_expr {
            return Err(DdlAdmissionError::unsupported(
                "ADD COLUMN with an expression default waits on its DDL course",
            ));
        } else {
            Some(text)
        }
    } else if info.field_type.has_flag(FieldTypeFlags::NOT_NULL) {
        // Go `table.GetZeroValue(col).ToString()`, for the families this
        // node's CREATE TABLE admits.
        Some(match info.field_type.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
            | FieldTypeCode::Bit => "0".to_owned(),
            FieldTypeCode::Float | FieldTypeCode::Double | FieldTypeCode::NewDecimal => {
                "0".to_owned()
            }
            FieldTypeCode::Varchar
            | FieldTypeCode::String
            | FieldTypeCode::VarString
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob => String::new(),
            FieldTypeCode::Date => "0000-00-00".to_owned(),
            FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                "0000-00-00 00:00:00".to_owned()
            }
            FieldTypeCode::Duration => "00:00:00".to_owned(),
            other => {
                return Err(DdlAdmissionError::unsupported(format!(
                    "ADD COLUMN ... NOT NULL of type {other:?} has no zero value this node can stamp yet"
                )))
            }
        })
    } else {
        None
    };
    if let Some(origin) = origin {
        info.set_origin_default_value(ColumnDefaultValue::str(&origin))
            .map_err(|error| DdlAdmissionError::new(error.to_string()))?;
    }
    Ok(info)
}

/// Go `buildColumnAndConstraint` + `columnDefToCol` for one column.
fn build_column(
    offset: usize,
    column: &ColumnDef,
    out_primary_key: Option<&Constraint>,
    table_charset: &str,
    table_collate: &str,
    context: &tidb_executor::StmtContext,
) -> Refusal<(ColumnInfo, Vec<Constraint>)> {
    let name = &column.name;
    if !column.qualifier.is_empty() {
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` carries a qualifier, which CREATE TABLE does not accept here"
        )));
    }

    // Go `getCharsetAndCollateInColumnDef`: the declared CHARSET, plus the LAST
    // COLLATE option, before the table's own pair fills the gaps.
    let declared_charset = column.ty.charset.clone();
    let mut declared_collate: Option<String> = None;
    for option in &column.options {
        if let ColumnOption::Collate(collate) = option {
            let info = get_collation_by_name(collate).map_err(|error| {
                DdlAdmissionError::new(format!(
                    "column `{name}` declares COLLATE {collate}: {error}"
                ))
            })?;
            if let Some(charset) = &declared_charset {
                if !charset.eq_ignore_ascii_case(&info.charset_name) {
                    return Err(DdlAdmissionError::new(format!(
                        "column `{name}` declares COLLATE {collate}, which is not valid for \
                         CHARACTER SET {charset}"
                    )));
                }
            }
            declared_collate = Some(info.name);
        }
    }
    let (charset, mut collate) = resolve_charset_collation(
        declared_charset.as_deref(),
        declared_collate.as_deref(),
        table_charset,
        table_collate,
    )?;
    // Go `OverwriteCollationWithBinaryFlag`: a bare `... BINARY` modifier means
    // the charset's *_bin collation, unless charset+collate were both explicit.
    if declared_charset.is_none() || declared_collate.is_none() {
        let code = column_type_code(&column.ty)?;
        if code.is_string() && column.ty.binary {
            collate = get_default_collation(&charset).map_err(|error| {
                DdlAdmissionError::new(format!("column `{name}` charset {charset}: {error}"))
            })?;
        }
    }

    // `build_field_type` is what actually stamps `binary`/`binary` on a type
    // that carries no charset, so the resolved pair above is only an input.
    let mut field_type = build_field_type(name, &column.ty, &charset, &collate)?;
    // Go `checkColumnAttributes` -- see the shared helper. This tier reports
    // it as an admission refusal rather than a coded client error, which is
    // the same reduction every other refusal here takes.
    if let Err(error) = tidb_executor::ddl::column_field_type::check_column_attributes(&field_type)
    {
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` is refused by checkColumnAttributes: {error:?}"
        )));
    }

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
    for option in &column.options {
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
                    constraints.push(Constraint {
                        kind: ConstraintKind::PrimaryKey,
                        name: name.clone(),
                        parts: vec![KeyPart {
                            name: name.clone(),
                            prefix_len: UNSPECIFIED_LENGTH,
                        }],
                        clustered: storage,
                        comment: String::new(),
                        invisible: false,
                        global: inline.global,
                        index_type: None,
                    });
                    field_type.add_flags(FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL);
                }
                InlineKeyKind::Unique => {
                    if field_type.has_flag(FieldTypeFlags::UNIQUE_KEY) {
                        continue;
                    }
                    constraints.push(Constraint {
                        kind: ConstraintKind::Unique,
                        name: name.clone(),
                        parts: vec![KeyPart {
                            name: name.clone(),
                            prefix_len: UNSPECIFIED_LENGTH,
                        }],
                        clustered: None,
                        comment: String::new(),
                        invisible: false,
                        global: inline.global,
                        index_type: None,
                    });
                    field_type.add_flags(FieldTypeFlags::UNIQUE_KEY);
                }
            },
            ColumnOption::Default(expr) => {
                staged_default = Some(stage_column_default(name, &field_type, expr, context)?);
                remove_on_update_now(&mut field_type);
            }
            ColumnOption::OnUpdate(expr) => {
                if !matches!(
                    field_type.code(),
                    FieldTypeCode::Timestamp | FieldTypeCode::Datetime
                ) {
                    return Err(DdlAdmissionError::new(format!(
                        "column `{name}` declares ON UPDATE on a type that is not TIMESTAMP or \
                         DATETIME"
                    )));
                }
                if !is_current_timestamp(expr) {
                    return Err(DdlAdmissionError::new(format!(
                        "column `{name}` declares an ON UPDATE that is not CURRENT_TIMESTAMP"
                    )));
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
                    return Err(DdlAdmissionError::new(format!(
                        "Incorrect column specifier for column '{name}'"
                    )));
                }
                field_type.add_flags(FieldTypeFlags::AUTO_INCREMENT | FieldTypeFlags::NOT_NULL);
            }
            // The persisted bit layout is validated and installed after the
            // clustered handle has been selected, because that decision is
            // part of the AUTO_RANDOM contract.
            ColumnOption::AutoRandom(_) => {}
            ColumnOption::Comment(comment) => info.comment = comment.clone(),
            // Already folded into the charset/collation resolution above.
            ColumnOption::Collate(collate) => {
                if field_type.has_charset() {
                    field_type.set_collation_name(collate.clone());
                }
            }
            // Go `ast.ColumnOptionCheck` with the flag off — the DEFAULT —
            // warns `tidb_enable_check_constraint is off`
            // (`ddl/add_column.go:577`, `errCheckConstraintIsOff`) and
            // IGNORES the option. The flag-on constraint machinery is
            // unported; a node that refused here diverged from every
            // default-configured Go server (probe 24). The cluster DDL
            // route drains this context's warnings into the connection's
            // buffer at admission (Session::drain_context_warnings), so
            // SHOW WARNINGS carries them as Go's does.
            ColumnOption::Check(_) => {
                context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
            }
            other => {
                return Err(DdlAdmissionError::new(format!(
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
            tidb_executor::DriverError::InvalidDefault(name.to_owned()),
        ));
    }

    // Only after the inline precheck does Go stamp a table-level PRIMARY KEY
    // naming this column.
    if !field_type.has_flag(FieldTypeFlags::PRI_KEY) {
        if let Some(primary_key) = out_primary_key {
            if primary_key
                .parts
                .iter()
                .any(|part| part.name.eq_ignore_ascii_case(name))
            {
                field_type.add_flags(FieldTypeFlags::PRI_KEY);
            }
        }
    }
    if field_type.has_flag(FieldTypeFlags::PRI_KEY) && has_null_flag {
        return Err(default_admission_error(
            tidb_executor::DriverError::PrimaryCantHaveNull,
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

/// Go `AlterColumn` followed by `updateColumnDefaultValue`: replaces one
/// existing column's DEFAULT in place, or drops it.
///
/// `default_value` is `None` for `DROP DEFAULT`, which Go answers by storing
/// no value and setting `NoDefaultValueFlag`; anything else goes through the
/// same staging and validation the column's own definition went through, so
/// an `ALTER` and a `CREATE` cannot disagree about what a spelling means.
pub(crate) fn set_column_default(
    name: &str,
    info: &mut ColumnInfo,
    default_value: Option<&Expr>,
    context: &tidb_executor::StmtContext,
) -> Refusal<()> {
    // Go clears both marks first, then either re-sets them for the DROP form
    // or lets the staging path decide.
    info.field_type.del_flags(FieldTypeFlags::NO_DEFAULT_VALUE);
    info.default_is_expr = false;
    let Some(expr) = default_value else {
        info.set_default_value(GoAny::nil())
            .map_err(|error| DdlAdmissionError::new(error.to_string()))?;
        info.field_type.add_flags(FieldTypeFlags::NO_DEFAULT_VALUE);
        return Ok(());
    };
    let field_type = info.field_type.clone();
    let staged = stage_column_default(name, &field_type, expr, context)?;
    // Go `updateColumnDefaultValue` turns a staged value that carries no
    // default at all into `ErrInvalidDefaultValue`, which is why
    // `sql_mode=''` plus `SET DEFAULT ''` on TEXT is 1067 here even though
    // the same spelling is accepted when the column is first defined.
    if !staged.has_default() {
        return Err(default_admission_error(
            tidb_executor::DriverError::InvalidDefault(name.to_owned()),
        ));
    }
    persist_column_default(name, info, staged, context)
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
            .position(|column| column.name.lowercase().eq_ignore_ascii_case(name))
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
fn build_table(
    create: &CreateTableStmt,
    mut columns: Vec<ColumnInfo>,
    constraints: Vec<Constraint>,
    charset: String,
    collate: String,
    clustered_mode: ClusteredIndexDefMode,
) -> Refusal<TableInfo> {
    let table_name = create
        .name
        .last()
        .cloned()
        .ok_or_else(|| DdlAdmissionError::new("CREATE TABLE names no table"))?;
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
    table.columns = columns.into();

    if constraints
        .iter()
        .filter(|constraint| constraint.kind == ConstraintKind::PrimaryKey)
        .count()
        > 1
    {
        return Err(DdlAdmissionError::new(
            "CREATE TABLE declares more than one PRIMARY KEY",
        ));
    }

    for constraint in &constraints {
        if constraint.kind == ConstraintKind::PrimaryKey {
            let single_int = is_single_int_primary_key(constraint, &table);
            if should_build_clustered_index(clustered_mode, constraint.clustered, single_int) {
                if single_int {
                    table.pk_is_handle = true;
                } else {
                    table.is_common_handle = true;
                    table.common_handle_version = 1;
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
        let mut index_columns = Vec::with_capacity(constraint.parts.len());
        let mut part_lengths: Vec<(tidb_datatype::FieldType, i64)> =
            Vec::with_capacity(constraint.parts.len());
        for part in &constraint.parts {
            let Some(column) = table.columns.iter_deref().find(|column| {
                column
                    .read()
                    .name
                    .lowercase()
                    .eq_ignore_ascii_case(&part.name)
            }) else {
                return Err(DdlAdmissionError::new(format!(
                    "index `{name}` names column `{}`, which the table does not declare",
                    part.name
                )));
            };
            // Go `checkIndexColumn` plus `buildIndexColumns`' normalization,
            // shared with the executor tier so the two builders cannot
            // disagree about which lengths are legal. This builder MODELS a
            // prefix -- `IndexColumn.length` is exactly what Go stores -- so
            // unlike the executor tier it keeps a legal one rather than
            // deferring; what it must not do is store an ILLEGAL one, which
            // is what it did before this call existed.
            let column = column.read();
            let length = prefix_length(&column.field_type, column.name.original(), part)?;
            part_lengths.push((column.field_type.clone(), length));
            index_columns.push(IndexColumn {
                name: column.name.clone(),
                offset: column.offset,
                length,
                ..IndexColumn::default()
            });
        }
        // Go `buildIndexColumns`: the sum of every key part's stored bytes
        // must stay within `config.MaxIndexLength`. Each part above may be
        // legal on its own and the total still refused.
        key_length_sum(
            part_lengths
                .iter()
                .map(|(field_type, length)| (field_type, *length)),
            constraint.parts.len(),
            primary || constraint.kind == ConstraintKind::Unique,
        )?;
        table.max_index_id += 1;
        table.indices.push_go(IndexInfo {
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
            ..IndexInfo::default()
        });
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
        .find(|column| {
            column
                .read()
                .name
                .lowercase()
                .eq_ignore_ascii_case(&part.name)
        })
        // Go `isIntCol` looks at the type alone: an UNSIGNED BIGINT primary
        // key is just as much a clustered handle as a signed one.
        .is_some_and(|column| column.read().field_type.code().is_type_integer())
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
        None => match mode {
            ClusteredIndexDefMode::On => true,
            ClusteredIndexDefMode::IntOnly => single_int_primary_key,
            ClusteredIndexDefMode::Off => false,
        },
    }
}

/// Go `ResolveCharsetCollation` over one `(column, table)` pair.
pub(crate) fn resolve_charset_collation(
    charset: Option<&str>,
    collate: Option<&str>,
    fallback_charset: &str,
    fallback_collate: &str,
) -> Refusal<(String, String)> {
    for (charset, collate) in [
        (charset, collate),
        (Some(fallback_charset), Some(fallback_collate)),
    ] {
        if let Some(collate) = collate.filter(|collate| !collate.is_empty()) {
            let info = get_collation_by_name(collate)
                .map_err(|error| DdlAdmissionError::new(format!("COLLATE {collate}: {error}")))?;
            if let Some(charset) = charset.filter(|charset| !charset.is_empty()) {
                if !charset.eq_ignore_ascii_case(&info.charset_name) {
                    // Go `ErrCollationCharsetMismatch` (1253), not the
                    // generic refusal: a client that offers a charset menu
                    // switches on this code.
                    return Err(DdlAdmissionError::with_code(
                        1253,
                        format!("COLLATION '{collate}' is not valid for CHARACTER SET '{charset}'"),
                    ));
                }
            }
            return Ok((info.charset_name, info.name));
        }
        if let Some(charset) = charset.filter(|charset| !charset.is_empty()) {
            let collate = get_default_collation(charset).map_err(|error| {
                DdlAdmissionError::new(format!("CHARACTER SET {charset}: {error}"))
            })?;
            return Ok((charset.to_ascii_lowercase(), collate));
        }
    }
    let (charset, collate) = get_default_charset_and_collate();
    Ok((charset.to_owned(), collate.to_owned()))
}

impl From<ColumnTypeError> for DdlAdmissionError {
    fn from(error: ColumnTypeError) -> Self {
        Self::new(error.reason)
    }
}

/// The shared [`shared_column_type_code`] reported as this builder's refusal.
fn column_type_code(declared: &ColumnType) -> Refusal<FieldTypeCode> {
    Ok(shared_column_type_code(declared)?)
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

/// Whether one expression is `CURRENT_TIMESTAMP` in any of Go's spellings.
fn is_current_timestamp(expr: &Expr) -> bool {
    match expr {
        Expr::Func { name, .. } => matches!(
            name.to_ascii_uppercase().as_str(),
            "CURRENT_TIMESTAMP" | "NOW" | "LOCALTIME" | "LOCALTIMESTAMP"
        ),
        Expr::Column(path) => matches!(
            path.as_slice(),
            [only] if only.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
        ),
        _ => false,
    }
}

/// A DEFAULT retained between Go's option-order storage stages and its final
/// `checkDefaultValue` pass.
enum StagedColumnDefault {
    /// A temporal marker is computed at row-write time and therefore has no
    /// settled spelling to cast through the final column type.
    TemporalMarker(String),
    /// A literal's exact metadata spelling, including fixed-binary padding.
    Settled(tidb_executor::ddl::SettledColumnDefault),
}

impl StagedColumnDefault {
    fn has_default(&self) -> bool {
        match self {
            Self::TemporalMarker(_) => true,
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

fn default_admission_error(error: tidb_executor::DriverError) -> DdlAdmissionError {
    let error = error.to_mysql_error();
    DdlAdmissionError::with_code(error.code, error.message)
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
    context: &tidb_executor::StmtContext,
) -> Refusal<StagedColumnDefault> {
    let built = tidb_executor::column_default::build(expr, field_type, |expr| {
        let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
            expr,
            &tidb_expr::rewriter::ZonedNoResolver::with_like_default_escape(
                context.session_zone(),
                context.like_default_escape(),
            ),
        )
        .map_err(|_| {
            tidb_executor::column_default::DefaultError::Unsupported(
                "a DEFAULT this node cannot evaluate",
            )
        })?;
        tidb_expr::eval_expression_once(&rewritten, context).map_err(|_| {
            tidb_executor::column_default::DefaultError::Unsupported(
                "a DEFAULT this node cannot evaluate",
            )
        })
    })
    .map_err(|error| default_admission_error(error.into_driver_error(name)))?;

    match built {
        tidb_executor::column_default::ColumnDefault::Computed(computed) => {
            if computed.is_expr() {
                return Err(DdlAdmissionError::new(format!(
                    "column `{name}` uses a computed DEFAULT this catalog writer cannot execute"
                )));
            }
            Ok(StagedColumnDefault::TemporalMarker(computed.text))
        }
        tidb_executor::column_default::ColumnDefault::Value(value) => {
            let zone = context.session_zone();
            let settled =
                tidb_executor::ddl::settle_column_default(value, field_type, name, context, &zone)
                    .map_err(default_admission_error)?;
            Ok(StagedColumnDefault::Settled(settled))
        }
    }
}

/// Go `checkDefaultValue` followed by `ColumnInfo.SetDefaultValue`, after the
/// final FieldType has been installed. The setter is load-bearing for BIT:
/// it retains the raw-byte shadow alongside the JSON-facing default value.
fn persist_column_default(
    name: &str,
    info: &mut ColumnInfo,
    staged: StagedColumnDefault,
    context: &tidb_executor::StmtContext,
) -> Refusal<()> {
    match staged {
        StagedColumnDefault::TemporalMarker(text) => {
            info.default_is_expr = false;
            info.set_default_value(ColumnDefaultValue::str(&text))
                .map_err(|error| DdlAdmissionError::new(error.to_string()))?;
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
                    tidb_executor::DriverError::PrimaryCantHaveNull,
                ));
            }
            let zone = context.session_zone();
            tidb_executor::ddl::validate_column_default(
                &settled.stored,
                &info.field_type,
                name,
                info.version,
                context.ddl_default_conversion_flags(),
                &zone,
            )
            .map_err(default_admission_error)?;
            let stored = if settled.stored.is_null() {
                GoAny::nil()
            } else {
                ColumnDefaultValue::string_bytes(settled.stored.sql_bytes().map_err(|_| {
                    DdlAdmissionError::new(format!("column `{name}` has an invalid default value"))
                })?)
                .into()
            };
            info.set_default_value(stored)
                .map_err(|error| DdlAdmissionError::new(error.to_string()))?;
        }
    }
    Ok(())
}

/// Classic bootstrap's exact DDL context.
///
/// Accepted Go constructs every `mysql.*` table with
/// `exprstatic.WithSQLMode(mysql.ModeNone)`. In particular, that makes the
/// empty `LONGTEXT` default on `mysql.global_priv.Priv` a warning while still
/// running the ordinary default validation and storage path.
fn bootstrap_ddl_statement_context() -> tidb_executor::StmtContext {
    tidb_executor::StmtContext::for_query()
        .with_strict(false)
        .with_date_modes(tidb_datatype::DateModes::default())
        .with_time_zone(SessionTimeZone::utc())
}

/// TiDB's shipped DDL mode in UTC, for non-session live compatibility
/// callers.
///
/// Live statement paths must pass their own context through
/// [`build_table_info_with_context`]. Keeping this constructor explicit makes
/// the legacy live wrappers deterministic without leaking bootstrap's
/// `ModeNone` into user DDL.
pub(crate) fn default_ddl_statement_context() -> tidb_executor::StmtContext {
    tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
        .with_time_zone(SessionTimeZone::utc())
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
