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
    InlineKeyKind, PrimaryKeyStorage, TableConstraint, TableOption, UnaryOp,
};
use tidb_datatype::{
    get_collation_by_name, get_default_charset_and_collate, get_default_collation, FieldType,
    FieldTypeCode, FieldTypeFlags,
};
// The declared-type -> `FieldType` rule set is SHARED with the runnable-path
// `CREATE TABLE` builder; see `column_field_type`'s module doc for why.
use tidb_executor::ddl::column_field_type::{
    build_field_type as build_shared_field_type, column_type_code as shared_column_type_code,
    ColumnTypeError, BINARY_CHARSET,
};
use tidb_model::column::{ColumnDefaultValue, ColumnInfo, CURR_LATEST_COLUMN_INFO_VERSION};
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
    use tidb_executor::ddl::index_prefix::{stored_index_length, PrefixError};

    let declared = (part.prefix_len != UNSPECIFIED_LENGTH).then_some(part.prefix_len);
    stored_index_length(field_type, column, declared, true).map_err(|error| match error {
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
        PrefixError::TooLongKey { length, max } => DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrTooLongKey,
            format!("Specified key was too long ({length} bytes); max key length is {max} bytes"),
        ),
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

/// Builds the `TableInfo` a real TiDB would persist for one `CREATE TABLE`.
///
/// `db_charset`/`db_collate` are the owning database's, which a table with no
/// charset option of its own inherits. The returned table carries no ID and no
/// `update_ts`: those belong to the transaction that publishes it.
pub fn build_table_info(
    create: &CreateTableStmt,
    db_charset: &str,
    db_collate: &str,
    clustered_mode: ClusteredIndexDefMode,
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

    // Go `GetCharsetAndCollateInTableOption`: the LAST declared pair wins, and
    // whatever it leaves unset the database supplies.
    let mut declared_charset = None;
    let mut declared_collate = None;
    let mut comment = String::new();
    let mut auto_inc_id = 0i64;
    for option in &create.table_options {
        match option {
            TableOption::CharacterSet(charset) => declared_charset = Some(charset.clone()),
            TableOption::Collate(collate) => declared_collate = Some(collate.clone()),
            TableOption::Comment(text) => comment = text.clone(),
            TableOption::AutoIncrement(value) => {
                auto_inc_id = value.parse().map_err(|_| {
                    DdlAdmissionError::new(format!(
                        "CREATE TABLE AUTO_INCREMENT = {value} is not an integer this node can store"
                    ))
                })?;
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
    table.comment = comment;
    table.auto_inc_id = auto_inc_id;
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
            "CREATE TABLE CHECK and FOREIGN KEY constraints are not supported by this node",
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
    options.index_type.map(|declared| match declared {
        tidb_ast::IndexType::HASH => IndexType::HASH,
        tidb_ast::IndexType::RTREE => IndexType::RTREE,
        _ => IndexType::BTREE,
    })
}

/// Go `buildColumnAndConstraint` + `columnDefToCol` for one column.
fn build_column(
    offset: usize,
    column: &ColumnDef,
    out_primary_key: Option<&Constraint>,
    table_charset: &str,
    table_collate: &str,
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

    let mut info = ColumnInfo {
        id: 0,
        name: CiString::new(name.clone()),
        offset: i32::try_from(offset).expect("a column offset fits in i32"),
        origin_default_value: None,
        origin_default_value_bit: None,
        default_value: None,
        default_value_bit: None,
        default_is_expr: false,
        generated_expr_string: String::new(),
        generated_stored: false,
        dependences: std::collections::BTreeSet::new(),
        field_type: FieldType::new(FieldTypeCode::LongLong),
        changing_field_type: None,
        state: SchemaState::PUBLIC,
        comment: String::new(),
        hidden: false,
        change_state_info: None,
        version: CURR_LATEST_COLUMN_INFO_VERSION,
    };

    let mut constraints = Vec::new();
    let mut has_default_value = false;
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
                has_default_value = set_default_value(name, &mut info, &field_type, expr)?;
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
            ColumnOption::Comment(comment) => info.comment = comment.clone(),
            // Already folded into the charset/collation resolution above.
            ColumnOption::Collate(collate) => {
                if field_type.has_charset() {
                    field_type.set_collation_name(collate.clone());
                }
            }
            other => {
                return Err(DdlAdmissionError::new(format!(
                    "column `{name}` carries {}, which this node does not support",
                    describe_column_option(other)
                )))
            }
        }
    }

    // Go `checkPriKeyConstraint`: a table-level PRIMARY KEY naming this column
    // stamps the flag even though the column itself declared nothing.
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
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` is a PRIMARY KEY column declared NULL"
        )));
    }

    // Go `processDefaultValue` then `processColumnFlags`.
    process_default_value(
        &mut info,
        &mut field_type,
        has_default_value,
        set_on_update_now,
    );
    process_column_flags(&mut field_type);
    info.field_type = field_type;
    Ok((info, constraints))
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
            info.default_value = Some(ColumnDefaultValue::str(if set_on_update_now {
                "0000-00-00 00:00:00"
            } else {
                "CURRENT_TIMESTAMP"
            }));
        }
        if field_type.code() == FieldTypeCode::Year && field_type.has_flag(FieldTypeFlags::NOT_NULL)
        {
            info.default_value = Some(ColumnDefaultValue::str("0000"));
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

/// Go `processColumnFlags`: the binary flag follows the resolved charset, and
/// `ZEROFILL` implies `UNSIGNED`.
fn process_column_flags(field_type: &mut FieldType) {
    if field_type.eval_type().is_string_kind() {
        if field_type.charset_name() == BINARY_CHARSET {
            field_type.add_flags(FieldTypeFlags::BINARY);
        } else {
            field_type.del_flags(FieldTypeFlags::BINARY);
        }
    }
    if field_type.code() == FieldTypeCode::Bit {
        field_type.del_flags(FieldTypeFlags::BINARY);
        field_type.add_flags(FieldTypeFlags::UNSIGNED);
    }
    if field_type.code() == FieldTypeCode::Year {
        field_type.del_flags(FieldTypeFlags::BINARY);
        field_type.add_flags(FieldTypeFlags::ZEROFILL);
    }
    if field_type.has_flag(FieldTypeFlags::ZEROFILL) {
        field_type.add_flags(FieldTypeFlags::UNSIGNED);
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
                if column.default_value.is_none()
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
    table.columns = columns;

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
        for part in &constraint.parts {
            let Some(column) = table
                .columns
                .iter()
                .find(|column| column.name.lowercase().eq_ignore_ascii_case(&part.name))
            else {
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
            let length = prefix_length(&column.field_type, column.name.original(), part)?;
            index_columns.push(IndexColumn {
                name: column.name.clone(),
                offset: column.offset,
                length: i32::try_from(length).unwrap_or(-1),
                ..IndexColumn::default()
            });
        }
        table.max_index_id += 1;
        table.indices.push(IndexInfo {
            id: table.max_index_id,
            name: CiString::new(name),
            table: CiString::default(),
            columns: index_columns,
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
        .iter()
        .find(|column| column.name.lowercase().eq_ignore_ascii_case(&part.name))
        // Go `isIntCol` looks at the type alone: an UNSIGNED BIGINT primary
        // key is just as much a clustered handle as a signed one.
        .is_some_and(|column| column.field_type.code().is_type_integer())
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
fn resolve_charset_collation(
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

/// Go `SetDefaultValue` -> `getDefaultValue` -> `checkColumnDefaultValue`,
/// bounded to the literal and `CURRENT_TIMESTAMP` defaults a `CREATE TABLE`
/// actually writes.
///
/// The stored form is always Go's own: a *string*, whatever the column type,
/// because `getDefaultValue` finishes with `Datum.ToString()`. An integer
/// default is stored as its decimal text, and a `TIMESTAMP`'s
/// `CURRENT_TIMESTAMP` as that literal word.
fn set_default_value(
    name: &str,
    info: &mut ColumnInfo,
    field_type: &FieldType,
    expr: &Expr,
) -> Refusal<bool> {
    let code = field_type.code();
    let is_time = matches!(
        code,
        FieldTypeCode::Timestamp | FieldTypeCode::Datetime | FieldTypeCode::Date
    );
    if is_time && is_current_timestamp(expr) {
        info.default_value = Some(ColumnDefaultValue::str("CURRENT_TIMESTAMP"));
        return Ok(true);
    }
    let Some(text) = literal_default_text(expr, code) else {
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` declares a DEFAULT this node cannot evaluate; it accepts a \
             literal or CURRENT_TIMESTAMP"
        )));
    };
    let Some(text) = text else {
        // `DEFAULT NULL`: Go records that a default was declared but stores
        // none, which is exactly what keeps `NoDefaultValueFlag` off.
        return Ok(true);
    };
    // Go `checkColumnDefaultValue`: in non-strict mode an EMPTY default on a
    // TEXT/BLOB column is stored but not counted as "has a default", so the
    // column still gets `NoDefaultValueFlag`; a non-empty one is an error.
    let blob_like = matches!(
        code,
        FieldTypeCode::Json
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
    );
    if blob_like {
        if !text.is_empty() {
            return Err(DdlAdmissionError::new(format!(
                "BLOB/TEXT/JSON column `{name}` can't have a default value"
            )));
        }
        let ignored = matches!(code, FieldTypeCode::Blob | FieldTypeCode::LongBlob);
        info.default_value = Some(ColumnDefaultValue::str(if code == FieldTypeCode::Json {
            "null"
        } else {
            ""
        }));
        return Ok(!ignored);
    }
    if matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) && !field_type.elems().is_empty() {
        // Go `getEnumDefaultValue`/`getSetDefaultValue`: the stored form is the
        // member's own spelling, matched case-insensitively.
        let matched = field_type
            .elems()
            .iter()
            .find(|elem| elem.eq_ignore_ascii_case(&text));
        let Some(matched) = matched else {
            return Err(DdlAdmissionError::new(format!(
                "column `{name}` has an invalid default value `{text}` for its {} members",
                if code == FieldTypeCode::Enum {
                    "ENUM"
                } else {
                    "SET"
                }
            )));
        };
        info.default_value = Some(ColumnDefaultValue::str(matched));
        return Ok(true);
    }
    info.default_value = Some(ColumnDefaultValue::str(&text));
    Ok(true)
}

/// The stored text of one literal default, or `None` when the expression is
/// not a literal this builder evaluates. `Some(None)` is `DEFAULT NULL`.
fn literal_default_text(expr: &Expr, code: FieldTypeCode) -> Option<Option<String>> {
    match expr {
        Expr::Null => Some(None),
        Expr::String(value) | Expr::RawString(value) => Some(Some(value.clone())),
        // Go evaluates TRUE/FALSE to the integers 1/0 before storing them.
        Expr::Bool(value) => Some(Some(i64::from(*value).to_string())),
        Expr::Int(digits) => Some(Some(normalize_numeric_default(digits, code))),
        Expr::Decimal(digits) => Some(Some(normalize_numeric_default(digits, code))),
        Expr::Float(value) => Some(Some(normalize_numeric_default(&value.to_string(), code))),
        Expr::Unary(UnaryOp::Minus, inner) => {
            let Some(Some(text)) = literal_default_text(inner, code) else {
                return None;
            };
            Some(Some(match text.strip_prefix('-') {
                Some(positive) => positive.to_owned(),
                None => format!("-{text}"),
            }))
        }
        Expr::Unary(UnaryOp::Plus, inner) => literal_default_text(inner, code),
        _ => None,
    }
}

/// Go converts a numeric default to the column's own type before storing it,
/// so an integer column's default is integer text.
fn normalize_numeric_default(text: &str, code: FieldTypeCode) -> String {
    if !code.is_type_integer() {
        return text.to_owned();
    }
    match text.parse::<f64>() {
        // Go's `ConvertTo` rounds half away from zero, as MySQL does.
        Ok(value) => format!("{}", value.round() as i64),
        Err(_) => text.to_owned(),
    }
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
