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

//! Go `planner/core/preprocess.go`'s grammar checks over DDL statements.
//!
//! Go's preprocessor walks every statement before it is planned, privilege
//! checked or run, and refuses the DDL whose names or declarations are wrong
//! on their face: an empty or space-terminated identifier, a second primary
//! key, an index named `PRIMARY`, a zero key length, a column type out of
//! range. Its order is the walk's: the statement's own checks on entry, the
//! constraint nodes beneath it, then the checks Go runs on leaving a CREATE
//! TABLE (`checkAutoIncrement`, `checkContainDotColumn`). The first error
//! stops the walk. The checks that need the infoschema (a LIKE source, a
//! DROP TEMPORARY target) stay with the session, which owns the catalog.

use tidb_ast::{
    AlterPartitionAction, AlterTableAction, ColumnDef, ColumnOption, ColumnTypeArg,
    CreateTableStmt, CreateTableTemporary, DdlStmt, IndexConstraintKind, IndexPart, Stmt,
    TableConstraint, TableOption,
};
use tidb_datatype::FieldTypeCode;

use crate::DriverError;

/// `mysql.MaxKeyParts`.
const MAX_KEY_PARTS: usize = 16;
/// `mysql.MaxFieldCharLength`.
const MAX_FIELD_CHAR_LENGTH: i64 = 255;
/// `mysql.MaxFieldVarCharLength`.
const MAX_FIELD_VARCHAR_LENGTH: i64 = 65535;
/// `mysql.MaxDoublePrecisionLength`.
const MAX_DOUBLE_PRECISION_LENGTH: i64 = 53;
/// `mysql.MaxFloatingTypeScale`.
const MAX_FLOATING_TYPE_SCALE: i64 = 30;
/// `mysql.MaxFloatingTypeWidth`.
const MAX_FLOATING_TYPE_WIDTH: i64 = 255;
/// `mysql.MaxDecimalScale`.
const MAX_DECIMAL_SCALE: i64 = 30;
/// `mysql.MaxDecimalWidth`.
const MAX_DECIMAL_WIDTH: i64 = 65;
/// `mysql.MaxBitDisplayWidth`.
const MAX_BIT_DISPLAY_WIDTH: i64 = 64;
/// `mysql.MaxTypeSetMembers`.
const MAX_TYPE_SET_MEMBERS: usize = 64;
/// `auth.UserNameMaxLength`.
const USER_NAME_MAX_LENGTH: usize = 32;
/// `auth.HostNameMaxLength`.
const HOST_NAME_MAX_LENGTH: usize = 255;

/// Go's preprocessor grammar checks over one DDL statement. `strict` is the
/// session's strict SQL mode, which decides whether an over-long VARCHAR is
/// an error or is left to be converted to TEXT/BLOB (`hasAutoConvertWarning`).
pub fn check_ddl_grammar(stmt: &Stmt, strict: bool) -> Result<(), DriverError> {
    let Stmt::Ddl(ddl) = stmt else {
        return Ok(());
    };
    match ddl.as_ref() {
        DdlStmt::CreateDatabase { name, .. } | DdlStmt::DropDatabase { name, .. } => {
            check_database_name(name)
        }
        // `ALTER DATABASE` with no name alters the current database.
        DdlStmt::AlterDatabase { name, .. } => name.as_deref().map_or(Ok(()), check_database_name),
        DdlStmt::CreateTable(create) => check_create_table(create, strict),
        DdlStmt::CreateView(create) => {
            check_table_name(path_name(&create.name))?;
            for column in &create.columns {
                if is_incorrect_identifier_name(column) {
                    return Err(DriverError::WrongColumnName(column.clone()));
                }
            }
            if !create.definer.current_user {
                check_definer_length(&create.definer.user, "user name", USER_NAME_MAX_LENGTH)?;
                check_definer_length(&create.definer.host, "host name", HOST_NAME_MAX_LENGTH)?;
            }
            Ok(())
        }
        DdlStmt::DropTable(drop) => {
            for path in &drop.names {
                check_table_name(path_name(path))?;
            }
            Ok(())
        }
        DdlStmt::DropSequence(drop) => {
            for path in &drop.names {
                check_table_name(path_name(path))?;
            }
            Ok(())
        }
        // Go checks the first pair only (`checkRenameTableGrammar`).
        DdlStmt::RenameTable(rename) => match rename.pairs.first() {
            Some((from, to)) => {
                check_table_name(path_name(from))?;
                check_table_name(path_name(to))
            }
            None => Ok(()),
        },
        DdlStmt::CreateIndex(create) => {
            check_table_name(path_name(&create.table))?;
            if create.name.is_empty() {
                return Err(wrong_name_for_index(&create.name));
            }
            check_index_info(&create.name, &create.parts)
        }
        DdlStmt::AlterTable(alter) => check_alter_table(alter),
        _ => Ok(()),
    }
}

/// Go `util.IsInCorrectIdentifierName`: an empty name, or one ending in a
/// space.
fn is_incorrect_identifier_name(name: &str) -> bool {
    name.is_empty() || name.ends_with(' ')
}

/// The last segment of a written object path: the object's own name.
fn path_name(path: &[String]) -> &str {
    path.last().map_or("", String::as_str)
}

/// `dbterror.ErrWrongDBName` (1102).
fn wrong_db_name(name: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: tidb_error::mysql::errcode::ErrWrongDBName,
        message: format!("Incorrect database name '{name}'"),
    }
}

/// `dbterror.ErrWrongNameForIndex` (1280).
pub(crate) fn wrong_name_for_index(name: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: tidb_error::mysql::errcode::ErrWrongNameForIndex,
        message: format!("Incorrect index name '{name}'"),
    }
}

/// `dbterror.ErrWrongPartitionName` (1567).
pub(crate) fn wrong_partition_name() -> DriverError {
    DriverError::DdlCoded {
        errno: tidb_error::mysql::errcode::ErrWrongPartitionName,
        message: "Incorrect partition name".to_owned(),
    }
}

fn check_database_name(name: &str) -> Result<(), DriverError> {
    if is_incorrect_identifier_name(name) {
        return Err(wrong_db_name(name));
    }
    Ok(())
}

fn check_table_name(name: &str) -> Result<(), DriverError> {
    if is_incorrect_identifier_name(name) {
        return Err(DriverError::Schema(crate::SchemaErrorKind::WrongTableName(
            name.to_owned(),
        )));
    }
    Ok(())
}

/// `dbterror.ErrWrongStringLength` (1470) over a view's DEFINER.
fn check_definer_length(value: &str, kind: &str, maximum: usize) -> Result<(), DriverError> {
    if value.len() > maximum {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::mysql::errcode::ErrWrongStringLength,
            message: format!(
                "String '{value}' is too long for {kind} (should be no longer than {maximum})"
            ),
        });
    }
    Ok(())
}

/// Go `checkCreateTableGrammar`, then the CREATE TABLE's constraint nodes,
/// then `checkAutoIncrement` and `checkContainDotColumn` on leaving it.
fn check_create_table(create: &CreateTableStmt, strict: bool) -> Result<(), DriverError> {
    let temporary = create.temporary != CreateTableTemporary::None;
    if temporary {
        for option in &create.table_options {
            match option {
                TableOption::ShardRowIdBits(_) => {
                    return Err(DriverError::OptOnTemporaryTable("shard_row_id_bits"))
                }
                TableOption::PlacementPolicy(_) => {
                    return Err(DriverError::OptOnTemporaryTable("PLACEMENT"))
                }
                _ => {}
            }
        }
    }
    check_table_name(path_name(&create.name))?;
    let mut primary_keys = 0;
    for column in &create.columns {
        if let Err(error) = check_column(column) {
            // Go issue #30328: outside strict mode an over-long VARCHAR is
            // converted to TEXT/BLOB with a warning instead
            // (`hasAutoConvertWarning`).
            let converts = !strict
                && matches!(error, ColumnCheckError::TooBigFieldLength)
                && is_varchar(column);
            if !converts {
                return Err(error.into_driver_error(column));
            }
        }
        primary_keys += check_column_options(temporary, &column.options)?;
        if primary_keys > 1 {
            return Err(DriverError::MultiplePrimaryKey);
        }
    }
    for constraint in &create.table_constraints {
        match constraint {
            TableConstraint::Index(index) => match index.kind {
                IndexConstraintKind::PrimaryKey => {
                    if primary_keys > 0 {
                        return Err(DriverError::MultiplePrimaryKey);
                    }
                    primary_keys += 1;
                    check_index_info(index.name.as_deref().unwrap_or(""), &index.parts)?;
                }
                IndexConstraintKind::Key
                | IndexConstraintKind::Index
                | IndexConstraintKind::Unique
                | IndexConstraintKind::UniqueKey
                | IndexConstraintKind::UniqueIndex => {
                    let name = index.name.as_deref().unwrap_or("");
                    check_index_info(name, &index.parts)?;
                    if index.is_empty_index {
                        return Err(wrong_name_for_index(name));
                    }
                }
                _ => {}
            },
            TableConstraint::ForeignKey(foreign_key) => {
                let name = foreign_key.name.as_deref().unwrap_or("");
                check_index_info(name, &foreign_key.parts)?;
            }
            _ => {}
        }
    }
    super::validate_table_options(&create.table_options)?;
    if create.ctas.is_some() {
        return Err(DriverError::unsupported(
            "'CREATE TABLE ... SELECT' is not implemented yet",
        ));
    }
    if create.columns.is_empty() && create.like_table.is_none() {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::mysql::errcode::ErrTableMustHaveColumns,
            message: "A table must have at least 1 column".to_owned(),
        });
    }
    if let Some(partitioning) = &create.partitioning {
        if partitioning
            .definitions
            .iter()
            .any(|definition| is_incorrect_identifier_name(&definition.name))
        {
            return Err(wrong_partition_name());
        }
    }
    check_auto_increment(create)?;
    check_contain_dot_column(create)
}

/// Go `checkAlterTableGrammar`.
fn check_alter_table(alter: &tidb_ast::AlterTableStmt) -> Result<(), DriverError> {
    check_table_name(path_name(&alter.name))?;
    for action in &alter.actions {
        if let AlterTableAction::RenameTable { new_name } = action {
            check_table_name(path_name(new_name))?;
        }
        let new_columns: &[ColumnDef] = match action {
            AlterTableAction::AddColumn { column, .. }
            | AlterTableAction::ModifyColumn { column, .. }
            | AlterTableAction::ChangeColumn { column, .. } => std::slice::from_ref(column),
            AlterTableAction::AddColumns { columns, .. } => columns,
            _ => &[],
        };
        for column in new_columns {
            check_column(column).map_err(|error| error.into_driver_error(column))?;
        }
        if let AlterTableAction::SetTableOptions { options } = action {
            super::validate_table_options(options)?;
        }
        match action {
            AlterTableAction::AddIndexConstraint(index)
                if matches!(
                    index.kind,
                    IndexConstraintKind::Key
                        | IndexConstraintKind::Index
                        | IndexConstraintKind::Unique
                        | IndexConstraintKind::UniqueIndex
                        | IndexConstraintKind::UniqueKey
                        | IndexConstraintKind::PrimaryKey
                ) =>
            {
                check_index_info(index.name.as_deref().unwrap_or(""), &index.parts)?;
            }
            AlterTableAction::AddStatistics { name, .. }
            | AlterTableAction::DropStatistics { name, .. }
                if is_incorrect_identifier_name(name) =>
            {
                return Err(DriverError::unsupported(format!(
                    "Incorrect statistics name: {name}"
                )));
            }
            AlterTableAction::Partition(AlterPartitionAction::Add {
                spec: tidb_ast::AddPartitionSpec::Definitions(definitions),
                ..
            }) if definitions
                .iter()
                .any(|definition| is_incorrect_identifier_name(&definition.name)) =>
            {
                return Err(wrong_partition_name());
            }
            _ => {}
        }
    }
    Ok(())
}

/// Go `checkIndexInfo`: the index name, the number of key parts, zero
/// prefix lengths and repeated columns.
fn check_index_info(name: &str, parts: &[IndexPart]) -> Result<(), DriverError> {
    if name.eq_ignore_ascii_case("PRIMARY") {
        return Err(wrong_name_for_index(name));
    }
    if parts.len() > MAX_KEY_PARTS {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::mysql::errcode::ErrTooManyKeyParts,
            message: format!("Too many key parts specified; max {MAX_KEY_PARTS} parts allowed"),
        });
    }
    for part in parts {
        if let IndexPart::Column {
            name,
            prefix_len: Some(0),
            ..
        } = part
        {
            return Err(DriverError::KeyPart0(name.clone()));
        }
    }
    // Go `checkDuplicateColumnName`.
    let mut seen: Vec<String> = Vec::with_capacity(parts.len());
    for part in parts {
        if let IndexPart::Column { name, .. } = part {
            let folded = name.to_lowercase();
            if seen.contains(&folded) {
                return Err(DriverError::DuplicateColumnName(name.clone()));
            }
            seen.push(folded);
        }
    }
    Ok(())
}

/// Go `checkColumnOptions`: whether the column declares an inline primary
/// key, refusing a virtual generated one and AUTO_RANDOM on a temporary
/// table.
fn check_column_options(temporary: bool, options: &[ColumnOption]) -> Result<usize, DriverError> {
    let mut primary = 0;
    let mut generated = false;
    let mut stored = false;
    for option in options {
        match option {
            ColumnOption::InlineKey(key)
                if matches!(key.kind, tidb_ast::InlineKeyKind::Primary { .. }) =>
            {
                primary = 1;
            }
            ColumnOption::Generated {
                stored: is_stored, ..
            } => {
                generated = true;
                stored = *is_stored;
            }
            ColumnOption::AutoRandom(_) if temporary => {
                return Err(DriverError::OptOnTemporaryTable("auto_random"));
            }
            _ => {}
        }
    }
    if primary > 0 && generated && !stored {
        return Err(DriverError::UnsupportedOnGeneratedColumn(
            "Defining a virtual generated column as primary key".to_owned(),
        ));
    }
    Ok(primary)
}

/// Why Go's `checkColumn` refuses a column definition.
enum ColumnCheckError {
    WrongColumnName,
    InvalidDefault,
    DisplayWidthOutOfRange,
    TooBigFieldLength,
    WrongFieldSpec,
    TooBigScale { scale: i64, maximum: i64 },
    TooBigDisplayWidth { maximum: i64 },
    MBiggerThanD,
    TooBigSet,
    IllegalSetValue(String),
    TooBigPrecision { precision: i64 },
    InvalidFieldSize,
}

impl ColumnCheckError {
    fn into_driver_error(self, column: &ColumnDef) -> DriverError {
        use tidb_error::mysql::errcode;
        let name = &column.name;
        let coded = |errno: u16, message: String| DriverError::DdlCoded { errno, message };
        match self {
            Self::WrongColumnName => DriverError::WrongColumnName(name.clone()),
            Self::InvalidDefault => coded(
                errcode::ErrInvalidDefault,
                format!("Invalid default value for '{name}'"),
            ),
            Self::DisplayWidthOutOfRange => coded(
                errcode::ErrTooBigDisplaywidth,
                format!("Display width out of range for column '{name}' (max = {})", u32::MAX),
            ),
            Self::TooBigFieldLength => {
                let maximum = if is_varchar(column) {
                    MAX_FIELD_VARCHAR_LENGTH
                } else {
                    MAX_FIELD_CHAR_LENGTH
                };
                coded(
                    errcode::ErrTooBigFieldlength,
                    format!(
                        "Column length too big for column '{name}' (max = {maximum}); use BLOB or TEXT instead"
                    ),
                )
            }
            Self::WrongFieldSpec => coded(
                errcode::ErrWrongFieldSpec,
                format!("Incorrect column specifier for column '{name}'"),
            ),
            Self::TooBigScale { scale, maximum } => coded(
                errcode::ErrTooBigScale,
                format!("Too big scale {scale} specified for column '{name}'. Maximum is {maximum}."),
            ),
            Self::TooBigDisplayWidth { maximum } => coded(
                errcode::ErrTooBigDisplaywidth,
                format!("Display width out of range for column '{name}' (max = {maximum})"),
            ),
            Self::MBiggerThanD => DriverError::MBiggerThanD(name.clone()),
            Self::TooBigSet => coded(
                errcode::ErrTooBigSet,
                format!("Too many strings for column {name} and SET"),
            ),
            Self::IllegalSetValue(value) => coded(
                errcode::ErrIllegalValueForType,
                format!("Illegal set '{value}' value found during parsing"),
            ),
            Self::TooBigPrecision { precision } => coded(
                errcode::ErrTooBigPrecision,
                format!(
                    "Too-big precision {precision} specified for '{name}'. Maximum is {MAX_DECIMAL_WIDTH}."
                ),
            ),
            Self::InvalidFieldSize => coded(
                errcode::ErrInvalidFieldSize,
                format!("Invalid size for column '{name}'."),
            ),
        }
    }
}

/// Whether the declared type is VARCHAR or VARBINARY (Go `TypeVarchar`).
fn is_varchar(column: &ColumnDef) -> bool {
    super::column_field_type::column_type_code(&column.ty)
        .is_ok_and(|code| code == FieldTypeCode::Varchar)
}

/// One numeric type argument as Go's parser stores it.
fn type_arg(argument: &ColumnTypeArg) -> Option<i64> {
    match argument {
        ColumnTypeArg::Text(text) => text.parse().ok(),
        ColumnTypeArg::Bytes(_) => None,
    }
}

/// Go `checkColumn`: the column's name, a NOW() default on a type that is
/// not a timestamp, and the declared type's range limits. A type this
/// preprocessor cannot read is left to the DDL builder, which reports it.
fn check_column(column: &ColumnDef) -> Result<(), ColumnCheckError> {
    if is_incorrect_identifier_name(&column.name) {
        return Err(ColumnCheckError::WrongColumnName);
    }
    let Ok(code) = super::column_field_type::column_type_code(&column.ty) else {
        return Ok(());
    };
    if is_invalid_default_value(column, code) {
        return Err(ColumnCheckError::InvalidDefault);
    }
    let args = &column.ty.args;
    let numeric = |index: usize| args.get(index).and_then(type_arg);
    let flen = if matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
        None
    } else {
        numeric(0)
    };
    if flen.is_some_and(|flen| flen > i64::from(u32::MAX)) {
        return Err(ColumnCheckError::DisplayWidthOutOfRange);
    }
    match code {
        FieldTypeCode::String => {
            if flen.is_some_and(|flen| flen > MAX_FIELD_CHAR_LENGTH) {
                return Err(ColumnCheckError::TooBigFieldLength);
            }
        }
        // Go checks a VARCHAR here only when its charset is already known
        // (`tp.GetCharset()` is set by the parser for VARBINARY and an
        // explicit CHARACTER SET); a binary charset admits 65535 bytes. A
        // multi-byte charset's limit is left to the DDL builder.
        FieldTypeCode::Varchar => {
            let binary = super::column_field_type::is_intrinsically_binary(&column.ty.name)
                || column
                    .ty
                    .charset
                    .as_deref()
                    .is_some_and(|charset| charset.eq_ignore_ascii_case("binary"));
            if binary && flen.is_some_and(|flen| flen > MAX_FIELD_VARCHAR_LENGTH) {
                return Err(ColumnCheckError::TooBigFieldLength);
            }
        }
        FieldTypeCode::Float | FieldTypeCode::Double => match (flen, numeric(1)) {
            (Some(flen), None) => {
                if code == FieldTypeCode::Float && flen > MAX_DOUBLE_PRECISION_LENGTH {
                    return Err(ColumnCheckError::WrongFieldSpec);
                }
            }
            (Some(flen), Some(decimal)) => {
                if decimal > MAX_FLOATING_TYPE_SCALE {
                    return Err(ColumnCheckError::TooBigScale {
                        scale: decimal,
                        maximum: MAX_FLOATING_TYPE_SCALE,
                    });
                }
                if flen > MAX_FLOATING_TYPE_WIDTH || flen == 0 {
                    return Err(ColumnCheckError::TooBigDisplayWidth {
                        maximum: MAX_FLOATING_TYPE_WIDTH,
                    });
                }
                if flen < decimal {
                    return Err(ColumnCheckError::MBiggerThanD);
                }
            }
            _ => {}
        },
        FieldTypeCode::Set => {
            if args.len() > MAX_TYPE_SET_MEMBERS {
                return Err(ColumnCheckError::TooBigSet);
            }
            for argument in args {
                if let ColumnTypeArg::Text(member) = argument {
                    if member.contains(',') {
                        return Err(ColumnCheckError::IllegalSetValue(member.clone()));
                    }
                }
            }
        }
        FieldTypeCode::NewDecimal => {
            let decimal = numeric(1);
            if let Some(decimal) = decimal.filter(|decimal| *decimal > MAX_DECIMAL_SCALE) {
                return Err(ColumnCheckError::TooBigScale {
                    scale: decimal,
                    maximum: MAX_DECIMAL_SCALE,
                });
            }
            if let Some(flen) = flen.filter(|flen| *flen > MAX_DECIMAL_WIDTH) {
                return Err(ColumnCheckError::TooBigPrecision { precision: flen });
            }
            if let (Some(flen), Some(decimal)) = (flen, decimal) {
                if flen < decimal {
                    return Err(ColumnCheckError::MBiggerThanD);
                }
            }
        }
        FieldTypeCode::Bit => {
            if let Some(flen) = flen {
                if flen <= 0 {
                    return Err(ColumnCheckError::InvalidFieldSize);
                }
                if flen > MAX_BIT_DISPLAY_WIDTH {
                    return Err(ColumnCheckError::TooBigDisplayWidth {
                        maximum: MAX_BIT_DISPLAY_WIDTH,
                    });
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Go `isInvalidDefaultValue`: the LAST default is `NOW()` (which the parser
/// spells `CURRENT_TIMESTAMP`) on a type that is neither TIMESTAMP nor
/// DATETIME.
fn is_invalid_default_value(column: &ColumnDef, code: FieldTypeCode) -> bool {
    let Some(default) = column.options.iter().rev().find_map(|option| match option {
        ColumnOption::Default(expr) => Some(expr),
        _ => None,
    }) else {
        return false;
    };
    !matches!(code, FieldTypeCode::Timestamp | FieldTypeCode::Datetime)
        && matches!(default, tidb_ast::Expr::Func { name, .. }
            if name.eq_ignore_ascii_case("current_timestamp"))
}

/// Whether `expr` is what Go's parser builds as a `*driver.ValueExpr`
/// (`Some(true)` for the NULL literal). A signed number is a unary
/// expression there, not a value.
fn value_expr_is_null(expr: &tidb_ast::Expr) -> Option<bool> {
    use tidb_ast::Expr;
    match expr {
        Expr::Null => Some(true),
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::CharsetString { .. }
        | Expr::CharsetBinary { .. }
        | Expr::Bool(_) => Some(false),
        _ => None,
    }
}

/// Go `checkAutoIncrementOp` over the option at `index`: whether it is
/// AUTO_INCREMENT, refusing a default the column cannot take.
fn check_auto_increment_op(column: &ColumnDef, index: usize) -> Result<bool, DriverError> {
    let options = &column.options;
    let invalid_default = || DriverError::DdlCoded {
        errno: tidb_error::mysql::errcode::ErrInvalidDefault,
        message: format!("Invalid default value for '{}'", column.name),
    };
    let mut has_auto_increment = false;
    if matches!(options[index], ColumnOption::AutoIncrement) {
        has_auto_increment = true;
        for later in &options[index + 1..] {
            let ColumnOption::Default(expr) = later else {
                continue;
            };
            if value_expr_is_null(expr) == Some(false) {
                return Err(invalid_default());
            }
            if matches!(expr, tidb_ast::Expr::Func { name, .. } if name.eq_ignore_ascii_case("current_date"))
            {
                return Err(invalid_default());
            }
        }
    }
    if let ColumnOption::Default(expr) = &options[index] {
        if index + 1 != options.len() {
            if value_expr_is_null(expr) == Some(true) {
                return Ok(has_auto_increment);
            }
            if options[index + 1..]
                .iter()
                .any(|later| matches!(later, ColumnOption::AutoIncrement))
            {
                // A plain `errors.Errorf`, so 1105 with the same text.
                return Err(DriverError::unsupported(format!(
                    "Invalid default value for '{}'",
                    column.name
                )));
            }
        }
    }
    Ok(has_auto_increment)
}

/// Go `checkAutoIncrement`: one AUTO_INCREMENT column at most, of an
/// integer or floating type.
fn check_auto_increment(create: &CreateTableStmt) -> Result<(), DriverError> {
    let mut auto_columns = Vec::new();
    for column in &create.columns {
        let mut has_auto_increment = false;
        for index in 0..column.options.len() {
            if check_auto_increment_op(column, index)? {
                has_auto_increment = true;
            }
        }
        if has_auto_increment {
            auto_columns.push(column);
        }
    }
    if auto_columns.len() > 1 {
        return Err(DriverError::WrongAutoKey);
    }
    for column in auto_columns {
        let integer_or_float =
            super::column_field_type::column_type_code(&column.ty).is_ok_and(|code| {
                matches!(
                    code,
                    FieldTypeCode::Tiny
                        | FieldTypeCode::Short
                        | FieldTypeCode::Long
                        | FieldTypeCode::Float
                        | FieldTypeCode::Double
                        | FieldTypeCode::LongLong
                        | FieldTypeCode::Int24
                )
            });
        if !integer_or_float {
            return Err(DriverError::unsupported(format!(
                "Incorrect column specifier for column '{}'",
                column.name
            )));
        }
    }
    Ok(())
}

/// Go `checkContainDotColumn`: a column written `db.t.c` must name the
/// table being created.
fn check_contain_dot_column(create: &CreateTableStmt) -> Result<(), DriverError> {
    let table = path_name(&create.name);
    let schema = match create.name.as_slice() {
        [schema, _] => schema.as_str(),
        _ => "",
    };
    for column in &create.columns {
        let (column_schema, column_table) = match column.qualifier.as_slice() {
            [table] => ("", table.as_str()),
            [schema, table] => (schema.as_str(), table.as_str()),
            _ => ("", ""),
        };
        if !column_schema.is_empty() && column_schema != schema {
            return Err(wrong_db_name(column_schema));
        }
        if !column_table.is_empty() && column_table != table {
            return Err(DriverError::Schema(crate::SchemaErrorKind::WrongTableName(
                column_table.to_owned(),
            )));
        }
    }
    Ok(())
}
