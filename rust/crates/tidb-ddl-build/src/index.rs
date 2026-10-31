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

//! Go `pkg/ddl/index.go`: an index definition's key parts built into
//! `IndexColumn`s (`buildIndexColumns`, with `checkIndexColumn`,
//! `getIndexColumnLength`, `checkIndexPrefixLength` and `indexColumnsLen`),
//! and the checks on its condition and expression parts.
//!
//! `buildIndexColumns` is the ONE place a key part's declared length is
//! judged and normalized. CREATE TABLE (`BuildTableInfo` -> `BuildIndexInfo`),
//! ADD INDEX (`createIndex`, the in-process catalog and the cluster job alike)
//! and the hypothetical index all run it over the table's `ColumnInfo`s, so
//! whether `a(10)` is legal and what length TiDB then stores cannot differ by
//! route. The read side of an accepted prefix -- the index encoding
//! (`tidb_tablecodec::cut_datum_by_prefix_len`) and every read-path decision that would
//! otherwise assume an index entry holds the whole column value -- is not
//! decided here.

use tidb_datatype::{FieldTypeCode, FieldTypeFlags, UNSPECIFIED_LENGTH};
use tidb_terror::MysqlError;

use tidb_model::column::ColumnInfo;
use tidb_model::index::{ColumnarIndexType, IndexColumn, IndexInfo};
use tidb_model::table_info::TableInfo;
use tidb_hack::GoCaseCompare;
use tidb_context::WarnErr;
use tidb_hack::GoToLower;
use tidb_table::kv_table::KvColumn;


/// Go `CheckAndBuildIndexConditionString` (`pkg/ddl/index.go`): the checked
/// condition restored as `IndexInfo.ConditionExprString`, empty without one.
/// `partitioned` is whether the table already has partitions; CREATE TABLE
/// refuses a partitioned partial index later, in `buildTablePartitionInfo`.
pub fn check_and_build_index_condition_string(
    partitioned: bool,
    column: &dyn Fn(&str) -> Option<IndexConditionColumn>,
    condition: Option<&tidb_ast::Expr>,
    connection: (&str, &str),
) -> Result<String, MysqlError> {
    let Some(condition) = condition else {
        return Ok(String::new());
    };
    if partitioned {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX.generate_with_stack_by_args(&[
                "partial index on partitioned table is not supported".into(),
            ]),
        ));
    }
    check_index_condition(column, condition, connection)?;
    // The condition's plain string literals carry the connection charset Go's
    // parser typed them with.
    Ok(condition.restore_with_flags_and_string_charset(
        tidb_table::generated_column::generated_restore_flags(),
        connection.0,
    ))
}

/// Go `checkExpressionIndexAutoIncrement` (3754): an expression index may not
/// read the AUTO_INCREMENT column unless
/// `tidb_enable_auto_increment_in_generated` allows it.
///
/// # Errors
///
/// 3754 naming the index.
pub fn check_expression_index_auto_increment(
    index_name: &str,
    hidden: &[(usize, crate::expression_index::HiddenIndexColumn)],
    auto_increment_column: Option<&str>,
    ctx: &tidb_metabuild::Context,
) -> Result<(), MysqlError> {
    let Some(auto) = auto_increment_column.filter(|_| !ctx.enable_auto_increment_in_generated()) else {
        return Ok(());
    };
    if hidden.iter().any(|(_, column)| {
        column
            .generated
            .dependencies
            .iter()
            .any(|dependency| dependency.go_lower_eq(auto))
    }) {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_EXPRESSION_INDEX_CAN_NOT_REFER
                .generate_with_stack_by_args(&[index_name.to_owned().into()]),
        ));
    }
    Ok(())
}

/// What Go's `checkIndexCondition` reads of the column a condition names.
pub struct IndexConditionColumn {
    /// The column's type.
    pub field_type: tidb_datatype::FieldType,
    /// Whether the column is generated.
    pub generated: bool,
}

/// The type Go's parser driver gives an `ast.ValueExpr` literal
/// (`types.DefaultTypeForValue`, under the connection's charset and
/// collation); `None` for any other expression.
pub fn index_condition_literal_type(
    expression: &tidb_ast::Expr,
    (charset, collation): (&str, &str),
) -> Option<tidb_datatype::FieldType> {
    use tidb_datatype::{default_field_type_for_value, FieldTypeValue};
    Some(match expression {
        tidb_ast::Expr::Int(text) => match text.parse::<i64>() {
            Ok(value) => {
                default_field_type_for_value(FieldTypeValue::Signed(value), charset, collation)
            }
            Err(_) => default_field_type_for_value(
                FieldTypeValue::Unsigned(text.parse().ok()?),
                charset,
                collation,
            ),
        },
        tidb_ast::Expr::Decimal(text) => {
            let (decimal, _) = tidb_datatype::MyDecimal::from_string(text.as_bytes());
            default_field_type_for_value(
                FieldTypeValue::Decimal {
                    display_len: decimal.to_string_bytes().len() as i64,
                    fraction_digits: i64::from(decimal.digits_frac()),
                },
                charset,
                collation,
            )
        }
        tidb_ast::Expr::Float(value) => {
            default_field_type_for_value(FieldTypeValue::Float64(*value), charset, collation)
        }
        tidb_ast::Expr::Hex(digits) => {
            let literal = tidb_datatype::parse_hex_str(&format!("0x{digits}")).ok()?;
            default_field_type_for_value(
                FieldTypeValue::HexLiteral(literal.as_bytes()),
                charset,
                collation,
            )
        }
        tidb_ast::Expr::Bit(value) => default_field_type_for_value(
            FieldTypeValue::BitLiteral(value.as_bytes()),
            charset,
            collation,
        ),
        tidb_ast::Expr::String(value) | tidb_ast::Expr::RawString(value) => {
            default_field_type_for_value(FieldTypeValue::String(value), charset, collation)
        }
        tidb_ast::Expr::CharsetString {
            charset: introducer,
            value,
        } => {
            let introducer = introducer.to_ascii_lowercase();
            let collation = tidb_charset::get_default_collation(&introducer).ok()?;
            default_field_type_for_value(FieldTypeValue::String(value), &introducer, &collation)
        }
        tidb_ast::Expr::CharsetBinary {
            charset: introducer,
            value,
        } => {
            let introducer = introducer.to_ascii_lowercase();
            let collation = tidb_charset::get_default_collation(&introducer).ok()?;
            index_condition_literal_type(value, (charset, collation.as_str()))?
                .with_charset_name(&introducer)
                .with_collation_name(&collation)
        }
        tidb_ast::Expr::Null => {
            default_field_type_for_value(FieldTypeValue::Null, charset, collation)
        }
        tidb_ast::Expr::Bool(value) => {
            default_field_type_for_value(FieldTypeValue::Bool(*value), charset, collation)
        }
        _ => return None,
    })
}

/// Go `checkIndexCondition` (`pkg/ddl/index.go`): a partial index's
/// condition is `column IS [NOT] NULL`, or `column <op> literal` with `op`
/// one of `= != > < >= <=`, over a column that is not generated and a
/// literal whose type fits the column's. `column` finds a table column by its
/// lowercase name; `connection` is the charset and collation a literal takes.
pub fn check_index_condition(
    column: &dyn Fn(&str) -> Option<IndexConditionColumn>,
    condition: &tidb_ast::Expr,
    connection: (&str, &str),
) -> Result<(), MysqlError> {
    use tidb_datatype::FieldTypeCode as Tp;
    let name_of = |expression: &tidb_ast::Expr| match expression {
        tidb_ast::Expr::Column(path) => path.last().map(|name| tidb_hack::go_to_lower(name)),
        _ => None,
    };
    let generated = |name: &str| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX.generate_with_stack_by_args(&[
                (format!("generated column {name} cannot be used in partial index condition"))
                    .into(),
            ]),
        )
    };
    match condition {
        tidb_ast::Expr::Is {
            expr,
            target: tidb_ast::IsTarget::Null,
            ..
        } => {
            let Some(name) = name_of(expr) else {
                return Err(MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&["partial index condition must include a column name in the IS NULL expression".into()]),
    ));
            };
            let Some(info) = column(&name) else {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
                        .generate_with_stack_by_args(&[(format!(
                    "column name {name} referenced in partial index condition is not found in table"
                ))
                        .into()]),
                ));
            };
            if info.generated {
                return Err(generated(&name));
            }
            Ok(())
        }
        tidb_ast::Expr::Binary(op, left, right) => {
            if !matches!(
                op,
                tidb_ast::BinaryOp::Eq
                    | tidb_ast::BinaryOp::Ne
                    | tidb_ast::BinaryOp::Gt
                    | tidb_ast::BinaryOp::Lt
                    | tidb_ast::BinaryOp::Ge
                    | tidb_ast::BinaryOp::Le
            ) {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
                        .generate_with_stack_by_args(&[(format!(
                            "binary operation {} is not supported",
                            op.opcode().name()
                        ))
                        .into()]),
                ));
            }
            let (name, another_side) = match (name_of(left), name_of(right)) {
                (Some(name), _) => (name, right.as_ref()),
                (None, Some(name)) => (name, left.as_ref()),
                (None, None) => {
                    return Err(MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&["partial index condition must include a column name in the binary operation".into()]),
    ));
                }
            };
            let Some(info) = column(&name) else {
                return Err(MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&[(format!(
                    "column name `{name}` referenced in partial index condition is not found in table"
                )).into()]),
    ));
            };
            if info.generated {
                return Err(generated(&name));
            }
            let Some(literal) = index_condition_literal_type(another_side, connection) else {
                return Err(MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&["partial index condition must include a literal value on the other side of the binary operation".into()]),
    ));
            };
            let incompatible = || {
                MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&[(format!(
                    "the type {} of the column `{name}` in partial index condition is not compatible with the literal value type {literal}",
                    info.field_type
                )).into()]),
    )
            };
            let column_tp = info.field_type.code();
            match literal.code() {
                Tp::Tiny | Tp::Short | Tp::Long | Tp::LongLong | Tp::Int24 | Tp::Bit | Tp::Year => {
                    if !matches!(
                        column_tp,
                        Tp::Tiny
                            | Tp::Short
                            | Tp::Long
                            | Tp::LongLong
                            | Tp::Int24
                            | Tp::Bit
                            | Tp::Year
                            | Tp::Enum
                            | Tp::Set
                    ) {
                        return Err(incompatible());
                    }
                    Ok(())
                }
                Tp::Float | Tp::Double | Tp::NewDecimal => {
                    if !matches!(column_tp, Tp::Float | Tp::Double | Tp::NewDecimal) {
                        return Err(incompatible());
                    }
                    Ok(())
                }
                Tp::Varchar
                | Tp::VarString
                | Tp::String
                | Tp::TinyBlob
                | Tp::MediumBlob
                | Tp::LongBlob
                | Tp::Blob => {
                    // Go `types.IsString`.
                    if matches!(
                        column_tp,
                        Tp::String
                            | Tp::Varchar
                            | Tp::VarString
                            | Tp::TinyBlob
                            | Tp::MediumBlob
                            | Tp::LongBlob
                            | Tp::Blob
                            | Tp::Unspecified
                    ) {
                        if info.field_type.charset_name() != literal.charset_name() {
                            return Err(MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&[(format!(
                                "the charset {} of the column `{name}` in partial index condition is not compatible with the literal value charset {}",
                                info.field_type.charset_name(),
                                literal.charset_name()
                            )).into()]),
    ));
                        }
                        return Ok(());
                    }
                    if matches!(
                        column_tp,
                        Tp::Timestamp | Tp::Date | Tp::Duration | Tp::NewDate | Tp::Datetime
                    ) || matches!(column_tp, Tp::Enum | Tp::Set)
                    {
                        return Ok(());
                    }
                    Err(incompatible())
                }
                Tp::Null => Err(MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&["= NULL is not supported in partial index condition because it is always false".into()]),
    )),
                _ => Err(MysqlError::from_terror(
        &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
            .generate_with_stack_by_args(&[(format!(
                    "the type {literal} of the literal value in partial index condition is not supported"
                )).into()]),
    )),
            }
        }
        _ => Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX.generate_with_stack_by_args(&[
                "the kind of partial index condition is not supported".into(),
            ]),
        )),
    }
}

/// Go `setGlobalIndexVersion`: chooses the persisted key format for a newly
/// created global index from the cluster capability and table/index shape.
pub fn set_global_index_version(table: &TableInfo, index: &mut IndexInfo) {
    // Go `getNullColInfos`: each key part by name among ALL the table's
    // columns (an expression part's hidden column is not public yet).
    let part_types = index
        .columns
        .iter_deref()
        .filter_map(|part| {
            let name = part.read().name.lowercase().to_owned();
            table
                .columns
                .iter_deref()
                .find(|column| column.read().name.lowercase() == name)
                .map(|column| column.read().field_type.clone())
        })
        .collect::<Vec<_>>();
    index.global_index_version = global_index_version(
        index.global,
        table.has_clustered_index(),
        index.unique,
        &part_types,
    );
}

/// Go `setGlobalIndexVersion`'s decision: V1 (entries keyed by partition
/// too) for a global index on a table without a clustered index that is not
/// unique or covers a column that may hold NULL (no NOT NULL, or
/// PreventNullInsert), when the cluster supports it; legacy otherwise.
#[must_use]
pub fn global_index_version(
    global: bool,
    clustered: bool,
    unique: bool,
    part_types: &[tidb_datatype::FieldType],
) -> u8 {
    if !tidb_model::index::get_global_index_v1_supported() || !global || clustered {
        return tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY;
    }
    let nullable = part_types.iter().any(|field_type| {
        !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
            || field_type.has_flag(tidb_datatype::FieldTypeFlags::PREVENT_NULL_INSERT)
    });
    if !unique || nullable {
        tidb_model::index::GLOBAL_INDEX_VERSION_V1
    } else {
        tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY
    }
}

/// Go `types.ErrorLength`: a key part declared with length zero.
const ERROR_LENGTH: i64 = 0;

/// Go `config.GetGlobalConfig().MaxIndexLength`, read where Go reads it.
fn max_index_length() -> i64 {
    tidb_config::config_tree::config::get_global_config().max_index_length
}

/// Go `ast.IndexPartSpecification` as `buildIndexColumns` reads it: the
/// column the part names -- an expression part already rewritten to its
/// hidden column by `BuildHiddenColumnInfo` -- and its declared length
/// (`types.UnspecifiedLength` when none was written).
#[derive(Clone, Copy, Debug)]
pub struct IndexPartSpecification<'a> {
    /// Go `ip.Column.Name`.
    pub column: &'a str,
    /// Go `ip.Length`.
    pub length: i64,
}

/// Go `model.FindColumnInfo` over a plain column slice.
fn find_column_info<'a>(columns: &'a [ColumnInfo], name: &str) -> Option<&'a ColumnInfo> {
    let name = tidb_hack::go_to_lower(name);
    columns.iter().find(|column| column.name.lowercase() == name)
}

/// Go `buildIndexColumns` (`pkg/ddl/index.go:116`): each key part resolved to
/// its column, checked (`checkIndexColumn`, for a non-columnar index), its
/// full-length CHAR prefix normalized away, and the running byte total held
/// to `config.MaxIndexLength`. Returns the `IndexColumn`s and whether one
/// part is an ARRAY (`IndexInfo.MVIndex`).
///
/// `ctx` is Go's `*metabuild.Context`, `None` where Go passes `nil` (the
/// job worker's `BuildIndexInfo`), which is the strictest reading: the
/// per-part length check is never suppressed and an over-long total is
/// always an error.
///
/// # Errors
///
/// Go's key-part errors (1072, 1089, 1167, 1170, 1391, 3152, 3753, 3757,
/// 3761, 1071, 1235, 8200).
pub fn build_index_columns(
    ctx: Option<&tidb_metabuild::Context>,
    columns: &[ColumnInfo],
    index_part_specifications: &[IndexPartSpecification<'_>],
    columnar_index_type: ColumnarIndexType,
) -> Result<(Vec<IndexColumn>, bool), MysqlError> {
    let mut idx_parts = Vec::with_capacity(index_part_specifications.len());
    let mut mv_index = false;
    let max_index_length = max_index_length();
    // The sum of length of all index columns.
    let mut sum_length = 0;
    for ip in index_part_specifications {
        let Some(col) = find_column_info(columns, ip.column) else {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS
                    .generate_with_stack(format!("column does not exist: {}", ip.column)),
            ));
        };
        let code = col.field_type.code();
        if columnar_index_type == ColumnarIndexType::VECTOR && code != FieldTypeCode::VectorFloat32
        {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_ADD_VECTOR_INDEX.generate_with_stack_by_args(
                    &[format!(
                        "only support vector type, but this is type: {}",
                        col.field_type
                    )
                    .into()],
                ),
            ));
        }
        if columnar_index_type == ColumnarIndexType::INVERTED && !code.is_type_stored_as_integer() {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX
                    .generate_with_stack_by_args(&[format!(
                        "only support integer type, but this is type: {}",
                        col.field_type
                    )
                    .into()]),
            ));
        }
        if columnar_index_type == ColumnarIndexType::FULLTEXT && !code.is_string() {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX
                    .generate_with_stack_by_args(&[format!(
                        "only support string type, but this is type: {}",
                        col.field_type
                    )
                    .into()]),
            ));
        }

        // return error in strict sql mode
        if columnar_index_type == ColumnarIndexType::NA {
            check_index_column(
                col,
                ip.length,
                ctx.is_some_and(|ctx| {
                    !ctx.get_sql_mode().has_strict_mode() || ctx.suppress_too_long_index_err()
                }),
            )?;
        }
        if col.field_type.is_array() {
            if mv_index {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_NOT_SUPPORTED_YET.generate_with_stack_by_args(&[
                        "more than one multi-valued key part per index".into(),
                    ]),
                ));
            }
            mv_index = true;
        }
        let mut index_col_len = ip.length;
        if index_col_len != UNSPECIFIED_LENGTH
            && code.is_type_char()
            && index_col_len == col.field_type.flen()
        {
            index_col_len = UNSPECIFIED_LENGTH;
        }
        sum_length += get_index_column_length(col, index_col_len, columnar_index_type)?;

        if !ctx.is_some_and(tidb_metabuild::Context::suppress_too_long_index_err)
            && sum_length > max_index_length
        {
            // The sum of all lengths must be shorter than the max length for
            // prefix. A multi-column or unique key over the limit is an error
            // rather than a warning.
            let truncate = ctx.filter(|ctx| {
                !ctx.get_sql_mode().has_strict_mode()
                    && col.get_flag() & u64::from(FieldTypeFlags::UNIQUE_KEY) == 0
                    && index_part_specifications.len() <= 1
            });
            let Some(ctx) = truncate else {
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_TOO_LONG_KEY
                        .generate_with_stack_by_args(&[sum_length.into(), max_index_length.into()]),
                ));
            };
            // Truncate the index length and warn in non-strict SQL mode.
            let col_len_per_uint = get_index_column_length(col, 1, columnar_index_type)?;
            index_col_len = max_index_length / col_len_per_uint;
            ctx.append_warning(WarnErr::Terror(tidb_dbterror::ERR_TOO_LONG_KEY.generate(
                format!(
                    "Specified key was too long ({sum_length} bytes); max key length is \
                     {max_index_length} bytes"
                ),
            )));
        }

        idx_parts.push(IndexColumn {
            name: col.name.clone(),
            offset: col.offset,
            length: index_col_len,
            ..IndexColumn::default()
        });
    }
    Ok((idx_parts, mv_index))
}

/// Go `checkIndexPrefixLength` (`pkg/ddl/index.go:216`): the bytes of every
/// key part of one index ([`index_columns_len`]) may not total more than
/// `config.MaxIndexLength`. Unlike [`build_index_columns`]' running sum there
/// is no downgrade and the error reports the whole total.
///
/// # Errors
///
/// 1072 for a part naming no column, 1071 carrying the total.
pub fn check_index_prefix_length(
    columns: &[ColumnInfo],
    idx_columns: &[IndexColumn],
    columnar_index_type: ColumnarIndexType,
) -> Result<(), MysqlError> {
    let idx_len = index_columns_len(columns, idx_columns, columnar_index_type)?;
    let max_index_length = max_index_length();
    if idx_len > max_index_length {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_TOO_LONG_KEY
                .generate_with_stack_by_args(&[idx_len.into(), max_index_length.into()]),
        ));
    }
    Ok(())
}

/// Go `indexColumnsLen`: the bytes every key part of `idx_cols` occupies,
/// each resolved against `cols` by name.
///
/// # Errors
///
/// 1072 for a part naming no column, or [`get_index_column_length`]'s.
pub fn index_columns_len(
    cols: &[ColumnInfo],
    idx_cols: &[IndexColumn],
    columnar_index_type: ColumnarIndexType,
) -> Result<i64, MysqlError> {
    let mut col_len = 0;
    for idx_col in idx_cols {
        let Some(col) = find_column_info(cols, idx_col.name.lowercase()) else {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS.generate_with_stack(format!(
                    "column does not exist: {}",
                    idx_col.name.lowercase()
                )),
            ));
        };
        col_len += get_index_column_length(col, idx_col.length, columnar_index_type)?;
    }
    Ok(col_len)
}

/// Go `checkIndexColumn` (`pkg/ddl/index.go:240`), run for every
/// non-columnar key part INCLUDING the hidden column an expression part was
/// rewritten into: four of its arms report a different error for a hidden
/// column (3761 for 1167, 3753 for 3152, 3757 for 1170), naming
/// `col.GeneratedExprString` rather than the column.
///
/// The order of the arms is observable -- a `blob` key part with a zero
/// length is 1391 and not 1170, and a `json` key part with a prefix is 3152
/// and not 1089 -- and is Go's.
///
/// # Errors
///
/// The first of Go's key-part errors the part trips.
pub fn check_index_column(
    col: &ColumnInfo,
    index_column_len: i64,
    suppress_too_long_key_err: bool,
) -> Result<(), MysqlError> {
    let code = col.field_type.code();
    let flen = col.field_type.flen();
    if code == FieldTypeCode::Null
        || (flen == 0 && (code.is_type_char() || code.is_type_varchar()))
    {
        if col.hidden {
            return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_KEY_COLUMN_FUNCTIONAL_INDEX.generate_with_stack_by_args(&[(col.generated_expr_string.clone()).to_string().into()])));
        }
        return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_KEY_COLUMN.generate_with_stack_by_args(&[(col.name.original().to_owned()).to_string().into()])));
    }

    // JSON column cannot index.
    if code == FieldTypeCode::Json && !col.field_type.is_array() {
        if col.hidden {
            return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION.generate_with_stack_by_args(&[])));
        }
        return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_JSON_USED_AS_KEY.generate_with_stack_by_args(&[(col.name.original().to_owned()).to_string().into()])));
    }

    if code == FieldTypeCode::VectorFloat32 {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX
                .generate_with_stack("only VECTOR INDEX can be added to vector column"),
        ));
    }

    // Length must be specified and non-zero for BLOB and TEXT column indexes.
    if code.is_type_blob() {
        if index_column_len == UNSPECIFIED_LENGTH {
            if col.hidden {
                return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_FUNCTIONAL_INDEX_ON_BLOB.generate_with_stack_by_args(&[])));
            }
            return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_BLOB_KEY_WITHOUT_LENGTH.generate_with_stack_by_args(&[(col.name.original().to_owned()).to_string().into()])));
        }
        if index_column_len == ERROR_LENGTH {
            return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_KEY_PART0.generate_with_stack_by_args(&[(col.name.original().to_owned()).to_string().into()])));
        }
    }

    // Length can only be specified for specifiable types.
    if index_column_len != UNSPECIFIED_LENGTH && !code.is_type_prefixable() {
        return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_INCORRECT_PREFIX_KEY.generate_with_stack_by_args(&[])));
    }

    // Key length must be shorter or equal to the column length.
    if index_column_len != UNSPECIFIED_LENGTH && code.is_type_char() {
        if flen < index_column_len {
            return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_INCORRECT_PREFIX_KEY.generate_with_stack_by_args(&[])));
        }
        // Length must be non-zero for char.
        if index_column_len == ERROR_LENGTH {
            return Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_KEY_PART0.generate_with_stack_by_args(&[(col.name.original().to_owned()).to_string().into()])));
        }
    }

    let mut index_column_len = index_column_len;
    if code.is_string() {
        index_column_len *= charset_maxlen(col.field_type.charset_name())
            .map_err(|error| MysqlError::unknown(error))?;
    }
    // Specified length must be shorter than the max length for prefix.
    let max_index_length = max_index_length();
    if index_column_len > max_index_length && !suppress_too_long_key_err {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_TOO_LONG_KEY
                .generate_with_stack_by_args(&[index_column_len.into(), max_index_length.into()]),
        ));
    }
    Ok(())
}

/// Go `charset.GetCharsetInfo(cs).Maxlen`, with Go's lookup error text.
fn charset_maxlen(charset: &str) -> Result<i64, String> {
    tidb_charset::get_charset_info(charset)
        .map(|info| i64::try_from(info.maxlen).expect("a charset's maxlen fits in i64"))
        .map_err(|error| error.to_string())
}

/// Go `getIndexColumnLength` (`pkg/ddl/index.go:307`): the BYTES one key part
/// occupies in the index.
///
/// This is not the declared length. A key part with no declared length takes
/// the COLUMN's own `flen` -- which is why `KEY(a, b)` over two
/// `varchar(600)` utf8mb4 columns is 4800 bytes even though neither part
/// carries a prefix -- and a fixed-width type takes its storage size from
/// `mysql.DefaultLengthOfMysqlTypes` regardless of its display width. A
/// columnar index writes no KV index, so each of its parts counts 1.
///
/// # Errors
///
/// Go `ErrUnsupportedCharset` for a string column whose charset is unknown.
pub fn get_index_column_length(
    col: &ColumnInfo,
    col_len: i64,
    columnar_index_type: ColumnarIndexType,
) -> Result<i64, MysqlError> {
    use tidb_datatype::FieldTypeCode as Code;

    if columnar_index_type != ColumnarIndexType::NA {
        // Columnar index does not actually create KV index, so it has length
        // of 0; 1 keeps the other calculations away from 0.
        return Ok(1);
    }

    let length = if col_len != UNSPECIFIED_LENGTH {
        col_len
    } else {
        // Go keeps `UnspecifiedLength` when the column's own flen is, which
        // is the same number.
        col.field_type.flen()
    };

    Ok(match col.field_type.code() {
        // A BIT column is packed eight values to the byte.
        Code::Bit => (length + 7) >> 3,
        Code::Varchar
        | Code::String
        | Code::VarString
        | Code::TinyBlob
        | Code::MediumBlob
        | Code::Blob
        | Code::LongBlob => {
            // Different charsets occupy different numbers of bytes on each
            // character.
            let maxlen = charset_maxlen(col.field_type.charset_name()).map_err(|_| {
                MysqlError::from_terror(
                    &tidb_dbterror::ERR_UNSUPPORTED_CHARSET.generate_with_stack_by_args(&[
                        col.field_type.charset_name().into(),
                        col.field_type.collation_name().into(),
                    ]),
                )
            })?;
            maxlen * length
        }
        Code::Tiny => 1,
        Code::Short => 2,
        Code::Int24 => 3,
        Code::Long => 4,
        Code::LongLong | Code::Double => 8,
        // `mysql.MaxFloatPrecisionLength` is 24: above it a FLOAT is stored
        // with double precision, so it costs a DOUBLE's eight bytes.
        Code::Float => {
            if length <= 24 {
                4
            } else {
                8
            }
        }
        Code::NewDecimal => (length / 9 * 4) + ((length % 9) + 1) / 2,
        Code::Year => 1,
        Code::Date | Code::Duration => 3,
        Code::Datetime => 8,
        Code::Timestamp => 4,
        // Go's `default` arm, which ENUM and SET reach too -- they are absent
        // from its switch even though `DefaultLengthOfMysqlTypes` names them.
        _ => length,
    })
}

#[cfg(test)]
mod index_column_tests {
    use super::*;

    /// Go `config.MaxIndexLength`'s default, in bytes.
    const MAX_INDEX_LENGTH: i64 = 3072;

    fn column(name: &str, field_type: tidb_datatype::FieldType) -> ColumnInfo {
        ColumnInfo {
            name: tidb_ast::CiString::new(name.to_owned()),
            field_type,
            ..ColumnInfo::default()
        }
    }

    fn char_type(code: FieldTypeCode, flen: i64, charset: &str) -> tidb_datatype::FieldType {
        let mut field_type = tidb_datatype::FieldType::new(code);
        field_type.set_flen(flen);
        field_type.set_charset_name(charset);
        field_type
    }

    /// Captured from real TiDB: `create table e1 (a int, key idx(a(3)))` is
    /// refused.
    #[test]
    fn a_length_on_a_non_prefixable_type_is_1089() {
        let int = column("a", tidb_datatype::FieldType::new(FieldTypeCode::Long));
        assert_eq!(check_index_column(&int, 3, false), Err(tidb_terror::MysqlError::from_terror(&tidb_dbterror::ERR_INCORRECT_PREFIX_KEY.generate_with_stack_by_args(&[]))));
        assert!(matches!(check_index_column(&int, UNSPECIFIED_LENGTH, false), Ok(())));
    }

    /// Captured: `create table e2 (a blob, key idx(a))` is refused, and
    /// `create table e5 (a text, key idx(a(5)))` is accepted.
    #[test]
    fn a_blob_key_part_must_carry_a_length() {
        let blob = column("a", tidb_datatype::FieldType::new(FieldTypeCode::Blob));
        assert_eq!(
            check_index_column(&blob, UNSPECIFIED_LENGTH, false),
            Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_BLOB_KEY_WITHOUT_LENGTH
                    .generate_with_stack_by_args(&["a".into()])
            ))
        );
        assert!(matches!(check_index_column(&blob, 5, false), Ok(())));
        // Zero is a different error from absent, and is checked first.
        assert_eq!(
            check_index_column(&blob, 0, false),
            Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_KEY_PART0.generate_with_stack_by_args(&["a".into()])
            ))
        );
    }

    /// Captured: `create table e3 (a varchar(5), key idx(a(10)))` is refused,
    /// and `create table e6 (a varchar(20), key idx(a(0)))` is refused with
    /// the other error.
    #[test]
    fn a_char_key_part_may_not_outrun_its_column() {
        let short = column("a", char_type(FieldTypeCode::Varchar, 5, "utf8mb4"));
        assert_eq!(
            check_index_column(&short, 10, false),
            Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_INCORRECT_PREFIX_KEY.generate_with_stack_by_args(&[])
            ))
        );
        assert_eq!(
            check_index_column(&short, 0, false),
            Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_KEY_PART0.generate_with_stack_by_args(&["a".into()])
            ))
        );
        assert!(matches!(check_index_column(&short, 3, false), Ok(())));
    }

    /// Go `buildIndexColumns` normalizes a full-length CHAR prefix away, so
    /// the index is an ordinary one and `SHOW CREATE TABLE` prints no `(n)`.
    /// A BLOB is NOT normalized: its declared length is kept as written.
    #[test]
    fn a_full_length_char_prefix_is_stored_as_no_prefix() {
        let mut text = tidb_datatype::FieldType::new(FieldTypeCode::Blob);
        text.set_flen(10);
        text.set_charset_name("utf8mb4");
        let columns = [
            column("a", char_type(FieldTypeCode::Varchar, 10, "utf8mb4")),
            column("t", text),
        ];
        let lengths = |part: IndexPartSpecification<'_>| {
            build_index_columns(None, &columns, &[part], ColumnarIndexType::NA)
                .map(|(parts, _)| parts[0].length)
        };
        assert!(matches!(lengths(IndexPartSpecification { column: "a", length: 10 }), Ok(UNSPECIFIED_LENGTH)));
        // One short of the column is a real prefix.
        assert!(matches!(lengths(IndexPartSpecification { column: "a", length: 9 }), Ok(9)));
        assert!(matches!(lengths(IndexPartSpecification { column: "t", length: 10 }), Ok(10)));
    }

    /// Go multiplies the length by the charset's bytes per character before
    /// comparing against `MaxIndexLength`, and reports the MULTIPLIED number.
    /// Outside strict mode the check is suppressed.
    #[test]
    fn the_length_limit_counts_bytes_not_characters() {
        let wide = column("a", char_type(FieldTypeCode::Varchar, 2000, "utf8mb4"));
        assert_eq!(
            check_index_column(&wide, 1000, false),
            Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_TOO_LONG_KEY
                    .generate_with_stack_by_args(&[4000.into(), MAX_INDEX_LENGTH.into()])
            ))
        );
        // The same 1000 characters fit in a single-byte charset.
        let narrow = column("a", char_type(FieldTypeCode::Varchar, 2000, "latin1"));
        assert!(matches!(check_index_column(&narrow, 1000, false), Ok(())));
        // Non-strict suppresses the check, as Go's suppressTooLongKeyErr does.
        assert!(matches!(check_index_column(&wide, 1000, true), Ok(())));
    }

    /// Go `checkIndexPrefixLength` reports the TOTAL, where the running sum
    /// in `buildIndexColumns` reports the sum at the part that crossed.
    #[test]
    fn the_prefix_length_check_reports_the_whole_total() {
        let columns = [
            column("a", char_type(FieldTypeCode::Varchar, 500, "utf8mb4")),
            column("b", char_type(FieldTypeCode::Varchar, 500, "utf8mb4")),
        ];
        let part = |name: &str| IndexColumn {
            name: tidb_ast::CiString::new(name.to_owned()),
            length: UNSPECIFIED_LENGTH,
            ..IndexColumn::default()
        };
        assert_eq!(
            check_index_prefix_length(&columns, &[part("a"), part("b")], ColumnarIndexType::NA),
            Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_TOO_LONG_KEY
                    .generate_with_stack_by_args(&[4000.into(), MAX_INDEX_LENGTH.into()])
            ))
        );
        assert!(matches!(
            check_index_prefix_length(&columns, &[part("a"), part("b")], ColumnarIndexType::VECTOR),
            Ok(())
        ));
    }
}

/// Go `GetName4AnonymousIndex` (`pkg/ddl/executor.go`): the name an `ADD INDEX`
/// or `CREATE INDEX` written without one takes, given the names of the
/// table's existing indexes.
///
/// The base is `column_name` (the first key column, or `expression_index`),
/// or `index_name` when the caller passes a truncated retry. `PRIMARY` is
/// reserved, so a column of that name starts at `primary_2`. A collision
/// (compared lowercased) tries `<column>_2`, `<column>_3`, ... from the first
/// index again; a candidate longer than 64 characters restarts from the
/// column's first 30 bytes, exactly as Go recurses.
pub fn get_name_for_anonymous_index(
    existing: &[&str],
    column_name: &str,
    index_name: &str,
) -> String {
    let mut id = 2;
    let mut name = column_name.to_owned();
    if !index_name.is_empty() {
        name = index_name.to_owned();
        id = 3;
    }
    if name.go_to_lower().go_equal_fold("PRIMARY") {
        name = format!("{column_name}_{id}");
        id = 3;
    }
    let mut position = 0;
    while position < existing.len() {
        if existing[position].go_to_lower() == name.go_to_lower() {
            name = format!("{column_name}_{id}");
            if crate::check_too_long_identifier(&name).is_err() {
                let truncated = go_byte_prefix(column_name, 30);
                name = get_name_for_anonymous_index(existing, truncated, &format!("{truncated}_2"));
            }
            position = 0;
            id += 1;
            continue;
        }
        position += 1;
    }
    name
}

/// Go `s[:n]` on a string: the first `n` bytes, here stopped at a character
/// boundary so the result stays a `str`.
fn go_byte_prefix(text: &str, bytes: usize) -> &str {
    let mut end = bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Go `IndexInfo.Invisible`, read off the statement that declares the index.
///
/// The visibility a statement writes and the `visible` flag the planner reads
/// are the same fact in two places, so it is resolved here, at the single
/// point where an `IndexOptions` becomes a `KvIndex` -- a `CREATE INDEX`, an
/// `ALTER TABLE ... ADD INDEX` and a `CREATE TABLE` key all pass through it.
/// A key with no visibility clause is visible, which is Go's default.
pub fn is_visible(options: &tidb_ast::IndexOptions) -> bool {
    options.visibility != Some(tidb_ast::IndexVisibility::Invisible)
}

/// The in-process table's `checkIndexCondition`.
pub fn validate_partial_index_condition(
    columns: &[KvColumn],
    expression: &tidb_ast::Expr,
    ctx: &tidb_metabuild::Context,
) -> Result<(), MysqlError> {
    let column = |name: &str| {
        columns
            .iter()
            .find(|column| column.name.go_lower_eq(name))
            .map(|column| IndexConditionColumn {
                field_type: column.field_type.clone(),
                generated: column.generated.is_some(),
            })
    };
    check_index_condition(&column, expression, ctx.get_expr_ctx().get_charset_info())
        
}

/// Go planner `checkDuplicateColumnName`: a named column may occur only once
/// in one index. Expression parts are independent hidden columns and do not
/// participate in this name set.
pub fn reject_duplicate_index_columns(
    parts: &[tidb_ast::IndexPart],
) -> Result<(), MysqlError> {
    let mut seen = std::collections::HashSet::with_capacity(parts.len());
    for part in parts {
        if let tidb_ast::IndexPart::Column { name, .. } = part {
            if !seen.insert(name.go_to_lower()) {
                return Err(tidb_terror::MysqlError::from_terror(&tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS.generate_with_stack_by_args(&[(name.clone()).to_string().into()])));
            }
        }
    }
    Ok(())
}


/// One column of the table an index is built over, as Go
/// `checkCreateGlobalIndex` reads it.
pub struct IndexedTableColumn<'a> {
    /// The column's name.
    pub name: &'a str,
    /// The column's type.
    pub field_type: &'a tidb_datatype::FieldType,
    /// Whether it is a VIRTUAL generated column (an expression part's hidden
    /// column is one).
    pub virtual_generated: bool,
}

/// Go `checkCreateGlobalIndex`: GLOBAL needs a partitioned table; a unique
/// index on one must cover every column the partition expression reads
/// unless it is GLOBAL (`checkPartitionKeysConstraint`, 8264); and a global
/// index over a prefix or virtual part warns that auto analyze skips it.
/// `partition_offsets` is `None` for an unpartitioned table.
///
/// # Errors
///
/// 8200 for GLOBAL on an unpartitioned table, 8264 for an uncovered unique
/// key.
#[allow(clippy::too_many_arguments)]
pub fn check_create_global_index(
    index_name: &str,
    unique: bool,
    global: bool,
    partition_offsets: Option<&[usize]>,
    columns: &[IndexedTableColumn<'_>],
    offsets: &[usize],
    prefix_lengths: &[i64],
    expression_parts: &[bool],
    ctx: &tidb_metabuild::Context,
) -> Result<(), MysqlError> {
    if global && partition_offsets.is_none() {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
                .generate_with_stack("Unsupported Global Index on non-partitioned table"),
        ));
    }
    if global {
        warn_global_index_needs_manual_analyze(
            ctx,
            index_name,
            offsets
                .iter()
                .zip(prefix_lengths)
                .zip(expression_parts.iter().chain(std::iter::repeat(&false)))
                .map(|((offset, prefix), expression)| {
                    // Go reads `tblInfo.Columns[col.Offset]`; an expression
                    // part's hidden column is not in the table yet and still
                    // carries `Offset` 0, so Go judges the first column.
                    let column = if *expression {
                        columns.first()
                    } else {
                        columns.get(*offset)
                    };
                    column.is_none_or(|column| column.virtual_generated)
                        || *prefix != tidb_datatype::UNSPECIFIED_LENGTH
                }),
        );
    }
    if let Some(partition_offsets) = partition_offsets.filter(|_| unique && !global) {
        if !partition_offsets
            .iter()
            .all(|offset| crate::table_partition::key_part_covers(offsets, prefix_lengths, *offset))
        {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_GLOBAL_INDEX_NOT_EXPLICITLY_SET
                    .generate_with_stack_by_args(&[index_name.to_owned().into()]),
            ));
        }
    }
    Ok(())
}

/// Go `validateGlobalIndexWithGeneratedColumns`: auto analyze cannot
/// sample a global index with a prefix or virtual generated part, so Go
/// warns once (`ErrWarnGlobalIndexNeedManuallyAnalyze`).
pub fn warn_global_index_needs_manual_analyze(
    ctx: &tidb_metabuild::Context,
    index_name: &str,
    mut part_is_prefix_or_virtual: impl Iterator<Item = bool>,
) {
    if part_is_prefix_or_virtual.any(|flag| flag) {
        ctx.append_warning(tidb_context::WarnErr::Terror(
            tidb_dbterror::ERR_WARN_GLOBAL_INDEX_NEED_MANUALLY_ANALYZE
                .fast_generate_by_args(&[index_name.into()]),
        ));
    }
}

#[cfg(test)]
mod anonymous_index_tests {
    use super::get_name_for_anonymous_index;

    /// Go `GetName4AnonymousIndex`: the first column names the index, a
    /// collision (compared lowercased) restarts the scan with `_2`, `_3`, ...,
    /// and `PRIMARY` is never taken.
    #[test]
    fn an_anonymous_index_is_named_as_go_names_it() {
        assert_eq!(get_name_for_anonymous_index(&[], "a", ""), "a");
        assert_eq!(get_name_for_anonymous_index(&["a_2", "A"], "a", ""), "a_3");
        assert_eq!(
            get_name_for_anonymous_index(&["x"], "primary", ""),
            "primary_2"
        );
        assert_eq!(
            get_name_for_anonymous_index(&["primary_2"], "Primary", ""),
            "Primary_3"
        );
        assert_eq!(
            get_name_for_anonymous_index(&["expression_index"], "expression_index", ""),
            "expression_index_2"
        );
    }

    /// A candidate past 64 characters restarts from the column's first 30
    /// bytes, with `<prefix>_2` as the first try.
    #[test]
    fn a_too_long_candidate_restarts_from_a_thirty_byte_prefix() {
        let column = "c".repeat(64);
        let prefix = "c".repeat(30);
        assert_eq!(
            get_name_for_anonymous_index(&[column.as_str()], &column, ""),
            format!("{prefix}_2")
        );
        let taken = format!("{prefix}_2");
        assert_eq!(
            get_name_for_anonymous_index(&[column.as_str(), taken.as_str()], &column, ""),
            format!("{prefix}_3")
        );
    }
}

use crate::table_info_build::DdlAdmissionError;

/// Go `BuildIndexInfo` for a statement's new index over `info`, after
/// `checkIndexNameAndColumns` built the hidden columns of its expression
/// parts into `info`: the key parts resolved, `checkCreateGlobalIndex`, the
/// comment's length and the partial-index condition.
pub fn build_index_info(
    info: &mut TableInfo,
    index: &IndexInfo,
    expressions: &[Option<tidb_ast::Expr>],
    condition: Option<&tidb_ast::Expr>,
    name: &str,
    context: &dyn crate::session_context::DdlSessionContext,
) -> Result<IndexInfo, DdlAdmissionError> {
    fn admission(error: impl Into<tidb_terror::MysqlError>) -> DdlAdmissionError {
        DdlAdmissionError::from_mysql_error(error.into())
    }
    crate::check_too_long_identifier(name).map_err(admission)?;
    // Go `onCreateIndex`'s `checkTooManyIndexes`, counting the new index.
    crate::table_info_build::check_too_many_indexes(info.indices.len() + 1).map_err(admission)?;
    let mut added = index.clone_like_go();
    added.name = tidb_ast::CiString::new(name.to_owned());
    // Go `checkIndexNameAndColumns` counts `t.Cols()`, the public columns,
    // plus the hidden columns the index adds.
    let public_columns = info
        .columns
        .iter_deref()
        .filter(|column| column.read().state == tidb_model::SchemaState::PUBLIC)
        .count();
    let mut hidden_columns = 0;
    if expressions.iter().any(Option::is_some) {
        let parts = added
            .columns
            .iter_deref()
            .zip(expressions)
            .map(|(column, expression)| match expression {
                Some(expr) => tidb_ast::IndexPart::Expr {
                    expr: expr.clone(),
                    desc: false,
                },
                None => {
                    let column = column.read();
                    tidb_ast::IndexPart::Column {
                        name: column.name.original().to_owned(),
                        prefix_len: (column.length != -1).then_some(column.length),
                        desc: false,
                    }
                }
            })
            .collect::<Vec<_>>();
        // Built against the VISIBLE columns, as in process: an expression can
        // never name an earlier index's hidden column.
        let visible = info
            .columns
            .iter_deref()
            .filter(|column| !column.read().hidden)
            .map(|column| {
                let column = column.read();
                (column.name.original().to_owned(), column.field_type.clone())
            })
            .collect::<Vec<_>>();
        let (names, types): (Vec<_>, Vec<_>) = visible.into_iter().unzip();
        let hidden =
            crate::expression_index::build_hidden_columns_with_like_default_escape(
                name,
                &parts,
                &names,
                &types,
                &context.session_zone(),
                context.like_default_escape(),
                context.connection_charset_info().0,
            )
            .map_err(admission)?;
        let auto_increment = info.columns.iter_deref().find_map(|column| {
            let column = column.read();
            column
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT)
                .then(|| column.name.original().to_owned())
        });
        crate::index::check_expression_index_auto_increment(
            name,
            &hidden,
            auto_increment.as_deref(),
            &context.metabuild_context(),
        )
        .map_err(admission)?;
        for (position, hidden) in hidden {
            let mut column =
                crate::table_info_build::hidden_index_column_info(hidden, info.columns.len());
            info.max_column_id += 1;
            column.id = info.max_column_id;
            let part = added
                .columns
                .get(position)
                .expect("a hidden column belongs to a key part");
            {
                let mut part = part.write();
                part.name = column.name.clone();
                part.length = -1;
            }
            info.columns.push_go(column);
            hidden_columns += 1;
        }
    }
    // Go `checkAddColumnTooManyColumns(len(t.Cols()) + len(hiddenCols))`
    // (`pkg/ddl/executor.go:4910`), before the key parts are built.
    crate::table_info_build::check_too_many_columns(public_columns + hidden_columns)?;
    let (columns, offsets, prefix_lengths) = resolve_index_columns(info, &mut added)?;
    let indexed = columns
        .iter()
        .map(|column| IndexedTableColumn {
            name: column.name.original(),
            field_type: &column.field_type,
            virtual_generated: !column.generated_expr_string.is_empty() && !column.generated_stored,
        })
        .collect::<Vec<_>>();
    let partition_offsets = match info.partition.as_ref() {
        Some(partition) => {
            let stored_columns = info.columns.iter_deref().collect::<Vec<_>>();
            let spec = tidb_table::table_from_meta::partition_spec_for(&stored_columns, &partition.read())
                .map_err(DdlAdmissionError::from_mysql_error)?;
            let mut partition_offsets = Vec::with_capacity(spec.dependencies.len());
            for dependency in &spec.dependencies {
                let Some(offset) = columns
                    .iter()
                    .position(|column| column.name.original().go_lower_eq(dependency))
                else {
                    return Err(DdlAdmissionError::plain(format!(
                            "the partition expression of `{}` reads column `{dependency}`, \
                             which the table does not define",
                            info.name.original()
                        ),
                    ));
                };
                partition_offsets.push(offset);
            }
            Some(partition_offsets)
        }
        None => None,
    };
    let expression_parts = expressions
        .iter()
        .map(Option::is_some)
        .collect::<Vec<_>>();
    crate::index::check_create_global_index(
        name,
        added.unique,
        added.global,
        partition_offsets.as_deref(),
        &indexed,
        &offsets,
        &prefix_lengths,
        &expression_parts,
        &context.metabuild_context(),
    )
    .map_err(admission)?;
    added.comment = crate::validate_comment_length(
        &added.comment,
        name,
        crate::CommentOwner::Index,
        &context.metabuild_context(),
    )
    .map_err(admission)?;
    crate::table_info_build::build_index_condition(info, &mut added, condition, &context.metabuild_context())?;
    Ok(added)
}

/// Go `BuildIndexInfo`'s `buildIndexColumns` over the table's columns, the new
/// hidden ones included: the index's key parts resolved to their columns and
/// normalized lengths. Returns the columns, each part's offset and length.
pub fn resolve_index_columns(
    info: &TableInfo,
    added: &mut IndexInfo,
) -> Result<(Vec<tidb_model::ColumnInfo>, Vec<usize>, Vec<i64>), DdlAdmissionError> {
    // Go's `IndexColumn.Offset` is a position in `TableInfo.Columns`, and the
    // loader reads it back that way, so it is resolved against the table
    // rather than trusted from the statement.
    let columns = info
        .columns
        .iter_deref()
        .map(|column| column.read().clone_like_go())
        .collect::<Vec<_>>();
    let mut offsets = Vec::with_capacity(added.columns.len());
    let mut specs: Vec<(String, i64)> = Vec::with_capacity(added.columns.len());
    for part in added.columns.iter_deref() {
        let part = part.read();
        let Some(offset) = columns
            .iter()
            .position(|column| column.name.lowercase() == part.name.lowercase())
        else {
            // Go `buildIndexColumns`' `GenWithStack("column does not exist: %s")`.
            return Err(DdlAdmissionError::from_terror(
                &tidb_dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS.generate_with_stack(format!(
                    "column does not exist: {}",
                    part.name.original()
                )),
            ));
        };
        offsets.push(offset);
        specs.push((part.name.original().to_owned(), part.length));
    }
    let specs = specs
        .iter()
        .map(|(column, length)| crate::index::IndexPartSpecification {
            column,
            length: *length,
        })
        .collect::<Vec<_>>();
    // Go `idxInfo.Columns, idxInfo.MVIndex, err = buildIndexColumns(...)`
    // over the table's columns, the new hidden ones included, run as the job
    // worker runs it (Go's nil context).
    let (index_columns, mv_index) = crate::index::build_index_columns(
        None,
        &columns,
        &specs,
        tidb_model::index::ColumnarIndexType::NA,
    )
    .map_err(|error| DdlAdmissionError::from_mysql_error(error.into()))?;
    added.mv_index = mv_index;
    for (part, built) in added.columns.iter_deref().zip(&index_columns) {
        let mut part = part.write();
        part.name = built.name.clone();
        part.offset = built.offset;
        // The WRITE path cuts entries by this length, so it must be the
        // normalized one `buildIndexColumns` settled.
        part.length = built.length;
    }
    let prefix_lengths = index_columns
        .iter()
        .map(|part| part.length)
        .collect::<Vec<_>>();
    Ok((columns, offsets, prefix_lengths))
}

/// The index a `CREATE INDEX` names before `BuildIndexInfo` resolves it
/// against the table: Go's `indexPartSpecifications` and `indexOption` as an
/// `IndexInfo` template (no id, parts unresolved), with each part's
/// expression for an expression part.
pub fn index_template(
    create: &tidb_ast::CreateIndexStmt,
    primary: bool,
) -> Result<(IndexInfo, Vec<Option<tidb_ast::Expr>>), DdlAdmissionError> {
    let unique = match create.kind {
        tidb_ast::IndexKind::Ordinary => false,
        tidb_ast::IndexKind::Unique => true,
        other => {
            return Err(DdlAdmissionError::unsupported(format!(
                "CREATE {} INDEX is not supported by this node",
                other.sql()
            )))
        }
    };
    let mut columns = Vec::with_capacity(create.parts.len());
    let mut expressions = Vec::with_capacity(create.parts.len());
    for part in &create.parts {
        match part {
            tidb_ast::IndexPart::Column {
                name, prefix_len, ..
            } => {
                columns.push(IndexColumn {
                    name: tidb_ast::CiString::new(name.clone()),
                    // Resolved against the stored table when the change is
                    // planned; the declared prefix travels in `length` and is
                    // validated (and normalized) there, where the column's
                    // type is known.
                    offset: 0,
                    length: prefix_len.unwrap_or(-1),
                    ..IndexColumn::default()
                });
                expressions.push(None);
            }
            // An expression part indexes a hidden generated column the plan
            // builds once the index has its name (`_V$_<index>_<part>`).
            tidb_ast::IndexPart::Expr { expr, .. } => {
                columns.push(IndexColumn {
                    offset: 0,
                    length: -1,
                    ..IndexColumn::default()
                });
                expressions.push(Some(expr.clone()));
            }
        }
    }
    if columns.is_empty() {
        return Err(DdlAdmissionError::plain(
            "CREATE INDEX names no column",
        ));
    }
    let index = IndexInfo {
        // The publishing transaction allocates it from the table's own
        // space, which is `TableInfo.MaxIndexID` and not the global one.
        id: 0,
        name: tidb_ast::CiString::new(create.name.clone()),
        columns: columns.into(),
        state: tidb_model::SchemaState::PUBLIC,
        comment: create.options.comment.clone().unwrap_or_default(),
        tp: create
            .options
            .index_type
            .unwrap_or(tidb_ast::IndexType::BTREE),
        unique,
        primary,
        invisible: create.options.visibility == Some(tidb_ast::IndexVisibility::Invisible),
        global: create.options.global,
        ..IndexInfo::default()
    };
    Ok((index, expressions))
}
