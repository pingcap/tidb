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

//! Bounded result-field derivation for table-less SELECT lists.
//!
//! Go's `pkg/executor/adapter.go` receives both the planner's output names and
//! the executor schema. This leaf owns only the part that is possible before
//! a catalog/schema resolver exists: expression display names, aliases, and
//! type metadata for literals and a small set of type-preserving operators.
//! A column reference or unknown function without an explicit type hint is an
//! error, never a guess from a runtime value.

use std::fmt;

use tidb_ast::{BinaryOp, CastType, Expr, SelectField, UnaryOp};
use tidb_datatype::{Charset, Collation, FieldTypeCode};
use tidb_planner::aggregation_descriptor::AggregateKind;

use crate::result_metadata::{
    FieldNameMetadata, IdentifierMetadata, ResultFieldTypeMetadata, UNSIGNED_FLAG,
};

/// Input for one projected result field.
#[derive(Clone, Debug, PartialEq)]
pub struct ResultFieldSpec {
    /// The source expression.
    pub expression: Expr,
    /// Optional `SELECT ... AS alias` spelling.
    pub alias: Option<String>,
    /// Schema-provided type metadata, when the expression is row-dependent.
    pub type_hint: Option<ResultFieldTypeMetadata>,
}

impl ResultFieldSpec {
    /// Creates a field specification without an alias or schema type hint.
    pub fn new(expression: Expr) -> Self {
        Self {
            expression,
            alias: None,
            type_hint: None,
        }
    }

    /// Adds an output alias. An empty alias is treated like Go's empty CIStr.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Adds an authoritative schema/executor type hint.
    pub fn with_type_hint(mut self, type_hint: ResultFieldTypeMetadata) -> Self {
        self.type_hint = Some(type_hint);
        self
    }
}

/// Output from table-less result-field derivation.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedResultField {
    /// Source-shaped names consumed by `col_names_to_result_fields`.
    pub names: FieldNameMetadata,
    /// Source-shaped return type metadata.
    pub field_type: ResultFieldTypeMetadata,
}

/// Explicit failures at the schema boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultFieldResolveError {
    /// A wildcard needs relation/schema columns and cannot be expanded here.
    WildcardRequiresSchema,
    /// A row-dependent expression was supplied without a schema type hint.
    MissingType {
        /// Canonical expression display text.
        expression: String,
    },
    /// The expression shape is known but no source-backed return type exists.
    UnsupportedType {
        /// Canonical expression display text.
        expression: String,
        /// Why no source-backed type is available.
        reason: &'static str,
    },
}

impl fmt::Display for ResultFieldResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WildcardRequiresSchema => f.write_str("result-field wildcard requires schema"),
            Self::MissingType { expression } => {
                write!(f, "missing result-field type for {expression}")
            }
            Self::UnsupportedType { expression, reason } => write!(
                f,
                "unsupported result-field type for {expression}: {reason}"
            ),
        }
    }
}

impl std::error::Error for ResultFieldResolveError {}

/// Resolves a table-less select list into source-shaped result fields.
pub fn resolve_select_fields(
    fields: &[SelectField],
    default_collation: Collation,
) -> Result<Vec<ResolvedResultField>, ResultFieldResolveError> {
    let specs = fields
        .iter()
        .map(|field| match field {
            SelectField::Wildcard(_) => Err(ResultFieldResolveError::WildcardRequiresSchema),
            SelectField::Expr { expr, alias } => Ok(ResultFieldSpec {
                expression: expr.clone(),
                alias: alias.clone(),
                type_hint: None,
            }),
        })
        .collect::<Result<Vec<_>, _>>()?;
    resolve_result_fields(&specs, default_collation)
}

/// Resolves explicitly supplied field specifications.
pub fn resolve_result_fields(
    specs: &[ResultFieldSpec],
    default_collation: Collation,
) -> Result<Vec<ResolvedResultField>, ResultFieldResolveError> {
    specs
        .iter()
        .map(|spec| {
            let names = names_for_expression(&spec.expression, spec.alias.as_deref());
            let field_type = spec
                .type_hint
                .clone()
                .map(Ok)
                .unwrap_or_else(|| infer_type(&spec.expression, default_collation))?;
            Ok(ResolvedResultField { names, field_type })
        })
        .collect()
}

fn names_for_expression(expr: &Expr, alias: Option<&str>) -> FieldNameMetadata {
    let alias = alias.filter(|value| !value.is_empty());
    let (original_table, database, table, original_column) = match expr {
        Expr::Column(path) if !path.is_empty() => {
            let original_column = IdentifierMetadata::new(path.last().unwrap().clone());
            let table = path
                .get(path.len().saturating_sub(2))
                .cloned()
                .unwrap_or_default();
            let database = if path.len() > 2 {
                path.first().cloned().unwrap_or_default()
            } else {
                String::new()
            };
            (
                IdentifierMetadata::new(table.clone()),
                IdentifierMetadata::new(database),
                IdentifierMetadata::new(table),
                original_column,
            )
        }
        _ => (
            IdentifierMetadata::new(""),
            IdentifierMetadata::new(""),
            IdentifierMetadata::new(""),
            IdentifierMetadata::new(""),
        ),
    };
    let column = alias.map_or_else(
        || IdentifierMetadata::new(display_name(expr)),
        IdentifierMetadata::new,
    );
    FieldNameMetadata {
        original_table,
        original_column,
        database,
        table,
        column,
    }
}

fn display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(path) => path.last().cloned().unwrap_or_default(),
        Expr::Int(value) | Expr::Decimal(value) => value.clone(),
        Expr::Float(value) => value.to_string(),
        Expr::String(value) | Expr::RawString(value) => format!("'{value}'"),
        Expr::Null => "NULL".to_owned(),
        Expr::Bool(value) => value.to_string().to_ascii_uppercase(),
        Expr::Hex(value) => format!("x'{value}'"),
        Expr::Bit(value) => format!("b'{value}'"),
        // The AST intentionally keeps expression restore private to its
        // statement owner. Preserve an explicit, deterministic fallback
        // rather than claiming source-text fidelity for unsupported shapes.
        other => format!("{other:?}"),
    }
}

fn infer_type(
    expr: &Expr,
    default_collation: Collation,
) -> Result<ResultFieldTypeMetadata, ResultFieldResolveError> {
    let expression = display_name(expr);
    let type_metadata = |code, flags, flen, decimal, collation| ResultFieldTypeMetadata {
        code,
        flags,
        flen,
        decimal,
        collation,
    };
    let unresolved = |reason| {
        Err(ResultFieldResolveError::UnsupportedType {
            expression: expression.clone(),
            reason,
        })
    };
    let result = match expr {
        Expr::Null => Ok(type_metadata(
            FieldTypeCode::Null,
            0,
            Some(0),
            Some(0),
            Collation::Binary,
        )),
        Expr::Bool(_) => Ok(type_metadata(
            FieldTypeCode::LongLong,
            0,
            Some(1),
            Some(0),
            Collation::Binary,
        )),
        Expr::Int(value) => Ok(type_metadata(
            FieldTypeCode::LongLong,
            0,
            Some(value.len() as u32),
            Some(0),
            Collation::Binary,
        )),
        Expr::Decimal(value) => {
            let scale = value
                .split_once('.')
                .map_or(0, |(_, frac)| frac.len() as u8);
            Ok(type_metadata(
                FieldTypeCode::NewDecimal,
                0,
                Some(value.len() as u32 + 1),
                Some(scale),
                Collation::Binary,
            ))
        }
        Expr::Float(value) => {
            let flen = value.to_string().len() as u32;
            Ok(type_metadata(
                FieldTypeCode::Double,
                0,
                Some(flen),
                None,
                Collation::Binary,
            ))
        }
        Expr::String(value) | Expr::RawString(value) => Ok(type_metadata(
            FieldTypeCode::VarString,
            0,
            Some(value.len() as u32),
            None,
            default_collation,
        )),
        Expr::CharsetString { charset, value } => {
            let collation =
                Charset::from_name(charset).map_or(default_collation, Charset::default_collation);
            Ok(type_metadata(
                FieldTypeCode::VarString,
                0,
                Some(value.len() as u32),
                None,
                collation,
            ))
        }
        Expr::Hex(value) => Ok(type_metadata(
            FieldTypeCode::VarString,
            UNSIGNED_FLAG,
            Some((value.len() * 3) as u32),
            Some(0),
            Collation::Binary,
        )),
        Expr::Bit(value) => Ok(type_metadata(
            FieldTypeCode::VarString,
            0,
            Some((value.len() * 3) as u32),
            Some(0),
            Collation::Binary,
        )),
        Expr::Paren(inner) => infer_type(inner, default_collation),
        Expr::Unary(op, inner) => match op {
            UnaryOp::Not | UnaryOp::NotKeyword => Ok(type_metadata(
                FieldTypeCode::LongLong,
                0,
                Some(1),
                Some(0),
                Collation::Binary,
            )),
            UnaryOp::Plus | UnaryOp::Minus | UnaryOp::BitNeg => {
                infer_type(inner, default_collation)
            }
        },
        Expr::Binary(op, left, right) => {
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NullEq
                    | BinaryOp::Ge
                    | BinaryOp::Gt
                    | BinaryOp::Le
                    | BinaryOp::Lt
                    | BinaryOp::Ne
                    | BinaryOp::LogicAnd
                    | BinaryOp::LogicOr
                    | BinaryOp::LogicXor
            ) {
                Ok(type_metadata(
                    FieldTypeCode::LongLong,
                    0,
                    Some(1),
                    Some(0),
                    Collation::Binary,
                ))
            } else {
                let left = infer_type(left, default_collation)?;
                let right = infer_type(right, default_collation)?;
                let code =
                    if left.code == FieldTypeCode::Double || right.code == FieldTypeCode::Double {
                        FieldTypeCode::Double
                    } else if left.code == FieldTypeCode::NewDecimal
                        || right.code == FieldTypeCode::NewDecimal
                    {
                        FieldTypeCode::NewDecimal
                    } else {
                        FieldTypeCode::LongLong
                    };
                Ok(type_metadata(code, 0, None, None, Collation::Binary))
            }
        }
        Expr::Cast(cast) => match &cast.cast_type {
            CastType::Signed => Ok(type_metadata(
                FieldTypeCode::LongLong,
                0,
                None,
                Some(0),
                Collation::Binary,
            )),
            CastType::Unsigned => Ok(type_metadata(
                FieldTypeCode::LongLong,
                UNSIGNED_FLAG,
                None,
                Some(0),
                Collation::Binary,
            )),
            CastType::Char { len, charset } => {
                let collation = charset
                    .as_deref()
                    .and_then(Charset::from_name)
                    .map_or(default_collation, Charset::default_collation);
                Ok(type_metadata(
                    FieldTypeCode::VarString,
                    0,
                    *len,
                    None,
                    collation,
                ))
            }
            CastType::Binary { len } => Ok(type_metadata(
                FieldTypeCode::VarString,
                0,
                *len,
                None,
                Collation::Binary,
            )),
            CastType::Decimal { flen, scale } => Ok(type_metadata(
                FieldTypeCode::NewDecimal,
                0,
                (*flen != 0).then_some(*flen),
                Some(*scale as u8),
                Collation::Binary,
            )),
            CastType::Date => Ok(type_metadata(
                FieldTypeCode::Date,
                0,
                Some(10),
                None,
                Collation::Binary,
            )),
            CastType::DateTime { fsp } => Ok(type_metadata(
                FieldTypeCode::Datetime,
                0,
                Some(19),
                fsp.map(|v| v as u8),
                Collation::Binary,
            )),
            CastType::Time { fsp } => Ok(type_metadata(
                FieldTypeCode::Duration,
                0,
                Some(10),
                fsp.map(|v| v as u8),
                Collation::Binary,
            )),
            CastType::Year => Ok(type_metadata(
                FieldTypeCode::Year,
                0,
                Some(4),
                Some(0),
                Collation::Binary,
            )),
            CastType::Double | CastType::Float => Ok(type_metadata(
                FieldTypeCode::Double,
                0,
                None,
                None,
                Collation::Binary,
            )),
            CastType::Json => unresolved("JSON result metadata requires a JSON value domain"),
        },
        Expr::Func { name, args } => {
            infer_function(name, args, default_collation).ok_or_else(|| {
                ResultFieldResolveError::MissingType {
                    expression: expression.clone(),
                }
            })?
        }
        Expr::Aggregate { name, args, .. } => infer_aggregate(name, args, default_collation),
        Expr::Window { name, args, .. } => {
            // An aggregate keeps its aggregate result metadata when used as
            // a window function. In particular, the variance/stddev family
            // remains DOUBLE(23); scalar inference has no authority for
            // those names and used to reject an otherwise executable query.
            match AggregateKind::from_name(name) {
                Some(_) => infer_aggregate(name, args, default_collation),
                None => infer_function(name, args, default_collation).ok_or_else(|| {
                    ResultFieldResolveError::MissingType {
                        expression: expression.clone(),
                    }
                })?,
            }
        }
        Expr::Column(_) => Err(ResultFieldResolveError::MissingType {
            expression: expression.clone(),
        }),
        _ => unresolved("expression has no dependency-closed result type"),
    };
    result
}

fn infer_function(
    name: &str,
    args: &[Expr],
    default_collation: Collation,
) -> Option<Result<ResultFieldTypeMetadata, ResultFieldResolveError>> {
    let name = name.to_ascii_uppercase();
    if matches!(
        name.as_str(),
        "CONCAT"
            | "CONCAT_WS"
            | "LOWER"
            | "UPPER"
            | "LEFT"
            | "RIGHT"
            | "SUBSTRING"
            | "TRIM"
            | "REPLACE"
            | "LPAD"
            | "RPAD"
            | "HEX"
            | "UNHEX"
    ) {
        return Some(Ok(ResultFieldTypeMetadata {
            code: FieldTypeCode::VarString,
            flags: 0,
            flen: None,
            decimal: None,
            collation: default_collation,
        }));
    }
    if matches!(
        name.as_str(),
        "LENGTH"
            | "CHAR_LENGTH"
            | "OCTET_LENGTH"
            | "ASCII"
            | "BIT_LENGTH"
            | "BIT_COUNT"
            | "FIELD"
            | "FIND_IN_SET"
            | "SIGN"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "DAYOFMONTH"
            | "COUNT"
    ) {
        return Some(Ok(ResultFieldTypeMetadata {
            code: FieldTypeCode::LongLong,
            flags: 0,
            flen: None,
            decimal: Some(0),
            collation: Collation::Binary,
        }));
    }
    if matches!(
        name.as_str(),
        "ABS"
            | "CEIL"
            | "CEILING"
            | "FLOOR"
            | "ROUND"
            | "TRUNCATE"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "COALESCE"
            | "IF"
            | "IFNULL"
            | "NULLIF"
    ) {
        return args.first().map(|arg| infer_type(arg, default_collation));
    }
    if matches!(
        name.as_str(),
        "PI" | "SQRT"
            | "POW"
            | "POWER"
            | "EXP"
            | "LN"
            | "LOG"
            | "LOG2"
            | "LOG10"
            | "SIN"
            | "COS"
            | "TAN"
    ) {
        return Some(Ok(ResultFieldTypeMetadata {
            code: FieldTypeCode::Double,
            flags: 0,
            flen: None,
            decimal: None,
            collation: Collation::Binary,
        }));
    }
    None
}

fn infer_aggregate(
    name: &str,
    args: &[Expr],
    default_collation: Collation,
) -> Result<ResultFieldTypeMetadata, ResultFieldResolveError> {
    let kind = AggregateKind::from_name(name);
    if kind == Some(AggregateKind::Count) {
        return Ok(ResultFieldTypeMetadata {
            code: FieldTypeCode::LongLong,
            // Go `typeInfer4Count` declares COUNT as binary and non-null,
            // with MaxIntWidth (20) plus the sign slot advertised on the
            // protocol (`pkg/expression/aggregation/base_func.go:170-177`).
            flags: tidb_protocol::BINARY_FLAG | crate::result_metadata::NOT_NULL_FLAG,
            flen: Some(21),
            decimal: Some(0),
            collation: Collation::Binary,
        });
    }
    if matches!(
        kind,
        Some(
            AggregateKind::VarPop
                | AggregateKind::VarSamp
                | AggregateKind::StddevPop
                | AggregateKind::StddevSamp
        )
    ) {
        // Go `typeInfer4PopOrSamp`: every variance/stddev spelling returns
        // DOUBLE(23) with unspecified scale, independent of its input type.
        return Ok(ResultFieldTypeMetadata {
            code: FieldTypeCode::Double,
            flags: 0,
            flen: Some(23),
            decimal: None,
            collation: Collation::Binary,
        });
    }
    args.first().map_or_else(
        || {
            Err(ResultFieldResolveError::MissingType {
                expression: name.to_owned(),
            })
        },
        |arg| infer_type(arg, default_collation),
    )
}
