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

//! Go `types/parser_driver.ValueExpr` built around a datum: what
//! [`tidb_ast::Expr::Value`] carries, read back by the rewriter.

use tidb_datatype::{Datum, FieldType};

/// Go `driver.ValueExpr`'s `Datum` and `Type`.
#[derive(Debug, Clone, PartialEq)]
pub struct ValueExprDatum {
    /// Go `ValueExpr.Datum`.
    pub datum: Datum,
    /// Go `ValueExpr.Type`.
    pub field_type: FieldType,
}

impl ValueExprDatum {
    /// Go `&driver.ValueExpr{Datum: d}` with `v.Type = ft`, as the AST node
    /// [`tidb_ast::Expr::Value`].
    #[must_use]
    pub fn into_expr(self) -> tidb_ast::Expr {
        let restored = self.restore();
        tidb_ast::Expr::Value(tidb_ast::ValueExpr {
            value: tidb_ast::ParamValue::new(self),
            restored,
        })
    }

    /// The datum an [`tidb_ast::Expr::Value`] holds.
    #[must_use]
    pub fn of(value: &tidb_ast::ValueExpr) -> Option<&Self> {
        value.value.downcast_ref::<Self>()
    }
}

impl ValueExprDatum {
    /// Go `ValueExpr.Restore` under `format.DefaultRestoreFlags`.
    fn restore(&self) -> String {
        match &self.datum {
            Datum::Null => "NULL".to_owned(),
            Datum::Int(value) => value.to_string(),
            Datum::UInt(value) => value.to_string(),
            Datum::Real(value) => tidb_ast::Expr::Float(*value).restore(),
            // `KindString`: the type's charset introducer, then the quoted
            // value with `\` doubled.
            Datum::String(value) => {
                let charset = self.field_type.charset_name();
                let introducer = if charset.is_empty() {
                    String::new()
                } else {
                    format!("_{}", charset.to_uppercase())
                };
                format!("{introducer}{}", quote(value.bytes()))
            }
            Datum::Bytes(value) => quote(value),
            Datum::Decimal(_) => self.datum.sql_string().unwrap_or_default(),
            other => match other.sql_string() {
                Ok(text) => quote(text.as_bytes()),
                Err(_) => format!("{other:?}"),
            },
        }
    }
}

fn quote(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}
