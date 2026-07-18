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

//! Conversion from an executed [`Datum`] back to a literal AST expression.
//!
//! Go normally keeps evaluated values in `types.Datum` and constructs
//! `expression.Constant` nodes directly (`pkg/types/datum.go` and
//! `pkg/expression/constant.go`). This seed executor still evaluates parsed
//! AST expressions, so subquery and `VALUES(col)` rewrites need the equivalent
//! lossless literal boundary. Keeping it here makes that Rust-specific bridge
//! one physical owner rather than crate-root behavior.

use tidb_ast::{Expr, UnaryOp};
use tidb_datatype::Datum;

/// Turns an executed value back into a literal expression, so a resolved
/// value can be spliced into an expression and evaluated normally.
pub(crate) fn value_to_literal(value: Datum) -> Expr {
    match value {
        // Negative integers round-trip through `Expr::Int`'s i64 parse.
        Datum::Int(value) => Expr::Int(value.to_string()),
        // `Expr::Int` parses nonnegative text through u64 before choosing the
        // signed or unsigned Datum domain, so this does not narrow the value.
        Datum::UInt(value) => Expr::Int(value.to_string()),
        Datum::String(value) => match String::from_utf8(value.into_bytes()) {
            Ok(text) => Expr::String(text),
            Err(error) => Expr::Hex(hex_bytes(error.as_bytes())),
        },
        Datum::Bytes(bytes) => Expr::Hex(hex_bytes(&bytes)),
        // Decimal magnitude is sign-free in the AST; negation is a separate
        // node, matching parser construction.
        Datum::Decimal(value) => {
            let rendered = value.to_string();
            match rendered.strip_prefix('-') {
                Some(magnitude) => Expr::Unary(
                    UnaryOp::Minus,
                    Box::new(Expr::Decimal(magnitude.to_string())),
                ),
                None => Expr::Decimal(rendered),
            }
        }
        Datum::Real(value) => Expr::Float(value),
        Datum::Null => Expr::Null,
        Datum::MinNotNull | Datum::MaxValue => {
            unreachable!("range sentinels never enter the SQL row-value domain")
        }
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}
