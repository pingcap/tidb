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

//! `SHOW CHARACTER SET` / `SHOW CHARSET` typed payload.

use crate::Expr;

/// TiDB's character-set catalog inspection statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCharsetStmt {
    /// Optional filter over the character-set catalog rows.
    pub filter: Option<ShowCharsetFilter>,
}

/// The Go shared SHOW filter alternatives for `CHARSET`.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowCharsetFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowCharsetStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW CHARSET");
        match &self.filter {
            None => {}
            Some(ShowCharsetFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowCharsetFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}
