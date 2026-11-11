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

//! Go `pkg/expression/fts_helper.go`.

use crate::column::Column;
use crate::expression::Expression;
use tidb_datatype::Datum;

/// Go `ast.FTSMatchWord`.
const FTS_MATCH_WORD: &str = "fts_match_word";

/// Go `FTSInfo`: an easy to use struct for interpreting a FullTextSearch
/// expression.
#[derive(Clone, Debug)]
pub struct FtsInfo {
    /// Go `Query`.
    pub query: String,
    /// Go `Column`.
    pub column: Column,
}

/// Go `ContainsFullTextSearchFn`: whether the expression tree contains a
/// possible FullTextSearch function.
#[must_use]
pub fn contains_full_text_search_fn(expr: &Expression) -> bool {
    match expr {
        Expression::ScalarFunction(x) => {
            x.func_name.lowercase() == FTS_MATCH_WORD || x.args.iter().any(contains_full_text_search_fn)
        }
        _ => false,
    }
}

/// Go `InterpretFullTextSearchExpr`: the FullText search `expr` is, or `None`
/// when it is not one.
#[must_use]
pub fn interpret_full_text_search_expr(expr: &Expression) -> Option<FtsInfo> {
    let Expression::ScalarFunction(x) = expr else {
        return None;
    };
    if x.func_name.lowercase() != FTS_MATCH_WORD {
        return None;
    }
    let [Expression::Constant(query), Expression::Column(column)] = x.args.as_slice() else {
        return None;
    };
    // Go `Datum.GetString`: the datum's bytes, whatever its kind.
    let query = match &query.value {
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        _ => String::new(),
    };
    Some(FtsInfo {
        query,
        column: column.clone(),
    })
}
