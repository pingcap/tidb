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

//! The implicit casts Go's JSON builtin classes put around their arguments.
//!
//! Each `json*FunctionClass.getFunction` in `pkg/expression/builtin_json.go`
//! passes `newBaseBuiltinFuncWithTp` a per-argument eval-type list, and that
//! constructor wraps every argument that is not already of its type with
//! `WrapWithCastAsJSON` / `WrapWithCastAsString`. The wrapper is visible in a
//! plan: `1 member of (j)` explains as
//! `json_memberof(cast(1, json BINARY), t.j)`, and a JSON function used where
//! a string is expected as `cast(json_set(...), var_string(16777216))`.
//!
//! The classes then clear `ParseToJSONFlag` on the cast of every VALUE
//! argument (`DisableParseJSONFlag4Expr`), so a SQL string stays a JSON string
//! instead of being parsed as a document.

use tidb_datatype::EvalType;

use crate::aggregation::wrap_cast::{wrap_with_cast_as_json, wrap_with_cast_as_string};
use crate::expr_util::predicates::disable_parse_json_flag_4_expr;
use crate::expression::Expression;
use crate::EvalError;

use super::is_newly_built_cast;

/// One entry of a Go class's `argTps`.
#[derive(Clone, Copy)]
enum ArgKind {
    /// `ETJson`; a string source is parsed as a JSON document.
    Document,
    /// `ETJson` followed by `DisableParseJSONFlag4Expr`.
    Value,
    /// `ETString`.
    Text,
}

/// The `argTps` list of Go's class for `name` called with `count` arguments;
/// `None` for a function that does not wrap its arguments this way.
fn argument_kinds(name: &str, count: usize) -> Option<Vec<ArgKind>> {
    use ArgKind::{Document, Text, Value};
    let document_then_text = || {
        let mut kinds = vec![Document];
        kinds.extend(std::iter::repeat_n(Text, count.saturating_sub(1)));
        kinds
    };
    // `json_doc, (path, value)...`: the loop `for i := 1; i < len(args)-1; i += 2`.
    let document_then_pairs = || {
        let mut kinds = vec![Document];
        let mut index = 1;
        while index + 1 < count {
            kinds.push(Text);
            kinds.push(Value);
            index += 2;
        }
        kinds
    };
    Some(match name {
        "json_type" | "json_depth" | "json_storage_free" | "json_storage_size" | "json_pretty" => {
            vec![Document]
        }
        "json_extract" | "json_remove" | "json_keys" | "json_length" | "json_contains_path"
        | "json_search" => document_then_text(),
        "json_set" | "json_insert" | "json_replace" | "json_array_append" | "json_array_insert" => {
            document_then_pairs()
        }
        "json_merge" | "json_merge_preserve" | "json_merge_patch" => vec![Document; count],
        "json_array" => vec![Value; count],
        "json_object" => (0..count)
            .map(|index| if index % 2 == 0 { Text } else { Value })
            .collect(),
        "json_memberof" => vec![Value, Document],
        "json_contains" => {
            let mut kinds = vec![Document, Document];
            kinds.extend(std::iter::repeat_n(Text, count.saturating_sub(2)));
            kinds
        }
        "json_overlaps" | "json_schema_valid" => vec![Document, Document],
        "json_unquote" | "json_quote" => vec![Text],
        _ => return None,
    })
}

/// Wraps `args` the way Go's `newBaseBuiltinFuncWithTp` does for the JSON
/// function `name`. `fold` is `BuildCastFunction`'s closing `FoldConstant`,
/// which Go skips for a cast whose result is JSON.
pub(super) fn wrap_json_function_arguments(
    name: &str,
    args: Vec<Expression>,
    connection: (&str, &str),
    fold: &dyn Fn(&mut Expression),
) -> Result<Vec<Expression>, EvalError> {
    let kinds = if name == "isnull" {
        // Go `isNullFunctionClass.getFunction`: a JSON argument is tested as
        // its string form (`argTp = types.ETString`).
        match args.as_slice() {
            [arg]
                if arg
                    .static_type()
                    .is_some_and(|tp| tp.eval_type() == EvalType::Json) =>
            {
                Some(vec![ArgKind::Text])
            }
            _ => None,
        }
    } else {
        argument_kinds(name, args.len())
    };
    let Some(kinds) = kinds else {
        return Ok(args);
    };
    if kinds.len() != args.len() {
        // A malformed call (wrong argument count) is rejected by the class's
        // own check; leave its arguments as they are.
        return Ok(args);
    }
    args.into_iter()
        .zip(kinds)
        .map(|(arg, kind)| {
            Ok(match kind {
                ArgKind::Document => wrap_with_cast_as_json(arg)?,
                ArgKind::Value => {
                    let mut wrapped = wrap_with_cast_as_json(arg)?;
                    disable_parse_json_flag_4_expr(&mut wrapped);
                    wrapped
                }
                ArgKind::Text => {
                    let was_cast = is_newly_built_cast(&arg);
                    let mut wrapped = wrap_with_cast_as_string(arg, connection)?;
                    if is_newly_built_cast(&wrapped) && !was_cast {
                        fold(&mut wrapped);
                    }
                    wrapped
                }
            })
        })
        .collect()
}
