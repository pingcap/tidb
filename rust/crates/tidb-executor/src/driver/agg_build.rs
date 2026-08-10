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

//! Aggregate function descriptors: the Go `aggregation.NewAggFuncDesc` /
//! `baseFuncDesc.TypeInfer` step, and the AST rewrite that lifts aggregate
//! calls out of a select field so what remains reads the aggregation's output.
//!
//! Type inference and the rewrite are one concern because they run as one
//! step: [`substitute_aggregates`] walks a field, hands each aggregate call to
//! [`build_agg_func`] (which is [`agg_kind_and_type`] plus the argument
//! rewrite), and replaces the call with a reference to the output column the
//! descriptor produced. [`AggOutputResolver`] is what binds those references
//! when the residual expression is later rewritten.
//!
//! Window functions ride the same path: a window call is hoisted the same way
//! an aggregate is, so [`hoisted_window_index`] and [`expr_has_hoisted_window`]
//! read the markers [`substitute_aggregates`] leaves behind.

use super::*;
/// Go `aggregation.NewAggFuncDesc` + `baseFuncDesc.TypeInfer`: the aggregate
/// kind and the result type inferred for its argument.
pub(crate) fn agg_kind_and_type(
    name: &str,
    args: &[Expression],
) -> Result<(AggKind, FieldType), DriverError> {
    // Every aggregate here reads its FIRST argument for type inference;
    // `APPROX_PERCENTILE` is the only one that also reads a second.
    let null = Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::Null,
        FieldType::new(FieldTypeCode::LongLong),
    ));
    let arg = args.first().unwrap_or(&null);
    Ok(match name {
        // Go `typeInfer4Count`: a binary `BIGINT(21)` that never returns NULL
        // -- an empty group (and an empty window frame) counts 0.
        "COUNT" => {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.set_flen(21);
            t.set_decimal(0);
            t.add_flags(
                tidb_datatype::FieldTypeFlags::BINARY | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
            (AggKind::Count, t)
        }
        // Go `typeInfer4Sum`: DOUBLE for a real argument, DECIMAL for every
        // other numeric one -- `SUM` over a BIGINT column is a DECIMAL in
        // MySQL, not a BIGINT (captured: `sum(a)` reports type 246).
        "SUM" => {
            let real = arg
                .static_type()
                .is_some_and(|t| t.eval_type() == tidb_datatype::EvalType::Real);
            let t = if real {
                FieldType::new(FieldTypeCode::Double)
            } else {
                FieldType::new(FieldTypeCode::NewDecimal)
            };
            (AggKind::Sum, t)
        }
        // Go `typeInfer4MaxMin`: the result carries the argument's
        // own type (with NOT NULL dropped, which this seed does not
        // track on result columns).
        //
        // The head of that function rewrites the ARGUMENT rather than the
        // result, so it is applied where the argument is built
        // ([`cast_float_scalar_arg_to_double`]) and this arm stays a plain
        // "carry the argument's type".
        "MIN" | "MAX" => {
            let mut t = arg
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            t.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            let kind = if name == "MIN" {
                AggKind::Min
            } else {
                AggKind::Max
            };
            (kind, t)
        }
        // Go `typeInfer4Avg`: DOUBLE for real arguments, otherwise
        // DECIMAL. The decimal scale Go derives from
        // div_precision_increment is display metadata this seed
        // does not set on result columns (documented deferral).
        "AVG" => {
            let code = arg
                .static_type()
                .map_or(FieldTypeCode::NewDecimal, |t| match t.code() {
                    FieldTypeCode::Float | FieldTypeCode::Double => FieldTypeCode::Double,
                    _ => FieldTypeCode::NewDecimal,
                });
            (AggKind::Avg, FieldType::new(code))
        }
        // Go `typeInfer4BitFuncs`: a binary `BIGINT(21) UNSIGNED` that never
        // returns NULL -- an empty (or all-NULL) input folds to the
        // operator's identity, not NULL. The UNSIGNED flag is what makes an
        // all-NULL `BIT_AND` read back as `18446744073709551615`, and a view
        // over one describe as `bigint(21) unsigned NO` (captured from TiDB).
        "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.set_flen(21);
            t.set_decimal(0);
            t.add_flags(
                tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::UNSIGNED,
            );
            let op = match name {
                "BIT_AND" => crate::hash_agg::BitOp::And,
                "BIT_OR" => crate::hash_agg::BitOp::Or,
                _ => crate::hash_agg::BitOp::Xor,
            };
            (AggKind::Bit(op), t)
        }
        // Go `typeInfer4PopOrSamp`: a nullable `DOUBLE(23)` with an
        // unspecified scale, regardless of the argument's own type.
        // The parser canonicalizes `VARIANCE` to `VAR_POP` and
        // `STD`/`STDDEV` to `STDDEV_POP`, so only the four canonical names
        // reach here.
        "VAR_POP" | "VAR_SAMP" | "STDDEV_POP" | "STDDEV_SAMP" => {
            let mut t = FieldType::new(FieldTypeCode::Double);
            t.set_flen(23);
            t.set_decimal(tidb_datatype::UNSPECIFIED_FSP);
            let kind = AggKind::Variance {
                sample: matches!(name, "VAR_SAMP" | "STDDEV_SAMP"),
                sqrt: matches!(name, "STDDEV_POP" | "STDDEV_SAMP"),
            };
            (kind, t)
        }
        // Go `typeInfer4JsonArrayAgg`/`typeInfer4JsonObjectAgg`: a binary
        // JSON column with no written width (captured: type 245, flen -1,
        // decimals -1, the BINARY flag set).
        "JSON_ARRAYAGG" | "JSON_OBJECTAGG" => {
            let mut t = FieldType::new(FieldTypeCode::Json);
            t.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
            // The VALUE argument's own field type -- `JSON_ARRAYAGG`'s first
            // argument, `JSON_OBJECTAGG`'s second -- decides how a
            // BINARY-charset string embeds: Go's `getRealJSONValue` tags the
            // JSON `Opaque` it builds with `ft.GetType()`, the source
            // column's exact MySQL type code (captured: VARBINARY is 15,
            // fixed-length BINARY(n) is 254 and zero-padded to `n`, the
            // TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB family is 249/252/250/251).
            let value_arg = if name == "JSON_ARRAYAGG" {
                args.first()
            } else {
                args.get(1)
            };
            let value_type = value_arg
                .and_then(Expression::static_type)
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::VarString));
            let kind = if name == "JSON_ARRAYAGG" {
                AggKind::JsonArrayAgg { value_type }
            } else {
                // The KEY argument's own field type decides 3144 -- Go:
                // `e.args[0].GetType(sctx).GetCharset() == charset.CharsetBin`
                // -- a STATIC property of the declared argument type, not
                // the evaluated key datum (see `AggKind::JsonObjectAgg`'s own
                // doc for why the datum kind alone is not enough).
                let key_is_binary = args
                    .first()
                    .and_then(Expression::static_type)
                    .is_some_and(FieldType::is_binary_string);
                AggKind::JsonObjectAgg {
                    value_type,
                    key_is_binary,
                }
            };
            (kind, t)
        }
        // Go `typeInfer4ApproxCountDistinct` delegates to `typeInfer4Count`,
        // so the result is COUNT's own NOT NULL binary `BIGINT(21)`.
        "APPROX_COUNT_DISTINCT" => {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.set_flen(21);
            t.set_decimal(0);
            t.add_flags(
                tidb_datatype::FieldTypeFlags::BINARY | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
            (AggKind::ApproxCountDistinct, t)
        }
        // Go `typeInfer4ApproxPercentile`: two arguments, the second a
        // CONSTANT percentage in [1, 100], and a result type read off the
        // first argument.
        "APPROX_PERCENTILE" => {
            let [_, percent_arg] = args else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE should take 2 arguments",
                ));
            };
            let Some(folded) = fold_constant(percent_arg) else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE should take a constant expression as percentage argument",
                ));
            };
            let Some(percent) = constant_eval_int(&folded) else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE: Percentage value cannot be NULL",
                ));
            };
            if percent <= 0 || percent > 100 {
                return Err(DriverError::PercentageOutOfRange(percent));
            }
            let arg_type = arg.static_type().cloned();
            let code = arg_type
                .as_ref()
                .map_or(FieldTypeCode::LongLong, |t| t.code());
            let ret = match code {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong => FieldType::new(FieldTypeCode::LongLong),
                FieldTypeCode::Double | FieldTypeCode::Float => {
                    FieldType::new(FieldTypeCode::Double)
                }
                FieldTypeCode::NewDecimal => {
                    let mut t = FieldType::new(FieldTypeCode::NewDecimal);
                    t.set_flen(MAX_DECIMAL_WIDTH);
                    let scale = arg_type.as_ref().map_or(-1, FieldType::decimal);
                    t.set_decimal(if (0..=MAX_DECIMAL_SCALE).contains(&scale) {
                        scale
                    } else {
                        MAX_DECIMAL_SCALE
                    });
                    t
                }
                FieldTypeCode::Date
                | FieldTypeCode::Datetime
                | FieldTypeCode::NewDate
                | FieldTypeCode::Timestamp => arg_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
                _ => {
                    let mut t = arg_type
                        .clone()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                    t.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                    t
                }
            };
            // `buildApproxPercentile` picks a typed accumulator by the
            // argument's EVAL type -- and `getEvalTypeForApproxPercentile`
            // forces ENUM/SET/BIT to the string domain. Every other eval type
            // (a string column, say) gets Go's `basePercentile`, which
            // appends NULL for every group.
            let eval_type = arg_type.as_ref().map(FieldType::eval_type);
            let ranks = !matches!(
                code,
                FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
            ) && matches!(
                eval_type,
                Some(
                    tidb_datatype::EvalType::Int
                        | tidb_datatype::EvalType::Real
                        | tidb_datatype::EvalType::Decimal
                        | tidb_datatype::EvalType::Datetime
                        | tidb_datatype::EvalType::Timestamp
                        | tidb_datatype::EvalType::Duration
                )
            );
            (AggKind::ApproxPercentile(ranks.then_some(percent)), ret)
        }
        _ => {
            return Err(DriverError::unsupported(
                "this aggregate function is deferred",
            ))
        }
    })
}

/// Go `mysql.MaxDecimalWidth`, the width `APPROX_PERCENTILE` gives a DECIMAL
/// result.
const MAX_DECIMAL_WIDTH: i64 = 65;
/// Go `mysql.MaxDecimalScale`.
const MAX_DECIMAL_SCALE: i64 = 30;

/// Go `typeInfer4GroupConcat`: derive the result's character metadata from
/// every value argument plus the separator literal, then fill empty metadata
/// from the connection charset/collation.
fn group_concat_result_type(
    args: &[Expression],
    separator: &tidb_ast::TypedString,
) -> Result<FieldType, DriverError> {
    let mut collation_args = args.to_vec();
    let mut separator_type = FieldType::parser(FieldTypeCode::VarString);
    separator_type.set_charset_name(separator.charset.clone());
    separator_type.set_collation_name(separator.collation.clone());
    collation_args.push(Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::new_string(separator.value.clone()),
        separator_type,
    )));

    let derived = tidb_expr::collation_derive::check_and_derive_collation_from_exprs(
        "group_concat",
        tidb_datatype::EvalType::String,
        &collation_args,
    )
    .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
    let (connection_charset, connection_collation) =
        tidb_expr::collation_derive::connection_charset_info();
    let charset = if derived.charset.is_empty() {
        connection_charset.to_owned()
    } else {
        derived.charset
    };
    let collation = if derived.collation.is_empty() {
        if charset == connection_charset {
            connection_collation.to_owned()
        } else {
            tidb_datatype::get_default_collation(&charset)
                .unwrap_or_else(|_| connection_collation.to_owned())
        }
    } else {
        derived.collation
    };

    let mut result = FieldType::new(FieldTypeCode::VarString);
    result.set_charset_name(charset);
    result.set_collation_name(collation);
    result.set_flen(16_777_216); // mysql.MaxBlobWidth
    result.set_decimal(0);
    Ok(result)
}

/// The value of a row-independent expression, or `None` when it reads a
/// column (Go's `ConstLevel() == ConstNone`).
///
/// Go's expression rewriter FOLDS a constant subtree into one `Constant`
/// before the aggregate descriptor inspects it, which is why
/// `APPROX_PERCENTILE(v, -1)` -- a unary minus over a literal, not a literal
/// -- passes Go's constant check. Folding here at the point of use reaches the
/// same answer without a rewriter-wide folding pass.
fn fold_constant(expr: &Expression) -> Option<Datum> {
    match expr {
        Expression::Constant(constant) => Some(constant.value.clone()),
        Expression::Column(_) | Expression::CorrelatedColumn(_) => None,
        Expression::ScalarFunction(function) => {
            if !function.args.iter().all(|arg| fold_constant(arg).is_some()) {
                return None;
            }
            let chunk = {
                let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
                chunk.set_num_virtual_rows(1);
                chunk
            };
            expr.eval(&crate::StmtContext::for_query(), chunk.get_row(0))
                .ok()
        }
    }
}

/// Go `Constant.EvalInt` for a literal percentage argument.
///
/// The tail of Go's `EvalInt` is `dt.GetInt64()`, an UNCONVERTED read of the
/// datum's own int64 field: only an integer (or a string, which takes the
/// `ToInt64` branch above it) yields the number as written. A DECIMAL literal
/// stores nothing in that field, so `APPROX_PERCENTILE(v, 50.5)` reports
/// "Percentage value 0"; a FLOAT literal stores its IEEE-754 bits there, so
/// `APPROX_PERCENTILE(v, 50e0)` reports "Percentage value
/// 4632233691727265792" (both captured from TiDB). `None` is Go's `isNull`.
fn constant_eval_int(value: &Datum) -> Option<i64> {
    match value {
        Datum::Null => None,
        Datum::Int(number) => Some(*number),
        Datum::UInt(number) => Some(*number as i64),
        // Go's `dt.Kind() == KindString` branch, which DOES convert.
        Datum::String(_) | Datum::Bytes(_) => Some(value.to_i64().map_or(0, |result| result.value)),
        Datum::Real(number) | Datum::Float32(number) => Some(number.to_bits() as i64),
        _ => Some(0),
    }
}

/// The aggregation's output columns, addressed by name.
///
/// Go rewrites `HAVING`/`ORDER BY` to reference the aggregation's output
/// schema (`resolveHavingAndOrderBy` + `buildProjection`), so those clauses see
/// the aggregate results rather than the source rows. This resolver is that
/// output schema: a name is a select field's alias or column name, or an
/// aggregate's restored text.
pub(crate) struct AggOutputResolver {
    pub(crate) names: Vec<String>,
    pub(crate) types: Vec<FieldType>,
    /// The statement's session `time_zone` (see [`ColumnResolver::time_zone`]),
    /// carried over from the source scope the aggregation reads.
    pub(crate) zone: tidb_expr::SessionTimeZone,
}

impl ColumnResolver for AggOutputResolver {
    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        self.zone.clone()
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let index = self
            .names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))?;
        Some((index, self.types[index].clone(), (index + 1) as i64))
    }
}

/// Go `havingWindowAndOrderbyExprResolver`: rewrites a `HAVING`/`ORDER BY`
/// expression so every aggregate in it refers to an aggregation output column,
/// appending a hidden aggregate when the select list does not already compute
/// it.
///
/// The substitution is textual in the same sense Go's is structural: an
/// aggregate node becomes a column reference whose name is the aggregate's
/// restored text, which [`AggOutputResolver`] then binds to the output column.
///
/// Only the expression forms the expression rewriter itself supports are
/// walked (literals, parentheses, unary, binary, columns, aggregates); any
/// other form would fail to rewrite anyway and is returned unchanged.
/// The in-place walk [`substitute_aggregates`] runs: each aggregate,
/// `GROUPING()` call or unprojected column becomes a reference to an
/// aggregation output column, appending that column when it is new.
struct AggregateSubstitutor<'a, 'r> {
    agg_funcs: &'a mut Vec<AggFunc>,
    names: &'a mut Vec<String>,
    types: &'a mut Vec<FieldType>,
    grouping_specs: &'a mut Vec<GroupingSpec>,
    group_by_names: &'a [String],
    resolver: &'a ScopeResolver<'r>,
    /// The first failure, which stops the walk.
    error: Option<DriverError>,
}

impl AggregateSubstitutor<'_, '_> {
    /// Rewrites one node, reporting whether its children still need walking.
    fn substitute(&mut self, expr: &mut tidb_ast::Expr) -> Result<bool, DriverError> {
        use tidb_ast::Expr;
        // GROUPING() is hoisted the same way an aggregate is: the value is
        // computed by the rollup pass into an output column, and the clause
        // reads that column. A GROUPING() only HAVING or ORDER BY needs
        // becomes a hidden column and is trimmed by the final projection.
        if let Some(args) = grouping_call_args(expr) {
            let display = expr.restore();
            let (name, _) = add_grouping_column(
                args,
                display,
                self.agg_funcs,
                self.names,
                self.types,
                self.grouping_specs,
                self.group_by_names,
            )?;
            *expr = Expr::Column(vec![name]);
            return Ok(false);
        }
        match expr {
            // A subquery carries its own scope; nothing inside it is this
            // aggregation's to hoist. The operand BESIDE the subquery still
            // is, which is what makes `HAVING COUNT(*) IN (SELECT 2)` work.
            Expr::Subquery(_) | Expr::Exists { .. } => Ok(false),
            Expr::InSubquery { expr: operand, .. } => {
                let mut owned = std::mem::replace(operand.as_mut(), Expr::Null);
                self.walk(&mut owned);
                *operand.as_mut() = owned;
                Ok(false)
            }
            Expr::CompareSubquery { left, .. } => {
                let mut owned = std::mem::replace(left.as_mut(), Expr::Null);
                self.walk(&mut owned);
                *left.as_mut() = owned;
                Ok(false)
            }
            // A hoisted window column is computed ABOVE the aggregation, so it
            // is neither grouped nor aggregated and must be left alone here; it
            // resolves once the window stage has appended it.
            Expr::Column(path)
                if path
                    .last()
                    .is_some_and(|name| crate::window::is_window_column(name)) =>
            {
                Ok(false)
            }
            // A column that HAVING/ORDER BY references but the select list does
            // not project: Go carries it out of the aggregation as a hidden
            // FIRST_ROW column, exactly as it does for a selected group column,
            // whether or not the column is grouped. Whether an UNGROUPED one
            // may be read at all is `only_full_group_by`'s question, asked once
            // at the top of the pipeline over the clauses as written -- this
            // path must not re-decide it from the grouped-name list alone,
            // which knows nothing of the candidate-key dependency that permits
            // `GROUP BY id ORDER BY z` on a primary-keyed table.
            Expr::Column(path) => {
                let name = path.last().cloned().unwrap_or_default();
                // `__apply_N` is not a real column: it is the placeholder a
                // correlated subquery's extraction left behind, standing in for
                // the column an Apply appends above the aggregation once the
                // subquery is bound and run. It carries no ONLY_FULL_GROUP_BY
                // obligation of its own, so it passes through untouched.
                if name.starts_with("__apply_")
                    || self
                        .names
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(&name))
                {
                    return Ok(false);
                }
                let carrier = rewrite_expr_resolved(expr, self.resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let ftype = carrier
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                self.agg_funcs.push(AggFunc {
                    kind: AggKind::FirstRow,
                    arg: Some(carrier),
                    extra_args: Vec::new(),
                    distinct: false,
                    order_by: Vec::new(),
                    arg_orig_name: String::new(),
                });
                self.names.push(name.clone());
                self.types.push(ftype);
                *expr = Expr::Column(vec![name]);
                Ok(false)
            }
            // GROUP_CONCAT is substituted the same way: the aggregate is
            // hoisted and the field becomes a reference to its output column.
            // Its ARGUMENTS are not walked -- they belong to the aggregate,
            // which reads the source rows, not the aggregation's output.
            Expr::Aggregate { .. } | Expr::GroupConcat { .. } => {
                let text = expr.restore();
                if !self
                    .names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&text))
                {
                    let (func, ftype) = build_agg_func(expr, self.resolver)?;
                    self.agg_funcs.push(func);
                    self.names.push(text.clone());
                    self.types.push(ftype);
                }
                *expr = Expr::Column(vec![text]);
                Ok(false)
            }
            _ => Ok(true),
        }
    }

    /// Walks one child expression the enclosing node is skipping.
    fn walk(&mut self, expr: &mut tidb_ast::Expr) {
        tidb_ast::Visitable::accept(expr, self);
    }
}

impl tidb_ast::Visitor for AggregateSubstitutor<'_, '_> {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if self.error.is_some() {
            return true;
        }
        let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
            return false;
        };
        match self.substitute(expr) {
            Ok(walk_children) => !walk_children,
            Err(error) => {
                self.error = Some(error);
                true
            }
        }
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.error.is_none()
    }
}

pub(crate) fn substitute_aggregates(
    expr: &tidb_ast::Expr,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_names: &[String],
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    let mut owned = expr.clone();
    let mut substitutor = AggregateSubstitutor {
        agg_funcs,
        names,
        types,
        grouping_specs,
        group_by_names,
        resolver,
        error: None,
    };
    tidb_ast::Visitable::accept(&mut owned, &mut substitutor);
    match substitutor.error {
        Some(error) => Err(error),
        None => Ok(owned),
    }
}

/// The window-call index a select field IS, once
/// [`crate::window::hoist_windows`] has replaced the call with its computed
/// column.
pub(crate) fn hoisted_window_index(expr: &tidb_ast::Expr) -> Option<usize> {
    let tidb_ast::Expr::Column(path) = expr else {
        return None;
    };
    let name = path.last()?;
    crate::window::is_window_column(name)
        .then(|| crate::window::window_column_index(name))
        .flatten()
}

/// Whether `expr` reads a hoisted window column anywhere inside a larger
/// expression.
pub(crate) fn expr_has_hoisted_window(expr: &tidb_ast::Expr) -> bool {
    struct Finder {
        found: bool,
    }
    impl tidb_ast::Visitor for Finder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_ref::<tidb_ast::Expr>() {
                if path
                    .last()
                    .is_some_and(|name| crate::window::is_window_column(name))
                {
                    self.found = true;
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut finder = Finder { found: false };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut finder);
    finder.found
}

/// Builds one aggregate function (and its Go-inferred result type) from an
/// `Expr::Aggregate` node.
/// The head of Go `typeInfer4MaxMin` (`pkg/expression/aggregation/base_func.go`):
/// a `MAX`/`MIN` argument that is a SCALAR FUNCTION of type FLOAT is wrapped in
/// a cast to DOUBLE before its type is read.
///
/// Go's reason is representational: a `float32` result is carried in the
/// `float64` field of a `Datum`, so an argument extracted into a projection
/// would otherwise disagree with its own 4-byte cell. The wrap is on the
/// ARGUMENT, which is why the result widens as a VALUE and not merely as a
/// label -- relabelling the result DOUBLE while the argument still produced a
/// `Float32` wrote 4 bytes into an 8-byte cell and aborted the process.
///
/// A FLOAT COLUMN is deliberately left alone: it is not a scalar function, so
/// `max(c)` stays narrow while `max(ifnull(c, 0))` over the same column
/// widens. Captured from Go as `12.191` against `12.190999984741211`.
fn cast_float_scalar_arg_to_double(arg: Expression) -> Expression {
    if !matches!(arg, Expression::ScalarFunction(_))
        || arg.static_type().map(FieldType::code) != Some(FieldTypeCode::Float)
    {
        return arg;
    }
    /// Go `mysql.MaxRealWidth`.
    const MAX_REAL_WIDTH: i64 = 23;
    let mut ret_type = FieldType::new(FieldTypeCode::Double);
    ret_type.set_flen(MAX_REAL_WIDTH);
    ret_type.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
    Expression::ScalarFunction(tidb_expr::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new("cast_double"),
        ret_type,
        vec![arg],
    ))
}

pub(crate) fn build_agg_func(
    expr: &tidb_ast::Expr,
    resolver: &ScopeResolver<'_>,
) -> Result<(AggFunc, FieldType), DriverError> {
    // GROUP_CONCAT is its own AST shape: it carries a separator and its own
    // row ORDER BY rather than being a one-argument aggregate.
    if let tidb_ast::Expr::GroupConcat {
        distinct,
        args,
        order_by,
        separator,
    } = expr
    {
        // `GROUP_CONCAT(a, b, ...)` concatenates its arguments per row before
        // the rows are joined; the first argument rides `arg` and the rest
        // ride `extra_args`.
        let Some((first, rest)) = args.split_first() else {
            return Err(DriverError::unsupported(
                "GROUP_CONCAT requires at least one argument",
            ));
        };
        let arg = rewrite_expr_resolved(first, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        let mut extra_args = Vec::with_capacity(rest.len());
        for extra in rest {
            extra_args.push(
                rewrite_expr_resolved(extra, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
            );
        }
        // The aggregate's own ORDER BY items resolve against the SOURCE row,
        // the same scope the concatenated argument does.
        let mut order_items = Vec::with_capacity(order_by.len());
        for item in order_by {
            let expr = rewrite_expr_resolved(&item.expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            order_items.push((expr, item.desc));
        }
        // The 1260 truncation message names args[0] by its `OrigName`, which
        // the rewrite above discards; capture it while the AST path is still
        // in hand.
        let arg_orig_name = match first {
            tidb_ast::Expr::Column(path) => resolver.orig_name(path).unwrap_or_default(),
            _ => String::new(),
        };
        let mut collation_args = Vec::with_capacity(1 + extra_args.len());
        collation_args.push(arg.clone());
        collation_args.extend(extra_args.iter().cloned());
        let ret_type = group_concat_result_type(&collation_args, separator)?;
        return Ok((
            AggFunc {
                kind: AggKind::GroupConcat {
                    separator: separator.value.clone(),
                },
                arg: Some(arg),
                extra_args,
                distinct: *distinct,
                order_by: order_items,
                arg_orig_name,
            },
            ret_type,
        ));
    }
    let tidb_ast::Expr::Aggregate {
        name,
        distinct,
        args,
    } = expr
    else {
        return Err(DriverError::unsupported("not an aggregate function"));
    };
    // `COUNT(DISTINCT a, b, ...)` is the one non-GROUP_CONCAT aggregate the
    // parser lets through with more than one argument (`parse_aggregate`
    // rejects a bare `COUNT(a, b)` and every multi-argument `SUM`/`AVG`/etc.
    // at parse time), so only COUNT needs an `extra_args`-carrying path here.
    let Some((first, rest)) = args.split_first() else {
        return Err(DriverError::unsupported(
            "multi-argument aggregates are deferred",
        ));
    };
    if !rest.is_empty()
        && !matches!(
            name.as_str(),
            "COUNT" | "JSON_OBJECTAGG" | "APPROX_COUNT_DISTINCT" | "APPROX_PERCENTILE"
        )
    {
        return Err(DriverError::unsupported(
            "multi-argument aggregates are deferred",
        ));
    }
    // A subquery inside an aggregate's own argument (`SUM((SELECT ...))`,
    // `SUM(CASE WHEN EXISTS(...) THEN v END)`) would need to run once per
    // SOURCE row, before the aggregate accumulates it -- an Apply BELOW the
    // aggregation, rather than the Apply above it this driver builds for a
    // select-field/HAVING/ORDER BY subquery (which reads the already-grouped
    // value). That per-row Apply is not built here; refuse precisely rather
    // than let the per-row rewriter reject it with its generic message.
    if expr_has_subquery(first) || rest.iter().any(expr_has_subquery) {
        return Err(DriverError::unsupported(
            "a subquery inside an aggregate function's argument is not supported yet",
        ));
    }
    let mut arg = rewrite_expr_resolved(first, resolver)
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    if matches!(name.as_str(), "MIN" | "MAX") {
        arg = cast_float_scalar_arg_to_double(arg);
    }
    let mut extra_args = Vec::with_capacity(rest.len());
    for extra in rest {
        extra_args.push(
            rewrite_expr_resolved(extra, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }
    let mut all_args = Vec::with_capacity(1 + extra_args.len());
    all_args.push(arg.clone());
    all_args.extend(extra_args.iter().cloned());
    let (kind, ftype) = agg_kind_and_type(name, &all_args)?;
    // `APPROX_PERCENTILE`'s percentage rides the KIND, not the argument list:
    // it is a plan-time constant Go reads once in `buildApproxPercentile`,
    // never a per-row input.
    if matches!(kind, AggKind::ApproxPercentile(_)) {
        extra_args.clear();
    }
    Ok((
        AggFunc {
            kind,
            arg: Some(arg),
            extra_args,
            distinct: *distinct,
            order_by: Vec::new(),
            arg_orig_name: String::new(),
        },
        ftype,
    ))
}

#[cfg(test)]
mod source_tests {
    use super::*;

    fn column(unique_id: i64, field_type: FieldType) -> Expression {
        Expression::Column(tidb_expr::column::Column::new(unique_id, field_type))
    }

    // Go pkg/expression/aggregation/base_func_test.go::
    // TestBaseFunc_InferAggRetType.
    #[test]
    fn test_base_func_infer_agg_ret_type() {
        for (unique_id, data_type) in [
            (1, FieldType::new(FieldTypeCode::Double)),
            (2, FieldType::new(FieldTypeCode::Bit)),
        ] {
            let mut not_null_type = data_type.clone();
            not_null_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            let argument = column(unique_id, not_null_type);
            for name in ["MAX", "MIN"] {
                let (_, result_type) = agg_kind_and_type(name, std::slice::from_ref(&argument))
                    .expect("MAX/MIN type inference");
                assert_eq!(result_type, data_type, "{name} over {data_type:?}");
            }
        }

        let mut non_binary_type = FieldType::new(FieldTypeCode::VarString);
        non_binary_type.set_charset_name("utf8mb4");
        non_binary_type.set_collation_name("utf8mb4_0900_ai_ci");
        let non_binary = column(3, non_binary_type);
        let matching_separator = tidb_ast::TypedString::new(" ", "utf8mb4", "utf8mb4_0900_ai_ci");
        let result =
            group_concat_result_type(std::slice::from_ref(&non_binary), &matching_separator)
                .unwrap();
        assert_eq!(result.charset_name(), "utf8mb4");
        assert_eq!(result.collation_name(), "utf8mb4_0900_ai_ci");
        assert_eq!(result.flen(), 16_777_216);
        assert_eq!(result.decimal(), 0);

        let empty_separator = tidb_ast::TypedString::new(",", "", "");
        let numeric = column(4, FieldType::new(FieldTypeCode::LongLong));
        let result =
            group_concat_result_type(std::slice::from_ref(&numeric), &empty_separator).unwrap();
        assert_eq!(result.charset_name(), "utf8mb4");
        assert_eq!(result.collation_name(), "utf8mb4_bin");

        let result =
            group_concat_result_type(std::slice::from_ref(&non_binary), &empty_separator).unwrap();
        assert_eq!(result.charset_name(), "utf8mb4");
        assert_eq!(result.collation_name(), "utf8mb4_0900_ai_ci");
    }
}
