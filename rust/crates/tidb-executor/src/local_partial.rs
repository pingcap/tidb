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

//! The partial rows a coprocessor answers for a pushed partial aggregation,
//! computed in this tier when a scan answers it locally (a dirty read, a
//! refused remote shape): the coprocessor's own aggregation, Go's unistore
//! `aggExec` over `aggregation.NewDistAggFunc`
//! ([`tidb_expression_aggregation::DistAggregator`]), so a local answer and a
//! remote one are the same partial rows.

use tidb_datatype::{Datum, FieldType};
use tidb_distsql::remote_scan::{PushdownAggregateFunction, PushdownAggregateKind, PushdownPartialAggregate};
use tidb_expression_aggregation::{AggFunctionMode, DistAggregate, DistAggregator};
use tidb_proto::tipb::ExprType;
use tidb_driver_error::ExecError;

/// Go `aggExec.processAllRows` over the rows `next_row` reads, then its
/// partial results: each group's functions, then its keys.
pub(crate) fn local_partial_rows(
    aggregate: &PushdownPartialAggregate,
    input_types: &[FieldType],
    ctx: &crate::StmtContext,
    mut next_row: impl FnMut() -> Result<Option<Vec<Datum>>, ExecError>,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let (functions, group_by) = match aggregate {
        PushdownPartialAggregate::Global { functions, .. } => (functions, Vec::new()),
        PushdownPartialAggregate::Grouped {
            group_by,
            functions,
            ..
        } => (functions, group_by.clone()),
    };
    let functions = functions
        .iter()
        .map(dist_aggregate)
        .collect::<Result<Vec<_>, _>>()?;
    let mut aggregator = DistAggregator::new(group_by, functions, input_types);
    let zone = tidb_exprctx::Columns::time_zone(ctx);
    while let Some(row) = next_row()? {
        aggregator
            .update(ctx, &zone, &row)
            .map_err(ExecError::unsupported)?;
    }
    Ok(aggregator.finish())
}

/// The distributed aggregate a pushed function lowers to (`cop_reader`'s
/// `lower_aggregate_function`), in `Partial1Mode`.
fn dist_aggregate(function: &PushdownAggregateFunction) -> Result<DistAggregate, ExecError> {
    let kind = match function.kind {
        PushdownAggregateKind::Count => ExprType::Count,
        PushdownAggregateKind::Sum => ExprType::Sum,
        PushdownAggregateKind::Min => ExprType::Min,
        PushdownAggregateKind::Max => ExprType::Max,
        PushdownAggregateKind::FirstRow => ExprType::First,
        PushdownAggregateKind::BitAnd => ExprType::AggBitAnd,
        PushdownAggregateKind::BitOr => ExprType::AggBitOr,
        PushdownAggregateKind::BitXor => ExprType::AggBitXor,
        PushdownAggregateKind::MinCount | PushdownAggregateKind::MaxCount => {
            return Err(ExecError::unsupported(
                "MIN_COUNT and MAX_COUNT have no distributed aggregate",
            ));
        }
    };
    let args = match &function.input {
        Some(input) => vec![input.clone()],
        // Go lowers `COUNT(*)` to `COUNT(1)`.
        None => vec![tidb_expr::expression::Expression::Constant(
            tidb_expr::constant::Constant::new_one(),
        )],
    };
    DistAggregate::new(kind, args, AggFunctionMode::Partial1).map_err(ExecError::unsupported)
}
