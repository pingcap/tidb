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

//! One canonical executor runtime for core aggregate partial states.

use tidb_datatype::Datum;
use tidb_expr::EvalError;
use tidb_planner::aggregation_descriptor::AggregateKind;

use super::aggregate_distinct::DistinctChecker;
use crate::ExecError;

pub mod avg;
pub mod count;
pub mod max_min;
pub mod spill;
pub mod sum;
pub mod variance;

pub use avg::{AvgFloat64State, AvgState};
pub use count::{CountDistinctIntState, CountState};
pub use max_min::MaxMinState;
pub use sum::{
    SumDecimalState, SumFloat64State, SumInt64State, SumIntError, SumState, SumUint64State,
};
pub use variance::VarianceState;

/// Folds resolved values through the state family selected by canonical kind.
pub fn fold_values(
    kind: AggregateKind,
    distinct: bool,
    values: &[Datum],
    div_precision_increment: u32,
) -> Result<Datum, ExecError> {
    let is_variance = VarianceState::supports(kind);
    let mut values = values.to_vec();
    if distinct && !is_variance {
        let mut checker = DistinctChecker::new();
        values.retain(|value| checker.check(std::slice::from_ref(value)));
    }

    match kind {
        AggregateKind::Count => {
            let mut partial = CountState::new();
            for value in &values {
                partial.update(value);
            }
            let mut destination = CountState::new();
            destination.merge_from(&partial);
            Ok(Datum::Int(destination.result()))
        }
        AggregateKind::Sum => {
            let mut partial = SumState::new();
            for value in &values {
                partial.update(&promote_sum_input(value))?;
            }
            let mut destination = SumState::new();
            destination.merge_from(&partial)?;
            Ok(destination.result().unwrap_or(Datum::Null))
        }
        AggregateKind::SumInt => {
            let mut partial = SumState::new();
            for value in &values {
                partial.update(value)?;
            }
            let mut destination = SumState::new();
            destination.merge_from(&partial)?;
            Ok(destination.result().unwrap_or(Datum::Null))
        }
        AggregateKind::Avg => {
            let mut partial = AvgState::new();
            for value in &values {
                partial.update(&promote_sum_input(value))?;
            }
            let mut destination = AvgState::new();
            destination.merge_from(&partial)?;
            destination.result(div_precision_increment)
        }
        AggregateKind::Max | AggregateKind::Min => {
            let mut partial = MaxMinState::new(kind).expect("MAX/MIN kind was matched");
            for value in &values {
                partial.update(value)?;
            }
            let mut destination = MaxMinState::new(kind).expect("MAX/MIN kind was matched");
            destination.merge_from(&partial)?;
            Ok(destination.result())
        }
        AggregateKind::VarPop
        | AggregateKind::VarSamp
        | AggregateKind::StddevPop
        | AggregateKind::StddevSamp => {
            // Variance DISTINCT is deliberately not routed through the
            // generic Datum tuple checker. Go owns a dedicated float64 set
            // whose partial states are unioned before finalization.
            let mut partial = VarianceState::new(kind, distinct)?;
            for value in &values {
                partial.update(value)?;
            }
            let mut destination = VarianceState::new(kind, distinct)?;
            destination.merge_from(&partial)?;
            Ok(destination.result().map_or(Datum::Null, Datum::Real))
        }
        AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor => {
            crate::bit_agg::fold_bit_values(kind, &values)
        }
        AggregateKind::FirstRow => Ok(crate::first_row::fold_first_row(&values)),
        _ => Err(ExecError::Eval(EvalError::Unsupported(
            "aggregate function",
        ))),
    }
}

fn promote_sum_input(value: &Datum) -> Datum {
    match value {
        Datum::Int(value) => Datum::Decimal(tidb_datatype::Decimal::from_int(*value)),
        Datum::UInt(value) => Datum::Decimal(tidb_datatype::Decimal::from_uint(*value)),
        value => value.clone(),
    }
}
