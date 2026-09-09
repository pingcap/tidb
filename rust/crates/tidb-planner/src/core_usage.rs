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

//! Shared planner helpers from Go `pkg/planner/util/coreusage`.

use tidb_expr::aggregation::{AggDescError, AggFuncDesc, AggFunctionMode};
use tidb_expr::Columns;

/// Go `coreusage.WrapCastForAggFuncs`: cast aggregate arguments to the
/// descriptor result type except when they already carry a partial result.
pub fn wrap_cast_for_agg_funcs(
    ctx: &impl Columns,
    agg_funcs: &mut [AggFuncDesc],
) -> Result<(), AggDescError> {
    for agg_func in agg_funcs {
        if !matches!(
            agg_func.mode,
            AggFunctionMode::Final | AggFunctionMode::Partial2
        ) {
            agg_func.base.wrap_cast_for_agg_args(ctx)?;
        }
    }
    Ok(())
}
