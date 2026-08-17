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

//! The aggregate and window function NAMES from
//! `pkg/parser/ast/functions.go:820`-`:856` (`AggFunc*`) and `:950`-`:970`
//! (`WindowFunc*`).
//!
//! These are the exact spellings `baseFuncDesc.TypeInfer` switches on, and
//! they are the lower-cased form `newBaseFuncDesc` normalizes to. They live
//! here rather than in `tidb-ast` because `tidb-ast` carries no function-name
//! table yet; moving them there later is a re-export, not a rewrite.

/// Go `ast.AggFuncCount`.
pub const COUNT: &str = "count";
/// Go `ast.AggFuncSum`.
pub const SUM: &str = "sum";
/// Go `ast.AggFuncSumInt`: the integer-specialized SUM the coprocessor uses.
pub const SUM_INT: &str = "sum_int";
/// Go `ast.AggFuncAvg`.
pub const AVG: &str = "avg";
/// Go `ast.AggFuncFirstRow`: the planner's carrier for a group-by column.
pub const FIRST_ROW: &str = "firstrow";
/// Go `ast.AggFuncMax`.
pub const MAX: &str = "max";
/// Go `ast.AggFuncMin`.
pub const MIN: &str = "min";
/// Go `ast.AggFuncGroupConcat`.
pub const GROUP_CONCAT: &str = "group_concat";
/// Go `ast.AggFuncBitOr`.
pub const BIT_OR: &str = "bit_or";
/// Go `ast.AggFuncBitXor`.
pub const BIT_XOR: &str = "bit_xor";
/// Go `ast.AggFuncBitAnd`.
pub const BIT_AND: &str = "bit_and";
/// Go `ast.AggFuncVarPop`.
pub const VAR_POP: &str = "var_pop";
/// Go `ast.AggFuncVarSamp`.
pub const VAR_SAMP: &str = "var_samp";
/// Go `ast.AggFuncStddevPop`.
pub const STDDEV_POP: &str = "stddev_pop";
/// Go `ast.AggFuncStddevSamp`.
pub const STDDEV_SAMP: &str = "stddev_samp";
/// Go `ast.AggFuncJsonArrayagg`.
pub const JSON_ARRAYAGG: &str = "json_arrayagg";
/// Go `ast.AggFuncJsonObjectAgg`.
pub const JSON_OBJECTAGG: &str = "json_objectagg";
/// Go `ast.AggFuncApproxCountDistinct`.
pub const APPROX_COUNT_DISTINCT: &str = "approx_count_distinct";
/// Go `ast.AggFuncApproxPercentile`.
pub const APPROX_PERCENTILE: &str = "approx_percentile";

/// Go `ast.WindowFuncRowNumber`.
pub const ROW_NUMBER: &str = "row_number";
/// Go `ast.WindowFuncRank`.
pub const RANK: &str = "rank";
/// Go `ast.WindowFuncDenseRank`.
pub const DENSE_RANK: &str = "dense_rank";
/// Go `ast.WindowFuncCumeDist`.
pub const CUME_DIST: &str = "cume_dist";
/// Go `ast.WindowFuncPercentRank`.
pub const PERCENT_RANK: &str = "percent_rank";
/// Go `ast.WindowFuncNtile`.
pub const NTILE: &str = "ntile";
/// Go `ast.WindowFuncLead`.
pub const LEAD: &str = "lead";
/// Go `ast.WindowFuncLag`.
pub const LAG: &str = "lag";
/// Go `ast.WindowFuncFirstValue`.
pub const FIRST_VALUE: &str = "first_value";
/// Go `ast.WindowFuncLastValue`.
pub const LAST_VALUE: &str = "last_value";
/// Go `ast.WindowFuncNthValue`.
pub const NTH_VALUE: &str = "nth_value";
