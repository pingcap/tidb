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


//! Go `pkg/executor/aggfuncs/func_varsamp.go`: `VAR_SAMP`, the variance of
//! `func_varpop.rs` with its own final result.

use crate::func_varpop::{var_pop, var_pop_distinct, VarianceFinal};

/// `VAR_SAMP`'s final result.
const VAR_SAMP: VarianceFinal = VarianceFinal { sample: true, sqrt: false };

var_pop!(VarSamp4Float64, VAR_SAMP);
var_pop_distinct!(VarSampOriginal4DistinctFloat64, VAR_SAMP, merges: false);
var_pop_distinct!(VarSampPartial4DistinctFloat64, VAR_SAMP, merges: true);
