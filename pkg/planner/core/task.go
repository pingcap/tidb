// Copyright 2017 PingCAP, Inc.
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

package core

// HeavyFunctionNameMap stores function names that is worth to do HeavyFunctionOptimize.
// Currently this only applies to Vector data types and their functions. The HeavyFunctionOptimize
// eliminate the usage of the function in TopN operators to avoid vector distance re-calculation
// of TopN in the root task.
var HeavyFunctionNameMap = map[string]struct{}{
	"vec_cosine_distance":        {},
	"vec_l1_distance":            {},
	"vec_l2_distance":            {},
	"vec_negative_inner_product": {},
	"vec_dims":                   {},
	"vec_l2_norm":                {},
}

