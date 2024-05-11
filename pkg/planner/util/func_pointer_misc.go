// Copyright 2024 PingCAP, Inc.
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

package util

import "github.com/pingcap/tidb/pkg/planner/core/base"

// this file is used for passing function pointer at init(){} to avoid some import cycles.

// HasMaxOneRowUtil is used in baseLogicalPlan implementation of LogicalPlan interface, while
// the original HasMaxOneRowUtil has some dependency of original core pkg: like Datasource which
// hasn't been moved out of core pkg, so associative func pointer is introduced.
// todo: (1) arenatlx, remove this func pointer when concrete Logical Operators moved out.
var HasMaxOneRowUtil func(p base.LogicalPlan, childMaxOneRow []bool) bool
