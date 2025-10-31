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

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// Init initializes LoadData.
func (p LoadData) Init(ctx base.PlanContext) *LoadData {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeLoadData, 0)
	return &p
}

// Init initializes ImportInto.
func (p ImportInto) Init(ctx base.PlanContext) *ImportInto {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeImportInto, 0)
	return &p
}

// Init initializes ScalarSubqueryEvalCtx
func (p ScalarSubqueryEvalCtx) Init(ctx base.PlanContext, offset int) *ScalarSubqueryEvalCtx {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeScalarSubQuery, offset)
	return &p
}
