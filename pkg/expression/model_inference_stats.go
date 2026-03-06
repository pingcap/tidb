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

package expression

import "github.com/pingcap/tidb/pkg/sessionctx/stmtctx"

type modelInferenceStatsTargetProvider interface {
	ModelInferenceStatsTarget() (stmtctx.ModelInferenceRole, int, bool)
}

type modelInferenceStatsEvalContext struct {
	EvalContext
	role   stmtctx.ModelInferenceRole
	planID int
}

// ModelInferenceStatsTarget returns the stats target metadata.
func (c *modelInferenceStatsEvalContext) ModelInferenceStatsTarget() (stmtctx.ModelInferenceRole, int, bool) {
	return c.role, c.planID, true
}

// WithModelInferenceStatsTarget attaches model inference role and plan ID to EvalContext.
func WithModelInferenceStatsTarget(ctx EvalContext, role stmtctx.ModelInferenceRole, planID int) EvalContext {
	if ctx == nil {
		return nil
	}
	return &modelInferenceStatsEvalContext{
		EvalContext: ctx,
		role:        role,
		planID:      planID,
	}
}

// ModelInferenceStatsTargetFromContext returns the model inference role and plan ID if present.
func ModelInferenceStatsTargetFromContext(ctx EvalContext) (stmtctx.ModelInferenceRole, int, bool) {
	for ctx != nil {
		if provider, ok := ctx.(modelInferenceStatsTargetProvider); ok {
			return provider.ModelInferenceStatsTarget()
		}
		if assertCtx, ok := ctx.(*assertionEvalContext); ok {
			ctx = assertCtx.EvalContext
			continue
		}
		break
	}
	return "", 0, false
}
