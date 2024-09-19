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

package context

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/stretchr/testify/require"
)

func TestContextDetach(t *testing.T) {
	warnHandler := contextutil.NewStaticWarnHandler(5)
	planCacheTracker := contextutil.NewPlanCacheTracker(warnHandler)
	rangeFallbackHandler := contextutil.NewRangeFallbackHandler(&planCacheTracker, warnHandler)
	obj := &RangerContext{
		TypeCtx:                  types.DefaultStmtNoWarningContext,
		ErrCtx:                   errctx.StrictNoWarningContext,
		ExprCtx:                  exprstatic.NewExprContext(),
		RangeFallbackHandler:     &rangeFallbackHandler,
		PlanCacheTracker:         &planCacheTracker,
		OptimizerFixControl:      map[uint64]string{1: "a"},
		UseCache:                 true,
		InPreparedPlanBuilding:   true,
		RegardNULLAsPoint:        true,
		OptPrefixIndexSingleScan: true,
	}

	ignorePath := []string{
		"$.TypeCtx",
		"$.ErrCtx",
		"$.ExprCtx",
		"$.RangeFallbackHandler",
		"$.PlanCacheTracker",
	}
	deeptest.AssertRecursivelyNotEqual(t, obj, &RangerContext{}, deeptest.WithIgnorePath(ignorePath))

	staticObj := obj.Detach(obj.ExprCtx)

	deeptest.AssertDeepClonedEqual(t, obj, staticObj, deeptest.WithIgnorePath(ignorePath))

	require.Equal(t, obj.TypeCtx, staticObj.TypeCtx)
	require.Equal(t, obj.ErrCtx, staticObj.ErrCtx)
	require.Equal(t, obj.ExprCtx, staticObj.ExprCtx)
	require.Equal(t, obj.RangeFallbackHandler, staticObj.RangeFallbackHandler)
	require.Equal(t, obj.PlanCacheTracker, staticObj.PlanCacheTracker)
}
