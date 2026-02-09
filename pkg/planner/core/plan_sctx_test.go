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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type nonSessionPlanCtx struct {
	planctx.PlanContext
}

type badUnwrapperPlanCtx struct {
	planctx.PlanContext
}

func (*badUnwrapperPlanCtx) UnwrapAsInternalSctx() any {
	return struct{}{}
}

func TestAsSctxDirectAndWrapped(t *testing.T) {
	baseCtx := mock.NewContext()

	sctx, err := AsSctx(baseCtx)
	require.NoError(t, err)
	require.Same(t, sessionctx.Context(baseCtx), sctx)

	overrideExprCtx := exprctx.WithNullRejectCheck(baseCtx.GetExprCtx())
	wrappedPctx := planctx.WithExprCtx(baseCtx, overrideExprCtx)
	require.NotSame(t, baseCtx, wrappedPctx)

	sctx, err = AsSctx(wrappedPctx)
	require.NoError(t, err)
	require.Same(t, sessionctx.Context(baseCtx), sctx)
}

func TestAsSctxFailurePaths(t *testing.T) {
	nonSession := &nonSessionPlanCtx{}
	if intest.EnableAssert {
		require.Panics(t, func() {
			_, _ = AsSctx(nonSession)
		})
	} else {
		_, err := AsSctx(nonSession)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing planctx.InternalSctxUnwrapper")
	}

	bad := &badUnwrapperPlanCtx{PlanContext: mock.NewContext()}
	if intest.EnableAssert {
		require.Panics(t, func() {
			_, _ = AsSctx(bad)
		})
	} else {
		_, err := AsSctx(bad)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unwrapped value type is struct {}")
	}
}
