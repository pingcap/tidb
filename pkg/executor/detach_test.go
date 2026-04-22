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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockSimpleExecutor struct {
	exec.BaseExecutorV2
}

//nolint:constructor
func TestDetachExecutor(t *testing.T) {
	// call `Detach` on a mock executor will fail
	_, ok := Detach(&mockSimpleExecutor{})
	require.False(t, ok)

	// call `Detach` on a TableReaderExecutor will succeed
	oldExec := &TableReaderExecutor{
		tableReaderExecutorContext: tableReaderExecutorContext{
			ectx: exprstatic.NewExprContext(),
		},
	}
	newExec, ok := Detach(oldExec)
	require.True(t, ok)
	require.NotSame(t, oldExec, newExec)

	// call `Detach` on a `TableReaderExecutor` with `mockSimpleExecutor` as child will fail
	sess := mock.NewContext()
	oldExec = &TableReaderExecutor{
		tableReaderExecutorContext: tableReaderExecutorContext{
			ectx: exprstatic.NewExprContext(),
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sess.GetSessionVars(), nil, 0, &mockSimpleExecutor{}),
	}
	_, ok = Detach(oldExec)
	require.False(t, ok)

	// call `Detach` on a `TableReaderExecutor` with another `TableReaderExecutor` as child will succeed
	child := &TableReaderExecutor{
		tableReaderExecutorContext: tableReaderExecutorContext{
			ectx: exprstatic.NewExprContext(),
		},
	}
	parent := &TableReaderExecutor{
		tableReaderExecutorContext: tableReaderExecutorContext{
			ectx: exprstatic.NewExprContext(),
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sess.GetSessionVars(), nil, 0, child),
	}
	newExec, ok = Detach(parent)
	require.True(t, ok)
	require.NotSame(t, parent, newExec)
	require.NotSame(t, child, newExec.AllChildren()[0])

	// Force WithTruncateErrLevel to actually build a wrapper before switching
	// back to LevelError, so the detach path exercises wrapped ExprCtx/EvalCtx.
	scopedSess := sessionctx.WithTruncateErrLevel(
		sessionctx.WithTruncateErrLevel(sess, errctx.LevelWarn),
		errctx.LevelError,
	)
	treCtx := newTableReaderExecutorContext(scopedSess)
	detachedTRECtx := treCtx.Detach()
	require.IsType(t, &exprstatic.ExprContext{}, detachedTRECtx.ectx)
	detachedTRErrCtx := detachedTRECtx.ectx.GetEvalCtx().ErrCtx()
	require.Equal(t, errctx.LevelError,
		detachedTRErrCtx.LevelForGroup(errctx.ErrGroupTruncate))

	iluCtx := newIndexLookUpExecutorContext(scopedSess)
	detachedILUCtx := iluCtx.Detach()
	require.IsType(t, &exprstatic.ExprContext{}, detachedILUCtx.ectx)
	detachedILUErrCtx := detachedILUCtx.ectx.GetEvalCtx().ErrCtx()
	require.Equal(t, errctx.LevelError,
		detachedILUErrCtx.LevelForGroup(errctx.ErrGroupTruncate))

	projCtx := newProjectionExecutorContext(scopedSess)
	detachedProjCtx := projCtx.Detach()
	require.IsType(t, &exprstatic.EvalContext{}, detachedProjCtx.evalCtx)
	detachedProjErrCtx := detachedProjCtx.evalCtx.ErrCtx()
	require.Equal(t, errctx.LevelError,
		detachedProjErrCtx.LevelForGroup(errctx.ErrGroupTruncate))

	selCtx := selectionExecutorContext{evalCtx: scopedSess.GetExprCtx().GetEvalCtx()}
	detachedSelCtx := selCtx.Detach()
	require.IsType(t, &exprstatic.EvalContext{}, detachedSelCtx.evalCtx)
	detachedSelErrCtx := detachedSelCtx.evalCtx.ErrCtx()
	require.Equal(t, errctx.LevelError,
		detachedSelErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
}
