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

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
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
}
