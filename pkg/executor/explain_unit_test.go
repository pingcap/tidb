// Copyright 2019 PingCAP, Inc.
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
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var (
	_ exec.Executor = &mockErrorOperator{}
)

type mockErrorOperator struct {
	exec.BaseExecutor
	toPanic bool
	closed  bool
}

func (e *mockErrorOperator) Open(_ context.Context) error {
	return nil
}

func (e *mockErrorOperator) Next(_ context.Context, _ *chunk.Chunk) error {
	if e.toPanic {
		panic("next panic")
	}
	return errors.New("next error")
}

func (e *mockErrorOperator) Close() error {
	e.closed = true
	return errors.New("close error")
}

func getColumns() []*expression.Column {
	return []*expression.Column{
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

// close() must be called after next() to avoid goroutines leak
func TestExplainAnalyzeInvokeNextAndClose(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	schema := expression.NewSchema(getColumns()...)
	baseExec := exec.NewBaseExecutor(ctx, schema, 0)
	explainExec := &ExplainExec{
		BaseExecutor: baseExec,
		explain:      &core.Explain{},
	}
	// mockErrorOperator returns errors
	mockOpr := mockErrorOperator{baseExec, false, false}
	explainExec.analyzeExec = &mockOpr
	explainExec.explain.Analyze = true
	tmpCtx := context.Background()
	_, err := explainExec.generateExplainInfo(tmpCtx)
	require.EqualError(t, err, "next error, close error")
	require.True(t, mockOpr.closed)

	// mockErrorOperator panic
	explainExec = &ExplainExec{
		BaseExecutor: baseExec,
		explain:      &core.Explain{},
	}
	mockOpr = mockErrorOperator{baseExec, true, false}
	explainExec.analyzeExec = &mockOpr
	explainExec.explain.Analyze = true
	_, err = explainExec.generateExplainInfo(tmpCtx)
	require.EqualError(t, err, "next panic, close error")
	require.True(t, mockOpr.closed)
}
