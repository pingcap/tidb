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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/mock"
)

var (
	_ Executor = &mockErrorOperator{}
)

type mockErrorOperator struct {
	baseExecutor
	toPanic bool
	closed  bool
}

func (e *mockErrorOperator) Open(ctx context.Context) error {
	return nil
}

func (e *mockErrorOperator) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.toPanic {
		panic("next panic")
	} else {
		return errors.New("next error")
	}
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
	baseExec := newBaseExecutor(ctx, schema, nil)
	explainExec := &ExplainExec{
		baseExecutor: baseExec,
		explain:      nil,
	}
	// mockErrorOperator returns errors
	mockOpr := mockErrorOperator{baseExec, false, false}
	explainExec.analyzeExec = &mockOpr
	tmpCtx := context.Background()
	_, err := explainExec.generateExplainInfo(tmpCtx)

	expectedStr := "next error, close error"
	if err != nil && (err.Error() != expectedStr || !mockOpr.closed) {
		t.Errorf(err.Error())
	}
	// mockErrorOperator panic
	mockOpr = mockErrorOperator{baseExec, true, false}
	explainExec.analyzeExec = &mockOpr
	defer func() {
		if panicErr := recover(); panicErr == nil || !mockOpr.closed {
			t.Errorf("panic test failed: without panic or close() is not called")
		}
	}()

	_, err = explainExec.generateExplainInfo(tmpCtx)
	if err != nil {
		t.Errorf(err.Error())
	}
}
