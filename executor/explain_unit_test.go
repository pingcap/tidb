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

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"testing"
)

var (
	_ Executor = &mockErrorOperator{}
)

type mockErrorOperator struct {
	baseExecutor
}

func (e *mockErrorOperator) Open(ctx context.Context) error {
	return nil
}

func (e *mockErrorOperator) Next(ctx context.Context, req *chunk.Chunk) error {
	return errors.New("next error")
}

func (e *mockErrorOperator) Close() error {
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
	explainExec.analyzeExec = &mockErrorOperator{baseExec}
	tmpCtx := context.Background()
	_, err := explainExec.generateExplainInfo(tmpCtx)

	expectedStr := "next error, close error"
	if err.Error() != expectedStr {
		t.Errorf(err.Error())
	}
}
