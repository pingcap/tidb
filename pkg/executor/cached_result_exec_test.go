// Copyright 2025 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type mockCachedResultTable struct {
	table.Table
	putCount int
}

func (*mockCachedResultTable) Init(sqlexec.SQLExecutor) error {
	return nil
}

func (*mockCachedResultTable) TryReadFromCache(uint64, time.Duration) (kv.MemBuffer, bool) {
	return nil, false
}

func (*mockCachedResultTable) UpdateLockForRead(context.Context, kv.Storage, uint64, time.Duration) {
}

func (*mockCachedResultTable) WriteLockAndKeepAlive(context.Context, chan struct{}, *uint64, chan error) {
}

func (*mockCachedResultTable) GetCachedResult(table.ResultCacheKey, []byte) ([]*chunk.Chunk, []*types.FieldType, bool) {
	return nil, nil, false
}

func (t *mockCachedResultTable) PutCachedResult(table.ResultCacheKey, []byte, []*chunk.Chunk, []*types.FieldType) bool {
	t.putCount++
	return true
}

type mockResultSourceExec struct {
	exec.BaseExecutor
	rows     [][]types.Datum
	cursor   int
	closeErr error
}

func (e *mockResultSourceExec) Open(context.Context) error {
	e.cursor = 0
	return nil
}

func (e *mockResultSourceExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.rows) {
		return nil
	}
	for i := range e.rows[e.cursor] {
		req.AppendDatum(i, &e.rows[e.cursor][i])
	}
	e.cursor++
	return nil
}

func (e *mockResultSourceExec) Close() error {
	return e.closeErr
}

func TestCachedResultExecCloseErrorDoesNotBackfill(t *testing.T) {
	sctx := mock.NewContext()
	ft := types.NewFieldType(mysql.TypeLonglong)
	col := &expression.Column{UniqueID: 1, Index: 0, RetType: ft}
	schema := expression.NewSchema(col)

	tblInfo := &model.TableInfo{ID: 1, Columns: []*model.ColumnInfo{{ID: 1, Offset: 0, FieldType: *ft}}}
	cachedTbl := &mockCachedResultTable{Table: tables.MockTableFromMeta(tblInfo)}

	original := &mockResultSourceExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, schema, 1),
		rows:         [][]types.Datum{{types.NewIntDatum(7)}},
		closeErr:     errors.New("close failed"),
	}

	wrapped := &CachedResultExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, schema, 2, original),
		original:     original,
		cachedTable:  cachedTbl,
		cacheKey:     table.ResultCacheKey{ParamHash: 1},
	}

	require.NoError(t, wrapped.Open(context.Background()))

	req := wrapped.NewChunk()
	require.NoError(t, wrapped.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, int64(7), req.GetRow(0).GetInt64(0))

	require.NoError(t, wrapped.Next(context.Background(), req))
	require.Zero(t, req.NumRows())

	err := wrapped.Close()
	require.EqualError(t, err, "close failed")
	require.Zero(t, cachedTbl.putCount)
	require.False(t, sctx.GetSessionVars().StmtCtx.ReadFromResultCache)
}
