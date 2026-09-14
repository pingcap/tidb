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

package aggregate

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type externalAggTestSource struct {
	exec.BaseExecutor
	rows   [][]types.Datum
	cursor int
}

func (s *externalAggTestSource) Open(context.Context) error { s.cursor = 0; return nil }
func (s *externalAggTestSource) Next(_ context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for s.cursor < len(s.rows) && chk.NumRows() < 32 {
		for i, d := range s.rows[s.cursor] {
			chk.AppendDatum(i, &d)
		}
		s.cursor++
	}
	return nil
}

func externalAggTestInput(t *testing.T, mode aggregation.AggFunctionMode, rows [][]types.Datum) (
	*mock.Context, *externalAggTestSource, *expression.Schema, []*aggregation.AggFuncDesc, []expression.Expression,
) {
	sctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	inputSchema := expression.NewSchema(
		&expression.Column{Index: 0, UniqueID: 1, RetType: intType},
		&expression.Column{Index: 1, UniqueID: 2, RetType: intType},
		&expression.Column{Index: 2, UniqueID: 3, RetType: types.NewFieldType(mysql.TypeNewDecimal)},
	)
	inputRows := make([][]types.Datum, len(rows))
	for i, row := range rows {
		var decimal types.Datum
		if !row[1].IsNull() {
			decimal = types.NewDecimalDatum(types.NewDecFromInt(row[1].GetInt64()))
		}
		inputRows[i] = []types.Datum{row[0], row[1], decimal}
	}
	source := &externalAggTestSource{BaseExecutor: exec.NewBaseExecutor(sctx, inputSchema, 1), rows: inputRows}
	descs := make([]*aggregation.AggFuncDesc, 0, 5)
	output := expression.NewSchema()
	for i, name := range []string{ast.AggFuncFirstRow, ast.AggFuncCount, ast.AggFuncSum, ast.AggFuncMin, ast.AggFuncMax} {
		arg := expression.Expression(inputSchema.Columns[1])
		if name == ast.AggFuncSum {
			arg = inputSchema.Columns[2]
		}
		if name == ast.AggFuncFirstRow {
			arg = inputSchema.Columns[0]
		}
		desc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), name, []expression.Expression{arg}, false)
		require.NoError(t, err)
		desc.Mode = mode
		descs = append(descs, desc)
		output.Append(&expression.Column{Index: i, UniqueID: int64(i + 3), RetType: desc.RetTp})
	}
	return sctx, source, output, descs, []expression.Expression{inputSchema.Columns[0]}
}

func collectExternalAggRows(t *testing.T, e exec.Executor) []string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, e.Open(ctx))
	defer func() { require.NoError(t, e.Close()) }()
	var result []string
	for {
		chk := chunk.New(e.RetFieldTypes(), 32, 32)
		require.NoError(t, e.Next(ctx, chk))
		if chk.NumRows() == 0 {
			break
		}
		for i := range chk.NumRows() {
			row := chk.GetRow(i).GetDatumRow(e.RetFieldTypes())
			parts := make([]string, len(row))
			for j, d := range row {
				if d.IsNull() {
					parts[j] = "NULL"
					continue
				}
				var err error
				parts[j], err = d.ToString()
				require.NoError(t, err)
			}
			result = append(result, strings.Join(parts, ","))
		}
	}
	sort.Strings(result)
	return result
}

func TestExternalHashAgg(t *testing.T) {
	rows := make([][]types.Datum, 0, 4802)
	for repeat := range 8 {
		for group := range 600 {
			value := types.NewIntDatum(int64(repeat + 1))
			if group%13 == 0 {
				value = types.Datum{}
			}
			rows = append(rows, []types.Datum{types.NewIntDatum(int64(group)), value})
		}
	}
	rows = append(rows, []types.Datum{{}, types.NewIntDatum(3)}, []types.Datum{{}, {}})
	for _, mode := range []aggregation.AggFunctionMode{aggregation.CompleteMode, aggregation.FinalMode} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			sctx, source, schema, descs, groups := externalAggTestInput(t, mode, rows)
			baseline := &HashAggExec{
				BaseExecutor: exec.NewBaseExecutor(sctx, schema, 2, source),
				Sc:           sctx.GetSessionVars().StmtCtx, GroupByItems: groups, IsUnparallelExec: true,
			}
			for i, desc := range descs {
				baseline.PartialAggFuncs = append(baseline.PartialAggFuncs, aggfuncs.Build(sctx.GetExprCtx(), desc, i))
			}
			want := collectExternalAggRows(t, baseline)
			for _, limit := range []int64{64 << 10, 16 << 20} {
				t.Run(fmt.Sprint(limit), func(t *testing.T) {
					store := objstore.NewMemStorage()
					require.NoError(t, store.WriteFile(context.Background(), "another-task/keep", []byte("keep")))
					e, err := NewExternalHashAgg(sctx, schema, 3, source, descs, groups, store, "task-1", limit)
					require.NoError(t, err)
					got := collectExternalAggRows(t, e)
					require.Equal(t, want, got)
					require.Equal(t, limit == 64<<10, e.spilled)
					require.Zero(t, e.tracker.BytesConsumed())
					var files []string
					require.NoError(t, store.WalkDir(context.Background(), nil, func(name string, _ int64) error { files = append(files, name); return nil }))
					require.Equal(t, []string{"another-task/keep"}, files)
				})
			}
		})
	}
}

type externalAggFailStore struct {
	storeapi.Storage
	writeErr, readErr error
}

func (s *externalAggFailStore) WriteFile(ctx context.Context, name string, data []byte) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	return s.Storage.WriteFile(ctx, name, data)
}
func (s *externalAggFailStore) ReadFile(ctx context.Context, name string) ([]byte, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	return s.Storage.ReadFile(ctx, name)
}

func TestExternalHashAggFailure(t *testing.T) {
	rows := make([][]types.Datum, 1000)
	for i := range rows {
		rows[i] = []types.Datum{types.NewIntDatum(int64(i)), types.NewIntDatum(1)}
	}
	for _, readFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(readFailure), func(t *testing.T) {
			sctx, source, schema, descs, groups := externalAggTestInput(t, aggregation.CompleteMode, rows)
			store := &externalAggFailStore{Storage: objstore.NewMemStorage()}
			injected := errors.New("injected object storage failure")
			if readFailure {
				store.readErr = injected
			} else {
				store.writeErr = injected
			}
			e, err := NewExternalHashAgg(sctx, schema, 3, source, descs, groups, store, "task-fail", 64<<10)
			require.NoError(t, err)
			require.NoError(t, e.Open(context.Background()))
			chk := chunk.New(e.RetFieldTypes(), 32, 32)
			err = e.Next(context.Background(), chk)
			require.ErrorIs(t, err, injected)
			require.Same(t, err, e.Next(context.Background(), chk))
			require.NoError(t, e.Close())
		})
	}
}

func TestExternalHashAggEmptyAndCanceled(t *testing.T) {
	sctx, source, schema, descs, groups := externalAggTestInput(t, aggregation.CompleteMode, nil)
	e, err := NewExternalHashAgg(sctx, schema, 3, source, descs, groups, objstore.NewMemStorage(), "task-empty", 64<<10)
	require.NoError(t, err)
	require.Empty(t, collectExternalAggRows(t, e))
	require.NoError(t, e.Open(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, e.Next(ctx, chunk.New(e.RetFieldTypes(), 32, 32)), context.Canceled)
	require.NoError(t, e.Close())
}

func TestExternalHashAggObjectStorage(t *testing.T) {
	uri := os.Getenv("TIDB_IMPORT_QUERY_S3_URI")
	if uri == "" {
		t.Skip("set TIDB_IMPORT_QUERY_S3_URI for an S3 integration run")
	}
	backend, err := objstore.ParseBackend(uri, nil)
	require.NoError(t, err)
	store, err := objstore.NewWithDefaultOpt(context.Background(), backend)
	require.NoError(t, err)
	defer store.Close()
	rows := make([][]types.Datum, 1200)
	for i := range rows {
		rows[i] = []types.Datum{types.NewIntDatum(int64(i % 400)), types.NewIntDatum(1)}
	}
	sctx, source, schema, descs, groups := externalAggTestInput(t, aggregation.CompleteMode, rows)
	e, err := NewExternalHashAgg(sctx, schema, 3, source, descs, groups, store, "s3-final-agg-integration", 64<<10)
	require.NoError(t, err)
	got := collectExternalAggRows(t, e)
	require.Len(t, got, 400)
	require.True(t, e.spilled)
	for _, row := range got {
		parts := strings.Split(row, ",")
		require.Len(t, parts, 5)
		require.Equal(t, "3", parts[1])
		decimal := new(types.MyDecimal)
		require.NoError(t, decimal.FromString([]byte(parts[2])))
		require.Zero(t, decimal.Compare(types.NewDecFromInt(3)))
		require.Equal(t, []string{"1", "1"}, parts[3:])
	}
	var leftovers []string
	require.NoError(t, store.WalkDir(context.Background(), &storeapi.WalkOption{SubDir: e.attemptPrefix}, func(name string, _ int64) error { leftovers = append(leftovers, name); return nil }))
	require.Empty(t, leftovers)
}

type externalAggBlockingSource struct {
	exec.BaseExecutor
	started chan struct{}
}

func (s *externalAggBlockingSource) Next(ctx context.Context, _ *chunk.Chunk) error {
	close(s.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestExternalHashAggCloseDuringNext(t *testing.T) {
	sctx, source, schema, descs, groups := externalAggTestInput(t, aggregation.CompleteMode, nil)
	blocking := &externalAggBlockingSource{BaseExecutor: source.BaseExecutor, started: make(chan struct{})}
	e, err := NewExternalHashAgg(sctx, schema, 3, blocking, descs, groups, objstore.NewMemStorage(), "cancel-query", 64<<10)
	require.NoError(t, err)
	require.NoError(t, e.Open(context.Background()))
	done := make(chan error, 1)
	go func() { done <- e.Next(context.Background(), chunk.New(e.RetFieldTypes(), 32, 32)) }()
	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close failed to cancel reader")
	}
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestExternalHashAggCollationSpill(t *testing.T) {
	old := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(old)
	sctx := mock.NewContext()
	strType := types.NewFieldType(mysql.TypeVarchar)
	strType.SetCharset("utf8mb4")
	strType.SetCollate("utf8mb4_general_ci")
	intType := types.NewFieldType(mysql.TypeLonglong)
	input := expression.NewSchema(&expression.Column{Index: 0, UniqueID: 1, RetType: strType}, &expression.Column{Index: 1, UniqueID: 2, RetType: intType})
	var rows [][]types.Datum
	for repeat := range 4 {
		for i := range 700 {
			key := fmt.Sprintf("GROUP-%04d", i)
			if repeat%2 == 1 {
				key = strings.ToLower(key)
			}
			rows = append(rows, []types.Datum{types.NewStringDatum(key), types.NewIntDatum(1)})
		}
	}
	source := &externalAggTestSource{BaseExecutor: exec.NewBaseExecutor(sctx, input, 1), rows: rows}
	first, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{input.Columns[0]}, false)
	require.NoError(t, err)
	count, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{input.Columns[1]}, false)
	require.NoError(t, err)
	output := expression.NewSchema(&expression.Column{Index: 0, UniqueID: 3, RetType: strType}, &expression.Column{Index: 1, UniqueID: 4, RetType: count.RetTp})
	e, err := NewExternalHashAgg(sctx, output, 3, source, []*aggregation.AggFuncDesc{first, count}, []expression.Expression{input.Columns[0]}, objstore.NewMemStorage(), "collation-query", 64<<10)
	require.NoError(t, err)
	result := collectExternalAggRows(t, e)
	require.True(t, e.spilled)
	require.Len(t, result, 700)
	for _, row := range result {
		require.True(t, strings.HasSuffix(row, ",4"), row)
	}
}
