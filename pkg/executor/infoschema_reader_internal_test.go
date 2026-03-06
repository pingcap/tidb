// Copyright 2023 PingCAP, Inc.
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
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/modelruntime"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/yalue/onnxruntime_go"
)

func TestSetDataFromCheckConstraints(t *testing.T) {
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  ast.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: ast.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t2_c1"),
					Table:      ast.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: ast.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t3_c1"),
					Table:      ast.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt := memtableRetriever{is: mockIs, extractor: &plannercore.InfoSchemaCheckConstraintsExtractor{}}
	sctx := defaultCtx()
	err := mt.setDataFromCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 4, len(mt.rows[0])) // 4 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
}

func TestSetDataFromTiDBCheckConstraints(t *testing.T) {
	mt := memtableRetriever{}
	sctx := defaultCtx()
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  ast.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: ast.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t2_c1"),
					Table:      ast.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: ast.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t3_c1"),
					Table:      ast.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt.is = mockIs
	mt.extractor = &plannercore.InfoSchemaTiDBCheckConstraintsExtractor{}
	err := mt.setDataFromTiDBCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 6, len(mt.rows[0])) // 6 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
	require.Equal(t, types.NewStringDatum("t2"), mt.rows[0][4])
	require.Equal(t, types.NewIntDatum(2), mt.rows[0][5])
}

func TestSetDataFromKeywords(t *testing.T) {
	mt := memtableRetriever{}
	err := mt.setDataFromKeywords()
	require.NoError(t, err)
	require.Equal(t, types.NewStringDatum("ADD"), mt.rows[0][0]) // Keyword: ADD
	require.Equal(t, types.NewIntDatum(1), mt.rows[0][1])        // Reserved: true(1)
}

func TestSetDataForModelVersions(t *testing.T) {
	inputSchema, err := types.ParseBinaryJSONFromString(`{"input":"x"}`)
	require.NoError(t, err)
	outputSchema, err := types.ParseBinaryJSONFromString(`{"output":"y"}`)
	require.NoError(t, err)
	options, err := types.ParseBinaryJSONFromString(`{"batch":1}`)
	require.NoError(t, err)

	createdAt := types.NewTime(types.FromGoTime(time.Date(2026, 2, 21, 1, 2, 3, 0, time.UTC)), mysql.TypeDatetime, types.DefaultFsp)
	deletedAt := types.NewTime(types.FromGoTime(time.Date(2026, 2, 21, 4, 5, 6, 0, time.UTC)), mysql.TypeDatetime, types.DefaultFsp)
	rows := []chunk.Row{
		chunk.MutRowFromDatums(types.MakeDatums(
			"test",
			"model_a",
			int64(10),
			int64(2),
			"onnx",
			"local://model_a.onnx",
			"checksum-a",
			inputSchema,
			outputSchema,
			options,
			"public",
			createdAt,
			deletedAt,
		)).ToRow(),
		chunk.MutRowFromDatums(types.MakeDatums(
			"test",
			"model_b",
			int64(11),
			int64(1),
			"onnx",
			"local://model_b.onnx",
			"checksum-b",
			inputSchema,
			outputSchema,
			nil,
			"public",
			createdAt,
			nil,
		)).ToRow(),
	}

	sctx := newRestrictedSQLContext(rows)
	mt := memtableRetriever{table: &model.TableInfo{Name: ast.NewCIStr("TIDB_MODEL_VERSIONS")}}
	data, err := mt.retrieve(context.Background(), sctx)
	require.NoError(t, err)
	require.Len(t, data, 2)
	require.Contains(t, sctx.lastSQL, "mysql.tidb_model_version")

	row := data[0]
	require.Equal(t, "test", row[0].GetString())
	require.Equal(t, "model_a", row[1].GetString())
	require.Equal(t, int64(10), row[2].GetInt64())
	require.Equal(t, int64(2), row[3].GetInt64())
	require.Equal(t, "onnx", row[4].GetString())
	require.Equal(t, "local://model_a.onnx", row[5].GetString())
	require.Equal(t, "checksum-a", row[6].GetString())
	require.Equal(t, inputSchema, row[7].GetMysqlJSON())
	require.Equal(t, outputSchema, row[8].GetMysqlJSON())
	require.Equal(t, options, row[9].GetMysqlJSON())
	require.Equal(t, "public", row[10].GetString())
	require.Equal(t, createdAt, row[11].GetMysqlTime())
	require.Equal(t, deletedAt, row[12].GetMysqlTime())

	row = data[1]
	require.Equal(t, "model_b", row[1].GetString())
	require.True(t, row[9].IsNull())
	require.True(t, row[12].IsNull())
}

func TestSetDataForModelCache(t *testing.T) {
	now := time.Date(2026, 2, 21, 10, 20, 30, 0, time.UTC)
	cache := modelruntime.NewSessionCache(modelruntime.SessionCacheOptions{
		Capacity: 2,
		TTL:      30 * time.Second,
		Now: func() time.Time {
			return now
		},
	})
	originalCache := modelruntime.GetProcessSessionCache()
	modelruntime.SetProcessSessionCache(cache)
	defer modelruntime.SetProcessSessionCache(originalCache)

	key := modelruntime.SessionKeyFromParts(42, 3, []string{"input_a", "input_b"}, []string{"output"})
	addSessionToCache(t, cache, key)

	mt := memtableRetriever{table: &model.TableInfo{Name: ast.NewCIStr("TIDB_MODEL_CACHE")}}
	data, err := mt.retrieve(context.Background(), defaultCtx())
	require.NoError(t, err)
	require.Len(t, data, 1)

	row := data[0]
	require.Equal(t, int64(42), row[0].GetInt64())
	require.Equal(t, int64(3), row[1].GetInt64())
	require.Equal(t, "input_a,input_b", row[2].GetString())
	require.Equal(t, "output", row[3].GetString())
	expectedCachedAt := types.NewTime(types.FromGoTime(now), mysql.TypeDatetime, types.DefaultFsp)
	require.Equal(t, expectedCachedAt, row[4].GetMysqlTime())
	require.Equal(t, int64(30), row[5].GetInt64())
	expectedExpiresAt := types.NewTime(types.FromGoTime(now.Add(30*time.Second)), mysql.TypeDatetime, types.DefaultFsp)
	require.Equal(t, expectedExpiresAt, row[6].GetMysqlTime())
}

type restrictedSQLContext struct {
	*mock.Context
	rows    []chunk.Row
	lastSQL string
}

func newRestrictedSQLContext(rows []chunk.Row) *restrictedSQLContext {
	ctx := &restrictedSQLContext{
		Context: mock.NewContext(),
		rows:    rows,
	}
	ctx.BindDomainAndSchValidator(domain.NewMockDomain(), nil)
	return ctx
}

func (c *restrictedSQLContext) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, sql string, _ ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	c.lastSQL = sql
	return c.rows, nil, nil
}

func (c *restrictedSQLContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return c
}

type stubDynamicSession struct{}

func (s *stubDynamicSession) Run([]onnxruntime_go.Value, []onnxruntime_go.Value) error {
	return nil
}

func (s *stubDynamicSession) RunWithOptions([]onnxruntime_go.Value, []onnxruntime_go.Value, *onnxruntime_go.RunOptions) error {
	return nil
}

func (s *stubDynamicSession) Destroy() error {
	return nil
}

func addSessionToCache(t *testing.T, cache *modelruntime.SessionCache, key modelruntime.SessionKey) {
	t.Helper()
	method := reflect.ValueOf(cache).MethodByName("GetOrCreate")
	require.True(t, method.IsValid())
	createType := method.Type().In(1)
	createFunc := reflect.MakeFunc(createType, func(_ []reflect.Value) []reflect.Value {
		session := &stubDynamicSession{}
		return []reflect.Value{
			reflect.ValueOf(session).Convert(createType.Out(0)),
			reflect.Zero(createType.Out(1)),
		}
	})
	results := method.Call([]reflect.Value{reflect.ValueOf(key), createFunc})
	require.Len(t, results, 2)
	require.True(t, results[1].IsNil())
}
