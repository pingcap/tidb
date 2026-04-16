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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestSetDataFromCheckConstraints(t *testing.T) {
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  pmodel.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: pmodel.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t2_c1"),
					Table:      pmodel.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: pmodel.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t3_c1"),
					Table:      pmodel.NewCIStr("t3"),
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
			Name:  pmodel.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: pmodel.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t2_c1"),
					Table:      pmodel.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: pmodel.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t3_c1"),
					Table:      pmodel.NewCIStr("t3"),
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

func TestParseRoutineDefinitionUsesStoredCharsetAndCollation(t *testing.T) {
	origParseOneStmt := parseOneStmtForRoutine
	defer func() {
		parseOneStmtForRoutine = origParseOneStmt
	}()

	var gotSQL string
	var gotCharset string
	var gotCollation string
	parseOneStmtForRoutine = func(p *parser.Parser, sql, charset, collation string) (ast.StmtNode, error) {
		gotSQL = sql
		gotCharset = charset
		gotCollation = collation
		return p.ParseOneStmt(sql, charset, collation)
	}

	routine := &model.ProcedureInfo{
		Type:                "PROCEDURE",
		ParameterStr:        "IN p_label VARCHAR(10)",
		DefinitionUTF8:      "BEGIN SELECT 1; END",
		CharacterSetClient:  "latin1",
		CollationConnection: "latin1_bin",
	}

	_, err := parseRoutineDefinition(routine)
	require.NoError(t, err)
	require.Equal(t, "latin1", gotCharset)
	require.Equal(t, "latin1_bin", gotCollation)
	require.Contains(t, gotSQL, "create procedure p(")
	require.Contains(t, gotSQL, "IN p_label VARCHAR(10)")
}

type mockRoutineRestrictedSQLContext struct {
	sessionctx.Context
	rows     []chunk.Row
	lastSQL  string
	lastArgs []any
}

func (c *mockRoutineRestrictedSQLContext) ParseWithParams(context.Context, string, ...any) (ast.StmtNode, error) {
	return nil, nil
}

func (c *mockRoutineRestrictedSQLContext) ExecRestrictedStmt(context.Context, ast.StmtNode, ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*resolve.ResultField, error) {
	return nil, nil, nil
}

func (c *mockRoutineRestrictedSQLContext) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, sql string, args ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	c.lastSQL = sql
	c.lastArgs = args

	schemas := getRoutineQueryFilterArg(args, 0)
	names := getRoutineQueryFilterArg(args, 1)
	if len(schemas) == 0 && len(names) == 0 {
		return c.rows, nil, nil
	}

	filtered := make([]chunk.Row, 0, len(c.rows))
	for _, row := range c.rows {
		if len(schemas) > 0 && !schemas.Exist(strings.ToLower(row.GetString(0))) {
			continue
		}
		if len(names) > 0 && !names.Exist(strings.ToLower(row.GetString(1))) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered, nil, nil
}

func getRoutineQueryFilterArg(args []any, idx int) set.StringSet {
	if idx >= len(args) {
		return nil
	}
	switch v := args[idx].(type) {
	case []string:
		ret := set.NewStringSet()
		for _, s := range v {
			ret.Insert(strings.ToLower(s))
		}
		return ret
	case []any:
		ret := set.NewStringSet()
		for _, item := range v {
			if s, ok := item.(string); ok {
				ret.Insert(strings.ToLower(s))
			}
		}
		return ret
	}
	return nil
}

func makeRoutineChunkRow(schema, name, routineType string) chunk.Row {
	enumValue := uint64(1)
	if strings.EqualFold(routineType, "PROCEDURE") {
		enumValue = 2
	}
	datums := []types.Datum{
		types.NewStringDatum(schema),
		types.NewStringDatum(name),
		types.NewMysqlEnumDatum(types.Enum{Name: routineType, Value: enumValue}),
		types.NewStringDatum("BEGIN SELECT 1; END"),
		types.NewStringDatum("IN p_id INT"),
		types.NewIntDatum(0),
		types.NewMysqlEnumDatum(types.Enum{Name: "READS SQL DATA", Value: 3}),
		types.NewMysqlEnumDatum(types.Enum{Name: "DEFINER", Value: 3}),
		types.NewStringDatum("root@%"),
		types.NewMysqlSetDatum(types.Set{Name: "ONLY_FULL_GROUP_BY", Value: 32}, ""),
		types.NewStringDatum("utf8mb4"),
		types.NewStringDatum("utf8mb4_bin"),
		types.NewStringDatum("utf8mb4_bin"),
		types.NewTimeDatum(types.ZeroTimestamp),
		types.NewTimeDatum(types.ZeroTimestamp),
		types.Datum{},
		types.Datum{},
		types.Datum{},
	}
	return chunk.MutRowFromDatums(datums).ToRow()
}

func TestSetDataForParametersPrunesRoutineParsingBySpecificSchemaAndName(t *testing.T) {
	extractor := plannercore.NewInfoSchemaParametersExtractor()
	extractor.ColPredicates = map[string]set.StringSet{
		"specific_schema": set.NewStringSet("test"),
		"specific_name":   set.NewStringSet("proc_target"),
	}
	sctx := &mockRoutineRestrictedSQLContext{
		Context: defaultCtx(),
		rows: []chunk.Row{
			makeRoutineChunkRow("test", "proc_target", "PROCEDURE"),
			makeRoutineChunkRow("test", "proc_other", "PROCEDURE"),
			makeRoutineChunkRow("test", "func_other", "FUNCTION"),
			makeRoutineChunkRow("test2", "proc_cross_schema", "PROCEDURE"),
		},
	}
	mt := memtableRetriever{extractor: extractor}

	origParseOneStmt := parseOneStmtForRoutine
	defer func() {
		parseOneStmtForRoutine = origParseOneStmt
	}()

	parseCount := 0
	parseOneStmtForRoutine = func(p *parser.Parser, sql, charset, collation string) (ast.StmtNode, error) {
		parseCount++
		return origParseOneStmt(p, sql, charset, collation)
	}

	err := mt.setDataForParameters(context.Background(), sctx)
	require.NoError(t, err)
	require.Contains(t, sctx.lastSQL, "where lower(route_schema) in (%?) and lower(name) in (%?)")
	require.Len(t, sctx.lastArgs, 2)
	require.Equal(t, 1, parseCount)
	require.Len(t, mt.rows, 1)
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("proc_target"), mt.rows[0][2])
}

func TestSetDataFromKeywords(t *testing.T) {
	mt := memtableRetriever{}
	err := mt.setDataFromKeywords()
	require.NoError(t, err)
	require.Equal(t, types.NewStringDatum("ADD"), mt.rows[0][0]) // Keyword: ADD
	require.Equal(t, types.NewIntDatum(1), mt.rows[0][1])        // Reserved: true(1)
}
