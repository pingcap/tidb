// Copyright 2022 PingCAP, Inc.
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
	"math"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/stretchr/testify/require"
)

func TestStmtSummaryRetriverV2_TableStatementsSummary(t *testing.T) {
	data := infoschema.NewData()
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	infoSchemaBuilder := infoschema.NewBuilder(nil, schemaCacheSize, nil, data, schemaCacheSize > 0)
	err := infoSchemaBuilder.InitWithDBInfos(nil, nil, nil, nil, 0)
	require.NoError(t, err)
	infoSchema := infoSchemaBuilder.Build(math.MaxUint64)
	table, err := infoSchema.TableByName(context.Background(), metadef.InformationSchemaName, ast.NewCIStr(infoschema.TableStatementsSummary))
	require.NoError(t, err)
	columns := table.Meta().Columns

	stmtSummary := stmtsummaryv2.NewStmtSummary4Test(1000)
	defer stmtSummary.Close()
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest1"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest1"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest2"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest2"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest3"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest3"))

	retriever := stmtSummaryRetrieverV2{
		stmtSummary: stmtSummary,
		table:       table.Meta(),
		columns:     columns,
	}
	defer func() {
		require.NoError(t, retriever.close())
	}()

	ctx := context.Background()
	sctx := mock.NewContext()
	tz, _ := time.LoadLocation("Asia/Shanghai")
	sctx.ResetSessionAndStmtTimeZone(tz)

	var results [][]types.Datum
	for {
		rows, err := retriever.retrieve(ctx, sctx)
		require.NoError(t, err)
		if len(rows) == 0 {
			break
		}
		results = append(results, rows...)
	}
	require.Len(t, results, 3)
}

func TestStmtSummaryRetriverV2_TableStatementsSummaryEvicted(t *testing.T) {
	data := infoschema.NewData()
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	infoSchemaBuilder := infoschema.NewBuilder(nil, schemaCacheSize, nil, data, schemaCacheSize > 0)
	err := infoSchemaBuilder.InitWithDBInfos(nil, nil, nil, nil, 0)
	require.NoError(t, err)
	infoSchema := infoSchemaBuilder.Build(math.MaxUint64)
	table, err := infoSchema.TableByName(context.Background(), metadef.InformationSchemaName, ast.NewCIStr(infoschema.TableStatementsSummaryEvicted))
	require.NoError(t, err)
	columns := table.Meta().Columns

	stmtSummary := stmtsummaryv2.NewStmtSummary4Test(1)
	defer stmtSummary.Close()
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest1"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest1"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest2"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest2"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest3"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest3"))

	retriever := stmtSummaryRetrieverV2{
		stmtSummary: stmtSummary,
		table:       table.Meta(),
		columns:     columns,
	}
	defer func() {
		require.NoError(t, retriever.close())
	}()

	ctx := context.Background()
	sctx := mock.NewContext()
	tz, _ := time.LoadLocation("Asia/Shanghai")
	sctx.ResetSessionAndStmtTimeZone(tz)

	var results [][]types.Datum
	for {
		rows, err := retriever.retrieve(ctx, sctx)
		require.NoError(t, err)
		if len(rows) == 0 {
			break
		}
		results = append(results, rows...)
	}
	require.Len(t, results, 1)
	require.Equal(t, int64(2), results[0][2].GetInt64())
}

func TestStmtSummaryRetriverV2_TableStatementsSummaryHistory(t *testing.T) {
	filename1 := "tidb-statements-2022-12-27T16-21-20.245.log"
	filename2 := "tidb-statements.log"

	file, err := os.Create(filename1)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename1))
	}()
	_, err = file.WriteString("{\"begin\":1672128520,\"end\":1672128530,\"digest\":\"digest1\",\"exec_count\":1}\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest2\",\"exec_count\":2}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	file, err = os.Create(filename2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename2))
	}()
	_, err = file.WriteString("{\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest3\",\"exec_count\":3}\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":1672129380,\"end\":1672129390,\"digest\":\"digest4\",\"exec_count\":4}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	stmtSummary := stmtsummaryv2.NewStmtSummary4Test(2)
	defer stmtSummary.Close()
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest1"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest1"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest2"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest2"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest3"))
	stmtSummary.Add(stmtsummaryv2.GenerateStmtExecInfo4Test("digest3"))

	data := infoschema.NewData()
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	infoSchemaBuilder := infoschema.NewBuilder(nil, schemaCacheSize, nil, data, schemaCacheSize > 0)
	err = infoSchemaBuilder.InitWithDBInfos(nil, nil, nil, nil, 0)
	require.NoError(t, err)
	infoSchema := infoSchemaBuilder.Build(math.MaxUint64)
	table, err := infoSchema.TableByName(context.Background(), metadef.InformationSchemaName, ast.NewCIStr(infoschema.TableStatementsSummaryHistory))
	require.NoError(t, err)
	columns := table.Meta().Columns

	retriever := stmtSummaryRetrieverV2{
		stmtSummary: stmtSummary,
		table:       table.Meta(),
		columns:     columns,
	}
	defer func() {
		require.NoError(t, retriever.close())
	}()

	ctx := context.Background()
	sctx := mock.NewContext()
	tz, _ := time.LoadLocation("Asia/Shanghai")
	sctx.ResetSessionAndStmtTimeZone(tz)

	var results [][]types.Datum
	for {
		rows, err := retriever.retrieve(ctx, sctx)
		require.NoError(t, err)
		if len(rows) == 0 {
			break
		}
		results = append(results, rows...)
	}
	require.Len(t, results, 7)
}

// TestStmtSummaryRetriverV2_TableTiDBStatementsStatsUnsupported verifies that
// querying a cumulative statement summary table (TIDB_STATEMENTS_STATS) under
// the v2 persistent path returns an explicit unsupported error instead of
// panicking inside initSummaryRowsReader. The same guard fires for the
// cluster cumulative variant (CLUSTER_TIDB_STATEMENTS_STATS), so both table
// names are exercised.
func TestStmtSummaryRetriverV2_TableTiDBStatementsStatsUnsupported(t *testing.T) {
	cumulativeTables := []string{
		infoschema.TableTiDBStatementsStats,
		infoschema.ClusterTableTiDBStatementsStats,
	}
	for _, tableName := range cumulativeTables {
		verifyCumulativeTableUnsupported(t, tableName)
	}
}

// verifyCumulativeTableUnsupported builds a v2 retriever for the given
// cumulative statement summary table and asserts the unsupported error is
// returned instead of panicking. Defined as a helper so the stmtSummary
// resource defer lives in its own scope, not inside the caller loop.
func verifyCumulativeTableUnsupported(t *testing.T, tableName string) {
	t.Helper()

	data := infoschema.NewData()
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	infoSchemaBuilder := infoschema.NewBuilder(nil, schemaCacheSize, nil, data, schemaCacheSize > 0)
	err := infoSchemaBuilder.InitWithDBInfos(nil, nil, nil, nil, 0)
	require.NoError(t, err)
	infoSchema := infoSchemaBuilder.Build(math.MaxUint64)
	table, err := infoSchema.TableByName(context.Background(), metadef.InformationSchemaName, ast.NewCIStr(tableName))
	require.NoError(t, err)
	columns := table.Meta().Columns

	stmtSummary := stmtsummaryv2.NewStmtSummary4Test(1)
	defer stmtSummary.Close()

	retriever := stmtSummaryRetrieverV2{
		stmtSummary: stmtSummary,
		table:       table.Meta(),
		columns:     columns,
	}
	require.NoError(t, retriever.close())

	ctx := context.Background()
	sctx := mock.NewContext()
	tz, _ := time.LoadLocation("Asia/Shanghai")
	sctx.ResetSessionAndStmtTimeZone(tz)

	_, err = retriever.retrieve(ctx, sctx)
	require.True(t, plannererrors.ErrNotSupportedYet.Equal(err), "table %s", tableName)
	require.ErrorContains(t, err, "cumulative statement summary table with persistent mode (v2)")
	require.Nil(t, retriever.rowsReader)
}
