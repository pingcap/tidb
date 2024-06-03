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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/stretchr/testify/require"
)

func TestStmtSummaryRetriverV2_TableStatementsSummary(t *testing.T) {
	data := infoschema.NewData()
	infoSchemaBuilder, err := infoschema.NewBuilder(nil, nil, data).InitWithDBInfos(nil, nil, nil, 0)
	require.NoError(t, err)
	infoSchema := infoSchemaBuilder.Build(math.MaxUint64)
	table, err := infoSchema.TableByName(util.InformationSchemaName, model.NewCIStr(infoschema.TableStatementsSummary))
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
	infoSchemaBuilder, err := infoschema.NewBuilder(nil, nil, data).InitWithDBInfos(nil, nil, nil, 0)
	require.NoError(t, err)
	infoSchema := infoSchemaBuilder.Build(math.MaxUint64)
	table, err := infoSchema.TableByName(util.InformationSchemaName, model.NewCIStr(infoschema.TableStatementsSummaryEvicted))
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
	infoSchemaBuilder, err := infoschema.NewBuilder(nil, nil, data).InitWithDBInfos(nil, nil, nil, 0)
	require.NoError(t, err)
	infoSchema := infoSchemaBuilder.Build(math.MaxUint64)
	table, err := infoSchema.TableByName(util.InformationSchemaName, model.NewCIStr(infoschema.TableStatementsSummaryHistory))
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
