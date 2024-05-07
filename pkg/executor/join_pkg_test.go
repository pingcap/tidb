// Copyright 2020 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestJoinExec(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/testRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/testRowContainerSpill"))
	}()
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
	}
	casTest := defaultHashJoinTestCase(colTypes, 0, false)

	runTest := func() {
		opt1 := testutil.MockDataSourceParameters{
			Rows: casTest.rows,
			Ctx:  casTest.ctx,
			GenDataFunc: func(row int, typ *types.FieldType) any {
				switch typ.GetType() {
				case mysql.TypeLong, mysql.TypeLonglong:
					return int64(row)
				case mysql.TypeDouble:
					return float64(row)
				default:
					panic("not implement")
				}
			},
		}
		opt2 := opt1
		opt1.DataSchema = expression.NewSchema(casTest.columns()...)
		opt2.DataSchema = expression.NewSchema(casTest.columns()...)
		dataSource1 := testutil.BuildMockDataSource(opt1)
		dataSource2 := testutil.BuildMockDataSource(opt2)
		dataSource1.PrepareChunks()
		dataSource2.PrepareChunks()

		executor := prepare4HashJoin(casTest, dataSource1, dataSource2)
		result := exec.NewFirstChunk(executor)
		{
			ctx := context.Background()
			chk := exec.NewFirstChunk(executor)
			err := executor.Open(ctx)
			require.NoError(t, err)
			for {
				err = executor.Next(ctx, chk)
				require.NoError(t, err)
				if chk.NumRows() == 0 {
					break
				}
				result.Append(chk, 0, chk.NumRows())
			}
			require.Equal(t, casTest.disk, executor.RowContainer.AlreadySpilledSafeForTest())
			err = executor.Close()
			require.NoError(t, err)
		}

		require.Equal(t, 4, result.NumCols())
		require.Equal(t, casTest.rows, result.NumRows())
		visit := make(map[int64]bool, casTest.rows)
		for i := 0; i < casTest.rows; i++ {
			val := result.Column(0).Int64s()[i]
			require.Equal(t, float64(val), result.Column(1).Float64s()[i])
			require.Equal(t, val, result.Column(2).Int64s()[i])
			require.Equal(t, float64(val), result.Column(3).Float64s()[i])
			visit[val] = true
		}
		for i := 0; i < casTest.rows; i++ {
			require.True(t, visit[int64(i)])
		}
	}

	concurrency := []int{1, 4}
	rows := []int{3, 1024, 4096}
	disk := []bool{false, true}
	for _, concurrency := range concurrency {
		for _, rows := range rows {
			for _, disk := range disk {
				casTest.concurrency = concurrency
				casTest.rows = rows
				casTest.disk = disk
				runTest()
			}
		}
	}
}
