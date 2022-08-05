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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestJoinExec(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testRowContainerSpill"))
	}()
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
	}
	casTest := defaultHashJoinTestCase(colTypes, 0, false)

	runTest := func() {
		opt1 := mockDataSourceParameters{
			rows: casTest.rows,
			ctx:  casTest.ctx,
			genDataFunc: func(row int, typ *types.FieldType) interface{} {
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
		opt1.schema = expression.NewSchema(casTest.columns()...)
		opt2.schema = expression.NewSchema(casTest.columns()...)
		dataSource1 := buildMockDataSource(opt1)
		dataSource2 := buildMockDataSource(opt2)
		dataSource1.prepareChunks()
		dataSource2.prepareChunks()

		exec := prepare4HashJoin(casTest, dataSource1, dataSource2)
		result := newFirstChunk(exec)
		{
			ctx := context.Background()
			chk := newFirstChunk(exec)
			err := exec.Open(ctx)
			require.NoError(t, err)
			for {
				err = exec.Next(ctx, chk)
				require.NoError(t, err)
				if chk.NumRows() == 0 {
					break
				}
				result.Append(chk, 0, chk.NumRows())
			}
			require.Equal(t, casTest.disk, exec.rowContainer.alreadySpilledSafeForTest())
			err = exec.Close()
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

func TestHashJoinRuntimeStats(t *testing.T) {
	stats := &hashJoinRuntimeStats{
		fetchAndBuildHashTable: 2 * time.Second,
		hashStat: hashStatistic{
			probeCollision:   1,
			buildTableElapse: time.Millisecond * 100,
		},
		fetchAndProbe:    int64(5 * time.Second),
		probe:            int64(4 * time.Second),
		concurrent:       4,
		maxFetchAndProbe: int64(2 * time.Second),
	}
	require.Equal(t, "build_hash_table:{total:2s, fetch:1.9s, build:100ms}, probe:{concurrency:4, total:5s, max:2s, probe:4s, fetch:1s, probe_collision:1}", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "build_hash_table:{total:4s, fetch:3.8s, build:200ms}, probe:{concurrency:4, total:10s, max:2s, probe:8s, fetch:2s, probe_collision:2}", stats.String())
}

func TestIndexJoinRuntimeStats(t *testing.T) {
	stats := indexLookUpJoinRuntimeStats{
		concurrency: 5,
		probe:       int64(time.Second),
		innerWorker: innerWorkerRuntimeStats{
			totalTime: int64(time.Second * 5),
			task:      16,
			construct: int64(100 * time.Millisecond),
			fetch:     int64(300 * time.Millisecond),
			build:     int64(250 * time.Millisecond),
			join:      int64(150 * time.Millisecond),
		},
	}
	require.Equal(t, "inner:{total:5s, concurrency:5, task:16, construct:100ms, fetch:300ms, build:250ms, join:150ms}, probe:1s", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "inner:{total:10s, concurrency:5, task:32, construct:200ms, fetch:600ms, build:500ms, join:300ms}, probe:2s", stats.String())
}
