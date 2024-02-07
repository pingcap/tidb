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

package cardinality_test

import (
	"math"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAvgColLen(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 varchar(100), c3 float, c4 datetime, c5 varchar(100))")
	testKit.MustExec("insert into t values(1, '1234567', 12.3, '2018-03-07 19:00:57', NULL)")
	testKit.MustExec("analyze table t")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.Equal(t, 1.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[0].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 8.0, cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[0].ID], statsTbl.RealtimeCount))
	require.Equal(t, 8.0, cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[0].ID], statsTbl.RealtimeCount))

	// The size of varchar type is LEN + BYTE, here is 1 + 7 = 8
	require.Equal(t, 8.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[1].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 8.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[2].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 8.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[3].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 8.0-3, cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[1].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(float32(12.3))), cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[2].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(types.ZeroTime)), cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[3].ID], statsTbl.RealtimeCount))
	require.Equal(t, 8.0-3+8, cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[1].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(float32(12.3))), cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[2].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(types.ZeroTime)), cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[3].ID], statsTbl.RealtimeCount))
	require.Equal(t, 8.0, cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[4].ID], statsTbl.RealtimeCount))
	require.Equal(t, 0.0, cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[4].ID], statsTbl.RealtimeCount))
	testKit.MustExec("insert into t values(132, '123456789112', 1232.3, '2018-03-07 19:17:29', NULL)")
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.Equal(t, 1.5, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[0].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 10.5, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[1].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 8.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[2].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 8.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[3].ID], statsTbl.RealtimeCount, false))
	require.Equal(t, 8.0, cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[0].ID], statsTbl.RealtimeCount))
	require.Equal(t, math.Round((10.5-math.Log2(10.5))*100)/100, cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[1].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(float32(12.3))), cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[2].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(types.ZeroTime)), cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[3].ID], statsTbl.RealtimeCount))
	require.Equal(t, 8.0, cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[0].ID], statsTbl.RealtimeCount))
	require.Equal(t, math.Round((10.5-math.Log2(10.5))*100)/100+8, cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[1].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(float32(12.3))), cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[2].ID], statsTbl.RealtimeCount))
	require.Equal(t, float64(unsafe.Sizeof(types.ZeroTime)), cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[3].ID], statsTbl.RealtimeCount))
	require.Equal(t, 8.0, cardinality.AvgColSizeChunkFormat(statsTbl.Columns[tableInfo.Columns[4].ID], statsTbl.RealtimeCount))
	require.Equal(t, 0.0, cardinality.AvgColSizeDataInDiskByRows(statsTbl.Columns[tableInfo.Columns[4].ID], statsTbl.RealtimeCount))
}
