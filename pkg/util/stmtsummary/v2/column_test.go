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

package stmtsummary

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestColumn(t *testing.T) {
	columns := []*model.ColumnInfo{
		{Name: ast.NewCIStr(ClusterTableInstanceColumnNameStr)},
		{Name: ast.NewCIStr(StmtTypeStr)},
		{Name: ast.NewCIStr(SchemaNameStr)},
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(DigestTextStr)},
		{Name: ast.NewCIStr(TableNamesStr)},
		{Name: ast.NewCIStr(IndexNamesStr)},
		{Name: ast.NewCIStr(SampleUserStr)},
		{Name: ast.NewCIStr(ExecCountStr)},
		{Name: ast.NewCIStr(SumLatencyStr)},
		{Name: ast.NewCIStr(MaxLatencyStr)},
		{Name: ast.NewCIStr(AvgRocksdbDeleteSkippedCountStr)},
		{Name: ast.NewCIStr(AvgRocksdbKeySkippedCountStr)},
		{Name: ast.NewCIStr(AvgRocksdbBlockCacheHitCountStr)},
		{Name: ast.NewCIStr(AvgRocksdbBlockReadCountStr)},
		{Name: ast.NewCIStr(AvgRocksdbBlockReadByteStr)},
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentCountStr)},
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentSizeStr)},
		{Name: ast.NewCIStr(AvgAffectedRowsStr)},
		{Name: ast.NewCIStr(AvgTidbCPUTimeStr)},
		{Name: ast.NewCIStr(AvgTikvCPUTimeStr)},
	}
	factories := makeColumnFactories(columns)
	info := GenerateStmtExecInfo4Test("digest")
	record := NewStmtRecord(info)
	record.Add(info)
	const rocksdbSum = uint64(1 << 63)
	record.SumRocksdbDeleteSkippedCount = rocksdbSum
	record.SumRocksdbKeySkippedCount = rocksdbSum
	record.SumRocksdbBlockCacheHitCount = rocksdbSum
	record.SumRocksdbBlockReadCount = rocksdbSum
	record.SumRocksdbBlockReadByte = rocksdbSum
	record.SumIARemoteReadSegmentCount = rocksdbSum
	record.SumIARemoteReadSegmentSize = rocksdbSum
	record.SumAffectedRows = rocksdbSum
	avgRocksdbSum := float64(rocksdbSum) / float64(record.ExecCount)
	doubleColumnExpected := map[string]float64{
		AvgRocksdbDeleteSkippedCountStr: avgRocksdbSum,
		AvgRocksdbKeySkippedCountStr:    avgRocksdbSum,
		AvgRocksdbBlockCacheHitCountStr: avgRocksdbSum,
		AvgRocksdbBlockReadCountStr:     avgRocksdbSum,
		AvgRocksdbBlockReadByteStr:      avgRocksdbSum,
		AvgIARemoteReadSegmentCountStr:  avgRocksdbSum,
		AvgIARemoteReadSegmentSizeStr:   avgRocksdbSum,
		AvgAffectedRowsStr:              avgRocksdbSum,
	}
	for n, f := range factories {
		column := f(mockColumnInfo{}, record)
		columnName := columns[n].Name.O
		if expected, ok := doubleColumnExpected[columnName]; ok {
			datum := types.NewDatum(column)
			row := chunk.MutRowFromTypes([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)})
			row.SetDatums(datum)
			require.Equal(t, expected, row.ToRow().GetFloat64(0), columnName)
		}
		switch columnName {
		case ClusterTableInstanceColumnNameStr:
			require.Equal(t, "instance_addr", column)
		case StmtTypeStr:
			require.Equal(t, record.StmtType, column)
		case SchemaNameStr:
			require.Equal(t, record.SchemaName, column)
		case DigestStr:
			require.Equal(t, record.Digest, column)
		case DigestTextStr:
			require.Equal(t, record.NormalizedSQL, column)
		case TableNamesStr:
			require.Equal(t, record.TableNames, column)
		case IndexNamesStr:
			require.Equal(t, strings.Join(record.IndexNames, ","), column)
		case SampleUserStr:
			require.Equal(t, info.User, column)
		case ExecCountStr:
			require.Equal(t, int64(1), column)
		case SumLatencyStr:
			require.Equal(t, int64(record.SumLatency), column)
		case MaxLatencyStr:
			require.Equal(t, int64(record.MaxLatency), column)
		case AvgRocksdbDeleteSkippedCountStr:
			require.Equal(t, avgRocksdbSum, column)
		case AvgRocksdbKeySkippedCountStr:
			require.Equal(t, avgRocksdbSum, column)
		case AvgRocksdbBlockCacheHitCountStr:
			require.Equal(t, avgRocksdbSum, column)
		case AvgRocksdbBlockReadCountStr:
			require.Equal(t, avgRocksdbSum, column)
		case AvgRocksdbBlockReadByteStr:
			require.Equal(t, avgRocksdbSum, column)
		case AvgAffectedRowsStr:
			require.Equal(t, avgRocksdbSum, column)
		case AvgTidbCPUTimeStr:
			require.Equal(t, int64(record.SumTidbCPU), column)
		case AvgTikvCPUTimeStr:
			require.Equal(t, int64(record.SumTikvCPU), column)
		}
	}

	smallRecord := &StmtRecord{
		ExecCount:                    1,
		SumRocksdbDeleteSkippedCount: 7,
		SumRocksdbKeySkippedCount:    19,
		SumRocksdbBlockCacheHitCount: 60,
		SumRocksdbBlockReadCount:     21103,
		SumRocksdbBlockReadByte:      4096,
		SumAffectedRows:              3,
	}
	smallCases := []struct {
		name     string
		expected float64
	}{
		{name: AvgRocksdbDeleteSkippedCountStr, expected: 7},
		{name: AvgRocksdbKeySkippedCountStr, expected: 19},
		{name: AvgRocksdbBlockCacheHitCountStr, expected: 60},
		{name: AvgRocksdbBlockReadCountStr, expected: 21103},
		{name: AvgRocksdbBlockReadByteStr, expected: 4096},
		{name: AvgAffectedRowsStr, expected: 3},
	}
	for _, tc := range smallCases {
		factory, ok := columnFactoryMap[tc.name]
		require.Truef(t, ok, "missing column factory: %s", tc.name)
		datum := types.NewDatum(factory(mockColumnInfo{}, smallRecord))

		row := chunk.MutRowFromTypes([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)})
		row.SetDatums(datum)
		require.Equal(t, tc.expected, row.ToRow().GetFloat64(0), tc.name)
	}
}

func TestIAAvgColumns(t *testing.T) {
	columns := []*model.ColumnInfo{
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentCountStr)},
		{Name: ast.NewCIStr(MaxIARemoteReadSegmentCountStr)},
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentSizeStr)},
		{Name: ast.NewCIStr(MaxIARemoteReadSegmentSizeStr)},
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentWaitTimeStr)},
		{Name: ast.NewCIStr(MaxIARemoteReadSegmentWaitTimeStr)},
	}
	factories := makeColumnFactories(columns)

	info1 := GenerateStmtExecInfo4Test("digest")
	info1.ExecDetail.ScanDetail.IaRemoteReadSegmentCount = 3
	info1.ExecDetail.ScanDetail.IaRemoteReadSegmentBytes = 4096
	info1.ExecDetail.ScanDetail.IaRemoteReadSegmentDuration = 5 * time.Millisecond

	info2 := GenerateStmtExecInfo4Test("digest")
	info2.ExecDetail.ScanDetail.IaRemoteReadSegmentCount = 5
	info2.ExecDetail.ScanDetail.IaRemoteReadSegmentBytes = 8192
	info2.ExecDetail.ScanDetail.IaRemoteReadSegmentDuration = 9 * time.Millisecond

	record := NewStmtRecord(info1)
	record.Add(info1)
	record.Add(info2)

	require.Equal(t, 4.0, factories[0](mockColumnInfo{}, record))
	require.Equal(t, uint64(5), factories[1](mockColumnInfo{}, record))
	require.Equal(t, 6144.0, factories[2](mockColumnInfo{}, record))
	require.Equal(t, uint64(8192), factories[3](mockColumnInfo{}, record))
	require.Equal(t, int64(7*time.Millisecond), factories[4](mockColumnInfo{}, record))
	require.Equal(t, int64(9*time.Millisecond), factories[5](mockColumnInfo{}, record))
}

func TestIAAvgColumnsChunkRoundTrip(t *testing.T) {
	columns := []*model.ColumnInfo{
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentCountStr)},
		{Name: ast.NewCIStr(MaxIARemoteReadSegmentCountStr)},
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentSizeStr)},
		{Name: ast.NewCIStr(MaxIARemoteReadSegmentSizeStr)},
		{Name: ast.NewCIStr(AvgIARemoteReadSegmentWaitTimeStr)},
		{Name: ast.NewCIStr(MaxIARemoteReadSegmentWaitTimeStr)},
	}
	factories := makeColumnFactories(columns)

	info1 := GenerateStmtExecInfo4Test("digest")
	info1.ExecDetail.ScanDetail.IaRemoteReadSegmentCount = 3
	info1.ExecDetail.ScanDetail.IaRemoteReadSegmentBytes = 4096
	info1.ExecDetail.ScanDetail.IaRemoteReadSegmentDuration = 5 * time.Millisecond

	info2 := GenerateStmtExecInfo4Test("digest")
	info2.ExecDetail.ScanDetail.IaRemoteReadSegmentCount = 5
	info2.ExecDetail.ScanDetail.IaRemoteReadSegmentBytes = 8192
	info2.ExecDetail.ScanDetail.IaRemoteReadSegmentDuration = 9 * time.Millisecond

	record := NewStmtRecord(info1)
	record.Add(info1)
	record.Add(info2)

	rowDatums := make([]types.Datum, len(factories))
	for i, factory := range factories {
		rowDatums[i] = types.NewDatum(factory(mockColumnInfo{}, record))
	}

	maxUnsignedType := types.NewFieldType(mysql.TypeLonglong)
	maxUnsignedType.SetFlag(mysql.UnsignedFlag)
	retTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeDouble),
		maxUnsignedType,
		types.NewFieldType(mysql.TypeDouble),
		maxUnsignedType.Clone(),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
	}
	mutRow := chunk.MutRowFromTypes(retTypes)
	mutRow.SetDatums(rowDatums...)
	row := mutRow.ToRow()

	require.Equal(t, 4.0, row.GetFloat64(0))
	require.Equal(t, uint64(5), row.GetUint64(1))
	require.Equal(t, 6144.0, row.GetFloat64(2))
	require.Equal(t, uint64(8192), row.GetUint64(3))
	require.Equal(t, int64(7*time.Millisecond), row.GetInt64(4))
	require.Equal(t, int64(9*time.Millisecond), row.GetInt64(5))
}

type mockColumnInfo struct{}

func (mockColumnInfo) getInstanceAddr() string {
	return "instance_addr"
}

func (mockColumnInfo) getTimeLocation() *time.Location {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	return loc
}
