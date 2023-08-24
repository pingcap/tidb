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

package cardinality

import (
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestPseudoTable(t *testing.T) {
	ti := &model.TableInfo{}
	colInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	ti.Columns = append(ti.Columns, colInfo)
	tbl := statistics.PseudoTable(ti)
	require.Len(t, tbl.Columns, 1)
	require.Greater(t, tbl.RealtimeCount, int64(0))
	sctx := mock.NewContext()
	count := ColumnLessRowCount(sctx, tbl, types.NewIntDatum(100), colInfo.ID)
	require.Equal(t, 3333, int(count))
	count, err := ColumnEqualRowCount(sctx, tbl, types.NewIntDatum(1000), colInfo.ID)
	require.NoError(t, err)
	require.Equal(t, 10, int(count))
	count, _ = ColumnBetweenRowCount(sctx, tbl, types.NewIntDatum(1000), types.NewIntDatum(5000), colInfo.ID)
	require.Equal(t, 250, int(count))
	ti.Columns = append(ti.Columns, &model.ColumnInfo{
		ID:        2,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		Hidden:    true,
		State:     model.StatePublic,
	})
	tbl = statistics.PseudoTable(ti)
	// We added a hidden column. The pseudo table still only have one column.
	require.Equal(t, len(tbl.Columns), 1)
}

func TestEnumRangeValues(t *testing.T) {
	tests := []struct {
		low         types.Datum
		high        types.Datum
		lowExclude  bool
		highExclude bool
		res         string
	}{
		{
			low:         types.NewIntDatum(0),
			high:        types.NewIntDatum(5),
			lowExclude:  false,
			highExclude: true,
			res:         "(0, 1, 2, 3, 4)",
		},
		{
			low:         types.NewIntDatum(math.MinInt64),
			high:        types.NewIntDatum(math.MaxInt64),
			lowExclude:  false,
			highExclude: false,
			res:         "",
		},
		{
			low:         types.NewUintDatum(0),
			high:        types.NewUintDatum(5),
			lowExclude:  false,
			highExclude: true,
			res:         "(0, 1, 2, 3, 4)",
		},
		{
			low:         types.NewDurationDatum(getDuration("0:00:00")),
			high:        types.NewDurationDatum(getDuration("0:00:05")),
			lowExclude:  false,
			highExclude: true,
			res:         "(00:00:00, 00:00:01, 00:00:02, 00:00:03, 00:00:04)",
		},
		{
			low:         types.NewDurationDatum(getDuration("0:00:00")),
			high:        types.NewDurationDatum(getDuration("0:00:05")),
			lowExclude:  false,
			highExclude: true,
			res:         "(00:00:00, 00:00:01, 00:00:02, 00:00:03, 00:00:04)",
		},
		{
			low:         types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			high:        types.NewTimeDatum(getTime(2017, 1, 5, mysql.TypeDate)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2017-01-01, 2017-01-02, 2017-01-03, 2017-01-04)",
		},
		{
			low:         types.NewTimeDatum(getTimeStamp(0, 0, 0, mysql.TypeTimestamp)),
			high:        types.NewTimeDatum(getTimeStamp(0, 0, 5, mysql.TypeTimestamp)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2017-01-01 00:00:00, 2017-01-01 00:00:01, 2017-01-01 00:00:02, 2017-01-01 00:00:03, 2017-01-01 00:00:04)",
		},
		{
			low:         types.NewTimeDatum(getTimeStamp(0, 0, 0, mysql.TypeDatetime)),
			high:        types.NewTimeDatum(getTimeStamp(0, 0, 5, mysql.TypeDatetime)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2017-01-01 00:00:00, 2017-01-01 00:00:01, 2017-01-01 00:00:02, 2017-01-01 00:00:03, 2017-01-01 00:00:04)",
		},
		// fix issue 11610
		{
			low:         types.NewIntDatum(math.MinInt64),
			high:        types.NewIntDatum(0),
			lowExclude:  false,
			highExclude: false,
			res:         "",
		},
		{
			low:         types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			high:        types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			lowExclude:  true,
			highExclude: true,
			res:         "",
		},
	}
	for _, test := range tests {
		vals := enumRangeValues(test.low, test.high, test.lowExclude, test.highExclude)
		str, err := types.DatumsToString(vals, true)
		require.NoError(t, err)
		require.Equal(t, test.res, str)
	}
}

func getDuration(value string) types.Duration {
	dur, _, _ := types.ParseDuration(nil, value, 0)
	return dur
}

func getTime(year, month, day int, timeType byte) types.Time {
	ret := types.NewTime(types.FromDate(year, month, day, 0, 0, 0, 0), timeType, types.DefaultFsp)
	return ret
}

func getTimeStamp(hour, min, sec int, timeType byte) types.Time {
	ret := types.NewTime(types.FromDate(2017, 1, 1, hour, min, sec, 0), timeType, 0)
	return ret
}
