// Copyright 2021 PingCAP, Inc.
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

package expression

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/stretchr/testify/require"
)

func TestInFunc(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.In]
	decimal1 := types.NewDecFromFloatForTest(123.121)
	decimal2 := types.NewDecFromFloatForTest(123.122)
	decimal3 := types.NewDecFromFloatForTest(123.123)
	decimal4 := types.NewDecFromFloatForTest(123.124)
	time1 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 1, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time2 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 2, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time3 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 3, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time4 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 4, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	duration1 := types.Duration{Duration: 12*time.Hour + 1*time.Minute + 1*time.Second}
	duration2 := types.Duration{Duration: 12*time.Hour + 1*time.Minute}
	duration3 := types.Duration{Duration: 12*time.Hour + 1*time.Second}
	duration4 := types.Duration{Duration: 12 * time.Hour}
	json1 := json.CreateBinary("123")
	json2 := json.CreateBinary("123.1")
	json3 := json.CreateBinary("123.2")
	json4 := json.CreateBinary("123.3")
	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{1, 1, 2, 3}, int64(1)},
		{[]interface{}{1, 0, 2, 3}, int64(0)},
		{[]interface{}{1, nil, 2, 3}, nil},
		{[]interface{}{nil, nil, 2, 3}, nil},
		{[]interface{}{uint64(0), 0, 2, 3}, int64(1)},
		{[]interface{}{uint64(math.MaxUint64), uint64(math.MaxUint64), 2, 3}, int64(1)},
		{[]interface{}{-1, uint64(math.MaxUint64), 2, 3}, int64(0)},
		{[]interface{}{uint64(math.MaxUint64), -1, 2, 3}, int64(0)},
		{[]interface{}{1, 0, 2, 3}, int64(0)},
		{[]interface{}{1.1, 1.2, 1.3}, int64(0)},
		{[]interface{}{1.1, 1.1, 1.2, 1.3}, int64(1)},
		{[]interface{}{decimal1, decimal2, decimal3, decimal4}, int64(0)},
		{[]interface{}{decimal1, decimal2, decimal3, decimal1}, int64(1)},
		{[]interface{}{"1.1", "1.1", "1.2", "1.3"}, int64(1)},
		{[]interface{}{"1.1", hack.Slice("1.1"), "1.2", "1.3"}, int64(1)},
		{[]interface{}{hack.Slice("1.1"), "1.1", "1.2", "1.3"}, int64(1)},
		{[]interface{}{time1, time2, time3, time1}, int64(1)},
		{[]interface{}{time1, time2, time3, time4}, int64(0)},
		{[]interface{}{duration1, duration2, duration3, duration4}, int64(0)},
		{[]interface{}{duration1, duration2, duration1, duration4}, int64(1)},
		{[]interface{}{json1, json2, json3, json4}, int64(0)},
		{[]interface{}{json1, json1, json3, json4}, int64(1)},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(fn, chunk.MutRowFromDatums(types.MakeDatums(tc.args...)).ToRow())
		require.NoError(t, err)
		require.Equalf(t, tc.res, d.GetValue(), "%v", types.MakeDatums(tc.args))
	}
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	strD1 := types.NewCollationStringDatum("a", "utf8_general_ci")
	strD2 := types.NewCollationStringDatum("Á", "utf8_general_ci")
	fn, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{strD1, strD2}))
	require.NoError(t, err)
	d, isNull, err := fn.evalInt(chunk.Row{})
	require.False(t, isNull)
	require.NoError(t, err)
	require.Equalf(t, int64(1), d, "%v, %v", strD1, strD2)
	chk1 := chunk.NewChunkWithCapacity(nil, 1)
	chk1.SetNumVirtualRows(1)
	chk2 := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeTiny)}, 1)
	err = fn.vecEvalInt(chk1, chk2.Column(0))
	require.NoError(t, err)
	require.Equal(t, int64(1), chk2.Column(0).GetInt64(0))
}
