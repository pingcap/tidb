// Copyright 2026 PingCAP, Inc.
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

package importer

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCanSkipDecimalCast(t *testing.T) {
	signedColInfo := &model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeNewDecimal),
	}
	signedColInfo.SetFlen(5)
	signedColInfo.SetDecimal(2)

	unsignedColInfo := &model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeNewDecimal),
	}
	unsignedColInfo.SetFlen(5)
	unsignedColInfo.SetDecimal(2)
	unsignedColInfo.AddFlag(mysql.UnsignedFlag)

	type testCase struct {
		valString string
		canSkip   bool
		colInfo   *model.ColumnInfo
	}

	tcs := []testCase{
		{"1.2", true, signedColInfo},
		{"1.23", true, signedColInfo},
		{"1.234", false, signedColInfo},
		{"-1.23", false, unsignedColInfo},
		{"1234.56", false, signedColInfo},
	}

	ctx := mock.NewContext()
	for _, tc := range tcs {
		dec := types.NewDecimalDatum(types.NewDecFromStringForTest(tc.valString))
		val, skip := canSkipCastForColumn(dec, tc.colInfo)
		require.Equal(t, tc.canSkip, skip)
		if skip {
			casted, err := table.CastColumnValue(ctx.GetExprCtx(), dec, tc.colInfo, false, false)
			require.NoError(t, err)
			require.Equal(t, tc.colInfo.GetFlen(), val.Length())
			require.Equal(t, tc.colInfo.GetDecimal(), val.Frac())
			require.Equal(t, 0, val.GetMysqlDecimal().Compare(casted.GetMysqlDecimal()))
			require.Equal(t, casted.Length(), val.Length())
			require.Equal(t, casted.Frac(), val.Frac())
		}
	}
}
