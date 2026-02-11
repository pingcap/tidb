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

	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestParquetStringLengthPostCheck(t *testing.T) {
	info := mydump.ColumnSkipCastInfo{
		CastingCheck: mydump.CastingCheckStringLength,
		TargetFlen:   3,
	}

	require.True(t, passStringLengthPostCheck(types.NewStringDatum("abc"), info))
	require.False(t, passStringLengthPostCheck(types.NewStringDatum("abcd"), info))
	require.True(t, passStringLengthPostCheck(types.NewStringDatum("中a"), info))
	require.True(t, passStringLengthPostCheck(types.NewStringDatum("中中文"), info))
	require.False(t, passStringLengthPostCheck(types.NewStringDatum("中中文a"), info))
}

func TestParquetDecimalPostCheck(t *testing.T) {
	decimalInfo := mydump.ColumnSkipCastInfo{
		CastingCheck:  mydump.CastingCheckDecimal,
		TargetFlen:    5,
		TargetDecimal: 2,
	}
	require.True(t, passDecimalPostCheck(types.NewDecimalDatum(types.NewDecFromStringForTest("123.45")), decimalInfo))
	require.False(t, passDecimalPostCheck(types.NewDecimalDatum(types.NewDecFromStringForTest("1234.56")), decimalInfo))
	require.False(t, passDecimalPostCheck(types.NewDecimalDatum(types.NewDecFromStringForTest("12.345")), decimalInfo))

	unsignedInfo := decimalInfo
	unsignedInfo.Unsigned = true
	require.True(t, passDecimalPostCheck(types.NewDecimalDatum(types.NewDecFromStringForTest("0.00")), unsignedInfo))
	require.False(t, passDecimalPostCheck(types.NewDecimalDatum(types.NewDecFromStringForTest("-0.01")), unsignedInfo))
}

func TestShouldSkipCastForInsertColumnNull(t *testing.T) {
	var (
		encoder *TableKVEncoder
		nullVal types.Datum
	)
	nullVal.SetNull()

	require.True(t, encoder.canSkipCastColumnValue(0, nullVal))

	encoder = &TableKVEncoder{
		insertColumnSkipCastInfos: []mydump.ColumnSkipCastInfo{
			{CastingCheck: mydump.CastingCheckNoSkip},
		},
	}
	require.True(t, encoder.canSkipCastColumnValue(0, nullVal))
}
