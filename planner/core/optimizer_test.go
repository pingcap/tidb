// Copyright 2019 PingCAP, Inc.
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

package core

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

// LogicalOptimize exports the `logicalOptimize` function for test packages and
// doesn't affect the normal package and access control of Golang (tricky ^_^)
var LogicalOptimize = logicalOptimize

func testDecimalConvert(t *testing.T, lDec, lLen, rDec, rLen int, lConvert, rConvert bool, cDec, cLen int) {
	lType := types.NewFieldType(mysql.TypeNewDecimal)
	lType.Decimal = lDec
	lType.Flen = lLen

	rType := types.NewFieldType(mysql.TypeNewDecimal)
	rType.Decimal = rDec
	rType.Flen = rLen

	cType, lCon, rCon := negotiateCommonType(lType, rType)
	require.Equal(t, cType.Tp, mysql.TypeNewDecimal)
	require.Equal(t, cType.Decimal, cDec)
	require.Equal(t, cType.Flen, cLen)
	require.Equal(t, lConvert, lCon)
	require.Equal(t, rConvert, rCon)
}

func TestMPPDecimalConvert(t *testing.T) {
	testDecimalConvert(t, 5, 9, 5, 8, false, false, 5, 9)
	testDecimalConvert(t, 5, 8, 5, 9, false, false, 5, 9)
	testDecimalConvert(t, 0, 8, 0, 11, true, false, 0, 11)
	testDecimalConvert(t, 0, 16, 0, 11, false, false, 0, 16)
	testDecimalConvert(t, 5, 9, 4, 9, true, true, 5, 10)
	testDecimalConvert(t, 5, 8, 4, 9, true, true, 5, 10)
	testDecimalConvert(t, 5, 9, 4, 8, false, true, 5, 9)
	testDecimalConvert(t, 10, 16, 0, 11, true, true, 10, 21)
	testDecimalConvert(t, 5, 19, 0, 20, false, true, 5, 25)
	testDecimalConvert(t, 20, 20, 0, 60, true, true, 20, 65)
	testDecimalConvert(t, 20, 40, 0, 60, false, true, 20, 65)
	testDecimalConvert(t, 0, 40, 0, 60, false, false, 0, 60)
}

func testJoinKeyTypeConvert(t *testing.T, leftType, rightType, retType *types.FieldType, lConvert, rConvert bool) {
	cType, lCon, rCon := negotiateCommonType(leftType, rightType)
	require.Equal(t, cType.Tp, retType.Tp)
	require.Equal(t, cType.Flen, retType.Flen)
	require.Equal(t, cType.Decimal, retType.Decimal)
	require.Equal(t, cType.Flag, retType.Flag)
	require.Equal(t, lConvert, lCon)
	require.Equal(t, rConvert, rCon)

}

func TestMPPJoinKeyTypeConvert(t *testing.T) {
	tinyIntType := &types.FieldType{
		Tp: mysql.TypeTiny,
	}
	tinyIntType.Flen, tinyIntType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTiny)

	unsignedTinyIntType := &types.FieldType{
		Tp: mysql.TypeTiny,
	}
	unsignedTinyIntType.Flen, tinyIntType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTiny)
	unsignedTinyIntType.Flag = mysql.UnsignedFlag

	bigIntType := &types.FieldType{
		Tp: mysql.TypeLonglong,
	}
	bigIntType.Flen, tinyIntType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)

	unsignedBigIntType := &types.FieldType{
		Tp: mysql.TypeLonglong,
	}
	unsignedBigIntType.Flen, tinyIntType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	unsignedBigIntType.Flag = mysql.UnsignedFlag

	decimalType := &types.FieldType{
		Tp:      mysql.TypeNewDecimal,
		Flen:    20,
		Decimal: 0,
	}

	testJoinKeyTypeConvert(t, tinyIntType, tinyIntType, tinyIntType, false, false)
	testJoinKeyTypeConvert(t, tinyIntType, unsignedTinyIntType, bigIntType, true, true)
	testJoinKeyTypeConvert(t, tinyIntType, bigIntType, bigIntType, true, false)
	testJoinKeyTypeConvert(t, bigIntType, tinyIntType, bigIntType, false, true)
	testJoinKeyTypeConvert(t, unsignedBigIntType, tinyIntType, decimalType, true, true)
	testJoinKeyTypeConvert(t, tinyIntType, unsignedBigIntType, decimalType, true, true)
	testJoinKeyTypeConvert(t, bigIntType, bigIntType, bigIntType, false, false)
	testJoinKeyTypeConvert(t, unsignedBigIntType, bigIntType, decimalType, true, true)
	testJoinKeyTypeConvert(t, bigIntType, unsignedBigIntType, decimalType, true, true)
}
