// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

// CompareFunc is a function to compare the two values in Row, the two columns must have the same type.
type CompareFunc func(a Row, aCol int, b Row, bCol int) int

// GetCompareFunc gets a compare function for the field type.
func GetCompareFunc(tp *types.FieldType) CompareFunc {
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(tp.Flag) {
			return cmpUint64
		}
		return cmpInt64
	case mysql.TypeFloat:
		return cmpFloat32
	case mysql.TypeDouble:
		return cmpFloat64
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return cmpString
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return cmpTime
	case mysql.TypeDuration:
		return cmpDuration
	case mysql.TypeNewDecimal:
		return cmpMyDecimal
	case mysql.TypeSet, mysql.TypeEnum:
		return cmpNameValue
	case mysql.TypeBit:
		return cmpBit
	case mysql.TypeJSON:
		return cmpJSON
	}
	return nil
}

func cmpNull(aNull, bNull bool) int {
	if aNull && bNull {
		return 0
	}
	if aNull {
		return -1
	}
	return 1
}

func cmpInt64(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	return types.CompareInt64(a.GetInt64(aCol), b.GetInt64(bCol))
}

func cmpUint64(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	return types.CompareUint64(a.GetUint64(aCol), b.GetUint64(bCol))
}

func cmpString(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	return types.CompareString(a.GetString(aCol), b.GetString(bCol))
}

func cmpFloat32(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	return types.CompareFloat64(float64(a.GetFloat32(aCol)), float64(b.GetFloat32(bCol)))
}

func cmpFloat64(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	return types.CompareFloat64(a.GetFloat64(aCol), b.GetFloat64(bCol))
}

func cmpMyDecimal(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	aDec, bDec := a.GetMyDecimal(aCol), b.GetMyDecimal(bCol)
	return aDec.Compare(bDec)
}

func cmpTime(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	aTime, bTime := a.GetTime(aCol), b.GetTime(bCol)
	return aTime.Compare(bTime)
}

func cmpDuration(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	aDur, bDur := a.GetDuration(aCol), b.GetDuration(bCol)
	return types.CompareInt64(int64(aDur.Duration), int64(bDur.Duration))
}

func cmpNameValue(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	_, aVal := a.getNameValue(aCol)
	_, bVal := b.getNameValue(bCol)
	return types.CompareUint64(aVal, bVal)
}

func cmpBit(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	aBit := types.BinaryLiteral(a.GetBytes(aCol))
	bBit := types.BinaryLiteral(b.GetBytes(bCol))
	aUint, err := aBit.ToInt()
	terror.Log(err)
	bUint, err := bBit.ToInt()
	terror.Log(err)
	return types.CompareUint64(aUint, bUint)
}

func cmpJSON(a Row, aCol int, b Row, bCol int) int {
	aNull, bNull := a.IsNull(aCol), b.IsNull(bCol)
	if aNull || bNull {
		return cmpNull(aNull, bNull)
	}
	aJ, bJ := a.GetJSON(aCol), b.GetJSON(bCol)
	cmp, err := json.CompareJSON(aJ, bJ)
	terror.Log(err)
	return cmp
}
