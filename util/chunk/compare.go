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
type CompareFunc = func(l Row, lCol int, r Row, rCol int) int

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

func cmpNull(lNull, rNull bool) int {
	if lNull && rNull {
		return 0
	}
	if lNull {
		return -1
	}
	return 1
}

func cmpInt64(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareInt64(l.GetInt64(lCol), r.GetInt64(rCol))
}

func cmpUint64(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareUint64(l.GetUint64(lCol), r.GetUint64(rCol))
}

func cmpString(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareString(l.GetString(lCol), r.GetString(rCol))
}

func cmpFloat32(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareFloat64(float64(l.GetFloat32(lCol)), float64(r.GetFloat32(rCol)))
}

func cmpFloat64(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareFloat64(l.GetFloat64(lCol), r.GetFloat64(rCol))
}

func cmpMyDecimal(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lDec, rDec := l.GetMyDecimal(lCol), r.GetMyDecimal(rCol)
	return lDec.Compare(rDec)
}

func cmpTime(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lTime, rTime := l.GetTime(lCol), r.GetTime(rCol)
	return lTime.Compare(rTime)
}

func cmpDuration(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lDur, rDur := l.GetDuration(lCol), r.GetDuration(rCol)
	return types.CompareInt64(int64(lDur.Duration), int64(rDur.Duration))
}

func cmpNameValue(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	_, lVal := l.getNameValue(lCol)
	_, rVal := r.getNameValue(rCol)
	return types.CompareUint64(lVal, rVal)
}

func cmpBit(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lBit := types.BinaryLiteral(l.GetBytes(lCol))
	rBit := types.BinaryLiteral(r.GetBytes(rCol))
	lUint, err := lBit.ToInt()
	terror.Log(err)
	rUint, err := rBit.ToInt()
	terror.Log(err)
	return types.CompareUint64(lUint, rUint)
}

func cmpJSON(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lJ, rJ := l.GetJSON(lCol), r.GetJSON(rCol)
	return json.CompareBinary(lJ, rJ)
}
