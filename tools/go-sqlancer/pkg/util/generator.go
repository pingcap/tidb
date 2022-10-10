// Copyright 2022 PingCAP, Inc.
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

package util

import (
	"bytes"
	"fmt"
	"math/rand"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	parser_types "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/collate"
)

// OpFuncGroupByRet is a map
var OpFuncGroupByRet = make(types.OpFuncIndex)

// RegisterToOpFnIndex is to register OpFuncEval
func RegisterToOpFnIndex(o types.OpFuncEval) {
	returnType := o.GetPossibleReturnType()

	for returnType != 0 {
		// i must be n-th power of 2: 1001100 => i(0000100)
		i := returnType &^ (returnType - 1)
		if _, ok := OpFuncGroupByRet[i]; !ok {
			OpFuncGroupByRet[i] = make(map[string]types.OpFuncEval)
		}
		if _, ok := OpFuncGroupByRet[i][o.GetName()]; !ok {
			OpFuncGroupByRet[i][o.GetName()] = o
		}
		// make the last non-zero bit to be zero: 1001100 => 1001000
		returnType &= (returnType - 1)
	}
}

// ConvertToBoolOrNull -1 NULL; 0 false; 1 true
func ConvertToBoolOrNull(a parser_driver.ValueExpr) int8 {
	switch a.Kind() {
	case tidb_types.KindNull:
		return -1
	case tidb_types.KindInt64:
		if a.GetValue().(int64) != 0 {
			return 1
		}
		return 0
	case tidb_types.KindUint64:
		if a.GetValue().(uint64) != 0 {
			return 1
		}
		return 0
	case tidb_types.KindFloat32:
		if a.GetFloat32() == 0 {
			return 0
		}
		return 1
	case tidb_types.KindFloat64:
		if a.GetFloat64() == 0 {
			return 0
		}
		return 1
	case tidb_types.KindString:
		s := a.GetValue().(string)
		re, _ := regexp.Compile(`^[-+]?[0-9]*\.?[0-9]+`)
		matchall := re.FindAllString(s, -1)
		if len(matchall) == 0 {
			return 0
		}
		numStr := matchall[0]
		match, _ := regexp.MatchString(`^[-+]?0*\.?0+$`, numStr)
		if match {
			return 0
		}
		return 1
	case tidb_types.KindMysqlDecimal:
		d := a.GetMysqlDecimal()
		if d.IsZero() {
			return 0
		}
		return 1
	case tidb_types.KindMysqlTime:
		t := a.GetMysqlTime()
		if t.IsZero() {
			return 0
		}
		return 1
	default:
		panic(fmt.Sprintf("unreachable kind: %d", a.Kind()))
	}
}

// CompareValueExpr TODO(mahjonp) because of CompareDatum can tell us whether there exists an truncate, we can use it in `comparisionValidator`
func CompareValueExpr(a, b parser_driver.ValueExpr) int {
	charset.GetCollationByName(b.Collation())
	statementContext := &stmtctx.StatementContext{AllowInvalidDate: true}
	statementContext.IgnoreTruncate.Store(true)
	res, _ := a.Compare(statementContext, &b.Datum, collate.GetCollator(b.GetType().GetCollate()))
	return res
}

// BufferOut is return string
func BufferOut(node ast.Node) (string, error) {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

// TransMysqlType TODO: decimal NOT Equals to float
func TransMysqlType(t *parser_types.FieldType) uint64 {
	switch t.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return types.TypeIntArg
	case mysql.TypeFloat, mysql.TypeDouble:
		return types.TypeFloatArg
	case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDatetime:
		return types.TypeDatetimeArg
	case mysql.TypeVarchar, mysql.TypeJSON, mysql.TypeVarString, mysql.TypeString:
		return types.TypeStringArg
		// Note: Null is base of all types
	case mysql.TypeNull:
		arr := []uint64{types.TypeIntArg, types.TypeFloatArg, types.TypeDatetimeArg, types.TypeStringArg}
		return arr[rand.Intn(len(arr))]
	default:
		panic(fmt.Sprintf("no implement for type: %s", t.String()))
	}
}

// TransStringType TODO: decimal NOT Equals to float
func TransStringType(s string) uint64 {
	s = strings.ToLower(s)
	switch {
	case strings.Contains(s, "string"), strings.Contains(s, "char"), strings.Contains(s, "text"), strings.Contains(s, "json"):
		return types.TypeStringArg
	case strings.Contains(s, "int"), strings.Contains(s, "long"), strings.Contains(s, "short"), strings.Contains(s, "tiny"):
		return types.TypeIntArg
	case strings.Contains(s, "float"), strings.Contains(s, "decimal"), strings.Contains(s, "double"):
		return types.TypeFloatArg
	case strings.Contains(s, "time"), strings.Contains(s, "date"):
		return types.TypeDatetimeArg
	default:
		panic(fmt.Sprintf("no implement for type: %s", s))
	}
}

// TransToMysqlType is to be transferred into mysql type
func TransToMysqlType(i uint64) byte {
	switch i {
	case types.TypeIntArg:
		return mysql.TypeLong
	case types.TypeFloatArg:
		return mysql.TypeDouble
	case types.TypeDatetimeArg:
		return mysql.TypeDatetime
	case types.TypeStringArg:
		return mysql.TypeVarchar
	default:
		panic(fmt.Sprintf("no implement this type: %d", i))
	}
}
