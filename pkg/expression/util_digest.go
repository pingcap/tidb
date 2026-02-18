// Copyright 2016 PingCAP, Inc.
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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// SQLDigestTextRetriever is used to find the normalized SQL statement text by SQL digests in statements_summary table.
// It's exported for test purposes. It's used by the `tidb_decode_sql_digests` builtin function, but also exposed to
// be used in other modules.
type SQLDigestTextRetriever struct {
	// SQLDigestsMap is the place to put the digests that's requested for getting SQL text and also the place to put
	// the query result.
	SQLDigestsMap map[string]string

	// Replace querying for test purposes.
	mockLocalData  map[string]string
	mockGlobalData map[string]string
	// There are two ways for querying information: 1) query specified digests by WHERE IN query, or 2) query all
	// information to avoid the too long WHERE IN clause. If there are more than `fetchAllLimit` digests needs to be
	// queried, the second way will be chosen; otherwise, the first way will be chosen.
	fetchAllLimit int
}

// NewSQLDigestTextRetriever creates a new SQLDigestTextRetriever.
func NewSQLDigestTextRetriever() *SQLDigestTextRetriever {
	return &SQLDigestTextRetriever{
		SQLDigestsMap: make(map[string]string),
		fetchAllLimit: 512,
	}
}

func (r *SQLDigestTextRetriever) runMockQuery(data map[string]string, inValues []any) (map[string]string, error) {
	if len(inValues) == 0 {
		return data, nil
	}
	res := make(map[string]string, len(inValues))
	for _, digest := range inValues {
		if text, ok := data[digest.(string)]; ok {
			res[digest.(string)] = text
		}
	}
	return res, nil
}

// runFetchDigestQuery runs query to the system tables to fetch the kv mapping of SQL digests and normalized SQL texts
// of the given SQL digests, if `inValues` is given, or all these mappings otherwise. If `queryGlobal` is false, it
// queries information_schema.statements_summary and information_schema.statements_summary_history; otherwise, it
// queries the cluster version of these two tables.
func (r *SQLDigestTextRetriever) runFetchDigestQuery(ctx context.Context, exec expropt.SQLExecutor, queryGlobal bool, inValues []any) (map[string]string, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	// If mock data is set, query the mock data instead of the real statements_summary tables.
	if !queryGlobal && r.mockLocalData != nil {
		return r.runMockQuery(r.mockLocalData, inValues)
	} else if queryGlobal && r.mockGlobalData != nil {
		return r.runMockQuery(r.mockGlobalData, inValues)
	}

	// Information in statements_summary will be periodically moved to statements_summary_history. Union them together
	// to avoid missing information when statements_summary is just cleared.
	stmt := "select digest, digest_text from information_schema.statements_summary union distinct " +
		"select digest, digest_text from information_schema.statements_summary_history"
	if queryGlobal {
		stmt = "select digest, digest_text from information_schema.cluster_statements_summary union distinct " +
			"select digest, digest_text from information_schema.cluster_statements_summary_history"
	}
	// Add the where clause if `inValues` is specified.
	if len(inValues) > 0 {
		stmt += " where digest in (" + strings.Repeat("%?,", len(inValues)-1) + "%?)"
	}

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, stmt, inValues...)
	if err != nil {
		return nil, err
	}

	res := make(map[string]string, len(rows))
	for _, row := range rows {
		res[row.GetString(0)] = row.GetString(1)
	}
	return res, nil
}

func (r *SQLDigestTextRetriever) updateDigestInfo(queryResult map[string]string) {
	for digest, text := range r.SQLDigestsMap {
		if len(text) > 0 {
			// The text of this digest is already known
			continue
		}
		sqlText, ok := queryResult[digest]
		if ok {
			r.SQLDigestsMap[digest] = sqlText
		}
	}
}

// RetrieveLocal tries to retrieve the SQL text of the SQL digests from local information.
func (r *SQLDigestTextRetriever) RetrieveLocal(ctx context.Context, exec expropt.SQLExecutor) error {
	if len(r.SQLDigestsMap) == 0 {
		return nil
	}

	var queryResult map[string]string
	if len(r.SQLDigestsMap) <= r.fetchAllLimit {
		inValues := make([]any, 0, len(r.SQLDigestsMap))
		for key := range r.SQLDigestsMap {
			inValues = append(inValues, key)
		}
		var err error
		queryResult, err = r.runFetchDigestQuery(ctx, exec, false, inValues)
		if err != nil {
			return errors.Trace(err)
		}

		if len(queryResult) == len(r.SQLDigestsMap) {
			r.SQLDigestsMap = queryResult
			return nil
		}
	} else {
		var err error
		queryResult, err = r.runFetchDigestQuery(ctx, exec, false, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateDigestInfo(queryResult)
	return nil
}

// RetrieveGlobal tries to retrieve the SQL text of the SQL digests from the information of the whole cluster.
func (r *SQLDigestTextRetriever) RetrieveGlobal(ctx context.Context, exec expropt.SQLExecutor) error {
	err := r.RetrieveLocal(ctx, exec)
	if err != nil {
		return errors.Trace(err)
	}

	// In some unit test environments it's unable to retrieve global info, and this function blocks it for tens of
	// seconds, which wastes much time during unit test. In this case, enable this failpoint to bypass retrieving
	// globally.
	failpoint.Inject("sqlDigestRetrieverSkipRetrieveGlobal", func() {
		failpoint.Return(nil)
	})

	var unknownDigests []any
	for k, v := range r.SQLDigestsMap {
		if len(v) == 0 {
			unknownDigests = append(unknownDigests, k)
		}
	}

	if len(unknownDigests) == 0 {
		return nil
	}

	var queryResult map[string]string
	if len(r.SQLDigestsMap) <= r.fetchAllLimit {
		queryResult, err = r.runFetchDigestQuery(ctx, exec, true, unknownDigests)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		queryResult, err = r.runFetchDigestQuery(ctx, exec, true, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateDigestInfo(queryResult)
	return nil
}

// ExprsToStringsForDisplay convert a slice of Expression to a slice of string using Expression.String(), and
// to make it better for display and debug, it also escapes the string to corresponding golang string literal,
// which means using \t, \n, \x??, \u????, ... to represent newline, control character, non-printable character,
// invalid utf-8 bytes and so on.
func ExprsToStringsForDisplay(ctx EvalContext, exprs []Expression) []string {
	strs := make([]string, len(exprs))
	for i, cond := range exprs {
		quote := `"`
		// We only need the escape functionality of strconv.Quote, the quoting is not needed,
		// so we trim the \" prefix and suffix here.
		strs[i] = strings.TrimSuffix(
			strings.TrimPrefix(
				strconv.Quote(cond.StringWithCtx(ctx, errors.RedactLogDisable)),
				quote),
			quote)
	}
	return strs
}

// HasColumnWithCondition tries to retrieve the expression (column or function) if it contains the target column.
func HasColumnWithCondition(e Expression, cond func(*Column) bool) bool {
	return hasColumnWithCondition(e, cond)
}

func hasColumnWithCondition(e Expression, cond func(*Column) bool) bool {
	switch v := e.(type) {
	case *Column:
		return cond(v)
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			if hasColumnWithCondition(arg, cond) {
				return true
			}
		}
	}
	return false
}

// ConstExprConsiderPlanCache indicates whether the expression can be considered as a constant expression considering planCache.
// If the expression is in plan cache, it should have a const level `ConstStrict` because it can be shared across statements.
// If the expression is not in plan cache, `ConstOnlyInContext` is enough because it is only used in one statement.
// Please notice that if the expression may be cached in other ways except plan cache, we should not use this function.
func ConstExprConsiderPlanCache(expr Expression, inPlanCache bool) bool {
	switch expr.ConstLevel() {
	case ConstStrict:
		return true
	case ConstOnlyInContext:
		return !inPlanCache
	default:
		return false
	}
}

// ExprsHasSideEffects checks if any of the expressions has side effects.
func ExprsHasSideEffects(exprs []Expression) bool {
	return slices.ContainsFunc(exprs, ExprHasSetVarOrSleep)
}

// ExprHasSetVarOrSleep checks if the expression has SetVar function or Sleep function.
func ExprHasSetVarOrSleep(expr Expression) bool {
	scalaFunc, isScalaFunc := expr.(*ScalarFunction)
	if !isScalaFunc {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar || scalaFunc.FuncName.L == ast.Sleep {
		return true
	}
	return slices.ContainsFunc(scalaFunc.GetArgs(), ExprHasSetVarOrSleep)
}

// ExecBinaryParam parse execute binary param arguments to datum slice.
func ExecBinaryParam(typectx types.Context, binaryParams []param.BinaryParam) (params []Expression, err error) {
	var (
		tmp any
	)

	params = make([]Expression, len(binaryParams))
	args := make([]types.Datum, len(binaryParams))
	for i := range args {
		tp := binaryParams[i].Tp
		isUnsigned := binaryParams[i].IsUnsigned

		switch tp {
		case mysql.TypeNull:
			var nilDatum types.Datum
			nilDatum.SetNull()
			args[i] = nilDatum
			continue

		case mysql.TypeTiny:
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(binaryParams[i].Val[0]))
			} else {
				args[i] = types.NewIntDatum(int64(int8(binaryParams[i].Val[0])))
			}
			continue

		case mysql.TypeShort, mysql.TypeYear:
			valU16 := binary.LittleEndian.Uint16(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU16))
			} else {
				args[i] = types.NewIntDatum(int64(int16(valU16)))
			}
			continue

		case mysql.TypeInt24, mysql.TypeLong:
			valU32 := binary.LittleEndian.Uint32(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU32))
			} else {
				args[i] = types.NewIntDatum(int64(int32(valU32)))
			}
			continue

		case mysql.TypeLonglong:
			valU64 := binary.LittleEndian.Uint64(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(valU64)
			} else {
				args[i] = types.NewIntDatum(int64(valU64))
			}
			continue

		case mysql.TypeFloat:
			args[i] = types.NewFloat32Datum(math.Float32frombits(binary.LittleEndian.Uint32(binaryParams[i].Val)))
			continue

		case mysql.TypeDouble:
			args[i] = types.NewFloat64Datum(math.Float64frombits(binary.LittleEndian.Uint64(binaryParams[i].Val)))
			continue

		case mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeDatetime:
			switch len(binaryParams[i].Val) {
			case 0:
				tmp = types.ZeroDatetimeStr
			case 4:
				_, tmp = binaryDate(0, binaryParams[i].Val)
			case 7:
				_, tmp = binaryDateTime(0, binaryParams[i].Val)
			case 11:
				_, tmp = binaryTimestamp(0, binaryParams[i].Val)
			case 13:
				_, tmp = binaryTimestampWithTZ(0, binaryParams[i].Val)
			default:
				err = mysql.ErrMalformPacket
				return
			}
			// TODO: generate the time datum directly
			var parseTime func(types.Context, string) (types.Time, error)
			switch tp {
			case mysql.TypeDate:
				parseTime = types.ParseDate
			case mysql.TypeDatetime:
				parseTime = types.ParseDatetime
			case mysql.TypeTimestamp:
				// To be compatible with MySQL, even the type of parameter is
				// TypeTimestamp, the return type should also be `Datetime`.
				parseTime = types.ParseDatetime
			}
			var time types.Time
			time, err = parseTime(typectx, tmp.(string))
			err = typectx.HandleTruncate(err)
			if err != nil {
				return
			}
			args[i] = types.NewDatum(time)
			continue

		case mysql.TypeDuration:
			fsp := 0
			switch len(binaryParams[i].Val) {
			case 0:
				tmp = "0"
			case 8:
				isNegative := binaryParams[i].Val[0]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				_, tmp = binaryDuration(1, binaryParams[i].Val, isNegative)
			case 12:
				isNegative := binaryParams[i].Val[0]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				_, tmp = binaryDurationWithMS(1, binaryParams[i].Val, isNegative)
				fsp = types.MaxFsp
			default:
				err = mysql.ErrMalformPacket
				return
			}
			// TODO: generate the duration datum directly
			var dur types.Duration
			dur, _, err = types.ParseDuration(typectx, tmp.(string), fsp)
			err = typectx.HandleTruncate(err)
			if err != nil {
				return
			}
			args[i] = types.NewDatum(dur)
			continue
		case mysql.TypeNewDecimal:
			if binaryParams[i].IsNull {
				args[i] = types.NewDecimalDatum(nil)
			} else {
				var dec types.MyDecimal
				if err := typectx.HandleTruncate(dec.FromString(binaryParams[i].Val)); err != nil && err != types.ErrTruncated {
					return nil, err
				}
				args[i] = types.NewDecimalDatum(&dec)
			}
			continue
		case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if binaryParams[i].IsNull {
				args[i] = types.NewBytesDatum(nil)
			} else {
				args[i] = types.NewBytesDatum(binaryParams[i].Val)
			}
			continue
		case mysql.TypeUnspecified, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeGeometry, mysql.TypeBit:
			if !binaryParams[i].IsNull {
				tmp = string(hack.String(binaryParams[i].Val))
			} else {
				tmp = nil
			}
			args[i] = types.NewDatum(tmp)
			continue
		default:
			err = param.ErrUnknownFieldType.GenWithStack("stmt unknown field type %d", tp)
			return
		}
	}

	for i := range params {
		ft := new(types.FieldType)
		types.InferParamTypeFromUnderlyingValue(args[i].GetValue(), ft)
		params[i] = &Constant{Value: args[i], RetType: ft}
	}
	return
}

func binaryDate(pos int, paramValues []byte) (int, string) {
	year := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
	pos += 2
	month := paramValues[pos]
	pos++
	day := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func binaryDateTime(pos int, paramValues []byte) (int, string) {
	pos, date := binaryDate(pos, paramValues)
	hour := paramValues[pos]
	pos++
	minute := paramValues[pos]
	pos++
	second := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func binaryTimestamp(pos int, paramValues []byte) (int, string) {
	pos, dateTime := binaryDateTime(pos, paramValues)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func binaryTimestampWithTZ(pos int, paramValues []byte) (int, string) {
	pos, timestamp := binaryTimestamp(pos, paramValues)
	tzShiftInMin := int16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
	tzShiftHour := tzShiftInMin / 60
	tzShiftAbsMin := tzShiftInMin % 60
	if tzShiftAbsMin < 0 {
		tzShiftAbsMin = -tzShiftAbsMin
	}
	pos += 2
	return pos, fmt.Sprintf("%s%+02d:%02d", timestamp, tzShiftHour, tzShiftAbsMin)
}

func binaryDuration(pos int, paramValues []byte, isNegative uint8) (int, string) {
	sign := ""
	if isNegative == 1 {
		sign = "-"
	}
	days := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	hours := paramValues[pos]
	pos++
	minutes := paramValues[pos]
	pos++
	seconds := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s%d %02d:%02d:%02d", sign, days, hours, minutes, seconds)
}

func binaryDurationWithMS(pos int, paramValues []byte,
	isNegative uint8) (int, string) {
	pos, dur := binaryDuration(pos, paramValues, isNegative)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dur, microSecond)
}

// IsConstNull is used to check whether the expression is a constant null expression.
// For example, `1 > NULL` is a constant null expression.
// Now we just assume that the first argrument is a column,
// the second argument is a constant null.
func IsConstNull(expr Expression) bool {
	if e, ok := expr.(*ScalarFunction); ok {
		switch e.FuncName.L {
		case ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE:
			if constExpr, ok := e.GetArgs()[1].(*Constant); ok && constExpr.Value.IsNull() && constExpr.DeferredExpr == nil {
				return true
			}
		}
	}
	return false
}

// IsColOpCol is to whether ScalarFunction meets col op col condition.
func IsColOpCol(sf *ScalarFunction) (_, _ *Column, _ bool) {
	args := sf.GetArgs()
	if len(args) == 2 {
		col2, ok2 := args[1].(*Column)
		col1, ok1 := args[0].(*Column)
		return col1, col2, ok1 && ok2
	}
	return nil, nil, false
}

// ExtractColumnsFromColOpCol is to extract columns from col op col condition.
func ExtractColumnsFromColOpCol(sf *ScalarFunction) (_, _ *Column) {
	args := sf.GetArgs()
	if len(args) == 2 {
		col2 := args[1].(*Column)
		col1 := args[0].(*Column)
		return col1, col2
	}
	return nil, nil
}
