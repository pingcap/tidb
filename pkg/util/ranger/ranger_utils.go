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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger

import (
	"bytes"
	"regexp"
	"slices"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

type sortRange struct {
	originalValue *Range
	encodedStart  []byte
	encodedEnd    []byte
}

// UnionRanges sorts `ranges`, union adjacent ones if possible.
// For two intervals [a, b], [c, d], we have guaranteed that a <= c. If b >= c. Then two intervals are overlapped.
// And this two can be merged as [a, max(b, d)].
// Otherwise they aren't overlapped.
func UnionRanges(sctx *rangerctx.RangerContext, ranges Ranges, mergeConsecutive bool) (Ranges, error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	objects := make([]*sortRange, 0, len(ranges))
	for _, ran := range ranges {
		left, err := codec.EncodeKey(sctx.TypeCtx.Location(), nil, ran.LowVal...)
		err = sctx.ErrCtx.HandleError(err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			left = kv.Key(left).PrefixNext()
		}
		right, err := codec.EncodeKey(sctx.TypeCtx.Location(), nil, ran.HighVal...)
		err = sctx.ErrCtx.HandleError(err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			right = kv.Key(right).PrefixNext()
		}
		objects = append(objects, &sortRange{originalValue: ran, encodedStart: left, encodedEnd: right})
	}
	slices.SortFunc(objects, func(i, j *sortRange) int {
		return bytes.Compare(i.encodedStart, j.encodedStart)
	})
	ranges = ranges[:0]
	lastRange := objects[0]
	for i := 1; i < len(objects); i++ {
		if (mergeConsecutive && bytes.Compare(lastRange.encodedEnd, objects[i].encodedStart) >= 0) ||
			(!mergeConsecutive && bytes.Compare(lastRange.encodedEnd, objects[i].encodedStart) > 0) {
			if bytes.Compare(lastRange.encodedEnd, objects[i].encodedEnd) < 0 {
				lastRange.encodedEnd = objects[i].encodedEnd
				lastRange.originalValue.HighVal = objects[i].originalValue.HighVal
				lastRange.originalValue.HighExclude = objects[i].originalValue.HighExclude
			}
		} else {
			ranges = append(ranges, lastRange.originalValue)
			lastRange = objects[i]
		}
	}
	ranges = append(ranges, lastRange.originalValue)
	return ranges, nil
}

func hasPrefix(lengths []int) bool {
	for _, l := range lengths {
		if l != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

// cutPrefixForPoints cuts the prefix of points according to the prefix length of the prefix index.
// It may modify the point.value and point.excl. The modification is in-place.
// This function doesn't require the start and end points to be paired in the input.
func cutPrefixForPoints(points []*point, length int, tp *types.FieldType) {
	if length == types.UnspecifiedLength {
		return
	}
	for _, p := range points {
		if p == nil {
			continue
		}
		cut := CutDatumByPrefixLen(&p.value, length, tp)
		// In two cases, we need to convert the exclusive point to an inclusive point.
		// case 1: we actually cut the value to accommodate the prefix index.
		if cut ||
			// case 2: the value is already equal to the prefix index.
			// For example, col_varchar > 'xx' should be converted to range [xx, +inf) when the prefix index length of
			// `col_varchar` is 2. Otherwise, we would miss values like 'xxx' if we execute (xx, +inf) index range scan.
			(p.start && ReachPrefixLen(&p.value, length, tp)) {
			p.excl = false
		}
	}
}

// CutDatumByPrefixLen cuts the datum according to the prefix length.
// If it's binary or ascii encoded, we will cut it by bytes rather than characters.
func CutDatumByPrefixLen(v *types.Datum, length int, tp *types.FieldType) bool {
	if (v.Kind() == types.KindString || v.Kind() == types.KindBytes) && length != types.UnspecifiedLength {
		colCharset := tp.GetCharset()
		colValue := v.GetBytes()
		if colCharset == charset.CharsetBin || colCharset == charset.CharsetASCII {
			if len(colValue) > length {
				// truncate value and limit its length
				if v.Kind() == types.KindBytes {
					v.SetBytes(colValue[:length])
				} else {
					v.SetString(v.GetString()[:length], tp.GetCollate())
				}
				return true
			}
		} else if utf8.RuneCount(colValue) > length {
			rs := bytes.Runes(colValue)
			truncateStr := string(rs[:length])
			// truncate value and limit its length
			v.SetString(truncateStr, tp.GetCollate())
			return true
		}
	}
	return false
}

// ReachPrefixLen checks whether the length of v is equal to the prefix length.
func ReachPrefixLen(v *types.Datum, length int, tp *types.FieldType) bool {
	if (v.Kind() == types.KindString || v.Kind() == types.KindBytes) && length != types.UnspecifiedLength {
		colCharset := tp.GetCharset()
		colValue := v.GetBytes()
		if colCharset == charset.CharsetBin || colCharset == charset.CharsetASCII {
			return len(colValue) == length
		}
		return utf8.RuneCount(colValue) == length
	}
	return false
}

// In util/ranger, for each datum that is used in the Range, we will convert data type for them.
// But we cannot use the FieldType of column directly. e.g. the column a is int32 and we have a > 1111111111111111111.
// Obviously the constant is bigger than MaxInt32, so we will get overflow error if we use the FieldType of column a.
// In util/ranger here, we usually use "newTp" to emphasize its difference from the original FieldType of the column.
func newFieldType(tp *types.FieldType) *types.FieldType {
	switch tp.GetType() {
	// To avoid overflow error.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		newTp := types.NewFieldType(mysql.TypeLonglong)
		newTp.SetFlag(tp.GetFlag())
		newTp.SetCharset(tp.GetCharset())
		return newTp
	// To avoid data truncate error.
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		newTp := types.NewFieldTypeWithCollation(tp.GetType(), tp.GetCollate(), types.UnspecifiedLength)
		newTp.SetCharset(tp.GetCharset())
		return newTp
	default:
		return tp
	}
}

// points2EqOrInCond constructs a 'EQUAL' or 'IN' scalar function based on the
// 'points'. `col` is the target column to construct the Equal or In condition.
// NOTE:
// 1. 'points' should not be empty.
func points2EqOrInCond(ctx expression.BuildContext, points []*point, col *expression.Column) expression.Expression {
	// len(points) cannot be 0 here, since we impose early termination in ExtractEqAndInCondition
	// Constant and Column args should have same RetType, simply get from first arg
	retType := col.GetType(ctx.GetEvalCtx())
	args := make([]expression.Expression, 0, len(points)/2)
	args = append(args, col)
	orArgs := make([]expression.Expression, 0, 2)
	for i := 0; i < len(points); i = i + 2 {
		if points[i].value.IsNull() {
			orArgs = append(orArgs, expression.NewFunctionInternal(ctx, ast.IsNull, retType, col))
		} else {
			value := &expression.Constant{
				Value:   points[i].value,
				RetType: retType,
			}
			args = append(args, value)
		}
	}
	var result expression.Expression
	if len(args) > 1 {
		funcName := ast.EQ
		if len(args) > 2 {
			funcName = ast.In
		}
		result = expression.NewFunctionInternal(ctx, funcName, col.GetType(ctx.GetEvalCtx()), args...)
	}
	if len(orArgs) == 0 {
		return result
	}
	if result != nil {
		orArgs = append(orArgs, result)
	}
	if len(orArgs) == 1 {
		return orArgs[0]
	}
	return expression.NewFunctionInternal(ctx, ast.LogicOr, col.GetType(ctx.GetEvalCtx()), orArgs...)
}

// RangesToString print a list of Ranges into a string which can appear in an SQL as a condition.
func RangesToString(sc *stmtctx.StatementContext, rans Ranges, colNames []string) (string, error) {
	for _, ran := range rans {
		if len(ran.LowVal) != len(ran.HighVal) {
			return "", errors.New("range length mismatch")
		}
	}
	var buffer bytes.Buffer
	for i, ran := range rans {
		buffer.WriteString("(")
		for j := range ran.LowVal {
			buffer.WriteString("(")

			// The `Exclude` information is only useful for the last columns.
			// If it's not the last column, it should always be false, which means it's inclusive.
			lowExclude := false
			if ran.LowExclude && j == len(ran.LowVal)-1 {
				lowExclude = true
			}
			highExclude := false
			if ran.HighExclude && j == len(ran.LowVal)-1 {
				highExclude = true
			}

			// sanity check: only last column of the `Range` can be an interval
			if j < len(ran.LowVal)-1 {
				cmp, err := ran.LowVal[j].Compare(sc.TypeCtx(), &ran.HighVal[j], ran.Collators[j])
				if err != nil {
					return "", errors.New("comparing values error: " + err.Error())
				}
				if cmp != 0 {
					return "", errors.New("unexpected form of range")
				}
			}
			str, err := RangeSingleColToString(sc, ran.LowVal[j], ran.HighVal[j], lowExclude, highExclude, colNames[j], ran.Collators[j])
			if err != nil {
				return "false", err
			}
			buffer.WriteString(str)
			buffer.WriteString(")")
			if j < len(ran.LowVal)-1 {
				// Conditions on different columns of a range are implicitly connected with AND.
				buffer.WriteString(" and ")
			}
		}
		buffer.WriteString(")")
		if i < len(rans)-1 {
			// Conditions of different ranges are implicitly connected with OR.
			buffer.WriteString(" or ")
		}
	}
	result := buffer.String()

	// Simplify some useless conditions.
	if matched, err := regexp.MatchString(`^\(*true\)*$`, result); matched || (err != nil) {
		return "true", nil
	}
	return result, nil
}

// RangeSingleColToString prints a single column of a Range into a string which can appear in an SQL as a condition.
func RangeSingleColToString(sc *stmtctx.StatementContext, lowVal, highVal types.Datum, lowExclude, highExclude bool, colName string, collator collate.Collator) (string, error) {
	// case 1: low and high are both special values(null, min not null, max value)
	lowKind := lowVal.Kind()
	highKind := highVal.Kind()
	if (lowKind == types.KindNull || lowKind == types.KindMinNotNull || lowKind == types.KindMaxValue) &&
		(highKind == types.KindNull || highKind == types.KindMinNotNull || highKind == types.KindMaxValue) {
		if lowKind == types.KindNull && highKind == types.KindNull && !lowExclude && !highExclude {
			return colName + " is null", nil
		}
		if lowKind == types.KindNull && highKind == types.KindMaxValue && !lowExclude {
			return "true", nil
		}
		if lowKind == types.KindMinNotNull && highKind == types.KindMaxValue {
			return colName + " is not null", nil
		}
		return "false", nil
	}

	var buf bytes.Buffer
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)

	// case 2: low value and high value are the same, and low value and high value are both inclusive.
	cmp, err := lowVal.Compare(sc.TypeCtx(), &highVal, collator)
	if err != nil {
		return "false", errors.Trace(err)
	}
	if cmp == 0 && !lowExclude && !highExclude && !lowVal.IsNull() {
		buf.WriteString(colName)
		buf.WriteString(" = ")
		lowValExpr := driver.ValueExpr{Datum: lowVal}
		err := lowValExpr.Restore(restoreCtx)
		if err != nil {
			return "false", errors.Trace(err)
		}
		return buf.String(), nil
	}

	// case 3: it's an interval.
	useOR := false
	noLowerPart := false

	// Handle the low value part.
	if lowKind == types.KindNull {
		buf.WriteString(colName + " is null")
		useOR = true
	} else if lowKind == types.KindMinNotNull {
		noLowerPart = true
	} else {
		buf.WriteString(colName)
		if lowExclude {
			buf.WriteString(" > ")
		} else {
			buf.WriteString(" >= ")
		}
		lowValExpr := driver.ValueExpr{Datum: lowVal}
		err := lowValExpr.Restore(restoreCtx)
		if err != nil {
			return "false", errors.Trace(err)
		}
	}

	if !noLowerPart {
		if useOR {
			buf.WriteString(" or ")
		} else {
			buf.WriteString(" and ")
		}
	}

	// Handle the high value part
	if highKind == types.KindMaxValue {
		buf.WriteString("true")
	} else {
		buf.WriteString(colName)
		if highExclude {
			buf.WriteString(" < ")
		} else {
			buf.WriteString(" <= ")
		}
		highValExpr := driver.ValueExpr{Datum: highVal}
		err := highValExpr.Restore(restoreCtx)
		if err != nil {
			return "false", errors.Trace(err)
		}
	}

	return buf.String(), nil
}
