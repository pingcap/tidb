// Copyright 2015 PingCAP, Inc.
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

package table

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

func getColDefaultValue(ctx expression.BuildContext, col *model.ColumnInfo, defaultVal any, args *getColOriginDefaultValue) (types.Datum, error) {
	if defaultVal == nil {
		return getColDefaultValueFromNil(ctx, col, args)
	}

	switch col.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDatetime:
	default:
		value, err := CastColumnValue(ctx, types.NewDatum(defaultVal), col, false, false)
		if err != nil {
			return types.Datum{}, err
		}
		return value, nil
	}

	// Check and get timestamp/datetime default value.
	var needChangeTimeZone bool
	var explicitTz *time.Location
	// If the column's default value is not ZeroDatetimeStr nor CurrentTimestamp, should use the time zone of the default value itself.
	if col.GetType() == mysql.TypeTimestamp {
		if vv, ok := defaultVal.(string); ok && vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			needChangeTimeZone = true
			// For col.Version = 0, the timezone information of default value is already lost, so use the system timezone as the default value timezone.
			explicitTz = timeutil.SystemLocation()
			if col.Version >= model.ColumnInfoVersion1 {
				explicitTz = time.UTC
			}
		}
	}
	value, err := expression.GetTimeValue(ctx, defaultVal, col.GetType(), col.GetDecimal(), explicitTz)
	if err != nil {
		return types.Datum{}, errGetDefaultFailed.GenWithStackByArgs(col.Name)
	}
	// If the column's default value is not ZeroDatetimeStr or CurrentTimestamp, convert the default value to the current session time zone.
	if needChangeTimeZone {
		t := value.GetMysqlTime()
		err = t.ConvertTimeZone(explicitTz, ctx.GetEvalCtx().Location())
		if err != nil {
			return value, err
		}
		value.SetMysqlTime(t)
	}
	return value, nil
}

func getColDefaultValueFromNil(ctx expression.BuildContext, col *model.ColumnInfo, args *getColOriginDefaultValue) (types.Datum, error) {
	if !mysql.HasNotNullFlag(col.GetFlag()) {
		return types.Datum{}, nil
	}
	if col.GetType() == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		defEnum, err := types.ParseEnumValue(col.FieldType.GetElems(), 1)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewCollateMysqlEnumDatum(defEnum, col.GetCollate()), nil
	}
	if mysql.HasAutoIncrementFlag(col.GetFlag()) {
		// Auto increment column doesn't have default value and we should not return error.
		return GetZeroValue(col), nil
	}
	evalCtx := ctx.GetEvalCtx()
	var strictSQLMode bool
	if args != nil {
		strictSQLMode = args.StrictSQLMode
	} else {
		strictSQLMode = evalCtx.SQLMode().HasStrictMode()
	}
	if !strictSQLMode {
		evalCtx.AppendWarning(ErrNoDefaultValue.FastGenByArgs(col.Name))
		return GetZeroValue(col), nil
	}
	ec := evalCtx.ErrCtx()
	var err error
	if mysql.HasNoDefaultValueFlag(col.GetFlag()) {
		err = ErrNoDefaultValue.FastGenByArgs(col.Name)
	} else {
		err = ErrColumnCantNull.FastGenByArgs(col.Name)
	}
	if ec.HandleError(err) == nil {
		return GetZeroValue(col), nil
	}
	return types.Datum{}, ErrNoDefaultValue.GenWithStackByArgs(col.Name)
}

// GetZeroValue gets zero value for given column type.
func GetZeroValue(col *model.ColumnInfo) types.Datum {
	var d types.Datum
	switch col.GetType() {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			d.SetUint64(0)
		} else {
			d.SetInt64(0)
		}
	case mysql.TypeYear:
		d.SetInt64(0)
	case mysql.TypeFloat:
		d.SetFloat32(0)
	case mysql.TypeDouble:
		d.SetFloat64(0)
	case mysql.TypeNewDecimal:
		d.SetLength(col.GetFlen())
		d.SetFrac(col.GetDecimal())
		d.SetMysqlDecimal(new(types.MyDecimal))
	case mysql.TypeString:
		if col.GetFlen() > 0 && col.GetCharset() == charset.CharsetBin {
			d.SetBytes(make([]byte, col.GetFlen()))
		} else {
			d.SetString("", col.GetCollate())
		}
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString("", col.GetCollate())
	case mysql.TypeDuration:
		d.SetMysqlDuration(types.ZeroDuration)
	case mysql.TypeDate:
		d.SetMysqlTime(types.ZeroDate)
	case mysql.TypeTimestamp:
		d.SetMysqlTime(types.ZeroTimestamp)
	case mysql.TypeDatetime:
		d.SetMysqlTime(types.ZeroDatetime)
	case mysql.TypeBit:
		d.SetMysqlBit(types.ZeroBinaryLiteral)
	case mysql.TypeSet:
		d.SetMysqlSet(types.Set{}, col.GetCollate())
	case mysql.TypeEnum:
		d.SetMysqlEnum(types.Enum{}, col.GetCollate())
	case mysql.TypeJSON:
		d.SetMysqlJSON(types.CreateBinaryJSON(nil))
	case mysql.TypeTiDBVectorFloat32:
		d.SetVectorFloat32(types.ZeroVectorFloat32)
	}
	return d
}

// OptionalFsp convert a FieldType.GetDecimal() to string.
func OptionalFsp(fieldType *types.FieldType) string {
	fsp := fieldType.GetDecimal()
	if fsp == 0 {
		return ""
	}
	return "(" + strconv.Itoa(fsp) + ")"
}

// FillVirtualColumnValue will calculate the virtual column value by evaluating generated
// expression using rows from a chunk, and then fill this value into the chunk.
func FillVirtualColumnValue(virtualRetTypes []*types.FieldType, virtualColumnIndex []int,
	expCols []*expression.Column, colInfos []*model.ColumnInfo, ectx exprctx.BuildContext, req *chunk.Chunk) error {
	if len(virtualColumnIndex) == 0 {
		return nil
	}

	virCols := chunk.NewChunkWithCapacity(virtualRetTypes, req.Capacity())
	iter := chunk.NewIterator4Chunk(req)
	evalCtx := ectx.GetEvalCtx()
	tc := evalCtx.TypeCtx()
	for i, idx := range virtualColumnIndex {
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			datum, err := expCols[idx].EvalVirtualColumn(evalCtx, row)
			if err != nil {
				return err
			}
			// Because the expression might return different type from
			// the generated column, we should wrap a CAST on the result.
			castDatum, err := CastColumnValue(ectx, datum, colInfos[idx], false, true)
			if err != nil {
				return err
			}

			// Clip to zero if get negative value after cast to unsigned.
			if mysql.HasUnsignedFlag(colInfos[idx].FieldType.GetFlag()) && !castDatum.IsNull() && tc.Flags().AllowNegativeToUnsigned() {
				switch datum.Kind() {
				case types.KindInt64:
					if datum.GetInt64() < 0 {
						castDatum = GetZeroValue(colInfos[idx])
					}
				case types.KindFloat32, types.KindFloat64:
					if types.RoundFloat(datum.GetFloat64()) < 0 {
						castDatum = GetZeroValue(colInfos[idx])
					}
				case types.KindMysqlDecimal:
					if datum.GetMysqlDecimal().IsNegative() {
						castDatum = GetZeroValue(colInfos[idx])
					}
				}
			}

			// Handle the bad null error.
			if (mysql.HasNotNullFlag(colInfos[idx].GetFlag()) || mysql.HasPreventNullInsertFlag(colInfos[idx].GetFlag())) && castDatum.IsNull() {
				castDatum = GetZeroValue(colInfos[idx])
			}
			virCols.AppendDatum(i, &castDatum)
		}
		req.SetCol(idx, virCols.Column(i))
	}
	return nil
}
