// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
// See the License for the specific language governing permissions and
// limitations under the License.

package column

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// Col provides meta data describing a table column.
type Col struct {
	model.ColumnInfo
}

// PrimaryKeyName defines primary key name.
const PrimaryKeyName = "PRIMARY"

// IndexedCol defines an index with info.
type IndexedCol struct {
	model.IndexInfo
	X kv.Index
}

type indexKey struct {
	value []interface{}
	h     int64
}

func (c *Col) getTypeStr() string {
	ans := []string{types.FieldTypeToStr(c.Tp, c.Charset)}
	if c.Flen != -1 {
		if c.Decimal == -1 {
			ans = append(ans, fmt.Sprintf("(%d)", c.Flen))
		} else {
			ans = append(ans, fmt.Sprintf("(%d, %d)", c.Flen, c.Decimal))
		}
	}
	if mysql.HasUnsignedFlag(c.Flag) {
		ans = append(ans, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(c.Flag) {
		ans = append(ans, "ZEROFILL")
	}
	if mysql.HasBinaryFlag(c.Flag) {
		ans = append(ans, "BINARY")
	}
	if c.Charset != "" && c.Charset != charset.CharsetBin {
		ans = append(ans, fmt.Sprintf("CHARACTER SET %s", c.Charset))
	}
	if c.Collate != "" {
		ans = append(ans, fmt.Sprintf("COLLATE %s", c.Collate))
	}
	return strings.Join(ans, " ")
}

// String implements fmt.Stringer interface.
func (c *Col) String() string {
	ans := []string{c.Name.O, types.FieldTypeToStr(c.Tp, c.Charset)}
	if mysql.HasAutoIncrementFlag(c.Flag) {
		ans = append(ans, "AUTO_INCREMENT")
	}
	if mysql.HasNotNullFlag(c.Flag) {
		ans = append(ans, "NOT NULL")
	}
	return strings.Join(ans, " ")
}

// FindCol finds column in cols by name.
func FindCol(cols []*Col, name string) (c *Col) {
	for _, c = range cols {
		if strings.EqualFold(c.Name.O, name) {
			return
		}
	}
	return nil
}

// FindCols finds columns in cols by names.
func FindCols(cols []*Col, names []string) ([]*Col, error) {
	var rcols []*Col
	for _, name := range names {
		col := FindCol(cols, name)
		if col != nil {
			rcols = append(rcols, col)
		} else {
			return nil, errors.Errorf("unknown column %s", name)
		}
	}

	return rcols, nil
}

// FindOnUpdateCols finds columns have OnUpdateNow flag.
func FindOnUpdateCols(cols []*Col) []*Col {
	var rcols []*Col
	for _, c := range cols {
		if mysql.HasOnUpdateNowFlag(c.Flag) {
			rcols = append(rcols, c)
		}
	}

	return rcols
}

func newParseColError(err error, c *Col) error {
	return errors.Errorf("parse err %v at column %s (type %s)", err, c.Name, types.FieldTypeToStr(c.Tp, c.Charset))
}

// CastValue casts a value based on column's type.
func (c *Col) CastValue(ctx context.Context, val interface{}) (casted interface{}, err error) {
	if val == nil {
		return
	}
	switch c.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		intVal, errCode := c.normalizeIntegerValue(val)
		if errCode == errCodeType {
			casted = intVal
			err = c.TypeError(val)
			return
		}
		return c.castIntegerValue(intVal, errCode)
	case mysql.TypeFloat, mysql.TypeDouble:
		return c.castFloatValue(val)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		switch v := val.(type) {
		case int64:
			casted, err = mysql.ParseTimeFromNum(v, c.Tp, c.Decimal)
			if err != nil {
				err = newParseColError(err, c)
			}
		case string:
			casted, err = mysql.ParseTime(v, c.Tp, c.Decimal)
			if err != nil {
				err = newParseColError(err, c)
			}
		case mysql.Time:
			var t mysql.Time
			t, err = v.Convert(c.Tp)
			if err != nil {
				err = newParseColError(err, c)
				return
			}
			casted, err = t.RoundFrac(c.Decimal)
			if err != nil {
				err = newParseColError(err, c)
			}
		default:
			err = c.TypeError(val)
		}
	case mysql.TypeDuration:
		switch v := val.(type) {
		case string:
			casted, err = mysql.ParseDuration(v, c.Decimal)
			if err != nil {
				err = newParseColError(err, c)
			}
		case mysql.Time:
			var t mysql.Duration
			t, err = v.ConvertToDuration()
			if err != nil {
				err = newParseColError(err, c)
				return
			}
			casted, err = t.RoundFrac(c.Decimal)
			if err != nil {
				err = newParseColError(err, c)
			}
		case mysql.Duration:
			casted, err = v.RoundFrac(c.Decimal)
		default:
			err = c.TypeError(val)
		}
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		strV := ""
		switch v := val.(type) {
		case mysql.Time:
			strV = v.String()
		case mysql.Duration:
			strV = v.String()
		case []byte:
			if c.Charset == charset.CharsetBin {
				casted = v
				return
			}
			strV = string(v)
		default:
			strV = fmt.Sprintf("%v", val)
		}
		if (c.Flen != types.UnspecifiedLength) && (len(strV) > c.Flen) {
			strV = strV[:c.Flen]
		}
		casted = strV
	case mysql.TypeDecimal, mysql.TypeNewDecimal:
		switch v := val.(type) {
		case string:
			casted, err = mysql.ParseDecimal(v)
			if err != nil {
				err = newParseColError(err, c)
			}
		case int8:
			casted = mysql.NewDecimalFromInt(int64(v), 0)
		case int16:
			casted = mysql.NewDecimalFromInt(int64(v), 0)
		case int32:
			casted = mysql.NewDecimalFromInt(int64(v), 0)
		case int64:
			casted = mysql.NewDecimalFromInt(int64(v), 0)
		case int:
			casted = mysql.NewDecimalFromInt(int64(v), 0)
		case uint8:
			casted = mysql.NewDecimalFromUint(uint64(v), 0)
		case uint16:
			casted = mysql.NewDecimalFromUint(uint64(v), 0)
		case uint32:
			casted = mysql.NewDecimalFromUint(uint64(v), 0)
		case uint64:
			casted = mysql.NewDecimalFromUint(uint64(v), 0)
		case uint:
			casted = mysql.NewDecimalFromUint(uint64(v), 0)
		case float32:
			casted = mysql.NewDecimalFromFloat(float64(v))
		case float64:
			casted = mysql.NewDecimalFromFloat(float64(v))
		case mysql.Decimal:
			casted = v
		}
	default:
		err = c.TypeError(val)
	}
	return
}

// TypeError returns error for invalid value type.
func (c *Col) TypeError(v interface{}) error {
	return errors.Errorf("cannot use %v (type %T) in assignment to, or comparison with, column %s (type %s)",
		v, v, c.Name, types.FieldTypeToStr(c.Tp, c.Charset))
}

func (c *Col) isUnsignedLongLongType() bool {
	return mysql.HasUnsignedFlag(c.Flag) && c.Tp == mysql.TypeLonglong
}

const (
	errCodeOK               = 0
	errCodeOverflowLower    = -1
	errCodeOverflowUpper    = -2
	errCodeOverflowMaxInt64 = -3
	errCodeType             = -4
)

func (c *Col) normalizeIntegerFromFloat(v float64) (val int64, errCode int) {
	if v > 0 {
		v = math.Floor(v + 0.5)
	} else {
		v = math.Ceil(v - 0.5)
	}

	if mysql.HasUnsignedFlag(c.Flag) {
		if v < 0 {
			errCode = errCodeOverflowLower
		} else if v > math.MaxUint64 {
			errCode = errCodeOverflowUpper
		}
	} else {
		if v < math.MinInt64 {
			errCode = errCodeOverflowLower
		} else if v > math.MaxInt64 {
			errCode = errCodeOverflowUpper
		}
	}
	val = int64(v)
	return
}

func (c *Col) normalizeIntegerFromUint(v uint64) (val int64, errCode int) {
	if v > math.MaxInt64 {
		if c.isUnsignedLongLongType() {
			errCode = errCodeOverflowMaxInt64
		} else {
			errCode = errCodeOverflowUpper
		}
	}
	val = int64(v)
	return
}

func (c *Col) normalizeIntegerFromString(v string) (val int64, errCode int) {
	v = strings.Trim(v, " \t\r\n")
	if c.Tp == mysql.TypeYear {
		yyal, err := mysql.ParseYear(v)
		if err != nil {
			errCode = errCodeOverflowUpper
		}
		val = int64(yyal)
	} else {
		fval, ferr := types.StrToFloat(v)
		if ferr != nil {
			errCode = errCodeType
		} else {
			val, errCode = c.normalizeIntegerFromFloat(fval)
		}
	}
	return
}

// Normalize integer values and do first round of value check.
func (c *Col) normalizeIntegerValue(x interface{}) (val int64, errCode int) {
	switch v := x.(type) {
	case int8:
		val = int64(v)
	case uint8:
		val = int64(v)
	case int16:
		val = int64(v)
	case uint16:
		val = int64(v)
	case int32:
		val = int64(v)
	case uint32:
		val = int64(v)
	case int64:
		val = int64(v)
	case uint64:
		val, errCode = c.normalizeIntegerFromUint(uint64(v))
	case int:
		val = int64(v)
	case uint:
		val, errCode = c.normalizeIntegerFromUint(uint64(v))
	case float32:
		val, errCode = c.normalizeIntegerFromFloat(float64(v))
	case float64:
		val, errCode = c.normalizeIntegerFromFloat(v)
	case string:
		val, errCode = c.normalizeIntegerFromString(v)
	default:
		errCode = errCodeType
	}
	return
}

func (c *Col) castIntegerValue(val int64, errCode int) (casted interface{}, err error) {
	unsigned := mysql.HasUnsignedFlag(c.Flag)
	var overflow bool
	switch c.Tp {
	case mysql.TypeTiny:
		if unsigned {
			if val > math.MaxUint8 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = uint8(math.MaxUint8)
			} else if val < 0 || errCode == errCodeOverflowLower {
				overflow = true
				casted = uint8(0)
			} else {
				casted = uint8(val)
			}
		} else {
			if val > math.MaxInt8 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = int8(math.MaxInt8)
			} else if val < math.MinInt8 || errCode == errCodeOverflowLower {
				overflow = true
				casted = int8(math.MinInt8)
			} else {
				casted = int8(val)
			}
		}
	case mysql.TypeShort:
		if unsigned {
			if val > math.MaxUint16 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = uint16(math.MaxUint16)
			} else if val < 0 || errCode == errCodeOverflowLower {
				overflow = true
				casted = uint16(0)
			} else {
				casted = uint16(val)
			}
		} else {
			if val > math.MaxInt16 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = int16(math.MaxInt16)
			} else if val < math.MinInt16 || errCode == errCodeOverflowLower {
				overflow = true
				casted = int16(math.MinInt16)
			} else {
				casted = int16(val)
			}
		}
	case mysql.TypeYear:
		if val > int64(mysql.MaxYear) || errCode == errCodeOverflowUpper {
			overflow = true
			casted = mysql.MaxYear
		} else if val < int64(mysql.MinYear) {
			overflow = true
			casted = mysql.MinYear
		} else {
			casted, _ = mysql.AdjustYear(int(val))
		}
	case mysql.TypeInt24:
		if unsigned {
			if val < 0 || errCode == errCodeOverflowLower {
				overflow = true
				casted = uint32(0)
			} else if val > 1<<24-1 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = uint32(1<<24 - 1)
			} else {
				casted = uint32(val)
			}
		} else {
			if val > 1<<23-1 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = int32(1<<23 - 1)
			} else if val < -1<<23 || errCode == errCodeOverflowLower {
				overflow = true
				casted = int32(-1 << 23)
			} else {
				casted = int32(val)
			}
		}
	case mysql.TypeLong:
		if unsigned {
			if val > math.MaxUint32 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = uint32(math.MaxUint32)
			} else if (val < 0 && errCode != errCodeOverflowMaxInt64) || errCode == errCodeOverflowLower {
				overflow = true
				casted = uint32(0)
			} else {
				casted = uint32(val)
			}
		} else {
			if val > math.MaxInt32 || errCode == errCodeOverflowUpper {
				overflow = true
				casted = int32(math.MaxInt32)
			} else if val < math.MinInt32 || errCode == errCodeOverflowLower {
				overflow = true
				casted = int32(math.MinInt32)
			} else {
				casted = int32(val)
			}
		}
	case mysql.TypeLonglong:
		// TypeLonglong overflow has already been handled by normalizeInteger
		if unsigned {
			if errCode == errCodeOverflowUpper {
				overflow = true
				casted = uint64(math.MaxUint64)
			} else if (val < 0 && errCode != errCodeOverflowMaxInt64) || errCode == errCodeOverflowLower {
				overflow = true
				casted = uint64(0)
			} else {
				casted = uint64(val)
			}
		} else {
			if errCode == errCodeOverflowUpper {
				overflow = true
				casted = int64(math.MaxInt64)
			} else if errCode == errCodeOverflowLower {
				overflow = true
				casted = int64(math.MinInt64)
			} else {
				casted = int64(val)
			}
		}
	}
	if overflow {
		err = types.Overflow(val, c.Tp)
	}
	return
}

func (c *Col) castFloatValue(x interface{}) (casted interface{}, err error) {
	var fval float64
	switch v := x.(type) {
	case int8:
		fval = float64(v)
	case uint8:
		fval = float64(v)
	case int16:
		fval = float64(v)
	case uint16:
		fval = float64(v)
	case int32:
		fval = float64(v)
	case uint32:
		fval = float64(v)
	case int64:
		fval = float64(v)
	case uint64:
		fval = float64(v)
	case int:
		fval = float64(v)
	case uint:
		fval = float64(v)
	case float32:
		fval = float64(v)
	case float64:
		fval = v
	case string:
		v = strings.Trim(v, " \t\r\n")
		fval, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return float64(0), c.TypeError(x)
		}
	default:
		return nil, c.TypeError(x)
	}
	switch c.Tp {
	case mysql.TypeFloat:
		if fval < float64(math.MaxFloat32)*(-1) {
			casted = float32(math.MaxFloat32) * -1
		} else if fval > float64(math.MaxFloat32) {
			casted = float32(math.MaxFloat32)
		} else {
			casted = float32(fval)
		}
	case mysql.TypeDouble:
		casted = fval
	}
	return
}

// CastValues casts values based on columns type.
func CastValues(ctx context.Context, rec []interface{}, cols []*Col) (err error) {
	for _, c := range cols {
		rec[c.Offset], err = c.CastValue(ctx, rec[c.Offset])
		if err != nil {
			return
		}
	}
	return
}

// ColDesc describes column information like MySQL desc and show columns do.
type ColDesc struct {
	Field        string
	Type         string
	Collation    string
	Null         string
	Key          string
	DefaultValue interface{}
	Extra        string
	Privileges   string
	Comment      string
}

const defaultPrivileges string = "select,insert,update,references"

func (c *Col) getTypeDesc() string {
	ans := []string{types.FieldTypeToStr(c.Tp, c.Charset)}
	if c.Flen != -1 {
		if c.Decimal == -1 {
			ans = append(ans, fmt.Sprintf("(%d)", c.Flen))
		} else {
			ans = append(ans, fmt.Sprintf("(%d, %d)", c.Flen, c.Decimal))
		}
	}
	if mysql.HasUnsignedFlag(c.Flag) {
		ans = append(ans, "UNSIGNED")
	}
	return strings.Join(ans, " ")
}

// NewColDesc returns a new ColDesc for a column.
func NewColDesc(col *Col) *ColDesc {
	// TODO: if we have no primary key and a unique index which's columns are all not null
	// we will set these columns' flag as PriKeyFlag
	// see https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
	// create table
	name := col.Name
	nullFlag := "YES"
	if mysql.HasNotNullFlag(col.Flag) {
		nullFlag = "NO"
	}
	keyFlag := ""
	if mysql.HasPriKeyFlag(col.Flag) {
		keyFlag = "PRI"
	} else if mysql.HasUniKeyFlag(col.Flag) {
		keyFlag = "UNI"
	} else if mysql.HasMultipleKeyFlag(col.Flag) {
		keyFlag = "MUL"
	}
	var defaultValue interface{}
	if !mysql.HasNoDefaultValueFlag(col.Flag) {
		defaultValue = col.DefaultValue
	}

	extra := ""
	if mysql.HasAutoIncrementFlag(col.Flag) {
		extra = "auto_increment"
	} else if mysql.HasOnUpdateNowFlag(col.Flag) {
		extra = "on update CURRENT_TIMESTAMP"
	}

	return &ColDesc{
		Field:        name.O,
		Type:         col.getTypeDesc(),
		Collation:    col.Collate,
		Null:         nullFlag,
		Key:          keyFlag,
		DefaultValue: defaultValue,
		Extra:        extra,
		Privileges:   defaultPrivileges,
		Comment:      "",
	}
}

// ColDescFieldNames returns the fields name in result set for desc and show columns.
func ColDescFieldNames(full bool) []string {
	if full {
		return []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	}
	return []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
}

// CheckOnce checks if there are duplicated column names in cols.
func CheckOnce(cols []*Col) error {
	m := map[string]struct{}{}
	for _, v := range cols {
		name := v.Name
		_, ok := m[name.L]
		if ok {
			return errors.Errorf("column specified twice - %s", name)
		}

		m[name.L] = struct{}{}
	}

	return nil
}

// CheckNotNull checks if nil value set to a column with NotNull flag is set.
func (c *Col) CheckNotNull(data interface{}) error {
	if mysql.HasNotNullFlag(c.Flag) && data == nil {
		return errors.Errorf("Column %s can't be null.", c.Name)
	}
	return nil
}

// CheckNotNull checks if row has nil value set to a column with NotNull flag set.
func CheckNotNull(cols []*Col, row []interface{}) error {
	for _, c := range cols {
		if err := c.CheckNotNull(row[c.Offset]); err != nil {
			return err
		}
	}
	return nil
}

// FetchValues fetches indexed values from a row.
func (idx *IndexedCol) FetchValues(r []interface{}) ([]interface{}, error) {
	var vals []interface{}
	for _, ic := range idx.Columns {
		if ic.Offset < 0 || ic.Offset > len(r) {
			return nil, errors.New("Index column offset out of bound")
		}
		vals = append(vals, r[ic.Offset])
	}
	return vals, nil
}
