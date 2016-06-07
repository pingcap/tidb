// Copyright 2016 The ql Authors. All rights reserved.
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

package table

import (
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// Column provides meta data describing a table column.
type Column struct {
	model.ColumnInfo
}

// PrimaryKeyName defines primary key name.
const PrimaryKeyName = "PRIMARY"

// IndexedColumn defines an index with info.
type IndexedColumn struct {
	model.IndexInfo
	X Index
}

// String implements fmt.Stringer interface.
func (c *Column) String() string {
	ans := []string{c.Name.O, types.TypeToStr(c.Tp, c.Charset)}
	if mysql.HasAutoIncrementFlag(c.Flag) {
		ans = append(ans, "AUTO_INCREMENT")
	}
	if mysql.HasNotNullFlag(c.Flag) {
		ans = append(ans, "NOT NULL")
	}
	return strings.Join(ans, " ")
}

// FindCol finds column in cols by name.
func FindCol(cols []*Column, name string) *Column {
	for _, col := range cols {
		if strings.EqualFold(col.Name.O, name) {
			return col
		}
	}
	return nil
}

// FindCols finds columns in cols by names.
func FindCols(cols []*Column, names []string) ([]*Column, error) {
	var rcols []*Column
	for _, name := range names {
		col := FindCol(cols, name)
		if col != nil {
			rcols = append(rcols, col)
		} else {
			return nil, errUnknownColumn.Gen("unknown column %s", name)
		}
	}

	return rcols, nil
}

// FindOnUpdateCols finds columns which have OnUpdateNow flag.
func FindOnUpdateCols(cols []*Column) []*Column {
	var rcols []*Column
	for _, col := range cols {
		if mysql.HasOnUpdateNowFlag(col.Flag) {
			rcols = append(rcols, col)
		}
	}

	return rcols
}

// CastValues casts values based on columns type.
func CastValues(ctx context.Context, rec []types.Datum, cols []*Column) (err error) {
	for _, c := range cols {
		var converted types.Datum
		converted, err = CastValue(ctx, rec[c.Offset], c)
		if err != nil {
			return errors.Trace(err)
		}
		rec[c.Offset] = converted
	}
	return nil
}

// CastValue casts a value based on column type.
func CastValue(ctx context.Context, val types.Datum, col *Column) (casted types.Datum, err error) {
	casted, err = val.ConvertTo(&col.FieldType)
	if err != nil {
		if variable.GetSessionVars(ctx).StrictSQLMode {
			return casted, errors.Trace(err)
		}
		// TODO: add warnings.
		log.Warnf("cast value error %v", err)
	}
	return casted, nil
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

// GetTypeDesc gets the description for column type.
func (c *Column) GetTypeDesc() string {
	desc := c.FieldType.CompactStr()
	if mysql.HasUnsignedFlag(c.Flag) {
		desc += " UNSIGNED"
	}
	return desc
}

// NewColDesc returns a new ColDesc for a column.
func NewColDesc(col *Column) *ColDesc {
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
		Type:         col.GetTypeDesc(),
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
func CheckOnce(cols []*Column) error {
	m := map[string]struct{}{}
	for _, col := range cols {
		name := col.Name
		_, ok := m[name.L]
		if ok {
			return errDuplicateColumn.Gen("column specified twice - %s", name)
		}

		m[name.L] = struct{}{}
	}

	return nil
}

// CheckNotNull checks if nil value set to a column with NotNull flag is set.
func (c *Column) CheckNotNull(data types.Datum) error {
	if mysql.HasNotNullFlag(c.Flag) && data.IsNull() {
		return errColumnCantNull.Gen("Column %s can't be null.", c.Name)
	}
	return nil
}

// IsPKHandleColumn checks if the column is primary key handle column.
func (c *Column) IsPKHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.Flag) && tbInfo.PKIsHandle
}

// CheckNotNull checks if row has nil value set to a column with NotNull flag set.
func CheckNotNull(cols []*Column, row []types.Datum) error {
	for _, c := range cols {
		if err := c.CheckNotNull(row[c.Offset]); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// FetchValues fetches indexed values from a row.
func (idx *IndexedColumn) FetchValues(r []types.Datum) ([]types.Datum, error) {
	vals := make([]types.Datum, len(idx.Columns))
	for i, ic := range idx.Columns {
		if ic.Offset < 0 || ic.Offset > len(r) {
			return nil, errIndexOutBound.Gen("Index column offset out of bound")
		}
		vals[i] = r[ic.Offset]
	}
	return vals, nil
}

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx context.Context, col *model.ColumnInfo) (types.Datum, bool, error) {
	// Check no default value flag.
	if mysql.HasNoDefaultValueFlag(col.Flag) && col.Tp != mysql.TypeEnum {
		err := errNoDefaultValue.Gen("Field '%s' doesn't have a default value", col.Name)
		if ctx != nil {
			sessVars := variable.GetSessionVars(ctx)
			if !sessVars.StrictSQLMode {
				// TODO: add warning.
				return getZeroValue(col), true, nil
			}
		}
		return types.Datum{}, false, errors.Trace(err)
	}

	// Check and get timestamp/datetime default value.
	if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
		if col.DefaultValue == nil {
			return types.Datum{}, true, nil
		}

		value, err := evaluator.GetTimeValue(ctx, col.DefaultValue, col.Tp, col.Decimal)
		if err != nil {
			return types.Datum{}, true, errGetDefaultFailed.Gen("Field '%s' get default value fail - %s",
				col.Name, errors.Trace(err))
		}
		return value, true, nil
	} else if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		if col.DefaultValue == nil && mysql.HasNotNullFlag(col.Flag) {
			return types.NewDatum(col.FieldType.Elems[0]), true, nil
		}
	}

	return types.NewDatum(col.DefaultValue), true, nil
}

func getZeroValue(col *model.ColumnInfo) types.Datum {
	var d types.Datum
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(col.Flag) {
			d.SetUint64(0)
		} else {
			d.SetInt64(0)
		}
	case mysql.TypeFloat:
		d.SetFloat32(0)
	case mysql.TypeDouble:
		d.SetFloat64(0)
	case mysql.TypeNewDecimal:
		d.SetMysqlDecimal(mysql.NewDecimalFromInt(0, 0))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		d.SetString("")
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetBytes([]byte{})
	case mysql.TypeDuration:
		d.SetMysqlDuration(mysql.ZeroDuration)
	case mysql.TypeDate, mysql.TypeNewDate:
		d.SetMysqlTime(mysql.ZeroDate)
	case mysql.TypeTimestamp:
		d.SetMysqlTime(mysql.ZeroTimestamp)
	case mysql.TypeDatetime:
		d.SetMysqlTime(mysql.ZeroDatetime)
	case mysql.TypeBit:
		d.SetMysqlBit(mysql.Bit{Value: 0, Width: mysql.MinBitWidth})
	case mysql.TypeSet:
		d.SetMysqlSet(mysql.Set{})
	}
	return d
}
