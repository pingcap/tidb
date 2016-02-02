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
// See the License for the specific language governing permissions and
// limitations under the License.

package perfschema

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// castValues casts values based on columns type.
func castValues(rec []interface{}, cols []*model.ColumnInfo) (err error) {
	for _, c := range cols {
		rec[c.Offset], err = types.Convert(rec[c.Offset], &c.FieldType)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkNotNull checks if nil value set to a column with NotNull flag is set.
func checkNotNull(c *model.ColumnInfo, data interface{}) error {
	if mysql.HasNotNullFlag(c.Flag) && data == nil {
		return errors.Errorf("Column %s can't be null.", c.Name)
	}
	return nil
}

// checkNotNulls checks if row has nil value set to a set of columns with NotNull flag set.
func checkNotNulls(cols []*model.ColumnInfo, row []interface{}) error {
	for _, c := range cols {
		if err := checkNotNull(c, row[c.Offset]); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func decodeValue(data []byte, cols []*model.ColumnInfo) ([]interface{}, error) {
	values, err := codec.Decode(data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(values) != len(cols) {
		return nil, errors.Errorf("Column count does not match, expect %d, actual %d", len(cols), len(values))
	}

	var rvalues []interface{}
	for i, col := range cols {
		// TODO: support more types if we really need.
		switch col.Tp {
		case mysql.TypeString, mysql.TypeVarchar:
			val := string(values[i].([]byte))
			rvalues = append(rvalues, val)
		case mysql.TypeEnum:
			val, err := mysql.ParseEnumValue(col.Elems, values[i].(uint64))
			if err != nil {
				return nil, errors.Trace(err)
			}
			rvalues = append(rvalues, val.String())
		}
	}
	return rvalues, nil
}

// findCol finds column in cols by name.
func findCol(cols []*model.ColumnInfo, name string) *model.ColumnInfo {
	for _, col := range cols {
		if strings.EqualFold(col.Name.O, name) {
			return col
		}
	}
	return nil
}

// findCols finds columns in cols by names.
func findCols(cols []*model.ColumnInfo, names []string) ([]*model.ColumnInfo, error) {
	var rcols []*model.ColumnInfo
	for _, name := range names {
		col := findCol(cols, name)
		if col != nil {
			rcols = append(rcols, col)
		} else {
			return nil, errors.Errorf("unknown column %s", name)
		}
	}

	return rcols, nil
}

// getColDefaultValue gets default value of the column.
func getColDefaultValue(col *model.ColumnInfo) (interface{}, bool, error) {
	// Check no default value flag.
	if mysql.HasNoDefaultValueFlag(col.Flag) && col.Tp != mysql.TypeEnum {
		return nil, false, errors.Errorf("Field '%s' doesn't have a default value", col.Name)
	}

	// Check and get timestamp/datetime default value.
	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		if col.DefaultValue == nil && mysql.HasNotNullFlag(col.Flag) {
			return col.FieldType.Elems[0], true, nil
		}
	}

	return col.DefaultValue, true, nil
}
