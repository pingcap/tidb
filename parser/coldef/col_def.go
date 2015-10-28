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

package coldef

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See: http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	TableIdent    table.Ident
	IndexColNames []*IndexColName
}

// String implements fmt.Stringer interface.
func (rd *ReferenceDef) String() string {
	cns := make([]string, 0, len(rd.IndexColNames))
	for _, icn := range rd.IndexColNames {
		cns = append(cns, icn.String())
	}
	return fmt.Sprintf("REFERENCES %s (%s)", rd.TableIdent, strings.Join(cns, ", "))
}

// Clone clones a new ReferenceDef from old ReferenceDef.
func (rd *ReferenceDef) Clone() *ReferenceDef {
	cnames := make([]*IndexColName, 0, len(rd.IndexColNames))
	for _, idxColName := range rd.IndexColNames {
		t := *idxColName
		cnames = append(cnames, &t)
	}
	return &ReferenceDef{TableIdent: rd.TableIdent, IndexColNames: cnames}
}

// IndexColName is used for parsing index column name from SQL.
type IndexColName struct {
	ColumnName string
	Length     int
}

// String implements fmt.Stringer interface.
func (icn *IndexColName) String() string {
	if icn.Length >= 0 {
		return fmt.Sprintf("%s(%d)", icn.ColumnName, icn.Length)
	}
	return icn.ColumnName
}

// ColumnDef is used for parsing column definition from SQL.
type ColumnDef struct {
	Name        string
	Tp          *types.FieldType
	Constraints []*ConstraintOpt
}

// String implements fmt.Stringer interface.
func (c *ColumnDef) String() string {
	ans := []string{c.Name}

	for _, x := range c.Constraints {
		ans = append(ans, x.String())
	}
	return strings.Join(ans, " ")
}

func getDefaultValue(c *ConstraintOpt, tp byte, fsp int) (interface{}, error) {
	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
		value, err := expression.GetTimeValue(nil, c.Evalue, tp, fsp)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Value is nil means `default null`.
		if value == nil {
			return nil, nil
		}

		// If value is mysql.Time, convert it to string.
		if vv, ok := value.(mysql.Time); ok {
			return vv.String(), nil
		}

		return value, nil
	}
	v := expression.FastEval(c.Evalue)
	return types.RawData(v), nil
}

func removeOnUpdateNowFlag(c *column.Col) {
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.Flag) {
		c.Flag &= ^uint(mysql.OnUpdateNowFlag)
	}
}

func setTimestampDefaultValue(c *column.Col, hasDefaultValue bool, setOnUpdateNow bool) {
	if hasDefaultValue {
		return
	}

	// For timestamp Col, if is not set default value or not set null, use current timestamp.
	if mysql.HasTimestampFlag(c.Flag) && mysql.HasNotNullFlag(c.Flag) {
		if setOnUpdateNow {
			c.DefaultValue = expression.ZeroTimestamp
		} else {
			c.DefaultValue = expression.CurrentTimestamp
		}
	}
}

func setNoDefaultValueFlag(c *column.Col, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if !mysql.HasNotNullFlag(c.Flag) {
		return
	}

	// Check if it is an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	if !mysql.HasAutoIncrementFlag(c.Flag) && !mysql.HasTimestampFlag(c.Flag) {
		c.Flag |= mysql.NoDefaultValueFlag
	}
}

func checkDefaultValue(c *column.Col, hasDefaultValue bool) error {
	if !hasDefaultValue {
		return nil
	}

	if c.DefaultValue != nil {
		return nil
	}

	// Set not null but default null is invalid.
	if mysql.HasNotNullFlag(c.Flag) {
		return errors.Errorf("invalid default value for %s", c.Name)
	}

	return nil
}

// ColumnDefToCol converts converts ColumnDef to Col and TableConstraints.
func ColumnDefToCol(offset int, colDef *ColumnDef) (*column.Col, []*TableConstraint, error) {
	constraints := []*TableConstraint{}
	col := &column.Col{
		ColumnInfo: model.ColumnInfo{
			Offset:    offset,
			Name:      model.NewCIStr(colDef.Name),
			FieldType: *colDef.Tp,
		},
	}

	// Check and set TimestampFlag and OnUpdateNowFlag.
	if col.Tp == mysql.TypeTimestamp {
		col.Flag |= mysql.TimestampFlag
		col.Flag |= mysql.OnUpdateNowFlag
		col.Flag |= mysql.NotNullFlag
	}

	// If flen is not assigned, assigned it by type.
	if col.Flen == types.UnspecifiedLength {
		col.Flen = mysql.GetDefaultFieldLength(col.Tp)
	}
	if col.Decimal == types.UnspecifiedLength {
		col.Decimal = mysql.GetDefaultDecimal(col.Tp)
	}

	setOnUpdateNow := false
	hasDefaultValue := false
	if colDef.Constraints != nil {
		keys := []*IndexColName{
			{
				colDef.Name,
				colDef.Tp.Flen,
			},
		}
		for _, v := range colDef.Constraints {
			switch v.Tp {
			case ConstrNotNull:
				col.Flag |= mysql.NotNullFlag
			case ConstrNull:
				col.Flag &= ^uint(mysql.NotNullFlag)
				removeOnUpdateNowFlag(col)
			case ConstrAutoIncrement:
				col.Flag |= mysql.AutoIncrementFlag
			case ConstrPrimaryKey:
				constraint := &TableConstraint{Tp: ConstrPrimaryKey, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.PriKeyFlag
			case ConstrUniq:
				constraint := &TableConstraint{Tp: ConstrUniq, ConstrName: colDef.Name, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ConstrIndex:
				constraint := &TableConstraint{Tp: ConstrIndex, ConstrName: colDef.Name, Keys: keys}
				constraints = append(constraints, constraint)
			case ConstrUniqIndex:
				constraint := &TableConstraint{Tp: ConstrUniqIndex, ConstrName: colDef.Name, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ConstrKey:
				constraint := &TableConstraint{Tp: ConstrKey, ConstrName: colDef.Name, Keys: keys}
				constraints = append(constraints, constraint)
			case ConstrUniqKey:
				constraint := &TableConstraint{Tp: ConstrUniqKey, ConstrName: colDef.Name, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ConstrDefaultValue:
				value, err := getDefaultValue(v, colDef.Tp.Tp, colDef.Tp.Decimal)
				if err != nil {
					return nil, nil, errors.Errorf("invalid default value - %s", errors.Trace(err))
				}
				col.DefaultValue = value
				hasDefaultValue = true
				removeOnUpdateNowFlag(col)
			case ConstrOnUpdate:
				if !expression.IsCurrentTimeExpr(v.Evalue) {
					return nil, nil, errors.Errorf("invalid ON UPDATE for - %s", col.Name)
				}

				col.Flag |= mysql.OnUpdateNowFlag
				setOnUpdateNow = true
			case ConstrFulltext:
				// Do nothing.
			case ConstrComment:
				// Do nothing.
			}
		}
	}

	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)

	err := checkDefaultValue(col, hasDefaultValue)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if col.Charset == charset.CharsetBin {
		col.Flag |= mysql.BinaryFlag
	}
	return col, constraints, nil
}
