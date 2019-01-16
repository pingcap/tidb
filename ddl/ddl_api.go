// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package ddl

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/schemautil"
	"github.com/pingcap/tidb/util/set"
	log "github.com/sirupsen/logrus"
)

func (d *ddl) CreateSchema(ctx sessionctx.Context, schema model.CIStr, charsetInfo *ast.CharsetOpt) (err error) {
	is := d.GetInformationSchema(ctx)
	_, ok := is.SchemaByName(schema)
	if ok {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(schema)
	}

	if err = checkTooLongSchema(schema); err != nil {
		return errors.Trace(err)
	}

	schemaID, err := d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	dbInfo := &model.DBInfo{
		Name: schema,
	}

	if charsetInfo != nil {
		err = checkCharsetAndCollation(charsetInfo.Chs, charsetInfo.Col)
		if err != nil {
			return errors.Trace(err)
		}
		dbInfo.Charset = charsetInfo.Chs
		dbInfo.Collate = charsetInfo.Col
	} else {
		dbInfo.Charset, dbInfo.Collate = charset.GetDefaultCharsetAndCollate()
	}

	job := &model.Job{
		SchemaID:   schemaID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropSchema(ctx sessionctx.Context, schema model.CIStr) (err error) {
	is := d.GetInformationSchema(ctx)
	old, ok := is.SchemaByName(schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	job := &model.Job{
		SchemaID:   old.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkTooLongSchema(schema model.CIStr) error {
	if len(schema.L) > mysql.MaxDatabaseNameLength {
		return ErrTooLongIdent.GenWithStackByArgs(schema)
	}
	return nil
}

func checkTooLongTable(table model.CIStr) error {
	if len(table.L) > mysql.MaxTableNameLength {
		return ErrTooLongIdent.GenWithStackByArgs(table)
	}
	return nil
}

func checkTooLongIndex(index model.CIStr) error {
	if len(index.L) > mysql.MaxIndexIdentifierLen {
		return ErrTooLongIdent.GenWithStackByArgs(index)
	}
	return nil
}

func setColumnFlagWithConstraint(colMap map[string]*table.Column, v *ast.Constraint) {
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		for _, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			c.Flag |= mysql.PriKeyFlag
			// Primary key can not be NULL.
			c.Flag |= mysql.NotNullFlag
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		for i, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					c.Flag |= mysql.MultipleKeyFlag
				} else {
					c.Flag |= mysql.UniqueKeyFlag
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		for i, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set.
				c.Flag |= mysql.MultipleKeyFlag
			}
		}
	}
}

func buildColumnsAndConstraints(ctx sessionctx.Context, colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint) ([]*table.Column, []*ast.Constraint, error) {
	var cols []*table.Column
	colMap := map[string]*table.Column{}
	// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
	var outPriKeyConstraint *ast.Constraint
	for _, v := range constraints {
		if v.Tp == ast.ConstraintPrimaryKey {
			outPriKeyConstraint = v
			break
		}
	}
	for i, colDef := range colDefs {
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef, outPriKeyConstraint)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		col.State = model.StatePublic
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[colDef.Name.Name.L] = col
	}
	// Traverse table Constraints and set col.flag.
	for _, v := range constraints {
		setColumnFlagWithConstraint(colMap, v)
	}
	return cols, constraints, nil
}

func setCharsetCollationFlenDecimal(tp *types.FieldType) error {
	tp.Charset = strings.ToLower(tp.Charset)
	tp.Collate = strings.ToLower(tp.Collate)
	if len(tp.Charset) == 0 {
		switch tp.Tp {
		case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeEnum, mysql.TypeSet:
			tp.Charset, tp.Collate = charset.GetDefaultCharsetAndCollate()
		default:
			tp.Charset = charset.CharsetBin
			tp.Collate = charset.CharsetBin
		}
	} else {
		if !charset.ValidCharsetAndCollation(tp.Charset, tp.Collate) {
			return errUnsupportedCharset.GenWithStackByArgs(tp.Charset, tp.Collate)
		}
		if len(tp.Collate) == 0 {
			var err error
			tp.Collate, err = charset.GetDefaultCollation(tp.Charset)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	// Use default value for flen or decimal when they are unspecified.
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.Tp)
	if tp.Flen == types.UnspecifiedLength {
		tp.Flen = defaultFlen
		if mysql.HasUnsignedFlag(tp.Flag) && tp.Tp != mysql.TypeLonglong && mysql.IsIntegerType(tp.Tp) {
			// Issue #4684: the flen of unsigned integer(except bigint) is 1 digit shorter than signed integer
			// because it has no prefix "+" or "-" character.
			tp.Flen--
		}
	}
	if tp.Decimal == types.UnspecifiedLength {
		tp.Decimal = defaultDecimal
	}
	return nil
}

// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
func buildColumnAndConstraint(ctx sessionctx.Context, offset int,
	colDef *ast.ColumnDef, outPriKeyConstraint *ast.Constraint) (*table.Column, []*ast.Constraint, error) {
	err := setCharsetCollationFlenDecimal(colDef.Tp)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	col, cts, err := columnDefToCol(ctx, offset, colDef, outPriKeyConstraint)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, cts, nil
}

// checkColumnDefaultValue checks the default value of the column.
// In non-strict SQL mode, if the default value of the column is an empty string, the default value can be ignored.
// In strict SQL mode, TEXT/BLOB/JSON can't have not null default values.
// In NO_ZERO_DATE SQL mode, TIMESTAMP/DATE/DATETIME type can't have zero date like '0000-00-00' or '0000-00-00 00:00:00'.
func checkColumnDefaultValue(ctx sessionctx.Context, col *table.Column, value interface{}) (bool, interface{}, error) {
	hasDefaultValue := true
	if value != nil && (col.Tp == mysql.TypeJSON ||
		col.Tp == mysql.TypeTinyBlob || col.Tp == mysql.TypeMediumBlob ||
		col.Tp == mysql.TypeLongBlob || col.Tp == mysql.TypeBlob) {
		// In non-strict SQL mode.
		if !ctx.GetSessionVars().SQLMode.HasStrictMode() && value == "" {
			if col.Tp == mysql.TypeBlob || col.Tp == mysql.TypeLongBlob {
				// The TEXT/BLOB default value can be ignored.
				hasDefaultValue = false
			}
			// In non-strict SQL mode, if the column type is json and the default value is null, it is initialized to an empty array.
			if col.Tp == mysql.TypeJSON {
				value = `null`
			}
			sc := ctx.GetSessionVars().StmtCtx
			sc.AppendWarning(errBlobCantHaveDefault.GenWithStackByArgs(col.Name.O))
			return hasDefaultValue, value, nil
		}
		// In strict SQL mode or default value is not an empty string.
		return hasDefaultValue, value, errBlobCantHaveDefault.GenWithStackByArgs(col.Name.O)
	}
	if value != nil && ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() &&
		ctx.GetSessionVars().SQLMode.HasStrictMode() && types.IsTypeTime(col.Tp) {
		if vv, ok := value.(string); ok {
			t, err := types.ParseTime(nil, vv, col.Tp, 6)
			if err != nil {
				// Ignores ParseTime error, because ParseTime error has been dealt in getDefaultValue
				// Some builtin function like CURRENT_TIMESTAMP() will cause ParseTime error.
				return hasDefaultValue, value, nil
			}
			if t.Time == types.ZeroTime {
				return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
			}
		}
	}
	return hasDefaultValue, value, nil
}

// isExplicitTimeStamp is used to check if explicit_defaults_for_timestamp is on or off.
// Check out this link for more details.
// https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_explicit_defaults_for_timestamp
func isExplicitTimeStamp() bool {
	// TODO: implement the behavior as MySQL when explicit_defaults_for_timestamp = off, then this function could return false.
	return true
}

// columnDefToCol converts ColumnDef to Col and TableConstraints.
// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
func columnDefToCol(ctx sessionctx.Context, offset int, colDef *ast.ColumnDef, outPriKeyConstraint *ast.Constraint) (*table.Column, []*ast.Constraint, error) {
	var constraints = make([]*ast.Constraint, 0)
	col := table.ToColumn(&model.ColumnInfo{
		Offset:    offset,
		Name:      colDef.Name.Name,
		FieldType: *colDef.Tp,
	})

	if !isExplicitTimeStamp() {
		// Check and set TimestampFlag, OnUpdateNowFlag and NotNullFlag.
		if col.Tp == mysql.TypeTimestamp {
			col.Flag |= mysql.TimestampFlag
			col.Flag |= mysql.OnUpdateNowFlag
			col.Flag |= mysql.NotNullFlag
		}
	}

	setOnUpdateNow := false
	hasDefaultValue := false
	hasNullFlag := false
	if colDef.Options != nil {
		length := types.UnspecifiedLength

		keys := []*ast.IndexColName{
			{
				Column: colDef.Name,
				Length: length,
			},
		}

		for _, v := range colDef.Options {
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				col.Flag |= mysql.NotNullFlag
			case ast.ColumnOptionNull:
				col.Flag &= ^mysql.NotNullFlag
				removeOnUpdateNowFlag(col)
				hasNullFlag = true
			case ast.ColumnOptionAutoIncrement:
				col.Flag |= mysql.AutoIncrementFlag
			case ast.ColumnOptionPrimaryKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.PriKeyFlag
			case ast.ColumnOptionUniqKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ast.ColumnOptionDefaultValue:
				value, err := getDefaultValue(ctx, v, colDef.Tp.Tp, colDef.Tp.Decimal)
				if err != nil {
					return nil, nil, ErrColumnBadNull.GenWithStack("invalid default value - %s", err)
				}
				if hasDefaultValue, value, err = checkColumnDefaultValue(ctx, col, value); err != nil {
					return nil, nil, errors.Trace(err)
				}
				if err = col.SetDefaultValue(value); err != nil {
					return nil, nil, errors.Trace(err)
				}
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionOnUpdate:
				// TODO: Support other time functions.
				if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
					if !expression.IsCurrentTimestampExpr(v.Expr) {
						return nil, nil, ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
					}
				} else {
					return nil, nil, ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
				col.Flag |= mysql.OnUpdateNowFlag
				setOnUpdateNow = true
			case ast.ColumnOptionComment:
				err := setColumnComment(ctx, col, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
			case ast.ColumnOptionGenerated:
				var buf = bytes.NewBuffer([]byte{})
				v.Expr.Format(buf)
				col.GeneratedExprString = buf.String()
				col.GeneratedStored = v.Stored
				_, dependColNames := findDependedColumnNames(colDef)
				col.Dependences = dependColNames
			case ast.ColumnOptionFulltext:
				// TODO: Support this type.
			}
		}
	}

	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)
	if col.FieldType.EvalType().IsStringKind() && col.Charset == charset.CharsetBin {
		col.Flag |= mysql.BinaryFlag
	}
	if col.Tp == mysql.TypeBit {
		// For BIT field, it's charset is binary but does not have binary flag.
		col.Flag &= ^mysql.BinaryFlag
		col.Flag |= mysql.UnsignedFlag
	}
	if col.Tp == mysql.TypeYear {
		// For Year field, it's charset is binary but does not have binary flag.
		col.Flag &= ^mysql.BinaryFlag
		col.Flag |= mysql.ZerofillFlag
	}
	// If you specify ZEROFILL for a numeric column, MySQL automatically adds the UNSIGNED attribute to the column.
	// See https://dev.mysql.com/doc/refman/5.7/en/numeric-type-overview.html for more details.
	// But some types like bit and year, won't show its unsigned flag in `show create table`.
	if mysql.HasZerofillFlag(col.Flag) {
		col.Flag |= mysql.UnsignedFlag
	}
	err := checkPriKeyConstraint(col, hasDefaultValue, hasNullFlag, outPriKeyConstraint)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = checkColumnValueConstraint(col)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = checkDefaultValue(ctx, col, hasDefaultValue)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, constraints, nil
}

func getDefaultValue(ctx sessionctx.Context, c *ast.ColumnOption, tp byte, fsp int) (interface{}, error) {
	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
		vd, err := expression.GetTimeValue(ctx, c.Expr, tp, fsp)
		value := vd.GetValue()
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Value is nil means `default null`.
		if value == nil {
			return nil, nil
		}

		// If value is types.Time, convert it to string.
		if vv, ok := value.(types.Time); ok {
			return vv.String(), nil
		}

		return value, nil
	}
	v, err := expression.EvalAstExpr(ctx, c.Expr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if v.IsNull() {
		return nil, nil
	}

	if v.Kind() == types.KindBinaryLiteral || v.Kind() == types.KindMysqlBit {
		if tp == mysql.TypeBit ||
			tp == mysql.TypeString || tp == mysql.TypeVarchar || tp == mysql.TypeVarString ||
			tp == mysql.TypeBlob || tp == mysql.TypeLongBlob || tp == mysql.TypeMediumBlob || tp == mysql.TypeTinyBlob ||
			tp == mysql.TypeJSON {
			// For BinaryLiteral / string fields, when getting default value we cast the value into BinaryLiteral{}, thus we return
			// its raw string content here.
			return v.GetBinaryLiteral().ToString(), nil
		}
		// For other kind of fields (e.g. INT), we supply its integer value so that it acts as integers.
		return v.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
	}

	if tp == mysql.TypeBit {
		if v.Kind() == types.KindInt64 || v.Kind() == types.KindUint64 {
			// For BIT fields, convert int into BinaryLiteral.
			return types.NewBinaryLiteralFromUint(v.GetUint64(), -1).ToString(), nil
		}
	}

	return v.ToString()
}

func removeOnUpdateNowFlag(c *table.Column) {
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.Flag) {
		c.Flag &= ^mysql.OnUpdateNowFlag
	}
}

func setTimestampDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	if hasDefaultValue {
		return
	}

	// For timestamp Col, if is not set default value or not set null, use current timestamp.
	if mysql.HasTimestampFlag(c.Flag) && mysql.HasNotNullFlag(c.Flag) {
		if setOnUpdateNow {
			if err := c.SetDefaultValue(types.ZeroDatetimeStr); err != nil {
				log.Error(errors.ErrorStack(err))
			}
		} else {
			if err := c.SetDefaultValue(strings.ToUpper(ast.CurrentTimestamp)); err != nil {
				log.Error(errors.ErrorStack(err))
			}

		}
	}
}

func setNoDefaultValueFlag(c *table.Column, hasDefaultValue bool) {
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

func checkDefaultValue(ctx sessionctx.Context, c *table.Column, hasDefaultValue bool) error {
	if !hasDefaultValue {
		return nil
	}

	if c.GetDefaultValue() != nil {
		if _, err := table.GetColDefaultValue(ctx, c.ToInfo()); err != nil {
			return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
		}
		return nil
	}
	// Primary key default null is invalid.
	if mysql.HasPriKeyFlag(c.Flag) {
		return ErrPrimaryCantHaveNull
	}

	// Set not null but default null is invalid.
	if mysql.HasNotNullFlag(c.Flag) {
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}

	return nil
}

// checkPriKeyConstraint check all parts of a PRIMARY KEY must be NOT NULL
func checkPriKeyConstraint(col *table.Column, hasDefaultValue, hasNullFlag bool, outPriKeyConstraint *ast.Constraint) error {
	// Primary key should not be null.
	if mysql.HasPriKeyFlag(col.Flag) && hasDefaultValue && col.GetDefaultValue() == nil {
		return types.ErrInvalidDefault.GenWithStackByArgs(col.Name)
	}
	// Set primary key flag for outer primary key constraint.
	// Such as: create table t1 (id int , age int, primary key(id))
	if !mysql.HasPriKeyFlag(col.Flag) && outPriKeyConstraint != nil {
		for _, key := range outPriKeyConstraint.Keys {
			if key.Column.Name.L != col.Name.L {
				continue
			}
			col.Flag |= mysql.PriKeyFlag
			break
		}
	}
	// Primary key should not be null.
	if mysql.HasPriKeyFlag(col.Flag) && hasNullFlag {
		return ErrPrimaryCantHaveNull
	}
	return nil
}

func checkColumnValueConstraint(col *table.Column) error {
	if col.Tp != mysql.TypeEnum && col.Tp != mysql.TypeSet {
		return nil
	}
	valueMap := make(map[string]string, len(col.Elems))
	for i := range col.Elems {
		val := strings.ToLower(col.Elems[i])
		if _, ok := valueMap[val]; ok {
			tpStr := "ENUM"
			if col.Tp == mysql.TypeSet {
				tpStr = "SET"
			}
			return types.ErrDuplicatedValueInType.GenWithStackByArgs(col.Name, valueMap[val], tpStr)
		}
		valueMap[val] = col.Elems[i]
	}
	return nil
}

func checkDuplicateColumn(cols []interface{}) error {
	colNames := set.StringSet{}
	var nameLower string
	for _, col := range cols {
		switch x := col.(type) {
		case *ast.ColumnDef:
			nameLower = x.Name.Name.L
		case model.CIStr:
			nameLower = x.L
		default:
			nameLower = ""
		}
		if colNames.Exist(nameLower) {
			return infoschema.ErrColumnExists.GenWithStackByArgs(nameLower)
		}
		colNames.Insert(nameLower)
	}
	return nil
}

func checkGeneratedColumn(colDefs []*ast.ColumnDef) error {
	var colName2Generation = make(map[string]columnGenerationInDDL, len(colDefs))
	for i, colDef := range colDefs {
		generated, depCols := findDependedColumnNames(colDef)
		if !generated {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   true,
				dependences: depCols,
			}
		}
	}
	for _, colDef := range colDefs {
		colName := colDef.Name.Name.L
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkTooLongColumn(cols []interface{}) error {
	var colName string
	for _, col := range cols {
		switch x := col.(type) {
		case *ast.ColumnDef:
			colName = x.Name.Name.O
		case model.CIStr:
			colName = x.O
		default:
			colName = ""
		}
		if len(colName) > mysql.MaxColumnNameLength {
			return ErrTooLongIdent.GenWithStackByArgs(colName)
		}
	}
	return nil
}

func checkTooManyColumns(colDefs []*ast.ColumnDef) error {
	if uint32(len(colDefs)) > atomic.LoadUint32(&TableColumnCountLimit) {
		return errTooManyFields
	}
	return nil
}

// checkColumnsAttributes checks attributes for multiple columns.
func checkColumnsAttributes(colDefs []*ast.ColumnDef) error {
	for _, colDef := range colDefs {
		if err := checkColumnAttributes(colDef.Name.OrigColName(), colDef.Tp); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkColumnFieldLength check the maximum length limit for different character set varchar type columns.
func checkColumnFieldLength(schema *model.DBInfo, colDefs []*ast.ColumnDef, tbInfo *model.TableInfo) error {
	for _, colDef := range colDefs {
		if colDef.Tp.Tp == mysql.TypeVarchar {
			var setCharset string
			setCharset = mysql.DefaultCharset
			if len(schema.Charset) != 0 {
				setCharset = schema.Charset
			}
			if len(tbInfo.Charset) != 0 {
				setCharset = tbInfo.Charset
			}

			err := IsTooBigFieldLength(colDef.Tp.Flen, colDef.Name.Name.O, setCharset)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// IsTooBigFieldLength check if the varchar type column exceeds the maximum length limit.
func IsTooBigFieldLength(colDefTpFlen int, colDefName, setCharset string) error {
	desc, err := charset.GetCharsetDesc(setCharset)
	if err != nil {
		return errors.Trace(err)
	}
	maxFlen := mysql.MaxFieldVarCharLength
	maxFlen /= desc.Maxlen
	if colDefTpFlen != types.UnspecifiedLength && colDefTpFlen > maxFlen {
		return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDefName, maxFlen)
	}
	return nil
}

// checkColumnAttributes check attributes for single column.
func checkColumnAttributes(colName string, tp *types.FieldType) error {
	switch tp.Tp {
	case mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeFloat:
		if tp.Flen < tp.Decimal {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colName)
		}
	case mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
		if tp.Decimal != types.UnspecifiedFsp && (tp.Decimal < types.MinFsp || tp.Decimal > types.MaxFsp) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Decimal, colName, types.MaxFsp)
		}
	}
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, foreign bool) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		if foreign {
			return infoschema.ErrCannotAddForeign
		}
		return ErrDupKeyName.GenWithStack("duplicate key name %s", name)
	}
	namesMap[nameLower] = true
	return nil
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint, foreign bool) {
	if constr.Name == "" && len(constr.Keys) > 0 {
		colName := constr.Keys[0].Column.Name.L
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, mysql.PrimaryKeyName) {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			// We loop forever until we find constrName that haven't been used.
			if foreign {
				constrName = fmt.Sprintf("fk_%s_%d", colName, i)
			} else {
				constrName = fmt.Sprintf("%s_%d", colName, i)
			}
			i++
		}
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func checkConstraintNames(constraints []*ast.Constraint) error {
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			err := checkDuplicateConstraint(fkNames, constr.Name, true)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			err := checkDuplicateConstraint(constrNames, constr.Name, false)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Set empty constraint names.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			setEmptyConstraintName(fkNames, constr, true)
		} else {
			setEmptyConstraintName(constrNames, constr, false)
		}
	}

	return nil
}

func buildTableInfo(ctx sessionctx.Context, d *ddl, tableName model.CIStr, cols []*table.Column, constraints []*ast.Constraint) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name: tableName,
	}
	// When this function is called by MockTableInfo, we should set a particular table id.
	// So the `ddl` structure may be nil.
	if d != nil {
		tbInfo.ID, err = d.genGlobalID()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, v := range cols {
		v.ID = allocateColumnID(tbInfo)
		tbInfo.Columns = append(tbInfo.Columns, v.ToInfo())
	}
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			for _, fk := range tbInfo.ForeignKeys {
				if fk.Name.L == strings.ToLower(constr.Name) {
					return nil, infoschema.ErrCannotAddForeign
				}
			}
			var fk model.FKInfo
			fk.Name = model.NewCIStr(constr.Name)
			fk.RefTable = constr.Refer.Table.Name
			fk.State = model.StatePublic
			for _, key := range constr.Keys {
				if table.FindCol(cols, key.Column.Name.O) == nil {
					return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
				}
				fk.Cols = append(fk.Cols, key.Column.Name)
			}
			for _, key := range constr.Refer.IndexColNames {
				fk.RefCols = append(fk.RefCols, key.Column.Name)
			}
			fk.OnDelete = int(constr.Refer.OnDelete.ReferOpt)
			fk.OnUpdate = int(constr.Refer.OnUpdate.ReferOpt)
			if len(fk.Cols) != len(fk.RefCols) {
				return nil, infoschema.ErrForeignKeyNotMatch.GenWithStackByArgs(tbInfo.Name.O)
			}
			if len(fk.Cols) == 0 {
				// TODO: In MySQL, this case will report a parse error.
				return nil, infoschema.ErrCannotAddForeign
			}
			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, &fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			var col *table.Column
			for _, key := range constr.Keys {
				col = table.FindCol(cols, key.Column.Name.O)
				if col == nil {
					return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
				}
				// Virtual columns cannot be used in primary key.
				if col.IsGenerated() && !col.GeneratedStored {
					return nil, errUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
				}
			}
			if len(constr.Keys) == 1 {
				switch col.Tp {
				case mysql.TypeLong, mysql.TypeLonglong,
					mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
					tbInfo.PKIsHandle = true
					// Avoid creating index for PK handle column.
					continue
				}
			}
		}
		// build index info.
		idxInfo, err := buildIndexInfo(tbInfo, model.NewCIStr(constr.Name), constr.Keys, model.StatePublic)
		if err != nil {
			return nil, errors.Trace(err)
		}
		//check if the index is primary or uniqiue.
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			idxInfo.Primary = true
			idxInfo.Unique = true
			idxInfo.Name = model.NewCIStr(mysql.PrimaryKeyName)
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			idxInfo.Unique = true
		}
		// set index type.
		if constr.Option != nil {
			idxInfo.Comment, err = validateCommentLength(ctx.GetSessionVars(),
				constr.Option.Comment,
				maxCommentLength,
				errTooLongIndexComment.GenWithStackByArgs(idxInfo.Name.String(), maxCommentLength))
			if err != nil {
				return nil, errors.Trace(err)
			}
			if constr.Option.Tp == model.IndexTypeInvalid {
				// Use btree as default index type.
				idxInfo.Tp = model.IndexTypeBtree
			} else {
				idxInfo.Tp = constr.Option.Tp
			}
		} else {
			// Use btree as default index type.
			idxInfo.Tp = model.IndexTypeBtree
		}
		idxInfo.ID = allocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return
}

func (d *ddl) CreateTableWithLike(ctx sessionctx.Context, ident, referIdent ast.Ident, ifNotExists bool) error {
	is := d.GetInformationSchema(ctx)
	_, ok := is.SchemaByName(referIdent.Schema)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
	}
	referTbl, err := is.TableByName(referIdent.Schema, referIdent.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
	}
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	if is.TableExists(ident.Schema, ident.Name) {
		if ifNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrTableExists.GenWithStackByArgs(ident))
			return nil
		}
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}

	tblInfo := *referTbl.Meta()
	tblInfo.Name = ident.Name
	tblInfo.AutoIncID = 0
	tblInfo.ForeignKeys = nil
	tblInfo.ID, err = d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// BuildTableInfoFromAST builds model.TableInfo from a SQL statement.
// The SQL string should be a create table statement.
// Don't use this function to build a partitioned table.
func BuildTableInfoFromAST(s *ast.CreateTableStmt) (*model.TableInfo, error) {
	return buildTableInfoWithCheck(mock.NewContext(), nil, s)
}

func buildTableInfoWithCheck(ctx sessionctx.Context, d *ddl, s *ast.CreateTableStmt) (*model.TableInfo, error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	colDefs := s.Cols
	var colObjects []interface{}
	for _, col := range colDefs {
		colObjects = append(colObjects, col)
	}
	if err := checkTooLongTable(ident.Name); err != nil {
		return nil, errors.Trace(err)
	}
	if err := checkDuplicateColumn(colObjects); err != nil {
		return nil, errors.Trace(err)
	}
	if err := checkGeneratedColumn(colDefs); err != nil {
		return nil, errors.Trace(err)
	}
	if err := checkTooLongColumn(colObjects); err != nil {
		return nil, errors.Trace(err)
	}
	if err := checkTooManyColumns(colDefs); err != nil {
		return nil, errors.Trace(err)
	}

	if err := checkColumnsAttributes(colDefs); err != nil {
		return nil, errors.Trace(err)
	}

	cols, newConstraints, err := buildColumnsAndConstraints(ctx, colDefs, s.Constraints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = checkConstraintNames(newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tbInfo *model.TableInfo
	tbInfo, err = buildTableInfo(ctx, d, ident.Name, cols, newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pi, err := buildTablePartitionInfo(ctx, d, s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if pi != nil {
		switch pi.Type {
		case model.PartitionTypeRange:
			err = checkPartitionByRange(ctx, tbInfo, pi, s, cols, newConstraints)
		case model.PartitionTypeHash:
			err = checkPartitionByHash(pi)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err = checkRangePartitioningKeysConstraints(ctx, s, tbInfo, newConstraints); err != nil {
			return nil, errors.Trace(err)
		}
		tbInfo.Partition = pi
	}
	return tbInfo, nil
}

func (d *ddl) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) (err error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		return d.CreateTableWithLike(ctx, ident, referIdent, s.IfNotExists)
	}
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	if is.TableExists(ident.Schema, ident.Name) {
		if s.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrTableExists.GenWithStackByArgs(ident))
			return nil
		}
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}

	tbInfo, err := buildTableInfoWithCheck(ctx, d, s)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo},
	}

	err = handleTableOptions(s.Options, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate)
	if err != nil {
		return errors.Trace(err)
	}
	if err = checkColumnFieldLength(schema, s.Cols, tbInfo); err != nil {
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	if err == nil {
		if tbInfo.AutoIncID > 1 {
			// Default tableAutoIncID base is 0.
			// If the first ID is expected to greater than 1, we need to do rebase.
			err = d.handleAutoIncID(tbInfo, schema.ID)
		}
	}

	// table exists, but if_not_exists flags is true, so we ignore this error.
	if infoschema.ErrTableExists.Equal(err) && s.IfNotExists {
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) RestoreTable(ctx sessionctx.Context, tbInfo *model.TableInfo, schemaID, autoID, dropJobID int64, snapshotTS uint64) (err error) {
	is := d.GetInformationSchema(ctx)
	// Check schema exist.
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
		))
	}
	// Check not exist table with same name.
	if ok := is.TableExists(schema.Name, tbInfo.Name); ok {
		return infoschema.ErrTableExists.GenWithStackByArgs(tbInfo.Name)
	}

	tbInfo.State = model.StateNone
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tbInfo.ID,
		Type:       model.ActionRestoreTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo, autoID, dropJobID, snapshotTS, restoreTableCheckFlagNone},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) (err error) {
	ident := ast.Ident{Name: s.ViewName.Name, Schema: s.ViewName.Schema}
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	if is.TableExists(ident.Schema, ident.Name) {
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}
	if err = checkTooLongTable(ident.Name); err != nil {
		return err
	}
	viewInfo, cols := buildViewInfoWithTableColumns(ctx, s)

	var colObjects []interface{}
	for _, col := range viewInfo.Cols {
		colObjects = append(colObjects, col)
	}

	if err = checkTooLongColumn(colObjects); err != nil {
		return err
	}
	if err = checkDuplicateColumn(colObjects); err != nil {
		return err
	}

	tbInfo, err := buildTableInfo(ctx, d, ident.Name, cols, nil)
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo, s.OrReplace},
	}
	if v, ok := ctx.GetSessionVars().GetSystemVar("character_set_client"); ok {
		tbInfo.Charset = v
	}
	if v, ok := ctx.GetSessionVars().GetSystemVar("collation_connection"); ok {
		tbInfo.Collate = v
	}
	err = checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.doDDLJob(ctx, job)

	return d.callHookOnChanged(err)
}

func buildViewInfoWithTableColumns(ctx sessionctx.Context, s *ast.CreateViewStmt) (*model.ViewInfo, []*table.Column) {
	viewInfo := &model.ViewInfo{Definer: s.Definer, Algorithm: s.Algorithm,
		Security: s.Security, SelectStmt: s.Select.Text(), CheckOption: s.CheckOption}

	if s.Definer.CurrentUser {
		viewInfo.Definer = ctx.GetSessionVars().User
	}

	var schemaCols = s.Select.(*ast.SelectStmt).Fields.Fields
	viewInfo.Cols = make([]model.CIStr, len(schemaCols))
	for i, v := range schemaCols {
		viewInfo.Cols[i] = v.AsName
	}

	var tableColumns = make([]*table.Column, len(schemaCols))
	if s.Cols == nil {
		for i, v := range schemaCols {
			tableColumns[i] = table.ToColumn(&model.ColumnInfo{
				Name:   v.AsName,
				ID:     int64(i),
				Offset: i,
				State:  model.StatePublic,
			})
		}
	} else {
		for i, v := range s.Cols {
			tableColumns[i] = table.ToColumn(&model.ColumnInfo{
				Name:   v,
				ID:     int64(i),
				Offset: i,
				State:  model.StatePublic,
			})
		}
	}

	return viewInfo, tableColumns
}

func checkPartitionByHash(pi *model.PartitionInfo) error {
	if err := checkAddPartitionTooManyPartitions(pi.Num); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkPartitionByRange(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo, s *ast.CreateTableStmt, cols []*table.Column, newConstraints []*ast.Constraint) error {
	// Range columns partition only implements the parser, so it will not be checked.
	if s.Partition.ColumnNames != nil {
		return nil
	}

	if err := checkPartitionNameUnique(tbInfo, pi); err != nil {
		return errors.Trace(err)
	}

	if err := checkCreatePartitionValue(ctx, tbInfo, pi, cols); err != nil {
		return errors.Trace(err)
	}

	if err := checkAddPartitionTooManyPartitions(uint64(len(pi.Definitions))); err != nil {
		return errors.Trace(err)
	}

	if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
		return errors.Trace(err)
	}

	if err := checkPartitionFuncType(ctx, s, cols, tbInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkCharsetAndCollation(cs string, co string) error {
	if !charset.ValidCharsetAndCollation(cs, co) {
		return ErrUnknownCharacterSet.GenWithStackByArgs(cs)
	}
	return nil
}

// handleAutoIncID handles auto_increment option in DDL. It creates a ID counter for the table and initiates the counter to a proper value.
// For example if the option sets auto_increment to 10. The counter will be set to 9. So the next allocated ID will be 10.
func (d *ddl) handleAutoIncID(tbInfo *model.TableInfo, schemaID int64) error {
	alloc := autoid.NewAllocator(d.store, tbInfo.GetDBID(schemaID), tbInfo.IsAutoIncColUnsigned())
	tbInfo.State = model.StatePublic
	tb, err := table.TableFromMeta(alloc, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	if err = tb.RebaseAutoID(nil, tbInfo.AutoIncID-1, false); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func setDefaultTableCharsetAndCollation(tbInfo *model.TableInfo) (err error) {
	if len(tbInfo.Charset) == 0 {
		tbInfo.Charset = mysql.DefaultCharset
	}

	if len(tbInfo.Collate) == 0 {
		tbInfo.Collate, err = charset.GetDefaultCollation(tbInfo.Charset)
	}
	return
}

// handleTableOptions updates tableInfo according to table options.
func handleTableOptions(options []*ast.TableOption, tbInfo *model.TableInfo) error {
	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionComment:
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCharset:
			tbInfo.Charset = op.StrValue
		case ast.TableOptionCollate:
			tbInfo.Collate = op.StrValue
		case ast.TableOptionCompression:
			tbInfo.Compression = op.StrValue
		case ast.TableOptionShardRowID:
			if hasAutoIncrementColumn(tbInfo) && op.UintValue != 0 {
				return errUnsupportedShardRowIDBits
			}
			tbInfo.ShardRowIDBits = op.UintValue
			if tbInfo.ShardRowIDBits > shardRowIDBitsMax {
				tbInfo.ShardRowIDBits = shardRowIDBitsMax
			}
		}
	}

	if err := setDefaultTableCharsetAndCollation(tbInfo); err != nil {
		log.Error(errors.ErrorStack(err))
	}
	return nil
}

func hasAutoIncrementColumn(tbInfo *model.TableInfo) bool {
	for _, col := range tbInfo.Columns {
		if mysql.HasAutoIncrementFlag(col.Flag) {
			return true
		}
	}
	return false
}

// isIgnorableSpec checks if the spec type is ignorable.
// Some specs are parsed by ignored. This is for compatibility.
func isIgnorableSpec(tp ast.AlterTableType) bool {
	// AlterTableLock/AlterTableAlgorithm are ignored.
	return tp == ast.AlterTableLock || tp == ast.AlterTableAlgorithm
}

// getCharsetAndCollateInTableOption will iterate the charset and collate in the options,
// and returns the last charset and collate in options. If there is no charset in the options,
// the returns charset will be "", the same as collate.
func getCharsetAndCollateInTableOption(startIdx int, options []*ast.TableOption) (charset, collate string) {
	charsets := make([]string, len(options))
	collates := make([]string, len(options))
	for i := startIdx; i < len(options); i++ {
		opt := options[i]
		// we set the charset to the last option. example: alter table t charset latin1 charset utf8 collate utf8_bin;
		// the charset will be utf8, collate will be utf8_bin
		switch opt.Tp {
		case ast.TableOptionCharset:
			charsets = append(charsets, opt.StrValue)
		case ast.TableOptionCollate:
			collates = append(collates, opt.StrValue)
		}
	}

	if len(charsets) != 0 {
		charset = charsets[len(charsets)-1]
	}

	if len(collates) != 0 {
		collate = collates[len(collates)-1]
	}
	return
}

func (d *ddl) AlterTable(ctx sessionctx.Context, ident ast.Ident, specs []*ast.AlterTableSpec) (err error) {
	// Only handle valid specs.
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	for _, spec := range specs {
		if isIgnorableSpec(spec.Tp) {
			continue
		}
		validSpecs = append(validSpecs, spec)
	}

	if len(validSpecs) != 1 {
		// TODO: Hanlde len(validSpecs) == 0.
		// Now we only allow one schema changing at the same time.
		return errRunMultiSchemaChanges
	}

	for _, spec := range validSpecs {
		var handledCharsetOrCollate bool
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			if len(spec.NewColumns) != 1 {
				return errRunMultiSchemaChanges
			}
			err = d.AddColumn(ctx, ident, spec)
		case ast.AlterTableAddPartitions:
			err = d.AddTablePartitions(ctx, ident, spec)
		case ast.AlterTableCoalescePartitions:
			err = d.CoalescePartitions(ctx, ident, spec)
		case ast.AlterTableDropColumn:
			err = d.DropColumn(ctx, ident, spec.OldColumnName.Name)
		case ast.AlterTableDropIndex:
			err = d.DropIndex(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableDropPartition:
			err = d.DropTablePartition(ctx, ident, spec)
		case ast.AlterTableTruncatePartition:
			err = d.TruncateTablePartition(ctx, ident, spec)
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = d.CreateIndex(ctx, ident, false, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = d.CreateIndex(ctx, ident, true, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintForeignKey:
				err = d.CreateForeignKey(ctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				err = ErrUnsupportedModifyPrimaryKey.GenWithStackByArgs("add")
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			err = d.DropForeignKey(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			err = d.ModifyColumn(ctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = d.ChangeColumn(ctx, ident, spec)
		case ast.AlterTableAlterColumn:
			err = d.AlterColumn(ctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			isAlterTable := true
			err = d.RenameTable(ctx, ident, newIdent, isAlterTable)
		case ast.AlterTableDropPrimaryKey:
			err = ErrUnsupportedModifyPrimaryKey.GenWithStackByArgs("drop")
		case ast.AlterTableRenameIndex:
			err = d.RenameIndex(ctx, ident, spec)
		case ast.AlterTableOption:
			for i, opt := range spec.Options {
				switch opt.Tp {
				case ast.TableOptionShardRowID:
					if opt.UintValue > shardRowIDBitsMax {
						opt.UintValue = shardRowIDBitsMax
					}
					err = d.ShardRowID(ctx, ident, opt.UintValue)
				case ast.TableOptionAutoIncrement:
					err = d.RebaseAutoID(ctx, ident, int64(opt.UintValue))
				case ast.TableOptionComment:
					spec.Comment = opt.StrValue
					err = d.AlterTableComment(ctx, ident, spec)
				case ast.TableOptionCharset, ast.TableOptionCollate:
					// getCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						continue
					}
					toCharset, toCollate := getCharsetAndCollateInTableOption(i, spec.Options)
					err = d.AlterTableCharsetAndCollate(ctx, ident, toCharset, toCollate)
					handledCharsetOrCollate = true
				}

				if err != nil {
					return errors.Trace(err)
				}
			}
		default:
			// Nothing to do now.
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) RebaseAutoID(ctx sessionctx.Context, ident ast.Ident, newBase int64) error {
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	autoIncID, err := t.Allocator(ctx).NextGlobalAutoID(t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	newBase = mathutil.MaxInt64(newBase, autoIncID)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionRebaseAutoID,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newBase},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ShardRowID shards the implicit row ID by adding shard value to the row ID's first few bits.
func (d *ddl) ShardRowID(ctx sessionctx.Context, tableIdent ast.Ident, uVal uint64) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, tableIdent)
	if err != nil {
		return errors.Trace(err)
	}
	if hasAutoIncrementColumn(t.Meta()) && uVal != 0 {
		return errUnsupportedShardRowIDBits
	}
	job := &model.Job{
		Type:       model.ActionShardRowID,
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{uVal},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) getSchemaAndTableByIdent(ctx sessionctx.Context, tableIdent ast.Ident) (dbInfo *model.DBInfo, t table.Table, err error) {
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(tableIdent.Schema)
	if !ok {
		return nil, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(tableIdent.Schema)
	}
	t, err = is.TableByName(tableIdent.Schema, tableIdent.Name)
	if err != nil {
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableIdent.Schema, tableIdent.Name)
	}
	return schema, t, nil
}

func checkColumnConstraint(col *ast.ColumnDef, ti ast.Ident) error {
	for _, constraint := range col.Options {
		switch constraint.Tp {
		case ast.ColumnOptionAutoIncrement:
			return errUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint AUTO_INCREMENT when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionPrimaryKey:
			return errUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint PRIMARY KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionUniqKey:
			return errUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint UNIQUE KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		}
	}

	return nil
}

// AddColumn will add a new column to the table.
func (d *ddl) AddColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	// Check whether the added column constraints are supported.
	err := checkColumnConstraint(specNewColumn, ti)
	if err != nil {
		return errors.Trace(err)
	}

	colName := specNewColumn.Name.Name.O
	if err = checkColumnAttributes(colName, specNewColumn.Tp); err != nil {
		return errors.Trace(err)
	}

	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + 1); err != nil {
		return errors.Trace(err)
	}
	// Check whether added column has existed.
	col := table.FindCol(t.Cols(), colName)
	if col != nil {
		return infoschema.ErrColumnExists.GenWithStackByArgs(colName)
	}

	// If new column is a generated column, do validation.
	// NOTE: Because now we can only append columns to table,
	// we dont't need check whether the column refers other
	// generated columns occurring later in table.
	for _, option := range specNewColumn.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			referableColNames := make(map[string]struct{}, len(t.Cols()))
			for _, col := range t.Cols() {
				referableColNames[col.Name.L] = struct{}{}
			}
			_, dependColNames := findDependedColumnNames(specNewColumn)
			if err = columnNamesCover(referableColNames, dependColNames); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if len(colName) > mysql.MaxColumnNameLength {
		return ErrTooLongIdent.GenWithStackByArgs(colName)
	}

	// Ingore table constraints now, maybe return error later.
	// We use length(t.Cols()) as the default offset firstly, we will change the
	// column's offset later.
	col, _, err = buildColumnAndConstraint(ctx, len(t.Cols()), specNewColumn, nil)
	if err != nil {
		return errors.Trace(err)
	}
	col.OriginDefaultValue, err = generateOriginDefaultValue(col.ToInfo())
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col, spec.Position, 0},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AddTablePartitions will add a new partition to the table.
func (d *ddl) AddTablePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}
	// We don't support add hash type partition now.
	if meta.Partition.Type == model.PartitionTypeHash {
		return errors.Trace(ErrUnsupportedAddPartition)
	}

	partInfo, err := buildPartitionInfo(meta, d, spec)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkAddPartitionTooManyPartitions(uint64(len(meta.Partition.Definitions) + len(partInfo.Definitions)))
	if err != nil {
		return errors.Trace(err)
	}

	err = checkPartitionNameUnique(meta, partInfo)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkAddPartitionValue(meta, partInfo)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		Type:       model.ActionAddTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// CoalescePartitions coalesce partitions can be used with a table that is partitioned by hash or key to reduce the number of partitions by number.
func (d *ddl) CoalescePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	// Coalesce partition can only be used on hash/key partitions.
	if meta.Partition.Type == model.PartitionTypeRange {
		return errors.Trace(ErrCoalesceOnlyOnHashPartition)
	}

	// We don't support coalesce partitions hash type partition now.
	if meta.Partition.Type == model.PartitionTypeHash {
		return errors.Trace(ErrUnsupportedCoalescePartition)
	}

	return errors.Trace(err)
}

func (d *ddl) TruncateTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	var pid int64
	pid, err = tables.FindPartitionByName(meta, spec.Name)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		Type:       model.ActionTruncateTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{pid},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}
	err = checkDropTablePartition(meta, spec.Name)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		Type:       model.ActionDropTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{spec.Name},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (d *ddl) DropColumn(ctx sessionctx.Context, ti ast.Ident, colName model.CIStr) error {
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	// Check whether dropped column has existed.
	col := table.FindCol(t.Cols(), colName.L)
	if col == nil {
		return ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}

	tblInfo := t.Meta()
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		return errors.Trace(err)
	}
	// We don't support dropping column with PK handle covered now.
	if col.IsPKHandleColumn(tblInfo) {
		return errUnsupportedPKHandle
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// modifiableCharsetAndCollation returns error when the charset or collation is not modifiable.
func modifiableCharsetAndCollation(toCharset, toCollate, origCharset, origCollate string) error {
	if !charset.ValidCharsetAndCollation(toCharset, toCollate) {
		return ErrUnknownCharacterSet.GenWithStackByArgs(toCharset, toCollate)
	}

	if toCharset == charset.CharsetUTF8MB4 || (toCharset == charset.CharsetUTF8 && origCharset != charset.CharsetUTF8MB4) {
		// TiDB treats all the data as utf8mb4, so we support changing the charset to utf8mb4.
		// And not allow to change utf8mb4 to utf8.
		return nil
	}

	if toCharset != origCharset {
		msg := fmt.Sprintf("charset from %s to %s", origCharset, toCharset)
		return errUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	if toCollate != origCollate {
		msg := fmt.Sprintf("collate from %s to %s", origCollate, toCollate)
		return errUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	return nil
}

// modifiable checks if the 'origin' type can be modified to 'to' type with out the need to
// change or check existing data in the table.
// It returns true if the two types has the same Charset and Collation, the same sign, both are
// integer types or string types, and new Flen and Decimal must be greater than or equal to origin.
func modifiable(origin *types.FieldType, to *types.FieldType) error {
	if to.Flen > 0 && to.Flen < origin.Flen {
		msg := fmt.Sprintf("length %d is less than origin %d", to.Flen, origin.Flen)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	if to.Decimal > 0 && to.Decimal < origin.Decimal {
		msg := fmt.Sprintf("decimal %d is less than origin %d", to.Decimal, origin.Decimal)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	if err := modifiableCharsetAndCollation(to.Charset, to.Collate, origin.Charset, origin.Collate); err != nil {
		return errors.Trace(err)
	}

	toUnsigned := mysql.HasUnsignedFlag(to.Flag)
	originUnsigned := mysql.HasUnsignedFlag(origin.Flag)
	if originUnsigned != toUnsigned {
		msg := fmt.Sprintf("unsigned %v not match origin %v", toUnsigned, originUnsigned)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	switch origin.Tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch to.Tp {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return nil
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch to.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return nil
		}
	case mysql.TypeEnum:
		if origin.Tp == to.Tp {
			if len(to.Elems) < len(origin.Elems) {
				msg := fmt.Sprintf("the number of enum column's elements is less than the original: %d", len(origin.Elems))
				return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
			}
			for index, originElem := range origin.Elems {
				toElem := to.Elems[index]
				if originElem != toElem {
					msg := fmt.Sprintf("cannot modify enum column value %s to %s", originElem, toElem)
					return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
				}
			}
			return nil
		}
		msg := fmt.Sprintf("cannot modify enum type column's to type %s", to.String())
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	default:
		if origin.Tp == to.Tp {
			return nil
		}
	}
	msg := fmt.Sprintf("type %v not match origin %v", to.Tp, origin.Tp)
	return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
}

func setDefaultValue(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) error {
	value, err := getDefaultValue(ctx, option, col.Tp, col.Decimal)
	if err != nil {
		return ErrColumnBadNull.GenWithStack("invalid default value - %s", err)
	}
	err = col.SetDefaultValue(value)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(checkDefaultValue(ctx, col, true))
}

func setColumnComment(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) error {
	value, err := expression.EvalAstExpr(ctx, option.Expr)
	if err != nil {
		return errors.Trace(err)
	}
	col.Comment, err = value.ToString()
	return errors.Trace(err)
}

// setDefaultAndComment is only used in getModifiableColumnJob.
func setDefaultAndComment(ctx sessionctx.Context, col *table.Column, options []*ast.ColumnOption) error {
	if len(options) == 0 {
		return nil
	}
	var hasDefaultValue, setOnUpdateNow bool
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			value, err := getDefaultValue(ctx, opt, col.Tp, col.Decimal)
			if err != nil {
				return ErrColumnBadNull.GenWithStack("invalid default value - %s", err)
			}
			if hasDefaultValue, value, err = checkColumnDefaultValue(ctx, col, value); err != nil {
				return errors.Trace(err)
			}
			if err = col.SetDefaultValue(value); err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionComment:
			err := setColumnComment(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionNotNull:
			col.Flag |= mysql.NotNullFlag
		case ast.ColumnOptionNull:
			col.Flag &= ^mysql.NotNullFlag
		case ast.ColumnOptionAutoIncrement:
			col.Flag |= mysql.AutoIncrementFlag
		case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
			return errUnsupportedModifyColumn.GenWithStack("unsupported modify column constraint - %v", opt.Tp)
		case ast.ColumnOptionOnUpdate:
			// TODO: Support other time functions.
			if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
				if !expression.IsCurrentTimestampExpr(opt.Expr) {
					return ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
			} else {
				return ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			col.Flag |= mysql.OnUpdateNowFlag
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			var buf = bytes.NewBuffer([]byte{})
			opt.Expr.Format(buf)
			col.GeneratedExprString = buf.String()
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			for _, colName := range findColumnNamesInExpr(opt.Expr) {
				col.Dependences[colName.Name.L] = struct{}{}
			}
		default:
			// TODO: Support other types.
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs(opt.Tp))
		}
	}

	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)

	if hasDefaultValue {
		return errors.Trace(checkDefaultValue(ctx, col, true))
	}

	return nil
}

func (d *ddl) getModifiableColumnJob(ctx sessionctx.Context, ident ast.Ident, originalColName model.CIStr,
	spec *ast.AlterTableSpec) (*model.Job, error) {
	specNewColumn := spec.NewColumns[0]
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name)
	}
	newColName := specNewColumn.Name.Name
	// If we want to rename the column name, we need to check whether it already exists.
	if newColName.L != originalColName.L {
		c := table.FindCol(t.Cols(), newColName.L)
		if c != nil {
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
		}
	}

	// Constraints in the new column means adding new constraints. Errors should thrown,
	// which will be done by `setDefaultAndComment` later.
	if specNewColumn.Tp == nil {
		// Make sure the column definition is simple field type.
		return nil, errors.Trace(errUnsupportedModifyColumn)
	}

	if err = checkColumnAttributes(specNewColumn.Name.OrigColName(), specNewColumn.Tp); err != nil {
		return nil, errors.Trace(err)
	}

	newCol := table.ToColumn(&model.ColumnInfo{
		ID: col.ID,
		// We use this PR(https://github.com/pingcap/tidb/pull/6274) as the dividing line to define whether it is a new version or an old version TiDB.
		// The old version TiDB initializes the column's offset and state here.
		// The new version TiDB doesn't initialize the column's offset and state, and it will do the initialization in run DDL function.
		// When we do the rolling upgrade the following may happen:
		// a new version TiDB builds the DDL job that doesn't be set the column's offset and state,
		// and the old version TiDB is the DDL owner, it doesn't get offset and state from the store. Then it will encounter errors.
		// So here we set offset and state to support the rolling upgrade.
		Offset:             col.Offset,
		State:              col.State,
		OriginDefaultValue: col.OriginDefaultValue,
		FieldType:          *specNewColumn.Tp,
		Name:               newColName,
	})

	err = setCharsetCollationFlenDecimal(&newCol.FieldType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = modifiable(&col.FieldType, &newCol.FieldType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = setDefaultAndComment(ctx, newCol, specNewColumn.Options); err != nil {
		return nil, errors.Trace(err)
	}

	// Copy index related options to the new spec.
	indexFlags := col.FieldType.Flag & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.FieldType.Flag |= indexFlags
	if mysql.HasPriKeyFlag(col.FieldType.Flag) {
		newCol.FieldType.Flag |= mysql.NotNullFlag
		// TODO: If user explicitly set NULL, we should throw error ErrPrimaryCantHaveNull.
	}

	// We don't support modifying column from not_auto_increment to auto_increment.
	if !mysql.HasAutoIncrementFlag(col.Flag) && mysql.HasAutoIncrementFlag(newCol.Flag) {
		return nil, errUnsupportedModifyColumn.GenWithStackByArgs("set auto_increment")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	var modifyColumnTp byte
	if !mysql.HasNotNullFlag(col.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		if err = checkForNullValue(ctx, col.Tp == newCol.Tp, ident.Schema, ident.Name, col.Name, newCol.Name); err != nil {
			return nil, errors.Trace(err)
		}
		// `modifyColumnTp` indicates that there is a type modification.
		modifyColumnTp = mysql.TypeNull
	}

	if err = checkColumnFieldLength(schema, spec.NewColumns, t.Meta()); err != nil {
		return nil, errors.Trace(err)
	}

	// As same with MySQL, we don't support modifying the stored status for generated columns.
	if err = checkModifyGeneratedColumn(t.Cols(), col, newCol); err != nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{&newCol, originalColName, spec.Position, modifyColumnTp},
	}
	return job, nil
}

// ChangeColumn renames an existing column and modifies the column's definition,
// currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ChangeColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(spec.OldColumnName.Schema.O) != 0 && ident.Schema.L != spec.OldColumnName.Schema.L {
		return ErrWrongDBName.GenWithStackByArgs(spec.OldColumnName.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}
	if len(spec.OldColumnName.Table.O) != 0 && ident.Name.L != spec.OldColumnName.Table.L {
		return ErrWrongTableName.GenWithStackByArgs(spec.OldColumnName.Table.O)
	}

	job, err := d.getModifiableColumnJob(ctx, ident, spec.OldColumnName.Name, spec)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ModifyColumn does modification on an existing column, currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ModifyColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}

	originalColName := specNewColumn.Name.Name
	job, err := d.getModifiableColumnJob(ctx, ident, originalColName, spec)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) AlterColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}

	colName := specNewColumn.Name.Name
	// Check whether alter column has existed.
	col := table.FindCol(t.Cols(), colName.L)
	if col == nil {
		return errBadField.GenWithStackByArgs(colName, ident.Name)
	}

	// Clean the NoDefaultValueFlag value.
	col.Flag &= ^mysql.NoDefaultValueFlag
	if len(specNewColumn.Options) == 0 {
		err = col.SetDefaultValue(nil)
		if err != nil {
			return errors.Trace(err)
		}
		setNoDefaultValueFlag(col, false)
	} else {
		err = setDefaultValue(ctx, col, specNewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionSetDefaultValue,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableComment updates the table comment information.
func (d *ddl) AlterTableComment(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionModifyTableComment,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{spec.Comment},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableCharset changes the table charset and collate.
func (d *ddl) AlterTableCharsetAndCollate(ctx sessionctx.Context, ident ast.Ident, toCharset, toCollate string) error {
	// use the last one.
	if toCharset == "" && toCollate == "" {
		return ErrUnknownCharacterSet.GenWithStackByArgs(toCharset)
	}

	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	origCharset := tb.Meta().Charset
	origCollate := tb.Meta().Collate
	if toCharset == "" {
		// charset does not change.
		toCharset = origCharset
	}

	if toCollate == "" {
		// get the default collation of the charset.
		toCollate, err = charset.GetDefaultCollation(toCharset)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if origCharset == toCharset && origCollate == toCollate {
		// nothing to do.
		return nil
	}

	if err = modifiableCharsetAndCollation(toCharset, toCollate, origCharset, origCollate); err != nil {
		return errors.Trace(err)
	}

	for _, col := range tb.Meta().Cols() {
		if col.Tp == mysql.TypeVarchar {
			if err = IsTooBigFieldLength(col.Flen, col.Name.O, toCharset); err != nil {
				return errors.Trace(err)
			}
		}
	}
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionModifyTableCharsetAndCollate,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{toCharset, toCollate},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// RenameIndex renames an index.
// In TiDB, indexes are case-insensitive (so index 'a' and 'A" are considered the same index),
// but index names are case-sensitive (we can rename index 'a' to 'A')
func (d *ddl) RenameIndex(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	duplicate, err := validateRenameIndex(spec.FromKey, spec.ToKey, tb.Meta())
	if duplicate {
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionRenameIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{spec.FromKey, spec.ToKey},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropTable will proceed even if some table in the list does not exists.
func (d *ddl) DropTable(ctx sessionctx.Context, ti ast.Ident) (err error) {
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil || tb.Meta().IsView() {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropView will proceed even if some view in the list does not exists.
func (d *ddl) DropView(ctx sessionctx.Context, ti ast.Ident) (err error) {
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	if !tb.Meta().IsView() {
		return ErrTableIsNotView.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionDropView,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) TruncateTable(ctx sessionctx.Context, ti ast.Ident) error {
	is := d.GetInformationSchema(ctx)
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}
	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	newTableID, err := d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) RenameTable(ctx sessionctx.Context, oldIdent, newIdent ast.Ident, isAlterTable bool) error {
	is := d.GetInformationSchema(ctx)
	oldSchema, ok := is.SchemaByName(oldIdent.Schema)
	if !ok {
		if isAlterTable {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		if is.TableExists(newIdent.Schema, newIdent.Name) {
			return infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		return errFileNotFound.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	oldTbl, err := is.TableByName(oldIdent.Schema, oldIdent.Name)
	if err != nil {
		if isAlterTable {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		if is.TableExists(newIdent.Schema, newIdent.Name) {
			return infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		return errFileNotFound.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	if isAlterTable && newIdent.Schema.L == oldIdent.Schema.L && newIdent.Name.L == oldIdent.Name.L {
		// oldIdent is equal to newIdent, do nothing
		return nil
	}
	newSchema, ok := is.SchemaByName(newIdent.Schema)
	if !ok {
		return errErrorOnRename.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name, newIdent.Schema, newIdent.Name)
	}
	if is.TableExists(newIdent.Schema, newIdent.Name) {
		return infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
	}

	job := &model.Job{
		SchemaID:   newSchema.ID,
		TableID:    oldTbl.Meta().ID,
		Type:       model.ActionRenameTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{oldSchema.ID, newIdent.Name},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func getAnonymousIndex(t table.Table, colName model.CIStr) model.CIStr {
	id := 2
	l := len(t.Indices())
	indexName := colName
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			i = -1
			id++
		}
	}
	return indexName
}

func (d *ddl) CreateIndex(ctx sessionctx.Context, ti ast.Ident, unique bool, indexName model.CIStr,
	idxColNames []*ast.IndexColName, indexOption *ast.IndexOption) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		indexName = getAnonymousIndex(t, idxColNames[0].Column.Name)
	}

	if indexInfo := schemautil.FindIndexByName(indexName.L, t.Meta().Indices); indexInfo != nil {
		return ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return errors.Trace(err)
	}

	// Check before put the job is put to the queue.
	// This check is redudant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	_, err = buildIndexColumns(t.Meta().Columns, idxColNames)
	if err != nil {
		return errors.Trace(err)
	}

	if indexOption != nil {
		// May be truncate comment here, when index comment too long and sql_mode is't strict.
		indexOption.Comment, err = validateCommentLength(ctx.GetSessionVars(),
			indexOption.Comment,
			maxCommentLength,
			errTooLongIndexComment.GenWithStackByArgs(indexName.String(), maxCommentLength))
		if err != nil {
			return errors.Trace(err)
		}
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, idxColNames, indexOption},
		Priority:   ctx.GetSessionVars().DDLReorgPriority,
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildFKInfo(fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef, cols []*table.Column) (*model.FKInfo, error) {
	var fkInfo model.FKInfo
	fkInfo.Name = fkName
	fkInfo.RefTable = refer.Table.Name

	fkInfo.Cols = make([]model.CIStr, len(keys))
	for i, key := range keys {
		if table.FindCol(cols, key.Column.Name.O) == nil {
			return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
		}
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.RefCols = make([]model.CIStr, len(refer.IndexColNames))
	for i, key := range refer.IndexColNames {
		fkInfo.RefCols[i] = key.Column.Name
	}

	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUpdate = int(refer.OnUpdate.ReferOpt)

	return &fkInfo, nil

}

func (d *ddl) CreateForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	fkInfo, err := buildFKInfo(fkName, keys, refer, t.Cols())
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)

}

func (d *ddl) DropForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropIndex(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	if indexInfo := schemautil.FindIndexByName(indexName.L, t.Meta().Indices); indexInfo == nil {
		return ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func isDroppableColumn(tblInfo *model.TableInfo, colName model.CIStr) error {
	// Check whether there are other columns depend on this column or not.
	for _, col := range tblInfo.Columns {
		for dep := range col.Dependences {
			if dep == colName.L {
				return errDependentByGeneratedColumn.GenWithStackByArgs(dep)
			}
		}
	}
	if len(tblInfo.Columns) == 1 {
		return ErrCantRemoveAllFields.GenWithStack("can't drop only column %s in table %s",
			colName, tblInfo.Name)
	}
	// We don't support dropping column with index covered now.
	// We must drop the index first, then drop the column.
	if isColumnWithIndex(colName.L, tblInfo.Indices) {
		return errCantDropColWithIndex.GenWithStack("can't drop column %s with index covered now", colName)
	}
	return nil
}

// validateCommentLength checks comment length of table, column, index and partition.
// If comment length is more than the standard length truncate it
// and store the comment length upto the standard comment length size.
func validateCommentLength(vars *variable.SessionVars, comment string, maxLen int, err error) (string, error) {
	if len(comment) > maxLen {
		if vars.StrictSQLMode {
			return "", err
		}
		vars.StmtCtx.AppendWarning(err)
		return comment[:maxLen], nil
	}
	return comment, nil
}

func buildPartitionInfo(meta *model.TableInfo, d *ddl, spec *ast.AlterTableSpec) (*model.PartitionInfo, error) {
	if meta.Partition.Type == model.PartitionTypeRange && len(spec.PartDefinitions) == 0 {
		return nil, errors.Trace(ErrPartitionsMustBeDefined)
	}
	part := &model.PartitionInfo{
		Type:    meta.Partition.Type,
		Expr:    meta.Partition.Expr,
		Columns: meta.Partition.Columns,
		Enable:  meta.Partition.Enable,
	}
	buf := new(bytes.Buffer)
	for _, def := range spec.PartDefinitions {
		for _, expr := range def.LessThan {
			tp := expr.GetType().Tp
			if !(tp == mysql.TypeLong || tp == mysql.TypeLonglong) {
				expr.Format(buf)
				if strings.EqualFold(buf.String(), "MAXVALUE") {
					continue
				}
				buf.Reset()
				return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(buf.String(), "partition function")
			}
		}
		pid, err1 := d.genGlobalID()
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			ID:      pid,
			Comment: def.Comment,
		}

		buf := new(bytes.Buffer)
		for _, expr := range def.LessThan {
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		part.Definitions = append(part.Definitions, piDef)
	}
	return part, nil
}
