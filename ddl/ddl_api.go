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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	field_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

func (d *ddl) CreateSchema(ctx sessionctx.Context, schema model.CIStr, charsetInfo *ast.CharsetOpt) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	_, ok := is.SchemaByName(schema)
	if ok {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(schema)
	}

	if err = checkTooLongSchema(schema); err != nil {
		return errors.Trace(err)
	}

	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	schemaID := genIDs[0]
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
		SchemaName: dbInfo.Name.L,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}
func (d *ddl) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) (err error) {
	// Resolve target charset and collation from options.
	var toCharset, toCollate string
	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			if toCharset == "" {
				toCharset = val.Value
			} else if toCharset != val.Value {
				return ErrConflictingDeclarations.GenWithStackByArgs(toCharset, val.Value)
			}
		case ast.DatabaseOptionCollate:
			info, err := charset.GetCollationByName(val.Value)
			if err != nil {
				return errors.Trace(err)
			}
			if toCharset == "" {
				toCharset = info.CharsetName
			} else if toCharset != info.CharsetName {
				return ErrConflictingDeclarations.GenWithStackByArgs(toCharset, info.CharsetName)
			}
			toCollate = info.Name
		}
	}
	if toCollate == "" {
		if toCollate, err = charset.GetDefaultCollation(toCharset); err != nil {
			return errors.Trace(err)
		}
	}

	// Check if need to change charset/collation.
	dbName := model.NewCIStr(stmt.Name)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}
	if dbInfo.Charset == toCharset && dbInfo.Collate == toCollate {
		return nil
	}

	// Check the current TiDB limitations.
	if err = modifiableCharsetAndCollation(toCharset, toCollate, dbInfo.Charset, dbInfo.Collate); err != nil {
		return errors.Trace(err)
	}

	// Do the DDL job.
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		Type:       model.ActionModifySchemaCharsetAndCollate,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{toCharset, toCollate},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropSchema(ctx sessionctx.Context, schema model.CIStr) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	old, ok := is.SchemaByName(schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	job := &model.Job{
		SchemaID:   old.ID,
		SchemaName: old.Name.L,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	if err != nil {
		return errors.Trace(err)
	}
	if !config.TableLockEnabled() {
		return nil
	}
	// Clear table locks hold by the session.
	tbs := is.SchemaTables(schema)
	lockTableIDs := make([]int64, 0)
	for _, tb := range tbs {
		if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
			lockTableIDs = append(lockTableIDs, tb.Meta().ID)
		}
	}
	ctx.ReleaseTableLockByTableIDs(lockTableIDs)
	return nil
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
	constraints []*ast.Constraint, tblCharset, tblCollate, dbCharset, dbCollate string) ([]*table.Column, []*ast.Constraint, error) {
	colMap := map[string]*table.Column{}
	// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
	var outPriKeyConstraint *ast.Constraint
	for _, v := range constraints {
		if v.Tp == ast.ConstraintPrimaryKey {
			outPriKeyConstraint = v
			break
		}
	}
	cols := make([]*table.Column, 0, len(colDefs))
	for i, colDef := range colDefs {
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef, outPriKeyConstraint, tblCharset, tblCollate, dbCharset, dbCollate)
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

// ResolveCharsetCollation will resolve the charset by the order: table charset > database charset > server default charset,
// and it will also resolve the collate by the order: table collate > database collate > server default collate.
func ResolveCharsetCollation(tblCharset, tblCollate, dbCharset, dbCollate string) (string, string, error) {
	if len(tblCharset) != 0 {
		// tblCollate is not specified by user.
		if len(tblCollate) == 0 {
			defCollate, err := charset.GetDefaultCollation(tblCharset)
			if err != nil {
				// return terror is better.
				return "", "", ErrUnknownCharacterSet.GenWithStackByArgs(tblCharset)
			}
			return tblCharset, defCollate, nil
		}
		return tblCharset, tblCollate, nil
	}

	if len(dbCharset) != 0 {
		// dbCollate is not specified by user.
		if len(dbCollate) == 0 {
			defCollate, err := charset.GetDefaultCollation(dbCharset)
			if err != nil {
				return "", "", ErrUnknownCharacterSet.GenWithStackByArgs(dbCharset)
			}
			return dbCharset, defCollate, nil
		}
		return dbCharset, dbCollate, nil
	}

	charset, collate := charset.GetDefaultCharsetAndCollate()
	return charset, collate, nil
}

func typesNeedCharset(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeEnum, mysql.TypeSet:
		return true
	}
	return false
}

func setCharsetCollationFlenDecimal(tp *types.FieldType, specifiedCollates []string, tblCharset, tblCollate, dbCharset, dbCollate string) error {
	tp.Charset = strings.ToLower(tp.Charset)
	tp.Collate = strings.ToLower(tp.Collate)
	if len(tp.Charset) == 0 {
		if typesNeedCharset(tp.Tp) {
			if len(specifiedCollates) == 0 {
				// Both the charset and collate are not specified.
				var err error
				tp.Charset, tp.Collate, err = ResolveCharsetCollation(tblCharset, tblCollate, dbCharset, dbCollate)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				// The charset is not specified but the collate is.
				// We should derive charset from it's collate specified rather than getting from table and db.
				// It is handled like mysql's logic, use derived charset to judge conflict with next collate.
				for _, spc := range specifiedCollates {
					derivedCollation, err := charset.GetCollationByName(spc)
					if err != nil {
						return errors.Trace(err)
					}
					if len(tp.Charset) == 0 {
						tp.Charset = derivedCollation.CharsetName
					} else if tp.Charset != derivedCollation.CharsetName {
						return ErrCollationCharsetMismatch.GenWithStackByArgs(derivedCollation.Name, tp.Charset)
					}
					tp.Collate = derivedCollation.Name
				}
			}
		} else {
			tp.Charset = charset.CharsetBin
			tp.Collate = charset.CharsetBin
		}
	} else {
		if !charset.ValidCharsetAndCollation(tp.Charset, tp.Collate) {
			return errUnsupportedCharset.GenWithStackByArgs(tp.Charset, tp.Collate)
		}
		if len(tp.Collate) == 0 {
			if len(specifiedCollates) == 0 {
				// The charset is specified, but the collate is not.
				var err error
				tp.Collate, err = charset.GetDefaultCollation(tp.Charset)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				// Both the charset and collate are specified.
				for _, spc := range specifiedCollates {
					derivedCollation, err := charset.GetCollationByName(spc)
					if err != nil {
						return errors.Trace(err)
					}
					if tp.Charset != derivedCollation.CharsetName {
						return ErrCollationCharsetMismatch.GenWithStackByArgs(derivedCollation.Name, tp.Charset)
					}
					tp.Collate = derivedCollation.Name
				}
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
	colDef *ast.ColumnDef, outPriKeyConstraint *ast.Constraint, tblCharset, tblCollate, dbCharset, dbCollate string) (*table.Column, []*ast.Constraint, error) {
	// specifiedCollates refers to collates in colDef.Options, should handle them together.
	specifiedCollates := extractCollateFromOption(colDef)

	if err := setCharsetCollationFlenDecimal(colDef.Tp, specifiedCollates, tblCharset, tblCollate, dbCharset, dbCollate); err != nil {
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
			timeValue, err := expression.GetTimeValue(ctx, vv, col.Tp, col.Decimal)
			if err != nil {
				return hasDefaultValue, value, errors.Trace(err)
			}
			if timeValue.GetMysqlTime().Time == types.ZeroTime {
				return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
			}
		}
	}
	return hasDefaultValue, value, nil
}

func convertTimestampDefaultValToUTC(ctx sessionctx.Context, defaultVal interface{}, col *table.Column) (interface{}, error) {
	if defaultVal == nil || col.Tp != mysql.TypeTimestamp {
		return defaultVal, nil
	}
	if vv, ok := defaultVal.(string); ok {
		if vv != types.ZeroDatetimeStr && strings.ToUpper(vv) != strings.ToUpper(ast.CurrentTimestamp) {
			t, err := types.ParseTime(ctx.GetSessionVars().StmtCtx, vv, col.Tp, col.Decimal)
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			err = t.ConvertTimeZone(ctx.GetSessionVars().Location(), time.UTC)
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			defaultVal = t.String()
		}
	}
	return defaultVal, nil
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
		// TODO: remove this version field after there is no old version.
		Version: model.CurrLatestColumnInfoVersion,
	})

	if !isExplicitTimeStamp() {
		// Check and set TimestampFlag, OnUpdateNowFlag and NotNullFlag.
		if col.Tp == mysql.TypeTimestamp {
			col.Flag |= mysql.TimestampFlag
			col.Flag |= mysql.OnUpdateNowFlag
			col.Flag |= mysql.NotNullFlag
		}
	}
	var err error
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

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

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
				// Check PriKeyFlag first to avoid extra duplicate constraints.
				if col.Flag&mysql.PriKeyFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys}
					constraints = append(constraints, constraint)
					col.Flag |= mysql.PriKeyFlag
				}
			case ast.ColumnOptionUniqKey:
				// Check UniqueFlag first to avoid extra duplicate constraints.
				if col.Flag&mysql.UniqueFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Keys: keys}
					constraints = append(constraints, constraint)
					col.Flag |= mysql.UniqueKeyFlag
				}
			case ast.ColumnOptionDefaultValue:
				hasDefaultValue, err = setDefaultValue(ctx, col, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionOnUpdate:
				// TODO: Support other time functions.
				if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
					if !expression.IsValidCurrentTimestampExpr(v.Expr, colDef.Tp) {
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
				sb.Reset()
				err = v.Expr.Restore(restoreCtx)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				col.GeneratedExprString = sb.String()
				col.GeneratedStored = v.Stored
				_, dependColNames := findDependedColumnNames(colDef)
				col.Dependences = dependColNames
			case ast.ColumnOptionCollate:
				if field_types.HasCharset(colDef.Tp) {
					col.FieldType.Collate = v.StrValue
				}
			case ast.ColumnOptionFulltext:
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrTableCantHandleFt)
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
	err = checkPriKeyConstraint(col, hasDefaultValue, hasNullFlag, outPriKeyConstraint)
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
	err = checkColumnFieldLength(col)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, constraints, nil
}

func getDefaultValue(ctx sessionctx.Context, col *table.Column, c *ast.ColumnOption) (interface{}, error) {
	tp, fsp := col.FieldType.Tp, col.FieldType.Decimal
	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
		switch x := c.Expr.(type) {
		case *ast.FuncCallExpr:
			if x.FnName.L == ast.CurrentTimestamp {
				defaultFsp := 0
				if len(x.Args) == 1 {
					if val := x.Args[0].(*driver.ValueExpr); val != nil {
						defaultFsp = int(val.GetInt64())
					}
				}
				if defaultFsp != fsp {
					return nil, ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
				}
			}
		}
		vd, err := expression.GetTimeValue(ctx, c.Expr, tp, fsp)
		value := vd.GetValue()
		if err != nil {
			return nil, ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
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
		// For other kind of fields (e.g. INT), we supply its integer as string value.
		value, err := v.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return nil, err
		}
		return strconv.FormatUint(value, 10), nil
	}

	switch tp {
	case mysql.TypeSet:
		return setSetDefaultValue(v, col)
	case mysql.TypeDuration:
		if v, err = v.ConvertTo(ctx.GetSessionVars().StmtCtx, &col.FieldType); err != nil {
			return "", errors.Trace(err)
		}
	case mysql.TypeBit:
		if v.Kind() == types.KindInt64 || v.Kind() == types.KindUint64 {
			// For BIT fields, convert int into BinaryLiteral.
			return types.NewBinaryLiteralFromUint(v.GetUint64(), -1).ToString(), nil
		}
	}

	return v.ToString()
}

// setSetDefaultValue sets the default value for the set type. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
func setSetDefaultValue(v types.Datum, col *table.Column) (string, error) {
	if v.Kind() == types.KindInt64 {
		setCnt := len(col.Elems)
		maxLimit := int64(1<<uint(setCnt) - 1)
		val := v.GetInt64()
		if val < 1 || val > maxLimit {
			return "", ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
		}
		setVal, err := types.ParseSetValue(col.Elems, uint64(val))
		if err != nil {
			return "", errors.Trace(err)
		}
		v.SetMysqlSet(setVal)
		return v.ToString()
	}

	str, err := v.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	if str == "" {
		return str, nil
	}

	valMap := make(map[string]struct{}, len(col.Elems))
	dVals := strings.Split(strings.ToLower(str), ",")
	for _, dv := range dVals {
		valMap[dv] = struct{}{}
	}
	var existCnt int
	for dv := range valMap {
		for i := range col.Elems {
			e := strings.ToLower(col.Elems[i])
			if e == dv {
				existCnt++
				break
			}
		}
	}
	if existCnt != len(valMap) {
		return "", ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	setVal, err := types.ParseSetName(col.Elems, str)
	if err != nil {
		return "", ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	v.SetMysqlSet(setVal)

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
				context.Background()
				logutil.Logger(ddlLogCtx).Error("set default value failed", zap.Error(err))
			}
		} else {
			if err := c.SetDefaultValue(strings.ToUpper(ast.CurrentTimestamp)); err != nil {
				logutil.Logger(ddlLogCtx).Error("set default value failed", zap.Error(err))
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
	colName := model.NewCIStr("")
	for _, col := range cols {
		switch x := col.(type) {
		case *ast.ColumnDef:
			colName = x.Name.Name
		case model.CIStr:
			colName = x
		default:
			colName.O, colName.L = "", ""
		}
		if colNames.Exist(colName.L) {
			return infoschema.ErrColumnExists.GenWithStackByArgs(colName.O)
		}
		colNames.Insert(colName.L)
	}
	return nil
}

func checkIsAutoIncrementColumn(colDefs *ast.ColumnDef) bool {
	for _, option := range colDefs.Options {
		if option.Tp == ast.ColumnOptionAutoIncrement {
			return true
		}
	}
	return false
}

func checkGeneratedColumn(colDefs []*ast.ColumnDef) error {
	var colName2Generation = make(map[string]columnGenerationInDDL, len(colDefs))
	var exists bool
	var autoIncrementColumn string
	for i, colDef := range colDefs {
		for _, option := range colDef.Options {
			if option.Tp == ast.ColumnOptionGenerated {
				if err := checkIllegalFn4GeneratedColumn(colDef.Name.Name.L, option.Expr); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if checkIsAutoIncrementColumn(colDef) {
			exists, autoIncrementColumn = true, colDef.Name.Name.L
		}
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

	// Check whether the generated column refers to any auto-increment columns
	if exists {
		for colName, generated := range colName2Generation {
			if _, found := generated.dependences[autoIncrementColumn]; found {
				return ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(colName)
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

func checkColumnFieldLength(col *table.Column) error {
	if col.Tp == mysql.TypeVarchar {
		if err := IsTooBigFieldLength(col.Flen, col.Name.O, col.Charset); err != nil {
			return errors.Trace(err)
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
		Name:    tableName,
		Version: model.CurrLatestTableInfoVersion,
	}
	// When this function is called by MockTableInfo, we should set a particular table id.
	// So the `ddl` structure may be nil.
	if d != nil {
		genIDs, err := d.genGlobalIDs(1)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tbInfo.ID = genIDs[0]
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
			lastCol, err := checkPKOnGeneratedColumn(tbInfo, constr.Keys)
			if err != nil {
				return nil, err
			}
			if len(constr.Keys) == 1 && !config.GetGlobalConfig().AlterPrimaryKey {
				switch lastCol.Tp {
				case mysql.TypeLong, mysql.TypeLonglong,
					mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
					tbInfo.PKIsHandle = true
					// Avoid creating index for PK handle column.
					continue
				}
			}
		}

		if constr.Tp == ast.ConstraintFulltext {
			sc := ctx.GetSessionVars().StmtCtx
			sc.AppendWarning(ErrTableCantHandleFt)
			continue
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
			idxInfo.Comment, err = validateCommentLength(ctx.GetSessionVars(), idxInfo.Name.String(), constr.Option)
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

// checkTableInfoValid uses to check table info valid. This is used to validate table info.
func checkTableInfoValid(tblInfo *model.TableInfo) error {
	_, err := tables.TableFromMeta(nil, tblInfo)
	return err
}

func buildTableInfoWithLike(ident ast.Ident, referTblInfo *model.TableInfo) *model.TableInfo {
	tblInfo := *referTblInfo
	// Check non-public column and adjust column offset.
	newColumns := referTblInfo.Cols()
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			newIndices = append(newIndices, idx)
		}
	}
	tblInfo.Columns = newColumns
	tblInfo.Indices = newIndices
	tblInfo.Name = ident.Name
	tblInfo.AutoIncID = 0
	tblInfo.ForeignKeys = nil
	if referTblInfo.Partition != nil {
		pi := *referTblInfo.Partition
		pi.Definitions = make([]model.PartitionDefinition, len(referTblInfo.Partition.Definitions))
		copy(pi.Definitions, referTblInfo.Partition.Definitions)
		tblInfo.Partition = &pi
	}
	return &tblInfo
}

// BuildTableInfoFromAST builds model.TableInfo from a SQL statement.
// The SQL string should be a create table statement.
// Don't use this function to build a partitioned table.
func BuildTableInfoFromAST(s *ast.CreateTableStmt) (*model.TableInfo, error) {
	return buildTableInfoWithCheck(mock.NewContext(), nil, s, mysql.DefaultCharset, "")
}

func buildTableInfoWithCheck(ctx sessionctx.Context, d *ddl, s *ast.CreateTableStmt, dbCharset, dbCollate string) (*model.TableInfo, error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	colDefs := s.Cols
	colObjects := make([]interface{}, 0, len(colDefs))
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

	tableCharset, tableCollate, err := getCharsetAndCollateInTableOption(0, s.Options)
	if err != nil {
		return nil, err
	}

	// The column charset haven't been resolved here.
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, colDefs, s.Constraints, tableCharset, tableCollate, dbCharset, dbCollate)
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
	tbInfo.Collate = tableCollate
	tbInfo.Charset = tableCharset

	if s.Partition != nil {
		err := checkPartitionExprValid(ctx, tbInfo, s.Partition.Expr)
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
				err = checkPartitionByRange(ctx, tbInfo, pi, cols, s)
			case model.PartitionTypeHash:
				err = checkPartitionByHash(ctx, pi, s, cols, tbInfo)
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
			if err = checkRangePartitioningKeysConstraints(ctx, s, tbInfo, newConstraints); err != nil {
				return nil, errors.Trace(err)
			}
			tbInfo.Partition = pi
		}
	}

	if err = handleTableOptions(s.Options, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	if err = resolveDefaultTableCharsetAndCollation(tbInfo, dbCharset, dbCollate); err != nil {
		return nil, errors.Trace(err)
	}

	if err = checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate); err != nil {
		return nil, errors.Trace(err)
	}

	return tbInfo, nil
}

func (d *ddl) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) (err error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var referTbl table.Table
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := is.SchemaByName(referIdent.Schema)
		if !ok {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, err = is.TableByName(referIdent.Schema, referIdent.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
	}
	if is.TableExists(ident.Schema, ident.Name) {
		if s.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrTableExists.GenWithStackByArgs(ident))
			return nil
		}
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}

	// build tableInfo
	var tbInfo *model.TableInfo
	if s.ReferTable != nil {
		tbInfo = buildTableInfoWithLike(ident, referTbl.Meta())
		count := 1
		if tbInfo.Partition != nil {
			count += len(tbInfo.Partition.Definitions)
		}
		var genIDs []int64
		genIDs, err = d.genGlobalIDs(count)
		if err != nil {
			return errors.Trace(err)
		}
		tbInfo.ID = genIDs[0]
		if tbInfo.Partition != nil {
			for i := 0; i < len(tbInfo.Partition.Definitions); i++ {
				tbInfo.Partition.Definitions[i].ID = genIDs[i+1]
			}
		}
	} else {
		tbInfo, err = buildTableInfoWithCheck(ctx, d, s, schema.Charset, schema.Collate)
	}

	if err != nil {
		return errors.Trace(err)
	}
	tbInfo.State = model.StatePublic
	err = checkTableInfoValid(tbInfo)
	if err != nil {
		return err
	}
	tbInfo.State = model.StateNone

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo},
	}

	err = d.doDDLJob(ctx, job)
	if err == nil {
		// do pre-split and scatter.
		sp, ok := d.store.(kv.SplitableStore)
		if ok && atomic.LoadUint32(&EnableSplitTableRegion) != 0 {
			var (
				preSplit      func()
				scatterRegion bool
			)
			val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBScatterRegion)
			if err != nil {
				logutil.Logger(context.Background()).Warn("[ddl] won't scatter region", zap.Error(err))
			} else {
				scatterRegion = variable.TiDBOptOn(val)
			}
			pi := tbInfo.GetPartitionInfo()
			if pi != nil {
				preSplit = func() { splitPartitionTableRegion(ctx, sp, pi, scatterRegion) }
			} else {
				preSplit = func() { splitTableRegion(ctx, sp, tbInfo, scatterRegion) }
			}
			if scatterRegion {
				preSplit()
			} else {
				go preSplit()
			}
		}
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

func (d *ddl) RecoverTable(ctx sessionctx.Context, tbInfo *model.TableInfo, schemaID, autoID, dropJobID int64, snapshotTS uint64) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
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
		SchemaName: schema.Name.L,
		Type:       model.ActionRecoverTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo, autoID, dropJobID, snapshotTS, recoverTableCheckFlagNone},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// preSplitAndScatter performs pre-split and scatter of the table's regions.
func (d *ddl) preSplitAndScatter(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo) {
	sp, ok := d.store.(kv.SplitableStore)
	if !ok || atomic.LoadUint32(&EnableSplitTableRegion) == 0 {
		return
	}
	var (
		preSplit      func()
		scatterRegion bool
	)
	val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBScatterRegion)
	if err != nil {
		logutil.Logger(context.Background()).Warn("[ddl] won't scatter region", zap.Error(err))
	} else {
		scatterRegion = variable.TiDBOptOn(val)
	}
	if pi == nil {
		pi = tbInfo.GetPartitionInfo()
	}
	if pi != nil {
		preSplit = func() { splitPartitionTableRegion(ctx, sp, pi, scatterRegion) }
	} else {
		preSplit = func() { splitTableRegion(ctx, sp, tbInfo, scatterRegion) }
	}
	if scatterRegion {
		preSplit()
	} else {
		go preSplit()
	}
}

func (d *ddl) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) (err error) {
	ident := ast.Ident{Name: s.ViewName.Name, Schema: s.ViewName.Schema}
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	oldView, err := is.TableByName(ident.Schema, ident.Name)
	if err == nil && !s.OrReplace {
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}

	var oldViewTblID int64
	if oldView != nil {
		if !oldView.Meta().IsView() {
			return ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "VIEW")
		}
		oldViewTblID = oldView.Meta().ID
	}

	if err = checkTooLongTable(ident.Name); err != nil {
		return err
	}
	viewInfo, err := buildViewInfo(ctx, s)
	if err != nil {
		return err
	}

	cols := make([]*table.Column, len(s.Cols))
	colObjects := make([]interface{}, 0, len(s.Cols))

	for i, v := range s.Cols {
		cols[i] = table.ToColumn(&model.ColumnInfo{
			Name:   v,
			ID:     int64(i),
			Offset: i,
			State:  model.StatePublic,
		})
		colObjects = append(colObjects, v)
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
		SchemaName: schema.Name.L,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo, s.OrReplace, oldViewTblID},
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

func buildViewInfo(ctx sessionctx.Context, s *ast.CreateViewStmt) (*model.ViewInfo, error) {
	// Always Use `format.RestoreNameBackQuotes` to restore `SELECT` statement despite the `ANSI_QUOTES` SQL Mode is enabled or not.
	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := s.Select.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return nil, err
	}

	return &model.ViewInfo{Definer: s.Definer, Algorithm: s.Algorithm,
		Security: s.Security, SelectStmt: sb.String(), CheckOption: s.CheckOption, Cols: nil}, nil
}

func checkPartitionByHash(ctx sessionctx.Context, pi *model.PartitionInfo, s *ast.CreateTableStmt, cols []*table.Column, tbInfo *model.TableInfo) error {
	if err := checkAddPartitionTooManyPartitions(pi.Num); err != nil {
		return err
	}
	if err := checkNoHashPartitions(ctx, pi.Num); err != nil {
		return err
	}
	if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
		return err
	}
	return checkPartitionFuncType(ctx, s, cols, tbInfo)
}

func checkPartitionByRange(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo, cols []*table.Column, s *ast.CreateTableStmt) error {
	if err := checkPartitionNameUnique(pi); err != nil {
		return err
	}

	if err := checkAddPartitionTooManyPartitions(uint64(len(pi.Definitions))); err != nil {
		return err
	}

	if err := checkNoRangePartitions(len(pi.Definitions)); err != nil {
		return err
	}

	if len(pi.Columns) == 0 {
		if err := checkCreatePartitionValue(ctx, tbInfo, pi, cols); err != nil {
			return err
		}

		// s maybe nil when add partition.
		if s == nil {
			return nil
		}

		if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
			return err
		}
		return checkPartitionFuncType(ctx, s, cols, tbInfo)
	}

	// Check for range columns partition.
	if err := checkRangeColumnsPartitionType(tbInfo, pi.Columns); err != nil {
		return err
	}

	if s != nil {
		for _, def := range s.Partition.Definitions {
			exprs := def.Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs
			if err := checkRangeColumnsTypeAndValuesMatch(ctx, tbInfo, pi.Columns, exprs); err != nil {
				return err
			}
		}
	}

	return checkRangeColumnsPartitionValue(ctx, tbInfo, pi)
}

func checkRangeColumnsPartitionType(tbInfo *model.TableInfo, columns []model.CIStr) error {
	for _, col := range columns {
		colInfo := getColumnInfoByName(tbInfo, col.L)
		if colInfo == nil {
			return errors.Trace(ErrFieldNotFoundPart)
		}
		// The permitted data types are shown in the following list:
		// All integer types
		// DATE and DATETIME
		// CHAR, VARCHAR, BINARY, and VARBINARY
		// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns.html
		switch colInfo.FieldType.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		case mysql.TypeDate, mysql.TypeDatetime:
		case mysql.TypeVarchar, mysql.TypeString:
		default:
			return ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.O)
		}
	}
	return nil
}

func checkRangeColumnsPartitionValue(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo) error {
	// Range columns partition key supports multiple data types with integer、datetime、string.
	defs := pi.Definitions
	if len(defs) < 1 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("RANGE")
	}

	curr := &defs[0]
	if len(curr.LessThan) != len(pi.Columns) {
		return errors.Trace(ast.ErrPartitionColumnList)
	}
	var prev *model.PartitionDefinition
	for i := 1; i < len(defs); i++ {
		prev, curr = curr, &defs[i]
		succ, err := checkTwoRangeColumns(ctx, curr, prev, pi, tbInfo)
		if err != nil {
			return err
		}
		if !succ {
			return errors.Trace(ErrRangeNotIncreasing)
		}
	}
	return nil
}

func checkTwoRangeColumns(ctx sessionctx.Context, curr, prev *model.PartitionDefinition, pi *model.PartitionInfo, tbInfo *model.TableInfo) (bool, error) {
	if len(curr.LessThan) != len(pi.Columns) {
		return false, errors.Trace(ast.ErrPartitionColumnList)
	}
	for i := 0; i < len(pi.Columns); i++ {
		// Special handling for MAXVALUE.
		if strings.EqualFold(curr.LessThan[i], partitionMaxValue) {
			// If current is maxvalue, it certainly >= previous.
			return true, nil
		}
		if strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// Current is not maxvalue, and previous is maxvalue.
			return false, nil
		}

		// Current and previous is the same.
		if strings.EqualFold(curr.LessThan[i], prev.LessThan[i]) {
			continue
		}

		// The tuples of column values used to define the partitions are strictly increasing:
		// PARTITION p0 VALUES LESS THAN (5,10,'ggg')
		// PARTITION p1 VALUES LESS THAN (10,20,'mmm')
		// PARTITION p2 VALUES LESS THAN (15,30,'sss')
		succ, err := parseAndEvalBoolExpr(ctx, fmt.Sprintf("(%s) > (%s)", curr.LessThan[i], prev.LessThan[i]), tbInfo)
		if err != nil {
			return false, err
		}

		if succ {
			return true, nil
		}
	}
	return false, nil
}

func parseAndEvalBoolExpr(ctx sessionctx.Context, expr string, tbInfo *model.TableInfo) (bool, error) {
	e, err := expression.ParseSimpleExprWithTableInfo(ctx, expr, tbInfo)
	if err != nil {
		return false, err
	}
	res, _, err1 := e.EvalInt(ctx, chunk.Row{})
	if err1 != nil {
		return false, err1
	}
	return res > 0, nil
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
	var alloc autoid.Allocator
	if tbInfo.AutoIdCache > 0 {
		alloc = autoid.NewAllocator(d.store, tbInfo.GetDBID(schemaID), tbInfo.IsAutoIncColUnsigned(), autoid.CustomAutoIncCacheOption(tbInfo.AutoIdCache))
	} else {
		alloc = autoid.NewAllocator(d.store, tbInfo.GetDBID(schemaID), tbInfo.IsAutoIncColUnsigned())
	}
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

func resolveDefaultTableCharsetAndCollation(tbInfo *model.TableInfo, dbCharset, dbCollate string) (err error) {
	chr, collate, err := ResolveCharsetCollation(tbInfo.Charset, tbInfo.Collate, dbCharset, dbCollate)
	if err != nil {
		return errors.Trace(err)
	}
	if len(tbInfo.Charset) == 0 {
		tbInfo.Charset = chr
	}

	if len(tbInfo.Collate) == 0 {
		tbInfo.Collate = collate
	}
	return
}

// handleTableOptions updates tableInfo according to table options.
func handleTableOptions(options []*ast.TableOption, tbInfo *model.TableInfo) error {
	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionAutoIdCache:
			if op.UintValue > uint64(math.MaxInt64) {
				// TODO: Refine this error.
				return errors.New("table option auto_id_cache overflows int64")
			}
			tbInfo.AutoIdCache = int64(op.UintValue)
		case ast.TableOptionComment:
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCompression:
			tbInfo.Compression = op.StrValue
		case ast.TableOptionShardRowID:
			if op.UintValue > 0 && tbInfo.PKIsHandle {
				return errUnsupportedShardRowIDBits
			}
			tbInfo.ShardRowIDBits = op.UintValue
			if tbInfo.ShardRowIDBits > shardRowIDBitsMax {
				tbInfo.ShardRowIDBits = shardRowIDBitsMax
			}
			tbInfo.MaxShardRowIDBits = tbInfo.ShardRowIDBits
		case ast.TableOptionPreSplitRegion:
			tbInfo.PreSplitRegions = op.UintValue
		}
	}
	if tbInfo.PreSplitRegions > tbInfo.ShardRowIDBits {
		tbInfo.PreSplitRegions = tbInfo.ShardRowIDBits
	}
	return nil
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
func getCharsetAndCollateInTableOption(startIdx int, options []*ast.TableOption) (chs, coll string, err error) {
	for i := startIdx; i < len(options); i++ {
		opt := options[i]
		// we set the charset to the last option. example: alter table t charset latin1 charset utf8 collate utf8_bin;
		// the charset will be utf8, collate will be utf8_bin
		switch opt.Tp {
		case ast.TableOptionCharset:
			info, err := charset.GetCharsetDesc(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.Name
			} else if chs != info.Name {
				return "", "", ErrConflictingDeclarations.GenWithStackByArgs(chs, info.Name)
			}
			if len(coll) == 0 {
				coll = info.DefaultCollation
			}
		case ast.TableOptionCollate:
			info, err := charset.GetCollationByName(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", ErrCollationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			coll = info.Name
		}
	}
	return
}

// resolveAlterTableSpec resolves alter table algorithm and removes ignore table spec in specs.
// returns valied specs, and the occurred error.
func resolveAlterTableSpec(ctx sessionctx.Context, specs []*ast.AlterTableSpec) ([]*ast.AlterTableSpec, error) {
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	algorithm := ast.AlterAlgorithmDefault
	for _, spec := range specs {
		if spec.Tp == ast.AlterTableAlgorithm {
			// Find the last AlterTableAlgorithm.
			algorithm = spec.Algorithm
		}
		if isIgnorableSpec(spec.Tp) {
			continue
		}
		validSpecs = append(validSpecs, spec)
	}

	if len(validSpecs) > 1 {
		// Now we only allow one schema changing at the same time.
		return nil, errRunMultiSchemaChanges
	}

	// Verify whether the algorithm is supported.
	for _, spec := range validSpecs {
		resolvedAlgorithm, err := ResolveAlterAlgorithm(spec, algorithm)
		if err != nil {
			if algorithm != ast.AlterAlgorithmCopy {
				return nil, errors.Trace(err)
			}
			// For the compatibility, we return warning instead of error when the algorithm is COPY,
			// because the COPY ALGORITHM is not supported in TiDB.
			ctx.GetSessionVars().StmtCtx.AppendError(err)
		}

		spec.Algorithm = resolvedAlgorithm
	}

	// Only handle valid specs.
	return validSpecs, nil
}

func (d *ddl) AlterTable(ctx sessionctx.Context, ident ast.Ident, specs []*ast.AlterTableSpec) (err error) {
	validSpecs, err := resolveAlterTableSpec(ctx, specs)
	if err != nil {
		return errors.Trace(err)
	}

	is := d.infoHandle.Get()
	if is.TableIsView(ident.Schema, ident.Name) {
		return ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "BASE TABLE")
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
		case ast.AlterTableDropPrimaryKey:
			err = d.DropIndex(ctx, ident, model.NewCIStr(mysql.PrimaryKeyName))
		case ast.AlterTableRenameIndex:
			err = d.RenameIndex(ctx, ident, spec)
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
				err = d.CreatePrimaryKey(ctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintFulltext:
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrTableCantHandleFt)
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
		case ast.AlterTablePartition:
			// Prevent silent succeed if user executes ALTER TABLE x PARTITION BY ...
			err = errors.New("alter table partition is unsupported")
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
				case ast.TableOptionAutoIdCache:
					if opt.UintValue > uint64(math.MaxInt64) {
						// TODO: Refine this error.
						return errors.New("table option auto_id_cache overflows int64")
					}
					err = d.AlterTableAutoIDCache(ctx, ident, int64(opt.UintValue))
				case ast.TableOptionComment:
					spec.Comment = opt.StrValue
					err = d.AlterTableComment(ctx, ident, spec)
				case ast.TableOptionCharset, ast.TableOptionCollate:
					// getCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						continue
					}
					var toCharset, toCollate string
					toCharset, toCollate, err = getCharsetAndCollateInTableOption(i, spec.Options)
					if err != nil {
						return err
					}
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
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	autoIncID, err := t.Allocator(ctx).NextGlobalAutoID(t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	// If newBase < autoIncID, we need to do a rebase before returning.
	// Assume there are 2 TiDB servers: TiDB-A with allocator range of 0 ~ 30000; TiDB-B with allocator range of 30001 ~ 60000.
	// If the user sends SQL `alter table t1 auto_increment = 100` to TiDB-B,
	// and TiDB-B finds 100 < 30001 but returns without any handling,
	// then TiDB-A may still allocate 99 for auto_increment column. This doesn't make sense for the user.
	newBase = mathutil.MaxInt64(newBase, autoIncID)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
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
	if uVal == t.Meta().ShardRowIDBits {
		// Nothing need to do.
		return nil
	}
	if uVal > 0 && t.Meta().PKIsHandle {
		return errUnsupportedShardRowIDBits
	}
	err = verifyNoOverflowShardBits(d.sessPool, t, uVal)
	if err != nil {
		return err
	}
	job := &model.Job{
		Type:       model.ActionShardRowID,
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{uVal},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) getSchemaAndTableByIdent(ctx sessionctx.Context, tableIdent ast.Ident) (dbInfo *model.DBInfo, t table.Table, err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
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

func checkUnsupportedColumnConstraint(col *ast.ColumnDef, ti ast.Ident) error {
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

	err := checkUnsupportedColumnConstraint(specNewColumn, ti)
	if err != nil {
		return errors.Trace(err)
	}

	colName := specNewColumn.Name.Name.O
	if err = checkColumnAttributes(colName, specNewColumn.Tp); err != nil {
		return errors.Trace(err)
	}

	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
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
	// NOTE: we do check whether the column refers other generated
	// columns occurring later in a table, but we don't handle the col offset.
	for _, option := range specNewColumn.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			if err := checkIllegalFn4GeneratedColumn(specNewColumn.Name.Name.L, option.Expr); err != nil {
				return errors.Trace(err)
			}

			if option.Stored {
				return errUnsupportedOnGeneratedColumn.GenWithStackByArgs("Adding generated stored column through ALTER TABLE")
			}

			_, dependColNames := findDependedColumnNames(specNewColumn)
			if err = checkAutoIncrementRef(specNewColumn.Name.Name.L, dependColNames, t.Meta()); err != nil {
				return errors.Trace(err)
			}
			duplicateColNames := make(map[string]struct{}, len(dependColNames))
			for k := range dependColNames {
				duplicateColNames[k] = struct{}{}
			}
			cols := t.Cols()

			if err = checkDependedColExist(dependColNames, cols); err != nil {
				return errors.Trace(err)
			}

			if err = verifyColumnGenerationSingle(duplicateColNames, cols, spec.Position); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if len(colName) > mysql.MaxColumnNameLength {
		return ErrTooLongIdent.GenWithStackByArgs(colName)
	}

	// Ignore table constraints now, maybe return error later.
	// We use length(t.Cols()) as the default offset firstly, we will change the
	// column's offset later.
	col, _, err = buildColumnAndConstraint(ctx, len(t.Cols()), specNewColumn, nil, t.Meta().Charset, t.Meta().Collate, schema.Charset, schema.Collate)
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
		SchemaName: schema.Name.L,
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
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	partInfo, err := buildPartitionInfo(ctx, meta, d, spec)
	if err != nil {
		return errors.Trace(err)
	}

	// partInfo contains only the new added partition, we have to combine it with the
	// old partitions to check all partitions is strictly increasing.
	tmp := *partInfo
	tmp.Definitions = append(pi.Definitions, tmp.Definitions...)
	err = checkPartitionByRange(ctx, meta, &tmp, t.Cols(), nil)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionAddTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partInfo},
	}

	err = d.doDDLJob(ctx, job)
	if err == nil {
		d.preSplitAndScatter(ctx, meta, partInfo)
	}
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

	switch meta.Partition.Type {
	// We don't support coalesce partitions hash type partition now.
	case model.PartitionTypeHash:
		return errors.Trace(ErrUnsupportedCoalescePartition)

	// Key type partition cannot be constructed currently, ignoring it for now.
	case model.PartitionTypeKey:

	// Coalesce partition can only be used on hash/key partitions.
	default:
		return errors.Trace(ErrCoalesceOnlyOnHashPartition)
	}

	return errors.Trace(err)
}

func (d *ddl) TruncateTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	// TODO: Support truncate multiple partitions
	if len(spec.PartitionNames) != 1 {
		return errRunMultiSchemaChanges
	}

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
	pid, err = tables.FindPartitionByName(meta, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		SchemaName: schema.Name.L,
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
	// TODO: Support drop multiple partitions
	if len(spec.PartitionNames) != 1 {
		return errRunMultiSchemaChanges
	}

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

	partName := spec.PartitionNames[0].L
	err = checkDropTablePartition(meta, partName)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionDropTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partName},
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
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
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
		SchemaName: schema.Name.L,
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
		return ErrUnknownCharacterSet.GenWithStack("Unknown character set: '%s', collation: '%s'", toCharset, toCollate)
	}
	if (origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8MB4) ||
		(origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8) ||
		(origCharset == charset.CharsetUTF8MB4 && toCharset == charset.CharsetUTF8MB4) {
		// TiDB only allow utf8 to be changed to utf8mb4, or changing the collation when the charset is utf8/utf8mb4.
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
	unsupportedMsg := fmt.Sprintf("type %v not match origin %v", to.CompactStr(), origin.CompactStr())
	switch origin.Tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch to.Tp {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		default:
			return errUnsupportedModifyColumn.GenWithStackByArgs(unsupportedMsg)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch to.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		default:
			return errUnsupportedModifyColumn.GenWithStackByArgs(unsupportedMsg)
		}
	case mysql.TypeEnum:
		if origin.Tp != to.Tp {
			msg := fmt.Sprintf("cannot modify enum type column's to type %s", to.String())
			return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}
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
	case mysql.TypeNewDecimal:
		// The root cause is modifying decimal precision needs to rewrite binary representation of that decimal.
		if to.Flen != origin.Flen || to.Decimal != origin.Decimal {
			return errUnsupportedModifyColumn.GenWithStackByArgs("can't change decimal column precision")
		}
	default:
		if origin.Tp != to.Tp {
			return errUnsupportedModifyColumn.GenWithStackByArgs(unsupportedMsg)
		}
	}

	if to.Flen > 0 && to.Flen < origin.Flen {
		msg := fmt.Sprintf("length %d is less than origin %d", to.Flen, origin.Flen)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	if to.Decimal > 0 && to.Decimal < origin.Decimal {
		msg := fmt.Sprintf("decimal %d is less than origin %d", to.Decimal, origin.Decimal)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}

	toUnsigned := mysql.HasUnsignedFlag(to.Flag)
	originUnsigned := mysql.HasUnsignedFlag(origin.Flag)
	if originUnsigned != toUnsigned {
		msg := fmt.Sprintf("can't change unsigned integer to signed or vice versa")
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}

	err := modifiableCharsetAndCollation(to.Charset, to.Collate, origin.Charset, origin.Collate)
	return errors.Trace(err)
}

func setDefaultValue(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) (bool, error) {
	hasDefaultValue := false
	value, err := getDefaultValue(ctx, col, option)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}

	if hasDefaultValue, value, err = checkColumnDefaultValue(ctx, col, value); err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	value, err = convertTimestampDefaultValToUTC(ctx, value, col)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	err = col.SetDefaultValue(value)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	return hasDefaultValue, nil
}

func setColumnComment(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) error {
	value, err := expression.EvalAstExpr(ctx, option.Expr)
	if err != nil {
		return errors.Trace(err)
	}
	col.Comment, err = value.ToString()
	return errors.Trace(err)
}

// processColumnOptions is only used in getModifiableColumnJob.
func processColumnOptions(ctx sessionctx.Context, col *table.Column, options []*ast.ColumnOption) error {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	var hasDefaultValue, setOnUpdateNow bool
	var err error
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			hasDefaultValue, err = setDefaultValue(ctx, col, opt)
			if err != nil {
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
				if !expression.IsValidCurrentTimestampExpr(opt.Expr, &col.FieldType) {
					return ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
			} else {
				return ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			col.Flag |= mysql.OnUpdateNowFlag
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			sb.Reset()
			err = opt.Expr.Restore(restoreCtx)
			if err != nil {
				return errors.Trace(err)
			}
			col.GeneratedExprString = sb.String()
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			col.GeneratedExpr = opt.Expr
			for _, colName := range findColumnNamesInExpr(opt.Expr) {
				col.Dependences[colName.Name.L] = struct{}{}
			}
		case ast.ColumnOptionCollate:
			col.Collate = opt.StrValue
		case ast.ColumnOptionReference:
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs("with references"))
		case ast.ColumnOptionFulltext:
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs("with full text"))
		default:
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs(fmt.Sprintf("unknown column option type: %d", opt.Tp)))
		}
	}

	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)

	if col.Tp == mysql.TypeBit {
		col.Flag |= mysql.UnsignedFlag
	}

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
	// Check the column with foreign key.
	if fkInfo := getColumnForeignKeyInfo(originalColName.L, t.Meta().ForeignKeys); fkInfo != nil {
		return nil, errFKIncompatibleColumns.GenWithStackByArgs(originalColName, fkInfo.Name)
	}

	// Constraints in the new column means adding new constraints. Errors should thrown,
	// which will be done by `processColumnOptions` later.
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
		Version:            col.Version,
	})

	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.Charset) == 0 && t.Meta().Version < model.TableInfoVersion1 {
		newCol.FieldType.Charset = col.FieldType.Charset
		newCol.FieldType.Collate = col.FieldType.Collate
	}
	// specifiedCollates refers to collates in colDef.Option. When setting charset and collate here we
	// should take the collate in colDef.Option into consideration rather than handling it separately
	specifiedCollates := extractCollateFromOption(specNewColumn)

	err = setCharsetCollationFlenDecimal(&newCol.FieldType, specifiedCollates, t.Meta().Charset, t.Meta().Collate, schema.Charset, schema.Collate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = processColumnOptions(ctx, newCol, specNewColumn.Options); err != nil {
		return nil, errors.Trace(err)
	}

	if err = modifiable(&col.FieldType, &newCol.FieldType); err != nil {
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
	// Disallow modifying column from auto_increment to not auto_increment if the session variable `AllowRemoveAutoInc` is false.
	if !ctx.GetSessionVars().AllowRemoveAutoInc && mysql.HasAutoIncrementFlag(col.Flag) && !mysql.HasAutoIncrementFlag(newCol.Flag) {
		return nil, errUnsupportedModifyColumn.GenWithStackByArgs("to remove auto_increment without @@tidb_allow_remove_auto_inc enabled")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	var modifyColumnTp byte
	if !mysql.HasNotNullFlag(col.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		if err = checkForNullValue(ctx, col.Tp != newCol.Tp, ident.Schema, ident.Name, newCol.Name, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		// `modifyColumnTp` indicates that there is a type modification.
		modifyColumnTp = mysql.TypeNull
	}

	if err = checkColumnFieldLength(newCol); err != nil {
		return nil, err
	}

	if err = checkColumnWithIndexConstraint(t.Meta(), col.ColumnInfo, newCol.ColumnInfo); err != nil {
		return nil, err
	}

	// As same with MySQL, we don't support modifying the stored status for generated columns.
	if err = checkModifyGeneratedColumn(t, col, newCol, specNewColumn); err != nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{&newCol, originalColName, spec.Position, modifyColumnTp},
	}
	return job, nil
}

// checkColumnWithIndexConstraint is used to check the related index constraint of the modified column.
// Index has a max-prefix-length constraint. eg: a varchar(100), index idx(a), modifying column a to a varchar(4000)
// will cause index idx to break the max-prefix-length constraint.
func checkColumnWithIndexConstraint(tbInfo *model.TableInfo, originalCol, newCol *model.ColumnInfo) error {
	var columns []*model.ColumnInfo
	for _, indexInfo := range tbInfo.Indices {
		containColumn := false
		for _, col := range indexInfo.Columns {
			if col.Name.L == originalCol.Name.L {
				containColumn = true
				break
			}
		}
		if containColumn == false {
			continue
		}
		if columns == nil {
			columns = make([]*model.ColumnInfo, 0, len(tbInfo.Columns))
			columns = append(columns, tbInfo.Columns...)
			// replace old column with new column.
			for i, col := range columns {
				if col.Name.L != originalCol.Name.L {
					continue
				}
				columns[i] = newCol.Clone()
				columns[i].Name = originalCol.Name
				break
			}
		}
		err := checkIndexPrefixLength(columns, indexInfo.Columns)
		if err != nil {
			return err
		}
	}
	return nil
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
		return ErrBadField.GenWithStackByArgs(colName, ident.Name)
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
		hasDefaultValue, err := setDefaultValue(ctx, col, specNewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
		if err = checkDefaultValue(ctx, col, hasDefaultValue); err != nil {
			return errors.Trace(err)
		}
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
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
		SchemaName: schema.Name.L,
		Type:       model.ActionModifyTableComment,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{spec.Comment},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableAutoIDCache updates the table comment information.
func (d *ddl) AlterTableAutoIDCache(ctx sessionctx.Context, ident ast.Ident, newCache int64) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionModifyTableAutoIdCache,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newCache},
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

	if toCharset == "" {
		// charset does not change.
		toCharset = tb.Meta().Charset
	}

	if toCollate == "" {
		// get the default collation of the charset.
		toCollate, err = charset.GetDefaultCollation(toCharset)
		if err != nil {
			return errors.Trace(err)
		}
	}
	doNothing, err := checkAlterTableCharset(tb.Meta(), schema, toCharset, toCollate)
	if err != nil {
		return err
	}
	if doNothing {
		return nil
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionModifyTableCharsetAndCollate,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{toCharset, toCollate},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// checkAlterTableCharset uses to check is it possible to change the charset of table.
// This function returns 2 variable:
// doNothing: if doNothing is true, means no need to change any more, because the target charset is same with the charset of table.
// err: if err is not nil, means it is not possible to change table charset to target charset.
func checkAlterTableCharset(tblInfo *model.TableInfo, dbInfo *model.DBInfo, toCharset, toCollate string) (doNothing bool, err error) {
	origCharset := tblInfo.Charset
	origCollate := tblInfo.Collate
	// Old version schema charset maybe modified when load schema if TreatOldVersionUTF8AsUTF8MB4 was enable.
	// So even if the origCharset equal toCharset, we still need to do the ddl for old version schema.
	if origCharset == toCharset && origCollate == toCollate && tblInfo.Version >= model.TableInfoVersion2 {
		// nothing to do.
		doNothing = true
		for _, col := range tblInfo.Columns {
			if col.Charset == charset.CharsetBin {
				continue
			}
			if col.Charset == toCharset && col.Collate == toCollate {
				continue
			}
			doNothing = false
		}
		if doNothing {
			return doNothing, nil
		}
	}

	if len(origCharset) == 0 {
		// The table charset may be "", if the table is create in old TiDB version, such as v2.0.8.
		// This DDL will update the table charset to default charset.
		origCharset, origCollate, err = ResolveCharsetCollation("", "", dbInfo.Charset, dbInfo.Collate)
		if err != nil {
			return doNothing, err
		}
	}

	if err = modifiableCharsetAndCollation(toCharset, toCollate, origCharset, origCollate); err != nil {
		return doNothing, err
	}

	for _, col := range tblInfo.Columns {
		if col.Tp == mysql.TypeVarchar {
			if err = IsTooBigFieldLength(col.Flen, col.Name.O, toCharset); err != nil {
				return doNothing, err
			}
		}
		if col.Charset == charset.CharsetBin {
			continue
		}
		if len(col.Charset) == 0 {
			continue
		}
		if err = modifiableCharsetAndCollation(toCharset, toCollate, col.Charset, col.Collate); err != nil {
			return doNothing, err
		}
	}
	return doNothing, nil
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
		SchemaName: schema.Name.L,
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
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if tb.Meta().IsView() {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	if err != nil {
		return errors.Trace(err)
	}
	if !config.TableLockEnabled() {
		return nil
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
		ctx.ReleaseTableLockByTableIDs([]int64{tb.Meta().ID})
	}
	return nil
}

// DropView will proceed even if some view in the list does not exists.
func (d *ddl) DropView(ctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if !tb.Meta().IsView() {
		return ErrWrongObject.GenWithStackByArgs(ti.Schema, ti.Name, "VIEW")
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionDropView,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) TruncateTable(ctx sessionctx.Context, ti ast.Ident) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	newTableID := genIDs[0]
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok && config.TableLockEnabled() {
		// AddTableLock here to avoid this ddl job was executed successfully but the session was been kill before return.
		// The session will release all table locks it holds, if we don't add the new locking table id here,
		// the session may forget to release the new locked table id when this ddl job was executed successfully
		// but the session was killed before return.
		ctx.AddTableLock(([]model.TableLockTpInfo{{SchemaID: schema.ID, TableID: newTableID, Tp: tb.Meta().Lock.Tp}}))
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	if err != nil {
		if config.TableLockEnabled() {
			ctx.ReleaseTableLockByTableIDs([]int64{newTableID})
		}
		return errors.Trace(err)
	}

	oldTblInfo := tb.Meta()
	if oldTblInfo.PreSplitRegions > 0 {
		if _, tb, err := d.getSchemaAndTableByIdent(ctx, ti); err == nil {
			d.preSplitAndScatter(ctx, tb.Meta(), tb.Meta().GetPartitionInfo())
		}
	}

	if !config.TableLockEnabled() {
		return nil
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
		ctx.ReleaseTableLockByTableIDs([]int64{tb.Meta().ID})
	}
	return nil
}

func (d *ddl) RenameTable(ctx sessionctx.Context, oldIdent, newIdent ast.Ident, isAlterTable bool) error {
	is := d.GetInfoSchemaWithInterceptor(ctx)
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
	if err := checkTooLongTable(newIdent.Name); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   newSchema.ID,
		TableID:    oldTbl.Meta().ID,
		SchemaName: newSchema.Name.L,
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
	if strings.EqualFold(indexName.L, mysql.PrimaryKeyName) {
		indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
		id = 3
	}
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			i = -1
			id++
		}
	}
	return indexName
}

func (d *ddl) CreatePrimaryKey(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr,
	idxColNames []*ast.IndexColName, indexOption *ast.IndexOption) error {
	if !config.GetGlobalConfig().AlterPrimaryKey {
		return ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported add primary key, alter-primary-key is false")
	}

	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return ErrTooLongIdent.GenWithStackByArgs(mysql.PrimaryKeyName)
	}

	indexName = model.NewCIStr(mysql.PrimaryKeyName)
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		return infoschema.ErrMultiplePriKey
	}

	tblInfo := t.Meta()
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	_, err = buildIndexColumns(tblInfo.Columns, idxColNames)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = checkPKOnGeneratedColumn(tblInfo, idxColNames); err != nil {
		return err
	}

	if tblInfo.GetPartitionInfo() != nil {
		if err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), idxColNames, tblInfo, true); err != nil {
			return err
		}
	}

	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if _, err = validateCommentLength(ctx.GetSessionVars(), indexName.String(), indexOption); err != nil {
		return errors.Trace(err)
	}

	unique := true
	sqlMode := ctx.GetSessionVars().SQLMode
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddPrimaryKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, idxColNames, indexOption, sqlMode},
		Priority:   ctx.GetSessionVars().DDLReorgPriority,
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) CreateIndex(ctx sessionctx.Context, ti ast.Ident, unique bool, indexName model.CIStr,
	idxColNames []*ast.IndexColName, indexOption *ast.IndexOption) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		indexName = getAnonymousIndex(t, idxColNames[0].Column.Name)
	}

	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		return ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	_, err = buildIndexColumns(tblInfo.Columns, idxColNames)
	if err != nil {
		return errors.Trace(err)
	}
	if unique && tblInfo.GetPartitionInfo() != nil {
		if err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), idxColNames, tblInfo, false); err != nil {
			return err
		}
	}
	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if _, err = validateCommentLength(ctx.GetSessionVars(), indexName.String(), indexOption); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
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
		SchemaName: schema.Name.L,
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
		SchemaName: schema.Name.L,
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

	indexInfo := t.Meta().FindIndexByName(indexName.L)
	var isPK bool
	if indexName.L == strings.ToLower(mysql.PrimaryKeyName) &&
		// Before we fixed #14243, there might be a general index named `primary` but not a primary key.
		(indexInfo == nil || indexInfo.Primary) {
		isPK = true
	}
	if isPK {
		if !config.GetGlobalConfig().AlterPrimaryKey {
			return ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported drop primary key when alter-primary-key is false")

		}
		// If the table's PKIsHandle is true, we can't find the index from the table. So we check the value of PKIsHandle.
		if indexInfo == nil && !t.Meta().PKIsHandle {
			return ErrCantDropFieldOrKey.GenWithStack("Can't DROP 'PRIMARY'; check that column/key exists")
		}
		if t.Meta().PKIsHandle {
			return ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported drop primary key when the table's pkIsHandle is true")
		}
	}
	if indexInfo == nil {
		return ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Check for drop index on auto_increment column.
	err = checkDropIndexOnAutoIncrementColumn(t.Meta(), indexInfo)
	if err != nil {
		return errors.Trace(err)
	}

	jobTp := model.ActionDropIndex
	if isPK {
		jobTp = model.ActionDropPrimaryKey
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       jobTp,
		SchemaName: schema.Name.L,
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
	// Check the column with foreign key.
	if fkInfo := getColumnForeignKeyInfo(colName.L, tblInfo.ForeignKeys); fkInfo != nil {
		return errFkColumnCannotDrop.GenWithStackByArgs(colName, fkInfo.Name)
	}
	return nil
}

// validateCommentLength checks comment length of table, column, index and partition.
// If comment length is more than the standard length truncate it
// and store the comment length upto the standard comment length size.
func validateCommentLength(vars *variable.SessionVars, indexName string, indexOption *ast.IndexOption) (string, error) {
	if indexOption == nil {
		return "", nil
	}

	maxLen := MaxCommentLength
	if len(indexOption.Comment) > maxLen {
		err := errTooLongIndexComment.GenWithStackByArgs(indexName, maxLen)
		if vars.StrictSQLMode {
			return "", err
		}
		vars.StmtCtx.AppendWarning(err)
		indexOption.Comment = indexOption.Comment[:maxLen]
	}
	return indexOption.Comment, nil
}

func buildPartitionInfo(ctx sessionctx.Context, meta *model.TableInfo, d *ddl, spec *ast.AlterTableSpec) (*model.PartitionInfo, error) {
	if meta.Partition.Type == model.PartitionTypeRange {
		if len(spec.PartDefinitions) == 0 {
			return nil, ast.ErrPartitionsMustBeDefined.GenWithStackByArgs(meta.Partition.Type)
		}
	} else {
		// we don't support ADD PARTITION for all other partition types yet.
		return nil, errors.Trace(ErrUnsupportedAddPartition)
	}

	part := &model.PartitionInfo{
		Type:    meta.Partition.Type,
		Expr:    meta.Partition.Expr,
		Columns: meta.Partition.Columns,
		Enable:  meta.Partition.Enable,
	}

	genIDs, err := d.genGlobalIDs(len(spec.PartDefinitions))
	if err != nil {
		return nil, err
	}
	for ith, def := range spec.PartDefinitions {
		if err := def.Clause.Validate(part.Type, len(part.Columns)); err != nil {
			return nil, err
		}
		// For RANGE partition only VALUES LESS THAN should be possible.
		clause := def.Clause.(*ast.PartitionDefinitionClauseLessThan)
		if len(part.Columns) > 0 {
			if err := checkRangeColumnsTypeAndValuesMatch(ctx, meta, part.Columns, clause.Exprs); err != nil {
				return nil, err
			}
		}

		comment, _ := def.Comment()
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			ID:      genIDs[ith],
			Comment: comment,
		}

		buf := new(bytes.Buffer)
		for _, expr := range clause.Exprs {
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		part.Definitions = append(part.Definitions, piDef)
	}
	return part, nil
}

func checkRangeColumnsTypeAndValuesMatch(ctx sessionctx.Context, meta *model.TableInfo, colNames []model.CIStr, exprs []ast.ExprNode) error {
	// Validate() has already checked len(colNames) = len(exprs)
	// create table ... partition by range columns (cols)
	// partition p0 values less than (expr)
	// check the type of cols[i] and expr is consistent.
	for i, colExpr := range exprs {
		if _, ok := colExpr.(*ast.MaxValueExpr); ok {
			continue
		}

		colName := colNames[i]
		colInfo := getColumnInfoByName(meta, colName.L)
		if colInfo == nil {
			return errors.Trace(ErrFieldNotFoundPart)
		}
		colType := &colInfo.FieldType

		val, err := expression.EvalAstExpr(ctx, colExpr)
		if err != nil {
			return err
		}

		// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
		switch colType.Tp {
		case mysql.TypeDate, mysql.TypeDatetime:
			switch val.Kind() {
			case types.KindString, types.KindBytes:
			default:
				return ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		}
	}
	return nil
}

// extractCollateFromOption take collates(may multiple) in option into consideration
// when handle charset and collate of a column, rather than handling it separately.
func extractCollateFromOption(def *ast.ColumnDef) []string {
	specifiedCollates := make([]string, 0, 0)
	for i := 0; i < len(def.Options); i++ {
		op := def.Options[i]
		if op.Tp == ast.ColumnOptionCollate {
			specifiedCollates = append(specifiedCollates, op.StrValue)
			def.Options = append(def.Options[:i], def.Options[i+1:]...)
			// maintain the correct index
			i--
		}
	}
	return specifiedCollates
}

// LockTables uses to execute lock tables statement.
func (d *ddl) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	lockTables := make([]model.TableLockTpInfo, 0, len(stmt.TableLocks))
	sessionInfo := model.SessionInfo{
		ServerID:  d.GetID(),
		SessionID: ctx.GetSessionVars().ConnectionID,
	}
	uniqueTableID := make(map[int64]struct{})
	// Check whether the table was already locked by another.
	for _, tl := range stmt.TableLocks {
		tb := tl.Table
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := d.getSchemaAndTableByIdent(ctx, ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() {
			return table.ErrUnsupportedOp.GenWithStackByArgs()
		}
		err = checkTableLocked(t.Meta(), tl.Type, sessionInfo)
		if err != nil {
			return err
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		lockTables = append(lockTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID, Tp: tl.Type})
	}

	unlockTables := ctx.GetAllTableLocks()
	arg := &lockTablesArg{
		LockTables:   lockTables,
		UnlockTables: unlockTables,
		SessionInfo:  sessionInfo,
	}
	job := &model.Job{
		SchemaID:   lockTables[0].SchemaID,
		TableID:    lockTables[0].TableID,
		Type:       model.ActionLockTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{arg},
	}
	// AddTableLock here is avoiding this job was executed successfully but the session was killed before return.
	ctx.AddTableLock(lockTables)
	err := d.doDDLJob(ctx, job)
	if err == nil {
		ctx.ReleaseTableLocks(unlockTables)
		ctx.AddTableLock(lockTables)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// UnlockTables uses to execute unlock tables statement.
func (d *ddl) UnlockTables(ctx sessionctx.Context, unlockTables []model.TableLockTpInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	arg := &lockTablesArg{
		UnlockTables: unlockTables,
		SessionInfo: model.SessionInfo{
			ServerID:  d.GetID(),
			SessionID: ctx.GetSessionVars().ConnectionID,
		},
	}
	job := &model.Job{
		SchemaID:   unlockTables[0].SchemaID,
		TableID:    unlockTables[0].TableID,
		Type:       model.ActionUnlockTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{arg},
	}

	err := d.doDDLJob(ctx, job)
	if err == nil {
		ctx.ReleaseAllTableLocks()
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func throwErrIfInMemOrSysDB(ctx sessionctx.Context, dbLowerName string) error {
	if util.IsMemOrSysDB(dbLowerName) {
		if ctx.GetSessionVars().User != nil {
			return infoschema.ErrAccessDenied.GenWithStackByArgs(ctx.GetSessionVars().User.Username, ctx.GetSessionVars().User.Hostname)
		}
		return infoschema.ErrAccessDenied.GenWithStackByArgs("", "")
	}
	return nil
}

func (d *ddl) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	uniqueTableID := make(map[int64]struct{})
	cleanupTables := make([]model.TableLockTpInfo, 0, len(tables))
	unlockedTablesNum := 0
	// Check whether the table was already locked by another.
	for _, tb := range tables {
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := d.getSchemaAndTableByIdent(ctx, ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() {
			return table.ErrUnsupportedOp
		}
		// Maybe the table t was not locked, but still try to unlock this table.
		// If we skip unlock the table here, the job maybe not consistent with the job.Query.
		// eg: unlock tables t1,t2;  If t2 is not locked and skip here, then the job will only unlock table t1,
		// and this behaviour is not consistent with the sql query.
		if !t.Meta().IsLocked() {
			unlockedTablesNum++
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		cleanupTables = append(cleanupTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID})
	}
	// If the num of cleanupTables is 0, or all cleanupTables is unlocked, just return here.
	if len(cleanupTables) == 0 || len(cleanupTables) == unlockedTablesNum {
		return nil
	}

	arg := &lockTablesArg{
		UnlockTables: cleanupTables,
		IsCleanup:    true,
	}
	job := &model.Job{
		SchemaID:   cleanupTables[0].SchemaID,
		TableID:    cleanupTables[0].TableID,
		Type:       model.ActionUnlockTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{arg},
	}
	err := d.doDDLJob(ctx, job)
	if err == nil {
		ctx.ReleaseTableLocks(cleanupTables)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

type lockTablesArg struct {
	LockTables    []model.TableLockTpInfo
	IndexOfLock   int
	UnlockTables  []model.TableLockTpInfo
	IndexOfUnlock int
	SessionInfo   model.SessionInfo
	IsCleanup     bool
}
