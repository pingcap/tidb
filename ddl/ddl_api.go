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
	"encoding/hex"
	"encoding/json"
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
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/domainutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

const expressionIndexPrefix = "_V$"

func (d *ddl) CreateSchema(ctx sessionctx.Context, schema model.CIStr, charsetInfo *ast.CharsetOpt) error {
	dbInfo := &model.DBInfo{Name: schema}
	if charsetInfo != nil {
		chs, coll, err := ResolveCharsetCollation(ast.CharsetOpt{Chs: charsetInfo.Chs, Col: charsetInfo.Col})
		if err != nil {
			return errors.Trace(err)
		}
		dbInfo.Charset = chs
		dbInfo.Collate = coll
	} else {
		dbInfo.Charset, dbInfo.Collate = charset.GetDefaultCharsetAndCollate()
	}
	return d.CreateSchemaWithInfo(ctx, dbInfo, OnExistError, false /*tryRetainID*/)
}

func (d *ddl) CreateSchemaWithInfo(
	ctx sessionctx.Context,
	dbInfo *model.DBInfo,
	onExist OnExist,
	tryRetainID bool,
) error {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	_, ok := is.SchemaByName(dbInfo.Name)
	if ok {
		err := infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
		switch onExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		case OnExistError, OnExistReplace:
			// FIXME: can we implement MariaDB's CREATE OR REPLACE SCHEMA?
			return err
		}
	}

	if err := checkTooLongSchema(dbInfo.Name); err != nil {
		return errors.Trace(err)
	}

	if err := checkCharsetAndCollation(dbInfo.Charset, dbInfo.Collate); err != nil {
		return errors.Trace(err)
	}

	// FIXME: support `tryRetainID`.
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	dbInfo.ID = genIDs[0]

	job := &model.Job{
		SchemaID:   dbInfo.ID,
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
			info, err := collate.GetCollationByName(val.Value)
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

func buildColumnsAndConstraints(
	ctx sessionctx.Context,
	colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint,
	tblCharset string,
	tblCollate string,
) ([]*table.Column, []*ast.Constraint, error) {
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
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef, outPriKeyConstraint, tblCharset, tblCollate)
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

// ResolveCharsetCollation will resolve the charset and collate by the order of parameters:
// * If any given ast.CharsetOpt is not empty, the resolved charset and collate will be returned.
// * If all ast.CharsetOpts are empty, the default charset and collate will be returned.
func ResolveCharsetCollation(charsetOpts ...ast.CharsetOpt) (string, string, error) {
	for _, v := range charsetOpts {
		if v.Col != "" {
			collation, err := collate.GetCollationByName(v.Col)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			if v.Chs != "" && collation.CharsetName != v.Chs {
				return "", "", charset.ErrCollationCharsetMismatch.GenWithStackByArgs(v.Col, v.Chs)
			}
			return collation.CharsetName, v.Col, nil
		}
		if v.Chs != "" {
			coll, err := charset.GetDefaultCollation(v.Chs)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			return v.Chs, coll, err
		}
	}
	chs, coll := charset.GetDefaultCharsetAndCollate()
	return chs, coll, nil
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

func setCharsetCollationFlenDecimal(tp *types.FieldType, colCharset, colCollate string) error {
	if typesNeedCharset(tp.Tp) {
		tp.Charset = colCharset
		tp.Collate = colCollate
	} else {
		tp.Charset = charset.CharsetBin
		tp.Collate = charset.CharsetBin
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

// buildColumnAndConstraint builds table.Column and ast.Constraint from the parameters.
// outPriKeyConstraint is the primary key constraint out of column definition. For example:
// `create table t1 (id int , age int, primary key(id));`
func buildColumnAndConstraint(
	ctx sessionctx.Context,
	offset int,
	colDef *ast.ColumnDef,
	outPriKeyConstraint *ast.Constraint,
	tblCharset string,
	tblCollate string,
) (*table.Column, []*ast.Constraint, error) {
	// specifiedCollate refers to the last collate specified in colDef.Options.
	chs, coll, err := getCharsetAndCollateInColumnDef(colDef)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	chs, coll, err = ResolveCharsetCollation(
		ast.CharsetOpt{Chs: chs, Col: coll},
		ast.CharsetOpt{Chs: tblCharset, Col: tblCollate},
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if err := setCharsetCollationFlenDecimal(colDef.Tp, chs, coll); err != nil {
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
			timeValue, err := expression.GetTimeValue(ctx, vv, col.Tp, int8(col.Decimal))
			if err != nil {
				return hasDefaultValue, value, errors.Trace(err)
			}
			if timeValue.GetMysqlTime().CoreTime() == types.ZeroCoreTime {
				return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
			}
		}
	}
	return hasDefaultValue, value, nil
}

func checkSequenceDefaultValue(col *table.Column) error {
	if mysql.IsIntegerType(col.Tp) {
		return nil
	}
	return ErrColumnTypeUnsupportedNextValue.GenWithStackByArgs(col.ColumnInfo.Name.O)
}

func convertTimestampDefaultValToUTC(ctx sessionctx.Context, defaultVal interface{}, col *table.Column) (interface{}, error) {
	if defaultVal == nil || col.Tp != mysql.TypeTimestamp {
		return defaultVal, nil
	}
	if vv, ok := defaultVal.(string); ok {
		if vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			t, err := types.ParseTime(ctx.GetSessionVars().StmtCtx, vv, col.Tp, int8(col.Decimal))
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

		keys := []*ast.IndexPartSpecification{
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
			case ast.ColumnOptionCheck:
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("Column check"))
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
	err = checkColumnValueConstraint(col, col.Collate)
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

// getDefaultValue will get the default value for column.
// 1: get the expr restored string for the column which uses sequence next value as default value.
// 2: get specific default value for the other column.
func getDefaultValue(ctx sessionctx.Context, col *table.Column, c *ast.ColumnOption) (interface{}, bool, error) {
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
					return nil, false, ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
				}
			}
		}
		vd, err := expression.GetTimeValue(ctx, c.Expr, tp, int8(fsp))
		value := vd.GetValue()
		if err != nil {
			return nil, false, ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
		}

		// Value is nil means `default null`.
		if value == nil {
			return nil, false, nil
		}

		// If value is types.Time, convert it to string.
		if vv, ok := value.(types.Time); ok {
			return vv.String(), false, nil
		}

		return value, false, nil
	}
	// handle default next value of sequence. (keep the expr string)
	str, isSeqExpr, err := tryToGetSequenceDefaultValue(c)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if isSeqExpr {
		return str, true, nil
	}

	// evaluate the non-sequence expr to a certain value.
	v, err := expression.EvalAstExpr(ctx, c.Expr)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if v.IsNull() {
		return nil, false, nil
	}

	if v.Kind() == types.KindBinaryLiteral || v.Kind() == types.KindMysqlBit {
		if tp == mysql.TypeBit ||
			tp == mysql.TypeString || tp == mysql.TypeVarchar || tp == mysql.TypeVarString ||
			tp == mysql.TypeBlob || tp == mysql.TypeLongBlob || tp == mysql.TypeMediumBlob || tp == mysql.TypeTinyBlob ||
			tp == mysql.TypeJSON {
			// For BinaryLiteral / string fields, when getting default value we cast the value into BinaryLiteral{}, thus we return
			// its raw string content here.
			return v.GetBinaryLiteral().ToString(), false, nil
		}
		// For other kind of fields (e.g. INT), we supply its integer as string value.
		value, err := v.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return nil, false, err
		}
		return strconv.FormatUint(value, 10), false, nil
	}

	switch tp {
	case mysql.TypeSet:
		val, err := setSetDefaultValue(v, col)
		return val, false, err
	case mysql.TypeDuration:
		if v, err = v.ConvertTo(ctx.GetSessionVars().StmtCtx, &col.FieldType); err != nil {
			return "", false, errors.Trace(err)
		}
	case mysql.TypeBit:
		if v.Kind() == types.KindInt64 || v.Kind() == types.KindUint64 {
			// For BIT fields, convert int into BinaryLiteral.
			return types.NewBinaryLiteralFromUint(v.GetUint64(), -1).ToString(), false, nil
		}
	}

	val, err := v.ToString()
	return val, false, err
}

func tryToGetSequenceDefaultValue(c *ast.ColumnOption) (expr string, isExpr bool, err error) {
	if f, ok := c.Expr.(*ast.FuncCallExpr); ok && f.FnName.L == ast.NextVal {
		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		if err := c.Expr.Restore(restoreCtx); err != nil {
			return "", true, err
		}
		return sb.String(), true, nil
	}
	return "", false, nil
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
		v.SetMysqlSet(setVal, col.Collate)
		return v.ToString()
	}

	str, err := v.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	if str == "" {
		return str, nil
	}

	ctor := collate.GetCollator(col.Collate)
	valMap := make(map[string]struct{}, len(col.Elems))
	dVals := strings.Split(str, ",")
	for _, dv := range dVals {
		valMap[string(ctor.Key(dv))] = struct{}{}
	}
	var existCnt int
	for dv := range valMap {
		for i := range col.Elems {
			e := string(ctor.Key(col.Elems[i]))
			if e == dv {
				existCnt++
				break
			}
		}
	}
	if existCnt != len(valMap) {
		return "", ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	setVal, err := types.ParseSetName(col.Elems, str, col.Collate)
	if err != nil {
		return "", ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	v.SetMysqlSet(setVal, col.Collate)

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
				logutil.BgLogger().Error("set default value failed", zap.Error(err))
			}
		} else {
			if err := c.SetDefaultValue(strings.ToUpper(ast.CurrentTimestamp)); err != nil {
				logutil.BgLogger().Error("set default value failed", zap.Error(err))
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
		if c.DefaultIsExpr {
			return nil
		}
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

func checkColumnValueConstraint(col *table.Column, collation string) error {
	if col.Tp != mysql.TypeEnum && col.Tp != mysql.TypeSet {
		return nil
	}
	valueMap := make(map[string]bool, len(col.Elems))
	ctor := collate.GetCollator(collation)
	for i := range col.Elems {
		val := string(ctor.Key(col.Elems[i]))
		if _, ok := valueMap[val]; ok {
			tpStr := "ENUM"
			if col.Tp == mysql.TypeSet {
				tpStr = "SET"
			}
			return types.ErrDuplicatedValueInType.GenWithStackByArgs(col.Name, col.Elems[i], tpStr)
		}
		valueMap[val] = true
	}
	return nil
}

func checkDuplicateColumn(cols []*model.ColumnInfo) error {
	colNames := set.StringSet{}
	for _, col := range cols {
		colName := col.Name
		if colNames.Exist(colName.L) {
			return infoschema.ErrColumnExists.GenWithStackByArgs(colName.O)
		}
		colNames.Insert(colName.L)
	}
	return nil
}

func containsColumnOption(colDef *ast.ColumnDef, opTp ast.ColumnOptionType) bool {
	for _, option := range colDef.Options {
		if option.Tp == opTp {
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
		if containsColumnOption(colDef, ast.ColumnOptionAutoIncrement) {
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

func checkTooLongColumn(cols []*model.ColumnInfo) error {
	for _, col := range cols {
		colName := col.Name.O
		if len(colName) > mysql.MaxColumnNameLength {
			return ErrTooLongIdent.GenWithStackByArgs(colName)
		}
	}
	return nil
}

func checkTooManyColumns(colDefs []*model.ColumnInfo) error {
	if uint32(len(colDefs)) > atomic.LoadUint32(&TableColumnCountLimit) {
		return errTooManyFields
	}
	return nil
}

// checkColumnsAttributes checks attributes for multiple columns.
func checkColumnsAttributes(colDefs []*model.ColumnInfo) error {
	for _, colDef := range colDefs {
		if err := checkColumnAttributes(colDef.Name.O, &colDef.FieldType); err != nil {
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
		if tp.Decimal != int(types.UnspecifiedFsp) && (tp.Decimal < int(types.MinFsp) || tp.Decimal > int(types.MaxFsp)) {
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

// checkInvisibleIndexOnPK check if primary key is invisible index.
// Note: PKIsHandle == true means the table already has a visible primary key,
// we do not need do a check for this case and return directly,
// because whether primary key is invisible has been check when creating table.
func checkInvisibleIndexOnPK(tblInfo *model.TableInfo) error {
	if tblInfo.PKIsHandle {
		return nil
	}
	pk := getPrimaryKey(tblInfo)
	if pk != nil && pk.Invisible {
		return ErrPKIndexCantBeInvisible
	}
	return nil
}

// getPrimaryKey extract the primary key in a table and return `IndexInfo`
// The returned primary key could be explicit or implicit.
// If there is no explicit primary key in table,
// the first UNIQUE INDEX on NOT NULL columns will be the implicit primary key.
// For more information about implicit primary key, see
// https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html
func getPrimaryKey(tblInfo *model.TableInfo) *model.IndexInfo {
	var implicitPK *model.IndexInfo

	for _, key := range tblInfo.Indices {
		if key.Primary {
			// table has explicit primary key
			return key
		}
		// The case index without any columns should never happen, but still do a check here
		if len(key.Columns) == 0 {
			continue
		}
		// find the first unique key with NOT NULL columns
		if implicitPK == nil && key.Unique {
			// ensure all columns in unique key have NOT NULL flag
			allColNotNull := true
			skip := false
			for _, idxCol := range key.Columns {
				col := model.FindColumnInfo(tblInfo.Cols(), idxCol.Name.L)
				// This index has a column in DeleteOnly state,
				// or it is expression index (it defined on a hidden column),
				// it can not be implicit PK, go to next index iterator
				if col == nil || col.Hidden {
					skip = true
					break
				}
				if !mysql.HasNotNullFlag(col.Flag) {
					allColNotNull = false
					break
				}
			}
			if skip {
				continue
			}
			if allColNotNull {
				implicitPK = key
			}
		}
	}
	return implicitPK
}

func setTableAutoRandomBits(ctx sessionctx.Context, tbInfo *model.TableInfo, colDefs []*ast.ColumnDef) error {
	pkColName := tbInfo.GetPkName()
	for _, col := range colDefs {
		if containsColumnOption(col, ast.ColumnOptionAutoRandom) {
			if col.Tp.Tp != mysql.TypeLonglong {
				return ErrInvalidAutoRandom.GenWithStackByArgs(
					fmt.Sprintf(autoid.AutoRandomOnNonBigIntColumn, types.TypeStr(col.Tp.Tp)))
			}
			if !tbInfo.PKIsHandle || col.Name.Name.L != pkColName.L {
				errMsg := fmt.Sprintf(autoid.AutoRandomPKisNotHandleErrMsg, col.Name.Name.O)
				return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
			}
			if containsColumnOption(col, ast.ColumnOptionAutoIncrement) {
				return ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
			}
			if containsColumnOption(col, ast.ColumnOptionDefaultValue) {
				return ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
			}

			autoRandBits, err := extractAutoRandomBitsFromColDef(col)
			if err != nil {
				return errors.Trace(err)
			}

			layout := autoid.NewAutoRandomIDLayout(col.Tp, autoRandBits)
			if autoRandBits == 0 {
				return ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNonPositive)
			} else if autoRandBits > autoid.MaxAutoRandomBits {
				errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
					autoid.MaxAutoRandomBits, autoRandBits, col.Name.Name.O)
				return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
			}
			tbInfo.AutoRandomBits = autoRandBits

			msg := fmt.Sprintf(autoid.AutoRandomAvailableAllocTimesNote, layout.IncrementalBitsCapacity())
			ctx.GetSessionVars().StmtCtx.AppendNote(errors.Errorf(msg))
		}
	}
	return nil
}

func extractAutoRandomBitsFromColDef(colDef *ast.ColumnDef) (uint64, error) {
	for _, op := range colDef.Options {
		if op.Tp == ast.ColumnOptionAutoRandom {
			return convertAutoRandomBitsToUnsigned(op.AutoRandomBitLength)
		}
	}
	return 0, nil
}

func convertAutoRandomBitsToUnsigned(autoRandomBits int) (uint64, error) {
	if autoRandomBits == types.UnspecifiedLength {
		return autoid.DefaultAutoRandomBits, nil
	} else if autoRandomBits < 0 {
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNonPositive)
	}
	return uint64(autoRandomBits), nil
}

func buildTableInfo(
	ctx sessionctx.Context,
	tableName model.CIStr,
	cols []*table.Column,
	constraints []*ast.Constraint,
	charset string,
	collate string) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name:    tableName,
		Version: model.CurrLatestTableInfoVersion,
		Charset: charset,
		Collate: collate,
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
			fk, err := buildFKInfo(model.NewCIStr(constr.Name), constr.Keys, constr.Refer, cols, tbInfo)
			if err != nil {
				return nil, err
			}
			fk.State = model.StatePublic

			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			lastCol, err := checkPKOnGeneratedColumn(tbInfo, constr.Keys)
			if err != nil {
				return nil, err
			}
			if !config.GetGlobalConfig().AlterPrimaryKey {
				singleIntPK := isSingleIntPK(constr, lastCol)
				clusteredIdx := ctx.GetSessionVars().EnableClusteredIndex
				if singleIntPK || clusteredIdx {
					// Primary key cannot be invisible.
					if constr.Option != nil && constr.Option.Visibility == ast.IndexVisibilityInvisible {
						return nil, ErrPKIndexCantBeInvisible
					}
				}
				if singleIntPK {
					tbInfo.PKIsHandle = true
					// Avoid creating index for PK handle column.
					continue
				}
				if clusteredIdx {
					tbInfo.IsCommonHandle = true
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
		// check if the index is primary or unique.
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
			if constr.Option.Visibility == ast.IndexVisibilityInvisible {
				idxInfo.Invisible = true
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

func isSingleIntPK(constr *ast.Constraint, lastCol *model.ColumnInfo) bool {
	if len(constr.Keys) != 1 {
		return false
	}
	switch lastCol.Tp {
	case mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return true
	}
	return false
}

// checkTableInfoValidExtra is like checkTableInfoValid, but also assumes the
// table info comes from untrusted source and performs further checks such as
// name length and column count.
// (checkTableInfoValid is also used in repairing objects which don't perform
// these checks. Perhaps the two functions should be merged together regardless?)
func checkTableInfoValidExtra(tbInfo *model.TableInfo) error {
	if err := checkTooLongTable(tbInfo.Name); err != nil {
		return err
	}

	if err := checkDuplicateColumn(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooLongColumn(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooManyColumns(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}
	if err := checkColumnsAttributes(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}

	// FIXME: perform checkConstraintNames
	if err := checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate); err != nil {
		return errors.Trace(err)
	}

	oldState := tbInfo.State
	tbInfo.State = model.StatePublic
	err := checkTableInfoValid(tbInfo)
	tbInfo.State = oldState
	return err
}

func checkTableInfoValidWithStmt(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) error {
	// All of these rely on the AST structure of expressions, which were
	// lost in the model (got serialized into strings).
	if err := checkGeneratedColumn(s.Cols); err != nil {
		return errors.Trace(err)
	}
	if s.Partition != nil {
		err := checkPartitionExprValid(ctx, tbInfo, s.Partition.Expr)
		if err != nil {
			return errors.Trace(err)
		}

		pi := tbInfo.Partition
		if pi != nil {
			switch pi.Type {
			case model.PartitionTypeRange:
				err = checkPartitionByRange(ctx, tbInfo, s)
			case model.PartitionTypeHash:
				err = checkPartitionByHash(ctx, tbInfo, s)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}

		if err = checkPartitioningKeysConstraints(ctx, s, tbInfo); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkTableInfoValid uses to check table info valid. This is used to validate table info.
func checkTableInfoValid(tblInfo *model.TableInfo) error {
	_, err := tables.TableFromMeta(nil, tblInfo)
	if err != nil {
		return err
	}
	return checkInvisibleIndexOnPK(tblInfo)
}

func buildTableInfoWithLike(ident ast.Ident, referTblInfo *model.TableInfo) (*model.TableInfo, error) {
	// Check the referred table is a real table object.
	if referTblInfo.IsSequence() || referTblInfo.IsView() {
		return nil, ErrWrongObject.GenWithStackByArgs(ident.Schema, referTblInfo.Name, "BASE TABLE")
	}
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
	if tblInfo.TiFlashReplica != nil {
		replica := *tblInfo.TiFlashReplica
		// Keep the tiflash replica setting, remove the replica available status.
		replica.AvailablePartitionIDs = nil
		replica.Available = false
		tblInfo.TiFlashReplica = &replica
	}
	if referTblInfo.Partition != nil {
		pi := *referTblInfo.Partition
		pi.Definitions = make([]model.PartitionDefinition, len(referTblInfo.Partition.Definitions))
		copy(pi.Definitions, referTblInfo.Partition.Definitions)
		tblInfo.Partition = &pi
	}
	return &tblInfo, nil
}

// BuildTableInfoFromAST builds model.TableInfo from a SQL statement.
// Note: TableID and PartitionID are left as uninitialized value.
func BuildTableInfoFromAST(s *ast.CreateTableStmt) (*model.TableInfo, error) {
	return buildTableInfoWithCheck(mock.NewContext(), s, mysql.DefaultCharset, "")
}

// buildTableInfoWithCheck builds model.TableInfo from a SQL statement.
// Note: TableID and PartitionIDs are left as uninitialized value.
func buildTableInfoWithCheck(ctx sessionctx.Context, s *ast.CreateTableStmt, dbCharset, dbCollate string) (*model.TableInfo, error) {
	tbInfo, err := buildTableInfoWithStmt(ctx, s, dbCharset, dbCollate)
	if err != nil {
		return nil, err
	}
	// Fix issue 17952 which will cause partition range expr can't be parsed as Int.
	// checkTableInfoValidWithStmt will do the constant fold the partition expression first,
	// then checkTableInfoValidExtra will pass the tableInfo check successfully.
	if err = checkTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return nil, err
	}
	if err = checkTableInfoValidExtra(tbInfo); err != nil {
		return nil, err
	}
	return tbInfo, nil
}

// buildTableInfoWithStmt builds model.TableInfo from a SQL statement without validity check
func buildTableInfoWithStmt(ctx sessionctx.Context, s *ast.CreateTableStmt, dbCharset, dbCollate string) (*model.TableInfo, error) {
	colDefs := s.Cols
	tableCharset, tableCollate, err := getCharsetAndCollateInTableOption(0, s.Options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableCharset, tableCollate, err = ResolveCharsetCollation(
		ast.CharsetOpt{Chs: tableCharset, Col: tableCollate},
		ast.CharsetOpt{Chs: dbCharset, Col: dbCollate},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// The column charset haven't been resolved here.
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, colDefs, s.Constraints, tableCharset, tableCollate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = checkConstraintNames(newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tbInfo *model.TableInfo
	tbInfo, err = buildTableInfo(ctx, s.Table.Name, cols, newConstraints, tableCharset, tableCollate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = setTableAutoRandomBits(ctx, tbInfo, colDefs); err != nil {
		return nil, errors.Trace(err)
	}

	tbInfo.Partition, err = buildTablePartitionInfo(ctx, s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = handleTableOptions(s.Options, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}
	return tbInfo, nil
}

func (d *ddl) assignTableID(tbInfo *model.TableInfo) error {
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo.ID = genIDs[0]
	return nil
}

func (d *ddl) assignPartitionIDs(tbInfo *model.TableInfo) error {
	if tbInfo.Partition == nil {
		return nil
	}
	partitionDefs := tbInfo.Partition.Definitions
	genIDs, err := d.genGlobalIDs(len(partitionDefs))
	if err != nil {
		return errors.Trace(err)
	}
	for i := range partitionDefs {
		partitionDefs[i].ID = genIDs[i]
	}
	return nil
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

	// build tableInfo
	var tbInfo *model.TableInfo
	if s.ReferTable != nil {
		tbInfo, err = buildTableInfoWithLike(ident, referTbl.Meta())
	} else {
		tbInfo, err = buildTableInfoWithStmt(ctx, s, schema.Charset, schema.Collate)
	}
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return err
	}

	onExist := OnExistError
	if s.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, schema.Name, tbInfo, onExist, false /*tryRetainID*/)
}

func (d *ddl) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName model.CIStr,
	tbInfo *model.TableInfo,
	onExist OnExist,
	tryRetainID bool,
) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	var oldViewTblID int64
	if oldTable, err := is.TableByName(schema.Name, tbInfo.Name); err == nil {
		err = infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schema.Name, Name: tbInfo.Name})
		switch onExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		case OnExistReplace:
			// only CREATE OR REPLACE VIEW is supported at the moment.
			if tbInfo.View != nil {
				if oldTable.Meta().IsView() {
					oldViewTblID = oldTable.Meta().ID
					break
				}
				// The object to replace isn't a view.
				return ErrWrongObject.GenWithStackByArgs(dbName, tbInfo.Name, "VIEW")
			}
			return err
		default:
			return err
		}
	}

	// FIXME: Implement `tryRetainID`
	if err := d.assignTableID(tbInfo); err != nil {
		return errors.Trace(err)
	}
	if err := d.assignPartitionIDs(tbInfo); err != nil {
		return errors.Trace(err)
	}

	if err := checkTableInfoValidExtra(tbInfo); err != nil {
		return err
	}

	var actionType model.ActionType
	args := []interface{}{tbInfo}
	switch {
	case tbInfo.View != nil:
		actionType = model.ActionCreateView
		args = append(args, onExist == OnExistReplace, oldViewTblID)
	case tbInfo.Sequence != nil:
		actionType = model.ActionCreateSequence
	default:
		actionType = model.ActionCreateTable
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		SchemaName: schema.Name.L,
		Type:       actionType,
		BinlogInfo: &model.HistoryInfo{},
		Args:       args,
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		// table exists, but if_not_exists flags is true, so we ignore this error.
		if onExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			err = nil
		}
	} else if actionType == model.ActionCreateTable {
		d.preSplitAndScatter(ctx, tbInfo, tbInfo.GetPartitionInfo())
		if tbInfo.AutoIncID > 1 {
			// Default tableAutoIncID base is 0.
			// If the first ID is expected to greater than 1, we need to do rebase.
			if err = d.handleAutoIncID(tbInfo, schema.ID, autoid.RowIDAllocType); err != nil {
				return errors.Trace(err)
			}
		}
		if tbInfo.AutoRandID > 1 {
			// Default tableAutoRandID base is 0.
			// If the first ID is expected to greater than 1, we need to do rebase.
			err = d.handleAutoIncID(tbInfo, schema.ID, autoid.AutoRandomType)
		}
	}

	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// preSplitAndScatter performs pre-split and scatter of the table's regions.
// If `pi` is not nil, will only split region for `pi`, this is used when add partition.
func (d *ddl) preSplitAndScatter(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo) {
	sp, ok := d.store.(kv.SplittableStore)
	if !ok || atomic.LoadUint32(&EnableSplitTableRegion) == 0 {
		return
	}
	var (
		preSplit      func()
		scatterRegion bool
	)
	val, err := variable.GetGlobalSystemVar(ctx.GetSessionVars(), variable.TiDBScatterRegion)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] won't scatter region", zap.Error(err))
	} else {
		scatterRegion = variable.TiDBOptOn(val)
	}
	if pi != nil {
		preSplit = func() { splitPartitionTableRegion(ctx, sp, tbInfo, pi, scatterRegion) }
	} else {
		preSplit = func() { splitTableRegion(ctx, sp, tbInfo, scatterRegion) }
	}
	if scatterRegion {
		preSplit()
	} else {
		go preSplit()
	}
}

func (d *ddl) RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schemaID, tbInfo := recoverInfo.SchemaID, recoverInfo.TableInfo
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
		Args: []interface{}{tbInfo, recoverInfo.CurAutoIncID, recoverInfo.DropJobID,
			recoverInfo.SnapshotTS, recoverTableCheckFlagNone, recoverInfo.CurAutoRandID},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) (err error) {
	viewInfo, err := buildViewInfo(ctx, s)
	if err != nil {
		return err
	}

	cols := make([]*table.Column, len(s.Cols))
	for i, v := range s.Cols {
		cols[i] = table.ToColumn(&model.ColumnInfo{
			Name:   v,
			ID:     int64(i),
			Offset: i,
			State:  model.StatePublic,
		})
	}

	tblCharset := ""
	tblCollate := ""
	if v, ok := ctx.GetSessionVars().GetSystemVar("character_set_client"); ok {
		tblCharset = v
	}
	if v, ok := ctx.GetSessionVars().GetSystemVar("collation_connection"); ok {
		tblCollate = v
	}

	tbInfo, err := buildTableInfo(ctx, s.ViewName.Name, cols, nil, tblCharset, tblCollate)
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := OnExistError
	if s.OrReplace {
		onExist = OnExistReplace
	}

	return d.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, onExist, false /*tryRetainID*/)
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

func checkPartitionByHash(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) error {
	pi := tbInfo.Partition
	if err := checkAddPartitionTooManyPartitions(pi.Num); err != nil {
		return err
	}
	if err := checkNoHashPartitions(ctx, pi.Num); err != nil {
		return err
	}
	if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
		return err
	}
	return checkPartitionFuncType(ctx, s, tbInfo)
}

// checkPartitionByRange checks validity of a "BY RANGE" partition.
func checkPartitionByRange(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) error {
	pi := tbInfo.Partition
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
		if err := checkCreatePartitionValue(ctx, tbInfo); err != nil {
			return err
		}

		// s maybe nil when add partition.
		if s == nil {
			return nil
		}

		if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
			return err
		}
		return checkPartitionFuncType(ctx, s, tbInfo)
	}

	// Check for range columns partition.
	if err := checkRangeColumnsPartitionType(tbInfo); err != nil {
		return err
	}

	if s != nil {
		for _, def := range s.Partition.Definitions {
			exprs := def.Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs
			if err := checkRangeColumnsTypeAndValuesMatch(ctx, tbInfo, exprs); err != nil {
				return err
			}
		}
	}

	return checkRangeColumnsPartitionValue(ctx, tbInfo)
}

func checkRangeColumnsPartitionType(tbInfo *model.TableInfo) error {
	for _, col := range tbInfo.Partition.Columns {
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

func checkRangeColumnsPartitionValue(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	// Range columns partition key supports multiple data types with integer、datetime、string.
	pi := tbInfo.Partition
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
	if co != "" {
		if _, err := collate.GetCollationByName(co); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// handleAutoIncID handles auto_increment option in DDL. It creates a ID counter for the table and initiates the counter to a proper value.
// For example if the option sets auto_increment to 10. The counter will be set to 9. So the next allocated ID will be 10.
func (d *ddl) handleAutoIncID(tbInfo *model.TableInfo, schemaID int64, tp autoid.AllocatorType) error {
	allocs := autoid.NewAllocatorsFromTblInfo(d.store, schemaID, tbInfo)
	tbInfo.State = model.StatePublic
	tb, err := table.TableFromMeta(allocs, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	if tp == autoid.RowIDAllocType {
		if err = tb.RebaseAutoID(nil, tbInfo.AutoIncID-1, false, tp); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err = tb.RebaseAutoID(nil, tbInfo.AutoRandID-1, false, tp); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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
		case ast.TableOptionAutoRandomBase:
			tbInfo.AutoRandID = int64(op.UintValue)
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
		case ast.TableOptionCharset, ast.TableOptionCollate:
			// We don't handle charset and collate here since they're handled in `getCharsetAndCollateInTableOption`.
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

// getCharsetAndCollateInColumnDef will iterate collate in the options, validate it by checking the charset
// of column definition. If there's no collate in the option, the default collate of column's charset will be used.
func getCharsetAndCollateInColumnDef(def *ast.ColumnDef) (chs, coll string, err error) {
	chs = def.Tp.Charset
	coll = def.Tp.Collate
	if chs != "" && coll == "" {
		if coll, err = charset.GetDefaultCollation(chs); err != nil {
			return "", "", errors.Trace(err)
		}
	}
	for _, opt := range def.Options {
		if opt.Tp == ast.ColumnOptionCollate {
			info, err := collate.GetCollationByName(opt.StrValue)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			if chs == "" {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", ErrCollationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			coll = info.Name
		}
	}
	return
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
			info, err := collate.GetCollationByName(opt.StrValue)
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

func needToOverwriteColCharset(options []*ast.TableOption) bool {
	for i := len(options) - 1; i >= 0; i-- {
		opt := options[i]
		switch opt.Tp {
		case ast.TableOptionCharset:
			// Only overwrite columns charset if the option contains `CONVERT TO`.
			return opt.UintValue == ast.TableOptionCharsetWithConvertTo
		}
	}
	return false
}

// resolveAlterTableSpec resolves alter table algorithm and removes ignore table spec in specs.
// returns valied specs, and the occurred error.
func resolveAlterTableSpec(ctx sessionctx.Context, specs []*ast.AlterTableSpec) ([]*ast.AlterTableSpec, error) {
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	algorithm := ast.AlgorithmTypeDefault
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

	// Verify whether the algorithm is supported.
	for _, spec := range validSpecs {
		resolvedAlgorithm, err := ResolveAlterAlgorithm(spec, algorithm)
		if err != nil {
			if algorithm != ast.AlgorithmTypeCopy {
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

func isSameTypeMultiSpecs(specs []*ast.AlterTableSpec) bool {
	specType := specs[0].Tp
	for _, spec := range specs {
		if spec.Tp != specType {
			return false
		}
	}
	return true
}

func (d *ddl) AlterTable(ctx sessionctx.Context, ident ast.Ident, specs []*ast.AlterTableSpec) (err error) {
	validSpecs, err := resolveAlterTableSpec(ctx, specs)
	if err != nil {
		return errors.Trace(err)
	}

	is := d.infoHandle.Get()
	if is.TableIsView(ident.Schema, ident.Name) || is.TableIsSequence(ident.Schema, ident.Name) {
		return ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "BASE TABLE")
	}

	if len(validSpecs) > 1 {
		if isSameTypeMultiSpecs(validSpecs) {
			switch validSpecs[0].Tp {
			case ast.AlterTableAddColumns:
				err = d.AddColumns(ctx, ident, validSpecs)
			case ast.AlterTableDropColumn:
				err = d.DropColumns(ctx, ident, validSpecs)
			default:
				return errRunMultiSchemaChanges
			}
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		}
		return errRunMultiSchemaChanges
	}

	for _, spec := range validSpecs {
		var handledCharsetOrCollate bool
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			if len(spec.NewColumns) != 1 {
				err = d.AddColumns(ctx, ident, []*ast.AlterTableSpec{spec})
			} else {
				err = d.AddColumn(ctx, ident, spec)
			}
		case ast.AlterTableAddPartitions:
			err = d.AddTablePartitions(ctx, ident, spec)
		case ast.AlterTableCoalescePartitions:
			err = d.CoalescePartitions(ctx, ident, spec)
		case ast.AlterTableReorganizePartition:
			err = errors.Trace(errUnsupportedReorganizePartition)
		case ast.AlterTableCheckPartitions:
			err = errors.Trace(errUnsupportedCheckPartition)
		case ast.AlterTableRebuildPartition:
			err = errors.Trace(errUnsupportedRebuildPartition)
		case ast.AlterTableOptimizePartition:
			err = errors.Trace(errUnsupportedOptimizePartition)
		case ast.AlterTableRemovePartitioning:
			err = errors.Trace(errUnsupportedRemovePartition)
		case ast.AlterTableRepairPartition:
			err = errors.Trace(errUnsupportedRepairPartition)
		case ast.AlterTableDropColumn:
			err = d.DropColumn(ctx, ident, spec)
		case ast.AlterTableDropIndex:
			err = d.DropIndex(ctx, ident, model.NewCIStr(spec.Name), spec.IfExists)
		case ast.AlterTableDropPrimaryKey:
			err = d.DropIndex(ctx, ident, model.NewCIStr(mysql.PrimaryKeyName), spec.IfExists)
		case ast.AlterTableRenameIndex:
			err = d.RenameIndex(ctx, ident, spec)
		case ast.AlterTableDropPartition:
			err = d.DropTablePartition(ctx, ident, spec)
		case ast.AlterTableTruncatePartition:
			err = d.TruncateTablePartition(ctx, ident, spec)
		case ast.AlterTableExchangePartition:
			err = d.ExchangeTablePartition(ctx, ident, spec)
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = d.CreateIndex(ctx, ident, ast.IndexKeyTypeNone, model.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = d.CreateIndex(ctx, ident, ast.IndexKeyTypeUnique, model.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, false) // IfNotExists should be not applied
			case ast.ConstraintForeignKey:
				// NOTE: we do not handle `symbol` and `index_name` well in the parser and we do not check ForeignKey already exists,
				// so we just also ignore the `if not exists` check.
				err = d.CreateForeignKey(ctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				err = d.CreatePrimaryKey(ctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintFulltext:
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrTableCantHandleFt)
			case ast.ConstraintCheck:
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("ADD CONSTRAINT CHECK"))
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			// NOTE: we do not check `if not exists` and `if exists` for ForeignKey now.
			err = d.DropForeignKey(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			err = d.ModifyColumn(ctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = d.ChangeColumn(ctx, ident, spec)
		case ast.AlterTableRenameColumn:
			err = d.RenameColumn(ctx, ident, spec)
		case ast.AlterTableAlterColumn:
			err = d.AlterColumn(ctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			isAlterTable := true
			err = d.RenameTable(ctx, ident, newIdent, isAlterTable)
		case ast.AlterTableAlterPartition:
			err = d.AlterTablePartition(ctx, ident, spec)
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
					err = d.RebaseAutoID(ctx, ident, int64(opt.UintValue), autoid.RowIDAllocType)
				case ast.TableOptionAutoIdCache:
					if opt.UintValue > uint64(math.MaxInt64) {
						// TODO: Refine this error.
						return errors.New("table option auto_id_cache overflows int64")
					}
					err = d.AlterTableAutoIDCache(ctx, ident, int64(opt.UintValue))
				case ast.TableOptionAutoRandomBase:
					err = d.RebaseAutoID(ctx, ident, int64(opt.UintValue), autoid.AutoRandomType)
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
					needsOverwriteCols := needToOverwriteColCharset(spec.Options)
					err = d.AlterTableCharsetAndCollate(ctx, ident, toCharset, toCollate, needsOverwriteCols)
					handledCharsetOrCollate = true
				}

				if err != nil {
					return errors.Trace(err)
				}
			}
		case ast.AlterTableSetTiFlashReplica:
			err = d.AlterTableSetTiFlashReplica(ctx, ident, spec.TiFlashReplica)
		case ast.AlterTableOrderByColumns:
			err = d.OrderByColumns(ctx, ident)
		case ast.AlterTableIndexInvisible:
			err = d.AlterIndexVisibility(ctx, ident, spec.IndexName, spec.Visibility)
		case ast.AlterTableAlterCheck:
			ctx.GetSessionVars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("ALTER CHECK"))
		case ast.AlterTableDropCheck:
			ctx.GetSessionVars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("DROP CHECK"))
		case ast.AlterTableWithValidation:
			ctx.GetSessionVars().StmtCtx.AppendWarning(errUnsupportedAlterTableWithValidation)
		case ast.AlterTableWithoutValidation:
			ctx.GetSessionVars().StmtCtx.AppendWarning(errUnsupportedAlterTableWithoutValidation)
		default:
			// Nothing to do now.
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) RebaseAutoID(ctx sessionctx.Context, ident ast.Ident, newBase int64, tp autoid.AllocatorType) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	var actionType model.ActionType
	switch tp {
	case autoid.AutoRandomType:
		tbInfo := t.Meta()
		if tbInfo.AutoRandomBits == 0 {
			return errors.Trace(ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomRebaseNotApplicable))
		}
		var autoRandColTp types.FieldType
		for _, c := range tbInfo.Columns {
			if mysql.HasPriKeyFlag(c.Flag) {
				autoRandColTp = c.FieldType
				break
			}
		}
		layout := autoid.NewAutoRandomIDLayout(&autoRandColTp, tbInfo.AutoRandomBits)
		if layout.IncrementalMask()&newBase != newBase {
			errMsg := fmt.Sprintf(autoid.AutoRandomRebaseOverflow, newBase, layout.IncrementalBitsCapacity())
			return errors.Trace(ErrInvalidAutoRandom.GenWithStackByArgs(errMsg))
		}
		actionType = model.ActionRebaseAutoRandomBase
	case autoid.RowIDAllocType:
		actionType = model.ActionRebaseAutoID
	}

	autoID, err := t.Allocators(ctx).Get(tp).NextGlobalAutoID(t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	// If newBase < autoID, we need to do a rebase before returning.
	// Assume there are 2 TiDB servers: TiDB-A with allocator range of 0 ~ 30000; TiDB-B with allocator range of 30001 ~ 60000.
	// If the user sends SQL `alter table t1 auto_increment = 100` to TiDB-B,
	// and TiDB-B finds 100 < 30001 but returns without any handling,
	// then TiDB-A may still allocate 99 for auto_increment column. This doesn't make sense for the user.
	newBase = mathutil.MaxInt64(newBase, autoID)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       actionType,
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

func checkAndCreateNewColumn(ctx sessionctx.Context, ti ast.Ident, schema *model.DBInfo, spec *ast.AlterTableSpec, t table.Table, specNewColumn *ast.ColumnDef) (*table.Column, error) {
	err := checkUnsupportedColumnConstraint(specNewColumn, ti)
	if err != nil {
		return nil, errors.Trace(err)
	}

	colName := specNewColumn.Name.Name.O
	// Check whether added column has existed.
	col := table.FindCol(t.Cols(), colName)
	if col != nil {
		err = infoschema.ErrColumnExists.GenWithStackByArgs(colName)
		if spec.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil, nil
		}
		return nil, err
	}
	if err = checkColumnAttributes(colName, specNewColumn.Tp); err != nil {
		return nil, errors.Trace(err)
	}
	if len(colName) > mysql.MaxColumnNameLength {
		return nil, ErrTooLongIdent.GenWithStackByArgs(colName)
	}

	// If new column is a generated column, do validation.
	// NOTE: we do check whether the column refers other generated
	// columns occurring later in a table, but we don't handle the col offset.
	for _, option := range specNewColumn.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			if err := checkIllegalFn4GeneratedColumn(specNewColumn.Name.Name.L, option.Expr); err != nil {
				return nil, errors.Trace(err)
			}

			if option.Stored {
				return nil, ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Adding generated stored column through ALTER TABLE")
			}

			_, dependColNames := findDependedColumnNames(specNewColumn)
			if err = checkAutoIncrementRef(specNewColumn.Name.Name.L, dependColNames, t.Meta()); err != nil {
				return nil, errors.Trace(err)
			}
			duplicateColNames := make(map[string]struct{}, len(dependColNames))
			for k := range dependColNames {
				duplicateColNames[k] = struct{}{}
			}
			cols := t.Cols()

			if err = checkDependedColExist(dependColNames, cols); err != nil {
				return nil, errors.Trace(err)
			}

			if err = verifyColumnGenerationSingle(duplicateColNames, cols, spec.Position); err != nil {
				return nil, errors.Trace(err)
			}
		}
		// Specially, since sequence has been supported, if a newly added column has a
		// sequence nextval function as it's default value option, it won't fill the
		// known rows with specific sequence next value under current add column logic.
		// More explanation can refer: TestSequenceDefaultLogic's comment in sequence_test.go
		if option.Tp == ast.ColumnOptionDefaultValue {
			_, isSeqExpr, err := tryToGetSequenceDefaultValue(option)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if isSeqExpr {
				return nil, errors.Trace(ErrAddColumnWithSequenceAsDefault.GenWithStackByArgs(specNewColumn.Name.Name.O))
			}
		}
	}

	tableCharset, tableCollate, err := ResolveCharsetCollation(
		ast.CharsetOpt{Chs: t.Meta().Charset, Col: t.Meta().Collate},
		ast.CharsetOpt{Chs: schema.Charset, Col: schema.Collate},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Ignore table constraints now, they will be checked later.
	// We use length(t.Cols()) as the default offset firstly, we will change the column's offset later.
	col, _, err = buildColumnAndConstraint(
		ctx,
		len(t.Cols()),
		specNewColumn,
		nil,
		tableCharset,
		tableCollate,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	col.OriginDefaultValue, err = generateOriginDefaultValue(col.ToInfo())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return col, nil
}

// AddColumn will add a new column to the table.
func (d *ddl) AddColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + 1); err != nil {
		return errors.Trace(err)
	}
	col, err := checkAndCreateNewColumn(ctx, ti, schema, spec, t, specNewColumn)
	if err != nil {
		return errors.Trace(err)
	}
	// Added column has existed and if_not_exists flag is true.
	if col == nil {
		return nil
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
	// column exists, but if_not_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnExists.Equal(err) && spec.IfNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AddColumns will add multi new columns to the table.
func (d *ddl) AddColumns(ctx sessionctx.Context, ti ast.Ident, specs []*ast.AlterTableSpec) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	// Check all the columns at once.
	addingColumnNames := make(map[string]bool)
	dupColumnNames := make(map[string]bool)
	for _, spec := range specs {
		for _, specNewColumn := range spec.NewColumns {
			if !addingColumnNames[specNewColumn.Name.Name.L] {
				addingColumnNames[specNewColumn.Name.Name.L] = true
				continue
			}
			if !spec.IfNotExists {
				return errors.Trace(infoschema.ErrColumnExists.GenWithStackByArgs(specNewColumn.Name.Name.O))
			}
			dupColumnNames[specNewColumn.Name.Name.L] = true
		}
	}
	columns := make([]*table.Column, 0, len(addingColumnNames))
	positions := make([]*ast.ColumnPosition, 0, len(addingColumnNames))
	offsets := make([]int, 0, len(addingColumnNames))
	ifNotExists := make([]bool, 0, len(addingColumnNames))
	newColumnsCount := 0
	// Check the columns one by one.
	for _, spec := range specs {
		for _, specNewColumn := range spec.NewColumns {
			if spec.IfNotExists && dupColumnNames[specNewColumn.Name.Name.L] {
				err = infoschema.ErrColumnExists.GenWithStackByArgs(specNewColumn.Name.Name.O)
				ctx.GetSessionVars().StmtCtx.AppendNote(err)
				continue
			}
			col, err := checkAndCreateNewColumn(ctx, ti, schema, spec, t, specNewColumn)
			if err != nil {
				return errors.Trace(err)
			}
			// Added column has existed and if_not_exists flag is true.
			if col == nil && spec.IfNotExists {
				continue
			}
			columns = append(columns, col)
			positions = append(positions, spec.Position)
			offsets = append(offsets, 0)
			ifNotExists = append(ifNotExists, spec.IfNotExists)
			newColumnsCount++
		}
	}
	if newColumnsCount == 0 {
		return nil
	}
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + newColumnsCount); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionAddColumns,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{columns, positions, offsets, ifNotExists},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
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
	meta.Partition = &tmp
	err = checkPartitionByRange(ctx, meta, nil)
	meta.Partition = pi
	if err != nil {
		if ErrSameNamePartition.Equal(err) && spec.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
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
	if ErrSameNamePartition.Equal(err) && spec.IfNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
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

	pids := make([]int64, len(spec.PartitionNames))
	for i, name := range spec.PartitionNames {
		pid, err := tables.FindPartitionByName(meta, name.L)
		if err != nil {
			return errors.Trace(err)
		}
		pids[i] = pid
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionTruncateTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{pids},
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

	partNames := make([]string, len(spec.PartitionNames))
	for i, partCIName := range spec.PartitionNames {
		partNames[i] = partCIName.L
	}
	err = checkDropTablePartition(meta, partNames)
	if err != nil {
		if ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionDropTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partNames},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		if ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkFieldTypeCompatible(ft *types.FieldType, other *types.FieldType) bool {
	// int(1) could match the type with int(8)
	partialEqual := ft.Tp == other.Tp &&
		ft.Decimal == other.Decimal &&
		ft.Charset == other.Charset &&
		ft.Collate == other.Collate &&
		(ft.Flen == other.Flen || ft.StorageLength() != types.VarStorageLen) &&
		mysql.HasUnsignedFlag(ft.Flag) == mysql.HasUnsignedFlag(other.Flag) &&
		mysql.HasAutoIncrementFlag(ft.Flag) == mysql.HasAutoIncrementFlag(other.Flag) &&
		mysql.HasNotNullFlag(ft.Flag) == mysql.HasNotNullFlag(other.Flag) &&
		mysql.HasZerofillFlag(ft.Flag) == mysql.HasZerofillFlag(other.Flag) &&
		mysql.HasBinaryFlag(ft.Flag) == mysql.HasBinaryFlag(other.Flag) &&
		mysql.HasPriKeyFlag(ft.Flag) == mysql.HasPriKeyFlag(other.Flag)
	if !partialEqual || len(ft.Elems) != len(other.Elems) {
		return false
	}
	for i := range ft.Elems {
		if ft.Elems[i] != other.Elems[i] {
			return false
		}
	}
	return true
}

func checkTiFlashReplicaCompatible(source *model.TiFlashReplicaInfo, target *model.TiFlashReplicaInfo) bool {
	if source == target {
		return true
	}
	if source == nil || target == nil {
		return false
	}
	if source.Count != target.Count ||
		source.Available != target.Available || len(source.LocationLabels) != len(target.LocationLabels) {
		return false
	}
	for i, lable := range source.LocationLabels {
		if target.LocationLabels[i] != lable {
			return false
		}
	}
	return true
}

func checkTableDefCompatible(source *model.TableInfo, target *model.TableInfo) error {
	// check auto_random
	if source.AutoRandomBits != target.AutoRandomBits ||
		source.Charset != target.Charset ||
		source.Collate != target.Collate ||
		source.ShardRowIDBits != target.ShardRowIDBits ||
		source.MaxShardRowIDBits != target.MaxShardRowIDBits ||
		!checkTiFlashReplicaCompatible(source.TiFlashReplica, target.TiFlashReplica) {
		return errors.Trace(ErrTablesDifferentMetadata)
	}
	if len(source.Cols()) != len(target.Cols()) {
		return errors.Trace(ErrTablesDifferentMetadata)
	}
	// Col compatible check
	for i, sourceCol := range source.Cols() {
		targetCol := target.Cols()[i]
		if isVirtualGeneratedColumn(sourceCol) != isVirtualGeneratedColumn(targetCol) {
			return ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Exchanging partitions for non-generated columns")
		}
		// It should strictyle compare expressions for generated columns
		if sourceCol.Name.L != targetCol.Name.L ||
			sourceCol.Hidden != targetCol.Hidden ||
			!checkFieldTypeCompatible(&sourceCol.FieldType, &targetCol.FieldType) ||
			sourceCol.GeneratedExprString != targetCol.GeneratedExprString {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		if sourceCol.State != model.StatePublic ||
			targetCol.State != model.StatePublic {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		if sourceCol.ID != targetCol.ID {
			return ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("column: %s", sourceCol.Name))
		}
	}
	if len(source.Indices) != len(target.Indices) {
		return errors.Trace(ErrTablesDifferentMetadata)
	}
	for _, sourceIdx := range source.Indices {
		var compatIdx *model.IndexInfo
		for _, targetIdx := range target.Indices {
			if strings.EqualFold(sourceIdx.Name.L, targetIdx.Name.L) {
				compatIdx = targetIdx
			}
		}
		// No match index
		if compatIdx == nil {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		// Index type is not compatible
		if sourceIdx.Tp != compatIdx.Tp ||
			sourceIdx.Unique != compatIdx.Unique ||
			sourceIdx.Primary != compatIdx.Primary {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		// The index column
		if len(sourceIdx.Columns) != len(compatIdx.Columns) {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		for i, sourceIdxCol := range sourceIdx.Columns {
			compatIdxCol := compatIdx.Columns[i]
			if sourceIdxCol.Length != compatIdxCol.Length ||
				sourceIdxCol.Name.L != compatIdxCol.Name.L {
				return errors.Trace(ErrTablesDifferentMetadata)
			}
		}
		if sourceIdx.ID != compatIdx.ID {
			return ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("index: %s", sourceIdx.Name))
		}
	}

	return nil
}

func checkExchangePartition(pt *model.TableInfo, nt *model.TableInfo) error {
	if nt.IsView() || nt.IsSequence() {
		return errors.Trace(ErrCheckNoSuchTable)
	}
	if pt.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}
	if nt.GetPartitionInfo() != nil {
		return errors.Trace(ErrPartitionExchangePartTable.GenWithStackByArgs(nt.Name))
	}

	if nt.ForeignKeys != nil {
		return errors.Trace(ErrPartitionExchangeForeignKey.GenWithStackByArgs(nt.Name))
	}

	// NOTE: if nt is temporary table, it should be checked
	return nil
}

func (d *ddl) ExchangeTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	ptSchema, pt, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	ptMeta := pt.Meta()

	ntIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
	ntSchema, nt, err := d.getSchemaAndTableByIdent(ctx, ntIdent)
	if err != nil {
		return errors.Trace(err)
	}

	ntMeta := nt.Meta()

	err = checkExchangePartition(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	partName := spec.PartitionNames[0].L

	// NOTE: if pt is subPartitioned, it should be checked

	defID, err := tables.FindPartitionByName(ptMeta, partName)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkTableDefCompatible(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   ntSchema.ID,
		TableID:    ntMeta.ID,
		SchemaName: ntSchema.Name.L,
		Type:       model.ActionExchangeTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{defID, ptSchema.ID, ptMeta.ID, partName, spec.WithValidation},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (d *ddl) DropColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	isDropable, err := checkIsDroppableColumn(ctx, t, spec)
	if err != nil {
		return err
	}
	if !isDropable {
		return nil
	}
	colName := spec.OldColumnName.Name

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionDropColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colName},
	}

	err = d.doDDLJob(ctx, job)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if ErrCantDropFieldOrKey.Equal(err) && spec.IfExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropColumns will drop multi-columns from the table, now we don't support drop the column with index covered.
func (d *ddl) DropColumns(ctx sessionctx.Context, ti ast.Ident, specs []*ast.AlterTableSpec) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo := t.Meta()

	dropingColumnNames := make(map[string]bool)
	dupColumnNames := make(map[string]bool)
	for _, spec := range specs {
		if !dropingColumnNames[spec.OldColumnName.Name.L] {
			dropingColumnNames[spec.OldColumnName.Name.L] = true
		} else {
			if spec.IfExists {
				dupColumnNames[spec.OldColumnName.Name.L] = true
				continue
			}
			return errors.Trace(ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", spec.OldColumnName.Name.O))
		}
	}

	ifExists := make([]bool, 0, len(specs))
	colNames := make([]model.CIStr, 0, len(specs))
	for _, spec := range specs {
		if spec.IfExists && dupColumnNames[spec.OldColumnName.Name.L] {
			err = ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", spec.OldColumnName.Name.L)
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			continue
		}
		isDropable, err := checkIsDroppableColumn(ctx, t, spec)
		if err != nil {
			return err
		}
		// Column can't drop and if_exists flag is true.
		if !isDropable && spec.IfExists {
			continue
		}
		colNames = append(colNames, spec.OldColumnName.Name)
		ifExists = append(ifExists, spec.IfExists)
	}
	if len(colNames) == 0 {
		return nil
	}
	if len(tblInfo.Columns) == len(colNames) {
		return ErrCantRemoveAllFields.GenWithStack("can't drop all columns in table %s",
			tblInfo.Name)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionDropColumns,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colNames, ifExists},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkIsDroppableColumn(ctx sessionctx.Context, t table.Table, spec *ast.AlterTableSpec) (isDrapable bool, err error) {
	tblInfo := t.Meta()
	// Check whether dropped column has existed.
	colName := spec.OldColumnName.Name
	col := table.FindCol(t.VisibleCols(), colName.L)
	if col == nil {
		err = ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
		if spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return false, nil
		}
		return false, err
	}

	if err = isDroppableColumn(tblInfo, colName); err != nil {
		return false, errors.Trace(err)
	}
	// We don't support dropping column with PK handle covered now.
	if col.IsPKHandleColumn(tblInfo) {
		return false, errUnsupportedPKHandle
	}
	return true, nil
}

// checkModifyCharsetAndCollation returns error when the charset or collation is not modifiable.
// needRewriteCollationData is used when trying to modify the collation of a column, it is true when the column is with
// index because index of a string column is collation-aware.
func checkModifyCharsetAndCollation(toCharset, toCollate, origCharset, origCollate string, needRewriteCollationData bool) error {
	if !charset.ValidCharsetAndCollation(toCharset, toCollate) {
		return ErrUnknownCharacterSet.GenWithStack("Unknown character set: '%s', collation: '%s'", toCharset, toCollate)
	}

	if needRewriteCollationData && collate.NewCollationEnabled() && !collate.CompatibleCollate(origCollate, toCollate) {
		return errUnsupportedModifyCollation.GenWithStackByArgs(origCollate, toCollate)
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
		msg := fmt.Sprintf("change collate from %s to %s", origCollate, toCollate)
		return errUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	return nil
}

// CheckModifyTypeCompatible checks whether changes column type to another is compatible considering
// field length and precision.
func CheckModifyTypeCompatible(origin *types.FieldType, to *types.FieldType) error {
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
	return nil
}

// checkModifyTypes checks if the 'origin' type can be modified to 'to' type with out the need to
// change or check existing data in the table.
// It returns error if the two types has incompatible Charset and Collation, different sign, different
// digital/string types, or length of new Flen and Decimal is less than origin.
func checkModifyTypes(origin *types.FieldType, to *types.FieldType, needRewriteCollationData bool) error {
	err := CheckModifyTypeCompatible(origin, to)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkModifyCharsetAndCollation(to.Charset, to.Collate, origin.Charset, origin.Collate, needRewriteCollationData)
	return errors.Trace(err)
}

func setDefaultValue(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) (bool, error) {
	hasDefaultValue := false
	value, isSeqExpr, err := getDefaultValue(ctx, col, option)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	if isSeqExpr {
		if err := checkSequenceDefaultValue(col); err != nil {
			return false, errors.Trace(err)
		}
		col.DefaultIsExpr = isSeqExpr
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
			return errUnsupportedModifyColumn.GenWithStack("can't change column constraint - %v", opt.Tp)
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
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs("can't modify with references"))
		case ast.ColumnOptionFulltext:
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs("can't modify with full text"))
		case ast.ColumnOptionCheck:
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs("can't modify with check"))
		// Ignore ColumnOptionAutoRandom. It will be handled later.
		case ast.ColumnOptionAutoRandom:
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

	var chs, coll string
	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.Charset) == 0 && t.Meta().Version < model.TableInfoVersion1 {
		chs = col.FieldType.Charset
		coll = col.FieldType.Collate
	} else {
		chs, coll, err = getCharsetAndCollateInColumnDef(specNewColumn)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chs, coll, err = ResolveCharsetCollation(
			ast.CharsetOpt{Chs: chs, Col: coll},
			ast.CharsetOpt{Chs: t.Meta().Charset, Col: t.Meta().Collate},
			ast.CharsetOpt{Chs: schema.Charset, Col: schema.Collate},
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if err = setCharsetCollationFlenDecimal(&newCol.FieldType, chs, coll); err != nil {
		return nil, errors.Trace(err)
	}

	if err = processColumnOptions(ctx, newCol, specNewColumn.Options); err != nil {
		return nil, errors.Trace(err)
	}

	if err = checkModifyTypes(&col.FieldType, &newCol.FieldType, isColumnWithIndex(col.Name.L, t.Meta().Indices)); err != nil {
		if strings.Contains(err.Error(), "Unsupported modifying collation") {
			colErrMsg := "Unsupported modifying collation of column '%s' from '%s' to '%s' when index is defined on it."
			err = errUnsupportedModifyCollation.GenWithStack(colErrMsg, col.Name.L, col.Collate, newCol.Collate)
		}
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
		return nil, errUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	// Disallow modifying column from auto_increment to not auto_increment if the session variable `AllowRemoveAutoInc` is false.
	if !ctx.GetSessionVars().AllowRemoveAutoInc && mysql.HasAutoIncrementFlag(col.Flag) && !mysql.HasAutoIncrementFlag(newCol.Flag) {
		return nil, errUnsupportedModifyColumn.GenWithStackByArgs("can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled")
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

	var newAutoRandBits uint64
	if newAutoRandBits, err = checkAutoRandom(t.Meta(), col, specNewColumn); err != nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{&newCol, originalColName, spec.Position, modifyColumnTp, newAutoRandBits},
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
		if !containColumn {
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

func checkAutoRandom(tableInfo *model.TableInfo, originCol *table.Column, specNewColumn *ast.ColumnDef) (uint64, error) {
	// Disallow add/drop actions on auto_random.
	var oldRandBits uint64
	if tableInfo.PKIsHandle && (tableInfo.GetPkName().L == originCol.Name.L) {
		oldRandBits = tableInfo.AutoRandomBits
	}
	newRandBits, err := extractAutoRandomBitsFromColDef(specNewColumn)
	if err != nil {
		return 0, errors.Trace(err)
	}
	switch {
	case oldRandBits == newRandBits:
		break
	case oldRandBits == 0 || newRandBits == 0:
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterErrMsg)
	case autoid.MaxAutoRandomBits < newRandBits:
		errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
			autoid.MaxAutoRandomBits, newRandBits, specNewColumn.Name.Name.O)
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	case oldRandBits < newRandBits:
		break // Increasing auto_random shard bits is allowed.
	case oldRandBits > newRandBits:
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomDecreaseBitErrMsg)
	}

	if oldRandBits != 0 {
		// Disallow changing the column field type.
		if originCol.Tp != specNewColumn.Tp.Tp {
			return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomModifyColTypeErrMsg)
		}
		// Disallow changing auto_increment on auto_random column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionAutoIncrement) != mysql.HasAutoIncrementFlag(originCol.Flag) {
			return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
		}
		// Disallow specifying a default value on auto_random column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionDefaultValue) {
			return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
	}
	return newRandBits, nil
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
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.GenWithStackByArgs(spec.OldColumnName.Name, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// RenameColumn renames an existing column.
func (d *ddl) RenameColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	oldColName := spec.OldColumnName.Name
	newColName := spec.NewColumnName.Name
	if oldColName.L == newColName.L {
		return nil
	}

	schema, tbl, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	oldCol := table.FindCol(tbl.VisibleCols(), oldColName.L)
	if oldCol == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldColName, ident.Name)
	}

	allCols := tbl.Cols()
	colWithNewNameAlreadyExist := table.FindCol(allCols, newColName.L) != nil
	if colWithNewNameAlreadyExist {
		return infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
	}

	if fkInfo := getColumnForeignKeyInfo(oldColName.L, tbl.Meta().ForeignKeys); fkInfo != nil {
		return errFKIncompatibleColumns.GenWithStackByArgs(oldColName, fkInfo.Name)
	}

	// Check generated expression.
	for _, col := range allCols {
		if col.GeneratedExpr == nil {
			continue
		}
		dependedColNames := findColumnNamesInExpr(col.GeneratedExpr)
		for _, name := range dependedColNames {
			if name.Name.L == oldColName.L {
				return ErrBadField.GenWithStackByArgs(oldColName.O, "generated column function")
			}
		}
	}

	newCol := oldCol.Clone()
	newCol.Name = newColName
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbl.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{&newCol, oldColName, spec.Position, 0},
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
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
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

// AlterTableCharsetAndCollate changes the table charset and collate.
func (d *ddl) AlterTableCharsetAndCollate(ctx sessionctx.Context, ident ast.Ident, toCharset, toCollate string, needsOverwriteCols bool) error {
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
	doNothing, err := checkAlterTableCharset(tb.Meta(), schema, toCharset, toCollate, needsOverwriteCols)
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
		Args:       []interface{}{toCharset, toCollate, needsOverwriteCols},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableSetTiFlashReplica sets the TiFlash replicas info.
func (d *ddl) AlterTableSetTiFlashReplica(ctx sessionctx.Context, ident ast.Ident, replicaInfo *ast.TiFlashReplicaSpec) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	tbReplicaInfo := tb.Meta().TiFlashReplica
	if tbReplicaInfo != nil && tbReplicaInfo.Count == replicaInfo.Count &&
		len(tbReplicaInfo.LocationLabels) == len(replicaInfo.Labels) {
		changed := false
		for i, lable := range tbReplicaInfo.LocationLabels {
			if replicaInfo.Labels[i] != lable {
				changed = true
				break
			}
		}
		if !changed {
			return nil
		}
	}

	err = checkTiFlashReplicaCount(ctx, replicaInfo.Count)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionSetTiFlashReplica,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{*replicaInfo},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkTiFlashReplicaCount(ctx sessionctx.Context, replicaCount uint64) error {
	// Check the tiflash replica count should be less than the total tiflash stores.
	tiflashStoreCnt, err := infoschema.GetTiFlashStoreCount(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if replicaCount > tiflashStoreCnt {
		return errors.Errorf("the tiflash replica count: %d should be less than the total tiflash server count: %d", replicaCount, tiflashStoreCnt)
	}
	return nil
}

// UpdateTableReplicaInfo updates the table flash replica infos.
func (d *ddl) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	is := d.infoHandle.Get()
	tb, ok := is.TableByID(physicalID)
	if !ok {
		tb, _ = is.FindTableByPartitionID(physicalID)
		if tb == nil {
			return infoschema.ErrTableNotExists.GenWithStack("Table which ID = %d does not exist.", physicalID)
		}
	}
	tbInfo := tb.Meta()
	if tbInfo.TiFlashReplica == nil || (tbInfo.ID == physicalID && tbInfo.TiFlashReplica.Available == available) ||
		(tbInfo.ID != physicalID && available == tbInfo.TiFlashReplica.IsPartitionAvailable(physicalID)) {
		return nil
	}

	db, ok := is.SchemaByTable(tbInfo)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStack("Database of table `%s` does not exist.", tb.Meta().Name)
	}

	job := &model.Job{
		SchemaID:   db.ID,
		TableID:    tb.Meta().ID,
		SchemaName: db.Name.L,
		Type:       model.ActionUpdateTiFlashReplicaStatus,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{available, physicalID},
	}
	err := d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// checkAlterTableCharset uses to check is it possible to change the charset of table.
// This function returns 2 variable:
// doNothing: if doNothing is true, means no need to change any more, because the target charset is same with the charset of table.
// err: if err is not nil, means it is not possible to change table charset to target charset.
func checkAlterTableCharset(tblInfo *model.TableInfo, dbInfo *model.DBInfo, toCharset, toCollate string, needsOverwriteCols bool) (doNothing bool, err error) {
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

	// The table charset may be "", if the table is create in old TiDB version, such as v2.0.8.
	// This DDL will update the table charset to default charset.
	origCharset, origCollate, err = ResolveCharsetCollation(
		ast.CharsetOpt{Chs: origCharset, Col: origCollate},
		ast.CharsetOpt{Chs: dbInfo.Charset, Col: dbInfo.Collate},
	)
	if err != nil {
		return doNothing, err
	}

	if err = checkModifyCharsetAndCollation(toCharset, toCollate, origCharset, origCollate, false); err != nil {
		return doNothing, err
	}
	if !needsOverwriteCols {
		// If we don't change the charset and collation of columns, skip the next checks.
		return doNothing, nil
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
		if err = checkModifyCharsetAndCollation(toCharset, toCollate, col.Charset, col.Collate, isColumnWithIndex(col.Name.L, tblInfo.Indices)); err != nil {
			if strings.Contains(err.Error(), "Unsupported modifying collation") {
				colErrMsg := "Unsupported converting collation of column '%s' from '%s' to '%s' when index is defined on it."
				err = errUnsupportedModifyCollation.GenWithStack(colErrMsg, col.Name.L, col.Collate, toCollate)
			}
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
	if tb.Meta().IsSequence() {
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
	if tb.Meta().IsView() || tb.Meta().IsSequence() {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(schema.Name.O, tb.Meta().Name.O)
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
		ctx.AddTableLock([]model.TableLockTpInfo{{SchemaID: schema.ID, TableID: newTableID, Tp: tb.Meta().Lock.Tp}})
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
		return ErrErrorOnRename.GenWithStackByArgs(
			fmt.Sprintf("%s.%s", oldIdent.Schema, oldIdent.Name),
			fmt.Sprintf("%s.%s", newIdent.Schema, newIdent.Name),
			168,
			fmt.Sprintf("Database `%s` doesn't exist", newIdent.Schema))
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
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption) error {
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
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil ||
		// If the table's PKIsHandle is true, it also means that this table has a primary key.
		t.Meta().PKIsHandle {
		return infoschema.ErrMultiplePriKey
	}

	// Primary keys cannot include expression index parts. A primary key requires the generated column to be stored,
	// but expression index parts are implemented as virtual generated columns, not stored generated columns.
	for _, idxPart := range indexPartSpecifications {
		if idxPart.Expr != nil {
			return ErrFunctionalIndexPrimaryKey
		}
	}

	tblInfo := t.Meta()
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	indexColumns, err := buildIndexColumns(tblInfo.Columns, indexPartSpecifications)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = checkPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
		return err
	}

	if tblInfo.GetPartitionInfo() != nil {
		if err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexColumns, tblInfo, true); err != nil {
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
		SchemaName: schema.Name.L,
		Type:       model.ActionAddPrimaryKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, indexPartSpecifications, indexOption, sqlMode},
		Priority:   ctx.GetSessionVars().DDLReorgPriority,
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildHiddenColumnInfo(ctx sessionctx.Context, t table.Table, indexPartSpecifications []*ast.IndexPartSpecification, indexName model.CIStr) ([]*model.ColumnInfo, error) {
	tblInfo := t.Meta()
	hiddenCols := make([]*model.ColumnInfo, 0, len(indexPartSpecifications))
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		idxPart.Column = &ast.ColumnName{Name: model.NewCIStr(fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i))}
		// Check whether the hidden columns have existed.
		col := table.FindCol(t.Cols(), idxPart.Column.Name.L)
		if col != nil {
			// TODO: Use expression index related error.
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name.String())
		}
		idxPart.Length = types.UnspecifiedLength
		// The index part is an expression, prepare a hidden column for it.
		if len(idxPart.Column.Name.L) > mysql.MaxColumnNameLength {
			// TODO: Refine the error message.
			return nil, ErrTooLongIdent.GenWithStackByArgs("hidden column")
		}
		// TODO: refine the error message.
		if err := checkIllegalFn4GeneratedColumn("expression index", idxPart.Expr); err != nil {
			return nil, errors.Trace(err)
		}

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		sb.Reset()
		err := idxPart.Expr.Restore(restoreCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		expr, err := expression.RewriteSimpleExprWithTableInfo(ctx, tblInfo, idxPart.Expr)
		if err != nil {
			// TODO: refine the error message.
			return nil, err
		}
		if _, ok := expr.(*expression.Column); ok {
			return nil, ErrFunctionalIndexOnField
		}

		colInfo := &model.ColumnInfo{
			Name:                idxPart.Column.Name,
			GeneratedExprString: sb.String(),
			GeneratedStored:     false,
			Version:             model.CurrLatestColumnInfoVersion,
			Dependences:         make(map[string]struct{}),
			Hidden:              true,
			FieldType:           *expr.GetType(),
		}
		checkDependencies := make(map[string]struct{})
		for _, colName := range findColumnNamesInExpr(idxPart.Expr) {
			colInfo.Dependences[colName.Name.O] = struct{}{}
			checkDependencies[colName.Name.O] = struct{}{}
		}
		if err = checkDependedColExist(checkDependencies, t.Cols()); err != nil {
			return nil, errors.Trace(err)
		}
		if err = checkAutoIncrementRef("", colInfo.Dependences, tblInfo); err != nil {
			return nil, errors.Trace(err)
		}
		idxPart.Expr = nil
		hiddenCols = append(hiddenCols, colInfo)
	}
	return hiddenCols, nil
}

func (d *ddl) CreateIndex(ctx sessionctx.Context, ti ast.Ident, keyType ast.IndexKeyType, indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	// not support Spatial and FullText index
	if keyType == ast.IndexKeyTypeFullText || keyType == ast.IndexKeyTypeSpatial {
		return errUnsupportedIndexType.GenWithStack("FULLTEXT and SPATIAL index is not supported")
	}
	unique := keyType == ast.IndexKeyTypeUnique
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		colName := model.NewCIStr("expression_index")
		if indexPartSpecifications[0].Column != nil {
			colName = indexPartSpecifications[0].Column.Name
		}
		indexName = getAnonymousIndex(t, colName)
	}

	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		if indexInfo.State != model.StatePublic {
			// NOTE: explicit error message. See issue #18363.
			err = ErrDupKeyName.GenWithStack("index already exist %s; "+
				"a background job is trying to add the same index, "+
				"please check by `ADMIN SHOW DDL JOBS`", indexName)
		} else {
			err = ErrDupKeyName.GenWithStack("index already exist %s", indexName)
		}
		if ifNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()

	// Build hidden columns if necessary.
	hiddenCols, err := buildHiddenColumnInfo(ctx, t, indexPartSpecifications, indexName)
	if err != nil {
		return err
	}
	if len(hiddenCols) > 0 && !config.GetGlobalConfig().Experimental.AllowsExpressionIndex {
		return ErrUnsupportedExpressionIndex
	}
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + len(hiddenCols)); err != nil {
		return errors.Trace(err)
	}

	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	indexColumns, err := buildIndexColumns(append(tblInfo.Columns, hiddenCols...), indexPartSpecifications)
	if err != nil {
		return errors.Trace(err)
	}
	if unique && tblInfo.GetPartitionInfo() != nil {
		if err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexColumns, tblInfo, false); err != nil {
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
		Args:       []interface{}{unique, indexName, indexPartSpecifications, indexOption, hiddenCols},
		Priority:   ctx.GetSessionVars().DDLReorgPriority,
	}

	err = d.doDDLJob(ctx, job)
	// key exists, but if_not_exists flags is true, so we ignore this error.
	if ErrDupKeyName.Equal(err) && ifNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildFKInfo(fkName model.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef, cols []*table.Column, tbInfo *model.TableInfo) (*model.FKInfo, error) {
	if len(keys) != len(refer.IndexPartSpecifications) {
		return nil, infoschema.ErrForeignKeyNotMatch.GenWithStackByArgs("foreign key without name")
	}

	// all base columns of stored generated columns
	baseCols := make(map[string]struct{})
	for _, col := range cols {
		if col.IsGenerated() && col.GeneratedStored {
			for name := range col.Dependences {
				baseCols[name] = struct{}{}
			}
		}
	}

	fkInfo := &model.FKInfo{
		Name:     fkName,
		RefTable: refer.Table.Name,
		Cols:     make([]model.CIStr, len(keys)),
	}

	for i, key := range keys {
		// Check add foreign key to generated columns
		// For more detail, see https://dev.mysql.com/doc/refman/8.0/en/innodb-foreign-key-constraints.html#innodb-foreign-key-generated-columns
		for _, col := range cols {
			if col.Name.L != key.Column.Name.L {
				continue
			}
			if col.IsGenerated() {
				// Check foreign key on virtual generated columns
				if !col.GeneratedStored {
					return nil, infoschema.ErrCannotAddForeign
				}

				// Check wrong reference options of foreign key on stored generated columns
				switch refer.OnUpdate.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, errWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON UPDATE " + refer.OnUpdate.ReferOpt.String())
				}
				switch refer.OnDelete.ReferOpt {
				case ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, errWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON DELETE " + refer.OnDelete.ReferOpt.String())
				}
				continue
			}
			// Check wrong reference options of foreign key on base columns of stored generated columns
			if _, ok := baseCols[col.Name.L]; ok {
				switch refer.OnUpdate.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
				switch refer.OnDelete.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
			}
		}
		if table.FindCol(cols, key.Column.Name.O) == nil {
			return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
		}
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.RefCols = make([]model.CIStr, len(refer.IndexPartSpecifications))
	for i, key := range refer.IndexPartSpecifications {
		fkInfo.RefCols[i] = key.Column.Name
	}

	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUpdate = int(refer.OnUpdate.ReferOpt)

	return fkInfo, nil
}

func (d *ddl) CreateForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName model.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	fkInfo, err := buildFKInfo(fkName, keys, refer, t.Cols(), t.Meta())
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

func (d *ddl) DropIndex(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr, ifExists bool) error {
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
		err = ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
		if ifExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
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
		SchemaName: schema.Name.L,
		Type:       jobTp,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexName},
	}

	err = d.doDDLJob(ctx, job)
	// index not exists, but if_exists flags is true, so we ignore this error.
	if ErrCantDropFieldOrKey.Equal(err) && ifExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
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
			return nil, errors.Trace(err)
		}
		if err := checkTooLongTable(def.Name); err != nil {
			return nil, err
		}
		// For RANGE partition only VALUES LESS THAN should be possible.
		clause := def.Clause.(*ast.PartitionDefinitionClauseLessThan)
		if len(part.Columns) > 0 {
			if err := checkRangeColumnsTypeAndValuesMatch(ctx, meta, clause.Exprs); err != nil {
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

func checkRangeColumnsTypeAndValuesMatch(ctx sessionctx.Context, meta *model.TableInfo, exprs []ast.ExprNode) error {
	// Validate() has already checked len(colNames) = len(exprs)
	// create table ... partition by range columns (cols)
	// partition p0 values less than (expr)
	// check the type of cols[i] and expr is consistent.
	colNames := meta.Partition.Columns
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
		if t.Meta().IsView() || t.Meta().IsSequence() {
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
		if t.Meta().IsView() || t.Meta().IsSequence() {
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

func (d *ddl) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	// Existence of DB and table has been checked in the preprocessor.
	oldTableInfo, ok := (ctx.Value(domainutil.RepairedTable)).(*model.TableInfo)
	if !ok || oldTableInfo == nil {
		return ErrRepairTableFail.GenWithStack("Failed to get the repaired table")
	}
	oldDBInfo, ok := (ctx.Value(domainutil.RepairedDatabase)).(*model.DBInfo)
	if !ok || oldDBInfo == nil {
		return ErrRepairTableFail.GenWithStack("Failed to get the repaired database")
	}
	// By now only support same DB repair.
	if createStmt.Table.Schema.L != oldDBInfo.Name.L {
		return ErrRepairTableFail.GenWithStack("Repaired table should in same database with the old one")
	}

	// It is necessary to specify the table.ID and partition.ID manually.
	newTableInfo, err := buildTableInfoWithCheck(ctx, createStmt, oldTableInfo.Charset, oldTableInfo.Collate)
	if err != nil {
		return errors.Trace(err)
	}
	// Override newTableInfo with oldTableInfo's element necessary.
	// TODO: There may be more element assignments here, and the new TableInfo should be verified with the actual data.
	newTableInfo.ID = oldTableInfo.ID
	if err = checkAndOverridePartitionID(newTableInfo, oldTableInfo); err != nil {
		return err
	}
	newTableInfo.AutoIncID = oldTableInfo.AutoIncID
	// If any old columnInfo has lost, that means the old column ID lost too, repair failed.
	for i, newOne := range newTableInfo.Columns {
		old := getColumnInfoByName(oldTableInfo, newOne.Name.L)
		if old == nil {
			return ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " has lost")
		}
		if newOne.Tp != old.Tp {
			return ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " type should be the same")
		}
		if newOne.Flen != old.Flen {
			logutil.BgLogger().Warn("[ddl] admin repair table : Column " + newOne.Name.L + " flen is not equal to the old one")
		}
		newTableInfo.Columns[i].ID = old.ID
	}
	// If any old indexInfo has lost, that means the index ID lost too, so did the data, repair failed.
	for i, newOne := range newTableInfo.Indices {
		old := getIndexInfoByNameAndColumn(oldTableInfo, newOne)
		if old == nil {
			return ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " has lost")
		}
		if newOne.Tp != old.Tp {
			return ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " type should be the same")
		}
		newTableInfo.Indices[i].ID = old.ID
	}

	newTableInfo.State = model.StatePublic
	err = checkTableInfoValid(newTableInfo)
	if err != nil {
		return err
	}
	newTableInfo.State = model.StateNone

	job := &model.Job{
		SchemaID:   oldDBInfo.ID,
		TableID:    newTableInfo.ID,
		SchemaName: oldDBInfo.Name.L,
		Type:       model.ActionRepairTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableInfo},
	}
	err = d.doDDLJob(ctx, job)
	if err == nil {
		// Remove the old TableInfo from repairInfo before domain reload.
		domainutil.RepairInfo.RemoveFromRepairInfo(oldDBInfo.Name.L, oldTableInfo.Name.L)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) OrderByColumns(ctx sessionctx.Context, ident ast.Ident) error {
	_, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().GetPkColInfo() != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("ORDER BY ignored as there is a user-defined clustered index in the table '%s'", ident.Name))
	}
	return nil
}

func (d *ddl) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	sequenceInfo, err := buildSequenceInfo(stmt, ident)
	if err != nil {
		return err
	}
	// TiDB describe the sequence within a tableInfo, as a same-level object of a table and view.
	tbInfo, err := buildTableInfo(ctx, ident.Name, nil, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.Sequence = sequenceInfo

	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, ident.Schema, tbInfo, onExist, false /*tryRetainID*/)
}

func (d *ddl) DropSequence(ctx sessionctx.Context, ti ast.Ident, ifExists bool) (err error) {
	schema, tbl, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if !tbl.Meta().IsSequence() {
		err = ErrWrongObject.GenWithStackByArgs(ti.Schema, ti.Name, "SEQUENCE")
		if ifExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbl.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionDropSequence,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) AlterIndexVisibility(ctx sessionctx.Context, ident ast.Ident, indexName model.CIStr, visibility ast.IndexVisibility) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return err
	}

	invisible := false
	if visibility == ast.IndexVisibilityInvisible {
		invisible = true
	}

	skip, err := validateAlterIndexVisibility(indexName, invisible, tb.Meta())
	if err != nil {
		return errors.Trace(err)
	}
	if skip {
		return nil
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionAlterIndexVisibility,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexName, invisible},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildPlacementSpecReplicasAndConstraint(rule *placement.RuleOp, replicas uint64, cnstr string) ([]*placement.RuleOp, error) {
	var err error
	cnstr = strings.TrimSpace(cnstr)
	rules := make([]*placement.RuleOp, 0, 1)
	if len(cnstr) > 0 && cnstr[0] == '[' {
		// can not emit REPLICAS with an array label
		if replicas == 0 {
			return rules, errors.Errorf("array CONSTRAINTS should be with a positive REPLICAS")
		}
		rule.Count = int(replicas)

		constraints := []string{}

		err = json.Unmarshal([]byte(cnstr), &constraints)
		if err != nil {
			return rules, err
		}

		err = placement.CheckLabelConstraints(rule, constraints)
		if err != nil {
			return rules, err
		}

		rules = append(rules, rule)
	} else if len(cnstr) > 0 && cnstr[0] == '{' {
		constraints := map[string]int{}
		err = json.Unmarshal([]byte(cnstr), &constraints)
		if err != nil {
			return rules, err
		}

		rulesLen := len(rules)
		ruleCnt := int(replicas)
		for labels, cnt := range constraints {
			newRule := rule.Clone()
			if cnt <= 0 {
				err = errors.Errorf("count should be positive, but got %d", cnt)
				break
			}
			if replicas != 0 {
				ruleCnt -= cnt
				if ruleCnt < 0 {
					rules = rules[:rulesLen]
					err = errors.Errorf("REPLICAS should be larger or equal to the number of total replicas, but got %d", replicas)
					break
				}
			}
			newRule.Count = cnt
			err = placement.CheckLabelConstraints(newRule, strings.Split(strings.TrimSpace(labels), ","))
			if err != nil {
				break
			}
			rules = append(rules, newRule)
		}
		rule.Count = ruleCnt

		if rule.Count > 0 {
			rules = append(rules, rule)
		}
	} else {
		err = errors.Errorf("constraint should be a JSON array or object, but got '%s'", cnstr)
	}
	return rules, err
}

func buildPlacementSpecs(specs []*ast.PlacementSpec, tableID, partitionID int64) ([]*placement.RuleOp, error) {
	rules := make([]*placement.RuleOp, 0, len(specs))

	var err error
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	for _, spec := range specs {
		rule := &placement.RuleOp{
			Rule: &placement.Rule{
				GroupID:  placement.RuleDefaultGroupID,
				Override: true,
			},
		}

		switch spec.Role {
		case ast.PlacementRoleFollower:
			rule.Role = placement.Follower
		case ast.PlacementRoleLeader:
			rule.Role = placement.Leader
		case ast.PlacementRoleLearner:
			rule.Role = placement.Learner
		case ast.PlacementRoleVoter:
			rule.Role = placement.Voter
		default:
			err = errors.Errorf("unknown role: %d", spec.Role)
		}

		if err == nil {
			switch spec.Tp {
			case ast.PlacementAdd:
				rule.Action = placement.RuleOpAdd
			case ast.PlacementAlter:
				rule.Action = placement.RuleOpAdd

				// alter will overwrite all things
				// drop all rules that will be overrided
				newRules := rules[0:]

				for _, r := range rules {
					if r.Role != rule.Role {
						newRules = append(newRules, r)
					}
				}

				rules = newRules

				// delete previous definitions
				rules = append(rules, &placement.RuleOp{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						// ROLE is useless for PD, prevent two alter statements from overriding each other
						Role: rule.Role,
					},
				})
			default:
				err = errors.Errorf("unknown action type: %d", spec.Tp)
			}
		}

		if err == nil {
			var newRules []*placement.RuleOp
			newRules, err = buildPlacementSpecReplicasAndConstraint(rule, spec.Replicas, spec.Constraints)
			rules = append(rules, newRules...)
		}

		if err != nil {
			sb.Reset()
			if e := spec.Restore(restoreCtx); e != nil {
				return rules, ErrInvalidPlacementSpec.GenWithStackByArgs("", err)
			}
			return rules, ErrInvalidPlacementSpec.GenWithStackByArgs(sb.String(), err)
		}
	}
	return rules, nil
}

func (d *ddl) AlterTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	meta := tb.Meta()
	if meta.Partition == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := tables.FindPartitionByName(meta, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	rules, err := buildPlacementSpecs(spec.PlacementSpecs, meta.ID, partitionID)
	if err != nil {
		return errors.Trace(err)
	}

	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(partitionID)))
	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(partitionID+1)))
	for _, rule := range rules {
		rule.Index = placement.RuleIndexPartition
		rule.StartKeyHex = startKey
		rule.EndKeyHex = endKey
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionAlterTableAlterPartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partitionID, rules},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}
