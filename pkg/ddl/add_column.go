// Copyright 2024 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

func (w *worker) onAddColumn(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropColumn(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	tblInfo, columnInfo, colFromArgs, pos, ifNotExists, err := checkAddColumn(jobCtx.metaMut, job)
	if err != nil {
		if ifNotExists && infoschema.ErrColumnExists.Equal(err) {
			job.Warning = toTError(err)
			job.State = model.JobStateDone
			return ver, nil
		}
		return ver, errors.Trace(err)
	}
	if columnInfo == nil {
		columnInfo = InitAndAddColumnToTable(tblInfo, colFromArgs)
		logutil.DDLLogger().Info("run add column job", zap.Stringer("job", job), zap.Reflect("columnInfo", *columnInfo))
		if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	originalState := columnInfo.State
	switch columnInfo.State {
	case model.StateNone:
		// none -> delete only
		columnInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		columnInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		columnInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteReorganization
		job.MarkNonRevertible()
	case model.StateWriteReorganization:
		// reorganization -> public
		// Adjust table column offset.
		failpoint.InjectCall("onAddColumnStateWriteReorg")
		offset, err := LocateOffsetToMove(columnInfo.Offset, pos, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tblInfo.MoveColumnInfo(columnInfo.Offset, offset)
		columnInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		addColumnEvent := notifier.NewAddColumnEvent(tblInfo, []*model.ColumnInfo{columnInfo})
		err = asyncNotifyEvent(jobCtx, addColumnEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", columnInfo.State)
	}

	return ver, errors.Trace(err)
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
	if utf8.RuneCountInString(colName) > mysql.MaxColumnNameLength {
		return nil, dbterror.ErrTooLongIdent.GenWithStackByArgs(colName)
	}

	return CreateNewColumn(ctx, schema, spec, t, specNewColumn)
}

func checkUnsupportedColumnConstraint(col *ast.ColumnDef, ti ast.Ident) error {
	for _, constraint := range col.Options {
		switch constraint.Tp {
		case ast.ColumnOptionAutoIncrement:
			return dbterror.ErrUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint AUTO_INCREMENT when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionPrimaryKey:
			return dbterror.ErrUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint PRIMARY KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionUniqKey:
			return dbterror.ErrUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint UNIQUE KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionAutoRandom:
			errMsg := fmt.Sprintf(autoid.AutoRandomAlterAddColumn, col.Name, ti.Schema, ti.Name)
			return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
		}
	}

	return nil
}

// CreateNewColumn creates a new column according to the column information.
func CreateNewColumn(ctx sessionctx.Context, schema *model.DBInfo, spec *ast.AlterTableSpec, t table.Table, specNewColumn *ast.ColumnDef) (*table.Column, error) {
	// If new column is a generated column, do validation.
	// NOTE: we do check whether the column refers other generated
	// columns occurring later in a table, but we don't handle the col offset.
	for _, option := range specNewColumn.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			if err := checkIllegalFn4Generated(specNewColumn.Name.Name.L, typeColumn, option.Expr); err != nil {
				return nil, errors.Trace(err)
			}

			if option.Stored {
				return nil, dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Adding generated stored column through ALTER TABLE")
			}

			_, dependColNames, err := findDependedColumnNames(schema.Name, t.Meta().Name, specNewColumn)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !ctx.GetSessionVars().EnableAutoIncrementInGenerated {
				if err := checkAutoIncrementRef(specNewColumn.Name.Name.L, dependColNames, t.Meta()); err != nil {
					return nil, errors.Trace(err)
				}
			}
			duplicateColNames := make(map[string]struct{}, len(dependColNames))
			for k := range dependColNames {
				duplicateColNames[k] = struct{}{}
			}
			cols := t.Cols()

			if err := checkDependedColExist(dependColNames, cols); err != nil {
				return nil, errors.Trace(err)
			}

			if err := verifyColumnGenerationSingle(duplicateColNames, cols, spec.Position); err != nil {
				return nil, errors.Trace(err)
			}
		}
		// Specially, since sequence has been supported, if a newly added column has a
		// sequence nextval function as it's default value option, it won't fill the
		// known rows with specific sequence next value under current add column logic.
		// More explanation can refer: TestSequenceDefaultLogic's comment in sequence_test.go
		if option.Tp == ast.ColumnOptionDefaultValue {
			if f, ok := option.Expr.(*ast.FuncCallExpr); ok {
				switch f.FnName.L {
				case ast.NextVal:
					if _, err := getSequenceDefaultValue(option); err != nil {
						return nil, errors.Trace(err)
					}
					return nil, errors.Trace(dbterror.ErrAddColumnWithSequenceAsDefault.GenWithStackByArgs(specNewColumn.Name.Name.O))
				case ast.Rand, ast.UUID, ast.UUIDToBin, ast.Replace, ast.Upper:
					return nil, errors.Trace(dbterror.ErrBinlogUnsafeSystemFunction.GenWithStackByArgs())
				}
			}
		}
	}

	tableCharset, tableCollate, err := ResolveCharsetCollation([]ast.CharsetOpt{
		{Chs: t.Meta().Charset, Col: t.Meta().Collate},
		{Chs: schema.Charset, Col: schema.Collate},
	}, ctx.GetSessionVars().DefaultCollationForUTF8MB4)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Ignore table constraints now, they will be checked later.
	// We use length(t.Cols()) as the default offset firstly, we will change the column's offset later.
	col, _, err := buildColumnAndConstraint(
		NewMetaBuildContextWithSctx(ctx),
		len(t.Cols()),
		specNewColumn,
		nil,
		tableCharset,
		tableCollate,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	originDefVal, err := generateOriginDefaultValue(col.ToInfo(), ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = col.SetOriginDefaultValue(originDefVal)
	return col, err
}

// buildColumnAndConstraint builds table.Column and ast.Constraint from the parameters.
// outPriKeyConstraint is the primary key constraint out of column definition. For example:
// `create table t1 (id int , age int, primary key(id));`
func buildColumnAndConstraint(
	ctx *metabuild.Context,
	offset int,
	colDef *ast.ColumnDef,
	outPriKeyConstraint *ast.Constraint,
	tblCharset string,
	tblCollate string,
) (*table.Column, []*ast.Constraint, error) {
	if colName := colDef.Name.Name.L; colName == model.ExtraHandleName.L {
		return nil, nil, dbterror.ErrWrongColumnName.GenWithStackByArgs(colName)
	}

	// specifiedCollate refers to the last collate specified in colDef.Options.
	chs, coll, err := getCharsetAndCollateInColumnDef(colDef, ctx.GetDefaultCollationForUTF8MB4())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	chs, coll, err = ResolveCharsetCollation([]ast.CharsetOpt{
		{Chs: chs, Col: coll},
		{Chs: tblCharset, Col: tblCollate},
	}, ctx.GetDefaultCollationForUTF8MB4())
	chs, coll = OverwriteCollationWithBinaryFlag(colDef, chs, coll, ctx.GetDefaultCollationForUTF8MB4())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if err := setCharsetCollationFlenDecimal(ctx, colDef.Tp, colDef.Name.Name.O, chs, coll); err != nil {
		return nil, nil, errors.Trace(err)
	}
	decodeEnumSetBinaryLiteralToUTF8(colDef.Tp, chs)
	col, cts, err := columnDefToCol(ctx, offset, colDef, outPriKeyConstraint)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, cts, nil
}

// getCharsetAndCollateInColumnDef will iterate collate in the options, validate it by checking the charset
// of column definition. If there's no collate in the option, the default collate of column's charset will be used.
func getCharsetAndCollateInColumnDef(def *ast.ColumnDef, defaultUTF8MB4Coll string) (chs, coll string, err error) {
	chs = def.Tp.GetCharset()
	coll = def.Tp.GetCollate()
	if chs != "" && coll == "" {
		if coll, err = GetDefaultCollation(chs, defaultUTF8MB4Coll); err != nil {
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
				return "", "", dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			coll = info.Name
		}
	}
	return
}

// OverwriteCollationWithBinaryFlag is used to handle the case like
//
//	CREATE TABLE t (a VARCHAR(255) BINARY) CHARSET utf8 COLLATE utf8_general_ci;
//
// The 'BINARY' sets the column collation to *_bin according to the table charset.
func OverwriteCollationWithBinaryFlag(colDef *ast.ColumnDef, chs, coll string, defaultUTF8MB4Coll string) (newChs string, newColl string) {
	ignoreBinFlag := colDef.Tp.GetCharset() != "" && (colDef.Tp.GetCollate() != "" || containsColumnOption(colDef, ast.ColumnOptionCollate))
	if ignoreBinFlag {
		return chs, coll
	}
	needOverwriteBinColl := types.IsString(colDef.Tp.GetType()) && mysql.HasBinaryFlag(colDef.Tp.GetFlag())
	if needOverwriteBinColl {
		newColl, err := GetDefaultCollation(chs, defaultUTF8MB4Coll)
		if err != nil {
			return chs, coll
		}
		return chs, newColl
	}
	return chs, coll
}

func setCharsetCollationFlenDecimal(ctx *metabuild.Context, tp *types.FieldType, colName, colCharset, colCollate string) error {
	var err error
	if typesNeedCharset(tp.GetType()) {
		tp.SetCharset(colCharset)
		tp.SetCollate(colCollate)
	} else {
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CharsetBin)
	}

	// Use default value for flen or decimal when they are unspecified.
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.GetType())
	if tp.GetDecimal() == types.UnspecifiedLength {
		tp.SetDecimal(defaultDecimal)
	}
	if tp.GetFlen() == types.UnspecifiedLength {
		tp.SetFlen(defaultFlen)
		if mysql.HasUnsignedFlag(tp.GetFlag()) && tp.GetType() != mysql.TypeLonglong && mysql.IsIntegerType(tp.GetType()) {
			// Issue #4684: the flen of unsigned integer(except bigint) is 1 digit shorter than signed integer
			// because it has no prefix "+" or "-" character.
			tp.SetFlen(tp.GetFlen() - 1)
		}
	} else {
		// Adjust the field type for blob/text types if the flen is set.
		if err = adjustBlobTypesFlen(tp, colCharset); err != nil {
			return err
		}
	}
	return checkTooBigFieldLengthAndTryAutoConvert(ctx, tp, colName)
}

func decodeEnumSetBinaryLiteralToUTF8(tp *types.FieldType, chs string) {
	if tp.GetType() != mysql.TypeEnum && tp.GetType() != mysql.TypeSet {
		return
	}
	enc := charset.FindEncoding(chs)
	for i, elem := range tp.GetElems() {
		if !tp.GetElemIsBinaryLit(i) {
			continue
		}
		s, err := enc.Transform(nil, hack.Slice(elem), charset.OpDecodeReplace)
		if err != nil {
			logutil.DDLLogger().Warn("decode enum binary literal to utf-8 failed", zap.Error(err))
		}
		tp.SetElem(i, string(hack.String(s)))
	}
	tp.CleanElemIsBinaryLit()
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

// checkTooBigFieldLengthAndTryAutoConvert will check whether the field length is too big
// in non-strict mode and varchar column. If it is, will try to adjust to blob or text, see issue #30328
func checkTooBigFieldLengthAndTryAutoConvert(ctx *metabuild.Context, tp *types.FieldType, colName string) error {
	if !ctx.GetSQLMode().HasStrictMode() && tp.GetType() == mysql.TypeVarchar {
		err := types.IsVarcharTooBigFieldLength(tp.GetFlen(), colName, tp.GetCharset())
		if err != nil && terror.ErrorEqual(types.ErrTooBigFieldLength, err) {
			tp.SetType(mysql.TypeBlob)
			if err = adjustBlobTypesFlen(tp, tp.GetCharset()); err != nil {
				return err
			}
			if tp.GetCharset() == charset.CharsetBin {
				ctx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colName, "VARBINARY", "BLOB"))
			} else {
				ctx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colName, "VARCHAR", "TEXT"))
			}
		}
	}
	return nil
}

// columnDefToCol converts ColumnDef to Col and TableConstraints.
// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
func columnDefToCol(ctx *metabuild.Context, offset int, colDef *ast.ColumnDef, outPriKeyConstraint *ast.Constraint) (*table.Column, []*ast.Constraint, error) {
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
		if col.GetType() == mysql.TypeTimestamp {
			col.AddFlag(mysql.TimestampFlag | mysql.OnUpdateNowFlag | mysql.NotNullFlag)
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
			format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

		for _, v := range colDef.Options {
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				col.AddFlag(mysql.NotNullFlag)
			case ast.ColumnOptionNull:
				col.DelFlag(mysql.NotNullFlag)
				removeOnUpdateNowFlag(col)
				hasNullFlag = true
			case ast.ColumnOptionAutoIncrement:
				col.AddFlag(mysql.AutoIncrementFlag | mysql.NotNullFlag)
			case ast.ColumnOptionPrimaryKey:
				// Check PriKeyFlag first to avoid extra duplicate constraints.
				if col.GetFlag()&mysql.PriKeyFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys,
						Option: &ast.IndexOption{PrimaryKeyTp: v.PrimaryKeyTp}}
					if v.StrValue == "Global" {
						constraint.Option.Global = true
					}
					constraints = append(constraints, constraint)
					col.AddFlag(mysql.PriKeyFlag)
					// Add NotNullFlag early so that processColumnFlags() can see it.
					col.AddFlag(mysql.NotNullFlag)
				}
			case ast.ColumnOptionUniqKey:
				// Check UniqueFlag first to avoid extra duplicate constraints.
				if col.GetFlag()&mysql.UniqueFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Keys: keys}
					if v.StrValue == "Global" {
						constraint.Option = &ast.IndexOption{Global: true}
					}
					constraints = append(constraints, constraint)
					col.AddFlag(mysql.UniqueKeyFlag)
				}
			case ast.ColumnOptionDefaultValue:
				hasDefaultValue, err = SetDefaultValue(ctx.GetExprCtx(), col, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionOnUpdate:
				// TODO: Support other time functions.
				if !(col.GetType() == mysql.TypeTimestamp || col.GetType() == mysql.TypeDatetime) {
					return nil, nil, dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
				if !expression.IsValidCurrentTimestampExpr(v.Expr, colDef.Tp) {
					return nil, nil, dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
				col.AddFlag(mysql.OnUpdateNowFlag)
				setOnUpdateNow = true
			case ast.ColumnOptionComment:
				err := setColumnComment(ctx.GetExprCtx(), col, v)
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
				_, dependColNames, err := findDependedColumnNames(pmodel.NewCIStr(""), pmodel.NewCIStr(""), colDef)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				col.Dependences = dependColNames
			case ast.ColumnOptionCollate:
				if field_types.HasCharset(colDef.Tp) {
					col.FieldType.SetCollate(v.StrValue)
				}
			case ast.ColumnOptionFulltext:
				ctx.AppendWarning(dbterror.ErrTableCantHandleFt.FastGenByArgs())
			case ast.ColumnOptionCheck:
				if !variable.EnableCheckConstraint.Load() {
					ctx.AppendWarning(errCheckConstraintIsOff)
				} else {
					// Check the column CHECK constraint dependency lazily, after fill all the name.
					// Extract column constraint from column option.
					constraint := &ast.Constraint{
						Tp:           ast.ConstraintCheck,
						Expr:         v.Expr,
						Enforced:     v.Enforced,
						Name:         v.ConstraintName,
						InColumn:     true,
						InColumnName: colDef.Name.Name.O,
					}
					constraints = append(constraints, constraint)
				}
			}
		}
	}

	if err = processAndCheckDefaultValueAndColumn(ctx.GetExprCtx(), col, outPriKeyConstraint, hasDefaultValue, setOnUpdateNow, hasNullFlag); err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, constraints, nil
}

// isExplicitTimeStamp is used to check if explicit_defaults_for_timestamp is on or off.
// Check out this link for more details.
// https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_explicit_defaults_for_timestamp
func isExplicitTimeStamp() bool {
	// TODO: implement the behavior as MySQL when explicit_defaults_for_timestamp = off, then this function could return false.
	return true
}

// SetDefaultValue sets the default value of the column.
func SetDefaultValue(ctx expression.BuildContext, col *table.Column, option *ast.ColumnOption) (hasDefaultValue bool, err error) {
	var value any
	var isSeqExpr bool
	value, isSeqExpr, err = getDefaultValue(
		exprctx.CtxWithHandleTruncateErrLevel(ctx, errctx.LevelError),
		col, option,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	if isSeqExpr {
		if err := checkSequenceDefaultValue(col); err != nil {
			return false, errors.Trace(err)
		}
		col.DefaultIsExpr = isSeqExpr
	}

	// When the default value is expression, we skip check and convert.
	if !col.DefaultIsExpr {
		if hasDefaultValue, value, err = checkColumnDefaultValue(ctx, col, value); err != nil {
			return hasDefaultValue, errors.Trace(err)
		}
		value, err = convertTimestampDefaultValToUTC(ctx.GetEvalCtx().TypeCtx(), value, col)
		if err != nil {
			return hasDefaultValue, errors.Trace(err)
		}
	} else {
		hasDefaultValue = true
	}
	err = setDefaultValueWithBinaryPadding(col, value)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	return hasDefaultValue, nil
}

// getFuncCallDefaultValue gets the default column value of function-call expression.
func getFuncCallDefaultValue(col *table.Column, option *ast.ColumnOption, expr *ast.FuncCallExpr) (any, bool, error) {
	switch expr.FnName.L {
	case ast.CurrentTimestamp, ast.CurrentDate: // CURRENT_TIMESTAMP() and CURRENT_DATE()
		tp, fsp := col.FieldType.GetType(), col.FieldType.GetDecimal()
		if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
			defaultFsp := 0
			if len(expr.Args) == 1 {
				if val := expr.Args[0].(*driver.ValueExpr); val != nil {
					defaultFsp = int(val.GetInt64())
				}
			}
			if defaultFsp != fsp {
				return nil, false, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
			}
		}
		return nil, false, nil
	case ast.NextVal:
		// handle default next value of sequence. (keep the expr string)
		str, err := getSequenceDefaultValue(option)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		return str, true, nil
	case ast.Rand, ast.UUID, ast.UUIDToBin: // RAND(), UUID() and UUID_TO_BIN()
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		str, err := restoreFuncCall(expr)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		col.DefaultIsExpr = true
		return str, false, nil
	case ast.DateFormat: // DATE_FORMAT()
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		// Support DATE_FORMAT(NOW(),'%Y-%m'), DATE_FORMAT(NOW(),'%Y-%m-%d'),
		// DATE_FORMAT(NOW(),'%Y-%m-%d %H.%i.%s'), DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s').
		nowFunc, ok := expr.Args[0].(*ast.FuncCallExpr)
		if ok && nowFunc.FnName.L == ast.Now {
			if err := expression.VerifyArgsWrapper(nowFunc.FnName.L, len(nowFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			valExpr, isValue := expr.Args[1].(ast.ValueExpr)
			if !isValue || (valExpr.GetString() != "%Y-%m" && valExpr.GetString() != "%Y-%m-%d" &&
				valExpr.GetString() != "%Y-%m-%d %H.%i.%s" && valExpr.GetString() != "%Y-%m-%d %H:%i:%s") {
				return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(), valExpr)
			}
			str, err := restoreFuncCall(expr)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			col.DefaultIsExpr = true
			return str, false, nil
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	case ast.Replace:
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		funcCall := expr.Args[0]
		// Support REPLACE(CONVERT(UPPER(UUID()) USING UTF8MB4), '-', ''))
		if convertFunc, ok := funcCall.(*ast.FuncCallExpr); ok && convertFunc.FnName.L == ast.Convert {
			if err := expression.VerifyArgsWrapper(convertFunc.FnName.L, len(convertFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			funcCall = convertFunc.Args[0]
		}
		// Support REPLACE(UPPER(UUID()), '-', '').
		if upperFunc, ok := funcCall.(*ast.FuncCallExpr); ok && upperFunc.FnName.L == ast.Upper {
			if err := expression.VerifyArgsWrapper(upperFunc.FnName.L, len(upperFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			if uuidFunc, ok := upperFunc.Args[0].(*ast.FuncCallExpr); ok && uuidFunc.FnName.L == ast.UUID {
				if err := expression.VerifyArgsWrapper(uuidFunc.FnName.L, len(uuidFunc.Args)); err != nil {
					return nil, false, errors.Trace(err)
				}
				str, err := restoreFuncCall(expr)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				col.DefaultIsExpr = true
				return str, false, nil
			}
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	case ast.Upper:
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		// Support UPPER(SUBSTRING_INDEX(USER(), '@', 1)).
		if substringIndexFunc, ok := expr.Args[0].(*ast.FuncCallExpr); ok && substringIndexFunc.FnName.L == ast.SubstringIndex {
			if err := expression.VerifyArgsWrapper(substringIndexFunc.FnName.L, len(substringIndexFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			if userFunc, ok := substringIndexFunc.Args[0].(*ast.FuncCallExpr); ok && userFunc.FnName.L == ast.User {
				if err := expression.VerifyArgsWrapper(userFunc.FnName.L, len(userFunc.Args)); err != nil {
					return nil, false, errors.Trace(err)
				}
				valExpr, isValue := substringIndexFunc.Args[1].(ast.ValueExpr)
				if !isValue || valExpr.GetString() != "@" {
					return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(), valExpr)
				}
				str, err := restoreFuncCall(expr)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				col.DefaultIsExpr = true
				return str, false, nil
			}
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	case ast.StrToDate: // STR_TO_DATE()
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		// Support STR_TO_DATE('1980-01-01', '%Y-%m-%d').
		if _, ok1 := expr.Args[0].(ast.ValueExpr); ok1 {
			if _, ok2 := expr.Args[1].(ast.ValueExpr); ok2 {
				str, err := restoreFuncCall(expr)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				col.DefaultIsExpr = true
				return str, false, nil
			}
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	case ast.JSONObject, ast.JSONArray, ast.JSONQuote: // JSON_OBJECT(), JSON_ARRAY(), JSON_QUOTE()
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		str, err := restoreFuncCall(expr)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		col.DefaultIsExpr = true
		return str, false, nil

	case ast.VecFromText:
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		str, err := restoreFuncCall(expr)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		col.DefaultIsExpr = true
		return str, false, nil

	default:
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(), expr.FnName.String())
	}
}

// getDefaultValue will get the default value for column.
// 1: get the expr restored string for the column which uses sequence next value as default value.
// 2: get specific default value for the other column.
func getDefaultValue(ctx exprctx.BuildContext, col *table.Column, option *ast.ColumnOption) (any, bool, error) {
	// handle default value with function call
	tp, fsp := col.FieldType.GetType(), col.FieldType.GetDecimal()
	if x, ok := option.Expr.(*ast.FuncCallExpr); ok {
		val, isSeqExpr, err := getFuncCallDefaultValue(col, option, x)
		if val != nil || isSeqExpr || err != nil {
			return val, isSeqExpr, err
		}
		// If the function call is ast.CurrentTimestamp, it needs to be continuously processed.
	}

	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime || tp == mysql.TypeDate {
		vd, err := expression.GetTimeValue(ctx, option.Expr, tp, fsp, nil)
		value := vd.GetValue()
		if err != nil {
			return nil, false, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
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

	// evaluate the non-function-call expr to a certain value.
	v, err := expression.EvalSimpleAst(ctx, option.Expr)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if v.IsNull() {
		return nil, false, nil
	}

	if v.Kind() == types.KindBinaryLiteral || v.Kind() == types.KindMysqlBit {
		if types.IsTypeBlob(tp) || tp == mysql.TypeJSON || tp == mysql.TypeTiDBVectorFloat32 {
			// BLOB/TEXT/JSON column cannot have a default value.
			// Skip the unnecessary decode procedure.
			return v.GetString(), false, err
		}
		if tp == mysql.TypeBit || tp == mysql.TypeString || tp == mysql.TypeVarchar ||
			tp == mysql.TypeVarString || tp == mysql.TypeEnum || tp == mysql.TypeSet {
			// For BinaryLiteral or bit fields, we decode the default value to utf8 string.
			str, err := v.GetBinaryStringDecoded(types.StrictFlags, col.GetCharset())
			if err != nil {
				// Overwrite the decoding error with invalid default value error.
				err = dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
			}
			return str, false, err
		}
		// For other kind of fields (e.g. INT), we supply its integer as string value.
		value, err := v.GetBinaryLiteral().ToInt(ctx.GetEvalCtx().TypeCtx())
		if err != nil {
			return nil, false, err
		}
		return strconv.FormatUint(value, 10), false, nil
	}

	switch tp {
	case mysql.TypeSet:
		val, err := getSetDefaultValue(v, col)
		return val, false, err
	case mysql.TypeEnum:
		val, err := getEnumDefaultValue(v, col)
		return val, false, err
	case mysql.TypeDuration, mysql.TypeDate:
		if v, err = v.ConvertTo(ctx.GetEvalCtx().TypeCtx(), &col.FieldType); err != nil {
			return "", false, errors.Trace(err)
		}
	case mysql.TypeBit:
		if v.Kind() == types.KindInt64 || v.Kind() == types.KindUint64 {
			// For BIT fields, convert int into BinaryLiteral.
			return types.NewBinaryLiteralFromUint(v.GetUint64(), -1).ToString(), false, nil
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeFloat, mysql.TypeDouble:
		// For these types, convert it to standard format firstly.
		// like integer fields, convert it into integer string literals. like convert "1.25" into "1" and "2.8" into "3".
		// if raise a error, we will use original expression. We will handle it in check phase
		if temp, err := v.ConvertTo(ctx.GetEvalCtx().TypeCtx(), &col.FieldType); err == nil {
			v = temp
		}
	}

	val, err := v.ToString()
	return val, false, err
}

func getSequenceDefaultValue(c *ast.ColumnOption) (expr string, err error) {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	if err := c.Expr.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func setDefaultValueWithBinaryPadding(col *table.Column, value any) error {
	err := col.SetDefaultValue(value)
	if err != nil {
		return err
	}
	// https://dev.mysql.com/doc/refman/8.0/en/binary-varbinary.html
	// Set the default value for binary type should append the paddings.
	if value != nil {
		if col.GetType() == mysql.TypeString && types.IsBinaryStr(&col.FieldType) && len(value.(string)) < col.GetFlen() {
			padding := make([]byte, col.GetFlen()-len(value.(string)))
			col.DefaultValue = string(append([]byte(col.DefaultValue.(string)), padding...))
		}
	}
	return nil
}

func setColumnComment(ctx expression.BuildContext, col *table.Column, option *ast.ColumnOption) error {
	value, err := expression.EvalSimpleAst(ctx, option.Expr)
	if err != nil {
		return errors.Trace(err)
	}
	if col.Comment, err = value.ToString(); err != nil {
		return errors.Trace(err)
	}

	evalCtx := ctx.GetEvalCtx()
	col.Comment, err = validateCommentLength(evalCtx.ErrCtx(), evalCtx.SQLMode(), col.Name.L, &col.Comment, dbterror.ErrTooLongFieldComment)
	return errors.Trace(err)
}

func processAndCheckDefaultValueAndColumn(ctx expression.BuildContext, col *table.Column,
	outPriKeyConstraint *ast.Constraint, hasDefaultValue, setOnUpdateNow, hasNullFlag bool) error {
	processDefaultValue(col, hasDefaultValue, setOnUpdateNow)
	processColumnFlags(col)

	err := checkPriKeyConstraint(col, hasDefaultValue, hasNullFlag, outPriKeyConstraint)
	if err != nil {
		return errors.Trace(err)
	}
	if err = checkColumnValueConstraint(col, col.GetCollate()); err != nil {
		return errors.Trace(err)
	}
	if err = checkDefaultValue(ctx, col, hasDefaultValue); err != nil {
		return errors.Trace(err)
	}
	if err = checkColumnFieldLength(col); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func restoreFuncCall(expr *ast.FuncCallExpr) (string, error) {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	if err := expr.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// getSetDefaultValue gets the default value for the set type. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
func getSetDefaultValue(v types.Datum, col *table.Column) (string, error) {
	if v.Kind() == types.KindInt64 {
		setCnt := len(col.GetElems())
		maxLimit := int64(1<<uint(setCnt) - 1)
		val := v.GetInt64()
		if val < 1 || val > maxLimit {
			return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
		}
		setVal, err := types.ParseSetValue(col.GetElems(), uint64(val))
		if err != nil {
			return "", errors.Trace(err)
		}
		v.SetMysqlSet(setVal, col.GetCollate())
		return v.ToString()
	}

	str, err := v.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	if str == "" {
		return str, nil
	}
	setVal, err := types.ParseSetName(col.GetElems(), str, col.GetCollate())
	if err != nil {
		return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	v.SetMysqlSet(setVal, col.GetCollate())

	return v.ToString()
}

// getEnumDefaultValue gets the default value for the enum type. See https://dev.mysql.com/doc/refman/5.7/en/enum.html.
func getEnumDefaultValue(v types.Datum, col *table.Column) (string, error) {
	if v.Kind() == types.KindInt64 {
		val := v.GetInt64()
		if val < 1 || val > int64(len(col.GetElems())) {
			return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
		}
		enumVal, err := types.ParseEnumValue(col.GetElems(), uint64(val))
		if err != nil {
			return "", errors.Trace(err)
		}
		v.SetMysqlEnum(enumVal, col.GetCollate())
		return v.ToString()
	}
	str, err := v.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	// Ref: https://dev.mysql.com/doc/refman/8.0/en/enum.html
	// Trailing spaces are automatically deleted from ENUM member values in the table definition when a table is created.
	str = strings.TrimRight(str, " ")
	enumVal, err := types.ParseEnumName(col.GetElems(), str, col.GetCollate())
	if err != nil {
		return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	v.SetMysqlEnum(enumVal, col.GetCollate())

	return v.ToString()
}

func removeOnUpdateNowFlag(c *table.Column) {
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.GetFlag()) {
		c.DelFlag(mysql.OnUpdateNowFlag)
	}
}

func processDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	setTimestampDefaultValue(c, hasDefaultValue, setOnUpdateNow)

	setYearDefaultValue(c, hasDefaultValue)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(c, hasDefaultValue)
}

func setYearDefaultValue(c *table.Column, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if c.GetType() == mysql.TypeYear && mysql.HasNotNullFlag(c.GetFlag()) {
		if err := c.SetDefaultValue("0000"); err != nil {
			logutil.DDLLogger().Error("set default value failed", zap.Error(err))
		}
	}
}

func setTimestampDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	if hasDefaultValue {
		return
	}

	// For timestamp Col, if is not set default value or not set null, use current timestamp.
	if mysql.HasTimestampFlag(c.GetFlag()) && mysql.HasNotNullFlag(c.GetFlag()) {
		if setOnUpdateNow {
			if err := c.SetDefaultValue(types.ZeroDatetimeStr); err != nil {
				logutil.DDLLogger().Error("set default value failed", zap.Error(err))
			}
		} else {
			if err := c.SetDefaultValue(strings.ToUpper(ast.CurrentTimestamp)); err != nil {
				logutil.DDLLogger().Error("set default value failed", zap.Error(err))
			}
		}
	}
}

func setNoDefaultValueFlag(c *table.Column, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if !mysql.HasNotNullFlag(c.GetFlag()) {
		return
	}

	// Check if it is an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	if !mysql.HasAutoIncrementFlag(c.GetFlag()) && !mysql.HasTimestampFlag(c.GetFlag()) {
		c.AddFlag(mysql.NoDefaultValueFlag)
	}
}

func checkDefaultValue(ctx exprctx.BuildContext, c *table.Column, hasDefaultValue bool) (err error) {
	if !hasDefaultValue {
		return nil
	}

	if c.GetDefaultValue() != nil {
		if c.DefaultIsExpr {
			if mysql.HasAutoIncrementFlag(c.GetFlag()) {
				return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
			}
			return nil
		}
		_, err = table.GetColDefaultValue(
			exprctx.CtxWithHandleTruncateErrLevel(ctx, errctx.LevelError),
			c.ToInfo(),
		)
		if err != nil {
			return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
		}
		return nil
	}
	// Primary key default null is invalid.
	if mysql.HasPriKeyFlag(c.GetFlag()) {
		return dbterror.ErrPrimaryCantHaveNull
	}

	// Set not null but default null is invalid.
	if mysql.HasNotNullFlag(c.GetFlag()) {
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}

	return nil
}

func checkColumnFieldLength(col *table.Column) error {
	if col.GetType() == mysql.TypeVarchar {
		if err := types.IsVarcharTooBigFieldLength(col.GetFlen(), col.Name.O, col.GetCharset()); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// checkPriKeyConstraint check all parts of a PRIMARY KEY must be NOT NULL
func checkPriKeyConstraint(col *table.Column, hasDefaultValue, hasNullFlag bool, outPriKeyConstraint *ast.Constraint) error {
	// Primary key should not be null.
	if mysql.HasPriKeyFlag(col.GetFlag()) && hasDefaultValue && col.GetDefaultValue() == nil {
		return types.ErrInvalidDefault.GenWithStackByArgs(col.Name)
	}
	// Set primary key flag for outer primary key constraint.
	// Such as: create table t1 (id int , age int, primary key(id))
	if !mysql.HasPriKeyFlag(col.GetFlag()) && outPriKeyConstraint != nil {
		for _, key := range outPriKeyConstraint.Keys {
			if key.Expr == nil && key.Column.Name.L != col.Name.L {
				continue
			}
			col.AddFlag(mysql.PriKeyFlag)
			break
		}
	}
	// Primary key should not be null.
	if mysql.HasPriKeyFlag(col.GetFlag()) && hasNullFlag {
		return dbterror.ErrPrimaryCantHaveNull
	}
	return nil
}

func checkColumnValueConstraint(col *table.Column, collation string) error {
	if col.GetType() != mysql.TypeEnum && col.GetType() != mysql.TypeSet {
		return nil
	}
	valueMap := make(map[string]bool, len(col.GetElems()))
	ctor := collate.GetCollator(collation)
	enumLengthLimit := config.GetGlobalConfig().EnableEnumLengthLimit
	desc, err := charset.GetCharsetInfo(col.GetCharset())
	if err != nil {
		return errors.Trace(err)
	}
	for i := range col.GetElems() {
		val := string(ctor.Key(col.GetElems()[i]))
		// According to MySQL 8.0 Refman:
		// The maximum supported length of an individual ENUM element is M <= 255 and (M x w) <= 1020,
		// where M is the element literal length and w is the number of bytes required for the maximum-length character in the character set.
		// See https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html for more details.
		if enumLengthLimit && (len(val) > 255 || len(val)*desc.Maxlen > 1020) {
			return dbterror.ErrTooLongValueForType.GenWithStackByArgs(col.Name)
		}
		if _, ok := valueMap[val]; ok {
			tpStr := "ENUM"
			if col.GetType() == mysql.TypeSet {
				tpStr = "SET"
			}
			return types.ErrDuplicatedValueInType.GenWithStackByArgs(col.Name, col.GetElems()[i], tpStr)
		}
		valueMap[val] = true
	}
	return nil
}

// checkColumnDefaultValue checks the default value of the column.
// In non-strict SQL mode, if the default value of the column is an empty string, the default value can be ignored.
// In strict SQL mode, TEXT/BLOB/JSON can't have not null default values.
// In NO_ZERO_DATE SQL mode, TIMESTAMP/DATE/DATETIME type can't have zero date like '0000-00-00' or '0000-00-00 00:00:00'.
func checkColumnDefaultValue(ctx exprctx.BuildContext, col *table.Column, value any) (bool, any, error) {
	hasDefaultValue := true

	if value != nil && col.GetType() == mysql.TypeTiDBVectorFloat32 {
		// In any SQL mode we don't allow VECTOR column to have a default value.
		// Note that expression default is still supported.
		return hasDefaultValue, value, errors.Errorf("VECTOR column '%-.192s' can't have a literal default. Use expression default instead: ((VEC_FROM_TEXT('...')))", col.Name.O)
	}
	if value != nil && (col.GetType() == mysql.TypeJSON ||
		col.GetType() == mysql.TypeTinyBlob || col.GetType() == mysql.TypeMediumBlob ||
		col.GetType() == mysql.TypeLongBlob || col.GetType() == mysql.TypeBlob) {
		// In non-strict SQL mode.
		if !ctx.GetEvalCtx().SQLMode().HasStrictMode() && value == "" {
			if col.GetType() == mysql.TypeBlob || col.GetType() == mysql.TypeLongBlob {
				// The TEXT/BLOB default value can be ignored.
				hasDefaultValue = false
			}
			// In non-strict SQL mode, if the column type is json and the default value is null, it is initialized to an empty array.
			if col.GetType() == mysql.TypeJSON {
				value = `null`
			}
			ctx.GetEvalCtx().AppendWarning(dbterror.ErrBlobCantHaveDefault.FastGenByArgs(col.Name.O))
			return hasDefaultValue, value, nil
		}
		// In strict SQL mode or default value is not an empty string.
		return hasDefaultValue, value, dbterror.ErrBlobCantHaveDefault.GenWithStackByArgs(col.Name.O)
	}
	if value != nil && ctx.GetEvalCtx().SQLMode().HasNoZeroDateMode() &&
		ctx.GetEvalCtx().SQLMode().HasStrictMode() && types.IsTypeTime(col.GetType()) {
		if vv, ok := value.(string); ok {
			timeValue, err := expression.GetTimeValue(ctx, vv, col.GetType(), col.GetDecimal(), nil)
			if err != nil {
				return hasDefaultValue, value, errors.Trace(err)
			}
			if timeValue.GetMysqlTime().CoreTime() == types.ZeroCoreTime {
				return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
			}
		}
	}
	if value != nil && col.GetType() == mysql.TypeBit {
		v, ok := value.(string)
		if !ok {
			return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
		}

		uintVal, err := types.BinaryLiteral(v).ToInt(ctx.GetEvalCtx().TypeCtx())
		if err != nil {
			return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
		}
		intest.Assert(col.GetFlen() > 0 && col.GetFlen() <= 64)
		if col.GetFlen() < 64 && uintVal >= 1<<(uint64(col.GetFlen())) {
			return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
		}
	}
	return hasDefaultValue, value, nil
}

func checkSequenceDefaultValue(col *table.Column) error {
	if mysql.IsIntegerType(col.GetType()) {
		return nil
	}
	return dbterror.ErrColumnTypeUnsupportedNextValue.GenWithStackByArgs(col.ColumnInfo.Name.O)
}

func convertTimestampDefaultValToUTC(tc types.Context, defaultVal any, col *table.Column) (any, error) {
	if defaultVal == nil || col.GetType() != mysql.TypeTimestamp {
		return defaultVal, nil
	}
	if vv, ok := defaultVal.(string); ok {
		if vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			t, err := types.ParseTime(tc, vv, col.GetType(), col.GetDecimal())
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			err = t.ConvertTimeZone(tc.Location(), time.UTC)
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			defaultVal = t.String()
		}
	}
	return defaultVal, nil
}

// processColumnFlags is used by columnDefToCol and processColumnOptions. It is intended to unify behaviors on `create/add` and `modify/change` statements. Check tidb#issue#19342.
func processColumnFlags(col *table.Column) {
	if col.FieldType.EvalType().IsStringKind() {
		if col.GetCharset() == charset.CharsetBin {
			col.AddFlag(mysql.BinaryFlag)
		} else {
			col.DelFlag(mysql.BinaryFlag)
		}
	}
	if col.GetType() == mysql.TypeBit {
		// For BIT field, it's charset is binary but does not have binary flag.
		col.DelFlag(mysql.BinaryFlag)
		col.AddFlag(mysql.UnsignedFlag)
	}
	if col.GetType() == mysql.TypeYear {
		// For Year field, it's charset is binary but does not have binary flag.
		col.DelFlag(mysql.BinaryFlag)
		col.AddFlag(mysql.ZerofillFlag)
	}

	// If you specify ZEROFILL for a numeric column, MySQL automatically adds the UNSIGNED attribute to the column.
	// See https://dev.mysql.com/doc/refman/5.7/en/numeric-type-overview.html for more details.
	// But some types like bit and year, won't show its unsigned flag in `show create table`.
	if mysql.HasZerofillFlag(col.GetFlag()) {
		col.AddFlag(mysql.UnsignedFlag)
	}
}

func adjustBlobTypesFlen(tp *types.FieldType, colCharset string) error {
	cs, err := charset.GetCharsetInfo(colCharset)
	// when we meet the unsupported charset, we do not adjust.
	if err != nil {
		return err
	}
	l := tp.GetFlen() * cs.Maxlen
	if tp.GetType() == mysql.TypeBlob {
		if l <= tinyBlobMaxLength {
			logutil.DDLLogger().Info(fmt.Sprintf("Automatically convert BLOB(%d) to TINYBLOB", tp.GetFlen()))
			tp.SetFlen(tinyBlobMaxLength)
			tp.SetType(mysql.TypeTinyBlob)
		} else if l <= blobMaxLength {
			tp.SetFlen(blobMaxLength)
		} else if l <= mediumBlobMaxLength {
			logutil.DDLLogger().Info(fmt.Sprintf("Automatically convert BLOB(%d) to MEDIUMBLOB", tp.GetFlen()))
			tp.SetFlen(mediumBlobMaxLength)
			tp.SetType(mysql.TypeMediumBlob)
		} else if l <= longBlobMaxLength {
			logutil.DDLLogger().Info(fmt.Sprintf("Automatically convert BLOB(%d) to LONGBLOB", tp.GetFlen()))
			tp.SetFlen(longBlobMaxLength)
			tp.SetType(mysql.TypeLongBlob)
		}
	}
	return nil
}
