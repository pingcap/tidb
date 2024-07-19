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

// Copyright 2016 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package table

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

// Column provides meta data describing a table column.
type Column struct {
	*model.ColumnInfo
	// If this column is a generated column, the expression will be stored here.
	GeneratedExpr *ClonableExprNode
	// If this column has default expr value, this expression will be stored here.
	DefaultExpr ast.ExprNode
}

// ClonableExprNode is a wrapper for ast.ExprNode.
type ClonableExprNode struct {
	ctor     func() ast.ExprNode
	internal ast.ExprNode
}

// NewClonableExprNode creates a ClonableExprNode.
func NewClonableExprNode(ctor func() ast.ExprNode, internal ast.ExprNode) *ClonableExprNode {
	return &ClonableExprNode{
		ctor:     ctor,
		internal: internal,
	}
}

// Clone makes a "copy" of internal ast.ExprNode by reconstructing it.
func (n *ClonableExprNode) Clone() ast.ExprNode {
	intest.AssertNotNil(n.ctor)
	if n.ctor == nil {
		return n.internal
	}
	return n.ctor()
}

// Internal returns the reference of the internal ast.ExprNode.
// Note: only use this method when you are sure that the internal ast.ExprNode is not modified concurrently.
func (n *ClonableExprNode) Internal() ast.ExprNode {
	return n.internal
}

// String implements fmt.Stringer interface.
func (c *Column) String() string {
	ans := []string{c.Name.O, types.TypeToStr(c.GetType(), c.GetCharset())}
	if mysql.HasAutoIncrementFlag(c.GetFlag()) {
		ans = append(ans, "AUTO_INCREMENT")
	}
	if mysql.HasNotNullFlag(c.GetFlag()) {
		ans = append(ans, "NOT NULL")
	}
	return strings.Join(ans, " ")
}

// ToInfo casts Column to model.ColumnInfo
// NOTE: DONT modify return value.
func (c *Column) ToInfo() *model.ColumnInfo {
	return c.ColumnInfo
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

// FindColLowerCase finds column in cols by name. It assumes the name is lowercase.
func FindColLowerCase(cols []*Column, name string) *Column {
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}
	return nil
}

// ToColumn converts a *model.ColumnInfo to *Column.
func ToColumn(col *model.ColumnInfo) *Column {
	return &Column{
		col,
		nil,
		nil,
	}
}

// FindCols finds columns in cols by names.
// If pkIsHandle is false and name is ExtraHandleName, the extra handle column will be added.
// If any columns don't match, return nil and the first missing column's name.
// Please consider FindColumns() first for a better performance.
func FindCols(cols []*Column, names []string, pkIsHandle bool) ([]*Column, string) {
	var rcols []*Column
	for _, name := range names {
		col := FindCol(cols, name)
		if col != nil {
			rcols = append(rcols, col)
		} else if name == model.ExtraHandleName.L && !pkIsHandle {
			col := &Column{}
			col.ColumnInfo = model.NewExtraHandleColInfo()
			col.ColumnInfo.Offset = len(cols)
			rcols = append(rcols, col)
		} else {
			return nil, name
		}
	}

	return rcols, ""
}

// FindColumns finds columns in cols by names with a better performance than FindCols().
// It assumes names are lowercase.
func FindColumns(cols []*Column, names []string, pkIsHandle bool) (foundCols []*Column, missingOffset int) {
	var rcols []*Column
	for i, name := range names {
		col := FindColLowerCase(cols, name)
		if col != nil {
			rcols = append(rcols, col)
		} else if name == model.ExtraHandleName.L && !pkIsHandle {
			col := &Column{}
			col.ColumnInfo = model.NewExtraHandleColInfo()
			col.ColumnInfo.Offset = len(cols)
			rcols = append(rcols, col)
		} else {
			return nil, i
		}
	}
	return rcols, -1
}

// FindOnUpdateCols finds columns which have OnUpdateNow flag.
func FindOnUpdateCols(cols []*Column) []*Column {
	var rcols []*Column
	for _, col := range cols {
		if mysql.HasOnUpdateNowFlag(col.GetFlag()) {
			rcols = append(rcols, col)
		}
	}

	return rcols
}

// truncateTrailingSpaces truncates trailing spaces for CHAR[(M)] column.
// fix: https://github.com/pingcap/tidb/issues/3660
func truncateTrailingSpaces(v *types.Datum) {
	if v.Kind() == types.KindNull {
		return
	}
	b := v.GetBytes()
	length := len(b)
	for length > 0 && b[length-1] == ' ' {
		length--
	}
	b = b[:length]
	str := string(hack.String(b))
	v.SetString(str, v.Collation())
}

// convertToIncorrectStringErr converts ErrInvalidCharacterString to ErrTruncatedWrongValueForField.
// The first argument is the invalid character in bytes.
func convertToIncorrectStringErr(err error, colName string) error {
	inErr, ok := errors.Cause(err).(*errors.Error)
	if !ok {
		return err
	}
	args := inErr.Args()
	if len(args) != 2 {
		return err
	}
	invalidStrHex, ok := args[1].(string)
	if !ok {
		return err
	}
	var res strings.Builder
	for i := 0; i < len(invalidStrHex); i++ {
		if i%2 == 0 {
			res.WriteString("\\x")
		}
		res.WriteByte(invalidStrHex[i])
	}
	return ErrTruncatedWrongValueForField.FastGen("Incorrect string value '%s' for column '%s'", res.String(), colName)
}

// handleZeroDatetime handles Timestamp/Datetime/Date zero date and invalid dates.
// Currently only called from CastValue.
// returns:
//
//	value (possibly adjusted)
//	boolean; true if break error/warning handling in CastValue and return what was returned from this
//	error
func handleZeroDatetime(ec errctx.Context, mode mysql.SQLMode, col *model.ColumnInfo, casted types.Datum, str string, tmIsInvalid bool) (types.Datum, bool, error) {
	tm := casted.GetMysqlTime()

	var (
		zeroV types.Time
		zeroT string
	)
	switch col.GetType() {
	case mysql.TypeDate:
		zeroV, zeroT = types.ZeroDate, types.DateStr
	case mysql.TypeDatetime:
		zeroV, zeroT = types.ZeroDatetime, types.DateTimeStr
	case mysql.TypeTimestamp:
		zeroV, zeroT = types.ZeroTimestamp, types.TimestampStr
	}

	// ref https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_date
	// if NO_ZERO_DATE is not enabled, '0000-00-00' is permitted and inserts produce no warning
	// if NO_ZERO_DATE is enabled, '0000-00-00' is permitted and inserts produce a warning
	// If NO_ZERO_DATE mode and strict mode are enabled, '0000-00-00' is not permitted and inserts produce an error, unless IGNORE is given as well. For INSERT IGNORE and UPDATE IGNORE, '0000-00-00' is permitted and inserts produce a warning.
	// if NO_ZERO_IN_DATE is not enabled, dates with zero parts are permitted and inserts produce no warning
	// if NO_ZERO_IN_DATE is enabled, dates with zero parts are inserted as '0000-00-00' and produce a warning
	// If NO_ZERO_IN_DATE mode and strict mode are enabled, dates with zero parts are not permitted and inserts produce an error, unless IGNORE is given as well. For INSERT IGNORE and UPDATE IGNORE, dates with zero parts are inserted as '0000-00-00' and produce a warning.

	ignoreErr := ec.LevelForGroup(errctx.ErrGroupDupKey) != errctx.LevelError

	// Timestamp in MySQL is since EPOCH 1970-01-01 00:00:00 UTC and can by definition not have invalid dates!
	// Zero date is special for MySQL timestamp and *NOT* 1970-01-01 00:00:00, but 0000-00-00 00:00:00!
	// in MySQL 8.0, the Timestamp's case is different to Datetime/Date, as shown below:
	//
	// |              | NZD               | NZD|ST  | ELSE              | ELSE|ST  |
	// | ------------ | ----------------- | ------- | ----------------- | -------- |
	// | `0000-00-01` | Truncate + Warning| Error   | Truncate + Warning| Error    |
	// | `0000-00-00` | Success + Warning | Error   | Success           | Success  |
	//
	// * **NZD**: NO_ZERO_DATE_MODE
	// * **ST**: STRICT_TRANS_TABLES
	// * **ELSE**: empty or NO_ZERO_IN_DATE_MODE
	if tm.IsZero() && col.GetType() == mysql.TypeTimestamp {
		innerErr := types.ErrWrongValue.FastGenByArgs(zeroT, str)
		if mode.HasStrictMode() && !ignoreErr && (tmIsInvalid || mode.HasNoZeroDateMode()) {
			return types.NewDatum(zeroV), true, errors.Trace(innerErr)
		}

		if tmIsInvalid || mode.HasNoZeroDateMode() {
			ec.AppendWarning(innerErr)
		}
		return types.NewDatum(zeroV), true, nil
	} else if tmIsInvalid && col.GetType() == mysql.TypeTimestamp {
		// Prevent from being stored! Invalid timestamp!
		warn := types.ErrWrongValue.FastGenByArgs(zeroT, str)
		if mode.HasStrictMode() {
			return types.NewDatum(zeroV), true, errors.Trace(warn)
		}
		// no strict mode, truncate to 0000-00-00 00:00:00
		ec.AppendWarning(warn)
		return types.NewDatum(zeroV), true, nil
	} else if tm.IsZero() || tm.InvalidZero() {
		if tm.IsZero() {
			// Don't care NoZeroDate mode if time val is invalid.
			if !tmIsInvalid && !mode.HasNoZeroDateMode() {
				return types.NewDatum(zeroV), true, nil
			}
		} else if tm.InvalidZero() {
			if !mode.HasNoZeroInDateMode() {
				return casted, true, nil
			}
		}

		innerErr := types.ErrWrongValue.FastGenByArgs(zeroT, str)
		if mode.HasStrictMode() && !ignoreErr {
			return types.NewDatum(zeroV), true, errors.Trace(innerErr)
		}

		// TODO: as in MySQL 8.0's implement, warning message is `types.ErrWarnDataOutOfRange`,
		// but this error message need a `rowIdx` argument, in this context, the `rowIdx` is missing.
		// And refactor this function seems too complicated, so we set the warning message the same to error's.
		ec.AppendWarning(innerErr)
		return types.NewDatum(zeroV), true, nil
	}

	return casted, false, nil
}

// CastValue casts a value based on column type.
// If forceIgnoreTruncate is true, truncated errors will be ignored.
// If returnErr is true, directly return any conversion errors.
// It's safe now and it's the same as the behavior of select statement.
// Set it to true only in FillVirtualColumnValue and UnionScanExec.Next()
// If the handle of err is changed latter, the behavior of forceIgnoreTruncate also need to change.
// TODO: change the third arg to TypeField. Not pass ColumnInfo.
func CastValue(sctx variable.SessionVarsProvider, val types.Datum, col *model.ColumnInfo, returnErr, forceIgnoreTruncate bool) (casted types.Datum, err error) {
	vars := sctx.GetSessionVars()
	sc := vars.StmtCtx
	return castColumnValue(sc.TypeCtx(), sc.ErrCtx(), vars.SQLMode, val, col, vars.ConnectionID, returnErr, forceIgnoreTruncate)
}

// CastColumnValue casts a value based on column type with expression BuildContext
func CastColumnValue(ctx expression.BuildContext, val types.Datum, col *model.ColumnInfo, returnErr, forceIgnoreTruncate bool) (casted types.Datum, err error) {
	evalCtx := ctx.GetEvalCtx()
	return castColumnValue(evalCtx.TypeCtx(), evalCtx.ErrCtx(), evalCtx.SQLMode(), val, col, ctx.ConnectionID(), returnErr, forceIgnoreTruncate)
}

// castColumnValue casts a value based on column type.
func castColumnValue(tc types.Context, ec errctx.Context, sqlMode mysql.SQLMode, val types.Datum, col *model.ColumnInfo, connID uint64, returnErr, forceIgnoreTruncate bool) (casted types.Datum, err error) {
	casted, err = val.ConvertTo(tc, &col.FieldType)
	// TODO: make sure all truncate errors are handled by ConvertTo.
	if returnErr && err != nil {
		return casted, err
	}
	if err != nil && types.ErrTruncated.Equal(err) && col.GetType() != mysql.TypeSet && col.GetType() != mysql.TypeEnum {
		str, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Warn("Datum ToString failed", zap.Stringer("Datum", val), zap.Error(err1))
		}
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs(col.FieldType.CompactStr(), str)
	} else if !casted.IsNull() &&
		(col.GetType() == mysql.TypeDate || col.GetType() == mysql.TypeDatetime || col.GetType() == mysql.TypeTimestamp) {
		str, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Warn("Datum ToString failed", zap.Stringer("Datum", val), zap.Error(err1))
			str = val.GetString()
		}
		if innCasted, exit, innErr := handleZeroDatetime(ec, sqlMode, col, casted, str, types.ErrWrongValue.Equal(err)); exit {
			return innCasted, innErr
		}
	} else if err != nil && charset.ErrInvalidCharacterString.Equal(err) {
		err = convertToIncorrectStringErr(err, col.Name.O)
		logutil.BgLogger().Debug("incorrect string value", zap.Uint64("conn", connID), zap.Error(err))
	}

	err = tc.HandleTruncate(err)

	if forceIgnoreTruncate {
		err = nil
	} else if err != nil {
		return casted, err
	}

	if col.GetType() == mysql.TypeString && !types.IsBinaryStr(&col.FieldType) {
		truncateTrailingSpaces(&casted)
	}
	return casted, err
}

// ColDesc describes column information like MySQL desc and show columns do.
type ColDesc struct {
	Field string
	Type  string
	// Charset is nil if the column doesn't have a charset, or a string indicating the charset name.
	Charset any
	// Collation is nil if the column doesn't have a collation, or a string indicating the collation name.
	Collation    any
	Null         string
	Key          string
	DefaultValue any
	Extra        string
	Privileges   string
	Comment      string
}

const defaultPrivileges = "select,insert,update,references"

// NewColDesc returns a new ColDesc for a column.
func NewColDesc(col *Column) *ColDesc {
	// TODO: if we have no primary key and a unique index which's columns are all not null
	// we will set these columns' flag as PriKeyFlag
	// see https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
	// create table
	name := col.Name
	nullFlag := "YES"
	if mysql.HasNotNullFlag(col.GetFlag()) {
		nullFlag = "NO"
	}
	keyFlag := ""
	if mysql.HasPriKeyFlag(col.GetFlag()) {
		keyFlag = "PRI"
	} else if mysql.HasUniKeyFlag(col.GetFlag()) {
		keyFlag = "UNI"
	} else if mysql.HasMultipleKeyFlag(col.GetFlag()) {
		keyFlag = "MUL"
	}
	var defaultValue any
	if !mysql.HasNoDefaultValueFlag(col.GetFlag()) {
		defaultValue = col.GetDefaultValue()
		if defaultValStr, ok := defaultValue.(string); ok {
			if (col.GetType() == mysql.TypeTimestamp || col.GetType() == mysql.TypeDatetime) &&
				strings.EqualFold(defaultValStr, ast.CurrentTimestamp) &&
				col.GetDecimal() > 0 {
				defaultValue = fmt.Sprintf("%s(%d)", defaultValStr, col.GetDecimal())
			}
		}
	}

	extra := ""
	if mysql.HasAutoIncrementFlag(col.GetFlag()) {
		extra = "auto_increment"
	} else if mysql.HasOnUpdateNowFlag(col.GetFlag()) {
		// in order to match the rules of mysql 8.0.16 version
		// see https://github.com/pingcap/tidb/issues/10337
		extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP" + OptionalFsp(&col.FieldType)
	} else if col.IsGenerated() {
		if col.GeneratedStored {
			extra = "STORED GENERATED"
		} else {
			extra = "VIRTUAL GENERATED"
		}
	} else if col.DefaultIsExpr {
		extra = "DEFAULT_GENERATED"
	}

	desc := &ColDesc{
		Field:        name.O,
		Type:         col.GetTypeDesc(),
		Charset:      col.GetCharset(),
		Collation:    col.GetCollate(),
		Null:         nullFlag,
		Key:          keyFlag,
		DefaultValue: defaultValue,
		Extra:        extra,
		Privileges:   defaultPrivileges,
		Comment:      col.Comment,
	}
	if !field_types.HasCharset(&col.ColumnInfo.FieldType) {
		desc.Charset = nil
		desc.Collation = nil
	}
	return desc
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
			return errDuplicateColumn.GenWithStackByArgs(name)
		}

		m[name.L] = struct{}{}
	}

	return nil
}

// CheckNotNull checks if nil value set to a column with NotNull flag is set.
// When caller is LOAD DATA, `rowCntInLoadData` should be greater than 0 and it
// will return a ErrWarnNullToNotnull when error.
// Otherwise, it will return a ErrColumnCantNull when error.
func (c *Column) CheckNotNull(data *types.Datum, rowCntInLoadData uint64) error {
	if (mysql.HasNotNullFlag(c.GetFlag()) || mysql.HasPreventNullInsertFlag(c.GetFlag())) && data.IsNull() {
		if rowCntInLoadData > 0 {
			return ErrWarnNullToNotnull.GenWithStackByArgs(c.Name, rowCntInLoadData)
		}
		return ErrColumnCantNull.GenWithStackByArgs(c.Name)
	}
	return nil
}

// HandleBadNull handles the bad null error.
// When caller is LOAD DATA, `rowCntInLoadData` should be greater than 0 the
// error is ErrWarnNullToNotnull.
// Otherwise, the error is ErrColumnCantNull.
// If BadNullAsWarning is true, it will append the error as a warning, else return the error.
func (c *Column) HandleBadNull(ec errctx.Context, d *types.Datum, rowCntInLoadData uint64) error {
	if err := c.CheckNotNull(d, rowCntInLoadData); err != nil {
		if ec.HandleError(err) == nil {
			*d = GetZeroValue(c.ToInfo())
			return nil
		}
		return err
	}
	return nil
}

// IsPKHandleColumn checks if the column is primary key handle column.
func (c *Column) IsPKHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.GetFlag()) && tbInfo.PKIsHandle
}

// IsCommonHandleColumn checks if the column is common handle column.
func (c *Column) IsCommonHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.GetFlag()) && tbInfo.IsCommonHandle
}

type getColOriginDefaultValue struct {
	StrictSQLMode bool
}

// GetColOriginDefaultValue gets default value of the column from original default value.
func GetColOriginDefaultValue(ctx expression.BuildContext, col *model.ColumnInfo) (types.Datum, error) {
	return getColDefaultValue(ctx, col, col.GetOriginDefaultValue(), nil)
}

// GetColOriginDefaultValueWithoutStrictSQLMode gets default value of the column from original default value with Strict SQL mode.
func GetColOriginDefaultValueWithoutStrictSQLMode(ctx expression.BuildContext, col *model.ColumnInfo) (types.Datum, error) {
	return getColDefaultValue(ctx, col, col.GetOriginDefaultValue(), &getColOriginDefaultValue{
		StrictSQLMode: false,
	})
}

// CheckNoDefaultValueForInsert checks if the column has no default value before insert data.
// CheckNoDefaultValueForInsert extracts the check logic from getColDefaultValueFromNil,
// since getColDefaultValueFromNil function is public path and both read/write and other places use it.
// But CheckNoDefaultValueForInsert logic should only check before insert.
func CheckNoDefaultValueForInsert(sc *stmtctx.StatementContext, col *model.ColumnInfo) error {
	if mysql.HasNoDefaultValueFlag(col.GetFlag()) && !col.DefaultIsExpr && col.GetDefaultValue() == nil && col.GetType() != mysql.TypeEnum {
		ignoreErr := sc.ErrGroupLevel(errctx.ErrGroupBadNull) != errctx.LevelError
		if !ignoreErr {
			return ErrNoDefaultValue.GenWithStackByArgs(col.Name)
		}
		if !mysql.HasNotNullFlag(col.GetFlag()) {
			sc.AppendWarning(ErrNoDefaultValue.FastGenByArgs(col.Name))
		}
	}
	return nil
}

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx expression.BuildContext, col *model.ColumnInfo) (types.Datum, error) {
	defaultValue := col.GetDefaultValue()
	if !col.DefaultIsExpr {
		return getColDefaultValue(ctx, col, defaultValue, nil)
	}
	return getColDefaultExprValue(ctx, col, defaultValue.(string))
}

// EvalColDefaultExpr eval default expr node to explicit default value.
func EvalColDefaultExpr(ctx expression.BuildContext, col *model.ColumnInfo, defaultExpr ast.ExprNode) (types.Datum, error) {
	d, err := expression.EvalSimpleAst(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastColumnValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

func getColDefaultExprValue(ctx expression.BuildContext, col *model.ColumnInfo, defaultValue string) (types.Datum, error) {
	var defaultExpr ast.ExprNode
	expr := fmt.Sprintf("select %s", defaultValue)
	stmts, _, err := parser.New().ParseSQL(expr)
	if err == nil {
		defaultExpr = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	d, err := expression.EvalSimpleAst(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastColumnValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

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
