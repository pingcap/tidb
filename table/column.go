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
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	field_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

// Column provides meta data describing a table column.
type Column struct {
	*model.ColumnInfo
	// If this column is a generated column, the expression will be stored here.
	GeneratedExpr ast.ExprNode
	// If this column has default expr value, this expression will be stored here.
	DefaultExpr ast.ExprNode
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
// If any columns don't match, return nil and the first missing column's name
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

func handleWrongASCIIValue(ctx sessionctx.Context, col *model.ColumnInfo, casted *types.Datum, str string, i int) (types.Datum, error) {
	sc := ctx.GetSessionVars().StmtCtx
	err := ErrTruncatedWrongValueForField.FastGen("incorrect ascii value %x(%s) for column %s", casted.GetBytes(), str, col.Name)
	logutil.BgLogger().Error("incorrect ASCII value", zap.Uint64("conn", ctx.GetSessionVars().ConnectionID), zap.Error(err))
	truncateVal := types.NewStringDatum(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

func handleWrongUtf8Value(ctx sessionctx.Context, col *model.ColumnInfo, casted *types.Datum, str string, i int) (types.Datum, error) {
	sc := ctx.GetSessionVars().StmtCtx
	err := ErrTruncatedWrongValueForField.FastGen("incorrect utf8 value %x(%s) for column %s", casted.GetBytes(), str, col.Name)
	logutil.BgLogger().Error("incorrect UTF-8 value", zap.Uint64("conn", ctx.GetSessionVars().ConnectionID), zap.Error(err))
	// Truncate to valid utf8 string.
	truncateVal := types.NewStringDatum(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

func handleZeroDatetime(ctx sessionctx.Context, col *model.ColumnInfo, casted types.Datum, str string, tmIsInvalid bool) (types.Datum, bool, error) {
	sc := ctx.GetSessionVars().StmtCtx
	tm := casted.GetMysqlTime()
	mode := ctx.GetSessionVars().SQLMode

	var (
		zeroV types.Time
		zeroT string
	)
	switch col.Tp {
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

	ignoreErr := sc.DupKeyAsWarning

	// in MySQL 8.0, the Timestamp's case is different to Datetime/Date, as shown below:
	//
	// |              | NZD               | NZD|ST  | ELSE              | ELSE|ST  |
	// | ------------ | ----------------- | ------- | ----------------- | -------- |
	// | `0000-00-01` | Success + Warning | Error   | Success + Warning | Error    |
	// | `0000-00-00` | Success + Warning | Error   | Success           | Success  |
	//
	// * **NZD**: NO_ZERO_DATE_MODE
	// * **ST**: STRICT_TRANS_TABLES
	// * **ELSE**: empty or NO_ZERO_IN_DATE_MODE
	if tm.IsZero() && col.Tp == mysql.TypeTimestamp {
		innerErr := types.ErrWrongValue.GenWithStackByArgs(zeroT, str)
		if mode.HasStrictMode() && !ignoreErr && (tmIsInvalid || mode.HasNoZeroDateMode()) {
			return types.NewDatum(zeroV), true, innerErr
		}

		if tmIsInvalid || mode.HasNoZeroDateMode() {
			sc.AppendWarning(innerErr)
		}
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

		innerErr := types.ErrWrongValue.GenWithStackByArgs(zeroT, str)
		if mode.HasStrictMode() && !ignoreErr {
			return types.NewDatum(zeroV), true, innerErr
		}

		// TODO: as in MySQL 8.0's implement, warning message is `types.ErrWarnDataOutOfRange`,
		// but this error message need a `rowIdx` argument, in this context, the `rowIdx` is missing.
		// And refactor this function seems too complicated, so we set the warning message the same to error's.
		sc.AppendWarning(innerErr)
		return types.NewDatum(zeroV), true, nil
	}

	return casted, false, nil
}

// CastValue casts a value based on column type.
// If forceIgnoreTruncate is true, truncated errors will be ignored.
// If returnOverflow is true, don't handle overflow errors in this function.
// It's safe now and it's the same as the behavior of select statement.
// Set it to true only in FillVirtualColumnValue and UnionScanExec.Next()
// If the handle of err is changed latter, the behavior of forceIgnoreTruncate also need to change.
// TODO: change the third arg to TypeField. Not pass ColumnInfo.
func CastValue(ctx sessionctx.Context, val types.Datum, col *model.ColumnInfo, returnErr, forceIgnoreTruncate bool) (casted types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	// Set the reorg attribute for cast value functionality.
	if col.ChangeStateInfo != nil {
		origin := ctx.GetSessionVars().StmtCtx.InReorgAttribute
		ctx.GetSessionVars().StmtCtx.InReorgAttribute = true
		defer func() {
			ctx.GetSessionVars().StmtCtx.InReorgAttribute = origin
		}()
	}
	casted, err = val.ConvertTo(sc, &col.FieldType)
	// TODO: make sure all truncate errors are handled by ConvertTo.
	if returnErr && err != nil {
		return casted, err
	}
	if err != nil && types.ErrTruncated.Equal(err) && col.Tp != mysql.TypeSet && col.Tp != mysql.TypeEnum {
		str, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Warn("Datum ToString failed", zap.Stringer("Datum", val), zap.Error(err1))
		}
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs(col.FieldType.CompactStr(), str)
	} else if (sc.InInsertStmt || sc.InUpdateStmt) && !casted.IsNull() &&
		(val.Kind() != types.KindMysqlTime || !val.GetMysqlTime().IsZero()) &&
		(col.Tp == mysql.TypeDate || col.Tp == mysql.TypeDatetime || col.Tp == mysql.TypeTimestamp) {
		if innCasted, exit, innErr := handleZeroDatetime(ctx, col, casted, val.GetString(), types.ErrWrongValue.Equal(err)); exit {
			return innCasted, innErr
		}
	}

	err = sc.HandleTruncate(err)

	if forceIgnoreTruncate {
		err = nil
	} else if err != nil {
		return casted, err
	}

	if col.Tp == mysql.TypeString && !types.IsBinaryStr(&col.FieldType) {
		truncateTrailingSpaces(&casted)
	}

	if col.Charset == charset.CharsetASCII {
		if ctx.GetSessionVars().SkipASCIICheck {
			return casted, nil
		}

		str := casted.GetString()
		for i := 0; i < len(str); i++ {
			if str[i] > unicode.MaxASCII {
				casted, err = handleWrongASCIIValue(ctx, col, &casted, str, i)
				break
			}
		}
		if forceIgnoreTruncate {
			err = nil
		}
		return casted, err
	}

	if ctx.GetSessionVars().SkipUTF8Check {
		return casted, nil
	}

	if !mysql.IsUTF8Charset(col.Charset) {
		return casted, nil
	}
	str := casted.GetString()
	utf8Charset := col.Charset == mysql.UTF8Charset
	doMB4CharCheck := utf8Charset && config.GetGlobalConfig().CheckMb4ValueInUTF8
	for i, w := 0, 0; i < len(str); i += w {
		runeValue, width := utf8.DecodeRuneInString(str[i:])
		if runeValue == utf8.RuneError {
			if strings.HasPrefix(str[i:], string(utf8.RuneError)) {
				w = width
				continue
			}
			casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
			break
		} else if width > 3 && doMB4CharCheck {
			// Handle non-BMP characters.
			casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
			break
		}
		w = width
	}

	if forceIgnoreTruncate {
		err = nil
	}
	return casted, err
}

// ColDesc describes column information like MySQL desc and show columns do.
type ColDesc struct {
	Field string
	Type  string
	// Charset is nil if the column doesn't have a charset, or a string indicating the charset name.
	Charset interface{}
	// Collation is nil if the column doesn't have a collation, or a string indicating the collation name.
	Collation    interface{}
	Null         string
	Key          string
	DefaultValue interface{}
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
		defaultValue = col.GetDefaultValue()
		if defaultValStr, ok := defaultValue.(string); ok {
			if (col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime) &&
				strings.EqualFold(defaultValStr, ast.CurrentTimestamp) &&
				col.Decimal > 0 {
				defaultValue = fmt.Sprintf("%s(%d)", defaultValStr, col.Decimal)
			}
		}
	}

	extra := ""
	if mysql.HasAutoIncrementFlag(col.Flag) {
		extra = "auto_increment"
	} else if mysql.HasOnUpdateNowFlag(col.Flag) {
		// in order to match the rules of mysql 8.0.16 version
		// see https://github.com/pingcap/tidb/issues/10337
		extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP" + OptionalFsp(&col.FieldType)
	} else if col.IsGenerated() {
		if col.GeneratedStored {
			extra = "STORED GENERATED"
		} else {
			extra = "VIRTUAL GENERATED"
		}
	}

	desc := &ColDesc{
		Field:        name.O,
		Type:         col.GetTypeDesc(),
		Charset:      col.Charset,
		Collation:    col.Collate,
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
func (c *Column) CheckNotNull(data *types.Datum) error {
	if (mysql.HasNotNullFlag(c.Flag) || mysql.HasPreventNullInsertFlag(c.Flag)) && data.IsNull() {
		return ErrColumnCantNull.GenWithStackByArgs(c.Name)
	}
	return nil
}

// HandleBadNull handles the bad null error.
// If BadNullAsWarning is true, it will append the error as a warning, else return the error.
func (c *Column) HandleBadNull(d *types.Datum, sc *stmtctx.StatementContext) error {
	if err := c.CheckNotNull(d); err != nil {
		if sc.BadNullAsWarning {
			sc.AppendWarning(err)
			*d = GetZeroValue(c.ToInfo())
			return nil
		}
		return err
	}
	return nil
}

// IsPKHandleColumn checks if the column is primary key handle column.
func (c *Column) IsPKHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.Flag) && tbInfo.PKIsHandle
}

// IsCommonHandleColumn checks if the column is common handle column.
func (c *Column) IsCommonHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.Flag) && tbInfo.IsCommonHandle
}

// CheckNotNull checks if row has nil value set to a column with NotNull flag set.
func CheckNotNull(cols []*Column, row []types.Datum) error {
	for _, c := range cols {
		if err := c.CheckNotNull(&row[c.Offset]); err != nil {
			return err
		}
	}
	return nil
}

// GetColOriginDefaultValue gets default value of the column from original default value.
func GetColOriginDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	return getColDefaultValue(ctx, col, col.GetOriginDefaultValue())
}

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	defaultValue := col.GetDefaultValue()
	if !col.DefaultIsExpr {
		return getColDefaultValue(ctx, col, defaultValue)
	}
	return getColDefaultExprValue(ctx, col, defaultValue.(string))
}

// EvalColDefaultExpr eval default expr node to explicit default value.
func EvalColDefaultExpr(ctx sessionctx.Context, col *model.ColumnInfo, defaultExpr ast.ExprNode) (types.Datum, error) {
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

func getColDefaultExprValue(ctx sessionctx.Context, col *model.ColumnInfo, defaultValue string) (types.Datum, error) {
	var defaultExpr ast.ExprNode
	expr := fmt.Sprintf("select %s", defaultValue)
	stmts, _, err := parser.New().Parse(expr, "", "")
	if err == nil {
		defaultExpr = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

func getColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo, defaultVal interface{}) (types.Datum, error) {
	if defaultVal == nil {
		return getColDefaultValueFromNil(ctx, col)
	}

	if col.Tp != mysql.TypeTimestamp && col.Tp != mysql.TypeDatetime {
		value, err := CastValue(ctx, types.NewDatum(defaultVal), col, false, false)
		if err != nil {
			return types.Datum{}, err
		}
		return value, nil
	}

	// Check and get timestamp/datetime default value.
	sc := ctx.GetSessionVars().StmtCtx
	var needChangeTimeZone bool
	// If the column's default value is not ZeroDatetimeStr nor CurrentTimestamp, should use the time zone of the default value itself.
	if col.Tp == mysql.TypeTimestamp {
		if vv, ok := defaultVal.(string); ok && vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			needChangeTimeZone = true
			originalTZ := sc.TimeZone
			// For col.Version = 0, the timezone information of default value is already lost, so use the system timezone as the default value timezone.
			sc.TimeZone = timeutil.SystemLocation()
			if col.Version >= model.ColumnInfoVersion1 {
				sc.TimeZone = time.UTC
			}
			defer func() { sc.TimeZone = originalTZ }()
		}
	}
	value, err := expression.GetTimeValue(ctx, defaultVal, col.Tp, int8(col.Decimal))
	if err != nil {
		return types.Datum{}, errGetDefaultFailed.GenWithStackByArgs(col.Name)
	}
	// If the column's default value is not ZeroDatetimeStr or CurrentTimestamp, convert the default value to the current session time zone.
	if needChangeTimeZone {
		t := value.GetMysqlTime()
		err = t.ConvertTimeZone(sc.TimeZone, ctx.GetSessionVars().Location())
		if err != nil {
			return value, err
		}
		value.SetMysqlTime(t)
	}
	return value, nil
}

func getColDefaultValueFromNil(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.Datum{}, nil
	}
	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		defEnum, err := types.ParseEnumValue(col.FieldType.Elems, 1)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewCollateMysqlEnumDatum(defEnum, col.Collate), nil
	}
	if mysql.HasAutoIncrementFlag(col.Flag) {
		// Auto increment column doesn't has default value and we should not return error.
		return GetZeroValue(col), nil
	}
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	if !vars.StrictSQLMode {
		sc.AppendWarning(ErrNoDefaultValue.FastGenByArgs(col.Name))
		return GetZeroValue(col), nil
	}
	if sc.BadNullAsWarning {
		sc.AppendWarning(ErrColumnCantNull.FastGenByArgs(col.Name))
		return GetZeroValue(col), nil
	}
	return types.Datum{}, ErrNoDefaultValue.FastGenByArgs(col.Name)
}

// GetZeroValue gets zero value for given column type.
func GetZeroValue(col *model.ColumnInfo) types.Datum {
	var d types.Datum
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(col.Flag) {
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
		d.SetLength(col.Flen)
		d.SetFrac(col.Decimal)
		d.SetMysqlDecimal(new(types.MyDecimal))
	case mysql.TypeString:
		if col.Flen > 0 && col.Charset == charset.CharsetBin {
			d.SetBytes(make([]byte, col.Flen))
		} else {
			d.SetString("", col.Collate)
		}
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString("", col.Collate)
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
		d.SetMysqlSet(types.Set{}, col.Collate)
	case mysql.TypeEnum:
		d.SetMysqlEnum(types.Enum{}, col.Collate)
	case mysql.TypeJSON:
		d.SetMysqlJSON(json.CreateBinary(nil))
	}
	return d
}

// OptionalFsp convert a FieldType.Decimal to string.
func OptionalFsp(fieldType *types.FieldType) string {
	fsp := fieldType.Decimal
	if fsp == 0 {
		return ""
	}
	return "(" + strconv.Itoa(fsp) + ")"
}
