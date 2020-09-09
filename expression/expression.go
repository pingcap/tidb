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

package expression

import (
	goJSON "encoding/json"
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
)

// EvalAstExpr evaluates ast expression directly.
var EvalAstExpr func(sctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error)

// RewriteAstExpr rewrite ast expression directly.
var RewriteAstExpr func(sctx sessionctx.Context, expr ast.ExprNode, schema *Schema) (Expression, error)

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler

	// Eval evaluates an expression through a row.
	Eval(row chunk.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx sessionctx.Context, row chunk.Row) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(ctx sessionctx.Context, row chunk.Row) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(ctx sessionctx.Context, row chunk.Row) (val json.BinaryJSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// Clone copies an expression totally.
	Clone() Expression

	// Equal checks whether two expressions are equal.
	Equal(ctx sessionctx.Context, e Expression) bool

	// IsCorrelated checks if this expression has correlated key.
	IsCorrelated() bool

	// ConstItem checks if this expression is constant item, regardless of query evaluation state.
	// An expression is constant item if it:
	// refers no tables.
	// refers no correlated column.
	// refers no subqueries that refers any tables.
	// refers no non-deterministic functions.
	// refers no statement parameters.
	ConstItem() bool

	// Decorrelate try to decorrelate the expression by schema.
	Decorrelate(schema *Schema) Expression

	// ResolveIndices resolves indices by the given schema. It will copy the original expression and return the copied one.
	ResolveIndices(schema *Schema) (Expression, error)

	// resolveIndices is called inside the `ResolveIndices` It will perform on the expression itself.
	resolveIndices(schema *Schema) error

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string

	// HashCode creates the hashcode for expression which can be used to identify itself from other expression.
	// It generated as the following:
	// Constant: ConstantFlag+encoded value
	// Column: ColumnFlag+encoded value
	// ScalarFunction: SFFlag+encoded function name + encoded arg_1 + encoded arg_2 + ...
	HashCode(sc *stmtctx.StatementContext) []byte
}

// CNFExprs stands for a CNF expression.
type CNFExprs []Expression

// Clone clones itself.
func (e CNFExprs) Clone() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	for _, expr := range e {
		cnf = append(cnf, expr.Clone())
	}
	return cnf
}

func isColumnInOperand(c *Column) bool {
	return c.InOperand
}

// IsEQCondFromIn checks if an expression is equal condition converted from `[not] in (subq)`.
func IsEQCondFromIn(expr Expression) bool {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return false
	}
	cols := make([]*Column, 0, 1)
	cols = ExtractColumnsFromExpressions(cols, sf.GetArgs(), isColumnInOperand)
	return len(cols) > 0
}

// EvalBool evaluates expression list to a boolean value. The first returned value
// indicates bool result of the expression list, the second returned value indicates
// whether the result of the expression list is null, it can only be true when the
// first returned values is false.
func EvalBool(ctx sessionctx.Context, exprList CNFExprs, row chunk.Row) (bool, bool, error) {
	hasNull := false
	for _, expr := range exprList {
		data, err := expr.Eval(row)
		if err != nil {
			return false, false, err
		}
		if data.IsNull() {
			// For queries like `select a in (select a from s where t.b = s.b) from t`,
			// if result of `t.a = s.a` is null, we cannot return immediately until
			// we have checked if `t.b = s.b` is null or false, because it means
			// subquery is empty, and we should return false as the result of the whole
			// exprList in that case, instead of null.
			if !IsEQCondFromIn(expr) {
				return false, false, nil
			}
			hasNull = true
			continue
		}

		i, err := data.ToBool(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return false, false, err
		}
		if i == 0 {
			return false, false, nil
		}
	}
	if hasNull {
		return false, true, nil
	}
	return true, false, nil
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx sessionctx.Context, conditions []Expression, funcName string) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr := NewFunctionInternal(ctx, funcName,
		types.NewFieldType(mysql.TypeTiny),
		composeConditionWithBinaryOp(ctx, conditions[:length/2], funcName),
		composeConditionWithBinaryOp(ctx, conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

func extractBinaryOpItems(conditions *ScalarFunction, funcName string) []Expression {
	var ret []Expression
	for _, arg := range conditions.GetArgs() {
		if sf, ok := arg.(*ScalarFunction); ok && sf.FuncName.L == funcName {
			ret = append(ret, extractBinaryOpItems(sf, funcName)...)
		} else {
			ret = append(ret, arg)
		}
	}
	return ret
}

// FlattenDNFConditions extracts DNF expression's leaf item.
// e.g. or(or(a=1, a=2), or(a=3, a=4)), we'll get [a=1, a=2, a=3, a=4].
func FlattenDNFConditions(DNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(DNFCondition, ast.LogicOr)
}

// FlattenCNFConditions extracts CNF expression's leaf item.
// e.g. and(and(a>1, a>2), and(a>3, a>4)), we'll get [a>1, a>2, a>3, a>4].
func FlattenCNFConditions(CNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(CNFCondition, ast.LogicAnd)
}

// Assignment represents a set assignment in Update, such as
// Update t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	Col  *Column
	Expr Expression
}

// VarAssignment represents a variable assignment in Set, such as set global a = 1.
type VarAssignment struct {
	Name        string
	Expr        Expression
	IsDefault   bool
	IsGlobal    bool
	IsSystem    bool
	ExtendValue *Constant
}

// splitNormalFormItems split CNF(conjunctive normal form) like "a and b and c", or DNF(disjunctive normal form) like "a or b or c"
func splitNormalFormItems(onExpr Expression, funcName string) []Expression {
	switch v := onExpr.(type) {
	case *ScalarFunction:
		if v.FuncName.L == funcName {
			var ret []Expression
			for _, arg := range v.GetArgs() {
				ret = append(ret, splitNormalFormItems(arg, funcName)...)
			}
			return ret
		}
	}
	return []Expression{onExpr}
}

// SplitCNFItems splits CNF items.
// CNF means conjunctive normal form, e.g. "a and b and c".
func SplitCNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicAnd)
}

// SplitDNFItems splits DNF items.
// DNF means disjunctive normal form, e.g. "a or b or c".
func SplitDNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicOr)
}

// EvaluateExprWithNull sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
func EvaluateExprWithNull(ctx sessionctx.Context, schema *Schema, expr Expression) Expression {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i] = EvaluateExprWithNull(ctx, schema, arg)
		}
		return NewFunctionInternal(ctx, x.FuncName.L, x.RetType, args...)
	case *Column:
		if !schema.Contains(x) {
			return x
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(x)
		}
	}
	return expr
}

// TableInfo2Schema converts table info to schema with empty DBName.
func TableInfo2Schema(ctx sessionctx.Context, tbl *model.TableInfo) *Schema {
	return TableInfo2SchemaWithDBName(ctx, model.CIStr{}, tbl)
}

// TableInfo2SchemaWithDBName converts table info to schema.
func TableInfo2SchemaWithDBName(ctx sessionctx.Context, dbName model.CIStr, tbl *model.TableInfo) *Schema {
	cols := ColumnInfos2ColumnsWithDBName(ctx, dbName, tbl.Name, tbl.Columns)
	keys := make([]KeyInfo, 0, len(tbl.Indices)+1)
	for _, idx := range tbl.Indices {
		if !idx.Unique || idx.State != model.StatePublic {
			continue
		}
		ok := true
		newKey := make([]*Column, 0, len(idx.Columns))
		for _, idxCol := range idx.Columns {
			find := false
			for i, col := range tbl.Columns {
				if idxCol.Name.L == col.Name.L {
					if !mysql.HasNotNullFlag(col.Flag) {
						break
					}
					newKey = append(newKey, cols[i])
					find = true
					break
				}
			}
			if !find {
				ok = false
				break
			}
		}
		if ok {
			keys = append(keys, newKey)
		}
	}
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				keys = append(keys, KeyInfo{cols[i]})
				break
			}
		}
	}
	schema := NewSchema(cols...)
	schema.SetUniqueKeys(keys)
	return schema
}

// ColumnInfos2ColumnsWithDBName converts a slice of ColumnInfo to a slice of Column.
func ColumnInfos2ColumnsWithDBName(ctx sessionctx.Context, dbName, tblName model.CIStr, colInfos []*model.ColumnInfo) []*Column {
	columns := make([]*Column, 0, len(colInfos))
	for _, col := range colInfos {
		if col.State != model.StatePublic {
			continue
		}
		newCol := &Column{
			OrigColName: col.Name,
			OrigTblName: tblName,
			ColName:     col.Name,
			TblName:     tblName,
			DBName:      dbName,
			RetType:     &col.FieldType,
			ID:          col.ID,
			UniqueID:    ctx.GetSessionVars().AllocPlanColumnID(),
			Index:       col.Offset,
		}
		columns = append(columns, newCol)
	}
	return columns
}

// NewValuesFunc creates a new values function.
func NewValuesFunc(ctx sessionctx.Context, offset int, retTp *types.FieldType) *ScalarFunction {
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, offset, retTp}
	bt, err := fc.getFunction(ctx, nil)
	terror.Log(err)
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.Values),
		RetType:  retTp,
		Function: bt,
	}
}

// IsBinaryLiteral checks whether an expression is a binary literal
func IsBinaryLiteral(expr Expression) bool {
	con, ok := expr.(*Constant)
	return ok && con.Value.Kind() == types.KindBinaryLiteral
}

<<<<<<< HEAD
=======
func canFuncBePushed(sf *ScalarFunction, storeType kv.StoreType) bool {
	// Use the failpoint to control whether to push down an expression in the integration test.
	// Push down all expression if the `failpoint expression` is `all`, otherwise, check
	// whether scalar function's name is contained in the enabled expression list (e.g.`ne,eq,lt`).
	// If neither of the above is true, switch to original logic.
	failpoint.Inject("PushDownTestSwitcher", func(val failpoint.Value) {
		enabled := val.(string)
		if enabled == "all" {
			failpoint.Return(true)
		}
		exprs := strings.Split(enabled, ",")
		for _, expr := range exprs {
			if strings.ToLower(strings.TrimSpace(expr)) == sf.FuncName.L {
				failpoint.Return(true)
			}
		}
	})

	ret := false
	switch sf.FuncName.L {
	case
		// op functions.
		ast.LogicAnd,
		ast.LogicOr,
		ast.LogicXor,
		ast.UnaryNot,
		ast.And,
		ast.Or,
		ast.Xor,
		ast.BitNeg,
		ast.LeftShift,
		ast.RightShift,
		ast.UnaryMinus,

		// compare functions.
		ast.LT,
		ast.LE,
		ast.EQ,
		ast.NE,
		ast.GE,
		ast.GT,
		ast.NullEQ,
		ast.In,
		ast.IsNull,
		ast.Like,
		ast.IsTruthWithoutNull,
		ast.IsTruthWithNull,
		ast.IsFalsity,

		// arithmetical functions.
		ast.Plus,
		ast.Minus,
		ast.Mul,
		ast.Div,
		ast.Abs,

		// math functions.
		ast.Ceil,
		ast.Ceiling,
		ast.Floor,
		ast.Sqrt,
		ast.Sign,
		ast.Ln,
		ast.Log,
		ast.Log2,
		ast.Log10,
		ast.Exp,
		ast.Pow,
		// Rust use the llvm math functions, which have different precision with Golang/MySQL(cmath)
		// open the following switchers if we implement them in coprocessor via `cmath`
		// ast.Sin,
		// ast.Asin,
		// ast.Cos,
		// ast.Acos,
		// ast.Tan,
		// ast.Atan,
		// ast.Atan2,
		// ast.Cot,
		ast.Radians,
		ast.Degrees,
		ast.Conv,
		ast.CRC32,

		// control flow functions.
		ast.Case,
		ast.If,
		ast.Ifnull,
		ast.Coalesce,

		// string functions.
		ast.Length,
		ast.BitLength,
		ast.Concat,
		ast.ConcatWS,
		// ast.Locate,
		ast.Replace,
		ast.ASCII,
		ast.Hex,
		ast.Reverse,
		ast.LTrim,
		ast.RTrim,
		// ast.Left,
		ast.Strcmp,
		ast.Space,
		ast.Elt,
		ast.Field,

		// json functions.
		ast.JSONType,
		ast.JSONExtract,
		// FIXME: JSONUnquote is incompatible with Coprocessor
		// ast.JSONUnquote,
		ast.JSONObject,
		ast.JSONArray,
		ast.JSONMerge,
		ast.JSONSet,
		ast.JSONInsert,
		// ast.JSONReplace,
		ast.JSONRemove,
		ast.JSONLength,

		// date functions.
		ast.DateFormat,
		ast.FromDays,
		// ast.ToDays,
		ast.DayOfYear,
		ast.DayOfMonth,
		ast.Year,
		ast.Month,
		// FIXME: the coprocessor cannot keep the same behavior with TiDB in current compute framework
		// ast.Hour,
		// ast.Minute,
		// ast.Second,
		// ast.MicroSecond,
		// ast.DayName,
		ast.PeriodAdd,
		ast.PeriodDiff,
		ast.TimestampDiff,
		ast.DateAdd,
		ast.FromUnixTime,

		// encryption functions.
		ast.MD5,
		ast.SHA1,
		ast.UncompressedLength,

		ast.Cast,

		// misc functions.
		ast.InetNtoa,
		ast.InetAton,
		ast.Inet6Ntoa,
		ast.Inet6Aton,
		ast.IsIPv4,
		ast.IsIPv4Compat,
		ast.IsIPv4Mapped,
		ast.IsIPv6:
		ret = true

	// A special case: Only push down Round by signature
	case ast.Round:
		switch sf.Function.PbCode() {
		case
			tipb.ScalarFuncSig_RoundReal,
			tipb.ScalarFuncSig_RoundInt,
			tipb.ScalarFuncSig_RoundDec:
			ret = true
		}
	case
		ast.Substring,
		ast.Substr:
		switch sf.Function.PbCode() {
		case
			tipb.ScalarFuncSig_Substring2ArgsUTF8,
			tipb.ScalarFuncSig_Substring3ArgsUTF8:
			ret = true
		}
	case ast.Rand:
		switch sf.Function.PbCode() {
		case
			tipb.ScalarFuncSig_RandWithSeedFirstGen:
			ret = true
		}
	}
	if ret {
		switch storeType {
		case kv.TiFlash:
			ret = scalarExprSupportedByFlash(sf)
		case kv.TiKV:
			ret = scalarExprSupportedByTiKV(sf)
		case kv.TiDB:
			ret = scalarExprSupportedByTiDB(sf)
		}
	}
	if ret {
		ret = IsPushDownEnabled(sf.FuncName.L, storeType)
	}
	return ret
}

func storeTypeMask(storeType kv.StoreType) uint32 {
	if storeType == kv.UnSpecified {
		return 1<<kv.TiKV | 1<<kv.TiFlash | 1<<kv.TiDB
	}
	return 1 << storeType
}

// IsPushDownEnabled returns true if the input expr is not in the expr_pushdown_blacklist
func IsPushDownEnabled(name string, storeType kv.StoreType) bool {
	value, exists := DefaultExprPushDownBlacklist.Load().(map[string]uint32)[name]
	if exists {
		mask := storeTypeMask(storeType)
		return !(value&mask == mask)
	}

	if storeType != kv.TiFlash && name == ast.AggFuncApproxCountDistinct {
		// Can not push down approx_count_distinct to other store except tiflash by now.
		return false
	}

	return true
}

// DefaultExprPushDownBlacklist indicates the expressions which can not be pushed down to TiKV.
var DefaultExprPushDownBlacklist *atomic.Value

func init() {
	DefaultExprPushDownBlacklist = new(atomic.Value)
	DefaultExprPushDownBlacklist.Store(make(map[string]uint32))
}

func canScalarFuncPushDown(scalarFunc *ScalarFunction, pc PbConverter, storeType kv.StoreType) bool {
	pbCode := scalarFunc.Function.PbCode()
	if pbCode <= tipb.ScalarFuncSig_Unspecified {
		failpoint.Inject("PanicIfPbCodeUnspecified", func() {
			panic(errors.Errorf("unspecified PbCode: %T", scalarFunc.Function))
		})
		return false
	}

	// Check whether this function can be pushed.
	if !canFuncBePushed(scalarFunc, storeType) {
		return false
	}

	// Check whether all of its parameters can be pushed.
	for _, arg := range scalarFunc.GetArgs() {
		if !canExprPushDown(arg, pc, storeType) {
			return false
		}
	}

	if metadata := scalarFunc.Function.metadata(); metadata != nil {
		var err error
		_, err = proto.Marshal(metadata)
		if err != nil {
			logutil.BgLogger().Error("encode metadata", zap.Any("metadata", metadata), zap.Error(err))
			return false
		}
	}
	return true
}

func canExprPushDown(expr Expression, pc PbConverter, storeType kv.StoreType) bool {
	if storeType == kv.TiFlash && expr.GetType().Tp == mysql.TypeDuration {
		return false
	}
	switch x := expr.(type) {
	case *CorrelatedColumn:
		return pc.conOrCorColToPBExpr(expr) != nil && pc.columnToPBExpr(&x.Column) != nil
	case *Constant:
		return pc.conOrCorColToPBExpr(expr) != nil
	case *Column:
		return pc.columnToPBExpr(x) != nil
	case *ScalarFunction:
		return canScalarFuncPushDown(x, pc, storeType)
	}
	return false
}

// PushDownExprs split the input exprs into pushed and remained, pushed include all the exprs that can be pushed down
func PushDownExprs(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client, storeType kv.StoreType) (pushed []Expression, remained []Expression) {
	pc := PbConverter{sc: sc, client: client}
	for _, expr := range exprs {
		if canExprPushDown(expr, pc, storeType) {
			pushed = append(pushed, expr)
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

// CanExprsPushDown return true if all the expr in exprs can be pushed down
func CanExprsPushDown(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client, storeType kv.StoreType) bool {
	_, remained := PushDownExprs(sc, exprs, client, storeType)
	return len(remained) == 0
}

func scalarExprSupportedByTiKV(function *ScalarFunction) bool {
	switch function.FuncName.L {
	case ast.Substr, ast.Substring, ast.DateAdd, ast.TimestampDiff,
		ast.FromUnixTime:
		return false
	default:
		return true
	}
}

func scalarExprSupportedByTiDB(function *ScalarFunction) bool {
	switch function.FuncName.L {
	case ast.Substr, ast.Substring, ast.DateAdd, ast.TimestampDiff,
		ast.FromUnixTime:
		return false
	default:
		return true
	}
}

func scalarExprSupportedByFlash(function *ScalarFunction) bool {
	switch function.FuncName.L {
	case ast.Plus, ast.Minus, ast.Div, ast.Mul, ast.GE, ast.LE,
		ast.EQ, ast.NE, ast.LT, ast.GT, ast.Ifnull, ast.IsNull,
		ast.Or, ast.In, ast.Mod, ast.And, ast.LogicOr, ast.LogicAnd,
		ast.Like, ast.UnaryNot, ast.Case, ast.Month, ast.Substr,
		ast.Substring, ast.TimestampDiff, ast.DateFormat, ast.FromUnixTime,
		ast.JSONLength, ast.If, ast.BitNeg, ast.Xor:
		return true
	case ast.Cast:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_CastIntAsDecimal:
			return true
		default:
			return false
		}
	case ast.DateAdd:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_AddDateDatetimeInt, tipb.ScalarFuncSig_AddDateStringInt:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

>>>>>>> 0c36203... expression: add new scalar function IsTruthWithNull (#19621)
// wrapWithIsTrue wraps `arg` with istrue function if the return type of expr is not
// type int, otherwise, returns `arg` directly.
// The `keepNull` controls what the istrue function will return when `arg` is null:
// 1. keepNull is true and arg is null, the istrue function returns null.
// 2. keepNull is false and arg is null, the istrue function returns 0.
func wrapWithIsTrue(ctx sessionctx.Context, keepNull bool, arg Expression) (Expression, error) {
	if arg.GetType().EvalType() == types.ETInt {
		return arg, nil
	}
	var fc *isTrueOrFalseFunctionClass
	if keepNull {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithNull, 1, 1}, opcode.IsTruth, keepNull}
	} else {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithoutNull, 1, 1}, opcode.IsTruth, keepNull}
	}
	f, err := fc.getFunction(ctx, []Expression{arg})
	if err != nil {
		return nil, err
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(ast.IsTruthWithoutNull),
		Function: f,
		RetType:  f.getRetTp(),
	}
	if keepNull {
		sf.FuncName = model.NewCIStr(ast.IsTruthWithNull)
	}
	return FoldConstant(sf), nil
}
