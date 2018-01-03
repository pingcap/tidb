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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
)

// ExpressionsToPB converts expression to tipb.Expr.
func ExpressionsToPB(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client) (pbExpr *tipb.Expr, pushed []Expression, remained []Expression) {
	pc := PbConverter{client: client, sc: sc}
	for _, expr := range exprs {
		v := pc.ExprToPB(expr)
		if v == nil {
			remained = append(remained, expr)
			continue
		}
		pushed = append(pushed, expr)
		if pbExpr == nil {
			pbExpr = v
		} else {
			// Merge multiple converted pb expression into a CNF.
			pbExpr = &tipb.Expr{
				Tp:       tipb.ExprType_And,
				Children: []*tipb.Expr{pbExpr, v},
			}
		}
	}
	return
}

// ExpressionsToPBList converts expressions to tipb.Expr list for new plan.
func ExpressionsToPBList(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client) (pbExpr []*tipb.Expr) {
	pc := PbConverter{client: client, sc: sc}
	for _, expr := range exprs {
		v := pc.ExprToPB(expr)
		pbExpr = append(pbExpr, v)
	}
	return
}

// PbConverter supplys methods to convert TiDB expressions to TiPB.
type PbConverter struct {
	client kv.Client
	sc     *stmtctx.StatementContext
}

// NewPBConverter creates a PbConverter.
func NewPBConverter(client kv.Client, sc *stmtctx.StatementContext) PbConverter {
	return PbConverter{client: client, sc: sc}
}

// ExprToPB converts Expression to TiPB.
func (pc PbConverter) ExprToPB(expr Expression) *tipb.Expr {
	switch x := expr.(type) {
	case *Constant:
		return pc.constantToPBExpr(x)
	case *Column:
		return pc.columnToPBExpr(x)
	case *ScalarFunction:
		return pc.scalarFuncToPBExpr(x)
	}
	return nil
}

func (pc PbConverter) constantToPBExpr(con *Constant) *tipb.Expr {
	var (
		tp  tipb.ExprType
		val []byte
		ft  = con.GetType()
	)
	d, err := con.Eval(nil)
	if err != nil {
		log.Errorf("Fail to eval constant, err: %s", err.Error())
		return nil
	}

	switch d.Kind() {
	case types.KindNull:
		tp = tipb.ExprType_Null
	case types.KindInt64:
		tp = tipb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		tp = tipb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		tp = tipb.ExprType_String
		val = d.GetBytes()
	case types.KindBytes:
		tp = tipb.ExprType_Bytes
		val = d.GetBytes()
	case types.KindFloat32:
		tp = tipb.ExprType_Float32
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		tp = tipb.ExprType_Float64
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		tp = tipb.ExprType_MysqlDuration
		val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		tp = tipb.ExprType_MysqlDecimal
		val = codec.EncodeDecimal(nil, d)
	case types.KindMysqlTime:
		if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, int64(tipb.ExprType_MysqlTime)) {
			tp = tipb.ExprType_MysqlTime
			loc := pc.sc.TimeZone
			t := d.GetMysqlTime()
			if t.Type == mysql.TypeTimestamp && loc != time.UTC {
				err := t.ConvertTimeZone(loc, time.UTC)
				terror.Log(errors.Trace(err))
			}
			v, err := t.ToPackedUint()
			if err != nil {
				log.Errorf("Fail to encode value, err: %s", err.Error())
				return nil
			}
			val = codec.EncodeUint(nil, v)
			return &tipb.Expr{Tp: tp, Val: val, FieldType: toPBFieldType(ft)}
		}
		return nil
	default:
		return nil
	}
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp, Val: val, FieldType: toPBFieldType(ft)}
}

func toPBFieldType(ft *types.FieldType) *tipb.FieldType {
	return &tipb.FieldType{
		Tp:      int32(ft.Tp),
		Flag:    uint32(ft.Flag),
		Flen:    int32(ft.Flen),
		Decimal: int32(ft.Decimal),
		Charset: ft.Charset,
		Collate: collationToProto(ft.Collate),
	}
}

func collationToProto(c string) int32 {
	v, ok := mysql.CollationNames[c]
	if ok {
		return int32(v)
	}
	return int32(mysql.DefaultCollationID)
}

func (pc PbConverter) columnToPBExpr(column *Column) *tipb.Expr {
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	switch column.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry, mysql.TypeUnspecified:
		return nil
	}

	if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, kv.ReqSubTypeBasic) {
		return &tipb.Expr{
			Tp:        tipb.ExprType_ColumnRef,
			Val:       codec.EncodeInt(nil, int64(column.Index)),
			FieldType: toPBFieldType(column.RetType),
		}
	}
	id := column.ID
	// Zero Column ID is not a column from table, can not support for now.
	if id == 0 || id == -1 {
		return nil
	}

	return &tipb.Expr{
		Tp:  tipb.ExprType_ColumnRef,
		Val: codec.EncodeInt(nil, id)}
}

func (pc PbConverter) scalarFuncToPBExpr(expr *ScalarFunction) *tipb.Expr {
	switch expr.FuncName.L {
	case ast.LT, ast.LE, ast.EQ, ast.NE, ast.GE, ast.GT,
		ast.NullEQ:
		return pc.compareOpsToPBExpr(expr)
	case ast.Like:
		return pc.likeToPBExpr(expr)
	case ast.Plus, ast.Minus, ast.Mul, ast.Div:
		return pc.arithmeticalOpsToPBExpr(expr)
	case ast.LogicAnd, ast.LogicOr, ast.UnaryNot, ast.LogicXor:
		return pc.logicalOpsToPBExpr(expr)
	case ast.And, ast.Or, ast.BitNeg, ast.Xor:
		return pc.bitwiseFuncToPBExpr(expr)
	case ast.Case, ast.Coalesce, ast.If, ast.Ifnull, ast.IsNull, ast.IsTruth, ast.IsFalsity, ast.In:
		return pc.builtinFuncToPBExpr(expr)
	case ast.JSONType, ast.JSONExtract, ast.JSONUnquote, ast.JSONValid,
		ast.JSONObject, ast.JSONArray, ast.JSONMerge, ast.JSONSet,
		ast.JSONInsert, ast.JSONReplace, ast.JSONRemove, ast.JSONContains:
		return pc.jsonFuncToPBExpr(expr)
	case ast.DateFormat:
		return pc.dateFuncToPBExpr(expr)
	default:
		return nil
	}
}

func (pc PbConverter) compareOpsToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.LT:
		tp = tipb.ExprType_LT
	case ast.LE:
		tp = tipb.ExprType_LE
	case ast.EQ:
		tp = tipb.ExprType_EQ
	case ast.NE:
		tp = tipb.ExprType_NE
	case ast.GE:
		tp = tipb.ExprType_GE
	case ast.GT:
		tp = tipb.ExprType_GT
	case ast.NullEQ:
		tp = tipb.ExprType_NullEQ
	}
	return pc.convertToPBExpr(expr, tp)
}

func (pc PbConverter) likeToPBExpr(expr *ScalarFunction) *tipb.Expr {
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tipb.ExprType_Like)) {
		return nil
	}
	return pc.convertToPBExpr(expr, tipb.ExprType_Like)
}

func (pc PbConverter) arithmeticalOpsToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.Plus:
		tp = tipb.ExprType_Plus
	case ast.Minus:
		tp = tipb.ExprType_Minus
	case ast.Mul:
		tp = tipb.ExprType_Mul
	case ast.Div:
		tp = tipb.ExprType_Div
	case ast.Mod:
		tp = tipb.ExprType_Mod
	case ast.IntDiv:
		tp = tipb.ExprType_IntDiv
	}
	return pc.convertToPBExpr(expr, tp)
}

func (pc PbConverter) logicalOpsToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.LogicAnd:
		tp = tipb.ExprType_And
	case ast.LogicOr:
		tp = tipb.ExprType_Or
	case ast.LogicXor:
		tp = tipb.ExprType_Xor
	case ast.UnaryNot:
		tp = tipb.ExprType_Not
	}
	return pc.convertToPBExpr(expr, tp)
}

func (pc PbConverter) bitwiseFuncToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.And:
		tp = tipb.ExprType_BitAnd
	case ast.Or:
		tp = tipb.ExprType_BitOr
	case ast.Xor:
		tp = tipb.ExprType_BitXor
	case ast.LeftShift:
		tp = tipb.ExprType_LeftShift
	case ast.RightShift:
		tp = tipb.ExprType_RighShift
	case ast.BitNeg:
		tp = tipb.ExprType_BitNeg
	}
	return pc.convertToPBExpr(expr, tp)
}

func (pc PbConverter) jsonFuncToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp = jsonFunctionNameToPB[expr.FuncName.L]
	return pc.convertToPBExpr(expr, tp)
}

func (pc PbConverter) dateFuncToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.DateFormat:
		tp = tipb.ExprType_DateFormat
	}
	return pc.convertToPBExpr(expr, tp)
}

// GroupByItemToPB converts group by items to pb.
func GroupByItemToPB(sc *stmtctx.StatementContext, client kv.Client, expr Expression) *tipb.ByItem {
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		return nil
	}
	return &tipb.ByItem{Expr: e}
}

// SortByItemToPB converts order by items to pb.
func SortByItemToPB(sc *stmtctx.StatementContext, client kv.Client, expr Expression, desc bool) *tipb.ByItem {
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		return nil
	}
	return &tipb.ByItem{Expr: e, Desc: desc}
}

func (pc PbConverter) builtinFuncToPBExpr(expr *ScalarFunction) *tipb.Expr {
	switch expr.FuncName.L {
	case ast.Case, ast.If, ast.Ifnull, ast.Nullif:
		return pc.controlFuncsToPBExpr(expr)
	case ast.Coalesce, ast.IsNull, ast.In:
		return pc.otherFuncsToPBExpr(expr)
	default:
		return nil
	}
}

func (pc PbConverter) otherFuncsToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.Coalesce:
		tp = tipb.ExprType_Coalesce
	case ast.IsNull:
		tp = tipb.ExprType_IsNull
	case ast.In:
		tp = tipb.ExprType_In
	}
	return pc.convertToPBExpr(expr, tp)
}

func (pc PbConverter) controlFuncsToPBExpr(expr *ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.If:
		tp = tipb.ExprType_If
	case ast.Ifnull:
		tp = tipb.ExprType_IfNull
	case ast.Case:
		tp = tipb.ExprType_Case
	case ast.Nullif:
		tp = tipb.ExprType_NullIf
	}
	return pc.convertToPBExpr(expr, tp)
}

func (pc PbConverter) convertToPBExpr(expr *ScalarFunction, tp tipb.ExprType) *tipb.Expr {
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	children := make([]*tipb.Expr, 0, len(expr.GetArgs()))
	for _, arg := range expr.GetArgs() {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, kv.ReqSubTypeSignature) {
		code := expr.Function.PbCode()
		if code > 0 {
			return &tipb.Expr{Tp: tipb.ExprType_ScalarFunc, Sig: code, Children: children, FieldType: toPBFieldType(expr.RetType)}
		}
		return nil
	}
	return &tipb.Expr{Tp: tp, Children: children}
}
