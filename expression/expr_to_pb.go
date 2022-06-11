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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	ast "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ExpressionsToPBList converts expressions to tipb.Expr list for new plan.
func ExpressionsToPBList(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client) (pbExpr []*tipb.Expr, err error) {
	pc := PbConverter{client: client, sc: sc}
	for _, expr := range exprs {
		v := pc.ExprToPB(expr)
		if v == nil {
			return nil, ErrInternal.GenWithStack("expression %v cannot be pushed down", expr)
		}
		pbExpr = append(pbExpr, v)
	}
	return
}

// PbConverter supplies methods to convert TiDB expressions to TiPB.
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
		pbExpr := pc.conOrCorColToPBExpr(expr)
		if pbExpr == nil {
			return nil
		}
		return pbExpr
	case *CorrelatedColumn:
		return pc.conOrCorColToPBExpr(expr)
	case *Column:
		return pc.columnToPBExpr(x)
	case *ScalarFunction:
		return pc.scalarFuncToPBExpr(x)
	}
	return nil
}

func (pc PbConverter) conOrCorColToPBExpr(expr Expression) *tipb.Expr {
	ft := expr.GetType()
	d, err := expr.Eval(chunk.Row{})
	if err != nil {
		logutil.BgLogger().Error("eval constant or correlated column", zap.String("expression", expr.ExplainInfo()), zap.Error(err))
		return nil
	}
	tp, val, ok := pc.encodeDatum(ft, d)
	if !ok {
		return nil
	}

	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp, Val: val, FieldType: ToPBFieldType(ft)}
}

func (pc *PbConverter) encodeDatum(ft *types.FieldType, d types.Datum) (tipb.ExprType, []byte, bool) {
	var (
		tp  tipb.ExprType
		val []byte
	)
	switch d.Kind() {
	case types.KindNull:
		tp = tipb.ExprType_Null
	case types.KindInt64:
		tp = tipb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		tp = tipb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString, types.KindBinaryLiteral:
		tp = tipb.ExprType_String
		val = d.GetBytes()
	case types.KindMysqlBit:
		tp = tipb.ExprType_MysqlBit
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
		var err error
		val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if err != nil {
			logutil.BgLogger().Error("encode decimal", zap.Error(err))
			return tp, nil, false
		}
	case types.KindMysqlTime:
		if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, int64(tipb.ExprType_MysqlTime)) {
			tp = tipb.ExprType_MysqlTime
			val, err := codec.EncodeMySQLTime(pc.sc, d.GetMysqlTime(), ft.GetType(), nil)
			if err != nil {
				logutil.BgLogger().Error("encode mysql time", zap.Error(err))
				return tp, nil, false
			}
			return tp, val, true
		}
		return tp, nil, false
	case types.KindMysqlEnum:
		tp = tipb.ExprType_MysqlEnum
		val = codec.EncodeUint(nil, d.GetUint64())
	default:
		return tp, nil, false
	}
	return tp, val, true
}

// ToPBFieldType converts *types.FieldType to *tipb.FieldType.
func ToPBFieldType(ft *types.FieldType) *tipb.FieldType {
	return &tipb.FieldType{
		Tp:      int32(ft.GetType()),
		Flag:    uint32(ft.GetFlag()),
		Flen:    int32(ft.GetFlen()),
		Decimal: int32(ft.GetDecimal()),
		Charset: ft.GetCharset(),
		Collate: collate.CollationToProto(ft.GetCollate()),
		Elems:   ft.GetElems(),
	}
}

// FieldTypeFromPB converts *tipb.FieldType to *types.FieldType.
func FieldTypeFromPB(ft *tipb.FieldType) *types.FieldType {
	ft1 := types.NewFieldTypeBuilder().SetType(byte(ft.Tp)).SetFlag(uint(ft.Flag)).SetFlen(int(ft.Flen)).SetDecimal(int(ft.Decimal)).SetCharset(ft.Charset).SetCollate(collate.ProtoToCollation(ft.Collate)).BuildP()
	ft1.SetElems(ft.Elems)
	return ft1
}

func (pc PbConverter) columnToPBExpr(column *Column) *tipb.Expr {
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	switch column.GetType().GetType() {
	case mysql.TypeBit:
		if !IsPushDownEnabled(ast.TypeStr(column.GetType().GetType()), kv.TiKV) {
			return nil
		}
	case mysql.TypeSet, mysql.TypeGeometry, mysql.TypeUnspecified:
		return nil
	case mysql.TypeEnum:
		if !IsPushDownEnabled("enum", kv.UnSpecified) {
			return nil
		}
	}

	if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, kv.ReqSubTypeBasic) {
		return &tipb.Expr{
			Tp:        tipb.ExprType_ColumnRef,
			Val:       codec.EncodeInt(nil, int64(column.Index)),
			FieldType: ToPBFieldType(column.RetType),
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
	// Check whether this function has ProtoBuf signature.
	pbCode := expr.Function.PbCode()
	if pbCode <= tipb.ScalarFuncSig_Unspecified {
		failpoint.Inject("PanicIfPbCodeUnspecified", func() {
			panic(errors.Errorf("unspecified PbCode: %T", expr.Function))
		})
		return nil
	}

	// Check whether this function can be pushed.
	if !canFuncBePushed(expr, kv.UnSpecified) {
		return nil
	}

	// Check whether all of its parameters can be pushed.
	children := make([]*tipb.Expr, 0, len(expr.GetArgs()))
	for _, arg := range expr.GetArgs() {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}

	var encoded []byte
	if metadata := expr.Function.metadata(); metadata != nil {
		var err error
		encoded, err = proto.Marshal(metadata)
		if err != nil {
			logutil.BgLogger().Error("encode metadata", zap.Any("metadata", metadata), zap.Error(err))
			return nil
		}
	}

	// put collation information into the RetType enforcedly and push it down to TiKV/MockTiKV
	tp := *expr.RetType
	if collate.NewCollationEnabled() {
		_, str1 := expr.CharsetAndCollation()
		tp.SetCollate(str1)
	}

	// Construct expression ProtoBuf.
	return &tipb.Expr{
		Tp:        tipb.ExprType_ScalarFunc,
		Val:       encoded,
		Sig:       pbCode,
		Children:  children,
		FieldType: ToPBFieldType(&tp),
	}
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
