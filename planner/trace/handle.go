package trace

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

type Handle struct {
	RecordCh chan *Record
	Session  sessionctx.Context
}

type Record struct {
	TableID int64
	CETrace []CETraceRecord
}

type CETraceRecord struct {
	Type     string
	Expr     string
	RowCount uint64
}

func NewHandle(ctx sessionctx.Context) *Handle {
	h := &Handle{
		RecordCh: make(chan *Record, 16),
		Session:  ctx,
	}
	return h
}

func (h *Handle) Run(rec *Record) {
	ctx := context.Background()
	sql := "insert into mysql.optimizer_trace value (%?, %?, %?, %?, %?)"
	is := h.Session.GetInfoSchema().(infoschema.InfoSchema)
	tbl, ok := is.TableByID(rec.TableID)
	if !ok {
		logutil.BgLogger().Warn("[CE Trace] Failed to find table in infoschema", zap.Int64("table id", rec.TableID))
	}
	tblInfo := tbl.Meta()
	tableName := tblInfo.Name.O
	dbInfo, ok := is.SchemaByTable(tblInfo)
	if !ok {
		logutil.BgLogger().Warn("[CE Trace] Failed to find db in infoschema", zap.Int64("table id", rec.TableID), zap.String("table name", tableName))
	}
	dbName := dbInfo.Name.O
	exec := h.Session.(sqlexec.RestrictedSQLExecutor)
	for _, CERec := range rec.CETrace {
		stmt, err := exec.ParseWithParams(ctx, sql, CERec.Type, dbName, tableName, CERec.Expr, CERec.RowCount)
		if err != nil {
			logutil.BgLogger().Warn("[CE Trace] Error from ParseWithParams", zap.Error(err))
			continue
		}
		_, _, err = exec.ExecRestrictedStmt(ctx, stmt)
		if err != nil {
			logutil.BgLogger().Warn("[CE Trace] Error from ExecRestrictedStmt", zap.Error(err))
		}
	}
}

func RangesToString(rans []*ranger.Range, colNames []string) string {
	for _, ran := range rans {
		if len(ran.LowVal) != len(ran.HighVal) {
			logutil.BgLogger().Warn("[CE Trace] RangeToString", zap.String("err", "length mismatch"))
			return ""
		}
	}
	var buffer bytes.Buffer
	for i, ran := range rans {
		buffer.WriteString("(")
		for j := range ran.LowVal {
			buffer.WriteString("(")
			lowExclude := false
			if ran.LowExclude && j == len(ran.LowVal)-1 {
				lowExclude = true
			}
			highExclude := false
			if ran.HighExclude && j == len(ran.LowVal)-1 {
				highExclude = true
			}
			buffer.WriteString(RangeSingleColToString(ran.LowVal[j], ran.HighVal[j], lowExclude, highExclude, colNames[j]))
			buffer.WriteString(")")
			if j < len(ran.LowVal)-1 {
				buffer.WriteString(" and ")
			}
		}
		buffer.WriteString(")")
		if i < len(rans)-1 {
			buffer.WriteString(" and ")
		}
	}
	return buffer.String()
}

func RangeSingleColToString(lowVal, highVal types.Datum, lowExclude, highExclude bool, colName string) string {
	// low and high are both special values(null, min not null, max value)
	lowKind := lowVal.Kind()
	highKind := highVal.Kind()
	if (lowKind == types.KindNull || lowKind == types.KindMinNotNull || lowKind == types.KindMaxValue) &&
		(highKind == types.KindNull || highKind == types.KindMinNotNull || highKind == types.KindMaxValue) {
		if lowKind == types.KindNull && highKind == types.KindNull && !lowExclude && !highExclude {
			return colName + " is null"
		}
		if lowKind == types.KindNull && highKind == types.KindMaxValue && !lowExclude {
			return "true"
		}
		if lowKind == types.KindMinNotNull && highKind == types.KindMaxValue {
			return colName + " is not null"
		}
		return "false"
	}

	var buffer bytes.Buffer
	useOR := false
	restoreCtx := format.NewRestoreCtx(0, &buffer)
	// low value part
	if lowKind == types.KindNull {
		buffer.WriteString(colName + " is null")
		useOR = true
	} else if lowKind == types.KindMinNotNull {
		buffer.WriteString("true")
	} else {
		buffer.WriteString(colName)
		if lowExclude {
			buffer.WriteString(" > ")
		} else {
			buffer.WriteString(" >= ")
		}
		lowValExpr := driver.ValueExpr{Datum: lowVal}
		err := lowValExpr.Restore(restoreCtx)
		if err != nil {
			logutil.BgLogger().Warn("[CE Trace] Error when restoring value expr", zap.Error(err))
		}
	}

	if useOR {
		buffer.WriteString(" or ")
	} else {
		buffer.WriteString(" and ")
	}

	// high value part
	if highKind == types.KindMaxValue {
		buffer.WriteString("true")
	} else {
		buffer.WriteString(colName)
		if highExclude {
			buffer.WriteString(" < ")
		} else {
			buffer.WriteString(" <= ")
		}
		highValExpr := driver.ValueExpr{Datum: highVal}
		err := highValExpr.Restore(restoreCtx)
		if err != nil {
			logutil.BgLogger().Warn("[CE Trace] Error when restoring value expr", zap.Error(err))
		}
	}

	return buffer.String()
}

func ExprToString(e expression.Expression) string {
	switch expr := e.(type) {
	case *expression.ScalarFunction:
		var buffer bytes.Buffer
		fmt.Fprintf(&buffer, "`%s`(", expr.FuncName.L)
		switch expr.FuncName.L {
		case ast.Cast:
			for _, arg := range expr.GetArgs() {
				buffer.WriteString(ExprToString(arg))
				buffer.WriteString(", ")
				buffer.WriteString(expr.RetType.String())
			}
		default:
			for i, arg := range expr.GetArgs() {
				buffer.WriteString(ExprToString(arg))
				if i+1 != len(expr.GetArgs()) {
					buffer.WriteString(", ")
				}
			}
		}
		buffer.WriteString(")")
		return buffer.String()
	case *expression.Column:
		return expr.String()
	case *expression.CorrelatedColumn:
		return expr.String()
	case *expression.Constant:
		return expr.String()
	}
	return ""
}
