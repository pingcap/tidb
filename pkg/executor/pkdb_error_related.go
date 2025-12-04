package executor

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

const (
	// MAXDEFFENTLEN diagnostics area maximum length except message text.
	MAXDEFFENTLEN = 64
	// MAXMESSAGELEN message text maximum length
	MAXMESSAGELEN = 128
	// MAXMYSQLERRNO max mysql error num.
	MAXMYSQLERRNO = math.MaxUint16
)

// SignalExec signal operator.
type SignalExec struct {
	exec.BaseExecutor
	done       bool
	SQLState   string
	SignalCons []*plannercore.SignalInfo
}

func (b *executorBuilder) buildSignalExec(v *plannercore.Signal) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &SignalExec{
		BaseExecutor: base,
		done:         false,
		SQLState:     v.SQLState,
		SignalCons:   v.SignalCons,
	}
	return e
}

func getConditionValue(ctx sessionctx.Context, name string, col *model.ColumnInfo, val *types.Datum) (*types.Datum, error) {
	warningspre := ctx.GetSessionVars().StmtCtx.WarningCount()
	varVar, err := table.CastValue(ctx, *val, col, false, false)
	if err != nil {
		varVar.SetNull()
		originErr := errors.Cause(err)
		if err1, ok := originErr.(*errors.Error); ok {
			if err1.Code() == mysql.ErrDataTooLong {
				return &varVar, exeerrors.ErrCondItemTooLong.GenWithStackByArgs(name)
			}
		}
		return &varVar, err
	}
	warningsend := ctx.GetSessionVars().StmtCtx.WarningCount()
	if warningsend > warningspre {
		ret := ctx.GetSessionVars().StmtCtx.TruncateWarnings(int(warningspre))
		for _, lastErr := range ret {
			if err1, ok := lastErr.Err.(*errors.Error); ok &&
				err1.Code() == mysql.ErrDataTooLong {
				ctx.GetSessionVars().StmtCtx.AppendWarning(exeerrors.WarnCondItemTruncated.GenWithStackByArgs(name))
			} else {
				ctx.GetSessionVars().StmtCtx.AppendWarning(lastErr.Err)
			}
		}
	}
	return &varVar, nil
}

// Next Implement signal function
func (e *SignalExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	defer func() { e.done = true }()
	var signalErr terror.TiDBError
	signalErr.SQLSTATE = e.SQLState

	// https://dev.mysql.com/doc/refman/8.0/en/signal.html#signal-condition-information-items:~:text=error%20occurred.-,Signal%20Condition%20Information%20Items,-The%20following%20table
	switch e.SQLState[:2] {
	case "01":
		signalErr.MYSQLERRNO = mysql.ErrSignalWarn
		signalErr.MESSAGETEXT = mysql.MySQLErrName[mysql.ErrSignalWarn].Raw
	case "02":
		signalErr.MYSQLERRNO = mysql.ErrSignalNotFound
		signalErr.MESSAGETEXT = mysql.MySQLErrName[mysql.ErrSignalNotFound].Raw
	default:
		signalErr.MYSQLERRNO = mysql.ErrSignalException
		signalErr.MESSAGETEXT = mysql.MySQLErrName[mysql.ErrSignalException].Raw
	}
	for _, signalCon := range e.SignalCons {
		var val types.Datum
		var err error
		var code int64
		name, err := ast.GetSignalString(signalCon.Name)
		if err != nil {
			return err
		}

		val, err = signalCon.Expr.Eval(e.Ctx().GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			return err
		}
		if val.IsNull() {
			return exeerrors.ErrWrongValueForVar.GenWithStackByArgs(name, "NULL")
		}

		col := &model.ColumnInfo{Name: pmodel.NewCIStr(name), FieldType: *types.NewFieldType(mysql.TypeVarString)}
		// gets signal information item value
		switch signalCon.Name {
		case ast.TICLASSORIGIN:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.CLASSORIGIN = varVar.GetString()
		case ast.TISUBCLASSORIGIN:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.SUBCLASSORIGIN = varVar.GetString()
		case ast.TICONSTRAINTCATALOG:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.CONSTRAINTCATALOG = varVar.GetString()
		case ast.TICONSTRAINTSCHEMA:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.CONSTRAINTSCHEMA = varVar.GetString()
		case ast.TICONSTRAINTNAME:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.CONSTRAINTNAME = varVar.GetString()
		case ast.TICATALOGNAME:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.CATALOGNAME = varVar.GetString()
		case ast.TISCHEMANAME:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.SCHEMANAME = varVar.GetString()
		case ast.TITABLENAME:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.TABLENAME = varVar.GetString()
		case ast.TICOLUMNNAME:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.COLUMNNAME = varVar.GetString()
		case ast.TICURSORNAME:
			col.FieldType.SetFlen(MAXDEFFENTLEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.CURSORNAME = varVar.GetString()
		case ast.TIMESSAGETEXT:
			col.FieldType.SetFlen(MAXMESSAGELEN)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				return err
			}
			signalErr.MESSAGETEXT = varVar.GetString()
		case ast.TIMYSQLERRNO:
			col.FieldType = *types.NewFieldType(mysql.TypeLonglong)
			varVar, err := getConditionValue(e.Ctx(), name, col, &val)
			if err != nil {
				if err1, ok := err.(*errors.Error); ok {
					if err1.Code() == mysql.ErrDataOutOfRange {
						code, _, _ := signalCon.Expr.EvalString(e.Ctx().GetExprCtx().GetEvalCtx(), chunk.Row{})
						return exeerrors.ErrWrongValueForVar.GenWithStackByArgs("MYSQL_ERRNO", code)
					}
				}
				return err
			}
			code = varVar.GetInt64()
			if code <= 0 || code > MAXMYSQLERRNO {
				code, _, _ := signalCon.Expr.EvalString(e.Ctx().GetExprCtx().GetEvalCtx(), chunk.Row{})
				return exeerrors.ErrWrongValueForVar.GenWithStackByArgs("MYSQL_ERRNO", code)
			}
			signalErr.MYSQLERRNO = code
		default:
			return errors.Errorf("unspport condition_information_item_name")
		}
	}
	if e.SQLState[:2] == "01" {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(&signalErr)
		return nil
	}
	return &signalErr
}

// GetDiagnosticsExec gets error/warnings operator.
type GetDiagnosticsExec struct {
	exec.BaseExecutor
	done       bool
	Area       int
	Statements []*plannercore.DiagnosticsStatement
	Con        *plannercore.DiagnosticsCondition
}

func (b *executorBuilder) buildGetDiagnosticsExec(v *plannercore.GetDiagnostics) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &GetDiagnosticsExec{
		BaseExecutor: base,
		done:         false,
		Area:         v.Area,
		Statements:   v.Statements,
		Con:          v.Con,
	}
	return e
}

// https://dev.mysql.com/doc/refman/8.0/en/diagnostics-area.html#:~:text=condition%20information%20items%20...-,Diagnostics%20Area%20Information%20Items,-The%20diagnostics%20area
func getErrorClassorigin(sqlstate string) string {
	if ((sqlstate[0] >= '0' && sqlstate[0] <= '4') || (sqlstate[0] >= 'A' && sqlstate[0] <= 'H')) &&
		((sqlstate[1] >= '0' && sqlstate[1] <= '9') || (sqlstate[1] >= 'A' && sqlstate[1] <= 'Z')) {
		return "ISO 9075"
	}
	return "TIDB"
}

// gets results from TiDBError
func (e *GetDiagnosticsExec) updateVarsByDiagnosticsInfo(v *terror.TiDBError) error {
	for _, con := range e.Con.Cons {
		var d types.Datum
		var f *types.FieldType
		switch con.Con {
		case ast.TICLASSORIGIN:
			d = types.NewDatum(v.CLASSORIGIN)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TISUBCLASSORIGIN:
			d = types.NewDatum(v.SUBCLASSORIGIN)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TICONSTRAINTCATALOG:
			d = types.NewDatum(v.CONSTRAINTCATALOG)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TICONSTRAINTSCHEMA:
			d = types.NewDatum(v.CONSTRAINTSCHEMA)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TICONSTRAINTNAME:
			d = types.NewDatum(v.CONSTRAINTNAME)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TICATALOGNAME:
			d = types.NewDatum(v.CATALOGNAME)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TISCHEMANAME:
			d = types.NewDatum(v.SCHEMANAME)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TITABLENAME:
			d = types.NewDatum(v.TABLENAME)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TICOLUMNNAME:
			d = types.NewDatum(v.COLUMNNAME)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TICURSORNAME:
			d = types.NewDatum(v.CURSORNAME)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TIMESSAGETEXT:
			d = types.NewDatum(v.MESSAGETEXT)
			f = types.NewFieldType(mysql.TypeString)
		case ast.TIMYSQLERRNO:
			d = types.NewDatum(v.MYSQLERRNO)
			f = types.NewFieldType(mysql.TypeLonglong)
		case ast.TIRETURNEDSQLSTATE:
			d = types.NewDatum(v.SQLSTATE)
			f = types.NewFieldType(mysql.TypeString)
		default:
			return errors.Errorf("Unspport Diagnostics condition information item name %d", con.Con)
		}
		err := con.UpdateVariable(e.Ctx(), f, d)
		if err != nil {
			return err
		}
	}
	return nil
}

// Next Implement gets diagnostics function
func (e *GetDiagnosticsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done {
		return nil
	}
	defer func() { e.done = true }()
	var data *stmtctx.BackupStmtCtx
	switch e.Area {
	case ast.TICURRENT:
		data = e.Ctx().GetSessionVars().StmtCtx.BackupForHandler()
	case ast.TISTACKED:
		data = e.Ctx().GetSessionVars().GetLastBackupStmtCtx()
		if data == nil {
			return exeerrors.ErrGetStackedDaWithoutActiveHandler
		}
	default:
		return errors.Errorf("Unspport Diagnostics area %d", e.Area)
	}
	if e.Statements != nil {
		for _, statement := range e.Statements {
			var res int64
			switch statement.Con {
			case ast.TINUMBER:
				res = int64(len(data.Warnings))
			case ast.TIROWCOUNT:
				res = data.AffectedRows
			default:
				return errors.Errorf("Unspport Diagnostics statement information item name %d", e.Area)
			}
			d := types.NewDatum(res)
			f := types.NewFieldType(mysql.TypeLonglong)
			err := statement.UpdateVariable(e.Ctx(), f, d)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if e.Con != nil {
		val, err := e.Con.ConditionID.Eval(e.Ctx().GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			return err
		}
		col := &model.ColumnInfo{Name: pmodel.NewCIStr(""), FieldType: *types.NewFieldType(mysql.TypeLong)}
		varVar, err := table.CastValue(e.Ctx(), val, col, false, false)
		if err != nil {
			varVar.SetNull()
		}
		if varVar.IsNull() || int64(len(data.Warnings)) < varVar.GetInt64() || varVar.GetInt64() <= 0 {
			e.Ctx().GetSessionVars().StmtCtx.AppendError(exeerrors.ErrDaInvalidConditionNumber)
			return nil
		}
		readErr := data.Warnings[varVar.GetInt64()-1]
		var m *mysql.SQLError
		originErr := errors.Cause(readErr.Err)
		switch v := originErr.(type) {
		case *terror.Error:
			m = terror.ToSQLError(v)
			errCode := int(m.Code)
			errStatus, ok := mysql.MySQLState[uint16(errCode)]
			if !ok {
				errStatus = mysql.DefaultMySQLState
			}
			classorigin := getErrorClassorigin(mysql.DefaultMySQLState)
			f := &terror.TiDBError{CLASSORIGIN: classorigin, SUBCLASSORIGIN: classorigin, MESSAGETEXT: m.Message, MYSQLERRNO: int64(m.Code), SQLSTATE: errStatus}

			err = e.updateVarsByDiagnosticsInfo(f)
			if err != nil {
				return err
			}
			return nil
		case *terror.TiDBError:
			err = e.updateVarsByDiagnosticsInfo(v)
			if err != nil {
				return err
			}
			return nil
		default:
			classorigin := getErrorClassorigin(mysql.DefaultMySQLState)
			f := &terror.TiDBError{CLASSORIGIN: classorigin, SUBCLASSORIGIN: classorigin, MESSAGETEXT: originErr.Error(), MYSQLERRNO: int64(mysql.ErrUnknown), SQLSTATE: mysql.DefaultMySQLState}
			err = e.updateVarsByDiagnosticsInfo(f)
			if err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}
