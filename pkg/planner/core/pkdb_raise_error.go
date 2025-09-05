package core

import (
	"context"
	"errors"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func signalCheck(node *ast.Signal) (string, error) {
	var SQLState string
	tags := make([]int, ast.TILASTPROPERTY+1)
	switch x := node.ErrorCon.(type) {
	case *ast.ProcedureErrorState:
		SQLState = x.CodeStatus
		if !IsSqlstateValid(x.CodeStatus) || IsSQLStateCompletion(x.CodeStatus) {
			return SQLState, plannererrors.ErrSpBadSQLstate.GenWithStackByArgs(x.CodeStatus)
		}
	default:
		return SQLState, errors.New("unsupport signal condition value")
	}

	for _, SignalInf := range node.SignalCons {
		if tags[SignalInf.Name] == 1 {
			name, err := ast.GetSignalString(SignalInf.Name)
			if err != nil {
				return SQLState, err
			}
			return SQLState, plannererrors.ErrDupSignalSet.GenWithStackByArgs(name)
		}
		tags[SignalInf.Name] = 1
	}
	return SQLState, nil
}

func (b *PlanBuilder) buildSignal(ctx context.Context, node *ast.Signal) (base.Plan, error) {
	var p Signal
	var err error
	p.SQLState, err = signalCheck(node)
	if err != nil {
		return &p, err
	}

	if node.SignalCons == nil {
		return &p, nil
	}
	SignalInfos := make([]*SignalInfo, 0, len(node.SignalCons))
	for _, SignalInf := range node.SignalCons {
		mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
		expr, _, err := b.rewrite(ctx, SignalInf.Value, mockTablePlan, nil, true, nil)
		if err != nil {
			return &p, err
		}
		SignalInfos = append(SignalInfos, &SignalInfo{SignalInf.Name, expr})
	}
	p.SignalCons = SignalInfos
	return &p, nil
}

func (b *PlanBuilder) buildGetDiagnostics(ctx context.Context, node *ast.GetDiagnosticsStmt) (base.Plan, error) {
	var p GetDiagnostics
	p.Area = node.Area
	for _, con := range node.Infors {
		switch v := con.(type) {
		case *ast.StatementInfoItem:
			p.Statements = append(p.Statements, &DiagnosticsStatement{v.Name, v.IsVariable, v.Condition})
		case *ast.DiagnosticsConds:
			mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
			expr, _, err := b.rewrite(ctx, v.Num, mockTablePlan, nil, true, nil)
			if err != nil {
				b.ctx.GetSessionVars().StmtCtx.AppendError(err)
				return &p, nil
			}
			dia := &DiagnosticsCondition{ConditionID: expr}
			dia.Cons = make([]*DiagnosticsStatement, 0, len(v.Conds))
			for _, con := range v.Conds {
				dia.Cons = append(dia.Cons, &DiagnosticsStatement{con.Name, con.IsVariable, con.Condition})
			}
			p.Con = dia
		default:
			return &p, plannererrors.ErrUnsupportedType.GenWithStack("Unsupported type %T", con)
		}
	}
	if p.Statements != nil && p.Con != nil {
		return &p, errors.New("statement_information_item and condition_information_item cannot exist at the same time")
	}
	return &p, nil
}
