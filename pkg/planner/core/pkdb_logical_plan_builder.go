package core

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func tryGetProcedureIntVariable(sessVars *variable.SessionVars, procedureVar *ast.ProcedureVar) (uint64, error) {
	_, d, notFind := sessVars.GetProcedureVariable(procedureVar.Name.L)
	if notFind {
		return 0, plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(procedureVar.Name.O)
	}
	err := sessVars.AddUchangableName(procedureVar.Name.L)
	if err != nil {
		return 0, err
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.AddFlag(mysql.UnsignedFlag)
	d, err = d.ConvertTo(sessVars.StmtCtx.TypeCtx(), tp)
	if err != nil {
		return 0, err
	}
	return d.GetUint64(), nil
}
