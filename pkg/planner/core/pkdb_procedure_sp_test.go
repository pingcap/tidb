// Copyright 2022-2023 PingCAP, Inc.

package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestNewVariableVars(t *testing.T) {
	// variable test
	builder, _ := NewPlanBuilder().Init(MockContext(), nil, &hint.QBHintHandler{})
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	tp := types.NewFieldType(mysql.TypeInt24)
	decl := &ast.ProcedureDecl{DeclNames: []string{"t1"}, DeclType: tp}
	err := builder.newVariableVars(context.Background(), "", con, decl)
	require.Nil(t, err)
	require.Equal(t, len(con.Vars), 1)
	resVar := variable.NewProcedureVars("t1", tp)
	require.Equal(t, *con.Vars[0], *resVar)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 1)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[0].(*UpdateVariables), UpdateVariables{"t1", "", con, tp})
	oldPlan := &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan := oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)

	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]
	con = variable.NewProcedureContext(variable.BLOCKLABEL)
	decl = &ast.ProcedureDecl{DeclNames: []string{"t1", "t2", "t3"}, DeclType: tp}
	err = builder.newVariableVars(context.Background(), "", con, decl)
	require.Nil(t, err)
	require.Equal(t, len(con.Vars), 3)
	resVar2 := variable.NewProcedureVars("t2", tp)
	require.Equal(t, *con.Vars[0], *resVar)
	require.Equal(t, *con.Vars[1], *resVar2)
	resVar3 := variable.NewProcedureVars("t3", tp)
	require.Equal(t, *con.Vars[2], *resVar3)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 3)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[0].(*UpdateVariables), UpdateVariables{"t1", "", con, tp})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[1].(*UpdateVariables), UpdateVariables{"t2", "", con, tp})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[2].(*UpdateVariables), UpdateVariables{"t3", "", con, tp})
	// support copy
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[0].(*UpdateVariables).context == newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*UpdateVariables).context)
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[0].(*UpdateVariables).context == newPlan.ProcedureExecPlan.ProcedureCommandList[2].(*UpdateVariables).context)

	//unsupport repeat name
	decl = &ast.ProcedureDecl{DeclNames: []string{"t1"}, DeclType: tp}
	builder.procedureNowContext = con
	err = builder.newVariableVars(context.Background(), "", con, decl)
	require.Error(t, err)

	// support add variable name
	decl = &ast.ProcedureDecl{DeclNames: []string{"t4"}, DeclType: tp}
	err = builder.newVariableVars(context.Background(), "", con, decl)
	require.Nil(t, err)
	require.Equal(t, len(con.Vars), 4)

	// Different level support the same name
	con1 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con1.SetRoot(con)
	builder.procedureNowContext = con1
	err = builder.newVariableVars(context.Background(), "", con1, decl)
	require.Nil(t, err)
	require.Equal(t, len(con1.Vars), 1)

	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	require.NotNil(t, newPlan.ProcedureExecPlan.ProcedureCommandList[4].(*UpdateVariables).context.GetRoot())
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[4].(*UpdateVariables).context.GetRoot() == newPlan.ProcedureExecPlan.ProcedureCommandList[3].(*UpdateVariables).context)

	// cursor test
	curs := &ast.ProcedureCursor{
		CurName: "t1", Selectstring: &ast.SelectStmt{},
	}
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]
	con = variable.NewProcedureContext(variable.BLOCKLABEL)
	builder.procedureNowContext = con
	err = builder.newVariableVars(context.Background(), "", con, curs)
	require.Nil(t, err)
	require.Equal(t, len(con.Cursors), 1)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 1)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[0].(*ResetProcedurceCursor), ResetProcedurceCursor{curName: "t1", context: con})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)

	// can add cursor
	curs1 := &ast.ProcedureCursor{
		CurName: "t2", Selectstring: &ast.SelectStmt{},
	}
	err = builder.newVariableVars(context.Background(), "", con, curs1)
	require.Nil(t, err)
	require.Equal(t, len(con.Cursors), 2)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 2)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[1].(*ResetProcedurceCursor), ResetProcedurceCursor{curName: "t2", context: con})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[0].(*ResetProcedurceCursor).context ==
		newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*ResetProcedurceCursor).context)

	// unsupport repeat name
	err = builder.newVariableVars(context.Background(), "", con, curs)
	require.Error(t, err)

	// Different level support the same name
	con1 = variable.NewProcedureContext(variable.BLOCKLABEL)
	con1.SetRoot(con)
	builder.procedureNowContext = con1
	err = builder.newVariableVars(context.Background(), "", con1, curs)
	require.Nil(t, err)
	require.Equal(t, len(con1.Cursors), 1)
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[0].(*ResetProcedurceCursor).context ==
		newPlan.ProcedureExecPlan.ProcedureCommandList[2].(*ResetProcedurceCursor).context.GetRoot())

	// test handler
	con = variable.NewProcedureContext(variable.BLOCKLABEL)
	builder.procedureNowContext = con
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]
	numCode := &ast.ProcedureErrorVal{ErrorNum: 1000}
	handlerInfo := &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_CONTINUE,
		ErrorCon:      []ast.ErrNode{numCode},
		Operate:       &ast.SelectStmt{},
	}
	handlerInfo.Operate.SetText(nil, "select 1;")
	err = builder.newVariableVars(context.Background(), "", con, handlerInfo)
	require.Nil(t, err)
	require.Equal(t, len(con.Handles), 1)
	hanldercon := builder.procedurePlan.ProcedureCommandList[1].(*ProcedureSaveIP).context
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[0].(*ProcedureGoToEnd), ProcedureGoToEnd{label: &variable.ProcedureLabel{LabelType: variable.HANDLELABEL, LabelBegin: 0, LabelEnd: 4}, dest: 4})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[1].(*ProcedureSaveIP), ProcedureSaveIP{context: hanldercon, dest: 1})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[2].(*executeBaseSQL), executeBaseSQL{context: hanldercon, cacheStmt: &CacheAst{sql: "select 1;", isInvalid: false, stmts: []ast.StmtNode{handlerInfo.Operate}}})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[3].(*ProcedureOutputIP), ProcedureOutputIP{context: hanldercon})
	base := variable.ProcedureHandleBase{
		Handle:        ast.PROCEDUR_CONTINUE,
		Operate:       1,
		HandleContext: hanldercon,
	}
	require.Equal(t, con.Handles[0], &variable.ProcedureHandleCode{Codes: 1000, ProcedureHandleBase: base})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	// NeedSet not finished yet, ignore comparison
	hanldercon.NeedSet = nil
	require.Equal(t, newPlan, oldPlan)
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*ProcedureSaveIP).context ==
		newPlan.ProcedureExecPlan.ProcedureCommandList[2].(*executeBaseSQL).context)
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*ProcedureSaveIP).context ==
		newPlan.ProcedureExecPlan.ProcedureCommandList[3].(*ProcedureOutputIP).context)

	// can add more code
	numCode2 := &ast.ProcedureErrorVal{ErrorNum: 2000}
	handlerInfo2 := &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_CONTINUE,
		ErrorCon:      []ast.ErrNode{numCode2},
		Operate:       &ast.SelectStmt{},
	}
	err = builder.newVariableVars(context.Background(), "", con, handlerInfo2)
	require.Nil(t, err)
	require.Equal(t, len(con.Handles), 2)
	require.Equal(t, con.Handles[1].(*variable.ProcedureHandleCode).Operate, uint(5))
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 8)
	hanldercon2 := builder.procedurePlan.ProcedureCommandList[5].(*ProcedureSaveIP).context
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	// NeedSet not finished yet, ignore comparison
	hanldercon2.NeedSet = nil
	require.Equal(t, newPlan, oldPlan)
	require.NotNil(t, newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*ProcedureSaveIP).context.GetRoot())
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*ProcedureSaveIP).context.GetRoot() ==
		newPlan.ProcedureExecPlan.ProcedureCommandList[5].(*ProcedureSaveIP).context.GetRoot())

	// unsupport repeat name
	err = builder.newVariableVars(context.Background(), "", con, handlerInfo)
	require.Error(t, err)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// handler error condition
	errCon := &ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_NOT_FOUND}
	handlerErrorCon := &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_CONTINUE,
		ErrorCon:      []ast.ErrNode{errCon},
		Operate:       &ast.SelectStmt{},
	}
	err = builder.newVariableVars(context.Background(), "", con, handlerErrorCon)
	require.Nil(t, err)
	require.Equal(t, len(con.Handles), 3)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	err = builder.newVariableVars(context.Background(), "", con, handlerErrorCon)
	require.Error(t, err)

	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// handler sqlstate
	errState := &ast.ProcedureErrorState{CodeStatus: "01242"}
	handlerErrorState := &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_CONTINUE,
		ErrorCon:      []ast.ErrNode{errState},
		Operate:       &ast.SelectStmt{},
	}
	err = builder.newVariableVars(context.Background(), "", con, handlerErrorState)
	require.Nil(t, err)
	require.Equal(t, len(con.Handles), 4)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	hanldercon2 = builder.procedurePlan.ProcedureCommandList[1].(*ProcedureSaveIP).context
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	// NeedSet not finished yet, ignore comparison
	hanldercon2.NeedSet = nil
	require.NotNil(t, newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*ProcedureSaveIP).context.GetRoot())
	require.True(t, newPlan.ProcedureExecPlan.ProcedureCommandList[1].(*ProcedureSaveIP).context ==
		newPlan.ProcedureExecPlan.ProcedureCommandList[2].(*executeBaseSQL).context)

	err = builder.newVariableVars(context.Background(), "", con, handlerErrorState)
	require.Error(t, err)

	//handler sqlstate check
	errState1 := &ast.ProcedureErrorState{CodeStatus: "0124"}
	handlerErrorState1 := &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_CONTINUE,
		ErrorCon:      []ast.ErrNode{errState1},
		Operate:       &ast.SelectStmt{},
	}
	err = builder.newVariableVars(context.Background(), "", con, handlerErrorState1)
	require.Error(t, err)
	errState1.CodeStatus = "00241"
	err = builder.newVariableVars(context.Background(), "", con, handlerErrorState1)
	require.Error(t, err)
	errState1.CodeStatus = "a0241"
	err = builder.newVariableVars(context.Background(), "", con, handlerErrorState1)
	require.Error(t, err)

	// support multicode
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]
	con = variable.NewProcedureContext(variable.BLOCKLABEL)
	mutiHandlerError := &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_CONTINUE,
		ErrorCon:      []ast.ErrNode{numCode2, errState, errCon, numCode},
		Operate:       &ast.SelectStmt{},
	}
	err = builder.newVariableVars(context.Background(), "", con, mutiHandlerError)
	require.Nil(t, err)
	require.Equal(t, len(con.Handles), 4)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	con.NeedSet = nil
	hanldercon2 = builder.procedurePlan.ProcedureCommandList[1].(*ProcedureSaveIP).context
	hanldercon2.NeedSet = nil
	require.Equal(t, newPlan, oldPlan)

	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	builder.procedureGoSet = builder.procedureGoSet[:0]

	builder.procedureGoSet = append(builder.procedureGoSet, &variable.ProcedureLabel{LabelType: variable.BLOCKLABEL})
	con = variable.NewProcedureContext(variable.BLOCKLABEL)
	numCode2 = &ast.ProcedureErrorVal{ErrorNum: 2000}
	handlerInfo2 = &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_EXIT,
		ErrorCon:      []ast.ErrNode{numCode2},
		Operate:       &ast.SelectStmt{},
	}
	handlerInfo2.Operate.SetText(nil, "select 1;")
	err = builder.newVariableVars(context.Background(), "", con, handlerInfo2)
	require.Nil(t, err)
	require.Equal(t, len(con.Handles), 1)
	hanldercon = builder.procedurePlan.ProcedureCommandList[2].(*executeBaseSQL).context
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[0].(*ProcedureGoToEnd), ProcedureGoToEnd{label: &variable.ProcedureLabel{LabelType: variable.HANDLELABEL, LabelBegin: 0, LabelEnd: 4}, dest: 4})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[1].(*ProcedureNoNeedSave), ProcedureNoNeedSave{dest: 1, context: builder.procedurePlan.ProcedureCommandList[1].(*ProcedureNoNeedSave).GetContext()})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[2].(*executeBaseSQL), executeBaseSQL{context: hanldercon, cacheStmt: &CacheAst{sql: "select 1;", isInvalid: false, stmts: []ast.StmtNode{handlerInfo.Operate}}})
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[3].(*ProcedureGoToEndWithOutStmt), ProcedureGoToEndWithOutStmt{label: &variable.ProcedureLabel{LabelType: variable.BLOCKLABEL}, context: builder.procedurePlan.ProcedureCommandList[3].GetContext()})
	base = variable.ProcedureHandleBase{
		Handle:        ast.PROCEDUR_EXIT,
		Operate:       1,
		HandleContext: hanldercon,
	}
	require.Equal(t, con.Handles[0], &variable.ProcedureHandleCode{Codes: 2000, ProcedureHandleBase: base})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	con.NeedSet = nil
	hanldercon2 = builder.procedurePlan.ProcedureCommandList[2].(*executeBaseSQL).context
	hanldercon2.NeedSet = nil
	require.Equal(t, newPlan, oldPlan)
}

func TestProcedureNodePlan(t *testing.T) {
	ctx := context.Background()
	// base block test
	block := &ast.ProcedureBlock{}
	sctx := MockContext()
	builder, _ := NewPlanBuilder().Init(sctx, nil, &hint.QBHintHandler{})
	err := builder.procedureNodePlan(ctx, block, "")
	require.Nil(t, err)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 1)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.IsType(t, &ProcedureClearBlockVar{}, builder.procedurePlan.ProcedureCommandList[0])
	// add handler test
	numCode := &ast.ProcedureErrorVal{ErrorNum: 2000}
	handlerInfo := &ast.ProcedureErrorControl{
		ControlHandle: ast.PROCEDUR_EXIT,
		ErrorCon:      []ast.ErrNode{numCode},
		Operate:       &ast.SelectStmt{},
	}
	block.ProcedureVars = append(block.ProcedureVars, handlerInfo)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]
	err = builder.procedureNodePlan(ctx, block, "")
	require.Nil(t, err)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 5)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[4].(*ProcedureGoToEndWithOutStmt), ProcedureGoToEndWithOutStmt{label: &variable.ProcedureLabel{LabelType: variable.BLOCKLABEL, LabelEnd: 5}, dest: 5, context: builder.procedurePlan.ProcedureCommandList[4].GetContext()})
	require.Equal(t, len(builder.procedureGoSet), 0)
	oldPlan := &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan := oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[3].(*executeBaseSQL).context.NeedSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// add select test
	block.ProcedureProcStmts = append(block.ProcedureProcStmts, &ast.SelectStmt{})
	block.ProcedureProcStmts = append(block.ProcedureProcStmts, &ast.SelectStmt{})
	err = builder.procedureNodePlan(ctx, block, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 7)
	require.Equal(t, *builder.procedurePlan.ProcedureCommandList[4].(*ProcedureGoToEndWithOutStmt), ProcedureGoToEndWithOutStmt{label: &variable.ProcedureLabel{LabelType: variable.BLOCKLABEL, LabelEnd: 7}, dest: 7, context: builder.procedurePlan.ProcedureCommandList[4].GetContext()})
	require.Equal(t, len(builder.procedureGoSet), 0)
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[3].(*executeBaseSQL).context.NeedSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// lableBlock test
	lableBlock := &ast.ProcedureLabelBlock{LabelName: "t1", Block: block}
	err = builder.procedureNodePlan(ctx, lableBlock, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 7)
	blockCon := builder.procedurePlan.ProcedureCommandList[0].GetContext()
	require.Equal(t, blockCon.GetRoot().Label.Name, "t1")
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[3].(*executeBaseSQL).context.NeedSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// lableBlock error
	lableBlock.LabelError = true
	lableBlock.LabelEnd = "t2"
	err = builder.procedureNodePlan(ctx, lableBlock, "")
	require.ErrorContains(t, err, "t2")

	ifBlock := &ast.ProcedureIfBlock{IfExpr: &ast.FuncCallExpr{}, ProcedureIfStmts: []ast.StmtNode{&ast.SelectStmt{}}}
	ifLevel := &ast.ProcedureIfInfo{}
	err = builder.procedureNodePlan(ctx, ifLevel, "")
	require.ErrorContains(t, err, "tidb procdure if block is nil")
	builder.procedureNowContext = nil
	builder.procedureGoSet = builder.procedureGoSet[:0]
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 0)
	// add if block
	ifLevel.IfBody = ifBlock
	err = builder.procedureNodePlan(ctx, ifLevel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 2)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(2))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// add else
	ifBlock.ProcedureElseStmt = &ast.ProcedureElseBlock{
		ProcedureIfStmts: []ast.StmtNode{&ast.SelectStmt{}},
	}
	err = builder.procedureNodePlan(ctx, ifLevel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(3))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(4))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[3], &executeBaseSQL{})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	//add elseif
	ifBlock1 := &ast.ProcedureIfBlock{IfExpr: &ast.FuncCallExpr{}, ProcedureIfStmts: []ast.StmtNode{&ast.SelectStmt{}}}
	ifBlock.ProcedureElseStmt = ifBlock1
	err = builder.procedureNodePlan(ctx, ifLevel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 5)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(3))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(5))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[3].(*ProcedureIfGo).dest, uint(5))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &executeBaseSQL{})
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	//add elseif else
	ifBlock1.ProcedureElseStmt = &ast.ProcedureElseBlock{
		ProcedureIfStmts: []ast.StmtNode{&ast.SelectStmt{}},
	}

	ifBlock.ProcedureElseStmt = ifBlock1
	err = builder.procedureNodePlan(ctx, ifLevel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 7)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(3))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(7))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[3].(*ProcedureIfGo).dest, uint(6))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[5].(*ProcedureGoToEnd).dest, uint(7))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[6], &executeBaseSQL{})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// if ... elseif ... elseif ... else
	ifBlock2 := &ast.ProcedureIfBlock{IfExpr: &ast.FuncCallExpr{}, ProcedureIfStmts: []ast.StmtNode{&ast.SelectStmt{}}}
	ifBlock1.ProcedureElseStmt = ifBlock2
	ifBlock2.ProcedureElseStmt = &ast.ProcedureElseBlock{
		ProcedureIfStmts: []ast.StmtNode{&ast.SelectStmt{}},
	}
	err = builder.procedureNodePlan(ctx, ifLevel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 10)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(3))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(10))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[3].(*ProcedureIfGo).dest, uint(6))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[5].(*ProcedureGoToEnd).dest, uint(10))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[6].(*ProcedureIfGo).dest, uint(9))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[7], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[8].(*ProcedureGoToEnd).dest, uint(10))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[9], &executeBaseSQL{})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// while
	whileBlock := &ast.ProcedureWhileStmt{
		Condition: &ast.FuncCallExpr{},
		Body:      []ast.StmtNode{&ast.SelectStmt{}},
	}
	err = builder.procedureNodePlan(ctx, whileBlock, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 3)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(3))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).errorDest, uint(3))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToStart).dest, uint(0))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// Repeat
	repeatBlock := &ast.ProcedureRepeatStmt{
		Condition: &ast.FuncCallExpr{},
		Body:      []ast.StmtNode{&ast.SelectStmt{}},
	}
	err = builder.procedureNodePlan(ctx, repeatBlock, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 2)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureIfGo).dest, uint(0))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureIfGo).errorDest, uint(2))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// Loop
	loopBlock := &ast.ProcedureLoopStmt{
		Body: []ast.StmtNode{&ast.SelectStmt{}},
	}
	err = builder.procedureNodePlan(ctx, loopBlock, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 2)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureGoToStart).dest, uint(0))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// loop label
	loopLabel := &ast.ProcedureLabelLoop{
		LabelName: "t1",
		Block:     repeatBlock,
	}

	err = builder.procedureNodePlan(ctx, loopLabel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 2)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureIfGo).dest, uint(0))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureIfGo).errorDest, uint(2))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureIfGo).context.GetRoot().Label.Name, "t1")
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureIfGo).context.GetRoot().Label.LabelBegin, uint(0))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[1].(*ProcedureIfGo).context.GetRoot().Label.LabelEnd, uint(2))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	loopLabel.Block = whileBlock
	err = builder.procedureNodePlan(ctx, loopLabel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 3)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(3))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).errorDest, uint(3))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToStart).dest, uint(0))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.GetRoot().Label.Name, "t1")
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.GetRoot().Label.LabelBegin, uint(0))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.GetRoot().Label.LabelEnd, uint(3))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	//jump test
	// misfind label
	jumplabel := &ast.ProcedureJump{
		Name: "t2",
	}
	whileBlock.Body = append(whileBlock.Body, jumplabel)
	err = builder.procedureNodePlan(ctx, loopLabel, "")
	require.ErrorContains(t, err, "no matching label")
	builder.procedureGoSet = builder.procedureGoSet[:0]
	builder.procedureNowContext = nil
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	jumplabel.Name = "t1"
	err = builder.procedureNodePlan(ctx, loopLabel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(4))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).errorDest, uint(4))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[2], &ProcedureGoToStart{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[3].(*ProcedureGoToStart).dest, uint(0))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToStart).dest, uint(0))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	jumplabel.IsLeave = true
	err = builder.procedureNodePlan(ctx, loopLabel, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 4)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).dest, uint(4))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).errorDest, uint(4))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[2], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[3].(*ProcedureGoToStart).dest, uint(0))
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(4))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.GetRoot().NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureIfGo).context.GetRoot().ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[1].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	// block loop
	lableBlock.LabelError = false
	lableBlock.Block.ProcedureProcStmts = append(lableBlock.Block.ProcedureProcStmts, jumplabel)
	err = builder.procedureNodePlan(ctx, lableBlock, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 8)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[7].(*ProcedureGoToEnd).dest, uint(8))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.GetRoot().NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.GetRoot().ErrorSet = nil
	builder.procedurePlan.ProcedureCommandList[3].(*executeBaseSQL).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[3].(*executeBaseSQL).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	jumplabel.IsLeave = false
	err = builder.procedureNodePlan(ctx, lableBlock, "")
	require.ErrorContains(t, err, "no matching label")
	builder.procedureGoSet = builder.procedureGoSet[:0]
	builder.procedureNowContext = nil
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	block = &ast.ProcedureBlock{}
	cursor := &ast.ProcedureCursor{CurName: "t1", Selectstring: &ast.SelectStmt{}}
	openCur := &ast.ProcedureOpenCur{CurName: "t1"}
	closeCur := &ast.ProcedureCloseCur{CurName: "t1"}
	fetchInto := &ast.ProcedureFetchInto{CurName: "t1", Variables: []string{"t1"}}
	tp := types.NewFieldType(mysql.TypeInt24)
	decl := &ast.ProcedureDecl{DeclNames: []string{"t1"}, DeclType: tp}
	block.ProcedureVars = append(block.ProcedureVars, decl)
	block.ProcedureVars = append(block.ProcedureVars, cursor)
	block.ProcedureProcStmts = append(block.ProcedureProcStmts, openCur)
	block.ProcedureProcStmts = append(block.ProcedureProcStmts, fetchInto)
	block.ProcedureProcStmts = append(block.ProcedureProcStmts, closeCur)
	err = builder.procedureNodePlan(ctx, block, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 6)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &ProcedureClearBlockVar{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &UpdateVariables{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[2], &ResetProcedurceCursor{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[3], &OpenProcedurceCursor{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &ProcedurceFetchInto{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[5], &ProcedurceCursorClose{})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.NeedSet = nil
	builder.procedurePlan.ProcedureCommandList[0].(*ProcedureClearBlockVar).context.ErrorSet = nil
	require.Equal(t, newPlan, oldPlan)

	openCur.CurName = "t2"
	err = builder.procedureNodePlan(ctx, block, "")
	require.ErrorContains(t, err, "Undefined CURSOR: t2")
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]
	builder.procedureNowContext = nil
	builder.procedureGoSet = builder.procedureGoSet[:0]

	//test simple case
	simpleCase := &ast.SimpleCaseStmt{
		Condition: &ast.CaseExpr{},
		WhenCases: []*ast.SimpleWhenThenStmt{{Expr: &ast.CaseExpr{}, ProcedureStmts: []ast.StmtNode{&ast.SelectStmt{}}},
			{Expr: &ast.CaseExpr{}, ProcedureStmts: []ast.StmtNode{&ast.SelectStmt{}}}},
	}
	err = builder.procedureNodePlan(ctx, simpleCase, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 5)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &procedureSimpleCase{})
	require.False(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSimpleCase).hasElse)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSimpleCase).errDest, uint(5))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[2], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(5))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[3], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[4].(*ProcedureGoToEnd).dest, uint(5))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	simpleCase.ElseCases = []ast.StmtNode{&ast.SelectStmt{}, &ast.SelectStmt{}}
	err = builder.procedureNodePlan(ctx, simpleCase, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 7)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &procedureSimpleCase{})
	require.True(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSimpleCase).hasElse)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSimpleCase).errDest, uint(7))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[2], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(7))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[3], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[4].(*ProcedureGoToEnd).dest, uint(7))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[5], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[6], &executeBaseSQL{})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	searchCase := &ast.SearchCaseStmt{
		WhenCases: []*ast.SearchWhenThenStmt{{Expr: &ast.CaseExpr{}, ProcedureStmts: []ast.StmtNode{&ast.SelectStmt{}}},
			{Expr: &ast.CaseExpr{}, ProcedureStmts: []ast.StmtNode{&ast.SelectStmt{}}}},
	}

	err = builder.procedureNodePlan(ctx, searchCase, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 5)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &procedureSearchCase{})
	require.False(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSearchCase).hasElse)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSearchCase).errDest, uint(5))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[2], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(5))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[3], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[4].(*ProcedureGoToEnd).dest, uint(5))
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]

	searchCase.ElseCases = []ast.StmtNode{&ast.SelectStmt{}, &ast.SelectStmt{}}
	err = builder.procedureNodePlan(ctx, searchCase, "")
	require.Nil(t, err)
	require.Nil(t, builder.procedureNowContext)
	require.Equal(t, len(builder.procedureGoSet), 0)
	require.Equal(t, len(builder.procedurePlan.ProcedureCommandList), 7)
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[0], &procedureSearchCase{})
	require.True(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSearchCase).hasElse)
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[0].(*procedureSearchCase).errDest, uint(7))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[1], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[2], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[2].(*ProcedureGoToEnd).dest, uint(7))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[3], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[4], &ProcedureGoToEnd{})
	require.Equal(t, builder.procedurePlan.ProcedureCommandList[4].(*ProcedureGoToEnd).dest, uint(7))
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[5], &executeBaseSQL{})
	require.IsType(t, builder.procedurePlan.ProcedureCommandList[6], &executeBaseSQL{})
	oldPlan = &ProcedurePlan{
		ProcedureExecPlan: builder.procedurePlan,
	}
	newPlan = oldPlan.deepCopy()
	require.Equal(t, newPlan, oldPlan)
	builder.procedurePlan.ProcedureCommandList = builder.procedurePlan.ProcedureCommandList[:0]
}
