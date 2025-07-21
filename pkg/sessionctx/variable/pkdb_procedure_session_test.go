// Copyright 2023-2023 PingCAP, Inc.

package variable_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type temp struct {
	name string
}

func (t *temp) GetName() string {
	return t.name
}

func (t *temp) NeedUpdateCache() bool {
	return false
}

func (t *temp) GetRow() chunk.Row {
	return chunk.Row{}
}

func (t *temp) CloseCurs() error {
	return nil
}

func (t *temp) NextRow() {
}

func (t *temp) Reset() {

}

func (t *temp) GetLevel() uint {
	return 0
}

func (t *temp) IsOpen() bool {
	return false
}

func (t *temp) Clone(map[*variable.ProcedureContext]*variable.ProcedureContext) variable.CursorRes {
	return &temp{t.name}
}

func TestProcedureFlag(t *testing.T) {
	v := variable.NewSessionVars(nil)
	v.SetInCallProcedure()
	require.True(t, v.GetCallProcedure())
	v.OutCallProcedure(false)
	require.False(t, v.GetCallProcedure())

	// test recursive call
	v.SetInCallProcedure() // 1
	require.True(t, v.GetCallProcedure())
	v.SetInCallProcedure() // 2
	require.True(t, v.GetCallProcedure())
	v.OutCallProcedure(false) //1
	require.True(t, v.GetCallProcedure())
	v.OutCallProcedure(false) // 0
	require.False(t, v.GetCallProcedure())
	v.SetInCallProcedure() // 1
	require.True(t, v.GetCallProcedure())
	v.SetInCallProcedure() // 2
	require.True(t, v.GetCallProcedure())
	v.SetInCallProcedure() // 3
	require.True(t, v.GetCallProcedure())

	v.SetInCallProcedure() // 4
	require.True(t, v.GetCallProcedure())
	v.OutCallProcedure(false) // 3
	require.True(t, v.GetCallProcedure())
	v.OutCallProcedure(false) // 2
	require.True(t, v.GetCallProcedure())
	v.OutCallProcedure(false) // 1
	require.True(t, v.GetCallProcedure())
	v.OutCallProcedure(false) // 0
	require.False(t, v.GetCallProcedure())

	// test clear cache
	v.SetInCallProcedure() // 1
	require.True(t, v.GetCallProcedure())
	v.ProcedurePlanCache["1"] = 1
	v.ProcedurePlanCache["2"] = 1
	v.ProcedurePlanCache["3"] = 1
	v.OutCallProcedure(false) // 0
	require.False(t, v.GetCallProcedure())
	// The cache is not full and the cache is not cleared
	require.Equal(t, len(v.ProcedurePlanCache), 3)

	variable.StoredProgramCacheSize.Store(5)
	v.SetInCallProcedure() // 1
	require.True(t, v.GetCallProcedure())
	v.ProcedurePlanCache["4"] = 1
	v.ProcedurePlanCache["5"] = 1
	v.ProcedurePlanCache["6"] = 1
	v.ProcedurePlanCache["7"] = 1
	require.Equal(t, len(v.ProcedurePlanCache), 7)
	v.OutCallProcedure(false) // 0
	require.False(t, v.GetCallProcedure())
	// The cache is full, clear the cache
	require.Equal(t, len(v.ProcedurePlanCache), 0)
}

func TestProcedureVars(t *testing.T) {
	fd := types.NewFieldType(mysql.TypeTiny)
	varI := variable.NewProcedureVars("t1", fd)
	require.Equal(t, varI.GetName(), "t1")
	varI = variable.NewProcedureVars("T1", fd)
	//ignore case
	require.Equal(t, varI.GetName(), "t1")
}

func TestProcedureHandle(t *testing.T) {
	base := variable.ProcedureHandleBase{
		Handle:  variable.EXIT,
		Operate: 0,
	}
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	code := &variable.ProcedureHandleCode{
		ProcedureHandleBase: base,
	}
	require.Equal(t, code.GetType(), variable.CODE)
	con.Handles = append(con.Handles, code)

	status := &variable.ProcedureHandleStatus{
		ProcedureHandleBase: base,
	}
	require.Equal(t, status.GetType(), variable.STATUS)
	con.Handles = append(con.Handles, status)

	state := &variable.ProcedureHandleState{
		ProcedureHandleBase: base,
	}
	require.Equal(t, state.GetType(), variable.STATE)
	con.Handles = append(con.Handles, state)
}

func TestProcedureFindLabel(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	//NULL
	require.Nil(t, con.FindLabel("t1", false))
	require.Nil(t, con.FindLabel("t1", true))
	label := &variable.ProcedureLabel{
		Name:      "t2",
		LabelType: variable.BLOCKLABEL,
	}
	con.Label = label
	require.Nil(t, con.FindLabel("t1", true))
	require.Equal(t, con.FindLabel("t2", true), con)
	// block find root
	con2 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con2.SetRoot(con)
	require.Equal(t, con2.FindLabel("t2", true), con)
	con4 := variable.NewProcedureContext(variable.JUMPLABEL)
	con4.SetRoot(con)
	require.Equal(t, con4.FindLabel("t2", true), con)
	con5 := variable.NewProcedureContext(variable.LOOPLABEL)
	con5.SetRoot(con)
	require.Equal(t, con5.FindLabel("t2", true), con)

	// find label end at handler
	con3 := variable.NewProcedureContext(variable.HANDLELABEL)
	con3.SetRoot(con)
	require.Nil(t, con3.FindLabel("t2", true))

	// handler iterate not find block
	require.Nil(t, con.FindLabel("t2", false))
	// handler iterate support loop
	con.Label.LabelType = variable.LOOPLABEL
	require.Equal(t, con.FindLabel("t2", false), con)
}

func TestProcedureCheckLabel(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	require.False(t, con.CheckLabel("t1"))
	label := &variable.ProcedureLabel{
		Name:      "t2",
		LabelType: variable.BLOCKLABEL,
	}
	con.Label = label
	require.False(t, con.CheckLabel("t1"))
	require.True(t, con.CheckLabel("t2"))

	con2 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con2.SetRoot(con)
	require.True(t, con2.CheckLabel("t2"))

	con.Label.LabelType = variable.LOOPLABEL
	require.True(t, con2.CheckLabel("t2"))

	con.Label.LabelType = variable.JUMPLABEL
	require.True(t, con2.CheckLabel("t2"))
	// find label end at handler
	con2.TypeFlag = variable.HANDLELABEL
	require.False(t, con2.CheckLabel("t2"))
}

func TestProcedureResetCur(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	err := con.ResetCur("t1")
	require.ErrorContains(t, err, "Undefined CURSOR")
	curs := &temp{"t1"}
	con.Cursors = append(con.Cursors, curs)
	err = con.ResetCur("t1")
	require.Nil(t, err)

	// not find root
	con1 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con1.SetRoot(con)
	err = con1.ResetCur("t1")
	require.ErrorContains(t, err, "Undefined CURSOR")

	curs1 := &temp{"t2"}
	con.Cursors = append(con.Cursors, curs1)
	err = con.ResetCur("t1")
	require.Nil(t, err)
	err = con.ResetCur("t2")
	require.Nil(t, err)
}

func TestProcedureFindCur(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	_, err := con.FindCurs("t1")
	require.ErrorContains(t, err, "Undefined CURSOR")
	curs := &temp{"t1"}
	con.Cursors = append(con.Cursors, curs)
	cursF, err := con.FindCurs("t1")
	require.Nil(t, err)
	require.Equal(t, cursF, curs)
	curs2 := &temp{"t2"}
	con.Cursors = append(con.Cursors, curs2)
	cursF, err = con.FindCurs("t1")
	require.Nil(t, err)
	require.Equal(t, cursF, curs)
	cursF, err = con.FindCurs("t2")
	require.Nil(t, err)
	require.Equal(t, cursF, curs2)
	// find root
	con1 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con1.SetRoot(con)
	cursF, err = con1.FindCurs("t2")
	require.Nil(t, err)
	require.Equal(t, cursF, curs2)
	con1.TypeFlag = variable.JUMPLABEL
	cursF, err = con1.FindCurs("t2")
	require.Nil(t, err)
	require.Equal(t, cursF, curs2)
	con1.TypeFlag = variable.LOOPLABEL
	cursF, err = con1.FindCurs("t2")
	require.Nil(t, err)
	require.Equal(t, cursF, curs2)
	con1.TypeFlag = variable.HANDLELABEL
	cursF, err = con1.FindCurs("t2")
	require.Nil(t, err)
	require.Equal(t, cursF, curs2)
}

func TestProcedureCheckVar(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	fd := types.NewFieldType(mysql.TypeTiny)
	varI := variable.NewProcedureVars("T1", fd)
	varI1 := variable.NewProcedureVars("T2", fd)
	con.Vars = append(con.Vars, varI)
	con.Vars = append(con.Vars, varI1)
	require.True(t, con.CheckVarName("T1"))
	require.True(t, con.CheckVarName("T2"))

	// not find root
	con2 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con2.SetRoot(con)
	require.False(t, con2.CheckVarName("T1"))
	require.False(t, con2.CheckVarName("T2"))
}

func TestProcedureFindVar(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	fd := types.NewFieldType(mysql.TypeTiny)
	varI := variable.NewProcedureVars("T1", fd)
	varI1 := variable.NewProcedureVars("T2", fd)
	con.Vars = append(con.Vars, varI)
	con.Vars = append(con.Vars, varI1)
	require.True(t, con.FindVarName("t1"))
	require.True(t, con.FindVarName("t2"))

	// find root
	con2 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con2.SetRoot(con)
	require.True(t, con2.FindVarName("t1"))
	require.True(t, con2.FindVarName("t2"))
}

func TestRootInfo(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	con.SetRoot(nil)
	require.Nil(t, con.GetRoot())
	con2 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con.SetRoot(con2)
	require.Equal(t, con.GetRoot(), con2)
}

func TestUpdateVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	sc := tk.Session().(sessionctx.Context)
	sc.GetSessionVars().SetInCallProcedure()
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	err := sc.GetSessionVars().SetProcedureContext(con)
	require.Nil(t, err)
	core.ResetCallStatus(sc)
	d := types.NewStringDatum("sdsfaw")
	err = core.UpdateVariableVar("t1", d, sc.GetSessionVars())
	require.Error(t, err, "Unknown system variable")
	fd := types.NewFieldType(mysql.TypeTiny)
	varI := variable.NewProcedureVars("T1", fd)
	varI1 := variable.NewProcedureVars("T2", fd)
	con.Vars = append(con.Vars, varI)
	con.Vars = append(con.Vars, varI1)
	d = types.NewStringDatum("111111111111111")
	err = core.UpdateVariableVar("t1", d, sc.GetSessionVars())
	require.Error(t, err, "constant 111111111111111 overflows tinyint")
	_, res, _ := con.GetProcedureVariable("t1")
	require.True(t, res.IsNull())

	d = types.NewStringDatum("111")
	err = core.UpdateVariableVar("t1", d, sc.GetSessionVars())
	require.Nil(t, err)
	_, res, _ = con.GetProcedureVariable("t1")
	require.Equal(t, res.GetInt64(), int64(111))

	// err ignore
	//sc.TruncateAsWarning = true
	err = core.UpdateVariableVar("t1", d, sc.GetSessionVars())
	require.Nil(t, err)

	// support find root
	con2 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con.SetRoot(con2)
	d = types.NewStringDatum("112")
	err = core.UpdateVariableVar("t1", d, sc.GetSessionVars())
	require.Nil(t, err)
	_, res, _ = con.GetProcedureVariable("t1")
	require.Equal(t, res.GetInt64(), int64(112))
}

func TestGetVariable(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	_, _, ok := con.GetProcedureVariable("t1")
	require.True(t, ok)
	fd := types.NewFieldType(mysql.TypeTiny)
	varI := variable.NewProcedureVars("T1", fd)
	con.Vars = append(con.Vars, varI)
	vfd, val, ok := con.GetProcedureVariable("t1")
	require.Equal(t, vfd, fd)
	require.True(t, val.IsNull())
	require.False(t, ok)

	con1 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con1.SetRoot(con)
	vfd, val, ok = con1.GetProcedureVariable("t1")
	require.Equal(t, vfd, fd)
	require.True(t, val.IsNull())
	require.False(t, ok)
}

func TestSetProceContext(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	err := vars.SetProcedureContext(con)
	require.Error(t, err, "cannot be used outside a stored procedure")
	vars.SetInCallProcedure()
	err = vars.SetProcedureContext(con)
	require.Nil(t, err)
}

func TestGetSessVar(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	fd := types.NewFieldType(mysql.TypeTiny)
	varI := variable.NewProcedureVars("T1", fd)
	con.Vars = append(con.Vars, varI)
	vars := variable.NewSessionVars(nil)
	vars.SetInCallProcedure()
	vars.SetProcedureContext(con)
	vars.OutCallProcedure(false)
	_, _, ok := vars.GetProcedureVariable("t1")
	require.True(t, ok)
	vars.SetInCallProcedure()
	_, _, ok = vars.GetProcedureVariable("t1")
	require.False(t, ok)
}

func TestFindHandle(t *testing.T) {
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	handlebase := variable.ProcedureHandleBase{}
	code := &variable.ProcedureHandleCode{
		Codes:               1132,
		ProcedureHandleBase: handlebase,
	}
	con.Handles = append(con.Handles, code)
	state := &variable.ProcedureHandleState{
		State:               ast.PROCEDUR_SQLWARNING,
		ProcedureHandleBase: handlebase,
	}
	con.Handles = append(con.Handles, state)
	status := &variable.ProcedureHandleStatus{
		Status:              "5862",
		ProcedureHandleBase: handlebase,
	}
	con.Handles = append(con.Handles, status)
	require.Nil(t, con.TryFindHandle(0, "1111", false))

	require.Equal(t, con.TryFindHandle(1132, "1111", false), &code.ProcedureHandleBase)
	require.Equal(t, con.TryFindHandle(0, "5862", false), &status.ProcedureHandleBase)
	require.Equal(t, con.TryFindHandle(0, "0100", false), &status.ProcedureHandleBase)
	require.Equal(t, con.TryFindHandle(0, "5861", true), &state.ProcedureHandleBase)

	con.Handles = con.Handles[:0]
	con.Handles = append(con.Handles, code)
	con.Handles = append(con.Handles, status)
	require.Nil(t, con.TryFindHandle(0, "1111", true))
	state2 := &variable.ProcedureHandleState{
		State:               ast.PROCEDUR_NOT_FOUND,
		ProcedureHandleBase: handlebase,
	}
	con.Handles = append(con.Handles, state2)
	require.Nil(t, con.TryFindHandle(0, "1111", true))
	require.Equal(t, con.TryFindHandle(0, "0200", false), &state2.ProcedureHandleBase)
	con.Handles = con.Handles[:0]
	con.Handles = append(con.Handles, code)
	con.Handles = append(con.Handles, status)
	state3 := &variable.ProcedureHandleState{
		State:               ast.PROCEDUR_SQLEXCEPTION,
		ProcedureHandleBase: handlebase,
	}
	con.Handles = append(con.Handles, state3)
	require.Nil(t, con.TryFindHandle(0, "0111", false))
	require.Nil(t, con.TryFindHandle(0, "0211", false))
	require.Equal(t, con.TryFindHandle(0, "0300", false), &state3.ProcedureHandleBase)

	// find root
	con1 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con1.SetRoot(con)
	require.Equal(t, con1.TryFindHandle(0, "0300", false), &state3.ProcedureHandleBase)
	con2 := variable.NewProcedureContext(variable.HANDLELABEL)
	con2.SetRoot(con)
	require.Nil(t, con2.TryFindHandle(0, "0300", false))
	con2.SetRoot(con1)
	require.Equal(t, con2.TryFindHandle(0, "0300", false), &state3.ProcedureHandleBase)
}

func TestAddUchangableName(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	require.Nil(t, vars.AddUchangableName("zxz"))
	vars.SetInCallProcedure()
	require.Nil(t, vars.AddUchangableName("zxz"))
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	vars.SetProcedureContext(con)
	require.Nil(t, vars.AddUchangableName("zxz"))
	require.Error(t, vars.AddUpdatableVarName("zxz"))
	vars.OutCallProcedure(false)
	require.Nil(t, vars.AddUpdatableVarName("zxz"))
	require.False(t, vars.CheckUnchangableVariable("zxz", true))
	require.False(t, vars.CheckUnchangableVariable("zxz", false))
	vars.SetInCallProcedure()
	require.True(t, vars.CheckUnchangableVariable("zxz", true))
	require.True(t, vars.CheckUnchangableVariable("zxz", false))
}

func TestAddUpdateName(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	require.Nil(t, vars.AddUpdatableVarName("zxz"))
	vars.SetInCallProcedure()
	require.Nil(t, vars.AddUpdatableVarName("zxz"))
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	vars.SetProcedureContext(con)
	require.Nil(t, vars.AddUpdatableVarName("zxz"))
	require.Error(t, vars.AddUchangableName("zxz"))
	vars.OutCallProcedure(false)
	require.Nil(t, vars.AddUchangableName("zxz"))
}

func TestCopyContext(t *testing.T) {
	contextMap := make(map[*variable.ProcedureContext]*variable.ProcedureContext)
	oldCon := variable.NewProcedureContext(0)
	newCon := oldCon.CopyContext(contextMap)
	require.Equal(t, oldCon, newCon)
	// already cached
	require.Equal(t, 1, len(contextMap))
	// use cache
	newCon1 := oldCon.CopyContext(contextMap)
	require.True(t, newCon1 == newCon)
	require.Equal(t, 1, len(contextMap))
	// recursive copy
	contextMap = make(map[*variable.ProcedureContext]*variable.ProcedureContext)
	oldCon = variable.NewProcedureContext(1)
	oldCon.SetRoot(variable.NewProcedureContext(2))
	newCon = oldCon.CopyContext(contextMap)
	require.Equal(t, 2, len(contextMap))
	require.NotNil(t, newCon.GetRoot())
	newCon1 = oldCon.CopyContext(contextMap)
	require.True(t, newCon1 == newCon)
	require.Equal(t, 2, len(contextMap))
	// recursive copy Used directly by cached objects
	oldCon2 := variable.NewProcedureContext(3)
	oldCon2.SetRoot(oldCon)
	newCon3 := oldCon2.CopyContext(contextMap)
	require.Equal(t, 3, len(contextMap))
	require.True(t, newCon3.GetRoot() == newCon)

	// test copy Vars
	contextMap = make(map[*variable.ProcedureContext]*variable.ProcedureContext)
	oldVarCon := variable.NewProcedureContext(0)
	oldVarCon.Vars = append(oldVarCon.Vars, variable.NewProcedureVars("s1", types.NewFieldType(mysql.TypeString)))
	oldVarCon.Vars = append(oldVarCon.Vars, variable.NewProcedureVars("s2", types.NewFieldType(mysql.TypeString)))
	newVarCon := oldVarCon.CopyContext(contextMap)
	require.Equal(t, oldVarCon.Vars, newVarCon.Vars)
	require.Equal(t, 1, len(contextMap))
	// cached
	newVarCon1 := oldVarCon.CopyContext(contextMap)
	require.True(t, newVarCon1 == newVarCon)
	// test root
	oldVarCon1 := variable.NewProcedureContext(0)
	oldVarCon1.Vars = append(oldVarCon1.Vars, variable.NewProcedureVars("s3", types.NewFieldType(mysql.TypeString)))
	oldVarCon1.Vars = append(oldVarCon1.Vars, variable.NewProcedureVars("s4", types.NewFieldType(mysql.TypeString)))
	oldVarCon1.SetRoot(oldVarCon)
	newVarCon2 := oldVarCon1.CopyContext(contextMap)
	require.Equal(t, newVarCon2, oldVarCon1)
	require.Equal(t, 2, len(contextMap))
	require.True(t, newVarCon2.GetRoot() == newVarCon1)

	// test copy Handles
	contextMap = make(map[*variable.ProcedureContext]*variable.ProcedureContext)
	oldHanldeCon := variable.NewProcedureContext(0)
	baseHanldeBase := variable.NewProcedureContext(variable.HANDLELABEL)
	baseHanldeBase.SetRoot(oldHanldeCon)
	base := variable.ProcedureHandleBase{1, 1, baseHanldeBase}
	status := &variable.ProcedureHandleStatus{"status", base}
	state := &variable.ProcedureHandleState{2, base}
	oldHanldeCon.Handles = append(oldHanldeCon.Handles, status)
	oldHanldeCon.Handles = append(oldHanldeCon.Handles, state)
	code := &variable.ProcedureHandleCode{121, base}
	oldHanldeCon.Handles = append(oldHanldeCon.Handles, code)
	newHanlde := oldHanldeCon.CopyContext(contextMap)
	require.Equal(t, newHanlde, oldHanldeCon)
	require.Equal(t, 2, len(contextMap))
	require.True(t, newHanlde.Handles[0].(*variable.ProcedureHandleStatus).HandleContext == newHanlde.Handles[1].(*variable.ProcedureHandleState).HandleContext)
	require.True(t, newHanlde.Handles[0].(*variable.ProcedureHandleStatus).HandleContext == newHanlde.Handles[2].(*variable.ProcedureHandleCode).HandleContext)

	// test copy curs
	contextMap = make(map[*variable.ProcedureContext]*variable.ProcedureContext)
	oldCurCon := variable.NewProcedureContext(0)
	oldCurCon.Cursors = append(oldCon.Cursors, &temp{"t1"})
	oldCurCon.Cursors = append(oldCon.Cursors, &temp{"t2"})
	newCurCon := oldCurCon.CopyContext(contextMap)
	require.Equal(t, oldCurCon, newCurCon)
	require.Equal(t, 1, len(contextMap))

	// test copy TypeFlag\Label
	contextMap = make(map[*variable.ProcedureContext]*variable.ProcedureContext)
	oldTypeCon := variable.NewProcedureContext(0)
	oldTypeCon.TypeFlag = 3
	oldTypeCon.Label = &variable.ProcedureLabel{"sss", 1, 2, 3}
	newTypeCon := oldTypeCon.CopyContext(contextMap)
	require.Equal(t, newTypeCon, oldTypeCon)
	require.Equal(t, 1, len(contextMap))
}
