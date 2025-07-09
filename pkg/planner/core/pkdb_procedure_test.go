// Copyright 2022-2023 PingCAP, Inc.

package core_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCursorExecute(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	cache := make([]core.NeedCloseCur, 0, 10)
	sctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int key)")
	tk.MustExec("insert into t1 values (1),(2),(3);")
	sctx.GetSessionVars().SetInCallProcedure()
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	resSetCursor := core.NewResetProcedurceCursor("t1", con)
	var id uint
	cache, err := resSetCursor.Execute(ctx, sctx, &id, cache)
	require.False(t, resSetCursor.Hanlable())
	require.ErrorContains(t, err, "Undefined CURSOR: t1")
	require.Equal(t, id, uint(1))
	cursor := core.NewProcedurceCurInfo("t1", "select id from t1 order by id;", con)
	con.Cursors = append(con.Cursors, cursor)
	cache, err = resSetCursor.Execute(ctx, sctx, &id, cache)
	require.False(t, resSetCursor.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(2))
	require.Equal(t, resSetCursor.GetContext(), con)
	require.Equal(t, len(cache), 0)

	openCur := core.NewOpenProcedurceCursor("t1", con)
	cache, err = openCur.Execute(ctx, sctx, &id, cache)
	require.True(t, openCur.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))
	require.Equal(t, len(cache), 1)
	cache, err = openCur.Execute(ctx, sctx, &id, cache)
	require.True(t, openCur.Hanlable())
	require.ErrorContains(t, err, "Cursor is already open")
	require.Equal(t, id, uint(4))
	require.Equal(t, openCur.GetContext(), con)
	require.Equal(t, len(cache), 1)
	var1 := []string{"a1"}
	fetch := core.NewProcedurceFetchInto("t1", con, var1)
	cache, err = fetch.Execute(ctx, sctx, &id, cache)
	require.ErrorContains(t, err, "Unknown system variable")
	require.True(t, fetch.Hanlable())
	tp := types.NewFieldType(mysql.TypeInt24)
	vars1 := variable.NewProcedureVars("a1", tp)
	require.Equal(t, id, uint(5))
	con.Vars = append(con.Vars, vars1)
	cache, err = fetch.Execute(ctx, sctx, &id, cache)
	require.True(t, fetch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(6))

	_, datum, notFind := con.GetProcedureVariable("a1")
	require.False(t, notFind)
	require.Equal(t, datum.GetInt64(), int64(2))

	cache, err = fetch.Execute(ctx, sctx, &id, cache)
	require.True(t, fetch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(7))
	_, datum, notFind = con.GetProcedureVariable("a1")
	require.False(t, notFind)
	require.Equal(t, datum.GetInt64(), int64(3))

	cache, err = fetch.Execute(ctx, sctx, &id, cache)
	require.True(t, fetch.Hanlable())
	require.ErrorContains(t, err, "No data - zero rows fetched, selected, or processed")

	closeCur := core.NewProcedurceCursorClose("t1", con)
	cache, err = closeCur.Execute(ctx, sctx, &id, cache)
	require.True(t, closeCur.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(9))
	require.Equal(t, len(cache), 0)

	_, err = closeCur.Execute(ctx, sctx, &id, cache)
	require.True(t, closeCur.Hanlable())
	require.ErrorContains(t, err, "Cursor is not open")
	require.Equal(t, id, uint(10))
}

func TestJumpExecute(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	cache := make([]core.NeedCloseCur, 0, 10)
	label := &variable.ProcedureLabel{
		Name:       "t1",
		LabelType:  variable.BLOCKLABEL,
		LabelBegin: 3,
		LabelEnd:   8,
	}
	var id uint
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	ctx := context.Background()
	sctx := tk.Session().(sessionctx.Context)
	goToStart := core.NewProcedureGoToStart(10, label)
	require.True(t, goToStart.SetDest(label, 3))
	cache, err := goToStart.Execute(ctx, sctx, &id, cache)
	require.False(t, goToStart.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))
	require.Nil(t, goToStart.GetContext())

	goToEnd := core.NewProcedureGoToEnd(label)
	require.True(t, goToEnd.SetDest(label, 10))
	cache, err = goToEnd.Execute(ctx, sctx, &id, cache)
	require.False(t, goToEnd.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(10))
	require.Nil(t, goToEnd.GetContext())

	saveIP := core.NewProcedureSaveIP(con, uint(20))
	cache, err = saveIP.Execute(ctx, sctx, &id, cache)
	require.False(t, saveIP.Hanlable())
	require.Nil(t, err)
	require.Equal(t, len(con.ProcedureReturn), 1)
	require.Equal(t, saveIP.GetContext(), con)
	require.Equal(t, id, uint(21))

	outputIP := core.NewProcedureOutputIP(con)
	cache, err = outputIP.Execute(ctx, sctx, &id, cache)
	require.False(t, outputIP.Hanlable())
	require.Nil(t, err)
	require.Equal(t, len(con.ProcedureReturn), 0)
	require.Equal(t, outputIP.GetContext(), con)
	require.Equal(t, id, uint(10))

	cache, err = outputIP.Execute(ctx, sctx, &id, cache)
	require.False(t, outputIP.Hanlable())
	require.ErrorContains(t, err, "Insufficient procedureReturn data")

	noNeedSave := core.NewProcedureNoNeedSave(con, uint(50))
	_, err = noNeedSave.Execute(ctx, sctx, &id, cache)
	require.False(t, noNeedSave.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(51))
	require.Equal(t, noNeedSave.GetContext(), con)
}

func TestLogicalJudgmentExecute(t *testing.T) {
	var cache []core.NeedCloseCur
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	tk.Session().GetSessionVars().SetInCallProcedure()
	sctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	label := &variable.ProcedureLabel{
		Name:       "t1",
		LabelType:  variable.BLOCKLABEL,
		LabelBegin: 3,
		LabelEnd:   8,
	}
	label2 := &variable.ProcedureLabel{
		Name:       "t2",
		LabelType:  variable.BLOCKLABEL,
		LabelBegin: 3,
		LabelEnd:   8,
	}
	ifGo := core.NewProcedureIfGo(con, label, "1 in (select id from t1)")
	require.True(t, ifGo.SetErrorDest(label, 10))
	require.False(t, ifGo.SetErrorDest(label2, 12))

	require.True(t, ifGo.SetDest(label, 9))
	require.False(t, ifGo.SetDest(label2, 12))
	var id uint
	//err jump to errorDest
	_, err := ifGo.Execute(ctx, sctx, &id, cache)
	require.True(t, ifGo.Hanlable())
	require.ErrorContains(t, err, "Table 'test.t1' doesn't exist")
	require.Equal(t, id, uint(10))
	// false jump to Dest
	tk.MustExec("create table t1 (id int)")
	cache, err = ifGo.Execute(ctx, sctx, &id, cache)
	require.True(t, ifGo.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(9))
	// true jump to next id.
	tk.MustExec("insert into t1 values (1)")
	_, err = ifGo.Execute(ctx, sctx, &id, cache)
	require.True(t, ifGo.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(10))
}

func TestBaseProcedure(t *testing.T) {
	var cache []core.NeedCloseCur
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	ctx := context.Background()
	tk.Session().GetSessionVars().SetInCallProcedure()
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	sctx := tk.Session().(sessionctx.Context)
	var id uint
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")
	insertBase := core.NewExecuteBaseSQL("insert into t1 value (1)", con)
	require.Equal(t, con, insertBase.GetContext())
	cache, err := insertBase.Execute(ctx, sctx, &id, cache)
	require.True(t, insertBase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(1))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	selectBase := core.NewExecuteBaseSQL("select * from t1", con)
	require.Equal(t, con, selectBase.GetContext())
	cache, err = selectBase.Execute(ctx, sctx, &id, cache)
	require.True(t, selectBase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(2))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	deleteBase := core.NewExecuteBaseSQL("delete from t1", con)
	require.Equal(t, con, deleteBase.GetContext())
	cache, err = deleteBase.Execute(ctx, sctx, &id, cache)
	require.True(t, deleteBase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))
	tk.MustQuery("select * from t1").Check(testkit.Rows())

	//support find local variable
	insertBase = core.NewExecuteBaseSQL("insert into t1 value (a)", con)

	tp := types.NewFieldType(mysql.TypeInt24)
	vars1 := variable.NewProcedureVars("a", tp)
	con.Vars = append(con.Vars, vars1)
	err = core.UpdateVariableVar("a", types.NewDatum(10), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = insertBase.Execute(ctx, sctx, &id, cache)
	require.True(t, insertBase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(4))
	tk.MustQuery("select * from t1").Check(testkit.Rows("10"))

	selectBase = core.NewExecuteBaseSQL("select * from t1 where id = a", con)
	cache, err = selectBase.Execute(ctx, sctx, &id, cache)
	require.True(t, selectBase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(5))
	tk.Res[0].Check(testkit.Rows("10"))
	tk.ClearProcedureRes()

	deleteBase = core.NewExecuteBaseSQL("delete from t1 where id = a", con)
	_, err = deleteBase.Execute(ctx, sctx, &id, cache)
	require.True(t, deleteBase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(6))
	tk.ClearProcedureRes()
}

func TestProcedureCase(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	var cache []core.NeedCloseCur
	tk.InProcedure()
	ctx := context.Background()
	sctx := tk.Session().(sessionctx.Context)
	tk.Session().GetSessionVars().SetInCallProcedure()
	var id uint

	tk.MustExec("use test")
	con := variable.NewProcedureContext(variable.BLOCKLABEL)
	simpleCase := core.NewProcedureSimpleCase(con, false, "select max((select id from t1))", 15, 11, "utf8mb4_bin")
	simpleCase.AddDest(core.NewConditionDest("1", 3))

	simpleCase.AddDest(core.NewConditionDest(" 2", 6))

	simpleCase.AddDest(core.NewConditionDest("3", 7))

	cache, err := simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.ErrorContains(t, err, "Table 'test.t1' doesn't exist")
	require.Equal(t, id, uint(15))

	tk.MustExec("create table t1 (id int key)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.ErrorContains(t, err, "Case not found for CASE statement")
	require.Equal(t, id, uint(15))
	tk.MustExec("insert into t1 values(1)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))

	tk.MustExec("insert into t1 values(2)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.ErrorContains(t, err, "Subquery returns more than 1 row")
	require.Equal(t, id, uint(15))

	tk.MustExec("truncate table t1;")
	tk.MustExec("insert into t1 values(3)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(7))

	tk.MustExec("update t1 set id = 2")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(6))

	tk.MustExec("update t1 set id = 10")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.ErrorContains(t, err, "Case not found for CASE statement")
	require.Equal(t, id, uint(15))

	tk.MustExec("drop table t1")
	simpleCase = core.NewProcedureSimpleCase(con, true, "max((select id from t1))", 15, 11, "utf8mb4_bin")
	simpleCase.AddDest(core.NewConditionDest("1", 3))
	simpleCase.AddDest(core.NewConditionDest("2", 6))
	simpleCase.AddDest(core.NewConditionDest("3", 7))

	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.ErrorContains(t, err, "Table 'test.t1' doesn't exist")
	require.Equal(t, id, uint(15))

	tk.MustExec("create table t1 (id int key)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(11))

	tk.MustExec("insert into t1 values(1)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))

	tk.MustExec("insert into t1 values(2)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.ErrorContains(t, err, "Subquery returns more than 1 row")
	require.Equal(t, id, uint(15))

	tk.MustExec("truncate table t1;")
	tk.MustExec("insert into t1 values(3)")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(7))

	tk.MustExec("update t1 set id = 2")
	cache, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(6))

	tk.MustExec("update t1 set id = 10")
	_, err = simpleCase.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleCase.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(11))
}

func TestProcedureSearchCase(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	var cache []core.NeedCloseCur
	tk.InProcedure()
	ctx := context.Background()
	sctx := tk.Session().(sessionctx.Context)
	tk.Session().GetSessionVars().SetInCallProcedure()
	var id uint

	tk.MustExec("use test")
	con := variable.NewProcedureContext(variable.BLOCKLABEL)

	simpleSearch := core.NewProcedureSearchCase(con, false, 15, 11, "utf8mb4_bin")
	simpleSearch.AddDest(core.NewConditionDest("id > 20", 3))
	simpleSearch.AddDest(core.NewConditionDest("id > 10", 6))
	simpleSearch.AddDest(core.NewConditionDest("id > 5", 7))

	cache, err := simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.ErrorContains(t, err, "Unknown column 'id' in 'field list")
	require.Equal(t, id, uint(15))

	tp := types.NewFieldType(mysql.TypeInt24)
	varInfo := variable.NewProcedureVars("id", tp)
	con.Vars = append(con.Vars, varInfo)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.ErrorContains(t, err, "Case not found for CASE statement")
	require.Equal(t, id, uint(15))

	err = core.UpdateVariableVar("id", types.NewDatum(6), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(7))

	err = core.UpdateVariableVar("id", types.NewDatum(11), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(6))

	err = core.UpdateVariableVar("id", types.NewDatum(21), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))

	simpleSearch = core.NewProcedureSearchCase(con, true, 15, 11, "utf8mb4_bin")
	simpleSearch.AddDest(core.NewConditionDest("id > 20", 3))
	simpleSearch.AddDest(core.NewConditionDest("id > 10", 6))
	simpleSearch.AddDest(core.NewConditionDest("id > 5", 7))

	err = core.UpdateVariableVar("id", types.NewDatum(6), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(7))

	err = core.UpdateVariableVar("id", types.NewDatum(11), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(6))

	err = core.UpdateVariableVar("id", types.NewDatum(21), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))

	err = core.UpdateVariableVar("id", types.NewDatum(1), sctx.GetSessionVars())
	require.Nil(t, err)
	cache, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(11))

	con.Vars = con.Vars[:0]
	tp = types.NewFieldType(mysql.TypeVarString)
	varInfo = variable.NewProcedureVars("id", tp)
	con.Vars = append(con.Vars, varInfo)
	err = core.UpdateVariableVar("id", types.NewDatum(21), sctx.GetSessionVars())
	require.Nil(t, err)
	_, err = simpleSearch.Execute(ctx, sctx, &id, cache)
	require.True(t, simpleSearch.Hanlable())
	require.Nil(t, err)
	require.Equal(t, id, uint(3))
}

func TestProcedureCursorClose(t *testing.T) {
	store := testkit.CreateMockStore(t)
	var id uint
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	cache := make([]core.NeedCloseCur, 0, 10)
	sctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int key)")
	tk.MustExec("insert into t1 values (1),(2),(3);")
	sctx.GetSessionVars().SetInCallProcedure()
	con1 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con2 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con3 := variable.NewProcedureContext(variable.BLOCKLABEL)
	con2.SetRoot(con1)
	con3.SetRoot(con2)
	cursor := core.NewProcedurceCurInfo("t1", "select id from t1 order by id;", con1)
	con1.Cursors = append(con1.Cursors, cursor)
	cursor = core.NewProcedurceCurInfo("t2", "select id from t1 order by id;", con1)
	con1.Cursors = append(con1.Cursors, cursor)
	cursor = core.NewProcedurceCurInfo("t1", "select id from t1 order by id;", con2)
	con2.Cursors = append(con2.Cursors, cursor)
	cursor = core.NewProcedurceCurInfo("t1", "select id from t1 order by id;", con3)
	con3.Cursors = append(con3.Cursors, cursor)
	openCur1 := core.NewOpenProcedurceCursor("t1", con1)
	cache, err := openCur1.Execute(ctx, sctx, &id, cache)
	require.True(t, openCur1.Hanlable())
	require.Nil(t, err)
	require.Equal(t, len(cache), 1)
	openCur2 := core.NewOpenProcedurceCursor("t1", con2)
	cache, err = openCur2.Execute(ctx, sctx, &id, cache)
	require.True(t, openCur2.Hanlable())
	require.Nil(t, err)
	require.Equal(t, len(cache), 2)

	openCur3 := core.NewOpenProcedurceCursor("t1", con3)
	cache, err = openCur3.Execute(ctx, sctx, &id, cache)
	require.True(t, openCur3.Hanlable())
	require.Nil(t, err)
	require.Equal(t, len(cache), 3)
	openCur4 := core.NewOpenProcedurceCursor("t2", con3)
	cache, err = openCur4.Execute(ctx, sctx, &id, cache)
	require.True(t, openCur4.Hanlable())
	require.Nil(t, err)
	require.Equal(t, len(cache), 4)
	closeCur2 := core.NewProcedurceCursorClose("t2", con2)
	cache, err = closeCur2.Execute(ctx, sctx, &id, cache)
	require.True(t, closeCur2.Hanlable())
	require.Nil(t, err)
	require.Equal(t, len(cache), 2)
	cache = core.ReleseAll(cache)
	require.Equal(t, len(cache), 0)
	for _, curs := range con1.Cursors {
		require.False(t, curs.IsOpen())
	}
	for _, curs := range con2.Cursors {
		require.False(t, curs.IsOpen())
	}
	for _, curs := range con3.Cursors {
		require.False(t, curs.IsOpen())
	}
}
