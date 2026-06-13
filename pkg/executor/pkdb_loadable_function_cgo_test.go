//go:build cgo
// +build cgo

// Copyright 2026 PingCAP, Inc.

package executor_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestLoadableFunctionMockedCreateSelectDrop(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)
	expression.ResetMockUDFCountersForTest()

	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.InProcedure()
	tk1.MustExec("use test")

	tkNoCurrDB := testkit.NewTestKit(t, store)
	tkNoCurrDB.InProcedure()

	tk1.MustExec("create function udf1 returns integer soname 'udf1.so'")
	tk1.MustQuery("select name, ret, dl, type from mysql.func where name='udf1'").Check(
		testkit.Rows("udf1 2 udf1.so function"),
	)

	tk1.MustQuery("select udf1(1)").Check(testkit.Rows("2"))
	tkNoCurrDB.MustQuery("select udf1(41)").Check(testkit.Rows("42"))

	initCalls, deinitCalls, callCalls := expression.MockUDFCountersForTest()
	require.GreaterOrEqual(t, initCalls, 2)
	require.Equal(t, initCalls, deinitCalls)
	require.GreaterOrEqual(t, callCalls, 2)

	err := tk1.ExecToErr("create function udf1 returns integer soname 'udf1.so'")
	require.ErrorContains(t, err, "[executor:1125]Function 'udf1' already exists")
	tk1.MustExec("create function if not exists udf1 returns integer soname 'udf1.so'")

	// DROP UDF doesn't require a current database.
	tkNoCurrDB.MustExec("drop function udf1")
	// ErrSpDoesNotExist
	tk1.MustGetErrCode("select udf1(1)", 1305)

	tkNoCurrDB.MustExec("drop function if exists udf1")
	tk1.MustQuery("select name from mysql.func where name='udf1'").Check(testkit.Rows())
}

func TestLoadableFunctionMockedErrorCases(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	// soName contains path separator -> ErrUdfNoPaths (1124)
	tk.MustGetErrCode("create function bad_udf returns integer soname '../bad.so'", 1124)

	// mock missing shared library -> ErrCantOpenLibrary (1126)
	tk.MustGetErrCode("create function open_fail returns integer soname 'missing.so'", 1126)

	// mock missing symbol -> ErrCantFindDlEntry (1127)
	tk.MustGetErrCode("create function missing_symbol returns integer soname 'udf1.so'", 1127)

	// name collision with native builtin function -> ErrNativeFctNameCollision (1585)
	tk.MustGetErrCode("create function abs returns integer soname 'udf1.so'", 1585)
	// Special native functions are not in funcs map, but still should be treated as native
	// to match MySQL behavior (e.g. CAST / GET_VAR / internal functions).
	tk.MustGetErrCode("create function `cast` returns integer soname 'udf1.so'", 1585)
	tk.MustGetErrCode("create function getvar returns integer soname 'udf1.so'", 1585)
	tk.MustGetErrCode("create function getprocedurevar returns integer soname 'udf1.so'", 1585)
	tk.MustGetErrCode("create function from_binary returns integer soname 'udf1.so'", 1585)
	tk.MustGetErrCode("create function to_binary returns integer soname 'udf1.so'", 1585)

	tk.MustQuery("select name from mysql.func where name in ('bad_udf', 'open_fail', 'missing_symbol', 'abs', 'cast', 'getvar', 'getprocedurevar', 'from_binary', 'to_binary')").Check(testkit.Rows())
}

func TestLoadableFunctionPluginDirValidation(t *testing.T) {
	restoreConf := config.RestoreFunc()
	t.Cleanup(restoreConf)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.PluginDir = ""
	})
	err := tk.ExecToErr("create function udf_plugin_dir_empty returns integer soname 'udf1.so'")
	require.ErrorContains(t, err, "[expression:1126]")
	require.ErrorContains(t, err, "plugin-dir")
	tk.MustQuery("select name from mysql.func where name='udf_plugin_dir_empty'").Check(testkit.Rows())

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.PluginDir = "relative-plugin-dir"
	})
	err = tk.ExecToErr("create function udf_plugin_dir_relative returns integer soname 'udf1.so'")
	require.ErrorContains(t, err, "[expression:1126]")
	require.ErrorContains(t, err, "absolute")
	tk.MustQuery("select name from mysql.func where name='udf_plugin_dir_relative'").Check(testkit.Rows())
}

func TestLoadableFunctionMockedDropPrivilegeDenied(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()

	tk.MustExec("create function udf_priv returns integer soname 'udf1.so'")

	// Trigger privilege check in dropLoadableFunction (DELETE on mysql.func).
	tk.Session().GetSessionVars().User = &auth.UserIdentity{AuthUsername: "u1", AuthHostname: "h1"}
	mgr := &udfPrivManager{allowInsert: true, allowDelete: false}
	privilege.BindPrivilegeManager(tk.Session(), mgr)

	tk.MustGetErrCode("drop function udf_priv", 1142)
	mgr.allowDelete = true
	tk.MustExec("drop function udf_priv")
}

func TestLoadableFunctionMockedCreatePrivilegeDenied(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()

	// CREATE FUNCTION ... SONAME requires INSERT privilege on mysql.func.
	tk.Session().GetSessionVars().User = &auth.UserIdentity{AuthUsername: "u1", AuthHostname: "h1"}
	mgr := &udfPrivManager{allowInsert: false, allowDelete: true}
	privilege.BindPrivilegeManager(tk.Session(), mgr)

	tk.MustGetErrCode("create function udf_priv_create returns integer soname 'udf1.so'", 1142)

	mgr.allowInsert = true
	tk.MustExec("create function udf_priv_create returns integer soname 'udf1.so'")
	tk.MustExec("drop function udf_priv_create")
}

func TestLoadableFunctionMockedExecutePrivilegeCompatibility(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.InProcedure()
	tkRoot.MustExec("create function udf_exec returns integer soname 'udf1.so'")
	tkRoot.MustExec(`create user 'udf_user'@'%'`)

	tkUser := testkit.NewTestKit(t, store)
	tkUser.InProcedure()
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "udf_user", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	// Match MySQL: loadable UDF invocation is not gated by GRANT EXECUTE ON *.*.
	tkUser.MustQuery("select udf_exec(1)").Check(testkit.Rows("2"))
}

func TestLoadableFunctionMockedSelectOnTable(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("create function udf_tbl returns integer soname 'udf1.so'")

	tk.MustExec("drop table if exists t_udf_tbl")
	tk.MustExec("create table t_udf_tbl (id int)")
	tk.MustExec("insert into t_udf_tbl values (1), (2), (3)")
	tk.MustQuery("select udf_tbl(id) from t_udf_tbl order by id").Check(
		testkit.Rows("2", "3", "4"),
	)
}

func TestLoadableFunctionMockedInitFailure(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	// Create succeeds (no init at load time), but evaluation fails at runtime init().
	tk.MustExec("create function init_fail returns integer soname 'udf1.so'")
	//t.Cleanup(func() { tk.MustExec("drop function if exists init_fail") })
	tk.MustGetErrCode("select init_fail(1)", 1123)
}

func TestLoadableFunctionMockedReloadFromMySQLFunc(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("create function udf_reload returns integer soname 'udf1.so'")
	tk.MustQuery("select udf_reload(1)").Check(testkit.Rows("2"))

	// Simulate a fresh node: clear in-memory registry, then rely on infoschema reload.
	expression.DropLoadableFunction("udf_reload")
	tk.MustGetErrCode("select udf_reload(1)", 1305)

	// A schema diff reload (ActionCreateProcedure/DropProcedure) will trigger reloadLoadableFunctions.
	tk.MustExec("create procedure reload_udf_p() begin select 1; end;")
	tk.MustQuery("select udf_reload(1)").Check(testkit.Rows("2"))
	tk.MustExec("drop procedure reload_udf_p")
	tk.MustExec("drop function udf_reload")
}

func TestLoadableFunctionMockedConcurrentSelect(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create function udf_conc returns integer soname 'udf1.so'")

	const (
		sessionN = 8
		iterN    = 20
	)

	tks := make([]*testkit.TestKit, 0, sessionN)
	for i := 0; i < sessionN; i++ {
		tk := testkit.NewTestKit(t, store)
		tk.InProcedure()
		tk.MustExec("use test")
		tks = append(tks, tk)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, sessionN)
	for i := 0; i < sessionN; i++ {
		tk := tks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterN; j++ {
				rs, err := tk.Exec("select udf_conc(1)")
				if err != nil {
					errCh <- err
					return
				}
				rows, err := session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
				if err != nil {
					errCh <- err
					return
				}
				if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "2" {
					errCh <- fmt.Errorf("unexpected result: %v", rows)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	tk.MustExec("drop function udf_conc")
}

func TestLoadableFunctionMockedCreateVisibleBeforeMySQLFuncRow(t *testing.T) {
	// Regression test: UDF should not become callable before it is persisted in mysql.func
	// and published by schema version update.
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)

	tkCreate := testkit.NewTestKit(t, store)
	tkCreate.InProcedure()
	tkCreate.MustExec("use test")

	tkObserver := testkit.NewTestKit(t, store)
	tkObserver.InProcedure()
	tkObserver.MustExec("use test")

	createDone := make(chan error, 1)
	createFinished := false

	fpPause := "github.com/pingcap/tidb/pkg/ddl/pauseAfterCreateLoadableFunctionRegistered"
	require.NoError(t, failpoint.Enable(fpPause, "pause"))
	paused := true
	t.Cleanup(func() {
		if paused {
			_ = failpoint.Disable(fpPause)
		}
		// Avoid leaking a blocked CREATE FUNCTION goroutine if the test fails early.
		if !createFinished {
			select {
			case <-createDone:
			case <-time.After(10 * time.Second):
			}
		}
		// Best-effort cleanup if DROP FUNCTION is unimplemented in this fork.
		expression.DropLoadableFunction("udf_pause")
		tkObserver.MustExec("delete from mysql.func where name='udf_pause' and type='function'")
	})

	go func() {
		_, err := tkCreate.Exec("create function udf_pause returns integer soname 'udf1.so'")
		createDone <- err
	}()

	// CREATE FUNCTION should be blocked by failpoint pause.
	select {
	case err := <-createDone:
		t.Fatalf("create function finished unexpectedly: %v", err)
	case <-time.After(time.Second):
	}

	// At the pause point, mysql.func has not been updated and the UDF must not be callable.
	tkObserver.MustQuery("select count(*) from mysql.func where name='udf_pause' and type='function'").Check(testkit.Rows("0"))
	tkObserver.MustGetErrCode("select udf_pause(1)", 1305)

	require.NoError(t, failpoint.Disable(fpPause))
	paused = false

	select {
	case err := <-createDone:
		createFinished = true
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting create function to finish")
	}
	tkObserver.MustQuery("select count(*) from mysql.func where name='udf_pause' and type='function'").Check(testkit.Rows("1"))
	tkObserver.MustQuery("select udf_pause(1)").Check(testkit.Rows("2"))
}

func TestLoadableFunctionMockedCreateFailureLeavesMySQLFuncResidue(t *testing.T) {
	// Regression test: when CREATE FUNCTION fails, mysql.func should not keep a residue
	// entry that may be loaded on later infoschema reload.
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	fpFail := "github.com/pingcap/tidb/pkg/ddl/failAfterCreateLoadableFunctionInserted"
	testfailpoint.Enable(t, fpFail, "return(true)")
	t.Cleanup(func() {
		// Best-effort cleanup if DROP FUNCTION is unimplemented in this fork.
		expression.DropLoadableFunction("udf_residue")
		tk.MustExec("delete from mysql.func where name='udf_residue' and type='function'")
	})

	// CREATE FUNCTION fails (simulated by failpoint after INSERT).
	err := tk.ExecToErr("create function udf_residue returns integer soname 'udf1.so'")
	require.Error(t, err)

	// No metadata residue, and it isn't callable.
	tk.MustQuery("select name from mysql.func where name='udf_residue'").Check(testkit.Rows())
	tk.MustGetErrCode("select udf_residue(1)", 1305)

	// Later unrelated DDL should not "resurrect" a ghost UDF.
	tk.MustExec("create procedure reload_udf_residue_p() begin select 1; end;")
	tk.MustGetErrCode("select udf_residue(1)", 1305)
	tk.MustExec("drop procedure reload_udf_residue_p")
}
