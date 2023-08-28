// Copyright 2021 PingCAP, Inc.
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

package session

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/telemetry"
	"github.com/stretchr/testify/require"
)

// This test file have many problem.
// 1. Please use testkit to create dom, session and store.
// 2. Don't use CreateStoreAndBootstrap and BootstrapSession together. It will cause data race.
// Please do not add any test here. You can add test case at the bootstrap_update_test.go. After All problem fixed,
// We will overwrite this file by update_test.go.
func TestBootstrap(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "set global tidb_txn_mode=''")
	MustExec(t, se, "use mysql")
	r := MustExecToRecodeSet(t, se, "select * from user")
	require.NotNil(t, r)

	ctx := context.Background()
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())

	rows := statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", nil, nil, nil, "", "N", time.Now(), nil)
	r.Close()

	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "anyhost"}, []byte(""), []byte(""), nil))

	MustExec(t, se, "use test")

	// Check privilege tables.
	MustExec(t, se, "SELECT * from mysql.global_priv")
	MustExec(t, se, "SELECT * from mysql.db")
	MustExec(t, se, "SELECT * from mysql.tables_priv")
	MustExec(t, se, "SELECT * from mysql.columns_priv")
	MustExec(t, se, "SELECT * from mysql.global_grants")

	// Check privilege tables.
	r = MustExecToRecodeSet(t, se, "SELECT COUNT(*) from mysql.global_variables")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, globalVarsCount(), req.GetRow(0).GetInt64(0))
	require.NoError(t, r.Close())

	// Check a storage operations are default autocommit after the second start.
	MustExec(t, se, "USE test")
	MustExec(t, se, "drop table if exists t")
	MustExec(t, se, "create table t (id int)")
	unsetStoreBootstrapped(store.UUID())
	se.Close()

	se, err = CreateSession4Test(store)
	require.NoError(t, err)
	MustExec(t, se, "USE test")
	MustExec(t, se, "insert t values (?)", 3)

	se, err = CreateSession4Test(store)
	require.NoError(t, err)
	MustExec(t, se, "USE test")
	r = MustExecToRecodeSet(t, se, "select * from t")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	rows = statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, 3)
	MustExec(t, se, "drop table if exists t")
	se.Close()

	// Try to do bootstrap dml jobs on an already bootstrapped TiDB system will not cause fatal.
	// For https://github.com/pingcap/tidb/issues/1096
	se, err = CreateSession4Test(store)
	require.NoError(t, err)
	doDMLWorks(se)
	r = MustExecToRecodeSet(t, se, "select * from mysql.expr_pushdown_blacklist where name = 'date_add'")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	se.Close()
}

func globalVarsCount() int64 {
	var count int64
	for _, v := range variable.GetSysVars() {
		if v.HasGlobalScope() {
			count++
		}
	}
	return count
}

// testBootstrapWithError :
// When a session failed in bootstrap process (for example, the session is killed after doDDLWorks()).
// We should make sure that the following session could finish the bootstrap process.
func TestBootstrapWithError(t *testing.T) {
	ctx := context.Background()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// bootstrap
	{
		se := &session{
			store:       store,
			sessionVars: variable.NewSessionVars(nil),
		}
		se.functionUsageMu.builtinFunctionUsage = make(telemetry.BuiltinFunctionsUsage)
		se.txn.init()
		se.mu.values = make(map[fmt.Stringer]interface{})
		se.SetValue(sessionctx.Initing, true)
		err := InitDDLJobTables(store, meta.BaseDDLTableVersion)
		require.NoError(t, err)
		err = InitMDLTable(store)
		require.NoError(t, err)
		err = InitDDLJobTables(store, meta.BackfillTableVersion)
		require.NoError(t, err)
		dom, err := domap.Get(store)
		require.NoError(t, err)
		domain.BindDomain(se, dom)
		b, err := checkBootstrapped(se)
		require.False(t, b)
		require.NoError(t, err)
		doDDLWorks(se)
	}

	dom, err := domap.Get(store)
	require.NoError(t, err)
	dom.Close()

	dom1, err := BootstrapSession(store)
	require.NoError(t, err)
	defer dom1.Close()

	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "USE mysql")
	r := MustExecToRecodeSet(t, se, `select * from user`)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())

	row := req.GetRow(0)
	rows := statistics.RowToDatums(row, r.Fields())
	match(t, rows, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", nil, nil, nil, "", "N", time.Now(), nil)
	require.NoError(t, r.Close())

	MustExec(t, se, "USE test")
	// Check privilege tables.
	MustExec(t, se, "SELECT * from mysql.global_priv")
	MustExec(t, se, "SELECT * from mysql.db")
	MustExec(t, se, "SELECT * from mysql.tables_priv")
	MustExec(t, se, "SELECT * from mysql.columns_priv")
	// Check role tables.
	MustExec(t, se, "SELECT * from mysql.role_edges")
	MustExec(t, se, "SELECT * from mysql.default_roles")
	// Check global variables.
	r = MustExecToRecodeSet(t, se, "SELECT COUNT(*) from mysql.global_variables")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	v := req.GetRow(0)
	require.Equal(t, globalVarsCount(), v.GetInt64(0))
	require.NoError(t, r.Close())

	r = MustExecToRecodeSet(t, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="bootstrapped"`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, []byte("True"), row.GetBytes(0))
	require.NoError(t, r.Close())

	MustExec(t, se, "SELECT * from mysql.tidb_background_subtask")
	MustExec(t, se, "SELECT * from mysql.tidb_background_subtask_history")

	// Check tidb_ttl_table_status table
	MustExec(t, se, "SELECT * from mysql.tidb_ttl_table_status")
}

func TestDDLTableCreateBackfillTable(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	ver, err := m.CheckDDLTableVersion()
	require.NoError(t, err)
	require.GreaterOrEqual(t, ver, meta.BackfillTableVersion)

	// downgrade `mDDLTableVersion`
	m.SetDDLTables(meta.MDLTableVersion)
	MustExec(t, se, "drop table mysql.tidb_background_subtask")
	MustExec(t, se, "drop table mysql.tidb_background_subtask_history")
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// to upgrade session for create ddl related tables
	dom.Close()
	dom, err = BootstrapSession(store)
	require.NoError(t, err)

	se = CreateSessionAndSetID(t, store)
	MustExec(t, se, "select * from mysql.tidb_background_subtask")
	MustExec(t, se, "select * from mysql.tidb_background_subtask_history")
	dom.Close()
}

// TestUpgrade tests upgrading
func TestUpgrade(t *testing.T) {
	ctx := context.Background()

	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := CreateSessionAndSetID(t, store)

	MustExec(t, se, "USE mysql")

	// bootstrap with currentBootstrapVersion
	r := MustExecToRecodeSet(t, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	row := req.GetRow(0)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	require.Equal(t, 1, row.Len())
	require.Equal(t, []byte(fmt.Sprintf("%d", currentBootstrapVersion)), row.GetBytes(0))
	require.NoError(t, r.Close())

	se1 := CreateSessionAndSetID(t, store)
	ver, err := getBootstrapVersion(se1)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// Do something to downgrade the store.
	// downgrade meta bootstrap version
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(1))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, se1, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	MustExec(t, se1, fmt.Sprintf(`delete from mysql.global_variables where VARIABLE_NAME="%s"`, variable.TiDBDistSQLScanConcurrency))
	MustExec(t, se1, `commit`)
	unsetStoreBootstrapped(store.UUID())
	// Make sure the version is downgraded.
	r = MustExecToRecodeSet(t, se1, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	require.NoError(t, r.Close())

	ver, err = getBootstrapVersion(se1)
	require.NoError(t, err)
	require.Equal(t, int64(0), ver)
	dom.Close()
	// Create a new session then upgrade() will run automatically.
	dom, err = BootstrapSession(store)
	require.NoError(t, err)

	se2 := CreateSessionAndSetID(t, store)
	r = MustExecToRecodeSet(t, se2, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, []byte(fmt.Sprintf("%d", currentBootstrapVersion)), row.GetBytes(0))
	require.NoError(t, r.Close())

	ver, err = getBootstrapVersion(se2)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// Verify that 'new_collation_enabled' is false.
	r = MustExecToRecodeSet(t, se2, fmt.Sprintf(`SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='%s'`, tidbNewCollationEnabled))
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, "False", req.GetRow(0).GetString(0))
	require.NoError(t, r.Close())
	dom.Close()
}

func TestIssue17979_1(t *testing.T) {
	ctx := context.Background()

	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	// test issue 20900, upgrade from v3.0 to v4.0.11+
	seV3 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(58))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV3, "update mysql.tidb set variable_value='58' where variable_name='tidb_server_version'")
	MustExec(t, seV3, "delete from mysql.tidb where variable_name='default_oom_action'")
	MustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(58), ver)
	dom.Close()
	domV4, err := BootstrapSession(store)
	require.NoError(t, err)
	seV4 := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r := MustExecToRecodeSet(t, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, variable.OOMActionLog, req.GetRow(0).GetString(0))
	domV4.Close()
}

func TestIssue17979_2(t *testing.T) {
	ctx := context.Background()

	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// test issue 20900, upgrade from v4.0.11 to v4.0.11
	seV3 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(59))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV3, "update mysql.tidb set variable_value=59 where variable_name='tidb_server_version'")
	MustExec(t, seV3, "delete from mysql.tidb where variable_name='default_iim_action'")
	MustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(59), ver)
	dom.Close()
	domV4, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domV4.Close()
	seV4 := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r := MustExecToRecodeSet(t, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 0, req.NumRows())
}

// TestIssue20900_2 tests that a user can upgrade from TiDB 2.1 to latest,
// and their configuration remains similar. This helps protect against the
// case that a user had a 32G query memory limit in 2.1, but it is now a 1G limit
// in TiDB 4.0+. I tested this process, and it does correctly upgrade from 2.1 -> 4.0,
// but from 4.0 -> 5.0, the new default is picked up.

func TestIssue20900_2(t *testing.T) {
	ctx := context.Background()

	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// test issue 20900, upgrade from v4.0.8 to v4.0.9+
	seV3 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(52))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV3, "update mysql.tidb set variable_value=52 where variable_name='tidb_server_version'")
	MustExec(t, seV3, "delete from mysql.tidb where variable_name='default_memory_quota_query'")
	MustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(52), ver)
	dom.Close()
	domV4, err := BootstrapSession(store)
	require.NoError(t, err)
	seV4 := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r := MustExecToRecodeSet(t, seV4, "select @@tidb_mem_quota_query")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "1073741824", req.GetRow(0).GetString(0))
	require.Equal(t, int64(1073741824), seV4.GetSessionVars().MemQuotaQuery)
	r = MustExecToRecodeSet(t, seV4, "select variable_value from mysql.tidb where variable_name='default_memory_quota_query'")
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 0, req.NumRows())
	domV4.Close()
}

func TestANSISQLMode(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := CreateSessionAndSetID(t, store)

	MustExec(t, se, "USE mysql")
	MustExec(t, se, `set @@global.sql_mode="NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI"`)
	MustExec(t, se, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	unsetStoreBootstrapped(store.UUID())
	se.Close()

	// Do some clean up, BootstrapSession will not create a new domain otherwise.
	dom.Close()
	domap.Delete(store)

	// Set ANSI sql_mode and bootstrap again, to cover a bugfix.
	// Once we have a SQL like that:
	// select variable_value from mysql.tidb where variable_name = "system_tz"
	// it fails to execute in the ANSI sql_mode, and makes TiDB cluster fail to bootstrap.
	dom1, err := BootstrapSession(store)
	require.NoError(t, err)
	defer dom1.Close()
	se = CreateSessionAndSetID(t, store)
	MustExec(t, se, "select @@global.sql_mode")
	se.Close()
}

func TestOldPasswordUpgrade(t *testing.T) {
	pwd := "abc"
	oldpwd := fmt.Sprintf("%X", auth.Sha1Hash([]byte(pwd)))
	newpwd, err := oldPasswordUpgrade(oldpwd)
	require.NoError(t, err)
	require.Equal(t, "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E", newpwd)
}

func TestBootstrapInitExpensiveQueryHandle(t *testing.T) {
	store, _ := CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	se, err := createSession(store)
	require.NoError(t, err)
	dom := domain.GetDomain(se)
	require.NotNil(t, dom)
	defer dom.Close()
	require.NotNil(t, dom.ExpensiveQueryHandle())
}

func TestStmtSummary(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)

	r := MustExecToRecodeSet(t, se, "select variable_value from mysql.global_variables where variable_name='tidb_enable_stmt_summary'")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	row := req.GetRow(0)
	require.Equal(t, []byte("ON"), row.GetBytes(0))
	require.NoError(t, r.Close())
}

type bindTestStruct struct {
	originText   string
	bindText     string
	db           string
	originWithDB string
	bindWithDB   string
	deleteText   string
}

func TestUpdateBindInfo(t *testing.T) {
	bindCases := []bindTestStruct{
		{
			originText:   "select * from t where a > ?",
			bindText:     "select /*+ use_index(t, idxb) */ * from t where a > 1",
			db:           "test",
			originWithDB: "select * from `test` . `t` where `a` > ?",
			bindWithDB:   "SELECT /*+ use_index(`t` `idxb`)*/ * FROM `test`.`t` WHERE `a` > 1",
			deleteText:   "select * from test.t where a > 1",
		},
		{
			originText:   "select count ( ? ), max ( a ) from t group by b",
			bindText:     "select /*+ use_index(t, idx) */ count(1), max(a) from t group by b",
			db:           "test",
			originWithDB: "select count ( ? ) , max ( `a` ) from `test` . `t` group by `b`",
			bindWithDB:   "SELECT /*+ use_index(`t` `idx`)*/ count(1),max(`a`) FROM `test`.`t` GROUP BY `b`",
			deleteText:   "select count(1), max(a) from test.t group by b",
		},
		{
			originText:   "select * from `test` . `t` where `a` = (_charset) ?",
			bindText:     "SELECT * FROM test.t WHERE a = _utf8\\'ab\\'",
			db:           "test",
			originWithDB: "select * from `test` . `t` where `a` = ?",
			bindWithDB:   "SELECT * FROM `test`.`t` WHERE `a` = 'ab'",
			deleteText:   "select * from test.t where a = 'c'",
		},
	}

	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)

	MustExec(t, se, "alter table mysql.bind_info drop column if exists plan_digest")
	MustExec(t, se, "alter table mysql.bind_info drop column if exists sql_digest")
	for _, bindCase := range bindCases {
		sql := fmt.Sprintf("insert into mysql.bind_info values('%s', '%s', '%s', 'enabled', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')",
			bindCase.originText,
			bindCase.bindText,
			bindCase.db,
		)
		MustExec(t, se, sql)

		upgradeToVer67(se, version66)
		r := MustExecToRecodeSet(t, se, `select original_sql, bind_sql, default_db, status from mysql.bind_info where source != 'builtin'`)
		req := r.NewChunk(nil)
		require.NoError(t, r.Next(ctx, req))
		row := req.GetRow(0)
		require.Equal(t, bindCase.originWithDB, row.GetString(0))
		require.Equal(t, bindCase.bindWithDB, row.GetString(1))
		require.Equal(t, "", row.GetString(2))
		require.Equal(t, bindinfo.Enabled, row.GetString(3))
		require.NoError(t, r.Close())
		sql = fmt.Sprintf("drop global binding for %s", bindCase.deleteText)
		MustExec(t, se, sql)
		r = MustExecToRecodeSet(t, se, `select original_sql, bind_sql, status from mysql.bind_info where source != 'builtin'`)
		require.NoError(t, r.Next(ctx, req))
		row = req.GetRow(0)
		require.Equal(t, bindCase.originWithDB, row.GetString(0))
		require.Equal(t, bindCase.bindWithDB, row.GetString(1))
		require.Equal(t, "deleted", row.GetString(2))
		require.NoError(t, r.Close())
		sql = fmt.Sprintf("delete from mysql.bind_info where original_sql = '%s'", bindCase.originWithDB)
		MustExec(t, se, sql)
	}
}

func TestUpdateDuplicateBindInfo(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "alter table mysql.bind_info drop column if exists plan_digest")
	MustExec(t, se, "alter table mysql.bind_info drop column if exists sql_digest")

	MustExec(t, se, `insert into mysql.bind_info values('select * from t', 'select /*+ use_index(t, idx_a)*/ * from t', 'test', 'enabled', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	// The latest one.
	MustExec(t, se, `insert into mysql.bind_info values('select * from test . t', 'select /*+ use_index(t, idx_b)*/ * from test.t', 'test', 'enabled', '2021-01-04 14:50:58.257', '2021-01-09 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)

	MustExec(t, se, `insert into mysql.bind_info values('select * from t where a < ?', 'select * from t use index(idx) where a < 1', 'test', 'deleted', '2021-06-04 17:04:43.333', '2021-06-04 17:04:43.335', 'utf8', 'utf8_general_ci', 'manual')`)
	MustExec(t, se, `insert into mysql.bind_info values('select * from t where a < ?', 'select * from t ignore index(idx) where a < 1', 'test', 'enabled', '2021-06-04 17:04:43.335', '2021-06-04 17:04:43.335', 'utf8', 'utf8_general_ci', 'manual')`)
	MustExec(t, se, `insert into mysql.bind_info values('select * from test . t where a <= ?', 'select * from test.t use index(idx) where a <= 1', '', 'deleted', '2021-06-04 17:04:43.345', '2021-06-04 17:04:45.334', 'utf8', 'utf8_general_ci', 'manual')`)
	MustExec(t, se, `insert into mysql.bind_info values('select * from test . t where a <= ?', 'select * from test.t ignore index(idx) where a <= 1', '', 'enabled', '2021-06-04 17:04:45.334', '2021-06-04 17:04:45.334', 'utf8', 'utf8_general_ci', 'manual')`)

	upgradeToVer67(se, version66)

	r := MustExecToRecodeSet(t, se, `select original_sql, bind_sql, default_db, status, create_time from mysql.bind_info where source != 'builtin' order by create_time`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 3, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "select * from `test` . `t`", row.GetString(0))
	require.Equal(t, "SELECT /*+ use_index(`t` `idx_b`)*/ * FROM `test`.`t`", row.GetString(1))
	require.Equal(t, "", row.GetString(2))
	require.Equal(t, bindinfo.Enabled, row.GetString(3))
	require.Equal(t, "2021-01-04 14:50:58.257", row.GetTime(4).String())
	row = req.GetRow(1)
	require.Equal(t, "select * from `test` . `t` where `a` < ?", row.GetString(0))
	require.Equal(t, "SELECT * FROM `test`.`t` IGNORE INDEX (`idx`) WHERE `a` < 1", row.GetString(1))
	require.Equal(t, "", row.GetString(2))
	require.Equal(t, bindinfo.Enabled, row.GetString(3))
	require.Equal(t, "2021-06-04 17:04:43.335", row.GetTime(4).String())
	row = req.GetRow(2)
	require.Equal(t, "select * from `test` . `t` where `a` <= ?", row.GetString(0))
	require.Equal(t, "SELECT * FROM `test`.`t` IGNORE INDEX (`idx`) WHERE `a` <= 1", row.GetString(1))
	require.Equal(t, "", row.GetString(2))
	require.Equal(t, bindinfo.Enabled, row.GetString(3))
	require.Equal(t, "2021-06-04 17:04:45.334", row.GetTime(4).String())

	require.NoError(t, r.Close())
	MustExec(t, se, "delete from mysql.bind_info where original_sql = 'select * from test . t'")
}

func TestUpgradeClusteredIndexDefaultValue(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV67 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(67))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV67, "update mysql.tidb set variable_value='67' where variable_name='tidb_server_version'")
	MustExec(t, seV67, "UPDATE mysql.global_variables SET VARIABLE_VALUE = 'OFF' where VARIABLE_NAME = 'tidb_enable_clustered_index'")
	require.Equal(t, uint64(1), seV67.GetSessionVars().StmtCtx.AffectedRows())
	MustExec(t, seV67, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV67)
	require.NoError(t, err)
	require.Equal(t, int64(67), ver)
	dom.Close()

	domV68, err := BootstrapSession(store)
	require.NoError(t, err)
	seV68 := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV68)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	r := MustExecToRecodeSet(t, seV68, `select @@global.tidb_enable_clustered_index, @@session.tidb_enable_clustered_index`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "ON", row.GetString(0))
	require.Equal(t, "ON", row.GetString(1))
	domV68.Close()
}

func TestForIssue23387(t *testing.T) {
	// For issue https://github.com/pingcap/tidb/issues/23387
	saveCurrentBootstrapVersion := currentBootstrapVersion
	currentBootstrapVersion = version57

	// Bootstrap to an old version, create a user.
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	dom, err := BootstrapSession(store)
	require.NoError(t, err)

	se := CreateSessionAndSetID(t, store)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"), nil)
	MustExec(t, se, "create user quatest")
	dom.Close()
	// Upgrade to a newer version, check the user's privilege.
	currentBootstrapVersion = saveCurrentBootstrapVersion
	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	se = CreateSessionAndSetID(t, store)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"), nil)
	rs, err := exec(se, "show grants for quatest")
	require.NoError(t, err)
	rows, err := ResultSetToStringSlice(context.Background(), se, rs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "GRANT USAGE ON *.* TO 'quatest'@'%'", rows[0][0])
}

func TestReferencesPrivilegeOnColumn(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)

	defer func() {
		MustExec(t, se, "drop user if exists issue28531")
		MustExec(t, se, "drop table if exists t1")
	}()

	MustExec(t, se, "create user if not exists issue28531")
	MustExec(t, se, "use test")
	MustExec(t, se, "drop table if exists t1")
	MustExec(t, se, "create table t1 (a int)")
	MustExec(t, se, "GRANT select (a), update (a),insert(a), references(a) on t1 to issue28531")
}

func TestAnalyzeVersionUpgradeFrom300To500(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 5.1+ or above.
	ver300 := 33
	seV3 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV3, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver300))
	MustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBAnalyzeVersion))
	MustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check tidb_analyze_version should not exist.
	res := MustExecToRecodeSet(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBAnalyzeVersion))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in version no lower than 5.x, tidb_enable_index_merge should be 1.
	res = MustExecToRecodeSet(t, seCurVer, "select @@tidb_analyze_version")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "1", row.GetString(0))
}

func TestIndexMergeInNewCluster(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	// Indicates we are in a new cluster.
	require.Equal(t, int64(notBootstrapped), getStoreBootstrapVersion(store))
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)

	// In a new created cluster(above 5.4+), tidb_enable_index_merge is 1 by default.
	MustExec(t, se, "use test;")
	r := MustExecToRecodeSet(t, se, "select @@tidb_enable_index_merge;")
	require.NotNil(t, r)

	ctx := context.Background()
	chk := r.NewChunk(nil)
	err = r.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(1), row.GetInt64(0))
}

func TestIndexMergeUpgradeFrom300To540(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 5.4+.
	ver300 := 33
	seV3 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV3, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver300))
	MustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableIndexMerge))
	MustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check tidb_enable_index_merge shoudle not exist.
	res := MustExecToRecodeSet(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableIndexMerge))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 5.x, tidb_enable_index_merge should be off.
	res = MustExecToRecodeSet(t, seCurVer, "select @@tidb_enable_index_merge")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(0), row.GetInt64(0))
}

func TestIndexMergeUpgradeFrom400To540(t *testing.T) {
	for i := 0; i < 2; i++ {
		func() {
			ctx := context.Background()
			store, dom := CreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			// upgrade from 4.0.0 to 5.4+.
			ver400 := 46
			seV4 := CreateSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			err = m.FinishBootstrap(int64(ver400))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			MustExec(t, seV4, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver400))
			MustExec(t, seV4, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", variable.Off, variable.TiDBEnableIndexMerge))
			MustExec(t, seV4, "commit")
			unsetStoreBootstrapped(store.UUID())
			ver, err := getBootstrapVersion(seV4)
			require.NoError(t, err)
			require.Equal(t, int64(ver400), ver)

			// We are now in 4.0.0, tidb_enable_index_merge is off.
			res := MustExecToRecodeSet(t, seV4, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableIndexMerge))
			chk := res.NewChunk(nil)
			err = res.Next(ctx, chk)
			require.NoError(t, err)
			require.Equal(t, 1, chk.NumRows())
			row := chk.GetRow(0)
			require.Equal(t, 2, row.Len())
			require.Equal(t, variable.Off, row.GetString(1))

			if i == 0 {
				// For the first time, We set tidb_enable_index_merge as on.
				// And after upgrade to 5.x, tidb_enable_index_merge should remains to be on.
				// For the second it should be off.
				MustExec(t, seV4, "set global tidb_enable_index_merge = on")
			}
			dom.Close()
			// Upgrade to 5.x.
			domCurVer, err := BootstrapSession(store)
			require.NoError(t, err)
			defer domCurVer.Close()
			seCurVer := CreateSessionAndSetID(t, store)
			ver, err = getBootstrapVersion(seCurVer)
			require.NoError(t, err)
			require.Equal(t, currentBootstrapVersion, ver)

			// We are now in 5.x, tidb_enable_index_merge should be on because we enable it in 4.0.0.
			res = MustExecToRecodeSet(t, seCurVer, "select @@tidb_enable_index_merge")
			chk = res.NewChunk(nil)
			err = res.Next(ctx, chk)
			require.NoError(t, err)
			require.Equal(t, 1, chk.NumRows())
			row = chk.GetRow(0)
			require.Equal(t, 1, row.Len())
			if i == 0 {
				require.Equal(t, int64(1), row.GetInt64(0))
			} else {
				require.Equal(t, int64(0), row.GetInt64(0))
			}
		}()
	}
}

func TestUpgradeToVer85(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "alter table mysql.bind_info drop column if exists plan_digest")
	MustExec(t, se, "alter table mysql.bind_info drop column if exists sql_digest")

	MustExec(t, se, `insert into mysql.bind_info values('select * from t', 'select /*+ use_index(t, idx_a)*/ * from t', 'test', 'using', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	MustExec(t, se, `insert into mysql.bind_info values('select * from t1', 'select /*+ use_index(t1, idx_a)*/ * from t1', 'test', 'enabled', '2021-01-05 14:50:58.257', '2021-01-05 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	MustExec(t, se, `insert into mysql.bind_info values('select * from t2', 'select /*+ use_index(t2, idx_a)*/ * from t2', 'test', 'disabled', '2021-01-06 14:50:58.257', '2021-01-06 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	MustExec(t, se, `insert into mysql.bind_info values('select * from t3', 'select /*+ use_index(t3, idx_a)*/ * from t3', 'test', 'deleted', '2021-01-07 14:50:58.257', '2021-01-07 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	MustExec(t, se, `insert into mysql.bind_info values('select * from t4', 'select /*+ use_index(t4, idx_a)*/ * from t4', 'test', 'invalid', '2021-01-08 14:50:58.257', '2021-01-08 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	upgradeToVer85(se, version84)

	r := MustExecToRecodeSet(t, se, `select count(*) from mysql.bind_info where status = 'enabled'`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, int64(2), row.GetInt64(0))

	require.NoError(t, r.Close())
	MustExec(t, se, "delete from mysql.bind_info where default_db = 'test'")
}

func TestInitializeSQLFile(t *testing.T) {
	testEmptyInitSQLFile(t)
	testInitSystemVariable(t)
	testInitUsers(t)
	testErrorHappenWhileInit(t)
}

func testEmptyInitSQLFile(t *testing.T) {
	// An non-existent sql file would stop the bootstrap of the tidb cluster
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = "non-existent.sql"
	defer func() {
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	dom, err := BootstrapSession(store)
	require.Nil(t, dom)
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

func testInitSystemVariable(t *testing.T) {
	// We create an initialize-sql-file and then bootstrap the server with it.
	// The observed behavior should be that tidb_enable_noop_variables is now
	// disabled, and the feature works as expected.
	initializeSQLFile, err := os.CreateTemp("", "init.sql")
	require.NoError(t, err)
	defer func() {
		path := initializeSQLFile.Name()
		err = initializeSQLFile.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()
	// Implicitly test multi-line init files
	_, err = initializeSQLFile.WriteString(
		"CREATE DATABASE initsqlfiletest;\n" +
			"SET GLOBAL tidb_enable_noop_variables = OFF;\n")
	require.NoError(t, err)

	// Create a mock store
	// Set the config parameter for initialize sql file
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = initializeSQLFile.Name()
	defer func() {
		require.NoError(t, store.Close())
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	// Bootstrap with the InitializeSQLFile config option
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)
	ctx := context.Background()
	r, err := exec(se, `SHOW VARIABLES LIKE 'query_cache_type'`)
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows()) // not shown in noopvariables mode
	require.NoError(t, r.Close())

	r, err = exec(se, `SHOW VARIABLES LIKE 'tidb_enable_noop_variables'`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, []byte("OFF"), row.GetBytes(1))
	require.NoError(t, r.Close())
}

func testInitUsers(t *testing.T) {
	// Two sql files are set to 'initialize-sql-file' one after another,
	// and only the first one are executed.
	var err error
	sqlFiles := make([]*os.File, 2)
	for i, name := range []string{"1.sql", "2.sql"} {
		sqlFiles[i], err = os.CreateTemp("", name)
		require.NoError(t, err)
	}
	defer func() {
		for _, sqlFile := range sqlFiles {
			path := sqlFile.Name()
			err = sqlFile.Close()
			require.NoError(t, err)
			err = os.Remove(path)
			require.NoError(t, err)
		}
	}()
	_, err = sqlFiles[0].WriteString(`
CREATE USER cloud_admin;
GRANT BACKUP_ADMIN, RESTORE_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT DASHBOARD_CLIENT on *.* TO 'cloud_admin'@'%';
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_VARIABLES_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_STATUS_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_CONNECTION_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_USER_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_TABLES_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_REPLICA_WRITER_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT CREATE USER ON *.* TO 'cloud_admin'@'%';
GRANT RELOAD ON *.* TO 'cloud_admin'@'%';
GRANT PROCESS ON *.* TO 'cloud_admin'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON mysql.* TO 'cloud_admin'@'%';
GRANT SELECT ON information_schema.* TO 'cloud_admin'@'%';
GRANT SELECT ON performance_schema.* TO 'cloud_admin'@'%';
GRANT SHOW DATABASES on *.* TO 'cloud_admin'@'%';
GRANT REFERENCES ON *.* TO 'cloud_admin'@'%';
GRANT SELECT ON *.* TO 'cloud_admin'@'%';
GRANT INDEX ON *.* TO 'cloud_admin'@'%';
GRANT INSERT ON *.* TO 'cloud_admin'@'%';
GRANT UPDATE ON *.* TO 'cloud_admin'@'%';
GRANT DELETE ON *.* TO 'cloud_admin'@'%';
GRANT CREATE ON *.* TO 'cloud_admin'@'%';
GRANT DROP ON *.* TO 'cloud_admin'@'%';
GRANT ALTER ON *.* TO 'cloud_admin'@'%';
GRANT CREATE VIEW ON *.* TO 'cloud_admin'@'%';
GRANT SHUTDOWN, CONFIG ON *.* TO 'cloud_admin'@'%';
REVOKE SHUTDOWN, CONFIG ON *.* FROM root;

DROP USER root;
`)
	require.NoError(t, err)
	_, err = sqlFiles[1].WriteString("drop user cloud_admin;")
	require.NoError(t, err)

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[0].Name()
	defer func() {
		require.NoError(t, store.Close())
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	// Bootstrap with the first sql file
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	se := CreateSessionAndSetID(t, store)
	ctx := context.Background()
	// 'cloud_admin' has been created successfully
	r, err := exec(se, `select user from mysql.user where user = 'cloud_admin'`)
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "cloud_admin", row.GetString(0))
	require.NoError(t, r.Close())
	// 'root' has been deleted successfully
	r, err = exec(se, `select user from mysql.user where user = 'root'`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	require.NoError(t, r.Close())
	dom.Close()

	runBootstrapSQLFile = false

	// Bootstrap with the second sql file, which would not been executed.
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[1].Name()
	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	se = CreateSessionAndSetID(t, store)
	r, err = exec(se, `select user from mysql.user where user = 'cloud_admin'`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, "cloud_admin", row.GetString(0))
	require.NoError(t, r.Close())
	dom.Close()
}

func testErrorHappenWhileInit(t *testing.T) {
	// 1. parser error in sql file (1.sql) makes the bootstrap panic
	// 2. other errors in sql file (2.sql) will be ignored
	var err error
	sqlFiles := make([]*os.File, 2)
	for i, name := range []string{"1.sql", "2.sql"} {
		sqlFiles[i], err = os.CreateTemp("", name)
		require.NoError(t, err)
	}
	defer func() {
		for _, sqlFile := range sqlFiles {
			path := sqlFile.Name()
			err = sqlFile.Close()
			require.NoError(t, err)
			err = os.Remove(path)
			require.NoError(t, err)
		}
	}()
	_, err = sqlFiles[0].WriteString("create table test.t (c in);")
	require.NoError(t, err)
	_, err = sqlFiles[1].WriteString(`
create table test.t (c int);
insert into test.t values ("abc"); -- invalid statement
`)
	require.NoError(t, err)

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[0].Name()
	defer func() {
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	// Bootstrap with the first sql file
	dom, err := BootstrapSession(store)
	require.Nil(t, dom)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	runBootstrapSQLFile = false

	// Bootstrap with the second sql file, which would not been executed.
	store, err = mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[1].Name()
	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	se := CreateSessionAndSetID(t, store)
	ctx := context.Background()
	_, err = exec(se, `use test;`)
	require.NoError(t, err)
	// Table t has been created.
	r, err := exec(se, `show tables;`)
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "t", row.GetString(0))
	require.NoError(t, r.Close())
	// But data is failed to inserted since the error
	r, err = exec(se, `select * from test.t`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	require.NoError(t, r.Close())
	dom.Close()
}

func TestTiDBEnablePagingVariable(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	se := CreateSessionAndSetID(t, store)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	for _, sql := range []string{
		"select @@global.tidb_enable_paging",
		"select @@session.tidb_enable_paging",
	} {
		r := MustExecToRecodeSet(t, se, sql)
		require.NotNil(t, r)

		req := r.NewChunk(nil)
		err := r.Next(context.Background(), req)
		require.NoError(t, err)
		require.NotEqual(t, 0, req.NumRows())

		rows := statistics.RowToDatums(req.GetRow(0), r.Fields())
		if variable.DefTiDBEnablePaging {
			match(t, rows, "1")
		} else {
			match(t, rows, "0")
		}
		r.Close()
	}
}

func TestTiDBOptRangeMaxSizeWhenUpgrading(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from v6.3.0 to v6.4.0+.
	ver94 := 94
	seV630 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver94))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV630, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver94))
	MustExec(t, seV630, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBOptRangeMaxSize))
	MustExec(t, seV630, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV630)
	require.NoError(t, err)
	require.Equal(t, int64(ver94), ver)

	// We are now in 6.3.0, check tidb_opt_range_max_size should not exist.
	res := MustExecToRecodeSet(t, seV630, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBOptRangeMaxSize))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in version no lower than v6.4.0, tidb_opt_range_max_size should be 0.
	res = MustExecToRecodeSet(t, seCurVer, "select @@session.tidb_opt_range_max_size")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "0", row.GetString(0))

	res = MustExecToRecodeSet(t, seCurVer, "select @@global.tidb_opt_range_max_size")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "0", row.GetString(0))
}

func TestTiDBOptAdvancedJoinHintWhenUpgrading(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from v6.6.0 to v7.0.0+.
	ver134 := 134
	seV660 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver134))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV660, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver134))
	MustExec(t, seV660, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBOptAdvancedJoinHint))
	MustExec(t, seV660, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV660)
	require.NoError(t, err)
	require.Equal(t, int64(ver134), ver)

	// We are now in 6.6.0, check tidb_opt_advanced_join_hint should not exist.
	res := MustExecToRecodeSet(t, seV660, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBOptAdvancedJoinHint))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in version no lower than v7.0.0, tidb_opt_advanced_join_hint should be false.
	res = MustExecToRecodeSet(t, seCurVer, "select @@session.tidb_opt_advanced_join_hint;")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(0), row.GetInt64(0))

	res = MustExecToRecodeSet(t, seCurVer, "select @@global.tidb_opt_advanced_join_hint;")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(0), row.GetInt64(0))
}

func TestTiDBOptAdvancedJoinHintInNewCluster(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	// Indicates we are in a new cluster.
	require.Equal(t, int64(notBootstrapped), getStoreBootstrapVersion(store))
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)

	// In a new created cluster(above 7.0+), tidb_opt_advanced_join_hint is true by default.
	MustExec(t, se, "use test;")
	r := MustExecToRecodeSet(t, se, "select @@tidb_opt_advanced_join_hint;")
	require.NotNil(t, r)

	ctx := context.Background()
	chk := r.NewChunk(nil)
	err = r.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(1), row.GetInt64(0))
}

func TestTiDBCostModelInNewCluster(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	// Indicates we are in a new cluster.
	require.Equal(t, int64(notBootstrapped), getStoreBootstrapVersion(store))
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := CreateSessionAndSetID(t, store)

	// In a new created cluster(above 6.5+), tidb_cost_model_version is 2 by default.
	MustExec(t, se, "use test;")
	r := MustExecToRecodeSet(t, se, "select @@tidb_cost_model_version;")
	require.NotNil(t, r)

	ctx := context.Background()
	chk := r.NewChunk(nil)
	err = r.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "2", row.GetString(0))
}

func TestTiDBCostModelUpgradeFrom300To650(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 6.5+.
	ver300 := 33
	seV3 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV3, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver300))
	MustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBCostModelVersion))
	MustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check TiDBCostModelVersion should not exist.
	res := MustExecToRecodeSet(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBCostModelVersion))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())

	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 6.5+, TiDBCostModelVersion should be 1.
	res = MustExecToRecodeSet(t, seCurVer, "select @@tidb_cost_model_version")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "1", row.GetString(0))
}

func TestTiDBCostModelUpgradeFrom610To650(t *testing.T) {
	for i := 0; i < 2; i++ {
		func() {
			ctx := context.Background()
			store, dom := CreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			// upgrade from 6.1 to 6.5+.
			ver61 := 91
			seV61 := CreateSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			err = m.FinishBootstrap(int64(ver61))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			MustExec(t, seV61, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver61))
			MustExec(t, seV61, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "1", variable.TiDBCostModelVersion))
			MustExec(t, seV61, "commit")
			unsetStoreBootstrapped(store.UUID())
			ver, err := getBootstrapVersion(seV61)
			require.NoError(t, err)
			require.Equal(t, int64(ver61), ver)

			// We are now in 6.1, tidb_cost_model_version is 1.
			res := MustExecToRecodeSet(t, seV61, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBCostModelVersion))
			chk := res.NewChunk(nil)
			err = res.Next(ctx, chk)
			require.NoError(t, err)
			require.Equal(t, 1, chk.NumRows())
			row := chk.GetRow(0)
			require.Equal(t, 2, row.Len())
			require.Equal(t, "1", row.GetString(1))
			res.Close()

			if i == 0 {
				// For the first time, We set tidb_cost_model_version to 2.
				// And after upgrade to 6.5, tidb_cost_model_version should be 2.
				// For the second it should be 1.
				MustExec(t, seV61, "set global tidb_cost_model_version = 2")
			}
			dom.Close()
			// Upgrade to 6.5.
			domCurVer, err := BootstrapSession(store)
			require.NoError(t, err)
			defer domCurVer.Close()
			seCurVer := CreateSessionAndSetID(t, store)
			ver, err = getBootstrapVersion(seCurVer)
			require.NoError(t, err)
			require.Equal(t, currentBootstrapVersion, ver)

			// We are now in 6.5.
			res = MustExecToRecodeSet(t, seCurVer, "select @@tidb_cost_model_version")
			chk = res.NewChunk(nil)
			err = res.Next(ctx, chk)
			require.NoError(t, err)
			require.Equal(t, 1, chk.NumRows())
			row = chk.GetRow(0)
			require.Equal(t, 1, row.Len())
			if i == 0 {
				require.Equal(t, "2", row.GetString(0))
			} else {
				require.Equal(t, "1", row.GetString(0))
			}
			res.Close()
		}()
	}
}

func TestTiDBGCAwareUpgradeFrom630To650(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.3 to 6.5+.
	ver63 := version93
	seV63 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver63))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV63, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver63))
	MustExec(t, seV63, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "1", variable.TiDBEnableGCAwareMemoryTrack))
	MustExec(t, seV63, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV63)
	require.NoError(t, err)
	require.Equal(t, int64(ver63), ver)

	// We are now in 6.3, tidb_enable_gc_aware_memory_track is ON.
	res := MustExecToRecodeSet(t, seV63, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableGCAwareMemoryTrack))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "1", row.GetString(1))

	// Upgrade to 6.5.
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 6.5.
	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableGCAwareMemoryTrack))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))
}

func TestTiDBServerMemoryLimitUpgradeTo651_1(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.5.0 to 6.5.1+.
	ver132 := version132
	seV132 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver132))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV132, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver132))
	MustExec(t, seV132, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", variable.TiDBServerMemoryLimit))
	MustExec(t, seV132, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV132)
	require.NoError(t, err)
	require.Equal(t, int64(ver132), ver)

	// We are now in 6.5.0, tidb_server_memory_limit is 0.
	res := MustExecToRecodeSet(t, seV132, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBServerMemoryLimit))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))

	// Upgrade to 6.5.1+.
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 6.5.1+.
	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBServerMemoryLimit))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, variable.DefTiDBServerMemoryLimit, row.GetString(1))
}

func TestTiDBServerMemoryLimitUpgradeTo651_2(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.5.0 to 6.5.1+.
	ver132 := version132
	seV132 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver132))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV132, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver132))
	MustExec(t, seV132, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "70%", variable.TiDBServerMemoryLimit))
	MustExec(t, seV132, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV132)
	require.NoError(t, err)
	require.Equal(t, int64(ver132), ver)

	// We are now in 6.5.0, tidb_server_memory_limit is "70%".
	res := MustExecToRecodeSet(t, seV132, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBServerMemoryLimit))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "70%", row.GetString(1))

	// Upgrade to 6.5.1+.
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 6.5.1+.
	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBServerMemoryLimit))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "70%", row.GetString(1))
}

func TestTiDBGlobalVariablesDefaultValueUpgradeFrom630To660(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.3.0 to 6.6.0.
	ver630 := version93
	seV630 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver630))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV630, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver630))
	MustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", variable.TiDBEnableForeignKey))
	MustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", variable.ForeignKeyChecks))
	MustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", variable.TiDBEnableHistoricalStats))
	MustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", variable.TiDBEnablePlanReplayerCapture))
	MustExec(t, seV630, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV630)
	require.NoError(t, err)
	require.Equal(t, int64(ver630), ver)

	// We are now in 6.3.0.
	upgradeVars := []string{variable.TiDBEnableForeignKey, variable.ForeignKeyChecks, variable.TiDBEnableHistoricalStats, variable.TiDBEnablePlanReplayerCapture}
	varsValueList := []string{"OFF", "OFF", "OFF", "OFF"}
	for i := range upgradeVars {
		res := MustExecToRecodeSet(t, seV630, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", upgradeVars[i]))
		chk := res.NewChunk(nil)
		err = res.Next(ctx, chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		row := chk.GetRow(0)
		require.Equal(t, 2, row.Len())
		require.Equal(t, varsValueList[i], row.GetString(1))
	}

	// Upgrade to 6.6.0.
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seV660 := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV660)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 6.6.0.
	varsValueList = []string{"ON", "ON", "ON", "ON"}
	for i := range upgradeVars {
		res := MustExecToRecodeSet(t, seV660, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", upgradeVars[i]))
		chk := res.NewChunk(nil)
		err = res.Next(ctx, chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		row := chk.GetRow(0)
		require.Equal(t, 2, row.Len())
		require.Equal(t, varsValueList[i], row.GetString(1))
	}
}

func TestTiDBStoreBatchSizeUpgradeFrom650To660(t *testing.T) {
	for i := 0; i < 2; i++ {
		func() {
			ctx := context.Background()
			store, dom := CreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			// upgrade from 6.5 to 6.6.
			ver65 := version132
			seV65 := CreateSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			err = m.FinishBootstrap(int64(ver65))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			MustExec(t, seV65, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver65))
			MustExec(t, seV65, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", variable.TiDBStoreBatchSize))
			MustExec(t, seV65, "commit")
			unsetStoreBootstrapped(store.UUID())
			ver, err := getBootstrapVersion(seV65)
			require.NoError(t, err)
			require.Equal(t, int64(ver65), ver)

			// We are now in 6.5, tidb_store_batch_size is 0.
			res := MustExecToRecodeSet(t, seV65, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBStoreBatchSize))
			chk := res.NewChunk(nil)
			err = res.Next(ctx, chk)
			require.NoError(t, err)
			require.Equal(t, 1, chk.NumRows())
			row := chk.GetRow(0)
			require.Equal(t, 2, row.Len())
			require.Equal(t, "0", row.GetString(1))
			res.Close()

			if i == 0 {
				// For the first time, We set tidb_store_batch_size to 1.
				// And after upgrade to 6.6, tidb_store_batch_size should be 1.
				// For the second it should be the latest default value.
				MustExec(t, seV65, "set global tidb_store_batch_size = 1")
			}
			dom.Close()
			// Upgrade to 6.6.
			domCurVer, err := BootstrapSession(store)
			require.NoError(t, err)
			defer domCurVer.Close()
			seCurVer := CreateSessionAndSetID(t, store)
			ver, err = getBootstrapVersion(seCurVer)
			require.NoError(t, err)
			require.Equal(t, currentBootstrapVersion, ver)

			// We are now in 6.6.
			res = MustExecToRecodeSet(t, seCurVer, "select @@tidb_store_batch_size")
			chk = res.NewChunk(nil)
			err = res.Next(ctx, chk)
			require.NoError(t, err)
			require.Equal(t, 1, chk.NumRows())
			row = chk.GetRow(0)
			require.Equal(t, 1, row.Len())
			if i == 0 {
				require.Equal(t, "1", row.GetString(0))
			} else {
				require.Equal(t, "4", row.GetString(0))
			}
			res.Close()
		}()
	}
}

func TestTiDBUpgradeToVer136(t *testing.T) {
	store, _ := CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver135 := version135
	seV135 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver135))
	require.NoError(t, err)
	MustExec(t, seV135, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver135))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV135)
	require.NoError(t, err)
	require.Equal(t, int64(ver135), ver)

	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	ver, err = getBootstrapVersion(seV135)
	require.NoError(t, err)
	require.Less(t, int64(ver135), ver)
	dom.Close()
}

func TestTiDBUpgradeToVer140(t *testing.T) {
	store, _ := CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver139 := version139
	resetTo139 := func(s Session) {
		txn, err := store.Begin()
		require.NoError(t, err)
		m := meta.NewMeta(txn)
		err = m.FinishBootstrap(int64(ver139))
		require.NoError(t, err)
		MustExec(t, s, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver139))
		err = txn.Commit(context.Background())
		require.NoError(t, err)

		unsetStoreBootstrapped(store.UUID())
		ver, err := getBootstrapVersion(s)
		require.NoError(t, err)
		require.Equal(t, int64(ver139), ver)
	}

	// drop column task_key and then upgrade
	s := CreateSessionAndSetID(t, store)
	MustExec(t, s, "alter table mysql.tidb_global_task drop column task_key")
	resetTo139(s)
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	ver, err := getBootstrapVersion(s)
	require.NoError(t, err)
	require.Less(t, int64(ver139), ver)
	dom.Close()

	// upgrade with column task_key exists
	s = CreateSessionAndSetID(t, store)
	resetTo139(s)
	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	ver, err = getBootstrapVersion(s)
	require.NoError(t, err)
	require.Less(t, int64(ver139), ver)
	dom.Close()
}

func TestTiDBNonPrepPlanCacheUpgradeFrom540To700(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// bootstrap to 5.4
	ver54 := version82
	seV54 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver54))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV54, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver54))
	MustExec(t, seV54, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableNonPreparedPlanCache))
	MustExec(t, seV54, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV54)
	require.NoError(t, err)
	require.Equal(t, int64(ver54), ver)

	// We are now in 5.4, check TiDBCostModelVersion should not exist.
	res := MustExecToRecodeSet(t, seV54, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableNonPreparedPlanCache))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())

	// Upgrade to 7.0
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 7.0
	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableNonPreparedPlanCache))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "OFF", row.GetString(1)) // tidb_enable_non_prepared_plan_cache = off

	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBNonPreparedPlanCacheSize))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "100", row.GetString(1)) // tidb_non_prepared_plan_cache_size = 100
}

func TestTiDBStatsLoadPseudoTimeoutUpgradeFrom610To650(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.1 to 6.5+.
	ver61 := version91
	seV61 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver61))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV61, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver61))
	MustExec(t, seV61, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", variable.TiDBStatsLoadPseudoTimeout))
	MustExec(t, seV61, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV61)
	require.NoError(t, err)
	require.Equal(t, int64(ver61), ver)

	// We are now in 6.1, tidb_stats_load_pseudo_timeout is OFF.
	res := MustExecToRecodeSet(t, seV61, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBStatsLoadPseudoTimeout))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))

	// Upgrade to 6.5.
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 6.5.
	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBStatsLoadPseudoTimeout))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "1", row.GetString(1))
}

func TestTiDBTiDBOptTiDBOptimizerEnableNAAJWhenUpgradingToVer138(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver137 := version137
	seV137 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver137))
	require.NoError(t, err)
	MustExec(t, seV137, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver137))
	MustExec(t, seV137, "update mysql.GLOBAL_VARIABLES set variable_value='OFF' where variable_name='tidb_enable_null_aware_anti_join'")
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV137)
	require.NoError(t, err)
	require.Equal(t, int64(ver137), ver)

	res := MustExecToRecodeSet(t, seV137, "select * from mysql.GLOBAL_VARIABLES where variable_name='tidb_enable_null_aware_anti_join'")
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "OFF", row.GetString(1))

	// Upgrade to version 138.
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	res = MustExecToRecodeSet(t, seCurVer, "select * from mysql.GLOBAL_VARIABLES where variable_name='tidb_enable_null_aware_anti_join'")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "ON", row.GetString(1))
}

func TestTiDBUpgradeToVer143(t *testing.T) {
	store, _ := CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver142 := version142
	seV142 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver142))
	require.NoError(t, err)
	MustExec(t, seV142, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver142))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV142)
	require.NoError(t, err)
	require.Equal(t, int64(ver142), ver)

	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	ver, err = getBootstrapVersion(seV142)
	require.NoError(t, err)
	require.Less(t, int64(ver142), ver)
	dom.Close()
}

func TestTiDBLoadBasedReplicaReadThresholdUpgradingToVer141(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 7.0 to 7.1.
	ver70 := version139
	seV70 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver70))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, seV70, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver70))
	MustExec(t, seV70, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", variable.TiDBLoadBasedReplicaReadThreshold))
	MustExec(t, seV70, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV70)
	require.NoError(t, err)
	require.Equal(t, int64(ver70), ver)

	// We are now in 7.0, tidb_load_based_replica_read_threshold is 0.
	res := MustExecToRecodeSet(t, seV70, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBLoadBasedReplicaReadThreshold))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))

	// Upgrade to 7.1.
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 7.1.
	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBLoadBasedReplicaReadThreshold))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "1s", row.GetString(1))
}

func TestTiDBPlanCacheInvalidationOnFreshStatsWhenUpgradingToVer144(t *testing.T) {
	ctx := context.Background()
	store, _ := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// bootstrap as version143
	ver143 := version143
	seV143 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver143))
	require.NoError(t, err)
	MustExec(t, seV143, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver143))
	// simulate a real ver143 where `tidb_plan_cache_invalidation_on_fresh_stats` doesn't exist yet
	MustExec(t, seV143, "delete from mysql.GLOBAL_VARIABLES where variable_name='tidb_plan_cache_invalidation_on_fresh_stats'")
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	unsetStoreBootstrapped(store.UUID())

	// upgrade to ver144
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err := getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// the value in the table is set to OFF automatically
	res := MustExecToRecodeSet(t, seCurVer, "select * from mysql.GLOBAL_VARIABLES where variable_name='tidb_plan_cache_invalidation_on_fresh_stats'")
	chk := res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, "OFF", row.GetString(1))

	// the session and global variable is also OFF
	res = MustExecToRecodeSet(t, seCurVer, "select @@session.tidb_plan_cache_invalidation_on_fresh_stats, @@global.tidb_plan_cache_invalidation_on_fresh_stats")
	chk = res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, int64(0), row.GetInt64(0))
	require.Equal(t, int64(0), row.GetInt64(1))
}

func TestTiDBUpgradeToVer145(t *testing.T) {
	store, _ := CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver144 := version144
	seV144 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver144))
	require.NoError(t, err)
	MustExec(t, seV144, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver144))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV144)
	require.NoError(t, err)
	require.Equal(t, int64(ver144), ver)

	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	ver, err = getBootstrapVersion(seV144)
	require.NoError(t, err)
	require.Less(t, int64(ver144), ver)
	dom.Close()
}

func TestTiDBUpgradeToVer170(t *testing.T) {
	store, _ := CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ver169 := version169
	seV169 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver169))
	require.NoError(t, err)
	MustExec(t, seV169, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver169))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV169)
	require.NoError(t, err)
	require.Equal(t, int64(ver169), ver)

	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	ver, err = getBootstrapVersion(seV169)
	require.NoError(t, err)
	require.Less(t, int64(ver169), ver)
	dom.Close()
}
