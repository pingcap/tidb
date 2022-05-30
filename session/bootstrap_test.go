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
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

// This test file have many problem.
// 1. Please use testkit to create dom, session and store.
// 2. Don't use createStoreAndBootstrap and BootstrapSession together. It will cause data race.
// Please do not add any test here. You can add test case at the bootstrap_update_test.go. After All problem fixed,
// We will overwrite this file by update_test.go.
func TestBootstrap(t *testing.T) {
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := createSessionAndSetID(t, store)

	mustExec(t, se, "use mysql")
	r := mustExec(t, se, "select * from user")
	require.NotNil(t, r)

	ctx := context.Background()
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())

	rows := statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y")

	ok := se.Auth(&auth.UserIdentity{Username: "root", Hostname: "anyhost"}, []byte(""), []byte(""))
	require.True(t, ok)

	mustExec(t, se, "use test")

	// Check privilege tables.
	rs := mustExec(t, se, "SELECT * from mysql.global_priv")
	require.NoError(t, rs.Close())
	rs = mustExec(t, se, "SELECT * from mysql.db")
	require.NoError(t, rs.Close())
	rs = mustExec(t, se, "SELECT * from mysql.tables_priv")
	require.NoError(t, rs.Close())
	rs = mustExec(t, se, "SELECT * from mysql.columns_priv")
	require.NoError(t, rs.Close())
	rs = mustExec(t, se, "SELECT * from mysql.global_grants")
	require.NoError(t, rs.Close())

	// Check privilege tables.
	r = mustExec(t, se, "SELECT COUNT(*) from mysql.global_variables")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, globalVarsCount(), req.GetRow(0).GetInt64(0))

	// Check a storage operations are default autocommit after the second start.
	mustExec(t, se, "USE test")
	mustExec(t, se, "drop table if exists t")
	mustExec(t, se, "create table t (id int)")
	unsetStoreBootstrapped(store.UUID())
	se.Close()

	se, err = CreateSession4Test(store)
	require.NoError(t, err)
	mustExec(t, se, "USE test")
	mustExec(t, se, "insert t values (?)", 3)

	se, err = CreateSession4Test(store)
	require.NoError(t, err)
	mustExec(t, se, "USE test")
	r = mustExec(t, se, "select * from t")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	rows = statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, 3)
	mustExec(t, se, "drop table if exists t")
	se.Close()

	// Try to do bootstrap dml jobs on an already bootstrapped TiDB system will not cause fatal.
	// For https://github.com/pingcap/tidb/issues/1096
	se, err = CreateSession4Test(store)
	require.NoError(t, err)
	doDMLWorks(se)
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
			sessionVars: variable.NewSessionVars(),
		}
		se.txn.init()
		se.mu.values = make(map[fmt.Stringer]interface{})
		se.SetValue(sessionctx.Initing, true)

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
	domap.Delete(store)
	dom.Close()

	dom1, err := BootstrapSession(store)
	require.NoError(t, err)
	defer dom1.Close()

	se := createSessionAndSetID(t, store)
	mustExec(t, se, "USE mysql")
	r := mustExec(t, se, `select * from user`)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())

	row := req.GetRow(0)
	rows := statistics.RowToDatums(row, r.Fields())
	match(t, rows, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y")
	require.NoError(t, r.Close())

	mustExec(t, se, "USE test")
	// Check privilege tables.
	mustExec(t, se, "SELECT * from mysql.global_priv")
	mustExec(t, se, "SELECT * from mysql.db")
	mustExec(t, se, "SELECT * from mysql.tables_priv")
	mustExec(t, se, "SELECT * from mysql.columns_priv")
	// Check role tables.
	mustExec(t, se, "SELECT * from mysql.role_edges")
	mustExec(t, se, "SELECT * from mysql.default_roles")
	// Check global variables.
	r = mustExec(t, se, "SELECT COUNT(*) from mysql.global_variables")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	v := req.GetRow(0)
	require.Equal(t, globalVarsCount(), v.GetInt64(0))
	require.NoError(t, r.Close())

	r = mustExec(t, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="bootstrapped"`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, []byte("True"), row.GetBytes(0))
	require.NoError(t, r.Close())
}

// TestUpgrade tests upgrading
func TestUpgrade(t *testing.T) {
	ctx := context.Background()

	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := createSessionAndSetID(t, store)

	mustExec(t, se, "USE mysql")

	// bootstrap with currentBootstrapVersion
	r := mustExec(t, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	row := req.GetRow(0)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	require.Equal(t, 1, row.Len())
	require.Equal(t, []byte(fmt.Sprintf("%d", currentBootstrapVersion)), row.GetBytes(0))
	require.NoError(t, r.Close())

	se1 := createSessionAndSetID(t, store)
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
	mustExec(t, se1, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	mustExec(t, se1, fmt.Sprintf(`delete from mysql.global_variables where VARIABLE_NAME="%s"`, variable.TiDBDistSQLScanConcurrency))
	mustExec(t, se1, `commit`)
	unsetStoreBootstrapped(store.UUID())
	// Make sure the version is downgraded.
	r = mustExec(t, se1, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
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

	se2 := createSessionAndSetID(t, store)
	r = mustExec(t, se2, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
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
	r = mustExec(t, se2, fmt.Sprintf(`SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='%s'`, tidbNewCollationEnabled))
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

	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	// test issue 20900, upgrade from v3.0 to v4.0.11+
	seV3 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(58))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV3, "update mysql.tidb set variable_value='58' where variable_name='tidb_server_version'")
	mustExec(t, seV3, "delete from mysql.tidb where variable_name='default_oom_action'")
	mustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(58), ver)
	dom.Close()
	domV4, err := BootstrapSession(store)
	require.NoError(t, err)
	seV4 := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r := mustExec(t, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, variable.OOMActionLog, req.GetRow(0).GetString(0))
	domV4.Close()
}

func TestIssue17979_2(t *testing.T) {
	ctx := context.Background()

	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// test issue 20900, upgrade from v4.0.11 to v4.0.11
	seV3 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(59))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV3, "update mysql.tidb set variable_value=59 where variable_name='tidb_server_version'")
	mustExec(t, seV3, "delete from mysql.tidb where variable_name='default_iim_action'")
	mustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(59), ver)
	dom.Close()
	domV4, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domV4.Close()
	seV4 := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r := mustExec(t, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
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

	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// test issue 20900, upgrade from v4.0.8 to v4.0.9+
	seV3 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(52))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV3, "update mysql.tidb set variable_value=52 where variable_name='tidb_server_version'")
	mustExec(t, seV3, "delete from mysql.tidb where variable_name='default_memory_quota_query'")
	mustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(52), ver)
	dom.Close()
	domV4, err := BootstrapSession(store)
	require.NoError(t, err)
	seV4 := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r := mustExec(t, seV4, "select @@tidb_mem_quota_query")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "1073741824", req.GetRow(0).GetString(0))
	require.Equal(t, int64(1073741824), seV4.GetSessionVars().MemQuotaQuery)
	r = mustExec(t, seV4, "select variable_value from mysql.tidb where variable_name='default_memory_quota_query'")
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 0, req.NumRows())
	domV4.Close()
}

func TestANSISQLMode(t *testing.T) {
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := createSessionAndSetID(t, store)

	mustExec(t, se, "USE mysql")
	mustExec(t, se, `set @@global.sql_mode="NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI"`)
	mustExec(t, se, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
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
	se = createSessionAndSetID(t, store)
	mustExec(t, se, "select @@global.sql_mode")
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
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
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
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := createSessionAndSetID(t, store)

	r := mustExec(t, se, "select variable_value from mysql.global_variables where variable_name='tidb_enable_stmt_summary'")
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
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := createSessionAndSetID(t, store)
	for _, bindCase := range bindCases {
		sql := fmt.Sprintf("insert into mysql.bind_info values('%s', '%s', '%s', 'enabled', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')",
			bindCase.originText,
			bindCase.bindText,
			bindCase.db,
		)
		mustExec(t, se, sql)

		upgradeToVer67(se, version66)
		r := mustExec(t, se, `select original_sql, bind_sql, default_db, status from mysql.bind_info where source != 'builtin'`)
		req := r.NewChunk(nil)
		require.NoError(t, r.Next(ctx, req))
		row := req.GetRow(0)
		require.Equal(t, bindCase.originWithDB, row.GetString(0))
		require.Equal(t, bindCase.bindWithDB, row.GetString(1))
		require.Equal(t, "", row.GetString(2))
		require.Equal(t, bindinfo.Enabled, row.GetString(3))
		require.NoError(t, r.Close())
		sql = fmt.Sprintf("drop global binding for %s", bindCase.deleteText)
		mustExec(t, se, sql)
		r = mustExec(t, se, `select original_sql, bind_sql, status from mysql.bind_info where source != 'builtin'`)
		require.NoError(t, r.Next(ctx, req))
		row = req.GetRow(0)
		require.Equal(t, bindCase.originWithDB, row.GetString(0))
		require.Equal(t, bindCase.bindWithDB, row.GetString(1))
		require.Equal(t, "deleted", row.GetString(2))
		require.NoError(t, r.Close())
		sql = fmt.Sprintf("delete from mysql.bind_info where original_sql = '%s'", bindCase.originWithDB)
		mustExec(t, se, sql)
	}
}

func TestUpdateDuplicateBindInfo(t *testing.T) {
	ctx := context.Background()
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := createSessionAndSetID(t, store)
	mustExec(t, se, `insert into mysql.bind_info values('select * from t', 'select /*+ use_index(t, idx_a)*/ * from t', 'test', 'enabled', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	// The latest one.
	mustExec(t, se, `insert into mysql.bind_info values('select * from test . t', 'select /*+ use_index(t, idx_b)*/ * from test.t', 'test', 'enabled', '2021-01-04 14:50:58.257', '2021-01-09 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)

	mustExec(t, se, `insert into mysql.bind_info values('select * from t where a < ?', 'select * from t use index(idx) where a < 1', 'test', 'deleted', '2021-06-04 17:04:43.333', '2021-06-04 17:04:43.335', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExec(t, se, `insert into mysql.bind_info values('select * from t where a < ?', 'select * from t ignore index(idx) where a < 1', 'test', 'enabled', '2021-06-04 17:04:43.335', '2021-06-04 17:04:43.335', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExec(t, se, `insert into mysql.bind_info values('select * from test . t where a <= ?', 'select * from test.t use index(idx) where a <= 1', '', 'deleted', '2021-06-04 17:04:43.345', '2021-06-04 17:04:45.334', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExec(t, se, `insert into mysql.bind_info values('select * from test . t where a <= ?', 'select * from test.t ignore index(idx) where a <= 1', '', 'enabled', '2021-06-04 17:04:45.334', '2021-06-04 17:04:45.334', 'utf8', 'utf8_general_ci', 'manual')`)

	upgradeToVer67(se, version66)

	r := mustExec(t, se, `select original_sql, bind_sql, default_db, status, create_time from mysql.bind_info where source != 'builtin' order by create_time`)
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
	mustExec(t, se, "delete from mysql.bind_info where original_sql = 'select * from test . t'")
}

func TestUpgradeClusteredIndexDefaultValue(t *testing.T) {
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV67 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(67))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV67, "update mysql.tidb set variable_value='67' where variable_name='tidb_server_version'")
	mustExec(t, seV67, "UPDATE mysql.global_variables SET VARIABLE_VALUE = 'OFF' where VARIABLE_NAME = 'tidb_enable_clustered_index'")
	require.Equal(t, uint64(1), seV67.GetSessionVars().StmtCtx.AffectedRows())
	mustExec(t, seV67, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV67)
	require.NoError(t, err)
	require.Equal(t, int64(67), ver)
	dom.Close()

	domV68, err := BootstrapSession(store)
	require.NoError(t, err)
	seV68 := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV68)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	r := mustExec(t, seV68, `select @@global.tidb_enable_clustered_index, @@session.tidb_enable_clustered_index`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "INT_ONLY", row.GetString(0))
	require.Equal(t, "INT_ONLY", row.GetString(1))
	domV68.Close()
}

func TestUpgradeVersion66(t *testing.T) {
	ctx := context.Background()
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	seV65 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(65))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV65, "update mysql.tidb set variable_value='65' where variable_name='tidb_server_version'")
	mustExec(t, seV65, "set @@global.tidb_track_aggregate_memory_usage = 0")
	mustExec(t, seV65, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV65)
	require.NoError(t, err)
	require.Equal(t, int64(65), ver)
	dom.Close()
	domV66, err := BootstrapSession(store)
	require.NoError(t, err)

	seV66 := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV66)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r := mustExec(t, seV66, `select @@global.tidb_track_aggregate_memory_usage, @@session.tidb_track_aggregate_memory_usage`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, int64(1), row.GetInt64(0))
	require.Equal(t, int64(1), row.GetInt64(1))
	domV66.Close()
}

func TestUpgradeVersion74(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		oldValue int
		newValue int
	}{
		{200, 3000},
		{3000, 3000},
		{3001, 3001},
	}

	for _, ca := range cases {
		func() {
			store, dom := createStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			seV73 := createSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			err = m.FinishBootstrap(int64(73))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			mustExec(t, seV73, "update mysql.tidb set variable_value='72' where variable_name='tidb_server_version'")
			mustExec(t, seV73, "set @@global.tidb_stmt_summary_max_stmt_count = "+strconv.Itoa(ca.oldValue))
			mustExec(t, seV73, "commit")
			unsetStoreBootstrapped(store.UUID())
			ver, err := getBootstrapVersion(seV73)
			require.NoError(t, err)
			require.Equal(t, int64(72), ver)
			dom.Close()
			domV74, err := BootstrapSession(store)
			require.NoError(t, err)
			defer domV74.Close()
			seV74 := createSessionAndSetID(t, store)
			ver, err = getBootstrapVersion(seV74)
			require.NoError(t, err)
			require.Equal(t, currentBootstrapVersion, ver)
			r := mustExec(t, seV74, `SELECT @@global.tidb_stmt_summary_max_stmt_count`)
			req := r.NewChunk(nil)
			require.NoError(t, r.Next(ctx, req))
			require.Equal(t, 1, req.NumRows())
			row := req.GetRow(0)
			require.Equal(t, strconv.Itoa(ca.newValue), row.GetString(0))
		}()
	}
}

func TestUpgradeVersion75(t *testing.T) {
	ctx := context.Background()

	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV74 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(74))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV74, "update mysql.tidb set variable_value='74' where variable_name='tidb_server_version'")
	mustExec(t, seV74, "commit")
	mustExec(t, seV74, "ALTER TABLE mysql.user DROP PRIMARY KEY")
	mustExec(t, seV74, "ALTER TABLE mysql.user MODIFY COLUMN Host CHAR(64)")
	mustExec(t, seV74, "ALTER TABLE mysql.user ADD PRIMARY KEY(Host, User)")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV74)
	require.NoError(t, err)
	require.Equal(t, int64(74), ver)
	r := mustExec(t, seV74, `desc mysql.user`)
	req := r.NewChunk(nil)
	row := req.GetRow(0)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "host", strings.ToLower(row.GetString(0)))
	require.Equal(t, "char(64)", strings.ToLower(row.GetString(1)))
	dom.Close()
	domV75, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domV75.Close()
	seV75 := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seV75)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	r = mustExec(t, seV75, `desc mysql.user`)
	req = r.NewChunk(nil)
	row = req.GetRow(0)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "host", strings.ToLower(row.GetString(0)))
	require.Equal(t, "char(255)", strings.ToLower(row.GetString(1)))
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

	se := createSessionAndSetID(t, store)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"))
	mustExec(t, se, "create user quatest")
	dom.Close()
	// Upgrade to a newer version, check the user's privilege.
	currentBootstrapVersion = saveCurrentBootstrapVersion
	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	se = createSessionAndSetID(t, store)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"))
	rs, err := exec(se, "show grants for quatest")
	require.NoError(t, err)
	rows, err := ResultSetToStringSlice(context.Background(), se, rs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "GRANT USAGE ON *.* TO 'quatest'@'%'", rows[0][0])
}

func TestReferencesPrivilegeOnColumn(t *testing.T) {
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := createSessionAndSetID(t, store)

	defer func() {
		mustExec(t, se, "drop user if exists issue28531")
		mustExec(t, se, "drop table if exists t1")
	}()

	mustExec(t, se, "create user if not exists issue28531")
	mustExec(t, se, "use test")
	mustExec(t, se, "drop table if exists t1")
	mustExec(t, se, "create table t1 (a int)")
	mustExec(t, se, "GRANT select (a), update (a),insert(a), references(a) on t1 to issue28531")
}

func TestAnalyzeVersionUpgradeFrom300To500(t *testing.T) {
	ctx := context.Background()
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 5.1+ or above.
	ver300 := 33
	seV3 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV3, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver300))
	mustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBAnalyzeVersion))
	mustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check tidb_analyze_version should not exist.
	res := mustExec(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBAnalyzeVersion))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in version no lower than 5.x, tidb_enable_index_merge should be 1.
	res = mustExec(t, seCurVer, "select @@tidb_analyze_version")
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
	se := createSessionAndSetID(t, store)

	// In a new created cluster(above 5.4+), tidb_enable_index_merge is 1 by default.
	mustExec(t, se, "use test;")
	r := mustExec(t, se, "select @@tidb_enable_index_merge;")
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
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 5.4+.
	ver300 := 33
	seV3 := createSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	mustExec(t, seV3, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver300))
	mustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableIndexMerge))
	mustExec(t, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check tidb_enable_index_merge shoudle not exist.
	res := mustExec(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableIndexMerge))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := createSessionAndSetID(t, store)
	ver, err = getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// We are now in 5.x, tidb_enable_index_merge should be off.
	res = mustExec(t, seCurVer, "select @@tidb_enable_index_merge")
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
			store, dom := createStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			// upgrade from 4.0.0 to 5.4+.
			ver400 := 46
			seV4 := createSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			err = m.FinishBootstrap(int64(ver400))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			mustExec(t, seV4, fmt.Sprintf("update mysql.tidb set variable_value=%d where variable_name='tidb_server_version'", ver400))
			mustExec(t, seV4, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", variable.Off, variable.TiDBEnableIndexMerge))
			mustExec(t, seV4, "commit")
			unsetStoreBootstrapped(store.UUID())
			ver, err := getBootstrapVersion(seV4)
			require.NoError(t, err)
			require.Equal(t, int64(ver400), ver)

			// We are now in 4.0.0, tidb_enable_index_merge is off.
			res := mustExec(t, seV4, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", variable.TiDBEnableIndexMerge))
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
				mustExec(t, seV4, "set global tidb_enable_index_merge = on")
			}
			dom.Close()
			// Upgrade to 5.x.
			domCurVer, err := BootstrapSession(store)
			require.NoError(t, err)
			defer domCurVer.Close()
			seCurVer := createSessionAndSetID(t, store)
			ver, err = getBootstrapVersion(seCurVer)
			require.NoError(t, err)
			require.Equal(t, currentBootstrapVersion, ver)

			// We are now in 5.x, tidb_enable_index_merge should be on because we enable it in 4.0.0.
			res = mustExec(t, seCurVer, "select @@tidb_enable_index_merge")
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
	store, dom := createStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := createSessionAndSetID(t, store)
	mustExec(t, se, `insert into mysql.bind_info values('select * from t', 'select /*+ use_index(t, idx_a)*/ * from t', 'test', 'using', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExec(t, se, `insert into mysql.bind_info values('select * from t1', 'select /*+ use_index(t1, idx_a)*/ * from t1', 'test', 'enabled', '2021-01-05 14:50:58.257', '2021-01-05 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExec(t, se, `insert into mysql.bind_info values('select * from t2', 'select /*+ use_index(t2, idx_a)*/ * from t2', 'test', 'disabled', '2021-01-06 14:50:58.257', '2021-01-06 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExec(t, se, `insert into mysql.bind_info values('select * from t3', 'select /*+ use_index(t3, idx_a)*/ * from t3', 'test', 'deleted', '2021-01-07 14:50:58.257', '2021-01-07 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExec(t, se, `insert into mysql.bind_info values('select * from t4', 'select /*+ use_index(t4, idx_a)*/ * from t4', 'test', 'invalid', '2021-01-08 14:50:58.257', '2021-01-08 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	upgradeToVer85(se, version84)

	r := mustExec(t, se, `select count(*) from mysql.bind_info where status = 'enabled'`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, int64(2), row.GetInt64(0))

	require.NoError(t, r.Close())
	mustExec(t, se, "delete from mysql.bind_info where default_db = 'test'")
}
