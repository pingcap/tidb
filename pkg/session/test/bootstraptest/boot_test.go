// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bootstraptest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func match(t *testing.T, row []types.Datum, expected ...any) {
	require.Len(t, row, len(expected))
	for i := range row {
		if _, ok := expected[i].(time.Time); ok {
			// Since password_last_changed is set to default current_timestamp, we pass this check.
			continue
		}
		got := fmt.Sprintf("%v", row[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		require.Equal(t, need, got, i)
	}
}

func TestWriteDDLTableVersionToMySQLTiDB(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	ddlTableVer, err := m.GetDDLTableVersion()
	require.NoError(t, err)

	// Verify that 'ddl_table_version' has been set to the correct value
	se := session.CreateSessionAndSetID(t, store)
	r := session.MustExecToRecodeSet(t, se, fmt.Sprintf(`SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='%s'`, session.TiDBDDLTableVersionForTest))
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, fmt.Appendf(nil, "%d", ddlTableVer), req.GetRow(0).GetBytes(0))
	require.NoError(t, r.Close())
	dom.Close()
}

func TestTiDBHistoryTableConsistent(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	se := session.CreateSessionAndSetID(t, store)
	query := `select (select group_concat(column_name) from information_schema.columns where table_name='tidb_background_subtask' order by ordinal_position)
	               = (select group_concat(column_name) from information_schema.columns where table_name='tidb_background_subtask_history' order by ordinal_position);`
	r := session.MustExecToRecodeSet(t, se, query)
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, int64(1), row.GetInt64(0))

	query = `select (select group_concat(column_name) from information_schema.columns where table_name='tidb_global_task' order by ordinal_position)
	              = (select group_concat(column_name) from information_schema.columns where table_name='tidb_global_task_history' order by ordinal_position);`
	r = session.MustExecToRecodeSet(t, se, query)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, int64(1), row.GetInt64(0))

	dom.Close()
}

func TestANSISQLMode(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := session.CreateSessionAndSetID(t, store)

	session.MustExec(t, se, "USE mysql")
	session.MustExec(t, se, `set @@global.sql_mode="NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI"`)
	session.MustExec(t, se, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	store.SetOption(session.StoreBootstrappedKey, nil)
	se.Close()

	// Do some clean up, BootstrapSession will not create a new domain otherwise.
	dom.Close()

	// Set ANSI sql_mode and bootstrap again, to cover a bugfix.
	// Once we have a SQL like that:
	// select variable_value from mysql.tidb where variable_name = "system_tz"
	// it fails to execute in the ANSI sql_mode, and makes TiDB cluster fail to bootstrap.
	dom1, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom1.Close()
	se = session.CreateSessionAndSetID(t, store)
	session.MustExec(t, se, "select @@global.sql_mode")
	se.Close()
}

func TestStmtSummary(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := session.CreateSessionAndSetID(t, store)

	r := session.MustExecToRecodeSet(t, se, "select variable_value from mysql.global_variables where variable_name='tidb_enable_stmt_summary'")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	row := req.GetRow(0)
	require.Equal(t, []byte("ON"), row.GetBytes(0))
	require.NoError(t, r.Close())
}

func TestReferencesPrivilegeOnColumn(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := session.CreateSessionAndSetID(t, store)

	defer func() {
		session.MustExec(t, se, "drop user if exists issue28531")
		session.MustExec(t, se, "drop table if exists t1")
	}()

	session.MustExec(t, se, "create user if not exists issue28531")
	session.MustExec(t, se, "use test")
	session.MustExec(t, se, "drop table if exists t1")
	session.MustExec(t, se, "create table t1 (a int)")
	session.MustExec(t, se, "GRANT select (a), update (a),insert(a), references(a) on t1 to issue28531")
}

func TestTiDBEnablePagingVariable(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	se := session.CreateSessionAndSetID(t, store)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	for _, sql := range []string{
		"select @@global.tidb_enable_paging",
		"select @@session.tidb_enable_paging",
	} {
		r := session.MustExecToRecodeSet(t, se, sql)
		require.NotNil(t, r)

		req := r.NewChunk(nil)
		err := r.Next(context.Background(), req)
		require.NoError(t, err)
		require.NotEqual(t, 0, req.NumRows())

		rows := statistics.RowToDatums(req.GetRow(0), r.Fields())
		if vardef.DefTiDBEnablePaging {
			match(t, rows, "1")
		} else {
			match(t, rows, "0")
		}
		r.Close()
	}
}

func TestDDLTableCreateDDLNotifierTable(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := session.CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	ver, err := m.GetDDLTableVersion()
	require.NoError(t, err)
	require.GreaterOrEqual(t, ver, meta.DDLNotifierTableVersion)

	// downgrade DDL table version
	m.SetDDLTableVersion(meta.BackfillTableVersion)
	session.MustExec(t, se, "drop table mysql.tidb_ddl_notifier")
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// to upgrade session for create ddl notifier table
	dom.Close()
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)

	se = session.CreateSessionAndSetID(t, store)
	session.MustExec(t, se, "select * from mysql.tidb_ddl_notifier")
	dom.Close()
}

func TestIssue17979_1(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	// test issue 20900, upgrade from v3.0 to v4.0.11+
	seV3 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(58))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV3, 58)
	session.MustExec(t, seV3, "delete from mysql.tidb where variable_name='default_oom_action'")
	session.MustExec(t, seV3, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(58), ver)
	dom.Close()
	domV4, err := session.BootstrapSession(store)
	require.NoError(t, err)
	seV4 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r := session.MustExecToRecodeSet(t, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, vardef.OOMActionLog, req.GetRow(0).GetString(0))
	domV4.Close()
}

func TestIssue17979_2(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// test issue 20900, upgrade from v4.0.11 to v4.0.11
	seV3 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(59))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV3, 59)
	session.MustExec(t, seV3, "delete from mysql.tidb where variable_name='default_iim_action'")
	session.MustExec(t, seV3, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(59), ver)
	dom.Close()
	domV4, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domV4.Close()
	seV4 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r := session.MustExecToRecodeSet(t, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
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
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// test issue 20900, upgrade from v4.0.8 to v4.0.9+
	seV3 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(52))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV3, 52)
	session.MustExec(t, seV3, "delete from mysql.tidb where variable_name='default_memory_quota_query'")
	session.MustExec(t, seV3, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(52), ver)
	dom.Close()
	domV4, err := session.BootstrapSession(store)
	require.NoError(t, err)
	seV4 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r := session.MustExecToRecodeSet(t, seV4, "select @@tidb_mem_quota_query")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "1073741824", req.GetRow(0).GetString(0))
	require.Equal(t, int64(1073741824), seV4.GetSessionVars().MemQuotaQuery)
	r = session.MustExecToRecodeSet(t, seV4, "select variable_value from mysql.tidb where variable_name='default_memory_quota_query'")
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 0, req.NumRows())
	domV4.Close()
}

func TestUpgradeClusteredIndexDefaultValue(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV67 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(67))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV67, 67)
	session.MustExec(t, seV67, "UPDATE mysql.global_variables SET VARIABLE_VALUE = 'OFF' where VARIABLE_NAME = 'tidb_enable_clustered_index'")
	require.Equal(t, uint64(1), seV67.GetSessionVars().StmtCtx.AffectedRows())
	session.MustExec(t, seV67, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV67)
	require.NoError(t, err)
	require.Equal(t, int64(67), ver)
	dom.Close()

	domV68, err := session.BootstrapSession(store)
	require.NoError(t, err)
	seV68 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV68)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	r := session.MustExecToRecodeSet(t, seV68, `select @@global.tidb_enable_clustered_index, @@session.tidb_enable_clustered_index`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "ON", row.GetString(0))
	require.Equal(t, "ON", row.GetString(1))
	domV68.Close()
}

func TestAnalyzeVersionUpgradeFrom300To500(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 5.1+ or above.
	ver300 := 33
	seV3 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV3, ver300)
	session.MustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBAnalyzeVersion))
	session.MustExec(t, seV3, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check tidb_analyze_version should not exist.
	res := session.MustExecToRecodeSet(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBAnalyzeVersion))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// We are now in version no lower than 5.x, tidb_enable_index_merge should be 1.
	res = session.MustExecToRecodeSet(t, seCurVer, "select @@tidb_analyze_version")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "1", row.GetString(0))
}

func TestIndexMergeUpgradeFrom300To540(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 5.4+.
	ver300 := 33
	seV3 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV3, ver300)
	session.MustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableIndexMerge))
	session.MustExec(t, seV3, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check tidb_enable_index_merge should not exist.
	res := session.MustExecToRecodeSet(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableIndexMerge))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// We are now in 5.x, tidb_enable_index_merge should be off.
	res = session.MustExecToRecodeSet(t, seCurVer, "select @@tidb_enable_index_merge")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(0), row.GetInt64(0))
}

// We set tidb_enable_index_merge as on.
// And after upgrade to 5.x, tidb_enable_index_merge should remains to be on.
func TestIndexMergeUpgradeFrom400To540Enable(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	testIndexMergeUpgradeFrom400To540(t, true)
}

func TestIndexMergeUpgradeFrom400To540Disable(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}
	testIndexMergeUpgradeFrom400To540(t, false)
}

func testIndexMergeUpgradeFrom400To540(t *testing.T, enable bool) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 4.0.0 to 5.4+.
	ver400 := 46
	seV4 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver400))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV4, ver400)
	session.MustExec(t, seV4, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", vardef.Off, vardef.TiDBEnableIndexMerge))
	session.MustExec(t, seV4, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, int64(ver400), ver)

	// We are now in 4.0.0, tidb_enable_index_merge is off.
	res := session.MustExecToRecodeSet(t, seV4, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableIndexMerge))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, vardef.Off, row.GetString(1))

	if enable {
		// For the first time, We set tidb_enable_index_merge as on.
		// And after upgrade to 5.x, tidb_enable_index_merge should remains to be on.
		// For the second it should be off.
		session.MustExec(t, seV4, "set global tidb_enable_index_merge = on")
	}
	dom.Close()
	// Upgrade to 5.x.
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// We are now in 5.x, tidb_enable_index_merge should be on because we enable it in 4.0.0.
	res = session.MustExecToRecodeSet(t, seCurVer, "select @@tidb_enable_index_merge")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	if enable {
		require.Equal(t, int64(1), row.GetInt64(0))
	} else {
		require.Equal(t, int64(0), row.GetInt64(0))
	}
}

func TestTiDBOptRangeMaxSizeWhenUpgrading(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from v6.3.0 to v6.4.0+.
	ver94 := 94
	seV630 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver94))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV630, ver94)
	session.MustExec(t, seV630, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBOptRangeMaxSize))
	session.MustExec(t, seV630, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV630)
	require.NoError(t, err)
	require.Equal(t, int64(ver94), ver)

	// We are now in 6.3.0, check tidb_opt_range_max_size should not exist.
	res := session.MustExecToRecodeSet(t, seV630, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBOptRangeMaxSize))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// We are now in version no lower than v6.4.0, tidb_opt_range_max_size should be 0.
	res = session.MustExecToRecodeSet(t, seCurVer, "select @@session.tidb_opt_range_max_size")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "0", row.GetString(0))

	res = session.MustExecToRecodeSet(t, seCurVer, "select @@global.tidb_opt_range_max_size")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "0", row.GetString(0))
}

func TestTiDBOptAdvancedJoinHintWhenUpgrading(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from v6.6.0 to v7.0.0+.
	ver134 := 134
	seV660 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver134))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV660, ver134)
	session.MustExec(t, seV660, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBOptAdvancedJoinHint))
	session.MustExec(t, seV660, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV660)
	require.NoError(t, err)
	require.Equal(t, int64(ver134), ver)

	// We are now in 6.6.0, check tidb_opt_advanced_join_hint should not exist.
	res := session.MustExecToRecodeSet(t, seV660, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBOptAdvancedJoinHint))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// We are now in version no lower than v7.0.0, tidb_opt_advanced_join_hint should be false.
	res = session.MustExecToRecodeSet(t, seCurVer, "select @@session.tidb_opt_advanced_join_hint;")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(0), row.GetInt64(0))

	res = session.MustExecToRecodeSet(t, seCurVer, "select @@global.tidb_opt_advanced_join_hint;")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(0), row.GetInt64(0))
}

func TestTiDBCostModelUpgradeFrom300To650(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 3.0.0 to 6.5+.
	ver300 := 33
	seV3 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver300))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV3, ver300)
	session.MustExec(t, seV3, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBCostModelVersion))
	session.MustExec(t, seV3, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(ver300), ver)

	// We are now in 3.0.0, check TiDBCostModelVersion should not exist.
	res := session.MustExecToRecodeSet(t, seV3, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBCostModelVersion))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())

	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// We are now in 6.5+, TiDBCostModelVersion should be 1.
	res = session.MustExecToRecodeSet(t, seCurVer, "select @@tidb_cost_model_version")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, "1", row.GetString(0))
}

func TestTiDBCostModelUpgradeFrom610To650(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	for i := range 2 {
		func() {
			ctx := context.Background()
			store, dom := session.CreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			// upgrade from 6.1 to 6.5+.
			ver61 := 91
			seV61 := session.CreateSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMutator(txn)
			err = m.FinishBootstrap(int64(ver61))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			session.RevertVersionAndVariables(t, seV61, ver61)
			session.MustExec(t, seV61, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "1", vardef.TiDBCostModelVersion))
			session.MustExec(t, seV61, "commit")
			store.SetOption(session.StoreBootstrappedKey, nil)
			ver, err := session.GetBootstrapVersion(seV61)
			require.NoError(t, err)
			require.Equal(t, int64(ver61), ver)

			// We are now in 6.1, tidb_cost_model_version is 1.
			res := session.MustExecToRecodeSet(t, seV61, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBCostModelVersion))
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
				session.MustExec(t, seV61, "set global tidb_cost_model_version = 2")
			}
			dom.Close()
			// Upgrade to 6.5.
			domCurVer, err := session.BootstrapSession(store)
			require.NoError(t, err)
			defer domCurVer.Close()
			seCurVer := session.CreateSessionAndSetID(t, store)
			ver, err = session.GetBootstrapVersion(seCurVer)
			require.NoError(t, err)
			require.Equal(t, session.CurrentBootstrapVersion, ver)

			// We are now in 6.5.
			res = session.MustExecToRecodeSet(t, seCurVer, "select @@tidb_cost_model_version")
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

func TestIndexJoinMultiPatternByUpgrade650To840(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// Upgrade from 6.5.0 to 8.4+ or above.
	ver650 := 109
	seV7 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver650))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV7, ver650)
	session.MustExec(t, seV7, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableINLJoinInnerMultiPattern))
	session.MustExec(t, seV7, "commit")
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV7)
	require.NoError(t, err)
	require.Equal(t, int64(ver650), ver)

	// We are now in 6.5.0, check tidb_enable_inl_join_inner_multi_pattern should not exist.
	res := session.MustExecToRecodeSet(t, seV7, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableINLJoinInnerMultiPattern))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// We are now in version no lower than 8.4, tidb_enable_inl_join_inner_multi_pattern be off.
	res = session.MustExecToRecodeSet(t, seCurVer, "select @@global.tidb_enable_inl_join_inner_multi_pattern")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, int64(0), row.GetInt64(0))
}
