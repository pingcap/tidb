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

//lint:file-ignore bootstrap test file uses actual schemas from parent package instead of literals

package session_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	sessionpkg "github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/teststore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func RunMySQLDBTables(t *testing.T) {
	require.Len(t, sessionpkg.GetSystemTablesOfBaseNextGenVersion(), 52, "DO NOT CHANGE IT")
	for _, verBoot := range sessionpkg.GetVersionedBootstrapSchemas() {
		for _, schInfo := range verBoot.Databases {
			testTableBasicInfoSlice(t, schInfo.Tables, "IF NOT EXISTS mysql.%s (")
		}
	}
	reservedIDs := make([]int64, 0, len(sessionpkg.GetExportedDDLTableVersionTables())*2)
	for _, v := range sessionpkg.GetExportedDDLTableVersionTables() {
		for _, tbl := range v.Tables {
			reservedIDs = append(reservedIDs, tbl.ID)
		}
	}
	for _, verBoot := range sessionpkg.GetVersionedBootstrapSchemas() {
		for _, schInfo := range verBoot.Databases {
			for _, tblInfo := range sessionpkg.GetDatabaseBasicInfoTables(schInfo) {
				reservedIDs = append(reservedIDs, tblInfo.ID)
			}
		}
	}
	for _, db := range sessionpkg.GetSystemDatabases() {
		reservedIDs = append(reservedIDs, db.ID)
	}
	slices.Sort(reservedIDs)
	require.IsIncreasing(t, reservedIDs, "reserved IDs shouldn't be reused")
	require.Greater(t, reservedIDs[0], metadef.ReservedGlobalIDLowerBound, "reserved ID should be greater than ReservedGlobalIDLowerBound")
	require.LessOrEqual(t, reservedIDs[len(reservedIDs)-1], metadef.ReservedGlobalIDUpperBound, "reserved ID should be less than or equal to ReservedGlobalIDUpperBound")
}

// This test file have many problem.
// 1. Please use testkit to create dom, session and store.
// 2. Don't use sessionpkg.ExportedCreateStoreAndBootstrap and BootstrapSession together. It will cause data race.
// Please do not add any test here. You can add test case at the bootstrap_update_test.go. After All problem fixed,
// We will overwrite this file by update_test.go.
func RunBootstrap(t *testing.T) {
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedMustExec(t, se, "set global tidb_txn_mode=''")
	sessionpkg.ExportedMustExec(t, se, "use mysql")
	r := sessionpkg.ExportedMustExecToRecodeSet(t, se, "select * from user")
	require.NotNil(t, r)

	ctx := context.Background()
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())

	rows := statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", nil, nil, nil, "", "N", time.Now(), nil, 0)
	r.Close()

	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "anyhost"}, []byte(""), []byte(""), nil))

	sessionpkg.ExportedMustExec(t, se, "use test")

	// Check privilege tables.
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.global_priv")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.db")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.tables_priv")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.columns_priv")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.global_grants")

	// Check privilege tables.
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se, "SELECT COUNT(*) from mysql.global_variables")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, globalVarsCount(), req.GetRow(0).GetInt64(0))
	require.NoError(t, r.Close())

	// Check a storage operations are default autocommit after the second start.
	sessionpkg.ExportedMustExec(t, se, "USE test")
	sessionpkg.ExportedMustExec(t, se, "drop table if exists t")
	sessionpkg.ExportedMustExec(t, se, "create table t (id int)")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	se.Close()

	se, err = sessionpkg.ExportedCreateSession4Test(store)
	require.NoError(t, err)
	sessionpkg.ExportedMustExec(t, se, "USE test")
	sessionpkg.ExportedMustExec(t, se, "insert t values (?)", 3)

	se, err = sessionpkg.ExportedCreateSession4Test(store)
	require.NoError(t, err)
	sessionpkg.ExportedMustExec(t, se, "USE test")
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se, "select * from t")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	rows = statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, 3)
	sessionpkg.ExportedMustExec(t, se, "drop table if exists t")
	se.Close()

	// Try to do bootstrap dml jobs on an already bootstrapped TiDB system will not cause fatal.
	// For https://github.com/pingcap/tidb/issues/1096
	se, err = sessionpkg.ExportedCreateSession4Test(store)
	require.NoError(t, err)
	sessionpkg.ExportedDoDMLWorks(se)
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se, "select * from mysql.expr_pushdown_blacklist where name = 'date_add'")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	se.Close()
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se, fmt.Sprintf("select * from mysql.bind_info where original_sql = '%s'", bindinfo.BuiltinPseudoSQL4BindLock))
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
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
func RunBootstrapWithError(t *testing.T) {
	ctx := context.Background()
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// system tables are not created in `doDDLWorks` for next-gen.
	if kerneltype.IsNextGen() {
		require.NoError(t, sessionpkg.ExportedBootstrapSchemas(store))
	}
	// Init DDL tables BEFORE creating the session/domain, so the domain's
	// infoschema includes the DDL tables when the DDL submitter starts.
	err = sessionpkg.ExportedInitDDLTables(store)
	require.NoError(t, err)
	// bootstrap
	{
		se, err := sessionpkg.ExportedCreateSession4Test(store)
		require.NoError(t, err)
		sessionSe := sessionpkg.ExportedGetSession(se)
		require.NotNil(t, sessionSe)
		dom, err := sessionpkg.ExportedDomap().Get(store)
		require.NoError(t, err)
		require.NoError(t, dom.Start(ddl.Bootstrap))
		b, err := sessionpkg.ExportedCheckBootstrapped(se)
		require.False(t, b)
		require.NoError(t, err)
		sessionpkg.ExportedDoDDLWorks(se)
	}

	dom, err := sessionpkg.ExportedDomap().Get(store)
	require.NoError(t, err)
	dom.Close()

	dom1, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer dom1.Close()

	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedMustExec(t, se, "USE mysql")
	r := sessionpkg.ExportedMustExecToRecodeSet(t, se, `select * from user`)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())

	row := req.GetRow(0)
	rows := statistics.RowToDatums(row, r.Fields())
	match(t, rows, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", nil, nil, nil, "", "N", time.Now(), nil, 0)
	require.NoError(t, r.Close())

	sessionpkg.ExportedMustExec(t, se, "USE test")
	// Check privilege tables.
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.global_priv")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.db")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.tables_priv")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.columns_priv")
	// Check role tables.
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.role_edges")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.default_roles")
	// Check global variables.
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se, "SELECT COUNT(*) from mysql.global_variables")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	v := req.GetRow(0)
	require.Equal(t, globalVarsCount(), v.GetInt64(0))
	require.NoError(t, r.Close())

	r = sessionpkg.ExportedMustExecToRecodeSet(t, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="bootstrapped"`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, []byte("True"), row.GetBytes(0))
	require.NoError(t, r.Close())

	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.tidb_background_subtask")
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.tidb_background_subtask_history")

	// Check tidb_ttl_table_status table
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.tidb_ttl_table_status")
	// Check mysql.tidb_workload_values table
	sessionpkg.ExportedMustExec(t, se, "SELECT * from mysql.tidb_workload_values")
}

func RunDDLTableCreateBackfillTable(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/skipCheckReservedSchemaObjInNextGen", "return(true)")
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	ver, err := m.GetDDLTableVersion()
	require.NoError(t, err)
	require.GreaterOrEqual(t, ver, meta.BackfillTableVersion)

	// downgrade `mDDLTableVersion`
	require.NoError(t, m.SetDDLTableVersion(meta.MDLTableVersion))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	dom.Close()

	txn, err = store.Begin()
	require.NoError(t, err)
	m = meta.NewMutator(txn)
	systemDBID, err := m.GetSystemDBID()
	require.NoError(t, err)
	require.NoError(t, m.DropTableOrView(systemDBID, metadef.TiDBBackgroundSubtaskTableID))
	require.NoError(t, m.DropTableOrView(systemDBID, metadef.TiDBBackgroundSubtaskHistoryTableID))
	// TODO(lance6716): remove it after tidb_ddl_notifier GA
	require.NoError(t, m.DropTableOrView(systemDBID, metadef.TiDBDDLNotifierTableID))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// to upgrade session for create ddl related tables
	dom.Close()
	dom, err = sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)

	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedMustExec(t, se, "select * from mysql.tidb_background_subtask")
	sessionpkg.ExportedMustExec(t, se, "select * from mysql.tidb_background_subtask_history")
	dom.Close()
}

// TestUpgrade tests upgrading
func RunUpgrade(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()

	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)

	sessionpkg.ExportedMustExec(t, se, "USE mysql")

	// bootstrap with sessionpkg.CurrentBootstrapVersion
	r := sessionpkg.ExportedMustExecToRecodeSet(t, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	row := req.GetRow(0)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	require.Equal(t, 1, row.Len())
	require.Equal(t, fmt.Appendf(nil, "%d", sessionpkg.CurrentBootstrapVersion), row.GetBytes(0))
	require.NoError(t, r.Close())

	se1 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err := sessionpkg.GetBootstrapVersion(se1)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// Do something to downgrade the store.
	// downgrade meta bootstrap version
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(1))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedMustExec(t, se1, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	sessionpkg.ExportedMustExec(t, se1, "update mysql.global_variables set variable_value='off' where variable_name='tidb_enable_dist_task'")
	sessionpkg.ExportedMustExec(t, se1, fmt.Sprintf(`delete from mysql.global_variables where VARIABLE_NAME="%s"`, vardef.TiDBDistSQLScanConcurrency))
	sessionpkg.ExportedMustExec(t, se1, `commit`)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	sessionpkg.ExportedRevertVersionAndVariables(t, se1, 0)
	// Make sure the version is downgraded.
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se1, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	require.NoError(t, r.Close())

	ver, err = sessionpkg.GetBootstrapVersion(se1)
	require.NoError(t, err)
	require.Equal(t, int64(0), ver)
	dom.Close()
	// Create a new session then upgrade() will run automatically.
	dom, err = sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)

	se2 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se2, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version"`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Equal(t, fmt.Appendf(nil, "%d", sessionpkg.CurrentBootstrapVersion), row.GetBytes(0))
	require.NoError(t, r.Close())

	ver, err = sessionpkg.GetBootstrapVersion(se2)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// Verify that 'new_collation_enabled' is false.
	r = sessionpkg.ExportedMustExecToRecodeSet(t, se2, fmt.Sprintf(`SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='%s'`, sessionpkg.GetTidbNewCollationEnabled()))
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, "False", req.GetRow(0).GetString(0))
	require.NoError(t, r.Close())

	r = sessionpkg.ExportedMustExecToRecodeSet(t, se2, "admin show ddl jobs 1000;")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	rowCnt := req.NumRows()
	for i := range rowCnt {
		jobType := req.GetRow(i).GetString(3) // get job type.
		// Should not use multi-schema change in bootstrap DDL because the job arguments may be changed.
		require.False(t, strings.Contains(jobType, "multi-schema"))
	}
	require.NoError(t, r.Close())

	dom.Close()
}

func RunOldPasswordUpgrade(t *testing.T) {
	pwd := "abc"
	oldpwd := fmt.Sprintf("%X", auth.Sha1Hash([]byte(pwd)))
	newpwd, err := sessionpkg.ExportedOldPasswordUpgrade(oldpwd)
	require.NoError(t, err)
	require.Equal(t, "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E", newpwd)
}

func RunBootstrapInitExpensiveQueryHandle(t *testing.T) {
	store, _ := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	se, err := sessionpkg.ExportedCreateSession(store)
	require.NoError(t, err)
	dom := domain.GetDomain(se)
	require.NotNil(t, dom)
	defer dom.Close()
	require.NotNil(t, dom.ExpensiveQueryHandle())
}

func RunForIssue23387(t *testing.T) {
	// For issue https://github.com/pingcap/tidb/issues/23387
	saveCurrentBootstrapVersion := sessionpkg.CurrentBootstrapVersion
	sessionpkg.CurrentBootstrapVersion = sessionpkg.GetVersion57()

	// Bootstrap to an old version, create a user.
	store, err := teststore.NewMockStoreWithoutBootstrap()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)

	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"), nil)
	sessionpkg.ExportedMustExec(t, se, "create user quatest")
	dom.Close()
	// Upgrade to a newer version, check the user's privilege.
	sessionpkg.CurrentBootstrapVersion = saveCurrentBootstrapVersion
	dom, err = sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	se = sessionpkg.ExportedCreateSessionAndSetID(t, store)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"), nil)
	rs, err := sessionpkg.ExportedExec(se, "show grants for quatest")
	require.NoError(t, err)
	rows, err := sessionpkg.ExportedResultSetToStringSlice(context.Background(), se, rs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "GRANT USAGE ON *.* TO 'quatest'@'%'", rows[0][0])
}

func RunIndexMergeInNewCluster(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	// Indicates we are in a new cluster.
	require.Equal(t, sessionpkg.GetNotBootstrapped(), sessionpkg.GetStoreBootstrapVersionWithCache(store))
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)

	// In a new created cluster(above 5.4+), tidb_enable_index_merge is 1 by default.
	sessionpkg.ExportedMustExec(t, se, "use test;")
	r := sessionpkg.ExportedMustExecToRecodeSet(t, se, "select @@tidb_enable_index_merge;")
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

func RunTiDBOptAdvancedJoinHintInNewCluster(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	// Indicates we are in a new cluster.
	require.Equal(t, sessionpkg.GetNotBootstrapped(), sessionpkg.GetStoreBootstrapVersionWithCache(store))
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)

	// In a new created cluster(above 7.0+), tidb_opt_advanced_join_hint is true by default.
	sessionpkg.ExportedMustExec(t, se, "use test;")
	r := sessionpkg.ExportedMustExecToRecodeSet(t, se, "select @@tidb_opt_advanced_join_hint;")
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

func RunTiDBCostModelInNewCluster(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	// Indicates we are in a new cluster.
	require.Equal(t, sessionpkg.GetNotBootstrapped(), sessionpkg.GetStoreBootstrapVersionWithCache(store))
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)

	// In a new created cluster(above 6.5+), tidb_cost_model_version is 2 by default.
	sessionpkg.ExportedMustExec(t, se, "use test;")
	r := sessionpkg.ExportedMustExecToRecodeSet(t, se, "select @@tidb_cost_model_version;")
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

func RunTiDBGCAwareUpgradeFrom630To650(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.3 to 6.5+.
	ver63 := sessionpkg.GetVersion93()
	seV63 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver63)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV63, ver63)
	sessionpkg.ExportedMustExec(t, seV63, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "1", vardef.TiDBEnableGCAwareMemoryTrack))
	sessionpkg.ExportedMustExec(t, seV63, "commit")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV63)
	require.NoError(t, err)
	require.Equal(t, ver63, ver)

	// We are now in 6.3, tidb_enable_gc_aware_memory_track is ON.
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV63, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableGCAwareMemoryTrack))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "1", row.GetString(1))

	// Upgrade to 6.5.
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// We are now in 6.5.
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableGCAwareMemoryTrack))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))
}

func RunTiDBServerMemoryLimitUpgradeTo651_1(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.5.0 to 6.5.1+.
	ver132 := sessionpkg.GetVersion132()
	seV132 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver132)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV132, ver132)
	sessionpkg.ExportedMustExec(t, seV132, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", vardef.TiDBServerMemoryLimit))
	sessionpkg.ExportedMustExec(t, seV132, "commit")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV132)
	require.NoError(t, err)
	require.Equal(t, ver132, ver)

	// We are now in 6.5.0, tidb_server_memory_limit is 0.
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV132, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBServerMemoryLimit))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))

	// Upgrade to 6.5.1+.
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// We are now in 6.5.1+.
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBServerMemoryLimit))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, vardef.DefTiDBServerMemoryLimit, row.GetString(1))
}

func RunTiDBServerMemoryLimitUpgradeTo651_2(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.5.0 to 6.5.1+.
	ver132 := sessionpkg.GetVersion132()
	seV132 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver132)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV132, ver132)
	sessionpkg.ExportedMustExec(t, seV132, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "70%", vardef.TiDBServerMemoryLimit))
	sessionpkg.ExportedMustExec(t, seV132, "commit")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV132)
	require.NoError(t, err)
	require.Equal(t, ver132, ver)

	// We are now in 6.5.0, tidb_server_memory_limit is "70%".
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV132, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBServerMemoryLimit))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "70%", row.GetString(1))

	// Upgrade to 6.5.1+.
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// We are now in 6.5.1+.
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBServerMemoryLimit))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "70%", row.GetString(1))
}

func RunTiDBGlobalVariablesDefaultValueUpgradeFrom630To660(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.3.0 to 6.6.0.
	ver630 := sessionpkg.GetVersion93()
	seV630 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver630)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV630, ver630)
	sessionpkg.ExportedMustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", vardef.TiDBEnableForeignKey))
	sessionpkg.ExportedMustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", vardef.ForeignKeyChecks))
	sessionpkg.ExportedMustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", vardef.TiDBEnableHistoricalStats))
	sessionpkg.ExportedMustExec(t, seV630, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "OFF", vardef.TiDBEnablePlanReplayerCapture))
	sessionpkg.ExportedMustExec(t, seV630, "commit")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV630)
	require.NoError(t, err)
	require.Equal(t, ver630, ver)

	// We are now in 6.3.0.
	upgradeVars := []string{vardef.TiDBEnableForeignKey, vardef.ForeignKeyChecks, vardef.TiDBEnableHistoricalStats, vardef.TiDBEnablePlanReplayerCapture}
	varsValueList := []string{"OFF", "OFF", "OFF", "OFF"}
	for i := range upgradeVars {
		res := sessionpkg.ExportedMustExecToRecodeSet(t, seV630, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", upgradeVars[i]))
		chk := res.NewChunk(nil)
		err = res.Next(ctx, chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		row := chk.GetRow(0)
		require.Equal(t, 2, row.Len())
		require.Equal(t, varsValueList[i], row.GetString(1))
	}

	// Upgrade to 6.6.0.
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seV660 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seV660)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// We are now in 6.6.0.
	varsValueList = []string{"ON", "ON", "ON", "ON"}
	for i := range upgradeVars {
		res := sessionpkg.ExportedMustExecToRecodeSet(t, seV660, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", upgradeVars[i]))
		chk := res.NewChunk(nil)
		err = res.Next(ctx, chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		row := chk.GetRow(0)
		require.Equal(t, 2, row.Len())
		require.Equal(t, varsValueList[i], row.GetString(1))
	}
}

func RunTiDBStoreBatchSizeUpgradeFrom650To660(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	for i := range 2 {
		func() {
			ctx := context.Background()
			store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			// upgrade from 6.5 to 6.6.
			ver65 := sessionpkg.GetVersion132()
			seV65 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMutator(txn)
			err = m.FinishBootstrap(ver65)
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			sessionpkg.ExportedRevertVersionAndVariables(t, seV65, ver65)
			sessionpkg.ExportedMustExec(t, seV65, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", vardef.TiDBStoreBatchSize))
			sessionpkg.ExportedMustExec(t, seV65, "commit")
			store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
			ver, err := sessionpkg.GetBootstrapVersion(seV65)
			require.NoError(t, err)
			require.Equal(t, ver65, ver)

			// We are now in 6.5, tidb_store_batch_size is 0.
			res := sessionpkg.ExportedMustExecToRecodeSet(t, seV65, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBStoreBatchSize))
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
				sessionpkg.ExportedMustExec(t, seV65, "set global tidb_store_batch_size = 1")
			}
			dom.Close()
			// Upgrade to 6.6.
			domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
			require.NoError(t, err)
			defer domCurVer.Close()
			seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
			ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
			require.NoError(t, err)
			require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

			// We are now in 6.6.
			res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, "select @@tidb_store_batch_size")
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

func RunTiDBUpgradeToVer136(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver135 := sessionpkg.GetVersion135()
	seV135 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver135)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV135, ver135)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV135)
	require.NoError(t, err)
	require.Equal(t, ver135, ver)

	sessionpkg.ExportedMustExec(t, seV135, "ALTER TABLE mysql.tidb_background_subtask DROP INDEX idx_task_key;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/reorgMetaRecordFastReorgDisabled", `return`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/reorgMetaRecordFastReorgDisabled"))
	})
	sessionpkg.ExportedMustExec(t, seV135, "set global tidb_ddl_enable_fast_reorg = 1")
	do.Close()
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	ver, err = sessionpkg.GetBootstrapVersion(seV135)
	require.NoError(t, err)
	require.True(t, ddl.LastReorgMetaFastReorgDisabled)

	require.Less(t, ver135, ver)
	dom.Close()
}

func RunTiDBUpgradeToVer140(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver139 := sessionpkg.GetVersion139()
	resetTo139 := func(s sessionapi.Session) {
		txn, err := store.Begin()
		require.NoError(t, err)
		m := meta.NewMutator(txn)
		err = m.FinishBootstrap(ver139)
		require.NoError(t, err)
		sessionpkg.ExportedRevertVersionAndVariables(t, s, ver139)
		err = txn.Commit(context.Background())
		require.NoError(t, err)

		store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
		ver, err := sessionpkg.GetBootstrapVersion(s)
		require.NoError(t, err)
		require.Equal(t, ver139, ver)
	}
	checkUpgraded := func() {
		s := sessionpkg.ExportedCreateSessionAndSetID(t, store)
		defer s.Close()
		ver, err := sessionpkg.GetBootstrapVersion(s)
		require.NoError(t, err)
		require.Less(t, ver139, ver)
	}

	// drop column task_key and then upgrade
	s := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedMustExec(t, s, "alter table mysql.tidb_global_task drop column task_key")
	resetTo139(s)
	s.Close()
	do.Close()
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	checkUpgraded()

	// Create the reset session while the current domain is still alive; sessions
	// bound to a closed domain can fail schema validation on commit.
	s = sessionpkg.ExportedCreateSessionAndSetID(t, store)
	resetTo139(s)
	s.Close()
	dom.Close()
	dom, err = sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	checkUpgraded()
	dom.Close()
}

func RunTiDBNonPrepPlanCacheUpgradeFrom540To700(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// bootstrap to 5.4
	ver54 := sessionpkg.GetVersion82()
	seV54 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver54)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV54, ver54)
	sessionpkg.ExportedMustExec(t, seV54, fmt.Sprintf("delete from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableNonPreparedPlanCache))
	sessionpkg.ExportedMustExec(t, seV54, "commit")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV54)
	require.NoError(t, err)
	require.Equal(t, ver54, ver)

	// We are now in 5.4, check TiDBCostModelVersion should not exist.
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV54, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableNonPreparedPlanCache))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())

	// Upgrade to 7.0
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// We are now in 7.0
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBEnableNonPreparedPlanCache))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "OFF", row.GetString(1)) // tidb_enable_non_prepared_plan_cache = off

	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBNonPreparedPlanCacheSize))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "100", row.GetString(1)) // tidb_non_prepared_plan_cache_size = 100
}

func RunTiDBStatsLoadPseudoTimeoutUpgradeFrom610To650(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 6.1 to 6.5+.
	ver61 := sessionpkg.GetVersion91()
	seV61 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver61)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV61, ver61)
	sessionpkg.ExportedMustExec(t, seV61, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", vardef.TiDBStatsLoadPseudoTimeout))
	sessionpkg.ExportedMustExec(t, seV61, "commit")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV61)
	require.NoError(t, err)
	require.Equal(t, ver61, ver)

	// We are now in 6.1, tidb_stats_load_pseudo_timeout is OFF.
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV61, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBStatsLoadPseudoTimeout))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))

	// Upgrade to 6.5.
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// We are now in 6.5.
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBStatsLoadPseudoTimeout))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "1", row.GetString(1))
}

func RunTiDBTiDBOptTiDBOptimizerEnableNAAJWhenUpgradingToVer138(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver137 := sessionpkg.GetVersion137()
	seV137 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver137)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV137, ver137)
	sessionpkg.ExportedMustExec(t, seV137, "update mysql.GLOBAL_VARIABLES set variable_value='OFF' where variable_name='tidb_enable_null_aware_anti_join'")
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV137)
	require.NoError(t, err)
	require.Equal(t, ver137, ver)

	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV137, "select * from mysql.GLOBAL_VARIABLES where variable_name='tidb_enable_null_aware_anti_join'")
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "OFF", row.GetString(1))

	// Upgrade to version 138.
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, "select * from mysql.GLOBAL_VARIABLES where variable_name='tidb_enable_null_aware_anti_join'")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "ON", row.GetString(1))
}

func RunTiDBUpgradeToVer143(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver142 := sessionpkg.GetVersion142()
	seV142 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver142)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV142, ver142)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV142)
	require.NoError(t, err)
	require.Equal(t, ver142, ver)

	do.Close()
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	ver, err = sessionpkg.GetBootstrapVersion(seV142)
	require.NoError(t, err)
	require.Less(t, ver142, ver)
	dom.Close()
}

func RunTiDBLoadBasedReplicaReadThresholdUpgradingToVer141(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// upgrade from 7.0 to 7.1.
	ver70 := sessionpkg.GetVersion139()
	seV70 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver70)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV70, ver70)
	sessionpkg.ExportedMustExec(t, seV70, fmt.Sprintf("update mysql.GLOBAL_VARIABLES set variable_value='%s' where variable_name='%s'", "0", vardef.TiDBLoadBasedReplicaReadThreshold))
	sessionpkg.ExportedMustExec(t, seV70, "commit")
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV70)
	require.NoError(t, err)
	require.Equal(t, ver70, ver)

	// We are now in 7.0, tidb_load_based_replica_read_threshold is 0.
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV70, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBLoadBasedReplicaReadThreshold))
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "0", row.GetString(1))

	// Upgrade to 7.1.
	do.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// We are now in 7.1.
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, fmt.Sprintf("select * from mysql.GLOBAL_VARIABLES where variable_name='%s'", vardef.TiDBLoadBasedReplicaReadThreshold))
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "1s", row.GetString(1))
}

func RunTiDBPlanCacheInvalidationOnFreshStatsWhenUpgradingToVer144(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// bootstrap as sessionpkg.GetVersion143()
	ver143 := sessionpkg.GetVersion143()
	seV143 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver143)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV143, ver143)
	// simulate a real ver143 where `tidb_plan_cache_invalidation_on_fresh_stats` doesn't exist yet
	sessionpkg.ExportedMustExec(t, seV143, "delete from mysql.GLOBAL_VARIABLES where variable_name='tidb_plan_cache_invalidation_on_fresh_stats'")
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)

	// upgrade to ver144
	do.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err := sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// the value in the table is set to OFF automatically
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, "select * from mysql.GLOBAL_VARIABLES where variable_name='tidb_plan_cache_invalidation_on_fresh_stats'")
	chk := res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, "OFF", row.GetString(1))

	// the session and global variable is also OFF
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, "select @@session.tidb_plan_cache_invalidation_on_fresh_stats, @@global.tidb_plan_cache_invalidation_on_fresh_stats")
	chk = res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, int64(0), row.GetInt64(0))
	require.Equal(t, int64(0), row.GetInt64(1))
}

func RunTiDBUpgradeToVer145(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ver144 := sessionpkg.GetVersion144()
	seV144 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver144)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV144, ver144)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV144)
	require.NoError(t, err)
	require.Equal(t, ver144, ver)

	do.Close()
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	ver, err = sessionpkg.GetBootstrapVersion(seV144)
	require.NoError(t, err)
	require.Less(t, ver144, ver)
	dom.Close()
}

func RunTiDBUpgradeToVer170(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ver169 := sessionpkg.GetVersion169()
	seV169 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver169)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV169, ver169)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV169)
	require.NoError(t, err)
	require.Equal(t, ver169, ver)

	do.Close()
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	ver, err = sessionpkg.GetBootstrapVersion(seV169)
	require.NoError(t, err)
	require.Less(t, ver169, ver)
	dom.Close()
}

func RunTiDBUpgradeToVer176(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ver175 := sessionpkg.GetVersion175()
	seV175 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver175)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV175, ver175)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV175)
	require.NoError(t, err)
	require.Equal(t, ver175, ver)

	do.Close()
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	ver, err = sessionpkg.GetBootstrapVersion(seV175)
	require.NoError(t, err)
	require.Less(t, ver175, ver)
	// Avoid reusing the old session when checking the new table.
	// Otherwise it may access the previous domain, which has already been closed.
	newSession := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedMustExec(t, newSession, "SELECT * from mysql.tidb_global_task_history")
	dom.Close()
}

func RunTiDBUpgradeToVer177(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, do := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ver176 := sessionpkg.GetVersion176()
	seV176 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver176)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV176, ver176)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV176)
	require.NoError(t, err)
	require.Equal(t, ver176, ver)

	do.Close()
	dom, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	ver, err = sessionpkg.GetBootstrapVersion(seV176)
	require.NoError(t, err)
	require.Less(t, ver176, ver)
	// Avoid reusing the old session when checking the new table.
	// Otherwise it may access the previous domain, which has already been closed.
	newSession := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedMustExec(t, newSession, "SELECT * from mysql.dist_framework_meta")
	dom.Close()
}

func RunTiDBUpgradeToVer209(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// bootstrap as sessionpkg.GetVersion198(), version 199~208 is reserved for v8.1.x bugfix patch.
	ver198 := sessionpkg.GetVersion198()
	seV198 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver198)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV198, ver198)
	// simulate a real ver198 where `tidb_resource_control_strict_mode` doesn't exist yet
	sessionpkg.ExportedMustExec(t, seV198, "delete from mysql.GLOBAL_VARIABLES where variable_name='tidb_resource_control_strict_mode'")
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)

	// upgrade to ver209
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err := sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// the value in the table is set to OFF automatically
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, "select * from mysql.GLOBAL_VARIABLES where variable_name='tidb_resource_control_strict_mode'")
	chk := res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, "OFF", row.GetString(1))

	// the global variable is also OFF
	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, "select @@global.tidb_resource_control_strict_mode")
	chk = res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, int64(0), row.GetInt64(0))
	require.Equal(t, false, vardef.EnableResourceControlStrictMode.Load())
}

func RunIssue61890(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/skipCheckReservedSchemaObjInNextGen", "return(true)")
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	s1 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedMustExec(t, s1, "drop table mysql.global_variables")
	sessionpkg.ExportedMustExec(t, s1, "create table mysql.global_variables(`VARIABLE_NAME` varchar(64) NOT NULL PRIMARY KEY clustered, `VARIABLE_VALUE` varchar(16383) DEFAULT NULL)")

	s2 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	sessionpkg.ExportedInitGlobalVariableIfNotExists(s2, vardef.TiDBEnableINLJoinInnerMultiPattern, vardef.Off)

	dom.Close()
}

func RunKeyspaceEtcdNamespace(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("keyspace is not supported in classic kernel")
	}
	keyspaceMeta := keyspacepb.KeyspaceMeta{}
	keyspaceMeta.Id = 2
	keyspaceMeta.Name = keyspace.System
	makeStore(t, &keyspaceMeta, true)
}

func RunNullKeyspaceEtcdNamespace(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("next-gen kernel doesn't have the NULL keyspace concept")
	}
	makeStore(t, nil, false)
}

func makeStore(t *testing.T, keyspaceMeta *keyspacepb.KeyspaceMeta, isHasPrefix bool) {
	integration.BeforeTestExternal(t)
	var store kv.Storage
	var err error
	if keyspaceMeta != nil {
		store, err = mockstore.NewMockStore(
			mockstore.WithCurrentKeyspaceMeta(keyspaceMeta),
			mockstore.WithStoreType(mockstore.EmbedUnistore),
		)
	} else {
		store, err = mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	}
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	// Build a mockEtcdBackend.
	mockStore := &mockEtcdBackend{
		Storage: store,
		pdAddrs: []string{cluster.Members[0].GRPCURL()}}
	etcdClient := cluster.RandClient()

	ctx := context.Background()
	require.NoError(t, ddl.StartOwnerManager(ctx, mockStore))
	t.Cleanup(func() {
		ddl.CloseOwnerManager(mockStore)
	})
	dom, err := sessionpkg.ExportedDomapGetWithEtcdClient(mockStore, etcdClient, nil)
	require.NoError(t, err)
	defer dom.Close()

	checkETCDNameSpace(t, dom, isHasPrefix)
}

func checkETCDNameSpace(t *testing.T, dom *domain.Domain, isHasPrefix bool) {
	namespacePrefix := keyspace.MakeKeyspaceEtcdNamespace(dom.Store().GetCodec())
	testKeyWithoutPrefix := "/testkey"
	testVal := "test"
	var expectTestKey string
	if isHasPrefix {
		expectTestKey = namespacePrefix + testKeyWithoutPrefix
	} else {
		expectTestKey = testKeyWithoutPrefix
	}

	// Put key value into etcd.
	_, err := dom.GetEtcdClient().Put(context.Background(), testKeyWithoutPrefix, testVal)
	require.NoError(t, err)

	// Use expectTestKey to get the key from etcd.
	getResp, err := dom.UnprefixedEtcdCli().Get(context.Background(), expectTestKey)
	require.NoError(t, err)
	require.Equal(t, len(getResp.Kvs), 1)

	if isHasPrefix {
		getResp, err = dom.UnprefixedEtcdCli().Get(context.Background(), testKeyWithoutPrefix)
		require.NoError(t, err)
		require.Equal(t, 0, len(getResp.Kvs))
	}
}

type mockEtcdBackend struct {
	kv.Storage
	pdAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return mebd.pdAddrs, nil
}

func (mebd *mockEtcdBackend) TLSConfig() *tls.Config { return nil }

func (mebd *mockEtcdBackend) StartGCWorker() error { return nil }

func RunTiDBUpgradeToVer240(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}
	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver239 := sessionpkg.GetVersion239()
	seV239 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver239)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV239, ver239)
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)

	// Check if the required indexes already exist in mysql.analyze_jobs (they are created by default in new clusters)
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV239, "show create table mysql.analyze_jobs")
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	require.Contains(t, string(chk.GetRow(0).GetBytes(1)), "idx_schema_table_state")
	require.Contains(t, string(chk.GetRow(0).GetBytes(1)), "idx_schema_table_partition_state")

	// Check that the indexes still exist after upgrading to the new version and that no errors occurred during the upgrade.
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err := sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	res = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, "show create table mysql.analyze_jobs")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	require.Contains(t, string(chk.GetRow(0).GetBytes(1)), "idx_schema_table_state")
	require.Contains(t, string(chk.GetRow(0).GetBytes(1)), "idx_schema_table_partition_state")
}

func RunTiDBUpgradeToVer252(t *testing.T) {
	// NOTE: this case needed to be passed in both classic and next-gen kernel.
	// in the first release of next-gen kernel, the version is 250.
	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver250 := sessionpkg.GetVersion250()
	seV250 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver250)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV250, ver250)
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)

	getBindInfoSQLFn := func(se sessionapi.Session) string {
		res := sessionpkg.ExportedMustExecToRecodeSet(t, se, "show create table mysql.bind_info")
		chk := res.NewChunk(nil)
		err = res.Next(ctx, chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		return string(chk.GetRow(0).GetBytes(1))
	}
	createTblSQL := getBindInfoSQLFn(seV250)
	require.Contains(t, createTblSQL, "`create_time` timestamp(6)")
	require.Contains(t, createTblSQL, "`update_time` timestamp(6)")
	// revert it back to timestamp(3) for testing. we must set below fields to
	// simulate the real session in upgrade process.
	seV250.SetValue(sessionctx.Initing, true)
	seV250.GetSessionVars().SQLMode = mysql.ModeNone
	res := sessionpkg.ExportedMustExecToRecodeSet(t, seV250, "select create_time,update_time from mysql.bind_info")
	chk := res.NewChunk(nil)
	err = res.Next(ctx, chk)
	for i := range chk.NumRows() {
		getTime := chk.GetRow(i).GetTime(0)
		getTime = chk.GetRow(i).GetTime(1)
		_ = getTime
	}
	sessionpkg.ExportedMustExecute(seV250, "alter table mysql.bind_info modify create_time timestamp(3)")
	sessionpkg.ExportedMustExecute(seV250, "alter table mysql.bind_info modify update_time timestamp(3)")
	createTblSQL = getBindInfoSQLFn(seV250)
	require.Contains(t, createTblSQL, "`create_time` timestamp(3)")
	require.Contains(t, createTblSQL, "`update_time` timestamp(3)")

	// do upgrade to latest version
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err := sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)
	// check if the columns have been changed to timestamp(6)
	createTblSQL = getBindInfoSQLFn(seCurVer)
	require.Contains(t, createTblSQL, "`create_time` timestamp(6)")
	require.Contains(t, createTblSQL, "`update_time` timestamp(6)")
}

func RunTiDBUpgradeToVer254(t *testing.T) {
	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver253 := sessionpkg.GetVersion253()
	seV253 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver253)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV253, ver253)
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)

	getTableCreateSQLFn := func(se sessionapi.Session, tableName string) string {
		res := sessionpkg.ExportedMustExecToRecodeSet(t, se, fmt.Sprintf("show create table mysql.%s", tableName))
		chk := res.NewChunk(nil)
		err = res.Next(ctx, chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		return string(chk.GetRow(0).GetBytes(1))
	}

	// Verify the indexes exist after bootstrap
	createWatchSQL := getTableCreateSQLFn(seV253, "tidb_runaway_watch")
	require.Contains(t, createWatchSQL, "idx_start_time")
	createWatchDoneSQL := getTableCreateSQLFn(seV253, "tidb_runaway_watch_done")
	require.Contains(t, createWatchDoneSQL, "idx_done_time")

	// Remove the indexes to simulate the old version
	seV253.SetValue(sessionctx.Initing, true)
	seV253.GetSessionVars().SQLMode = mysql.ModeNone
	sessionpkg.ExportedMustExecute(seV253, "ALTER TABLE mysql.tidb_runaway_watch DROP INDEX idx_start_time")
	sessionpkg.ExportedMustExecute(seV253, "ALTER TABLE mysql.tidb_runaway_watch_done DROP INDEX idx_done_time")
	createWatchSQL = getTableCreateSQLFn(seV253, "tidb_runaway_watch")
	require.NotContains(t, createWatchSQL, "idx_start_time")
	createWatchDoneSQL = getTableCreateSQLFn(seV253, "tidb_runaway_watch_done")
	require.NotContains(t, createWatchDoneSQL, "idx_done_time")

	// Upgrade to current version
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err := sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// Verify the indexes have been created after upgrade
	createWatchSQL = getTableCreateSQLFn(seCurVer, "tidb_runaway_watch")
	require.Contains(t, createWatchSQL, "idx_start_time")
	createWatchDoneSQL = getTableCreateSQLFn(seCurVer, "tidb_runaway_watch_done")
	require.Contains(t, createWatchDoneSQL, "idx_done_time")
}

func RunWriteClusterIDToMySQLTiDBWhenUpgradingTo242(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// `cluster_id` is inserted for a new TiDB cluster.
	se := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	r := sessionpkg.ExportedMustExecToRecodeSet(t, se, `select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME='cluster_id'`)
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.NotEmpty(t, req.GetRow(0).GetBytes(0))
	require.NoError(t, r.Close())
	se.Close()

	// bootstrap as sessionpkg.GetVersion241()
	ver241 := sessionpkg.GetVersion241()
	seV241 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver241)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV241, ver241)
	// remove the cluster_id entry from mysql.tidb table
	sessionpkg.ExportedMustExec(t, seV241, "delete from mysql.tidb where variable_name='cluster_id'")
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)
	ver, err := sessionpkg.GetBootstrapVersion(seV241)
	require.NoError(t, err)
	require.Equal(t, ver241, ver)
	seV241.Close()

	// upgrade to current version
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err = sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)

	// check if the cluster_id has been set in the `mysql.tidb` table during upgrade
	r = sessionpkg.ExportedMustExecToRecodeSet(t, seCurVer, `select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME='cluster_id'`)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.NotEmpty(t, req.GetRow(0).GetBytes(0))
	require.NoError(t, r.Close())
	seCurVer.Close()
}

func RunBindInfoUniqueIndex(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// bootstrap as sessionpkg.GetVersion245()
	ver245 := sessionpkg.GetVersion245()
	seV245 := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(ver245)
	require.NoError(t, err)
	sessionpkg.ExportedRevertVersionAndVariables(t, seV245, ver245)
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(sessionpkg.StoreBootstrappedKey, nil)

	// remove the unique index on mysql.bind_info for testing
	sessionpkg.ExportedMustExec(t, seV245, "alter table mysql.bind_info drop index digest_index")

	// insert duplicated values into mysql.bind_info
	for _, sqlDigest := range []string{"null", "'x'", "'y'"} {
		for _, planDigest := range []string{"null", "'x'", "'y'"} {
			insertStmt := fmt.Sprintf(`insert into mysql.bind_info values (
             "sql", "bind_sql", "db", "disabled", NOW(), NOW(), "", "", "", %s, %s, null)`,
				sqlDigest, planDigest)
			sessionpkg.ExportedMustExec(t, seV245, insertStmt)
			sessionpkg.ExportedMustExec(t, seV245, insertStmt)
		}
	}

	// upgrade to current version
	dom.Close()
	domCurVer, err := sessionpkg.ExportedBootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := sessionpkg.ExportedCreateSessionAndSetID(t, store)
	ver, err := sessionpkg.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, sessionpkg.CurrentBootstrapVersion, ver)
}

func RunVersionedBootstrapSchemas(t *testing.T) {
	// make sure that later change won't affect existing version schemas.
	require.Len(t, sessionpkg.GetVersionedBootstrapSchemas()[0].Databases[0].Tables, 52)
	require.Len(t, sessionpkg.GetVersionedBootstrapSchemas()[0].Databases[1].Tables, 0)

	versions := make([]int, 0, len(sessionpkg.GetVersionedBootstrapSchemas()))
	allIDs := make([]int64, 0, len(sessionpkg.GetVersionedBootstrapSchemas()))
	for _, vbs := range sessionpkg.GetVersionedBootstrapSchemas() {
		versions = append(versions, int(vbs.Version))
		for _, db := range vbs.Databases {
			require.Greater(t, db.ID, metadef.ReservedGlobalIDLowerBound)
			require.LessOrEqual(t, db.ID, metadef.ReservedGlobalIDUpperBound)
			allIDs = append(allIDs, db.ID)

			testTableBasicInfoSlice(t, db.Tables, "IF NOT EXISTS mysql.%s (")
			for _, tbl := range db.Tables {
				allIDs = append(allIDs, tbl.ID)
			}
		}
	}
	require.IsIncreasing(t, versions,
		"versions in versionedBootstrapSchemas should be monotonically increasing, and cannot have duplicate versions")
	slices.Sort(allIDs)
	require.IsIncreasing(t, allIDs, "versionedBootstrapSchemas should not have duplicate IDs")
}

func RunCheckSystemTableConstraint(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(*model.TableInfo)
		errMsg    string
	}{
		{
			name: "valid system table",
			setupFunc: func(tblInfo *model.TableInfo) {
				// No partition, no SepAutoInc
				tblInfo.Partition = nil
				tblInfo.Version = model.CurrLatestTableInfoVersion
				tblInfo.AutoIDCache = 0
			},
		},
		{
			name: "table with partition should fail",
			setupFunc: func(tblInfo *model.TableInfo) {
				tblInfo.Partition = &model.PartitionInfo{
					Type:        ast.PartitionTypeRange,
					Enable:      true,
					Definitions: []model.PartitionDefinition{},
				}
				tblInfo.Version = model.CurrLatestTableInfoVersion
				tblInfo.AutoIDCache = 0
			},
			errMsg: "system table should not be partitioned table",
		},
		{
			name: "table with SepAutoInc should fail - version 5 and AutoIDCache 1",
			setupFunc: func(tblInfo *model.TableInfo) {
				tblInfo.Partition = nil
				tblInfo.Version = model.CurrLatestTableInfoVersion
				tblInfo.AutoIDCache = 1
			},
			errMsg: "system table should not use AUTO_ID_CACHE=1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tblInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("test_table")}
			tt.setupFunc(tblInfo)

			err := sessionpkg.ExportedCheckSystemTableConstraint(tblInfo)
			if len(tt.errMsg) > 0 {
				require.ErrorContains(t, err, tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Bootstrap schema variables to satisfy the bootstrap linter.
// These variables reference the actual bootstrap schemas from the parent package.

// testSchemaDefinition provides the schema definition required by the bootstrap linter
var testSchemaDefinition = sessionpkg.GetSystemTablesOfBaseNextGenVersion()

// testVersionedBootstrapSchema provides the versioned bootstrap schema required by the bootstrap linter
var testVersionedBootstrapSchema = sessionpkg.GetSystemDatabases()
