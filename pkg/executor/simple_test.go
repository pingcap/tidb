// Copyright 2025 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestRefreshTableStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("create table t2 (a int, b int, index idx(a))")
	tk.MustExec("insert into t2 values (1,1), (2,2), (3,3)")
	tk.MustExec("analyze table t1, t2 all columns with 1 topn, 2 buckets")

	is := dom.InfoSchema()
	handle := dom.StatsHandle()
	ctx := context.Background()
	tbl1, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tbl1Meta := tbl1.Meta()
	tbl1Stats := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tbl2Meta := tbl2.Meta()
	tbl2Stats := handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	tk.MustExec("refresh stats t1, test.t1")
	tbl1StatsUpdated := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tbl2StatsUpdated := handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	require.NotSame(t, tbl1Stats, tbl1StatsUpdated)
	require.Nil(t, tbl1StatsUpdated.GetIdx(1), "index stats shouldn't be loaded in lite mode")
	require.Same(t, tbl2Stats, tbl2StatsUpdated)
	tk.MustExec("REFRESH STATS *.* FULL")
	tbl1StatsUpdated = handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotNil(t, tbl1StatsUpdated.GetIdx(1), "index stats should be loaded in full mode")
	tbl2StatsUpdated = handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	require.NotSame(t, tbl2Stats, tbl2StatsUpdated)
	require.NotNil(t, tbl2StatsUpdated.GetIdx(1), "index stats should be loaded in full mode")
}

func TestRefreshStatsWarningsForMissingObjects(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("analyze table t all columns")

	vars := tk.Session().GetSessionVars()

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats missing_db.*")
	warnings := vars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, infoschema.ErrDatabaseNotExists.FastGenByArgs("missing_db").Error(), warnings[0].Err.Error())

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats test.t_missing, test.t")
	warnings = vars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, infoschema.ErrTableNotExists.FastGenByArgs("test", "t_missing").Error(), warnings[0].Err.Error())

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats t, t1")
	warnings = vars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, infoschema.ErrTableNotExists.FastGenByArgs("test", "t1").Error(), warnings[0].Err.Error())

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats t")
	require.Len(t, vars.StmtCtx.GetWarnings(), 0)
}

func TestRefreshAllNonExistentTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("analyze table t1 all columns with 1 topn, 2 buckets")

	is := dom.InfoSchema()
	handle := dom.StatsHandle()
	ctx := context.Background()
	tbl1, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl1Meta := tbl1.Meta()
	tbl1Stats := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tk.MustExec("refresh stats missing_db.*, t2")
	tbl1StatsUpdated := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.Same(t, tbl1Stats, tbl1StatsUpdated)
}

func TestRefreshStatsNoTables(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("refresh stats *.*")
}

func TestRefreshStatsRequiresDefaultDB(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustGetDBError("refresh stats t1", plannererrors.ErrNoDB)
}

func TestRefreshStatsWhenDatabaseIsEmpty(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	vars := tk.Session().GetSessionVars()
	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats test.*")
	require.Len(t, vars.StmtCtx.GetWarnings(), 0)
}

func TestRefreshStatsPrivilegeChecks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_refresh_priv")
	tk.MustExec("create table t_refresh_priv (a int)")

	t.Run("table scope requires select", func(t *testing.T) {
		tk.MustExec("drop user if exists 'refresh_reader'@'%'")
		tk.MustExec("create user 'refresh_reader'@'%'")

		tkUser := testkit.NewTestKit(t, store)
		require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "refresh_reader", Hostname: "%"}, nil, nil, nil))
		tkUser.MustGetErrCode("refresh stats test.t_refresh_priv", errno.ErrTableaccessDenied)

		tk.MustExec("grant select on test.t_refresh_priv to 'refresh_reader'@'%'")
		tkUser.MustExec("refresh stats test.t_refresh_priv")
	})

	t.Run("database scope requires select", func(t *testing.T) {
		tk.MustExec("drop user if exists 'refresh_db_reader'@'%'")
		tk.MustExec("create user 'refresh_db_reader'@'%'")

		tkUser := testkit.NewTestKit(t, store)
		require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "refresh_db_reader", Hostname: "%"}, nil, nil, nil))
		tkUser.MustGetErrCode("refresh stats test.*", errno.ErrDBaccessDenied)

		tk.MustExec("grant select on test.* to 'refresh_db_reader'@'%'")
		tkUser.MustExec("refresh stats test.*")
	})

	t.Run("global scope requires global select", func(t *testing.T) {
		tk.MustExec("drop user if exists 'refresh_global_reader'@'%'")
		tk.MustExec("create user 'refresh_global_reader'@'%'")

		tkUser := testkit.NewTestKit(t, store)
		require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "refresh_global_reader", Hostname: "%"}, nil, nil, nil))
		tkUser.MustGetErrCode("refresh stats *.*", errno.ErrPrivilegeCheckFail)

		tk.MustExec("grant select on *.* to 'refresh_global_reader'@'%'")
		tkUser.MustExec("refresh stats *.*")
	})
}

func TestRefreshStatsWithRestoreAdmin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	const user = "restore_admin_tester"
	defer tk.MustExec("drop user if exists '" + user + "'@'%'")

	tk.MustExec("drop user if exists '" + user + "'@'%'")
	tk.MustExec("create user '" + user + "'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: user, Hostname: "%"}, nil, nil, nil))
	tkUser.MustGetErrCode("refresh stats *.*", errno.ErrPrivilegeCheckFail)

	tk.MustExec("grant restore_admin on *.* to '" + user + "'@'%'")
	tkUser.MustExec("refresh stats *.*")
}
