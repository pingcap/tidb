// Copyright 2024 PingCAP, Inc.
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

package fastcreatetable

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestSwitchFastCreateTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn.Context().Session)

	tk.MustQuery("show global variables like 'tidb_enable_fast_create_table'").Check(testkit.Rows("tidb_enable_fast_create_table ON"))

	tk.MustExec("create database db1;")
	tk.MustExec("create database db2;")
	tk.MustExec("create table db1.tb1(id int);")
	tk.MustExec("create table db1.tb2(id int);")
	tk.MustExec("create table db2.tb1(id int);")

	tk.MustExec("set global tidb_enable_fast_create_table=ON")
	tk.MustQuery("show global variables like 'tidb_enable_fast_create_table'").Check(testkit.Rows("tidb_enable_fast_create_table ON"))

	tk.MustExec("set global tidb_enable_fast_create_table=0")
	tk.MustQuery("show global variables like 'tidb_enable_fast_create_table'").Check(testkit.Rows("tidb_enable_fast_create_table OFF"))

	tk.MustGetErrMsg("set global tidb_enable_fast_create_table='wrong'", "[variable:1231]Variable 'tidb_enable_fast_create_table' can't be set to the value of 'wrong'")
}

func TestDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn.Context().Session)

	tk.MustExec("set global tidb_enable_fast_create_table=ON")

	tk.MustExec("create database db")
	// Create Table
	tk.MustExec("create table db.tb1(id int)")
	tk.MustExec("create table db.tb2(id int)")
	// create table twice
	tk.MustGetErrMsg("create table db.tb1(id int)", "[schema:1050]Table 'db.tb1' already exists")

	// Truncate Table
	tk.MustExec("truncate table db.tb1")

	// Drop Table
	tk.MustExec("drop table db.tb1")

	// Rename Table
	tk.MustExec("rename table db.tb2 to db.tb3")

	// Drop Database
	tk.MustExec("drop database db")

	// create again
	tk.MustExec("create database db")
	// Create Table
	tk.MustExec("create table db.tb1(id int)")
	tk.MustExec("create table db.tb2(id int)")
}

func TestMergedJob(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ctx := context.Background()
	var wg util.WaitGroupWrapper

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_fast_create_table=ON")

	startSchedule := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs", func() {
		<-startSchedule
	})

	// this job will be run first.
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk1.MustExec("create table t(a int)")
	})
	require.Eventually(t, func() bool {
		gotJobs, err := ddl.GetAllDDLJobs(ctx, tk.Session())
		require.NoError(t, err)
		return len(gotJobs) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// below 2 jobs are merged into 1, they will fail together.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterGetJobFromLimitCh", func(ch chan *ddl.JobWrapper) {
		require.Eventually(t, func() bool {
			return len(ch) == 1
		}, 10*time.Second, 100*time.Millisecond)
	})
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk1.MustExecToErr("create table t1(id int AUTO_INCREMENT, c int) AUTO_INCREMENT 1000")
	})
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk1.MustExecToErr("create table t(a int)")
	})
	require.Eventually(t, func() bool {
		gotJobs, err := ddl.GetAllDDLJobs(ctx, tk.Session())
		require.NoError(t, err)
		return len(gotJobs) == 2 && gotJobs[1].Type == model.ActionCreateTables
	}, 10*time.Second, 100*time.Millisecond)

	// below 2 jobs are merged into the third group, they will succeed together.
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk1.MustExec("create table t1(id int AUTO_INCREMENT, c int) AUTO_INCREMENT 100")
	})
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk1.MustExec("create table t2(id int AUTO_INCREMENT, c int) AUTO_INCREMENT 100")
	})
	require.Eventually(t, func() bool {
		gotJobs, err := ddl.GetAllDDLJobs(ctx, tk.Session())
		require.NoError(t, err)
		return len(gotJobs) == 3 && gotJobs[2].Type == model.ActionCreateTables
	}, 10*time.Second, 100*time.Millisecond)

	// start to run the jobs
	close(startSchedule)
	wg.Wait()

	// Test the correctness of auto id after failed merge job 2
	tk.MustExec("insert into test.t1(c) values(1)")
	tk.MustQuery("select * from test.t1").Check(testkit.Rows("100 1"))
}

func TestCreateTableFulltextIndexTiCI(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(false)`)
	defer testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
	err := tk.ExecToErr("create table t_ft (id int, title varchar(200), fulltext (title) with parser ngram)")
	require.ErrorContains(t, err, "mock create TiCI index failed")
}

func TestDropTableFulltextIndexTiCI(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`)
	tk.MustExec("create table t_ft (id int, title varchar(200), fulltext (title) with parser ngram)")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/tici/MockDropTiCIIndexSuccess", `return(false)`)
	defer testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/tici/MockDropTiCIIndexSuccess")
	err := tk.ExecToErr("drop table t_ft")
	require.ErrorContains(t, err, "mock drop TiCI index failed")
}

func TestCreateTablesWithFulltextIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_fast_create_table=ON")

	startSchedule := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs", func() { <-startSchedule })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`)
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess"))
	}()

	var wg util.WaitGroupWrapper
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk1.MustExec("create table ft1(id int, v text, fulltext index ft(v))")
	})
	wg.Run(func() {
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		tk2.MustExec("create table ft2(id int, v text, fulltext index ft(v))")
	})

	ctx := context.Background()
	require.Eventually(t, func() bool {
		jobs, err := ddl.GetAllDDLJobs(ctx, tk.Session())
		require.NoError(t, err)
		return len(jobs) == 1 && jobs[0].Type == model.ActionCreateTables
	}, 10*time.Second, 100*time.Millisecond)

	close(startSchedule)
	wg.Wait()

	tk.MustQuery("show tables like 'ft1'").Check(testkit.Rows("ft1"))
	tk.MustQuery("show tables like 'ft2'").Check(testkit.Rows("ft2"))
}

func TestCreateTablesWithFulltextIndexTiCIFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_fast_create_table=ON")

	startSchedule := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs", func() { <-startSchedule })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(false)`)
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess"))
	}()

	var wg util.WaitGroupWrapper
	errCh := make(chan error, 2)
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		_, err := tk1.Exec("create table ft3(id int, v text, fulltext index ft(v))")
		errCh <- err
	})
	wg.Run(func() {
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		_, err := tk2.Exec("create table ft4(id int, v text, fulltext index ft(v))")
		errCh <- err
	})

	ctx := context.Background()
	require.Eventually(t, func() bool {
		jobs, err := ddl.GetAllDDLJobs(ctx, tk.Session())
		require.NoError(t, err)
		return len(jobs) == 1 && jobs[0].Type == model.ActionCreateTables
	}, 10*time.Second, 100*time.Millisecond)

	close(startSchedule)
	wg.Wait()
	err1 := <-errCh
	err2 := <-errCh
	require.Error(t, err1)
	require.Error(t, err2)
	require.Contains(t, err1.Error(), "mock create TiCI index failed")
	require.Contains(t, err2.Error(), "mock create TiCI index failed")

	tk.MustGetErrCode("select * from ft3", errno.ErrNoSuchTable)
	tk.MustGetErrCode("select * from ft4", errno.ErrNoSuchTable)
}

func TestDropTableWithFulltextIndexTriggersTiCI(t *testing.T) {
	t.Skip("requires etcd client to run TiCI operations")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")
	tk.MustExec("create table ft (id int, c text)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`)
	tk.MustExec("alter table ft add fulltext index f_idx(c)")
	var called atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/tici/MockDropTiCIIndexSuccess", func(b *bool) {
		called.Store(true)
		*b = true
	})
	_ = tk.ExecToErr("drop table ft")
	require.True(t, called.Load())
}
