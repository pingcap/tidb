// Copyright 2022 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

const tableModifyLease = 600 * time.Millisecond

func TestLockTableReadOnly(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, tableModifyLease)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1,t2")
	defer func() {
		tk1.MustExec("alter table t1 read write")
		tk1.MustExec("alter table t2 read write")
		tk1.MustExec("drop table if exists t1,t2")
	}()
	tk1.MustExec("create table t1 (a int key, b int)")
	tk1.MustExec("create table t2 (a int key)")

	tk1.MustExec("alter table t1 read only")
	tk1.MustQuery("select * from t1")
	tk2.MustQuery("select * from t1")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("insert into t1 set a=1, b=2"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("update t1 set a=1"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("delete from t1"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("insert into t1 set a=1, b=2"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("update t1 set a=1"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("delete from t1"), infoschema.ErrTableLocked))

	tk2.MustExec("alter table t1 read only")
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("insert into t1 set a=1, b=2"), infoschema.ErrTableLocked))

	tk1.MustExec("alter table t1 read write")
	tk1.MustExec("lock tables t1 read")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	tk1.MustExec("lock tables t1 write")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	tk1.MustExec("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	tk1.MustExec("unlock tables")

	tk1.MustExec("alter table t1 read only")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked))
	tk1.MustExec("admin cleanup table lock t1")
	tk2.MustExec("insert into t1 set a=1, b=2")
}

// TestConcurrentLockTables test concurrent lock/unlock tables.
func TestConcurrentLockTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tableModifyLease)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("create table t1 (a int)")

	// Test concurrent lock tables read.
	sql1 := "lock tables t1 read"
	sql2 := "lock tables t1 read"
	testParallelExecSQL(t, store, dom, sql1, sql2, tk1.Session(), tk2.Session(), func(t *testing.T, err1, err2 error) {
		require.NoError(t, err1)
		require.NoError(t, err2)
	})
	tk1.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write.
	sql1 = "lock tables t1 write"
	sql2 = "lock tables t1 write"
	testParallelExecSQL(t, store, dom, sql1, sql2, tk1.Session(), tk2.Session(), func(t *testing.T, err1, err2 error) {
		require.NoError(t, err1)
		require.True(t, terror.ErrorEqual(err2, infoschema.ErrTableLocked))
	})
	tk1.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write local.
	sql1 = "lock tables t1 write local"
	sql2 = "lock tables t1 write local"
	testParallelExecSQL(t, store, dom, sql1, sql2, tk1.Session(), tk2.Session(), func(t *testing.T, err1, err2 error) {
		require.NoError(t, err1)
		require.True(t, terror.ErrorEqual(err2, infoschema.ErrTableLocked))
	})

	tk1.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func testParallelExecSQL(t *testing.T, store kv.Storage, dom *domain.Domain, sql1, sql2 string, se1, se2 sessiontypes.Session, f func(t *testing.T, err1, err2 error)) {
	callback := &callback.TestDDLCallback{}
	times := 0
	callback.OnJobRunBeforeExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			sess := testkit.NewTestKit(t, store).Session()
			err := sessiontxn.NewTxn(context.Background(), sess)
			require.NoError(t, err)
			jobs, err := ddl.GetAllDDLJobs(sess)
			require.NoError(t, err)
			qLen = len(jobs)
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	d.SetHook(callback)

	var wg util.WaitGroupWrapper
	var err1 error
	var err2 error
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DDLJobQueue.
	go func() {
		var qLen int
		for {
			sess := testkit.NewTestKit(t, store).Session()
			err := sessiontxn.NewTxn(context.Background(), sess)
			require.NoError(t, err)
			jobs, err := ddl.GetAllDDLJobs(sess)
			require.NoError(t, err)
			qLen = len(jobs)
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Run(func() {
		_, err1 = se1.Execute(context.Background(), sql1)
	})
	wg.Run(func() {
		<-ch
		_, err2 = se2.Execute(context.Background(), sql2)
	})

	wg.Wait()
	f(t, err1, err2)
}
