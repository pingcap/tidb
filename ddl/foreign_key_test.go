// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

func testCreateForeignKey(t *testing.T, d ddl.DDL, ctx sessionctx.Context, dbInfo *model.DBInfo, tblInfo *model.TableInfo, fkName string, keys []string, refTable string, refKeys []string, onDelete model.ReferOptionType, onUpdate model.ReferOptionType) *model.Job {
	FKName := model.NewCIStr(fkName)
	Keys := make([]model.CIStr, len(keys))
	for i, key := range keys {
		Keys[i] = model.NewCIStr(key)
	}

	RefTable := model.NewCIStr(refTable)
	RefKeys := make([]model.CIStr, len(refKeys))
	for i, key := range refKeys {
		RefKeys[i] = model.NewCIStr(key)
	}

	fkInfo := &model.FKInfo{
		Name:     FKName,
		RefTable: RefTable,
		RefCols:  RefKeys,
		Cols:     Keys,
		OnDelete: int(onDelete),
		OnUpdate: int(onUpdate),
		State:    model.StateNone,
	}

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}
	err := sessiontxn.NewTxn(context.Background(), ctx)
	require.NoError(t, err)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.DoDDLJob(ctx, job)
	require.NoError(t, err)
	return job
}

func testDropForeignKey(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo, tblInfo *model.TableInfo, foreignKeyName string) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(foreignKeyName)},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func getForeignKey(t table.Table, name string) *model.FKInfo {
	for _, fk := range t.Meta().ForeignKeys {
		// only public foreign key can be read.
		if fk.State != model.StatePublic {
			continue
		}
		if fk.Name.L == strings.ToLower(name) {
			return fk
		}
	}
	return nil
}

func TestForeignKey(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	d := dom.DDL()
	dbInfo, err := testSchemaInfo(store, "test_foreign")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), dom.DDL(), dbInfo)
	tblInfo, err := testTableInfo(store, "t", 3)
	require.NoError(t, err)

	testCreateTable(t, testkit.NewTestKit(t, store).Session(), d, dbInfo, tblInfo)

	// fix data race
	var mu sync.Mutex
	checkOK := false
	var hookErr error
	tc := &ddl.TestDDLCallback{}
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if job.State != model.JobStateDone {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		var t table.Table
		t, err = testGetTableWithError(store, dbInfo.ID, tblInfo.ID)
		if err != nil {
			hookErr = errors.Trace(err)
			return
		}
		fk := getForeignKey(t, "c1_fk")
		if fk == nil {
			hookErr = errors.New("foreign key not exists")
			return
		}
		checkOK = true
	}
	tc.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	ctx := testkit.NewTestKit(t, store).Session()
	job := testCreateForeignKey(t, d, ctx, dbInfo, tblInfo, "c1_fk", []string{"c1"}, "t2", []string{"c1"}, model.ReferOptionCascade, model.ReferOptionSetNull)
	testCheckJobDone(t, store, job.ID, true)
	require.NoError(t, err)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	require.NoError(t, hErr)
	require.True(t, ok)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})

	mu.Lock()
	checkOK = false
	mu.Unlock()
	// fix data race pr/#9491
	tc2 := &ddl.TestDDLCallback{}
	onJobUpdatedExportedFunc2 := func(job *model.Job) {
		if job.State != model.JobStateDone {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		var t table.Table
		t, err = testGetTableWithError(store, dbInfo.ID, tblInfo.ID)
		if err != nil {
			hookErr = errors.Trace(err)
			return
		}
		fk := getForeignKey(t, "c1_fk")
		if fk != nil {
			hookErr = errors.New("foreign key has not been dropped")
			return
		}
		checkOK = true
	}
	tc2.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc2)
	d.SetHook(tc2)

	job = testDropForeignKey(t, ctx, d, dbInfo, tblInfo, "c1_fk")
	testCheckJobDone(t, store, job.ID, false)
	mu.Lock()
	hErr = hookErr
	ok = checkOK
	mu.Unlock()
	require.NoError(t, hErr)
	require.True(t, ok)
	d.SetHook(originalHook)

	tk := testkit.NewTestKit(t, store)
	jobID := testDropTable(tk, t, dbInfo.Name.L, tblInfo.Name.L, dom)
	testCheckJobDone(t, store, jobID, false)

	require.NoError(t, err)
}

func TestCreateTableWithForeignKeyMetaInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int,b int as (a) virtual);")
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id) ON UPDATE RESTRICT ON DELETE CASCADE)")
	tb1Info := getTableInfo(t, dom, "test", "t1")
	tb2Info := getTableInfo(t, dom, "test2", "t2")
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test", "t1")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[0])
	tb2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb2Info.ForeignKeys[0])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb2Info.Indices))
	require.Equal(t, "fk_b", tb2Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`t2`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `test`.`t1` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT", tb2Info.ForeignKeys[0].String("test2", "t2"))

	tk.MustExec("create table t3 (id int, b int, index idx_b(b), foreign key fk_b(b) references t2(id) ON UPDATE SET NULL ON DELETE NO ACTION)")
	tb2Info = getTableInfo(t, dom, "test2", "t2")
	tb3Info := getTableInfo(t, dom, "test2", "t3")
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t3")))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	tb2ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 1, len(tb2ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t3"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb2ReferredFKs[0])
	tb3ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t3")
	require.Equal(t, 0, len(tb3ReferredFKs))
	require.Equal(t, 1, len(tb3Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t2"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		OnDelete:  4,
		OnUpdate:  3,
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[0])
	require.Equal(t, 1, len(tb3Info.Indices))
	require.Equal(t, "idx_b", tb3Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`t3`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `t2` (`id`) ON DELETE NO ACTION ON UPDATE SET NULL", tb3Info.ForeignKeys[0].String("test2", "t3"))

	tk.MustExec("create table t5 (id int key, a int, b int, foreign key (a) references t5(id));")
	tb5Info := getTableInfo(t, dom, "test2", "t5")
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t5")))
	require.Equal(t, 1, len(tb5Info.ForeignKeys))
	tb5ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t5")
	require.Equal(t, 1, len(tb5ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t5"),
		ChildFKName: model.NewCIStr("fk_1"),
	}, *tb5ReferredFKs[0])
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_1"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t5"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb5Info.ForeignKeys[0])
	require.Equal(t, 1, len(tb5Info.Indices))
	require.Equal(t, "fk_1", tb5Info.Indices[0].Name.L)
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test", "t1")))
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t3")))
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t5")))

	tk.MustExec("set @@global.tidb_enable_foreign_key=0")
	tk.MustExec("drop database test2")
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t3")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t5")))
}

func TestCreateTableWithForeignKeyMetaInfo2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("create database test2")
	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("use test2")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id) ON UPDATE RESTRICT ON DELETE CASCADE)")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int as (a) virtual);")
	tb1Info := getTableInfo(t, dom, "test", "t1")
	tb2Info := getTableInfo(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[0])
	tb2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb2Info.ForeignKeys[0])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb2Info.Indices))
	require.Equal(t, "fk_b", tb2Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`t2`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `test`.`t1` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT", tb2Info.ForeignKeys[0].String("test2", "t2"))

	tk.MustExec("create table t3 (id int key, a int, foreign key fk_a(a) references test.t1(id) ON DELETE CASCADE ON UPDATE RESTRICT, foreign key fk_a2(a) references test2.t2(id))")
	tb1Info = getTableInfo(t, dom, "test", "t1")
	tb3Info := getTableInfo(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 2, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test"),
		ChildTable:  model.NewCIStr("t3"),
		ChildFKName: model.NewCIStr("fk_a"),
	}, *tb1ReferredFKs[0])
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[1])
	tb3ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb3ReferredFKs))
	require.Equal(t, 2, len(tb3Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_a"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[0])
	require.Equal(t, model.FKInfo{
		ID:        2,
		Name:      model.NewCIStr("fk_a2"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t2"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[1])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb3Info.Indices))
	require.Equal(t, "fk_a", tb3Info.Indices[0].Name.L)
	require.Equal(t, "`test`.`t3`, CONSTRAINT `fk_a` FOREIGN KEY (`a`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT", tb3Info.ForeignKeys[0].String("test", "t3"))
	require.Equal(t, "`test`.`t3`, CONSTRAINT `fk_a2` FOREIGN KEY (`a`) REFERENCES `test2`.`t2` (`id`)", tb3Info.ForeignKeys[1].String("test", "t3"))

	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("drop table test2.t2")
	tb1Info = getTableInfo(t, dom, "test", "t1")
	tb3Info = getTableInfo(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test"),
		ChildTable:  model.NewCIStr("t3"),
		ChildFKName: model.NewCIStr("fk_a"),
	}, *tb1ReferredFKs[0])
	tb3ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb3ReferredFKs))
	require.Equal(t, 2, len(tb3Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_a"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[0])
	require.Equal(t, model.FKInfo{
		ID:        2,
		Name:      model.NewCIStr("fk_a2"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t2"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[1])
}

func TestCreateTableWithForeignKeyMetaInfo3(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int as (a) virtual);")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tk.MustExec("create table t3 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tk.MustExec("create table t4 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	tk.MustExec("drop table t3")
	tk.MustExec("create table t5 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	require.Equal(t, 3, len(tb1ReferredFKs))
	require.Equal(t, "t2", tb1ReferredFKs[0].ChildTable.L)
	require.Equal(t, "t3", tb1ReferredFKs[1].ChildTable.L)
	require.Equal(t, "t4", tb1ReferredFKs[2].ChildTable.L)
}

func TestCreateTableWithForeignKeyPrivilegeCheck(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create user 'u1'@'%' identified by '';")
	tk.MustExec("grant create on *.* to 'u1'@'%';")
	tk.MustExec("create table t1 (id int key);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost", CurrentUser: true, AuthUsername: "u1", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	err := tk2.ExecToErr("create table t2 (a int, foreign key fk(a) references t1(id));")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]REFERENCES command denied to user 'u1'@'%' for table 't1'", err.Error())

	tk.MustExec("grant references on test.t1 to 'u1'@'%';")
	tk2.MustExec("create table t2 (a int, foreign key fk(a) references t1(id));")
	tk2.MustExec("create table t3 (id int key)")
	err = tk2.ExecToErr("create table t4 (a int, foreign key fk(a) references t1(id), foreign key (a) references t3(id));")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]REFERENCES command denied to user 'u1'@'%' for table 't3'", err.Error())

	tk.MustExec("grant references on test.t3 to 'u1'@'%';")
	tk2.MustExec("create table t4 (a int, foreign key fk(a) references t1(id), foreign key (a) references t3(id));")
}

func TestRenameTableWithForeignKeyMetaInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("create database test2")
	tk.MustExec("create database test3")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))")
	tk.MustExec("rename table test.t1 to test2.t2")
	// check the schema diff
	diff := getLatestSchemaDiff(t, tk)
	require.Equal(t, model.ActionRenameTable, diff.Type)
	require.Equal(t, 0, len(diff.AffectedOpts))
	tk.MustQuery("show create table test2.t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `fk` (`a`),\n" +
		"  CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `test2`.`t2` (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tblInfo := getTableInfo(t, dom, "test2", "t2")
	tbReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 1, len(tblInfo.ForeignKeys))
	require.Equal(t, 1, len(tbReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk"),
	}, *tbReferredFKs[0])
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t2"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		State:     model.StatePublic,
		Version:   1,
	}, *tblInfo.ForeignKeys[0])

	tk.MustExec("drop table test2.t2")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int as (a) virtual);")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tk.MustExec("use test2")
	tk.MustExec("rename table test.t2 to test2.tt2")
	// check the schema diff
	diff = getLatestSchemaDiff(t, tk)
	require.Equal(t, model.ActionRenameTable, diff.Type)
	require.Equal(t, 0, len(diff.AffectedOpts))
	tb1Info := getTableInfo(t, dom, "test", "t1")
	tb2Info := getTableInfo(t, dom, "test2", "tt2")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("tt2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[0])
	tb2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "tt2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb2Info.ForeignKeys[0])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb2Info.Indices))
	require.Equal(t, "fk_b", tb2Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`tt2`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `test`.`t1` (`id`)", tb2Info.ForeignKeys[0].String("test2", "tt2"))

	tk.MustExec("rename table test.t1 to test3.tt1")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test3", "tt1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, 1, len(tb1ReferredFKs[0].Cols))
	// check the schema diff
	diff = getLatestSchemaDiff(t, tk)
	require.Equal(t, model.ActionRenameTable, diff.Type)
	require.Equal(t, 1, len(diff.AffectedOpts))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("tt2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[0])
	tbl2Info := getTableInfo(t, dom, "test2", "tt2")
	tb2ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test2", "tt2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test3"),
		RefTable:  model.NewCIStr("tt1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		State:     model.StatePublic,
		Version:   1,
	}, *tbl2Info.ForeignKeys[0])
	tk.MustQuery("show create table test2.tt2").Check(testkit.Rows("tt2 CREATE TABLE `tt2` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `fk_b` (`b`),\n" +
		"  CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `test3`.`tt1` (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestCreateTableWithForeignKeyDML(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int);")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustExec("update t1 set a = 2 where id = 1")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id))")

	tk.MustExec("commit")
}

func TestCreateTableWithForeignKeyError(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("use test")

	cases := []struct {
		prepare []string
		refer   string
		create  string
		err     string
	}{
		{
			refer:  "create table t1 (id int, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references T_unknown(b));",
			err:    "[schema:1824]Failed to open the referenced table 'T_unknown'",
		},
		{
			refer:  "create table t1 (id int, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(c_unknown));",
			err:    "[schema:3734]Failed to add the foreign key constraint. Missing column 'c_unknown' for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int key, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk(c_unknown) references t1(id));",
			err:    "[ddl:1072]Key column 'c_unknown' doesn't exist in table",
		},
		{
			refer:  "create table t1 (id int, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int, a int, b int not null, index(b));",
			create: "create table t2 (a int, b int not null, foreign key fk_b(b) references t1(b) on update set null);",
			err:    "[schema:1830]Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
		},
		{
			refer:  "create table t1 (id int, a int, b int not null, index(b));",
			create: "create table t2 (a int, b int not null, foreign key fk_b(b) references t1(b) on delete set null);",
			err:    "[schema:1830]Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
		},
		{
			refer:  "create table t1 (id int key, a int, b int as (a) virtual, index(b));",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
			err:    "[schema:3733]Foreign key 'fk_b' uses virtual column 'b' which is not supported.",
		},
		{
			refer:  "create table t1 (id int key, a int, b int, index(b));",
			create: "create table t2 (a int, b int as (a) virtual, foreign key fk_b(b) references t1(b));",
			err:    "[schema:3733]Foreign key 'fk_b' uses virtual column 'b' which is not supported.",
		},
		{
			refer:  "create table t1 (id int key, a int);",
			create: "create table t2 (a int, b varchar(10), foreign key fk(b) references t1(id));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'id' in foreign key constraint 'fk' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a int not null, index(a));",
			create: "create table t2 (a int, b int unsigned, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a bigint, index(a));",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10) charset utf8, index(a));",
			create: "create table t2 (a int, b varchar(10) charset utf8mb4, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10) collate utf8_bin, index(a));",
			create: "create table t2 (a int, b varchar(10) collate utf8mb4_bin, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10));",
			create: "create table t2 (a int, b varchar(10), foreign key fk_b(b) references t1(a));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10), index (a(5)));",
			create: "create table t2 (a int, b varchar(10), foreign key fk_b(b) references t1(a));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int key, a int, index(a));",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(id, a));",
			err:    "[schema:1239]Incorrect foreign key definition for 'fk_b': Key reference and table reference don't match",
		},
		{
			create: "create table t2 (a int key, foreign key (a) references t2(a));",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			create: "create table t2 (a int, b int, index(a,b), index(b,a), foreign key (a,b) references t2(a,b));",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			create: "create table t2 (a int, b int, index(a,b), foreign key (a,b) references t2(b,a));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't2'",
		},
		{
			prepare: []string{
				"set @@foreign_key_checks=0;",
				"create table t2 (a int, b int, index(a), foreign key (a) references t1(id));",
			},
			create: "create table t1 (id int, a int);",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'",
		},
		{
			prepare: []string{
				"set @@foreign_key_checks=0;",
				"create table t2 (a int, b int, index(a), foreign key (a) references t1(id));",
			},
			create: "create table t1 (id bigint key, a int);",
			err:    "[ddl:3780]Referencing column 'a' and referenced column 'id' in foreign key constraint 'fk_1' are incompatible.",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create temporary table t1 (id int key, b int, index(b))",
			create: "create table t2 (a int, b int, foreign key fk(b) references t1(b))",
			err:    "[schema:1824]Failed to open the referenced table 't1'",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create global temporary table t1 (id int key, b int, index(b)) on commit delete rows",
			create: "create table t2 (a int, b int, foreign key fk(b) references t1(b))",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create table t1 (id int key, b int, index(b))",
			create: "create temporary table t2 (a int, b int, foreign key fk(b) references t1(b))",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create table t1 (id int key, b int, index(b))",
			create: "create global temporary table t2 (a int, b int, foreign key fk(b) references t1(b)) on commit delete rows",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			create: "create table t1 (a int, foreign key ``(a) references t1(a));",
			err:    "[ddl:1280]Incorrect index name ''",
		},
		{
			create: "create table t1 (a int, constraint `` foreign key (a) references t1(a));",
			err:    "[ddl:1280]Incorrect index name ''",
		},
		{
			create: "create table t1 (a int, constraint `fk` foreign key (a,a) references t1(a, b));",
			err:    "[schema:1060]Duplicate column name 'a'",
		},
		{
			refer:  "create table t1(a int, b int, index(a,b));",
			create: "create table t2 (a int, b int, foreign key (a,b) references t1(a,a));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int key, b int, index(b))",
			create: "create table t2 (a int, b int, index fk_1(a), foreign key (b) references t1(b));",
			err:    "[ddl:1061]duplicate key name fk_1",
		},
		{
			refer:  "create table t1 (id int key);",
			create: "create table t2 (id int key, foreign key name5678901234567890123456789012345678901234567890123456789012345(id) references t1(id));",
			err:    "[ddl:1059]Identifier name 'name5678901234567890123456789012345678901234567890123456789012345' is too long",
		},
		{
			refer:  "create table t1 (id int key);",
			create: "create table t2 (id int key, constraint name5678901234567890123456789012345678901234567890123456789012345 foreign key (id) references t1(id));",
			err:    "[ddl:1059]Identifier name 'name5678901234567890123456789012345678901234567890123456789012345' is too long",
		},
		{
			create: "create table t2 (id int key, constraint fk foreign key (id) references name5678901234567890123456789012345678901234567890123456789012345.t1(id));",
			err:    "[ddl:1059]Identifier name 'name5678901234567890123456789012345678901234567890123456789012345' is too long",
		},
		{
			prepare: []string{
				"set @@foreign_key_checks=0;",
			},
			create: "create table t2 (id int key, constraint fk foreign key (id) references name5678901234567890123456789012345678901234567890123456789012345(id));",
			err:    "[ddl:1059]Identifier name 'name5678901234567890123456789012345678901234567890123456789012345' is too long",
		},
		{
			prepare: []string{
				"set @@foreign_key_checks=0;",
			},
			create: "create table t2 (id int key, constraint fk foreign key (id) references t1(name5678901234567890123456789012345678901234567890123456789012345));",
			err:    "[ddl:1059]Identifier name 'name5678901234567890123456789012345678901234567890123456789012345' is too long",
		},
	}
	for _, ca := range cases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("set @@foreign_key_checks=1")
		for _, sql := range ca.prepare {
			tk.MustExec(sql)
		}
		if ca.refer != "" {
			tk.MustExec(ca.refer)
		}
		err := tk.ExecToErr(ca.create)
		require.Error(t, err, ca.create)
		require.Equal(t, ca.err, err.Error(), ca.create)
	}

	passCases := [][]string{
		{
			"create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))",
		},
		{
			"create table t1 (id int key, b int not null, index(b))",
			"create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
		},
		{
			"create table t1 (id int key, a varchar(10), index(a));",
			"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
		},
		{
			"create table t1 (id int key, a decimal(10,5), index(a));",
			"create table t2 (a int, b decimal(20, 10), foreign key fk_b(b) references t1(a));",
		},
		{
			"create table t1 (id int key, a varchar(10), index (a(10)));",
			"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
		},
		{
			"set @@foreign_key_checks=0;",
			"create table t2 (a int, b int, foreign key fk_b(b) references t_unknown(b));",
			"set @@foreign_key_checks=1;",
		},
		{
			"create table t2 (a int, b int, index(a,b), index(b,a), foreign key (a,b) references t2(b,a));",
		},
		{
			"create table t1 (a int key, b int, index(b))",
			"create table t2 (a int, b int, foreign key (a) references t1(a), foreign key (b) references t1(b));",
		},
		{
			"create table t1 (id int key);",
			"create table t2 (id int key, foreign key name567890123456789012345678901234567890123456789012345678901234(id) references t1(id));",
		},
	}
	for _, ca := range passCases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		for _, sql := range ca {
			tk.MustExec(sql)
		}
	}
}

func TestModifyColumnWithForeignKey(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")

	tk.MustExec("create table t1 (id int key, b varchar(10), index(b));")
	tk.MustExec("create table t2 (a varchar(10), constraint fk foreign key (a) references t1(b));")
	tk.MustExec("insert into t1 values (1, '123456789');")
	tk.MustExec("insert into t2 values ('123456789');")
	tk.MustGetErrMsg("alter table t1 modify column b varchar(5);", "[ddl:1833]Cannot change column 'b': used in a foreign key constraint 'fk' of table 'test.t2'")
	tk.MustGetErrMsg("alter table t1 modify column b bigint;", "[ddl:3780]Referencing column 'a' and referenced column 'b' in foreign key constraint 'fk' are incompatible.")
	tk.MustExec("alter table t1 modify column b varchar(20);")
	tk.MustGetErrMsg("alter table t1 modify column b varchar(10);", "[ddl:1833]Cannot change column 'b': used in a foreign key constraint 'fk' of table 'test.t2'")
	tk.MustExec("alter table t2 modify column a varchar(20);")
	tk.MustExec("alter table t2 modify column a varchar(21);")
	tk.MustGetErrMsg("alter table t2 modify column a varchar(5);", "[ddl:1832]Cannot change column 'a': used in a foreign key constraint 'fk'")
	tk.MustGetErrMsg("alter table t2 modify column a bigint;", "[ddl:3780]Referencing column 'a' and referenced column 'b' in foreign key constraint 'fk' are incompatible.")

	tk.MustExec("drop table t2")
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id int key, b decimal(10, 5), index(b));")
	tk.MustExec("create table t2 (a decimal(10, 5), constraint fk foreign key (a) references t1(b));")
	tk.MustExec("insert into t1 values (1, 12345.67891);")
	tk.MustExec("insert into t2 values (12345.67891);")
	tk.MustGetErrMsg("alter table t1 modify column b decimal(10, 6);", "[ddl:1833]Cannot change column 'b': used in a foreign key constraint 'fk' of table 'test.t2'")
	tk.MustGetErrMsg("alter table t1 modify column b decimal(10, 3);", "[ddl:1833]Cannot change column 'b': used in a foreign key constraint 'fk' of table 'test.t2'")
	tk.MustGetErrMsg("alter table t1 modify column b decimal(5, 2);", "[ddl:1833]Cannot change column 'b': used in a foreign key constraint 'fk' of table 'test.t2'")
	tk.MustGetErrMsg("alter table t1 modify column b decimal(20, 10);", "[ddl:1833]Cannot change column 'b': used in a foreign key constraint 'fk' of table 'test.t2'")
	tk.MustGetErrMsg("alter table t2 modify column a decimal(30, 15);", "[ddl:1832]Cannot change column 'a': used in a foreign key constraint 'fk'")
	tk.MustGetErrMsg("alter table t2 modify column a decimal(5, 2);", "[ddl:1832]Cannot change column 'a': used in a foreign key constraint 'fk'")
}

func TestDropChildTableForeignKeyMetaInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int, CONSTRAINT fk foreign key (a) references t1(id))")
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	tk.MustExec("drop table t1")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 0, len(tb1ReferredFKs))

	tk.MustExec("create table t1 (id int key, b int, index(b))")
	tk.MustExec("create table t2 (a int, b int, foreign key fk (a) references t1(b));")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	tk.MustExec("drop table t2")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 0, len(tb1ReferredFKs))
}

func TestDropForeignKeyMetaInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int, CONSTRAINT fk foreign key (a) references t1(id))")
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	tk.MustExec("alter table t1 drop foreign key fk")
	tbl1Info := getTableInfo(t, dom, "test", "t1")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 0, len(tbl1Info.ForeignKeys))
	require.Equal(t, 0, len(tb1ReferredFKs))

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id int key, b int, index(b))")
	tk.MustExec("create table t2 (a int, b int, foreign key fk (a) references t1(b));")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	tk.MustExec("alter table t2 drop foreign key fk")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 0, len(tb1ReferredFKs))
	tbl2Info := getTableInfo(t, dom, "test", "t2")
	require.Equal(t, 0, len(tbl2Info.ForeignKeys))
}

func TestTruncateOrDropTableWithForeignKeyReferred(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("use test")

	cases := []struct {
		prepares    []string
		tbl         string
		truncateErr string
		dropErr     string
	}{
		{
			prepares: []string{
				"create table t1 (id int key, b int not null, index(b))",
				"create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
			},
			tbl:         "t1",
			truncateErr: "[ddl:1701]Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk_b`)",
			dropErr:     "[ddl:3730]Cannot drop table 't1' referenced by a foreign key constraint 'fk_b' on table 't2'.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a varchar(10), index(a));",
				"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
			},
			tbl:         "t1",
			truncateErr: "[ddl:1701]Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk_b`)",
			dropErr:     "[ddl:3730]Cannot drop table 't1' referenced by a foreign key constraint 'fk_b' on table 't2'.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a varchar(10), index (a(10)));",
				"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
			},
			tbl:         "t1",
			truncateErr: "[ddl:1701]Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk_b`)",
			dropErr:     "[ddl:3730]Cannot drop table 't1' referenced by a foreign key constraint 'fk_b' on table 't2'.",
		},
	}

	for _, ca := range cases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		for _, sql := range ca.prepares {
			tk.MustExec(sql)
		}
		truncateSQL := fmt.Sprintf("truncate table %v", ca.tbl)
		tk.MustExec("set @@foreign_key_checks=1;")
		err := tk.ExecToErr(truncateSQL)
		require.Error(t, err)
		require.Equal(t, ca.truncateErr, err.Error())
		dropSQL := fmt.Sprintf("drop table %v", ca.tbl)
		err = tk.ExecToErr(dropSQL)
		require.Error(t, err)
		require.Equal(t, ca.dropErr, err.Error())

		tk.MustExec("set @@foreign_key_checks=0;")
		tk.MustExec(truncateSQL)
	}
	passCases := [][]string{
		{
			"create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))",
			"truncate table t1",
			"drop table t1",
		},
		{
			"create table t1 (id int key, a varchar(10), index (a(10)));",
			"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
			"drop table t1, t2",
		},
		{
			"set @@foreign_key_checks=0;",
			"create table t1 (id int key, a varchar(10), index (a(10)));",
			"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
			"truncate table t1",
			"drop table t1",
		},
	}
	for _, ca := range passCases {
		tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("set @@foreign_key_checks=1;")
		for _, sql := range ca {
			tk.MustExec(sql)
		}
	}
}

func TestTruncateOrDropTableWithForeignKeyReferred2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	d := dom.DDL()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk2.MustExec("set @@foreign_key_checks=1;")
	tk2.MustExec("use test")

	tk.MustExec("create table t1 (id int key, a int);")

	var wg sync.WaitGroup
	var truncateErr, dropErr error
	testTruncate := true
	tc := &ddl.TestDDLCallback{}
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateNone {
			return
		}
		if job.Type != model.ActionCreateTable {
			return
		}
		wg.Add(1)
		if testTruncate {
			go func() {
				defer wg.Done()
				truncateErr = tk2.ExecToErr("truncate table t1")
			}()
		} else {
			go func() {
				defer wg.Done()
				dropErr = tk2.ExecToErr("drop table t1")
			}()
		}
		// make sure tk2's ddl job already put into ddl job queue.
		time.Sleep(time.Millisecond * 100)
	}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	tk.MustExec("create table t2 (a int, b int, foreign key fk(b) references t1(id));")
	wg.Wait()
	require.Error(t, truncateErr)
	require.Equal(t, "[ddl:1701]Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk`)", truncateErr.Error())

	tk.MustExec("drop table t2")
	testTruncate = false
	tk.MustExec("create table t2 (a int, b int, foreign key fk(b) references t1(id));")
	wg.Wait()
	require.Error(t, dropErr)
	require.Equal(t, "[ddl:1701]Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk`)", dropErr.Error())
}

func TestDropTableWithForeignKeyReferred(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")

	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references t1(id));")
	tk.MustExec("create table t3 (id int key, b int, foreign key fk_b(b) references t2(id));")
	err := tk.ExecToErr("drop table if exists t1,t2;")
	require.Error(t, err)
	require.Equal(t, "[ddl:3730]Cannot drop table 't2' referenced by a foreign key constraint 'fk_b' on table 't3'.", err.Error())
	tk.MustQuery("show tables").Check(testkit.Rows("t1", "t2", "t3"))
}

func TestDropIndexNeededInForeignKey(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	cases := []struct {
		prepares []string
		drops    []string
		err      string
	}{
		{
			prepares: []string{
				"create table t1 (id int key, b int, index idx (b))",
				"create table t2 (a int, b int, index idx (b), foreign key fk_b(b) references t1(b));",
			},
			drops: []string{
				"alter table t1 drop index idx",
				"alter table t2 drop index idx",
			},
			err: "[ddl:1553]Cannot drop index 'idx': needed in a foreign key constraint",
		},
		{
			prepares: []string{
				"create table t1 (id int, b int, index idx (id, b))",
				"create table t2 (a int, b int, index idx (b, a), foreign key fk_b(b) references t1(id));",
			},
			drops: []string{
				"alter table t1 drop index idx",
				"alter table t2 drop index idx",
			},
			err: "[ddl:1553]Cannot drop index 'idx': needed in a foreign key constraint",
		},
	}

	for _, ca := range cases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		for _, sql := range ca.prepares {
			tk.MustExec(sql)
		}
		for _, drop := range ca.drops {
			// even disable foreign key check, still can't drop the index used by foreign key.
			tk.MustExec("set @@foreign_key_checks=0;")
			err := tk.ExecToErr(drop)
			require.Error(t, err)
			require.Equal(t, ca.err, err.Error())
			tk.MustExec("set @@foreign_key_checks=1;")
			err = tk.ExecToErr(drop)
			require.Error(t, err)
			require.Equal(t, ca.err, err.Error())
		}
	}
	passCases := [][]string{
		{
			"create table t1 (id int key, b int, index idxb (b))",
			"create table t2 (a int, b int key, index idxa (a),index idxb (b), foreign key fk_b(b) references t1(id));",
			"alter table t1 drop index idxb",
			"alter table t2 drop index idxa",
			"alter table t2 drop index idxb",
		},
		{
			"create table t1 (id int key, b int, index idxb (b), unique index idx(b, id))",
			"create table t2 (a int, b int key, index idx (b, a),index idxb (b), index idxab(a, b), foreign key fk_b(b) references t1(b));",
			"alter table t1 drop index idxb",
			"alter table t1 add index idxb (b)",
			"alter table t1 drop index idx",
			"alter table t2 drop index idx",
			"alter table t2 add index idx (b, a)",
			"alter table t2 drop index idxb",
			"alter table t2 drop index idxab",
		},
	}
	tk.MustExec("set @@foreign_key_checks=1;")
	for _, ca := range passCases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		for _, sql := range ca {
			tk.MustExec(sql)
		}
	}
}

func TestDropIndexNeededInForeignKey2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	d := dom.DDL()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk2.MustExec("set @@foreign_key_checks=1;")
	tk2.MustExec("use test")
	tk.MustExec("create table t1 (id int key, b int)")
	tk.MustExec("create table t2 (a int, b int, index idx1 (b),index idx2 (b), foreign key (b) references t1(id));")

	var wg sync.WaitGroup
	var dropErr error
	tc := &ddl.TestDDLCallback{}
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StatePublic || job.Type != model.ActionDropIndex {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			dropErr = tk2.ExecToErr("alter table t2 drop index idx2")
		}()
		// make sure tk2's ddl job already put into ddl job queue.
		time.Sleep(time.Millisecond * 100)
	}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	tk.MustExec("alter table t2 drop index idx1")
	wg.Wait()
	require.Error(t, dropErr)
	require.Equal(t, "[ddl:1553]Cannot drop index 'idx2': needed in a foreign key constraint", dropErr.Error())
}

func getTableInfo(t *testing.T, dom *domain.Domain, db, tb string) *model.TableInfo {
	err := dom.Reload()
	require.NoError(t, err)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(tb))
	require.NoError(t, err)
	_, exist := is.TableByID(tbl.Meta().ID)
	require.True(t, exist)
	return tbl.Meta()
}

func getTableInfoReferredForeignKeys(t *testing.T, dom *domain.Domain, db, tb string) []*model.ReferredFKInfo {
	err := dom.Reload()
	require.NoError(t, err)
	return dom.InfoSchema().GetTableReferredForeignKeys(db, tb)
}

func TestDropColumnWithForeignKey(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")

	tk.MustExec("create table t1 (id int key, a int, b int, index(b), CONSTRAINT fk foreign key (a) references t1(b))")
	tk.MustGetErrMsg("alter table t1 drop column a;", "[ddl:1828]Cannot drop column 'a': needed in a foreign key constraint 'fk'")
	tk.MustGetErrMsg("alter table t1 drop column b;", "[ddl:1829]Cannot drop column 'b': needed in a foreign key constraint 'fk' of table 't1'")

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (a int, b int, constraint fk foreign key (a) references t1(b));")
	tk.MustGetErrMsg("alter table t1 drop column b;", "[ddl:1829]Cannot drop column 'b': needed in a foreign key constraint 'fk' of table 't2'")
	tk.MustGetErrMsg("alter table t2 drop column a;", "[ddl:1828]Cannot drop column 'a': needed in a foreign key constraint 'fk'")
}

func TestRenameColumnWithForeignKeyMetaInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")

	tk.MustExec("create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))")
	tk.MustExec("alter table t1 change id kid int")
	tk.MustExec("alter table t1 rename column a to aa")
	tbl1Info := getTableInfo(t, dom, "test", "t1")
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tbl1Info.ForeignKeys))
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, "kid", tb1ReferredFKs[0].Cols[0].L)
	require.Equal(t, "kid", tbl1Info.ForeignKeys[0].RefCols[0].L)
	require.Equal(t, "aa", tbl1Info.ForeignKeys[0].Cols[0].L)

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id int key, b int, index(b))")
	tk.MustExec("create table t2 (a int, b int, foreign key fk(a) references t1(b));")
	tk.MustExec("alter table t2 change a aa int")
	tbl1Info = getTableInfo(t, dom, "test", "t1")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, 1, len(tb1ReferredFKs[0].Cols))
	require.Equal(t, "b", tb1ReferredFKs[0].Cols[0].L)
	tbl2Info := getTableInfo(t, dom, "test", "t2")
	tb2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[0].Cols))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[0].RefCols))
	require.Equal(t, "aa", tbl2Info.ForeignKeys[0].Cols[0].L)
	require.Equal(t, "b", tbl2Info.ForeignKeys[0].RefCols[0].L)

	tk.MustExec("alter table t1 change id kid int")
	tk.MustExec("alter table t1 change b bb int")
	tbl1Info = getTableInfo(t, dom, "test", "t1")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, 1, len(tb1ReferredFKs[0].Cols))
	require.Equal(t, "bb", tb1ReferredFKs[0].Cols[0].L)
	tbl2Info = getTableInfo(t, dom, "test", "t2")
	tb2ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[0].Cols))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[0].RefCols))
	require.Equal(t, "aa", tbl2Info.ForeignKeys[0].Cols[0].L)
	require.Equal(t, "bb", tbl2Info.ForeignKeys[0].RefCols[0].L)

	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1 (id int key, b int, index(b))")
	tk.MustExec("create table t2 (a int, b int, foreign key (a) references t1(b), foreign key (b) references t1(b));")
	tk.MustExec("alter table t1 change b bb int")
	tbl1Info = getTableInfo(t, dom, "test", "t1")
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 2, len(tb1ReferredFKs))
	require.Equal(t, 1, len(tb1ReferredFKs[0].Cols))
	require.Equal(t, 1, len(tb1ReferredFKs[1].Cols))
	require.Equal(t, "bb", tb1ReferredFKs[0].Cols[0].L)
	require.Equal(t, "bb", tb1ReferredFKs[1].Cols[0].L)
	tbl2Info = getTableInfo(t, dom, "test", "t2")
	tb2ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 2, len(tbl2Info.ForeignKeys))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[0].Cols))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[0].RefCols))
	require.Equal(t, "a", tbl2Info.ForeignKeys[0].Cols[0].L)
	require.Equal(t, "bb", tbl2Info.ForeignKeys[0].RefCols[0].L)
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[1].Cols))
	require.Equal(t, 1, len(tbl2Info.ForeignKeys[1].RefCols))
	require.Equal(t, "b", tbl2Info.ForeignKeys[1].Cols[0].L)
	require.Equal(t, "bb", tbl2Info.ForeignKeys[1].RefCols[0].L)
	tk.MustExec("alter table t2 rename column a to aa")
	tk.MustExec("alter table t2 change b bb int")
	tk.MustQuery("show create table t2").
		Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
			"  `aa` int(11) DEFAULT NULL,\n" +
			"  `bb` int(11) DEFAULT NULL,\n" +
			"  KEY `fk_1` (`aa`),\n  KEY `fk_2` (`bb`),\n" +
			"  CONSTRAINT `fk_1` FOREIGN KEY (`aa`) REFERENCES `test`.`t1` (`bb`),\n" +
			"  CONSTRAINT `fk_2` FOREIGN KEY (`bb`) REFERENCES `test`.`t1` (`bb`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestDropDatabaseWithForeignKeyReferred(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")

	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references t1(id));")
	tk.MustExec("create database test2")
	tk.MustExec("create table test2.t3 (id int key, b int, foreign key fk_b(b) references test.t2(id));")
	err := tk.ExecToErr("drop database test;")
	require.Error(t, err)
	require.Equal(t, "[ddl:3730]Cannot drop table 't2' referenced by a foreign key constraint 'fk_b' on table 't3'.", err.Error())
	tk.MustExec("set @@foreign_key_checks=0;")
	tk.MustExec("drop database test")

	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("create database test")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references t1(id));")
	err = tk.ExecToErr("drop database test;")
	require.Error(t, err)
	require.Equal(t, "[ddl:3730]Cannot drop table 't2' referenced by a foreign key constraint 'fk_b' on table 't3'.", err.Error())
	tk.MustExec("drop table test2.t3")
	tk.MustExec("drop database test")
}

func TestDropDatabaseWithForeignKeyReferred2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	d := dom.DDL()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk2.MustExec("set @@foreign_key_checks=1;")
	tk2.MustExec("use test")
	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references t1(id));")
	tk.MustExec("create database test2")
	var wg sync.WaitGroup
	var dropErr error
	tc := &ddl.TestDDLCallback{}
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateNone {
			return
		}
		if job.Type != model.ActionCreateTable {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			dropErr = tk2.ExecToErr("drop database test")
		}()
		// make sure tk2's ddl job already put into ddl job queue.
		time.Sleep(time.Millisecond * 100)
	}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	tk.MustExec("create table test2.t3 (id int key, b int, foreign key fk_b(b) references test.t2(id));")
	wg.Wait()
	require.Error(t, dropErr)
	require.Equal(t, "[ddl:3730]Cannot drop table 't2' referenced by a foreign key constraint 'fk_b' on table 't3'.", dropErr.Error())
	tk.MustExec("drop table test2.t3")
	tk.MustExec("drop database test")
}

func TestAddForeignKey(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, b int);")
	tk.MustExec("create table t2 (id int key, b int);")
	tk.MustExec("alter table t2 add index(b)")
	tk.MustExec("alter table t2 add foreign key (b) references t1(id);")
	tbl2Info := getTableInfo(t, dom, "test", "t2")
	require.Equal(t, int64(1), tbl2Info.MaxForeignKeyID)
	tk.MustGetDBError("alter table t2 add foreign key (b) references t1(b);", infoschema.ErrForeignKeyNoIndexInParent)
	tk.MustExec("alter table t1 add index(b)")
	tk.MustExec("alter table t2 add foreign key (b) references t1(b);")
	tk.MustGetDBError("alter table t2 add foreign key (b) references t2(b);", infoschema.ErrCannotAddForeign)
	// Test auto-create index when create foreign key constraint.
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (id int key, b int);")
	tk.MustExec("alter table t2 add constraint fk foreign key (b) references t1(b);")
	tbl2Info = getTableInfo(t, dom, "test", "t2")
	require.Equal(t, 1, len(tbl2Info.Indices))
	require.Equal(t, "fk", tbl2Info.Indices[0].Name.L)
	require.Equal(t, model.StatePublic, tbl2Info.Indices[0].State)
	tk.MustQuery("select b from t2 use index(fk)").Check(testkit.Rows())
	res := tk.MustQuery("explain select b from t2 use index(fk)")
	plan := bytes.NewBuffer(nil)
	rows := res.Rows()
	for _, row := range rows {
		for _, c := range row {
			plan.WriteString(c.(string))
			plan.WriteString(" ")
		}
	}
	require.Regexp(t, ".*IndexReader.*index:fk.*", plan.String())

	// Test add multiple foreign key constraint in one statement.
	tk.MustExec("alter table t2 add column c int, add column d int, add column e int;")
	tk.MustExec("alter table t2 add index idx_c(c, d, e)")
	tk.MustExec("alter table t2 add constraint fk_c foreign key (c) references t1(b), " +
		"add constraint fk_d foreign key (d) references t1(b)," +
		"add constraint fk_e foreign key (e) references t1(b)")
	tbl2Info = getTableInfo(t, dom, "test", "t2")
	require.Equal(t, 4, len(tbl2Info.Indices))
	names := []string{"fk", "idx_c", "fk_d", "fk_e"}
	for i, idx := range tbl2Info.Indices {
		require.Equal(t, names[i], idx.Name.L)
		require.Equal(t, model.StatePublic, idx.State)
	}
	names = []string{"fk", "fk_c", "fk_d", "fk_e"}
	for i, fkInfo := range tbl2Info.ForeignKeys {
		require.Equal(t, names[i], fkInfo.Name.L)
		require.Equal(t, model.StatePublic, fkInfo.State)
	}
	tk.MustGetDBError("insert into t2 (id, b) values (1,1)", plannercore.ErrNoReferencedRow2)
	tk.MustGetDBError("insert into t2 (id, c) values (1,1)", plannercore.ErrNoReferencedRow2)
	tk.MustGetDBError("insert into t2 (id, d) values (1,1)", plannercore.ErrNoReferencedRow2)
	tk.MustGetDBError("insert into t2 (id, e) values (1,1)", plannercore.ErrNoReferencedRow2)

	// Test add multiple foreign key constraint in one statement but failed.
	tk.MustExec("alter table t2 drop foreign key fk")
	tk.MustExec("alter table t2 drop foreign key fk_c")
	tk.MustExec("alter table t2 drop foreign key fk_d")
	tk.MustExec("alter table t2 drop foreign key fk_e")
	tk.MustGetDBError("alter table t2 add constraint fk_c foreign key (c) references t1(b), "+
		"add constraint fk_d foreign key (d) references t1(b),"+
		"add constraint fk_e foreign key (e) references t1(unknown_col)", infoschema.ErrForeignKeyNoColumnInParent)
	tbl2Info = getTableInfo(t, dom, "test", "t2")
	require.Equal(t, 0, len(tbl2Info.ForeignKeys))
	tk.MustGetDBError("alter table t2 drop index idx_c, add constraint fk_c foreign key (c) references t1(b)", dbterror.ErrDropIndexNeededInForeignKey)
}

func TestAddForeignKey2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	d := dom.DDL()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (id int key, b int, index(b));")
	var wg sync.WaitGroup
	var addErr error
	tc := &ddl.TestDDLCallback{}
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StatePublic || job.Type != model.ActionDropIndex {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			addErr = tk2.ExecToErr("alter table t2 add foreign key (b) references t1(id);")
		}()
		// make sure tk2's ddl job already put into ddl job queue.
		time.Sleep(time.Millisecond * 100)
	}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	tk.MustExec("alter table t2 drop index b")
	wg.Wait()
	require.Error(t, addErr)
	require.Equal(t, "[ddl:-1]Failed to add the foreign key constraint. Missing index for 'fk_1' foreign key columns in the table 't2'", addErr.Error())
}

func TestAddForeignKey3(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	d := dom.DDL()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("create table t1 (id int key, b int, index(b));")
	tk.MustExec("create table t2 (id int, b int, index(id), index(b));")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")

	var insertErrs []error
	var deleteErrs []error
	tc := &ddl.TestDDLCallback{}
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type != model.ActionAddForeignKey {
			return
		}
		if job.SchemaState == model.StateWriteOnly || job.SchemaState == model.StateWriteReorganization {
			err := tk2.ExecToErr("insert into t2 values (10, 10)")
			insertErrs = append(insertErrs, err)
			err = tk2.ExecToErr("delete from t1 where id = 1")
			deleteErrs = append(deleteErrs, err)
		}
	}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	tk.MustExec("alter table t2 add foreign key (id) references t1(id) on delete cascade")
	require.Equal(t, 2, len(insertErrs))
	for _, err := range insertErrs {
		require.Error(t, err)
		require.Equal(t, "[planner:1452]Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk_1` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE)", err.Error())
	}
	for _, err := range deleteErrs {
		require.Error(t, err)
		require.Equal(t, "[planner:1451]Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk_1` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE)", err.Error())
	}
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 1", "2 2", "3 3"))
}

func TestAlterTableAddForeignKeyError(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1;")
	tk.MustExec("use test")
	cases := []struct {
		prepares []string
		alter    string
		err      string
	}{
		{
			prepares: []string{
				"create table t1 (id int, a int, b int);",
				"create table t2 (a int, b int);",
			},
			alter: "alter  table t2 add foreign key fk(b) references t_unknown(id)",
			err:   "[schema:1824]Failed to open the referenced table 't_unknown'",
		},
		{
			prepares: []string{
				"create table t1 (id int, a int, b int);",
				"create table t2 (a int, b int);",
			},
			alter: "alter  table t2 add foreign key fk(b) references t1(c_unknown)",
			err:   "[schema:3734]Failed to add the foreign key constraint. Missing column 'c_unknown' for constraint 'fk' in the referenced table 't1'",
		},
		{
			prepares: []string{
				"create table t1 (id int, a int, b int);",
				"create table t2 (a int, b int);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(b)",
			err:   "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			prepares: []string{
				"create table t1 (id int, a int, b int not null, index(b));",
				"create table t2 (a int, b int not null);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(b) on update set null",
			err:   "[schema:1830]Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
		},
		{
			prepares: []string{
				"create table t1 (id int, a int, b int not null, index(b));",
				"create table t2 (a int, b int not null);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(b) on delete set null",
			err:   "[schema:1830]Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a int, b int as (a) virtual, index(b));",
				"create table t2 (a int, b int);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(b)",
			err:   "[schema:3733]Foreign key 'fk_b' uses virtual column 'b' which is not supported.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a int, b int, index(b));",
				"create table t2 (a int, b int as (a) virtual);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(b)",
			err:   "[schema:3733]Foreign key 'fk_b' uses virtual column 'b' which is not supported.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a int);",
				"create table t2 (a int, b varchar(10));",
			},
			alter: "alter  table t2 add foreign key fk(b) references t1(id)",
			err:   "[ddl:3780]Referencing column 'b' and referenced column 'id' in foreign key constraint 'fk' are incompatible.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a int not null, index(a));",
				"create table t2 (a int, b int unsigned);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(a)",
			err:   "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a bigint, index(a));",
				"create table t2 (a int, b int);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(a)",
			err:   "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a varchar(10) charset utf8, index(a));",
				"create table t2 (a int, b varchar(10) charset utf8mb4);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(a)",
			err:   "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a varchar(10) collate utf8_bin, index(a));",
				"create table t2 (a int, b varchar(10) collate utf8mb4_bin);",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(a)",
			err:   "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a varchar(10));",
				"create table t2 (a int, b varchar(10));",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(a)",
			err:   "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a varchar(10), index (a(5)));",
				"create table t2 (a int, b varchar(10));",
			},
			alter: "alter  table t2 add foreign key fk_b(b) references t1(a)",
			err:   "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			prepares: []string{
				"create table t1 (id int key, a int)",
				"create table t2 (id int,     b int, index(b))",
				"insert into t2 values (1,1)",
			},
			alter: "alter table t2 add foreign key fk_b(b) references t1(id)",
			err:   "[ddl:1452]Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `t1` (`id`))",
		},
		{
			prepares: []string{
				"create table t1 (id int, a int, b int, index(a,b))",
				"create table t2 (id int, a int, b int, index(a,b))",
				"insert into t2 values (1, 1, null), (2, null, 1), (3, null, null), (4, 1, 1)",
			},
			alter: "alter table t2 add foreign key fk_b(a, b) references t1(a, b)",
			err:   "[ddl:1452]Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk_b` FOREIGN KEY (`a`, `b`) REFERENCES `t1` (`a`, `b`))",
		},
		{
			prepares: []string{
				"create table t1 (id int key);",
				"create table t2 (a int, b int unique);",
			},
			alter: "alter  table t2 add foreign key name5678901234567890123456789012345678901234567890123456789012345(b) references t1(id)",
			err:   "[ddl:1059]Identifier name 'name5678901234567890123456789012345678901234567890123456789012345' is too long",
		},
		{
			prepares: []string{
				"create table t1 (id int key);",
				"create table t2 (a int, b int unique);",
			},
			alter: "alter  table t2 add constraint name5678901234567890123456789012345678901234567890123456789012345 foreign key (b) references t1(id)",
			err:   "[ddl:1059]Identifier name 'name5678901234567890123456789012345678901234567890123456789012345' is too long",
		},
	}
	for i, ca := range cases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		for _, sql := range ca.prepares {
			tk.MustExec(sql)
		}
		err := tk.ExecToErr(ca.alter)
		require.Error(t, err, fmt.Sprintf("%v, %v", i, ca.err))
		require.Equal(t, ca.err, err.Error())
	}

	passCases := [][]string{
		{
			"create table t1 (id int key, a int, b int, index(a))",
			"alter table t1 add foreign key fk(a) references t1(id)",
		},
		{
			"create table t1 (id int key, b int not null, index(b))",
			"create table t2 (a int, b int, index(b));",
			"alter table t2 add foreign key fk_b(b) references t1(b)",
		},
		{
			"create table t1 (id int key, a varchar(10), index(a));",
			"create table t2 (a int, b varchar(20), index(b));",
			"alter table t2 add foreign key fk_b(b) references t1(a)",
		},
		{
			"create table t1 (id int key, a decimal(10,5), index(a));",
			"create table t2 (a int, b decimal(20, 10), index(b));",
			"alter table t2 add foreign key fk_b(b) references t1(a)",
		},
		{
			"create table t1 (id int key, a varchar(10), index (a(10)));",
			"create table t2 (a int, b varchar(20), index(b));",
			"alter table t2 add foreign key fk_b(b) references t1(a)",
		},
		{
			"create table t1 (id int key, a int)",
			"create table t2 (id int,     b int, index(b))",
			"insert into t2 values (1, null)",
			"alter table t2 add foreign key fk_b(b) references t1(id)",
		},
		{
			"create table t1 (id int, a int, b int, index(a,b))",
			"create table t2 (id int, a int, b int, index(a,b))",
			"insert into t2 values (1, 1, null), (2, null, 1), (3, null, null)",
			"alter table t2 add foreign key fk_b(a, b) references t1(a, b)",
		},
		{
			"set @@foreign_key_checks=0;",
			"create table t1 (id int, a int, b int, index(a,b))",
			"create table t2 (id int, a int, b int, index(a,b))",
			"insert into t2 values (1, 1, 1)",
			"alter table t2 add foreign key fk_b(a, b) references t1(a, b)",
			"set @@foreign_key_checks=1;",
		},
		{
			"set @@foreign_key_checks=0;",
			"create table t2 (a int, b int, index(b));",
			"alter table t2 add foreign key fk_b(b) references t_unknown(a)",
			"set @@foreign_key_checks=1;",
		},
		{
			"create table t1 (id int key);",
			"create table t2 (a int, b int unique);",
			"alter  table t2 add foreign key name567890123456789012345678901234567890123456789012345678901234(b) references t1(id)",
		},
	}
	for _, ca := range passCases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		for _, sql := range ca {
			tk.MustExec(sql)
		}
	}
}

func TestRenameTablesWithForeignKey(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=0;")
	tk.MustExec("create database test1")
	tk.MustExec("create database test2")
	tk.MustExec("use test")
	tk.MustExec("create table t0 (id int key, b int);")
	tk.MustExec("create table t1 (id int key, b int, index(b), foreign key fk(b) references t2(id));")
	tk.MustExec("create table t2 (id int key, b int, index(b), foreign key fk(b) references t1(id));")
	tk.MustExec("rename table test.t1 to test1.tt1, test.t2 to test2.tt2, test.t0 to test.tt0")

	// check the schema diff
	diff := getLatestSchemaDiff(t, tk)
	require.Equal(t, model.ActionRenameTables, diff.Type)
	require.Equal(t, 3, len(diff.AffectedOpts))

	// check referred foreign key information.
	t1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	t2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t2")
	require.Equal(t, 0, len(t1ReferredFKs))
	require.Equal(t, 0, len(t2ReferredFKs))
	tt1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test1", "tt1")
	tt2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "tt2")
	require.Equal(t, 1, len(tt1ReferredFKs))
	require.Equal(t, 1, len(tt2ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("tt2"),
		ChildFKName: model.NewCIStr("fk"),
	}, *tt1ReferredFKs[0])
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test1"),
		ChildTable:  model.NewCIStr("tt1"),
		ChildFKName: model.NewCIStr("fk"),
	}, *tt2ReferredFKs[0])

	// check show create table information
	tk.MustQuery("show create table test1.tt1").Check(testkit.Rows("tt1 CREATE TABLE `tt1` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  CONSTRAINT `fk` FOREIGN KEY (`b`) REFERENCES `test2`.`tt2` (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table test2.tt2").Check(testkit.Rows("tt2 CREATE TABLE `tt2` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  CONSTRAINT `fk` FOREIGN KEY (`b`) REFERENCES `test1`.`tt1` (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func getLatestSchemaDiff(t *testing.T, tk *testkit.TestKit) *model.SchemaDiff {
	ctx := tk.Session()
	err := sessiontxn.NewTxn(context.Background(), ctx)
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	ver, err := m.GetSchemaVersion()
	require.NoError(t, err)
	diff, err := m.GetSchemaDiff(ver)
	require.NoError(t, err)
	return diff
}
