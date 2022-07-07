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
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func testCreateForeignKey(t *testing.T, d ddl.DDL, ctx sessionctx.Context, dbInfo *model.DBInfo, tblInfo *model.TableInfo, fkName string, keys []string, refTable string, refKeys []string, onDelete ast.ReferOptionType, onUpdate ast.ReferOptionType) *model.Job {
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
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()

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
	tc.OnJobUpdatedExported = func(job *model.Job) {
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
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	ctx := testkit.NewTestKit(t, store).Session()
	job := testCreateForeignKey(t, d, ctx, dbInfo, tblInfo, "c1_fk", []string{"c1"}, "t2", []string{"c1"}, ast.ReferOptionCascade, ast.ReferOptionSetNull)
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
	tc2.OnJobUpdatedExported = func(job *model.Job) {
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
