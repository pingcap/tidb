// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	goctx "golang.org/x/net/context"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level:  logLevel,
		Format: "highlight",
	})
	TestingT(t)
}

func testCreateStore(c *C, name string) kv.Storage {
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	return store
}

func testNewContext(d *ddl) context.Context {
	ctx := mock.NewContext()
	ctx.Store = d.store
	return ctx
}

func testNewDDL(ctx goctx.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handle, hook Callback, lease time.Duration) *ddl {
	return newDDL(ctx, etcdCli, store, infoHandle, hook, lease, nil)
}

func getSchemaVer(c *C, ctx context.Context) int64 {
	err := ctx.NewTxn()
	c.Assert(err, IsNil)
	m := meta.NewMeta(ctx.Txn())
	ver, err := m.GetSchemaVersion()
	c.Assert(err, IsNil)
	return ver
}

type historyJobArgs struct {
	ver    int64
	db     *model.DBInfo
	tbl    *model.TableInfo
	tblIDs map[int64]struct{}
}

func checkEqualTable(c *C, t1, t2 *model.TableInfo) {
	c.Assert(t1.ID, Equals, t2.ID)
	c.Assert(t1.Name, Equals, t2.Name)
	c.Assert(t1.Charset, Equals, t2.Charset)
	c.Assert(t1.Collate, Equals, t2.Collate)
	c.Assert(t1.PKIsHandle, DeepEquals, t2.PKIsHandle)
	c.Assert(t1.Comment, DeepEquals, t2.Comment)
	c.Assert(t1.AutoIncID, DeepEquals, t2.AutoIncID)
}

func checkHistoryJob(c *C, job *model.Job) {
	c.Assert(job.State, Equals, model.JobStateSynced)
}

func checkHistoryJobArgs(c *C, ctx context.Context, id int64, args *historyJobArgs) {
	c.Assert(ctx.NewTxn(), IsNil)
	t := meta.NewMeta(ctx.Txn())
	historyJob, err := t.GetHistoryDDLJob(id)
	c.Assert(err, IsNil)

	if args.tbl != nil {
		c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
		checkEqualTable(c, historyJob.BinlogInfo.TableInfo, args.tbl)
		return
	}

	// for handling schema job
	c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
	c.Assert(historyJob.BinlogInfo.DBInfo, DeepEquals, args.db)
	// only for creating schema job
	if args.db != nil && len(args.tblIDs) == 0 {
		return
	}
}

func testCreateIndex(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args: []interface{}{unique, model.NewCIStr(indexName),
			[]*ast.IndexColName{{
				Column: &ast.ColumnName{Name: model.NewCIStr(colName)},
				Length: types.UnspecifiedLength}}},
	}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testDropIndex(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(indexName)},
	}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}
