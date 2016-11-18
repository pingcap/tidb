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
	"fmt"
	"os"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func testCreateStore(c *C, name string) kv.Storage {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open(fmt.Sprintf("memory://%s", name))
	c.Assert(err, IsNil)
	return store
}

func testNewContext(c *C, d *ddl) context.Context {
	ctx := d.newMockContext()
	variable.BindSessionVars(ctx)
	return ctx
}

func getSchemaVer(c *C, ctx context.Context) int64 {
	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)
	c.Assert(txn, NotNil)
	m := meta.NewMeta(txn)
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

func checkHistoryJobArgs(c *C, ctx context.Context, id int64, args *historyJobArgs) {
	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	historyJob, err := t.GetHistoryDDLJob(id)
	c.Assert(err, IsNil)

	var v int64
	var ids []int64
	tbl := &model.TableInfo{}
	if args.tbl != nil {
		historyJob.DecodeArgs(&v, &tbl)
		c.Assert(v, Equals, args.ver)
		checkEqualTable(c, tbl, args.tbl)
		return
	}
	// only for create schema job
	db := &model.DBInfo{}
	if args.db != nil && len(args.tblIDs) == 0 {
		historyJob.DecodeArgs(&v, &db)
		c.Assert(v, Equals, args.ver)
		c.Assert(db, DeepEquals, args.db)
		return
	}
	// only for drop schema job
	historyJob.DecodeArgs(&v, &db, &ids)
	c.Assert(v, Equals, args.ver)
	c.Assert(db, DeepEquals, args.db)
	for _, id := range ids {
		c.Assert(args.tblIDs, HasKey, id)
		delete(args.tblIDs, id)
	}
	c.Assert(len(args.tblIDs), Equals, 0)
}

func init() {
	logLevel := os.Getenv("log_level")
	log.SetLevelByString(logLevel)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}
