// Copyright 2017 PingCAP, Inc.
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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testSuite) TestAnalyzeTable(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`ANALYZE TABLE mysql.GLOBAL_VARIABLES`)
	ctx := tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("mysql"), model.NewCIStr("GLOBAL_VARIABLES"))
	c.Check(err, IsNil)
	tableID := t.Meta().ID

	err = ctx.NewTxn()
	c.Check(err, IsNil)
	txn := ctx.Txn()
	mt := meta.NewMeta(txn)
	tpb, err := mt.GetTableStats(tableID)
	c.Check(err, IsNil)
	c.Check(tpb, NotNil)
	tStats, err := statistics.TableFromPB(t.Meta(), tpb)
	//c.Check(err, IsNil)
	//c.Check(tStats, NotNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	//tk.MustExec("create index ind_a on t (a)")
	tk.MustExec("insert into t (a,b) values (1,1)")
	tk.MustExec("insert into t (a,b) values (1,2)")
	tk.MustExec("insert into t (a,b) values (2,1)")
	tk.MustExec("insert into t (a,b) values (2,2)")
	tk.MustExec("analyze table t")
	ctx = tk.Se.(context.Context)
	is = sessionctx.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Check(err, IsNil)
	tableID = t.Meta().ID

	err = ctx.NewTxn()
	c.Check(err, IsNil)
	txn = ctx.Txn()
	mt = meta.NewMeta(txn)
	tpb, err = mt.GetTableStats(tableID)
	c.Check(err, IsNil)
	c.Check(tpb, NotNil)
	tStats, err = statistics.TableFromPB(t.Meta(), tpb)
	c.Check(err, IsNil)
	c.Check(tStats, NotNil)

	col := tStats.Columns[0]
	sc := new(variable.StatementContext)
	count, err := col.EqualRowCount(sc, types.NewIntDatum(1))
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))

	col = tStats.Columns[1]
	count, err = col.EqualRowCount(sc, types.NewIntDatum(2))
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))
}
