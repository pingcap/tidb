// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package checksum_test

import (
	"context"
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testChecksumSuite{})

type testChecksumSuite struct {
	mock *mock.Cluster
}

func (s *testChecksumSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testChecksumSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testChecksumSuite) getTableInfo(c *C, db, table string) *model.TableInfo {
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil)
	cDBName := model.NewCIStr(db)
	cTableName := model.NewCIStr(table)
	tableInfo, err := info.TableByName(cDBName, cTableName)
	c.Assert(err, IsNil)
	return tableInfo.Meta()
}

func (s *testChecksumSuite) TestChecksum(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")
	tableInfo1 := s.getTableInfo(c, "test", "t1")
	exe1, err := checksum.NewExecutorBuilder(tableInfo1, math.MaxUint64).
		SetConcurrency(variable.DefChecksumTableConcurrency).
		Build()
	c.Assert(err, IsNil)
	c.Assert(exe1.Each(func(r *kv.Request) error {
		c.Assert(r.NotFillCache, IsTrue)
		c.Assert(r.Concurrency, Equals, variable.DefChecksumTableConcurrency)
		return nil
	}), IsNil)
	c.Assert(exe1.Len(), Equals, 1)
	resp, err := exe1.Execute(context.TODO(), s.mock.Storage.GetClient(), func() {})
	c.Assert(err, IsNil)
	// Cluster returns a dummy checksum (all fields are 1).
	c.Assert(resp.Checksum, Equals, uint64(1), Commentf("%v", resp))
	c.Assert(resp.TotalKvs, Equals, uint64(1), Commentf("%v", resp))
	c.Assert(resp.TotalBytes, Equals, uint64(1), Commentf("%v", resp))

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("alter table t2 add index i2(a);")
	tk.MustExec("insert into t2 values (10);")
	tableInfo2 := s.getTableInfo(c, "test", "t2")
	exe2, err := checksum.NewExecutorBuilder(tableInfo2, math.MaxUint64).Build()
	c.Assert(err, IsNil)
	c.Assert(exe2.Len(), Equals, 2, Commentf("%v", tableInfo2))
	resp2, err := exe2.Execute(context.TODO(), s.mock.Storage.GetClient(), func() {})
	c.Assert(err, IsNil)
	c.Assert(resp2.Checksum, Equals, uint64(0), Commentf("%v", resp2))
	c.Assert(resp2.TotalKvs, Equals, uint64(2), Commentf("%v", resp2))
	c.Assert(resp2.TotalBytes, Equals, uint64(2), Commentf("%v", resp2))

	// Test rewrite rules
	tk.MustExec("alter table t1 add index i2(a);")
	tableInfo1 = s.getTableInfo(c, "test", "t1")
	oldTable := metautil.Table{Info: tableInfo1}
	exe2, err = checksum.NewExecutorBuilder(tableInfo2, math.MaxUint64).
		SetOldTable(&oldTable).Build()
	c.Assert(err, IsNil)
	c.Assert(exe2.Len(), Equals, 2)
	rawReqs, err := exe2.RawRequests()
	c.Assert(err, IsNil)
	c.Assert(rawReqs, HasLen, 2)
	for _, rawReq := range rawReqs {
		c.Assert(rawReq.Rule, NotNil)
	}
	resp2, err = exe2.Execute(context.TODO(), s.mock.Storage.GetClient(), func() {})
	c.Assert(err, IsNil)
	c.Assert(resp2, NotNil)

	// Test commonHandle ranges

	tk.MustExec("drop table if exists t3;")
	tk.MustExec("create table t3 (a char(255), b int, primary key(a) CLUSTERED);")
	tk.MustExec("insert into t3 values ('fffffffff', 1), ('010101010', 2), ('394393fj39efefe', 3);")
	tableInfo3 := s.getTableInfo(c, "test", "t3")
	exe3, err := checksum.NewExecutorBuilder(tableInfo3, math.MaxUint64).Build()
	c.Assert(err, IsNil)
	first := true
	c.Assert(exe3.Each(func(req *kv.Request) error {
		if first {
			first = false
			ranges, err := backup.BuildTableRanges(tableInfo3)
			c.Assert(err, IsNil)
			c.Assert(req.KeyRanges, DeepEquals, ranges[:1], Commentf("%v", req.KeyRanges))
		}
		return nil
	}), IsNil)
}
