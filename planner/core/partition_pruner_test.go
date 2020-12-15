// Copyright 2019 PingCAP, Inc.
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

package core_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testPartitionPruneSuit{})

type testPartitionPruneSuit struct {
	store    kv.Storage
	dom      *domain.Domain
	ctx      sessionctx.Context
	testData testutil.TestData
}

func (s *testPartitionPruneSuit) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_partition")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testPartitionPruneSuit) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.testData, err = testutil.LoadTestSuiteData("testdata", "partition_pruner")
	c.Assert(err, IsNil)
}

func (s *testPartitionPruneSuit) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.dom.Close()
	s.store.Close()
}

func (s *testPartitionPruneSuit) TestHashPartitionPruner(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("create table t2(id int, a int, b int, primary key(id, a)) partition by hash(id + a) partitions 10;")
	tk.MustExec("create table t1(id int primary key, a int, b int) partition by hash(id) partitions 10;")
	tk.MustExec("create table t3(id int, a int, b int, primary key(id, a)) partition by hash(id) partitions 10;")
	tk.MustExec("create table t4(d datetime, a int, b int, primary key(d, a)) partition by hash(year(d)) partitions 10;")
	tk.MustExec("create table t5(d date, a int, b int, primary key(d, a)) partition by hash(month(d)) partitions 10;")
	tk.MustExec("create table t6(a int, b int) partition by hash(a) partitions 3;")
	tk.MustExec("create table t7(a int, b int) partition by hash(a + b) partitions 10;")

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPartitionPruneSuit) TestListPartitionPruner(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_partition;")
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("create table t1 (id int, a int, b int                 ) partition by list (    a    ) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t2 (a int, id int, b int) partition by list (a*3 + b - 2*a - b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t3 (b int, id int, a int) partition by list columns (a) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t4 (id int, a int, b int, primary key (a)) partition by list (    a    ) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10));")
	tk.MustExec("create table t5 (a int, id int, b int, unique key (a,b)) partition by list (a*3 + b - 2*a - b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t6 (b int, id int, a int, unique key (a,b)) partition by list columns (a) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t3 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t4 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10)")
	tk.MustExec("insert into t5 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t6 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec(`create table t7 (a int unsigned) partition by list (a)(partition p0 values in (0),partition p1 values in (1),partition pnull values in (null),partition p2 values in (2));`)
	tk.MustExec("insert into t7 values (null),(0),(1),(2);")

	// tk2 use to compare the result with normal table.
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("drop database if exists test_partition_2;")
	tk2.MustExec("create database test_partition_2")
	tk2.MustExec("use test_partition_2")
	tk2.MustExec("create table t1 (id int, a int, b int)")
	tk2.MustExec("create table t2 (a int, id int, b int)")
	tk2.MustExec("create table t3 (b int, id int, a int)")
	tk2.MustExec("create table t4 (id int, a int, b int, primary key (a));")
	tk2.MustExec("create table t5 (a int, id int, b int, unique key (a,b));")
	tk2.MustExec("create table t6 (b int, id int, a int, unique key (a,b));")
	tk2.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t3 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t4 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10)")
	tk2.MustExec("insert into t5 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t6 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec(`create table t7 (a int unsigned);`)
	tk2.MustExec("insert into t7 values (null),(0),(1),(2);")

	var input []string
	var output []struct {
		SQL    string
		Result []string
		Plan   []string
	}
	s.testData.GetTestCases(c, &input, &output)
	valid := false
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		result := tk.MustQuery(tt)
		result.Check(testkit.Rows(output[i].Result...))
		// If the query doesn't specified the partition, compare the result with normal table
		if !strings.Contains(tt, "partition(") {
			result.Check(tk2.MustQuery(tt).Rows())
			valid = true
		}
		c.Assert(valid, IsTrue)
	}
}

func (s *testPartitionPruneSuit) TestListColumnsPartitionPruner(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_partition;")
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("create table t1 (id int, a int, b int) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
	tk.MustExec("create table t2 (id int, a int, b int) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
	tk.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	// tk1 use to test partition table with index.
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("drop database if exists test_partition_1;")
	tk1.MustExec("create database test_partition_1")
	tk1.MustExec("use test_partition_1")
	tk1.MustExec("create table t1 (id int, a int, b int, unique key (a,b,id)) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
	tk1.MustExec("create table t2 (id int, a int, b int, unique key (a,b,id)) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
	tk1.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk1.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	// tk2 use to compare the result with normal table.
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("drop database if exists test_partition_2;")
	tk2.MustExec("create database test_partition_2")
	tk2.MustExec("use test_partition_2")
	tk2.MustExec("create table t1 (id int, a int, b int)")
	tk2.MustExec("create table t2 (id int, a int, b int)")
	tk2.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk2.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	var input []string
	var output []struct {
		SQL       string
		Result    []string
		Plan      []string
		IndexPlan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	valid := false
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			// Test for table reader.
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			// Test for plan with index.
			output[i].IndexPlan = s.testData.ConvertRowsToStrings(tk1.MustQuery("explain " + tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		result := tk.MustQuery(tt)
		idxResult := tk1.MustQuery(tt)
		result.Check(idxResult.Rows())
		result.Check(testkit.Rows(output[i].Result...))
		// If the query doesn't specified the partition, compare the result with normal table
		if !strings.Contains(tt, "partition(") {
			result.Check(tk2.MustQuery(tt).Rows())
			valid = true
		}
	}
	c.Assert(valid, IsTrue)
}
