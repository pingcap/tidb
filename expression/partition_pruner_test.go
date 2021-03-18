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

package expression_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testSuite2{})

type testSuite2 struct {
	store    kv.Storage
	dom      *domain.Domain
	ctx      sessionctx.Context
	testData testutil.TestData
}

func (s *testSuite2) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_partition")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuite2) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.testData, err = testutil.LoadTestSuiteData("testdata", "partition_pruner")
	c.Assert(err, IsNil)
}

func (s *testSuite2) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.dom.Close()
	s.store.Close()
}

func (s *testSuite2) TestHashPartitionPruner(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("drop table if exists t1, t2;")
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

func (s *testSuite2) TestIssue22898(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS test;")
	tk.MustExec("CREATE TABLE NT_RP3763 (COL1 TINYINT(8) SIGNED COMMENT \"NUMERIC NO INDEX\" DEFAULT 41,COL2 VARCHAR(20),COL3 DATETIME,COL4 BIGINT,COL5 FLOAT) PARTITION BY RANGE (COL1 * COL3) (PARTITION P0 VALUES LESS THAN (0),PARTITION P1 VALUES LESS THAN (10),PARTITION P2 VALUES LESS THAN (20),PARTITION P3 VALUES LESS THAN (30),PARTITION P4 VALUES LESS THAN (40),PARTITION P5 VALUES LESS THAN (50),PARTITION PMX VALUES LESS THAN MAXVALUE);")
	tk.MustExec("insert into NT_RP3763 (COL1,COL2,COL3,COL4,COL5) values(-82,\"夐齏醕皆磹漋甓崘潮嵙燷渏艂朼洛炷鉢儝鱈肇\",\"5748\\-06\\-26\\ 20:48:49\",-3133527360541070260,-2.624880003397658e+38);")
	tk.MustExec("insert into NT_RP3763 (COL1,COL2,COL3,COL4,COL5) values(48,\"簖鹩筈匹眜赖泽騈爷詵赺玡婙Ɇ郝鮙廛賙疼舢\",\"7228\\-12\\-13\\ 02:59:54\",-6181009269190017937,2.7731105531290494e+38);")
	tk.MustQuery("select * from `NT_RP3763` where `COL1` in (10, 48, -82);").Check(testkit.Rows("-82 夐齏醕皆磹漋甓崘潮嵙燷渏艂朼洛炷鉢儝鱈肇 5748-06-26 20:48:49 -3133527360541070260 -262488000000000000000000000000000000000", "48 簖鹩筈匹眜赖泽騈爷詵赺玡婙Ɇ郝鮙廛賙疼舢 7228-12-13 02:59:54 -6181009269190017937 277311060000000000000000000000000000000"))
	tk.MustQuery("select * from `NT_RP3763` where `COL1` in (48);").Check(testkit.Rows("48 簖鹩筈匹眜赖泽騈爷詵赺玡婙Ɇ郝鮙廛賙疼舢 7228-12-13 02:59:54 -6181009269190017937 277311060000000000000000000000000000000"))
}
