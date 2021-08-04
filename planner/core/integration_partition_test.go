// Copyright 2021 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = SerialSuites(&testIntegrationPartitionSerialSuite{})

type testIntegrationPartitionSerialSuite struct {
	testData testutil.TestData
	store    kv.Storage
	dom      *domain.Domain
}

func (s *testIntegrationPartitionSerialSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "integration_partition_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationPartitionSerialSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testIntegrationPartitionSerialSuite) SetUpTest(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testIntegrationPartitionSerialSuite) TearDownTest(c *C) {
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testIntegrationPartitionSerialSuite) TestListPartitionPruning(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create database list_partition_pruning")
	tk.MustExec("use list_partition_pruning")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5),
    partition p2 values in (6, 7, 8),
    partition p3 values in (9, 10, 11))`)

	var input []string
	var output []struct {
		SQL         string
		DynamicPlan []string
		StaticPlan  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			output[i].DynamicPlan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			output[i].StaticPlan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		tk.MustQuery(tt).Check(testkit.Rows(output[i].DynamicPlan...))
		tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
		tk.MustQuery(tt).Check(testkit.Rows(output[i].StaticPlan...))
	}
}

func (s *testIntegrationPartitionSerialSuite) TestListPartitionOrderLimit(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create database list_partition_order_limit")
	tk.MustExec("use list_partition_order_limit")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	vals := ""
	for i := 0; i < 50; i++ {
		if vals != "" {
			vals += ", "
		}
		vals += fmt.Sprintf("(%v, %v)", i*2+rand.Intn(2), i*2+rand.Intn(2))
	}
	tk.MustExec(`insert into tlist values ` + vals)
	tk.MustExec(`insert into tnormal values ` + vals)

	for _, orderCol := range []string{"a", "b"} {
		for _, limitNum := range []string{"1", "5", "20", "100"} {
			randCond := fmt.Sprintf("where %v > %v", []string{"a", "b"}[rand.Intn(2)], rand.Intn(100))
			rs := tk.MustQuery(fmt.Sprintf(`select * from tnormal %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			rsDynamic := tk.MustQuery(fmt.Sprintf(`select * from tlist %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			rsStatic := tk.MustQuery(fmt.Sprintf(`select * from tlist %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			rs.Check(rsDynamic.Rows())
			rs.Check(rsStatic.Rows())
		}
	}
}

func (s *testIntegrationPartitionSerialSuite) TestListPartitionAgg(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create database list_partition_agg")
	tk.MustExec("use list_partition_agg")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	vals := ""
	for i := 0; i < 50; i++ {
		if vals != "" {
			vals += ", "
		}
		vals += fmt.Sprintf("(%v, %v)", rand.Intn(100), rand.Intn(100))
	}
	tk.MustExec(`insert into tlist values ` + vals)
	tk.MustExec(`insert into tnormal values ` + vals)

	for _, aggFunc := range []string{"min", "max", "sum", "count"} {
		c1, c2 := "a", "b"
		for i := 0; i < 2; i++ {
			rs := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tnormal group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			rsDynamic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tlist group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			rsStatic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tlist group by %v`, c1, aggFunc, c2, c1)).Sort()

			rs.Check(rsDynamic.Rows())
			rs.Check(rsStatic.Rows())
		}
	}
}

func genListPartition(begin, end int) string {
	buf := &bytes.Buffer{}
	buf.WriteString("(")
	for i := begin; i < end-1; i++ {
		buf.WriteString(fmt.Sprintf("%v, ", i))
	}
	buf.WriteString(fmt.Sprintf("%v)", end-1))
	return buf.String()
}
