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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
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
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
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
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
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
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
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
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop database if exists test_partition;")
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("create table t1 (id int, a int, b int) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
	tk.MustExec("create table t2 (id int, a int, b int) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
	tk.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	// tk1 use to test partition table with index.
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("drop database if exists test_partition_1;")
	tk1.MustExec("create database test_partition_1")
	tk1.MustExec("use test_partition_1")
	tk1.MustExec("set @@session.tidb_enable_list_partition = ON")
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

	var input []struct {
		SQL    string
		Pruner string
	}
	var output []struct {
		SQL       string
		Result    []string
		Plan      []string
		IndexPlan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	valid := false
	for i, tt := range input {
		// Test for table without index.
		plan := tk.MustQuery("explain format = 'brief' " + tt.SQL)
		planTree := s.testData.ConvertRowsToStrings(plan.Rows())
		// Test for table with index.
		indexPlan := tk1.MustQuery("explain format = 'brief' " + tt.SQL)
		indexPlanTree := s.testData.ConvertRowsToStrings(indexPlan.Rows())
		s.testData.OnRecord(func() {
			output[i].SQL = tt.SQL
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt.SQL).Rows())
			// Test for table without index.
			output[i].Plan = planTree
			// Test for table with index.
			output[i].IndexPlan = indexPlanTree
		})
		// compare the plan.
		plan.Check(testkit.Rows(output[i].Plan...))
		indexPlan.Check(testkit.Rows(output[i].IndexPlan...))

		// compare the pruner information.
		s.checkPrunePartitionInfo(c, tt.SQL, tt.Pruner, planTree)
		s.checkPrunePartitionInfo(c, tt.SQL, tt.Pruner, indexPlanTree)

		// compare the result.
		result := tk.MustQuery(tt.SQL)
		idxResult := tk1.MustQuery(tt.SQL)
		result.Check(idxResult.Rows())
		result.Check(testkit.Rows(output[i].Result...))

		// If the query doesn't specified the partition, compare the result with normal table
		if !strings.Contains(tt.SQL, "partition(") {
			result.Check(tk2.MustQuery(tt.SQL).Rows())
			valid = true
		}
	}
	c.Assert(valid, IsTrue)
}

func (s *testPartitionPruneSuit) checkPrunePartitionInfo(c *C, query string, infos1 string, plan []string) {
	infos2 := s.getPartitionInfoFromPlan(plan)
	c.Assert(infos1, Equals, infos2, Commentf("the query is: %v, the plan is:\n%v", query, strings.Join(plan, "\n")))
}

type testTablePartitionInfo struct {
	Table      string
	Partitions string
}

// getPartitionInfoFromPlan uses to extract table partition information from the plan tree string. Here is an example, the plan is like below:
//          "Projection_7 80.00 root  test_partition.t1.id, test_partition.t1.a, test_partition.t1.b, test_partition.t2.id, test_partition.t2.a, test_partition.t2.b",
//          "└─HashJoin_9 80.00 root  CARTESIAN inner join",
//          "  ├─TableReader_12(Build) 8.00 root partition:p1 data:Selection_11",
//          "  │ └─Selection_11 8.00 cop[tikv]  1, eq(test_partition.t2.b, 6), in(test_partition.t2.a, 6, 7, 8)",
//          "  │   └─TableFullScan_10 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
//          "  └─TableReader_15(Probe) 10.00 root partition:p0 data:Selection_14",
//          "    └─Selection_14 10.00 cop[tikv]  1, eq(test_partition.t1.a, 5)",
//          "      └─TableFullScan_13 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"
//
// The return table partition info is: t1: p0; t2: p1
func (s *testPartitionPruneSuit) getPartitionInfoFromPlan(plan []string) string {
	infos := make([]testTablePartitionInfo, 0, 2)
	info := testTablePartitionInfo{}
	for _, row := range plan {
		partitions := s.getFieldValue("partition:", row)
		if partitions != "" {
			info.Partitions = partitions
			continue
		}
		tbl := s.getFieldValue("table:", row)
		if tbl != "" {
			info.Table = tbl
			infos = append(infos, info)
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Table != infos[j].Table {
			return infos[i].Table < infos[j].Table
		}
		return infos[i].Partitions < infos[j].Partitions
	})
	buf := bytes.NewBuffer(nil)
	for i, info := range infos {
		if i > 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(fmt.Sprintf("%v: %v", info.Table, info.Partitions))
	}
	return buf.String()
}

func (s *testPartitionPruneSuit) getFieldValue(prefix, row string) string {
	if idx := strings.Index(row, prefix); idx > 0 {
		start := idx + len(prefix)
		end := strings.Index(row[start:], " ")
		if end > 0 {
			value := row[start : start+end]
			value = strings.Trim(value, ",")
			return value
		}
	}
	return ""
}

func (s *testPartitionPruneSuit) TestListColumnsPartitionPrunerRandom(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	for count := 0; count < 5; count++ {
		partitionNum := rand.Intn(10) + 1
		valueNum := rand.Intn(10) + 1
		condNum := 20

		partitionDefs := make([][]string, partitionNum)
		for id := 0; id < valueNum; id++ {
			for a := 0; a < valueNum; a++ {
				for b := 0; b < valueNum; b++ {
					idx := rand.Intn(partitionNum)
					partitionDefs[idx] = append(partitionDefs[idx], fmt.Sprintf("(%v,%v,%v)", b, id, a))
				}
			}
		}
		validIdx := 0
		for _, def := range partitionDefs {
			if len(def) > 0 {
				partitionDefs[validIdx] = def
				validIdx++
			}
		}
		partitionDefs = partitionDefs[:validIdx]
		createSQL := bytes.NewBuffer(make([]byte, 0, 1024*1024))
		// Generate table definition.
		colNames := []string{"id", "a", "b"}
		createSQL.WriteString("create table t1 (id int, a int, b int")
		// Generate Index definition.
		if rand.Int()%2 == 0 {
			createSQL.WriteString(", index (")
			n := rand.Intn(len(colNames)) + 1
			cols := map[string]struct{}{}
			for i := 0; i < n; i++ {
				col := colNames[rand.Intn(len(colNames))]
				cols[col] = struct{}{}
			}
			cnt := 0
			for col := range cols {
				if cnt > 0 {
					createSQL.WriteString(",")
				}
				createSQL.WriteString(col)
				cnt++
			}
			createSQL.WriteString(")")
		}
		createSQL.WriteString(" ) partition by list columns (b, id, a) (")

		for i := range partitionDefs {
			if i > 0 {
				createSQL.WriteString(",")
			}
			createSQL.WriteString(fmt.Sprintf("partition p%v values in (", i))
			for idx, v := range partitionDefs[i] {
				if idx > 0 {
					createSQL.WriteString(",")
				}
				createSQL.WriteString(v)
			}
			createSQL.WriteString(")")
		}
		createSQL.WriteString(")")

		// Create table.
		tk.MustExec("drop database if exists test_partition;")
		tk.MustExec("create database test_partition")
		tk.MustExec("use test_partition")
		tk.MustExec("set @@session.tidb_enable_list_partition = ON")
		tk.MustExec(createSQL.String())

		tk1 := testkit.NewTestKit(c, s.store)
		tk1.MustExec("drop database if exists test_partition_1;")
		tk1.MustExec("create database test_partition_1")
		tk1.MustExec("use test_partition_1")
		tk1.MustExec("create table t1 (id int, a int, b int)")

		// prepare data.
		for _, def := range partitionDefs {
			insert := fmt.Sprintf("insert into t1 (b,id,a) values %v", strings.Join(def, ","))
			tk.MustExec(insert)
			tk1.MustExec(insert)

			// Test query without condition
			query := fmt.Sprintf("select * from t1 order by id,a,b")
			tk.MustQuery(query).Check(tk1.MustQuery(query).Rows())
		}

		// Test for single column condition.
		for i := 0; i < valueNum+1; i++ {
			query := fmt.Sprintf("select * from t1 where id = %v order by id,a,b", i)
			tk.MustQuery(query).Check(tk1.MustQuery(query).Rows())
			query = fmt.Sprintf("select * from t1 where a = %v order by id,a,b", i)
			tk.MustQuery(query).Check(tk1.MustQuery(query).Rows())
			query = fmt.Sprintf("select * from t1 where b = %v order by id,a,b", i)
			tk.MustQuery(query).Check(tk1.MustQuery(query).Rows())
		}
		// Test for multi-columns condition.
		genCond := func() string {
			col := colNames[rand.Intn(len(colNames))]
			value := rand.Intn(valueNum + 2)
			switch rand.Int() % 3 {
			case 0:
				return fmt.Sprintf(" %v = %v ", col, value)
			case 1:
				return fmt.Sprintf(" %v = %v ", value, col)
			default:
				buf := bytes.NewBuffer(nil)
				buf.WriteString(fmt.Sprintf(" %v in (", col))
				n := rand.Intn(valueNum+5) + 1
				for i := 0; i < n; i++ {
					if i > 0 {
						buf.WriteString(",")
					}
					value := rand.Intn(valueNum + 2)
					buf.WriteString(fmt.Sprintf("%v", value))
				}
				buf.WriteString(")")
				return buf.String()
			}
		}
		for i := 0; i < 500; i++ {
			condCnt := rand.Intn(condNum) + 1
			query := bytes.NewBuffer(nil)
			query.WriteString("select * from t1 where ")
			for j := 0; j < condCnt; j++ {
				if j > 0 {
					if rand.Int()%2 == 0 {
						query.WriteString(" and ")
					} else {
						query.WriteString(" or ")
					}
				}
				query.WriteString(genCond())
			}
			query.WriteString(" order by id,a,b")
			tk.MustQuery(query.String()).Check(tk1.MustQuery(query.String()).Rows())
		}
	}
}

func (s *testPartitionPruneSuit) TestIssue22635(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec(`
CREATE TABLE t1 (
  a int(11) DEFAULT NULL,
  b int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH( a )
PARTITIONS 4`)
	tk.MustQuery("SELECT (SELECT tt.a FROM t1  tt LIMIT 1) aa, COUNT(DISTINCT b) FROM t1  GROUP BY aa").Check(testkit.Rows()) // work fine without any error

	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustQuery("SELECT (SELECT tt.a FROM t1  tt LIMIT 1) aa, COUNT(DISTINCT b) FROM t1  GROUP BY aa").Check(testkit.Rows("1 1"))

	tk.MustExec("insert into t1 values (2, 2), (2, 2)")
	tk.MustQuery("SELECT (SELECT tt.a FROM t1  tt LIMIT 1) aa, COUNT(DISTINCT b) FROM t1  GROUP BY aa").Check(testkit.Rows("1 2"))

	tk.MustExec("insert into t1 values (3, 3), (3, 3), (3, 3)")
	tk.MustQuery("SELECT (SELECT tt.a FROM t1  tt LIMIT 1) aa, COUNT(DISTINCT b) FROM t1  GROUP BY aa").Check(testkit.Rows("1 3"))

	tk.MustExec("insert into t1 values (4, 4), (4, 4), (4, 4), (4, 4)")
	tk.MustQuery("SELECT (SELECT tt.a FROM t1  tt LIMIT 1) aa, COUNT(DISTINCT b) FROM t1  GROUP BY aa").Check(testkit.Rows("4 4"))
}

func (s *testPartitionPruneSuit) TestIssue22898(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS test;")
	tk.MustExec("CREATE TABLE NT_RP3763 (COL1 TINYINT(8) SIGNED COMMENT \"NUMERIC NO INDEX\" DEFAULT 41,COL2 VARCHAR(20),COL3 DATETIME,COL4 BIGINT,COL5 FLOAT) PARTITION BY RANGE (COL1 * COL3) (PARTITION P0 VALUES LESS THAN (0),PARTITION P1 VALUES LESS THAN (10),PARTITION P2 VALUES LESS THAN (20),PARTITION P3 VALUES LESS THAN (30),PARTITION P4 VALUES LESS THAN (40),PARTITION P5 VALUES LESS THAN (50),PARTITION PMX VALUES LESS THAN MAXVALUE);")
	tk.MustExec("insert into NT_RP3763 (COL1,COL2,COL3,COL4,COL5) values(-82,\"夐齏醕皆磹漋甓崘潮嵙燷渏艂朼洛炷鉢儝鱈肇\",\"5748\\-06\\-26\\ 20:48:49\",-3133527360541070260,-2.624880003397658e+38);")
	tk.MustExec("insert into NT_RP3763 (COL1,COL2,COL3,COL4,COL5) values(48,\"簖鹩筈匹眜赖泽騈爷詵赺玡婙Ɇ郝鮙廛賙疼舢\",\"7228\\-12\\-13\\ 02:59:54\",-6181009269190017937,2.7731105531290494e+38);")
	tk.MustQuery("select * from `NT_RP3763` where `COL1` in (10, 48, -82);").Check(testkit.Rows("-82 夐齏醕皆磹漋甓崘潮嵙燷渏艂朼洛炷鉢儝鱈肇 5748-06-26 20:48:49 -3133527360541070260 -262488000000000000000000000000000000000", "48 簖鹩筈匹眜赖泽騈爷詵赺玡婙Ɇ郝鮙廛賙疼舢 7228-12-13 02:59:54 -6181009269190017937 277311060000000000000000000000000000000"))
	tk.MustQuery("select * from `NT_RP3763` where `COL1` in (48);").Check(testkit.Rows("48 簖鹩筈匹眜赖泽騈爷詵赺玡婙Ɇ郝鮙廛賙疼舢 7228-12-13 02:59:54 -6181009269190017937 277311060000000000000000000000000000000"))
}

func (s *testPartitionPruneSuit) TestIssue23622(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int, b int) partition by range (a) (partition p0 values less than (0), partition p1 values less than (5));")
	tk.MustExec("insert into t2(a) values (-1), (1);")
	tk.MustQuery("select * from t2 where a > 10 or b is NULL order by a;").Check(testkit.Rows("-1 <nil>", "1 <nil>"))
}

func (s *testPartitionPruneSuit) Test22396(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS test;")
	tk.MustExec("CREATE TABLE test(a INT, b INT, PRIMARY KEY(a, b)) PARTITION BY RANGE (a + b) (PARTITION p0 VALUES LESS THAN (20),PARTITION p1 VALUES LESS THAN MAXVALUE);")
	tk.MustExec("INSERT INTO test(a, b) VALUES(1, 11),(2, 22),(3, 33),(10, 44),(9, 55);")
	tk.MustQuery("SELECT * FROM test WHERE a = 1;")
	tk.MustQuery("SELECT * FROM test WHERE b = 1;")
	tk.MustQuery("SELECT * FROM test WHERE a = 1 AND b = 1;")
	tk.MustQuery("SELECT * FROM test WHERE a + b = 2;")
}

func (s *testPartitionPruneSuit) TestIssue23608(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int) partition by hash (a) partitions 10")
	tk.MustExec("insert into t1 values (1), (2), (12), (3), (11), (13)")
	tk.MustQuery("select * from t1 where a not between 2 and 2").Sort().Check(testkit.Rows("1", "11", "12", "13", "3"))
	tk.MustQuery("select * from t1 where not (a < -20 or a > 20)").Sort().Check(testkit.Rows("1", "11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t1 where not (a > 0 and a < 10)").Sort().Check(testkit.Rows("11", "12", "13"))
	tk.MustQuery("select * from t1 where not (a < -20)").Sort().Check(testkit.Rows("1", "11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t1 where not (a > 20)").Sort().Check(testkit.Rows("1", "11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t1 where not (a = 1)").Sort().Check(testkit.Rows("11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t1 where not (a != 1)").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t2")
	tk.MustExec(`
create table t2(a int)
partition by range (a) (
    partition p0 values less than (0),
    partition p1 values less than (10),
    partition p2 values less than (20)
)`)
	tk.MustQuery("explain format = 'brief' select * from t2 where not (a < 5)").Check(testkit.Rows(
		"PartitionUnion 6666.67 root  ",
		"├─TableReader 3333.33 root  data:Selection",
		"│ └─Selection 3333.33 cop[tikv]  ge(test.t2.a, 5)",
		"│   └─TableFullScan 10000.00 cop[tikv] table:t2, partition:p1 keep order:false, stats:pseudo",
		"└─TableReader 3333.33 root  data:Selection",
		"  └─Selection 3333.33 cop[tikv]  ge(test.t2.a, 5)",
		"    └─TableFullScan 10000.00 cop[tikv] table:t2, partition:p2 keep order:false, stats:pseudo"))

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3(a int) partition by hash (a) partitions 10")
	tk.MustExec("insert into t3 values (1), (2), (12), (3), (11), (13)")
	tk.MustQuery("select * from t3 where a not between 2 and 2").Sort().Check(testkit.Rows("1", "11", "12", "13", "3"))
	tk.MustQuery("select * from t3 where not (a < -20 or a > 20)").Sort().Check(testkit.Rows("1", "11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t3 where not (a > 0 and a < 10)").Sort().Check(testkit.Rows("11", "12", "13"))
	tk.MustQuery("select * from t3 where not (a < -20)").Sort().Check(testkit.Rows("1", "11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t3 where not (a > 20)").Sort().Check(testkit.Rows("1", "11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t3 where not (a = 1)").Sort().Check(testkit.Rows("11", "12", "13", "2", "3"))
	tk.MustQuery("select * from t3 where not (a != 1)").Check(testkit.Rows("1"))
}
