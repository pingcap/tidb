// Copyright 2020 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestPointGetwithRangeAndListPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, unique index idx_a(a), index idx_b(b)) partition by list(a)(
		partition p0 values in (NULL, 1, 2, 3, 4),
			partition p1 values in (5, 6, 7, 8),
			partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange1(a int, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)

	// range partition table + unsigned int
	tk.MustExec(`create table trange2(a int unsigned, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)

	// insert data into list partition table
	tk.MustExec("insert into tlist values(1,1), (2,2), (3, 3), (4, 4), (5,5), (6, 6), (7,7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (NULL, NULL);")

	vals := make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into trange1 values " + strings.Join(vals, ","))
	tk.MustExec("insert into trange2 values " + strings.Join(vals, ","))

	// test PointGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a = {x}; // x >= 1 and x <= 100 Check if PointGet is used
		// select a from t where a={x}; // the result is {x}
		x := rand.Intn(100) + 1
		queryRange1 := fmt.Sprintf("select a from trange1 where a=%v", x)
		tk.MustHavePlan(queryRange1, "Point_Get") // check if PointGet is used
		tk.MustQuery(queryRange1).Check(testkit.Rows(fmt.Sprintf("%v", x)))

		queryRange2 := fmt.Sprintf("select a from trange1 where a=%v", x)
		tk.MustHavePlan(queryRange2, "Point_Get") // check if PointGet is used
		tk.MustQuery(queryRange2).Check(testkit.Rows(fmt.Sprintf("%v", x)))

		y := rand.Intn(12) + 1
		queryList := fmt.Sprintf("select a from tlist where a=%v", y)
		tk.MustHavePlan(queryList, "Point_Get") // check if PointGet is used
		tk.MustQuery(queryList).Check(testkit.Rows(fmt.Sprintf("%v", y)))
	}

	// test table dual
	queryRange1 := "select a from trange1 where a=200"
	tk.MustQuery("EXPLAIN FORMAT='brief' " + queryRange1).Check(testkit.Rows("Point_Get 1.00 root table:trange1, partition:dual, index:a(a) "))
	tk.MustQuery(queryRange1).Check(testkit.Rows())

	queryRange2 := "select a from trange2 where a=200"
	tk.MustQuery("EXPLAIN FORMAT='brief' " + queryRange2).Check(testkit.Rows("Point_Get 1.00 root table:trange2, partition:dual, index:a(a) "))
	tk.MustQuery(queryRange2).Check(testkit.Rows())

	queryList := "select a from tlist where a=200"
	tk.MustQuery("EXPLAIN FORMAT='brief' " + queryList).Check(testkit.Rows("Point_Get 1.00 root table:tlist, partition:dual, index:idx_a(a) "))
	tk.MustQuery(queryList).Check(testkit.Rows())

	// test PointGet for one partition
	queryOnePartition := "select a from t where a = -1"
	tk.MustExec("create table t(a int primary key, b int) PARTITION BY RANGE (a) (partition p0 values less than(1))")
	tk.MustExec("insert into t values (-1, 1), (-2, 1)")
	tk.MustExec("analyze table t")
	tk.MustHavePlan(queryOnePartition, "Point_Get")
	tk.MustQuery(queryOnePartition).Check(testkit.Rows(fmt.Sprintf("%v", -1)))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int primary key, b int) PARTITION BY list (a) (partition p0 values in (-1, -2))")
	tk.MustExec("insert into t values (-1, 1), (-2, 1)")
	tk.MustExec("analyze table t")
	tk.MustHavePlan(queryOnePartition, "Point_Get")
	tk.MustQuery(queryOnePartition).Check(testkit.Rows(fmt.Sprintf("%v", -1)))
}

func TestPartitionInfoDisable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_info_null")
	tk.MustExec(`CREATE TABLE t_info_null (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  date date NOT NULL,
  media varchar(32) NOT NULL DEFAULT '0',
  app varchar(32) NOT NULL DEFAULT '',
  xxx bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (id, date),
  UNIQUE KEY idx_media_id (media, date, app)
) PARTITION BY RANGE COLUMNS(date) (
  PARTITION p201912 VALUES LESS THAN ("2020-01-01"),
  PARTITION p202001 VALUES LESS THAN ("2020-02-01"),
  PARTITION p202002 VALUES LESS THAN ("2020-03-01"),
  PARTITION p202003 VALUES LESS THAN ("2020-04-01"),
  PARTITION p202004 VALUES LESS THAN ("2020-05-01"),
  PARTITION p202005 VALUES LESS THAN ("2020-06-01"),
  PARTITION p202006 VALUES LESS THAN ("2020-07-01"),
  PARTITION p202007 VALUES LESS THAN ("2020-08-01"),
  PARTITION p202008 VALUES LESS THAN ("2020-09-01"),
  PARTITION p202009 VALUES LESS THAN ("2020-10-01"),
  PARTITION p202010 VALUES LESS THAN ("2020-11-01"),
  PARTITION p202011 VALUES LESS THAN ("2020-12-01")
)`)
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t_info_null"))
	require.NoError(t, err)

	tbInfo := tbl.Meta()
	// Mock for a case that the tableInfo.Partition is not nil, but tableInfo.Partition.Enable is false.
	// That may happen when upgrading from a old version TiDB.
	tbInfo.Partition.Enable = false
	tbInfo.Partition.Num = 0

	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustQuery("explain select * from t_info_null where (date = '2020-10-02' or date = '2020-10-06') and app = 'xxx' and media = '19003006'").Check(testkit.Rows("Batch_Point_Get_5 2.00 root table:t_info_null, index:idx_media_id(media, date, app) keep order:false, desc:false"))
	tk.MustQuery("explain select * from t_info_null").Check(testkit.Rows("TableReader_5 10000.00 root  data:TableFullScan_4",
		"└─TableFullScan_4 10000.00 cop[tikv] table:t_info_null keep order:false, stats:pseudo"))
	// No panic.
	tk.MustQuery("select * from t_info_null where (date = '2020-10-02' or date = '2020-10-06') and app = 'xxx' and media = '19003006'").Check(testkit.Rows())
}

func TestOrderByAndLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_orderby_limit")
	tk.MustExec("use test_orderby_limit")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// range partition table
	tk.MustExec(`create table trange(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b)) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table
	tk.MustExec("create table thash(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b)) partition by hash(a) partitions 4;")

	// regular table
	tk.MustExec("create table tregular(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b))")

	// range partition table with int pk
	tk.MustExec(`create table trange_intpk(a int primary key, b int) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table with int pk
	tk.MustExec("create table thash_intpk(a int primary key, b int) partition by hash(a) partitions 4;")

	// regular table with int pk
	tk.MustExec("create table tregular_intpk(a int primary key, b int)")

	// range partition table with clustered index
	tk.MustExec(`create table trange_clustered(a int, b int, primary key(a, b) clustered) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table with clustered index
	tk.MustExec("create table thash_clustered(a int, b int, primary key(a, b) clustered) partition by hash(a) partitions 4;")

	// regular table with clustered index
	tk.MustExec("create table tregular_clustered(a int, b int, primary key(a, b) clustered)")

	listVals := make([]int, 0, 1000)

	for i := 0; i < 1000; i++ {
		listVals = append(listVals, i)
	}
	rand.Shuffle(len(listVals), func(i, j int) {
		listVals[i], listVals[j] = listVals[j], listVals[i]
	})

	var listVals1, listVals2, listVals3 string

	for i := 0; i <= 300; i++ {
		listVals1 += strconv.Itoa(listVals[i])
		if i != 300 {
			listVals1 += ","
		}
	}
	for i := 301; i <= 600; i++ {
		listVals2 += strconv.Itoa(listVals[i])
		if i != 600 {
			listVals2 += ","
		}
	}
	for i := 601; i <= 999; i++ {
		listVals3 += strconv.Itoa(listVals[i])
		if i != 999 {
			listVals3 += ","
		}
	}

	tk.MustExec(fmt.Sprintf(`create table tlist_intpk(a int primary key, b int) partition by list(a)(
		partition p1 values in (%s),
		partition p2 values in (%s),
		partition p3 values in (%s)
	)`, listVals1, listVals2, listVals3))
	tk.MustExec(fmt.Sprintf(`create table tlist(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b)) partition by list(a)(
		partition p1 values in (%s),
		partition p2 values in (%s),
		partition p3 values in (%s)
	)`, listVals1, listVals2, listVals3))
	tk.MustExec(fmt.Sprintf(`create table tlist_clustered(a int, b int, primary key(a, b)) partition by list(a)(
		partition p1 values in (%s),
		partition p2 values in (%s),
		partition p3 values in (%s)
	)`, listVals1, listVals2, listVals3))

	// generate some random data to be inserted
	vals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(550), rand.Intn(1000)))
	}

	dedupValsA := make([]string, 0, 1000)
	dedupMapA := make(map[int]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		valA := rand.Intn(550)
		if _, ok := dedupMapA[valA]; ok {
			continue
		}
		dedupValsA = append(dedupValsA, fmt.Sprintf("(%v, %v)", valA, rand.Intn(1000)))
		dedupMapA[valA] = struct{}{}
	}

	dedupValsAB := make([]string, 0, 1000)
	dedupMapAB := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		val := fmt.Sprintf("(%v, %v)", rand.Intn(550), rand.Intn(1000))
		if _, ok := dedupMapAB[val]; ok {
			continue
		}
		dedupValsAB = append(dedupValsAB, val)
		dedupMapAB[val] = struct{}{}
	}

	valInserted := strings.Join(vals, ",")
	valDedupAInserted := strings.Join(dedupValsA, ",")
	valDedupABInserted := strings.Join(dedupValsAB, ",")

	tk.MustExec("insert into trange values " + valInserted)
	tk.MustExec("insert into thash values " + valInserted)
	tk.MustExec("insert into tlist values" + valInserted)
	tk.MustExec("insert into tregular values " + valInserted)
	tk.MustExec("insert into trange_intpk values " + valDedupAInserted)
	tk.MustExec("insert into thash_intpk values " + valDedupAInserted)
	tk.MustExec("insert into tlist_intpk values " + valDedupAInserted)
	tk.MustExec("insert into tregular_intpk values " + valDedupAInserted)
	tk.MustExec("insert into trange_clustered values " + valDedupABInserted)
	tk.MustExec("insert into thash_clustered values " + valDedupABInserted)
	tk.MustExec("insert into tlist_clustered values " + valDedupABInserted)
	tk.MustExec("insert into tregular_clustered values " + valDedupABInserted)

	tk.MustExec("analyze table trange")
	tk.MustExec("analyze table trange_intpk")
	tk.MustExec("analyze table trange_clustered")
	tk.MustExec("analyze table thash")
	tk.MustExec("analyze table thash_intpk")
	tk.MustExec("analyze table thash_clustered")
	tk.MustExec("analyze table tregular")
	tk.MustExec("analyze table tregular_intpk")
	tk.MustExec("analyze table tregular_clustered")
	tk.MustExec("analyze table tlist")
	tk.MustExec("analyze table tlist_intpk")
	tk.MustExec("analyze table tlist_clustered")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test_orderby_limit"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if strings.HasPrefix(tblInfo.Name.L, "tr") || strings.HasPrefix(tblInfo.Name.L, "thash") || strings.HasPrefix(tblInfo.Name.L, "tlist") {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tikv\"")

	// test indexLookUp
	for i := 0; i < 50; i++ {
		// explain select * from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select * from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(549)
		y := rand.Intn(500) + 1
		queryPartition := fmt.Sprintf("select * from trange use index(idx_a) where a > %v order by a, b limit %v;", x, y)
		queryRegular := fmt.Sprintf("select * from tregular use index(idx_a) where a > %v order by a, b limit %v;", x, y)
		tk.MustHavePlan(queryPartition, "IndexLookUp") // check if IndexLookUp is used
		tk.MustQuery(queryPartition).Check(tk.MustQuery(queryRegular).Rows())
	}

	// test indexLookUp with order property pushed down.
	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			tk.MustExec("set tidb_partition_prune_mode = `static-only`")
		} else {
			tk.MustExec("set tidb_partition_prune_mode = `dynamic-only`")
		}
		// explain select * from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select * from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(549)
		y := rand.Intn(1000) + 1
		// Since we only use order by a not order by a, b, the result is not stable when we read both a and b.
		// We cut the max element so that the result can be stable.
		maxEle := tk.MustQuery(fmt.Sprintf("select ifnull(max(a), 1100) from (select * from tregular use index(idx_a) where a > %v order by a limit %v) t", x, y)).Rows()[0][0]
		queryRangePartitionWithLimitHint := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from trange use index(idx_a) where a > %v and a < %v order by a limit %v", x, maxEle, y)
		queryHashPartitionWithLimitHint := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from thash use index(idx_a) where a > %v and a < %v order by a limit %v", x, maxEle, y)
		queryListPartitionWithLimitHint := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from tlist use index(idx_a) where a > %v and a < %v order by a limit %v", x, maxEle, y)
		queryRegular := fmt.Sprintf("select * from tregular use index(idx_a) where a > %v and a < %v order by a limit %v;", x, maxEle, y)

		regularResult := tk.MustQuery(queryRegular).Sort().Rows()
		if len(regularResult) > 0 {
			tk.MustHavePlan(queryRangePartitionWithLimitHint, "Limit")
			tk.MustHavePlan(queryRangePartitionWithLimitHint, "IndexLookUp")
			tk.MustHavePlan(queryHashPartitionWithLimitHint, "Limit")
			tk.MustHavePlan(queryHashPartitionWithLimitHint, "IndexLookUp")
			tk.MustHavePlan(queryListPartitionWithLimitHint, "Limit")
			tk.MustHavePlan(queryListPartitionWithLimitHint, "IndexLookUp")
		}
		if i%2 != 0 {
			tk.MustNotHavePlan(queryRangePartitionWithLimitHint, "TopN") // fully pushed
			tk.MustNotHavePlan(queryHashPartitionWithLimitHint, "TopN")
			tk.MustNotHavePlan(queryListPartitionWithLimitHint, "TopN")
		}
		tk.MustQuery(queryRangePartitionWithLimitHint).Sort().Check(regularResult)
		tk.MustQuery(queryHashPartitionWithLimitHint).Sort().Check(regularResult)
		tk.MustQuery(queryListPartitionWithLimitHint).Sort().Check(regularResult)
	}

	// test indexLookUp with order property pushed down.
	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			tk.MustExec("set tidb_partition_prune_mode = `static-only`")
		} else {
			tk.MustExec("set tidb_partition_prune_mode = `dynamic-only`")
		}
		// explain select * from t where b > {y}  use index(idx_b) order by b limit {x}; // check if IndexLookUp is used
		// select * from t where b > {y} use index(idx_b) order by b limit {x}; // it can return the correct result
		x := rand.Intn(549)
		y := rand.Intn(500) + 1
		maxEle := tk.MustQuery(fmt.Sprintf("select ifnull(max(b), 2000) from (select * from tregular use index(idx_b) where b > %v order by b limit %v) t", x, y)).Rows()[0][0]
		queryRangePartitionWithLimitHint := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from trange use index(idx_b) where b > %v and b < %v order by b limit %v", x, maxEle, y)
		queryHashPartitionWithLimitHint := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from thash use index(idx_b) where b > %v and b < %v order by b limit %v", x, maxEle, y)
		queryListPartitionWithLimitHint := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from tlist use index(idx_b) where b > %v and b < %v order by b limit %v", x, maxEle, y)
		queryRegular := fmt.Sprintf("select * from tregular use index(idx_b) where b > %v and b < %v order by b limit %v;", x, maxEle, y)

		regularResult := tk.MustQuery(queryRegular).Sort().Rows()
		if len(regularResult) > 0 {
			tk.MustHavePlan(queryRangePartitionWithLimitHint, "Limit")
			tk.MustHavePlan(queryRangePartitionWithLimitHint, "IndexLookUp")
			tk.MustHavePlan(queryHashPartitionWithLimitHint, "Limit")
			tk.MustHavePlan(queryHashPartitionWithLimitHint, "IndexLookUp")
			tk.MustHavePlan(queryListPartitionWithLimitHint, "Limit")
			tk.MustHavePlan(queryListPartitionWithLimitHint, "IndexLookUp")
		}
		if i%2 != 0 {
			tk.MustNotHavePlan(queryRangePartitionWithLimitHint, "TopN") // fully pushed
			tk.MustNotHavePlan(queryHashPartitionWithLimitHint, "TopN")
			tk.MustNotHavePlan(queryListPartitionWithLimitHint, "TopN")
		}
		tk.MustQuery(queryRangePartitionWithLimitHint).Sort().Check(regularResult)
		tk.MustQuery(queryHashPartitionWithLimitHint).Sort().Check(regularResult)
		tk.MustQuery(queryListPartitionWithLimitHint).Sort().Check(regularResult)
	}

	tk.MustExec("set tidb_partition_prune_mode = default")

	// test tableReader
	for i := 0; i < 50; i++ {
		// explain select * from t where a > {y}  ignore index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select * from t where a > {y} ignore index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(549)
		y := rand.Intn(500) + 1
		queryPartition := fmt.Sprintf("select * from trange ignore index(idx_a, idx_ab) where a > %v order by a, b limit %v;", x, y)
		queryRegular := fmt.Sprintf("select * from tregular ignore index(idx_a, idx_ab) where a > %v order by a, b limit %v;", x, y)
		tk.MustHavePlan(queryPartition, "TableReader") // check if tableReader is used
		tk.MustQuery(queryPartition).Check(tk.MustQuery(queryRegular).Rows())
	}

	// test tableReader with order property pushed down.
	for i := 0; i < 50; i++ {
		// explain select * from t where a > {y}  ignore index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select * from t where a > {y} ignore index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(549)
		y := rand.Intn(500) + 1
		queryRangePartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from trange ignore index(idx_a, idx_ab) where a > %v order by a, b limit %v;", x, y)
		queryHashPartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from thash ignore index(idx_a, idx_ab) where a > %v order by a, b limit %v;", x, y)
		queryListPartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from tlist ignore index(idx_a, idx_ab) where a > %v order by a, b limit %v;", x, y)
		queryRegular := fmt.Sprintf("select * from tregular ignore index(idx_a) where a > %v order by a, b limit %v;", x, y)
		tk.MustHavePlan(queryRangePartition, "TableReader") // check if tableReader is used
		tk.MustHavePlan(queryHashPartition, "TableReader")
		tk.MustHavePlan(queryListPartition, "TableReader")
		tk.MustNotHavePlan(queryRangePartition, "Limit") // check if order property is not pushed
		tk.MustNotHavePlan(queryHashPartition, "Limit")
		tk.MustNotHavePlan(queryListPartition, "Limit")
		regularResult := tk.MustQuery(queryRegular).Rows()
		tk.MustQuery(queryRangePartition).Check(regularResult)
		tk.MustQuery(queryHashPartition).Check(regularResult)
		tk.MustQuery(queryListPartition).Check(regularResult)

		// test int pk
		// To be simplified, we only read column a.
		queryRangePartition = fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from trange_intpk use index(primary) where a > %v order by a limit %v", x, y)
		queryHashPartition = fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from thash_intpk use index(primary) where a > %v order by a limit %v", x, y)
		queryListPartition = fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from tlist_intpk use index(primary) where a > %v order by a limit %v", x, y)
		queryRegular = fmt.Sprintf("select a from tregular_intpk where a > %v order by a limit %v", x, y)
		tk.MustHavePlan(queryRangePartition, "TableReader")
		tk.MustHavePlan(queryHashPartition, "TableReader")
		tk.MustHavePlan(queryListPartition, "TableReader")
		tk.MustHavePlan(queryRangePartition, "Limit")   // check if order property is pushed
		tk.MustNotHavePlan(queryRangePartition, "TopN") // and is fully pushed
		tk.MustHavePlan(queryHashPartition, "Limit")
		tk.MustNotHavePlan(queryHashPartition, "TopN")
		tk.MustHavePlan(queryListPartition, "Limit")
		tk.MustNotHavePlan(queryListPartition, "TopN")
		regularResult = tk.MustQuery(queryRegular).Rows()
		tk.MustQuery(queryRangePartition).Check(regularResult)
		tk.MustQuery(queryHashPartition).Check(regularResult)
		tk.MustQuery(queryListPartition).Check(regularResult)

		// test clustered index
		queryRangePartition = fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from trange_clustered use index(primary) where a > %v order by a, b limit %v;", x, y)
		queryHashPartition = fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from thash_clustered use index(primary) where a > %v order by a, b limit %v;", x, y)
		queryListPartition = fmt.Sprintf("select /*+ LIMIT_TO_COP() */ * from tlist_clustered use index(primary) where a > %v order by a, b limit %v;", x, y)
		queryRegular = fmt.Sprintf("select * from tregular_clustered where a > %v order by a, b limit %v;", x, y)
		tk.MustHavePlan(queryRangePartition, "TableReader") // check if tableReader is used
		tk.MustHavePlan(queryHashPartition, "TableReader")
		tk.MustHavePlan(queryListPartition, "TableReader")
		tk.MustHavePlan(queryRangePartition, "Limit") // check if order property is pushed
		tk.MustHavePlan(queryHashPartition, "Limit")
		tk.MustHavePlan(queryListPartition, "Limit")
		tk.MustNotHavePlan(queryRangePartition, "TopN") // could fully pushed for TableScan executor
		tk.MustNotHavePlan(queryHashPartition, "TopN")
		tk.MustNotHavePlan(queryListPartition, "TopN")
		regularResult = tk.MustQuery(queryRegular).Rows()
		tk.MustQuery(queryRangePartition).Check(regularResult)
		tk.MustQuery(queryHashPartition).Check(regularResult)
		tk.MustQuery(queryListPartition).Check(regularResult)

		tk.MustExec(" set @@tidb_allow_mpp=1;")
		tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash,tikv\"")
		queryPartitionWithTiFlash := fmt.Sprintf("select /*+ read_from_storage(tiflash[trange_intpk]) */ * from trange_intpk where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash), fmt.Sprintf("%v", tk.MustQuery("explain "+queryPartitionWithTiFlash).Rows()))
		// but order is not pushed
		tk.MustNotHavePlan(queryPartitionWithTiFlash, "Limit")
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[trange_intpk]) */ /*+ LIMIT_TO_COP() */ * from trange_intpk where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash), fmt.Sprintf("%v", tk.MustQuery("explain "+queryPartitionWithTiFlash).Rows()))
		// but order is not pushed
		tk.MustNotHavePlan(queryPartitionWithTiFlash, "Limit")
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[trange_clustered]) */ * from trange_clustered where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash), fmt.Sprintf("%v", tk.MustQuery("explain "+queryPartitionWithTiFlash).Rows()))
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[trange_clustered]) */ /*+ LIMIT_TO_COP() */ * from trange_clustered where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash))
		// but order is not pushed
		tk.MustNotHavePlan(queryPartitionWithTiFlash, "Limit")
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[thash_intpk]) */ * from thash_intpk where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash), fmt.Sprintf("%v", tk.MustQuery("explain "+queryPartitionWithTiFlash).Rows()))
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[thash_intpk]) */ /*+ LIMIT_TO_COP() */ * from thash_intpk where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash))
		// but order is not pushed
		tk.MustNotHavePlan(queryPartitionWithTiFlash, "Limit")
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[thash_clustered]) */ * from thash_clustered where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash), fmt.Sprintf("%v", tk.MustQuery("explain "+queryPartitionWithTiFlash).Rows()))
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[thash_clustered]) */ /*+ LIMIT_TO_COP() */ * from thash_clustered where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash))
		// but order is not pushed
		tk.MustNotHavePlan(queryPartitionWithTiFlash, "Limit")
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[tlist_intpk]) */ * from tlist_intpk where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash), fmt.Sprintf("%v", tk.MustQuery("explain "+queryPartitionWithTiFlash).Rows()))
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[tlist_intpk]) */ /*+ LIMIT_TO_COP() */ * from tlist_intpk where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash))
		// but order is not pushed
		tk.MustNotHavePlan(queryPartitionWithTiFlash, "Limit")
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[tlist_clustered]) */ * from tlist_clustered where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash), fmt.Sprintf("%v", tk.MustQuery("explain "+queryPartitionWithTiFlash).Rows()))
		queryPartitionWithTiFlash = fmt.Sprintf("select /*+ read_from_storage(tiflash[tlist_clustered]) */ /*+ LIMIT_TO_COP() */ * from tlist_clustered where a > %v order by a limit %v", x, y)
		// check if tiflash is used
		require.True(t, tk.HasTiFlashPlan(queryPartitionWithTiFlash))
		// but order is not pushed
		tk.MustNotHavePlan(queryPartitionWithTiFlash, "Limit")
		tk.MustExec(" set @@tidb_allow_mpp=0;")
		tk.MustExec("set @@session.tidb_isolation_read_engines=\"tikv\"")
	}

	// test indexReader
	for i := 0; i < 50; i++ {
		// explain select a from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select a from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(549)
		y := rand.Intn(500) + 1
		queryPartition := fmt.Sprintf("select a from trange use index(idx_a) where a > %v order by a limit %v;", x, y)
		queryRegular := fmt.Sprintf("select a from tregular use index(idx_a) where a > %v order by a limit %v;", x, y)
		tk.MustHavePlan(queryPartition, "IndexReader") // check if indexReader is used
		tk.MustQuery(queryPartition).Check(tk.MustQuery(queryRegular).Rows())
	}

	// test indexReader with order property pushed down.
	for i := 0; i < 50; i++ {
		// explain select a from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select a from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(549)
		y := rand.Intn(500) + 1
		queryRangePartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from trange use index(idx_a) where a > %v order by a limit %v;", x, y)
		queryHashPartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from thash use index(idx_a) where a > %v order by a limit %v;", x, y)
		queryRegular := fmt.Sprintf("select a from tregular use index(idx_a) where a > %v order by a limit %v;", x, y)
		tk.MustHavePlan(queryRangePartition, "IndexReader") // check if indexReader is used
		tk.MustHavePlan(queryHashPartition, "IndexReader")
		tk.MustHavePlan(queryRangePartition, "Limit") // check if order property is pushed
		tk.MustHavePlan(queryHashPartition, "Limit")
		tk.MustNotHavePlan(queryRangePartition, "TopN") // fully pushed limit
		tk.MustNotHavePlan(queryHashPartition, "TopN")
		regularResult := tk.MustQuery(queryRegular).Rows()
		tk.MustQuery(queryRangePartition).Check(regularResult)
		tk.MustQuery(queryHashPartition).Check(regularResult)
	}

	// test indexReader use idx_ab(a, b) with a = {x} order by b limit {y}
	for i := 0; i < 50; i++ {
		x := rand.Intn(549)
		y := rand.Intn(500) + 1
		queryRangePartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from trange use index(idx_ab) where a = %v order by b limit %v;", x, y)
		queryHashPartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from thash use index(idx_ab) where a = %v order by b limit %v;", x, y)
		queryListPartition := fmt.Sprintf("select /*+ LIMIT_TO_COP() */ a from tlist use index(idx_ab) where a = %v order by b limit %v;", x, y)
		queryRegular := fmt.Sprintf("select a from tregular use index(idx_ab) where a = %v order by b limit %v;", x, y)
		tk.MustHavePlan(queryRangePartition, "IndexReader") // check if indexReader is used
		tk.MustHavePlan(queryHashPartition, "IndexReader")
		tk.MustHavePlan(queryListPartition, "IndexReader")
		tk.MustHavePlan(queryRangePartition, "Limit") // check if order property is pushed
		tk.MustHavePlan(queryHashPartition, "Limit")
		tk.MustHavePlan(queryListPartition, "Limit")
		tk.MustNotHavePlan(queryRangePartition, "TopN") // fully pushed limit
		tk.MustNotHavePlan(queryHashPartition, "TopN")
		tk.MustNotHavePlan(queryListPartition, "TopN")
		regularResult := tk.MustQuery(queryRegular).Rows()
		tk.MustQuery(queryRangePartition).Check(regularResult)
		tk.MustQuery(queryHashPartition).Check(regularResult)
		tk.MustQuery(queryListPartition).Check(regularResult)
	}

	// test indexMerge
	for i := 0; i < 50; i++ {
		// explain select /*+ use_index_merge(t) */ * from t where a > 2 or b < 5 order by a, b limit {x}; // check if IndexMerge is used
		// select /*+ use_index_merge(t) */ * from t where a > 2 or b < 5 order by a, b limit {x};  // can return the correct value
		y := rand.Intn(500) + 1
		queryHashPartition := fmt.Sprintf("select /*+ use_index_merge(thash) */ * from thash where a > 2 or b < 5 order by a, b limit %v;", y)
		queryRegular := fmt.Sprintf("select * from tregular where a > 2 or b < 5 order by a, b limit %v;", y)
		tk.MustHavePlan(queryHashPartition, "IndexMerge") // check if indexMerge is used
		tk.MustQuery(queryHashPartition).Check(tk.MustQuery(queryRegular).Rows())
	}

	// test sql killed when memory exceed `tidb_mem_quota_query`
	originMemQuota := tk.MustQuery("show variables like 'tidb_mem_quota_query'").Rows()[0][1].(string)
	originOOMAction := tk.MustQuery("show variables like 'tidb_mem_oom_action'").Rows()[0][1].(string)
	tk.MustExec("set session tidb_mem_quota_query=128")
	tk.MustExec("set global tidb_mem_oom_action=CANCEL")
	err := tk.QueryToErr("select /*+ LIMIT_TO_COP() */ a from trange use index(idx_a) where a > 1 order by a limit 2000")
	require.Error(t, err)
	require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
	tk.MustExec(fmt.Sprintf("set session tidb_mem_quota_query=%s", originMemQuota))
	tk.MustExec(fmt.Sprintf("set global tidb_mem_oom_action=%s", originOOMAction))
}

func TestBatchGetandPointGetwithHashPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_batchget_pointget")
	tk.MustExec("use test_batchget_pointget")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash partition table
	tk.MustExec("create table thash(a int, unique key(a)) partition by hash(a) partitions 4;")

	// regular partition table
	tk.MustExec("create table tregular(a int, unique key(a));")

	vals := make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular values " + strings.Join(vals, ","))

	// test PointGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a = {x}; // x >= 1 and x <= 100 Check if PointGet is used
		// select a from t where a={x}; // the result is {x}
		x := rand.Intn(100) + 1
		queryHash := fmt.Sprintf("select a from thash where a=%v", x)
		queryRegular := fmt.Sprintf("select a from tregular where a=%v", x)
		tk.MustHavePlan(queryHash, "Point_Get") // check if PointGet is used
		tk.MustQuery(queryHash).Check(tk.MustQuery(queryRegular).Rows())
	}

	// test empty PointGet
	queryHash := "select a from thash where a=200"
	tk.MustHavePlan(queryHash, "Point_Get") // check if PointGet is used
	tk.MustQuery(queryHash).Check(testkit.Rows())

	// test BatchGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
		// select a from t where where a in ({x1}, {x2}, ... {x10});
		points := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			x := rand.Intn(100) + 1
			points = append(points, fmt.Sprintf("%v", x))
		}

		queryHash := fmt.Sprintf("select a from thash where a in (%v)", strings.Join(points, ","))
		queryRegular := fmt.Sprintf("select a from tregular where a in (%v)", strings.Join(points, ","))
		tk.MustHavePlan(queryHash, "Point_Get") // check if PointGet is used
		tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	}
}

func TestView(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_view")
	tk.MustExec("use test_view")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table thash (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a varchar(10), b varchar(10), key(a)) partition by range columns(a) (
						partition p0 values less than ('300'),
						partition p1 values less than ('600'),
						partition p2 values less than ('900'),
						partition p3 values less than ('9999'))`)
	tk.MustExec(`create table t1 (a int, b int, key(a))`)
	tk.MustExec(`create table t2 (a varchar(10), b varchar(10), key(a))`)

	// insert the same data into thash and t1
	vals := make([]string, 0, 3000)
	for i := 0; i < 3000; i++ {
		vals = append(vals, fmt.Sprintf(`(%v, %v)`, rand.Intn(10000), rand.Intn(10000)))
	}
	tk.MustExec(fmt.Sprintf(`insert into thash values %v`, strings.Join(vals, ", ")))
	tk.MustExec(fmt.Sprintf(`insert into t1 values %v`, strings.Join(vals, ", ")))

	// insert the same data into trange and t2
	vals = vals[:0]
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf(`("%v", "%v")`, rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec(fmt.Sprintf(`insert into trange values %v`, strings.Join(vals, ", ")))
	tk.MustExec(fmt.Sprintf(`insert into t2 values %v`, strings.Join(vals, ", ")))

	// test views on a single table
	tk.MustExec(`create definer='root'@'localhost' view vhash as select a*2 as a, a+b as b from thash`)
	tk.MustExec(`create definer='root'@'localhost' view v1 as select a*2 as a, a+b as b from t1`)
	tk.MustExec(`create definer='root'@'localhost' view vrange as select concat(a, b) as a, a+b as b from trange`)
	tk.MustExec(`create definer='root'@'localhost' view v2 as select concat(a, b) as a, a+b as b from t2`)
	for i := 0; i < 100; i++ {
		xhash := rand.Intn(10000)
		tk.MustQuery(fmt.Sprintf(`select * from vhash where a>=%v`, xhash)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v1 where a>=%v`, xhash)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vhash where b>=%v`, xhash)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v1 where b>=%v`, xhash)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vhash where a>=%v and b>=%v`, xhash, xhash)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v1 where a>=%v and b>=%v`, xhash, xhash)).Sort().Rows())

		xrange := fmt.Sprintf(`"%v"`, rand.Intn(1000))
		tk.MustQuery(fmt.Sprintf(`select * from vrange where a>=%v`, xrange)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v2 where a>=%v`, xrange)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vrange where b>=%v`, xrange)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v2 where b>=%v`, xrange)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vrange where a>=%v and b<=%v`, xrange, xrange)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v2 where a>=%v and b<=%v`, xrange, xrange)).Sort().Rows())
	}

	// test views on both tables
	tk.MustExec(`create definer='root'@'localhost' view vboth as select thash.a+trange.a as a, thash.b+trange.b as b from thash, trange where thash.a=trange.a`)
	tk.MustExec(`create definer='root'@'localhost' view vt as select t1.a+t2.a as a, t1.b+t2.b as b from t1, t2 where t1.a=t2.a`)
	for i := 0; i < 100; i++ {
		x := rand.Intn(10000)
		tk.MustQuery(fmt.Sprintf(`select * from vboth where a>=%v`, x)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from vt where a>=%v`, x)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vboth where b>=%v`, x)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from vt where b>=%v`, x)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vboth where a>=%v and b>=%v`, x, x)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from vt where a>=%v and b>=%v`, x, x)).Sort().Rows())
	}
}

func TestDirectReadingwithIndexJoin(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_dr_join")
	tk.MustExec("use test_dr_join")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash and range partition
	tk.MustExec("create table thash (a int, b int, c int, primary key(a), index idx_b(b)) partition by hash(a) partitions 4;")
	tk.MustExec(`create table trange (a int, b int, c int, primary key(a), index idx_b(b)) partition by range(a) (
		 partition p0 values less than(1000),
		 partition p1 values less than(2000),
		 partition p2 values less than(3000),
		 partition p3 values less than(4000));`)

	// regualr table
	tk.MustExec(`create table tnormal (a int, b int, c int, primary key(a), index idx_b(b));`)
	tk.MustExec(`create table touter (a int, b int, c int);`)

	// generate some random data to be inserted
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", rand.Intn(4000), rand.Intn(4000), rand.Intn(4000)))
	}
	tk.MustExec("insert ignore into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into tnormal values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into touter values " + strings.Join(vals, ","))

	// test indexLookUp + hash
	queryPartition := "select /*+ INL_JOIN(touter, thash) */ * from touter join thash use index(idx_b) on touter.b = thash.b"
	queryRegular := "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal use index(idx_b) on touter.b = tnormal.b"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexLookUp, outer key:test_dr_join.touter.b, inner key:test_dr_join.thash.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.thash.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) 12487.50 root partition:all ",
		"  ├─Selection(Build) 12487.50 cop[tikv]  not(isnull(test_dr_join.thash.b))",
		"  │ └─IndexRangeScan 12500.00 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(test_dr_join.thash.b, test_dr_join.touter.b)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 12487.50 cop[tikv] table:thash keep order:false, stats:pseudo")) // check if IndexLookUp is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test tableReader + hash
	queryPartition = "select /*+ INL_JOIN(touter, thash) */ * from touter join thash on touter.a = thash.a"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal on touter.a = tnormal.a"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:TableReader, outer key:test_dr_join.touter.a, inner key:test_dr_join.thash.a, equal cond:eq(test_dr_join.touter.a, test_dr_join.thash.a)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.a))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─TableReader(Probe) 9990.00 root partition:all data:TableRangeScan",
		"  └─TableRangeScan 9990.00 cop[tikv] table:thash range: decided by [test_dr_join.touter.a], keep order:false, stats:pseudo")) // check if tableReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test indexReader + hash
	queryPartition = "select /*+ INL_JOIN(touter, thash) */ thash.b from touter join thash use index(idx_b) on touter.b = thash.b;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ tnormal.b from touter join tnormal use index(idx_b) on touter.b = tnormal.b;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexReader, outer key:test_dr_join.touter.b, inner key:test_dr_join.thash.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.thash.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexReader(Probe) 12487.50 root partition:all index:Selection",
		"  └─Selection 12487.50 cop[tikv]  not(isnull(test_dr_join.thash.b))",
		"    └─IndexRangeScan 12500.00 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(test_dr_join.thash.b, test_dr_join.touter.b)], keep order:false, stats:pseudo")) // check if indexReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test indexLookUp + range
	// explain select /*+ INL_JOIN(touter, tinner) */ * from touter join tinner use index(a) on touter.a = tinner.a;
	queryPartition = "select /*+ INL_JOIN(touter, trange) */ * from touter join trange use index(idx_b) on touter.b = trange.b;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal use index(idx_b) on touter.b = tnormal.b;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexLookUp, outer key:test_dr_join.touter.b, inner key:test_dr_join.trange.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.trange.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) 12487.50 root partition:all ",
		"  ├─Selection(Build) 12487.50 cop[tikv]  not(isnull(test_dr_join.trange.b))",
		"  │ └─IndexRangeScan 12500.00 cop[tikv] table:trange, index:idx_b(b) range: decided by [eq(test_dr_join.trange.b, test_dr_join.touter.b)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 12487.50 cop[tikv] table:trange keep order:false, stats:pseudo")) // check if IndexLookUp is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test tableReader + range
	queryPartition = "select /*+ INL_JOIN(touter, trange) */ * from touter join trange on touter.a = trange.a;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal on touter.a = tnormal.a;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:TableReader, outer key:test_dr_join.touter.a, inner key:test_dr_join.trange.a, equal cond:eq(test_dr_join.touter.a, test_dr_join.trange.a)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.a))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─TableReader(Probe) 9990.00 root partition:all data:TableRangeScan",
		"  └─TableRangeScan 9990.00 cop[tikv] table:trange range: decided by [test_dr_join.touter.a], keep order:false, stats:pseudo")) // check if tableReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test indexReader + range
	// explain select /*+ INL_JOIN(touter, tinner) */ tinner.a from touter join tinner on touter.a = tinner.a;
	queryPartition = "select /*+ INL_JOIN(touter, trange) */ trange.b from touter join trange use index(idx_b) on touter.b = trange.b;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ tnormal.b from touter join tnormal use index(idx_b) on touter.b = tnormal.b;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexReader, outer key:test_dr_join.touter.b, inner key:test_dr_join.trange.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.trange.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexReader(Probe) 12487.50 root partition:all index:Selection",
		"  └─Selection 12487.50 cop[tikv]  not(isnull(test_dr_join.trange.b))",
		"    └─IndexRangeScan 12500.00 cop[tikv] table:trange, index:idx_b(b) range: decided by [eq(test_dr_join.trange.b, test_dr_join.touter.b)], keep order:false, stats:pseudo")) // check if indexReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
}

func TestDynamicPruningUnderIndexJoin(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create database pruing_under_index_join")
	tk.MustExec("use pruing_under_index_join")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table tnormal (a int, b int, c int, primary key(a), index idx_b(b))`)
	tk.MustExec(`create table thash (a int, b int, c int, primary key(a), index idx_b(b)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table touter (a int, b int, c int)`)

	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", i, rand.Intn(10000), rand.Intn(10000)))
	}
	tk.MustExec(`insert into tnormal values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into touter values ` + strings.Join(vals, ", "))

	// case 1: IndexReader in the inner side
	tk.MustQuery(`explain format='brief' select /*+ INL_JOIN(touter, thash) */ thash.b from touter join thash use index(idx_b) on touter.b = thash.b`).Check(testkit.Rows(
		`IndexJoin 12487.50 root  inner join, inner:IndexReader, outer key:pruing_under_index_join.touter.b, inner key:pruing_under_index_join.thash.b, equal cond:eq(pruing_under_index_join.touter.b, pruing_under_index_join.thash.b)`,
		`├─TableReader(Build) 9990.00 root  data:Selection`,
		`│ └─Selection 9990.00 cop[tikv]  not(isnull(pruing_under_index_join.touter.b))`,
		`│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`└─IndexReader(Probe) 12487.50 root partition:all index:Selection`,
		`  └─Selection 12487.50 cop[tikv]  not(isnull(pruing_under_index_join.thash.b))`,
		`    └─IndexRangeScan 12500.00 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(pruing_under_index_join.thash.b, pruing_under_index_join.touter.b)], keep order:false, stats:pseudo`))
	tk.MustQuery(`select /*+ INL_JOIN(touter, thash) */ thash.b from touter join thash use index(idx_b) on touter.b = thash.b`).Sort().Check(
		tk.MustQuery(`select /*+ INL_JOIN(touter, tnormal) */ tnormal.b from touter join tnormal use index(idx_b) on touter.b = tnormal.b`).Sort().Rows())

	// case 2: TableReader in the inner side
	tk.MustQuery(`explain format='brief' select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(primary) on touter.b = thash.a`).Check(testkit.Rows(
		`IndexJoin 12487.50 root  inner join, inner:TableReader, outer key:pruing_under_index_join.touter.b, inner key:pruing_under_index_join.thash.a, equal cond:eq(pruing_under_index_join.touter.b, pruing_under_index_join.thash.a)`,
		`├─TableReader(Build) 9990.00 root  data:Selection`,
		`│ └─Selection 9990.00 cop[tikv]  not(isnull(pruing_under_index_join.touter.b))`,
		`│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`└─TableReader(Probe) 9990.00 root partition:all data:TableRangeScan`,
		`  └─TableRangeScan 9990.00 cop[tikv] table:thash range: decided by [pruing_under_index_join.touter.b], keep order:false, stats:pseudo`))
	tk.MustQuery(`select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(primary) on touter.b = thash.a`).Sort().Check(
		tk.MustQuery(`select /*+ INL_JOIN(touter, tnormal) */ tnormal.* from touter join tnormal use index(primary) on touter.b = tnormal.a`).Sort().Rows())

	// case 3: IndexLookUp in the inner side + read all inner columns
	tk.MustQuery(`explain format='brief' select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(idx_b) on touter.b = thash.b`).Check(testkit.Rows(
		`IndexJoin 12487.50 root  inner join, inner:IndexLookUp, outer key:pruing_under_index_join.touter.b, inner key:pruing_under_index_join.thash.b, equal cond:eq(pruing_under_index_join.touter.b, pruing_under_index_join.thash.b)`,
		`├─TableReader(Build) 9990.00 root  data:Selection`,
		`│ └─Selection 9990.00 cop[tikv]  not(isnull(pruing_under_index_join.touter.b))`,
		`│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`└─IndexLookUp(Probe) 12487.50 root partition:all `,
		`  ├─Selection(Build) 12487.50 cop[tikv]  not(isnull(pruing_under_index_join.thash.b))`,
		`  │ └─IndexRangeScan 12500.00 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(pruing_under_index_join.thash.b, pruing_under_index_join.touter.b)], keep order:false, stats:pseudo`,
		`  └─TableRowIDScan(Probe) 12487.50 cop[tikv] table:thash keep order:false, stats:pseudo`))
	tk.MustQuery(`select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(idx_b) on touter.b = thash.b`).Sort().Check(
		tk.MustQuery(`select /*+ INL_JOIN(touter, tnormal) */ tnormal.* from touter join tnormal use index(idx_b) on touter.b = tnormal.b`).Sort().Rows())
}

func TestBatchGetforRangeandListPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_pointget")
	tk.MustExec("use test_pointget")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, unique index idx_a(a), index idx_b(b)) partition by list(a)(
		partition p0 values in (1, 2, 3, 4),
			partition p1 values in (5, 6, 7, 8),
			partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange(a int, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)

	// hash partition table
	tk.MustExec("create table thash(a int unsigned, unique key(a)) partition by hash(a) partitions 4;")

	// insert data into list partition table
	tk.MustExec("insert into tlist values(1,1), (2,2), (3, 3), (4, 4), (5,5), (6, 6), (7,7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12);")
	// regular partition table
	tk.MustExec("create table tregular1(a int, unique key(a));")
	tk.MustExec("create table tregular2(a int, unique key(a));")

	vals := make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular1 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular2 values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)")

	// test BatchGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
		// select a from t where where a in ({x1}, {x2}, ... {x10});
		points := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			x := rand.Intn(100) + 1
			points = append(points, fmt.Sprintf("%v", x))
		}
		queryRegular1 := fmt.Sprintf("select a from tregular1 where a in (%v)", strings.Join(points, ","))

		queryHash := fmt.Sprintf("select a from thash where a in (%v)", strings.Join(points, ","))
		tk.MustHavePlan(queryHash, "Batch_Point_Get") // check if BatchGet is used
		tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryRange := fmt.Sprintf("select a from trange where a in (%v)", strings.Join(points, ","))
		tk.MustHavePlan(queryRange, "Batch_Point_Get") // check if BatchGet is used
		tk.MustQuery(queryRange).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		points = make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			x := rand.Intn(12) + 1
			points = append(points, fmt.Sprintf("%v", x))
		}
		queryRegular2 := fmt.Sprintf("select a from tregular2 where a in (%v)", strings.Join(points, ","))
		queryList := fmt.Sprintf("select a from tlist where a in (%v)", strings.Join(points, ","))
		tk.MustHavePlan(queryList, "Batch_Point_Get") // check if BatchGet is used
		tk.MustQuery(queryList).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}

	// test different data type
	// unsigned flag
	// partition table and reguar table pair
	tk.MustExec(`create table trange3(a int unsigned, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)
	tk.MustExec("create table tregular3(a int unsigned, unique key(a));")
	vals = make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into trange3 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular3 values " + strings.Join(vals, ","))
	// test BatchGet
	// explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
	// select a from t where where a in ({x1}, {x2}, ... {x10});
	points := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		x := rand.Intn(100) + 1
		points = append(points, fmt.Sprintf("%v", x))
	}
	queryRegular := fmt.Sprintf("select a from tregular3 where a in (%v)", strings.Join(points, ","))
	queryRange := fmt.Sprintf("select a from trange3 where a in (%v)", strings.Join(points, ","))
	tk.MustHavePlan(queryRange, "Batch_Point_Get") // check if BatchGet is used
	tk.MustQuery(queryRange).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
}

func TestPartitionTableWithDifferentJoin(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_partition_joins")
	tk.MustExec("use test_partition_joins")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash and range partition
	tk.MustExec("create table thash(a int, b int, key(a)) partition by hash(a) partitions 4")
	tk.MustExec("create table tregular1(a int, b int, key(a))")

	tk.MustExec(`create table trange(a int, b int, key(a)) partition by range(a) (
		partition p0 values less than (200),
		partition p1 values less than (400),
		partition p2 values less than (600),
		partition p3 values less than (800),
		partition p4 values less than (1001))`)
	tk.MustExec("create table tregular2(a int, b int, key(a))")

	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular1 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular2 values " + strings.Join(vals, ","))

	// random params
	x1 := rand.Intn(1000)
	x2 := rand.Intn(1000)
	x3 := rand.Intn(1000)
	x4 := rand.Intn(1000)

	// group 1
	// hash_join range partition and hash partition
	queryHash := fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.b=thash.b and thash.a = %v and trange.a > %v;", x1, x2)
	queryRegular := fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.b=tregular1.b and tregular1.a = %v and tregular2.a > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "HashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a > %v;", x1)
	tk.MustHavePlan(queryHash, "HashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and trange.b = thash.b and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.b = tregular2.b and tregular1.a > %v;", x1)
	tk.MustHavePlan(queryHash, "HashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a = %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a = %v;", x1)
	tk.MustHavePlan(queryHash, "HashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 2
	// hash_join range partition and regular table
	queryHash = fmt.Sprintf("select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a >= %v and tregular1.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a >= %v and tregular1.a > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "HashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a in (%v, %v, %v);", x1, x2, x3)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a in (%v, %v, %v);", x1, x2, x3)
	tk.MustHavePlan(queryHash, "HashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and tregular1.a >= %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular1.a >= %v;", x1)
	tk.MustHavePlan(queryHash, "HashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 3
	// merge_join range partition and hash partition
	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.b=thash.b and thash.a = %v and trange.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.b=tregular1.b and tregular1.a = %v and tregular2.a > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "MergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a > %v;", x1)
	tk.MustHavePlan(queryHash, "MergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and trange.b = thash.b and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.b = tregular2.b and tregular1.a > %v;", x1)
	tk.MustHavePlan(queryHash, "MergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a = %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a = %v;", x1)
	tk.MustHavePlan(queryHash, "MergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 4
	// merge_join range partition and regular table
	queryHash = fmt.Sprintf("select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a >= %v and tregular1.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a >= %v and tregular1.a > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "MergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a in (%v, %v, %v);", x1, x2, x3)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a in (%v, %v, %v);", x1, x2, x3)
	tk.MustHavePlan(queryHash, "MergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and tregular1.a >= %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular1.a >= %v;", x1)
	tk.MustHavePlan(queryHash, "MergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// new table instances
	tk.MustExec("create table thash2(a int, b int, index idx(a)) partition by hash(a) partitions 4")
	tk.MustExec("create table tregular3(a int, b int, index idx(a))")

	tk.MustExec(`create table trange2(a int, b int, index idx(a)) partition by range(a) (
		partition p0 values less than (200),
		partition p1 values less than (400),
		partition p2 values less than (600),
		partition p3 values less than (800),
		partition p4 values less than (1001))`)
	tk.MustExec("create table tregular4(a int, b int, index idx(a))")

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into thash2 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular3 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into trange2 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular4 values " + strings.Join(vals, ","))

	// group 5
	// index_merge_join range partition and range partition
	// Currently don't support index merge join on two partition tables. Only test warning.
	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v;", x1)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v;", x1)
	// tk.MustHavePlan(queryHash, "IndexMergeJoin")
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v and trange2.a > %v;", x1, x2)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.a > %v;", x1, x2)
	// tk.MustHavePlan(queryHash, "IndexMergeJoin")
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v and trange.b > %v;", x1, x2)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular2.b > %v;", x1, x2)
	// tk.MustHavePlan(queryHash, "IndexMergeJoin")
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v and trange2.b > %v;", x1, x2)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.b > %v;", x1, x2)
	// tk.MustHavePlan(queryHash, "IndexMergeJoin")
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	// group 6
	// index_merge_join range partition and regualr table
	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v;", x1)
	tk.MustHavePlan(queryHash, "IndexMergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v and tregular4.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.a > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "IndexMergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v and trange.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular2.b > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "IndexMergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v and tregular4.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.b > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "IndexMergeJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 7
	// index_hash_join hash partition and hash partition
	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a in (%v, %v);", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v);", x1, x2)
	tk.MustHavePlan(queryHash, "IndexHashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a in (%v, %v) and thash2.a in (%v, %v);", x1, x2, x3, x4)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v) and tregular3.a in (%v, %v);", x1, x2, x3, x4)
	tk.MustHavePlan(queryHash, "IndexHashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a > %v and thash2.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a > %v and tregular3.b > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "IndexHashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 8
	// index_hash_join hash partition and hash partition
	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a in (%v, %v);", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v);", x1, x2)
	tk.MustHavePlan(queryHash, "IndexHashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a in (%v, %v) and tregular3.a in (%v, %v);", x1, x2, x3, x4)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v) and tregular3.a in (%v, %v);", x1, x2, x3, x4)
	tk.MustHavePlan(queryHash, "IndexHashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a > %v and tregular3.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a > %v and tregular3.b > %v;", x1, x2)
	tk.MustHavePlan(queryHash, "IndexHashJoin")
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
}

func TestMPPQueryExplainInfo(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database tiflash_partition_test")
	tk.MustExec("use tiflash_partition_test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table t(a int) partition by range(a) (
		  partition p0 values less than (5),
		  partition p1 values less than (10),
		  partition p2 values less than (15))`)
	tb := external.GetTableByName(t, tk, "tiflash_partition_test", "t")
	for _, partition := range tb.Meta().GetPartitionInfo().Definitions {
		err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.ID, true)
		require.NoError(t, err)
	}
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec(`insert into t values (2), (7), (12)`)
	tk.MustExec("set tidb_enforce_mpp=1")
	tk.MustPartition(`select * from t where a < 3`, "p0").Sort().Check(testkit.Rows("2"))
	tk.MustPartition(`select * from t where a < 8`, "p0,p1").Sort().Check(testkit.Rows("2", "7"))
	tk.MustPartition(`select * from t where a < 20`, "all").Sort().Check(testkit.Rows("12", "2", "7"))
	tk.MustPartition(`select * from t where a < 5 union all select * from t where a > 10`, "p0").Sort().Check(testkit.Rows("12", "2"))
	tk.MustPartition(`select * from t where a < 5 union all select * from t where a > 10`, "p2").Sort().Check(testkit.Rows("12", "2"))
}

func TestDML(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_DML")
	defer tk.MustExec(`drop database test_DML`)
	tk.MustExec("use test_DML")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table tinner (a int, b int)`)
	tk.MustExec(`create table thash (a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int) partition by range(a) (
		partition p0 values less than(10000),
		partition p1 values less than(20000),
		partition p2 values less than(30000),
		partition p3 values less than(40000),
		partition p4 values less than MAXVALUE)`)

	vals := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into tinner values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	// delete, insert, replace, update
	for i := 0; i < 200; i++ {
		var pattern string
		switch rand.Intn(4) {
		case 0: // delete
			col := []string{"a", "b"}[rand.Intn(2)]
			l := rand.Intn(40000)
			r := l + rand.Intn(5000)
			pattern = fmt.Sprintf(`delete from %%v where %v>%v and %v<%v`, col, l, col, r)
		case 1: // insert
			a, b := rand.Intn(40000), rand.Intn(40000)
			pattern = fmt.Sprintf(`insert into %%v values (%v, %v)`, a, b)
		case 2: // replace
			a, b := rand.Intn(40000), rand.Intn(40000)
			pattern = fmt.Sprintf(`replace into %%v(a, b) values (%v, %v)`, a, b)
		case 3: // update
			col := []string{"a", "b"}[rand.Intn(2)]
			l := rand.Intn(40000)
			r := l + rand.Intn(5000)
			x := rand.Intn(1000) - 500
			pattern = fmt.Sprintf(`update %%v set %v=%v+%v where %v>%v and %v<%v`, col, col, x, col, l, col, r)
		}
		for _, tbl := range []string{"tinner", "thash", "trange"} {
			tk.MustExec(fmt.Sprintf(pattern, tbl))
		}

		// check
		r := tk.MustQuery(`select * from tinner`).Sort().Rows()
		tk.MustQuery(`select * from thash`).Sort().Check(r)
		tk.MustQuery(`select * from trange`).Sort().Check(r)
	}
}

func TestUnion(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_union")
	defer tk.MustExec(`drop database test_union`)
	tk.MustExec("use test_union")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table t(a int, b int, key(a))`)
	tk.MustExec(`create table thash (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, key(a)) partition by range(a) (
		partition p0 values less than (10000),
		partition p1 values less than (20000),
		partition p2 values less than (30000),
		partition p3 values less than (40000))`)

	vals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into t values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	randRange := func() (int, int) {
		l, r := rand.Intn(40000), rand.Intn(40000)
		if l > r {
			l, r = r, l
		}
		return l, r
	}

	for i := 0; i < 100; i++ {
		a1l, a1r := randRange()
		a2l, a2r := randRange()
		b1l, b1r := randRange()
		b2l, b2r := randRange()
		for _, utype := range []string{"union all", "union distinct"} {
			pattern := fmt.Sprintf(`select * from %%v where a>=%v and a<=%v and b>=%v and b<=%v
			%v select * from %%v where a>=%v and a<=%v and b>=%v and b<=%v`, a1l, a1r, b1l, b1r, utype, a2l, a2r, b2l, b2r)
			r := tk.MustQuery(fmt.Sprintf(pattern, "t", "t")).Sort().Rows()
			tk.MustQuery(fmt.Sprintf(pattern, "thash", "thash")).Sort().Check(r)   // hash + hash
			tk.MustQuery(fmt.Sprintf(pattern, "trange", "trange")).Sort().Check(r) // range + range
			tk.MustQuery(fmt.Sprintf(pattern, "trange", "thash")).Sort().Check(r)  // range + hash
		}
	}
}

func TestSubqueries(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_subquery")
	defer tk.MustExec(`drop database test_subquery`)
	tk.MustExec("use test_subquery")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table touter (a int, b int, index(a))`)
	tk.MustExec(`create table tinner (a int, b int, c int, index(a))`)
	tk.MustExec(`create table thash (a int, b int, c int, index(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, c int, index(a)) partition by range(a) (
		partition p0 values less than(10000),
		partition p1 values less than(20000),
		partition p2 values less than(30000),
		partition p3 values less than(40000))`)

	outerVals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		outerVals = append(outerVals, fmt.Sprintf(`(%v, %v)`, rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into touter values ` + strings.Join(outerVals, ", "))
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf(`(%v, %v, %v)`, rand.Intn(40000), rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into tinner values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	// in
	for i := 0; i < 50; i++ {
		for _, op := range []string{"in", "not in"} {
			x := rand.Intn(40000)
			var r [][]any
			for _, t := range []string{"tinner", "thash", "trange"} {
				q := fmt.Sprintf(`select * from touter where touter.a %v (select %v.b from %v where %v.a > touter.b and %v.c > %v)`, op, t, t, t, t, x)
				if r == nil {
					r = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Sort().Check(r)
				}
			}
		}
	}

	// exist
	for i := 0; i < 50; i++ {
		for _, op := range []string{"exists", "not exists"} {
			x := rand.Intn(40000)
			var r [][]any
			for _, t := range []string{"tinner", "thash", "trange"} {
				q := fmt.Sprintf(`select * from touter where %v (select %v.b from %v where %v.a > touter.b and %v.c > %v)`, op, t, t, t, t, x)
				if r == nil {
					r = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Sort().Check(r)
				}
			}
		}
	}
}

func TestSplitRegion(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_split_region")
	tk.MustExec("use test_split_region")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table tnormal (a int, b int)`)
	tk.MustExec(`create table thash (a int, b int, index(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, index(a)) partition by range(a) (
		partition p0 values less than (10000),
		partition p1 values less than (20000),
		partition p2 values less than (30000),
		partition p3 values less than (40000))`)
	vals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into tnormal values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	tk.MustExec(`SPLIT TABLE thash INDEX a BETWEEN (1) AND (25000) REGIONS 10`)
	tk.MustExec(`SPLIT TABLE trange INDEX a BETWEEN (1) AND (25000) REGIONS 10`)

	result := tk.MustQuery(`select * from tnormal where a>=1 and a<=15000`).Sort().Rows()
	tk.MustPartition(`select * from trange where a>=1 and a<=15000`, "p0,p1").Sort().Check(result)
	tk.MustPartition(`select * from thash where a>=1 and a<=15000`, "all").Sort().Check(result)

	result = tk.MustQuery(`select * from tnormal where a in (1, 10001, 20001)`).Sort().Rows()
	tk.MustPartition(`select * from trange where a in (1, 10001, 20001)`, "p0,p1,p2").Sort().Check(result)
	tk.MustPartition(`select * from thash where a in (1, 10001, 20001)`, "p1").Sort().Check(result)
}

func TestParallelApply(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create database test_parallel_apply")
	tk.MustExec("use test_parallel_apply")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	tk.MustExec(`create table touter (a int, b int)`)
	tk.MustExec(`create table tinner (a int, b int, key(a))`)
	tk.MustExec(`create table thash (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, key(a)) partition by range(a) (
			  partition p0 values less than(10000),
			  partition p1 values less than(20000),
			  partition p2 values less than(30000),
			  partition p3 values less than(40000))`)

	vouter := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vouter = append(vouter, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec("insert into touter values " + strings.Join(vouter, ", "))

	vals := make([]string, 0, 2000)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec("insert into tinner values " + strings.Join(vals, ", "))
	tk.MustExec("insert into thash values " + strings.Join(vals, ", "))
	tk.MustExec("insert into trange values " + strings.Join(vals, ", "))

	// parallel apply + hash partition + IndexReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(thash.a) from thash use index(a) where thash.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#8)->Column#7`,
		`    └─IndexReader 10000.00 root partition:all index:HashAgg`, // IndexReader is a inner child of Apply
		`      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.thash.a)->Column#8`,
		`        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.thash.a, test_parallel_apply.touter.b)`,
		`          └─IndexFullScan 100000000.00 cop[tikv] table:thash, index:a(a) keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(thash.a) from thash use index(a) where thash.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.a) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + hash partition + TableReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(thash.b) from thash ignore index(a) where thash.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#8)->Column#7`,
		`    └─TableReader 10000.00 root partition:all data:HashAgg`, // TableReader is a inner child of Apply
		`      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.thash.b)->Column#8`,
		`        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.thash.a, test_parallel_apply.touter.b)`,
		`          └─TableFullScan 100000000.00 cop[tikv] table:thash keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(thash.b) from thash ignore index(a) where thash.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner ignore index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + hash partition + IndexLookUp as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─IndexLookUp 10000.00 root  `, // IndexLookUp is a inner child of Apply
		`      ├─Selection(Build) 80000000.00 cop[tikv]  gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)`,
		`      │ └─IndexFullScan 100000000.00 cop[tikv] table:tinner, index:a(a) keep order:false, stats:pseudo`,
		`      └─HashAgg(Probe) 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.tinner.b)->Column#9`,
		`        └─TableRowIDScan 80000000.00 cop[tikv] table:tinner keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(thash.b) from thash use index(a) where thash.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + range partition + IndexReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(trange.a) from trange use index(a) where trange.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#8)->Column#7`,
		`    └─IndexReader 10000.00 root partition:all index:HashAgg`, // IndexReader is a inner child of Apply
		`      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.trange.a)->Column#8`,
		`        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.trange.a, test_parallel_apply.touter.b)`,
		`          └─IndexFullScan 100000000.00 cop[tikv] table:trange, index:a(a) keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(trange.a) from trange use index(a) where trange.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.a) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + range partition + TableReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(trange.b) from trange ignore index(a) where trange.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#8)->Column#7`,
		`    └─TableReader 10000.00 root partition:all data:HashAgg`, // TableReader is a inner child of Apply
		`      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.trange.b)->Column#8`,
		`        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.trange.a, test_parallel_apply.touter.b)`,
		`          └─TableFullScan 100000000.00 cop[tikv] table:trange keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(trange.b) from trange ignore index(a) where trange.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner ignore index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + range partition + IndexLookUp as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─IndexLookUp 10000.00 root  `, // IndexLookUp is a inner child of Apply
		`      ├─Selection(Build) 80000000.00 cop[tikv]  gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)`,
		`      │ └─IndexFullScan 100000000.00 cop[tikv] table:tinner, index:a(a) keep order:false, stats:pseudo`,
		`      └─HashAgg(Probe) 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.tinner.b)->Column#9`,
		`        └─TableRowIDScan 80000000.00 cop[tikv] table:tinner keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(trange.b) from trange use index(a) where trange.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// random queries
	ops := []string{"!=", ">", "<", ">=", "<="}
	aggFuncs := []string{"sum", "count", "max", "min"}
	tbls := []string{"tinner", "thash", "trange"}
	for i := 0; i < 50; i++ {
		var r [][]any
		op := ops[rand.Intn(len(ops))]
		agg := aggFuncs[rand.Intn(len(aggFuncs))]
		x := rand.Intn(10000)
		for _, tbl := range tbls {
			q := fmt.Sprintf(`select * from touter where touter.a > (select %v(%v.b) from %v where %v.a%vtouter.b-%v)`, agg, tbl, tbl, tbl, op, x)
			if r == nil {
				r = tk.MustQuery(q).Sort().Rows()
			} else {
				tk.MustQuery(q).Sort().Check(r)
			}
		}
	}
}

func TestDirectReadingWithUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_unionscan")
	defer tk.MustExec(`drop database test_unionscan`)
	tk.MustExec("use test_unionscan")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table trange(a int, b int, index idx_a(a)) partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (30),
		partition p2 values less than (50))`)
	tk.MustExec(`create table thash(a int, b int, index idx_a(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table tnormal(a int, b int, index idx_a(a))`)

	vals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(50), rand.Intn(50)))
	}
	for _, tb := range []string{`trange`, `tnormal`, `thash`} {
		sql := fmt.Sprintf(`insert into %v values `+strings.Join(vals, ", "), tb)
		tk.MustExec(sql)
	}

	randCond := func(col string) string {
		la, ra := rand.Intn(50), rand.Intn(50)
		if la > ra {
			la, ra = ra, la
		}
		return fmt.Sprintf(`%v>=%v and %v<=%v`, col, la, col, ra)
	}

	tk.MustExec(`begin`)
	for i := 0; i < 1000; i++ {
		if i == 0 || rand.Intn(2) == 0 { // insert some inflight rows
			val := fmt.Sprintf("(%v, %v)", rand.Intn(50), rand.Intn(50))
			for _, tb := range []string{`trange`, `tnormal`, `thash`} {
				sql := fmt.Sprintf(`insert into %v values `+val, tb)
				tk.MustExec(sql)
			}
		} else {
			var sql string
			switch rand.Intn(3) {
			case 0: // table scan
				sql = `select * from %v ignore index(idx_a) where ` + randCond(`b`)
			case 1: // index reader
				sql = `select a from %v use index(idx_a) where ` + randCond(`a`)
			case 2: // index lookup
				sql = `select * from %v use index(idx_a) where ` + randCond(`a`) + ` and ` + randCond(`b`)
			}
			switch rand.Intn(2) {
			case 0: // order by a
				sql += ` order by a`
			case 1: // order by b
				sql += ` order by b`
			}

			var result [][]any
			for _, tb := range []string{`trange`, `tnormal`, `thash`} {
				q := fmt.Sprintf(sql, tb)
				tk.MustHavePlan(q, `UnionScan`)
				if result == nil {
					result = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Sort().Check(result)
				}
			}
		}
	}
	tk.MustExec(`rollback`)
}

func TestUnsignedPartitionColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_unsigned_partition")
	tk.MustExec("use test_unsigned_partition")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table thash_pk (a int unsigned, b int, primary key(a)) partition by hash (a) partitions 3`)
	tk.MustExec(`create table trange_pk (a int unsigned, b int, primary key(a)) partition by range (a) (
		partition p1 values less than (100000),
		partition p2 values less than (200000),
		partition p3 values less than (300000),
		partition p4 values less than (400000))`)
	tk.MustExec(`create table tnormal_pk (a int unsigned, b int, primary key(a))`)
	tk.MustExec(`create table thash_uniq (a int unsigned, b int, unique key(a)) partition by hash (a) partitions 3`)
	tk.MustExec(`create table trange_uniq (a int unsigned, b int, unique key(a)) partition by range (a) (
		partition p1 values less than (100000),
		partition p2 values less than (200000),
		partition p3 values less than (300000),
		partition p4 values less than (400000))`)
	tk.MustExec(`create table tnormal_uniq (a int unsigned, b int, unique key(a))`)

	valColA := make(map[int]struct{}, 1000)
	vals := make([]string, 0, 1000)
	for len(vals) < 1000 {
		a := rand.Intn(400000)
		if _, ok := valColA[a]; ok {
			continue
		}
		valColA[a] = struct{}{}
		vals = append(vals, fmt.Sprintf("(%v, %v)", a, rand.Intn(400000)))
	}
	valStr := strings.Join(vals, ", ")
	for _, tbl := range []string{"thash_pk", "trange_pk", "tnormal_pk", "thash_uniq", "trange_uniq", "tnormal_uniq"} {
		tk.MustExec(fmt.Sprintf("insert into %v values %v", tbl, valStr))
	}

	for i := 0; i < 100; i++ {
		scanCond := fmt.Sprintf("a %v %v", []string{">", "<"}[rand.Intn(2)], rand.Intn(400000))
		pointCond := fmt.Sprintf("a = %v", rand.Intn(400000))
		batchCond := fmt.Sprintf("a in (%v, %v, %v)", rand.Intn(400000), rand.Intn(400000), rand.Intn(400000))

		var rScan, rPoint, rBatch [][]any
		for tid, tbl := range []string{"tnormal_pk", "trange_pk", "thash_pk"} {
			// unsigned + TableReader
			scanSQL := fmt.Sprintf("select * from %v use index(primary) where %v", tbl, scanCond)
			tk.MustHavePlan(scanSQL, "TableReader")
			r := tk.MustQuery(scanSQL).Sort()
			if tid == 0 {
				rScan = r.Rows()
			} else {
				r.Check(rScan)
			}

			// unsigned + PointGet on PK
			pointSQL := fmt.Sprintf("select * from %v use index(primary) where %v", tbl, pointCond)
			tk.MustPointGet(pointSQL)
			r = tk.MustQuery(pointSQL).Sort()
			if tid == 0 {
				rPoint = r.Rows()
			} else {
				r.Check(rPoint)
			}

			// unsigned + BatchGet on PK
			batchSQL := fmt.Sprintf("select * from %v where %v", tbl, batchCond)
			tk.MustHavePlan(batchSQL, "Batch_Point_Get")
			r = tk.MustQuery(batchSQL).Sort()
			if tid == 0 {
				rBatch = r.Rows()
			} else {
				r.Check(rBatch)
			}
		}

		lookupCond := fmt.Sprintf("a %v %v", []string{">", "<"}[rand.Intn(2)], rand.Intn(400000))
		var rLookup [][]any
		for tid, tbl := range []string{"tnormal_uniq", "trange_uniq", "thash_uniq"} {
			// unsigned + IndexReader
			scanSQL := fmt.Sprintf("select a from %v use index(a) where %v", tbl, scanCond)
			tk.MustHavePlan(scanSQL, "IndexReader")
			r := tk.MustQuery(scanSQL).Sort()
			if tid == 0 {
				rScan = r.Rows()
			} else {
				r.Check(rScan)
			}

			// unsigned + IndexLookUp
			lookupSQL := fmt.Sprintf("select * from %v use index(a) where %v", tbl, lookupCond)
			tk.MustIndexLookup(lookupSQL)
			r = tk.MustQuery(lookupSQL).Sort()
			if tid == 0 {
				rLookup = r.Rows()
			} else {
				r.Check(rLookup)
			}

			// unsigned + PointGet on UniqueIndex
			pointSQL := fmt.Sprintf("select * from %v use index(a) where %v", tbl, pointCond)
			tk.MustPointGet(pointSQL)
			r = tk.MustQuery(pointSQL).Sort()
			if tid == 0 {
				rPoint = r.Rows()
			} else {
				r.Check(rPoint)
			}

			// unsigned + BatchGet on UniqueIndex
			batchSQL := fmt.Sprintf("select * from %v where %v", tbl, batchCond)
			tk.MustHavePlan(batchSQL, "Batch_Point_Get")
			r = tk.MustQuery(batchSQL).Sort()
			if tid == 0 {
				rBatch = r.Rows()
			} else {
				r.Check(rBatch)
			}
		}
	}
}

func TestDirectReadingWithAgg(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_dr_agg")
	tk.MustExec("use test_dr_agg")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, index idx_a(a), index idx_b(b)) partition by list(a)(
		partition p0 values in (1, 2, 3, 4),
		partition p1 values in (5, 6, 7, 8),
		partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange(a int, b int, index idx_a(a), index idx_b(b)) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table
	tk.MustExec(`create table thash(a int, b int) partition by hash(a) partitions 4;`)

	// regular table
	tk.MustExec("create table tregular1(a int, b int, index idx_a(a))")
	tk.MustExec("create table tregular2(a int, b int, index idx_a(a))")

	// generate some random data to be inserted
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1100), rand.Intn(2000)))
	}

	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular1 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(12)+1, rand.Intn(20)))
	}

	tk.MustExec("insert into tlist values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular2 values " + strings.Join(vals, ","))

	// test range partition
	for i := 0; i < 200; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		tk.MustHavePlan(queryPartition1, "StreamAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		tk.MustHavePlan(queryPartition2, "HashAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(1099)
		z := rand.Intn(1099)

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in(%v, %v, %v) group by a;", x, y, z)
		tk.MustHavePlan(queryPartition3, "StreamAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in (%v, %v, %v) group by a;", x, y, z)
		tk.MustHavePlan(queryPartition4, "HashAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}

	// test hash partition
	for i := 0; i < 200; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		tk.MustHavePlan(queryPartition1, "StreamAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		tk.MustHavePlan(queryPartition2, "HashAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(1099)
		z := rand.Intn(1099)

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in(%v, %v, %v) group by a;", x, y, z)
		tk.MustHavePlan(queryPartition3, "StreamAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in (%v, %v, %v) group by a;", x, y, z)
		tk.MustHavePlan(queryPartition4, "HashAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}

	// test list partition
	for i := 0; i < 200; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(12) + 1

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a > %v group by a;", x)
		tk.MustHavePlan(queryPartition1, "StreamAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a > %v group by a;", x)
		tk.MustHavePlan(queryPartition2, "HashAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(12) + 1
		z := rand.Intn(12) + 1

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a in(%v, %v, %v) group by a;", x, y, z)
		tk.MustHavePlan(queryPartition3, "StreamAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a in (%v, %v, %v) group by a;", x, y, z)
		tk.MustHavePlan(queryPartition4, "HashAgg") // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}
}

func TestIdexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_idx_merge")
	tk.MustExec("use test_idx_merge")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, primary key(a) clustered, index idx_b(b)) partition by list(a)(
		partition p0 values in (1, 2, 3, 4),
		partition p1 values in (5, 6, 7, 8),
		partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange(a int, b int, primary key(a) clustered, index idx_b(b)) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table
	tk.MustExec(`create table thash(a int, b int, primary key(a) clustered, index idx_b(b)) partition by hash(a) partitions 4;`)

	// regular table
	tk.MustExec("create table tregular1(a int, b int, primary key(a) clustered)")
	tk.MustExec("create table tregular2(a int, b int, primary key(a) clustered)")

	// generate some random data to be inserted
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1100), rand.Intn(2000)))
	}

	tk.MustExec("insert ignore into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into tregular1 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(12)+1, rand.Intn(20)))
	}

	tk.MustExec("insert ignore into tlist values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into tregular2 values " + strings.Join(vals, ","))

	// test range partition
	for i := 0; i < 100; i++ {
		x1 := rand.Intn(1099)
		x2 := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ use_index_merge(trange) */ * from trange where a > %v or b < %v;", x1, x2)
		queryRegular1 := fmt.Sprintf("select /*+ use_index_merge(tregular1) */ * from tregular1 where a > %v or b < %v;", x1, x2)
		tk.MustHavePlan(queryPartition1, "IndexMerge") // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ use_index_merge(trange) */ * from trange where a > %v or b > %v;", x1, x2)
		queryRegular2 := fmt.Sprintf("select /*+ use_index_merge(tregular1) */ * from tregular1 where a > %v or b > %v;", x1, x2)
		tk.MustHavePlan(queryPartition2, "IndexMerge") // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}

	// test hash partition
	for i := 0; i < 100; i++ {
		x1 := rand.Intn(1099)
		x2 := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ use_index_merge(thash) */ * from thash where a > %v or b < %v;", x1, x2)
		queryRegular1 := fmt.Sprintf("select /*+ use_index_merge(tregualr1) */ * from tregular1 where a > %v or b < %v;", x1, x2)
		tk.MustHavePlan(queryPartition1, "IndexMerge") // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ use_index_merge(thash) */ * from thash where a > %v or b > %v;", x1, x2)
		queryRegular2 := fmt.Sprintf("select /*+ use_index_merge(tregular1) */ * from tregular1 where a > %v or b > %v;", x1, x2)
		tk.MustHavePlan(queryPartition2, "IndexMerge") // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}

	// test list partition
	for i := 0; i < 100; i++ {
		x1 := rand.Intn(12) + 1
		x2 := rand.Intn(12) + 1
		queryPartition1 := fmt.Sprintf("select /*+ use_index_merge(tlist) */ * from tlist where a > %v or b < %v;", x1, x2)
		queryRegular1 := fmt.Sprintf("select /*+ use_index_merge(tregular2) */ * from tregular2 where a > %v or b < %v;", x1, x2)
		tk.MustHavePlan(queryPartition1, "IndexMerge") // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ use_index_merge(tlist) */ * from tlist where a > %v or b > %v;", x1, x2)
		queryRegular2 := fmt.Sprintf("select /*+ use_index_merge(tregular2) */ * from tregular2 where a > %v or b > %v;", x1, x2)
		tk.MustHavePlan(queryPartition2, "IndexMerge") // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}
}

func TestDropGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists p")
	tk.MustExec(`create table p (id int, c int) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("alter table p add unique idx(id)")

	failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/checkDropGlobalIndex", `return(true)`)
	tk.MustExec("alter table p drop index idx")
	failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/checkDropGlobalIndex")
}

func TestSelectLockOnPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists pt")
	tk.MustExec(`create table pt (id int primary key, k int, c int, index(k))
partition by range (id) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (11))`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	optimisticTableReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select id, k from pt ignore index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		tk2.MustExec("update pt set c = c + 1 where k = 5")
		_, err := tk.Exec("commit")
		require.Error(t, err) // Write conflict
	}

	optimisticIndexReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk.MustExec("begin")
		// This is not index reader actually.
		tk.MustQuery("select k from pt where k = 5 for update").Check(testkit.Rows("5"))
		tk2.MustExec("update pt set c = c + 1 where k = 5")
		_, err := tk.Exec("commit")
		require.Error(t, err)
	}

	optimisticIndexLookUp := func() {
		tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select c, k from pt use index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		tk2.MustExec("update pt set c = c + 1 where k = 5")
		_, err := tk.Exec("commit")
		require.Error(t, err)
	}

	pessimisticTableReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select id, k from pt ignore index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		ch := make(chan int, 2)
		go func() {
			tk2.MustExec("update pt set c = c + 1 where k = 5")
			ch <- 1
		}()
		time.Sleep(100 * time.Millisecond)
		ch <- 2

		// Check the operation in the goroutine is blocked, if not the first result in
		// the channel should be 1.
		require.Equal(t, 2, <-ch)

		tk.MustExec("commit")
		<-ch
		tk.MustQuery("select c from pt where k = 5").Check(testkit.Rows("6"))
	}

	pessimisticIndexReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk.MustExec("begin")
		// This is not index reader actually.
		tk.MustQuery("select k from pt where k = 5 for update").Check(testkit.Rows("5"))
		ch := make(chan int, 2)
		go func() {
			tk2.MustExec("update pt set c = c + 1 where k = 5")
			ch <- 1
		}()
		time.Sleep(100 * time.Millisecond)
		ch <- 2

		// Check the operation in the goroutine is blocked,
		require.Equal(t, 2, <-ch)

		tk.MustExec("commit")
		<-ch
		tk.MustQuery("select c from pt where k = 5").Check(testkit.Rows("6"))
	}

	pessimisticIndexLookUp := func() {
		tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select c, k from pt use index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		ch := make(chan int, 2)
		go func() {
			tk2.MustExec("update pt set c = c + 1 where k = 5")
			ch <- 1
		}()
		time.Sleep(100 * time.Millisecond)
		ch <- 2

		// Check the operation in the goroutine is blocked,
		require.Equal(t, 2, <-ch)

		tk.MustExec("commit")
		<-ch
		tk.MustQuery("select c from pt where k = 5").Check(testkit.Rows("6"))
	}

	partitionModes := []string{
		"'dynamic'",
		"'static'",
	}
	testCases := []func(){
		optimisticTableReader,
		optimisticIndexLookUp,
		optimisticIndexReader,
		pessimisticTableReader,
		pessimisticIndexReader,
		pessimisticIndexLookUp,
	}

	for _, mode := range partitionModes {
		tk.MustExec("set @@tidb_partition_prune_mode=" + mode)
		for _, c := range testCases {
			tk.MustExec("replace into pt values (5, 5, 5)")
			c()
		}
	}
}

func TestIssue26251(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk1.MustExec("set tidb_enable_global_index=default")
	}()
	tk1.MustExec("use test")
	tk1.MustExec("create table tp (id int primary key) partition by range (id) (partition p0 values less than (100));")
	tk1.MustExec("create table tn (id int primary key);")
	tk1.MustExec("insert into tp values(1),(2);")
	tk1.MustExec("insert into tn values(1),(2);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select * from tp,tn where tp.id=tn.id and tn.id<=1 for update;").Check(testkit.Rows("1 1"))

	ch := make(chan struct{}, 1)
	tk2.MustExec("begin pessimistic")
	go func() {
		// This query should block.
		tk2.MustQuery("select * from tn where id=1 for update;").Check(testkit.Rows("1"))
		ch <- struct{}{}
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		// Expected, query blocked, not finish within 100ms.
		tk1.MustExec("rollback")
	case <-ch:
		// Unexpected, test fail.
		t.Fail()
	}

	// Clean up
	<-ch
	tk2.MustExec("rollback")
}

func TestLeftJoinForUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database TestLeftJoinForUpdate")
	defer tk1.MustExec("drop database TestLeftJoinForUpdate")
	tk1.MustExec("use TestLeftJoinForUpdate")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use TestLeftJoinForUpdate")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use TestLeftJoinForUpdate")

	tk1.MustExec("drop table if exists nt, pt")
	tk1.MustExec("create table nt (id int, col varchar(32), primary key (id))")
	tk1.MustExec("create table pt (id int, col varchar(32), primary key (id)) partition by hash(id) partitions 4")

	resetData := func() {
		tk1.MustExec("truncate table nt")
		tk1.MustExec("truncate table pt")
		tk1.MustExec("insert into nt values (1, 'hello')")
		tk1.MustExec("insert into pt values (2, 'test')")
	}

	// ========================== First round of test ==================
	// partition table left join normal table.
	// =================================================================
	resetData()
	ch := make(chan int, 10)
	tk1.MustExec("begin pessimistic")
	// No union scan
	tk1.MustQuery("select * from pt left join nt on pt.id = nt.id for update").Check(testkit.Rows("2 test <nil> <nil>"))
	go func() {
		// Check the key is locked.
		tk2.MustExec("update pt set col = 'xxx' where id = 2")
		ch <- 2
	}()

	// Union scan
	tk1.MustExec("insert into pt values (1, 'world')")
	tk1.MustQuery("select * from pt left join nt on pt.id = nt.id for update").Sort().Check(testkit.Rows("1 world 1 hello", "2 test <nil> <nil>"))
	go func() {
		// Check the key is locked.
		tk3.MustExec("update nt set col = 'yyy' where id = 1")
		ch <- 3
	}()

	// Give chance for the goroutines to run first.
	time.Sleep(80 * time.Millisecond)
	ch <- 1
	tk1.MustExec("rollback")

	checkOrder := func() {
		require.Equal(t, <-ch, 1)
		v1 := <-ch
		v2 := <-ch
		require.True(t, (v1 == 2 && v2 == 3) || (v1 == 3 && v2 == 2))
	}
	checkOrder()

	// ========================== Another round of test ==================
	// normal table left join partition table.
	// ===================================================================
	resetData()
	tk1.MustExec("begin pessimistic")
	// No union scan
	tk1.MustQuery("select * from nt left join pt on pt.id = nt.id for update").Check(testkit.Rows("1 hello <nil> <nil>"))

	// Union scan
	tk1.MustExec("insert into pt values (1, 'world')")
	tk1.MustQuery("select * from nt left join pt on pt.id = nt.id for update").Check(testkit.Rows("1 hello 1 world"))
	go func() {
		tk2.MustExec("replace into pt values (1, 'aaa')")
		ch <- 2
	}()
	go func() {
		tk3.MustExec("update nt set col = 'bbb' where id = 1")
		ch <- 3
	}()
	time.Sleep(80 * time.Millisecond)
	ch <- 1
	tk1.MustExec("rollback")
	checkOrder()
}

func TestIssue31024(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database TestIssue31024")
	defer tk1.MustExec("drop database TestIssue31024")
	tk1.MustExec("use TestIssue31024")
	tk1.MustExec("create table t1 (c_datetime datetime, c1 int, c2 int, primary key (c_datetime), key(c1), key(c2))" +
		" partition by range (to_days(c_datetime)) " +
		"( partition p0 values less than (to_days('2020-02-01'))," +
		" partition p1 values less than (to_days('2020-04-01'))," +
		" partition p2 values less than (to_days('2020-06-01'))," +
		" partition p3 values less than maxvalue)")
	tk1.MustExec("create table t2 (c_datetime datetime, unique key(c_datetime))")
	tk1.MustExec("insert into t1 values ('2020-06-26 03:24:00', 1, 1), ('2020-02-21 07:15:33', 2, 2), ('2020-04-27 13:50:58', 3, 3)")
	tk1.MustExec("insert into t2 values ('2020-01-10 09:36:00'), ('2020-02-04 06:00:00'), ('2020-06-12 03:45:18')")
	tk1.MustExec("SET GLOBAL tidb_txn_mode = 'pessimistic'")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use TestIssue31024")

	ch := make(chan int, 10)
	tk1.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select /*+ use_index_merge(t1) */ * from t1 join t2 on t1.c_datetime >= t2.c_datetime where t1.c1 < 10 or t1.c2 < 10 for update")

	go func() {
		// Check the key is locked.
		tk2.MustExec("set @@tidb_partition_prune_mode='dynamic'")
		tk2.MustExec("begin pessimistic")
		tk2.MustExec("update t1 set c_datetime = '2020-06-26 03:24:00' where c1 = 1")
		ch <- 2
	}()

	// Give chance for the goroutines to run first.
	time.Sleep(80 * time.Millisecond)
	ch <- 1
	tk1.MustExec("rollback")

	require.Equal(t, <-ch, 1)
	require.Equal(t, <-ch, 2)

	tk2.MustExec("rollback")
}
