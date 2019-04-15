// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestAnalyzePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("analyze table t")

	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi := table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	do, err := session.GetDomain(s.store)
	c.Assert(err, IsNil)
	handle := do.StatsHandle()
	for _, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
		c.Assert(len(statsTbl.Columns), Equals, 3)
		c.Assert(len(statsTbl.Indices), Equals, 1)
		for _, col := range statsTbl.Columns {
			c.Assert(col.Len(), Greater, 0)
		}
		for _, idx := range statsTbl.Indices {
			c.Assert(idx.Len(), Greater, 0)
		}
	}

	tk.MustExec("drop table t")
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("alter table t analyze partition p0")
	is = executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi = table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)

	for i, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		if i == 0 {
			c.Assert(statsTbl.Pseudo, IsFalse)
			c.Assert(len(statsTbl.Columns), Equals, 3)
			c.Assert(len(statsTbl.Indices), Equals, 1)
		} else {
			c.Assert(statsTbl.Pseudo, IsTrue)
		}
	}
}

func (s *testSuite1) TestAnalyzeParameters(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}

	tk.MustExec("analyze table t")
	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 20)

	tk.MustExec("analyze table t with 4 buckets")
	tbl = s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 4)
}

func (s *testSuite1) TestFastAnalyze(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
	)
	c.Assert(err, IsNil)
	var dom *domain.Domain
	dom, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	executor.MaxSampleSize = 1000
	executor.RandSeed = 123

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	for i := 0; i < 3000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tblInfo.Meta().ID

	// construct 5 regions split by {600, 1200, 1800, 2400}
	splitKeys := generateTableSplitKeyForInt(tid, []int{600, 1200, 1800, 2400})
	manipulateCluster(cluster, splitKeys)

	tk.MustExec("analyze table t")

	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(fmt.Sprintln(tbl), Equals,
		"Table:37 Count:3000\n"+
			"index:1 ndv:3000\n"+
			"num: 12 lower_bound: 0 upper_bound: 5 repeats: 1\n"+
			"num: 12 lower_bound: 9 upper_bound: 16 repeats: 1\n"+
			"num: 12 lower_bound: 17 upper_bound: 21 repeats: 1\n"+
			"num: 12 lower_bound: 22 upper_bound: 26 repeats: 1\n"+
			"num: 12 lower_bound: 31 upper_bound: 35 repeats: 1\n"+
			"num: 12 lower_bound: 37 upper_bound: 47 repeats: 1\n"+
			"num: 12 lower_bound: 50 upper_bound: 55 repeats: 1\n"+
			"num: 12 lower_bound: 56 upper_bound: 67 repeats: 1\n"+
			"num: 12 lower_bound: 74 upper_bound: 84 repeats: 1\n"+
			"num: 12 lower_bound: 86 upper_bound: 98 repeats: 1\n"+
			"num: 12 lower_bound: 99 upper_bound: 109 repeats: 1\n"+
			"num: 12 lower_bound: 112 upper_bound: 117 repeats: 1\n"+
			"num: 12 lower_bound: 119 upper_bound: 129 repeats: 1\n"+
			"num: 12 lower_bound: 131 upper_bound: 138 repeats: 1\n"+
			"num: 12 lower_bound: 139 upper_bound: 150 repeats: 1\n"+
			"num: 12 lower_bound: 153 upper_bound: 161 repeats: 1\n"+
			"num: 12 lower_bound: 164 upper_bound: 176 repeats: 1\n"+
			"num: 12 lower_bound: 180 upper_bound: 186 repeats: 1\n"+
			"num: 12 lower_bound: 188 upper_bound: 196 repeats: 1\n"+
			"num: 12 lower_bound: 198 upper_bound: 202 repeats: 1\n"+
			"num: 12 lower_bound: 203 upper_bound: 211 repeats: 1\n"+
			"num: 12 lower_bound: 212 upper_bound: 217 repeats: 1\n"+
			"num: 12 lower_bound: 219 upper_bound: 224 repeats: 1\n"+
			"num: 12 lower_bound: 229 upper_bound: 235 repeats: 1\n"+
			"num: 12 lower_bound: 236 upper_bound: 242 repeats: 1\n"+
			"num: 12 lower_bound: 248 upper_bound: 256 repeats: 1\n"+
			"num: 12 lower_bound: 262 upper_bound: 274 repeats: 1\n"+
			"num: 12 lower_bound: 275 upper_bound: 290 repeats: 1\n"+
			"num: 12 lower_bound: 292 upper_bound: 307 repeats: 1\n"+
			"num: 12 lower_bound: 309 upper_bound: 319 repeats: 1\n"+
			"num: 12 lower_bound: 320 upper_bound: 330 repeats: 1\n"+
			"num: 12 lower_bound: 337 upper_bound: 343 repeats: 1\n"+
			"num: 12 lower_bound: 345 upper_bound: 358 repeats: 1\n"+
			"num: 12 lower_bound: 362 upper_bound: 373 repeats: 1\n"+
			"num: 12 lower_bound: 374 upper_bound: 379 repeats: 1\n"+
			"num: 12 lower_bound: 381 upper_bound: 394 repeats: 1\n"+
			"num: 12 lower_bound: 395 upper_bound: 398 repeats: 1\n"+
			"num: 12 lower_bound: 399 upper_bound: 406 repeats: 1\n"+
			"num: 12 lower_bound: 407 upper_bound: 414 repeats: 1\n"+
			"num: 12 lower_bound: 416 upper_bound: 420 repeats: 1\n"+
			"num: 12 lower_bound: 421 upper_bound: 431 repeats: 1\n"+
			"num: 12 lower_bound: 433 upper_bound: 438 repeats: 1\n"+
			"num: 12 lower_bound: 441 upper_bound: 452 repeats: 1\n"+
			"num: 12 lower_bound: 453 upper_bound: 462 repeats: 1\n"+
			"num: 12 lower_bound: 466 upper_bound: 474 repeats: 1\n"+
			"num: 12 lower_bound: 478 upper_bound: 484 repeats: 1\n"+
			"num: 12 lower_bound: 486 upper_bound: 492 repeats: 1\n"+
			"num: 12 lower_bound: 498 upper_bound: 507 repeats: 1\n"+
			"num: 12 lower_bound: 512 upper_bound: 520 repeats: 1\n"+
			"num: 12 lower_bound: 521 upper_bound: 527 repeats: 1\n"+
			"num: 12 lower_bound: 530 upper_bound: 545 repeats: 1\n"+
			"num: 12 lower_bound: 549 upper_bound: 555 repeats: 1\n"+
			"num: 12 lower_bound: 563 upper_bound: 574 repeats: 1\n"+
			"num: 12 lower_bound: 575 upper_bound: 587 repeats: 1\n"+
			"num: 12 lower_bound: 592 upper_bound: 596 repeats: 1\n"+
			"num: 12 lower_bound: 598 upper_bound: 608 repeats: 1\n"+
			"num: 12 lower_bound: 610 upper_bound: 615 repeats: 1\n"+
			"num: 12 lower_bound: 618 upper_bound: 632 repeats: 1\n"+
			"num: 12 lower_bound: 636 upper_bound: 648 repeats: 1\n"+
			"num: 12 lower_bound: 653 upper_bound: 662 repeats: 1\n"+
			"num: 12 lower_bound: 670 upper_bound: 675 repeats: 1\n"+
			"num: 12 lower_bound: 677 upper_bound: 680 repeats: 1\n"+
			"num: 12 lower_bound: 683 upper_bound: 694 repeats: 1\n"+
			"num: 12 lower_bound: 696 upper_bound: 703 repeats: 1\n"+
			"num: 12 lower_bound: 704 upper_bound: 714 repeats: 1\n"+
			"num: 12 lower_bound: 718 upper_bound: 730 repeats: 1\n"+
			"num: 12 lower_bound: 732 upper_bound: 737 repeats: 1\n"+
			"num: 12 lower_bound: 739 upper_bound: 743 repeats: 1\n"+
			"num: 12 lower_bound: 745 upper_bound: 753 repeats: 1\n"+
			"num: 12 lower_bound: 754 upper_bound: 766 repeats: 1\n"+
			"num: 12 lower_bound: 770 upper_bound: 779 repeats: 1\n"+
			"num: 12 lower_bound: 786 upper_bound: 794 repeats: 1\n"+
			"num: 12 lower_bound: 796 upper_bound: 806 repeats: 1\n"+
			"num: 12 lower_bound: 809 upper_bound: 814 repeats: 1\n"+
			"num: 12 lower_bound: 815 upper_bound: 822 repeats: 1\n"+
			"num: 12 lower_bound: 826 upper_bound: 838 repeats: 1\n"+
			"num: 12 lower_bound: 839 upper_bound: 851 repeats: 1\n"+
			"num: 12 lower_bound: 852 upper_bound: 865 repeats: 1\n"+
			"num: 12 lower_bound: 871 upper_bound: 877 repeats: 1\n"+
			"num: 12 lower_bound: 883 upper_bound: 898 repeats: 1\n"+
			"num: 12 lower_bound: 900 upper_bound: 910 repeats: 1\n"+
			"num: 12 lower_bound: 911 upper_bound: 918 repeats: 1\n"+
			"num: 12 lower_bound: 920 upper_bound: 927 repeats: 1\n"+
			"num: 12 lower_bound: 928 upper_bound: 932 repeats: 1\n"+
			"num: 12 lower_bound: 933 upper_bound: 948 repeats: 1\n"+
			"num: 12 lower_bound: 949 upper_bound: 952 repeats: 1\n"+
			"num: 12 lower_bound: 960 upper_bound: 969 repeats: 1\n"+
			"num: 12 lower_bound: 970 upper_bound: 986 repeats: 1\n"+
			"num: 12 lower_bound: 988 upper_bound: 1007 repeats: 1\n"+
			"num: 12 lower_bound: 1010 upper_bound: 1019 repeats: 1\n"+
			"num: 12 lower_bound: 1020 upper_bound: 1025 repeats: 1\n"+
			"num: 12 lower_bound: 1026 upper_bound: 1033 repeats: 1\n"+
			"num: 12 lower_bound: 1034 upper_bound: 1045 repeats: 1\n"+
			"num: 12 lower_bound: 1046 upper_bound: 1055 repeats: 1\n"+
			"num: 12 lower_bound: 1058 upper_bound: 1069 repeats: 1\n"+
			"num: 12 lower_bound: 1075 upper_bound: 1081 repeats: 1\n"+
			"num: 12 lower_bound: 1082 upper_bound: 1091 repeats: 1\n"+
			"num: 12 lower_bound: 1092 upper_bound: 1108 repeats: 1\n"+
			"num: 12 lower_bound: 1109 upper_bound: 1113 repeats: 1\n"+
			"num: 12 lower_bound: 1114 upper_bound: 1121 repeats: 1\n"+
			"num: 12 lower_bound: 1123 upper_bound: 1130 repeats: 1\n"+
			"num: 12 lower_bound: 1131 upper_bound: 1142 repeats: 1\n"+
			"num: 12 lower_bound: 1146 upper_bound: 1162 repeats: 1\n"+
			"num: 12 lower_bound: 1168 upper_bound: 1175 repeats: 1\n"+
			"num: 12 lower_bound: 1176 upper_bound: 1193 repeats: 1\n"+
			"num: 12 lower_bound: 1202 upper_bound: 1209 repeats: 1\n"+
			"num: 12 lower_bound: 1211 upper_bound: 1215 repeats: 1\n"+
			"num: 12 lower_bound: 1216 upper_bound: 1224 repeats: 1\n"+
			"num: 12 lower_bound: 1230 upper_bound: 1236 repeats: 1\n"+
			"num: 12 lower_bound: 1239 upper_bound: 1243 repeats: 1\n"+
			"num: 12 lower_bound: 1244 upper_bound: 1258 repeats: 1\n"+
			"num: 12 lower_bound: 1259 upper_bound: 1269 repeats: 1\n"+
			"num: 12 lower_bound: 1272 upper_bound: 1284 repeats: 1\n"+
			"num: 12 lower_bound: 1292 upper_bound: 1296 repeats: 1\n"+
			"num: 12 lower_bound: 1298 upper_bound: 1307 repeats: 1\n"+
			"num: 12 lower_bound: 1316 upper_bound: 1327 repeats: 1\n"+
			"num: 12 lower_bound: 1328 upper_bound: 1336 repeats: 1\n"+
			"num: 12 lower_bound: 1338 upper_bound: 1345 repeats: 1\n"+
			"num: 12 lower_bound: 1350 upper_bound: 1363 repeats: 1\n"+
			"num: 12 lower_bound: 1368 upper_bound: 1375 repeats: 1\n"+
			"num: 12 lower_bound: 1380 upper_bound: 1394 repeats: 1\n"+
			"num: 12 lower_bound: 1404 upper_bound: 1409 repeats: 1\n"+
			"num: 12 lower_bound: 1415 upper_bound: 1421 repeats: 1\n"+
			"num: 12 lower_bound: 1422 upper_bound: 1436 repeats: 1\n"+
			"num: 12 lower_bound: 1440 upper_bound: 1445 repeats: 1\n"+
			"num: 12 lower_bound: 1456 upper_bound: 1463 repeats: 1\n"+
			"num: 12 lower_bound: 1466 upper_bound: 1474 repeats: 1\n"+
			"num: 12 lower_bound: 1476 upper_bound: 1482 repeats: 1\n"+
			"num: 12 lower_bound: 1484 upper_bound: 1501 repeats: 1\n"+
			"num: 12 lower_bound: 1502 upper_bound: 1514 repeats: 1\n"+
			"num: 12 lower_bound: 1515 upper_bound: 1523 repeats: 1\n"+
			"num: 12 lower_bound: 1525 upper_bound: 1532 repeats: 1\n"+
			"num: 12 lower_bound: 1533 upper_bound: 1543 repeats: 1\n"+
			"num: 12 lower_bound: 1544 upper_bound: 1557 repeats: 1\n"+
			"num: 12 lower_bound: 1559 upper_bound: 1571 repeats: 1\n"+
			"num: 12 lower_bound: 1572 upper_bound: 1583 repeats: 1\n"+
			"num: 12 lower_bound: 1587 upper_bound: 1593 repeats: 1\n"+
			"num: 12 lower_bound: 1594 upper_bound: 1601 repeats: 1\n"+
			"num: 12 lower_bound: 1607 upper_bound: 1612 repeats: 1\n"+
			"num: 12 lower_bound: 1616 upper_bound: 1625 repeats: 1\n"+
			"num: 12 lower_bound: 1631 upper_bound: 1647 repeats: 1\n"+
			"num: 12 lower_bound: 1650 upper_bound: 1658 repeats: 1\n"+
			"num: 12 lower_bound: 1660 upper_bound: 1670 repeats: 1\n"+
			"num: 12 lower_bound: 1671 upper_bound: 1678 repeats: 1\n"+
			"num: 12 lower_bound: 1680 upper_bound: 1688 repeats: 1\n"+
			"num: 12 lower_bound: 1691 upper_bound: 1699 repeats: 1\n"+
			"num: 12 lower_bound: 1700 upper_bound: 1710 repeats: 1\n"+
			"num: 12 lower_bound: 1711 upper_bound: 1720 repeats: 1\n"+
			"num: 12 lower_bound: 1721 upper_bound: 1738 repeats: 1\n"+
			"num: 12 lower_bound: 1740 upper_bound: 1744 repeats: 1\n"+
			"num: 12 lower_bound: 1746 upper_bound: 1751 repeats: 1\n"+
			"num: 12 lower_bound: 1752 upper_bound: 1767 repeats: 1\n"+
			"num: 12 lower_bound: 1769 upper_bound: 1791 repeats: 1\n"+
			"num: 12 lower_bound: 1796 upper_bound: 1802 repeats: 1\n"+
			"num: 12 lower_bound: 1804 upper_bound: 1816 repeats: 1\n"+
			"num: 12 lower_bound: 1821 upper_bound: 1829 repeats: 1\n"+
			"num: 12 lower_bound: 1832 upper_bound: 1839 repeats: 1\n"+
			"num: 12 lower_bound: 1840 upper_bound: 1851 repeats: 1\n"+
			"num: 12 lower_bound: 1856 upper_bound: 1868 repeats: 1\n"+
			"num: 12 lower_bound: 1870 upper_bound: 1885 repeats: 1\n"+
			"num: 12 lower_bound: 1888 upper_bound: 1898 repeats: 1\n"+
			"num: 12 lower_bound: 1899 upper_bound: 1915 repeats: 1\n"+
			"num: 12 lower_bound: 1921 upper_bound: 1928 repeats: 1\n"+
			"num: 12 lower_bound: 1931 upper_bound: 1941 repeats: 1\n"+
			"num: 12 lower_bound: 1945 upper_bound: 1952 repeats: 1\n"+
			"num: 12 lower_bound: 1954 upper_bound: 1967 repeats: 1\n"+
			"num: 12 lower_bound: 1972 upper_bound: 1977 repeats: 1\n"+
			"num: 12 lower_bound: 1978 upper_bound: 1983 repeats: 1\n"+
			"num: 12 lower_bound: 1984 upper_bound: 1990 repeats: 1\n"+
			"num: 12 lower_bound: 1991 upper_bound: 1996 repeats: 1\n"+
			"num: 12 lower_bound: 2001 upper_bound: 2015 repeats: 1\n"+
			"num: 12 lower_bound: 2018 upper_bound: 2027 repeats: 1\n"+
			"num: 12 lower_bound: 2036 upper_bound: 2040 repeats: 1\n"+
			"num: 12 lower_bound: 2046 upper_bound: 2055 repeats: 1\n"+
			"num: 12 lower_bound: 2056 upper_bound: 2063 repeats: 1\n"+
			"num: 12 lower_bound: 2070 upper_bound: 2075 repeats: 1\n"+
			"num: 12 lower_bound: 2086 upper_bound: 2097 repeats: 1\n"+
			"num: 12 lower_bound: 2103 upper_bound: 2112 repeats: 1\n"+
			"num: 12 lower_bound: 2113 upper_bound: 2124 repeats: 1\n"+
			"num: 12 lower_bound: 2125 upper_bound: 2132 repeats: 1\n"+
			"num: 12 lower_bound: 2133 upper_bound: 2152 repeats: 1\n"+
			"num: 12 lower_bound: 2155 upper_bound: 2163 repeats: 1\n"+
			"num: 12 lower_bound: 2166 upper_bound: 2187 repeats: 1\n"+
			"num: 12 lower_bound: 2188 upper_bound: 2200 repeats: 1\n"+
			"num: 12 lower_bound: 2201 upper_bound: 2207 repeats: 1\n"+
			"num: 12 lower_bound: 2209 upper_bound: 2212 repeats: 1\n"+
			"num: 12 lower_bound: 2216 upper_bound: 2220 repeats: 1\n"+
			"num: 12 lower_bound: 2225 upper_bound: 2230 repeats: 1\n"+
			"num: 12 lower_bound: 2238 upper_bound: 2244 repeats: 1\n"+
			"num: 12 lower_bound: 2246 upper_bound: 2257 repeats: 1\n"+
			"num: 12 lower_bound: 2260 upper_bound: 2269 repeats: 1\n"+
			"num: 12 lower_bound: 2270 upper_bound: 2274 repeats: 1\n"+
			"num: 12 lower_bound: 2275 upper_bound: 2281 repeats: 1\n"+
			"num: 12 lower_bound: 2286 upper_bound: 2296 repeats: 1\n"+
			"num: 12 lower_bound: 2302 upper_bound: 2307 repeats: 1\n"+
			"num: 12 lower_bound: 2310 upper_bound: 2321 repeats: 1\n"+
			"num: 12 lower_bound: 2322 upper_bound: 2331 repeats: 1\n"+
			"num: 12 lower_bound: 2333 upper_bound: 2346 repeats: 1\n"+
			"num: 12 lower_bound: 2348 upper_bound: 2355 repeats: 1\n"+
			"num: 12 lower_bound: 2362 upper_bound: 2380 repeats: 1\n"+
			"num: 12 lower_bound: 2381 upper_bound: 2386 repeats: 1\n"+
			"num: 12 lower_bound: 2388 upper_bound: 2403 repeats: 1\n"+
			"num: 12 lower_bound: 2408 upper_bound: 2427 repeats: 1\n"+
			"num: 12 lower_bound: 2428 upper_bound: 2433 repeats: 1\n"+
			"num: 12 lower_bound: 2434 upper_bound: 2454 repeats: 1\n"+
			"num: 12 lower_bound: 2457 upper_bound: 2463 repeats: 1\n"+
			"num: 12 lower_bound: 2464 upper_bound: 2476 repeats: 1\n"+
			"num: 12 lower_bound: 2477 upper_bound: 2485 repeats: 1\n"+
			"num: 12 lower_bound: 2486 upper_bound: 2494 repeats: 1\n"+
			"num: 12 lower_bound: 2496 upper_bound: 2499 repeats: 1\n"+
			"num: 12 lower_bound: 2507 upper_bound: 2518 repeats: 1\n"+
			"num: 12 lower_bound: 2527 upper_bound: 2541 repeats: 1\n"+
			"num: 12 lower_bound: 2547 upper_bound: 2556 repeats: 1\n"+
			"num: 12 lower_bound: 2557 upper_bound: 2565 repeats: 1\n"+
			"num: 12 lower_bound: 2566 upper_bound: 2570 repeats: 1\n"+
			"num: 12 lower_bound: 2571 upper_bound: 2577 repeats: 1\n"+
			"num: 12 lower_bound: 2580 upper_bound: 2590 repeats: 1\n"+
			"num: 12 lower_bound: 2597 upper_bound: 2602 repeats: 1\n"+
			"num: 12 lower_bound: 2606 upper_bound: 2611 repeats: 1\n"+
			"num: 12 lower_bound: 2613 upper_bound: 2624 repeats: 1\n"+
			"num: 12 lower_bound: 2626 upper_bound: 2638 repeats: 1\n"+
			"num: 12 lower_bound: 2641 upper_bound: 2658 repeats: 1\n"+
			"num: 12 lower_bound: 2659 upper_bound: 2665 repeats: 1\n"+
			"num: 12 lower_bound: 2671 upper_bound: 2685 repeats: 1\n"+
			"num: 12 lower_bound: 2686 upper_bound: 2700 repeats: 1\n"+
			"num: 12 lower_bound: 2707 upper_bound: 2717 repeats: 1\n"+
			"num: 12 lower_bound: 2719 upper_bound: 2730 repeats: 1\n"+
			"num: 12 lower_bound: 2731 upper_bound: 2734 repeats: 1\n"+
			"num: 12 lower_bound: 2736 upper_bound: 2742 repeats: 1\n"+
			"num: 12 lower_bound: 2746 upper_bound: 2762 repeats: 1\n"+
			"num: 12 lower_bound: 2764 upper_bound: 2772 repeats: 1\n"+
			"num: 12 lower_bound: 2773 upper_bound: 2784 repeats: 1\n"+
			"num: 12 lower_bound: 2786 upper_bound: 2797 repeats: 1\n"+
			"num: 12 lower_bound: 2798 upper_bound: 2806 repeats: 1\n"+
			"num: 12 lower_bound: 2810 upper_bound: 2815 repeats: 1\n"+
			"num: 12 lower_bound: 2817 upper_bound: 2832 repeats: 1\n"+
			"num: 12 lower_bound: 2833 upper_bound: 2845 repeats: 1\n"+
			"num: 12 lower_bound: 2846 upper_bound: 2853 repeats: 1\n"+
			"num: 12 lower_bound: 2854 upper_bound: 2865 repeats: 1\n"+
			"num: 12 lower_bound: 2868 upper_bound: 2875 repeats: 1\n"+
			"num: 12 lower_bound: 2877 upper_bound: 2883 repeats: 1\n"+
			"num: 12 lower_bound: 2890 upper_bound: 2897 repeats: 1\n"+
			"num: 12 lower_bound: 2899 upper_bound: 2907 repeats: 1\n"+
			"num: 12 lower_bound: 2912 upper_bound: 2916 repeats: 1\n"+
			"num: 12 lower_bound: 2917 upper_bound: 2927 repeats: 1\n"+
			"num: 12 lower_bound: 2928 upper_bound: 2939 repeats: 1\n"+
			"num: 12 lower_bound: 2944 upper_bound: 2964 repeats: 1\n"+
			"num: 12 lower_bound: 2966 upper_bound: 2973 repeats: 1\n"+
			"num: 12 lower_bound: 2974 upper_bound: 2984 repeats: 1\n"+
			"num: 12 lower_bound: 2985 upper_bound: 2992 repeats: 1\n")

	tk.MustExec("set @@session.tidb_enable_fast_analyze=0")
}

func (s *testSuite1) TestAnalyzeTooLongColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", mysql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("analyze table t")
	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 0)
	c.Assert(tbl.Columns[1].TotColSize, Equals, int64(65559))
}
