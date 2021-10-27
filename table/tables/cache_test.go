package tables_test

import (
	"github.com/pingcap/tidb/testkit"
	"testing"
)

func TestCacheTableBasicScan(t *testing.T) {
	t.Parallel()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create  table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values" +
		"(1, 101, 1001), (3, 113, 1003), (5, 105, 1005), (7, 117, 1007), (9, 109, 1009)," +
		"(10, 110, 1010), (12, 112, 1012), (14, 114, 1014), (16, 116, 1016), (18, 118, 1018)",
	)
	tk.MustExec("alter table tmp1 cache")
	assertSelect := func() {
		// For TableReader
		// First read will read from original table
		tk.MustQuery("select * from tmp1 where id>3 order by id").Check(testkit.Rows(
			"5 105 1005", "7 117 1007", "9 109 1009",
			"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
		))
		// Second read will from cache table
		result := tk.MustQuery("explain format = 'brief' select * from tmp1 where id>4 order by id")
		result.Check(testkit.Rows("UnionScan 3333.33 root  gt(test.tmp1.id, 4)",
			"└─TableReader 3333.33 root  data:TableRangeScan",
			"  └─TableRangeScan 3333.33 cop[tikv] table:tmp1 range:(4,+inf], keep order:true, stats:pseudo"))
		tk.MustQuery("select * from tmp1 where id>4 order by id").Check(testkit.Rows(
			"5 105 1005", "7 117 1007", "9 109 1009",
			"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
		))
		// For IndexLookUpReader
		result = tk.MustQuery("explain format = 'brief' select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u")
		result.Check(testkit.Rows("UnionScan 3333.33 root  gt(test.tmp1.u, 101)",
			"└─IndexLookUp 3333.33 root  ",
			"  ├─IndexRangeScan(Build) 3333.33 cop[tikv] table:tmp1, index:u(u) range:(101,+inf], keep order:true, stats:pseudo",
			"  └─TableRowIDScan(Probe) 3333.33 cop[tikv] table:tmp1 keep order:false, stats:pseudo"))
		tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u").Check(testkit.Rows(
			"5 105 1005", "9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ id,u from tmp1 where u>101 order by id").Check(testkit.Rows(
			"3 113", "5 105", "7 117", "9 109", "10 110",
			"12 112", "14 114", "16 116", "18 118",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexMerge, temporary table should not use index merge
		tk.MustQuery("select /*+ use_index_merge(tmp1, primary, u) */ * from tmp1 where id>5 or u>110 order by u").Check(testkit.Rows(
			"9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))

		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 IndexMerge is inapplicable or disabled"))
	}
	assertSelect()

}
