// Copyright 2022 PingCAP, Inc.
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

package issuetest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(base.PhysicalPlan).Children()[0]
	}
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*core.PhysicalIndexScan)
	ts := idxLookUpPlan.TablePlans[0].(*core.PhysicalTableScan)

	require.NotEqual(t, is.Columns, ts.Columns)
}

func Test53726(t *testing.T) {
	// test for RemoveUnnecessaryFirstRow
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t7(c int); ")
	tk.MustExec("insert into t7 values (575932053), (-258025139);")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_8 8000.00 root  group by:Column#7, Column#8, funcs:firstrow(Column#7)->Column#3, funcs:firstrow(Column#8)->Column#4",
			"└─TableReader_9 8000.00 root  data:HashAgg_4",
			"  └─HashAgg_4 8000.00 cop[tikv]  group by:cast(test.t7.c, bigint(22) BINARY), cast(test.t7.c, decimal(10,0) BINARY), ",
			"    └─TableFullScan_7 10000.00 cop[tikv] table:t7 keep order:false, stats:pseudo"))

	tk.MustExec("analyze table t7")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#3, funcs:firstrow(Column#12)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#11, cast(test.t7.c, bigint(22) BINARY)->Column#12",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}

func TestIssue53857(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t9172e9ec (
  col_1 int(11) NOT NULL,
  col_2 json NOT NULL,
  col_3 char(119) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '1Fj!VQi4iQ)^PgL_+l',
  col_4 varchar(286) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'cU',
  col_5 float DEFAULT NULL,
  PRIMARY KEY (col_1,col_3) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY HASH (col_1) PARTITIONS 3;`)

	tk.MustExec(`INSERT INTO t9172e9ec VALUES
(1955454898,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','llE0aPj@+N_#k@Q','5=-l6',798.27545);`)

	tk.MustExec(`CREATE TABLE ta0c4280a (
  col_64 time NOT NULL,
  KEY idx_25 (col_64),
  UNIQUE KEY idx_26 (col_64),
  PRIMARY KEY (col_64) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_bin;`)

	tk.MustExec(`INSERT INTO ta0c4280a VALUES
('00:00:54'),('00:07:16'),('00:13:36'),('00:19:54'),('00:22:22'),('00:22:47'),('00:40:24'),('00:41:42'),('00:42:17'),
('22:54:18'),('22:56:38'),('23:22:58'),('23:32:42'),('23:41:17');`)

	tk.MustExec(`CREATE TABLE tld1ce2472 (
  col_52 json NOT NULL,
  col_53 date NOT NULL,
  col_54 datetime NOT NULL,
  col_55 json NOT NULL,
  col_56 datetime NOT NULL DEFAULT '2030-11-15 00:00:00',
  col_57 json NOT NULL,
  PRIMARY KEY (col_54,col_56) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;`)

	tk.MustExec(`INSERT INTO tld1ce2472 VALUES
('[\"vH8q1l9oIVwhmVpk8lFmxxzzGGLdKZ5ndMulSB8dzxw09VkwI8EWFv0vrXvHCbui\", \"S3bmEa6qMrBXZiOrfBaWG5EU6ma8RUyQ6TOxlpaA4WXQbunIfW5Dl7fPLGDzz9Ac\", \"dwxTL5ScIMJccTK8HE42NFkY5oGNXZD7yYQtaIYQIhr5h2XaZoGGxq8odXELEg8S\", \"zEbPnyABCd5r8TQlkaZnD3xuEc7ZdM6v4dEesbcTnVQKhwauQVEbFrTuobsj0eBR\"]', '1980-08-07', '2004-12-19 00:00:00', '[\"ghFazkT2xiNmjHey6pMCPzkQUnBl6ZDZNaJdkNOzxJgSLk1R1dnY49gfTHDvYqZI\", \"9oVaH4EPaXQ59PFEVuSjkNZzUWtWHpcwLIg4wIeIlWi13P7WotFfcjB2AwzYK10k\"]', '2008-09-21 00:00:00', '[0.8933399442803783, 0.2749574908374919, 0.2957384535419304, 0.18474097724774424, 0.7974148420776407]');`)
	tk.MustQuery(`
explain SELECT 1
FROM (
    SELECT RIGHT(ta0c4280a.col_64, 4) AS col_7284
    FROM ta0c4280a
    JOIN t9172e9ec
    WHERE ta0c4280a.col_64 IN ('09:41:40.00')
) AS cte_1466
JOIN (
    SELECT tld1ce2472.col_53 AS col_7282
    FROM tld1ce2472
) AS cte_1465
WHERE cte_1466.col_7284 BETWEEN '1f' AND '4f';
`).Check(testkit.Rows(
		"Projection_13 0.00 root  1->Column#15",
		"└─HashJoin_15 0.00 root  CARTESIAN inner join",
		"  ├─Projection_16(Build) 0.00 root  1->Column#19",
		"  │ └─TableDual_17 0.00 root  rows:0",
		"  └─Projection_18(Probe) 10000.00 root  1->Column#20",
		"    └─IndexReader_22 10000.00 root  index:IndexFullScan_21",
		"      └─IndexFullScan_21 10000.00 cop[tikv] table:tld1ce2472, index:PRIMARY(col_54, col_56) keep order:false, stats:pseudo",
	))
	tk.MustQuery(`
 SELECT 1
FROM (
    SELECT RIGHT(ta0c4280a.col_64, 4) AS col_7284
    FROM ta0c4280a
    JOIN t9172e9ec
    WHERE ta0c4280a.col_64 IN ('09:41:40.00')
) AS cte_1466
JOIN (
    SELECT tld1ce2472.col_53 AS col_7282
    FROM tld1ce2472
) AS cte_1465
WHERE cte_1466.col_7284 BETWEEN '1f' AND '4f';
`).Check(testkit.Rows())
}
