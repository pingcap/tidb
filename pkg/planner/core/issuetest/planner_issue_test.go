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

func TestIssue(t *testing.T) {
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
(-2124506745,'[7854852970062856851, 485712809120497816]','m9dYdi-QwRg#*o*A3Ip',')u\$3a$dfL4rc',8833.722),
(-1987900253,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','rA0yD=c3(zA#Tx4Me=%','5=-l6',8261.812),
(-1801039543,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','q','5=-l6',2249.8162),
(-1801039543,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','qNiB(j^v=9#m)5I','5=-l6',8015.3647),
(-1555981914,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','wREM','5=-l6',7211.5654),
(-1512956812,'[12237306431704586590, 15033675751746115877, 17890410345510922576]','YKec~g','pvNYq!TYOu',4839.073),
(-1427641173,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','k^j_b','5=-l6',1043.4517),
(-1241352378,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','xJnURwQG!s9If','5=-l6',1577.0089),
(-1137801237,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','q!8&+','5=-l6',6956.594),
(-1135860082,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','mFIwt_-nw^+=xzhFO7','5=-l6',9747.357),
(-833071871,'[3671565389575202116]','s(W6=&fu4+@@O1Dh4P=','B',6504.2065),
(-401167891,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','~3xV&h^)ZNPTlC','5=-l6',3470.9688),
(-385141748,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','oc','5=-l6',4505.947),
(-369638048,'[6692908082570525008, 11282158432044887632]','7CLPayG','!%u0*yeZTW#+OhWEc+',4696.878),
(-316801815,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','oMhb_vrKJTv!BIm0lNS','5=-l6',677.98785),
(-155320341,'[15037175073412781918, 6436057204829237547]','9DhQlP=','Ild(5Ft4C!^zrk@7O=F',2871.2407),
(-129161234,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','n3qVghjnkx*#zy9AaV','5=-l6',8387.211),
(154916136,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','mkJsR','5=-l6',5501.6943),
(241213743,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','xyaR%xp5m5g-H','5=-l6',8753.599),
(297737358,'[10881785351205027060, 10968098726351092777, 13447444998800967333, 10787113729586840005, 18164090871369150855]','ZZbM','G&L*a%V1',9692.497),
(361364887,'[13571935304690400155, 15428459843371141639, 5166067369409479765, 13376215633079336076, 14918734709157485711]','nYpg','K3&a9W^sbyS=nH~',NULL),
(461116246,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','m','5=-l6',4245.67),
(690428690,'[13874302886417823206, 5106557973216105875, 4500765527154768391, 698879997138293585, 4238860526309864992]','fZxMsQ','rb6R5NF',NULL),
(787126218,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','oh4fN@M_bk4#s&N','5=-l6',8638.904),
(1955454898,'[4784139917445341210, 2409847733994134699, 10707724437804402980, 6577977165263525461]','llE0aPj@+N_#k@Q','5=-l6',798.27545);`)

	tk.MustExec(`CREATE TABLE ta0c4280a (
  col_64 time NOT NULL,
  KEY idx_25 (col_64),
  UNIQUE KEY idx_26 (col_64),
  PRIMARY KEY (col_64) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_bin;`)

	tk.MustExec(`INSERT INTO ta0c4280a VALUES
('00:00:54'),('00:07:16'),('00:13:36'),('00:19:54'),('00:22:22'),('00:22:47'),('00:40:24'),('00:41:42'),('00:42:17'),
('00:45:20'),('00:47:50'),('01:26:40'),('01:47:38'),('01:49:41'),('01:56:35'),('01:59:55'),('01:59:58'),('02:01:25'),
('02:12:23'),('02:12:28'),('02:26:30'),('02:32:16'),('02:50:53'),('02:55:45'),('03:01:28'),('03:09:41'),('03:10:02'),
('03:18:21'),('03:22:04'),('03:24:53'),('03:32:52'),('04:15:04'),('04:39:11'),('04:44:55'),('04:48:13'),('04:50:28'),
('05:27:20'),('05:35:49'),('05:40:36'),('05:44:35'),('05:47:27'),('05:53:50'),('05:57:15'),('06:00:53'),('06:05:09'),
('06:23:28'),('06:31:53'),('06:48:26'),('06:50:34'),('06:58:10'),('06:58:34'),('07:02:03'),('07:10:37'),('07:19:28'),
('07:22:30'),('07:44:38'),('07:47:35'),('07:52:10'),('08:06:39'),('08:12:58'),('08:18:35'),('08:33:16'),('08:37:23'),
('08:44:13'),('08:46:17'),('08:47:15'),('08:58:06'),('09:02:19'),('09:10:07'),('09:13:38'),('09:18:38'),('09:41:40'),
('10:11:05'),('10:31:32'),('10:31:59'),('10:42:32'),('10:43:47'),('10:48:58'),('10:49:19'),('11:02:52'),('11:14:33'),
('11:20:24'),('11:26:11'),('11:27:17'),('11:29:45'),('11:39:55'),('11:41:01'),('11:45:15'),('11:46:21'),('11:48:16'),
('12:05:01'),('12:05:42'),('12:13:23'),('12:18:18'),('12:18:23'),('12:21:06'),('12:21:11'),('12:36:24'),('12:36:35'),
('12:40:39'),('12:47:46'),('12:48:36'),('12:52:06'),('12:53:30'),('13:06:13'),('13:07:50'),('13:10:11'),('13:11:11'),
('13:12:46'),('13:24:08'),('13:26:34'),('13:35:50'),('13:38:14'),('13:40:34'),('13:42:23'),('13:42:47'),('13:50:32'),
('13:51:59'),('14:00:39'),('14:01:32'),('14:14:22'),('14:32:35'),('14:36:32'),('14:36:59'),('14:38:41'),('14:41:52'),
('14:42:17'),('15:02:46'),('15:04:34'),('15:17:59'),('15:18:07'),('15:36:06'),('15:36:20'),('15:44:23'),('15:46:15'),
('15:51:33'),('15:59:15'),('16:04:02'),('16:22:21'),('16:37:16'),('16:42:40'),('16:47:08'),('17:03:05'),('17:09:33'),
('17:15:36'),('17:22:57'),('17:37:49'),('17:38:09'),('17:44:14'),('17:46:30'),('17:48:38'),('17:50:30'),('18:05:08'),
('18:17:02'),('18:20:07'),('18:31:48'),('18:37:57'),('18:42:36'),('18:52:06'),('18:55:54'),('18:57:08'),('19:07:16'),
('19:09:46'),('19:22:10'),('19:24:52'),('19:30:43'),('19:33:07'),('19:35:00'),('19:39:36'),('19:49:19'),('20:04:28'),
('20:18:42'),('20:24:42'),('20:36:50'),('20:36:52'),('20:37:39'),('20:41:41'),('21:11:41'),('21:15:56'),('21:28:25'),
('21:32:26'),('21:38:45'),('21:45:56'),('21:59:31'),('22:02:33'),('22:08:19'),('22:13:24'),('22:26:54'),('22:31:52'),
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
`).Check(testkit.Rows())
}
