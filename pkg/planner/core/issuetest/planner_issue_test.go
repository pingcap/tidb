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
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
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

	nodeW := resolve.NewNodeW(stmt)
	p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, domain.InfoSchema())
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

	tk.MustExec("analyze table t7 all columns")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#13, Column#14, funcs:firstrow(Column#13)->Column#3, funcs:firstrow(Column#14)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#13, cast(test.t7.c, bigint(22) BINARY)->Column#14",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}

func TestIssue54535(t *testing.T) {
	// test for tidb_enable_inl_join_inner_multi_pattern system variable
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set session tidb_enable_inl_join_inner_multi_pattern='ON'")
	tk.MustExec("create table ta(a1 int, a2 int, a3 int, index idx_a(a1))")
	tk.MustExec("create table tb(b1 int, b2 int, b3 int, index idx_b(b1))")
	tk.MustExec("analyze table ta")
	tk.MustExec("analyze table tb")

	tk.MustQuery("explain SELECT /*+ inl_join(tmp) */ * FROM ta, (SELECT b1, COUNT(b3) AS cnt FROM tb GROUP BY b1, b2) as tmp where ta.a1 = tmp.b1").
		Check(testkit.Rows(
			"Projection_9 9990.00 root  test.ta.a1, test.ta.a2, test.ta.a3, test.tb.b1, Column#9",
			"└─IndexJoin_16 9990.00 root  inner join, inner:HashAgg_14, outer key:test.ta.a1, inner key:test.tb.b1, equal cond:eq(test.ta.a1, test.tb.b1)",
			"  ├─TableReader_43(Build) 9990.00 root  data:Selection_42",
			"  │ └─Selection_42 9990.00 cop[tikv]  not(isnull(test.ta.a1))",
			"  │   └─TableFullScan_41 10000.00 cop[tikv] table:ta keep order:false, stats:pseudo",
			"  └─HashAgg_14(Probe) 9990.00 root  group by:test.tb.b1, test.tb.b2, funcs:count(Column#11)->Column#9, funcs:firstrow(test.tb.b1)->test.tb.b1",
			"    └─IndexLookUp_15 9990.00 root  ",
			"      ├─Selection_12(Build) 9990.00 cop[tikv]  not(isnull(test.tb.b1))",
			"      │ └─IndexRangeScan_10 10000.00 cop[tikv] table:tb, index:idx_b(b1) range: decided by [eq(test.tb.b1, test.ta.a1)], keep order:false, stats:pseudo",
			"      └─HashAgg_13(Probe) 9990.00 cop[tikv]  group by:test.tb.b1, test.tb.b2, funcs:count(test.tb.b3)->Column#11",
			"        └─TableRowIDScan_11 9990.00 cop[tikv] table:tb keep order:false, stats:pseudo"))
	// test for issues/55169
	tk.MustExec("create table t1(col_1 int, index idx_1(col_1));")
	tk.MustExec("create table t2(col_1 int, col_2 int, index idx_2(col_1));")
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(distinct col_2 order by col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
}

func TestIssue53175(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`set @@sql_mode = default`)
	tk.MustQuery(`select @@sql_mode REGEXP 'ONLY_FULL_GROUP_BY'`).Check(testkit.Rows("1"))
	tk.MustContainErrMsg(`select * from t group by null`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`create view v as select * from t group by null`)
	tk.MustContainErrMsg(`select * from v`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustQuery(`select * from t group by null`)
	tk.MustQuery(`select * from v`)
}

func TestIssue58476(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE t3 (id int PRIMARY KEY,c1 varchar(256),c2 varchar(256) GENERATED ALWAYS AS (concat(c1, c1)) VIRTUAL,KEY (id));")
	tk.MustExec("insert into t3(id, c1) values (50, 'c');")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").Check(testkit.Rows())
	tk.MustQuery("explain format='brief' SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").
		Check(testkit.Rows(
			`Projection 249.75 root  test.t3.id`,
			`└─Selection 249.75 root  ge(test.t3.c2, "a"), le(test.t3.c2, "b")`,
			`  └─Projection 9990.00 root  test.t3.id, test.t3.c2`,
			`    └─IndexMerge 9990.00 root  type: union`,
			`      ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t3, index:id(id) range:[-inf,100), keep order:false, stats:pseudo`,
			`      ├─TableRangeScan(Build) 3333.33 cop[tikv] table:t3 range:(0,+inf], keep order:false, stats:pseudo`,
			`      └─TableRowIDScan(Probe) 9990.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
}

func TestIssue59643(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustQuery(`explain format='brief' SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;
`).Check(testkit.Rows(
		"HashJoin 2.00 root  inner join, equal:[nulleq(Column#5, Column#11)]",
		"├─HashAgg(Build) 2.00 root  group by:Column#5, Column#6, funcs:firstrow(Column#5)->Column#5, funcs:firstrow(Column#6)->Column#6",
		"│ └─Union 2.00 root  ",
		"│   ├─HashAgg 1.00 root  group by:1, funcs:firstrow(1)->Column#1, funcs:firstrow(\"Alice\")->Column#2",
		"│   │ └─TableDual 1.00 root  rows:1",
		"│   └─Projection 1.00 root  <nil>->Column#5, Bob->Column#6",
		"│     └─TableDual 1.00 root  rows:1",
		"└─HashAgg(Probe) 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#11, funcs:firstrow(Column#12)->Column#12",
		"  └─Union 2.00 root  ",
		"    ├─Projection 1.00 root  1->Column#11, 100->Column#12",
		"    │ └─TableDual 1.00 root  rows:1",
		"    └─Projection 1.00 root  <nil>->Column#11, <nil>->Column#12",
		"      └─TableDual 1.00 root  rows:1"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
}

func TestIssue58451(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1 (a1 int, b1 int);")
	tk.MustExec("create table t2 (a2 int, b2 int);")
	tk.MustExec("insert into t1 values(1,1);")
	tk.MustQuery(`explain format='brief'
SELECT (4,5) IN (SELECT 8,0 UNION SELECT 8, 8) AS field1
FROM t1 AS table1
WHERE (EXISTS (SELECT SUBQUERY2_t1.a1 AS SUBQUERY2_field1 FROM t1 AS SUBQUERY2_t1)) OR table1.b1 >= 55
GROUP BY field1;`).Check(testkit.Rows("HashJoin 2.00 root  CARTESIAN left outer semi join, left side:HashAgg",
		"├─HashAgg(Build) 2.00 root  group by:Column#18, Column#19, funcs:firstrow(1)->Column#45",
		"│ └─Union 0.00 root  ",
		"│   ├─Projection 0.00 root  8->Column#18, 0->Column#19",
		"│   │ └─TableDual 0.00 root  rows:0",
		"│   └─Projection 0.00 root  8->Column#18, 8->Column#19",
		"│     └─TableDual 0.00 root  rows:0",
		"└─HashAgg(Probe) 2.00 root  group by:Column#10, funcs:firstrow(1)->Column#42",
		"  └─HashJoin 10000.00 root  CARTESIAN left outer semi join, left side:TableReader",
		"    ├─HashAgg(Build) 2.00 root  group by:Column#8, Column#9, funcs:firstrow(1)->Column#44",
		"    │ └─Union 0.00 root  ",
		"    │   ├─Projection 0.00 root  8->Column#8, 0->Column#9",
		"    │   │ └─TableDual 0.00 root  rows:0",
		"    │   └─Projection 0.00 root  8->Column#8, 8->Column#9",
		"    │     └─TableDual 0.00 root  rows:0",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:table1 keep order:false, stats:pseudo"))
	tk.MustQuery(`SELECT (4,5) IN (SELECT 8,0 UNION SELECT 8, 8) AS field1
FROM t1 AS table1
WHERE (EXISTS (SELECT SUBQUERY2_t1.a1 AS SUBQUERY2_field1 FROM t1 AS SUBQUERY2_t1)) OR table1.b1 >= 55
GROUP BY field1;`).Check(testkit.Rows("0"))
}

func TestIssue59902(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(a int primary key, b int);")
	tk.MustExec("create table t2(a int, b int, key idx(a));")
	tk.MustExec("set tidb_enable_inl_join_inner_multi_pattern=on;")
	tk.MustQuery("explain format='brief' select t1.b,(select count(*) from t2 where t2.a=t1.a) as a from t1 where t1.a=1;").
		Check(testkit.Rows(
			"Projection 1.00 root  test.t1.b, ifnull(Column#9, 0)->Column#9",
			"└─IndexJoin 1.00 root  left outer join, inner:HashAgg, left side:Point_Get, outer key:test.t1.a, inner key:test.t2.a, equal cond:eq(test.t1.a, test.t2.a)",
			"  ├─Point_Get(Build) 1.00 root table:t1 handle:1",
			"  └─HashAgg(Probe) 1.00 root  group by:test.t2.a, funcs:count(Column#10)->Column#9, funcs:firstrow(test.t2.a)->test.t2.a",
			"    └─IndexReader 1.00 root  index:HashAgg",
			"      └─HashAgg 1.00 cop[tikv]  group by:test.t2.a, funcs:count(1)->Column#10",
			"        └─Selection 1.00 cop[tikv]  not(isnull(test.t2.a))",
			"          └─IndexRangeScan 1.00 cop[tikv] table:t2, index:idx(a) range: decided by [eq(test.t2.a, test.t1.a)], keep order:false, stats:pseudo"))
}

func TestIssueABC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`CREATE TABLE tcs_order_pay_detail (
  id bigint(12) NOT NULL AUTO_INCREMENT COMMENT '序列号，主键',
  order_pay_id bigint(12) NOT NULL COMMENT '外键，用于关联交易信息(t_order_pay)',
  uuid varchar(32) NOT NULL COMMENT '支付明细唯一标识',
  ref_uuid varchar(32) DEFAULT NULL COMMENT '关联订单明细Id',
  amount decimal(16,0) NOT NULL COMMENT '交易数量，金额',
  currency_code varchar(10) NOT NULL COMMENT '货币类型',
  pay_tool varchar(10) NOT NULL COMMENT '支付工具',
  card_index_no varchar(30) DEFAULT NULL COMMENT '银行卡索引号',
  branch_id varchar(30) DEFAULT NULL COMMENT '银行分支机构',
  inst_id varchar(30) DEFAULT NULL COMMENT '银行机构',
  channel_type varchar(4) DEFAULT NULL COMMENT '支付渠道类型',
  inst_acct_no varchar(30) DEFAULT NULL COMMENT '银行内部号',
  decision_id varchar(24) DEFAULT NULL COMMENT '路由决策号',
  ref_local_uuid varchar(24) DEFAULT NULL COMMENT '引用本地支付明细的uuid，主要用于退款、认购等交易',
  latest_status varchar(4) DEFAULT NULL COMMENT '最新状态（冗余，提升性能）',
  bank_date varchar(8) DEFAULT NULL COMMENT '银行返回日期',
  bank_time varchar(6) DEFAULT NULL COMMENT '银行返回时间',
  latest_responsion_id bigint(12) DEFAULT NULL COMMENT '最新返回结果ID，关联tcs_order_pay_detail_responsion的ID',
  create_time datetime NOT NULL COMMENT '支付明细创建时间',
  modify_time datetime DEFAULT NULL COMMENT '支付明细最后更新时间',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY idx_uk_tcs_order_pay_detail_uuid (uuid),
  KEY idx_tcs_order_pay_detail_roi (order_pay_id),
  KEY idx_bank_date (bank_date),
  KEY idx_recordidx_paytool (pay_tool)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=131053894 COMMENT='订单支付明细信息表'`)
	tk.MustExec(`CREATE TABLE tcs_order_pay (
  id bigint(12) NOT NULL AUTO_INCREMENT COMMENT '序列号，主键',
  order_id varchar(32) NOT NULL COMMENT '外键，用于关联交易信息(t_order)',
  ref_order_id varchar(32) DEFAULT NULL COMMENT '关联订单Id',
  uuid varchar(24) NOT NULL COMMENT '支付唯一标识',
  seqno int(4) NOT NULL COMMENT '支付批次序列号（支持分批支付），范围[1, n]',
  amount decimal(16,0) NOT NULL COMMENT '交易数量，金额',
  currency_code varchar(10) NOT NULL COMMENT '货币类型',
  card_index_no varchar(30) DEFAULT NULL COMMENT '银行卡索引号',
  branch_id varchar(30) DEFAULT NULL COMMENT '银行分支机构',
  inst_id varchar(30) DEFAULT NULL COMMENT '银行机构',
  hold_amount decimal(16,0) DEFAULT NULL COMMENT '冻结、解冻金额',
  hold_code varchar(30) DEFAULT NULL COMMENT '冻结、解冻唯一标识',
  flag varchar(10) DEFAULT NULL COMMENT '支付标识（内部使用）',
  batch_no varchar(32) DEFAULT NULL COMMENT '取现操作批次号',
  phase_zone varchar(20) DEFAULT NULL COMMENT '支付操作域编号',
  latest_status varchar(4) DEFAULT NULL COMMENT '最新状态（冗余，提升性能）',
  sms_sent smallint(1) DEFAULT NULL COMMENT '是否已经发送短信',
  wtimeout_num int(6) DEFAULT '0' COMMENT '取现超时次数(withdraw timeout number)，默认为0',
  trans_code varchar(3) DEFAULT NULL COMMENT '交易码，描述交易类型',
  sub_trans_code varchar(6) DEFAULT NULL COMMENT '子交易码，描述交易子类型',
  pay_product_id decimal(12,0) DEFAULT NULL COMMENT '用于支付的产品Id',
  order_pay_id bigint(12) DEFAULT NULL COMMENT '关联的支付单Id，一般用于支付有先后的多个支付单',
  latest_responsion_id bigint(12) DEFAULT NULL COMMENT '最新返回结果ID，关联tcs_order_pay_responsion的ID',
  data_type varchar(5) DEFAULT 'N' COMMENT '数据类型',
  create_time datetime NOT NULL COMMENT '支付创建时间',
  modify_time datetime NOT NULL COMMENT '支付最后更新时间',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY idx_uk_tcs_order_pay_uuid (uuid),
  KEY idx_tcs_order_pay_roi (order_id),
  KEY idx_tcs_order_pay_bn (batch_no),
  KEY idx_tcs_order_pay_ls_ct (latest_status,create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=129856698 COMMENT='订单支付信息表'`)
	tk.MustExec(`CREATE TABLE tcs_order (
  id varchar(32) NOT NULL COMMENT '序列号，主键',
  user_id varchar(24) NOT NULL COMMENT '用户id，关联用户',
  apply_date varchar(8) NOT NULL COMMENT '交易申请日期，格式：yyyymmdd',
  apply_time varchar(6) NOT NULL COMMENT '交易申请时间，格式：hh24miss',
  amount decimal(16,0) DEFAULT NULL COMMENT '交易金额',
  ack_amount decimal(16,0) DEFAULT NULL COMMENT '交易确认金额',
  principal decimal(16,0) DEFAULT NULL COMMENT '交易本金',
  currency_code varchar(10) DEFAULT NULL COMMENT '金额货币类型',
  quty decimal(16,0) DEFAULT NULL COMMENT '交易份额',
  ack_quty decimal(16,0) DEFAULT NULL COMMENT '交易确认份额',
  quty_scale int(5) DEFAULT NULL COMMENT '份额小数位',
  apply_cost_type varchar(2) DEFAULT NULL COMMENT '交易申请价值类型',
  ack_cost_type varchar(2) DEFAULT NULL COMMENT '交易确认价值类型',
  product_id decimal(20,0) NOT NULL COMMENT '产品id，关联产品信息',
  product_name varchar(100) NOT NULL COMMENT '产品名称',
  product_type varchar(10) DEFAULT NULL COMMENT '产品类型',
  to_product_id decimal(20,0) DEFAULT NULL COMMENT '目标产品Id',
  to_product_name varchar(100) DEFAULT NULL COMMENT '目标产品名称',
  to_product_type varchar(10) DEFAULT NULL COMMENT '目标产品类型',
  fund_id varchar(24) DEFAULT NULL COMMENT '基金代码',
  fund_type varchar(2) DEFAULT NULL COMMENT '基金类型，理财产品、股票类基金、混合基金、海外基金、指数基金、债券基金、货币基金',
  merchant_id varchar(24) NOT NULL COMMENT '商户id，用于关联产品所属商户',
  work_date varchar(8) NOT NULL COMMENT '交易单的工作日，格式：yyyymmdd',
  trans_code varchar(3) NOT NULL COMMENT '交易码，描述交易类型',
  sub_trans_code varchar(6) NOT NULL COMMENT '子交易码，描述交易子类型',
  bus_scen_code varchar(10) DEFAULT NULL COMMENT '业务场景码',
  source varchar(2) NOT NULL COMMENT '收单来源，例如internet、移动端等',
  branch_code varchar(5) NOT NULL COMMENT '收单机构，例如qgg直销、第三方收单、代销等等',
  fee_type varchar(2) DEFAULT NULL COMMENT '收费方式',
  to_fee_type varchar(2) DEFAULT NULL COMMENT '目标产品收费类型',
  large_red_flag varchar(2) DEFAULT NULL COMMENT '巨额赎回标识, 0表示取消，1表示顺延',
  remark varchar(2000) DEFAULT NULL COMMENT '可选的备注信息',
  asset_last_confirm int(1) DEFAULT NULL COMMENT '是否是资产交易最后通知，1表示是，0表示否',
  display varchar(14) DEFAULT '1' COMMENT '是否显示，1表示是，0表示否',
  melon_type varchar(4) DEFAULT NULL COMMENT '分红方式',
  trans_flag varchar(4) DEFAULT NULL COMMENT '交易标识（冗余，提升性能）',
  latest_status varchar(4) DEFAULT NULL COMMENT '最新状态（冗余，提升性能）',
  discount decimal(8,4) DEFAULT NULL COMMENT '手续费折扣',
  fee decimal(12,4) DEFAULT NULL COMMENT '手续费',
  ref_3th_id varchar(32) DEFAULT NULL COMMENT '外部关联号，例如定投协议号等等',
  extend_scen varchar(3) DEFAULT '' COMMENT '交易扩展场景',
  hint varchar(10) DEFAULT '0' COMMENT '交易提示，例如是否有关联交易等等',
  confirm_nav decimal(16,4) DEFAULT NULL COMMENT '确认净值',
  create_time datetime NOT NULL COMMENT '交易创建时间',
  modify_time datetime NOT NULL COMMENT '交易最后更新时间',
  latest_status_id bigint(12) DEFAULT NULL COMMENT '最新状态表Id（冗余，提升性能）',
  PRIMARY KEY (id) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_tcs_order_create_time (create_time),
  KEY idx_tcs_order_refId (ref_3th_id),
  KEY idx_lateststatus_createtime (latest_status,create_time),
  KEY idx_recordidx_prdid_subtscd_brhcd (product_id,sub_trans_code,branch_code),
  KEY idx_recordidx_bhcd_sbtscd_dsply (branch_code,sub_trans_code,display),
  KEY idx_recordidx_prdid_brhcd_wkdt (product_id,branch_code,work_date),
  KEY idx_usid_wkdt (user_id,work_date),
  KEY idx_usid_lststs (user_id,latest_status),
  KEY idx_usid_prdtid (user_id,product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='交易订单表，记录用户的订单信息'`)
	tk.MustQuery(`explain SELECT
    /*+ NO_DECORRELATE() */ 
	o.apply_date AS applyDate,
	o.apply_time AS applyTime,
	o.id,
	o.source,
	o.amount,
	o.product_id AS productId,
	o.product_name AS productName,
	o.product_type AS productType,
	o.fund_id AS fundId,
	o.fund_type AS fundType,
	o.trans_code AS transCode 
FROM
	tcs_order o 
WHERE
	o.id = (
	SELECT
	    /*+ NO_DECORRELATE() */ 
		x.id 
	FROM
		(
		SELECT
			f.id AS id,
			min(
			concat( bank_date, bank_time )) 
		FROM
			(
			SELECT
				t.bus_scen_code,
				t.display,
				ref_3th_id,
				t.id,
				bank_date,
				bank_time 
			FROM
				tcs_order t,
				tcs_order_pay tp,
				tcs_order_pay_detail opd 
			WHERE
				t.user_id = '517702019092500070178054' 
				AND t.trans_code IN ( '001', '002', '012' ) 
				AND t.sub_trans_code != '0110' 
				AND t.product_id NOT IN ( 600001, 8100666666 ) 
				AND t.display = '1' 
				AND t.id = tp.order_id 
				AND opd.order_pay_id = tp.id 
				AND tp.phase_zone = 'Recharge1_Resp' 
				AND tp.latest_status = 'S' 
				AND opd.pay_tool IN ( '11', '12', '13' ) 
			) f 
		GROUP BY
			f.id 
		ORDER BY
			concat( bank_date, bank_time ) ASC 
			LIMIT 0,
			1 
		) x 
	)`).Check(testkit.Rows())
}
