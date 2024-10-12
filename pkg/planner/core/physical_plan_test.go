// Copyright 2017 PingCAP, Inc.
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

package core_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeBuildSucc(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tests := []struct {
		sql      string
		succ     bool
		statsVer int
	}{
		{
			sql:      "analyze table t with 0.1 samplerate",
			succ:     true,
			statsVer: 2,
		},
		{
			sql:      "analyze table t with 0.1 samplerate",
			succ:     false,
			statsVer: 1,
		},
		{
			sql:      "analyze table t with 10 samplerate",
			succ:     false,
			statsVer: 2,
		},
		{
			sql:      "analyze table t with 0.1 samplerate, 100000 samples",
			succ:     false,
			statsVer: 2,
		},
		{
			sql:      "analyze table t with 0.1 samplerate, 100000 samples",
			succ:     false,
			statsVer: 1,
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range tests {
		comment := fmt.Sprintf("The %v-th test failed", i)
		tk.MustExec(fmt.Sprintf("set @@tidb_analyze_version=%v", tt.statsVer))

		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		if tt.succ {
			require.NoError(t, err, comment)
		} else if err != nil {
			continue
		}
		nodeW := resolve.NewNodeW(stmt)
		err = core.Preprocess(context.Background(), tk.Session(), nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		_, _, err = planner.Optimize(context.Background(), tk.Session(), nodeW, is)
		if tt.succ {
			require.NoError(t, err, comment)
		} else {
			require.Error(t, err, comment)
		}
	}
}

func TestAnalyzeSetRate(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tests := []struct {
		sql  string
		rate float64
	}{
		{
			sql:  "analyze table t",
			rate: -1,
		},
		{
			sql:  "analyze table t with 0.1 samplerate",
			rate: 0.1,
		},
		{
			sql:  "analyze table t with 10000 samples",
			rate: -1,
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range tests {
		comment := fmt.Sprintf("The %v-th test failed", i)
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		err = core.Preprocess(context.Background(), tk.Session(), nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err, comment)
		p, _, err := planner.Optimize(context.Background(), tk.Session(), nodeW, is)
		require.NoError(t, err, comment)
		ana := p.(*core.Analyze)
		require.Equal(t, tt.rate, math.Float64frombits(ana.Opts[ast.AnalyzeOptSampleRate]))
	}
}

type overrideStore struct{ kv.Storage }

func (store overrideStore) GetClient() kv.Client {
	cli := store.Storage.GetClient()
	return overrideClient{cli}
}

type overrideClient struct{ kv.Client }

func (cli overrideClient) IsRequestTypeSupported(_, _ int64) bool {
	return false
}

func TestRequestTypeSupportedOff(t *testing.T) {
	store := testkit.CreateMockStore(t)
	se, err := session.CreateSession4Test(overrideStore{store})
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "use test")
	require.NoError(t, err)

	sql := "select * from t where a in (1, 10, 20)"
	expect := "TableReader(Table(t))->Sel([in(test.t.a, 1, 10, 20)])"

	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	nodeW := resolve.NewNodeW(stmt)
	p, _, err := planner.Optimize(context.TODO(), se, nodeW, is)
	require.NoError(t, err)
	require.Equal(t, expect, core.ToString(p), fmt.Sprintf("sql: %s", sql))
}

func TestDoSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "do 1 in (select a from t)",
			best: "LeftHashJoin{Dual->PointGet(Handle(t.a)1)}->Projection",
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for _, tt := range tests {
		comment := fmt.Sprintf("for %s", tt.sql)
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
		require.NoError(t, err)
		require.Equal(t, tt.best, core.ToString(p), comment)
	}
}

func TestIndexLookupCartesianJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	stmt, err := parser.New().ParseOneStmt("select /*+ TIDB_INLJ(t1, t2) */ * from t t1 join t t2", "", "")
	require.NoError(t, err)

	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	nodeW := resolve.NewNodeW(stmt)
	p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
	require.NoError(t, err)
	require.Equal(t, "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}", core.ToString(p))

	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	err = plannererrors.ErrInternal.GenWithStack("TIDB_INLJ hint is inapplicable without column equal ON condition")
	require.True(t, terror.ErrorEqual(err, lastWarn.Err))
}

func TestMPPHintsWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("explain select a, sum(b) from t group by a, c")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("create global binding for select a, sum(b) from t group by a, c using select /*+ read_from_storage(tiflash[t]), MPP_1PHASE_AGG() */ a, sum(b) from t group by a, c;")
	tk.MustExec("explain select a, sum(b) from t group by a, c")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select `a` , sum ( `b` ) from `test` . `t` group by `a` , `c`")
	require.Equal(t, res[0][1], "SELECT /*+ read_from_storage(tiflash[`t`]) MPP_1PHASE_AGG()*/ `a`,sum(`b`) FROM `test`.`t` GROUP BY `a`,`c`")
	tk.MustExec("create global binding for select a, sum(b) from t group by a, c using select /*+ read_from_storage(tiflash[t]), MPP_2PHASE_AGG() */ a, sum(b) from t group by a, c;")
	tk.MustExec("explain select a, sum(b) from t group by a, c")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select `a` , sum ( `b` ) from `test` . `t` group by `a` , `c`")
	require.Equal(t, res[0][1], "SELECT /*+ read_from_storage(tiflash[`t`]) MPP_2PHASE_AGG()*/ `a`,sum(`b`) FROM `test`.`t` GROUP BY `a`,`c`")
	tk.MustExec("drop global binding for select a, sum(b) from t group by a, c;")
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)

	tk.MustExec("explain select * from t t1, t t2 where t1.a=t2.a")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("create global binding for select * from t t1, t t2 where t1.a=t2.a using select /*+ read_from_storage(tiflash[t1, t2]), shuffle_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a")
	tk.MustExec("explain select * from t t1, t t2 where t1.a=t2.a")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t` as `t1` ) join `test` . `t` as `t2` where `t1` . `a` = `t2` . `a`")
	require.Equal(t, res[0][1], "SELECT /*+ read_from_storage(tiflash[`t1`, `t2`]) shuffle_join(`t1`, `t2`)*/ * FROM (`test`.`t` AS `t1`) JOIN `test`.`t` AS `t2` WHERE `t1`.`a` = `t2`.`a`")
	tk.MustExec("create global binding for select * from t t1, t t2 where t1.a=t2.a using select /*+ read_from_storage(tiflash[t1, t2]), broadcast_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a;")
	tk.MustExec("explain select * from t t1, t t2 where t1.a=t2.a")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t` as `t1` ) join `test` . `t` as `t2` where `t1` . `a` = `t2` . `a`")
	require.Equal(t, res[0][1], "SELECT /*+ read_from_storage(tiflash[`t1`, `t2`]) broadcast_join(`t1`, `t2`)*/ * FROM (`test`.`t` AS `t1`) JOIN `test`.`t` AS `t2` WHERE `t1`.`a` = `t2`.`a`")
	tk.MustExec("drop global binding for select * from t t1, t t2 where t1.a=t2.a;")
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)
}

func TestJoinHintCompatibilityWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t (a int, b int, c int, index idx_a(a), index idx_b(b))")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("select * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("select /*+ leading(t2), hash_join(t2) */ * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("create global binding for select * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b using select /*+ leading(t2), hash_join(t2) */ * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	tk.MustExec("select * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t` as `t1` join `test` . `t` as `t2` ) join `test` . `t` as `t3` where `t1` . `a` = `t2` . `a` and `t2` . `b` = `t3` . `b`")
	require.Equal(t, res[0][1], "SELECT /*+ leading(`t2`) hash_join(`t2`)*/ * FROM (`test`.`t` AS `t1` JOIN `test`.`t` AS `t2`) JOIN `test`.`t` AS `t3` WHERE `t1`.`a` = `t2`.`a` AND `t2`.`b` = `t3`.`b`")
	tk.MustExec("select * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("drop global binding for select * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)
}

func TestJoinHintCompatibilityWithVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t (a int, b int, c int, index idx_a(a), index idx_b(b))")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("select /*+ leading(t2), hash_join(t2) */ * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("set @@session.tidb_opt_advanced_join_hint=0")
	tk.MustExec("select /*+ leading(t2), hash_join(t2) */ * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.b = t3.b;")
	res := tk.MustQuery("show warnings").Rows()
	require.Equal(t, len(res) > 0, true)
}

func TestHintAlias(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tests := []struct {
		sql1 string
		sql2 string
	}{
		{
			sql1: "select /*+ TIDB_SMJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_INLJ(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ MERGE_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ INL_JOIN(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ TIDB_HJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_SMJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ HASH_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ MERGE_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ TIDB_INLJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_HJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ INL_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ HASH_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
	}
	ctx := context.TODO()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql1:%s sql2:%s", i, tt.sql1, tt.sql2)
		stmt1, err := p.ParseOneStmt(tt.sql1, "", "")
		require.NoError(t, err, comment)
		stmt2, err := p.ParseOneStmt(tt.sql2, "", "")
		require.NoError(t, err, comment)

		nodeW1 := resolve.NewNodeW(stmt1)
		p1, _, err := planner.Optimize(ctx, tk.Session(), nodeW1, is)
		require.NoError(t, err)
		nodeW2 := resolve.NewNodeW(stmt2)
		p2, _, err := planner.Optimize(ctx, tk.Session(), nodeW2, is)
		require.NoError(t, err)

		require.Equal(t, core.ToString(p2), core.ToString(p1))
	}
}

func TestDAGPlanBuilderSplitAvg(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tests := []struct {
		sql  string
		plan string
	}{
		{
			sql:  "select avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select /*+ HASH_AGG() */ avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for _, tt := range tests {
		comment := fmt.Sprintf("for %s", tt.sql)
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		err = core.Preprocess(context.Background(), tk.Session(), nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
		require.NoError(t, err, comment)

		require.Equal(t, tt.plan, core.ToString(p), comment)
		root, ok := p.(base.PhysicalPlan)
		if !ok {
			continue
		}
		testDAGPlanBuilderSplitAvg(t, root)
	}
}

func testDAGPlanBuilderSplitAvg(t *testing.T, root base.PhysicalPlan) {
	if p, ok := root.(*core.PhysicalTableReader); ok {
		if p.TablePlans != nil {
			baseAgg := p.TablePlans[len(p.TablePlans)-1]
			if agg, ok := baseAgg.(*core.PhysicalHashAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					require.Equal(t, aggfunc.RetTp, agg.Schema().Columns[i].RetType)
				}
			}
			if agg, ok := baseAgg.(*core.PhysicalStreamAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					require.Equal(t, aggfunc.RetTp, agg.Schema().Columns[i].RetType)
				}
			}
		}
	}

	childs := root.Children()
	if childs == nil {
		return
	}
	for _, son := range childs {
		testDAGPlanBuilderSplitAvg(t, son)
	}
}

func TestPhysicalPlanMemoryTrace(t *testing.T) {
	// PhysicalSort
	ls := core.PhysicalSort{}
	size := ls.MemoryUsage()
	ls.ByItems = append(ls.ByItems, &util.ByItems{})
	require.Greater(t, ls.MemoryUsage(), size)

	// PhysicalProperty
	pp := property.PhysicalProperty{}
	size = pp.MemoryUsage()
	pp.MPPPartitionCols = append(pp.MPPPartitionCols, &property.MPPPartitionColumn{})
	require.Greater(t, pp.MemoryUsage(), size)
}

func TestPhysicalTableScanExtractCorrelatedCols(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int, client_type tinyint, client_no char(18), taxpayer_no varchar(50), status tinyint, update_time datetime)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t1")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("create table t2 (id int, company_no char(18), name varchar(200), tax_registry_no varchar(30))")
	tk.MustExec("insert into t1(id, taxpayer_no, client_no, client_type, status, update_time) values (1, 'TAX001', 'Z9005', 1, 1, '2024-02-18 10:00:00'), (2, 'TAX002', 'Z9005', 1, 0, '2024-02-18 09:00:00'), (3, 'TAX003', 'Z9005', 2, 1, '2024-02-18 08:00:00'), (4, 'TAX004', 'Z9006', 1, 1, '2024-02-18 12:00:00')")
	tk.MustExec("insert into t2(id, company_no, name, tax_registry_no) values (1, 'Z9005', 'AA', 'aaa'), (2, 'Z9006', 'BB', 'bbb'), (3, 'Z9007', 'CC', 'ccc')")

	sql := "select company_no, ifnull((select /*+ read_from_storage(tiflash[test.t1]) */ taxpayer_no from test.t1 where client_no = c.company_no and client_type = 1 and status = 1 order by update_time desc limit 1), tax_registry_no) as tax_registry_no from test.t2 c where company_no = 'Z9005' limit 1"
	tk.MustExec(sql)
	info := tk.Session().ShowProcess()
	require.NotNil(t, info)
	p, ok := info.Plan.(base.Plan)
	require.True(t, ok)

	var findSelection func(p base.Plan) *core.PhysicalSelection
	findSelection = func(p base.Plan) *core.PhysicalSelection {
		if p == nil {
			return nil
		}
		switch v := p.(type) {
		case *core.PhysicalSelection:
			if len(v.Children()) == 1 {
				if ts, ok := v.Children()[0].(*core.PhysicalTableScan); ok && ts.Table.Name.L == "t1" {
					return v
				}
			}
			return nil
		case *core.PhysicalTableReader:
			for _, child := range v.TablePlans {
				if sel := findSelection(child); sel != nil {
					return sel
				}
			}
			return nil
		default:
			physicayPlan := p.(base.PhysicalPlan)
			for _, child := range physicayPlan.Children() {
				if sel := findSelection(child); sel != nil {
					return sel
				}
			}
			return nil
		}
	}
	sel := findSelection(p)
	require.NotNil(t, sel)
	ts := sel.Children()[0].(*core.PhysicalTableScan)
	require.NotNil(t, ts)
	// manually push down the condition `client_no = c.company_no`
	var selected expression.Expression
	for _, cond := range sel.Conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.Function.PbCode() == tipb.ScalarFuncSig_EQString {
			selected = cond
			break
		}
	}
	if selected != nil {
		core.PushedDown(sel, ts, []expression.Expression{selected}, 0.1)
	}

	pb, err := ts.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	// make sure the pushed down filter condition is correct
	require.Equal(t, 1, len(pb.TblScan.PushedDownFilterConditions))
	require.Equal(t, tipb.ExprType_ColumnRef, pb.TblScan.PushedDownFilterConditions[0].Children[0].Tp)
	// make sure the correlated columns are extracted correctly
	correlated := ts.ExtractCorrelatedCols()
	require.Equal(t, 1, len(correlated))
	require.Equal(t, "test.t2.company_no", correlated[0].StringWithCtx(tk.Session().GetExprCtx().GetEvalCtx(), errors.RedactLogDisable))
}

func TestAvoidColumnEvaluatorForProjBelowUnion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	getPhysicalPlan := func(sql string) base.Plan {
		tk.MustExec(sql)
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		return p
	}

	var findProjBelowUnion func(p base.Plan) (projsBelowUnion, normalProjs []*core.PhysicalProjection)
	findProjBelowUnion = func(p base.Plan) (projsBelowUnion, normalProjs []*core.PhysicalProjection) {
		if p == nil {
			return projsBelowUnion, normalProjs
		}
		switch v := p.(type) {
		case *core.PhysicalUnionAll:
			for _, child := range v.Children() {
				if proj, ok := child.(*core.PhysicalProjection); ok {
					projsBelowUnion = append(projsBelowUnion, proj)
				}
			}
		default:
			for _, child := range p.(base.PhysicalPlan).Children() {
				if proj, ok := child.(*core.PhysicalProjection); ok {
					normalProjs = append(normalProjs, proj)
				}
				subProjsBelowUnion, subNormalProjs := findProjBelowUnion(child)
				projsBelowUnion = append(projsBelowUnion, subProjsBelowUnion...)
				normalProjs = append(normalProjs, subNormalProjs...)
			}
		}
		return projsBelowUnion, normalProjs
	}

	checkResult := func(sql string) {
		p := getPhysicalPlan(sql)
		projsBelowUnion, normalProjs := findProjBelowUnion(p)
		if proj, ok := p.(*core.PhysicalProjection); ok {
			normalProjs = append(normalProjs, proj)
		}
		require.NotEmpty(t, projsBelowUnion)
		for _, proj := range projsBelowUnion {
			require.True(t, proj.AvoidColumnEvaluator)
		}
		for _, proj := range normalProjs {
			require.False(t, proj.AvoidColumnEvaluator)
		}
	}

	// Test setup
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1 (cc1 int, cc2 text);`)
	tk.MustExec(`insert into t1 values (1, 'aaaa'), (2, 'bbbb'), (3, 'cccc');`)
	tk.MustExec(`create table t2 (cc1 int, cc2 text, primary key(cc1));`)
	tk.MustExec(`insert into t2 values (2, '2');`)
	tk.MustExec(`set tidb_executor_concurrency = 1;`)
	tk.MustExec(`set tidb_window_concurrency = 100;`)

	testCases := []string{
		`select * from (SELECT DISTINCT cc2 as a, cc2 as b, cc1 as c FROM t2 UNION ALL SELECT count(1) over (partition by cc1), cc2, cc1 FROM t1) order by a, b, c;`,
		`select a+1, b+1 from (select cc1 as a, cc2 as b from t1 union select cc2, cc1 from t1) tmp`,
	}

	for _, sql := range testCases {
		checkResult(sql)
	}
}
