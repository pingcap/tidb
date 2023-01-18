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
	"fmt"
	"github.com/pingcap/tidb/util"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/require"
)

func TestInvalidRange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select * from t where a>? and a<?'")
	tk.MustExec("set @l=100, @r=10")
	tk.MustExec("execute st using @l, @r")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{{"TableDual_5"}}) // use TableDual directly instead of TableFullScan

	tk.MustExec("execute st using @l, @r")
	tk.MustExec("execute st using @l, @r")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue40093(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, b int, key(b, a))")
	tk.MustExec("prepare st from 'select * from t1 left join t2 on t1.a=t2.a where t2.b in (?)'")
	tk.MustExec("set @b=1")
	tk.MustExec("execute st using @b")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"Projection_9"},
			{"└─HashJoin_21"},
			{"  ├─IndexReader_26(Build)"},
			{"  │ └─IndexRangeScan_25"}, // RangeScan instead of FullScan
			{"  └─TableReader_24(Probe)"},
			{"    └─Selection_23"},
			{"      └─TableFullScan_22"},
		})

	tk.MustExec("execute st using @b")
	tk.MustExec("execute st using @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue38205(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `item` (`id` int, `vid` varbinary(16), `sid` int)")
	tk.MustExec("CREATE TABLE `lv` (`item_id` int, `sid` int, KEY (`sid`,`item_id`))")

	tk.MustExec("prepare stmt from 'SELECT /*+ TIDB_INLJ(lv, item) */ * FROM lv LEFT JOIN item ON lv.sid = item.sid AND lv.item_id = item.id WHERE item.sid = ? AND item.vid IN (?, ?)'")
	tk.MustExec("set @a=1, @b='1', @c='3'")
	tk.MustExec("execute stmt using @a, @b, @c")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"IndexJoin_10"},
			{"├─TableReader_19(Build)"},
			{"│ └─Selection_18"},
			{"│   └─TableFullScan_17"}, // RangeScan instead of FullScan
			{"└─IndexReader_9(Probe)"},
			{"  └─Selection_8"},
			{"    └─IndexRangeScan_7"},
		})

	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIgnoreInsertStmt(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	// do not cache native insert-stmt
	tk.MustExec("prepare st from 'insert into t values (1)'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// ignore-hint in insert-stmt can work
	tk.MustExec("prepare st from 'insert into t select * from t'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("prepare st from 'insert /*+ ignore_plan_cache() */ into t select * from t'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestCacheable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustExec("create table t3(a int, b int)")
	tbl := &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t3")}
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	// test non-SelectStmt/-InsertStmt/-DeleteStmt/-UpdateStmt/-SetOprStmt
	var stmt ast.Node = &ast.ShowStmt{}
	require.False(t, core.Cacheable(stmt, is))

	stmt = &ast.LoadDataStmt{}
	require.False(t, core.Cacheable(stmt, is))

	// test SetOprStmt
	stmt = &ast.SetOprStmt{}
	require.True(t, core.Cacheable(stmt, is))

	tableRefsClause := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: tbl}}}
	// test InsertStmt
	stmt = &ast.InsertStmt{Table: tableRefsClause} // insert-values-stmt
	require.False(t, core.Cacheable(stmt, is))
	stmt = &ast.InsertStmt{Table: tableRefsClause, Select: &ast.SelectStmt{}} // insert-select-stmt
	require.True(t, core.Cacheable(stmt, is))

	// test DeleteStmt
	whereExpr := &ast.FuncCallExpr{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	require.True(t, core.Cacheable(stmt, is))

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		require.False(t, core.Cacheable(stmt, is))
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	require.True(t, core.Cacheable(stmt, is))

	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt := &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	require.True(t, core.Cacheable(stmt, is))

	stmt.(*ast.DeleteStmt).TableHints = append(stmt.(*ast.DeleteStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
	})
	require.False(t, core.Cacheable(stmt, is))

	// test UpdateStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	require.True(t, core.Cacheable(stmt, is))

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		require.False(t, core.Cacheable(stmt, is))
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	require.True(t, core.Cacheable(stmt, is))

	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	require.True(t, core.Cacheable(stmt, is))

	stmt.(*ast.UpdateStmt).TableHints = append(stmt.(*ast.UpdateStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
	})
	require.False(t, core.Cacheable(stmt, is))

	// test SelectStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.SelectStmt{
		Where: whereExpr,
	}
	require.True(t, core.Cacheable(stmt, is))

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		require.False(t, core.Cacheable(stmt, is))
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	require.True(t, core.Cacheable(stmt, is))

	stmt = &ast.SelectStmt{
		Where: &ast.ExistsSubqueryExpr{},
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	require.False(t, core.Cacheable(stmt, is))

	limitStmt = &ast.Limit{}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	require.True(t, core.Cacheable(stmt, is))

	paramExpr := &driver.ParamMarkerExpr{}
	orderByClause := &ast.OrderByClause{Items: []*ast.ByItem{{Expr: paramExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	require.False(t, core.Cacheable(stmt, is))

	valExpr := &driver.ValueExpr{}
	orderByClause = &ast.OrderByClause{Items: []*ast.ByItem{{Expr: valExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	require.True(t, core.Cacheable(stmt, is))

	stmt.(*ast.SelectStmt).TableHints = append(stmt.(*ast.SelectStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
	})
	require.False(t, core.Cacheable(stmt, is))

	boundExpr := &ast.FrameBound{Expr: &driver.ParamMarkerExpr{}}
	require.False(t, core.Cacheable(boundExpr, is))

	// Partition table can not be cached.
	join := &ast.Join{
		Left:  &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t1")},
		Right: &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t2")},
	}
	stmt = &ast.SelectStmt{
		From: &ast.TableRefsClause{
			TableRefs: join,
		},
	}
	require.False(t, core.Cacheable(stmt, is))

	join = &ast.Join{
		Left: &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t3")},
	}
	stmt = &ast.SelectStmt{
		From: &ast.TableRefsClause{
			TableRefs: join,
		},
	}
	require.True(t, core.Cacheable(stmt, is))

}
