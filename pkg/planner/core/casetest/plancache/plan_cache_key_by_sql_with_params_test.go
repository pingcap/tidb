package plancache

import (
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestNewPlanCacheKeyBySQLWithParamsMatchesNewPlanCacheKeyWithLimitConstant(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=1")
	tk.MustExec("create table t (a int, key(a))")

	tk.MustExec("prepare st from 'select * from t where a = ? order by a limit 1'")
	tk.MustExec("set @a=1")
	tk.MustExec("execute st using @a")

	sctx := tk.Session().(sessionctx.Context)
	prepObj, err := sctx.GetSessionVars().GetPreparedStmtByName("st")
	if err != nil {
		t.Fatal(err)
	}
	stmt := prepObj.(*plannercore.PlanCacheStmt)

	key1, _, cacheable, reason, err := plannercore.NewPlanCacheKey(sctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if !cacheable {
		t.Fatalf("NewPlanCacheKey not cacheable: %s", reason)
	}

	isReadOnly := stmt.PreparedAst.IsReadOnly
	params := &plannercore.PlanCacheLookupParams{
		ParamSQL:            stmt.StmtText,
		SchemaVersion:       stmt.SchemaVersion,
		RelateVersion:       stmt.RelateVersion,
		LatestSchemaVersion: 0,
		IsReadOnly:          &isReadOnly,
		HasSubquery:         stmt.HasSubquery(),
		HasLimit:            stmt.HasLimit(),
		LimitValues:         stmt.GetLimitValues(),
		StatsVerHash:        0,
	}
	key2, cacheable, reason, err := plannercore.NewPlanCacheKeyBySQLWithParams(sctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if !cacheable {
		t.Fatalf("NewPlanCacheKeyBySQLWithParams not cacheable: %s", reason)
	}

	if key1 != key2 {
		t.Fatalf("plan cache keys mismatch\nNewPlanCacheKey: %q\nNewPlanCacheKeyBySQLWithParams: %q", key1, key2)
	}
}

func TestNewPlanCacheKeyBySQLWithParamsMatchesNewPlanCacheKeyWithLimitParam(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=1")
	tk.MustExec("create table t (a int, key(a))")

	tk.MustExec("prepare st from 'select * from t where a = ? order by a limit ?'")
	tk.MustExec("set @a=1, @l=10")
	tk.MustExec("execute st using @a, @l")

	sctx := tk.Session().(sessionctx.Context)
	prepObj, err := sctx.GetSessionVars().GetPreparedStmtByName("st")
	if err != nil {
		t.Fatal(err)
	}
	stmt := prepObj.(*plannercore.PlanCacheStmt)

	key1, _, cacheable, reason, err := plannercore.NewPlanCacheKey(sctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if !cacheable {
		t.Fatalf("NewPlanCacheKey not cacheable: %s", reason)
	}

	isReadOnly := stmt.PreparedAst.IsReadOnly
	params := &plannercore.PlanCacheLookupParams{
		ParamSQL:            stmt.StmtText,
		SchemaVersion:       stmt.SchemaVersion,
		RelateVersion:       stmt.RelateVersion,
		LatestSchemaVersion: 0,
		IsReadOnly:          &isReadOnly,
		HasSubquery:         stmt.HasSubquery(),
		HasLimit:            stmt.HasLimit(),
		LimitValues:         stmt.GetLimitValues(),
		StatsVerHash:        0,
	}
	key2, cacheable, reason, err := plannercore.NewPlanCacheKeyBySQLWithParams(sctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if !cacheable {
		t.Fatalf("NewPlanCacheKeyBySQLWithParams not cacheable: %s", reason)
	}

	if key1 != key2 {
		t.Fatalf("plan cache keys mismatch\nNewPlanCacheKey: %q\nNewPlanCacheKeyBySQLWithParams: %q", key1, key2)
	}
}

func TestNewPlanCacheKeyBySQLWithParamsMatchesNewPlanCacheKeyNoLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table customer (c_id int, c_d_id int, c_w_id int, c_discount int, c_last varchar(20), c_credit varchar(20), key(c_w_id, c_d_id, c_id))")
	tk.MustExec("create table warehouse (w_id int, w_tax int, key(w_id))")

	tk.MustExec("prepare st from 'select c_discount, c_last, c_credit, w_tax from customer, warehouse where w_id = ? and c_w_id = w_id and c_d_id = ? and c_id = ?'")
	tk.MustExec("set @w=1, @d=1, @c=1")
	tk.MustExec("execute st using @w, @d, @c")

	sctx := tk.Session().(sessionctx.Context)
	prepObj, err := sctx.GetSessionVars().GetPreparedStmtByName("st")
	if err != nil {
		t.Fatal(err)
	}
	stmt := prepObj.(*plannercore.PlanCacheStmt)

	key1, _, cacheable, reason, err := plannercore.NewPlanCacheKey(sctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if !cacheable {
		t.Fatalf("NewPlanCacheKey not cacheable: %s", reason)
	}

	isReadOnly := stmt.PreparedAst.IsReadOnly
	params := &plannercore.PlanCacheLookupParams{
		ParamSQL:            stmt.StmtText,
		SchemaVersion:       stmt.SchemaVersion,
		RelateVersion:       stmt.RelateVersion,
		LatestSchemaVersion: 0,
		IsReadOnly:          &isReadOnly,
		HasSubquery:         stmt.HasSubquery(),
		HasLimit:            stmt.HasLimit(),
		LimitValues:         stmt.GetLimitValues(),
		StatsVerHash:        0,
	}
	key2, cacheable, reason, err := plannercore.NewPlanCacheKeyBySQLWithParams(sctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if !cacheable {
		t.Fatalf("NewPlanCacheKeyBySQLWithParams not cacheable: %s", reason)
	}

	if key1 != key2 {
		t.Fatalf("plan cache keys mismatch\nNewPlanCacheKey: %q\nNewPlanCacheKeyBySQLWithParams: %q", key1, key2)
	}
}
