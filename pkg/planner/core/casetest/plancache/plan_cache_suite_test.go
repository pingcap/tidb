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

package plancache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	promtestutils "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestInitLRUWithSystemVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_prepared_plan_cache_size = 0") // MinValue: 1
	tk.MustQuery("select @@session.tidb_prepared_plan_cache_size").Check(testkit.Rows("1"))
	sessionVar := tk.Session().GetSessionVars()

	lru := plannercore.NewLRUPlanCache(uint(sessionVar.PreparedPlanCacheSize), 0, 0, tk.Session(), false)
	require.NotNil(t, lru)
}

func TestNonPreparedPlanCachePlanString(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)

	ctx := tk.Session()
	planString := func(sql string) string {
		stmts, err := session.Parse(ctx, sql)
		require.NoError(t, err)
		stmt := stmts[0]
		ret := &plannercore.PreprocessorReturn{}
		nodeW := resolve.NewNodeW(stmt)
		err = plannercore.Preprocess(context.Background(), ctx, nodeW, plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, nodeW, ret.InfoSchema)
		require.NoError(t, err)
		return plannercore.ToString(p)
	}
	defer func() {
		tk.MustExec("set global tidb_redact_log=MARKER")
	}()
	require.Equal(t, planString("select a from t where a < 1"), "IndexReader(Index(t.a)[[-inf,1)])")
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	require.Equal(t, planString("select a from t where a < 10"), "IndexReader(Index(t.a)[[-inf,10)])") // range 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_redact_log=MARKER")
	require.Equal(t, planString("select a from t where a < 10"), "IndexReader(Index(t.a)[[-inf,10)])") // range 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_redact_log=ON")
	require.Equal(t, planString("select a from t where a < 10"), "IndexReader(Index(t.a)[[-inf,10)])") // range 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	require.Equal(t, planString("select * from t where b < 1"), "TableReader(Table(t)->Sel([lt(test.t.b, 1)]))")
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	require.Equal(t, planString("select * from t where b < 10"), "TableReader(Table(t)->Sel([lt(test.t.b, 10)]))") // filter 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_redact_log=MARKER")
	require.Equal(t, planString("select * from t where b < 10"), "TableReader(Table(t)->Sel([lt(test.t.b, 10)]))") // filter 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_redact_log=ON")
	require.Equal(t, planString("select * from t where b < 10"), "TableReader(Table(t)->Sel([lt(test.t.b, 10)]))") // filter 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestJSONExtractPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_json_extract_plan_cache (id int primary key, doc varchar(255))")
	tk.MustExec(`insert into t_json_extract_plan_cache values (1, '{"a": 1, "b": 2}')`)

	tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")
	tk.MustExec(`prepare stmt from 'select id from t_json_extract_plan_cache where json_unquote(json_extract(doc, ?)) = ?'`)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec(`set @path = '$.a', @val = '1'`)
	tk.MustQuery("execute stmt using @path, @val").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`set @path = '$.b', @val = '2'`)
	tk.MustQuery("execute stmt using @path, @val").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec(`set @path = '$.missing', @val = '1'`)
	tk.MustQuery("execute stmt using @path, @val").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
	tk.MustQuery(`select id from t_json_extract_plan_cache where json_unquote(json_extract(doc, '$.a')) = '1'`).Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery(`select id from t_json_extract_plan_cache where json_unquote(json_extract(doc, '$.b')) = '2'`).Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery(`select id from t_json_extract_plan_cache where json_unquote(json_extract(doc, '$.missing')) = '1'`).Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create table t_json_extract_plan_cache_json (id int primary key, doc json)")
	tk.MustExec(`insert into t_json_extract_plan_cache_json values (1, '{"a": 1}')`)
	tk.MustQuery(`select id from t_json_extract_plan_cache_json where json_extract(doc, '$.a') is not null`).Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery(`select id from t_json_extract_plan_cache_json where json_extract(doc, '$.a') is not null`).Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestJSONExtractPlanCacheWithExpressionIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t_json_extract_expr_idx (
		id int primary key,
		doc varchar(255),
		key idx_a ((cast(json_unquote(json_extract(doc, '$.a')) as char(20))))
	)`)
	tk.MustExec(`insert into t_json_extract_expr_idx values
		(1, '{"a": "match", "b": "no"}'),
		(2, '{"a": "no", "b": "match"}')`)

	tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")
	tk.MustExec(`prepare stmt from 'select id from t_json_extract_expr_idx where cast(json_unquote(json_extract(doc, ?)) as char(20)) = ?'`)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec(`set @path = '$.a', @val = 'match'`)
	tk.MustQuery("execute stmt using @path, @val").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`set @path = '$.b', @val = 'match'`)
	tk.MustQuery("execute stmt using @path, @val").Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
	tk.MustQuery(`select id from t_json_extract_expr_idx where cast(json_unquote(json_extract(doc, '$.a')) as char(20)) = 'match'`).Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery(`select id from t_json_extract_expr_idx where cast(json_unquote(json_extract(doc, '$.a')) as char(20)) = 'match'`).Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery(`select id from t_json_extract_expr_idx where cast(json_unquote(json_extract(doc, '$.b')) as char(20)) = 'match'`).Check(testkit.Rows("2"))

	tk.MustExec(`create table t_json_extract_expr_idx_group (
		id int primary key,
		doc varchar(255),
		key idx_a ((cast(json_unquote(json_extract(doc, '$.a')) as char(20))))
	)`)
	tk.MustExec(`insert into t_json_extract_expr_idx_group values
		(1, '{"a": "one", "b": "same"}'),
		(2, '{"a": "two", "b": "same"}')`)
	tk.MustExec(`prepare stmt_group from 'select count(*) from t_json_extract_expr_idx_group group by cast(json_unquote(json_extract(doc, ?)) as char(20)) order by 1'`)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec(`set @path = '$.a'`)
	tk.MustQuery("execute stmt_group using @path").Check(testkit.Rows("1", "1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`set @path = '$.b'`)
	tk.MustQuery("execute stmt_group using @path").Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec(`prepare stmt_agg from 'select max(cast(json_unquote(json_extract(doc, ?)) as char(20))) from t_json_extract_expr_idx_group'`)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec(`set @path = '$.a'`)
	tk.MustQuery("execute stmt_agg using @path").Check(testkit.Rows("two"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`set @path = '$.b'`)
	tk.MustQuery("execute stmt_agg using @path").Check(testkit.Rows("same"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheInformationSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable(), coretestsdk.MockUnsignedTable()})

	stmt, err := p.ParseOneStmt("select avg(a),avg(b),avg(c) from t", "", "")
	require.NoError(t, err)
	nodeW := resolve.NewNodeW(stmt)
	err = plannercore.Preprocess(context.Background(), tk.Session(), nodeW, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
	require.NoError(t, err) // no error
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
	require.NoError(t, err) // no error
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
	require.NoError(t, err) // no error
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestNonPreparedPlanTypeRandomly(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, key(a))`)
	tk.MustExec(`create table t2 (a varchar(8), b varchar(8), key(a))`)
	tk.MustExec(`create table t3 (a double, b double, key(a))`)
	tk.MustExec(`create table t4 (a decimal(4, 2), b decimal(4, 2), key(a))`)
	tk.MustExec(`create table t5 (a year, b year, key(a))`)
	tk.MustExec(`create table t6 (a date, b date, key(a))`)
	tk.MustExec(`create table t7 (a datetime, b datetime, key(a))`)

	n := 30
	for range n {
		tk.MustExec(fmt.Sprintf(`insert into t1 values (%v, %v)`, randNonPrepTypeVal(t, n, "int"), randNonPrepTypeVal(t, n, "int")))
		tk.MustExec(fmt.Sprintf(`insert into t2 values (%v, %v)`, randNonPrepTypeVal(t, n, "varchar"), randNonPrepTypeVal(t, n, "varchar")))
		tk.MustExec(fmt.Sprintf(`insert into t3 values (%v, %v)`, randNonPrepTypeVal(t, n, "double"), randNonPrepTypeVal(t, n, "double")))
		tk.MustExec(fmt.Sprintf(`insert into t4 values (%v, %v)`, randNonPrepTypeVal(t, n, "decimal"), randNonPrepTypeVal(t, n, "decimal")))
		// TODO: fix it later
		//tk.MustExec(fmt.Sprintf(`insert into t5 values (%v, %v)`, randNonPrepTypeVal(t, n, "year"), randNonPrepTypeVal(t, n, "year")))
		tk.MustExec(fmt.Sprintf(`insert into t6 values (%v, %v)`, randNonPrepTypeVal(t, n, "date"), randNonPrepTypeVal(t, n, "date")))
		tk.MustExec(fmt.Sprintf(`insert into t7 values (%v, %v)`, randNonPrepTypeVal(t, n, "datetime"), randNonPrepTypeVal(t, n, "datetime")))
	}

	for range 200 {
		q := fmt.Sprintf(`select * from t%v where %v`, rand.Intn(7)+1, randNonPrepFilter(t, n))
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
		r0 := tk.MustQuery(q).Sort()            // the first execution
		tk.MustQuery(q).Sort().Check(r0.Rows()) // may hit the cache
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
		tk.MustQuery(q).Sort().Check(r0.Rows()) // disable the non-prep cache
	}
}

func randNonPrepFilter(t *testing.T, scale int) string {
	switch rand.Intn(4) {
	case 0: // >=
		return fmt.Sprintf(`a >= %v`, randNonPrepVal(t, scale))
	case 1: // <
		return fmt.Sprintf(`a < %v`, randNonPrepVal(t, scale))
	case 2: // =
		return fmt.Sprintf(`a = %v`, randNonPrepVal(t, scale))
	case 3: // in
		return fmt.Sprintf(`a in (%v, %v)`, randNonPrepVal(t, scale), randNonPrepVal(t, scale))
	}
	require.Error(t, errors.New(""))
	return ""
}

func randNonPrepVal(t *testing.T, scale int) string {
	return randNonPrepTypeVal(t, scale, [7]string{"int", "varchar", "double",
		"decimal", "year", "datetime", "date"}[rand.Intn(7)])
}

func randNonPrepTypeVal(t *testing.T, scale int, typ string) string {
	switch typ {
	case "int":
		return fmt.Sprintf("%v", rand.Intn(scale)-(scale/2))
	case "varchar":
		return fmt.Sprintf("'%v'", rand.Intn(scale)-(scale/2))
	case "double", "decimal":
		return fmt.Sprintf("%v", float64(rand.Intn(scale)-(scale/2))/float64(10))
	case "year":
		return fmt.Sprintf("%v", 2000+rand.Intn(scale))
	case "date":
		return fmt.Sprintf("'2023-01-%02d'", rand.Intn(scale)+1)
	case "timestamp", "datetime":
		return fmt.Sprintf("'2023-01-01 00:00:%02d'", rand.Intn(scale))
	default:
		require.Error(t, errors.New(typ))
		return ""
	}
}

func TestNonPreparedPlanCacheBasically(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, key(b), key(c, d))`)
	for i := range 20 {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v, %v, %v)", i, rand.Intn(20), rand.Intn(20), rand.Intn(20)))
	}

	queries := []string{
		"select * from t where a<10",
		"select * from t where a<13 and b<15",
		"select * from t where b=13",
		"select * from t where c<8",
		"select * from t where d>8",
		"select * from t where c=8 and d>10",
		"select * from t where a<12 and b<13 and c<12 and d>2",
		"select * from t where a in (1, 2, 3)",
		"select * from t where a<13 or b<15",
		"select * from t where a<13 or b<15 and c=13",
		"select * from t where a in (1, 2)",
		"select * from t where a in (1, 2) and b in (1, 2, 3)",
		"select * from t where a in (1, 2) and b < 15",
		"select * from t where a between 1 and 10",
		"select * from t where a between 1 and 10 and b < 15",
		"select * from t where a is null",
		"select * from t where a is not null",
	}

	for _, query := range queries {
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
		resultNormal := tk.MustQuery(query).Sort()
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
		tk.MustQuery(query)                                                    // first process
		tk.MustQuery(query).Sort().Check(resultNormal.Rows())                  // equal to the result without plan-cache
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // this plan is from plan-cache
	}
}

type unifiedPlanCacheFieldMetadata struct {
	columnName   string
	columnAsName string
	tableAsName  string
	dbName       string
	fieldType    byte
	flen         int
	decimal      int
	charset      string
	collation    string
	flag         uint
}

type unifiedPlanCacheQueryObservation struct {
	rows         [][]string
	fields       []unifiedPlanCacheFieldMetadata
	warnings     [][]any
	affectedRows uint64
	cacheHit     bool
}

func observeUnifiedPlanCacheQuery(t *testing.T, tk *testkit.TestKit, sql string) unifiedPlanCacheQueryObservation {
	return observeUnifiedPlanCacheQueryWithContext(context.Background(), t, tk, sql)
}

func observeUnifiedPlanCacheQueryWithContext(
	ctx context.Context,
	t *testing.T,
	tk *testkit.TestKit,
	sql string,
) unifiedPlanCacheQueryObservation {
	rs, err := tk.ExecWithContext(ctx, sql)
	require.NoError(t, err, sql)
	require.NotNil(t, rs, sql)

	fields := make([]unifiedPlanCacheFieldMetadata, 0, len(rs.Fields()))
	for _, field := range rs.Fields() {
		require.NotNil(t, field.Column, sql)
		fieldType := &field.Column.FieldType
		fields = append(fields, unifiedPlanCacheFieldMetadata{
			columnName:   field.Column.Name.O,
			columnAsName: field.ColumnAsName.O,
			tableAsName:  field.TableAsName.O,
			dbName:       field.DBName.O,
			fieldType:    fieldType.GetType(),
			flen:         fieldType.GetFlen(),
			decimal:      fieldType.GetDecimal(),
			charset:      fieldType.GetCharset(),
			collation:    fieldType.GetCollate(),
			flag:         fieldType.GetFlag(),
		})
	}

	rows, err := session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
	require.NoError(t, err, sql)
	observation := unifiedPlanCacheQueryObservation{
		rows:         rows,
		fields:       fields,
		affectedRows: tk.Session().AffectedRows(),
		cacheHit:     tk.Session().GetSessionVars().FoundInPlanCache,
	}
	observation.warnings = tk.MustQuery("show warnings").Rows()
	return observation
}

func requireUnifiedPlanCacheQueryEqual(t *testing.T, expected, actual unifiedPlanCacheQueryObservation, message string) {
	require.Equal(t, expected.rows, actual.rows, message+": rows")
	require.Equal(t, expected.fields, actual.fields, message+": fields")
	require.Equal(t, expected.warnings, actual.warnings, message+": warnings")
	require.Equal(t, expected.affectedRows, actual.affectedRows, message+": affected rows")
}

func observeUnifiedPlanCacheQueryAndParamSQL(
	t *testing.T,
	tk *testkit.TestKit,
	sql string,
) (unifiedPlanCacheQueryObservation, string) {
	var paramSQL string
	captureParamSQL := func(stmt ast.StmtNode) {
		result, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(tk.Session().GetPlanCtx(), stmt)
		require.NoError(t, err)
		require.True(t, supported, reason)
		paramSQL = result.ParamSQL
	}
	ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestIssue43667{}, captureParamSQL)
	observation := observeUnifiedPlanCacheQueryWithContext(ctx, t, tk, sql)
	require.NotEmpty(t, paramSQL)
	return observation, paramSQL
}

func newUnifiedPlanCacheMatrixTestKit(t *testing.T, store kv.Storage, mode string) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_for_subquery=on")
	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=on")
	switch mode {
	case "cache-off":
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=off")
		tk.MustExec("set tidb_enable_prepared_plan_cache=off")
	case "prepared":
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=off")
		tk.MustExec("set tidb_enable_prepared_plan_cache=on")
	case "non-prepared":
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	default:
		require.FailNow(t, "unknown plan cache matrix mode", mode)
	}
	return tk
}

func setUnifiedPlanCachePreparedArgs(tk *testkit.TestKit, args []string) string {
	assignments := make([]string, 0, len(args))
	using := make([]string, 0, len(args))
	for i, value := range args {
		name := fmt.Sprintf("@unified_matrix_arg_%d", i)
		assignments = append(assignments, name+"="+value)
		using = append(using, name)
	}
	tk.MustExec("set " + strings.Join(assignments, ", "))
	return strings.Join(using, ", ")
}

type unifiedPlanCacheMatrixCase struct {
	name        string
	preparedSQL string
	sql         [2]string
	args        [2][]string
}

func runUnifiedPlanCacheQueryMatrix(t *testing.T, store kv.Storage, testCases []unifiedPlanCacheMatrixCase) {
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baseline := [2]unifiedPlanCacheQueryObservation{
				observeUnifiedPlanCacheQuery(t, baselineTK, testCase.sql[0]),
				observeUnifiedPlanCacheQuery(t, baselineTK, testCase.sql[1]),
			}
			for i := range baseline {
				require.False(t, baseline[i].cacheHit)
				require.Empty(t, baseline[i].warnings)
			}

			orders := []struct {
				name    string
				indexes [2]int
			}{
				{name: "a_to_b", indexes: [2]int{0, 1}},
				{name: "b_to_a", indexes: [2]int{1, 0}},
			}
			for _, order := range orders {
				t.Run(order.name, func(t *testing.T) {
					preparedTK := newUnifiedPlanCacheMatrixTestKit(t, store, "prepared")
					preparedTK.MustExec(fmt.Sprintf("prepare unified_matrix_stmt from %q", testCase.preparedSQL))
					for execution, index := range order.indexes {
						using := setUnifiedPlanCachePreparedArgs(preparedTK, testCase.args[index])
						actual := observeUnifiedPlanCacheQuery(t, preparedTK, "execute unified_matrix_stmt using "+using)
						require.Equal(t, execution == 1, actual.cacheHit, "prepared execution %d", execution)
						requireUnifiedPlanCacheQueryEqual(t, baseline[index], actual, fmt.Sprintf("prepared execution %d", execution))
					}

					nonPreparedTK := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
					for execution, index := range order.indexes {
						actual := observeUnifiedPlanCacheQuery(t, nonPreparedTK, testCase.sql[index])
						require.Equal(t, execution == 1, actual.cacheHit, "non-prepared execution %d", execution)
						requireUnifiedPlanCacheQueryEqual(t, baseline[index], actual, fmt.Sprintf("non-prepared execution %d", execution))
					}
				})
			}
		})
	}
}

func TestNonPreparedPlanCacheUnifiedBehaviorMatrix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t2(a int, b int, key(a))")
	tk.MustExec("create table t3(a int, b int, key(a))")
	tk.MustExec(`create table t_special(
		id int, j json, e enum('a', 'b'), s set('x', 'y'), bit_col bit(4), key(id))`)
	tk.MustExec("insert into t values (1, 1), (1, 2), (2, 3), (3, 1)")
	tk.MustExec("insert into t2 select * from t")
	tk.MustExec("insert into t3 select * from t")
	tk.MustExec(`insert into t_special values
		(1, '{"k": 1}', 'a', 'x', b'0001'),
		(2, '{"k": 2}', 'b', 'x,y', b'0010'),
		(3, '{"k": 3}', 'a', 'y', b'0011')`)

	runUnifiedPlanCacheQueryMatrix(t, store, []unifiedPlanCacheMatrixCase{
		{
			name:        "having",
			preparedSQL: "select a, sum(b) as total from t where a > ? group by a having sum(b) > ? order by a",
			sql: [2]string{
				"select a, sum(b) as total from t where a > 0 group by a having sum(b) > 2 order by a",
				"select a, sum(b) as total from t where a > 1 group by a having sum(b) > 1 order by a",
			},
			args: [2][]string{{"0", "2"}, {"1", "1"}},
		},
		{
			name:        "window",
			preparedSQL: "select a, b, sum(b) over (order by a, b rows 1 preceding) as running_sum from t where b > ? order by a, b",
			sql: [2]string{
				"select a, b, sum(b) over (order by a, b rows 1 preceding) as running_sum from t where b > 1 order by a, b",
				"select a, b, sum(b) over (order by a, b rows 1 preceding) as running_sum from t where b > 2 order by a, b",
			},
			args: [2][]string{{"1"}, {"2"}},
		},
		{
			name:        "cte",
			preparedSQL: "with cte as (select a from t where b > ?) select a from cte where a > ? order by a",
			sql: [2]string{
				"with cte as (select a from t where b > 1) select a from cte where a > 0 order by a",
				"with cte as (select a from t where b > 2) select a from cte where a > 1 order by a",
			},
			args: [2][]string{{"1", "0"}, {"2", "1"}},
		},
		{
			name:        "set_operation",
			preparedSQL: "select a from t where b > ? union select a from t where b < ? order by 1",
			sql: [2]string{
				"select a from t where b > 1 union select a from t where b < 3 order by 1",
				"select a from t where b > 2 union select a from t where b < 2 order by 1",
			},
			args: [2][]string{{"1", "3"}, {"2", "2"}},
		},
		{
			name:        "intersect",
			preparedSQL: "select a from t where b > ? intersect select a from t where b < ? order by 1",
			sql: [2]string{
				"select a from t where b > 1 intersect select a from t where b < 3 order by 1",
				"select a from t where b > 2 intersect select a from t where b < 2 order by 1",
			},
			args: [2][]string{{"1", "3"}, {"2", "2"}},
		},
		{
			name:        "except",
			preparedSQL: "select a from t where b > ? except select a from t where b < ? order by 1",
			sql: [2]string{
				"select a from t where b > 0 except select a from t where b < 2 order by 1",
				"select a from t where b > 2 except select a from t where b < 4 order by 1",
			},
			args: [2][]string{{"0", "2"}, {"2", "4"}},
		},
		{
			name:        "three_table_join",
			preparedSQL: "select t.a, t.b, t2.b, t3.b from t join t2 on t.a=t2.a join t3 on t2.a=t3.a where t.b > ? order by t.a, t.b, t2.b, t3.b",
			sql: [2]string{
				"select t.a, t.b, t2.b, t3.b from t join t2 on t.a=t2.a join t3 on t2.a=t3.a where t.b > 1 order by t.a, t.b, t2.b, t3.b",
				"select t.a, t.b, t2.b, t3.b from t join t2 on t.a=t2.a join t3 on t2.a=t3.a where t.b > 2 order by t.a, t.b, t2.b, t3.b",
			},
			args: [2][]string{{"1"}, {"2"}},
		},
		{
			name:        "subquery",
			preparedSQL: "select a, b from t where b > ? and a in (select a from t2 where b > ?) order by a, b",
			sql: [2]string{
				"select a, b from t where b > 0 and a in (select a from t2 where b > 1) order by a, b",
				"select a, b from t where b > 1 and a in (select a from t2 where b > 2) order by a, b",
			},
			args: [2][]string{{"0", "1"}, {"1", "2"}},
		},
		{
			name:        "limit",
			preparedSQL: "select a, b from t where a > ? order by a, b limit 2",
			sql: [2]string{
				"select a, b from t where a > 0 order by a, b limit 2",
				"select a, b from t where a > 1 order by a, b limit 2",
			},
			args: [2][]string{{"0"}, {"1"}},
		},
		{
			name:        "preserved_null_bit_hex_literals",
			preparedSQL: "select a, null as null_literal, b'1' as bit_literal, x'0a' as hex_literal from t where a > ? order by a",
			sql: [2]string{
				"select a, null as null_literal, b'1' as bit_literal, x'0a' as hex_literal from t where a > 0 order by a",
				"select a, null as null_literal, b'1' as bit_literal, x'0a' as hex_literal from t where a > 1 order by a",
			},
			args: [2][]string{{"0"}, {"1"}},
		},
		{
			name: "json_enum_set_bit_columns",
			preparedSQL: "select id, j, e, s, bit_col from t_special " +
				"where id > ? and j = ? and e = ? and s = ? and bit_col >= ? order by id",
			sql: [2]string{
				"select id, j, e, s, bit_col from t_special " +
					"where id > 0 and j = '{\"k\": 1}' and e = 'a' and s = 'x' and bit_col >= 1 order by id",
				"select id, j, e, s, bit_col from t_special " +
					"where id > 1 and j = '{\"k\": 3}' and e = 'a' and s = 'y' and bit_col >= 3 order by id",
			},
			args: [2][]string{
				{"0", "'{\"k\": 1}'", "'a'", "'x'", "1"},
				{"1", "'{\"k\": 3}'", "'a'", "'y'", "3"},
			},
		},
	})
}

func TestNonPreparedPlanCacheUnifiedPreservedLiteralsInFilters(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table preserved_filter_literals(a int, key(a))")
	setupTK.MustExec("insert into preserved_filter_literals values (1), (2), (3)")

	cases := []struct {
		name string
		sql  string
		rows [][]string
	}{
		{
			name: "null",
			sql:  "select a from preserved_filter_literals where a = null order by a",
			rows: [][]string{},
		},
		{
			name: "bit",
			sql:  "select a from preserved_filter_literals where a > b'0' order by a",
			rows: [][]string{{"1"}, {"2"}, {"3"}},
		},
		{
			name: "hex",
			sql:  "select a from preserved_filter_literals where a > x'00' order by a",
			rows: [][]string{{"1"}, {"2"}, {"3"}},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baseline := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.sql)
			require.False(t, baseline.cacheHit)
			require.Equal(t, testCase.rows, baseline.rows)

			nonPreparedTK := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
			miss := observeUnifiedPlanCacheQuery(t, nonPreparedTK, testCase.sql)
			require.False(t, miss.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baseline, miss, "non-prepared miss")

			hit := observeUnifiedPlanCacheQuery(t, nonPreparedTK, testCase.sql)
			require.True(t, hit.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baseline, hit, "non-prepared hit")
		})
	}
}

func TestNonPreparedPlanCacheUnifiedPreservedLiteralStatementLRUIdentity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table preserved_literal_lru_identity(a int, b int, key(a))")
	setupTK.MustExec("insert into preserved_literal_lru_identity values (1, 30), (2, 10), (3, 20)")

	cases := []struct {
		name string
		a    string
		b    string
	}{
		{
			name: "order by positional literal",
			a:    "select a, b from preserved_literal_lru_identity order by 1",
			b:    "select a, b from preserved_literal_lru_identity order by 2",
		},
		{
			name: "limit literal",
			a:    "select a, b from preserved_literal_lru_identity order by a limit 1",
			b:    "select a, b from preserved_literal_lru_identity order by a limit 2",
		},
		{
			name: "null projection literal",
			a:    "select null as preserved_literal, a from preserved_literal_lru_identity order by a",
			b:    "select 0 as preserved_literal, a from preserved_literal_lru_identity order by a",
		},
		{
			name: "bit range literal",
			a:    "select a from preserved_literal_lru_identity where a > b'0' order by a",
			b:    "select a from preserved_literal_lru_identity where a > b'1' order by a",
		},
		{
			name: "hex range literal",
			a:    "select a from preserved_literal_lru_identity where a > x'00' order by a",
			b:    "select a from preserved_literal_lru_identity where a > x'02' order by a",
		},
		{
			name: "binary token spelling",
			a:    "select a from preserved_literal_lru_identity where a > b'0001' and a < X'04' order by a",
			b:    "select a from preserved_literal_lru_identity where a > b'1' and a < 0x04 order by a",
		},
		{
			name: "window frame literal",
			a:    "select a, sum(b) over (order by a rows 1 preceding) as running_sum from preserved_literal_lru_identity where a > 0 order by a",
			b:    "select a, sum(b) over (order by a rows 2 preceding) as running_sum from preserved_literal_lru_identity where a > 0 order by a",
		},
		{
			name: "named window frame literal",
			a:    "select a, sum(b) over named_window as running_sum from preserved_literal_lru_identity where a > 0 window named_window as (order by a rows 1 preceding) order by a",
			b:    "select a, sum(b) over named_window as running_sum from preserved_literal_lru_identity where a > 0 window named_window as (order by a rows 2 preceding) order by a",
		},
		{
			name: "date format literal",
			a:    "select a, date_format('2020-01-02', '%Y-%m-%d') as formatted from preserved_literal_lru_identity where a > 0 order by a",
			b:    "select a, date_format('2020-01-02', '%d/%m/%Y') as formatted from preserved_literal_lru_identity where a > 0 order by a",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baselineA := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.a)
			baselineB := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.b)
			require.False(t, baselineA.cacheHit)
			require.False(t, baselineB.cacheHit)

			executeWithLookup := func(tk *testkit.TestKit, sql string) (unifiedPlanCacheQueryObservation, bool) {
				lookups := make([]bool, 0, 1)
				ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedStmtLookup{}, func(hit bool) {
					lookups = append(lookups, hit)
				})
				observation := observeUnifiedPlanCacheQueryWithContext(ctx, t, tk, sql)
				require.Len(t, lookups, 1, "statement carrier lookup count")
				return observation, lookups[0]
			}

			orders := []struct {
				name           string
				first          string
				second         string
				firstBaseline  unifiedPlanCacheQueryObservation
				secondBaseline unifiedPlanCacheQueryObservation
			}{
				{name: "a_to_b", first: testCase.a, second: testCase.b, firstBaseline: baselineA, secondBaseline: baselineB},
				{name: "b_to_a", first: testCase.b, second: testCase.a, firstBaseline: baselineB, secondBaseline: baselineA},
			}
			for _, order := range orders {
				t.Run(order.name, func(t *testing.T) {
					tk := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")

					first, firstLookup := executeWithLookup(tk, order.first)
					require.False(t, firstLookup, "first statement carrier lookup must miss")
					require.False(t, first.cacheHit)
					requireUnifiedPlanCacheQueryEqual(t, order.firstBaseline, first, "first execution")

					second, secondLookup := executeWithLookup(tk, order.second)
					require.False(t, secondLookup, "changed preserve literal must use a different statement carrier")
					require.False(t, second.cacheHit)
					requireUnifiedPlanCacheQueryEqual(t, order.secondBaseline, second, "changed literal execution")

					firstAgain, firstAgainLookup := executeWithLookup(tk, order.first)
					require.True(t, firstAgainLookup, "original statement carrier must be reusable")
					require.True(t, firstAgain.cacheHit)
					requireUnifiedPlanCacheQueryEqual(t, order.firstBaseline, firstAgain, "original literal execution")
				})
			}
		})
	}
}

func TestNonPreparedPlanCacheUnifiedParamSQLRestoreFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table param_restore_fallback(a int)")
	tk.MustExec("insert into param_restore_fallback values (1), (2)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")

	paramSQLRestoreFailed := false
	injectFailure := func(stmt ast.StmtNode) {
		paramSQLRestoreFailed = true
		selectStmt := stmt.(*ast.SelectStmt)
		badLiteral := ast.NewValueExpr(struct{}{}, "", "")
		badLiteral.SetText(nil, "")
		selectStmt.Fields.Fields[0].Expr = badLiteral
	}
	ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedParam{}, injectFailure)

	tk.MustQueryWithContext(ctx, "select 1 from param_restore_fallback where a = 1").Check(testkit.Rows("1"))
	require.True(t, paramSQLRestoreFailed)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestNonPreparedPlanCacheUnifiedASTRestoreFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table ast_restore_failure(a int)")
	tk.MustExec("insert into ast_restore_failure values (1), (2)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")

	const failpointName = "github.com/pingcap/tidb/pkg/planner/core/mockNonPreparedPlanCacheASTRestoreError"
	require.NoError(t, failpoint.Enable(failpointName, `return(true)`))
	defer func() { _ = failpoint.Disable(failpointName) }()

	var parameterizedStmt ast.StmtNode
	ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedParam{}, func(stmt ast.StmtNode) {
		parameterizedStmt = stmt
	})
	_, err := tk.ExecWithContext(ctx, "select a from ast_restore_failure where a = 1")
	require.ErrorContains(t, err, "failed to restore ast.Node")
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.NotNil(t, parameterizedStmt)

	// The restore error must not leave generated markers attached to the AST.
	require.NoError(t, failpoint.Disable(failpointName))
	result, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(
		tk.Session().GetPlanCtx(), parameterizedStmt,
	)
	require.NoError(t, err)
	require.True(t, supported, reason)
	require.Equal(t, "SELECT a FROM `test`.`ast_restore_failure` WHERE `a`=?", result.ParamSQL)
	require.Equal(t, int64(1), result.ParamValues[0].GetValue())
}

func TestNonPreparedPlanCacheUnifiedOriginalParamMarkerBypass(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table original_param_marker(a int)")
	tk.MustExec("insert into original_param_marker values (1)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")

	postParameterizationCalled := false
	injectFailure := func(stmt ast.StmtNode) {
		where := stmt.(*ast.SelectStmt).Where.(*ast.BinaryOperationExpr)
		where.R = ast.NewParamMarkerExpr(-1)
	}
	ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedParam{}, injectFailure)
	ctx = context.WithValue(ctx, plannercore.PlanCacheKeyTestIssue43667{}, func(ast.StmtNode) {
		postParameterizationCalled = true
	})

	rs, err := tk.ExecWithContext(ctx, "select 1 from original_param_marker where a = 1")
	require.NoError(t, err)
	if rs != nil {
		require.NoError(t, rs.Close())
	}
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.False(t, postParameterizationCalled)
}

func TestNonPreparedPlanCacheUnifiedParamSQLParseFailureBypass(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table param_sql_parse_failure(a int)")
	tk.MustExec("insert into param_sql_parse_failure values (1), (2)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")

	corruptParamSQL := func(paramSQL *string) {
		*paramSQL = "select !!!"
	}
	ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedParamSQL{}, corruptParamSQL)
	unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
	before := promtestutils.ToFloat64(unsupported)

	tk.MustQueryWithContext(ctx, "select a from param_sql_parse_failure where a = 1").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	require.Equal(t, before+1, promtestutils.ToFloat64(unsupported))

	beforeExplain := promtestutils.ToFloat64(unsupported)
	tk.MustExecWithContext(ctx, "explain format='plan_cache' select a from param_sql_parse_failure where a = 1")
	warnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: failed to parse parameterized SQL")
	require.Equal(t, beforeExplain+1, promtestutils.ToFloat64(unsupported))
}

type unifiedPlanCacheDMLObservation struct {
	warnings     [][]any
	affectedRows uint64
	cacheHit     bool
}

func observeUnifiedPlanCacheDML(t *testing.T, tk *testkit.TestKit, sql string) unifiedPlanCacheDMLObservation {
	rs, err := tk.Exec(sql)
	require.NoError(t, err, sql)
	if rs != nil {
		require.NoError(t, rs.Close(), sql)
	}
	affectedRows := tk.Session().AffectedRows()
	cacheHit := tk.Session().GetSessionVars().FoundInPlanCache
	return unifiedPlanCacheDMLObservation{
		warnings:     tk.MustQuery("show warnings").Rows(),
		affectedRows: affectedRows,
		cacheHit:     cacheHit,
	}
}

func resetUnifiedPlanCacheDMLTable(t *testing.T, tk *testkit.TestKit, rows string) {
	tk.MustExec("drop table if exists unified_dml_matrix")
	tk.MustExec("create table unified_dml_matrix(a int, b int, key(a))")
	tk.MustExec("insert into unified_dml_matrix values " + rows)
}

type unifiedPlanCacheDMLCase struct {
	name        string
	preparedSQL string
	sql         [2]string
	args        [2][]string
	initialRows string
	setupTables func(*testkit.TestKit)
	finalQuery  string
	finalRows   [][]any
}

func (testCase unifiedPlanCacheDMLCase) resetTables(t *testing.T, tk *testkit.TestKit) {
	if testCase.setupTables != nil {
		testCase.setupTables(tk)
		return
	}
	resetUnifiedPlanCacheDMLTable(t, tk, testCase.initialRows)
}

func (testCase unifiedPlanCacheDMLCase) checkFinalRows(tk *testkit.TestKit) {
	query := testCase.finalQuery
	if query == "" {
		query = "select a, b from unified_dml_matrix order by a"
	}
	tk.MustQuery(query).Check(testCase.finalRows)
}

func runUnifiedPlanCacheDMLMatrix(t *testing.T, store kv.Storage, testCases []unifiedPlanCacheDMLCase) {
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baselineTK.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")
			testCase.resetTables(t, baselineTK)
			baseline := [2]unifiedPlanCacheDMLObservation{
				observeUnifiedPlanCacheDML(t, baselineTK, testCase.sql[0]),
				observeUnifiedPlanCacheDML(t, baselineTK, testCase.sql[1]),
			}
			for i := range baseline {
				require.False(t, baseline[i].cacheHit)
				require.Empty(t, baseline[i].warnings)
			}
			testCase.checkFinalRows(baselineTK)

			orders := []struct {
				name    string
				indexes [2]int
			}{
				{name: "a_to_b", indexes: [2]int{0, 1}},
				{name: "b_to_a", indexes: [2]int{1, 0}},
			}
			for _, order := range orders {
				t.Run(order.name, func(t *testing.T) {
					preparedTK := newUnifiedPlanCacheMatrixTestKit(t, store, "prepared")
					preparedTK.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")
					testCase.resetTables(t, preparedTK)
					preparedTK.MustExec(fmt.Sprintf("prepare unified_dml_matrix_stmt from %q", testCase.preparedSQL))
					for execution, index := range order.indexes {
						using := setUnifiedPlanCachePreparedArgs(preparedTK, testCase.args[index])
						actual := observeUnifiedPlanCacheDML(t, preparedTK, "execute unified_dml_matrix_stmt using "+using)
						require.Equal(t, execution == 1, actual.cacheHit, "prepared execution %d", execution)
						require.Equal(t, baseline[index].affectedRows, actual.affectedRows, "prepared affected rows %d", execution)
						require.Equal(t, baseline[index].warnings, actual.warnings, "prepared warnings %d", execution)
					}
					testCase.checkFinalRows(preparedTK)

					nonPreparedTK := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
					nonPreparedTK.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")
					nonPreparedTK.MustExec("set tidb_enable_non_prepared_plan_cache=off")
					testCase.resetTables(t, nonPreparedTK)
					nonPreparedTK.MustExec("set tidb_enable_non_prepared_plan_cache=on")
					nonPreparedTK.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
					for execution, index := range order.indexes {
						actual := observeUnifiedPlanCacheDML(t, nonPreparedTK, testCase.sql[index])
						require.Equal(t, execution == 1, actual.cacheHit, "non-prepared execution %d", execution)
						require.Equal(t, baseline[index].affectedRows, actual.affectedRows, "non-prepared affected rows %d", execution)
						require.Equal(t, baseline[index].warnings, actual.warnings, "non-prepared warnings %d", execution)
					}
					testCase.checkFinalRows(nonPreparedTK)
				})
			}
		})
	}
}

func TestNonPreparedPlanCacheUnifiedDMLBehaviorMatrix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	setupInsertSelect := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists unified_insert_select_source")
		tk.MustExec("drop table if exists unified_insert_select_target")
		tk.MustExec("create table unified_insert_select_source(a int, b int)")
		tk.MustExec("create table unified_insert_select_target(a int, b int, primary key (a, b))")
		tk.MustExec("insert into unified_insert_select_source values (1, 1), (2, 2), (3, 3)")
	}
	setupDuplicate := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists unified_duplicate_target")
		tk.MustExec("create table unified_duplicate_target(a int primary key, b int)")
		tk.MustExec("insert into unified_duplicate_target values (1, 0)")
	}

	runUnifiedPlanCacheDMLMatrix(t, store, []unifiedPlanCacheDMLCase{
		{
			name:        "insert",
			preparedSQL: "insert into unified_dml_matrix values (?, ?)",
			sql: [2]string{
				"insert into unified_dml_matrix values (1, 10)",
				"insert into unified_dml_matrix values (2, 20)",
			},
			args:        [2][]string{{"1", "10"}, {"2", "20"}},
			initialRows: "(9, 90)",
			finalRows:   testkit.Rows("1 10", "2 20", "9 90"),
		},
		{
			name:        "update",
			preparedSQL: "update unified_dml_matrix set b = ? where a = ?",
			sql: [2]string{
				"update unified_dml_matrix set b = 11 where a = 1",
				"update unified_dml_matrix set b = 22 where a = 2",
			},
			args:        [2][]string{{"11", "1"}, {"22", "2"}},
			initialRows: "(1, 10), (2, 20)",
			finalRows:   testkit.Rows("1 11", "2 22"),
		},
		{
			name:        "delete",
			preparedSQL: "delete from unified_dml_matrix where a = ?",
			sql: [2]string{
				"delete from unified_dml_matrix where a = 1",
				"delete from unified_dml_matrix where a = 2",
			},
			args:        [2][]string{{"1"}, {"2"}},
			initialRows: "(1, 10), (2, 20)",
			finalRows:   testkit.Rows(),
		},
		{
			name:        "insert select",
			preparedSQL: "insert into unified_insert_select_target(a, b) select a, b from unified_insert_select_source where a > ? and b < ?",
			sql: [2]string{
				"insert into unified_insert_select_target(a, b) select a, b from unified_insert_select_source where a > 1 and b < 3",
				"insert into unified_insert_select_target(a, b) select a, b from unified_insert_select_source where a > 2 and b < 4",
			},
			args:        [2][]string{{"1", "3"}, {"2", "4"}},
			setupTables: setupInsertSelect,
			finalQuery:  "select a, b from unified_insert_select_target order by a, b",
			finalRows:   testkit.Rows("2 2", "3 3"),
		},
		{
			name:        "on duplicate key update",
			preparedSQL: "insert into unified_duplicate_target(a, b) values (?, ?) on duplicate key update b = b + ?",
			sql: [2]string{
				"insert into unified_duplicate_target(a, b) values (1, 10) on duplicate key update b = b + 2",
				"insert into unified_duplicate_target(a, b) values (2, 20) on duplicate key update b = b + 3",
			},
			args:        [2][]string{{"1", "10", "2"}, {"2", "20", "3"}},
			setupTables: setupDuplicate,
			finalQuery:  "select a, b from unified_duplicate_target order by a",
			finalRows:   testkit.Rows("1 2", "2 20"),
		},
	})
}

func TestNonPreparedPlanCacheUnifiedMultiTableDMLGateE2E(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")

	type dmlCase struct {
		name       string
		first      string
		second     string
		explain    string
		reason     string
		finalQuery string
		finalRows  [][]any
	}
	cases := []dmlCase{
		{
			name:       "update",
			first:      "update multi_gate_left l join multi_gate_right r on l.a=r.a set l.b=11 where l.a=1",
			second:     "update multi_gate_left l join multi_gate_right r on l.a=r.a set l.b=12 where l.a=2",
			explain:    "update multi_gate_left l join multi_gate_right r on l.a=r.a set l.b=11 where l.a=1",
			reason:     "multiple-table UPDATE is not supported",
			finalQuery: "select a, b from multi_gate_left order by a",
			finalRows:  testkit.Rows("1 11", "2 12"),
		},
		{
			name:       "delete",
			first:      "delete l from multi_gate_left l join multi_gate_right r on l.a=r.a where l.a=1",
			second:     "delete l from multi_gate_left l join multi_gate_right r on l.a=r.a where l.a=2",
			explain:    "delete l from multi_gate_left l join multi_gate_right r on l.a=r.a where l.a=1",
			reason:     "multiple-table DELETE is not supported",
			finalQuery: "select a, b from multi_gate_left order by a",
			finalRows:  testkit.Rows(),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tk.MustExec("drop table if exists multi_gate_left")
			tk.MustExec("drop table if exists multi_gate_right")
			tk.MustExec("create table multi_gate_left(a int primary key, b int)")
			tk.MustExec("create table multi_gate_right(a int primary key)")
			tk.MustExec("insert into multi_gate_left values (1, 10), (2, 20)")
			tk.MustExec("insert into multi_gate_right values (1), (2)")

			execute := func(sql string) (uint64, bool, []bool, [][]any) {
				lookups := make([]bool, 0, 1)
				ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedStmtLookup{}, func(found bool) {
					lookups = append(lookups, found)
				})
				rs, err := tk.ExecWithContext(ctx, sql)
				require.NoError(t, err, sql)
				if rs != nil {
					require.NoError(t, rs.Close(), sql)
				}
				return tk.Session().AffectedRows(), tk.Session().GetSessionVars().FoundInPlanCache, lookups, tk.MustQuery("show warnings").Rows()
			}

			unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
			before := promtestutils.ToFloat64(unsupported)
			firstAffected, firstHit, firstLookups, firstWarnings := execute(testCase.first)
			secondAffected, secondHit, secondLookups, secondWarnings := execute(testCase.second)
			require.Equal(t, uint64(1), firstAffected)
			require.Equal(t, uint64(1), secondAffected)
			require.False(t, firstHit)
			require.False(t, secondHit)
			require.Empty(t, firstLookups)
			require.Empty(t, secondLookups)
			require.Empty(t, firstWarnings)
			require.Empty(t, secondWarnings)
			require.Greater(t, promtestutils.ToFloat64(unsupported), before, "multi-table DML precheck must increment unsupported metric")
			tk.MustQuery(testCase.finalQuery).Check(testCase.finalRows)

			beforeExplain := promtestutils.ToFloat64(unsupported)
			tk.MustExec("explain format='plan_cache' " + testCase.explain)
			warnings := tk.MustQuery("show warnings").Rows()
			require.Len(t, warnings, 1)
			require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: "+testCase.reason)
			require.Greater(t, promtestutils.ToFloat64(unsupported), beforeExplain, "Explain must count the multi-table DML bypass")
		})
	}
}

func TestNonPreparedPlanCacheUnifiedCacheabilityCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t2(a int, b int, key(a))")
	tk.MustExec("create table t3(a int, b int, key(a))")
	tk.MustExec("insert into t values (1, 1), (1, 2), (2, 3), (3, 1)")
	tk.MustExec("insert into t2 select * from t")
	tk.MustExec("insert into t3 select * from t")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustQuery("select @@tidb_enable_non_prepared_plan_cache_unified_cacheability_check").Check(testkit.Rows("0"))

	legacyHaving := "select a, sum(b) from t where a > 0 group by a having sum(b) > 2 order by a"
	tk.MustQuery(legacyHaving).Check(testkit.Rows("1 3", "2 3"))
	tk.MustQuery(legacyHaving).Check(testkit.Rows("1 3", "2 3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustQuery(legacyHaving).Check(testkit.Rows("1 3", "2 3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("select a, sum(b) from t where a > 0 group by a having sum(b) > 3 order by a").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	testCases := []struct {
		name   string
		first  string
		second string
	}{
		{
			name:   "window",
			first:  "select a, sum(b) over (order by a rows 1 preceding) from t where b > 1",
			second: "select a, sum(b) over (order by a rows 1 preceding) from t where b > 2",
		},
		{
			name:   "cte",
			first:  "with cte as (select a from t where b > 1) select a from cte where a > 0 order by a",
			second: "with cte as (select a from t where b > 2) select a from cte where a > 0 order by a",
		},
		{
			name:   "set operation",
			first:  "select a from t where b > 1 union select a from t where b < 3 order by 1",
			second: "select a from t where b > 2 union select a from t where b < 2 order by 1",
		},
		{
			name:   "three table join",
			first:  "select t.a from t join t2 on t.a=t2.a join t3 on t2.a=t3.a where t.b > 1",
			second: "select t.a from t join t2 on t.a=t2.a join t3 on t2.a=t3.a where t.b > 2",
		},
	}
	for _, testCase := range testCases {
		t.Logf("unified non-prepared plan cache case: %s", testCase.name)
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=off")
		firstBaseline := tk.MustQuery(testCase.first).Sort()
		secondBaseline := tk.MustQuery(testCase.second).Sort()
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
		tk.MustQuery(testCase.first).Sort().Check(firstBaseline.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery(testCase.second).Sort().Check(secondBaseline.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	}

	tk.MustExec("set tidb_enable_plan_cache_for_subquery=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=off")
	subqueryFirstBaseline := tk.MustQuery("select a from t where b > 0 and a in (select a from t2 where b > 1)").Sort()
	subquerySecondBaseline := tk.MustQuery("select a from t where b > 1 and a in (select a from t2 where b > 2)").Sort()
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustQuery("select a from t where b > 0 and a in (select a from t2 where b > 1)").Sort().Check(subqueryFirstBaseline.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("select a from t where b > 1 and a in (select a from t2 where b > 2)").Sort().Check(subquerySecondBaseline.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_enable_plan_cache_for_subquery=off")
	tk.MustQuery("select a from t where b > 2 and a in (select a from t2 where b > 3)")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_plan_cache_for_subquery=on")
	tk.MustQuery("select a from t where b > 3 and a in (select a from t2 where b > 4)")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=off")
	limitFirstBaseline := tk.MustQuery("select a from t where a > 0 order by a limit 2").Sort()
	limitSecondBaseline := tk.MustQuery("select a from t where a > 1 order by a limit 2").Sort()
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustQuery("select a from t where a > 0 order by a limit 2").Sort().Check(limitFirstBaseline.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("select a from t where a > 1 order by a limit 2").Sort().Check(limitSecondBaseline.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=off")
	tk.MustQuery("select a from t where a > 2 order by a limit 2")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=on")
	tk.MustQuery("select a from t where a > 3 order by a limit 2")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=off")
	tk.MustQuery(legacyHaving).Check(testkit.Rows("1 3", "2 3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustQuery(legacyHaving).Check(testkit.Rows("1 3", "2 3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=off")
	tk.MustExec("insert into t values (4, 4)")
	tk.MustExec("insert into t values (5, 5)")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")
	tk.MustExec("insert into t values (6, 6)")
	tk.MustExec("insert into t values (7, 7)")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("update t set b=16 where a=6")
	tk.MustExec("update t set b=17 where a=7")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("delete from t where a=6")
	tk.MustExec("delete from t where a=7")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t where a in (6, 7)").Check(testkit.Rows("0"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 1", "1 2", "2 3", "3 1", "4 4", "5 5"))

	tk.MustQuery("select _utf8mb4'a' from t where a=1").Check(testkit.Rows("a", "a"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("explain format='plan_cache' select _utf8mb4'a' from t where a=1")
	warnings := tk.MustQuery("show warnings").Rows()
	require.NotEmpty(t, warnings)
	require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: query has values with under-score charset")
}

func TestNonPreparedPlanCacheUnifiedLimitOffset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table unified_limit_offset(a int, key(a))")
	setupTK.MustExec("insert into unified_limit_offset values (1), (2), (3), (4)")

	cases := []struct {
		name   string
		first  string
		second string
	}{
		{
			name:   "offset_count",
			first:  "select a from unified_limit_offset where a > 0 order by a limit 1, 2",
			second: "select a from unified_limit_offset where a > 1 order by a limit 1, 2",
		},
		{
			name:   "limit_offset",
			first:  "select a from unified_limit_offset where a > 0 order by a limit 2 offset 1",
			second: "select a from unified_limit_offset where a > 1 order by a limit 2 offset 1",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baselineFirst := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.first)
			baselineSecond := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.second)

			nonPreparedTK := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
			miss := observeUnifiedPlanCacheQuery(t, nonPreparedTK, testCase.first)
			require.False(t, miss.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baselineFirst, miss, "param-limit enabled miss")
			hit := observeUnifiedPlanCacheQuery(t, nonPreparedTK, testCase.second)
			require.True(t, hit.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baselineSecond, hit, "param-limit enabled hit")

			bypassTK := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
			bypassTK.MustExec("set tidb_enable_plan_cache_for_param_limit=off")
			unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
			beforeBypass := promtestutils.ToFloat64(unsupported)
			bypassFirst := observeUnifiedPlanCacheQuery(t, bypassTK, testCase.first)
			require.False(t, bypassFirst.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baselineFirst, bypassFirst, "param-limit disabled first")
			bypassSecond := observeUnifiedPlanCacheQuery(t, bypassTK, testCase.second)
			require.False(t, bypassSecond.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baselineSecond, bypassSecond, "param-limit disabled second")
			require.Equal(t, beforeBypass+2, promtestutils.ToFloat64(unsupported), "parameterization bypass must count once per execution")

			beforeExplain := promtestutils.ToFloat64(unsupported)
			bypassTK.MustExec("explain format='plan_cache' " + testCase.first)
			warnings := bypassTK.MustQuery("show warnings").Rows()
			require.Len(t, warnings, 1)
			require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: query has 'limit ?' is un-cacheable")
			require.Equal(t, beforeExplain+1, promtestutils.ToFloat64(unsupported))
		})
	}
}

func TestNonPreparedPlanCacheUnifiedCacheabilityCheckGlobalVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select @@session.tidb_enable_non_prepared_plan_cache_unified_cacheability_check").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustQuery("select @@session.tidb_enable_non_prepared_plan_cache_unified_cacheability_check").Check(testkit.Rows("0"))

	newSession := testkit.NewTestKit(t, store)
	newSession.MustQuery("select @@session.tidb_enable_non_prepared_plan_cache_unified_cacheability_check").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheUnifiedCacheabilityCheckMetric(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=off")

	unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
	before := promtestutils.ToFloat64(unsupported)
	tk.MustExec("insert into t values (1)")
	require.Equal(t, before, promtestutils.ToFloat64(unsupported))

	tk.MustQuery("select _utf8mb4'a'").Check(testkit.Rows("a"))
	require.Equal(t, before+1, promtestutils.ToFloat64(unsupported))

	tk.MustExec("set @a=1")
	before = promtestutils.ToFloat64(unsupported)
	tk.MustQuery("select * from t where a=@a").Check(testkit.Rows("1"))
	require.Equal(t, before+1, promtestutils.ToFloat64(unsupported))
}

func TestNonPreparedPlanCacheUnifiedCacheabilityCheckRejects(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")

	// SELECT INTO is rejected before carrier construction and must continue with
	// ordinary planning on every execution.
	for i := 0; i < 2; i++ {
		tk.MustExec("select a into @a from t where a=1")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	// Locking reads are controlled by the DML gate even though they are SELECTs.
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=off")
	for i := 0; i < 2; i++ {
		tk.MustQuery("select * from t where a=1 for update").Check(testkit.Rows("1 1"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")

	// Explicit ignore_plan_cache() and an uncacheable function are checked by the
	// unified AST cacheability path.
	for i := 0; i < 2; i++ {
		tk.MustQuery("select /*+ ignore_plan_cache() */ * from t where a=1").Check(testkit.Rows("1 1"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("select connection_id() from t where a=1")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}

	// The unified switch must have no effect when the main non-prepared cache
	// switch is disabled.
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=off")
	tk.MustQuery("select * from t where a=1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheUnifiedLockingReadWithDMLEnabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table unified_locking_read(a int, b int, key(a))")
	setupTK.MustExec("insert into unified_locking_read values (1, 10), (2, 20)")

	for _, lockClause := range []string{"for update", "for share"} {
		t.Run(lockClause, func(t *testing.T) {
			query := "select a, b from unified_locking_read where a > 0 order by a " + lockClause
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baselineTK.MustExec("set tidb_enable_noop_functions=on")
			baseline := observeUnifiedPlanCacheQuery(t, baselineTK, query)
			require.False(t, baseline.cacheHit)

			tk := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
			tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")
			tk.MustExec("set tidb_enable_noop_functions=on")
			executeWithLookup := func(sql string) (unifiedPlanCacheQueryObservation, bool) {
				lookups := make([]bool, 0, 1)
				ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedStmtLookup{}, func(found bool) {
					lookups = append(lookups, found)
				})
				observation := observeUnifiedPlanCacheQueryWithContext(ctx, t, tk, sql)
				require.Len(t, lookups, 1, "locking read statement carrier lookup count")
				return observation, lookups[0]
			}

			first, firstLookup := executeWithLookup(query)
			require.False(t, firstLookup, "first locking read must miss the statement carrier")
			require.False(t, first.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baseline, first, "locking read miss")

			second, secondLookup := executeWithLookup(query)
			require.True(t, secondLookup, "second locking read must hit the statement carrier")
			require.True(t, second.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, baseline, second, "locking read hit")
		})
	}
}

func TestNonPreparedPlanCacheUnifiedDMLCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")

	updateQuery := "with cte(a) as (select 1) update t set b = b + 1 where a in (select a from cte)"
	tk.MustExec(updateQuery)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("with cte(a) as (select 1) update t set b = b + 2 where a in (select a from cte)")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 13", "2 20"))

	deleteQuery := "with cte(a) as (select 1) delete from t where a in (select a from cte)"
	tk.MustExec("insert into t values (1, 30)")
	tk.MustExec(deleteQuery)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("insert into t values (1, 40)")
	tk.MustExec(deleteQuery)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("2 20"))
}

func TestNonPreparedPlanCacheUnifiedCacheabilityCheckStateChanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table tp (a int, b int, key(a)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than maxvalue)`)
	tk.MustExec("insert into tp values (1, 10), (11, 20), (21, 30)")
	tk.MustExec("analyze table tp")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustExec("set tidb_partition_prune_mode=dynamic")

	first := tk.MustQuery("select b from tp where a > 0").Sort()
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	second := tk.MustQuery("select b from tp where a > 10").Sort()
	require.Equal(t, testkit.Rows("20", "30"), second.Rows())
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	// The carrier remains in the statement LRU, but the current partition mode
	// makes the same parameterized AST uncacheable.
	tk.MustExec("set tidb_partition_prune_mode=static")
	tk.MustQuery("select b from tp where a > 20").Sort().Check(testkit.Rows("30"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.Equal(t, testkit.Rows("10", "20", "30"), first.Rows())

	// A schema change must invalidate the plan value associated with the
	// statement carrier before the next execution.
	tk.MustExec("set tidb_partition_prune_mode=dynamic")
	tk.MustExec("alter table tp add column c int")
	tk.MustQuery("select b from tp where a > 10").Sort().Check(testkit.Rows("20", "30"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestNonPreparedPlanCacheUnifiedFixControlStateChanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table fix_plain(a int, key(a))")
	setupTK.MustExec("insert into fix_plain values (1), (2), (3), (4)")
	setupTK.MustExec(`create table fix_partition(a int, b int, key(a)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than maxvalue)`)
	setupTK.MustExec("insert into fix_partition values (1, 10), (11, 20), (21, 30)")
	setupTK.MustExec("analyze table fix_partition")
	setupTK.MustExec("create table fix_generated(a int, b int as (a + 1), key(a))")
	setupTK.MustExec("insert into fix_generated(a) values (1), (2), (3)")

	testCases := []struct {
		name        string
		fixControl  string
		reason      string
		firstSQL    string
		hitSQL      string
		rejectedSQL string
		restoredSQL string
	}{
		{
			name:        "Fix44823",
			fixControl:  "44823:1",
			reason:      "query has too many constants",
			firstSQL:    "select a from fix_plain where a > 0 and a < 4 order by a",
			hitSQL:      "select a from fix_plain where a > 1 and a < 5 order by a",
			rejectedSQL: "select a from fix_plain where a > 2 and a < 5 order by a",
			restoredSQL: "select a from fix_plain where a > 0 and a < 3 order by a",
		},
		{
			name:        "Fix33031",
			fixControl:  "33031:ON",
			reason:      "Fix33031 fix-control set and partitioned table",
			firstSQL:    "select b from fix_partition where a > 0 order by b",
			hitSQL:      "select b from fix_partition where a > 10 order by b",
			rejectedSQL: "select b from fix_partition where a > 20 order by b",
			restoredSQL: "select b from fix_partition where a > 1 order by b",
		},
		{
			name:        "Fix45798",
			fixControl:  "45798:OFF",
			reason:      "query accesses generated columns is un-cacheable",
			firstSQL:    "select b from fix_generated where a > 0 order by b",
			hitSQL:      "select b from fix_generated where a > 1 order by b",
			rejectedSQL: "select b from fix_generated where a > 2 order by b",
			restoredSQL: "select b from fix_generated where a > 0 order by b",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baselineTK.MustExec("set tidb_partition_prune_mode=dynamic")
			firstBaseline := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.firstSQL)
			hitBaseline := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.hitSQL)
			rejectedBaseline := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.rejectedSQL)
			restoredBaseline := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.restoredSQL)

			tk := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
			tk.MustExec("set tidb_partition_prune_mode=dynamic")
			first, paramSQL := observeUnifiedPlanCacheQueryAndParamSQL(t, tk, testCase.firstSQL)
			require.False(t, first.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, firstBaseline, first, "initial cache miss")
			carrier := tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL)
			require.NotNil(t, carrier)

			hit := observeUnifiedPlanCacheQuery(t, tk, testCase.hitSQL)
			require.True(t, hit.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, hitBaseline, hit, "initial cache hit")

			tk.MustExec("set tidb_opt_fix_control='" + testCase.fixControl + "'")
			require.Same(t, carrier, tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL))
			unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
			before := promtestutils.ToFloat64(unsupported)
			rejected := observeUnifiedPlanCacheQuery(t, tk, testCase.rejectedSQL)
			require.False(t, rejected.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, rejectedBaseline, rejected, "fix-control rejection")
			require.Equal(t, before+1, promtestutils.ToFloat64(unsupported))

			beforeExplain := promtestutils.ToFloat64(unsupported)
			tk.MustExec("explain format='plan_cache' " + testCase.rejectedSQL)
			require.Equal(t, beforeExplain+1, promtestutils.ToFloat64(unsupported))
			warnings := tk.MustQuery("show warnings").Rows()
			require.Len(t, warnings, 1)
			require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: "+testCase.reason)

			tk.MustExec("set tidb_opt_fix_control=''")
			require.Same(t, carrier, tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL))
			restored := observeUnifiedPlanCacheQuery(t, tk, testCase.restoredSQL)
			require.True(t, restored.cacheHit)
			requireUnifiedPlanCacheQueryEqual(t, restoredBaseline, restored, "restored fix-control cache hit")
		})
	}
}

func TestNonPreparedPlanCacheUnifiedStatementLRUHitAndCheckerReject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table lru_generated(a int, b int as (a + 1), key(a))")
	tk.MustExec("insert into lru_generated(a) values (1), (2), (3)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")

	queries := []string{
		"with source as (select a, b from lru_generated) select b from source where a > 0 order by b",
		"with source as (select a, b from lru_generated) select b from source where a > 1 order by b",
		"with source as (select a, b from lru_generated) select b from source where a > 2 order by b",
	}
	executeWithLookup := func(sql string) (unifiedPlanCacheQueryObservation, []bool) {
		lookups := make([]bool, 0, 1)
		ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedStmtLookup{}, func(found bool) {
			lookups = append(lookups, found)
		})
		observation := observeUnifiedPlanCacheQueryWithContext(ctx, t, tk, sql)
		require.Len(t, lookups, 1, "statement carrier lookup count")
		return observation, lookups
	}

	first, firstLookups := executeWithLookup(queries[0])
	require.Equal(t, []bool{false}, firstLookups)
	require.False(t, first.cacheHit)
	require.Equal(t, [][]string{{"2"}, {"3"}, {"4"}}, first.rows)

	hit, hitLookups := executeWithLookup(queries[1])
	require.Equal(t, []bool{true}, hitLookups)
	require.True(t, hit.cacheHit)
	require.Equal(t, [][]string{{"3"}, {"4"}}, hit.rows)

	tk.MustExec("set tidb_opt_fix_control='45798:OFF'")
	unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
	before := promtestutils.ToFloat64(unsupported)
	rejected, rejectedLookups := executeWithLookup(queries[2])
	require.Equal(t, []bool{true}, rejectedLookups)
	require.False(t, rejected.cacheHit)
	require.Equal(t, [][]string{{"4"}}, rejected.rows)
	require.Equal(t, before+1, promtestutils.ToFloat64(unsupported))

	tk.MustExec("explain format='plan_cache' " + queries[2])
	warnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: query accesses generated columns is un-cacheable")

	tk.MustExec("set tidb_opt_fix_control=''")
	restored, restoredLookups := executeWithLookup(queries[1])
	require.Equal(t, []bool{true}, restoredLookups)
	require.True(t, restored.cacheHit)
	require.Equal(t, hit.rows, restored.rows)
}

func TestNonPreparedPlanCacheUnifiedLegacyBehaviorMatrix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table legacy_matrix_t(a int, b int, key(a))")
	setupTK.MustExec("create table legacy_matrix_t2(a int, b int, key(a))")
	setupTK.MustExec("create table legacy_matrix_t3(a int, b int, key(a))")
	setupTK.MustExec("insert into legacy_matrix_t values (1, 1), (2, 2), (3, 3)")
	setupTK.MustExec("insert into legacy_matrix_t2 select * from legacy_matrix_t")
	setupTK.MustExec("insert into legacy_matrix_t3 select * from legacy_matrix_t")

	testCases := []struct {
		name        string
		first       string
		second      string
		reason      string
		metricDelta float64
	}{
		{
			name:        "window",
			first:       "select a, sum(b) over (order by a rows 1 preceding) from legacy_matrix_t where b > 1",
			second:      "select a, sum(b) over (order by a rows 1 preceding) from legacy_matrix_t where b > 2",
			reason:      "query has some unsupported Node",
			metricDelta: 2,
		},
		{
			name:        "cte",
			first:       "with source as (select a from legacy_matrix_t where b > 1) select a from source where a > 0 order by a",
			second:      "with source as (select a from legacy_matrix_t where b > 2) select a from source where a > 0 order by a",
			reason:      "query has some unsupported Node",
			metricDelta: 2,
		},
		{
			name:        "set operation",
			first:       "select a from legacy_matrix_t where b > 1 union select a from legacy_matrix_t where b < 3 order by 1",
			second:      "select a from legacy_matrix_t where b > 2 union select a from legacy_matrix_t where b < 2 order by 1",
			reason:      "not a SELECT statement",
			metricDelta: 0,
		},
		{
			name:        "three table join",
			first:       "select legacy_matrix_t.a from legacy_matrix_t join legacy_matrix_t2 on legacy_matrix_t.a=legacy_matrix_t2.a join legacy_matrix_t3 on legacy_matrix_t2.a=legacy_matrix_t3.a where legacy_matrix_t.b > 1",
			second:      "select legacy_matrix_t.a from legacy_matrix_t join legacy_matrix_t2 on legacy_matrix_t.a=legacy_matrix_t2.a join legacy_matrix_t3 on legacy_matrix_t2.a=legacy_matrix_t3.a where legacy_matrix_t.b > 2",
			reason:      "queries that have more than 2 tables are not supported",
			metricDelta: 0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
			baselineFirst := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.first)
			baselineSecond := observeUnifiedPlanCacheQuery(t, baselineTK, testCase.second)

			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
			tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=off")
			tk.MustExec("set tidb_enable_plan_cache_for_subquery=on")
			tk.MustExec("set tidb_enable_plan_cache_for_param_limit=on")

			unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
			before := promtestutils.ToFloat64(unsupported)
			lookupEvents := make([]bool, 0, 2)
			lookupCtx := func() context.Context {
				return context.WithValue(context.Background(), plannercore.PlanCacheKeyTestNonPreparedStmtLookup{}, func(found bool) {
					lookupEvents = append(lookupEvents, found)
				})
			}
			first := tk.MustQueryWithContext(lookupCtx(), testCase.first)
			require.Equal(t, fmt.Sprint(baselineFirst.rows), fmt.Sprint(first.Rows()))
			require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
			require.Empty(t, tk.MustQuery("show warnings").Rows())
			second := tk.MustQueryWithContext(lookupCtx(), testCase.second)
			require.Equal(t, fmt.Sprint(baselineSecond.rows), fmt.Sprint(second.Rows()))
			require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
			require.Empty(t, tk.MustQuery("show warnings").Rows())
			require.Empty(t, lookupEvents, "legacy bypass must not perform unified statement-LRU lookup")
			require.Equal(t, before+testCase.metricDelta, promtestutils.ToFloat64(unsupported), "legacy metric boundary")

			tk.MustExec("explain format='plan_cache' " + testCase.first)
			warnings := tk.MustQuery("show warnings").Rows()
			require.Len(t, warnings, 1)
			require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: "+testCase.reason)
		})
	}
}

func TestNonPreparedPlanCacheUnifiedViewSystemTemporaryTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table view_base(a int, b int, key(a))")
	setupTK.MustExec("insert into view_base values (1, 10), (2, 20), (3, 30)")
	setupTK.MustExec("create view unified_view as select a, b from view_base")
	setupTK.MustExec("create table system_base(a int)")

	t.Run("view caches and invalidates on schema change", func(t *testing.T) {
		query := [3]string{
			"select a, b from unified_view where a > 0 order by a",
			"select a, b from unified_view where a > 1 order by a",
			"select a, b from unified_view where a > 2 order by a",
		}
		baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
		baseline := [3]unifiedPlanCacheQueryObservation{
			observeUnifiedPlanCacheQuery(t, baselineTK, query[0]),
			observeUnifiedPlanCacheQuery(t, baselineTK, query[1]),
			observeUnifiedPlanCacheQuery(t, baselineTK, query[2]),
		}

		tk := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
		first, paramSQL := observeUnifiedPlanCacheQueryAndParamSQL(t, tk, query[0])
		require.False(t, first.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[0], first, "view initial miss")
		carrier := tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL)
		require.NotNil(t, carrier)

		hit := observeUnifiedPlanCacheQuery(t, tk, query[1])
		require.True(t, hit.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[1], hit, "view cache hit")

		tk.MustExec("alter table view_base add column c int")
		schemaMiss := observeUnifiedPlanCacheQuery(t, tk, query[2])
		require.False(t, schemaMiss.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[2], schemaMiss, "view schema-change miss")
		require.NotNil(t, tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL))

		postSchemaHit := observeUnifiedPlanCacheQuery(t, tk, query[1])
		require.True(t, postSchemaHit.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[1], postSchemaHit, "view post-schema cache hit")
	})

	t.Run("system table is rejected by physical checker", func(t *testing.T) {
		query := [2]string{
			"select table_name from information_schema.columns where table_schema = 'test' and table_name = 'system_base'",
			"select table_name from information_schema.columns where table_schema = 'test' and table_name = 'missing_base'",
		}
		baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
		baseline := [2]unifiedPlanCacheQueryObservation{
			observeUnifiedPlanCacheQuery(t, baselineTK, query[0]),
			observeUnifiedPlanCacheQuery(t, baselineTK, query[1]),
		}

		tk := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
		before := promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter())
		first, paramSQL := observeUnifiedPlanCacheQueryAndParamSQL(t, tk, query[0])
		require.False(t, first.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[0], first, "system table initial execution")
		carrier, ok := tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL).(*plannercore.PlanCacheStmt)
		require.True(t, ok)
		// Physical-plan rejection must not turn the reusable statement carrier
		// into an uncacheable carrier; the physical checker runs again per use.
		require.True(t, carrier.StmtCacheable)
		require.Equal(t, before, promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter()))

		second := observeUnifiedPlanCacheQuery(t, tk, query[1])
		require.False(t, second.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[1], second, "system table second execution")
		require.Equal(t, before, promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter()))

		tk.MustExec("explain format='plan_cache' " + query[0])
		warnings := tk.MustQuery("show warnings").Rows()
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: PhysicalMemTable plan is un-cacheable")
	})

	t.Run("temporary table is rejected by AST checker", func(t *testing.T) {
		tk := newUnifiedPlanCacheMatrixTestKit(t, store, "non-prepared")
		tk.MustExec("create temporary table unified_tmp(a int, key(a))")
		tk.MustExec("insert into unified_tmp values (1), (2)")
		query := [2]string{
			"select a from unified_tmp where a = 1",
			"select a from unified_tmp where a = 2",
		}
		baselineTK := newUnifiedPlanCacheMatrixTestKit(t, store, "cache-off")
		baselineTK.MustExec("create temporary table unified_tmp(a int, key(a))")
		baselineTK.MustExec("insert into unified_tmp values (1), (2)")
		baseline := [2]unifiedPlanCacheQueryObservation{
			observeUnifiedPlanCacheQuery(t, baselineTK, query[0]),
			observeUnifiedPlanCacheQuery(t, baselineTK, query[1]),
		}
		before := promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter())
		first, paramSQL := observeUnifiedPlanCacheQueryAndParamSQL(t, tk, query[0])
		require.False(t, first.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[0], first, "temporary table initial execution")
		require.Nil(t, tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL))
		require.Equal(t, before+1, promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter()))

		second := observeUnifiedPlanCacheQuery(t, tk, query[1])
		require.False(t, second.cacheHit)
		requireUnifiedPlanCacheQueryEqual(t, baseline[1], second, "temporary table second execution")
		require.Equal(t, before+2, promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter()))

		tk.MustExec("explain format='plan_cache' " + query[0])
		warnings := tk.MustQuery("show warnings").Rows()
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: query accesses temporary tables is un-cacheable")
	})
}

func TestNonPreparedPlanCacheUnifiedDoesNotCacheUncacheableCarrier(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table tp (a int, b int) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than maxvalue)`)
	tk.MustExec("insert into tp values (1, 10), (11, 20)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustExec("set tidb_partition_prune_mode=dynamic")
	tk.MustExec("set tidb_opt_enable_selected_partition_stats=on")

	stmt, err := parser.New().ParseOneStmt("select b from tp where a > 0", "", "")
	require.NoError(t, err)
	paramResult, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(tk.Session().GetPlanCtx(), stmt)
	require.NoError(t, err)
	require.True(t, supported, reason)

	carrierBefore := promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter())
	tk.MustQuery("select b from tp where a > 0").Sort().Check(testkit.Rows("10", "20"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.Nil(t, tk.Session().GetSessionVars().GetNonPreparedPlanCacheStmt(paramResult.ParamSQL))
	carrierRejected := promtestutils.ToFloat64(core_metrics.GetNonPrepPlanCacheUnsupportedCounter())
	require.Equal(t, carrierBefore+1, carrierRejected)

	tk.MustExec("explain format='plan_cache' select b from tp where a > 1")
	warnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0][2], "skip non-prepared plan-cache: static partition prune mode used")
}

func TestNonPreparedPlanCacheUnifiedUnsupportedMetricBoundaries(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t2(a int, b int, key(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("insert into t2 select * from t")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=off")
	tk.MustExec("set tidb_opt_enable_selected_partition_stats=on")

	unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
	before := promtestutils.ToFloat64(unsupported)
	tk.MustExec("insert into t values (3, 3)")
	require.Equal(t, before, promtestutils.ToFloat64(unsupported), "DML gate must not count")

	tk.MustQuery("select _utf8mb4'a' from t where a=1").Check(testkit.Rows("a"))
	precheckRejected := promtestutils.ToFloat64(unsupported)
	require.Equal(t, before+1, precheckRejected, "parameterization precheck must count once")

	tk.MustQuery("select /*+ ignore_plan_cache() */ a from t where a=1").Check(testkit.Rows("1"))
	astRejected := promtestutils.ToFloat64(unsupported)
	require.Equal(t, precheckRejected+1, astRejected, "public AST checker must count once")

	tk.MustExec(`create table tp_metric (a int, b int) partition by range (a) (
		partition p0 values less than (10), partition p1 values less than maxvalue)`)
	tk.MustExec("insert into tp_metric values (1, 1), (11, 11)")
	tk.MustExec("set tidb_partition_prune_mode=static")
	tk.MustQuery("select b from tp_metric where a > 0").Sort().Check(testkit.Rows("1", "11"))
	carrierRejected := promtestutils.ToFloat64(unsupported)
	require.Equal(t, astRejected+1, carrierRejected, "carrier rejection must count once")

	tk.MustExec("set tidb_enable_plan_cache_for_subquery=on")
	physicalQuery := "select * from t t1 where t1.a > (select max(t2.a) from t t2 where t2.b < t1.b and t2.b < 2)"
	physicalBefore := promtestutils.ToFloat64(unsupported)
	tk.MustQuery(physicalQuery)
	require.Equal(t, physicalBefore, promtestutils.ToFloat64(unsupported), "physical rejection must not duplicate unsupported count")

	tk.MustExec("explain format='plan_cache' " + physicalQuery)
	warnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0][2], "skip non-prepared plan-cache:")
	require.Contains(t, warnings[0][2], "PhysicalApply plan is un-cacheable")
}

func TestNonPreparedPlanCacheUnifiedReasonPriority(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_reason(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")
	query := "select _utf8mb4'a' into @reason_value from t_reason where a=1"

	// Legacy mode remains the compatibility path; its reason is intentionally
	// different from the unified parameterization-priority reason.
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=off")
	tk.MustExec("explain format='plan_cache' " + query)
	legacyWarnings := tk.MustQuery("show warnings").Rows()
	require.NotEmpty(t, legacyWarnings)
	for _, warning := range legacyWarnings {
		require.Contains(t, warning[2], "skip non-prepared plan-cache:")
	}

	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=on")
	tk.MustExec("explain format='plan_cache' " + query)
	unifiedWarnings := tk.MustQuery("show warnings").Rows()
	require.NotEmpty(t, unifiedWarnings)
	require.Contains(t, unifiedWarnings[0][2], "skip non-prepared plan-cache: SELECT INTO is not supported")

	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=off")
	precheckCases := []struct {
		name           string
		sql            string
		expectedReason string
	}{
		{
			name:           "charset beats limit",
			sql:            "select _utf8mb4'a' from t_reason limit 1",
			expectedReason: "query has values with under-score charset that cannot be preserved safely",
		},
	}
	values := make([]string, 201)
	for i := range values {
		values[i] = fmt.Sprintf("%d", i)
	}
	precheckCases = append(precheckCases, struct {
		name           string
		sql            string
		expectedReason string
	}{
		name:           "literal limit beats charset and limit clause",
		sql:            "select _utf8mb4'a', " + strings.Join(values, ", ") + " from t_reason limit 1",
		expectedReason: "query has too many constants",
	})
	for _, testCase := range precheckCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(testCase.sql, "", "")
			require.NoError(t, err)
			_, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(tk.Session().GetPlanCtx(), stmt)
			require.NoError(t, err)
			require.False(t, supported)
			require.Equal(t, testCase.expectedReason, reason)
		})
	}

	unsupported := core_metrics.GetNonPrepPlanCacheUnsupportedCounter()
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=off")
	before := promtestutils.ToFloat64(unsupported)
	tk.MustExec("explain format='plan_cache' select _utf8mb4'a' from t_reason where a=1 for update")
	dmlWarnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, dmlWarnings, 1)
	require.Contains(t, dmlWarnings[0][2], "skip non-prepared plan-cache: not a SELECT statement")
	require.Equal(t, before, promtestutils.ToFloat64(unsupported), "DML entry gate must not increment unsupported metric")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=on")

	before = promtestutils.ToFloat64(unsupported)
	tk.MustQuery("select /*+ ignore_plan_cache() */ a from t_reason where a=1")
	require.Equal(t, before+1, promtestutils.ToFloat64(unsupported), "AST checker rejection must increment unsupported metric")
	tk.MustExec("explain format='plan_cache' select /*+ ignore_plan_cache() */ a from t_reason where a=1")
	hintWarnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, hintWarnings, 1)
	require.Contains(t, hintWarnings[0][2], "skip non-prepared plan-cache: ignore plan cache by hint")
	require.GreaterOrEqual(t, promtestutils.ToFloat64(unsupported), before+2, "AST checker rejection must increment unsupported metric")

	tk.MustExec(`create table priority_partition (a int) partition by range (a) (
		partition p0 values less than (10), partition p1 values less than maxvalue)`)
	tk.MustExec("insert into priority_partition values (1), (11)")
	tk.MustExec("set tidb_partition_prune_mode=dynamic")
	tk.MustExec("set tidb_opt_enable_selected_partition_stats=on")
	before = promtestutils.ToFloat64(unsupported)
	tk.MustExec("explain format='plan_cache' select a from priority_partition where a > 0")
	physicalWarnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, physicalWarnings, 1)
	require.Contains(t, physicalWarnings[0][2], "skip non-prepared plan-cache: static partition prune mode used")
	require.GreaterOrEqual(t, promtestutils.ToFloat64(unsupported), before+1, "carrier rejection must increment unsupported metric")
}

func TestNonPreparedPlanCacheInternalSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	tk.Session().GetSessionVars().InRestrictedSQL = true
	tk.MustExecWithContext(ctx, "select * from t where a=1")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.Session().GetSessionVars().InRestrictedSQL = false
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPreparedPlanCachePlanSelectionRegressions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	runPreparedPlanCacheGroupByParamProjection(t, tk)
	runPreparedPlanCacheRedactExplain(t, tk)
	runPreparedPlanCacheIndexHintRangeScan(t, tk)
	runPreparedPlanCacheInvalidRange(t, tk)
	runPreparedPlanCacheLeftJoinRangeScan(t, tk)
	runPreparedPlanCacheInlJoinRangeScan(t, tk)
	runPreparedPlanCachePointGetSafety(t, tk)
}

func TestPreparedPlanCacheWarningRegressions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	runPreparedPlanCacheDisableEnable(t, tk)
	runPreparedPlanCacheLimitWarning(t, tk)
	runPreparedPlanCacheTypeConversionWarning(t, tk)
	runPreparedPlanCacheIndexRangeTypeWarning(t, tk)
	runPreparedPlanCacheConvFunction(t, tk)
	runPreparedPlanCacheForUpdateInTxn(t, tk)
}

func TestPreparedPlanCacheBatchPointGetEqAndInFixControl(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	fixControl := tk.MustQuery("select @@session.tidb_opt_fix_control").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.tidb_opt_fix_control='%v'", fixControl))
	}()

	tk.MustExec("set @@tidb_opt_fix_control = '44830:ON'")

	checkBatchPointGetExplain := func() {
		t.Helper()
		tkProcess := tk.Session().ShowProcess()
		ps := []*sessmgr.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
		require.NotEmpty(t, rows)
		require.Contains(t, fmt.Sprint(rows[0][0]), "Batch_Point_Get")
	}

	tk.MustExec("drop table if exists t_eq_in_batch_point_get")
	tk.MustExec("create table t_eq_in_batch_point_get (id int, coin varchar(32), primary key (id, coin))")
	tk.MustExec("insert into t_eq_in_batch_point_get values (1, '1'), (1, '2'), (2, '1'), (2, '2')")

	// issue:67852
	tk.MustExec("prepare st from 'select * from t_eq_in_batch_point_get where id=? and coin in (?, ?)'")
	tk.MustExec("set @a=1, @b='1', @c='2'")
	tk.MustQuery("execute st using @a, @b, @c").Sort().Check(testkit.Rows("1 1", "1 2"))
	checkBatchPointGetExplain()

	tk.MustQuery("execute st using @a, @b, @c").Sort().Check(testkit.Rows("1 1", "1 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Verify cache hit with changed EQ param
	tk.MustExec("set @a=2, @b='1', @c='2'")
	tk.MustQuery("execute st using @a, @b, @c").Sort().Check(testkit.Rows("2 1", "2 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Verify cache recovers after the duplicate-IN miss
	tk.MustExec("set @a=1, @b='1', @c='2'")
	tk.MustQuery("execute st using @a, @b, @c").Sort().Check(testkit.Rows("1 1", "1 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set @a=1, @b='1', @c='1'")
	tk.MustQuery("execute st using @a, @b, @c").Check(testkit.Rows("1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("deallocate prepare st")

	tk.MustExec("drop table if exists t_eq_in_batch_point_get_abc")
	tk.MustExec("create table t_eq_in_batch_point_get_abc (a int, b int, c int, primary key (a, b, c))")
	tk.MustExec("insert into t_eq_in_batch_point_get_abc values (1, 1, 1), (2, 1, 1), (1, 2, 1), (1, 3, 1)")

	// `IN` at the beginning of the key.
	tk.MustExec("prepare st_begin from 'select * from t_eq_in_batch_point_get_abc where a in (?, ?) and b=? and c=?'")
	tk.MustExec("set @a1=1, @a2=2, @b1=1, @c1=1")
	tk.MustQuery("execute st_begin using @a1, @a2, @b1, @c1").Sort().Check(testkit.Rows("1 1 1", "2 1 1"))
	checkBatchPointGetExplain()
	tk.MustQuery("execute st_begin using @a1, @a2, @b1, @c1").Sort().Check(testkit.Rows("1 1 1", "2 1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @a1=1, @a2=1, @b1=1, @c1=1")
	tk.MustQuery("execute st_begin using @a1, @a2, @b1, @c1").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("deallocate prepare st_begin")

	// `IN` in the middle of the key.
	tk.MustExec("prepare st_middle from 'select * from t_eq_in_batch_point_get_abc where a=? and b in (?, ?) and c=?'")
	tk.MustExec("set @a3=1, @b2=2, @b3=3, @c2=1")
	tk.MustQuery("execute st_middle using @a3, @b2, @b3, @c2").Sort().Check(testkit.Rows("1 2 1", "1 3 1"))
	checkBatchPointGetExplain()
	tk.MustQuery("execute st_middle using @a3, @b2, @b3, @c2").Sort().Check(testkit.Rows("1 2 1", "1 3 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @a3=1, @b2=2, @b3=2, @c2=1")
	tk.MustQuery("execute st_middle using @a3, @b2, @b3, @c2").Check(testkit.Rows("1 2 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("deallocate prepare st_middle")
}

func runPreparedPlanCacheGroupByParamProjection(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tableName := "t_group_by_param"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s(id int, col int)", tableName))
	tk.MustExec(fmt.Sprintf(`prepare stmt from "select id, ? as col1 from %s where col=? group by id,col1"`, tableName))
	tk.MustExec(`set @a=100, @b=100`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows()) // no error
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
	tk.MustExec(`deallocate prepare stmt`)
}

func runPreparedPlanCacheRedactExplain(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	preparedCache := tk.MustQuery("select @@session.tidb_enable_prepared_plan_cache").Rows()[0][0]
	execInfo := tk.MustQuery("select @@global.tidb_enable_collect_execution_info").Rows()[0][0]
	advancedJoinHint := tk.MustQuery("select @@session.tidb_opt_advanced_join_hint").Rows()[0][0]
	redactLog := tk.MustQuery("select @@global.tidb_redact_log").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.tidb_enable_prepared_plan_cache=%v", preparedCache))
		tk.MustExec(fmt.Sprintf("set @@global.tidb_enable_collect_execution_info=%v", execInfo))
		tk.MustExec(fmt.Sprintf("set @@session.tidb_opt_advanced_join_hint=%v", advancedJoinHint))
		tk.MustExec(fmt.Sprintf("set global tidb_redact_log='%v'", redactLog))
	}()
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@global.tidb_enable_collect_execution_info=0")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int, b int, c int, index idx(a, b))")
	tk.MustExec("prepare stmt1 from 'select /*+ inl_join(t2) */ * from t1 join t2 on t1.a = t2.a where t2.b in (?, ?, ?)'")
	tk.MustExec("set @a = 10, @b = 20, @c = 30, @d = 40, @e = 50, @f = 60")
	tk.MustExec("execute stmt1 using @a, @b, @c")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_redact_log=MARKER")
	tk.MustExec("execute stmt1 using @d, @e, @f")
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows(
		"IndexJoin_11 37.46 root  inner join, inner:IndexLookUp_28, outer key:test.t1.a, inner key:test.t2.a, equal cond:eq(test.t1.a, test.t2.a)",
		"├─TableReader_24(Build) 9990.00 root  data:Selection_23",
		"│ └─Selection_23 9990.00 cop[tikv]  not(isnull(test.t1.a))",
		"│   └─TableFullScan_22 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─IndexLookUp_28(Probe) 37.46 root  ",
		"  ├─Selection_27(Build) 37.46 cop[tikv]  not(isnull(test.t2.a))",
		"  │ └─IndexRangeScan_25 37.50 cop[tikv] table:t2, index:idx(a, b) range: decided by [eq(test.t2.a, test.t1.a) in(test.t2.b, ‹40›, ‹50›, ‹60›)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan_26(Probe) 37.46 cop[tikv] table:t2 keep order:false, stats:pseudo"))
	tk.MustExec(`deallocate prepare stmt1`)
}

func runPreparedPlanCacheIndexHintRangeScan(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tableName := "t_index_hint_range"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (a int, key (a))", tableName))
	tk.MustExec(fmt.Sprintf(`prepare st from "select /*+ use_index(%s, a) */ a from %s where a=? and a=?"`, tableName, tableName))
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a, @a`)
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.True(t, strings.Contains(plan[1][0].(string), "RangeScan")) // range-scan instead of full-scan

	tk.MustExec(`execute st using @a, @a`)
	tk.MustExec(`execute st using @a, @a`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`deallocate prepare st`)
}

func runPreparedPlanCacheInvalidRange(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tableName := "t_invalid_range"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (a int, key(a))", tableName))
	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s where a>? and a<?'", tableName))
	tk.MustExec("set @l=100, @r=10")
	tk.MustExec("execute st using @l, @r")

	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]any{{"TableDual_5"}}) // use TableDual directly instead of TableFullScan

	tk.MustExec("execute st using @l, @r")
	tk.MustExec("execute st using @l, @r")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("deallocate prepare st")
}

func runPreparedPlanCacheDisableEnable(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	origVal := tk.MustQuery("select @@session.tidb_enable_prepared_plan_cache").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.tidb_enable_prepared_plan_cache=%v", origVal))
	}()
	tableName := "t_prepared_toggle"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf(`create table %s(a int)`, tableName))
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(fmt.Sprintf(`prepare s from "select * from %s"`, tableName))
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=0`)
	tk.MustExec(`execute s`) // no error
	tk.MustExec(`deallocate prepare s`)
}

func runPreparedPlanCacheLeftJoinRangeScan(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	t1Name := "t_left_join_outer"
	t2Name := "t_left_join_inner"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", t1Name))
	tk.MustExec(fmt.Sprintf("drop table if exists %s", t2Name))
	tk.MustExec(fmt.Sprintf("create table %s (a int, b int)", t1Name))
	tk.MustExec(fmt.Sprintf("create table %s (a int, b int, key(b, a))", t2Name))
	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s left join %s on %s.a=%s.a where %s.b in (?)'",
		t1Name, t2Name, t1Name, t2Name, t2Name))
	tk.MustExec("set @b=1")
	tk.MustExec("execute st using @b")

	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]any{
			{"Projection_10"},
			{"└─HashJoin_12"},
			{"  ├─IndexReader_26(Build)"},
			{"  │ └─IndexRangeScan_25"}, // RangeScan instead of FullScan
			{"  └─TableReader_32(Probe)"},
			{"    └─Selection_31"},
			{"      └─TableFullScan_30"},
		})

	tk.MustExec("execute st using @b")
	tk.MustExec("execute st using @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("deallocate prepare st")
}

func runPreparedPlanCacheInlJoinRangeScan(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	advancedJoinHint := tk.MustQuery("select @@session.tidb_opt_advanced_join_hint").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.tidb_opt_advanced_join_hint=%v", advancedJoinHint))
	}()
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	itemName := "t_inlj_item"
	lvName := "t_inlj_lv"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", itemName))
	tk.MustExec(fmt.Sprintf("drop table if exists %s", lvName))
	tk.MustExec(fmt.Sprintf("CREATE TABLE %s (`id` int, `vid` varbinary(16), `sid` int)", itemName))
	tk.MustExec(fmt.Sprintf("CREATE TABLE %s (`item_id` int, `sid` int, KEY (`sid`,`item_id`))", lvName))

	tk.MustExec(fmt.Sprintf("prepare stmt from 'SELECT /*+ TIDB_INLJ(%s, %s) */ * FROM %s LEFT JOIN %s ON %s.sid = %s.sid AND %s.item_id = %s.id WHERE %s.sid = ? AND %s.vid IN (?, ?)'",
		lvName, itemName, lvName, itemName, lvName, itemName, lvName, itemName, itemName, itemName))
	tk.MustExec("set @a=1, @b='1', @c='3'")
	tk.MustExec("execute stmt using @a, @b, @c")

	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain format='brief' for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]any{
			{"IndexJoin"},
			{"├─TableReader(Build)"},
			{"│ └─Selection"},
			{"│   └─TableFullScan"}, // RangeScan instead of FullScan
			{"└─IndexReader(Probe)"},
			{"  └─Selection"},
			{"    └─IndexRangeScan"},
		})

	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("deallocate prepare stmt")
}

func runPreparedPlanCacheLimitWarning(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	fixControl := tk.MustQuery("select @@session.tidb_opt_fix_control").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.tidb_opt_fix_control='%v'", fixControl))
	}()
	tableName := "t_limit_warning"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (a int, key(a))", tableName))
	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from %s limit ?'`, tableName))
	tk.MustExec(`set @a=100000`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip prepared plan-cache: limit count is too large`))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustExec(`set @@tidb_opt_fix_control = "49736:ON"`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 force plan-cache: may use risky cached plan: limit count is too large`))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`deallocate prepare st`)
}

func runPreparedPlanCacheTypeConversionWarning(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tableName := "t_type_convert"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (a int, key(a))", tableName))
	tk.MustExec(fmt.Sprintf("prepare st from 'select a from %s where a in (?, ?)'", tableName))
	tk.MustExec("set @a=1.0, @b=2.0")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '1.0' may be converted to INT"))
	tk.MustExec("execute st using @a, @b")
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain format='brief' for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]any{
			{"IndexReader"},
			{"└─IndexRangeScan"}, // range scan not full scan
		})

	tk.MustExec("set @a=1, @b=2")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows()) // no warning for INT values
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // cacheable for INT
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery(fmt.Sprintf("explain format='brief' for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]any{
			{"IndexReader"},
			{"└─IndexRangeScan"}, // range scan not full scan
		})
	tk.MustExec("deallocate prepare st")
}

func runPreparedPlanCacheIndexRangeTypeWarning(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tableName := "t_range_type"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (a int, key(a))", tableName))
	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s use index(a) where a < ?'", tableName))
	tk.MustExec("set @a1=1.1")
	tk.MustExec("execute st using @a1")

	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.True(t, strings.Contains(rows[1][0].(string), "RangeScan")) // RangeScan not FullScan

	tk.MustExec("execute st using @a1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '1.1' may be converted to INT"))
	tk.MustExec("deallocate prepare st")
}

func TestPlanCacheWithLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")

	testCases := []struct {
		sql    string
		params []int
	}{
		{"prepare stmt from 'select * from t limit ?'", []int{1}},
		{"prepare stmt from 'select * from t limit 1, ?'", []int{1}},
		{"prepare stmt from 'select * from t limit ?, 1'", []int{1}},
		{"prepare stmt from 'select * from t limit ?, ?'", []int{1, 2}},
		{"prepare stmt from 'delete from t order by a limit ?'", []int{1}},
		{"prepare stmt from 'insert into t select * from t order by a desc limit ?'", []int{1}},
		{"prepare stmt from 'insert into t select * from t order by a desc limit ?, ?'", []int{1, 2}},
		{"prepare stmt from 'update t set a = 1 limit ?'", []int{1}},
		{"prepare stmt from '(select * from t order by a limit ?) union (select * from t order by a desc limit ?)'", []int{1, 2}},
	}

	for idx, testCase := range testCases {
		tk.MustExec(testCase.sql)
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		if idx < 9 {
			// none point get plan
			tk.MustExec("set @a0 = 6")
			tk.MustExec("execute stmt using " + strings.Join(using, ", "))
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
	}

	tk.MustExec("prepare stmt from 'select * from t limit ?'")
	tk.MustExec("set @a = 10001")
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: limit count is too large"))
}

func TestPlanCacheWithSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	testCases := []struct {
		sql            string
		params         []int
		cacheAble      string
		isDecorrelated bool
	}{
		{"select * from t t1 where exists (select 1 from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}, "1", false},      // exist
		{"select * from t t1 where t1.a in (select a from t t2 where t2.b < ?)", []int{1}, "1", false},                     // in
		{"select * from t t1 where t1.a > (select max(a) from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}, "0", false}, // scala
		{"select * from t t1 where t1.a > (select 1 from t t2 where t2.b<?)", []int{1}, "0", true},                         // uncorrelated
		{"select * from t t1 where exists (select b from t t2 where t1.a = t2.a and t2.b<? limit 1)", []int{1}, "1", false},
		{"select * from t t1 where exists (select b from t t2 where t1.a = t2.a and t2.b<? limit ?)", []int{1, 1}, "1", false},
	}

	// switch on
	for _, testCase := range testCases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", testCase.sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(testCase.cacheAble))
		if testCase.cacheAble == "0" {
			tk.MustExec("execute stmt using " + strings.Join(using, ", "))
			if testCase.isDecorrelated {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has uncorrelated sub-queries is un-cacheable"))
			} else {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: PhysicalApply plan is un-cacheable"))
			}
		}
	}
	// switch off
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = 0")
	for _, testCase := range testCases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", testCase.sql))
		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has sub-queries is un-cacheable"))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
}

func convertQueryToPrepExecStmt(q string) (normalQuery, prepStmt string, parameters []string) {
	// select ... from t where a = #?1# and b = #?2#
	normalQuery = strings.ReplaceAll(q, "#", "")
	normalQuery = strings.ReplaceAll(normalQuery, "?", "")
	vs := strings.Split(q, "#")
	for i := range vs {
		if len(vs[i]) == 0 {
			continue
		}
		if vs[i][0] == '?' {
			parameters = append(parameters, vs[i][1:])
			vs[i] = "?"
		}
	}
	return normalQuery, fmt.Sprintf(`prepare st from '%v'`, strings.Join(vs, "")), parameters
}

func planCachePointGetPrepareData(tk *testkit.TestKit) {
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`drop table if exists t2`)
	t := func() string {
		types := []string{"int", "varchar(10)", "decimal(10, 2)", "double"}
		return types[rand.Intn(len(types))]
	}
	tk.MustExec(fmt.Sprintf(`create table t1 (a %v, b %v, c %v, d %v, primary key(a), unique key(b), unique key(c), unique key(c))`, t(), t(), t(), t()))
	tk.MustExec(fmt.Sprintf(`create table t2 (a %v, b %v, c %v, d %v, primary key(a, b), unique key(c, d))`, t(), t(), t(), t()))

	vals := make([]string, 0, 50)
	for i := range 50 {
		vals = append(vals, fmt.Sprintf("('%v.%v', '%v.%v', '%v.%v', '%v.%v')",
			i-20, rand.Intn(5),
			i-20, rand.Intn(5),
			i-20, rand.Intn(5),
			i-20, rand.Intn(5)))
	}
	tk.MustExec(fmt.Sprintf(`insert into t1 values %v`, strings.Join(vals, ",")))
	tk.MustExec(`insert into t1 values ('31', '31', null, null), ('32', null, 32, null)`)
	tk.MustExec(fmt.Sprintf(`insert into t2 values %v`, strings.Join(vals, ",")))
}

func planCachePointGetQueries(isNonPrep bool, rounds int) []string {
	v := func() string {
		var vStr string
		switch rand.Intn(3) {
		case 0: // int
			vStr = fmt.Sprintf("%v", rand.Intn(50)-20)
		case 1: // double
			vStr = fmt.Sprintf("%v.%v", rand.Intn(50)-20, rand.Intn(100))
		default: // string
			vStr = fmt.Sprintf("'%v.%v'", rand.Intn(50)-20, rand.Intn(100))
		}
		if !isNonPrep {
			vStr = fmt.Sprintf("#?%v#", vStr)
		}
		return vStr
	}
	f := func() string {
		cols := []string{"a", "b", "c", "d"}
		col := cols[rand.Intn(len(cols))]
		ops := []string{"=", ">", "<", ">=", "<=", "in", "is null"}
		op := ops[rand.Intn(len(ops))]
		if op == "in" {
			return fmt.Sprintf("%v %v (%v, %v, %v)", col, op, v(), v(), v())
		} else if op == "is null" {
			return fmt.Sprintf("%v %v", col, op)
		}
		return fmt.Sprintf("%v %v %v", col, op, v())
	}
	queries := make([]string, 0, rounds*12)
	for range rounds {
		queries = append(queries, fmt.Sprintf("select * from t1 where %v", f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v", f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v and %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v and %v and %v", f(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v or %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v or %v and %v", f(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v", f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v", f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v and %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v and %v and %v", f(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v or %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v or %v and %v", f(), f(), f(), f()))
	}
	return queries
}

func planCacheIntConvertQueries(isNonPrep bool, rounds int) []string {
	cols := []string{"a", "b", "c", "d"}
	ops := []string{"=", ">", "<", ">=", "<=", "in", "is null"}
	v := func() string {
		var val string
		switch rand.Intn(3) {
		case 0:
			val = fmt.Sprintf("%v", 2000+rand.Intn(20)-10)
		case 1:
			val = fmt.Sprintf("%v.0", 2000+rand.Intn(20)-10)
		default:
			val = fmt.Sprintf("'%v'", 2000+rand.Intn(20)-10)
		}
		if !isNonPrep {
			val = fmt.Sprintf("#?%v#", val)
		}
		return val
	}
	f := func() string {
		col := cols[rand.Intn(len(cols))]
		op := ops[rand.Intn(len(ops))]
		if op == "is null" {
			return fmt.Sprintf("%v is null", col)
		} else if op == "in" {
			if rand.Intn(2) == 0 {
				return fmt.Sprintf("%v in (%v)", col, v())
			}
			return fmt.Sprintf("%v in (%v, %v, %v)", col, v(), v(), v())
		}
		return fmt.Sprintf("%v %v %v", col, op, v())
	}
	fields := func() string {
		var fs []string
		for _, f := range []string{"a", "b", "c", "d"} {
			if rand.Intn(4) == 0 {
				continue
			}
			fs = append(fs, f)
		}
		if len(fs) == 0 {
			return "*"
		}
		return strings.Join(fs, ", ")
	}
	queries := make([]string, 0, rounds*6)
	for range rounds {
		queries = append(queries, fmt.Sprintf("select %v from t where %v", fields(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v and %v", fields(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v and %v and %v", fields(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v or %v", fields(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v or %v or %v", fields(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v and %v or %v", fields(), f(), f(), f()))
	}
	return queries
}

func planCacheIntConvertPrepareData(tk *testkit.TestKit) {
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int, b year, c double, d varchar(16), key(a), key(b), key(c))`)
	vals := make([]string, 0, 50)
	for range 50 {
		a := fmt.Sprintf("%v", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			a = "null"
		}
		b := fmt.Sprintf("%v", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			b = "null"
		}
		c := fmt.Sprintf("%v.0", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			c = "null"
		}
		d := fmt.Sprintf("'%v'", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			d = "null"
		}
		vals = append(vals, fmt.Sprintf("(%s, %s, %s, %s)", a, b, c, d))
	}
	tk.MustExec("insert into t values " + strings.Join(vals, ","))
}

func planCacheIndexMergeQueries(isNonPrep bool, rounds int) []string {
	ops := []string{"=", ">", "<", ">=", "<=", "in", "mod", "is null"}
	f := func(col string) string {
		n := rand.Intn(20) - 10
		nStr := fmt.Sprintf("%v", n)
		if !isNonPrep {
			nStr = fmt.Sprintf("#?%v#", n)
		}

		op := ops[rand.Intn(len(ops))]
		if op == "in" {
			switch rand.Intn(3) {
			case 0: // 1 element
				return fmt.Sprintf("%s %s (%s)", col, op, nStr)
			case 1: // multiple same elements
				return fmt.Sprintf("%s %s (%s, %s, %s)", col, op, nStr, nStr, nStr)
			default: // multiple different elements
				if isNonPrep {
					return fmt.Sprintf("%s %s (%d, %d)", col, op, n, n+1)
				}
				return fmt.Sprintf("%s %s (#?%d#, #?%d#)", col, op, n, n+1)
			}
		} else if op == "mod" { // this filter cannot be used to build range
			return fmt.Sprintf("mod(%s, %s)=0", col, nStr)
		} else if op == "is null" {
			return fmt.Sprintf("%s %s", col, op)
		} else {
			return fmt.Sprintf("%s %s %s", col, op, nStr)
		}
	}
	fields := func() string {
		switch rand.Intn(5) {
		case 0:
			return "a"
		case 1:
			return "a, b"
		case 2:
			return "a, c"
		case 3:
			return "d"
		default:
			return "*"
		}
	}
	queries := make([]string, 0, rounds*12)
	for range rounds {
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where %s and %s", fields(), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s and %s", fields(), f("a"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s and %s and %s", fields(), f("a"), f("b"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where %s or %s", fields(), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s or %s", fields(), f("a"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s or %s or %s", fields(), f("a"), f("b"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where %s and %s and %s", fields(), f("a"), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s and %s and %s", fields(), f("a"), f("c"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s and %s and %s and %s", fields(), f("a"), f("b"), f("b"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where (%s and %s) or %s", fields(), f("a"), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s or (%s and %s)", fields(), f("a"), f("c"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s or (%s and %s) or %s", fields(), f("a"), f("b"), f("b"), f("c")))
	}
	return queries
}

func planCacheIndexMergePrepareData(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int, key(a), key(b), key(c))")
	vals := make([]string, 0, 50)
	v := func() string {
		if rand.Intn(10) == 0 {
			return "null"
		}
		return fmt.Sprintf("%d", rand.Intn(20)-10)
	}
	for range 50 {
		vals = append(vals, fmt.Sprintf("(%s, %s, %s, %s)", v(), v(), v(), v()))
	}
	tk.MustExec("insert into t values " + strings.Join(vals, ","))
}

func TestPlanCacheRandomCases(t *testing.T) {
	rounds := 20
	if testing.Short() {
		rounds = 10
	}
	store := testkit.CreateMockStore(t)
	t.Run("1", func(t *testing.T) {
		testRandomPlanCacheCases(t, store, planCacheIndexMergePrepareData, planCacheIndexMergeQueries, rounds)
	})
	t.Run("2", func(t *testing.T) {
		testRandomPlanCacheCases(t, store, planCacheIntConvertPrepareData, planCacheIntConvertQueries, rounds)
	})
	t.Run("3", func(t *testing.T) {
		testRandomPlanCacheCases(t, store, planCachePointGetPrepareData, planCachePointGetQueries, rounds)
	})
}

func testRandomPlanCacheCases(t *testing.T,
	store kv.Storage,
	prepFunc func(tk *testkit.TestKit),
	queryFunc func(isNonPrep bool, rounds int) []string,
	rounds int) {
	tk := testkit.NewTestKit(t, store)
	prepFunc(tk)

	tkNonPrepCache := testkit.NewTestKit(t, store)
	tkNonPrepCache.MustExec("use test")

	tk.MustExec("set tidb_enable_non_prepared_plan_cache=0")
	tkNonPrepCache.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	// nonprepared plan cache
	for _, q := range queryFunc(true, rounds) {
		result1 := tk.MustQuery(q).Sort()
		result2 := tkNonPrepCache.MustQuery(q).Sort()
		require.True(t, result1.Equal(result2.Rows()))

		result2 = tkNonPrepCache.MustQuery(q).Sort()
		require.True(t, result1.Equal(result2.Rows()))
	}

	// prepared plan cache
	for _, q := range queryFunc(false, rounds) {
		q, prepStmt, parameters := convertQueryToPrepExecStmt(q)
		result1 := tk.MustQuery(q).Sort()
		tk.MustExec(prepStmt)
		var xs []string
		for i, p := range parameters {
			tk.MustExec(fmt.Sprintf("set @x%d = %s", i, p))
			xs = append(xs, fmt.Sprintf("@x%d", i))
		}
		var execStmt string
		if len(xs) == 0 {
			execStmt = "execute st"
		} else {
			execStmt = fmt.Sprintf("execute st using %s", strings.Join(xs, ", "))
		}

		// first execution caches plan
		result2 := tk.MustQuery(execStmt).Sort()
		require.True(t, result1.Equal(result2.Rows()))

		// second execution uses cache
		result2 = tk.MustQuery(execStmt).Sort()
		require.True(t, result1.Equal(result2.Rows()))
	}
}

func TestPlanCacheSubquerySPMEffective(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	testCases := []struct {
		sql    string
		params []int
	}{
		{"select * from t t1 where exists (select /*/ 1 from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}}, // exist
		{"select * from t t1 where exists (select /*/ b from t t2 where t1.a = t2.a and t2.b < ? limit ?)", []int{1, 1}},
		{"select * from t t1 where t1.a in (select /*/ a from t t2 where t2.a > ? and t1.a = t2.a)", []int{1}},
		{"select * from t t1 where t1.a < (select /*/ sum(t2.a) from t t2 where t2.b = t1.b and t2.a > ?)", []int{1}},
	}

	// hint
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	tk.MustExec("deallocate prepare stmt")

	// binding before prepare
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "", 1)
		bindSQL := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec("create binding for " + sql + " using " + bindSQL)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}

	// binding after prepare
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "", 1)
		bindSQL := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("create binding for " + sql + " using " + bindSQL)
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func runPreparedPlanCachePointGetSafety(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tableName := "t_point_get_safety"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (a int, b int, c int, unique key(a, b))", tableName))

	// should use BatchPointGet
	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s where a=1 and b in (?, ?)'", tableName))
	tk.MustExec("set @a=1, @b=2")
	tk.MustExec("execute st using @a, @b")
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[0][0], "Batch_Point_Get_5") // use BatchPointGet
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: Batch/PointGet plans may be over-optimized"))

	// should use PointGet: unsafe PointGet
	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s where a=1 and b>=? and b<=?'", tableName))
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute st using @a, @b")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[0][0], "Point_Get_5") // use Point_Get_5
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // cannot hit

	// safe PointGet
	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s where a=1 and b=? and c<?'", tableName))
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute st using @a, @b")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Contains(t, rows[0][0], "Selection") // PointGet -> Selection
	require.Contains(t, rows[1][0], "Point_Get")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // can hit
	tk.MustExec("deallocate prepare st")
}

func TestNonPreparedPlanExplainWarning(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, d int, e enum('1', '2', '3'), s set('1', '2', '3'), j json, bt bit(8), key(b), key(c, d))`)
	tk.MustExec("create table t1(a int, b int, index idx_b(b)) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustExec("create table t3(a int, first_name varchar(50), last_name varchar(50), full_name varchar(101) generated always as (concat(first_name,' ',last_name)))")
	tk.MustExec("create or replace SQL SECURITY INVOKER view v as select a from t")
	tk.MustExec("analyze table t, t1, t2") // eliminate stats warnings
	tk.MustExec("set @@session.tidb_enable_non_prepared_plan_cache = 1")

	supported := []string{
		"select * from t where a<10",
		"select * from t where a<13 and b<15",
		"select * from t where b=13",
		"select * from t where c<8",
		"select * from t where d>8",
		"select * from t where c=8 and d>10",
		"select * from t where a<12 and b<13 and c<12 and d>2",
		"select * from t where a in (1, 2, 3)",
		"select * from t where a<13 or b<15",
		"select * from t where a<13 or b<15 and c=13",
		"select * from t where a in (1, 2)",
		"select * from t where a in (1, 2) and b in (1, 2, 3)",
		"select * from t where a in (1, 2) and b < 15",
		"select * from t where a between 1 and 10",
		"select * from t where a between 1 and 10 and b < 15",
		"select * from t where a+b=13",
		"select * from t where mod(a, 3)=1",
		"select * from t where d>now()",
		"select distinct a from t1 where a > 1 and b < 2",          // distinct
		"select count(*) from t1 where a > 1 and b < 2 group by a", // group by
		"select * from t1 order by a",                              // order by
		"select * from t3 where full_name = 'a b'",                 // generated column
		"select * from t3 where a > 1 and full_name = 'a b'",
		"select * from t1 where a in (1, 2)",                                 // Partitioned
		"select * from t2 where a in (1, 2) and b in (1, 2, 3)",              // Partitioned
		"select * from t1 where a in (1, 2) and b < 15",                      // Partitioned
		"select /*+ use_index(t1, idx_b) */ * from t1 where a > 1 and b < 2", // hint
	}

	unsupported := []string{
		"select a, sum(b) as c from t1 where a > 1 and b < 2 group by a having sum(b) > 1", // having
		"select * from (select * from t1) t",                                               // sub-query
		"select * from t1 where a in (select a from t)",                                    // uncorrelated sub-query
		"select * from t1 where a in (select a from t where a > t1.a)",                     // correlated sub-query
		"select * from t where j is null",                                                  // json
		"select * from t where j is not null",                                              // json
		"select * from t where j < 1",                                                      // json
		"select * from t where json_extract(j, '$.a') is not null",                         // json
		"select * from t where a > 1 and j < 1",
		"select * from t where e is null",     // enum
		"select * from t where e is not null", // enum
		"select * from t where e < '1'",       // enum
		"select * from t where a > 1 and e < '1'",
		"select * from t where s is null",     // set
		"select * from t where s is not null", // set
		"select * from t where s < '1'",       // set
		"select * from t where a > 1 and s < '1'",
		"select * from t where bt is null",     // bit
		"select * from t where bt is not null", // bit
		"select * from t where bt > 0",         // bit
		"select * from t where a > 1 and bt > 0",
		"select data_type from INFORMATION_SCHEMA.columns where table_name = 'v'", // memTable
		"select * from v",                                                         // view
		"select * from t where a = null",                                          // null
		"select * from t where false",                                             // table dual
	}

	reasons := []string{
		"skip non-prepared plan-cache: queries with HAVING clauses are not supported",
		"skip non-prepared plan-cache: queries that have sub-queries are not supported",
		"skip non-prepared plan-cache: query has some unsupported Node",
		"skip non-prepared plan-cache: query has some unsupported Node",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: access tables in system schema",
		"skip non-prepared plan-cache: queries that access views are not supported",
		"skip non-prepared plan-cache: query has null constants",
		"skip non-prepared plan-cache: some parameters may be overwritten when constant propagation",
	}

	all := append(supported, unsupported...)

	explainFormats := []string{
		types.ExplainFormatBrief,
		types.ExplainFormatDOT,
		types.ExplainFormatHint,
		types.ExplainFormatROW,
		types.ExplainFormatVerbose,
		types.ExplainFormatTraditional,
		types.ExplainFormatBinary,
		types.ExplainFormatTiDBJSON,
		types.ExplainFormatCostTrace,
	}
	// all cases no warnings use other format
	for _, q := range all {
		tk.MustExec("explain " + q)
		// tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustExec("explain " + q)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	for _, format := range explainFormats {
		for _, q := range all {
			tk.MustExec(fmt.Sprintf("explain format = '%v' %v", format, q))
			//tk.MustQuery("show warnings").Check(testkit.Rows())
			tk.MustQuery("show warnings").CheckNotContain("plan cache")
			tk.MustExec(fmt.Sprintf("explain format = '%v' %v", format, q))
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
	}

	// unsupported case with warning use 'plan_cache' format
	for idx, q := range unsupported {
		tk.MustExec("explain format = 'plan_cache'" + q)
		warn := tk.MustQuery("show warnings").Rows()[0]
		require.Equal(t, reasons[idx], warn[2], "idx: %d", idx)
	}
}

func TestNonPreparedPlanCachePanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	tk.MustExec("create table t (a varchar(255), b int, c char(10), primary key (c, a));")
	ctx := tk.Session().(sessionctx.Context)

	s := parser.New()
	for _, sql := range []string{
		"select 1 from t where a='x'",
		"select * from t where c='x'",
		"select * from t where a='x' and c='x'",
		"select * from t where a='x' and c='x' and b=1",
	} {
		stmtNode, err := s.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		nodeW := resolve.NewNodeW(stmtNode)
		err = plannercore.Preprocess(context.Background(), ctx, nodeW, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		_, _, err = planner.Optimize(context.TODO(), ctx, nodeW, preprocessorReturn.InfoSchema)
		require.NoError(t, err) // not panic
	}
}

func TestNonPreparedPlanCacheAutoStmtRetry(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int primary key, k int, UNIQUE KEY(k))")
	tk1.MustExec("insert into t values(1, 1)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk2.MustExec("use test")
	tk1.MustExec("begin")
	tk1.MustExec("update t set k=3 where id=1")

	var wg sync.WaitGroup
	var tk2Err error
	wg.Add(1)
	go func() {
		// trigger statement auto-retry on tk2
		_, tk2Err = tk2.Exec("insert into t values(3, 3)")
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	_, err := tk1.Exec("commit")
	require.NoError(t, err)
	wg.Wait()
	require.ErrorContains(t, tk2Err, "Duplicate entry")
}

func TestNonPreparedPlanCacheRegressions(t *testing.T) {
	runNonPreparedPlanCacheConcurrency(t)
	runNonPreparedPlanCacheASTMutation(t)
	runNonPreparedPlanCacheFieldNameMapping(t)
}

func runNonPreparedPlanCacheConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cycle (pk int key, val int)")
	var wg sync.WaitGroup
	concurrency := 30
	for i := range concurrency {
		tk.MustExec(fmt.Sprintf("insert into cycle values (%v,%v)", i, i))
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
			query := fmt.Sprintf("select (val) from cycle where pk = %v", id)
			for range 5000 {
				tk.MustQuery(query).Check(testkit.Rows(fmt.Sprintf("%v", id)))
			}
		}(i)
	}
	wg.Wait()
}

func runNonPreparedPlanCacheASTMutation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`create table cycle (pk int not null primary key, sk int not null, val int)`)
	tk.MustExec(`insert into cycle values (4, 4, 4)`)
	tk.MustExec(`insert into cycle values (7, 7, 7)`)

	tk.MustQuery(`select (val) from cycle where pk = 4`).Check(testkit.Rows("4"))
	tk.MustQuery(`select (val) from cycle where pk = 7`).Check(testkit.Rows("7"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	updateAST := func(stmt ast.StmtNode) {
		v := stmt.(*ast.SelectStmt).Where.(*ast.BinaryOperationExpr).R.(*driver.ValueExpr)
		v.Datum.SetInt64(7)
	}

	tctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestIssue43667{}, updateAST)
	tk.MustQueryWithContext(tctx, `select (val) from cycle where pk = 4`).Check(testkit.Rows("4"))
}

func runNonPreparedPlanCacheFieldNameMapping(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`CREATE TABLE t (id int NOT NULL, personId int NOT NULL,
      name varchar(255) NOT NULL, PRIMARY KEY (id, personId))`)
	tk.MustExec(`insert into t values (1, 1, '')`)

	cnt := 0
	checkFieldNames := func(names []*types.FieldName) {
		require.Equal(t, len(names), 2)
		require.Equal(t, names[0].String(), "test.t.user_id")
		require.Equal(t, names[1].String(), "test.t.user_personid")
		cnt += 1
	}
	tctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestIssue47133{}, checkFieldNames)
	tk.MustQueryWithContext(tctx, `SELECT id AS User_id, personId AS User_personId FROM t WHERE (id = 1 AND personId = 1)`).Check(
		testkit.Rows("1 1"))
	tk.MustQueryWithContext(tctx, `SELECT id AS User_id, personId AS User_personId FROM t WHERE (id = 1 AND personId = 1)`).Check(
		testkit.Rows("1 1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	require.Equal(t, cnt, 2)
}

func TestPlanCacheBindingIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create database test1`)
	tk.MustExec(`use test1`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`create database test2`)
	tk.MustExec(`use test2`)
	tk.MustExec(`create table t (a int)`)

	tk.MustExec(`prepare st1 from 'select * from test1.t'`)
	tk.MustExec(`execute st1`)
	tk.MustExec(`execute st1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`prepare st2 from 'select * from test2.t'`)
	tk.MustExec(`execute st2`)
	tk.MustExec(`execute st2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`create global binding using select /*+ ignore_plan_cache() */ * from test1.t`)
	tk.MustExec(`execute st1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`execute st1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`create global binding using select /*+ ignore_plan_cache() */ * from test2.t`)
	tk.MustExec(`execute st2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`execute st2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func runPreparedPlanCacheConvFunction(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tableName := "t_conv_fn"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf(`create table %s (v varchar(16))`, tableName))
	tk.MustExec(fmt.Sprintf(`insert into %s values ('156')`, tableName))
	tk.MustExec(fmt.Sprintf(`prepare stmt7 from 'select * from %s where v = conv(?, 16, 8)'`, tableName))
	tk.MustExec(`set @arg=0x6E`)
	tk.MustQuery(`execute stmt7 using @arg`).Check(testkit.Rows("156"))
	tk.MustQuery(`execute stmt7 using @arg`).Check(testkit.Rows("156"))
	tk.MustExec(`set @arg=0x70`)
	tk.MustQuery(`execute stmt7 using @arg`).Check(testkit.Rows()) // empty
	tk.MustExec(`deallocate prepare stmt7`)
}

func TestBuiltinFuncFlen(t *testing.T) {
	// same as TestIssue45378 and TestIssue45253
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1(c1 INT)`)
	tk.MustExec(`INSERT INTO t1 VALUES (1)`)

	funcs := []string{ast.Abs, ast.Acos, ast.Asin, ast.Atan, ast.Ceil, ast.Ceiling, ast.Cos,
		ast.CRC32, ast.Degrees, ast.Floor, ast.Ln, ast.Log, ast.Log2, ast.Log10, ast.Unhex,
		ast.Radians, ast.Rand, ast.Round, ast.Sign, ast.Sin, ast.Sqrt, ast.Tan, ast.SM3,
		ast.Quote, ast.RTrim, ast.ToBase64, ast.Trim, ast.Upper, ast.Ucase, ast.Hex,
		ast.BitLength, ast.CharLength, ast.Compress, ast.MD5, ast.SHA1, ast.SHA}
	args := []string{"2038330881", "'2038330881'", "'牵'", "-1", "''", "0"}

	for _, f := range funcs {
		for _, a := range args {
			q := fmt.Sprintf("SELECT c1 from t1 where %s(%s)", f, a)
			tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
			r1 := tk.MustQuery(q)
			tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
			r2 := tk.MustQuery(q)
			r1.Sort().Check(r2.Sort().Rows())
		}
	}
}

func TestWarningWithDisablePlanCacheStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t;")
	tk.MustExec("prepare st from 'select * from t';")
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec("execute st;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec("execute st;")
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
}

func randValueForMVIndex(colType string) string {
	randSize := 50
	colType = strings.ToLower(colType)
	switch colType {
	case "int":
		return fmt.Sprintf("%v", randSize-rand.Intn(randSize))
	case "string":
		return fmt.Sprintf("\"%v\"", rand.Intn(randSize))
	case "json-string":
		var array []string
		arraySize := 1 + rand.Intn(5)
		for range arraySize {
			array = append(array, randValueForMVIndex("string"))
		}
		return "'[" + strings.Join(array, ", ") + "]'"
	case "json-signed":
		var array []string
		arraySize := 1 + rand.Intn(5)
		for range arraySize {
			array = append(array, randValueForMVIndex("int"))
		}
		return "'[" + strings.Join(array, ", ") + "]'"
	default:
		return "unknown type " + colType
	}
}

func insertValuesForMVIndex(nRows int, colTypes ...string) string {
	stmtVals := make([]string, 0, nRows)
	for range nRows {
		var vals []string
		for _, colType := range colTypes {
			vals = append(vals, randValueForMVIndex(colType))
		}
		stmtVals = append(stmtVals, "("+strings.Join(vals, ", ")+")")
	}
	return strings.Join(stmtVals, ", ")
}

func verifyPlanCacheForMVIndex(t *testing.T, tk *testkit.TestKit, isIndexMerge, hitCache bool, queryTemplate string, colTypes ...string) {
	for range 5 {
		var vals []string
		for _, colType := range colTypes {
			vals = append(vals, randValueForMVIndex(colType))
		}

		query := queryTemplate
		var setStmt, usingStmt string
		for i, p := range vals {
			query = strings.Replace(query, "?", p, 1)
			if i > 0 {
				setStmt += ", "
				usingStmt += ", "
			}
			setStmt += fmt.Sprintf("@a%v=%v", i, p)
			usingStmt += fmt.Sprintf("@a%v", i)
		}
		result := tk.MustQuery(query).Sort()
		if isIndexMerge {
			tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
		}
		tk.MustExec(fmt.Sprintf("set %v", setStmt))
		tk.MustExec(fmt.Sprintf("prepare stmt from '%v'", queryTemplate))
		if isIndexMerge {
			tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
		}
		result1 := tk.MustQuery(fmt.Sprintf("execute stmt using %v", usingStmt)).Sort()
		result.Check(result1.Rows())
		if isIndexMerge && hitCache {
			tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
		}
		result2 := tk.MustQuery(fmt.Sprintf("execute stmt using %v", usingStmt)).Sort()
		result.Check(result2.Rows())
		if isIndexMerge && hitCache {
			tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
		}
		result3 := tk.MustQuery(fmt.Sprintf("execute stmt using %v", usingStmt)).Sort()
		result.Check(result3.Rows())
		if isIndexMerge && hitCache {
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // hit the cache
		}

		if isIndexMerge && hitCache { // check the plan
			result4 := tk.MustQuery(fmt.Sprintf("execute stmt using %v", usingStmt)).Sort()
			result.Check(result4.Rows())
			tkProcess := tk.Session().ShowProcess()
			ps := []*sessmgr.ProcessInfo{tkProcess}
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
			rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
			haveIndexMerge := false
			for _, r := range rows {
				if strings.Contains(r[0].(string), "IndexMerge") {
					haveIndexMerge = true
				}
			}
			require.True(t, haveIndexMerge) // IndexMerge has to be used.
		}
	}
}

func TestPlanCacheMVIndexRandomly(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_fix_control = "45798:on"`)

	// cases from TestIndexMergeFromComposedDNFCondition
	tk.MustExec(`drop table if exists t2`)
	tk.MustExec(`create table t2(a json, b json, c int, d int, e int, index idx(c, (cast(a as signed array))), index idx2((cast(b as signed array)), c), index idx3(c, d), index idx4(d))`)
	tk.MustExec(fmt.Sprintf("insert into t2 values %v", insertValuesForMVIndex(100, "json-signed", "json-signed", "int", "int", "int")))
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t2, idx2, idx) */ * from t2 where (? member of (a) and c=?) or (? member of (b) and c=?)`,
		`int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t2, idx2, idx) */ * from t2 where (? member of (a) and c=? and d=?) or (? member of (b) and c=? and d=?)`,
		`int`, `int`, `int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, false, false,
		`select /*+ use_index_merge(t2, idx2, idx) */ * from t2 where ( json_contains(a, ?) and c=? and d=?) or (? member of (b) and c=? and d=?)`,
		`json-signed`, `int`, `int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, false,
		`select /*+ use_index_merge(t2, idx2, idx) */ * from t2 where ( json_overlaps(a, ?) and c=? and d=?) or (? member of (b) and c=? and d=?)`,
		`json-signed`, `int`, `int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t2, idx2, idx, idx4) */ * from t2 where ( json_contains(a, ?) and d=?) or (? member of (b) and c=? and d=?)`,
		`json-signed`, `int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t2, idx2, idx) */ * from t2 where (? member of (a) and ? member of (b) and c=?) or (? member of (b) and c=?)`,
		`int`, `int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, false, true,
		`select * from t2 where (? member of (a) and ? member of (b) and c=?) or (? member of (b) and c=?) or e=?`,
		`int`, `int`, `int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t2, idx2, idx, idx4) */ * from t2 where (? member of (a) and ? member of (b) and c=?) or (? member of (b) and c=?) or d=?`,
		`int`, `int`, `int`, `int`, `int`, `int`)

	// cases from TestIndexMergeFromComposedCNFCondition
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1(a json, b json, c int, d int, index idx((cast(a as signed array))), index idx2((cast(b as signed array))))`)
	tk.MustExec(fmt.Sprintf("insert into t1 values %v", insertValuesForMVIndex(100, "json-signed", "json-signed", "int", "int")))
	tk.MustExec(`create table t2(a json, b json, c int, d int, index idx(c, (cast(a as signed array))), index idx2((cast(b as signed array)), c), index idx3(c, d), index idx4(d))`)
	tk.MustExec(fmt.Sprintf("insert into t2 values %v", insertValuesForMVIndex(100, "json-signed", "json-signed", "int", "int")))
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t1, idx, idx2) */ * from t1 where ? member of (a) and ? member of (b)`,
		`int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t2, idx, idx2) */ * from t2 where ? member of (a) and ? member of (b) and c=?`,
		`int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, true,
		`select /*+ use_index_merge(t2, idx, idx2, idx4) */ * from t2 where ? member of (a) and ? member of (b) and c=? and d=?`,
		`int`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, false,
		`select /*+ use_index_merge(t2, idx2, idx, idx3) */ * from t2 where json_contains(a, ?) and c=? and ? member of (b) and d=?`,
		`json-signed`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, true, false,
		`select /*+ use_index_merge(t2, idx2, idx, idx3) */ * from t2 where json_overlaps(a, ?) and c=? and ? member of (b) and d=?`,
		`json-signed`, `int`, `int`, `int`)
	verifyPlanCacheForMVIndex(t, tk, false, true,
		`select /*+ use_index_merge(t2, idx2, idx) */ * from t2 where ? member of (a) and c=? and c=?`,
		`int`, `int`, `int`)

	// case from TestIndexMergeIssue50265
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(pk varbinary(255) NOT NULL, domains json null, image_signatures json null, canonical_links json null, fpi json null,  KEY `domains` ((cast(`domains` as char(253) array))), KEY `image_signatures` ((cast(`image_signatures` as char(32) array))),KEY `canonical_links` ((cast(`canonical_links` as char(1000) array))), KEY `fpi` ((cast(`fpi` as signed array))))")
	tk.MustExec(fmt.Sprintf("insert into t values %v", insertValuesForMVIndex(100, "string", "json-string", "json-string", "json-string", "json-signed")))
	verifyPlanCacheForMVIndex(t, tk, false, false,
		`SELECT /*+ use_index_merge(t, domains, image_signatures, canonical_links, fpi) */ pk FROM t WHERE ? member of (domains) OR ? member of (image_signatures) OR ? member of (canonical_links) OR json_contains(fpi, "[69236881]") LIMIT 100`,
		`string`, `string`, `string`)

	// case from TestIndexMergeEliminateRedundantAndPaths
	tk.MustExec(`DROP table if exists t`)
	tk.MustExec("CREATE TABLE `t` (`pk` varbinary(255) NOT NULL,`nslc` json DEFAULT NULL,`fpi` json DEFAULT NULL,`point_of_sale_country` varchar(2) DEFAULT NULL,KEY `fpi` ((cast(`fpi` as signed array))),KEY `nslc` ((cast(`nslc` as char(1000) array)),`point_of_sale_country`),KEY `nslc_old` ((cast(`nslc` as char(1000) array))))")
	tk.MustExec(fmt.Sprintf("insert into t values %v", insertValuesForMVIndex(100, "string", "json-string", "json-signed", "string")))
	verifyPlanCacheForMVIndex(t, tk, true, true,
		"SELECT /*+ use_index_merge(t, fpi, nslc_old, nslc) */ * FROM   t WHERE   ? member of (fpi)   AND ? member of (nslc) LIMIT   100",
		"int", "string")

	// case from TestIndexMergeSingleCaseCouldFeelIndexMergeHint
	tk.MustExec(`DROP table if exists t`)
	tk.MustExec("CREATE TABLE t (nslc json DEFAULT NULL,fpi json DEFAULT NULL,point_of_sale_country int,KEY nslc ((cast(nslc as char(1000) array)),point_of_sale_country),KEY fpi ((cast(fpi as signed array))))")
	tk.MustExec(fmt.Sprintf("insert into t values %v", insertValuesForMVIndex(100, "json-string", "json-signed", "int")))
	verifyPlanCacheForMVIndex(t, tk, true, true,
		"SELECT  /*+ use_index_merge(t, nslc) */ *  FROM t WHERE  ? member of (fpi)  AND ? member of (nslc)  LIMIT  1",
		"int", "string")
	verifyPlanCacheForMVIndex(t, tk, true, true,
		"SELECT  /*+ use_index_merge(t, fpi) */ *  FROM t WHERE  ? member of (fpi)  AND ? member of (nslc)  LIMIT  1",
		"int", "string")
}

func TestPlanCacheMVIndexManually(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_fix_control = "45798:on"`)

	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	planSuiteData := GetPlanCacheSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		if strings.HasPrefix(strings.ToLower(input[i]), "select") ||
			strings.HasPrefix(strings.ToLower(input[i]), "execute") ||
			strings.HasPrefix(strings.ToLower(input[i]), "show") {
			result := tk.MustQuery(input[i])
			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(result.Rows())
			})
			result.Check(testkit.Rows(output[i].Result...))
		} else {
			tk.MustExec(input[i])
		}
	}
}

func BenchmarkPlanCacheBindingMatch(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec(`create global binding using select * from t where a=1`)

	tk.MustExec(`prepare st from 'select * from t where a=?'`)
	tk.MustExec(`set @a=1`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("execute st using @a")
	}
}

func BenchmarkPlanCacheInsert(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	tk.MustExec("prepare st from 'insert into t values (1)'")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("execute st")
	}
}

func BenchmarkNonPreparedPlanCacheDML(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified_cacheability_check=1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("insert into t values (1)")
		tk.MustExec("update t set a = 2 where a = 1")
		tk.MustExec("delete from t where a = 2")
	}
}

func TestIndexRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`CREATE TABLE t0 (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY)`)
	tk.MustExec(`CREATE TABLE t1(c0 FLOAT ZEROFILL, PRIMARY KEY(c0));`)
	tk.MustExec(`INSERT INTO t0 (id) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11);`)
	tk.MustExec("INSERT INTO t1(c0) VALUES (1);")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1;`)
	tk.MustQuery(`SELECT t0.* FROM t0 WHERE (id = 1 or id = 9223372036854775808);`).Check(testkit.Rows("1"))
	tk.MustQuery("SELECT t1.c0 FROM t1 WHERE t1.c0!=BIN(-1);").Check(testkit.Rows("1"))
}

func TestPlanCacheDirtyTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	for _, t1Dirty := range []bool{true, false} {
		for _, t2Dirty := range []bool{true, false} {
			tk.MustExec(`create table t1 (a int);`)
			tk.MustExec(`create table t2 (a int);`)
			tk.MustExec(`begin`)
			tk.MustExec(`prepare st from 'select 1 from t1, t2'`)
			if t1Dirty {
				tk.MustExec(`insert into t1 values (1)`)
			}
			if t2Dirty {
				tk.MustExec(`insert into t2 values (1)`)
			}
			tk.MustExec(`execute st`) // generate a cached plan with t1Dirty & t2Dirty
			tk.MustExec(`commit`)

			// test cases
			for _, testT1Dirty := range []bool{true, false} {
				for _, testT2Dirty := range []bool{true, false} {
					tk.MustExec(`begin`)
					if testT1Dirty {
						tk.MustExec(`insert into t1 values (1)`)
					}
					if testT2Dirty {
						tk.MustExec(`insert into t2 values (1)`)
					}
					tk.MustExec(`execute st`)

					if testT1Dirty == t1Dirty && testT2Dirty == t2Dirty {
						tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
					} else {
						tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
					}

					tk.MustExec(`commit`)
				}
			}
			tk.MustExec(`drop table t1, t2`)
		}
	}
}

func TestInstancePlanCacheAcrossSession(t *testing.T) {
	ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyEnableInstancePlanCache{}, true)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec(`use test`)
	tk1.MustExec(`create table t (a int)`)
	tk1.MustExec(`insert into t values (1), (2), (3), (4), (5)`)
	tk1.MustExecWithContext(ctx, `prepare st from 'select a from t where a < ?'`)
	tk1.MustExecWithContext(ctx, `set @a=2`)
	tk1.MustQueryWithContext(ctx, `execute st using @a`).Sort().Check(testkit.Rows(`1`))
	tk1.MustExecWithContext(ctx, `set @a=3`)
	tk1.MustQueryWithContext(ctx, `execute st using @a`).Sort().Check(testkit.Rows(`1`, `2`))
	tk1.MustQueryWithContext(ctx, `select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// session2 can share session1's cached plan
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExecWithContext(ctx, `use test`)
	tk2.MustExecWithContext(ctx, `prepare st from 'select a from t where a < ?'`)
	tk2.MustExecWithContext(ctx, `set @a=4`)
	tk2.MustQueryWithContext(ctx, `execute st using @a`).Sort().Check(testkit.Rows(`1`, `2`, `3`))
	tk2.MustQueryWithContext(ctx, `select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func runPreparedPlanCacheForUpdateInTxn(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	autocommit := tk.MustQuery("select @@session.autocommit").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.autocommit=%v", autocommit))
	}()
	tableName := "t_for_update"
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf(`create table %s (pk int, a int, primary key(pk))`, tableName))
	tk.MustExec(`set autocommit=on`)
	tk.MustQuery(`select @@autocommit`).Check(testkit.Rows("1"))
	tk.MustExec(`set @pk=1`)

	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from %s where pk=? for update'`, tableName))
	tk.MustExec(`execute st using @pk`)
	tk.MustExec(`execute st using @pk`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`begin`)
	tk.MustExec(`execute st using @pk`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // can't reuse since it's in txn now.
	tk.MustExec(`commit`)
	tk.MustExec(`deallocate prepare st`)
}

func TestNonPreparedPlanCacheSupportsFeatures(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	nonPreparedCache := tk.MustQuery("select @@session.tidb_enable_non_prepared_plan_cache").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.tidb_enable_non_prepared_plan_cache=%v", nonPreparedCache))
	}()
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	runNonPreparedPlanCacheHints(t, tk, "t_support_hints")
	runNonPreparedPlanCacheBindings(t, tk, "t_support_bindings")
	runNonPreparedPlanCacheSetVar(t, tk, "t_support_setvar")
	runNonPreparedPlanCacheIgnoreHint(t, tk, "t_support_ignore")
}

func runNonPreparedPlanCacheHints(t *testing.T, tk *testkit.TestKit, tableName string) {
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (pk int, a int, primary key(pk))", tableName))

	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(fmt.Sprintf("select  /*+ max_execution_time(2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(fmt.Sprintf("select  /*+ max_execution_time(2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func runNonPreparedPlanCacheBindings(t *testing.T, tk *testkit.TestKit, tableName string) {
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (pk int, a int, primary key(pk))", tableName))
	tk.MustExec(fmt.Sprintf("CREATE BINDING FOR select * from %s where pk >= ? USING select * from %s where pk >= ?", tableName, tableName))

	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	tk.MustExec(fmt.Sprintf("select  /*+ max_execution_time(2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select  /*+ max_execution_time(2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))
}

func runNonPreparedPlanCacheSetVar(t *testing.T, tk *testkit.TestKit, tableName string) {
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (pk int, a int, primary key(pk))", tableName))

	tk.MustExec(fmt.Sprintf("CREATE BINDING FOR select * from %s where pk >= ? USING select * from %s where pk >= ?", tableName, tableName))

	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	tk.MustExec(fmt.Sprintf("select /*+ set_var(max_execution_time=2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select /*+ set_var(max_execution_time=2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	tk.MustExec(fmt.Sprintf("DROP BINDING FOR select * from %s where pk >= ?", tableName))
	tk.MustExec(fmt.Sprintf("CREATE BINDING FOR select * from %s where pk >= ? USING select /*+ set_var(max_execution_time=2000) */ * from %s where pk >= ?", tableName, tableName))

	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	tk.MustExec(fmt.Sprintf("select /*+ set_var(max_execution_time=2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select /*+ set_var(max_execution_time=2000) */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))
}

func runNonPreparedPlanCacheIgnoreHint(t *testing.T, tk *testkit.TestKit, tableName string) {
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf("create table %s (pk int, a int, primary key(pk))", tableName))

	tk.MustExec(fmt.Sprintf("select /*+ ignore_plan_cache() */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))
	tk.MustExec(fmt.Sprintf("select /*+ ignore_plan_cache() */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))

	tk.MustExec(fmt.Sprintf("CREATE BINDING FOR select * from %s where pk >= ? USING select * from %s where pk >= ?", tableName, tableName))
	tk.MustExec(fmt.Sprintf("select  /*+ ignore_plan_cache() */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select  /*+ ignore_plan_cache() */ * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))

	tk.MustExec(fmt.Sprintf("DROP BINDING FOR select * from %s where pk >= ?", tableName))
	tk.MustExec(fmt.Sprintf("CREATE BINDING FOR select * from %s where pk >= ? USING select /*+ ignore_plan_cache() */ * from %s where pk >= ?", tableName, tableName))
	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(fmt.Sprintf("select * from %s where pk >= 1", tableName))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))

	tk.MustExec(fmt.Sprintf("DROP BINDING FOR select * from %s where pk >= ?", tableName))

	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s where pk >= ?'", tableName))
	tk.MustExec(`set @a=4`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 1"))

	tk.MustExec(fmt.Sprintf("prepare st from 'select /*+ ignore_plan_cache() */ * from %s where pk >= ?'", tableName))
	tk.MustExec(`set @a=4`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))

	tk.MustExec(fmt.Sprintf("CREATE BINDING FOR select * from %s where pk >= ? USING select /*+ ignore_plan_cache() */ * from %s where pk >= ?", tableName, tableName))
	tk.MustExec(fmt.Sprintf("prepare st from 'select * from %s where pk >= ?'", tableName))
	tk.MustExec(`set @a=4`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
}

func TestNonPreparedPlanCacheResourceGroup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t (pk int, a int, primary key(pk))`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1;`)

	// Check that the hint sets the resource group.
	tk.MustExec(`select  /*+ RESOURCE_GROUP(rg1) */ * from t where pk >= 1`)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StmtHints.HasResourceGroup)
	require.Equal(t, "rg1", tk.Session().GetSessionVars().StmtCtx.StmtHints.ResourceGroup)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))

	tk.MustExec(`select  /*+ RESOURCE_GROUP(rg10) */ * from t where pk >= 1`)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StmtHints.HasResourceGroup)
	require.Equal(t, "rg10", tk.Session().GetSessionVars().StmtCtx.StmtHints.ResourceGroup)

	tk.MustExec(`select  /*+ RESOURCE_GROUP(rg1) */ * from t where pk >= 1`)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StmtHints.HasResourceGroup)
	require.Equal(t, "rg1", tk.Session().GetSessionVars().StmtCtx.StmtHints.ResourceGroup)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 1"))

	tk.MustExec(`CREATE BINDING FOR select * from t where pk >= ? USING select /*+ RESOURCE_GROUP(rg2) */ * from t where pk >= ?`)

	// Test that the resource group comes from the binding.
	tk.MustExec(`select * from t where pk >= 1`)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StmtHints.HasResourceGroup)
	require.Equal(t, "rg2", tk.Session().GetSessionVars().StmtCtx.StmtHints.ResourceGroup)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))

	tk.MustExec(`select * from t where pk >= 1`)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StmtHints.HasResourceGroup)
	require.Equal(t, "rg2", tk.Session().GetSessionVars().StmtCtx.StmtHints.ResourceGroup)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	// Test that the resource group comes from the binding and the value in query is ignored.
	tk.MustExec(`select  /*+ RESOURCE_GROUP(rg1) */ * from t where pk >= 1`)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StmtHints.HasResourceGroup)
	require.Equal(t, "rg2", tk.Session().GetSessionVars().StmtCtx.StmtHints.ResourceGroup)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
}

func TestPreparedPlanCacheWorkWithoutMetadataLock(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.Exec(`set @@global.tidb_enable_metadata_lock=off`)

	tk.MustExec(`prepare stmt from 'select * from t where a = ?'`)
	tk.MustExec(`set @a=1`)

	// check that cache works without metadata lock
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 1"))
	tk.MustExec(`begin`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 1"))
	tk.MustExec(`insert into t values (1)`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 0"))
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 1"))
	tk.MustExec(`rollback`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("0 1"))
}

// TestPlanCacheSkipStatsOnBinding verifies that tidb_plan_cache_skip_stats_on_binding
// suppresses stats-version-based plan cache invalidation when a SQL binding is active.
//
// With the variable ON, ANALYZE does not invalidate the cache entry for a bound query
// because the binding pins the plan and stats changes cannot alter the chosen plan.
// Without a binding, or with the variable OFF, ANALYZE continues to invalidate as usual.
func TestPlanCacheSkipStatsOnBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key idx_b(b))`)
	tk.MustExec(`insert into t values (1,1),(2,2),(3,3)`)

	// Enable stats-version-based invalidation so that ANALYZE normally busts the cache.
	tk.MustExec(`set @@tidb_plan_cache_invalidation_on_fresh_stats = ON`)
	tk.MustExec(`set @@tidb_plan_cache_skip_stats_on_binding = ON`)

	// -- Part 1: No binding. ANALYZE must bust the cache. --
	tk.MustExec(`prepare st from 'select * from t where b=?'`)
	tk.MustExec(`set @v=1`)
	tk.MustExec(`execute st using @v`)
	tk.MustExec(`execute st using @v`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // cached

	tk.MustExec(`analyze table t`)
	tk.MustExec(`execute st using @v`)
	// Stats version changed, no binding → cache miss.
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	// -- Part 2: With binding + skip=ON. ANALYZE must NOT bust the cache. --
	tk.MustExec(`create binding using select /*+ use_index(t, idx_b) */ * from t where b=1`)
	tk.MustExec(`execute st using @v`) // first exec under binding → new key, cache miss
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(`execute st using @v`) // second exec → cache hit
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	tk.MustExec(`analyze table t`)
	tk.MustExec(`execute st using @v`)
	// Binding active + skip=ON → stats version excluded from key → cache hit.
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	// -- Part 3: skip=OFF restores the old behaviour: ANALYZE busts the cache even with a binding. --
	tk.MustExec(`set @@tidb_plan_cache_skip_stats_on_binding = OFF`)
	// Key now includes stats version; previous entry (without stats ver) is a different key → miss.
	tk.MustExec(`execute st using @v`)
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))
	tk.MustExec(`execute st using @v`) // warm the cache under the new key
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 1"))

	tk.MustExec(`analyze table t`)
	tk.MustExec(`execute st using @v`)
	// skip=OFF → stats version back in key → ANALYZE causes cache miss.
	tk.MustQuery(`select @@last_plan_from_binding, @@last_plan_from_cache`).Check(testkit.Rows("1 0"))

	tk.MustExec(`drop binding for select * from t where b=1`)
}
