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

package core_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestCollectFilters4MVIndexMutations(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, domains json null, images json null, KEY `a_domains_b` (a, (cast(`domains` as char(253) array)), b))")
	sql := "SELECT * FROM t WHERE   '15975127' member of (domains)   AND '15975128' member of (domains) AND a = 1 AND b = 2"

	par := parser.New()
	par.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})
	// Make sure the table schema is the new schema.
	err := domain.Reload()
	require.NoError(t, err)
	is := domain.InfoSchema()
	is = &infoschema.SessionExtendedInfoSchema{InfoSchema: is}
	require.NoError(t, tk.Session().PrepareTxnCtx(context.TODO()))
	require.NoError(t, sessiontxn.GetTxnManager(tk.Session()).OnStmtStart(context.TODO(), nil))
	stmt, err := par.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PlanID.Store(0)
	tk.Session().GetSessionVars().PlanColumnID.Store(0)
	nodeW := resolve.NewNodeW(stmt)
	err = core.Preprocess(context.Background(), tk.Session(), nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
	require.NoError(t, err)
	require.NoError(t, sessiontxn.GetTxnManager(tk.Session()).AdviseWarmup())
	builder, _ := core.NewPlanBuilder().Init(tk.Session().GetPlanCtx(), is, hint.NewQBHintHandler(nil))
	p, err := builder.Build(context.TODO(), nodeW)
	require.NoError(t, err)
	logicalP, err := core.LogicalOptimizeTest(context.TODO(), builder.GetOptFlag(), p.(base.LogicalPlan))
	require.NoError(t, err)

	ds, ok := logicalP.(*core.DataSource)
	for !ok {
		p := logicalP.Children()[0]
		ds, ok = p.(*core.DataSource)
	}
	cnfs := ds.AllConds
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	idxCols, ok := core.PrepareIdxColsAndUnwrapArrayType(
		tbl.Meta(),
		tbl.Meta().FindIndexByName("a_domains_b"),
		ds.TblCols,
		true,
	)
	require.True(t, ok)
	accessFilters, _, mvColOffset, mvFilterMutations := core.CollectFilters4MVIndexMutations(tk.Session().GetPlanCtx(), cnfs, idxCols)

	// assert mv col access filters.
	require.Equal(t, len(accessFilters), 3)
	sf, ok := accessFilters[0].(*expression.ScalarFunction)
	require.True(t, ok)
	require.Equal(t, sf.FuncName.L, ast.EQ)
	sf, ok = accessFilters[1].(*expression.ScalarFunction)
	require.True(t, ok)
	require.Equal(t, sf.FuncName.L, ast.JSONMemberOf)
	sf, ok = accessFilters[2].(*expression.ScalarFunction)
	require.True(t, ok)
	require.Equal(t, sf.FuncName.L, ast.EQ)

	// assert mv col offset
	require.Equal(t, mvColOffset, 1)

	// assert mv col condition mutations.
	require.Equal(t, len(mvFilterMutations), 2)
	sf, ok = mvFilterMutations[0].(*expression.ScalarFunction)
	require.True(t, ok)
	require.Equal(t, sf.FuncName.L, ast.JSONMemberOf)
	sf, ok = mvFilterMutations[1].(*expression.ScalarFunction)
	require.True(t, ok)
	require.Equal(t, sf.FuncName.L, ast.JSONMemberOf)
}

func TestMultiMVIndexRandom(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, testCase := range []struct {
		indexType     string
		insertValOpts randMVIndexValOpts
		queryValsOpts randMVIndexValOpts
	}{
		{"signed", randMVIndexValOpts{"signed", 0, 3}, randMVIndexValOpts{"signed", 0, 3}},
		{"unsigned", randMVIndexValOpts{"unsigned", 0, 3}, randMVIndexValOpts{"unsigned", 0, 3}}, // unsigned-index + unsigned-values
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 3, 3}},
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 1, 3}},
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 5, 3}},
		{"date", randMVIndexValOpts{"date", 0, 3}, randMVIndexValOpts{"date", 0, 3}},
	} {
		tk.MustExec("drop table if exists t1")
		tk.MustExec(fmt.Sprintf(`create table t1(pk int auto_increment primary key, a json, b json, c int, d int, index idx((cast(a as %v array))), index idx2((cast(b as %v array)), c), index idx3(c, d), index idx4(d))`, testCase.indexType, testCase.indexType))
		nRows := 20
		rows := make([]string, 0, nRows)
		for i := 0; i < nRows; i++ {
			va1, va2, vb1, vb2, vc, vd := randMVIndexValue(testCase.insertValOpts), randMVIndexValue(testCase.insertValOpts), randMVIndexValue(testCase.insertValOpts), randMVIndexValue(testCase.insertValOpts), rand.Intn(testCase.insertValOpts.distinct), rand.Intn(testCase.insertValOpts.distinct)
			if testCase.indexType == "date" {
				rows = append(rows, fmt.Sprintf(`(json_array(cast(%v as date), cast(%v as date)),  json_array(cast(%v as date), cast(%v as date)), %v, %v)`, va1, va2, vb1, vb2, vc, vd))
			} else {
				rows = append(rows, fmt.Sprintf(`('[%v, %v]', '[%v, %v]', %v, %v)`, va1, va2, vb1, vb2, vc, vd))
			}
		}
		tk.MustExec(fmt.Sprintf("insert into t1(a,b,c,d) values %v", strings.Join(rows, ", ")))
		randJColName := func() string {
			if rand.Intn(2) < 1 {
				return "a"
			}
			return "b"
		}
		randNColName := func() string {
			if rand.Intn(2) < 1 {
				return "c"
			}
			return "d"
		}
		nQueries := 20
		tk.MustExec(`set @@tidb_opt_fix_control = "45798:on"`)
		for i := 0; i < nQueries; i++ {
			cnf := true
			if i >= 10 {
				// cnf
				cnf = false
			}
			// composed condition at least two to make sense.
			conds, conds4PlanCache, params := randMVIndexCondsXNF4MemberOf(rand.Intn(3)+2, testCase.queryValsOpts, cnf, randJColName, randNColName)
			r1 := tk.MustQuery("select /*+ ignore_index(t1, idx, idx2, idx3, idx4) */ * from t1 where " + conds).Sort()
			tk.MustQuery("select /*+ use_index_merge(t1, idx, idx2, idx3, idx4) */ * from t1 where " + conds).Sort().Check(r1.Rows())

			prepareStmt := fmt.Sprintf(`prepare st from 'select /*+ use_index_merge(t, kj) */ * from t1 where %v'`, conds4PlanCache)
			tk.MustExec(prepareStmt)
			var setStmt, usingStmt string
			for i, p := range params {
				vName := fmt.Sprintf("@a%v", i)
				if i > 0 {
					setStmt += ", "
					usingStmt += ", "
				}
				setStmt += fmt.Sprintf("%v=%v", vName, p)
				usingStmt += vName
			}
			tk.MustExec("set " + setStmt)
			tk.MustQuery("execute st using " + usingStmt).Sort().Check(r1.Rows())
		}
	}
}
func TestMVIndexRandom(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, testCase := range []struct {
		indexType     string
		insertValOpts randMVIndexValOpts
		queryValsOpts randMVIndexValOpts
	}{
		{"signed", randMVIndexValOpts{"signed", 0, 3}, randMVIndexValOpts{"signed", 0, 3}},
		{"unsigned", randMVIndexValOpts{"unsigned", 0, 3}, randMVIndexValOpts{"unsigned", 0, 3}}, // unsigned-index + unsigned-values
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 3, 3}},
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 1, 3}},
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 5, 3}},
		{"date", randMVIndexValOpts{"date", 0, 3}, randMVIndexValOpts{"date", 0, 3}},
	} {
		tk.MustExec("drop table if exists t")
		tk.MustExec(fmt.Sprintf(`create table t(a int, j json, index kj((cast(j as %v array))))`, testCase.indexType))
		nRows := 20
		rows := make([]string, 0, nRows)
		for i := 0; i < nRows; i++ {
			va, v1, v2 := rand.Intn(testCase.insertValOpts.distinct), randMVIndexValue(testCase.insertValOpts), randMVIndexValue(testCase.insertValOpts)
			if testCase.indexType == "date" {
				rows = append(rows, fmt.Sprintf(`(%v, json_array(cast(%v as date), cast(%v as date)))`, va, v1, v2))
			} else {
				rows = append(rows, fmt.Sprintf(`(%v, '[%v, %v]')`, va, v1, v2))
			}
		}
		tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(rows, ", ")))
		randJColName := func() string {
			return "j"
		}
		randNColName := func() string {
			return "a"
		}
		nQueries := 20
		tk.MustExec(`set @@tidb_opt_fix_control = "45798:on"`)
		for i := 0; i < nQueries; i++ {
			conds, conds4PlanCache, params := randMVIndexConds(rand.Intn(3)+1, testCase.queryValsOpts, randJColName, randNColName)
			r1 := tk.MustQuery("select /*+ ignore_index(t, kj) */ * from t where " + conds).Sort()
			tk.MustQuery("select /*+ use_index_merge(t, kj) */ * from t where " + conds).Sort().Check(r1.Rows())

			prepareStmt := fmt.Sprintf(`prepare st from 'select /*+ use_index_merge(t, kj) */ * from t where %v'`, conds4PlanCache)
			tk.MustExec(prepareStmt)
			var setStmt, usingStmt string
			for i, p := range params {
				vName := fmt.Sprintf("@a%v", i)
				if i > 0 {
					setStmt += ", "
					usingStmt += ", "
				}
				setStmt += fmt.Sprintf("%v=%v", vName, p)
				usingStmt += vName
			}
			tk.MustExec("set " + setStmt)
			tk.MustQuery("execute st using " + usingStmt).Sort().Check(r1.Rows())
		}
	}
}

func TestPlanCacheMVIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE ti (
  item_pk varbinary(255) NOT NULL,
  item_id varchar(45) DEFAULT NULL,
  item_set_id varchar(45) DEFAULT NULL,
  m_id bigint(20) DEFAULT NULL,
  m_item_id varchar(127) DEFAULT NULL,
  m_item_set_id varchar(127) DEFAULT NULL,
  country varchar(2) DEFAULT NULL,
  domains json DEFAULT NULL,
  signatures json DEFAULT NULL,
  short_link json DEFAULT NULL,
  long_link json DEFAULT NULL,
  f_item_ids json DEFAULT NULL,
  f_profile_ids json DEFAULT NULL,
  product_sources json DEFAULT NULL,
  PRIMARY KEY (item_pk)  /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY item_id (item_id),
  KEY m_item_id (m_item_id),
  KEY m_item_set_id (m_item_set_id),
  KEY m_id (m_id),
  KEY item_set_id (item_set_id),
  KEY m_item_and_m_id (m_item_id, m_id),
  KEY domains ((cast(domains as char(253) array))),
  KEY signatures ((cast(signatures as char(32) array))),
  KEY f_profile_ids ((cast(f_profile_ids as unsigned array))),
  KEY short_link_old ((cast(short_link as char(1000) array))),
  KEY long_link ((cast(long_link as char(1000) array))),
  KEY f_item_ids ((cast(f_item_ids as unsigned array))),
  KEY short_link ((cast(short_link as char(1000) array)),country))`)

	for i := 0; i < 50; i++ {
		var insertVals []string
		insertVals = append(insertVals, fmt.Sprintf("'%v'", i))                                                                            // item_pk varbinary(255) NOT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", i))                                                                            // item_id varchar(45) DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", rand.Intn(30)))                                                                // item_set_id varchar(45) DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("%v", rand.Intn(30)))                                                                  // m_id bigint(20) DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", rand.Intn(30)))                                                                // m_item_id varchar(127) DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", rand.Intn(30)))                                                                // m_item_set_id varchar(127) DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", rand.Intn(3)))                                                                 // country varchar(2) DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", randArray(randMVIndexValOpts{valType: "string", maxStrLen: 5, distinct: 10}))) // domains json DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", randArray(randMVIndexValOpts{valType: "string", maxStrLen: 5, distinct: 10}))) // signatures json DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", randArray(randMVIndexValOpts{valType: "string", maxStrLen: 5, distinct: 10}))) // short_link json DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", randArray(randMVIndexValOpts{valType: "string", maxStrLen: 5, distinct: 10}))) // long_link json DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", randArray(randMVIndexValOpts{valType: "unsigned", distinct: 10})))             // f_item_ids json DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", randArray(randMVIndexValOpts{valType: "unsigned", distinct: 10})))             // f_profile_ids json DEFAULT NULL,
		insertVals = append(insertVals, fmt.Sprintf("'%v'", randArray(randMVIndexValOpts{valType: "string", maxStrLen: 5, distinct: 10}))) // product_sources json DEFAULT NULL,
		tk.MustExec(fmt.Sprintf("insert into ti values (%v)", strings.Join(insertVals, ",")))
	}

	tk.MustExec(`set @@tidb_opt_fix_control = "45798:on"`)

	check := func(hitCache bool, sql string, params ...string) {
		sqlWithoutParam := sql
		var setStmt, usingStmt string
		for i, p := range params {
			sqlWithoutParam = strings.Replace(sqlWithoutParam, "?", p, 1)
			if i > 0 {
				setStmt += ", "
				usingStmt += ", "
			}
			setStmt += fmt.Sprintf("@a%v=%v", i, p)
			usingStmt += fmt.Sprintf("@a%v", i)
		}
		result := tk.MustQuery(sqlWithoutParam).Sort()
		tk.MustExec(fmt.Sprintf("set %v", setStmt))
		tk.MustExec(fmt.Sprintf("prepare stmt from '%v'", sql))
		result1 := tk.MustQuery(fmt.Sprintf("execute stmt using %v", usingStmt)).Sort()
		result.Check(result1.Rows())
		result2 := tk.MustQuery(fmt.Sprintf("execute stmt using %v", usingStmt)).Sort()
		result.Check(result2.Rows())
		if hitCache {
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		} else {
			require.Greater(t, len(tk.MustQuery("show warnings").Rows()), 0) // show the reason
		}
	}
	randV := func(vs ...string) string {
		return vs[rand.Intn(len(vs))]
	}

	for i := 0; i < 50; i++ {
		check(true, `select * from ti where (? member of (short_link)) and (ti.country = ?)`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(3)))
		check(true, `select * from ti where (? member of (f_profile_ids) AND (ti.m_item_set_id = ?) AND (ti.country = ?))`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(3)))
		check(true, `select * from ti where (? member of (short_link))`, fmt.Sprintf("'%v'", rand.Intn(30)))
		check(true, `select * from ti where (? member of (long_link)) AND (ti.country = ?)`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(3)))
		check(true, `select * from ti where (? member of (long_link))`, fmt.Sprintf("'%v'", rand.Intn(30)))
		check(true, `select * from ti where (? member of (f_profile_ids) AND (ti.m_item_set_id = ?) AND (ti.country = ?))`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(3)))
		check(true, `select * from ti where (m_id = ? and m_item_id = ? and country = ?) OR (? member of (short_link) and not json_overlaps(product_sources, ?) and country = ?)`,
			fmt.Sprintf("%v", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(3)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(3)), randV(`'["0","1","2"]'`, `'["0"]'`, `'["1","2"]'`))
		check(true, `select * from ti where ? member of (domains) AND ? member of (signatures) AND ? member of (f_profile_ids) AND ? member of (short_link) AND ? member of (long_link) AND ? member of (f_item_ids)`,
			fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("%v", rand.Intn(30)))
		check(true, `select * from ti where ? member of (domains) AND ? member of (signatures) AND ? member of (f_profile_ids) AND ? member of (short_link) AND ? member of (long_link) AND ? member of (f_item_ids) AND m_item_id IS NULL AND m_id = ? AND country IS NOT NULL`,
			fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("%v", rand.Intn(30)), fmt.Sprintf("%v", rand.Intn(30)))
		check(true, `select * from ti where ? member of (f_profile_ids) AND ? member of (short_link) AND json_overlaps(product_sources, ?)`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), randV(`'["0","1","2"]'`, `'["0"]'`, `'["1","2"]'`))
		check(true, `select * from ti where ? member of (short_link) AND ti.country = "0" AND NOT ? member of (long_link) AND ti.m_item_id = "0"`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)))
		check(true, `select * from ti where ? member of (short_link) AND ? member of (long_link) OR ? member of (f_profile_ids) AND ti.m_item_id = "0" OR ti.m_item_set_id = ?`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)))
		check(true, `select * from ti where ? member of (domains) OR ? member of (signatures) OR (? member of (f_profile_ids))`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("%v", rand.Intn(30)))
		check(false, `select * from ti where ? member of (domains) OR ? member of (signatures) OR json_overlaps(f_profile_ids, ?)`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), randV(`"[0,1]"`, `"[0,1,2]"`, `"[0]"`))
		check(false, `select * from ti where ? member of (domains) OR ? member of (signatures) OR ? member of (f_profile_ids) OR json_contains(f_profile_ids, ?)`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("%v", rand.Intn(30)), randV(`"[0,1]"`, `"[0,1,2]"`, `"[0]"`))
		check(false, `select * from ti WHERE ? member of (domains) OR ? member of (signatures) OR ? member of (long_link) OR json_contains(f_profile_ids, ?)`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), randV(`"[0,1]"`, `"[0,1,2]"`, `"[0]"`))
		check(false, `select * from ti WHERE ? member of (domains) AND ? member of (signatures) AND json_contains(f_profile_ids, ?)`, fmt.Sprintf("'%v'", rand.Intn(30)), fmt.Sprintf("'%v'", rand.Intn(30)), randV(`'[0,1,2]'`, `'[0]'`, `'[1,2]'`))
	}
}

func randMVIndexCondsXNF4MemberOf(nConds int, valOpts randMVIndexValOpts, CNF bool, randJCol, randNCol func() string) (conds, conds4PlanCache string, params []string) {
	for i := 0; i < nConds; i++ {
		if i > 0 {
			if CNF {
				conds += " AND "
				conds4PlanCache += " AND "
			} else {
				conds += " OR "
				conds4PlanCache += " OR "
			}
		}
		cond, cond4PlanCache, param := randMVIndexCond(rand.Intn(4), valOpts, randJCol, randNCol)
		conds += cond
		conds4PlanCache += cond4PlanCache
		params = append(params, param)
	}
	return
}

func randMVIndexConds(nConds int, valOpts randMVIndexValOpts, randJCol, randNCol func() string) (conds, conds4PlanCache string, params []string) {
	for i := 0; i < nConds; i++ {
		if i > 0 {
			if rand.Intn(5) < 1 { // OR
				conds += " OR "
				conds4PlanCache += " OR "
			} else { // AND
				conds += " AND "
				conds4PlanCache += " AND "
			}
		}
		cond, cond4PlanCache, param := randMVIndexCond(rand.Intn(4), valOpts, randJCol, randNCol)
		conds += cond
		conds4PlanCache += cond4PlanCache
		params = append(params, param)
	}
	return
}

func randMVIndexCond(condType int, valOpts randMVIndexValOpts, randJCol, randNCol func() string) (cond, cond4PlanCache, param string) {
	switch condType {
	case 0: // member_of
		col, v := randJCol(), randMVIndexValue(valOpts)
		return fmt.Sprintf(`(%v member of (%v))`, v, col),
			fmt.Sprintf(`(? member of (%v))`, col), v
	case 1: // json_contains
		col, v := randJCol(), randArray(valOpts)
		return fmt.Sprintf(`json_contains(%v, '%v')`, col, v),
			fmt.Sprintf(`json_contains(%v, ?)`, col), fmt.Sprintf("'%v'", v)
	case 2: // json_overlaps
		col, v := randJCol(), randArray(valOpts)
		return fmt.Sprintf(`json_overlaps(%v, '%v')`, col, v),
			fmt.Sprintf(`json_overlaps(%v, ?)`, col), fmt.Sprintf("'%v'", v)
	default: // others
		col, v := randNCol(), rand.Intn(valOpts.distinct)
		return fmt.Sprintf(`%v < %v`, col, v),
			fmt.Sprintf(`%v < ?`, col), fmt.Sprintf("%v", v)
	}
}

func randArray(opts randMVIndexValOpts) string {
	n := rand.Intn(5) // n can be 0
	var vals []string
	for i := 0; i < n; i++ {
		vals = append(vals, randMVIndexValue(opts))
	}
	return "[" + strings.Join(vals, ", ") + "]"
}

type randMVIndexValOpts struct {
	valType   string // INT, UNSIGNED, STR, DATE
	maxStrLen int
	distinct  int
}

func randMVIndexValue(opts randMVIndexValOpts) string {
	switch strings.ToLower(opts.valType) {
	case "signed":
		return fmt.Sprintf("%v", rand.Intn(opts.distinct)-(opts.distinct/2))
	case "unsigned":
		return fmt.Sprintf("%v", rand.Intn(opts.distinct))
	case "string":
		return fmt.Sprintf(`"%v"`, strings.Repeat(fmt.Sprintf("%v", rand.Intn(opts.distinct)), rand.Intn(opts.maxStrLen)+1))
	case "date":
		return fmt.Sprintf(`"2000-01-%v"`, rand.Intn(opts.distinct)+1)
	}
	return ""
}
