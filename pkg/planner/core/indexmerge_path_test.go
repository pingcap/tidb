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
	sql := "SELECT * FROM t WHERE   15975127 member of (domains)   AND 15975128 member of (domains) AND a = 1 AND b = 2"

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
	err = core.Preprocess(context.Background(), tk.Session(), stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
	require.NoError(t, err)
	require.NoError(t, sessiontxn.GetTxnManager(tk.Session()).AdviseWarmup())
	builder, _ := core.NewPlanBuilder().Init(tk.Session().GetPlanCtx(), is, hint.NewQBHintHandler(nil))
	p, err := builder.Build(context.TODO(), stmt)
	require.NoError(t, err)
	logicalP, err := core.LogicalOptimizeTest(context.TODO(), builder.GetOptFlag(), p.(core.LogicalPlan))
	require.NoError(t, err)

	ds, ok := logicalP.(*core.DataSource)
	for !ok {
		p := logicalP.Children()[0]
		ds, ok = p.(*core.DataSource)
	}
	cnfs := ds.GetAllConds()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	idxCols, ok := core.PrepareCols4MVIndex(tbl.Meta(), tbl.Meta().FindIndexByName("a_domains_b"), ds.TblCols)
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
