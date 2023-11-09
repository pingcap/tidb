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
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

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
		for i := 0; i < nQueries; i++ {
			cnf := true
			if i >= 10 {
				// cnf
				cnf = false
			}
			// composed condition at least two to make sense.
			conds := randMVIndexCondsXNF4MemberOf(rand.Intn(3)+2, testCase.queryValsOpts, cnf, randJColName, randNColName)
			r1 := tk.MustQuery("select /*+ ignore_index(t1, idx, idx2, idx3, idx4) */ * from t1 where " + conds).Sort()
			tk.MustQuery("select /*+ use_index_merge(t1, idx, idx2, idx3, idx4) */ * from t1 where " + conds).Sort().Check(r1.Rows())
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
		for i := 0; i < nQueries; i++ {
			conds := randMVIndexConds(rand.Intn(3)+1, testCase.queryValsOpts, randJColName, randNColName)
			r1 := tk.MustQuery("select /*+ ignore_index(t, kj) */ * from t where " + conds).Sort()
			tk.MustQuery("select /*+ use_index_merge(t, kj) */ * from t where " + conds).Sort().Check(r1.Rows())
		}
	}
}

func randMVIndexCondsXNF4MemberOf(nConds int, valOpts randMVIndexValOpts, CNF bool, randJCol, randNCol func() string) string {
	var conds string
	for i := 0; i < nConds; i++ {
		if i > 0 {
			if CNF {
				conds += " AND "
			} else {
				conds += " OR "
			}
		}
		cond := randMVIndexCond(rand.Intn(4), valOpts, randJCol, randNCol)
		conds += cond
	}
	return conds
}

func randMVIndexConds(nConds int, valOpts randMVIndexValOpts, randJCol, randNCol func() string) string {
	var conds string
	for i := 0; i < nConds; i++ {
		if i > 0 {
			if rand.Intn(5) < 1 { // OR
				conds += " OR "
			} else { // AND
				conds += " AND "
			}
		}
		cond := randMVIndexCond(rand.Intn(4), valOpts, randJCol, randNCol)
		conds += cond
	}
	return conds
}

func randMVIndexCond(condType int, valOpts randMVIndexValOpts, randJCol, randNCol func() string) string {
	switch condType {
	case 0: // member_of
		return fmt.Sprintf(`(%v member of (%v))`, randMVIndexValue(valOpts), randJCol())
	case 1: // json_contains
		return fmt.Sprintf(`json_contains(%v, '%v')`, randJCol(), randArray(valOpts))
	case 2: // json_overlaps
		return fmt.Sprintf(`json_overlaps(%v, '%v')`, randJCol(), randArray(valOpts))
	default: // others
		return fmt.Sprintf(`%v < %v`, randNCol(), rand.Intn(valOpts.distinct))
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
