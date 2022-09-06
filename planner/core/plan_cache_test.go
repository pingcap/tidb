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
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

type mockParameterizer struct {
	action string
}

func (mp *mockParameterizer) Parameterize(originSQL string) (paramSQL string, params []expression.Expression, ok bool, err error) {
	switch mp.action {
	case "error":
		return "", nil, false, errors.New("error")
	case "not_support":
		return "", nil, false, nil
	}
	// only support SQL like 'select * from t where col {op} {int} and ...'
	prefix := "select * from t where "
	if !strings.HasPrefix(originSQL, prefix) {
		return "", nil, false, nil
	}
	buf := make([]byte, 0, 32)
	buf = append(buf, prefix...)
	for i, condStr := range strings.Split(originSQL[len(prefix):], "and") {
		if i > 0 {
			buf = append(buf, " and "...)
		}
		tmp := strings.Split(strings.TrimSpace(condStr), " ")
		if len(tmp) != 3 { // col {op} {val}
			return "", nil, false, nil
		}
		buf = append(buf, tmp[0]...)
		buf = append(buf, tmp[1]...)
		buf = append(buf, '?')

		intParam, err := strconv.Atoi(tmp[2])
		if err != nil {
			return "", nil, false, nil
		}
		params = append(params, &expression.Constant{Value: types.NewDatum(intParam), RetType: types.NewFieldType(mysql.TypeLong)})
	}
	return string(buf), params, true, nil
}

func TestInitLRUWithSystemVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_prepared_plan_cache_size = 0") // MinValue: 1
	tk.MustQuery("select @@session.tidb_prepared_plan_cache_size").Check(testkit.Rows("1"))
	sessionVar := tk.Session().GetSessionVars()

	lru := plannercore.NewLRUPlanCache(uint(sessionVar.PreparedPlanCacheSize), 0, 0, plannercore.PickPlanFromBucket)
	require.NotNil(t, lru)
}

func TestGeneralPlanCacheBasically(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, primary key(a), key(b), key(c, d))`)
	for i := 0; i < 20; i++ {
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
	}

	for _, query := range queries {
		tk.MustExec(`set tidb_enable_general_plan_cache=0`)
		resultNormal := tk.MustQuery(query).Sort()
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

		tk.MustExec(`set tidb_enable_general_plan_cache=1`)
		tk.MustQuery(query)                                                    // first process
		tk.MustQuery(query).Sort().Check(resultNormal.Rows())                  // equal to the result without plan-cache
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // this plan is from plan-cache
	}
}
