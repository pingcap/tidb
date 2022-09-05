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
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
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

func TestGeneralPlanCacheParameterizer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKitWithGeneralPlanCache(t, store)

	mp := new(mockParameterizer)
	tk.Session().SetValue(plannercore.ParameterizerKey, mp)

	tk.MustExec("set tidb_enable_general_plan_cache=1")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (0), (1), (2), (3), (4), (5)")
	tk.MustQuery("select * from t where a > 1").Sort().Check(testkit.Rows("2", "3", "4", "5"))
	tk.MustQuery("select * from t where a > 3").Sort().Check(testkit.Rows("4", "5"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where a > 1 and a < 5").Sort().Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("select * from t where a > 2 and a < 5").Sort().Check(testkit.Rows("3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	mp.action = "error"
	tk.MustQuery("select * from t where a > 2 and a < 5").Sort().Check(testkit.Rows("3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	mp.action = "not_support"
	tk.MustQuery("select * from t where a > 2 and a < 5").Sort().Check(testkit.Rows("3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	mp.action = ""
	tk.MustQuery("select * from t where a > 2 and a < 5").Sort().Check(testkit.Rows("3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestGeneralPlanCache4GeneratedParameterizer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKitWithGeneralPlanCache(t, store)

	tk.MustExec("set tidb_enable_general_plan_cache=1")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (0), (1), (2), (3), (4), (5)")
	tk.MustQuery("select * from t where a > 1").Sort().Check(testkit.Rows("2", "3", "4", "5"))
	tk.MustQuery("select * from t where a > 3").Sort().Check(testkit.Rows("4", "5"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where a > 1 and a < 5").Sort().Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("select * from t where a > 2 and a < 5").Sort().Check(testkit.Rows("3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("select * from t t1 join t t2 on t1.a = t2.a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("select * from t t1 join t t2 on t1.a = t2.a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}
