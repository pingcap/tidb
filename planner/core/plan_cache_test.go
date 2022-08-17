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
	case "panic":
		panic("panic")
	}
	// only support SQL like 'select * from t where col {op} {val} and ...'
	prefix := "select * from t where "
	if !strings.HasPrefix(originSQL, prefix) {
		return "", nil, false, nil
	}
	buf := make([]byte, 0, 32)
	buf = append(buf, prefix...)
	condStrs := strings.Split(originSQL[len(prefix):], "and")
	for i, condStr := range condStrs {
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
		param, err := mp.param(tmp[2])
		if err != nil {
			return "", nil, false, nil
		}
		params = append(params, param)
	}
	return string(buf), params, true, nil
}

func (mp *mockParameterizer) param(valStr string) (v expression.Expression, err error) {
	valStr = strings.TrimSpace(valStr)
	var t byte
	var val interface{}
	if valStr[0] == '"' { // string value
		val, err = strconv.Unquote(valStr)
		t = mysql.TypeVarchar
	} else if strings.Contains(valStr, ".") { // double
		val, err = strconv.ParseFloat(valStr, 64)
		t = mysql.TypeDouble
	} else { // int
		val, err = strconv.Atoi(valStr)
		t = mysql.TypeLong
	}
	return &expression.Constant{Value: types.NewDatum(val), RetType: types.NewFieldType(t)}, err
}

func TestGeneralPlanCacheParameterizer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKitWithGeneralPlanCache(t, store)

	mp := new(mockParameterizer)
	tk.Session().SetValue(plannercore.ParameterizerKey, mp)

	tk.MustExec("set tidb_enable_general_plan_cache=1")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key(a))")
	tk.MustExec("select * from t where a > 1")
	tk.MustExec("select * from t where a > 2")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("select * from t where a > 2 and a < 100")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("select * from t where a > 2 and a < 2200")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}
