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
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestCostModelTraceNew(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, key(b), INDEX idx_b(b))`)
	vals := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", i, i, i))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(vals, ", ")))
	tk.MustExec("analyze table t")
	tk.MustExec("set @@tidb_cost_model_version=2")
	tk.MustExec("set @@tidb_enable_index_merge_join=true")
	tk.MustExec("set @@tidb_enable_index_merge=true")

	for _, q := range []string{
		"select * from t where a=4",
		"select * from t where a in (1,2,3,4)",
		"select * from t",
		"select * from t where a<4",
		"select * from t use index(b) where b<4",
		"select * from t where a<4 order by b",
		"select * from t where a<4 order by b limit 3",
		"select sum(a) from t where a<4 group by b, c",
		"select max(a), b, c from t where a<4 group by b, c",
		"select * from t t1, t t2",
		"select * from t t1, t t2 where t1.a=t2.a",
		"select a from t union select b from t",
		"select * from t where a in (select b from t)",
		"select /*+ use_index_merge(t) */ * from t where a<4 or b < 4",
		"select /*+ tidb_inlj(t1, t2) */ * from t t1, t t2 where t1.b=t2.b",
		"select /*+ merge_join(t1, t2) */ * from t t1, t t2 where t1.b=t2.b",
		"select /*+ inl_join(t1, t2) */ * from t t1, t t2 where t1.b=t2.b",
		"select /*+ inl_hash_join(t1, t2) */ * from t t1, t t2 where t1.b=t2.b",
		"select /*+ hash_agg()*/count(*) from t where a<4 group by b, c",
		"select /*+ stream_agg()*/count(*) from t where a<4 group by b, c",
	} {
		//plan := tk.MustQuery(q).Rows()
		plan := tk.MustQuery("explain analyze format='true_card_cost' " + q).Rows()
		fmt.Printf("================%v\n", plan)
		//planCost, err := strconv.ParseFloat(plan[0][2].(string), 64)
		//require.Nil(t, err)

		// check whether cost mismatch
		ok := true
		warns := tk.MustQuery("show warnings").Rows()
		for _, warn := range warns {
			msg := warn[2].(string)
			if strings.HasPrefix(msg, "cost mismatch: ") {
				fmt.Printf("========%v\n", msg)
				ok = false
			}
		}
		require.True(t, ok)
	}
}

type Item struct {
	name  string
	value float64
}

type CostInput struct {
	params  []Item
	op      []string
	formula string
	cost    float64
}

func newCostInput(str string) CostInput {
	input := CostInput{params: make([]Item, 0)}
	a := strings.Split(str, ";")
	b := strings.Split(a[0], "=")
	input.formula = b[0]
	input.formula = strings.ReplaceAll(input.formula, ")/", ") /")
	input.formula = strings.ReplaceAll(input.formula, ")*", ") *")
	fmt.Printf("%s\n", str)
	fmt.Printf("formula: %s\n", b[0])
	input.cost, _ = strconv.ParseFloat(b[1], 64)
	c := strings.Split(a[1], ",")
	for _, v := range c {
		d := strings.Split(v, "=")
		val, _ := strconv.ParseFloat(d[1], 64)
		input.params = append(input.params, Item{d[0], val})
	}
	args := strings.Split(b[0], " ")
	for _, v := range args {
		if v == "(" {
			continue
		}
		found := false
		for _, v1 := range input.params {
			if v1.name == v {
				found = true
				break
			}
		}
		if !found {
			input.op = append(input.op, v)
		}
	}
	return input
}

func (c CostInput) Calc(t *testing.T, isTrace bool) *core.CostBuilder {
	i := 0
	isFirst := true
	var builder *core.CostBuilder
	for _, v := range c.params {
		a := core.NewCostItemForTest(v.name, v.value)
		if isFirst {
			builder = core.GetCostBuilderForTest(a, isTrace)
			isFirst = false
			continue
		}
		builder.EvalOpForTest(c.op[i], a)
		i++
	}
	val := builder.Value()
	fmt.Printf("val : %v\n", val)
	if isTrace {
		require.Equal(t, val.AsFormula(), c.formula)
	}
	require.Equal(t, val.GetCost(), c.cost)
	return builder
}

func TestCostBuilder(t *testing.T) {
	inputs := []string{"a + b * c * d=70;a=10,b=10,c=2,d=3",
		"a + b * c - d=27;a=10,b=10,c=2,d=3",
		"a + b * c - d * e=15;a=10,b=10,c=2,d=3,e=5",
		"a + b * c - d * e=0;a=10,b=10,c=2,d=3,e=10",
		"a + b * c - d * e * f=-30;a=10,b=10,c=2,d=3,e=10,f=2",
		"( a + b )* c=40;a=10,b=10,c=2",
		"( a + b )* c + d + e - f=-59.27;a=10,b=10,c=2,d=0.4,e=0.33,f=100",
		"( a + b )/ c=10;a=10,b=10,c=2",
		"( a + b )/ c - d=90;a=10,b=10,c=0.2,d=10",
		"( a + b )/ c - d * e=70;a=10,b=10,c=0.2,d=10,e=3",
	}
	for _, v := range inputs {
		i := newCostInput(v)
		i.Calc(t, true)
		i.Calc(t, false)
	}

	inputs2 := []string{"a + b * c * d=70;a=10,b=10,c=2,d=3",
		"a + b * c - d * e=20;a=10,b=10,c=2,d=2,e=5"}
	i1 := newCostInput(inputs2[0])
	b1 := i1.Calc(t, true)
	i2 := newCostInput(inputs2[1])
	b2 := i2.Calc(t, true)
	b1.EvalOpForTest("+", b2.Value())
	val := b1.Value()
	require.Equal(t, val.AsFormula(), i1.formula+" + ( "+i2.formula+" )")
	require.Equal(t, val.GetCost(), float64(90))

	i1 = newCostInput(inputs2[0])
	b1 = i1.Calc(t, true)
	b1.EvalOpForTest("*", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), i1.formula+" * ( "+i2.formula+" )")
	require.Equal(t, val.GetCost(), float64(1210))

	i1 = newCostInput(inputs2[0])
	b1 = i1.Calc(t, true)
	b1.EvalOpForTest(")*", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), "( "+i1.formula+" ) * ( "+i2.formula+" )")
	require.Equal(t, val.GetCost(), float64(1400))

	i1 = newCostInput(inputs2[0])
	b1 = i1.Calc(t, true)
	b1.EvalOpForTest("/", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), i1.formula+" / ( "+i2.formula+" )")
	require.Equal(t, val.GetCost(), float64(13))

	i1 = newCostInput(inputs2[0])
	b1 = i1.Calc(t, true)
	b1.EvalOpForTest(")/", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), "( "+i1.formula+" ) / ( "+i2.formula+" )")
	require.Equal(t, val.GetCost(), float64(3.5))

	i1 = newCostInput(inputs2[0])
	b1 = i1.Calc(t, true)
	b1.EvalOpForTest("-", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), i1.formula+" - ( "+i2.formula+" )")
	require.Equal(t, val.GetCost(), float64(50))

	i1 = newCostInput(inputs2[0])
	i2 = newCostInput(inputs2[1])
	b1 = i1.Calc(t, true).SetNameForTest("b1")
	b2 = i2.Calc(t, true).SetNameForTest("b2")
	b1.EvalOpForTest("+", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), "b1 + b2")
	require.Equal(t, val.GetCost(), float64(90))

	i1 = newCostInput(inputs2[0])
	i2 = newCostInput(inputs2[1])
	b1 = i1.Calc(t, true)
	b2 = i2.Calc(t, true).SetNameForTest("b2")
	b1.EvalOpForTest("+", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), i1.formula+" + b2")
	require.Equal(t, val.GetCost(), float64(90))

	i1 = newCostInput(inputs2[0])
	i2 = newCostInput(inputs2[1])
	b1 = i1.Calc(t, true)
	b2 = i2.Calc(t, true).SetNameForTest("b2")
	b1.EvalOpForTest("*", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), i1.formula+" * b2")
	require.Equal(t, val.GetCost(), float64(1210))

	i1 = newCostInput(inputs2[0])
	i2 = newCostInput(inputs2[1])
	b1 = i1.Calc(t, true).SetNameForTest("b1")
	b2 = i2.Calc(t, true).SetNameForTest("b2")
	b1.EvalOpForTest("*", b2.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), "b1 * b2")
	require.Equal(t, val.GetCost(), float64(1400))

	i1 = newCostInput(inputs2[0])
	i2 = newCostInput(inputs2[1])
	i3 := newCostInput(inputs2[0])
	b1 = i1.Calc(t, true).SetNameForTest("b1")
	b2 = i2.Calc(t, true).SetNameForTest("b2")
	b3 := i3.Calc(t, true).SetNameForTest("b3")
	b1.EvalOpForTest("*", b2.Value())
	b1.EvalOpForTest("+", b3.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), "b1 * b2 + b3")
	require.Equal(t, val.GetCost(), float64(1470))

	i1 = newCostInput(inputs2[0])
	i2 = newCostInput(inputs2[1])
	i3 = newCostInput(inputs2[0])
	i4 := newCostInput(inputs2[0])
	b1 = i1.Calc(t, true).SetNameForTest("b1")
	b2 = i2.Calc(t, true).SetNameForTest("b2")
	b3 = i3.Calc(t, true).SetNameForTest("b3")
	b4 := i4.Calc(t, true).SetNameForTest("b4")
	b1.EvalOpForTest("*", b2.Value())
	b1.EvalOpForTest("+", b3.Value())
	b1.EvalOpForTest("*", b4.Value())
	val = b1.Value()
	require.Equal(t, val.AsFormula(), "b1 * b2 + b3 * b4")
	require.Equal(t, val.GetCost(), float64(6300))
}
