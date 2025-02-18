package core_test

import (
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestUnity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (a int, b int, c int, key(a))`)
	tk.MustExec(`create table t2 (a int, b int, c int, key(a))`)
	tk.MustExec(`create table t3 (a int, b int, c int, key(a))`)
	tk.MustExec(`create table t4 (a int, b int, c int, primary key (a), key(c))`)
	tk.MustExec(`create table t5 (a int, b int, c int, primary key (a, b), key(c))`)
	//formatPrint(tk, `explain format='unity_online' select * from t1, t2 where t1.a=t2.a`)
	//formatPrint(tk, `explain format='unity_online' select 1 from t1, t2, t3 where t1.a=t2.a and t2.a=t3.a`)
	formatPrint(tk, `explain format='unity_online' select a from t5`)
}

func TestUnityMCV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (a int, b int, c int, key(a))`)
	for i := 0; i < 1024; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t1 values (%v, %v, %v)`, i, i, i))
	}
	for i := 0; i < 10; i++ {
		for j := 0; j <= i; j++ {
			tk.MustExec(fmt.Sprintf(`insert into t1 values (%v, %v, %v)`, i, i, i))
		}
	}
	tk.MustExec(`analyze table t1 with 10 topn`)
	formatPrint(tk, `explain format='unity_online' select a from t1`)
	formatPrint(tk, `explain format='unity_online' select a from t1`)
}

func formatPrint(tk *testkit.TestKit, sql string) {
	data := tk.MustQuery(sql).Rows()[0][0]
	jsonData := data.(string)

	var j core.UnityOutput
	if err := json.Unmarshal([]byte(jsonData), &j); err != nil {
		panic(err)
	}

	for _, t := range j.Tables {
		for _, c := range t.Columns {
			c.Histogram = []core.UnityHistBucket{}
		}
	}

	v, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))
}

func TestAsName(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt("select * from t as t1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(reflect.TypeOf(stmt.(*ast.SelectStmt).From.TableRefs.Left.(*ast.TableSource).Source))
}

func TestPredCols(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int)`)

	for _, cond := range []string{
		"a=1", "a>1", "a<=1", "a in (1, 2, 3)", "a is null", "a between 1 and 10",
	} {
		stmt := fmt.Sprintf(`explain analyze format='unity_offline' select * from t where %v`, cond)
		ret := tk.MustQuery(stmt).Rows()[0]
		var plans []*core.UnityOfflinePlan
		if err := json.Unmarshal([]byte(ret[0].(string)), &plans); err != nil {
			t.Fatal(err)
		}
		for _, p := range plans {
			fmt.Println(">>>>>>>>>> ", stmt)
			fmt.Println("-->> ", p.SubPlans[0].PreSequence.PredicateColumns)
			require.True(t, len(p.SubPlans[0].PreSequence.PredicateColumns) > 0)
		}
	}
}

func TestUnityVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	for _, v := range []string {
		"enable_hashjoin", "enable_mergejoin", "enable_nestloop", "enable_indexscan", "enable_seqscan", "enable_indexonlyscan",
	}{
		tk.MustQuery(`select @@` + v).Check(testkit.Rows("1"))
		tk.MustExec(`set global ` + v + `=0`)
		tk.MustQuery(`select @@` + v).Check(testkit.Rows("0"))
	}
}
