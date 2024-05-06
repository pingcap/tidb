package core_test

import (
	"encoding/json"
	"fmt"
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
	//formatPrint(tk, `explain format='unity' select * from t1, t2 where t1.a=t2.a`)
	//formatPrint(tk, `explain format='unity' select 1 from t1, t2, t3 where t1.a=t2.a and t2.a=t3.a`)
	formatPrint(tk, `explain format='unity' select a from t4`)
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
