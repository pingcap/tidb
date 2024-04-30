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
	data := tk.MustQuery(`explain format='unity' select * from t1, t2 where t1.a=t2.a`).Rows()[0][0]
	jsonData := data.(string)

	var j core.UnityOutput
	if err := json.Unmarshal([]byte(jsonData), &j); err != nil {
		panic(err)
	}
	v, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))
}
