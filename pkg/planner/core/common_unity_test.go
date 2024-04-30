package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestUnity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (a int, b int, c int, key(a))`)
	tk.MustExec(`create table t2 (a int, b int, c int, key(a))`)
	tk.MustQuery(`explain format='unity' select * from t1, t2 where t1.a=t2.a`).Check(
		testkit.Rows(`{"Tables":{"test.t1":{"AsName":"","Columns":{"test.t1.a":{"NDV":0,"Nulls":0,"Min":"","Max":"","Histogram":[]}},"Indexes":{"test.t1.a":{"NDV":0,"Nulls":0}},"RealtimeRows":10000,"ModifiedRows":0},"test.t2":{"AsName":"","Columns":{"test.t2.a":{"NDV":0,"Nulls":0,"Min":"","Max":"","Histogram":[]}},"Indexes":{"test.t2.a":{"NDV":0,"Nulls":0}},"RealtimeRows":10000,"ModifiedRows":0}},"Hints":["use_index(test.t2, test.t2.a)","merge_join(test.t2)","leading(test.t2, test.t1)","use_index(test.t1, test.t1.a)","merge_join(test.t1)","index_join(test.t2)","hash_join(test.t1)","leading(test.t1, test.t2)","index_join(test.t1)","hash_join(test.t2)"]}`))
}
