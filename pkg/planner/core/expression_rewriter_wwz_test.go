package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestFuck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test")
	tk.MustExec("create table test(id varchar(32))")
	tk.MustQuery("select table_name from information_schema.tables where lower(table_name) = 'TEST';").Check(testkit.Rows("test"))
}
