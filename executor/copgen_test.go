package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type TestSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func TestCopGen(t *testing.T) {
	store, clean, testGen := testkit.CreateMockStoreWithTestGen(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment primary key, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("insert into t(b, c) (select b, c from t)")
	tk.MustExec("insert into t(b, c) (select b, c from t)")
	tk.MustExec("create table t2(a int auto_increment primary key, b int, c int)")
	tk.MustExec("insert into t2(b, c) (select b, c from t)")

	err := testGen.AddTable("test", "t")
	require.NoError(t, err)
	err = testGen.AddTable("test", "t2")
	require.NoError(t, err)

	require.NoError(t, testGen.Prepare())
	tk.MustQuery("select * from t where a * 2 - 1 = 1").Check(testkit.Rows("1 1 1"))
	require.NoError(t, testGen.Dump("/tmp/copgen_test_data.json"))
}
