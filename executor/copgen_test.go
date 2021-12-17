package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

type TestSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func TestCopGen(t *testing.T) {
	config := &unistore.TestGenConfig{}

	store, dom, clean := testkit.CreateMockStoreAndDomain(t, mockstore.WithTestGen(config), mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		config.Cluster = c.(*unistore.Cluster)
	}))
	config.Init(store, dom)
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
	// fmt.Println("")
	// fmt.Println("run select")
	// tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3"))

	err := config.AddTable("test", "t")
	require.NoError(t, err)
	err = config.AddTable("test", "t2")
	require.NoError(t, err)

	fmt.Println("")
	fmt.Println("")
	tk.MustQuery("select * from t where a * 2 - 1 = 1").Check(testkit.Rows("1 1 1"))

	err = config.SaveTestData("/tmp/copgen_test_data.json")
	require.NoError(t, err)
}
