package bindinfo_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/stretchr/testify/require"
)

func setupStmtSummary() {
	stmtsummaryv2.Setup(&stmtsummaryv2.Config{
		Filename: "tidb-statements.log",
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = true
	})
}

func closeStmtSummary() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = false
	})
	stmtsummaryv2.GlobalStmtSummary.Close()
	stmtsummaryv2.GlobalStmtSummary = nil
	_ = os.Remove(config.GetGlobalConfig().Instance.StmtSummaryFilename)
}

func newTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return tk
}

func newTestKitWithRoot(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := newTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}

func TestXXX(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()
	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)

	// Clear all statements.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int)`)
	tk.MustExec(`select a from t where a=1`)

	bindHandle := bindinfo.NewGlobalBindingHandle(&mockSessionPool{tk.Session()})
	err := bindHandle.RecordInactiveBindings(time.Time{})
	fmt.Println("????????? ", err)
}
