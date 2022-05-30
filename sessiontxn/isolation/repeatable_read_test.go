package isolation_test

import (
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRepeatableReadProvider(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	initializeRepeatableReadProvider(t, tk, "PESSIMISTIC")

}

func initializeRepeatableReadProvider(t *testing.T, tk *testkit.TestKit, txnMode string) *isolation.RepeatableReadTxnContextProvider {
	tk.MustExec("set @@tx_isolation='REPEATABLE-READ'")
	tk.MustExec("begin pessimistic")
	provider := sessiontxn.GetTxnManager(tk.Session()).GetContextProvider()
	require.IsType(t, &isolation.RepeatableReadTxnContextProvider{}, provider)
	return provider.(*isolation.RepeatableReadTxnContextProvider)
}
