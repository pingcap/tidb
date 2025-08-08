package teststore

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testenv"
	"github.com/stretchr/testify/require"
)

// NewMockStoreWithoutBootstrap creates a mock store without bootstrap.
// If you need to bootstrap the store, use `testkit.CreateMockStore` or `testkit.CreateMockStoreAndDomain` instead.
func NewMockStoreWithoutBootstrap(t testing.TB, opts ...mockstore.MockTiKVStoreOption) kv.Storage {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)

	if kerneltype.IsNextGen() {
		testenv.UpdateConfigForNextgen(t)
		kvstore.SetSystemStorage(store)
	}
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}
