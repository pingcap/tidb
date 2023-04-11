package internal

import (
	"runtime"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/stretchr/testify/require"
	atomicutil "go.uber.org/atomic"
)

// CreateStoreAndBootstrap creates a mock store and bootstrap it.
func CreateStoreAndBootstrap(t *testing.T) (kv.Storage, *domain.Domain) {
	runtime.GOMAXPROCS(mathutil.Min(8, runtime.GOMAXPROCS(0)))
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	return store, dom
}

var sessionKitIDGenerator atomicutil.Uint64

// CreateSessionAndSetID creates a session and set connection ID.
func CreateSessionAndSetID(t *testing.T, store kv.Storage) session.Session {
	se, err := session.CreateSession4Test(store)
	se.SetConnectionID(sessionKitIDGenerator.Inc())
	require.NoError(t, err)
	return se
}
