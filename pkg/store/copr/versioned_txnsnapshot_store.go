package copr

import (
	"time"

	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

// versionedTxnSnapshotStore is a txnsnapshot.kvstore implementation used by coprocessor workers.
// It ensures requests that carry range_versions use the VersionedKv RPC at the client layer.
type versionedTxnSnapshotStore struct {
	store        *tikv.KVStore
	versionedRPC *versionedRPCClient
	codec        tikv.Codec
}

func newVersionedTxnSnapshotStore(store *tikv.KVStore, versionedRPC *versionedRPCClient, codec tikv.Codec) *versionedTxnSnapshotStore {
	return &versionedTxnSnapshotStore{
		store:        store,
		versionedRPC: versionedRPC,
		codec:        codec,
	}
}

func (s *versionedTxnSnapshotStore) CheckVisibility(startTime uint64) error {
	return s.store.CheckVisibility(startTime)
}

func (s *versionedTxnSnapshotStore) GetRegionCache() *tikv.RegionCache {
	return s.store.GetRegionCache()
}

func (s *versionedTxnSnapshotStore) GetLockResolver() *txnlock.LockResolver {
	return s.store.GetLockResolver()
}

func (s *versionedTxnSnapshotStore) GetTiKVClient() tikv.Client {
	client := s.store.GetTiKVClient()
	if s.versionedRPC == nil {
		return client
	}
	return &tikvClient{c: client, versionedRPC: s.versionedRPC, codec: s.codec}
}

func (s *versionedTxnSnapshotStore) SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID tikv.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := tikv.NewRegionRequestSender(s.GetRegionCache(), s.GetTiKVClient(), s.GetOracle())
	resp, _, err := sender.SendReq(bo, req, regionID, timeout)
	return resp, err
}

func (s *versionedTxnSnapshotStore) GetOracle() oracle.Oracle {
	return s.store.GetOracle()
}

func (s *versionedTxnSnapshotStore) Go(fn func()) error {
	return s.store.Go(fn)
}
