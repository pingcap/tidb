package preparesnap

import (
	"context"

	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/tikv/client-go/v2/tikv"
	"google.golang.org/grpc"
)

type Env interface {
	ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error)
	LoadRegionsInKeyRange(bo *tikv.Backoffer, startKey, endKey []byte) (regions []Region, err error)
}

type PrepareClient interface {
	Send(*brpb.PrepareSnapshotBackupRequest) error
	Recv() (*brpb.PrepareSnapshotBackupResponse, error)
}

type Region interface {
	GetMeta() *metapb.Region
	GetLeaderStoreID() uint64
}

type CliEnv struct {
	Cache *tikv.RegionCache
	Mgr   *utils.StoreManager
}

func (c CliEnv) ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error) {
	var cli brpb.Backup_PrepareSnapshotBackupClient
	c.Mgr.TryWithConn(ctx, storeID, func(cc *grpc.ClientConn) error {
		bcli := brpb.NewBackupClient(cc)
		c, err := bcli.PrepareSnapshotBackup(ctx)
		if err != nil {
			return errors.Annotatef(err, "failed to create prepare backup stream")
		}
		cli = c
		return nil
	})
	return cli, nil
}

func (c CliEnv) LoadRegionsInKeyRange(bo *tikv.Backoffer, startKey []byte, endKey []byte) (regions []Region, err error) {
	if len(endKey) == 0 {
		// This is encoded [0xff; 8].
		// Workaround for https://github.com/tikv/client-go/issues/1051.
		endKey = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	}
	rs, err := c.Cache.LoadRegionsInKeyRange(bo, startKey, endKey)
	if err != nil {
		return nil, err
	}
	rrs := make([]Region, 0, len(rs))
	for _, r := range rs {
		rrs = append(rrs, r)
	}
	return rrs, nil
}
