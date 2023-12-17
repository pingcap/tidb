package preparesnap

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Env interface {
	ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error)
	GetAllLiveStores(ctx context.Context) ([]*metapb.Store, error)
	LoadRegionsInKeyRange(ctx context.Context, startKey, endKey []byte) (regions []Region, err error)
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

func (c CliEnv) GetAllLiveStores(ctx context.Context) ([]*metapb.Store, error) {
	return c.Cache.PDClient().GetAllStores(ctx, pd.WithExcludeTombstone())
}

func (c CliEnv) ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error) {
	var cli brpb.Backup_PrepareSnapshotBackupClient
	err := c.Mgr.TryWithConn(ctx, storeID, func(cc *grpc.ClientConn) error {
		bcli := brpb.NewBackupClient(cc)
		c, err := bcli.PrepareSnapshotBackup(ctx)
		if err != nil {
			return errors.Annotatef(err, "failed to create prepare backup stream")
		}
		cli = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (c CliEnv) LoadRegionsInKeyRange(ctx context.Context, startKey []byte, endKey []byte) (regions []Region, err error) {
	bo := tikv.NewBackoffer(ctx, regionCacheMaxBackoffMs)
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

type RetryEnv struct {
	Env
	GetBackoffer func() utils.Backoffer
}

func (r RetryEnv) ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error) {
	rs := utils.InitialRetryState(50, 10*time.Second, 10*time.Second)
	bo := utils.Backoffer(&rs)
	if r.GetBackoffer != nil {
		bo = r.GetBackoffer()
	}
	return utils.WithRetryV2(ctx, bo, func(ctx context.Context) (PrepareClient, error) {
		cli, err := r.Env.ConnectToStore(ctx, storeID)
		if err != nil {
			log.Warn("Failed to connect to store, will retry.", zap.Uint64("store", storeID), logutil.ShortError(err))
			return nil, err
		}
		return cli, nil
	})
}
