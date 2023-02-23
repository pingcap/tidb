// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"time"

	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/engine"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Env is the interface required by the advancer.
type Env interface {
	// The region scanner provides the region information.
	TiKVClusterMeta
	// LogBackupService connects to the TiKV, so we can collect the region checkpoints.
	LogBackupService
	// StreamMeta connects to the metadata service (normally PD).
	StreamMeta
}

// PDRegionScanner is a simple wrapper over PD
// to adapt the requirement of `RegionScan`.
type PDRegionScanner struct {
	pd.Client
}

// RegionScan gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
func (c PDRegionScanner) RegionScan(ctx context.Context, key []byte, endKey []byte, limit int) ([]RegionWithLeader, error) {
	rs, err := c.Client.ScanRegions(ctx, key, endKey, limit)
	if err != nil {
		return nil, err
	}
	rls := make([]RegionWithLeader, 0, len(rs))
	for _, r := range rs {
		rls = append(rls, RegionWithLeader{
			Region: r.Meta,
			Leader: r.Leader,
		})
	}
	return rls, nil
}

func (c PDRegionScanner) Stores(ctx context.Context) ([]Store, error) {
	res, err := c.Client.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	r := make([]Store, 0, len(res))
	for _, re := range res {
		if !engine.IsTiFlash(re) {
			r = append(r, Store{
				BootAt: uint64(re.StartTimestamp),
				ID:     re.GetId(),
			})
		}
	}
	return r, nil
}

// clusterEnv is the environment for running in the real cluster.
type clusterEnv struct {
	clis *utils.StoreManager
	*AdvancerExt
	PDRegionScanner
}

// GetLogBackupClient gets the log backup client.
func (t clusterEnv) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	var cli logbackup.LogBackupClient
	err := t.clis.WithConn(ctx, storeID, func(cc *grpc.ClientConn) {
		cli = logbackup.NewLogBackupClient(cc)
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

// CliEnv creates the Env for CLI usage.
func CliEnv(cli *utils.StoreManager, etcdCli *clientv3.Client) Env {
	return clusterEnv{
		clis:            cli,
		AdvancerExt:     &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner: PDRegionScanner{cli.PDClient()},
	}
}

// TiDBEnv creates the Env by TiDB config.
func TiDBEnv(pdCli pd.Client, etcdCli *clientv3.Client, conf *config.Config) (Env, error) {
	tconf, err := conf.GetTiKVConfig().Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	return clusterEnv{
		clis: utils.NewStoreManager(pdCli, keepalive.ClientParameters{
			Time:    time.Duration(conf.TiKVClient.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(conf.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
		}, tconf),
		AdvancerExt:     &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner: PDRegionScanner{Client: pdCli},
	}, nil
}

type LogBackupService interface {
	// GetLogBackupClient gets the log backup client.
	GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error)
}

// StreamMeta connects to the metadata service (normally PD).
// It provides the global checkpoint information.
type StreamMeta interface {
	// Begin begins listen the task event change.
	Begin(ctx context.Context, ch chan<- TaskEvent) error
	// UploadV3GlobalCheckpointForTask uploads the global checkpoint to the meta store.
	UploadV3GlobalCheckpointForTask(ctx context.Context, taskName string, checkpoint uint64) error
	// ClearV3GlobalCheckpointForTask clears the global checkpoint to the meta store.
	ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error
}
