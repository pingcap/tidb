// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"

	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/stream"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
)

var (
	_ Env = &CliEnv{}
	_ Env = tidbEnv{}
)

type Env interface {
	RegionScanner
	LogBackupService
	StreamMeta
}

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

type CliEnv struct {
	PDRegionScanner
	*conn.Mgr
	stream.TaskEventClient
}

type tidbEnv struct {
	clis *conn.StoreManager
	*stream.TaskEventClient
	PDRegionScanner
}

// GetLogBackupClient gets the log backup client.
func (t tidbEnv) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	var cli logbackup.LogBackupClient
	err := t.clis.WithConn(ctx, storeID, func(cc *grpc.ClientConn) {
		cli = logbackup.NewLogBackupClient(cc)
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

type LogBackupService interface {
	// GetLogBackupClient gets the log backup client.
	GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error)
}

type StreamMeta interface {
	// Begin begins listen the task event change.
	Begin(ctx context.Context, ch chan<- stream.TaskEvent) error
	// UploadV3GlobalCheckpointForTask uploads the global checkpoint to the meta store.
	UploadV3GlobalCheckpointForTask(ctx context.Context, taskName string, checkpoint uint64) error
}
