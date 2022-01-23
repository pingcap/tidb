// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/keepalive"
)

var defaultKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

func TestRestoreConfigAdjust(t *testing.T) {
	cfg := &RestoreConfig{}
	cfg.adjustRestoreConfig()

	require.Equal(t, uint32(defaultRestoreConcurrency), cfg.Config.Concurrency)
	require.Equal(t, defaultSwitchInterval, cfg.Config.SwitchModeInterval)
	require.Equal(t, restore.DefaultMergeRegionKeyCount, cfg.MergeSmallRegionKeyCount)
	require.Equal(t, restore.DefaultMergeRegionSizeBytes, cfg.MergeSmallRegionSizeBytes)
}

func TestconfigureRestoreClient(t *testing.T) {
	cfg := Config{
		Concurrency: 1024,
	}
	restoreComCfg := RestoreCommonConfig{
		Online: true,
	}
	restoreCfg := &RestoreConfig{
		Config:              cfg,
		RestoreCommonConfig: restoreComCfg,
		DdlBatchSize:        128,
	}
	client := &restore.Client{}

	ctx := context.Background()
	err := configureRestoreClient(ctx, client, restoreCfg)
	require.NoError(t, err)
	require.Equal(t, client.GetBatchDdlSize(), 128)
	require.True(t, true, client.IsOnline())
}
