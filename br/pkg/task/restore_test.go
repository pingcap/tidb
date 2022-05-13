// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/stretchr/testify/require"
)

func TestRestoreConfigAdjust(t *testing.T) {
	cfg := &RestoreConfig{}
	cfg.adjustRestoreConfig()

	require.Equal(t, uint32(defaultRestoreConcurrency), cfg.Config.Concurrency)
	require.Equal(t, defaultSwitchInterval, cfg.Config.SwitchModeInterval)
	require.Equal(t, conn.DefaultMergeRegionKeyCount, cfg.MergeSmallRegionKeyCount)
	require.Equal(t, conn.DefaultMergeRegionSizeBytes, cfg.MergeSmallRegionSizeBytes)
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
