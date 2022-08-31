// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/stretchr/testify/require"
)

func TestGetCheckpointOfTask(t *testing.T) {
	task := stream.TaskStatus{
		Info: backuppb.StreamBackupTaskInfo{
			StartTs: 8,
		},
		Checkpoints: []streamhelper.Checkpoint{
			{
				ID: 1,
				TS: 10,
			},
			{
				ID: 2,
				TS: 8,
			},
			{
				ID: 6,
				TS: 12,
			},
		},
	}

	require.Equal(t, task.GetMinStoreCheckpoint().TS, uint64(8), "progress = %v", task.Checkpoints)
	task.Checkpoints[1].TS = 18

	require.Equal(t, task.GetMinStoreCheckpoint().TS, uint64(10))
	task.Checkpoints[0].TS = 14

	require.Equal(t, task.GetMinStoreCheckpoint().TS, uint64(12), "progress = %v", task.Checkpoints)
}
