// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestGetCheckpointOfTask(t *testing.T) {
	task := stream.TaskStatus{
		Info: backuppb.StreamBackupTaskInfo{
			StartTs: 8,
		},
		Progress: map[uint64]uint64{
			1: 10,
			2: 8,
			6: 12,
		},
	}

	require.Equal(t, task.GetCheckpoint(), uint64(8), "progress = %v", task.Progress)
	task.Progress[2] = 18

	require.Equal(t, task.GetCheckpoint(), uint64(10))
	task.Progress[1] = 14

	require.Equal(t, task.GetCheckpoint(), uint64(12), "progress = %v", task.Progress)
}
