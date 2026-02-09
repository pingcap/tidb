// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
	"context"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
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

func TestMetadataHelperReadFile(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpdir)
	require.Nil(t, err)
	helper := stream.NewMetadataHelper()
	filename1 := "full_data"
	filename2 := "misc_data"
	data1 := []byte("Test MetadataHelper. The data contains bare data (or maybe compressed data).")
	// data2 is the compressed content from data1
	data2 := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x0, 0x58, 0x15, 0x2, 0x0, 0x52, 0x44, 0xe, 0x14, 0xb0, 0x37, 0x1, 0xe7,
		0xa4, 0x1c, 0xd9, 0x3d, 0xc7, 0xee, 0xe7, 0x3e, 0x15, 0x14, 0x80, 0xdc, 0x25, 0x14, 0xc, 0x60, 0x24, 0x70,
		0xda, 0x6a, 0x47, 0xfc, 0x2d, 0xa4, 0x4e, 0x6f, 0xe, 0xa3, 0x9e, 0x2, 0xce, 0xa6, 0x98, 0xa, 0x67, 0x52,
		0xa7, 0x4e, 0x5b, 0x4f, 0x94, 0x92, 0xfb, 0x32, 0x2e, 0x38, 0x6d, 0xce, 0xf, 0x4d, 0x7c, 0x9, 0x1, 0x0,
		0xb2, 0x6c, 0x96, 0x1}

	// write data at first
	err = s.WriteFile(ctx, filename1, data1)
	require.NoError(t, err)
	err = s.WriteFile(ctx, filename2, append(append([]byte{}, data1...), data2...))
	require.NoError(t, err)

	helper.InitCacheEntry(filename2, 2)
	get_data, err := helper.ReadFile(ctx, filename1, 0, 0, backuppb.CompressionType_UNKNOWN, s, nil)
	require.NoError(t, err)
	require.Equal(t, data1, get_data)
	get_data, err = helper.ReadFile(ctx, filename2, 0, uint64(len(data1)), backuppb.CompressionType_UNKNOWN, s, nil)
	require.NoError(t, err)
	require.Equal(t, data1, get_data)
	get_data, err = helper.ReadFile(ctx, filename2, uint64(len(data1)), uint64(len(data2)),
		backuppb.CompressionType_ZSTD, s, nil)
	require.NoError(t, err)
	require.Equal(t, data1, get_data)
}

func TestFilterPath(t *testing.T) {
	type args struct {
		path         string
		shiftStartTS uint64
		restoreTS    uint64
	}
	tests := []struct {
		name     string
		args     args
		expected string
	}{
		{
			name: "normal: minDefaultTs < minTs",
			args: args{
				path:         "v1/backupmeta/000000000000000a-0000000000000005-000000000000000a-000000000000001e.meta", // flush=10, minDefault=5, min=10, max=30
				shiftStartTS: 5,
				restoreTS:    10,
			},
			expected: "v1/backupmeta/000000000000000a-0000000000000005-000000000000000a-000000000000001e.meta",
		},
		{
			name: "normal: minDefaultTs == minTs",
			args: args{
				path:         "v1/backupmeta/000000000000000a-000000000000000a-000000000000000a-000000000000001e.meta", // all = 10
				shiftStartTS: 5,
				restoreTS:    10,
			},
			expected: "v1/backupmeta/000000000000000a-000000000000000a-000000000000000a-000000000000001e.meta",
		},
		{
			name: "fallback: minDefaultTs == 0",
			args: args{
				path:         "v1/backupmeta/000000000000000a-0000000000000000-000000000000000a-000000000000001e.meta", // minDefault=0, min=10
				shiftStartTS: 5,
				restoreTS:    10,
			},
			expected: "v1/backupmeta/000000000000000a-0000000000000000-000000000000000a-000000000000001e.meta",
		},
		{
			name: "fallback: minDefaultTs > minTs, should fallback to minTs",
			args: args{
				path:         "v1/backupmeta/000000000000000a-0000000000000014-000000000000000a-000000000000001e.meta", // minDefault=20, min=10
				shiftStartTS: 5,
				restoreTS:    11, // fallback to 10, 11>10
			},
			expected: "v1/backupmeta/000000000000000a-0000000000000014-000000000000000a-000000000000001e.meta",
		},
		{
			name: "restoreTS < fallback minTs, should be filtered",
			args: args{
				path:         "v1/backupmeta/000000000000000a-0000000000000014-000000000000000a-000000000000001e.meta", // fallback to min=10
				shiftStartTS: 5,
				restoreTS:    9, // 9 < 10, should be filtered
			},
			expected: "",
		},
		{
			name: "maxTs < shiftStartTS, should be filtered",
			args: args{
				path:         "v1/backupmeta/000000000000000a-0000000000000005-000000000000000a-0000000000000004.meta", // max=4 < 5
				shiftStartTS: 5,
				restoreTS:    10,
			},
			expected: "",
		},
		{
			name: "invalid: minDefaultTs > minTs, fallback, but restoreTS < fallback",
			args: args{
				path:         "v1/backupmeta/000000000000000a-0000000000000014-000000000000000a-000000000000001e.meta",
				shiftStartTS: 5,
				restoreTS:    8, // fallback to 10, 8 < 10
			},
			expected: "",
		},
		{
			name: "non-matching file name format, preserved for compatibility",
			args: args{
				path:         "v1/backupmeta/unexpected_format.meta",
				shiftStartTS: 10,
				restoreTS:    10,
			},
			expected: "v1/backupmeta/unexpected_format.meta",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stream.FilterPathByTs(tt.args.path, tt.args.shiftStartTS, tt.args.restoreTS)
			require.Equal(t, tt.expected, got)
		})
	}
}
