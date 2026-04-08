// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoints

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/checkpoints/checkpointspb"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/stretchr/testify/require"
)

func TestMergeStatusCheckpoint(t *testing.T) {
	cpd := NewTableCheckpointDiff()

	m := StatusCheckpointMerger{EngineID: 0, Status: CheckpointStatusImported}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		hasStatus: false,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	}, cpd)

	m = StatusCheckpointMerger{EngineID: -1, Status: CheckpointStatusLoaded}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		hasStatus: false,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusLoaded,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	}, cpd)

	m = StatusCheckpointMerger{EngineID: WholeTableEngineID, Status: CheckpointStatusClosed}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		hasStatus: true,
		status:    CheckpointStatusClosed,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusLoaded,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	}, cpd)

	m = StatusCheckpointMerger{EngineID: -1, Status: CheckpointStatusAllWritten}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		hasStatus: true,
		status:    CheckpointStatusClosed,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusAllWritten,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	}, cpd)
}

func TestMergeInvalidStatusCheckpoint(t *testing.T) {
	cpd := NewTableCheckpointDiff()

	m := StatusCheckpointMerger{EngineID: 0, Status: CheckpointStatusLoaded}
	m.MergeInto(cpd)

	m = StatusCheckpointMerger{EngineID: -1, Status: CheckpointStatusAllWritten}
	m.SetInvalid()
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		hasStatus: true,
		status:    CheckpointStatusAllWritten / 10,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusLoaded,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusAllWritten / 10,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	}, cpd)
}

func TestMergeChunkCheckpoint(t *testing.T) {
	cpd := NewTableCheckpointDiff()

	key := ChunkCheckpointKey{Path: "/tmp/path/1.sql", Offset: 0}

	m := ChunkCheckpointMerger{
		EngineID: 2,
		Key:      key,
		Checksum: verification.MakeKVChecksum(700, 15, 1234567890),
		Pos:      1055,
		RealPos:  1053,
		RowID:    31,
	}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		engines: map[int32]engineCheckpointDiff{
			2: {
				chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
					key: {
						pos:      1055,
						realPos:  1053,
						rowID:    31,
						checksum: verification.MakeKVChecksum(700, 15, 1234567890),
					},
				},
			},
		},
	}, cpd)

	m = ChunkCheckpointMerger{
		EngineID: 2,
		Key:      key,
		Checksum: verification.MakeKVChecksum(800, 20, 1357924680),
		Pos:      1080,
		RealPos:  1070,
		RowID:    42,
	}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		engines: map[int32]engineCheckpointDiff{
			2: {
				chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
					key: {
						pos:      1080,
						realPos:  1070,
						rowID:    42,
						checksum: verification.MakeKVChecksum(800, 20, 1357924680),
					},
				},
			},
		},
	}, cpd)
}

func TestRebaseCheckpoint(t *testing.T) {
	cpd := NewTableCheckpointDiff()

	m := RebaseCheckpointMerger{
		AutoRandBase:  132861,
		AutoIncrBase:  132862,
		AutoRowIDBase: 132863,
	}
	m.MergeInto(cpd)

	expected := &TableCheckpointDiff{
		hasRebase:     true,
		autoRandBase:  132861,
		autoIncrBase:  132862,
		autoRowIDBase: 132863,
		engines:       make(map[int32]engineCheckpointDiff),
	}
	require.Equal(t, expected, cpd)

	// shouldn't go backwards
	m2 := RebaseCheckpointMerger{
		AutoRandBase:  131,
		AutoIncrBase:  132,
		AutoRowIDBase: 133,
	}
	m2.MergeInto(cpd)
	require.Equal(t, expected, cpd)
}

func TestApplyDiff(t *testing.T) {
	cp := TableCheckpoint{
		Status:        CheckpointStatusLoaded,
		AutoRandBase:  131,
		AutoIncrBase:  132,
		AutoRowIDBase: 133,
		Engines: map[int32]*EngineCheckpoint{
			-1: {
				Status: CheckpointStatusLoaded,
			},
			0: {
				Status: CheckpointStatusLoaded,
				Chunks: []*ChunkCheckpoint{
					{
						Key: ChunkCheckpointKey{Path: "/tmp/01.sql"},
						Chunk: mydump.Chunk{
							Offset:       0,
							RealOffset:   0,
							EndOffset:    20000,
							PrevRowIDMax: 0,
							RowIDMax:     1000,
						},
					},
					{
						Key: ChunkCheckpointKey{Path: "/tmp/04.sql"},
						Chunk: mydump.Chunk{
							Offset:       0,
							RealOffset:   0,
							EndOffset:    15000,
							PrevRowIDMax: 1000,
							RowIDMax:     1300,
						},
					},
				},
			},
		},
	}

	cpd := NewTableCheckpointDiff()
	(&StatusCheckpointMerger{EngineID: -1, Status: CheckpointStatusImported}).MergeInto(cpd)
	(&StatusCheckpointMerger{EngineID: WholeTableEngineID, Status: CheckpointStatusAllWritten}).MergeInto(cpd)
	(&StatusCheckpointMerger{EngineID: 1234, Status: CheckpointStatusAnalyzeSkipped}).MergeInto(cpd)
	(&RebaseCheckpointMerger{
		AutoRandBase:  1131,
		AutoIncrBase:  1132,
		AutoRowIDBase: 1133,
	}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 0,
		Key:      ChunkCheckpointKey{Path: "/tmp/01.sql"},
		Checksum: verification.MakeKVChecksum(3333, 4444, 5555),
		Pos:      6666,
		RealPos:  6565,
		RowID:    777,
	}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 5678,
		Key:      ChunkCheckpointKey{Path: "/tmp/04.sql"},
		Pos:      9999,
		RealPos:  9888,
		RowID:    888,
	}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 0,
		Key:      ChunkCheckpointKey{Path: "/tmp/03.sql"},
		Pos:      3636,
		RealPos:  3535,
		RowID:    2222,
	}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 0,
		Key:      ChunkCheckpointKey{Path: "/tmp/10.sql"},
		Pos:      4949,
		RealPos:  4848,
		RowID:    444,
	}).MergeInto(cpd)

	cp.Apply(cpd)

	require.Equal(t, TableCheckpoint{
		Status:        CheckpointStatusAllWritten,
		AutoRandBase:  1131,
		AutoIncrBase:  1132,
		AutoRowIDBase: 1133,
		Engines: map[int32]*EngineCheckpoint{
			-1: {
				Status: CheckpointStatusImported,
			},
			0: {
				Status: CheckpointStatusLoaded,
				Chunks: []*ChunkCheckpoint{
					{
						Key: ChunkCheckpointKey{Path: "/tmp/01.sql"},
						Chunk: mydump.Chunk{
							Offset:       6666,
							RealOffset:   6565,
							EndOffset:    20000,
							PrevRowIDMax: 777,
							RowIDMax:     1000,
						},
						Checksum: verification.MakeKVChecksum(3333, 4444, 5555),
					},
					{
						Key: ChunkCheckpointKey{Path: "/tmp/04.sql"},
						Chunk: mydump.Chunk{
							Offset:       0,
							RealOffset:   0,
							EndOffset:    15000,
							PrevRowIDMax: 1000,
							RowIDMax:     1300,
						},
					},
				},
			},
		},
	}, cp)
}

func TestCheckpointMarshallUnmarshall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "filecheckpoint")
	ctx := context.Background()
	fileChkp, err := NewFileCheckpointsDB(ctx, path)
	require.NoError(t, err)
	fileChkp.checkpoints.Checkpoints["a"] = &checkpointspb.TableCheckpointModel{
		Status:  uint32(CheckpointStatusLoaded),
		Engines: map[int32]*checkpointspb.EngineCheckpointModel{},
	}
	err = fileChkp.Close()
	require.NoError(t, err)

	fileChkp2, err := NewFileCheckpointsDB(ctx, path)
	require.NoError(t, err)
	// if not recover empty map explicitly, it will become nil
	require.NotNil(t, fileChkp2.checkpoints.Checkpoints["a"].Engines)
}

func TestSeparateCompletePath(t *testing.T) {
	testCases := []struct {
		complete       string
		expectFileName string
		expectPath     string
	}{
		{"", "", ""},
		{"/a/", "", "/a/"},
		{"test.log", "test.log", "."},
		{"./test.log", "test.log", "."},
		{"./tmp/test.log", "test.log", "tmp"},
		{"tmp/test.log", "test.log", "tmp"},
		{"/test.log", "test.log", "/"},
		{"/tmp/test.log", "test.log", "/tmp"},
		{"/a%3F%2Fbc/a%3F%2Fbc.log", "a%3F%2Fbc.log", "/a%3F%2Fbc"},
		{"/a??bc/a??bc.log", "a??bc.log", "/a??bc"},
		{"/t-%C3%8B%21s%60t/t-%C3%8B%21s%60t.log", "t-%C3%8B%21s%60t.log", "/t-%C3%8B%21s%60t"},
		{"/t-Ë!s`t/t-Ë!s`t.log", "t-Ë!s`t.log", "/t-Ë!s`t"},
		{"file:///a%3F%2Fbc/a%3F%2Fcd.log", "cd.log", "file:///a%3F/bc/a%3F"},
		{"file:///a?/bc/a?/cd.log", "a", "file:///?/bc/a?/cd.log"},
		{"file:///a/?/bc/a?/cd.log", "", "file:///a/?/bc/a?/cd.log"},
		{"file:///t-%C3%8B%21s%60t/t-%C3%8B%21s%60t.log", "t-Ë!s`t.log", "file:///t-%C3%8B%21s%60t"},
		{"file:///t-Ë!s`t/t-Ë!s`t.log", "t-Ë!s`t.log", "file:///t-%C3%8B%21s%60t"},
		{"s3://bucket2/test.log", "test.log", "s3://bucket2/"},
		{"s3://bucket2/test/test.log", "test.log", "s3://bucket2/test"},
		{"s3://bucket3/prefix/test.log?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw", "test.log",
			"s3://bucket3/prefix?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt%2BPaIbYKrKlEEMMF/ExCiJEX=XMLPUANw"},
	}

	for _, testCase := range testCases {
		fileName, newPath, err := separateCompletePath(testCase.complete)
		require.NoError(t, err)
		require.Equal(t, testCase.expectFileName, fileName)
		require.Equal(t, testCase.expectPath, newPath)
	}
}

func TestTableCheckpointApplyBases(t *testing.T) {
	tblCP := TableCheckpoint{
		AutoRowIDBase: 11,
		AutoIncrBase:  12,
		AutoRandBase:  13,
	}
	tblCP.Apply(&TableCheckpointDiff{
		hasRebase:     true,
		autoRowIDBase: 1,
		autoIncrBase:  2,
		autoRandBase:  3,
	})
	require.EqualValues(t, 11, tblCP.AutoRowIDBase)
	require.EqualValues(t, 12, tblCP.AutoIncrBase)
	require.EqualValues(t, 13, tblCP.AutoRandBase)
}
