package checkpoints

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints/checkpointspb"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
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
		RowID:    31,
	}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		engines: map[int32]engineCheckpointDiff{
			2: {
				chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
					key: {
						pos:      1055,
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
		RowID:    42,
	}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		engines: map[int32]engineCheckpointDiff{
			2: {
				chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
					key: {
						pos:      1080,
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

	m := RebaseCheckpointMerger{AllocBase: 10000}
	m.MergeInto(cpd)

	require.Equal(t, &TableCheckpointDiff{
		hasRebase: true,
		allocBase: 10000,
		engines:   make(map[int32]engineCheckpointDiff),
	}, cpd)
}

func TestApplyDiff(t *testing.T) {
	cp := TableCheckpoint{
		Status:    CheckpointStatusLoaded,
		AllocBase: 123,
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
							EndOffset:    20000,
							PrevRowIDMax: 0,
							RowIDMax:     1000,
						},
					},
					{
						Key: ChunkCheckpointKey{Path: "/tmp/04.sql"},
						Chunk: mydump.Chunk{
							Offset:       0,
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
	(&RebaseCheckpointMerger{AllocBase: 11111}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 0,
		Key:      ChunkCheckpointKey{Path: "/tmp/01.sql"},
		Checksum: verification.MakeKVChecksum(3333, 4444, 5555),
		Pos:      6666,
		RowID:    777,
	}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 5678,
		Key:      ChunkCheckpointKey{Path: "/tmp/04.sql"},
		Pos:      9999,
		RowID:    888,
	}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 0,
		Key:      ChunkCheckpointKey{Path: "/tmp/03.sql"},
		Pos:      3636,
		RowID:    2222,
	}).MergeInto(cpd)
	(&ChunkCheckpointMerger{
		EngineID: 0,
		Key:      ChunkCheckpointKey{Path: "/tmp/10.sql"},
		Pos:      4949,
		RowID:    444,
	}).MergeInto(cpd)

	cp.Apply(cpd)

	require.Equal(t, TableCheckpoint{
		Status:    CheckpointStatusAllWritten,
		AllocBase: 11111,
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
