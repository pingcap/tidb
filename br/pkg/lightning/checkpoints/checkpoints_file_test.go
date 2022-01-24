package checkpoints_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/stretchr/testify/require"
)

func newTestConfig() *config.Config {
	cfg := config.NewConfig()
	cfg.Mydumper.SourceDir = "/data"
	cfg.TaskID = 123
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "127.0.0.1:2379"
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.Addr = "127.0.0.1:8287"
	cfg.TikvImporter.SortedKVDir = "/tmp/sorted-kv"
	return cfg
}

func newFileCheckpointsDB(t *testing.T) (*checkpoints.FileCheckpointsDB, func()) {
	dir, err := os.MkdirTemp("", "checkpointDB***")
	require.NoError(t, err)
	cpdb := checkpoints.NewFileCheckpointsDB(filepath.Join(dir, "cp.pb"))
	ctx := context.Background()

	// 2. initialize with checkpoint data.
	cfg := newTestConfig()
	err = cpdb.Initialize(ctx, cfg, map[string]*checkpoints.TidbDBInfo{
		"db1": {
			Name: "db1",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"t1": {Name: "t1"},
				"t2": {Name: "t2"},
			},
		},
		"db2": {
			Name: "db2",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"t3": {Name: "t3"},
			},
		},
	})
	require.NoError(t, err)

	// 3. set some checkpoints

	err = cpdb.InsertEngineCheckpoints(ctx, "`db1`.`t2`", map[int32]*checkpoints.EngineCheckpoint{
		0: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{{
				Key: checkpoints.ChunkCheckpointKey{
					Path:   "/tmp/path/1.sql",
					Offset: 0,
				},
				FileMeta: mydump.SourceFileMeta{
					Path:     "/tmp/path/1.sql",
					Type:     mydump.SourceTypeSQL,
					FileSize: 12345,
				},
				Chunk: mydump.Chunk{
					Offset:       12,
					EndOffset:    102400,
					PrevRowIDMax: 1,
					RowIDMax:     5000,
				},
			}},
		},
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	require.NoError(t, err)

	err = cpdb.InsertEngineCheckpoints(ctx, "`db2`.`t3`", map[int32]*checkpoints.EngineCheckpoint{
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	require.NoError(t, err)

	// 4. update some checkpoints

	cpd := checkpoints.NewTableCheckpointDiff()
	scm := checkpoints.StatusCheckpointMerger{
		EngineID: 0,
		Status:   checkpoints.CheckpointStatusImported,
	}
	scm.MergeInto(cpd)
	scm = checkpoints.StatusCheckpointMerger{
		EngineID: checkpoints.WholeTableEngineID,
		Status:   checkpoints.CheckpointStatusAllWritten,
	}
	scm.MergeInto(cpd)
	rcm := checkpoints.RebaseCheckpointMerger{
		AllocBase: 132861,
	}
	rcm.MergeInto(cpd)
	cksum := checkpoints.TableChecksumMerger{
		Checksum: verification.MakeKVChecksum(4492, 686, 486070148910),
	}
	cksum.MergeInto(cpd)
	ccm := checkpoints.ChunkCheckpointMerger{
		EngineID: 0,
		Key:      checkpoints.ChunkCheckpointKey{Path: "/tmp/path/1.sql", Offset: 0},
		Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
		Pos:      55904,
		RowID:    681,
	}
	ccm.MergeInto(cpd)

	cpdb.Update(map[string]*checkpoints.TableCheckpointDiff{"`db1`.`t2`": cpd})
	return cpdb, func() {
		err := cpdb.Close()
		require.NoError(t, err)
		os.RemoveAll(dir)
	}
}

func setInvalidStatus(cpdb *checkpoints.FileCheckpointsDB) {
	cpd := checkpoints.NewTableCheckpointDiff()
	scm := checkpoints.StatusCheckpointMerger{
		EngineID: -1,
		Status:   checkpoints.CheckpointStatusAllWritten,
	}
	scm.SetInvalid()
	scm.MergeInto(cpd)

	cpdb.Update(map[string]*checkpoints.TableCheckpointDiff{
		"`db1`.`t2`": cpd,
		"`db2`.`t3`": cpd,
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	cpdb, clean := newFileCheckpointsDB(t)
	defer clean()

	// 5. get back the checkpoints

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	expect := &checkpoints.TableCheckpoint{
		Status:    checkpoints.CheckpointStatusAllWritten,
		AllocBase: 132861,
		Checksum:  verification.MakeKVChecksum(4492, 686, 486070148910),
		Engines: map[int32]*checkpoints.EngineCheckpoint{
			-1: {
				Status: checkpoints.CheckpointStatusLoaded,
				Chunks: []*checkpoints.ChunkCheckpoint{},
			},
			0: {
				Status: checkpoints.CheckpointStatusImported,
				Chunks: []*checkpoints.ChunkCheckpoint{{
					Key: checkpoints.ChunkCheckpointKey{
						Path:   "/tmp/path/1.sql",
						Offset: 0,
					},
					FileMeta: mydump.SourceFileMeta{
						Path:     "/tmp/path/1.sql",
						Type:     mydump.SourceTypeSQL,
						FileSize: 12345,
					},
					ColumnPermutation: []int{},
					Chunk: mydump.Chunk{
						Offset:       55904,
						EndOffset:    102400,
						PrevRowIDMax: 681,
						RowIDMax:     5000,
					},
					Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
				}},
			},
		},
	}
	require.Equal(t, expect, cp)

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	expect = &checkpoints.TableCheckpoint{
		Status: checkpoints.CheckpointStatusLoaded,
		Engines: map[int32]*checkpoints.EngineCheckpoint{
			-1: {
				Status: checkpoints.CheckpointStatusLoaded,
				Chunks: []*checkpoints.ChunkCheckpoint{},
			},
		},
	}
	require.Equal(t, expect, cp)

	cp, err = cpdb.Get(ctx, "`db3`.`not-exists`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))
}

func TestRemoveAllCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb, clean := newFileCheckpointsDB(t)
	defer clean()

	err := cpdb.RemoveCheckpoint(ctx, "all")
	require.NoError(t, err)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))
}

func TestRemoveOneCheckpoint(t *testing.T) {
	ctx := context.Background()
	cpdb, clean := newFileCheckpointsDB(t)
	defer clean()

	err := cpdb.RemoveCheckpoint(ctx, "`db1`.`t2`")
	require.NoError(t, err)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	require.Equal(t, checkpoints.CheckpointStatusLoaded, cp.Status)
}

func TestIgnoreAllErrorCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb, clean := newFileCheckpointsDB(t)
	defer clean()

	setInvalidStatus(cpdb)

	err := cpdb.IgnoreErrorCheckpoint(ctx, "all")
	require.NoError(t, err)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	require.Equal(t, checkpoints.CheckpointStatusLoaded, cp.Status)

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	require.Equal(t, checkpoints.CheckpointStatusLoaded, cp.Status)
}

func TestIgnoreOneErrorCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb, clean := newFileCheckpointsDB(t)
	defer clean()

	setInvalidStatus(cpdb)

	err := cpdb.IgnoreErrorCheckpoint(ctx, "`db1`.`t2`")
	require.NoError(t, err)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	require.Equal(t, checkpoints.CheckpointStatusLoaded, cp.Status)

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	require.Equal(t, checkpoints.CheckpointStatusAllWritten/10, cp.Status)
}

func TestDestroyAllErrorCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb, clean := newFileCheckpointsDB(t)
	defer clean()

	setInvalidStatus(cpdb)

	dtc, err := cpdb.DestroyErrorCheckpoint(ctx, "all")
	require.NoError(t, err)
	sort.Slice(dtc, func(i, j int) bool { return dtc[i].TableName < dtc[j].TableName })
	expect := []checkpoints.DestroyedTableCheckpoint{
		{
			TableName:   "`db1`.`t2`",
			MinEngineID: -1,
			MaxEngineID: 0,
		},
		{
			TableName:   "`db2`.`t3`",
			MinEngineID: -1,
			MaxEngineID: -1,
		},
	}
	require.Equal(t, expect, dtc)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))
}

func TestDestroyOneErrorCheckpoint(t *testing.T) {
	ctx := context.Background()
	cpdb, clean := newFileCheckpointsDB(t)
	defer clean()

	setInvalidStatus(cpdb)

	dtc, err := cpdb.DestroyErrorCheckpoint(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	expect := []checkpoints.DestroyedTableCheckpoint{
		{
			TableName:   "`db1`.`t2`",
			MinEngineID: -1,
			MaxEngineID: 0,
		},
	}
	require.Equal(t, expect, dtc)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	require.Equal(t, checkpoints.CheckpointStatusAllWritten/10, cp.Status)
}
