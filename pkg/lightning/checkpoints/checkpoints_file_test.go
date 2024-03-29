package checkpoints_test

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pingcap/errors"
	checkpoints2 "github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/config"
	mydump2 "github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/parser/model"
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

func newFileCheckpointsDB(t *testing.T) *checkpoints2.FileCheckpointsDB {
	dir := t.TempDir()
	ctx := context.Background()
	cpdb, err := checkpoints2.NewFileCheckpointsDB(ctx, filepath.Join(dir, "cp.pb"))
	require.NoError(t, err)

	// 2. initialize with checkpoint data.
	cfg := newTestConfig()
	err = cpdb.Initialize(ctx, cfg, map[string]*checkpoints2.TidbDBInfo{
		"db1": {
			Name: "db1",
			Tables: map[string]*checkpoints2.TidbTableInfo{
				"t1": {Name: "t1"},
				"t2": {Name: "t2"},
			},
		},
		"db2": {
			Name: "db2",
			Tables: map[string]*checkpoints2.TidbTableInfo{
				"t3": {
					Name: "t3",
					Desired: &model.TableInfo{
						Name: model.NewCIStr("t3"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// 3. set some checkpoints

	err = cpdb.InsertEngineCheckpoints(ctx, "`db1`.`t2`", map[int32]*checkpoints2.EngineCheckpoint{
		0: {
			Status: checkpoints2.CheckpointStatusLoaded,
			Chunks: []*checkpoints2.ChunkCheckpoint{{
				Key: checkpoints2.ChunkCheckpointKey{
					Path:   "/tmp/path/1.sql",
					Offset: 0,
				},
				FileMeta: mydump2.SourceFileMeta{
					Path:     "/tmp/path/1.sql",
					Type:     mydump2.SourceTypeSQL,
					FileSize: 12345,
				},
				Chunk: mydump2.Chunk{
					Offset:       12,
					EndOffset:    102400,
					PrevRowIDMax: 1,
					RowIDMax:     5000,
				},
			}},
		},
		-1: {
			Status: checkpoints2.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	require.NoError(t, err)

	err = cpdb.InsertEngineCheckpoints(ctx, "`db2`.`t3`", map[int32]*checkpoints2.EngineCheckpoint{
		-1: {
			Status: checkpoints2.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	require.NoError(t, err)

	// 4. update some checkpoints

	cpd := checkpoints2.NewTableCheckpointDiff()
	scm := checkpoints2.StatusCheckpointMerger{
		EngineID: 0,
		Status:   checkpoints2.CheckpointStatusImported,
	}
	scm.MergeInto(cpd)
	scm = checkpoints2.StatusCheckpointMerger{
		EngineID: checkpoints2.WholeTableEngineID,
		Status:   checkpoints2.CheckpointStatusAllWritten,
	}
	scm.MergeInto(cpd)
	rcm := checkpoints2.RebaseCheckpointMerger{
		AllocBase: 132861,
	}
	rcm.MergeInto(cpd)
	cksum := checkpoints2.TableChecksumMerger{
		Checksum: verification.MakeKVChecksum(4492, 686, 486070148910),
	}
	cksum.MergeInto(cpd)
	ccm := checkpoints2.ChunkCheckpointMerger{
		EngineID: 0,
		Key:      checkpoints2.ChunkCheckpointKey{Path: "/tmp/path/1.sql", Offset: 0},
		Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
		Pos:      55904,
		RowID:    681,
	}
	ccm.MergeInto(cpd)

	cpdb.Update(ctx, map[string]*checkpoints2.TableCheckpointDiff{"`db1`.`t2`": cpd})
	t.Cleanup(func() {
		err := cpdb.Close()
		require.NoError(t, err)
	})
	return cpdb
}

func setInvalidStatus(cpdb *checkpoints2.FileCheckpointsDB) {
	cpd := checkpoints2.NewTableCheckpointDiff()
	scm := checkpoints2.StatusCheckpointMerger{
		EngineID: -1,
		Status:   checkpoints2.CheckpointStatusAllWritten,
	}
	scm.SetInvalid()
	scm.MergeInto(cpd)

	cpdb.Update(context.Background(), map[string]*checkpoints2.TableCheckpointDiff{
		"`db1`.`t2`": cpd,
		"`db2`.`t3`": cpd,
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	cpdb := newFileCheckpointsDB(t)

	// 5. get back the checkpoints

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	expect := &checkpoints2.TableCheckpoint{
		Status:    checkpoints2.CheckpointStatusAllWritten,
		AllocBase: 132861,
		Checksum:  verification.MakeKVChecksum(4492, 686, 486070148910),
		Engines: map[int32]*checkpoints2.EngineCheckpoint{
			-1: {
				Status: checkpoints2.CheckpointStatusLoaded,
				Chunks: []*checkpoints2.ChunkCheckpoint{},
			},
			0: {
				Status: checkpoints2.CheckpointStatusImported,
				Chunks: []*checkpoints2.ChunkCheckpoint{{
					Key: checkpoints2.ChunkCheckpointKey{
						Path:   "/tmp/path/1.sql",
						Offset: 0,
					},
					FileMeta: mydump2.SourceFileMeta{
						Path:     "/tmp/path/1.sql",
						Type:     mydump2.SourceTypeSQL,
						FileSize: 12345,
					},
					ColumnPermutation: []int{},
					Chunk: mydump2.Chunk{
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
	expect = &checkpoints2.TableCheckpoint{
		Status: checkpoints2.CheckpointStatusLoaded,
		Engines: map[int32]*checkpoints2.EngineCheckpoint{
			-1: {
				Status: checkpoints2.CheckpointStatusLoaded,
				Chunks: []*checkpoints2.ChunkCheckpoint{},
			},
		},
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("t3"),
		},
	}
	require.Equal(t, expect, cp)

	cp, err = cpdb.Get(ctx, "`db3`.`not-exists`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))
}

func TestRemoveAllCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb := newFileCheckpointsDB(t)

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
	cpdb := newFileCheckpointsDB(t)

	err := cpdb.RemoveCheckpoint(ctx, "`db1`.`t2`")
	require.NoError(t, err)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.Nil(t, cp)
	require.True(t, errors.IsNotFound(err))

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	require.Equal(t, checkpoints2.CheckpointStatusLoaded, cp.Status)
}

func TestIgnoreAllErrorCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb := newFileCheckpointsDB(t)

	setInvalidStatus(cpdb)

	err := cpdb.IgnoreErrorCheckpoint(ctx, "all")
	require.NoError(t, err)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	require.Equal(t, checkpoints2.CheckpointStatusLoaded, cp.Status)

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	require.Equal(t, checkpoints2.CheckpointStatusLoaded, cp.Status)
}

func TestIgnoreOneErrorCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb := newFileCheckpointsDB(t)

	setInvalidStatus(cpdb)

	err := cpdb.IgnoreErrorCheckpoint(ctx, "`db1`.`t2`")
	require.NoError(t, err)

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	require.Equal(t, checkpoints2.CheckpointStatusLoaded, cp.Status)

	cp, err = cpdb.Get(ctx, "`db2`.`t3`")
	require.NoError(t, err)
	require.Equal(t, checkpoints2.CheckpointStatusAllWritten/10, cp.Status)
}

func TestDestroyAllErrorCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb := newFileCheckpointsDB(t)

	setInvalidStatus(cpdb)

	dtc, err := cpdb.DestroyErrorCheckpoint(ctx, "all")
	require.NoError(t, err)
	sort.Slice(dtc, func(i, j int) bool { return dtc[i].TableName < dtc[j].TableName })
	expect := []checkpoints2.DestroyedTableCheckpoint{
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
	cpdb := newFileCheckpointsDB(t)

	setInvalidStatus(cpdb)

	dtc, err := cpdb.DestroyErrorCheckpoint(ctx, "`db1`.`t2`")
	require.NoError(t, err)
	expect := []checkpoints2.DestroyedTableCheckpoint{
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
	require.Equal(t, checkpoints2.CheckpointStatusAllWritten/10, cp.Status)
}
