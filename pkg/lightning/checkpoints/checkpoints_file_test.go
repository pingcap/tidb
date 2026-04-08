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

package checkpoints_test

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

func newFileCheckpointsDB(t *testing.T, addIndexBySQL bool) *checkpoints.FileCheckpointsDB {
	dir := t.TempDir()
	ctx := context.Background()
	cpdb, err := checkpoints.NewFileCheckpointsDB(ctx, filepath.Join(dir, "cp.pb"))
	require.NoError(t, err)

	// 2. initialize with checkpoint data.
	cfg := newTestConfig()
	cfg.TikvImporter.AddIndexBySQL = addIndexBySQL
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
				"t3": {
					Name: "t3",
					Desired: &model.TableInfo{
						Name: ast.NewCIStr("t3"),
					},
				},
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
					RealOffset:   10,
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
		AutoRandBase:  132861,
		AutoIncrBase:  132862,
		AutoRowIDBase: 132863,
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
		RealPos:  55902,
		RowID:    681,
	}
	ccm.MergeInto(cpd)

	cpdb.Update(ctx, map[string]*checkpoints.TableCheckpointDiff{"`db1`.`t2`": cpd})
	t.Cleanup(func() {
		err := cpdb.Close()
		require.NoError(t, err)
	})
	return cpdb
}

func setInvalidStatus(cpdb *checkpoints.FileCheckpointsDB) {
	cpd := checkpoints.NewTableCheckpointDiff()
	scm := checkpoints.StatusCheckpointMerger{
		EngineID: -1,
		Status:   checkpoints.CheckpointStatusAllWritten,
	}
	scm.SetInvalid()
	scm.MergeInto(cpd)

	cpdb.Update(context.Background(), map[string]*checkpoints.TableCheckpointDiff{
		"`db1`.`t2`": cpd,
		"`db2`.`t3`": cpd,
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	expectT2 := &checkpoints.TableCheckpoint{
		Status:        checkpoints.CheckpointStatusAllWritten,
		AutoRandBase:  132861,
		AutoIncrBase:  132862,
		AutoRowIDBase: 132863,
		Checksum:      verification.MakeKVChecksum(4492, 686, 486070148910),
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
						RealOffset:   55902,
						EndOffset:    102400,
						PrevRowIDMax: 681,
						RowIDMax:     5000,
					},
					Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
				}},
			},
		},
	}

	expectT3 := &checkpoints.TableCheckpoint{
		Status: checkpoints.CheckpointStatusLoaded,
		Engines: map[int32]*checkpoints.EngineCheckpoint{
			-1: {
				Status: checkpoints.CheckpointStatusLoaded,
				Chunks: []*checkpoints.ChunkCheckpoint{},
			},
		},
	}

	expectT3AddIndexBySQL := &checkpoints.TableCheckpoint{
		Status: checkpoints.CheckpointStatusLoaded,
		Engines: map[int32]*checkpoints.EngineCheckpoint{
			-1: {
				Status: checkpoints.CheckpointStatusLoaded,
				Chunks: []*checkpoints.ChunkCheckpoint{},
			},
		},
		TableInfo: &model.TableInfo{
			Name: ast.NewCIStr("t3"),
		},
	}

	t.Run("normal", func(t *testing.T) {
		cpdb := newFileCheckpointsDB(t, false)
		cp, err := cpdb.Get(ctx, "`db1`.`t2`")
		require.NoError(t, err)
		require.Equal(t, expectT2, cp)

		cp, err = cpdb.Get(ctx, "`db2`.`t3`")
		require.NoError(t, err)
		require.Equal(t, expectT3, cp)

		cp, err = cpdb.Get(ctx, "`db3`.`not-exists`")
		require.Nil(t, cp)
		require.True(t, errors.IsNotFound(err))
	})

	t.Run("add-index-by-sql", func(t *testing.T) {
		cpdb := newFileCheckpointsDB(t, true)
		cp, err := cpdb.Get(ctx, "`db1`.`t2`")
		require.NoError(t, err)
		require.Equal(t, expectT2, cp)

		cp, err = cpdb.Get(ctx, "`db2`.`t3`")
		require.NoError(t, err)
		require.Equal(t, expectT3AddIndexBySQL, cp)

		cp, err = cpdb.Get(ctx, "`db3`.`not-exists`")
		require.Nil(t, cp)
		require.True(t, errors.IsNotFound(err))
	})
}

func TestRemoveAllCheckpoints(t *testing.T) {
	ctx := context.Background()
	cpdb := newFileCheckpointsDB(t, false)

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
	cpdb := newFileCheckpointsDB(t, false)

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
	cpdb := newFileCheckpointsDB(t, false)

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
	cpdb := newFileCheckpointsDB(t, false)

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
	cpdb := newFileCheckpointsDB(t, false)

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
	cpdb := newFileCheckpointsDB(t, false)

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
