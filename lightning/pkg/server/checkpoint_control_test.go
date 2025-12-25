// Copyright 2025 PingCAP, Inc.
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

package server

import (
	"context"
	"path/filepath"
	"testing"

	mockimport "github.com/pingcap/tidb/lightning/pkg/importinto/mock"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestImportIntoCheckpointControl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := mockimport.NewMockCheckpointManager(ctrl)
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendImportInto

	control := &ImportIntoCheckpointControl{
		cfg: cfg,
		mgr: mockMgr,
	}

	ctx := context.Background()

	tests := []struct {
		name      string
		operation func() error
		setup     func()
		wantErr   bool
	}{
		{
			name: "Remove",
			operation: func() error {
				return control.Remove(ctx, "db.t1")
			},
			setup: func() {
				mockMgr.EXPECT().Remove(ctx, "db.t1").Return(nil)
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: false,
		},
		{
			name: "IgnoreError",
			operation: func() error {
				return control.IgnoreError(ctx, "db.t1")
			},
			setup: func() {
				mockMgr.EXPECT().IgnoreError(ctx, "db.t1").Return(nil)
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: false,
		},
		{
			name: "Dump",
			operation: func() error {
				tempDir := t.TempDir()
				err := control.Dump(ctx, tempDir)
				if err == nil {
					require.FileExists(t, filepath.Join(tempDir, "tables.csv"))
					require.FileExists(t, filepath.Join(tempDir, "engines.csv"))
					require.FileExists(t, filepath.Join(tempDir, "chunks.csv"))
				}
				return err
			},
			setup: func() {
				mockMgr.EXPECT().DumpTables(ctx, gomock.Any()).Return(nil)
				mockMgr.EXPECT().DumpEngines(ctx, gomock.Any()).Return(nil)
				mockMgr.EXPECT().DumpChunks(ctx, gomock.Any()).Return(nil)
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: false,
		},
		{
			name: "GetLocalStoringTables",
			operation: func() error {
				res, err := control.GetLocalStoringTables(ctx)
				require.Nil(t, res)
				return err
			},
			setup:   func() {},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			err := tt.operation()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// newTestConfig creates a config suitable for file checkpoint testing
func newTestConfig(t *testing.T) *config.Config {
	dir := t.TempDir()
	cfg := config.NewConfig()
	cfg.Mydumper.SourceDir = "/data"
	cfg.TaskID = 0
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "127.0.0.1:2379"
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = filepath.Join(dir, "sorted-kv")
	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.Driver = config.CheckpointDriverFile
	cfg.Checkpoint.DSN = filepath.Join(dir, "cp.pb")
	return cfg
}

// setupFileCheckpointsDB creates a file-based checkpoint DB with test data
func setupFileCheckpointsDB(t *testing.T, cfg *config.Config) *checkpoints.FileCheckpointsDB {
	ctx := context.Background()
	cpdb, err := checkpoints.NewFileCheckpointsDB(ctx, cfg.Checkpoint.DSN)
	require.NoError(t, err)

	// Initialize with checkpoint data
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

	// Insert engine checkpoints
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

	t.Cleanup(func() {
		_ = cpdb.Close()
	})
	return cpdb
}

// setErrorStatus marks checkpoints as having errors for testing
func setErrorStatus(cpdb *checkpoints.FileCheckpointsDB) {
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

func TestLegacyCheckpointControl(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB)
		operation func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error
		verify    func(t *testing.T, ctx context.Context, cfg *config.Config)
		wantErr   bool
		errMsg    string
	}{
		{
			name:  "Remove single checkpoint",
			setup: func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB) {},
			operation: func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error {
				return ctl.Remove(ctx, "`db1`.`t2`")
			},
			verify: func(t *testing.T, ctx context.Context, cfg *config.Config) {
				cpdb, err := checkpoints.NewFileCheckpointsDB(ctx, cfg.Checkpoint.DSN)
				require.NoError(t, err)
				defer cpdb.Close()

				cp, err := cpdb.Get(ctx, "`db1`.`t2`")
				require.Nil(t, cp)
				require.Error(t, err)

				cp, err = cpdb.Get(ctx, "`db2`.`t3`")
				require.NoError(t, err)
				require.NotNil(t, cp)
			},
		},
		{
			name:  "Remove all checkpoints",
			setup: func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB) {},
			operation: func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error {
				return ctl.Remove(ctx, "all")
			},
			verify: func(t *testing.T, ctx context.Context, cfg *config.Config) {
				cpdb, err := checkpoints.NewFileCheckpointsDB(ctx, cfg.Checkpoint.DSN)
				require.NoError(t, err)
				defer cpdb.Close()

				cp, err := cpdb.Get(ctx, "`db1`.`t2`")
				require.Nil(t, cp)
				require.Error(t, err)

				cp, err = cpdb.Get(ctx, "`db2`.`t3`")
				require.Nil(t, cp)
				require.Error(t, err)
			},
		},
		{
			name: "IgnoreError single checkpoint",
			setup: func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB) {
				setErrorStatus(cpdb)
			},
			operation: func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error {
				return ctl.IgnoreError(ctx, "`db1`.`t2`")
			},
			verify: func(t *testing.T, ctx context.Context, cfg *config.Config) {
				cpdb, err := checkpoints.NewFileCheckpointsDB(ctx, cfg.Checkpoint.DSN)
				require.NoError(t, err)
				defer cpdb.Close()

				cp, err := cpdb.Get(ctx, "`db1`.`t2`")
				require.NoError(t, err)
				require.Equal(t, checkpoints.CheckpointStatusLoaded, cp.Status)

				cp, err = cpdb.Get(ctx, "`db2`.`t3`")
				require.NoError(t, err)
				require.Equal(t, checkpoints.CheckpointStatusAllWritten/10, cp.Status)
			},
		},
		{
			name: "IgnoreError all checkpoints",
			setup: func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB) {
				setErrorStatus(cpdb)
			},
			operation: func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error {
				return ctl.IgnoreError(ctx, "all")
			},
			verify: func(t *testing.T, ctx context.Context, cfg *config.Config) {
				cpdb, err := checkpoints.NewFileCheckpointsDB(ctx, cfg.Checkpoint.DSN)
				require.NoError(t, err)
				defer cpdb.Close()

				cp, err := cpdb.Get(ctx, "`db1`.`t2`")
				require.NoError(t, err)
				require.Equal(t, checkpoints.CheckpointStatusLoaded, cp.Status)

				cp, err = cpdb.Get(ctx, "`db2`.`t3`")
				require.NoError(t, err)
				require.Equal(t, checkpoints.CheckpointStatusLoaded, cp.Status)
			},
		},
		{
			name:  "Dump not supported for file checkpoint",
			setup: func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB) {},
			operation: func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error {
				dumpDir := filepath.Join(t.TempDir(), "dump")
				return ctl.Dump(ctx, dumpDir)
			},
			verify:  func(t *testing.T, ctx context.Context, cfg *config.Config) {},
			wantErr: true,
			errMsg:  "not unsupported",
		},
		{
			name: "GetLocalStoringTables with partial progress",
			setup: func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB) {
				cpd := checkpoints.NewTableCheckpointDiff()
				ccm := checkpoints.ChunkCheckpointMerger{
					EngineID: 0,
					Key:      checkpoints.ChunkCheckpointKey{Path: "/tmp/path/1.sql", Offset: 0},
					Pos:      100,
					RealPos:  100,
					RowID:    50,
				}
				ccm.MergeInto(cpd)
				cpdb.Update(context.Background(), map[string]*checkpoints.TableCheckpointDiff{"`db1`.`t2`": cpd})
			},
			operation: func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error {
				tables, err := ctl.GetLocalStoringTables(ctx)
				if err != nil {
					return err
				}
				require.Contains(t, tables, "`db1`.`t2`")
				require.Contains(t, tables["`db1`.`t2`"], int32(0))
				return nil
			},
			verify: func(t *testing.T, ctx context.Context, cfg *config.Config) {},
		},
		{
			name: "GetLocalStoringTables empty when imported",
			setup: func(t *testing.T, cpdb *checkpoints.FileCheckpointsDB) {
				cpd := checkpoints.NewTableCheckpointDiff()
				scm := checkpoints.StatusCheckpointMerger{
					EngineID: 0,
					Status:   checkpoints.CheckpointStatusImported,
				}
				scm.MergeInto(cpd)
				cpdb.Update(context.Background(), map[string]*checkpoints.TableCheckpointDiff{"`db1`.`t2`": cpd})
			},
			operation: func(ctx context.Context, ctl *LegacyCheckpointControl, cfg *config.Config) error {
				tables, err := ctl.GetLocalStoringTables(ctx)
				if err != nil {
					return err
				}
				require.Empty(t, tables)
				return nil
			},
			verify: func(t *testing.T, ctx context.Context, cfg *config.Config) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := newTestConfig(t)
			cpdb := setupFileCheckpointsDB(t, cfg)

			tt.setup(t, cpdb)

			ctl, err := NewLegacyCheckpointControl(cfg, nil)
			require.NoError(t, err)

			err = tt.operation(ctx, ctl, cfg)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					require.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				tt.verify(t, ctx, cfg)
			}
		})
	}
}

func TestNewCheckpointControl_LegacyBackend(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.TikvImporter.Backend = config.BackendLocal

	ctl, err := NewCheckpointControl(cfg, nil)
	require.NoError(t, err)
	require.IsType(t, &LegacyCheckpointControl{}, ctl)
}
