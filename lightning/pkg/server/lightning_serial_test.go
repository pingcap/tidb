// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/lightning/pkg/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestInitEnv(t *testing.T) {
	cfg := &config.GlobalConfig{
		App: config.GlobalLightning{StatusAddr: ":45678"},
	}
	err := initEnv(cfg)
	require.NoError(t, err)

	cfg.App.StatusAddr = ""
	cfg.App.Config.File = "."
	err = initEnv(cfg)
	require.EqualError(t, err, "can't use directory as log file name")
}

func TestRun(t *testing.T) {
	var sources []*closeTrackingStorage
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/lightning/pkg/server/afterCreateSourceStorage", func(st *storeapi.Storage) {
		tracked := &closeTrackingStorage{Storage: *st}
		sources = append(sources, tracked)
		*st = tracked
	})
	t.Cleanup(func() {
		require.NotEmpty(t, sources)
		for i, source := range sources {
			require.Equal(t, 1, source.closeCount, "source %d", i)
		}
	})
	globalConfig := config.NewGlobalConfig()
	globalConfig.TiDB.Host = "test.invalid"
	globalConfig.TiDB.Port = 4000
	globalConfig.TiDB.PdAddr = "test.invalid:2379"
	globalConfig.Mydumper.SourceDir = "not-exists"
	globalConfig.TikvImporter.Backend = config.BackendLocal
	globalConfig.TikvImporter.SortedKVDir = t.TempDir()
	lightning := New(globalConfig)
	cfg := config.NewConfig()
	err := cfg.LoadFromGlobal(globalConfig)
	require.NoError(t, err)
	err = lightning.RunOnceWithOptions(context.Background(), cfg)
	require.Error(t, err)
	require.Regexp(t, "`mydumper.data-source-dir` does not exist$", err.Error())

	path, _ := filepath.Abs(".")
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	logger, buffer := log.MakeTestLogger()
	require.NoError(t, err)
	o := &options{
		promRegistry: lightning.promRegistry,
		promFactory:  lightning.promFactory,
		logger:       logger,
		db:           db,
	}
	cfgCheckpoint := config.Config{
		Mydumper: config.MydumperRuntime{
			SourceDir:        "file://" + filepath.ToSlash(path),
			Filter:           []string{"*.*"},
			DefaultFileRules: true,
		},
		Checkpoint: config.Checkpoint{
			Enable: true,
			Driver: "invalid",
		},
	}
	err = lightning.run(ctx, &cfgCheckpoint, o)
	require.EqualError(t, err, "unknown backend ")
	mock.ExpectQuery("show config").WillReturnError(errors.New("lack privilege"))
	cfgKeyspaceName := config.NewConfig()
	cfgKeyspaceName.TikvImporter.Backend = config.BackendLocal
	cfgKeyspaceName.TikvImporter.LocalWriterMemCacheSize = config.SplitRegionSize
	cfgKeyspaceName.Mydumper = cfgCheckpoint.Mydumper
	cfgKeyspaceName.Checkpoint = cfgCheckpoint.Checkpoint
	err = lightning.run(ctx, cfgKeyspaceName, o)
	require.EqualError(t, err, "[Lightning:Checkpoint:ErrUnknownCheckpointDriver]unknown checkpoint driver 'invalid'")
	require.Contains(t, buffer.String(), `"acquired keyspace name","keyspaceName":""`)

	cfgKeyspaceName.TikvImporter.KeyspaceName = "test"
	err = lightning.run(ctx, cfgKeyspaceName, o)
	require.EqualError(t, err, "[Lightning:Checkpoint:ErrUnknownCheckpointDriver]unknown checkpoint driver 'invalid'")
	require.Contains(t, buffer.String(), `"acquired keyspace name","keyspaceName":"test"`)
	require.NoError(t, mock.ExpectationsWereMet())

	err = lightning.run(ctx, &config.Config{
		Mydumper: config.MydumperRuntime{
			SourceDir: ".",
			Filter:    []string{"*.*"},
		},
		Checkpoint: config.Checkpoint{
			Enable: true,
			Driver: "file",
			DSN:    "any-file",
		},
	}, o)
	require.Error(t, err)

	store, err := objstore.NewLocalStorage(path)
	require.NoError(t, err)
	borrowed := &closeTrackingStorage{Storage: store}
	t.Cleanup(borrowed.Close)
	o.dumpFileStorage = borrowed
	o.checkpointStorage = borrowed
	err = lightning.run(ctx, &cfgCheckpoint, o)
	require.EqualError(t, err, "unknown backend ")
	require.Zero(t, borrowed.closeCount)

	for _, borrowed := range []bool{false, true} {
		t.Run(fmt.Sprintf("controller-teardown/borrowed=%t", borrowed), func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("data"), 0600))
			cfg := config.NewConfig()
			cfg.TikvImporter.Backend = config.BackendTiDB
			cfg.Mydumper.SourceDir = "file://" + filepath.ToSlash(dir)
			cfg.Checkpoint.Enable = false
			cfg.App.TaskInfoSchemaName = ""
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			mock.ExpectQuery("SHOW VARIABLES").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}))
			mock.ExpectClose()
			o := &options{
				promRegistry: lightning.promRegistry,
				promFactory:  lightning.promFactory,
				logger:       logger,
				db:           db,
			}
			var tracked *closeTrackingStorage
			if borrowed {
				store, err := objstore.NewLocalStorage(dir)
				require.NoError(t, err)
				tracked = &closeTrackingStorage{Storage: store}
				t.Cleanup(tracked.Close)
				o.dumpFileStorage = tracked
				o.checkpointStorage = tracked
				o.checkpointName = "checkpoint.pb"
			}
			err = lightning.run(ctx, cfg, o)
			require.NoError(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
			if borrowed {
				require.Zero(t, tracked.closeCount)
			}
		})
	}
}

type closeTrackingStorage struct {
	storeapi.Storage
	closeCount int
}

func (s *closeTrackingStorage) Close() {
	s.closeCount++
	s.Storage.Close()
}

func TestInitDataSourceStorageOwnership(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		for _, stage := range []string{"success", "empty", "loader-error"} {
			t.Run(fmt.Sprintf("borrowed=%t/%s", borrowed, stage), func(t *testing.T) {
				dir := t.TempDir()
				if stage != "empty" {
					require.NoError(t, os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("data"), 0600))
				}
				cfg := config.NewConfig()
				cfg.TikvImporter.Backend = config.BackendTiDB
				cfg.Mydumper.SourceDir = "file://" + filepath.ToSlash(dir)
				if stage == "loader-error" {
					cfg.Mydumper.Filter = []string{"["}
				}
				l := &Lightning{curTask: cfg}
				o := &options{logger: log.L()}
				var tracked *closeTrackingStorage
				if borrowed {
					store, err := objstore.NewLocalStorage(dir)
					require.NoError(t, err)
					tracked = &closeTrackingStorage{Storage: store}
					o.dumpFileStorage = tracked
				} else {
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/lightning/pkg/server/afterCreateSourceStorage", func(st *storeapi.Storage) {
						tracked = &closeTrackingStorage{Storage: *st}
						*st = tracked
					})
				}
				mdl, store, err := l.initDataSource(context.Background(), cfg, o)
				require.NotNil(t, tracked)
				if stage == "success" {
					require.NoError(t, err)
					require.NotNil(t, mdl)
					require.Same(t, tracked, store)
				} else {
					require.Error(t, err)
					require.Nil(t, mdl)
					require.Nil(t, store)
				}
				if borrowed || stage == "success" {
					require.Zero(t, tracked.closeCount)
					tracked.Close()
				} else {
					require.Equal(t, 1, tracked.closeCount)
				}
			})
		}
	}
}

func TestCheckSystemRequirement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Local-backend is not supported on Windows")
		return
	}

	cfg := config.NewConfig()
	cfg.App.RegionConcurrency = 16
	cfg.App.CheckRequirements = true
	cfg.App.TableConcurrency = 4
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.LocalWriterMemCacheSize = 128 * units.MiB
	cfg.TikvImporter.RangeConcurrency = 16

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Tables: []*mydump.MDTableMeta{
				{
					TotalSize: 500 << 20,
				},
				{
					TotalSize: 150_000 << 20,
				},
			},
		},
		{
			Tables: []*mydump.MDTableMeta{
				{
					TotalSize: 150_800 << 20,
				},
				{
					TotalSize: 35 << 20,
				},
				{
					TotalSize: 100_000 << 20,
				},
			},
		},
		{
			Tables: []*mydump.MDTableMeta{
				{
					TotalSize: 240 << 20,
				},
				{
					TotalSize: 124_000 << 20,
				},
			},
		},
	}

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/GetRlimitValue", "return(139415)")
	require.NoError(t, err)
	err = failpoint.Enable("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/SetRlimitError", "return(true)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/SetRlimitError")
	}()
	// with this dbMetas, the estimated fds will be 139416, so should return error
	err = checkSystemRequirement(cfg, dbMetas)
	require.Error(t, err)

	err = failpoint.Disable("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/GetRlimitValue")
	require.NoError(t, err)

	// the min rlimit should be not smaller than the default min value (139416)
	err = failpoint.Enable("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/GetRlimitValue", "return(139416)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/GetRlimitValue")
	}()
	require.NoError(t, err)
	err = checkSystemRequirement(cfg, dbMetas)
	require.NoError(t, err)
}

func TestCheckSchemaConflict(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Checkpoint.Schema = "cp"
	cfg.Checkpoint.Driver = config.CheckpointDriverMySQL

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "test",
			Tables: []*mydump.MDTableMeta{
				{
					Name: checkpoints.CheckpointTableNameTable,
				},
				{
					Name: checkpoints.CheckpointTableNameEngine,
				},
			},
		},
		{
			Name: "cp",
			Tables: []*mydump.MDTableMeta{
				{
					Name: "test",
				},
			},
		},
	}
	err := checkSchemaConflict(cfg, dbMetas)
	require.NoError(t, err)

	dbMetas = append(dbMetas, &mydump.MDDatabaseMeta{
		Name: "cp",
		Tables: []*mydump.MDTableMeta{
			{
				Name: checkpoints.CheckpointTableNameChunk,
			},
			{
				Name: "test123",
			},
		},
	})
	err = checkSchemaConflict(cfg, dbMetas)
	require.Error(t, err)

	cfg.Checkpoint.Enable = false
	err = checkSchemaConflict(cfg, dbMetas)
	require.NoError(t, err)

	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.Driver = config.CheckpointDriverFile
	err = checkSchemaConflict(cfg, dbMetas)
	require.NoError(t, err)
}
