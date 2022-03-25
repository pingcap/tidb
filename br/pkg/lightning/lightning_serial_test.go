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

package lightning

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
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
	invalidGlue := glue.NewExternalTiDBGlue(nil, 0)
	o := &options{glue: invalidGlue}
	err = lightning.run(ctx, &config.Config{
		Mydumper: config.MydumperRuntime{
			SourceDir:        "file://" + filepath.ToSlash(path),
			Filter:           []string{"*.*"},
			DefaultFileRules: true,
		},
		Checkpoint: config.Checkpoint{
			Enable: true,
			Driver: "invalid",
		},
	}, o)
	require.EqualError(t, err, "[Lightning:Checkpoint:ErrUnknownCheckpointDriver]unknown checkpoint driver 'invalid'")

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

	err := failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue", "return(139415)")
	require.NoError(t, err)
	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/SetRlimitError", "return(true)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/SetRlimitError")
	}()
	// with this dbMetas, the estimated fds will be 139416, so should return error
	err = checkSystemRequirement(cfg, dbMetas)
	require.Error(t, err)

	err = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue")
	require.NoError(t, err)

	// the min rlimit should be not smaller than the default min value (139416)
	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue", "return(139416)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue")
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
