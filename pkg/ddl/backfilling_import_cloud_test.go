// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/ingestor/ingestctrl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestGlobalSortReadIndexInitWithoutLocalBackend(t *testing.T) {
	t.Cleanup(config.RestoreFunc())
	blockedTempDir := filepath.Join(t.TempDir(), "blocked")
	require.NoError(t, os.WriteFile(blockedTempDir, nil, 0o600))
	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.Store = config.StoreTypeTiKV
		cfg.TempDir = blockedTempDir
	})

	executor := &readIndexStepExecutor{
		job:             &model.Job{ReorgMeta: &model.DDLReorgMeta{}},
		cloudStorageURI: "s3://bucket",
	}
	require.NoError(t, executor.Init(context.Background()))
	require.True(t, executor.job.ReorgMeta.UseCloudStorage)
	require.Nil(t, executor.backend)
	require.Nil(t, executor.backendCfg)
	require.Nil(t, executor.metric)
	require.NoError(t, executor.Cleanup(context.Background()))
}

func TestCloudImportResourceModifiedUsesIngestor(t *testing.T) {
	ingestor := &ingestctrl.Ingestor{}
	ingestor.SetWorkerConcurrency(4)
	executor := &cloudImportExecutor{ingestor: ingestor}
	resource := &proto.StepResource{
		CPU: proto.NewAllocatable(4),
		Mem: proto.NewAllocatable(1 << 30),
	}
	require.NoError(t, executor.ResourceModified(context.Background(), resource))
	resource.CPU = proto.NewAllocatable(8)
	require.EqualError(t, executor.ResourceModified(context.Background(), resource), "engine not started")
	require.Equal(t, 4, ingestor.GetWorkerConcurrency())
}
