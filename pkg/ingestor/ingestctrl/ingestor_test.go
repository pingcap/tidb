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

package ingestctrl

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/servicediscovery"
)

type ingestorServiceDiscovery struct {
	servicediscovery.ServiceDiscovery
}

func (*ingestorServiceDiscovery) GetServiceURLs() []string {
	return []string{"http://127.0.0.1:2379"}
}

type ingestorPDClient struct {
	mockPdClient
}

func (c *ingestorPDClient) WithCallerComponent(caller.Component) pd.Client {
	return c
}

func (*ingestorPDClient) GetServiceDiscovery() servicediscovery.ServiceDiscovery {
	return &ingestorServiceDiscovery{}
}

func TestIngestorWithoutLocalStorage(t *testing.T) {
	ctx := context.Background()
	pdClient := &ingestorPDClient{}
	codecPDClient := tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdClient)
	blockedDir := filepath.Join(t.TempDir(), "blocked")
	require.NoError(t, os.WriteFile(blockedDir, nil, 0o600))
	cfg := getBackendConfig(t)
	cfg.LocalStoreDir = filepath.Join(blockedDir, "sorted-kv")
	cfg.StoreWriteBWLimit = 123

	ingestor, err := NewIngestor(ctx, &common.TLS{}, cfg, codecPDClient)
	require.NoError(t, err)
	require.Equal(t, 123, ingestor.GetWriteSpeedLimit())
	require.Same(t, codecPDClient.GetCodec(), ingestor.GetTiKVCodec())
	var jobWg sync.WaitGroup
	worker := ingestor.newRegionJobWorker(ctx, 1, nil, nil, &jobWg, nil)
	require.NoError(t, worker.Close())
	if worker, ok := worker.(*objStoreRegionJobWorker); ok {
		require.Same(t, ingestor.bufferPool, worker.bufPool)
	}
	buf := ingestor.bufferPool.NewBuffer()
	require.Len(t, buf.AllocBytes(1), 1)
	buf.Destroy()
	require.Positive(t, ingestor.TotalMemoryConsume())
	ingestor.Close()
	require.Zero(t, ingestor.TotalMemoryConsume())
	require.False(t, pdClient.closed)
	require.FileExists(t, blockedDir)

	// A local backend still needs local storage, and must retain caller-owned
	// PD even when engine-manager initialization fails after ingest setup.
	backend, err := NewBackend(ctx, &common.TLS{}, cfg, codecPDClient)
	require.Error(t, err)
	require.Nil(t, backend)
	require.False(t, pdClient.closed)
}

func TestBackendAndIngestorBufferLifetimes(t *testing.T) {
	backend, err := NewBackendForTest(context.Background(), getBackendConfig(t), mockStoreHelper{})
	require.NoError(t, err)
	require.Same(t, backend.bufferPool, backend.engineMgr.bufferPool)
	buf := backend.bufferPool.NewBuffer()
	require.Len(t, buf.AllocBytes(1), 1)
	buf.Destroy()
	require.Equal(t, backend.Ingestor.TotalMemoryConsume(), backend.TotalMemoryConsume())
	backend.CloseEngineMgr()
	buf = backend.bufferPool.NewBuffer()
	require.Len(t, buf.AllocBytes(1), 1)
	buf.Destroy()
	backend.Close()
}
