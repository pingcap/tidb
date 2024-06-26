// Copyright 2024 PingCAP, Inc.
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

package importclient_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type storeClient struct {
	split.SplitClient
	addr string
}

func (sc *storeClient) GetStore(_ context.Context, _ uint64) (*metapb.Store, error) {
	return &metapb.Store{
		Address: sc.addr,
	}, nil
}

type mockImportServer struct {
	import_sstpb.ImportSSTServer

	ErrCount int
}

func (s *mockImportServer) ClearFiles(_ context.Context, req *import_sstpb.ClearRequest) (*import_sstpb.ClearResponse, error) {
	return &import_sstpb.ClearResponse{Error: &import_sstpb.Error{Message: req.Prefix}}, nil
}

func (s *mockImportServer) Apply(_ context.Context, req *import_sstpb.ApplyRequest) (*import_sstpb.ApplyResponse, error) {
	return &import_sstpb.ApplyResponse{Error: &import_sstpb.Error{Message: req.StorageCacheId}}, nil
}

func (s *mockImportServer) Download(_ context.Context, req *import_sstpb.DownloadRequest) (*import_sstpb.DownloadResponse, error) {
	return &import_sstpb.DownloadResponse{Error: &import_sstpb.Error{Message: req.Name}}, nil
}

func (s *mockImportServer) SetDownloadSpeedLimit(_ context.Context, req *import_sstpb.SetDownloadSpeedLimitRequest) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	return &import_sstpb.SetDownloadSpeedLimitResponse{}, nil
}

func (s *mockImportServer) MultiIngest(_ context.Context, req *import_sstpb.MultiIngestRequest) (*import_sstpb.IngestResponse, error) {
	if s.ErrCount <= 0 {
		return nil, errors.Errorf("test")
	}
	s.ErrCount -= 1
	if req.Context == nil {
		return &import_sstpb.IngestResponse{}, nil
	}
	return &import_sstpb.IngestResponse{Error: &errorpb.Error{Message: req.Context.RequestSource}}, nil
}

func TestImportClient(t *testing.T) {
	ctx := context.Background()
	var port int
	var lis net.Listener
	var err error
	for port = 0; port < 1000; port += 1 {
		addr := fmt.Sprintf(":%d", 51111+port)
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		t.Log(err)
	}

	s := grpc.NewServer()
	import_sstpb.RegisterImportSSTServer(s, &mockImportServer{ErrCount: 3})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.Serve(lis)
		require.NoError(t, err)
	}()

	client := importclient.NewImportClient(&storeClient{addr: fmt.Sprintf(":%d", 51111+port)}, nil, keepalive.ClientParameters{})

	{
		resp, err := client.ClearFiles(ctx, 1, &import_sstpb.ClearRequest{Prefix: "test"})
		require.NoError(t, err)
		require.Equal(t, "test", resp.Error.Message)
	}

	{
		resp, err := client.ApplyKVFile(ctx, 1, &import_sstpb.ApplyRequest{StorageCacheId: "test"})
		require.NoError(t, err)
		require.Equal(t, "test", resp.Error.Message)
	}

	{
		resp, err := client.DownloadSST(ctx, 1, &import_sstpb.DownloadRequest{Name: "test"})
		require.NoError(t, err)
		require.Equal(t, "test", resp.Error.Message)
	}

	{
		_, err := client.SetDownloadSpeedLimit(ctx, 1, &import_sstpb.SetDownloadSpeedLimitRequest{SpeedLimit: 123})
		require.NoError(t, err)
	}

	{
		resp, err := client.MultiIngest(ctx, 1, &import_sstpb.MultiIngestRequest{Context: &kvrpcpb.Context{RequestSource: "test"}})
		require.NoError(t, err)
		require.Equal(t, "test", resp.Error.Message)
	}

	{
		err := client.CheckMultiIngestSupport(ctx, []uint64{1})
		require.NoError(t, err)
		err = client.CheckMultiIngestSupport(ctx, []uint64{3, 4, 5})
		require.Error(t, err)
	}

	err = client.CloseGrpcClient()
	require.NoError(t, err)

	s.Stop()
	lis.Close()
}
