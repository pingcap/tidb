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

package tici

import (
	"context"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type mockMetaServiceClient struct{}

func (mockMetaServiceClient) WorkerNodeHeartbeat(context.Context, *WorkerNodeHeartbeatRequest, ...grpc.CallOption) (*WorkerNodeHeartbeatResponse, error) {
	return &WorkerNodeHeartbeatResponse{}, nil
}

func (mockMetaServiceClient) ReaderNodeHeartbeat(context.Context, *ReaderNodeHeartbeatRequest, ...grpc.CallOption) (*ReaderNodeHeartbeatResponse, error) {
	return &ReaderNodeHeartbeatResponse{}, nil
}

func (mockMetaServiceClient) CreateIndex(_ context.Context, req *CreateIndexRequest, _ ...grpc.CallOption) (*CreateIndexResponse, error) {
	if req != nil {
		if data, err := req.Marshal(); err == nil {
			mockTiCICreateIndexRequest.Store(data)
		}
	}
	return &CreateIndexResponse{Status: ErrorCode_SUCCESS}, nil
}

func (mockMetaServiceClient) DropIndex(context.Context, *DropIndexRequest, ...grpc.CallOption) (*DropIndexResponse, error) {
	return &DropIndexResponse{Status: ErrorCode_SUCCESS}, nil
}

func (mockMetaServiceClient) AddPartition(_ context.Context, req *AddPartitionRequest, _ ...grpc.CallOption) (*AddPartitionResponse, error) {
	if req != nil {
		if data, err := req.Marshal(); err == nil {
			mockTiCIAddPartitionRequest.Store(data)
		}
	}
	indexIDs := []int64(nil)
	if req != nil {
		indexIDs = append(indexIDs, req.IndexIds...)
	}
	return &AddPartitionResponse{Status: ErrorCode_SUCCESS, IndexIds: indexIDs}, nil
}

func (mockMetaServiceClient) DropPartition(_ context.Context, req *DropPartitionRequest, _ ...grpc.CallOption) (*DropPartitionResponse, error) {
	if req != nil {
		if data, err := req.Marshal(); err == nil {
			mockTiCIDropPartitionRequest.Store(data)
		}
	}
	return &DropPartitionResponse{Status: ErrorCode_SUCCESS}, nil
}

func (mockMetaServiceClient) GetIndexProgress(context.Context, *GetIndexProgressRequest, ...grpc.CallOption) (*GetIndexProgressResponse, error) {
	return &GetIndexProgressResponse{
		Status: ErrorCode_SUCCESS,
		State:  GetIndexProgressResponse_COMPLETED,
	}, nil
}

func (mockMetaServiceClient) AppendDeltaFragMeta(context.Context, *AppendDeltaFragMetaRequest, ...grpc.CallOption) (*AppendFragMetaResponse, error) {
	return &AppendFragMetaResponse{}, nil
}

func (mockMetaServiceClient) AppendBaseFragMeta(context.Context, *AppendBaseFragMetaRequest, ...grpc.CallOption) (*AppendFragMetaResponse, error) {
	return &AppendFragMetaResponse{}, nil
}

func (mockMetaServiceClient) GetShardLocalCacheInfo(context.Context, *GetShardLocalCacheRequest, ...grpc.CallOption) (*GetShardLocalCacheResponse, error) {
	return &GetShardLocalCacheResponse{}, nil
}

func (mockMetaServiceClient) DebugGetShardManifest(context.Context, *DebugGetShardManifestRequest, ...grpc.CallOption) (*DebugGetShardManifestResponse, error) {
	return &DebugGetShardManifestResponse{}, nil
}

func (mockMetaServiceClient) FinishCompactFragments(context.Context, *FinishCompactFragRequest, ...grpc.CallOption) (*FinishCompactFragResponse, error) {
	return &FinishCompactFragResponse{}, nil
}

func (mockMetaServiceClient) GetImportStoragePrefix(context.Context, *GetImportStoragePrefixRequest, ...grpc.CallOption) (*GetImportStoragePrefixResponse, error) {
	return &GetImportStoragePrefixResponse{
		Status:     ErrorCode_SUCCESS,
		JobId:      1,
		StorageUri: "noop:///mock-tici/import/mock_job",
	}, nil
}

func (mockMetaServiceClient) PreSplitImportShards(_ context.Context, req *PreSplitImportShardsRequest, _ ...grpc.CallOption) (*PreSplitImportShardsResponse, error) {
	if req != nil {
		if data, err := req.Marshal(); err == nil {
			mockTiCIPreSplitImportShardsRequest.Store(data)
		}
	}
	return &PreSplitImportShardsResponse{Status: ErrorCode_SUCCESS}, nil
}

func (mockMetaServiceClient) FinishImportPartitionUpload(context.Context, *FinishImportPartitionUploadRequest, ...grpc.CallOption) (*FinishImportResponse, error) {
	return &FinishImportResponse{Status: ErrorCode_SUCCESS}, nil
}

func (mockMetaServiceClient) FinishImportPartitionBuild(context.Context, *FinishImportPartitionBuildRequest, ...grpc.CallOption) (*FinishImportResponse, error) {
	return &FinishImportResponse{Status: ErrorCode_SUCCESS}, nil
}

func (mockMetaServiceClient) FinishImportIndexUpload(context.Context, *FinishImportIndexUploadRequest, ...grpc.CallOption) (*FinishImportResponse, error) {
	return &FinishImportResponse{Status: ErrorCode_SUCCESS}, nil
}

// InstallMockTiCIManagerForTest replaces TiCI etcd/meta-service initialization with a
// lightweight in-process stub for callers from other packages.
func InstallMockTiCIManagerForTest(t testing.TB) {
	t.Helper()

	originalGetEtcdClient := getEtcdClientFunc
	originalNewManagerCtx := newManagerCtxFunc

	getEtcdClientFunc = func() (*clientv3.Client, error) {
		return nil, nil
	}
	newManagerCtxFunc = func(ctx context.Context, _ *clientv3.Client) (*ManagerCtx, error) {
		mockCtx, cancel := context.WithCancel(ctx)
		return &ManagerCtx{
			metaClient: &metaClient{client: mockMetaServiceClient{}},
			ctx:        mockCtx,
			cancel:     cancel,
		}, nil
	}

	t.Cleanup(func() {
		getEtcdClientFunc = originalGetEtcdClient
		newManagerCtxFunc = originalNewManagerCtx
	})
}
