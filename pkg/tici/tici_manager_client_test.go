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
//

package tici

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// MockMetaServiceClient mocks MetaServiceClient
type MockMetaServiceClient struct {
	mock.Mock
	MetaServiceClient
}

func (m *MockMetaServiceClient) CreateIndex(ctx context.Context, in *CreateIndexRequest, opts ...grpc.CallOption) (*CreateIndexResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*CreateIndexResponse), args.Error(1)
}
func (m *MockMetaServiceClient) GetImportStoragePrefix(ctx context.Context, in *GetImportStoragePrefixRequest, opts ...grpc.CallOption) (*GetImportStoragePrefixResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*GetImportStoragePrefixResponse), args.Error(1)
}
func (m *MockMetaServiceClient) FinishImportPartitionUpload(ctx context.Context, in *FinishImportPartitionUploadRequest, opts ...grpc.CallOption) (*FinishImportResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*FinishImportResponse), args.Error(1)
}
func (m *MockMetaServiceClient) FinishImportIndexUpload(ctx context.Context, in *FinishImportIndexUploadRequest, opts ...grpc.CallOption) (*FinishImportResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*FinishImportResponse), args.Error(1)
}
func (m *MockMetaServiceClient) GetShardLocalCacheInfo(ctx context.Context, in *GetShardLocalCacheRequest, opts ...grpc.CallOption) (*GetShardLocalCacheResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*GetShardLocalCacheResponse), args.Error(1)
}

func newTestTiCIManagerCtx(mockClient MetaServiceClient) *ManagerCtx {
	return &ManagerCtx{
		metaClient: &metaClient{
			conn:   nil, // Not used in tests
			client: mockClient,
		},
		ctx: context.Background(),
	}
}

func newTestTiCIManagerCtxWithCancel(mockClient MetaServiceClient) *ManagerCtx {
	ctx, cancel := context.WithCancel(context.Background())
	return &ManagerCtx{
		metaClient: &metaClient{
			conn:   nil, // Not used in tests
			client: mockClient,
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Add generic helper to match keyspace id on pointer request types.
// This expects the type argument to be a pointer type (e.g. *CreateIndexRequest)
// that implements GetKeyspaceId() uint32.
func matchKeyspace[T interface{ GetKeyspaceId() uint32 }](expect uint32) func(T) bool {
	return func(req T) bool {
		return req.GetKeyspaceId() == expect
	}
}

func TestCreateFulltextIndex(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	keyspaceID := uint32(123)
	ctx.SetKeyspaceID(keyspaceID)
	tblInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("c"), FieldType: types.FieldType{}}}, Version: 1}
	indexInfo := &model.IndexInfo{ID: 2, Name: ast.NewCIStr("idx"), Columns: []*model.IndexColumn{{Offset: 0}}, Unique: true}
	schemaName := "testdb"

	mockClient.
		On("CreateIndex", mock.Anything, mock.MatchedBy(matchKeyspace[*CreateIndexRequest](keyspaceID))).
		Return(&CreateIndexResponse{Status: ErrorCode_SUCCESS, IndexId: "2"}, nil).
		Once()
	err := ctx.CreateFulltextIndex(context.Background(), tblInfo, indexInfo, schemaName, nil)
	assert.NoError(t, err)

	mockClient.
		On("CreateIndex", mock.Anything, mock.MatchedBy(matchKeyspace[*CreateIndexRequest](keyspaceID))).
		Return(&CreateIndexResponse{Status: ErrorCode_UNKNOWN_ERROR, IndexId: "2", ErrorMessage: "fail"}, nil).
		Once()
	err = ctx.CreateFulltextIndex(context.Background(), tblInfo, indexInfo, schemaName, nil)
	require.ErrorContains(t, err, "fail")

	mockClient.
		On("CreateIndex", mock.Anything, mock.MatchedBy(matchKeyspace[*CreateIndexRequest](keyspaceID))).
		Return(&CreateIndexResponse{}, errors.New("rpc error")).
		Once()
	err = ctx.CreateFulltextIndex(context.Background(), tblInfo, indexInfo, schemaName, nil)
	require.ErrorContains(t, err, "rpc error")
}

func TestGetImportStoragePrefix(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	keyspaceID := uint32(456)
	ctx.SetKeyspaceID(keyspaceID)
	taskID := "tidb-task-123"
	tblID := int64(1)
	indexIDs := []int64{2, 3}

	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.MatchedBy(matchKeyspace[*GetImportStoragePrefixRequest](keyspaceID))).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "/s3/path?endpoint=http://127.0.0.1"}, nil).
		Once()
	path, jobID, err := ctx.GetCloudStoragePrefix(context.Background(), taskID, tblID, indexIDs)
	assert.Equal(t, uint64(100), jobID)
	assert.NoError(t, err)
	assert.Equal(t, "/s3/path?endpoint=http://127.0.0.1", path)

	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.MatchedBy(matchKeyspace[*GetImportStoragePrefixRequest](keyspaceID))).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_UNKNOWN_ERROR, ErrorMessage: "fail"}, nil).
		Once()
	_, _, err = ctx.GetCloudStoragePrefix(context.Background(), taskID, tblID, indexIDs)
	assert.Error(t, err)

	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.MatchedBy(matchKeyspace[*GetImportStoragePrefixRequest](keyspaceID))).
		Return(&GetImportStoragePrefixResponse{}, errors.New("rpc error")).
		Once()
	_, _, err = ctx.GetCloudStoragePrefix(context.Background(), taskID, tblID, indexIDs)
	assert.Error(t, err)
}

func TestFinishPartitionUpload(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	keyspaceID := uint32(789)
	ctx.SetKeyspaceID(keyspaceID)
	taskID := "tidb-task-123"
	lower, upper := []byte("a"), []byte("z")

	// 1st call – success
	mockClient.
		On("FinishImportPartitionUpload", mock.Anything, mock.MatchedBy(matchKeyspace[*FinishImportPartitionUploadRequest](keyspaceID))).
		Return(&FinishImportResponse{Status: ErrorCode_SUCCESS}, nil).
		Once()
	assert.NoError(t, ctx.FinishPartitionUpload(context.Background(), taskID, lower, upper, "/s3/path"))

	// 2nd call – business error from TiCI
	mockClient.
		On("FinishImportPartitionUpload", mock.Anything, mock.MatchedBy(matchKeyspace[*FinishImportPartitionUploadRequest](keyspaceID))).
		Return(&FinishImportResponse{Status: ErrorCode_UNKNOWN_ERROR, ErrorMessage: "fail"}, nil).
		Once()
	assert.Error(t, ctx.FinishPartitionUpload(context.Background(), taskID, lower, upper, "/s3/path"))

	// 3rd call – RPC error
	mockClient.
		On("FinishImportPartitionUpload", mock.Anything, mock.MatchedBy(matchKeyspace[*FinishImportPartitionUploadRequest](keyspaceID))).
		Return(&FinishImportResponse{}, errors.New("rpc error")).
		Once()
	assert.Error(t, ctx.FinishPartitionUpload(context.Background(), taskID, lower, upper, "/s3/path"))

	mockClient.AssertExpectations(t)
}

func TestFinishIndexUpload(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	keyspaceID := uint32(321)
	ctx.SetKeyspaceID(keyspaceID)
	taskID := "tidb-task-123"

	mockClient.
		On("FinishImportIndexUpload", mock.Anything, mock.MatchedBy(matchKeyspace[*FinishImportIndexUploadRequest](keyspaceID))).
		Return(&FinishImportResponse{Status: ErrorCode_SUCCESS}, nil).
		Once()
	err := ctx.FinishIndexUpload(context.Background(), taskID)
	assert.NoError(t, err)

	mockClient.
		On("FinishImportIndexUpload", mock.Anything, mock.MatchedBy(matchKeyspace[*FinishImportIndexUploadRequest](keyspaceID))).
		Return(&FinishImportResponse{Status: ErrorCode_UNKNOWN_ERROR, ErrorMessage: "fail"}, nil).
		Once()
	err = ctx.FinishIndexUpload(context.Background(), taskID)
	assert.Error(t, err)

	mockClient.
		On("FinishImportIndexUpload", mock.Anything, mock.MatchedBy(matchKeyspace[*FinishImportIndexUploadRequest](keyspaceID))).
		Return(&FinishImportResponse{}, errors.New("rpc error")).
		Once()
	err = ctx.FinishIndexUpload(context.Background(), taskID)
	assert.Error(t, err)
}

func TestFinishIndexUploadHelper(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	managerCtx := newTestTiCIManagerCtxWithCancel(mockClient)

	originalGetEtcdClient := getEtcdClientFunc
	originalNewManagerCtx := newManagerCtxFunc
	getEtcdClientFunc = func() (*clientv3.Client, error) {
		return &clientv3.Client{}, nil
	}
	newManagerCtxFunc = func(_ context.Context, _ *clientv3.Client) (*ManagerCtx, error) {
		return managerCtx, nil
	}
	defer func() {
		getEtcdClientFunc = originalGetEtcdClient
		newManagerCtxFunc = originalNewManagerCtx
	}()

	taskID := "tidb-task-123"
	mockClient.
		On("FinishImportIndexUpload", mock.Anything, mock.MatchedBy(func(req *FinishImportIndexUploadRequest) bool {
			return req.GetTidbTaskId() == taskID && req.GetKeyspaceId() == 0 && req.GetStatus() == ErrorCode_SUCCESS
		})).
		Return(&FinishImportResponse{Status: ErrorCode_SUCCESS}, nil).
		Once()

	err := FinishIndexUpload(context.Background(), nil, taskID)
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestScanRanges(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	tableID, indexID := int64(1), int64(2)

	keyRanges := []kv.KeyRange{
		{StartKey: []byte("a"), EndKey: []byte("b")},
		{StartKey: []byte("c"), EndKey: []byte("d")},
	}

	mockClient.
		On("GetShardLocalCacheInfo", mock.Anything, mock.Anything).
		Return(&GetShardLocalCacheResponse{
			Status: 0,
			ShardLocalCacheInfos: []*ShardLocalCacheInfo{
				{Shard: &ShardManifestHeader{ShardId: 1, StartKey: []byte("a"), EndKey: []byte("b"), Epoch: 1}, LocalCacheAddrs: []string{"addr1"}},
				{Shard: &ShardManifestHeader{ShardId: 2, StartKey: []byte("c"), EndKey: []byte("d"), Epoch: 1}, LocalCacheAddrs: []string{"addr2"}},
			},
		}, nil).
		Once()

	shardInfos, err := ctx.ScanRanges(context.Background(), tableID, indexID, keyRanges, 100)
	assert.NoError(t, err)
	assert.Len(t, shardInfos, 2)
	assert.Equal(t, uint64(1), shardInfos[0].Shard.ShardId)
	assert.Equal(t, []byte("a"), shardInfos[0].Shard.StartKey)
	assert.Equal(t, []byte("b"), shardInfos[0].Shard.EndKey)
	assert.Equal(t, []string{"addr1"}, shardInfos[0].LocalCacheAddrs)
	assert.Equal(t, uint64(2), shardInfos[1].Shard.ShardId)
	assert.Equal(t, []byte("c"), shardInfos[1].Shard.StartKey)
	assert.Equal(t, []byte("d"), shardInfos[1].Shard.EndKey)
	assert.Equal(t, []string{"addr2"}, shardInfos[1].LocalCacheAddrs)
}

func TestModelTableToTiCITableInfo(t *testing.T) {
	tblInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("c"), FieldType: types.FieldType{}}}, Version: 1}
	ti := ModelTableToTiCITableInfo(tblInfo, "db")
	assert.Equal(t, int64(1), ti.TableId)
	assert.Equal(t, "db", ti.DatabaseName)
}

func TestModelIndexToTiCIIndexInfo(t *testing.T) {
	tblInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("c"), FieldType: types.FieldType{}}}, Version: 1}
	indexInfo := &model.IndexInfo{ID: 2, Name: ast.NewCIStr("idx"), Columns: []*model.IndexColumn{{Offset: 0}}, Unique: true}
	ii := ModelIndexToTiCIIndexInfo(indexInfo, tblInfo)
	assert.Equal(t, int64(2), ii.IndexId)
	assert.Equal(t, int64(1), ii.TableId)
}

func TestCloneAndNormalizeTableInfo(t *testing.T) {
	tests := []struct {
		name      string
		input     *model.TableInfo
		wantError bool
		check     func(t *testing.T, input, output *model.TableInfo)
	}{
		{
			name: "table with longtext and json columns",
			input: &model.TableInfo{
				ID: 1,
				Columns: []*model.ColumnInfo{
					{
						ID: 1,
						FieldType: func() types.FieldType {
							ft := types.FieldType{}
							ft.SetType(mysql.TypeLongBlob)
							ft.SetFlen(0x7fffffff + 1) // Value larger than int32 max
							return ft
						}(),
					},
					{
						ID: 2,
						FieldType: func() types.FieldType {
							ft := types.FieldType{}
							ft.SetType(mysql.TypeJSON)
							ft.SetFlen(0x7fffffff + 2)
							return ft
						}(),
					},
					{
						ID: 3,
						FieldType: func() types.FieldType {
							ft := types.FieldType{}
							ft.SetType(mysql.TypeVarchar)
							ft.SetFlen(0x7fffffff + 3)
							return ft
						}(),
					},
				},
			},
			wantError: false,
			check: func(t *testing.T, input, output *model.TableInfo) {
				require.NotNil(t, output)
				require.Equal(t, input.ID, output.ID)
				require.Len(t, output.Columns, 3)

				// Check longtext column was narrowed
				require.Equal(t, int32(input.Columns[0].GetFlen()), int32(output.Columns[0].GetFlen()))
				require.NotEqual(t, input.Columns[0].GetFlen(), output.Columns[0].GetFlen())

				// Check json column was narrowed
				require.Equal(t, int32(input.Columns[1].GetFlen()), int32(output.Columns[1].GetFlen()))
				require.NotEqual(t, input.Columns[1].GetFlen(), output.Columns[1].GetFlen())

				// Check varchar column was unchanged
				require.Equal(t, input.Columns[2].GetFlen(), output.Columns[2].GetFlen())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := cloneAndNormalizeTableInfo(tt.input)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			tt.check(t, tt.input, output)
		})
	}
}
