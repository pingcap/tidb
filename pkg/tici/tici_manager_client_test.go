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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
func (m *MockMetaServiceClient) GetCloudStoragePath(ctx context.Context, in *GetImportStoragePathRequest, opts ...grpc.CallOption) (*GetImportStoragePathResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*GetImportStoragePathResponse), args.Error(1)
}
func (m *MockMetaServiceClient) MarkPartitionUploadFinished(ctx context.Context, in *FinishPartitionUploadRequest, opts ...grpc.CallOption) (*FinishPartitionUploadResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*FinishPartitionUploadResponse), args.Error(1)
}
func (m *MockMetaServiceClient) MarkTableUploadFinished(ctx context.Context, in *FinishImportIndexUploadRequest, opts ...grpc.CallOption) (*FinishImportIndexUploadResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*FinishImportIndexUploadResponse), args.Error(1)
}
func (m *MockMetaServiceClient) GetShardLocalCacheInfo(ctx context.Context, in *GetShardLocalCacheRequest, opts ...grpc.CallOption) (*GetShardLocalCacheResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*GetShardLocalCacheResponse), args.Error(1)
}

func newTestTiCIManagerCtx(mockClient MetaServiceClient) *ManagerCtx {
	return &ManagerCtx{
		conn:              nil,
		metaServiceClient: mockClient,
		ctx:               context.Background(),
	}
}

func TestCreateFulltextIndex(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	tblInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("c"), FieldType: types.FieldType{}}}, Version: 1}
	indexInfo := &model.IndexInfo{ID: 2, Name: ast.NewCIStr("idx"), Columns: []*model.IndexColumn{{Offset: 0}}, Unique: true}
	schemaName := "testdb"

	mockClient.
		On("CreateIndex", mock.Anything, mock.Anything).
		Return(&CreateIndexResponse{Status: 0, IndexId: "2"}, nil).
		Once()
	err := ctx.CreateFulltextIndex(context.Background(), tblInfo, indexInfo, schemaName)
	assert.NoError(t, err)

	mockClient.
		On("CreateIndex", mock.Anything, mock.Anything).
		Return(&CreateIndexResponse{Status: 1, IndexId: "2", ErrorMessage: "fail"}, nil).
		Once()
	err = ctx.CreateFulltextIndex(context.Background(), tblInfo, indexInfo, schemaName)
	assert.NoError(t, err)

	mockClient.
		On("CreateIndex", mock.Anything, mock.Anything).
		Return(&CreateIndexResponse{}, errors.New("rpc error")).
		Once()
	err = ctx.CreateFulltextIndex(context.Background(), tblInfo, indexInfo, schemaName)
	assert.Error(t, err)
}

func TestGetCloudStoragePath(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	tblInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("c"), FieldType: types.FieldType{}}}, Version: 1}
	indexInfo := &model.IndexInfo{ID: 2, Name: ast.NewCIStr("idx"), Columns: []*model.IndexColumn{{Offset: 0}}, Unique: true}
	schemaName := "testdb"
	lower, upper := []byte("a"), []byte("z")

	mockClient.
		On("GetCloudStoragePath", mock.Anything, mock.Anything).
		Return(&GetImportStoragePathResponse{Status: 0, S3Path: "/s3/path"}, nil).
		Once()
	path, err := ctx.GetCloudStoragePath(context.Background(), tblInfo, indexInfo, schemaName, lower, upper)
	assert.NoError(t, err)
	assert.Equal(t, "/s3/path", path)

	mockClient.
		On("GetCloudStoragePath", mock.Anything, mock.Anything).
		Return(&GetImportStoragePathResponse{Status: 1, ErrorMessage: "fail"}, nil).
		Once()
	_, err = ctx.GetCloudStoragePath(context.Background(), tblInfo, indexInfo, schemaName, lower, upper)
	assert.Error(t, err)

	mockClient.
		On("GetCloudStoragePath", mock.Anything, mock.Anything).
		Return(&GetImportStoragePathResponse{}, errors.New("rpc error")).
		Once()
	_, err = ctx.GetCloudStoragePath(context.Background(), tblInfo, indexInfo, schemaName, lower, upper)
	assert.Error(t, err)
}

func TestMarkPartitionUploadFinished(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	tableID := int64(1)
	indexID := int64(2)
	lower, upper := []byte("a"), []byte("z")

	// 1st call – success
	mockClient.
		On("MarkPartitionUploadFinished", mock.Anything, mock.Anything).
		Return(&FinishPartitionUploadResponse{Status: 0}, nil).
		Once()
	assert.NoError(t, ctx.MarkPartitionUploadFinished(context.Background(), tableID, indexID, lower, upper))

	// 2nd call – business error from TiCI
	mockClient.
		On("MarkPartitionUploadFinished", mock.Anything, mock.Anything).
		Return(&FinishPartitionUploadResponse{Status: 1, ErrorMessage: "fail"}, nil).
		Once()
	assert.Error(t, ctx.MarkPartitionUploadFinished(context.Background(), tableID, indexID, lower, upper))

	// 3rd call – RPC error
	mockClient.
		On("MarkPartitionUploadFinished", mock.Anything, mock.Anything).
		Return(&FinishPartitionUploadResponse{}, errors.New("rpc error")).
		Once()
	assert.Error(t, ctx.MarkPartitionUploadFinished(context.Background(), tableID, indexID, lower, upper))

	mockClient.AssertExpectations(t)
}

func TestMarkTableUploadFinished(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	tableID, indexID := int64(1), int64(2)

	mockClient.
		On("MarkTableUploadFinished", mock.Anything, mock.Anything).
		Return(&FinishImportIndexUploadResponse{Status: 0}, nil).
		Once()
	err := ctx.MarkTableUploadFinished(context.Background(), tableID, indexID)
	assert.NoError(t, err)

	mockClient.
		On("MarkTableUploadFinished", mock.Anything, mock.Anything).
		Return(&FinishImportIndexUploadResponse{Status: 1, ErrorMessage: "fail"}, nil).
		Once()
	err = ctx.MarkTableUploadFinished(context.Background(), tableID, indexID)
	assert.Error(t, err)

	mockClient.
		On("MarkTableUploadFinished", mock.Anything, mock.Anything).
		Return(&FinishImportIndexUploadResponse{}, errors.New("rpc error")).
		Once()
	err = ctx.MarkTableUploadFinished(context.Background(), tableID, indexID)
	assert.Error(t, err)
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

func TestModelPrimaryKeyToTiCIIndexInfo(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID:      1,
		Name:    ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("c"), FieldType: types.FieldType{}}},
		Version: 1,
		Indices: []*model.IndexInfo{
			{ID: 2, Name: ast.NewCIStr("pk"), Columns: []*model.IndexColumn{{Offset: 0}}, Unique: true, Primary: true},
		},
	}
	ii := ModelPrimaryKeyToTiCIIndexInfo(tblInfo)
	assert.NotNil(t, ii)
	assert.Equal(t, int64(2), ii.IndexId)

	// No primary key
	tblInfo.Indices = []*model.IndexInfo{}
	ii = ModelPrimaryKeyToTiCIIndexInfo(tblInfo)
	assert.Nil(t, ii)
}
