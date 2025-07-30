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
func (m *MockMetaServiceClient) GetCloudStoragePath(ctx context.Context, in *GetCloudStoragePathRequest, opts ...grpc.CallOption) (*GetCloudStoragePathResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*GetCloudStoragePathResponse), args.Error(1)
}
func (m *MockMetaServiceClient) MarkPartitionUploadFinished(ctx context.Context, in *MarkPartitionUploadFinishedRequest, opts ...grpc.CallOption) (*MarkPartitionUploadFinishedResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*MarkPartitionUploadFinishedResponse), args.Error(1)
}
func (m *MockMetaServiceClient) MarkTableUploadFinished(ctx context.Context, in *MarkTableUploadFinishedRequest, opts ...grpc.CallOption) (*MarkTableUploadFinishedResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*MarkTableUploadFinishedResponse), args.Error(1)
}

func newTestTiCIManagerCtx(mockClient MetaServiceClient) *ManagerCtx {
	return &ManagerCtx{
		Conn:              nil,
		metaServiceClient: mockClient,
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
		Return(&GetCloudStoragePathResponse{Status: 0, S3Path: "/s3/path"}, nil).
		Once()
	path, err := ctx.GetCloudStoragePath(context.Background(), tblInfo, indexInfo, schemaName, lower, upper)
	assert.NoError(t, err)
	assert.Equal(t, "/s3/path", path)

	mockClient.
		On("GetCloudStoragePath", mock.Anything, mock.Anything).
		Return(&GetCloudStoragePathResponse{Status: 1, ErrorMessage: "fail"}, nil).
		Once()
	_, err = ctx.GetCloudStoragePath(context.Background(), tblInfo, indexInfo, schemaName, lower, upper)
	assert.Error(t, err)

	mockClient.
		On("GetCloudStoragePath", mock.Anything, mock.Anything).
		Return(&GetCloudStoragePathResponse{}, errors.New("rpc error")).
		Once()
	_, err = ctx.GetCloudStoragePath(context.Background(), tblInfo, indexInfo, schemaName, lower, upper)
	assert.Error(t, err)
}

func TestMarkPartitionUploadFinished(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	s3Path := "/s3/path"

	// 1st call – success
	mockClient.
		On("MarkPartitionUploadFinished", mock.Anything, mock.Anything).
		Return(&MarkPartitionUploadFinishedResponse{Status: 0}, nil).
		Once()
	assert.NoError(t, ctx.MarkPartitionUploadFinished(context.Background(), s3Path))

	// 2nd call – business error from TiCI
	mockClient.
		On("MarkPartitionUploadFinished", mock.Anything, mock.Anything).
		Return(&MarkPartitionUploadFinishedResponse{Status: 1, ErrorMessage: "fail"}, nil).
		Once()
	assert.Error(t, ctx.MarkPartitionUploadFinished(context.Background(), s3Path))

	// 3rd call – RPC error
	mockClient.
		On("MarkPartitionUploadFinished", mock.Anything, mock.Anything).
		Return(&MarkPartitionUploadFinishedResponse{}, errors.New("rpc error")).
		Once()
	assert.Error(t, ctx.MarkPartitionUploadFinished(context.Background(), s3Path))

	mockClient.AssertExpectations(t)
}

func TestMarkTableUploadFinished(t *testing.T) {
	mockClient := new(MockMetaServiceClient)
	ctx := newTestTiCIManagerCtx(mockClient)
	tableID, indexID := int64(1), int64(2)

	mockClient.
		On("MarkTableUploadFinished", mock.Anything, mock.Anything).
		Return(&MarkTableUploadFinishedResponse{Status: 0}, nil).
		Once()
	err := ctx.MarkTableUploadFinished(context.Background(), tableID, indexID)
	assert.NoError(t, err)

	mockClient.
		On("MarkTableUploadFinished", mock.Anything, mock.Anything).
		Return(&MarkTableUploadFinishedResponse{Status: 1, ErrorMessage: "fail"}, nil).
		Once()
	err = ctx.MarkTableUploadFinished(context.Background(), tableID, indexID)
	assert.Error(t, err)

	mockClient.
		On("MarkTableUploadFinished", mock.Anything, mock.Anything).
		Return(&MarkTableUploadFinishedResponse{}, errors.New("rpc error")).
		Once()
	err = ctx.MarkTableUploadFinished(context.Background(), tableID, indexID)
	assert.Error(t, err)
}

func TestTiCIManagerCtxClose(t *testing.T) {
	ctx := &ManagerCtx{Conn: nil}
	assert.NoError(t, ctx.Close())
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
