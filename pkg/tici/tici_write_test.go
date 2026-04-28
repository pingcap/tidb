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

package tici

import (
	"context"
	"slices"
	"testing"

	"github.com/docker/go-units"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zaptest"
)

// newStubTICIFileWriter returns a real *TICIFileWriter whose underlying
// storage is the in‑memory mock used in tici_file_writer_test.go.
func newStubTICIFileWriter(t *testing.T, failWrite bool) (*FileWriter, *mockExternalFileWriter) {
	t.Helper()

	ctx := context.Background()
	writer := &mockExternalFileWriter{fail: failWrite}
	store := &mockExternalStorage{writer: writer}

	tfw, err := NewTICIFileWriter(ctx, store, "unit-test-file", 5*units.MiB, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("newStubTICIFileWriter: %v", err)
	}
	return tfw, writer
}

func TestTiCIDataWriterGroup_CreateFail(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.Anything).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_UNKNOWN_ERROR}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	assert.Nil(t, group)
}

func TestTiCIDataWriterGroup_WriteHeader(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.Anything).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	mockFileWriter, _ := newStubTICIFileWriter(t, false)
	err := group.WriteHeader(ctx, mockFileWriter, 1)
	assert.NoError(t, err)
}

func TestTiCIDataWriterGroup_WritePairs(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.Anything).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	mockFileWriter, _ := newStubTICIFileWriter(t, false)
	pairs := []*sst.Pair{{Key: []byte("k"), Value: []byte("v")}}
	err := group.WritePairs(ctx, mockFileWriter, pairs, 1)
	assert.NoError(t, err)
}

func TestTiCIDataWriterGroup_WritePairs_Fail(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.Anything).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	mockFileWriter, mockWriter := newStubTICIFileWriter(t, false)
	mockWriter.fail = true
	pairs := []*sst.Pair{{Key: []byte("k"), Value: []byte("v")}}
	err := group.WritePairs(ctx, mockFileWriter, pairs, 1)
	assert.Error(t, err)
}

func TestTiCIDataWriterGroup_FinishIndexUpload(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.Anything).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil)
	mockClient.
		On("FinishImportIndexUpload", mock.Anything, mock.Anything).
		Return(&FinishImportResponse{Status: ErrorCode_SUCCESS}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	err := group.FinishIndexUpload(ctx)
	assert.NoError(t, err)
}

func TestNewTiCIDataWriterGroupUsesInjectedManagerFactory(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	keyspaceID := uint32(123)
	taskID := "ddl/backfill/123"

	originalGetEtcdClient := getEtcdClientFunc
	originalNewManagerCtx := newManagerCtxFunc
	getEtcdClientFunc = func() (*clientv3.Client, error) {
		return nil, nil
	}
	newManagerCtxFunc = func(_ context.Context, _ *clientv3.Client) (*ManagerCtx, error) {
		return ticiMgr, nil
	}
	t.Cleanup(func() {
		getEtcdClientFunc = originalGetEtcdClient
		newManagerCtxFunc = originalNewManagerCtx
	})

	mockClient.
		On("GetImportStoragePrefix", mock.Anything, mock.MatchedBy(func(req *GetImportStoragePrefixRequest) bool {
			return req.GetTidbTaskId() == taskID &&
				req.GetTableId() == tbl.ID &&
				slices.Equal(req.GetIndexIds(), []int64{2}) &&
				req.GetKeyspaceId() == keyspaceID
		})).
		Return(&GetImportStoragePrefixResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()

	group, err := NewTiCIDataWriterGroup(ctx, tbl, "testdb", taskID, keyspaceID, []int64{2})
	require.NoError(t, err)
	require.NotNil(t, group)
	require.Equal(t, keyspaceID, ticiMgr.getKeyspaceID())
	mockClient.AssertExpectations(t)
}
