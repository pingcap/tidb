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
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func newTestTiCIDataWriter(t *testing.T) *DataWriter {
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{}}
	idx := &model.IndexInfo{ID: 2, Name: ast.NewCIStr("idx")}
	logger := zaptest.NewLogger(t).With(
		zap.Int64("tableID", tbl.ID),
		zap.String("tableName", tbl.Name.O),
		zap.Int64("indexID", idx.ID),
		zap.String("indexName", idx.Name.O),
	)
	return &DataWriter{
		tblInfo: tbl,
		idxInfo: idx,
		schema:  "testdb",
		logger:  logger,
	}
}

// newStubTICIFileWriter returns a real *TICIFileWriter whose underlying
// storage is the in‑memory mock used in tici_file_writer_test.go.
func newStubTICIFileWriter(t *testing.T, failWrite bool) (*FileWriter, *mockExternalFileWriter) {
	t.Helper()

	ctx := context.Background()
	writer := &mockExternalFileWriter{fail: failWrite}
	store := &mockExternalStorage{writer: writer}

	tfw, err := NewTICIFileWriter(ctx, store,
		"unit‑test‑file", 5*units.MiB, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("newStubTICIFileWriter: %v", err)
	}
	return tfw, writer
}

func TestDataWriterWriteHeader(t *testing.T) {
	w := newTestTiCIDataWriter(t)
	mockFileWriter, mockWriter := newStubTICIFileWriter(t, false)
	w.ticiFileWriter = mockFileWriter
	err := w.WriteHeader(context.Background(), uint64(time.Now().UnixNano()))
	assert.NoError(t, err)
	assert.Greater(t, len(mockWriter.writes), 0)
}

func TestDataWriterWriteHeader_NotInit(t *testing.T) {
	w := newTestTiCIDataWriter(t)
	err := w.WriteHeader(context.Background(), 1)
	assert.Error(t, err)
}

func TestDataWriterWriteHeader_ProtoFail(t *testing.T) {
	// Simulate a table/index that cannot be marshaled (e.g., nil)
	w := newTestTiCIDataWriter(t)
	w.tblInfo = nil
	w.idxInfo = nil
	w.logger = zaptest.NewLogger(t) // ensure logger is not nil
	mockWriter, _ := newStubTICIFileWriter(t, false)
	w.ticiFileWriter = mockWriter
	err := w.WriteHeader(context.Background(), 1)
	assert.Error(t, err)
}

func TestWritePairs(t *testing.T) {
	w := newTestTiCIDataWriter(t)

	mockFileWriter, mockWriter := newStubTICIFileWriter(t, false)
	w.ticiFileWriter = mockFileWriter

	pairs := []*sst.Pair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}

	err := w.WritePairs(context.Background(), pairs, len(pairs))
	assert.NoError(t, err)

	assert.Equal(t, len(pairs), len(mockWriter.writes))
}

func TestWritePairs_WriteRowFail(t *testing.T) {
	w := newTestTiCIDataWriter(t)

	mockFileWriter, _ := newStubTICIFileWriter(t, true)
	w.ticiFileWriter = mockFileWriter

	pairs := []*sst.Pair{
		{Key: []byte("k1"), Value: []byte("v1")},
	}
	err := w.WritePairs(context.Background(), pairs, len(pairs))
	assert.Error(t, err)
}

func TestCloseFileWriter(t *testing.T) {
	w := newTestTiCIDataWriter(t)
	mockFileWriter, mockWriter := newStubTICIFileWriter(t, false)
	w.ticiFileWriter = mockFileWriter
	err := w.CloseFileWriter(context.Background())
	assert.NoError(t, err)
	assert.True(t, mockWriter.closed)
}

func TestCloseFileWriter_NotInit(t *testing.T) {
	w := newTestTiCIDataWriter(t)
	err := w.CloseFileWriter(context.Background())
	assert.NoError(t, err)
}

func TestCloseFileWriter_CloseFail(t *testing.T) {
	w := newTestTiCIDataWriter(t)
	mockFileWriter, mockWriter := newStubTICIFileWriter(t, false)
	w.ticiFileWriter = mockFileWriter
	mockWriter.fail = true
	err := w.CloseFileWriter(context.Background())
	assert.Error(t, err)
}

func TestTiCIDataWriterGroup_CreateFail(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("SubmitImportIndexJob", mock.Anything, mock.Anything).
		Return(&ImportIndexJobResponse{Status: ErrorCode_UNKNOWN_ERROR}, nil).
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
		On("SubmitImportIndexJob", mock.Anything, mock.Anything).
		Return(&ImportIndexJobResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	for _, w := range group.writers {
		mockFileWriter, _ := newStubTICIFileWriter(t, false)
		w.ticiFileWriter = mockFileWriter
		// Ensure logger is not nil for safety
		if w.logger == nil {
			w.logger = zaptest.NewLogger(t)
		}
	}
	group.writable.Store(true)
	err := group.WriteHeader(ctx, 1)
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
		On("SubmitImportIndexJob", mock.Anything, mock.Anything).
		Return(&ImportIndexJobResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	for _, w := range group.writers {
		mockFileWriter, _ := newStubTICIFileWriter(t, false)
		w.ticiFileWriter = mockFileWriter
		if w.logger == nil {
			w.logger = zaptest.NewLogger(t)
		}
	}
	group.writable.Store(true)
	pairs := []*sst.Pair{{Key: []byte("k"), Value: []byte("v")}}
	err := group.WritePairs(ctx, pairs, 1)
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
		On("SubmitImportIndexJob", mock.Anything, mock.Anything).
		Return(&ImportIndexJobResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	for _, w := range group.writers {
		mockFileWriter, mockWriter := newStubTICIFileWriter(t, false)
		mockWriter.fail = true
		w.ticiFileWriter = mockFileWriter
		if w.logger == nil {
			w.logger = zaptest.NewLogger(t)
		}
	}
	group.writable.Store(true)
	pairs := []*sst.Pair{{Key: []byte("k"), Value: []byte("v")}}
	err := group.WritePairs(ctx, pairs, 1)
	assert.Error(t, err)
}

func TestSetTiCIDataWriterGroupWritable(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("SubmitImportIndexJob", mock.Anything, mock.Anything).
		Return(&ImportIndexJobResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	engineUUID := uuid.New()
	SetTiCIDataWriterGroupWritable(ctx, group, engineUUID, 0)
	assert.True(t, group.writable.Load())
	SetTiCIDataWriterGroupWritable(ctx, group, engineUUID, IndexEngineID)
	assert.False(t, group.writable.Load())
	SetTiCIDataWriterGroupWritable(ctx, nil, engineUUID, 0) // should not panic
}

func TestTiCIDataWriterGroup_InitTICIFileWriters_NotWritable(t *testing.T) {
	ctx := context.Background()
	group := &DataWriterGroup{}
	group.writable.Store(false)
	err := group.InitTICIFileWriters(ctx)
	assert.NoError(t, err)
}

func TestTiCIDataWriterGroup_FinishPartitionUpload_NotWritable(t *testing.T) {
	ctx := context.Background()
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("SubmitImportIndexJob", mock.Anything, mock.Anything).
		Return(&ImportIndexJobResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil).
		Once()
	mockClient.
		On("FinishImportPartitionUpload", mock.Anything, mock.Anything).
		Return(&FinishImportResponse{Status: ErrorCode_SUCCESS}, nil).
		Once()
	group := &DataWriterGroup{mgrCtx: ticiMgr}
	group.writable.Store(false)
	err := group.FinishPartitionUpload(ctx, nil, nil)
	assert.NoError(t, err)
}

func TestTiCIDataWriterGroup_FinishIndexUpload(t *testing.T) {
	ctx := context.Background()
	tbl := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), Indices: []*model.IndexInfo{
		{ID: 2, Name: ast.NewCIStr("idx"), FullTextInfo: &model.FullTextIndexInfo{}},
	}}
	mockClient := new(MockMetaServiceClient)
	ticiMgr := newTestTiCIManagerCtx(mockClient)
	mockClient.
		On("SubmitImportIndexJob", mock.Anything, mock.Anything).
		Return(&ImportIndexJobResponse{Status: ErrorCode_SUCCESS, JobId: 100, StorageUri: "s3://my-bucket/prefix"}, nil)
	mockClient.
		On("FinishImportIndexUpload", mock.Anything, mock.Anything).
		Return(&FinishImportResponse{Status: ErrorCode_SUCCESS}, nil).
		Once()
	group := newTiCIDataWriterGroupForTest(ctx, ticiMgr, tbl, "testdb")
	for _, w := range group.writers {
		mockFileWriter, _ := newStubTICIFileWriter(t, false)
		w.ticiFileWriter = mockFileWriter
		if w.logger == nil {
			w.logger = zaptest.NewLogger(t)
		}
	}

	err := group.FinishIndexUpload(ctx)
	assert.NoError(t, err)
}

func TestTiCIDataWriterGroup_CloseFileWriters_NotWritable(t *testing.T) {
	ctx := context.Background()
	group := &DataWriterGroup{}
	group.writable.Store(false)
	err := group.CloseFileWriters(ctx)
	assert.NoError(t, err)
}
