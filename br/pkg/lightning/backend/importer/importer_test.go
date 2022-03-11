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

package importer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	kvpb "github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/stretchr/testify/require"
)

type importerSuite struct {
	controller *gomock.Controller
	mockClient *mock.MockImportKVClient
	mockWriter *mock.MockImportKV_WriteEngineClient
	ctx        context.Context
	engineUUID []byte
	engine     *backend.OpenedEngine
	kvPairs    kv.Rows
}

const testPDAddr = "pd-addr:2379"

func createImportSuite(t *testing.T) *importerSuite {
	controller := gomock.NewController(t)
	mockClient := mock.NewMockImportKVClient(controller)
	mockWriter := mock.NewMockImportKV_WriteEngineClient(controller)
	importer := NewMockImporter(mockClient, testPDAddr)
	s := &importerSuite{controller: controller, mockClient: mockClient, mockWriter: mockWriter, ctx: context.Background()}
	engineUUID := uuid.MustParse("7e3f3a3c-67ce-506d-af34-417ec138fbcb")
	s.engineUUID = engineUUID[:]
	s.kvPairs = kv.MakeRowsFromKvPairs([]common.KvPair{
		{
			Key: []byte("k1"),
			Val: []byte("v1"),
		},
		{
			Key: []byte("k2"),
			Val: []byte("v2"),
		},
	})

	s.mockClient.EXPECT().
		OpenEngine(s.ctx, &kvpb.OpenEngineRequest{Uuid: s.engineUUID}).
		Return(nil, nil)

	var err error
	s.engine, err = importer.OpenEngine(s.ctx, &backend.EngineConfig{}, "`db`.`table`", -1)
	require.NoError(t, err)
	return s
}

func (s *importerSuite) tearDownTest() {
	s.controller.Finish()
}

func TestWriteRows(t *testing.T) {
	s := createImportSuite(t)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(&kvpb.WriteEngineRequest{
			Chunk: &kvpb.WriteEngineRequest_Head{
				Head: &kvpb.WriteHead{Uuid: s.engineUUID},
			},
		}).
		Return(nil)
	batchSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *kvpb.WriteEngineRequest) error {
			require.Equal(t, []*kvpb.Mutation{
				{Op: kvpb.Mutation_Put, Key: []byte("k1"), Value: []byte("v1")},
				{Op: kvpb.Mutation_Put, Key: []byte("k2"), Value: []byte("v2")},
			}, x.GetBatch().GetMutations())
			return nil
		}).
		After(headSendCall)
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, nil).
		After(batchSendCall)

	writer, err := s.engine.LocalWriter(s.ctx, nil)
	require.NoError(t, err)
	err = writer.WriteRows(s.ctx, nil, s.kvPairs)
	require.NoError(t, err)
	st, err := writer.Close(s.ctx)
	require.NoError(t, err)
	require.Nil(t, st)
}

func TestWriteHeadSendFailed(t *testing.T) {
	s := createImportSuite(t)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *kvpb.WriteEngineRequest) error {
			require.NotNil(t, x.GetHead())
			return errors.Annotate(context.Canceled, "fake unrecoverable write head error")
		})
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, errors.Annotate(context.Canceled, "fake unrecoverable close stream error")).
		After(headSendCall)

	writer, err := s.engine.LocalWriter(s.ctx, nil)
	require.NoError(t, err)
	err = writer.WriteRows(s.ctx, nil, s.kvPairs)
	require.Error(t, err)
	require.Regexp(t, "^fake unrecoverable write head error", err.Error())
}

func TestWriteBatchSendFailed(t *testing.T) {
	s := createImportSuite(t)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *kvpb.WriteEngineRequest) error {
			require.NotNil(t, x.GetHead())
			return nil
		})
	batchSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *kvpb.WriteEngineRequest) error {
			require.NotNil(t, x.GetBatch())
			return errors.Annotate(context.Canceled, "fake unrecoverable write batch error")
		}).
		After(headSendCall)
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, errors.Annotate(context.Canceled, "fake unrecoverable close stream error")).
		After(batchSendCall)

	writer, err := s.engine.LocalWriter(s.ctx, nil)
	require.NoError(t, err)
	err = writer.WriteRows(s.ctx, nil, s.kvPairs)
	require.Error(t, err)
	require.Regexp(t, "^fake unrecoverable write batch error", err.Error())
}

func TestWriteCloseFailed(t *testing.T) {
	s := createImportSuite(t)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *kvpb.WriteEngineRequest) error {
			require.NotNil(t, x.GetHead())
			return nil
		})
	batchSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *kvpb.WriteEngineRequest) error {
			require.NotNil(t, x.GetBatch())
			return nil
		}).
		After(headSendCall)
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, errors.Annotate(context.Canceled, "fake unrecoverable close stream error")).
		After(batchSendCall)

	writer, err := s.engine.LocalWriter(s.ctx, nil)
	require.NoError(t, err)
	err = writer.WriteRows(s.ctx, nil, s.kvPairs)
	require.Error(t, err)
	require.Regexp(t, "^fake unrecoverable close stream error", err.Error())
}

func TestCloseImportCleanupEngine(t *testing.T) {
	s := createImportSuite(t)
	defer s.tearDownTest()

	s.mockClient.EXPECT().
		CloseEngine(s.ctx, &kvpb.CloseEngineRequest{Uuid: s.engineUUID}).
		Return(nil, nil)
	s.mockClient.EXPECT().
		ImportEngine(s.ctx, &kvpb.ImportEngineRequest{Uuid: s.engineUUID, PdAddr: testPDAddr}).
		Return(nil, nil)
	s.mockClient.EXPECT().
		CleanupEngine(s.ctx, &kvpb.CleanupEngineRequest{Uuid: s.engineUUID}).
		Return(nil, nil)

	engine, err := s.engine.Close(s.ctx, nil)
	require.NoError(t, err)
	err = engine.Import(s.ctx, 1)
	require.NoError(t, err)
	err = engine.Cleanup(s.ctx)
	require.NoError(t, err)
}

func BenchmarkMutationAlloc(b *testing.B) {
	var g *kvpb.Mutation
	for i := 0; i < b.N; i++ {
		m := &kvpb.Mutation{
			Op:    kvpb.Mutation_Put,
			Key:   nil,
			Value: nil,
		}
		g = m
	}

	_ = g
}

func BenchmarkMutationPool(b *testing.B) {
	p := sync.Pool{
		New: func() interface{} {
			return &kvpb.Mutation{}
		},
	}
	var g *kvpb.Mutation

	for i := 0; i < b.N; i++ {
		m := p.Get().(*kvpb.Mutation)
		m.Op = kvpb.Mutation_Put
		m.Key = nil
		m.Value = nil

		g = m

		p.Put(m)
	}

	_ = g
}

func TestCheckTiDBVersion(t *testing.T) {
	var version string
	ctx := context.Background()

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/status", req.URL.Path)
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"version": version,
		})
		require.NoError(t, err)
	}))

	tls := common.NewTLSFromMockServer(mockServer)

	version = "5.7.25-TiDB-v4.0.0"
	require.Nil(t, checkTiDBVersionByTLS(ctx, tls, requiredMinTiDBVersion, requiredMaxTiDBVersion))

	version = "5.7.25-TiDB-v9999.0.0"
	err := checkTiDBVersionByTLS(ctx, tls, requiredMinTiDBVersion, requiredMaxTiDBVersion)
	require.Error(t, err)
	require.Regexp(t, "^TiDB version too new", err.Error())

	version = "5.7.25-TiDB-v7.0.0"
	err = checkTiDBVersionByTLS(ctx, tls, requiredMinTiDBVersion, requiredMaxTiDBVersion)
	require.Error(t, err)
	require.Regexp(t, "^TiDB version too new", err.Error())

	version = "5.7.25-TiDB-v7.0.0-beta"
	err = checkTiDBVersionByTLS(ctx, tls, requiredMinTiDBVersion, requiredMaxTiDBVersion)
	require.Error(t, err)
	require.Regexp(t, "^TiDB version too new", err.Error())

	version = "5.7.25-TiDB-v1.0.0"
	err = checkTiDBVersionByTLS(ctx, tls, requiredMinTiDBVersion, requiredMaxTiDBVersion)
	require.Error(t, err)
	require.Regexp(t, "^TiDB version too old", err.Error())
}
