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

package importinto

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	backendkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// recordWriter wraps an objectio.Writer and can inject a custom close error.
//
// The delegated Close always executes so the underlying writer's resources
// are released; the injected error is returned instead if set.
type recordWriter struct {
	objectio.Writer
	injectErr error
}

func (w *recordWriter) Close(ctx context.Context) error {
	err := w.Writer.Close(ctx)
	if w.injectErr != nil {
		return w.injectErr
	}
	return err
}

// recordStorage wraps a storeapi.Storage and wraps every created Writer in a
// recordWriter that can inject a custom close error.
type recordStorage struct {
	storeapi.Storage
	injectErr error
}

func (s *recordStorage) Create(ctx context.Context, name string, option *storeapi.WriterOption) (objectio.Writer, error) {
	writer, err := s.Storage.Create(ctx, name, option)
	if err != nil {
		return nil, err
	}
	return &recordWriter{Writer: writer, injectErr: s.injectErr}, nil
}

func newTestDataWriter(ctx context.Context, t *testing.T, store storeapi.Storage, closeCb func(*simplesst.WriterSummary)) *simplesst.EngineWriter {
	t.Helper()
	b := simplesst.NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2)
	if closeCb != nil {
		b = b.SetOnCloseFunc(closeCb)
	}
	w := simplesst.NewEngineWriter(b.Build(store, "/test", "data"))
	require.NoError(t, w.AppendRows(ctx, nil, backendkv.MakeRowsFromKvPairs([]common.KvPair{{
		Key: []byte("data-key"),
		Val: []byte("data-value"),
	}})))
	return w
}

func newTestIndexWriter(ctx context.Context, t *testing.T, store storeapi.Storage, closeCb func(*simplesst.WriterSummary)) *importer.IndexRouteWriter {
	t.Helper()
	b := simplesst.NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2)
	if closeCb != nil {
		b = b.SetOnCloseFunc(closeCb)
	}
	w := importer.NewIndexRouteWriter(zap.NewNop(), func(indexID int64) (*simplesst.Writer, error) {
		return b.Build(store, "/test", "index"), nil
	})
	require.NoError(t, w.AppendRows(ctx, nil, backendkv.GroupedPairs{
		1: []common.KvPair{{
			Key: []byte("index-key"),
			Val: []byte("index-value"),
		}},
	}))
	return w
}

func newTestWorker(ctx context.Context, dataWriter *simplesst.EngineWriter, indexWriter *importer.IndexRouteWriter) *chunkWorker {
	return &chunkWorker{
		ctx:         ctx,
		dataWriter:  dataWriter,
		indexWriter: indexWriter,
	}
}

// Original bug: dataWriter.Close() failure must not skip indexWriter.Close().
func TestChunkWorkerCloseIndexWriterAfterDataCloseError(t *testing.T) {
	ctx := context.Background()
	rootErr := errors.New("ai-native data writer close failed")

	var (
		dataCloseCount int32
		indexCloseCnt  int32
	)
	dataStore := &recordStorage{Storage: objstore.NewMemStorage(), injectErr: rootErr}
	indexStore := objstore.NewMemStorage()

	dataWriter := newTestDataWriter(ctx, t, dataStore, func(*simplesst.WriterSummary) { dataCloseCount++ })
	indexWriter := newTestIndexWriter(ctx, t, indexStore, func(*simplesst.WriterSummary) { indexCloseCnt++ })

	worker := newTestWorker(ctx, dataWriter, indexWriter)
	err := worker.Close()
	require.ErrorIs(t, err, rootErr)
	require.Equal(t, int32(1), indexCloseCnt, "index writer must still close when data writer Close fails")
	// data.OnCloseFunc does NOT fire when close fails (expected SST behavior).
	// The error propagation proves dataWriter.Close() was attempted.
	require.Zero(t, dataCloseCount, "data writer OnCloseFunc skipped on close error (expected)")
}

// Both writers close successfully.
func TestChunkWorkerCloseBothSucceed(t *testing.T) {
	ctx := context.Background()
	var dataCloseCount, indexCloseCount int32

	dataWriter := newTestDataWriter(ctx, t, objstore.NewMemStorage(), func(*simplesst.WriterSummary) { dataCloseCount++ })
	indexWriter := newTestIndexWriter(ctx, t, objstore.NewMemStorage(), func(*simplesst.WriterSummary) { indexCloseCount++ })

	worker := newTestWorker(ctx, dataWriter, indexWriter)
	err := worker.Close()
	require.NoError(t, err)
	require.Equal(t, int32(1), dataCloseCount)
	require.Equal(t, int32(1), indexCloseCount)
}

// Only indexWriter fails — dataWriter should still close.
func TestChunkWorkerCloseIndexWriterFails(t *testing.T) {
	ctx := context.Background()
	indexErr := errors.New("index close failed")

	var dataCloseCount int32
	dataWriter := newTestDataWriter(ctx, t, objstore.NewMemStorage(), func(*simplesst.WriterSummary) { dataCloseCount++ })
	indexWriter := newTestIndexWriter(ctx, t, &recordStorage{Storage: objstore.NewMemStorage(), injectErr: indexErr}, nil)

	worker := newTestWorker(ctx, dataWriter, indexWriter)
	err := worker.Close()
	require.ErrorIs(t, err, indexErr)
	require.Equal(t, int32(1), dataCloseCount, "data writer must close even when index writer fails")
}

// Both fail — firstErr must capture data writer's error (first in execution order).
func TestChunkWorkerCloseBothFail(t *testing.T) {
	ctx := context.Background()
	dataErr := errors.New("data close failed")
	indexErr := errors.New("index close failed")

	dataWriter := newTestDataWriter(ctx, t, &recordStorage{Storage: objstore.NewMemStorage(), injectErr: dataErr}, nil)
	indexWriter := newTestIndexWriter(ctx, t, &recordStorage{Storage: objstore.NewMemStorage(), injectErr: indexErr}, nil)

	worker := newTestWorker(ctx, dataWriter, indexWriter)
	err := worker.Close()
	require.ErrorIs(t, err, dataErr, "firstErr must preserve the data writer error")
}

// dataWriter is nil — only indexWriter is closed.
func TestChunkWorkerCloseDataWriterNil(t *testing.T) {
	ctx := context.Background()
	var indexCloseCount int32

	indexWriter := newTestIndexWriter(ctx, t, objstore.NewMemStorage(), func(*simplesst.WriterSummary) { indexCloseCount++ })

	worker := newTestWorker(ctx, nil, indexWriter)
	err := worker.Close()
	require.NoError(t, err)
	require.Equal(t, int32(1), indexCloseCount)
}

// indexWriter is nil — only dataWriter is closed.
func TestChunkWorkerCloseIndexWriterNil(t *testing.T) {
	ctx := context.Background()
	var dataCloseCount int32

	dataWriter := newTestDataWriter(ctx, t, objstore.NewMemStorage(), func(*simplesst.WriterSummary) { dataCloseCount++ })

	worker := newTestWorker(ctx, dataWriter, nil)
	err := worker.Close()
	require.NoError(t, err)
	require.Equal(t, int32(1), dataCloseCount)
}

// Both writers are nil — Close is a no-op.
func TestChunkWorkerCloseBothNil(t *testing.T) {
	err := newTestWorker(context.Background(), nil, nil).Close()
	require.NoError(t, err)
}

// Context canceled — Close falls back to a new context; both writers close.
//
// The fallback path is proven by the successful Close: if the canceled context
// were forwarded, the underlying SST writer would fail. A successful Close
// means the fallback context (context.Background + 30s timeout) was used.
func TestChunkWorkerCloseContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var dataCloseCount, indexCloseCount int32
	dataWriter := newTestDataWriter(context.Background(), t, objstore.NewMemStorage(), func(*simplesst.WriterSummary) { dataCloseCount++ })
	indexWriter := newTestIndexWriter(context.Background(), t, objstore.NewMemStorage(), func(*simplesst.WriterSummary) { indexCloseCount++ })

	worker := newTestWorker(ctx, dataWriter, indexWriter)
	err := worker.Close()
	require.NoError(t, err)
	require.Equal(t, int32(1), dataCloseCount, "data writer must close even with canceled context")
	require.Equal(t, int32(1), indexCloseCount, "index writer must close even with canceled context")
}
