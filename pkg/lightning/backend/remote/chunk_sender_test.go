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

package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

type receivedReq struct {
	clusterID uint64
	taskID    string
	writerID  uint64
	chunkID   uint64
	chunkData []byte
	flush     bool
}

type mockHTTPHandler struct {
	ch              chan receivedReq
	receivedChunkID uint64
	flushedChunkID  uint64
	mu              sync.Mutex
}

func (h *mockHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()

	query := r.URL.Query()
	taskID := query.Get("task_id")

	clusterID, err := strconv.ParseInt(query.Get("cluster_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writerID, err := strconv.ParseInt(query.Get("writer_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if r.Method == http.MethodPut {
		chunkID, err := strconv.ParseInt(query.Get("chunk_id"), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if uint64(chunkID) != h.receivedChunkID+1 {
			result := PutChunkResponse{
				FlushedChunkID: h.flushedChunkID,
				HandledChunkID: h.receivedChunkID,
			}
			bytes, err := json.Marshal(result)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, err = w.Write(bytes)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else {
			// Read the request body
			chunkData, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Send the chunk to the channel
			h.ch <- receivedReq{
				clusterID: uint64(clusterID),
				taskID:    taskID,
				writerID:  uint64(writerID),
				chunkID:   uint64(chunkID),
				chunkData: chunkData,
				flush:     false,
			}

			if len(chunkData) != 0 {
				// Update the received chunk id if the chunk is not empty
				h.receivedChunkID = uint64(chunkID)
			}
			result := PutChunkResponse{
				FlushedChunkID: h.flushedChunkID,
				HandledChunkID: h.receivedChunkID,
			}
			bytes, err := json.Marshal(result)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			_, err = w.Write(bytes)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	} else if r.Method == http.MethodPost {
		h.flushedChunkID = h.receivedChunkID
		h.ch <- receivedReq{
			clusterID: uint64(clusterID),
			taskID:    taskID,
			writerID:  uint64(writerID),
			chunkID:   0,
			chunkData: nil,
			flush:     true,
		}

		result := FlushResponse{
			FlushedChunkID: h.flushedChunkID,
		}
		bytes, err := json.Marshal(result)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err = w.Write(bytes)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func genMockEngine(ctx context.Context, clusterID uint64, loadDataTaskID, addr string) *engine {
	return &engine{
		logger:         log.L(),
		loadDataTaskID: loadDataTaskID,
		addr:           fmt.Sprintf("http://%s", addr),
		clusterID:      clusterID,

		httpClient: &http.Client{},
	}
}

func genHTTPServer(ch chan receivedReq) (*http.Server, *mockHTTPHandler, string, error) {
	handler := &mockHTTPHandler{
		ch: ch,
		mu: sync.Mutex{},
	}

	// get a random free port
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, nil, "", err
	}
	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, nil, "", err
	}
	defer l.Close()

	addr := l.Addr().String()
	server := &http.Server{
		Handler: handler,
		Addr:    l.Addr().String(),
	}

	return server, handler, addr, nil
}

func TestChunkSender(t *testing.T) {
	ctx := context.Background()
	loadDataTaskID := "test-task"
	writerID := uint64(1)
	clusterID := uint64(1)

	ch := make(chan receivedReq, 16)
	server, handler, addr, err := genHTTPServer(ch)
	require.NoError(t, err)
	go func() {
		server.ListenAndServe()
	}()

	engine := genMockEngine(ctx, clusterID, loadDataTaskID, addr)
	chunksStore, err := newChunkStore(loadDataTaskID, writerID, "")
	require.NoError(t, err)

	sender := newChunkSender(ctx, 1, engine, chunksStore)
	require.NotNil(t, sender)

	// Test putChunk normally
	chunkID, err := sender.putChunk(ctx, []byte("chunk1"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), chunkID)
	verfiyRequest(t, ch, clusterID, loadDataTaskID, writerID, chunkID, []byte("chunk1"), false)

	// Test flush
	err = sender.flush(ctx)
	require.NoError(t, err)
	verfiyRequest(t, ch, clusterID, loadDataTaskID, writerID, 0, nil, true)

	// Test putChunk after remote worker restart
	chunkID, err = sender.putChunk(ctx, []byte("chunk2"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), chunkID)
	verfiyRequest(t, ch, clusterID, loadDataTaskID, writerID, chunkID, []byte("chunk2"), false)

	// restart the remote worker
	handler.mu.Lock()
	handler.receivedChunkID = 1
	handler.mu.Unlock()
	// chunkSender should retput `chunk2`
	err = sender.flush(ctx)
	require.NoError(t, err)
	// received flush request
	verfiyRequest(t, ch, clusterID, loadDataTaskID, writerID, 0, nil, true)
	// received putting a empty chunk request
	verfiyRequest(t, ch, clusterID, loadDataTaskID, writerID, chunkID, []byte{}, false)
	// received putting `chunk2` request again
	verfiyRequest(t, ch, clusterID, loadDataTaskID, writerID, chunkID, []byte("chunk2"), false)
	// received flush request
	verfiyRequest(t, ch, clusterID, loadDataTaskID, writerID, 0, nil, true)

	server.Shutdown(ctx)
}

func verfiyRequest(t *testing.T, ch chan receivedReq, clusterID uint64, taskID string, writerID, chunkID uint64, chunkData []byte, flush bool) {
	select {
	case req := <-ch:
		require.Equal(t, clusterID, req.clusterID)
		require.Equal(t, taskID, req.taskID)
		require.Equal(t, writerID, req.writerID)
		require.Equal(t, chunkID, req.chunkID)
		require.Equal(t, chunkData, req.chunkData)
		require.Equal(t, flush, req.flush)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}
