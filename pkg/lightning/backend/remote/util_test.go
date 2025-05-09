// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"net/http"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEstimateEngineDataSize(t *testing.T) {
	logger := log.Logger{Logger: zap.NewNop()}

	t.Run("nil inputs return 0", func(t *testing.T) {
		size := EstimateEngineDataSize(nil, nil, false, logger)
		require.Equal(t, int64(0), size)
	})

	t.Run("empty table returns 0", func(t *testing.T) {
		tblMeta := &mydump.MDTableMeta{
			DataFiles: nil,
		}
		tblInfo := &checkpoints.TidbTableInfo{
			Core: &model.TableInfo{},
		}
		size := EstimateEngineDataSize(tblMeta, tblInfo, false, logger)
		require.Equal(t, int64(0), size)
	})

	t.Run("non-index engine returns correct size", func(t *testing.T) {
		tblMeta := &mydump.MDTableMeta{
			DataFiles: []mydump.FileInfo{
				{FileMeta: mydump.SourceFileMeta{RealSize: 100}},
				{FileMeta: mydump.SourceFileMeta{RealSize: 200}},
			},
		}
		tblInfo := &checkpoints.TidbTableInfo{
			Core: &model.TableInfo{},
		}
		size := EstimateEngineDataSize(tblMeta, tblInfo, false, logger)
		require.Equal(t, int64(300), size)
	})

	t.Run("index engine with no indices returns 0", func(t *testing.T) {
		tblMeta := &mydump.MDTableMeta{
			DataFiles: []mydump.FileInfo{
				{FileMeta: mydump.SourceFileMeta{RealSize: 100}},
			},
		}
		tblInfo := &checkpoints.TidbTableInfo{
			Core: &model.TableInfo{},
		}
		size := EstimateEngineDataSize(tblMeta, tblInfo, true, logger)
		require.Equal(t, int64(0), size)
	})

	t.Run("index engine with common handle returns 0", func(t *testing.T) {
		tblMeta := &mydump.MDTableMeta{
			DataFiles: []mydump.FileInfo{
				{FileMeta: mydump.SourceFileMeta{RealSize: 100}},
			},
		}
		tblInfo := &checkpoints.TidbTableInfo{
			Core: &model.TableInfo{
				IsCommonHandle: true,
				Indices:        []*model.IndexInfo{{ID: 1}},
			},
		}
		size := EstimateEngineDataSize(tblMeta, tblInfo, true, logger)
		require.Equal(t, int64(0), size)
	})

	t.Run("index ratio multiplier", func(t *testing.T) {
		tblMeta := &mydump.MDTableMeta{
			DataFiles: []mydump.FileInfo{
				{FileMeta: mydump.SourceFileMeta{RealSize: 100}},
			},
			IndexRatio: 2.0,
		}
		tblInfo := &checkpoints.TidbTableInfo{
			Core: &model.TableInfo{
				Indices: []*model.IndexInfo{{ID: 1}, {ID: 2}},
			},
		}
		size := EstimateEngineDataSize(tblMeta, tblInfo, true, logger)
		require.Equal(t, int64(200), size)
	})
}

func TestRecoverFromEngineCp(t *testing.T) {
	t.Run("invalid status returns false", func(t *testing.T) {
		cp := &checkpoints.EngineCheckpoint{
			Status: checkpoints.CheckpointStatusMaxInvalid,
		}
		require.False(t, HasRecoverableEngineProgress(cp))
	})

	t.Run("imported status returns false", func(t *testing.T) {
		cp := &checkpoints.EngineCheckpoint{
			Status: checkpoints.CheckpointStatusImported,
		}
		require.False(t, HasRecoverableEngineProgress(cp))
	})

	t.Run("no finished chunks returns false", func(t *testing.T) {
		cp := &checkpoints.EngineCheckpoint{
			Status: checkpoints.CheckpointStatusAllWritten,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Chunk: mydump.Chunk{},
				},
			},
		}
		require.False(t, HasRecoverableEngineProgress(cp))
	})

	t.Run("has finished chunks returns true", func(t *testing.T) {
		cp := &checkpoints.EngineCheckpoint{
			Status: checkpoints.CheckpointStatusAllWritten,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Chunk: mydump.Chunk{
						Offset: 10,
					},
					Key: checkpoints.ChunkCheckpointKey{
						Offset: 0,
					},
				},
			},
		}
		require.True(t, HasRecoverableEngineProgress(cp))
	})
}

func TestRetryableHTTPStatusCode(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		want       bool
	}{
		{"503 is retryable", http.StatusServiceUnavailable, true},
		{"500 is retryable", http.StatusInternalServerError, true},
		{"408 is retryable", http.StatusRequestTimeout, true},
		{"404 is not retryable", http.StatusNotFound, false},
		{"200 is not retryable", http.StatusOK, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetryableHTTPStatusCode(tt.statusCode)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseRemoteWorkerURL(t *testing.T) {
	tests := []struct {
		name      string
		location  string
		enableTLS bool
		want      string
	}{
		{
			name:      "normal http url",
			location:  "http://worker:8000/load_data",
			enableTLS: false,
			want:      "http://worker:8000",
		},
		{
			name:      "convert https to http when TLS disabled",
			location:  "https://worker:8000/load_data",
			enableTLS: false,
			want:      "http://worker:8000",
		},
		{
			name:      "keep https when TLS enabled",
			location:  "https://worker:8000/load_data",
			enableTLS: true,
			want:      "https://worker:8000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				Header: http.Header{
					"Location": []string{tt.location},
				},
			}
			got := parseRemoteWorkerURL(resp, tt.enableTLS)
			require.Equal(t, tt.want, got)
		})
	}
}
