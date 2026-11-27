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

package dumpservicetest

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

// MetadataService serves one metadata shard and counts discovery and scan requests.
type MetadataService struct {
	Client      *dumpservice.Client
	Address     string
	Discoveries atomic.Int64
	Scans       atomic.Int64
}

// NewMetadataService serves one whole metadata shard in reverse order. Metadata
// consumers share its scan, which also exercises stream buffer ownership.
func NewMetadataService(t *testing.T, source kv.Snapshot, completionError string) *MetadataService {
	t.Helper()
	service := &MetadataService{}
	socketPath := Start(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		start, end, err := DecodeRange(request)
		if err != nil || string(start) != "m" || string(end) != "n" {
			http.Error(writer, "expected the whole metadata shard [m, n)", http.StatusBadRequest)
			return
		}
		if request.URL.Path == "/shards" {
			service.Discoveries.Add(1)
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(ShardsResponse{Ranges: []HexRange{{
				StartKeyHex: hex.EncodeToString(start), EndKeyHex: hex.EncodeToString(end),
			}}}); err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		if request.URL.Path != "/data" {
			http.NotFound(writer, request)
			return
		}
		service.Scans.Add(1)
		iter, err := source.IterReverse(kv.Key(end), kv.Key(start))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		defer iter.Close()
		writer.Header().Add("Trailer", "x-dumper-scan-status")
		writer.Header().Add("Trailer", "x-dumper-scan-error")
		for iter.Valid() {
			if err := WriteRow(writer, iter.Key(), iter.Value()); err != nil {
				return
			}
			if err := iter.Next(); err != nil {
				writer.Header().Set("x-dumper-scan-status", "failed")
				writer.Header().Set("x-dumper-scan-error", url.QueryEscape(err.Error()))
				return
			}
		}
		if completionError != "" {
			writer.Header().Set("x-dumper-scan-status", "failed")
			writer.Header().Set("x-dumper-scan-error", url.QueryEscape(completionError))
			return
		}
		writer.Header().Set("x-dumper-scan-status", "complete")
	}))
	service.Address = (&url.URL{Scheme: "unix", Path: socketPath}).String()
	client, err := dumpservice.NewClient(service.Address)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	service.Client = client
	return service
}
