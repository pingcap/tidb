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

// Package dumpservicetest provides shared fixtures for Dumpling source and export tests.
package dumpservicetest

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

// Start serves a test handler over HTTP/2 on a temporary Unix socket.
func Start(t *testing.T, handler http.Handler) string {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "kv-store.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	serverDone := make(chan struct{})
	acceptedConn := make(chan net.Conn, 1)
	go func() {
		defer close(serverDone)
		connection, err := listener.Accept()
		acceptedConn <- connection
		if err != nil {
			return
		}
		defer connection.Close()
		server := &http2.Server{}
		server.ServeConn(connection, &http2.ServeConnOpts{
			Handler: handler,
		})
	}()

	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		// Closing the listener unblocks Accept, including when no client connected.
		if connection := <-acceptedConn; connection != nil {
			// ServeConn may have already closed the connection.
			_ = connection.Close()
		}
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			require.Fail(t, "test dump service did not stop")
		}
	})
	return socketPath
}

// ServeRequest serves logical keys from the test store.
func ServeRequest(store kv.Storage, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "unsupported kv test request", http.StatusNotFound)
		return
	}
	switch request.URL.Path {
	case "/shards":
		serveShards(store, writer, request)
	case "/data":
		serveScan(store, writer, request)
	default:
		http.Error(writer, "unsupported kv test request", http.StatusNotFound)
	}
}

// DecodeRange reads the hexadecimal range boundaries from a service request.
func DecodeRange(request *http.Request) ([]byte, []byte, error) {
	query := request.URL.Query()
	startKey, err := hex.DecodeString(query.Get("start_key_hex"))
	if err != nil {
		return nil, nil, err
	}
	endKey, err := hex.DecodeString(query.Get("end_key_hex"))
	if err != nil {
		return nil, nil, err
	}
	return startKey, endKey, nil
}

func serveShards(store kv.Storage, writer http.ResponseWriter, request *http.Request) {
	startKey, endKey, err := DecodeRange(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	ranges, err := shardRange(store, startKey, endKey)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	response := ShardsResponse{Ranges: make([]HexRange, 0, len(ranges))}
	for _, rangeToScan := range ranges {
		response.Ranges = append(response.Ranges, HexRange{
			StartKeyHex: hex.EncodeToString(rangeToScan.Start),
			EndKeyHex:   hex.EncodeToString(rangeToScan.End),
		})
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func serveScan(store kv.Storage, writer http.ResponseWriter, request *http.Request) {
	startKey, endKey, err := DecodeRange(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	writer.Header().Add("Trailer", "x-dumper-scan-status")
	writer.Header().Add("Trailer", "x-dumper-scan-error")
	if err := writeRange(store, writer, startKey, endKey); err != nil {
		writer.Header().Set("x-dumper-scan-status", "failed")
		writer.Header().Set("x-dumper-scan-error", url.QueryEscape(err.Error()))
		return
	}
	writer.Header().Set("x-dumper-scan-status", "complete")
}

func shardRange(store kv.Storage, startKey, endKey []byte) ([]dumpservice.Range, error) {
	txn, err := store.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = txn.Rollback() }()
	iterator, err := txn.Iter(kv.Key(startKey), kv.Key(endKey))
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	ranges := make([]dumpservice.Range, 0, 1)
	nextStart := append([]byte(nil), startKey...)
	first := true
	for iterator.Valid() {
		if first {
			first = false
		} else {
			nextEnd := append([]byte(nil), iterator.Key()...)
			ranges = append(ranges, dumpservice.Range{Start: nextStart, End: nextEnd})
			nextStart = nextEnd
		}
		if err := iterator.Next(); err != nil {
			return nil, err
		}
	}
	return append(ranges, dumpservice.Range{Start: nextStart, End: append([]byte(nil), endKey...)}), nil
}

func writeRange(store kv.Storage, output io.Writer, startKey, endKey []byte) error {
	txn, err := store.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	iterator, err := txn.Iter(kv.Key(startKey), kv.Key(endKey))
	if err != nil {
		return err
	}
	defer iterator.Close()
	for iterator.Valid() {
		if err := WriteRow(output, iterator.Key(), iterator.Value()); err != nil {
			return err
		}
		if err := iterator.Next(); err != nil {
			return err
		}
	}
	return nil
}

// WriteRow writes one key/value pair in the dump-service wire format.
func WriteRow(output io.Writer, key, value []byte) error {
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:], uint32(len(value)))
	for _, data := range [][]byte{header[:], key, value} {
		written, err := output.Write(data)
		if err != nil {
			return err
		}
		if written != len(data) {
			return io.ErrShortWrite
		}
	}
	return nil
}

// HexRange is the JSON representation of a service shard.
type HexRange struct {
	StartKeyHex string `json:"start_key_hex"`
	EndKeyHex   string `json:"end_key_hex"`
}

// ShardsResponse is the JSON response to a shard discovery request.
type ShardsResponse struct {
	Ranges []HexRange `json:"ranges"`
}
