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

package export

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	tf "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

func TestDumpServiceLifecycle(t *testing.T) {
	socketPath := startTestDumpService(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/data" {
			http.Error(writer, "unexpected request", http.StatusBadRequest)
			return
		}
		// An empty metadata scan represents a snapshot with no databases.
		writer.Header().Add("Trailer", "x-dumper-scan-status")
		writer.WriteHeader(http.StatusOK)
		writer.Header().Set("x-dumper-scan-status", "complete")
	}))
	conf := DefaultConfig()
	conf.OutputDirPath = t.TempDir()
	conf.StatusAddr = ""
	conf.DumpService = (&url.URL{Scheme: "unix", Path: socketPath}).String()
	dumper, err := NewDumper(context.Background(), conf)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dumper.Close()) })
	require.NoError(t, dumper.Dump())
	require.Nil(t, dumper.serviceClient.Load())
	info, err := os.Stat(socketPath)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSocket)
	connection, err := net.Dial("unix", socketPath)
	require.NoError(t, err)
	require.NoError(t, connection.Close())
}

func TestDumpServiceFromTiDBStorage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table kv_int (
		id bigint primary key clustered,
		name varchar(16) not null,
		note varchar(16),
		payload varbinary(8),
		amount decimal(10,2),
		created datetime(3),
		flags bit(4),
		status enum('new', 'done'),
		labels set('a', 'b'),
		name_len int as (length(name)),
		id_twice bigint as (id * 2) stored
	)`)
	tk.MustExec("insert into kv_int (id, name, note, payload, amount, created, flags, status, labels) values (1, 'alpha', null, x'00ff', -12.30, '2026-07-16 01:02:03.456', b'1010', 'done', 'a,b')")
	tk.MustExec("insert into kv_int (id, name, note, payload, amount, created, flags, status, labels) values (2, 'beta', '', x'', 0, '2020-01-02 03:04:05.000', b'0001', 'new', '')")
	tk.MustExec("alter table kv_int add column added int not null default 7, add column later_nullable varchar(8)")

	tk.MustExec("create table kv_common (tenant varchar(8), id int, value varchar(16), primary key (tenant, id) clustered)")
	tk.MustExec("insert into kv_common values ('acme', 9, 'common')")
	tk.MustExec("create table kv_partition (id int primary key, value varchar(16)) partition by range (id) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("insert into kv_partition values (1, 'first'), (11, 'second')")

	socketPath := startTestDumpService(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		serveKVTestRequest(store, writer, request)
	}))
	outputDir := t.TempDir()
	config := DefaultConfig()
	config.OutputDirPath = outputDir
	config.StatusAddr = ""
	config.DumpService = (&url.URL{Scheme: "unix", Path: socketPath}).String()
	config.FileType = FileFormatCSVString
	config.FileSize = 1
	config.NoHeader = true
	config.CsvOutputDialect = CSVDialectSnowflake
	config.TableFilter = tf.NewSchemasFilter("test")
	dumper, err := NewDumper(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dumper.Close())
	})
	require.NoError(t, dumper.Dump())
	info, err := os.Stat(socketPath)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSocket)
	// The service listener is still owned by the caller after export.
	connection, err := net.Dial("unix", socketPath)
	require.NoError(t, err)
	require.NoError(t, connection.Close())

	expectedFiles := map[string]string{
		"test.kv_int.0000000000000.csv":       `1,"alpha",\N,"00ff",-12.30,"2026-07-16 01:02:03.456","0a","done","a,b",7,\N` + "\r\n",
		"test.kv_int.0000000010000.csv":       `2,"beta","","",0.00,"2020-01-02 03:04:05.000","01","new","",7,\N` + "\r\n",
		"test.kv_common.0000000000000.csv":    `"acme",9,"common"` + "\r\n",
		"test.kv_partition.0000000000000.csv": `1,"first"` + "\r\n",
		"test.kv_partition.0000000010000.csv": `11,"second"` + "\r\n",
	}
	for name, expected := range expectedFiles {
		content, err := os.ReadFile(filepath.Join(outputDir, name))
		require.NoError(t, err, name)
		require.Equal(t, expected, string(content), name)
	}
	schemaFiles := []string{
		"test-schema-create.sql",
		"test.kv_int-schema.sql",
		"test.kv_common-schema.sql",
		"test.kv_partition-schema.sql",
	}
	for _, name := range schemaFiles {
		_, err := os.Stat(filepath.Join(outputDir, name))
		require.NoError(t, err, name)
	}
	entries, err := os.ReadDir(outputDir)
	require.NoError(t, err)
	actualFiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		actualFiles = append(actualFiles, entry.Name())
	}
	expectedNames := append(schemaFiles,
		"test.kv_int.0000000000000.csv",
		"test.kv_int.0000000010000.csv",
	)
	expectedNames = append(expectedNames,
		"test.kv_common.0000000000000.csv",
		"test.kv_partition.0000000000000.csv",
		"test.kv_partition.0000000010000.csv",
	)
	require.ElementsMatch(t, expectedNames, actualFiles)
}

func startTestDumpService(t *testing.T, handler http.Handler) string {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "kv-store.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		connection, err := listener.Accept()
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
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			require.Fail(t, "test dump service did not stop")
		}
	})
	return socketPath
}

func serveKVTestRequest(store kv.Storage, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "unsupported kv test request", http.StatusNotFound)
		return
	}
	switch request.URL.Path {
	case "/shards":
		serveKVTestShards(store, writer, request)
	case "/data":
		serveKVTestScan(store, writer, request)
	default:
		http.Error(writer, "unsupported kv test request", http.StatusNotFound)
	}
}

func decodeKVTestRange(request *http.Request) ([]byte, []byte, error) {
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

func serveKVTestShards(store kv.Storage, writer http.ResponseWriter, request *http.Request) {
	startKey, endKey, err := decodeKVTestRange(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	ranges, err := shardKVTestRange(store, startKey, endKey)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	response := testShardsResponse{Ranges: make([]testHexRange, 0, len(ranges))}
	for _, rangeToScan := range ranges {
		response.Ranges = append(response.Ranges, testHexRange{
			StartKeyHex: hex.EncodeToString(rangeToScan.Start),
			EndKeyHex:   hex.EncodeToString(rangeToScan.End),
		})
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func serveKVTestScan(store kv.Storage, writer http.ResponseWriter, request *http.Request) {
	startKey, endKey, err := decodeKVTestRange(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	writer.Header().Add("Trailer", "x-dumper-scan-status")
	writer.Header().Add("Trailer", "x-dumper-scan-error")
	if err := writeKVTestRange(store, writer, startKey, endKey); err != nil {
		writer.Header().Set("x-dumper-scan-status", "failed")
		writer.Header().Set("x-dumper-scan-error", url.QueryEscape(err.Error()))
		return
	}
	writer.Header().Set("x-dumper-scan-status", "complete")
}

func shardKVTestRange(store kv.Storage, startKey, endKey []byte) ([]dumpservice.Range, error) {
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

func writeKVTestRange(store kv.Storage, output io.Writer, startKey, endKey []byte) error {
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
		if err := writeKVTestRow(output, iterator.Key(), iterator.Value()); err != nil {
			return err
		}
		if err := iterator.Next(); err != nil {
			return err
		}
	}
	return nil
}

func writeKVTestRow(output io.Writer, key, value []byte) error {
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

type testHexRange struct {
	StartKeyHex string `json:"start_key_hex"`
	EndKeyHex   string `json:"end_key_hex"`
}

type testShardsResponse struct {
	Ranges []testHexRange `json:"ranges"`
}

func TestDumpServiceMetrics(t *testing.T) {
	socketPath := startTestDumpService(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/metrics" {
			http.Error(writer, "unexpected request", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(writer, "dump_service_scanned_ranges_total 3\n")
	}))
	client, err := dumpservice.NewClient((&url.URL{Scheme: "unix", Path: socketPath}).String())
	require.NoError(t, err)
	t.Cleanup(client.Close)
	recorder := httptest.NewRecorder()
	owner := &Dumper{}
	owner.serviceClient.Store(client)
	newMetricsHandler(owner).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "dump_service_scanned_ranges_total 3\n")
	owner.serviceClient.Store(nil)
	recorder = httptest.NewRecorder()
	newMetricsHandler(owner).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotContains(t, recorder.Body.String(), "dump_service_scanned_ranges_total")
}
