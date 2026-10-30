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
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/net/http2"
)

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
		"metadata",
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

func TestDumpServiceLifecycle(t *testing.T) {
	cases := []struct {
		name            string
		failingEndpoint string
		cancel          bool
	}{
		{name: "success"},
		{name: "data failure", failingEndpoint: "/data"},
		{name: "shard discovery failure", failingEndpoint: "/shards"},
		{name: "cancellation", cancel: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("create database lifecycle")
			tk.MustExec("create table lifecycle.t (id int primary key)")
			tk.MustExec("insert into lifecycle.t values (1), (2)")
			socket := startTestDumpService(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				startKey, _, err := decodeKVTestRange(request)
				if err != nil {
					http.Error(writer, err.Error(), http.StatusBadRequest)
					return
				}
				// Fail table data requests after metadata has loaded successfully.
				if len(startKey) > 0 && startKey[0] == 't' && request.URL.Path == testCase.failingEndpoint {
					http.Error(writer, "source unavailable", http.StatusServiceUnavailable)
					return
				}
				serveKVTestRequest(store, writer, request)
			}))
			conf := DefaultConfig()
			conf.DumpService = (&url.URL{Scheme: "unix", Path: socket}).String()
			conf.StatusAddr = ""
			conf.OutputDirPath = t.TempDir()
			conf.TableFilter = tf.NewSchemasFilter("lifecycle")
			conf.FileType = FileFormatCSVString
			core, logs := observer.New(zap.InfoLevel)
			conf.Logger = zap.New(core)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dumper, err := NewDumper(ctx, conf)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dumper.Close()) })
			if testCase.cancel {
				cancel()
			}
			err = dumper.Dump()
			if testCase.failingEndpoint != "" || testCase.cancel {
				if testCase.cancel {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorContains(t, err, "source unavailable")
				}
				require.Len(t, logs.FilterMessage("backup failed summary").All(), 1)
				_, err := os.Stat(filepath.Join(conf.OutputDirPath, "metadata"))
				require.ErrorIs(t, err, os.ErrNotExist)
				return
			}
			require.NoError(t, err)
			status := dumper.GetStatus()
			require.EqualValues(t, 1, status.TotalTables)
			require.EqualValues(t, 1, status.CompletedTables)
			require.EqualValues(t, 2, status.FinishedRows)
			require.Positive(t, status.FinishedBytes)
			require.NotNil(t, status.ProgressPercent)
			require.EqualValues(t, 100, *status.ProgressPercent)
			require.Equal(t, "100 %", status.Progress)
			require.EqualValues(t, 2, dumper.metrics.totalChunks.Load())
			require.EqualValues(t, 2, dumper.metrics.completedChunks.Load())
			require.Len(t, logs.FilterMessage("backup success summary").All(), 1)
			metadata, err := os.ReadFile(filepath.Join(conf.OutputDirPath, "metadata"))
			require.NoError(t, err)
			require.Contains(t, string(metadata), "Started dump at: ")
			require.Contains(t, string(metadata), "Finished dump at: ")
			require.NotContains(t, string(metadata), "SHOW MASTER STATUS")
		})
	}
}

func TestDumpServiceColumnFilterValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database column_filter")
	tk.MustExec("create table column_filter.z_parent (id int primary key clustered, keep int)")
	tk.MustExec("create table column_filter.a_child (id int primary key, parent_id int, foreign key (parent_id) references z_parent(id))")
	tk.MustExec("create table column_filter.partitioned (id int, keep int) partition by range (id) (partition p0 values less than maxvalue)")
	tk.MustExec("insert into column_filter.z_parent values (1,7)")
	tk.MustExec("insert into column_filter.a_child values (2,1)")

	for _, testCase := range []struct {
		name      string
		tables    []string
		columns   []string
		noSchemas bool
		noData    bool
		err       string
	}{
		{name: "empty projection", tables: []string{"column_filter.z_parent"}, columns: []string{"missing"}, err: "selects no writable columns"},
		{name: "empty data projection", tables: []string{"column_filter.z_parent"}, columns: []string{"missing"}, noSchemas: true, err: "selects no writable columns"},
		{name: "partition dependency", tables: []string{"column_filter.partitioned"}, columns: []string{"keep"}, err: "partition definition references a removed column"},
		{name: "foreign key parent", tables: []string{"column_filter.a_child", "column_filter.z_parent"}, columns: []string{"keep"}, err: "foreign key references removed column"},
		{name: "data only", tables: []string{"column_filter.a_child", "column_filter.z_parent"}, columns: []string{"keep"}, noSchemas: true},
		{name: "schema only", tables: []string{"column_filter.z_parent"}, columns: []string{"keep"}, noData: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			socket := startTestDumpService(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if testCase.noData {
					start, _, err := decodeKVTestRange(request)
					if err != nil {
						http.Error(writer, err.Error(), http.StatusBadRequest)
						return
					}
					if len(start) > 0 && start[0] == 't' {
						t.Error("schema-only export requested table records")
						http.Error(writer, "unexpected table request", http.StatusBadRequest)
						return
					}
				}
				serveKVTestRequest(store, writer, request)
			}))
			conf := DefaultConfig()
			conf.DumpService = (&url.URL{Scheme: "unix", Path: socket}).String()
			conf.OutputDirPath = t.TempDir()
			conf.StatusAddr = ""
			conf.FileType = FileFormatCSVString
			conf.NoSchemas = testCase.noSchemas
			conf.NoData = testCase.noData
			var err error
			conf.TableFilter, err = tf.Parse(testCase.tables)
			require.NoError(t, err)
			conf.columnFilter = newColumnFilterConfigForTest(t, columnFilterRule{
				Matcher: []string{"column_filter.z_parent", "column_filter.partitioned"}, Columns: testCase.columns,
			})
			dumper, err := NewDumper(context.Background(), conf)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dumper.Close()) })
			err = dumper.Dump()
			if testCase.err != "" {
				require.ErrorContains(t, err, testCase.err)
				entries, err := os.ReadDir(conf.OutputDirPath)
				require.NoError(t, err)
				require.Empty(t, entries, "invalid projection must fail before files are written")
				return
			}
			require.NoError(t, err)
			if testCase.noSchemas {
				content, err := os.ReadFile(filepath.Join(conf.OutputDirPath, "column_filter.z_parent.000000000.csv"))
				require.NoError(t, err)
				require.Equal(t, "\"keep\"\r\n7\r\n", string(content))
				_, err = os.Stat(filepath.Join(conf.OutputDirPath, "column_filter.z_parent-schema.sql"))
				require.ErrorIs(t, err, os.ErrNotExist)
				return
			}
			content, err := os.ReadFile(filepath.Join(conf.OutputDirPath, "column_filter.z_parent-schema.sql"))
			require.NoError(t, err)
			require.Contains(t, string(content), "`keep`")
			require.NotContains(t, string(content), "`id`")
			entries, err := os.ReadDir(conf.OutputDirPath)
			require.NoError(t, err)
			require.Len(t, entries, 3, "schema-only export writes database/table schemas and metadata")
		})
	}
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
