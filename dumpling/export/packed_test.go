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
	"bytes"
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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	tf "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

func TestPackedProtocolRows(t *testing.T) {
	testCases := []struct {
		name      string
		encoded   []byte
		key       []byte
		value     []byte
		end       bool
		errorText string
	}{
		{
			name:    "binary row",
			encoded: []byte{2, 0, 0, 0, 2, 0, 0, 0, 'k', 0, 'v', 0xff},
			key:     []byte{'k', 0},
			value:   []byte{'v', 0xff},
		},
		{
			name: "clean stream EOF",
			end:  true,
		},
		{
			name:      "empty key",
			encoded:   []byte{0, 0, 0, 0, 0, 0, 0, 0},
			errorText: "invalid packed row with empty key",
		},
		{
			name:      "truncated key size",
			encoded:   []byte{2, 0},
			errorText: "read packed row key size: unexpected EOF",
		},
		{
			name:      "truncated key",
			encoded:   []byte{2, 0, 0, 0, 0, 0, 0, 0, 'k'},
			errorText: "read packed row key: unexpected EOF",
		},
		{
			name:      "truncated value",
			encoded:   []byte{1, 0, 0, 0, 2, 0, 0, 0, 'k', 'v'},
			errorText: "read packed row value: unexpected EOF",
		},
	}
	for _, testCase := range testCases {
		key, value, end, err := readPackedRow(bytes.NewReader(testCase.encoded), make([]byte, 0, 8), make([]byte, 0, 8))
		if testCase.errorText != "" {
			require.EqualError(t, err, testCase.errorText, testCase.name)
			continue
		}
		require.NoError(t, err, testCase.name)
		require.Equal(t, testCase.key, key, testCase.name)
		require.Equal(t, testCase.value, value, testCase.name)
		require.Equal(t, testCase.end, end, testCase.name)
	}

	baseArgs := []string{
		"dumper",
		"--metadata-url", "bucket/backup.meta",
		"--unix-socket", "/tmp/packed.sock",
		"--scan-concurrency", "7",
	}
	require.Equal(
		t,
		baseArgs,
		cseDumperArgs("bucket/backup.meta", "/tmp/packed.sock", false, 7),
	)
	legacyArgs := append([]string{}, baseArgs...)
	legacyArgs = append(legacyArgs, "--legacy-encryption")
	require.Equal(
		t,
		legacyArgs,
		cseDumperArgs("bucket/backup.meta", "/tmp/packed.sock", true, 7),
	)

	var requestBody cseDumperScanRequest
	var requestMethod, requestURL, contentType string
	socketPath := filepath.Join(t.TempDir(), "packed.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	serverDone := make(chan struct{})
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			defer connection.Close()
			server := &http2.Server{}
			server.ServeConn(connection, &http2.ServeConnOpts{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requestMethod = request.Method
				requestURL = request.URL.String()
				contentType = request.Header.Get("Content-Type")
				if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
					http.Error(writer, err.Error(), http.StatusBadRequest)
					return
				}
				writer.Header().Add("Trailer", cseScanStatusTrailer)
				_, _ = writer.Write([]byte{1, 0, 0, 0, 1, 0, 0, 0, 'k', 'v'})
				writer.Header().Set(cseScanStatusTrailer, cseScanStatusComplete)
			})})
		}
		close(serverDone)
	}()
	client := newCSEDumperClient(socketPath)
	scan, err := client.scan(context.Background(), []byte{0, 0xff}, []byte{0x10})
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, requestMethod)
	require.Equal(t, "/scan", requestURL)
	require.Equal(t, "application/json", contentType)
	require.Equal(t, cseDumperScanRequest{StartKeyHex: "00ff", EndKeyHex: "10"}, requestBody)
	key, value, end, err := scan.readRow(nil, nil)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, []byte{'k'}, key)
	require.Equal(t, []byte{'v'}, value)
	_, _, end, err = scan.readRow(nil, nil)
	require.NoError(t, err)
	require.True(t, end)
	require.NoError(t, scan.close())
	client.close()
	require.NoError(t, listener.Close())
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		require.Fail(t, "HTTP/2 test server did not stop")
	}

	client.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Trailer: http.Header{
				http.CanonicalHeaderKey(cseScanStatusTrailer): []string{cseScanStatusFailed},
				http.CanonicalHeaderKey(cseScanErrorTrailer):  []string{"missing%20packed%20file"},
			},
		}, nil
	})
	scan, err = client.scan(context.Background(), []byte{1}, []byte{2})
	require.NoError(t, err)
	_, _, _, err = scan.readRow(nil, nil)
	require.EqualError(t, err, "cse-ctl dumper scan failed: missing packed file")

	client.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Trailer:    make(http.Header),
		}, nil
	})
	scan, err = client.scan(context.Background(), []byte{1}, []byte{2})
	require.NoError(t, err)
	_, _, _, err = scan.readRow(nil, nil)
	require.EqualError(t, err, "cse-ctl dumper scan ended without a completion trailer")
	diagnostics := readCSEDumperDiagnostics(strings.NewReader(strings.Repeat("x", maxCSEDiagnosticBytes+1)))
	require.Equal(t, strings.Repeat("x", maxCSEDiagnosticBytes)+"\ncse-ctl stderr truncated", diagnostics)

	exited := make(chan struct{})
	close(exited)
	client.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.Canceled
	})
	dumper := &cseDumper{
		process: &cseDumperProcess{done: exited, waitErr: context.Canceled, diagnostics: "test exit"},
		client:  client,
	}
	_, err = dumper.scan(context.Background(), []byte{1}, []byte{2})
	require.EqualError(t, err, "cse-ctl dumper exited while trying to serve scan: context canceled; stderr: test exit")

	client.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, cseMetricsURL, request.URL.String())
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/plain; version=0.0.4"}},
			Body:       io.NopCloser(strings.NewReader("native_br_packed_reader_scanned_shards_total 3\n")),
		}, nil
	})
	recorder := httptest.NewRecorder()
	owner := &Dumper{}
	owner.packedService.Store(client)
	newMetricsHandler(owner).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "native_br_packed_reader_scanned_shards_total 3\n")
	owner.packedService.Store(nil)
	recorder = httptest.NewRecorder()
	newMetricsHandler(owner).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotContains(t, recorder.Body.String(), "native_br_packed_reader_scanned_shards_total")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDumpPackedFromTiDBStorage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table packed_int (
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
	tk.MustExec("insert into packed_int (id, name, note, payload, amount, created, flags, status, labels) values (1, 'alpha', null, x'00ff', -12.30, '2026-07-16 01:02:03.456', b'1010', 'done', 'a,b')")
	tk.MustExec("insert into packed_int (id, name, note, payload, amount, created, flags, status, labels) values (2, 'beta', '', x'', 0, '2020-01-02 03:04:05.000', b'0001', 'new', '')")
	tk.MustExec("alter table packed_int add column added int not null default 7, add column later_nullable varchar(8)")

	tk.MustExec("create table packed_common (tenant varchar(8), id int, value varchar(16), primary key (tenant, id) clustered)")
	tk.MustExec("insert into packed_common values ('acme', 9, 'common')")
	tk.MustExec("create table packed_partition (id int primary key, value varchar(16)) partition by range (id) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("insert into packed_partition values (1, 'first'), (11, 'second')")

	scanner := newPackedTestCSEClient(t, store)
	outputDir := t.TempDir()
	config := DefaultConfig()
	config.OutputDirPath = outputDir
	config.StatusAddr = ""
	config.PackedBackup = "packed-test"
	config.FileType = FileFormatCSVString
	config.NoHeader = true
	config.CsvOutputDialect = CSVDialectSnowflake
	config.TableFilter = tf.NewSchemasFilter("test")
	dumper, err := NewDumper(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dumper.Close())
	})
	require.NoError(t, dumper.dumpPackedFrom(scanner))

	expectedFiles := map[string]string{
		"test.packed_int.000000000.csv": "" +
			`1,"alpha",\N,"00ff",-12.30,"2026-07-16 01:02:03.456","0a","done","a,b",7,\N` + "\r\n" +
			`2,"beta","","",0.00,"2020-01-02 03:04:05.000","01","new","",7,\N` + "\r\n",
		"test.packed_common.000000000.csv":    `"acme",9,"common"` + "\r\n",
		"test.packed_partition.000000000.csv": `1,"first"` + "\r\n" + `11,"second"` + "\r\n",
	}
	for name, expected := range expectedFiles {
		content, err := os.ReadFile(filepath.Join(outputDir, name))
		require.NoError(t, err, name)
		require.Equal(t, expected, string(content), name)
	}
	schemaFiles := []string{
		"test-schema-create.sql",
		"test.packed_int-schema.sql",
		"test.packed_common-schema.sql",
		"test.packed_partition-schema.sql",
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
	expectedNames := append(schemaFiles, "test.packed_int.000000000.csv")
	expectedNames = append(expectedNames, "test.packed_common.000000000.csv", "test.packed_partition.000000000.csv")
	require.ElementsMatch(t, expectedNames, actualFiles)
}

func newPackedTestCSEClient(t *testing.T, store kv.Storage) *cseDumperClient {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "packed-store.sock")
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
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				servePackedTestScan(store, writer, request)
			}),
		})
	}()

	client := newCSEDumperClient(socketPath)
	t.Cleanup(func() {
		client.close()
		require.NoError(t, listener.Close())
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			require.Fail(t, "packed test CSE server did not stop")
		}
	})
	return client
}

func servePackedTestScan(store kv.Storage, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/scan" {
		http.Error(writer, "unsupported packed test request", http.StatusNotFound)
		return
	}
	var scanRequest cseDumperScanRequest
	if err := json.NewDecoder(request.Body).Decode(&scanRequest); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	startKey, err := hex.DecodeString(scanRequest.StartKeyHex)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	endKey, err := hex.DecodeString(scanRequest.EndKeyHex)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	writer.Header().Add("Trailer", cseScanStatusTrailer)
	writer.Header().Add("Trailer", cseScanErrorTrailer)
	if err := writePackedTestRange(store, writer, startKey, endKey); err != nil {
		writer.Header().Set(cseScanStatusTrailer, cseScanStatusFailed)
		writer.Header().Set(cseScanErrorTrailer, url.QueryEscape(err.Error()))
		return
	}
	writer.Header().Set(cseScanStatusTrailer, cseScanStatusComplete)
}

func writePackedTestRange(store kv.Storage, output io.Writer, startKey, endKey []byte) error {
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
		if err := writePackedTestRow(output, iterator.Key(), iterator.Value()); err != nil {
			return err
		}
		if err := iterator.Next(); err != nil {
			return err
		}
	}
	return nil
}

func writePackedTestRow(output io.Writer, key, value []byte) error {
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
