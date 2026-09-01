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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
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
	require.Equal(
		t,
		append(baseArgs, "--legacy-encryption"),
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
	client, transport := newCSEDumperHTTPClient(socketPath)
	dumper := &cseDumper{client: client, transport: transport}
	scan, err := dumper.scan(context.Background(), []byte{0, 0xff}, []byte{0x10}, nil)
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
	transport.CloseIdleConnections()
	require.NoError(t, listener.Close())
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		require.Fail(t, "HTTP/2 test server did not stop")
	}

	dumper.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Trailer: http.Header{
				http.CanonicalHeaderKey(cseScanStatusTrailer): []string{cseScanStatusFailed},
				http.CanonicalHeaderKey(cseScanErrorTrailer):  []string{"missing%20packed%20file"},
			},
		}, nil
	})
	scan, err = dumper.scan(context.Background(), []byte{1}, []byte{2}, nil)
	require.NoError(t, err)
	_, _, _, err = scan.readRow(nil, nil)
	require.EqualError(t, err, "cse-ctl dumper scan failed: missing packed file")

	dumper.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Trailer:    make(http.Header),
		}, nil
	})
	scan, err = dumper.scan(context.Background(), []byte{1}, []byte{2}, nil)
	require.NoError(t, err)
	_, _, _, err = scan.readRow(nil, nil)
	require.EqualError(t, err, "cse-ctl dumper scan ended without a completion trailer")
	diagnostics := readCSEDumperStderr(strings.NewReader(
		"CSE packed perf part=setup manifest=1ms\n"+
			strings.Repeat("x", maxCSEDumperDiagnosticBytes+1)+"\nlast diagnostic",
	), nil)
	require.Equal(t, "1 cse-ctl diagnostic lines omitted\nlast diagnostic", diagnostics)

	dumper.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
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
	owner.packedService.Store(dumper)
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

func TestPackedRowsUseTiDBStorageEncoding(t *testing.T) {
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

	txn, err := store.Begin()
	require.NoError(t, err)
	databases, err := loadPackedDatabases(context.Background(), func(
		_ context.Context,
		startKey, endKey []byte,
		emit func(key, value []byte) error,
	) error {
		iterator, err := txn.Iter(kv.Key(startKey), kv.Key(endKey))
		if err != nil {
			return err
		}
		defer iterator.Close()
		for iterator.Valid() {
			if err := emit(iterator.Key(), iterator.Value()); err != nil {
				return err
			}
			if err := iterator.Next(); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, txn.Rollback())
	var database *model.DBInfo
	for _, candidate := range databases {
		if candidate.Name.L == "test" {
			database = candidate
			break
		}
	}
	require.NotNil(t, database)

	initColumnTypeSets()
	testCases := []struct {
		table string
		rows  []string
	}{
		{
			table: "packed_int",
			rows: []string{
				`1,"alpha",\N,"00ff",-12.30,"2026-07-16 01:02:03.456","0a","done","a,b",7,\N`,
				`2,"beta","","",0.00,"2020-01-02 03:04:05.000","01","new","",7,\N`,
			},
		},
		{
			table: "packed_common",
			rows:  []string{`"acme",9,"common"`},
		},
		{
			table: "packed_partition",
			rows:  []string{`1,"first"`, `11,"second"`},
		},
	}
	for _, testCase := range testCases {
		var table *model.TableInfo
		for _, candidate := range database.Deprecated.Tables {
			if candidate.Name.L == testCase.table {
				table = candidate
				break
			}
		}
		require.NotNil(t, table, testCase.table)
		rows := readPackedTestRows(t, store, table)
		require.Equal(t, testCase.rows, rows, testCase.table)
	}
}

func readPackedTestRows(t *testing.T, store kv.Storage, table *model.TableInfo) []string {
	t.Helper()
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Rollback())
	}()
	meta := newPackedTableMeta("test", table, "")
	option := &csvOption{
		nullValue:      "\\N",
		separator:      []byte(","),
		delimiter:      []byte(`"`),
		lineTerminator: []byte("\n"),
		binaryFormat:   BinaryFormatHEX,
	}
	rows := make([]string, 0)
	for _, tableID := range packedPhysicalTableIDs(table) {
		prefix := tablecodec.GenTableRecordPrefix(tableID)
		iterator, err := txn.Iter(prefix, prefix.PrefixNext())
		require.NoError(t, err)
		for iterator.Valid() {
			row := MakeRowReceiver(meta.ColumnTypes())
			packed := &packedRowIter{
				table:  table,
				key:    append([]byte(nil), iterator.Key()...),
				value:  append([]byte(nil), iterator.Value()...),
				args:   make([]any, meta.ColumnCount()),
				hasRow: true,
			}
			require.NoError(t, packed.Decode(row))
			var output bytes.Buffer
			row.WriteToBufferInCsv(&output, true, option)
			rows = append(rows, output.String())
			require.NoError(t, iterator.Next())
		}
		iterator.Close()
	}
	return rows
}
