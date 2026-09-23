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

package dumpservice

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

func TestDumpServiceURL(t *testing.T) {
	for _, address := range []string{
		"", "/tmp/dumper.sock", "unix://", "unix:///", "unix:relative.sock",
		"unix:/tmp/dumper.sock", "unix://localhost/tmp/dumper.sock",
		"unix://user@localhost/tmp/dumper.sock", "http://localhost/dumper.sock",
		"tcp://localhost:1234", "unix:///tmp/dumper.sock?", "unix:///tmp/dumper.sock?x=1",
		"unix:///tmp/dumper.sock#", "unix:///tmp/dumper.sock#fragment",
		"unix:///tmp/%00.sock", "unix:///tmp/%zz.sock",
	} {
		_, err := ParseSocketURL(address)
		require.Error(t, err, address)
		_, err = NewClient(address)
		require.Error(t, err, address)
	}
	for address, expected := range map[string]string{
		"unix:///tmp/dumper.sock":            "/tmp/dumper.sock",
		"unix:///tmp/dump%20service%23.sock": "/tmp/dump service#.sock",
	} {
		actual, err := ParseSocketURL(address)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		client, err := NewClient(address)
		require.NoError(t, err)
		client.Close()
	}
}

func TestDumpServiceClientErrors(t *testing.T) {
	client := newClient(filepath.Join(t.TempDir(), "missing.sock"))
	t.Cleanup(client.Close)
	_, err := client.Shards(context.Background(), []byte{1}, []byte{2})
	require.ErrorContains(t, err, "request dump service shards")
	_, err = client.Scan(context.Background(), []byte{1}, []byte{2})
	require.ErrorContains(t, err, "request dump service scan")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.Shards(ctx, []byte{1}, []byte{2})
	require.ErrorIs(t, err, context.Canceled)
	_, err = client.Scan(ctx, []byte{1}, []byte{2})
	require.ErrorIs(t, err, context.Canceled)
}

func TestKVProtocolRows(t *testing.T) {
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
			errorText: "invalid kv row with empty key",
		},
		{
			name:      "truncated key size",
			encoded:   []byte{2, 0},
			errorText: "read kv row key size: unexpected EOF",
		},
		{
			name:      "truncated key",
			encoded:   []byte{2, 0, 0, 0, 0, 0, 0, 0, 'k'},
			errorText: "read kv row key: unexpected EOF",
		},
		{
			name:      "truncated value",
			encoded:   []byte{1, 0, 0, 0, 2, 0, 0, 0, 'k', 'v'},
			errorText: "read kv row value: unexpected EOF",
		},
	}
	for _, testCase := range testCases {
		key, value, end, err := readKVRow(bytes.NewReader(testCase.encoded), make([]byte, 0, 8), make([]byte, 0, 8))
		if testCase.errorText != "" {
			require.EqualError(t, err, testCase.errorText, testCase.name)
			continue
		}
		require.NoError(t, err, testCase.name)
		require.Equal(t, testCase.key, key, testCase.name)
		require.Equal(t, testCase.value, value, testCase.name)
		require.Equal(t, testCase.end, end, testCase.name)
	}

	var requestMethod, requestPath, contentType, startKeyHex, endKeyHex string
	socketPath := filepath.Join(t.TempDir(), "kv.sock")
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
				requestPath = request.URL.Path
				contentType = request.Header.Get("Content-Type")
				startKeyHex = request.URL.Query().Get("start_key_hex")
				endKeyHex = request.URL.Query().Get("end_key_hex")
				writer.Header().Add("Trailer", "x-dumper-scan-status")
				_, _ = writer.Write([]byte{1, 0, 0, 0, 1, 0, 0, 0, 'k', 'v'})
				writer.Header().Set("x-dumper-scan-status", "complete")
			})})
		}
		close(serverDone)
	}()
	client := newClient(socketPath)
	scan, err := client.Scan(context.Background(), []byte{0, 0xff}, []byte{0x10})
	require.NoError(t, err)
	require.Equal(t, http.MethodGet, requestMethod)
	require.Equal(t, "/data", requestPath)
	require.Empty(t, contentType)
	require.Equal(t, "00ff", startKeyHex)
	require.Equal(t, "10", endKeyHex)
	key, value, end, err := scan.ReadRow(nil, nil)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, []byte{'k'}, key)
	require.Equal(t, []byte{'v'}, value)
	_, _, end, err = scan.ReadRow(nil, nil)
	require.NoError(t, err)
	require.True(t, end)
	require.NoError(t, scan.Close())
	client.Close()
	require.NoError(t, listener.Close())
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		require.Fail(t, "HTTP/2 test server did not stop")
	}

	client.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "/shards", request.URL.Path)
		require.Equal(t, "00ff", request.URL.Query().Get("start_key_hex"))
		require.Equal(t, "10", request.URL.Query().Get("end_key_hex"))
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(
				`{"ranges":[{"start_key_hex":"00ff","end_key_hex":"01"},{"start_key_hex":"01","end_key_hex":"10"}]}`,
			)),
		}, nil
	})
	ranges, err := client.Shards(context.Background(), []byte{0, 0xff}, []byte{0x10})
	require.NoError(t, err)
	require.Equal(t, []Range{
		{Start: []byte{0, 0xff}, End: []byte{1}},
		{Start: []byte{1}, End: []byte{0x10}},
	}, ranges)

	invalidShards := []shardsResponse{
		{},
		{Ranges: []hexRange{{StartKeyHex: "zz", EndKeyHex: "10"}}},
		{Ranges: []hexRange{{StartKeyHex: "00ff", EndKeyHex: "00ff"}}},
		{Ranges: []hexRange{{StartKeyHex: "01", EndKeyHex: "10"}}},
		{Ranges: []hexRange{
			{StartKeyHex: "00ff", EndKeyHex: "02"},
			{StartKeyHex: "01", EndKeyHex: "10"},
		}},
		{Ranges: []hexRange{{StartKeyHex: "00ff", EndKeyHex: "11"}}},
		{Ranges: []hexRange{{StartKeyHex: "00ff", EndKeyHex: "01"}}},
	}
	for _, sampled := range invalidShards {
		_, err := decodeShardRanges([]byte{0, 0xff}, []byte{0x10}, sampled.Ranges)
		require.Error(t, err)
	}

	client.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Trailer: http.Header{
				http.CanonicalHeaderKey("x-dumper-scan-status"): []string{"failed"},
				http.CanonicalHeaderKey("x-dumper-scan-error"):  []string{"missing%20kv%20file"},
			},
		}, nil
	})
	scan, err = client.Scan(context.Background(), []byte{1}, []byte{2})
	require.NoError(t, err)
	_, _, _, err = scan.ReadRow(nil, nil)
	require.EqualError(t, err, "dump service scan failed: missing kv file")

	client.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Trailer:    make(http.Header),
		}, nil
	})
	scan, err = client.Scan(context.Background(), []byte{1}, []byte{2})
	require.NoError(t, err)
	_, _, _, err = scan.ReadRow(nil, nil)
	require.EqualError(t, err, "dump service scan ended without a completion trailer")
	client.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, metricsURL, request.URL.String())
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/plain; version=0.0.4"}},
			Body:       io.NopCloser(strings.NewReader("dump_service_scanned_ranges_total 3\n")),
		}, nil
	})
	families, err := client.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Equal(t, "dump_service_scanned_ranges_total", families[0].GetName())
	require.Equal(t, float64(3), families[0].GetMetric()[0].GetUntyped().GetValue())
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
