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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseServiceURL(t *testing.T) {
	tests := []struct {
		raw              string
		schemePrefix     string
		address          string
		endpointNoScheme string
		endpointScheme   string
		isUnixFamily     bool
	}{
		{
			raw:              "http://127.0.0.1:2379",
			schemePrefix:     "http://",
			address:          "127.0.0.1:2379",
			endpointNoScheme: "127.0.0.1:2379",
			endpointScheme:   "http://127.0.0.1:2379",
		},
		{
			raw:              "https://127.0.0.1:2379",
			schemePrefix:     "https://",
			address:          "127.0.0.1:2379",
			endpointNoScheme: "127.0.0.1:2379",
			endpointScheme:   "https://127.0.0.1:2379",
		},
		{
			raw:              "unix://localhost:m0",
			schemePrefix:     "unix://",
			address:          "localhost:m0",
			endpointNoScheme: "unix://localhost:m0",
			endpointScheme:   "unix://localhost:m0",
			isUnixFamily:     true,
		},
		{
			raw:              "unix:///tmp/etcd.sock",
			schemePrefix:     "unix://",
			address:          "/tmp/etcd.sock",
			endpointNoScheme: "unix:///tmp/etcd.sock",
			endpointScheme:   "unix:///tmp/etcd.sock",
			isUnixFamily:     true,
		},
	}

	for _, test := range tests {
		parsed, err := ParseServiceURL(test.raw)
		require.NoError(t, err)
		require.Equal(t, test.schemePrefix, parsed.SchemePrefix())
		require.Equal(t, test.address, parsed.Address())
		require.Equal(t, test.endpointNoScheme, parsed.Endpoint(false))
		require.Equal(t, test.endpointScheme, parsed.Endpoint(true))
		require.Equal(t, test.endpointScheme, parsed.String())
		require.Equal(t, test.isUnixFamily, parsed.IsUnixFamily())
	}
}

func TestParseServiceURLRejectsInvalidInput(t *testing.T) {
	for _, raw := range []string{
		"",
		"ftp://127.0.0.1:2379",
		"http://127.0.0.1",
		"http://127.0.0.1:2379/path",
		"unix://",
	} {
		_, err := ParseServiceURL(raw)
		require.Error(t, err, raw)
	}
}

func TestNormalizeServiceURL(t *testing.T) {
	tests := []struct {
		raw           string
		defaultScheme string
		expected      string
	}{
		{raw: "127.0.0.1:2379", defaultScheme: URLSchemeHTTP, expected: "http://127.0.0.1:2379"},
		{raw: "http://127.0.0.1:2379", defaultScheme: URLSchemeHTTP, expected: "http://127.0.0.1:2379"},
		{raw: "https://127.0.0.1:2379", defaultScheme: URLSchemeHTTP, expected: "https://127.0.0.1:2379"},
		{raw: "unix://localhost:m0", defaultScheme: URLSchemeHTTP, expected: "unix://localhost:m0"},
	}

	for _, test := range tests {
		normalized, err := NormalizeServiceURL(test.raw, test.defaultScheme)
		require.NoError(t, err)
		require.Equal(t, test.expected, normalized)
	}

	for _, test := range []struct {
		raw           string
		defaultScheme string
	}{
		{raw: "invalid_pd_address", defaultScheme: URLSchemeHTTP},
		{raw: "127.0.0.1:2379", defaultScheme: "ftp"},
	} {
		_, err := NormalizeServiceURL(test.raw, test.defaultScheme)
		require.Error(t, err)
	}
}
