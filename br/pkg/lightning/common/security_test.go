// Copyright 2020 PingCAP, Inc.
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

package common_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func respondPathHandler(w http.ResponseWriter, req *http.Request) {
	_, _ = io.WriteString(w, `{"path":"`)
	_, _ = io.WriteString(w, req.URL.Path)
	_, _ = io.WriteString(w, `"}`)
}

func TestGetJSONInsecure(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	ctx := context.Background()
	u, err := url.Parse(mockServer.URL)
	require.NoError(t, err)

	tls, err := common.NewTLS("", "", "", u.Host)
	require.NoError(t, err)

	var result struct{ Path string }
	err = tls.GetJSON(ctx, "/aaa", &result)
	require.NoError(t, err)
	require.Equal(t, "/aaa", result.Path)
	err = tls.GetJSON(ctx, "/bbbb", &result)
	require.NoError(t, err)
	require.Equal(t, "/bbbb", result.Path)
}

func TestGetJSONSecure(t *testing.T) {
	mockServer := httptest.NewTLSServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	ctx := context.Background()
	tls := common.NewTLSFromMockServer(mockServer)

	var result struct{ Path string }
	err := tls.GetJSON(ctx, "/ccc", &result)
	require.NoError(t, err)
	require.Equal(t, "/ccc", result.Path)
	err = tls.GetJSON(ctx, "/dddd", &result)
	require.NoError(t, err)
	require.Equal(t, "/dddd", result.Path)
}

func TestInvalidTLS(t *testing.T) {
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.pem")
	_, err := common.NewTLS(caPath, "", "", "localhost")
	require.Regexp(t, "could not read ca certificate:.*", err.Error())

	err = os.WriteFile(caPath, []byte("invalid ca content"), 0o644)
	require.NoError(t, err)
	_, err = common.NewTLS(caPath, "", "", "localhost")
	require.Regexp(t, "failed to append ca certs", err.Error())

	certPath := filepath.Join(tempDir, "test.pem")
	keyPath := filepath.Join(tempDir, "test.key")
	_, err = common.NewTLS(caPath, certPath, keyPath, "localhost")
	require.Regexp(t, "could not load client key pair: open.*", err.Error())

	err = os.WriteFile(certPath, []byte("invalid cert content"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(keyPath, []byte("invalid key content"), 0o600)
	require.NoError(t, err)
	_, err = common.NewTLS(caPath, certPath, keyPath, "localhost")
	require.Regexp(t, "could not load client key pair: tls.*", err.Error())
}
