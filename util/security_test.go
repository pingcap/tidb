// Copyright 2022 PingCAP, Inc.
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

package util_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func respondPathHandler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, `{"path":"`)
	io.WriteString(w, req.URL.Path)
	io.WriteString(w, `"}`)
}

func TestGetJSONInsecure(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	u, err := url.Parse(mockServer.URL)
	require.NoError(t, err)

	tls, err := util.NewTLS("", "", "", u.Host, nil)
	require.NoError(t, err)

	var result struct{ Path string }
	err = tls.GetJSON("/aaa", &result)
	require.NoError(t, err)
	require.Equal(t, "/aaa", result.Path)
	err = tls.GetJSON("/bbbb", &result)
	require.NoError(t, err)
	require.Equal(t, "/bbbb", result.Path)
}

func TestGetJSONSecure(t *testing.T) {
	mockServer := httptest.NewTLSServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	tls := util.NewTLSFromMockServer(mockServer)

	var result struct{ Path string }
	err := tls.GetJSON("/ccc", &result)
	require.NoError(t, err)
	require.Equal(t, "/ccc", result.Path)
	err = tls.GetJSON("/dddd", &result)
	require.NoError(t, err)
	require.Equal(t, "/dddd", result.Path)
}

func TestInvalidTLS(t *testing.T) {
	tempDir := t.TempDir()

	caPath := filepath.Join(tempDir, "ca.pem")
	_, err := util.NewTLS(caPath, "", "", "localhost", nil)
	require.Regexp(t, "could not read ca certificate:.*", err.Error())

	err = ioutil.WriteFile(caPath, []byte("invalid ca content"), 0644)
	require.NoError(t, err)
	_, err = util.NewTLS(caPath, "", "", "localhost", nil)
	require.Regexp(t, "failed to append ca certs", err.Error())

	certPath := filepath.Join(tempDir, "test.pem")
	keyPath := filepath.Join(tempDir, "test.key")
	_, err = util.NewTLS(caPath, certPath, keyPath, "localhost", nil)
	require.Regexp(t, "could not load client key pair: open.*", err.Error())

	err = ioutil.WriteFile(certPath, []byte("invalid cert content"), 0644)
	require.NoError(t, err)
	err = ioutil.WriteFile(keyPath, []byte("invalid key content"), 0600)
	require.NoError(t, err)
	_, err = util.NewTLS(caPath, certPath, keyPath, "localhost", nil)
	require.Regexp(t, "could not load client key pair: tls.*", err.Error())
}

func TestCheckCN(t *testing.T) {
	dir, err := os.Getwd()
	require.NoError(t, err)

	dir = path.Join(dir, "tls_test")
	caPath, certPath, keyPath := getTestCertFile(dir, "server")
	// only allow client1 to visit
	serverTLS, err := util.ToTLSConfigWithVerify(caPath, certPath, keyPath, []string{"client1"})
	require.NoError(t, err)

	caPath1, certPath1, keyPath1 := getTestCertFile(dir, "client1")
	caData, certData, keyData := loadTLSContent(t, caPath1, certPath1, keyPath1)
	clientTLS1, err := util.ToTLSConfigWithVerifyByRawbytes(caData, certData, keyData, []string{})
	require.NoError(t, err)

	_, err = util.ToTLSConfigWithVerifyByRawbytes(caData, []byte{}, []byte{}, []string{})
	require.NoError(t, err)

	caPath2, certPath2, keyPath2 := getTestCertFile(dir, "client2")
	clientTLS2, err := util.ToTLSConfigWithVerify(caPath2, certPath2, keyPath2, nil)
	require.NoError(t, err)

	port := 9292
	url := fmt.Sprintf("https://127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	server := runServer(ctx, serverTLS, port, t)
	defer func() {
		cancel()
		server.Close()
	}()

	resp, err := util.ClientWithTLS(clientTLS1).Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "This an example server", string(body))

	// client2 can't visit server
	resp, err = util.ClientWithTLS(clientTLS2).Get(url)
	require.Regexp(t, ".*tls: bad certificate", err.Error())
	if resp != nil {
		err = resp.Body.Close()
		require.NoError(t, err)
	}
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("This an example server"))
}

func runServer(ctx context.Context, tlsCfg *tls.Config, port int, t *testing.T) *http.Server {
	http.HandleFunc("/", handler)
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: nil}

	conn, err := net.Listen("tcp", server.Addr)
	if err != nil {
		require.NoError(t, err)
	}

	tlsListener := tls.NewListener(conn, tlsCfg)
	go server.Serve(tlsListener)
	return server
}

func getTestCertFile(dir, role string) (string, string, string) {
	return path.Join(dir, "ca.pem"), path.Join(dir, fmt.Sprintf("%s.pem", role)), path.Join(dir, fmt.Sprintf("%s.key", role))
}

func loadTLSContent(t *testing.T, caPath, certPath, keyPath string) (caData, certData, keyData []byte) {
	// NOTE we make sure the file exists,so we don't need to check the error
	var err error
	caData, err = ioutil.ReadFile(caPath)
	require.NoError(t, err)

	certData, err = ioutil.ReadFile(certPath)
	require.NoError(t, err)

	keyData, err = ioutil.ReadFile(keyPath)
	require.NoError(t, err)
	return
}
