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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
)

type securitySuite struct{}

var _ = Suite(&securitySuite{})

func respondPathHandler(w http.ResponseWriter, req *http.Request) {
	_, _ = io.WriteString(w, `{"path":"`)
	_, _ = io.WriteString(w, req.URL.Path)
	_, _ = io.WriteString(w, `"}`)
}

func (s *securitySuite) TestGetJSONInsecure(c *C) {
	mockServer := httptest.NewServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	ctx := context.Background()
	u, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)

	tls, err := common.NewTLS("", "", "", u.Host)
	c.Assert(err, IsNil)

	var result struct{ Path string }
	err = tls.GetJSON(ctx, "/aaa", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/aaa")
	err = tls.GetJSON(ctx, "/bbbb", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/bbbb")
}

func (s *securitySuite) TestGetJSONSecure(c *C) {
	mockServer := httptest.NewTLSServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	ctx := context.Background()
	tls := common.NewTLSFromMockServer(mockServer)

	var result struct{ Path string }
	err := tls.GetJSON(ctx, "/ccc", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/ccc")
	err = tls.GetJSON(ctx, "/dddd", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/dddd")
}

func (s *securitySuite) TestInvalidTLS(c *C) {
	tempDir := c.MkDir()

	caPath := filepath.Join(tempDir, "ca.pem")
	_, err := common.NewTLS(caPath, "", "", "localhost")
	c.Assert(err, ErrorMatches, "could not read ca certificate:.*")

	err = os.WriteFile(caPath, []byte("invalid ca content"), 0o644)
	c.Assert(err, IsNil)
	_, err = common.NewTLS(caPath, "", "", "localhost")
	c.Assert(err, ErrorMatches, "failed to append ca certs")

	certPath := filepath.Join(tempDir, "test.pem")
	keyPath := filepath.Join(tempDir, "test.key")
	_, err = common.NewTLS(caPath, certPath, keyPath, "localhost")
	c.Assert(err, ErrorMatches, "could not load client key pair: open.*")

	err = os.WriteFile(certPath, []byte("invalid cert content"), 0o644)
	c.Assert(err, IsNil)
	err = os.WriteFile(keyPath, []byte("invalid key content"), 0o600)
	c.Assert(err, IsNil)
	_, err = common.NewTLS(caPath, certPath, keyPath, "localhost")
	c.Assert(err, ErrorMatches, "could not load client key pair: tls.*")
}
