// Copyright 2023 PingCAP, Inc.
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

package loadremotetest

import (
	"fmt"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/suite"
)

type mockGCSSuite struct {
	suite.Suite

	server *fakestorage.Server
	store  kv.Storage
	tk     *testkit.TestKit
}

var (
	gcsHost     = "127.0.0.1"
	gcsPort     = uint16(4443)
	gcsEndpoint = fmt.Sprintf("http://%s:%d", gcsHost, gcsPort)
)

func TestLoadRemote(t *testing.T) {
	suite.Run(t, &mockGCSSuite{})
}

func (s *mockGCSSuite) SetupSuite() {
	objects := []fakestorage.Object{
		{
			BucketName: "test-bucket",
			Name:       "no-new-line-at-end.csv",
			Content: []byte(`i,s
100,"test100"
101,"\""
102,"😄😄😄😄😄"
104,""`),
		},
		{
			BucketName: "test-bucket",
			Name:       "new-line-at-end.csv",
			Content: []byte(`i,s
100,"test100"
101,"\""
102,"😄😄😄😄😄"
104,""
`),
		},
	}

	var err error
	opt := fakestorage.Options{
		InitialObjects: objects,
		Scheme:         "http",
		Host:           gcsHost,
		Port:           gcsPort,
		PublicHost:     gcsHost,
	}
	s.server, err = fakestorage.NewServerWithOptions(opt)
	s.Require().NoError(err)
	s.store = testkit.CreateMockStore(s.T())
	s.tk = testkit.NewTestKit(s.T(), s.store)
	executor.InTest = true
}

func (s *mockGCSSuite) TearDownSuite() {
	s.server.Stop()
	executor.InTest = false
}
