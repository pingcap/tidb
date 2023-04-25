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

package loaddatatest

import (
	"fmt"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type mockGCSSuite struct {
	suite.Suite

	server *fakestorage.Server
	store  kv.Storage
	tk     *testkit.TestKit
}

var (
	gcsHost = "127.0.0.1"
	gcsPort = uint16(4443)
	// for fake gcs server, we must use this endpoint format
	// NOTE: must end with '/'
	gcsEndpointFormat = "http://%s:%d/storage/v1/"
	gcsEndpoint       = fmt.Sprintf(gcsEndpointFormat, gcsHost, gcsPort)
)

func TestLoadRemote(t *testing.T) {
	suite.Run(t, &mockGCSSuite{})
}

func (s *mockGCSSuite) SetupSuite() {
	s.Require().True(*realtikvtest.WithRealTiKV)
	var err error
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       gcsHost,
		Port:       gcsPort,
		PublicHost: gcsHost,
	}
	s.server, err = fakestorage.NewServerWithOptions(opt)
	s.Require().NoError(err)
	s.store = realtikvtest.CreateMockStoreAndSetup(s.T())
	s.tk = testkit.NewTestKit(s.T(), s.store)
}

func (s *mockGCSSuite) TearDownSuite() {
	s.server.Stop()
}

func (s *mockGCSSuite) enableFailpoint(path, term string) {
	require.NoError(s.T(), failpoint.Enable(path, term))
	s.T().Cleanup(func() {
		_ = failpoint.Disable(path)
	})
}

func checkClientErrorMessage(t *testing.T, err error, msg string) {
	require.Error(t, err)
	cause := errors.Cause(err)
	terr, ok := cause.(*errors.Error)
	require.True(t, ok, "%T", cause)
	require.Contains(t, terror.ToSQLError(terr).Error(), msg)
}
