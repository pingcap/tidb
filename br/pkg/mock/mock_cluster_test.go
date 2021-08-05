// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package mock_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClusterSuite{})

type testClusterSuite struct {
	mock *mock.Cluster
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testClusterSuite) TestSmoke(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	s.mock.Stop()
}
