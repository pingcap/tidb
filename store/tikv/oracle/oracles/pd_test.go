// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package oracles

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type testPdOracleSuite struct {
	o oracle.Oracle
}

var _ = Suite(&testPdOracleSuite{})

func (s *testPdOracleSuite) SetUpTest(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(cluster, []byte("a"), []byte("b"), []byte("c"))
	pdCli := mocktikv.NewPDClient(cluster)
	var err error
	s.o, err = NewPdOracle(pdCli, time.Second*10)
	c.Assert(err, IsNil)
}

func (s *testPdOracleSuite) TearDownSuite(c *C) {
	if s.o != nil {
		s.o.Close()
	}
}

func (s *testPdOracleSuite) TestIllegalOracle(c *C) {
	ts, err := s.o.GetTimestamp()
	c.Assert(err, IsNil)
	ts, err = s.o.GetTimestamp()
	c.Assert(err, IsNil)
	// set last ts to a big one
	s.o.(*pdOracle).setLastTS(ts * 2)
	_, err = s.o.GetTimestamp()
	c.Assert(err, NotNil)
	_, err = s.o.GetTimestamp()
	c.Assert(err, NotNil)
}
