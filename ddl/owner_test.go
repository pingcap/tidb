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

package ddl

import (
	"testing"

	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testOwnerSuite{})

type testOwnerSuite struct{}

func (s *testOwnerSuite) SetUpSuite(c *C) {
	NewOwnerChange = true
}

func (s *testOwnerSuite) TearDownSuite(c *C) {
	NewOwnerChange = false
}

func (s *testOwnerSuite) TestSimple(t *testing.T, c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_new_owner")
	defer store.Close()
	d := newDDL(store, nil, nil, testLease)
	defer d.Stop()

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	cli := clus.RandClient()
	d.setWorker(cli)
	val := d.worker.isOwner()
	c.Assert(val, IsTrue)
}
