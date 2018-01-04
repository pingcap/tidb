// Copyright 2016 PingCAP, Inc.
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

package etcdutil

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/types"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/testutil"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testEtcdutilSuite{})

type testEtcdutilSuite struct {
}

func newTestSingleConfig() *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = ioutil.TempDir("/tmp", "test_etcd")
	cfg.WalDir = ""

	pu, _ := url.Parse(testutil.AllocTestURL())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(testutil.AllocTestURL())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func cleanConfig(cfg *embed.Config) {
	// Clean data directory
	os.RemoveAll(cfg.Dir)
}

func (s *testEtcdutilSuite) TestMemberHelpers(c *C) {
	cfg1 := newTestSingleConfig()
	etcd1, err := embed.StartEtcd(cfg1)
	c.Assert(err, IsNil)

	ep1 := cfg1.LCUrls[0].String()
	client1, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep1},
	})
	c.Assert(err, IsNil)

	<-etcd1.Server.ReadyNotify()

	// Test ListEtcdMembers
	listResp1, err := ListEtcdMembers(client1)
	c.Assert(err, IsNil)
	c.Assert(len(listResp1.Members), Equals, 1)
	// types.ID is an alias of uint64.
	c.Assert(listResp1.Members[0].ID, Equals, uint64(etcd1.Server.ID()))

	// Test AddEtcdMember
	// Make a new etcd config.
	cfg2 := newTestSingleConfig()
	cfg2.Name = "etcd2"
	cfg2.InitialCluster = cfg1.InitialCluster + fmt.Sprintf(",%s=%s", cfg2.Name, &cfg2.LPUrls[0])
	cfg2.ClusterState = embed.ClusterStateFlagExisting

	// Add it to the cluster above.
	peerURL := cfg2.LPUrls[0].String()
	addResp, err := AddEtcdMember(client1, []string{peerURL})
	c.Assert(err, IsNil)

	etcd2, err := embed.StartEtcd(cfg2)
	c.Assert(err, IsNil)
	c.Assert(addResp.Member.ID, Equals, uint64(etcd2.Server.ID()))

	ep2 := cfg2.LCUrls[0].String()
	client2, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep2},
	})
	c.Assert(err, IsNil)

	<-etcd2.Server.ReadyNotify()
	c.Assert(err, IsNil)

	listResp2, err := ListEtcdMembers(client2)
	c.Assert(err, IsNil)
	c.Assert(len(listResp2.Members), Equals, 2)
	for _, m := range listResp2.Members {
		switch m.ID {
		case uint64(etcd1.Server.ID()):
		case uint64(etcd2.Server.ID()):
		default:
			c.Fatalf("unknown member: %v", m)
		}
	}

	// Test CheckClusterID
	urlmap, err := types.NewURLsMap(cfg2.InitialCluster)
	c.Assert(err, IsNil)
	err = CheckClusterID(etcd1.Server.Cluster().ID(), urlmap, &tls.Config{})
	c.Assert(err, IsNil)

	// Test RemoveEtcdMember
	_, err = RemoveEtcdMember(client1, uint64(etcd2.Server.ID()))
	c.Assert(err, IsNil)

	listResp3, err := ListEtcdMembers(client1)
	c.Assert(err, IsNil)
	c.Assert(len(listResp3.Members), Equals, 1)
	c.Assert(listResp3.Members[0].ID, Equals, uint64(etcd1.Server.ID()))

	etcd1.Close()
	etcd2.Close()
	cleanConfig(cfg1)
	cleanConfig(cfg2)
}
